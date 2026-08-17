#!/usr/bin/env python3
"""Isolated PaddleOCR worker for football scoreboard timing.

The module keeps PaddleOCR imports inside ``load_ocr_engine`` so its parsing
and location rules remain unit-testable on machines without the model stack.
The command-line protocol accepts one JSON object on stdin and emits one JSON
object on stdout.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import queue
import re
import socket
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from scoreboard_ocr import (
    ClockContinuityStateMachine,
    ScoreboardOcrError,
    ScoreboardProfile,
    parse_clock_texts,
    parse_score_texts,
    resolve_scoreboard_profile,
)


CLOCK_PATTERN = re.compile(r"(?<!\d)(\d{1,3})\s*[:.]\s*([0-5]\d)(?!\d)")
COMPACT_CLOCK_PATTERN = re.compile(r"(?<!\d)(\d{2,3})([0-5]\d)(?!\d)")
SCORE_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[-:]\s*(\d{1,2})(?!\d)")
SUPPORTED_EVENT_CODES = frozenset({"G", "OG", "YC", "RC"})
MAX_REASONABLE_MATCH_MINUTE = 150
MAX_REASONABLE_SCORE = 20
OCR_CROP_KINDS = frozenset({"clock", "score"})
MIN_PROFILE_TRUSTED_CLOCK_FRAMES = 3
MIN_PROFILE_TRUSTED_CLOCK_RATE = 0.20
MIN_PROFILE_CLOCK_PROGRESSION_SECONDS = 1


class WorkerError(RuntimeError):
    def __init__(
        self,
        kind: str,
        message: str,
        *,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = str(kind)
        self.message = str(message)
        self.diagnostics = dict(diagnostics or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "message": self.message,
            "diagnostics": self.diagnostics,
        }


@dataclass(frozen=True)
class ParsedScoreboard:
    clock_seconds: int | None
    score: tuple[int, int] | None
    ambiguous_clock: bool = False
    ambiguous_score: bool = False


@dataclass(frozen=True)
class FrameReading:
    frame_index: int
    frame_seconds: float
    texts: tuple[str, ...]
    clock_seconds: int | None
    score: tuple[int, int] | None
    mean_confidence: float | None = None
    ambiguous_clock: bool = False
    ambiguous_score: bool = False
    clock_texts: tuple[str, ...] = ()
    score_texts: tuple[str, ...] = ()
    continuity_status: str | None = None
    continuity_reason: str | None = None
    scoreboard_visible: bool = True


@dataclass(frozen=True)
class OcrCropRequest:
    """One already-cropped scoreboard image queued for recognition."""

    match_id: str
    video_pts: float
    kind: str
    profile: str
    crop: Any
    minimum_confidence: float = 0.35

    def validate(self) -> None:
        if not str(self.match_id).strip():
            raise ValueError("match_id must not be empty")
        if self.kind not in OCR_CROP_KINDS:
            raise ValueError(f"unsupported OCR crop kind: {self.kind!r}")
        if not str(self.profile).strip():
            raise ValueError("profile must not be empty")
        if self.crop is None:
            raise ValueError("crop must not be None")
        if not math.isfinite(float(self.video_pts)):
            raise ValueError("video_pts must be finite")
        if not 0 <= float(self.minimum_confidence) <= 1:
            raise ValueError("minimum_confidence must be in [0, 1]")


@dataclass(frozen=True)
class OcrCropResult:
    """Recognition output with the source timeline identity preserved."""

    match_id: str
    video_pts: float
    kind: str
    profile: str
    texts: tuple[str, ...]
    confidences: tuple[float, ...]
    batch_size: int
    inference_seconds: float

    @property
    def profile_id(self) -> str:
        return self.profile

    def as_dict(self) -> dict[str, Any]:
        return {
            "match_id": self.match_id,
            "video_pts": self.video_pts,
            "kind": self.kind,
            "profile": self.profile,
            "profile_id": self.profile,
            "texts": list(self.texts),
            "confidences": list(self.confidences),
            "batch_size": self.batch_size,
            "inference_seconds": self.inference_seconds,
        }


def _score_text(score: tuple[int, int] | None) -> str | None:
    if score is None:
        return None
    return f"{score[0]}-{score[1]}"


def _clock_text(clock_seconds: int | None) -> str | None:
    if clock_seconds is None:
        return None
    return f"{clock_seconds // 60:02d}:{clock_seconds % 60:02d}"


def parse_target_score(value: Any) -> tuple[int, int]:
    raw = str(value or "").strip().replace("\u2013", "-").replace("\u2014", "-")
    match = re.fullmatch(r"(\d{1,2})\s*[-:]\s*(\d{1,2})", raw)
    if match is None:
        raise WorkerError(
            "ocr_score_unreadable",
            f"invalid API target_score: {value!r}",
        )
    score = (int(match.group(1)), int(match.group(2)))
    if max(score) > MAX_REASONABLE_SCORE:
        raise WorkerError(
            "ocr_score_unreadable",
            f"API target_score is outside the supported range: {value!r}",
        )
    return score


def parse_event_minute(value: Any) -> int:
    raw = str(value).strip()
    match = re.fullmatch(r"(\d{1,3})(?:\s*\+\s*(\d{1,2}))?", raw)
    if match is None:
        raise WorkerError(
            "ocr_clock_unreadable",
            f"invalid API event minute: {value!r}",
        )
    minute = int(match.group(1)) + int(match.group(2) or 0)
    if minute > MAX_REASONABLE_MATCH_MINUTE:
        raise WorkerError(
            "ocr_clock_unreadable",
            f"API event minute is outside the supported range: {value!r}",
        )
    return minute


def parse_scoreboard_texts(texts: Iterable[str]) -> ParsedScoreboard:
    normalized = [
        str(text).strip().replace("\u2013", "-").replace("\u2014", "-")
        for text in texts
        if str(text).strip()
    ]
    candidates = normalized + ([" ".join(normalized)] if normalized else [])
    clocks: set[int] = set()
    scores: set[tuple[int, int]] = set()
    for candidate in candidates:
        for match in CLOCK_PATTERN.finditer(candidate):
            minute = int(match.group(1))
            second = int(match.group(2))
            if minute <= MAX_REASONABLE_MATCH_MINUTE:
                clocks.add(minute * 60 + second)
        for match in SCORE_PATTERN.finditer(candidate):
            score = (int(match.group(1)), int(match.group(2)))
            if max(score) <= MAX_REASONABLE_SCORE:
                scores.add(score)
    if not clocks:
        # PaddleOCR occasionally drops the clock separator (for example,
        # ``35:02`` becomes ``3502``). Restrict the fallback to four or five
        # digits so a compact score such as ``330`` is not treated as a clock.
        for candidate in candidates:
            for match in COMPACT_CLOCK_PATTERN.finditer(candidate):
                minute = int(match.group(1))
                second = int(match.group(2))
                if minute <= MAX_REASONABLE_MATCH_MINUTE:
                    clocks.add(minute * 60 + second)
    return ParsedScoreboard(
        clock_seconds=next(iter(clocks)) if len(clocks) == 1 else None,
        score=next(iter(scores)) if len(scores) == 1 else None,
        ambiguous_clock=len(clocks) > 1,
        ambiguous_score=len(scores) > 1,
    )


def frame_reading(
    frame_index: int,
    frame_seconds: float,
    texts: Iterable[str],
    confidences: Iterable[float] = (),
) -> FrameReading:
    text_tuple = tuple(str(text) for text in texts if str(text).strip())
    parsed = parse_scoreboard_texts(text_tuple)
    confidence_values = [float(value) for value in confidences]
    return FrameReading(
        frame_index=int(frame_index),
        frame_seconds=float(frame_seconds),
        texts=text_tuple,
        clock_seconds=parsed.clock_seconds,
        score=parsed.score,
        mean_confidence=(
            sum(confidence_values) / len(confidence_values)
            if confidence_values else None
        ),
        ambiguous_clock=parsed.ambiguous_clock,
        ambiguous_score=parsed.ambiguous_score,
    )


def split_frame_reading(
    frame_index: int,
    frame_seconds: float,
    clock_texts: Iterable[str],
    score_texts: Iterable[str],
    *,
    clock_confidences: Iterable[float] = (),
    score_confidences: Iterable[float] = (),
    tracker: ClockContinuityStateMachine,
    period: str | int | None = None,
) -> FrameReading:
    """Build one reading from independent clock and score recognition crops."""
    clock_values = tuple(str(text) for text in clock_texts if str(text).strip())
    score_values = tuple(str(text) for text in score_texts if str(text).strip())
    parsed_clock = parse_clock_texts(clock_values)
    parsed_score = parse_score_texts(score_values)
    scoreboard_visible = bool(clock_values or score_values)
    continuity = tracker.update(
        float(frame_seconds),
        clock_values,
        scoreboard_visible=scoreboard_visible,
        period=period,
    )
    confidence_values = [
        *[float(value) for value in clock_confidences],
        *[float(value) for value in score_confidences],
    ]
    return FrameReading(
        frame_index=int(frame_index),
        frame_seconds=float(frame_seconds),
        texts=clock_values + score_values,
        clock_seconds=continuity.clock_seconds,
        score=parsed_score.score,
        mean_confidence=(
            sum(confidence_values) / len(confidence_values)
            if confidence_values else None
        ),
        ambiguous_clock=parsed_clock.ambiguous,
        ambiguous_score=parsed_score.ambiguous,
        clock_texts=clock_values,
        score_texts=score_values,
        continuity_status=continuity.status,
        continuity_reason=continuity.reason,
        scoreboard_visible=scoreboard_visible,
    )


def _base_diagnostics(
    readings: Sequence[FrameReading],
    *,
    sample_interval_seconds: float,
    candidate_start_seconds: float,
) -> dict[str, Any]:
    sampled_count = len(readings)
    clock_readable_count = sum(
        reading.clock_seconds is not None for reading in readings
    )
    score_readable_count = sum(reading.score is not None for reading in readings)
    return {
        "sample_interval_seconds": sample_interval_seconds,
        "candidate_start_seconds": candidate_start_seconds,
        "sampled_frame_count": len(readings),
        "text_frame_count": sum(bool(reading.texts) for reading in readings),
        "clock_readable_frame_count": clock_readable_count,
        "clock_readable_rate": (
            round(clock_readable_count / sampled_count, 4) if sampled_count else 0.0
        ),
        "score_readable_frame_count": score_readable_count,
        "score_readable_rate": (
            round(score_readable_count / sampled_count, 4) if sampled_count else 0.0
        ),
        "clock_repaired_frame_count": sum(
            reading.continuity_status == "repaired" for reading in readings
        ),
        "scoreboard_missing_frame_count": sum(
            not reading.scoreboard_visible for reading in readings
        ),
        "ambiguous_frame_count": sum(
            reading.ambiguous_clock or reading.ambiguous_score
            for reading in readings
        ),
        "readings": [
            {
                "frame_index": reading.frame_index,
                "frame_seconds": round(reading.frame_seconds, 3),
                "texts": list(reading.texts),
                "clock_texts": list(reading.clock_texts),
                "score_texts": list(reading.score_texts),
                "clock": _clock_text(reading.clock_seconds),
                "score": _score_text(reading.score),
                "mean_confidence": (
                    round(reading.mean_confidence, 4)
                    if reading.mean_confidence is not None else None
                ),
                "ambiguous_clock": reading.ambiguous_clock,
                "ambiguous_score": reading.ambiguous_score,
                "continuity_status": reading.continuity_status,
                "continuity_reason": reading.continuity_reason,
                "scoreboard_visible": reading.scoreboard_visible,
            }
            for reading in readings
        ],
    }


def _validate_profile_content_quality(
    readings: Sequence[FrameReading], *, profile_id: str | None = None
) -> dict[str, Any]:
    """Reject a configured ROI that produces only isolated clock-like text."""
    sampled_count = len(readings)
    trusted = [
        reading
        for reading in readings
        if reading.clock_seconds is not None
        and bool(reading.clock_texts)
        and reading.continuity_status in {"accepted", "resynchronized"}
    ]
    trusted_count = len(trusted)
    trusted_rate = trusted_count / sampled_count if sampled_count else 0.0
    trusted_clocks = [
        reading.clock_seconds
        for reading in trusted
        if reading.clock_seconds is not None
    ]
    clock_progression = (
        max(trusted_clocks) - min(trusted_clocks) if trusted_clocks else 0
    )
    diagnostics = {
        "profile_id": profile_id,
        "sampled_frame_count": sampled_count,
        "trusted_clock_frame_count": trusted_count,
        "trusted_clock_rate": round(trusted_rate, 4),
        "minimum_trusted_clock_frames": MIN_PROFILE_TRUSTED_CLOCK_FRAMES,
        "minimum_trusted_clock_rate": MIN_PROFILE_TRUSTED_CLOCK_RATE,
        "clock_progression_seconds": clock_progression,
        "minimum_clock_progression_seconds": MIN_PROFILE_CLOCK_PROGRESSION_SECONDS,
        "scoreboard_missing_frame_count": sum(
            not reading.scoreboard_visible for reading in readings
        ),
        "continuity_status_counts": {
            status: sum(reading.continuity_status == status for reading in readings)
            for status in ("accepted", "resynchronized", "repaired", "rejected", "missing")
        },
    }
    if (
        trusted_count < MIN_PROFILE_TRUSTED_CLOCK_FRAMES
        or trusted_rate < MIN_PROFILE_TRUSTED_CLOCK_RATE
        or clock_progression < MIN_PROFILE_CLOCK_PROGRESSION_SECONDS
    ):
        raise WorkerError(
            "clock_profile_mismatch",
            "configured clock ROI did not produce enough trusted match-clock observations",
            diagnostics=diagnostics,
        )
    return diagnostics


def _raise_for_missing_scoreboard(
    readings: Sequence[FrameReading], diagnostics: Mapping[str, Any]
) -> None:
    if not readings or not any(reading.texts for reading in readings):
        raise WorkerError(
            "scoreboard_missing",
            "no scoreboard text was detected in the configured top-left ROI",
            diagnostics=diagnostics,
        )


def _locate_goal(
    readings: Sequence[FrameReading],
    *,
    target_score: tuple[int, int],
    candidate_start_seconds: float,
    sample_interval_seconds: float,
    stable_frames: int,
    anchor_lead_seconds: float,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    readable = [reading for reading in readings if reading.score is not None]
    if not readable:
        if any(reading.ambiguous_score for reading in readings):
            raise WorkerError(
                "ocr_ambiguous",
                "OCR produced conflicting score values",
                diagnostics=diagnostics,
            )
        raise WorkerError(
            "ocr_score_unreadable",
            "scoreboard text was found but no score could be parsed",
            diagnostics=diagnostics,
        )

    candidates: list[tuple[FrameReading, FrameReading]] = []
    for index in range(0, len(readings) - stable_frames + 1):
        current = readings[index]
        if current.score != target_score:
            continue
        if index > 0 and readings[index - 1].score == target_score:
            continue
        stable_window = readings[index : index + stable_frames]
        if any(reading.score != target_score for reading in stable_window):
            continue
        if any(
            right.frame_index != left.frame_index + 1
            for left, right in zip(stable_window, stable_window[1:])
        ):
            continue
        previous = next(
            (
                reading
                for reading in reversed(readings[:index])
                if reading.score is not None and reading.score != target_score
            ),
            None,
        )
        if previous is not None:
            candidates.append((current, previous))

    if len(candidates) > 1:
        raise WorkerError(
            "ocr_ambiguous",
            "multiple stable transitions to the API target score were detected",
            diagnostics={
                **diagnostics,
                "transition_frame_seconds": [
                    round(candidate.frame_seconds, 3)
                    for candidate, _previous in candidates
                ],
            },
        )
    if candidates and any(
        reading.score == target_score
        for reading in readings[: readings.index(candidates[0][0])]
    ):
        raise WorkerError(
            "ocr_ambiguous",
            "the target score was visible before the candidate transition",
            diagnostics={
                **diagnostics,
                "target_score": _score_text(target_score),
                "transition_frame_seconds": round(
                    candidates[0][0].frame_seconds, 3
                ),
            },
        )
    if not candidates:
        raise WorkerError(
            "ocr_no_score_transition",
            "the API target score did not appear as a stable new score",
            diagnostics={
                **diagnostics,
                "target_score": _score_text(target_score),
                "stable_frames_required": stable_frames,
            },
        )

    transition, previous = candidates[0]
    transition_absolute = candidate_start_seconds + transition.frame_seconds
    anchor = max(candidate_start_seconds, transition_absolute - anchor_lead_seconds)
    diagnostics.update(
        {
            "target_score": _score_text(target_score),
            "previous_score": _score_text(previous.score),
            "transition_frame_seconds": round(transition.frame_seconds, 3),
            "transition_clock": _clock_text(transition.clock_seconds),
            "stable_frames_required": stable_frames,
            "anchor_lead_seconds": anchor_lead_seconds,
        }
    )
    return {
        "anchor_seconds": round(anchor, 3),
        "method": "paddleocr_score_transition",
        "precision": "score_transition",
        "target_score": _score_text(target_score),
        "diagnostics": diagnostics,
    }


def _matching_clock_groups(
    readings: Sequence[FrameReading],
    *,
    event_minute: int,
    sample_interval_seconds: float,
) -> list[list[FrameReading]]:
    matching = [
        reading
        for reading in readings
        if reading.clock_seconds is not None
        and reading.clock_seconds // 60 == event_minute
    ]
    groups: list[list[FrameReading]] = []
    maximum_missing_gap = max(45.0, sample_interval_seconds * 3.1)
    for reading in matching:
        previous_match = groups[-1][-1] if groups else None
        intervening_readable = (
            [
                item
                for item in readings
                if previous_match is not None
                and previous_match.frame_seconds < item.frame_seconds < reading.frame_seconds
                and item.clock_seconds is not None
            ]
            if previous_match is not None else []
        )
        starts_new_group = (
            not groups
            or (
                previous_match is not None
                and reading.frame_seconds - previous_match.frame_seconds
                > maximum_missing_gap
            )
            or any(
                item.clock_seconds is not None
                and item.clock_seconds // 60 != event_minute
                for item in intervening_readable
            )
        )
        if starts_new_group:
            groups.append([reading])
        else:
            groups[-1].append(reading)
    return groups


def _locate_card_interval(
    readings: Sequence[FrameReading],
    *,
    event_minute: int,
    candidate_start_seconds: float,
    sample_interval_seconds: float,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    if any(reading.ambiguous_clock for reading in readings):
        raise WorkerError(
            "ocr_ambiguous",
            "OCR produced conflicting match-clock values",
            diagnostics=diagnostics,
        )
    clock_readings = [
        reading for reading in readings if reading.clock_seconds is not None
    ]
    clocks = [reading.clock_seconds for reading in clock_readings]
    if not clocks:
        raise WorkerError(
            "ocr_clock_unreadable",
            "scoreboard text was found but no match clock could be parsed",
            diagnostics=diagnostics,
        )
    groups = _matching_clock_groups(
        readings,
        event_minute=event_minute,
        sample_interval_seconds=sample_interval_seconds,
    )
    if len(groups) != 1:
        raise WorkerError(
            "ocr_ambiguous",
            (
                "the API event minute was not present in OCR clocks"
                if not groups
                else "the API event minute appeared in multiple disjoint OCR intervals"
            ),
            diagnostics={
                **diagnostics,
                "api_event_minute": event_minute,
                "clock_range": [
                    _clock_text(min(clocks)),
                    _clock_text(max(clocks)),
                ],
                "matching_interval_count": len(groups),
            },
        )
    group = groups[0]
    for previous, current in zip(group, group[1:]):
        assert previous.clock_seconds is not None
        assert current.clock_seconds is not None
        if current.clock_seconds < previous.clock_seconds - 2:
            raise WorkerError(
                "ocr_ambiguous",
                "OCR match clocks move backwards inside the matching interval",
                diagnostics={
                    **diagnostics,
                    "backward_clock_pair": [
                        _clock_text(previous.clock_seconds),
                        _clock_text(current.clock_seconds),
                    ],
                },
            )
    interval_start = candidate_start_seconds + group[0].frame_seconds
    interval_end = (
        candidate_start_seconds
        + group[-1].frame_seconds
        + sample_interval_seconds
    )
    diagnostics.update(
        {
            "api_event_minute": event_minute,
            "ocr_interval_clock_start": _clock_text(group[0].clock_seconds),
            "ocr_interval_clock_end": _clock_text(group[-1].clock_seconds),
        }
    )
    return {
        "anchor_seconds": None,
        "candidate_interval_start_seconds": round(interval_start, 3),
        "candidate_interval_end_seconds": round(interval_end, 3),
        "method": "paddleocr_clock_interval",
        "precision": "interval_only",
        "requires_tdeed": True,
        "diagnostics": diagnostics,
    }


def locate_from_readings(
    readings: Sequence[FrameReading], request: Mapping[str, Any]
) -> dict[str, Any]:
    ordered = sorted(readings, key=lambda reading: reading.frame_index)
    code = str(request.get("event_code") or "").upper().strip()
    if code not in SUPPORTED_EVENT_CODES:
        raise WorkerError("ocr_invalid_request", f"unsupported event code: {code!r}")
    sample_interval = float(request.get("sample_interval_seconds", 1.0))
    candidate_start = float(request.get("candidate_start_seconds", 0.0))
    if sample_interval <= 0 or candidate_start < 0:
        raise WorkerError("ocr_invalid_request", "invalid OCR timeline parameters")
    diagnostics = _base_diagnostics(
        ordered,
        sample_interval_seconds=sample_interval,
        candidate_start_seconds=candidate_start,
    )
    _raise_for_missing_scoreboard(ordered, diagnostics)
    if code in {"G", "OG"}:
        stable_frames = int(request.get("stable_frames", 2))
        anchor_lead_seconds = float(request.get("anchor_lead_seconds", 3.0))
        if stable_frames < 2:
            raise WorkerError("ocr_invalid_request", "stable_frames must be at least 2")
        if anchor_lead_seconds < 0:
            raise WorkerError(
                "ocr_invalid_request",
                "anchor_lead_seconds must not be negative",
            )
        target_score = parse_target_score(request.get("target_score"))
        try:
            return _locate_goal(
                ordered,
                target_score=target_score,
                candidate_start_seconds=candidate_start,
                sample_interval_seconds=sample_interval,
                stable_frames=stable_frames,
                anchor_lead_seconds=anchor_lead_seconds,
                diagnostics=diagnostics,
            )
        except WorkerError as score_error:
            event_minute = request.get("event_minute")
            if (
                score_error.kind not in {
                    "ocr_score_unreadable",
                    "ocr_no_score_transition",
                    "ocr_ambiguous",
                }
                or event_minute is None
                or not str(event_minute).strip()
            ):
                raise
            try:
                interval = _locate_card_interval(
                    ordered,
                    event_minute=parse_event_minute(event_minute),
                    candidate_start_seconds=candidate_start,
                    sample_interval_seconds=sample_interval,
                    diagnostics=dict(diagnostics),
                )
            except WorkerError:
                raise score_error
            interval["method"] = "paddleocr_goal_clock_interval"
            interval["score_transition_error"] = score_error.as_dict()
            return interval
    return _locate_card_interval(
        ordered,
        event_minute=parse_event_minute(request.get("event_minute")),
        candidate_start_seconds=candidate_start,
        sample_interval_seconds=sample_interval,
        diagnostics=diagnostics,
    )


def _extract_text_confidences(value: Any) -> list[tuple[str, float]]:
    extracted: list[tuple[str, float]] = []
    visited: set[int] = set()

    def visit(item: Any) -> None:
        if item is None:
            return
        if not isinstance(item, (str, int, float, bool)):
            identity = id(item)
            if identity in visited:
                return
            visited.add(identity)
        if isinstance(item, Mapping):
            singular_text = item.get("rec_text")
            singular_score = item.get("rec_score")
            if isinstance(singular_text, str):
                try:
                    confidence = float(singular_score)
                except (TypeError, ValueError):
                    confidence = 1.0
                extracted.append((singular_text, confidence))
                return
            texts = item.get("rec_texts")
            scores = item.get("rec_scores")
            if isinstance(texts, Sequence) and not isinstance(texts, (str, bytes)):
                score_values = (
                    scores
                    if isinstance(scores, Sequence) and not isinstance(scores, (str, bytes))
                    else []
                )
                for index, text in enumerate(texts):
                    confidence = score_values[index] if index < len(score_values) else 1.0
                    try:
                        extracted.append((str(text), float(confidence)))
                    except (TypeError, ValueError):
                        extracted.append((str(text), 1.0))
                return
            for nested in item.values():
                visit(nested)
            return
        if isinstance(item, (list, tuple)):
            if (
                len(item) == 2
                and isinstance(item[0], str)
                and isinstance(item[1], (int, float))
            ):
                extracted.append((item[0], float(item[1])))
                return
            for nested in item:
                visit(nested)
            return
        json_value = getattr(item, "json", None)
        if callable(json_value):
            try:
                visit(json_value())
            except Exception:
                return
        elif json_value is not None:
            visit(json_value)

    visit(value)
    deduplicated: list[tuple[str, float]] = []
    seen: set[tuple[str, float]] = set()
    for text, confidence in extracted:
        key = (text, round(confidence, 6))
        if key not in seen:
            seen.add(key)
            deduplicated.append((text, confidence))
    return deduplicated


def load_ocr_engine(language: str) -> Any:
    try:
        with contextlib.redirect_stdout(sys.stderr):
            try:
                from paddleocr import TextRecognition

                return TextRecognition(
                    model_name=os.environ.get(
                        "GIF_OCR_RECOGNITION_MODEL", "PP-OCRv6_medium_rec"
                    )
                )
            except ImportError:
                from paddleocr import PaddleOCR

            try:
                return PaddleOCR(
                    lang=language,
                    use_doc_orientation_classify=False,
                    use_doc_unwarping=False,
                    use_textline_orientation=False,
                )
            except (TypeError, ValueError):
                try:
                    return PaddleOCR(
                        use_angle_cls=False,
                        lang=language,
                        show_log=False,
                    )
                except TypeError:
                    return PaddleOCR(use_angle_cls=False, lang=language)
    except Exception as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            f"PaddleOCR could not be imported or initialized: {exc}",
        ) from exc


def recognize_frame(
    engine: Any, frame_path: Path, *, minimum_confidence: float
) -> tuple[list[str], list[float]]:
    try:
        with contextlib.redirect_stdout(sys.stderr):
            if hasattr(engine, "predict"):
                raw = engine.predict(str(frame_path))
            elif hasattr(engine, "ocr"):
                try:
                    raw = engine.ocr(str(frame_path), cls=False)
                except TypeError:
                    raw = engine.ocr(str(frame_path))
            else:
                raise AttributeError("PaddleOCR engine has no ocr/predict method")
    except Exception as exc:
        raise WorkerError(
            "ocr_inference_failed",
            f"PaddleOCR failed for {frame_path.name}: {exc}",
        ) from exc
    items = [
        (text, confidence)
        for text, confidence in _extract_text_confidences(raw)
        if confidence >= minimum_confidence and text.strip()
    ]
    return [text for text, _confidence in items], [confidence for _text, confidence in items]


def _split_batch_output(raw: Any, expected_count: int) -> list[Any]:
    if expected_count == 1:
        if isinstance(raw, Mapping):
            return [raw]
        try:
            values = list(raw)
        except TypeError:
            return [raw]
        # PaddleOCR v3 returns one result object per input. PaddleOCR v2 may
        # instead return the detections directly; both shapes are understood
        # by _extract_text_confidences.
        return [values[0] if len(values) == 1 else values]

    if isinstance(raw, Mapping) or isinstance(raw, (str, bytes)):
        raise WorkerError(
            "ocr_inference_failed",
            "PaddleOCR returned one result for a multi-image batch",
            diagnostics={"expected_result_count": expected_count},
        )
    try:
        values = list(raw)
    except TypeError as exc:
        raise WorkerError(
            "ocr_inference_failed",
            "PaddleOCR returned a non-iterable batch result",
            diagnostics={"expected_result_count": expected_count},
        ) from exc
    if len(values) != expected_count:
        raise WorkerError(
            "ocr_inference_failed",
            "PaddleOCR batch result count does not match the input count",
            diagnostics={
                "expected_result_count": expected_count,
                "actual_result_count": len(values),
            },
        )
    return values


def recognize_batch(
    engine: Any,
    crops: Sequence[Any],
    *,
    minimum_confidences: Sequence[float],
) -> list[tuple[list[str], list[float]]]:
    """Recognize one backend batch while preserving input/result alignment."""
    if not crops:
        return []
    if len(crops) != len(minimum_confidences):
        raise ValueError("crops and minimum_confidences must have equal lengths")
    inputs = [
        str(crop) if isinstance(crop, (PathLike, Path)) else crop
        for crop in crops
    ]
    try:
        with contextlib.redirect_stdout(sys.stderr):
            if hasattr(engine, "predict"):
                try:
                    raw = engine.predict(inputs, batch_size=len(inputs))
                except TypeError:
                    raw = engine.predict(inputs)
            elif hasattr(engine, "ocr"):
                try:
                    raw = engine.ocr(inputs, cls=False)
                except TypeError:
                    raw = engine.ocr(inputs)
            else:
                raise AttributeError("PaddleOCR engine has no ocr/predict method")
            per_crop = _split_batch_output(raw, len(inputs))
    except WorkerError:
        raise
    except Exception as exc:
        raise WorkerError(
            "ocr_inference_failed",
            f"PaddleOCR batch inference failed: {exc}",
            diagnostics={"batch_size": len(inputs)},
        ) from exc

    recognized: list[tuple[list[str], list[float]]] = []
    for raw_result, minimum_confidence in zip(
        per_crop, minimum_confidences
    ):
        items = [
            (text, confidence)
            for text, confidence in _extract_text_confidences(raw_result)
            if confidence >= minimum_confidence and text.strip()
        ]
        recognized.append(
            (
                [text for text, _confidence in items],
                [confidence for _text, confidence in items],
            )
        )
    return recognized


@dataclass
class _QueuedCrop:
    request: OcrCropRequest
    future: Future[OcrCropResult]


EngineFactory = Callable[[str], Any]


class BatchOcrWorker:
    """Persistent, bounded, cross-match PaddleOCR batching worker.

    The submitted crop object must remain usable until its returned future is
    complete. Paths and in-memory images accepted by PaddleOCR are supported.
    """

    def __init__(
        self,
        *,
        language: str = "en",
        max_batch_size: int = 8,
        batch_wait_seconds: float = 0.02,
        queue_capacity: int = 128,
        engine_factory: EngineFactory = load_ocr_engine,
        batch_recognizer: Callable[
            ..., list[tuple[list[str], list[float]]]
        ] = recognize_batch,
    ) -> None:
        if not str(language).strip():
            raise ValueError("language must not be empty")
        if not 1 <= int(max_batch_size) <= 64:
            raise ValueError("max_batch_size must be in [1, 64]")
        if not 0 <= float(batch_wait_seconds) <= 1:
            raise ValueError("batch_wait_seconds must be in [0, 1]")
        if int(queue_capacity) < int(max_batch_size):
            raise ValueError("queue_capacity must be at least max_batch_size")

        self.language = str(language)
        self.max_batch_size = int(max_batch_size)
        self.batch_wait_seconds = float(batch_wait_seconds)
        self.queue_capacity = int(queue_capacity)
        self._engine_factory = engine_factory
        self._batch_recognizer = batch_recognizer
        self._queue: queue.Queue[_QueuedCrop] = queue.Queue(
            maxsize=self.queue_capacity
        )
        self._state_lock = threading.Lock()
        self._ready = threading.Event()
        self._stop_requested = threading.Event()
        self._accepting = True
        self._terminal_error: WorkerError | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="scoreboard-ocr-batch",
            daemon=True,
        )
        self._thread.start()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def wait_until_ready(self, timeout: float | None = None) -> bool:
        ready = self._ready.wait(timeout)
        if ready and self._terminal_error is not None:
            raise self._terminal_error
        return ready

    def submit(
        self,
        *,
        match_id: str,
        video_pts: float,
        kind: str,
        profile: Any,
        crop: Any,
        minimum_confidence: float = 0.35,
        timeout: float | None = None,
    ) -> Future[OcrCropResult]:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must not be negative")
        if isinstance(profile, Mapping):
            profile_value = profile.get("profile_id") or profile.get("name")
        else:
            profile_value = getattr(profile, "profile_id", profile)
        request = OcrCropRequest(
            match_id=str(match_id),
            video_pts=float(video_pts),
            kind=str(kind).lower().strip(),
            profile=str(profile_value or ""),
            crop=crop,
            minimum_confidence=float(minimum_confidence),
        )
        request.validate()
        future: Future[OcrCropResult] = Future()
        item = _QueuedCrop(request=request, future=future)
        with self._state_lock:
            if not self._accepting:
                error = self._terminal_error or WorkerError(
                    "ocr_worker_closed",
                    "scoreboard OCR batch worker is closed",
                )
                raise error
            try:
                self._queue.put(item, block=timeout is not None, timeout=timeout)
            except queue.Full as exc:
                raise WorkerError(
                    "ocr_queue_full",
                    "scoreboard OCR queue is full",
                    diagnostics={"queue_capacity": self.queue_capacity},
                ) from exc
        return future

    def close(
        self,
        *,
        wait: bool = True,
        cancel_pending: bool = False,
        timeout: float | None = None,
    ) -> bool:
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must not be negative")
        with self._state_lock:
            self._accepting = False
            self._stop_requested.set()
        if cancel_pending:
            self._cancel_queued(
                WorkerError(
                    "ocr_worker_closed",
                    "scoreboard OCR request was cancelled during shutdown",
                )
            )
        if wait:
            self._thread.join(timeout)
        return not self._thread.is_alive()

    def __enter__(self) -> "BatchOcrWorker":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    @staticmethod
    def _error_for_request(
        error: WorkerError, request: OcrCropRequest
    ) -> WorkerError:
        return WorkerError(
            error.kind,
            error.message,
            diagnostics={
                **error.diagnostics,
                "match_id": request.match_id,
                "video_pts": request.video_pts,
                "kind": request.kind,
                "profile": request.profile,
            },
        )

    def _cancel_queued(self, error: WorkerError) -> None:
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            try:
                if not item.future.done():
                    item.future.set_exception(
                        self._error_for_request(error, item.request)
                    )
            finally:
                self._queue.task_done()

    def _fail_terminal(self, error: WorkerError) -> None:
        with self._state_lock:
            self._terminal_error = error
            self._accepting = False
            self._stop_requested.set()
        self._ready.set()
        self._cancel_queued(error)

    def _run(self) -> None:
        try:
            engine = self._engine_factory(self.language)
        except WorkerError as exc:
            self._fail_terminal(exc)
            return
        except Exception as exc:
            self._fail_terminal(
                WorkerError(
                    "ocr_model_unavailable",
                    f"PaddleOCR could not be initialized: {exc}",
                )
            )
            return
        self._ready.set()

        while not (self._stop_requested.is_set() and self._queue.empty()):
            try:
                first = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            batch = [first]
            deadline = time.monotonic() + self.batch_wait_seconds
            while len(batch) < self.max_batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    batch.append(self._queue.get(timeout=remaining))
                except queue.Empty:
                    break
            try:
                self._process_batch(engine, batch)
            except Exception as exc:
                error = WorkerError(
                    "ocr_inference_failed",
                    f"unexpected OCR batch worker failure: {exc}",
                    diagnostics={"batch_size": len(batch)},
                )
                for item in batch:
                    if not item.future.done():
                        item.future.set_exception(
                            self._error_for_request(error, item.request)
                        )
            finally:
                for _item in batch:
                    self._queue.task_done()

    def _process_batch(self, engine: Any, batch: Sequence[_QueuedCrop]) -> None:
        active = [
            item for item in batch
            if item.future.set_running_or_notify_cancel()
        ]
        if not active:
            return
        started = time.perf_counter()
        try:
            recognized = self._batch_recognizer(
                engine,
                [item.request.crop for item in active],
                minimum_confidences=[
                    item.request.minimum_confidence for item in active
                ],
            )
            if len(recognized) != len(active):
                raise WorkerError(
                    "ocr_inference_failed",
                    "OCR recognizer returned the wrong number of results",
                    diagnostics={
                        "expected_result_count": len(active),
                        "actual_result_count": len(recognized),
                    },
                )
            normalized: list[tuple[tuple[str, ...], tuple[float, ...]]] = []
            for result in recognized:
                if (
                    not isinstance(result, (list, tuple))
                    or len(result) != 2
                ):
                    raise WorkerError(
                        "ocr_inference_failed",
                        "OCR recognizer returned a malformed item result",
                    )
                texts, confidences = result
                text_values = tuple(str(text) for text in texts)
                confidence_values = tuple(float(value) for value in confidences)
                if len(text_values) != len(confidence_values):
                    raise WorkerError(
                        "ocr_inference_failed",
                        "OCR text and confidence counts do not match",
                    )
                normalized.append((text_values, confidence_values))
        except WorkerError as exc:
            for item in active:
                item.future.set_exception(
                    self._error_for_request(exc, item.request)
                )
            return
        except Exception as exc:
            error = WorkerError(
                "ocr_inference_failed",
                f"scoreboard OCR batch inference failed: {exc}",
                diagnostics={"batch_size": len(active)},
            )
            for item in active:
                item.future.set_exception(
                    self._error_for_request(error, item.request)
                )
            return

        inference_seconds = time.perf_counter() - started
        for item, (texts, confidences) in zip(active, normalized):
            request = item.request
            item.future.set_result(
                OcrCropResult(
                    match_id=request.match_id,
                    video_pts=request.video_pts,
                    kind=request.kind,
                    profile=request.profile,
                    texts=texts,
                    confidences=confidences,
                    batch_size=len(active),
                    inference_seconds=round(inference_seconds, 6),
                )
            )


def _ffprobe_for_ffmpeg(ffmpeg: str) -> str:
    path = Path(ffmpeg)
    name = path.name
    probe_name = name.replace("ffmpeg", "ffprobe", 1)
    return str(path.with_name(probe_name)) if probe_name != name else "ffprobe"


def _remaining_seconds(
    deadline_monotonic: float | None,
    *,
    stage: str,
) -> float | None:
    if deadline_monotonic is None:
        return None
    remaining = deadline_monotonic - time.monotonic()
    if remaining <= 0:
        raise WorkerError(
            "inference_timeout",
            f"scoreboard OCR timed out during {stage}",
            diagnostics={"stage": stage},
        )
    return remaining


def probe_video_dimensions(
    candidate_path: Path,
    *,
    ffmpeg: str,
    timeout_seconds: float | None = None,
) -> tuple[int, int]:
    command = [
        _ffprobe_for_ffmpeg(ffmpeg),
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height",
        "-of",
        "json",
        str(candidate_path),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkerError(
            "inference_timeout",
            "FFprobe timed out while inspecting the candidate video",
            diagnostics={"stage": "ffprobe", "timeout_seconds": timeout_seconds},
        ) from exc
    except OSError as exc:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            f"cannot inspect video dimensions with FFprobe: {exc}",
        ) from exc
    try:
        document = json.loads(completed.stdout or "{}")
        stream = document["streams"][0]
        width = int(stream["width"])
        height = int(stream["height"])
    except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFprobe did not return valid video dimensions",
            diagnostics={
                "return_code": completed.returncode,
                "ffprobe_stderr": (completed.stderr or "")[-2000:],
            },
        ) from exc
    if completed.returncode != 0 or width <= 0 or height <= 0:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFprobe could not inspect the candidate video",
            diagnostics={
                "return_code": completed.returncode,
                "ffprobe_stderr": (completed.stderr or "")[-2000:],
            },
        )
    return width, height


def _resolve_worker_profile(value: Any) -> ScoreboardProfile:
    try:
        return resolve_scoreboard_profile(value)
    except ScoreboardOcrError as exc:
        raise WorkerError(
            exc.kind,
            exc.message,
            diagnostics=exc.diagnostics,
        ) from exc


def extract_profile_frames(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    sample_interval_seconds: float,
    profile: ScoreboardProfile,
    maximum_frames: int = 300,
    deadline_monotonic: float | None = None,
) -> tuple[list[tuple[Path, Path]], dict[str, Any]]:
    """Extract aligned clock/score crops for an explicit layout profile."""
    frame_width, frame_height = probe_video_dimensions(
        candidate_path,
        ffmpeg=ffmpeg,
        timeout_seconds=_remaining_seconds(
            deadline_monotonic,
            stage="ffprobe",
        ),
    )
    try:
        rois = profile.scaled_rois(frame_width, frame_height)
    except ScoreboardOcrError as exc:
        raise WorkerError(
            exc.kind,
            exc.message,
            diagnostics=exc.diagnostics,
        ) from exc
    clock_x1, clock_y1, clock_x2, clock_y2 = rois["clock_roi"]
    score_x1, score_y1, score_x2, score_y2 = rois["score_roi"]
    frame_rate = 1.0 / sample_interval_seconds
    filter_graph = (
        f"[0:v]fps={frame_rate:.8f}[sampled];"
        "[sampled]split=2[clock_source][score_source];"
        f"[clock_source]crop={clock_x2 - clock_x1}:{clock_y2 - clock_y1}:"
        f"{clock_x1}:{clock_y1},scale=iw*3:ih*3:flags=lanczos[clock];"
        f"[score_source]crop={score_x2 - score_x1}:{score_y2 - score_y1}:"
        f"{score_x1}:{score_y1},scale=iw*3:ih*3:flags=lanczos[score]"
    )
    clock_pattern = output_dir / "clock_%06d.png"
    score_pattern = output_dir / "score_%06d.png"
    command = [
        ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(candidate_path),
        "-an",
        "-filter_complex",
        filter_graph,
        "-map",
        "[clock]",
        "-frames:v",
        str(maximum_frames),
        str(clock_pattern),
        "-map",
        "[score]",
        "-frames:v",
        str(maximum_frames),
        str(score_pattern),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=_remaining_seconds(
                deadline_monotonic,
                stage="frame_extraction",
            ),
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkerError(
            "inference_timeout",
            "FFmpeg timed out while extracting clock and score crops",
            diagnostics={"stage": "frame_extraction"},
        ) from exc
    except OSError as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            f"cannot start FFmpeg for scoreboard extraction: {exc}",
        ) from exc
    if completed.returncode != 0:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFmpeg could not extract the configured clock and score ROIs",
            diagnostics={"ffmpeg_stderr": (completed.stderr or "")[-2000:]},
        )
    clock_frames = sorted(output_dir.glob("clock_*.png"))
    score_frames = sorted(output_dir.glob("score_*.png"))
    if not clock_frames or len(clock_frames) != len(score_frames):
        raise WorkerError(
            "scoreboard_missing",
            "the candidate video did not produce aligned clock and score crops",
            diagnostics={
                "clock_frame_count": len(clock_frames),
                "score_frame_count": len(score_frames),
            },
        )
    return list(zip(clock_frames, score_frames)), {
        "profile_id": profile.profile_id,
        "reference_resolution": [
            profile.reference_width,
            profile.reference_height,
        ],
        "frame_resolution": [frame_width, frame_height],
        "clock_roi": list(rois["clock_roi"]),
        "score_roi": list(rois["score_roi"]),
    }


def extract_scoreboard_frames(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    sample_interval_seconds: float,
    roi_width_ratio: float,
    roi_height_ratio: float,
    maximum_frames: int = 300,
    timeout_seconds: float | None = None,
) -> list[Path]:
    frame_rate = 1.0 / sample_interval_seconds
    crop = (
        f"crop=trunc(iw*{roi_width_ratio:.6f}/2)*2:"
        f"trunc(ih*{roi_height_ratio:.6f}/2)*2:0:0"
    )
    output_pattern = output_dir / "scoreboard_%06d.png"
    command = [
        ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(candidate_path),
        "-an",
        "-vf",
        f"fps={frame_rate:.8f},{crop},scale=iw*3:ih*3:flags=lanczos",
        "-frames:v",
        str(maximum_frames),
        str(output_pattern),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkerError(
            "inference_timeout",
            "FFmpeg timed out while extracting scoreboard frames",
            diagnostics={"stage": "frame_extraction"},
        ) from exc
    except OSError as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            f"cannot start FFmpeg for scoreboard extraction: {exc}",
        ) from exc
    if completed.returncode != 0:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFmpeg could not extract scoreboard frames",
            diagnostics={"ffmpeg_stderr": (completed.stderr or "")[-2000:]},
        )
    frames = sorted(output_dir.glob("scoreboard_*.png"))
    if not frames:
        raise WorkerError(
            "scoreboard_missing",
            "the candidate video did not produce any scoreboard frames",
        )
    return frames


def _recognize_paths(
    engine: Any,
    paths: Sequence[Path],
    *,
    minimum_confidence: float,
    batch_size: int,
) -> tuple[list[tuple[list[str], list[float]]], list[str]]:
    recognized: list[tuple[list[str], list[float]]] = []
    failed_paths: list[str] = []
    for offset in range(0, len(paths), batch_size):
        path_batch = paths[offset : offset + batch_size]
        try:
            batch_results = recognize_batch(
                engine,
                path_batch,
                minimum_confidences=[minimum_confidence] * len(path_batch),
            )
        except WorkerError:
            failed_paths.extend(path.name for path in path_batch)
            batch_results = [([], []) for _path in path_batch]
        recognized.extend(batch_results)
    return recognized, failed_paths


def _recognize_paths_shared(
    batch_worker: BatchOcrWorker,
    paths: Sequence[Path],
    *,
    kinds: Sequence[str],
    match_id: str,
    profile_id: str,
    candidate_start_seconds: float,
    sample_interval: float,
    minimum_confidence: float,
    deadline_monotonic: float,
) -> tuple[list[tuple[list[str], list[float]]], list[str]]:
    if len(paths) != len(kinds):
        raise ValueError("paths and kinds must have equal lengths")
    pending: list[tuple[int, Future[OcrCropResult]]] = []
    all_futures: list[Future[OcrCropResult]] = []
    recognized: list[tuple[list[str], list[float]]] = [
        ([], []) for _path in paths
    ]
    paired_crops = "score" in kinds

    def collect_oldest() -> None:
        index, future = pending.pop(0)
        remaining = _remaining_seconds(
            deadline_monotonic,
            stage="batch_inference",
        )
        assert remaining is not None
        result = future.result(timeout=remaining)
        recognized[index] = (list(result.texts), list(result.confidences))

    try:
        for index, (path, kind) in enumerate(zip(paths, kinds)):
            # Keep one request from filling the global queue with hundreds of
            # crops. A small rolling window leaves room for other matches and
            # still gives the batcher enough work to form full batches.
            if len(pending) >= batch_worker.max_batch_size:
                collect_oldest()
            remaining = _remaining_seconds(
                deadline_monotonic,
                stage="ocr_queue",
            )
            assert remaining is not None
            frame_index = index // 2 if paired_crops else index
            while True:
                try:
                    future = batch_worker.submit(
                        match_id=match_id,
                        video_pts=(
                            candidate_start_seconds + frame_index * sample_interval
                        ),
                        kind=kind,
                        profile=profile_id,
                        crop=path,
                        minimum_confidence=minimum_confidence,
                        timeout=min(remaining, 0.1),
                    )
                    break
                except WorkerError as exc:
                    if exc.kind != "ocr_queue_full":
                        raise
                    remaining = _remaining_seconds(
                        deadline_monotonic,
                        stage="ocr_queue",
                    )
                    assert remaining is not None
            pending.append((index, future))
            all_futures.append(future)
        while pending:
            collect_oldest()
        return recognized, []
    except FutureTimeoutError as exc:
        for future in all_futures:
            future.cancel()
        raise WorkerError(
            "inference_timeout",
            "shared PaddleOCR batch inference timed out",
            diagnostics={
                "stage": "batch_inference",
                "backend_unhealthy": True,
            },
        ) from exc
    except WorkerError as exc:
        for future in all_futures:
            future.cancel()
        error_stage = str(exc.diagnostics.get("stage") or "batch_inference")
        backend_unhealthy = exc.kind in {
            "ocr_inference_failed",
            "ocr_model_unavailable",
            "ocr_worker_closed",
        } or (exc.kind == "inference_timeout" and error_stage == "batch_inference")
        raise WorkerError(
            exc.kind,
            exc.message,
            diagnostics={
                **exc.diagnostics,
                "stage": error_stage,
                "backend_unhealthy": backend_unhealthy,
            },
        ) from exc


def _request_period(request: Mapping[str, Any]) -> int | str | None:
    explicit = request.get("period")
    if explicit is not None:
        return explicit
    raw_minute = str(request.get("event_minute") or "").strip()
    if not raw_minute:
        return None
    if re.fullmatch(r"45\s*\+\s*\d{1,2}", raw_minute):
        return 1
    if re.fullmatch(r"90\s*\+\s*\d{1,2}", raw_minute):
        return 2
    try:
        return 2 if int(raw_minute) > 45 else 1
    except ValueError:
        return None


def _profile_readings(
    recognized: Sequence[tuple[list[str], list[float]]],
    *,
    profile: ScoreboardProfile,
    sample_interval: float,
    period: int | str | None,
) -> tuple[list[FrameReading], list[dict[str, Any]]]:
    tracker = ClockContinuityStateMachine(profile)
    readings: list[FrameReading] = []
    continuity_diagnostics: list[dict[str, Any]] = []
    for index in range(0, len(recognized), 2):
        clock_texts, clock_confidences = recognized[index]
        score_texts, score_confidences = recognized[index + 1]
        frame_seconds = (index // 2) * sample_interval
        reading = split_frame_reading(
            index // 2,
            frame_seconds,
            clock_texts,
            score_texts,
            clock_confidences=clock_confidences,
            score_confidences=score_confidences,
            tracker=tracker,
            period=period,
        )
        readings.append(reading)
        continuity_diagnostics.append(
            {
                "frame_index": reading.frame_index,
                "video_seconds": reading.frame_seconds,
                "clock_seconds": reading.clock_seconds,
                "status": reading.continuity_status,
                "reason": reading.continuity_reason,
                "clock_texts": list(reading.clock_texts),
                "score_texts": list(reading.score_texts),
                "scoreboard_visible": reading.scoreboard_visible,
            }
        )
    return readings, continuity_diagnostics


def run_request(
    request: Mapping[str, Any],
    *,
    engine: Any | None = None,
    batch_worker: BatchOcrWorker | None = None,
    request_timeout_seconds: float | None = None,
) -> dict[str, Any]:
    candidate_path = Path(str(request.get("candidate_path") or ""))
    if not candidate_path.is_file():
        raise WorkerError(
            "ocr_invalid_request",
            f"candidate video does not exist: {candidate_path}",
        )
    sample_interval = float(request.get("sample_interval_seconds", 1.0))
    roi_width = float(request.get("roi_width_ratio", 0.5))
    roi_height = float(request.get("roi_height_ratio", 0.25))
    minimum_confidence = float(request.get("minimum_confidence", 0.35))
    inference_batch_size = int(request.get("inference_batch_size", 8))
    profile_value = request.get("scoreboard_profile")
    if sample_interval <= 0 or (
        profile_value is None
        and (not 0 < roi_width <= 1 or not 0 < roi_height <= 1)
    ):
        raise WorkerError("ocr_invalid_request", "invalid frame-sampling parameters")
    if not 0 <= minimum_confidence <= 1:
        raise WorkerError("ocr_invalid_request", "invalid OCR confidence threshold")
    if not 1 <= inference_batch_size <= 64:
        raise WorkerError("ocr_invalid_request", "invalid OCR inference batch size")
    if request_timeout_seconds is not None and request_timeout_seconds <= 0:
        raise WorkerError("ocr_invalid_request", "request timeout must be positive")
    deadline_monotonic = (
        time.monotonic() + request_timeout_seconds
        if request_timeout_seconds is not None else None
    )

    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="scoreboard_ocr_") as directory:
        ffmpeg = str(request.get("ffmpeg") or "ffmpeg")
        profile: ScoreboardProfile | None = None
        profile_diagnostics: dict[str, Any] | None = None
        if profile_value is not None:
            profile = _resolve_worker_profile(profile_value)
            frame_pairs, profile_diagnostics = extract_profile_frames(
                candidate_path,
                Path(directory),
                ffmpeg=ffmpeg,
                sample_interval_seconds=sample_interval,
                profile=profile,
                deadline_monotonic=deadline_monotonic,
            )
            frames = [path for pair in frame_pairs for path in pair]
            crop_kinds = [kind for _pair in frame_pairs for kind in ("clock", "score")]
        else:
            frames = extract_scoreboard_frames(
                candidate_path,
                Path(directory),
                ffmpeg=ffmpeg,
                sample_interval_seconds=sample_interval,
                roi_width_ratio=roi_width,
                roi_height_ratio=roi_height,
                timeout_seconds=_remaining_seconds(
                    deadline_monotonic,
                    stage="frame_extraction",
                ),
            )
            crop_kinds = ["clock"] * len(frames)
        extraction_seconds = time.perf_counter() - started
        if batch_worker is None and engine is None:
            engine = load_ocr_engine(str(request.get("language") or "en"))
        inference_started = time.perf_counter()
        if batch_worker is not None:
            profile_id = profile.profile_id if profile is not None else "legacy_top_left"
            recognized, failed_frames = _recognize_paths_shared(
                batch_worker,
                frames,
                kinds=crop_kinds,
                match_id=str(request.get("match_id") or candidate_path.parent.name),
                profile_id=profile_id,
                candidate_start_seconds=float(
                    request.get("candidate_start_seconds", 0.0)
                ),
                sample_interval=sample_interval,
                minimum_confidence=minimum_confidence,
                deadline_monotonic=(
                    deadline_monotonic
                    if deadline_monotonic is not None
                    else time.monotonic() + 3600.0
                ),
            )
        else:
            recognized, failed_frames = _recognize_paths(
                engine,
                frames,
                minimum_confidence=minimum_confidence,
                batch_size=inference_batch_size,
            )
        if failed_frames and len(failed_frames) == len(frames):
            raise WorkerError(
                "ocr_model_unavailable",
                "PaddleOCR failed on every extracted scoreboard frame",
                diagnostics={"failed_frame_count": len(failed_frames)},
            )
        continuity_diagnostics: list[dict[str, Any]] | None = None
        profile_quality: dict[str, Any] | None = None
        if profile is not None:
            readings, continuity_diagnostics = _profile_readings(
                recognized,
                profile=profile,
                sample_interval=sample_interval,
                period=_request_period(request),
            )
            profile_quality = _validate_profile_content_quality(
                readings,
                profile_id=profile.profile_id,
            )
        else:
            readings = [
                frame_reading(
                    index,
                    index * sample_interval,
                    texts,
                    confidences,
                )
                for index, (texts, confidences) in enumerate(recognized)
            ]
        inference_seconds = time.perf_counter() - inference_started
        result = locate_from_readings(readings, request)
        diagnostics = result["diagnostics"]
        diagnostics.update(
            {
                "candidate_path": str(candidate_path.resolve()),
                "minimum_confidence": minimum_confidence,
                "inference_batch_size": inference_batch_size,
                "extraction_seconds": round(extraction_seconds, 3),
                "inference_seconds": round(inference_seconds, 3),
                "failed_frame_count": len(failed_frames),
            }
        )
        if profile_diagnostics is not None:
            diagnostics["scoreboard_profile"] = profile_diagnostics
            diagnostics["scoreboard_profile"]["content_quality"] = profile_quality
            diagnostics["clock_score_separate"] = True
            diagnostics["continuity"] = continuity_diagnostics
        else:
            diagnostics["roi_width_ratio"] = roi_width
            diagnostics["roi_height_ratio"] = roi_height
        return result


def _request_document(
    request: Any,
    *,
    engine_cache: dict[str, Any] | None = None,
    batch_worker: BatchOcrWorker | None = None,
    request_timeout_seconds: float | None = None,
) -> tuple[dict[str, Any], int]:
    try:
        if not isinstance(request, dict):
            raise WorkerError("ocr_invalid_request", "OCR request must be a JSON object")
        language = str(request.get("language") or "en")
        engine = None
        if engine_cache is not None and batch_worker is None:
            engine = engine_cache.get(language)
            if engine is None:
                engine = load_ocr_engine(language)
                engine_cache[language] = engine
        result = run_request(
            request,
            engine=engine,
            batch_worker=batch_worker,
            request_timeout_seconds=request_timeout_seconds,
        )
        document = {"ok": True, "result": result}
        return_code = 0
    except WorkerError as exc:
        document = {"ok": False, "error": exc.as_dict()}
        return_code = 2
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        error = WorkerError("ocr_invalid_request", str(exc))
        document = {"ok": False, "error": error.as_dict()}
        return_code = 2
    except Exception as exc:
        error = WorkerError("ocr_worker_failed", str(exc))
        document = {"ok": False, "error": error.as_dict()}
        return_code = 2
    return document, return_code


class _SocketOcrRuntime:
    def __init__(
        self,
        *,
        engine_factory: EngineFactory = load_ocr_engine,
        batch_recognizer: Callable[
            ..., list[tuple[list[str], list[float]]]
        ] = recognize_batch,
        max_batch_size: int = 8,
        batch_wait_seconds: float = 0.02,
        queue_capacity: int = 128,
    ) -> None:
        self._engine_factory = engine_factory
        self._batch_recognizer = batch_recognizer
        self._max_batch_size = max_batch_size
        self._batch_wait_seconds = batch_wait_seconds
        self._queue_capacity = queue_capacity
        self._workers: dict[str, BatchOcrWorker] = {}
        self._lock = threading.Lock()
        self._unhealthy = False

    def worker_for(self, language: str) -> BatchOcrWorker:
        normalized = str(language or "en").strip() or "en"
        with self._lock:
            if self._unhealthy:
                raise WorkerError(
                    "ocr_worker_closed",
                    "persistent OCR backend is restarting",
                    diagnostics={"backend_unhealthy": True},
                )
            worker = self._workers.get(normalized)
            if worker is None:
                worker = BatchOcrWorker(
                    language=normalized,
                    max_batch_size=self._max_batch_size,
                    batch_wait_seconds=self._batch_wait_seconds,
                    queue_capacity=self._queue_capacity,
                    engine_factory=self._engine_factory,
                    batch_recognizer=self._batch_recognizer,
                )
                self._workers[normalized] = worker
            return worker

    def mark_unhealthy(self) -> None:
        with self._lock:
            self._unhealthy = True
            workers = list(self._workers.values())
        for worker in workers:
            worker.close(wait=False, cancel_pending=True)

    def close(self, *, timeout: float = 2.0) -> None:
        with self._lock:
            self._unhealthy = True
            workers = list(self._workers.values())
            self._workers.clear()
        deadline = time.monotonic() + max(0.0, timeout)
        for worker in workers:
            worker.close(
                cancel_pending=True,
                timeout=max(0.0, deadline - time.monotonic()),
            )


def _write_socket_document(stream: Any, document: Mapping[str, Any]) -> bool:
    payload = (
        json.dumps(
            document,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    try:
        sendall = getattr(stream, "sendall", None)
        if callable(sendall):
            sendall(payload)
        else:
            stream.write(payload)
            stream.flush()
        return True
    except (OSError, ValueError, socket.timeout):
        return False


def _serve_socket_connection(
    connection: socket.socket,
    *,
    runtime: _SocketOcrRuntime,
    stop_requested: threading.Event,
    request_executor: Callable[..., tuple[dict[str, Any], int]],
) -> None:
    backend_unhealthy = False
    try:
        with connection:
            connection.settimeout(10.0)
            with connection.makefile("rb") as stream:
                try:
                    line = stream.readline(10_000_001)
                except (OSError, socket.timeout):
                    return
                if not line:
                    return
                if len(line) > 10_000_000:
                    document = {
                        "ok": False,
                        "error": WorkerError(
                            "ocr_invalid_request",
                            "OCR request is too large",
                        ).as_dict(),
                    }
                else:
                    try:
                        request = json.loads(line.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        document = {
                            "ok": False,
                            "error": WorkerError(
                                "ocr_invalid_request",
                                str(exc),
                            ).as_dict(),
                        }
                    else:
                        if (
                            isinstance(request, dict)
                            and request.get("command") == "shutdown"
                        ):
                            document = {
                                "ok": True,
                                "result": {"shutdown": True},
                            }
                            stop_requested.set()
                        else:
                            try:
                                if not isinstance(request, dict):
                                    raise ValueError(
                                        "OCR request must be a JSON object"
                                    )
                                timeout_seconds = float(
                                    request.pop("_request_timeout_seconds", 180.0)
                                )
                                if not 0 < timeout_seconds <= 3600:
                                    raise ValueError(
                                        "request timeout must be in (0, 3600]"
                                    )
                                batch_worker = runtime.worker_for(
                                    str(request.get("language") or "en")
                                )
                                document, _return_code = request_executor(
                                    request,
                                    batch_worker=batch_worker,
                                    request_timeout_seconds=max(
                                        0.05,
                                        timeout_seconds * 0.9,
                                    ),
                                )
                            except (TypeError, ValueError, WorkerError) as exc:
                                error = (
                                    exc if isinstance(exc, WorkerError)
                                    else WorkerError("ocr_invalid_request", str(exc))
                                )
                                document = {"ok": False, "error": error.as_dict()}
                            error = document.get("error")
                            if isinstance(error, Mapping):
                                diagnostics = error.get("diagnostics")
                                backend_unhealthy = bool(
                                    isinstance(diagnostics, Mapping)
                                    and diagnostics.get("backend_unhealthy")
                                )
                _write_socket_document(
                    connection if hasattr(connection, "sendall") else stream,
                    document,
                )
    except (OSError, ValueError, socket.timeout):
        # Client disconnects and stream teardown errors do not indicate a
        # damaged OCR engine. Drop the response and keep serving other jobs.
        return
    except Exception:
        backend_unhealthy = True
    finally:
        if backend_unhealthy:
            runtime.mark_unhealthy()
            stop_requested.set()


def serve_socket(
    socket_path: Path,
    *,
    engine_factory: EngineFactory = load_ocr_engine,
    batch_recognizer: Callable[
        ..., list[tuple[list[str], list[float]]]
    ] = recognize_batch,
    request_executor: Callable[..., tuple[dict[str, Any], int]] = _request_document,
) -> int:
    """Serve local JSON-line requests while retaining one recognition model."""
    socket_path = socket_path.resolve()
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists() or socket_path.is_symlink():
        socket_path.unlink(missing_ok=True)
    idle_seconds = max(
        30.0, float(os.environ.get("GIF_OCR_WORKER_IDLE_SECONDS", "900"))
    )
    max_clients = max(
        1,
        int(os.environ.get("GIF_OCR_MAX_CLIENTS", "32")),
    )
    runtime = _SocketOcrRuntime(
        engine_factory=engine_factory,
        batch_recognizer=batch_recognizer,
    )
    stop_requested = threading.Event()
    client_slots = threading.BoundedSemaphore(max_clients)
    handlers: set[threading.Thread] = set()
    handlers_lock = threading.Lock()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(str(socket_path))
        os.chmod(socket_path, 0o600)
        server.listen(max_clients)
        server.settimeout(0.25)
        last_request = time.monotonic()
        while (
            not stop_requested.is_set()
            and time.monotonic() - last_request < idle_seconds
        ):
            try:
                connection, _address = server.accept()
            except socket.timeout:
                continue
            except OSError:
                if stop_requested.is_set():
                    break
                raise
            last_request = time.monotonic()
            if not client_slots.acquire(blocking=False):
                with connection:
                    connection.settimeout(0.25)
                    with connection.makefile("rb") as stream:
                        _write_socket_document(
                            connection if hasattr(connection, "sendall") else stream,
                            {
                                "ok": False,
                                "error": WorkerError(
                                    "ocr_queue_full",
                                    "persistent OCR client limit reached",
                                    diagnostics={"max_clients": max_clients},
                                ).as_dict(),
                            },
                        )
                continue

            def handle(client: socket.socket = connection) -> None:
                try:
                    _serve_socket_connection(
                        client,
                        runtime=runtime,
                        stop_requested=stop_requested,
                        request_executor=request_executor,
                    )
                finally:
                    client_slots.release()
                    with handlers_lock:
                        handlers.discard(threading.current_thread())

            handler = threading.Thread(
                target=handle,
                name="scoreboard-ocr-client",
                daemon=True,
            )
            with handlers_lock:
                handlers.add(handler)
            handler.start()
    finally:
        stop_requested.set()
        server.close()
        socket_path.unlink(missing_ok=True)
        runtime.close(timeout=2.0)
        deadline = time.monotonic() + 2.0
        while True:
            with handlers_lock:
                active_handlers = list(handlers)
            if not active_handlers or time.monotonic() >= deadline:
                break
            for handler in active_handlers:
                handler.join(timeout=min(0.05, max(0.0, deadline - time.monotonic())))
    return 0


def main() -> int:
    if len(sys.argv) == 3 and sys.argv[1] == "--serve-socket":
        return serve_socket(Path(sys.argv[2]))
    try:
        request = json.loads(sys.stdin.read())
    except json.JSONDecodeError as exc:
        request = None
        document = {
            "ok": False,
            "error": WorkerError("ocr_invalid_request", str(exc)).as_dict(),
        }
        return_code = 2
    else:
        document, return_code = _request_document(request)
    print(json.dumps(document, ensure_ascii=False, separators=(",", ":")))
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
