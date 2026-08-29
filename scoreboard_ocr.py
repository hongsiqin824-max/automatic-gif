#!/usr/bin/env python3
"""Process-isolated client for optional scoreboard OCR event location.

This module intentionally imports neither PaddleOCR nor Torch. The model and
video-frame dependencies live behind ``scoreboard_ocr_worker.py`` so the live
pipeline can safely import this client from its normal Python environment.
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


ROOT = Path(__file__).resolve().parent
DEFAULT_WORKER = ROOT / "scoreboard_ocr_worker.py"
DEFAULT_COARSE_SAMPLE_INTERVAL_SECONDS = 10.0
DEFAULT_RECOVERY_SAMPLE_INTERVAL_SECONDS = 5.0
DEFAULT_FINE_SCAN_RADIUS_SECONDS = 15.0
GOAL_LIKE_EVENT_CODES = frozenset({"G", "OG", "PG"})
SUPPORTED_EVENT_CODES = GOAL_LIKE_EVENT_CODES | frozenset({"YC", "RC"})
STRUCTURED_ERROR_KINDS = frozenset(
    {
        "scoreboard_missing",
        "ocr_clock_unreadable",
        "ocr_score_unreadable",
        "ocr_no_score_transition",
        "ocr_exact_second_not_found",
        "ocr_ambiguous",
        "inference_timeout",
        "ocr_model_unavailable",
        "clock_profile_mismatch",
    }
)


Runner = Callable[..., subprocess.CompletedProcess[str]]


class ScoreboardOcrError(RuntimeError):
    """A structured, retry-aware OCR failure returned by the worker."""

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


Roi = tuple[int, int, int, int]
_SECOND_HALF_CLOCK_MODES = frozenset({"continuous", "reset", "auto"})
_CLOCK_PATTERN = re.compile(r"(?<!\d)(\d{1,3})\s*[:.]\s*([0-5]\d)(?!\d)")
_COMPACT_CLOCK_PATTERN = re.compile(r"(?<!\d)(\d{2,3})([0-5]\d)(?!\d)")
_STOPPAGE_CLOCK_PATTERN = re.compile(
    r"(?<!\d)(45|90)\s*\+\s*(\d{1,2})(?:\s*[:.]\s*([0-5]\d))?(?!\d)"
)
_ADDED_STOPWATCH_PATTERN = re.compile(
    r"(?<!\d)(45|90)\s*[:.]\s*00\s*\+\s*(\d{1,2})\s*[:.]\s*([0-5]\d)(?!\d)"
)
_SCORE_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[-:]\s*(\d{1,2})(?!\d)")
_J_LEAGUE_SCORE_PATTERN = re.compile(
    r"(?<!\d)(\d)\s*\.\s*1\s*(\d)(?!\d)"
)
_MAX_REASONABLE_MATCH_MINUTE = 150
_MAX_REASONABLE_SCORE = 20


def _coerce_roi(value: Any, *, field_name: str) -> Roi:
    if isinstance(value, Mapping):
        value = [value.get(key) for key in ("x1", "y1", "x2", "y2")]
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) != 4
    ):
        raise ValueError(f"{field_name} must contain x1, y1, x2, y2")
    try:
        roi = tuple(int(coordinate) for coordinate in value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} coordinates must be integers") from exc
    x1, y1, x2, y2 = roi
    if x1 < 0 or y1 < 0 or x2 <= x1 or y2 <= y1:
        raise ValueError(f"{field_name} must satisfy 0 <= x1 < x2 and 0 <= y1 < y2")
    return roi  # type: ignore[return-value]


@dataclass(frozen=True)
class ScoreboardProfile:
    """Pixel ROIs for one known scoreboard layout at a reference resolution."""

    profile_id: str
    reference_width: int
    reference_height: int
    clock_roi: Roi
    score_roi: Roi | None = None
    second_half_clock_mode: str = "continuous"
    aspect_ratio_tolerance: float = 0.04

    def __post_init__(self) -> None:
        profile_id = str(self.profile_id).strip()
        if not profile_id:
            raise ValueError("profile_id must not be empty")
        width = int(self.reference_width)
        height = int(self.reference_height)
        if width <= 0 or height <= 0:
            raise ValueError("reference resolution must be positive")
        clock_roi = _coerce_roi(self.clock_roi, field_name="clock_roi")
        score_roi = (
            _coerce_roi(self.score_roi, field_name="score_roi")
            if self.score_roi is not None
            else None
        )
        for field_name, roi in (("clock_roi", clock_roi), ("score_roi", score_roi)):
            if roi is not None and (roi[2] > width or roi[3] > height):
                raise ValueError(
                    f"{field_name} {roi!r} exceeds reference resolution {width}x{height}"
                )
        mode = str(self.second_half_clock_mode).strip().lower()
        if mode not in _SECOND_HALF_CLOCK_MODES:
            raise ValueError(
                "second_half_clock_mode must be continuous, reset, or auto"
            )
        tolerance = float(self.aspect_ratio_tolerance)
        if not 0 <= tolerance <= 0.25:
            raise ValueError("aspect_ratio_tolerance must be in [0, 0.25]")
        object.__setattr__(self, "profile_id", profile_id)
        object.__setattr__(self, "reference_width", width)
        object.__setattr__(self, "reference_height", height)
        object.__setattr__(self, "clock_roi", clock_roi)
        object.__setattr__(self, "score_roi", score_roi)
        object.__setattr__(self, "second_half_clock_mode", mode)
        object.__setattr__(self, "aspect_ratio_tolerance", tolerance)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "ScoreboardProfile":
        resolution = value.get("reference_resolution")
        if (
            isinstance(resolution, Sequence)
            and not isinstance(resolution, (str, bytes))
            and len(resolution) == 2
        ):
            reference_width, reference_height = resolution
        else:
            reference_width = value.get("reference_width")
            reference_height = value.get("reference_height")
        return cls(
            profile_id=str(value.get("profile_id") or value.get("name") or ""),
            reference_width=int(reference_width),
            reference_height=int(reference_height),
            clock_roi=_coerce_roi(value.get("clock_roi"), field_name="clock_roi"),
            score_roi=(
                _coerce_roi(value.get("score_roi"), field_name="score_roi")
                if value.get("score_roi") is not None
                else None
            ),
            second_half_clock_mode=str(
                value.get("second_half_clock_mode") or "continuous"
            ),
            aspect_ratio_tolerance=float(
                value.get("aspect_ratio_tolerance", 0.04)
            ),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "reference_resolution": [self.reference_width, self.reference_height],
            "clock_roi": list(self.clock_roi),
            "score_roi": list(self.score_roi) if self.score_roi is not None else None,
            "second_half_clock_mode": self.second_half_clock_mode,
            "aspect_ratio_tolerance": self.aspect_ratio_tolerance,
        }

    def scaled_rois(self, frame_width: int, frame_height: int) -> dict[str, Roi]:
        """Scale both ROIs to a frame, rejecting a mismatched aspect ratio."""
        width = int(frame_width)
        height = int(frame_height)
        if width <= 0 or height <= 0:
            raise ValueError("frame resolution must be positive")
        reference_aspect = self.reference_width / self.reference_height
        frame_aspect = width / height
        relative_difference = abs(frame_aspect - reference_aspect) / reference_aspect
        if relative_difference > self.aspect_ratio_tolerance:
            raise ScoreboardOcrError(
                "clock_profile_mismatch",
                "video aspect ratio does not match the configured scoreboard profile",
                diagnostics={
                    "profile_id": self.profile_id,
                    "reference_resolution": [
                        self.reference_width,
                        self.reference_height,
                    ],
                    "frame_resolution": [width, height],
                    "aspect_ratio_difference": round(relative_difference, 6),
                    "aspect_ratio_tolerance": self.aspect_ratio_tolerance,
                },
            )
        scale_x = width / self.reference_width
        scale_y = height / self.reference_height

        def scale(roi: Roi) -> Roi:
            x1, y1, x2, y2 = roi
            return (
                max(0, min(width - 1, round(x1 * scale_x))),
                max(0, min(height - 1, round(y1 * scale_y))),
                max(1, min(width, round(x2 * scale_x))),
                max(1, min(height, round(y2 * scale_y))),
            )

        return {
            "clock_roi": scale(self.clock_roi),
            "score_roi": scale(self.score_roi) if self.score_roi is not None else None,
        }


_SCOREBOARD_PROFILES: dict[str, ScoreboardProfile] = {}


def register_scoreboard_profile(
    profile: ScoreboardProfile, *, replace: bool = False
) -> None:
    """Register a named layout for configuration that refers to it by id."""
    if not isinstance(profile, ScoreboardProfile):
        raise TypeError("profile must be a ScoreboardProfile")
    if profile.profile_id in _SCOREBOARD_PROFILES and not replace:
        raise ValueError(f"scoreboard profile already registered: {profile.profile_id}")
    _SCOREBOARD_PROFILES[profile.profile_id] = profile


def resolve_scoreboard_profile(
    value: ScoreboardProfile | Mapping[str, Any] | str,
) -> ScoreboardProfile:
    if isinstance(value, ScoreboardProfile):
        return value
    if isinstance(value, Mapping):
        try:
            return ScoreboardProfile.from_mapping(value)
        except (TypeError, ValueError) as exc:
            raise ScoreboardOcrError(
                "clock_profile_mismatch",
                f"invalid scoreboard profile: {exc}",
                diagnostics={"profile": dict(value)},
            ) from exc
    profile_id = str(value).strip()
    profile = _SCOREBOARD_PROFILES.get(profile_id)
    if profile is None:
        raise ScoreboardOcrError(
            "clock_profile_mismatch",
            f"unknown scoreboard profile: {profile_id or '<empty>'}",
            diagnostics={
                "profile_id": profile_id or None,
                "available_profile_ids": sorted(_SCOREBOARD_PROFILES),
            },
        )
    return profile


@dataclass(frozen=True)
class ParsedMatchClock:
    clock_seconds: int | None
    display_text: str | None = None
    precision: str | None = None
    clock_format: str | None = None
    base_minute: int | None = None
    added_minutes: int | None = None
    ambiguous: bool = False
    candidates: tuple[int, ...] = ()


@dataclass(frozen=True)
class ParsedScore:
    score: tuple[int, int] | None
    ambiguous: bool = False
    candidates: tuple[tuple[int, int], ...] = ()


def _normalized_ocr_candidates(texts: Sequence[str] | str) -> list[str]:
    values = [texts] if isinstance(texts, str) else list(texts)
    normalized = [
        str(text).strip().replace("\u2013", "-").replace("\u2014", "-")
        for text in values
        if str(text).strip()
    ]
    joined = "".join(normalized)
    spaced = " ".join(normalized)
    result = list(normalized)
    for candidate in (joined, spaced):
        if candidate and candidate not in result:
            result.append(candidate)
    return result


def parse_clock_texts(texts: Sequence[str] | str) -> ParsedMatchClock:
    """Parse only a match clock; score-like tokens are deliberately ignored."""
    parsed: dict[int, tuple[str, str, int | None, int | None, str]] = {}
    candidates = _normalized_ocr_candidates(texts)
    added_stopwatches: dict[
        int, tuple[str, str, int | None, int | None, str]
    ] = {}
    for candidate in candidates:
        for match in _ADDED_STOPWATCH_PATTERN.finditer(candidate):
            base = int(match.group(1))
            added = int(match.group(2))
            second = int(match.group(3))
            total_minute = base + added
            if total_minute <= _MAX_REASONABLE_MATCH_MINUTE:
                added_stopwatches[total_minute * 60 + second] = (
                    f"{base}:00+{added:02d}:{second:02d}",
                    "added_stopwatch",
                    base,
                    added,
                    "second",
                )
    if added_stopwatches:
        seconds = tuple(sorted(added_stopwatches))
        if len(seconds) != 1:
            return ParsedMatchClock(None, ambiguous=True, candidates=seconds)
        clock_seconds = seconds[0]
        display, clock_format, base, added, precision = added_stopwatches[
            clock_seconds
        ]
        return ParsedMatchClock(
            clock_seconds,
            display_text=display,
            precision=precision,
            clock_format=clock_format,
            base_minute=base,
            added_minutes=added,
            candidates=seconds,
        )
    for candidate in candidates:
        stoppage_spans: list[tuple[int, int]] = []
        for match in _STOPPAGE_CLOCK_PATTERN.finditer(candidate):
            base = int(match.group(1))
            added = int(match.group(2))
            second_text = match.group(3)
            second = int(second_text or 0)
            total_minute = base + added
            if total_minute <= _MAX_REASONABLE_MATCH_MINUTE:
                display = f"{base}+{added}"
                if second_text is not None:
                    display += f":{second:02d}"
                parsed[total_minute * 60 + second] = (
                    display,
                    "stoppage",
                    base,
                    added,
                    "second" if second_text is not None else "minute",
                )
                stoppage_spans.append(match.span())
        for match in _CLOCK_PATTERN.finditer(candidate):
            if any(
                match.start() < stop and match.end() > start
                for start, stop in stoppage_spans
            ):
                continue
            minute = int(match.group(1))
            second = int(match.group(2))
            if minute <= _MAX_REASONABLE_MATCH_MINUTE:
                parsed[minute * 60 + second] = (
                    f"{minute:02d}:{second:02d}",
                    "continuous",
                    None,
                    None,
                    "second",
                )
    if not parsed:
        for candidate in candidates:
            for match in _COMPACT_CLOCK_PATTERN.finditer(candidate):
                minute = int(match.group(1))
                second = int(match.group(2))
                if minute <= _MAX_REASONABLE_MATCH_MINUTE:
                    parsed[minute * 60 + second] = (
                        f"{minute:02d}:{second:02d}",
                        "compact",
                        None,
                        None,
                        "second",
                    )
    seconds = tuple(sorted(parsed))
    if len(seconds) != 1:
        return ParsedMatchClock(
            None,
            ambiguous=len(seconds) > 1,
            candidates=seconds,
        )
    clock_seconds = seconds[0]
    display, clock_format, base, added, precision = parsed[clock_seconds]
    return ParsedMatchClock(
        clock_seconds,
        display_text=display,
        precision=precision,
        clock_format=clock_format,
        base_minute=base,
        added_minutes=added,
        candidates=seconds,
    )


def parse_score_texts(texts: Sequence[str] | str) -> ParsedScore:
    """Parse only a football score; clock values cannot become score values."""
    scores: set[tuple[int, int]] = set()
    values = [texts] if isinstance(texts, str) else list(texts)
    originals = [
        str(text).strip().replace("\u2013", "-").replace("\u2014", "-")
        for text in values
        if str(text).strip()
    ]

    def collect(values: Sequence[str]) -> None:
        for candidate in values:
            for match in _SCORE_PATTERN.finditer(candidate):
                score = (int(match.group(1)), int(match.group(2)))
                if max(score) <= _MAX_REASONABLE_SCORE:
                    scores.add(score)

    collect(originals)
    if not scores:
        # Some J.League scorebugs place a J1 logo between the two scores.
        # Recognition-only OCR commonly renders ``0 [J1] 0`` as ``0.10``.
        for candidate in originals:
            for match in _J_LEAGUE_SCORE_PATTERN.finditer(candidate):
                scores.add((int(match.group(1)), int(match.group(2))))
    if not scores:
        # Joined candidates are only a fallback for split tokens such as
        # ["1", "-", "0"]. Joining already-complete scores can create a
        # synthetic cross-boundary value (for example, 0-0 + 1-0 -> 0-01).
        collect(["".join(originals), " ".join(originals)])
    if not scores and len(originals) == 2:
        # Some compact scorebugs render the two scores in separate boxes with
        # no visible separator. Detection OCR then returns ["4", "0"]. This
        # fallback is intentionally restricted to exactly two numeric tokens;
        # clock crops are parsed independently and never reach this function.
        split_score = [
            int(value) for value in originals if re.fullmatch(r"\d{1,2}", value)
        ]
        if len(split_score) == 2 and max(split_score) <= _MAX_REASONABLE_SCORE:
            scores.add((split_score[0], split_score[1]))
    ordered = tuple(sorted(scores))
    return ParsedScore(
        ordered[0] if len(ordered) == 1 else None,
        ambiguous=len(ordered) > 1,
        candidates=ordered,
    )


@dataclass(frozen=True)
class ClockContinuityResult:
    video_seconds: float
    clock_seconds: int | None
    observed_clock_seconds: int | None
    status: str
    reason: str
    display_text: str | None = None
    period: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "video_seconds": self.video_seconds,
            "clock_seconds": self.clock_seconds,
            "observed_clock_seconds": self.observed_clock_seconds,
            "status": self.status,
            "reason": self.reason,
            "display_text": self.display_text,
            "period": self.period,
        }


def _period_number(period: str | int | None) -> int | None:
    if isinstance(period, int):
        return period if period in {1, 2} else None
    normalized = str(period or "").strip().lower().replace("-", "_")
    if normalized in {"1", "h1", "first", "first_half", "1h"}:
        return 1
    if normalized in {"2", "h2", "second", "second_half", "2h"}:
        return 2
    return None


class ClockContinuityStateMachine:
    """Repair short OCR outliers without inventing clocks across replay gaps."""

    def __init__(
        self,
        profile: ScoreboardProfile | None = None,
        *,
        maximum_repair_gap_seconds: float = 5.0,
        maximum_consecutive_repairs: int = 3,
        resync_observations: int = 2,
        second_half_clock_mode: str | None = None,
    ) -> None:
        if maximum_repair_gap_seconds <= 0:
            raise ValueError("maximum_repair_gap_seconds must be positive")
        if maximum_consecutive_repairs < 1:
            raise ValueError("maximum_consecutive_repairs must be positive")
        if resync_observations < 2:
            raise ValueError("resync_observations must be at least 2")
        clock_mode = (
            profile.second_half_clock_mode
            if profile is not None
            else str(second_half_clock_mode or "continuous").strip().lower()
        )
        if clock_mode not in _SECOND_HALF_CLOCK_MODES:
            raise ValueError(
                "second_half_clock_mode must be continuous, reset, or auto"
            )
        self.profile = profile
        self.second_half_clock_mode = clock_mode
        self.maximum_repair_gap_seconds = float(maximum_repair_gap_seconds)
        self.maximum_consecutive_repairs = int(maximum_consecutive_repairs)
        self.resync_observations = int(resync_observations)
        self._last_input_video_seconds: float | None = None
        self._last_video_seconds: float | None = None
        self._last_clock_seconds: int | None = None
        self._last_period: int | None = None
        self._last_clock_format: str | None = None
        self._last_stoppage_base: int | None = None
        self._last_added_minutes: int | None = None
        self._consecutive_repairs = 0
        self._resync_clock_seconds: int | None = None
        self._resync_video_seconds: float | None = None
        self._resync_count = 0

    def reset(self) -> None:
        self._last_input_video_seconds = None
        self._last_video_seconds = None
        self._last_clock_seconds = None
        self._last_period = None
        self._last_clock_format = None
        self._last_stoppage_base = None
        self._last_added_minutes = None
        self._consecutive_repairs = 0
        self._clear_resync()

    def _normalize_period_clock(
        self, parsed: ParsedMatchClock, period_number: int | None
    ) -> int:
        assert parsed.clock_seconds is not None
        clock_seconds = parsed.clock_seconds
        mode = self.second_half_clock_mode
        if period_number == 2:
            if mode == "reset":
                return clock_seconds + 45 * 60
            if mode == "auto" and (
                clock_seconds < 45 * 60
                or (
                    parsed.clock_format == "stoppage"
                    and parsed.base_minute == 45
                )
            ):
                return clock_seconds + 45 * 60
        return clock_seconds

    def _clear_resync(self) -> None:
        self._resync_clock_seconds = None
        self._resync_video_seconds = None
        self._resync_count = 0

    @staticmethod
    def _repair_unparsed_text(raw_text: str, expected: int) -> int | None:
        translated = raw_text.upper().translate(
            str.maketrans({"O": "0", "I": "1", "L": "1", "S": "5", "B": "8"})
        )
        match = re.search(r"([0-9]{1,3})\s*[:.]\s*([0-9]{2})", translated)
        if match is None or int(match.group(2)) > 59:
            return None
        minute = int(match.group(1))
        second = int(match.group(2))
        expected_minute, expected_second = divmod(expected, 60)
        if minute == expected_minute:
            repaired = minute * 60 + second
            return repaired if abs(repaired - expected) <= 2 else None
        if (
            second == expected_second
            and str(expected_minute).endswith(str(minute))
        ):
            return expected_minute * 60 + second
        return None

    def _accept(
        self,
        *,
        video_seconds: float,
        clock_seconds: int,
        observed_clock_seconds: int | None,
        status: str,
        reason: str,
        display_text: str | None,
        period: str | None,
        period_number: int | None,
        parsed_clock: ParsedMatchClock | None = None,
        preserve_resync: bool = False,
    ) -> ClockContinuityResult:
        self._last_video_seconds = video_seconds
        self._last_clock_seconds = clock_seconds
        self._last_period = period_number or self._last_period
        self._consecutive_repairs = (
            self._consecutive_repairs + 1 if status == "repaired" else 0
        )
        if parsed_clock is not None:
            self._last_clock_format = parsed_clock.clock_format
            self._last_stoppage_base = parsed_clock.base_minute
            self._last_added_minutes = parsed_clock.added_minutes
        if not preserve_resync:
            self._clear_resync()
        return ClockContinuityResult(
            video_seconds=video_seconds,
            clock_seconds=clock_seconds,
            observed_clock_seconds=observed_clock_seconds,
            status=status,
            reason=reason,
            display_text=display_text,
            period=period,
        )

    def _consider_resync(
        self,
        *,
        video_seconds: float,
        observed_clock_seconds: int,
        parsed_clock: ParsedMatchClock,
        period: str | None,
        period_number: int | None,
    ) -> ClockContinuityResult | None:
        if (
            self._resync_clock_seconds is not None
            and self._resync_video_seconds is not None
        ):
            elapsed_video = video_seconds - self._resync_video_seconds
            clock_advance = observed_clock_seconds - self._resync_clock_seconds
            allowed_deviation = max(1, round(elapsed_video * 0.35))
            continuous = bool(
                elapsed_video > 0
                and clock_advance > 0
                and abs(clock_advance - round(elapsed_video)) <= allowed_deviation
            )
        else:
            continuous = False
        if continuous:
            self._resync_count += 1
        else:
            self._resync_count = 1
        self._resync_clock_seconds = observed_clock_seconds
        self._resync_video_seconds = video_seconds
        if self._resync_count < self.resync_observations:
            return None
        return self._accept(
            video_seconds=video_seconds,
            clock_seconds=observed_clock_seconds,
            observed_clock_seconds=observed_clock_seconds,
            status="resynchronized",
            reason="continuous_observations_resynchronized",
            display_text=parsed_clock.display_text,
            period=period,
            period_number=period_number,
            parsed_clock=parsed_clock,
        )

    def update(
        self,
        video_seconds: float,
        clock: ParsedMatchClock | Sequence[str] | str | None,
        *,
        scoreboard_visible: bool = True,
        period: str | int | None = None,
    ) -> ClockContinuityResult:
        video_time = float(video_seconds)
        if video_time < 0:
            raise ValueError("video_seconds must not be negative")
        if (
            self._last_input_video_seconds is not None
            and video_time <= self._last_input_video_seconds
        ):
            raise ValueError("video_seconds must increase monotonically")
        self._last_input_video_seconds = video_time
        period_number = _period_number(period)
        period_text = str(period) if period is not None else None
        raw_text = ""
        if isinstance(clock, ParsedMatchClock):
            parsed = clock
        elif clock is None:
            parsed = ParsedMatchClock(None)
        else:
            raw_text = clock if isinstance(clock, str) else " ".join(map(str, clock))
            parsed = parse_clock_texts(clock)
        if not scoreboard_visible:
            self._consecutive_repairs = self.maximum_consecutive_repairs
            self._clear_resync()
            return ClockContinuityResult(
                video_seconds=video_time,
                clock_seconds=None,
                observed_clock_seconds=None,
                status="missing",
                reason="scoreboard_temporarily_missing",
                period=period_text,
            )
        if parsed.ambiguous:
            self._consecutive_repairs = self.maximum_consecutive_repairs
            self._clear_resync()
            return ClockContinuityResult(
                video_seconds=video_time,
                clock_seconds=None,
                observed_clock_seconds=None,
                status="rejected",
                reason="ambiguous_clock",
                period=period_text,
            )
        observed = (
            self._normalize_period_clock(parsed, period_number)
            if parsed.clock_seconds is not None
            else None
        )
        if self._last_clock_seconds is None or self._last_video_seconds is None:
            if observed is None:
                return ClockContinuityResult(
                    video_seconds=video_time,
                    clock_seconds=None,
                    observed_clock_seconds=None,
                    status="rejected",
                    reason="clock_unreadable_without_history",
                    period=period_text,
                )
            return self._accept(
                video_seconds=video_time,
                clock_seconds=observed,
                observed_clock_seconds=observed,
                status="accepted",
                reason="initial_clock",
                display_text=parsed.display_text,
                period=period_text,
                period_number=period_number,
                parsed_clock=parsed,
            )
        elapsed_video = video_time - self._last_video_seconds
        expected = self._last_clock_seconds + max(0, round(elapsed_video))
        can_repair = (
            elapsed_video <= self.maximum_repair_gap_seconds
            and self._consecutive_repairs < self.maximum_consecutive_repairs
        )
        if parsed.clock_seconds is None and not raw_text:
            if can_repair:
                return self._accept(
                    video_seconds=video_time,
                    clock_seconds=expected,
                    observed_clock_seconds=None,
                    status="repaired",
                    reason="single_frame_clock_missing",
                    display_text=None,
                    period=period_text,
                    period_number=period_number,
                )
            self._consecutive_repairs = self.maximum_consecutive_repairs
            return ClockContinuityResult(
                video_seconds=video_time,
                clock_seconds=None,
                observed_clock_seconds=None,
                status="missing",
                reason="clock_not_detected",
                period=period_text,
            )
        is_half_transition = (
            period_number == 2
            and self._last_period == 1
            and observed is not None
            and 45 * 60 <= observed <= 46 * 60 + 30
        )
        if is_half_transition:
            return self._accept(
                video_seconds=video_time,
                clock_seconds=observed,
                observed_clock_seconds=observed,
                status="accepted",
                reason="second_half_started",
                display_text=parsed.display_text,
                period=period_text,
                period_number=period_number,
                parsed_clock=parsed,
            )
        if observed is not None:
            clock_advance = observed - self._last_clock_seconds
            maximum_advance = max(3, round(elapsed_video * 1.8) + 2)
            coarse_stoppage_advance = (
                parsed.clock_format == "stoppage"
                and parsed.precision == "minute"
                and self._last_clock_format == "stoppage"
                and parsed.base_minute == self._last_stoppage_base
                and parsed.added_minutes is not None
                and self._last_added_minutes is not None
                and parsed.added_minutes == self._last_added_minutes + 1
            )
            coarse_stoppage_started = (
                parsed.clock_format == "stoppage"
                and parsed.precision == "minute"
                and self._last_clock_format != "stoppage"
                and parsed.added_minutes == 1
                and 0 < clock_advance <= 61
            )
            if coarse_stoppage_advance or coarse_stoppage_started:
                return self._accept(
                    video_seconds=video_time,
                    clock_seconds=observed,
                    observed_clock_seconds=observed,
                    status="accepted",
                    reason=(
                        "coarse_stoppage_advanced"
                        if coarse_stoppage_advance
                        else "coarse_stoppage_started"
                    ),
                    display_text=parsed.display_text,
                    period=period_text,
                    period_number=period_number,
                    parsed_clock=parsed,
                )
            if 0 <= clock_advance <= maximum_advance:
                return self._accept(
                    video_seconds=video_time,
                    clock_seconds=observed,
                    observed_clock_seconds=observed,
                    status="accepted",
                    reason="clock_paused" if clock_advance == 0 else "clock_continuous",
                    display_text=parsed.display_text,
                    period=period_text,
                    period_number=period_number,
                    parsed_clock=parsed,
                )
        if observed is not None and abs(observed - expected) > 15 * 60:
            can_repair = False
        repaired_from_text = (
            self._repair_unparsed_text(raw_text, expected)
            if observed is None and raw_text and can_repair
            else None
        )
        if repaired_from_text is not None:
            expected = repaired_from_text
        # Keep testing a coherent post-gap clock track while bounded repairs
        # protect callers from the first one or two isolated OCR outliers.
        if observed is not None:
            resynchronized = self._consider_resync(
                video_seconds=video_time,
                observed_clock_seconds=observed,
                parsed_clock=parsed,
                period=period_text,
                period_number=period_number,
            )
            if resynchronized is not None:
                return resynchronized
        if can_repair:
            return self._accept(
                video_seconds=video_time,
                clock_seconds=expected,
                observed_clock_seconds=observed,
                status="repaired",
                reason=(
                    "ocr_character_repaired"
                    if repaired_from_text is not None
                    else "continuity_outlier_repaired"
                ),
                display_text=parsed.display_text or raw_text or None,
                period=period_text,
                period_number=period_number,
                preserve_resync=observed is not None,
            )
        if observed is None:
            self._clear_resync()
        self._consecutive_repairs = self.maximum_consecutive_repairs
        return ClockContinuityResult(
            video_seconds=video_time,
            clock_seconds=None,
            observed_clock_seconds=observed,
            status="rejected",
            reason="clock_discontinuity",
            display_text=parsed.display_text or raw_text or None,
            period=period_text,
        )


@dataclass(frozen=True)
class ScoreboardOcrRequest:
    candidate_path: Path
    event_code: str
    target_score: str | None = None
    event_minute: str | int | None = None
    event_second: int | None = None
    candidate_start_seconds: float = 0.0
    sample_interval_seconds: float = 1.0
    anchor_lead_seconds: float = 3.0
    stable_frames: int = 2
    roi_width_ratio: float = 0.5
    roi_height_ratio: float = 0.25
    minimum_confidence: float = 0.35
    language: str = "en"
    ffmpeg: str = "ffmpeg"
    scoreboard_profile: ScoreboardProfile | Mapping[str, Any] | str | None = None
    clock_only: bool = False
    # Optional direct rolling-buffer input.  The manifest is intentionally
    # explicit so the worker can use concat demuxer safe mode without ever
    # accepting arbitrary protocol URLs from an OCR request.
    candidate_input_format: str | None = None
    candidate_seek_seconds: float = 0.0
    candidate_duration_seconds: float | None = None

    def validate(self) -> None:
        code = self.event_code.upper().strip()
        if code not in SUPPORTED_EVENT_CODES:
            raise ValueError(f"unsupported event code: {self.event_code!r}")
        if not Path(self.candidate_path).is_file():
            raise ValueError(f"candidate video does not exist: {self.candidate_path}")
        if not isinstance(self.clock_only, bool):
            raise ValueError("clock_only must be a boolean")
        input_format = self.candidate_input_format
        if input_format not in {None, "ffconcat"}:
            raise ValueError("candidate_input_format must be ffconcat or None")
        if self.candidate_seek_seconds < 0:
            raise ValueError("candidate_seek_seconds must not be negative")
        if self.candidate_duration_seconds is not None and self.candidate_duration_seconds <= 0:
            raise ValueError("candidate_duration_seconds must be positive")
        if input_format is None and (
            self.candidate_seek_seconds > 0 or self.candidate_duration_seconds is not None
        ):
            raise ValueError("candidate offsets require candidate_input_format=ffconcat")
        if (
            code in GOAL_LIKE_EVENT_CODES
            and not self.clock_only
            and self.event_second is None
            and not str(self.target_score or "").strip()
        ):
            raise ValueError(
                "target_score or event_second is required for goal-like events"
            )
        if (self.clock_only or code in {"YC", "RC"}) and not str(
            self.event_minute if self.event_minute is not None else ""
        ).strip():
            raise ValueError("event_minute is required for clock-based OCR")
        if self.event_second is not None:
            if isinstance(self.event_second, bool) or not isinstance(
                self.event_second, int
            ):
                raise ValueError("event_second must be a cumulative integer second")
            if not 0 <= self.event_second <= _MAX_REASONABLE_MATCH_MINUTE * 60 + 59:
                raise ValueError("event_second is outside the supported match clock range")
        if self.candidate_start_seconds < 0:
            raise ValueError("candidate_start_seconds must not be negative")
        if self.sample_interval_seconds <= 0:
            raise ValueError("sample_interval_seconds must be positive")
        if self.anchor_lead_seconds < 0:
            raise ValueError("anchor_lead_seconds must not be negative")
        if self.stable_frames < 2:
            raise ValueError("stable_frames must be at least 2")
        if not 0 < self.roi_width_ratio <= 1:
            raise ValueError("roi_width_ratio must be in (0, 1]")
        if not 0 < self.roi_height_ratio <= 1:
            raise ValueError("roi_height_ratio must be in (0, 1]")
        if not 0 <= self.minimum_confidence <= 1:
            raise ValueError("minimum_confidence must be in [0, 1]")
        if self.scoreboard_profile is not None:
            profile = resolve_scoreboard_profile(self.scoreboard_profile)
            if not self.clock_only and profile.score_roi is None:
                raise ValueError("scoreboard profile must define score_roi outside clock_only mode")

    def to_payload(self) -> dict[str, Any]:
        self.validate()
        payload = {
            "candidate_path": str(Path(self.candidate_path).resolve()),
            "event_code": self.event_code.upper().strip(),
            "target_score": self.target_score,
            "event_minute": self.event_minute,
            "event_second": (
                self.event_second
                if self.event_code.upper().strip() in GOAL_LIKE_EVENT_CODES
                else None
            ),
            "candidate_start_seconds": float(self.candidate_start_seconds),
            "sample_interval_seconds": float(self.sample_interval_seconds),
            "anchor_lead_seconds": float(self.anchor_lead_seconds),
            "stable_frames": int(self.stable_frames),
            "roi_width_ratio": float(self.roi_width_ratio),
            "roi_height_ratio": float(self.roi_height_ratio),
            "minimum_confidence": float(self.minimum_confidence),
            "language": self.language,
            "ffmpeg": self.ffmpeg,
        }
        if self.scoreboard_profile is not None:
            payload["scoreboard_profile"] = resolve_scoreboard_profile(
                self.scoreboard_profile
            ).to_payload()
        # Preserve the legacy wire payload unless the new mode is explicit.
        if self.clock_only:
            payload["clock_only"] = True
        if self.candidate_input_format is not None:
            payload["candidate_input_format"] = self.candidate_input_format
            payload["candidate_seek_seconds"] = float(self.candidate_seek_seconds)
            if self.candidate_duration_seconds is not None:
                payload["candidate_duration_seconds"] = float(
                    self.candidate_duration_seconds
                )
        return payload


def _decode_worker_document(
    completed: subprocess.CompletedProcess[str],
) -> dict[str, Any]:
    stdout = (completed.stdout or "").strip()
    document: Any = None
    decode_error: Exception | None = None
    for candidate in [stdout, *reversed(stdout.splitlines())]:
        if not candidate.strip():
            continue
        try:
            parsed = json.loads(candidate)
        except (TypeError, json.JSONDecodeError) as exc:
            decode_error = exc
            continue
        if isinstance(parsed, dict):
            document = parsed
            break
    if document is None:
        detail = (completed.stderr or stdout or "no process output").strip()
        error = ScoreboardOcrError(
            "ocr_worker_failed",
            "scoreboard OCR worker did not return valid JSON",
            diagnostics={
                "return_code": completed.returncode,
                "process_output": detail[-2000:],
            },
        )
        if decode_error is not None:
            raise error from decode_error
        raise error
    return document


def _persistent_worker_enabled(runner: Runner, persistent: bool | None) -> bool:
    if persistent is not None:
        return bool(persistent)
    configured = os.environ.get("GIF_OCR_PERSISTENT_WORKER", "1").strip().lower()
    return (
        runner is subprocess.run
        and configured not in {"0", "false", "no", "off"}
        and hasattr(socket, "AF_UNIX")
    )


def _persistent_socket_path(worker: Path, python: str) -> Path:
    configured = os.environ.get("GIF_OCR_SOCKET_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    try:
        worker_version = worker.stat().st_mtime_ns
    except OSError:
        worker_version = 0
    identity = f"{worker.resolve()}\0{python}\0{worker_version}".encode("utf-8")
    digest = hashlib.sha256(identity).hexdigest()[:12]
    # macOS resolves its temporary directory under /var/folders, which can push
    # Unix-domain socket names past the platform limit. /tmp keeps the same
    # machine-local lifetime while leaving enough room for the unique suffix.
    return Path("/tmp") / f"automatic_gif_ocr_{os.getuid()}_{digest}.sock"


def _connect_worker(socket_path: Path, timeout_seconds: float) -> socket.socket:
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    connection.settimeout(timeout_seconds)
    try:
        connection.connect(str(socket_path))
    except Exception:
        connection.close()
        raise
    return connection


def _ensure_persistent_worker(
    *,
    socket_path: Path,
    worker: Path,
    python: str,
    startup_timeout_seconds: float = 15.0,
) -> None:
    try:
        connection = _connect_worker(socket_path, 0.25)
    except OSError:
        pass
    else:
        connection.close()
        return

    try:
        import fcntl
    except ImportError as exc:
        raise ScoreboardOcrError(
            "ocr_model_unavailable",
            "persistent OCR worker requires Unix file locking",
        ) from exc

    lock_path = socket_path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            try:
                connection = _connect_worker(socket_path, 0.25)
            except OSError:
                try:
                    subprocess.Popen(
                        [python, str(worker.resolve()), "--serve-socket", str(socket_path)],
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        close_fds=True,
                        start_new_session=True,
                    )
                except OSError as exc:
                    raise ScoreboardOcrError(
                        "ocr_model_unavailable",
                        f"cannot start persistent scoreboard OCR worker: {exc}",
                        diagnostics={"python_executable": python},
                    ) from exc
                deadline = time.monotonic() + startup_timeout_seconds
                while True:
                    try:
                        connection = _connect_worker(socket_path, 0.25)
                    except OSError as exc:
                        if time.monotonic() >= deadline:
                            raise ScoreboardOcrError(
                                "ocr_model_unavailable",
                                "persistent scoreboard OCR worker did not become ready",
                                diagnostics={"socket_path": str(socket_path)},
                            ) from exc
                        time.sleep(0.05)
                    else:
                        connection.close()
                        break
            else:
                connection.close()
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _run_persistent_worker(
    payload: Mapping[str, Any],
    *,
    worker: Path,
    python: str,
    timeout_seconds: float,
    cancel_event: Any = None,
) -> subprocess.CompletedProcess[str]:
    socket_path = _persistent_socket_path(worker, python)
    deadline = time.monotonic() + timeout_seconds
    response = b""
    last_communication_error: OSError | None = None
    for attempt in range(2):
        if cancel_event is not None and cancel_event.is_set():
            raise ScoreboardOcrError(
                "ocr_request_cancelled",
                "scoreboard OCR request was cancelled during shutdown",
                diagnostics={"worker_mode": "persistent", "stage": "shutdown"},
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        _ensure_persistent_worker(
            socket_path=socket_path,
            worker=worker,
            python=python,
            startup_timeout_seconds=min(15.0, remaining),
        )
        wire_payload = dict(payload)
        wire_payload["_request_timeout_seconds"] = remaining
        try:
            connection = _connect_worker(socket_path, remaining)
            with connection:
                request_bytes = (
                    json.dumps(
                        wire_payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                    + b"\n"
                )
                if cancel_event is None:
                    with connection.makefile("rwb") as stream:
                        stream.write(request_bytes)
                        stream.flush()
                        response = stream.readline(10_000_001)
                else:
                    connection.sendall(request_bytes)
                    chunks: list[bytes] = []
                    response_size = 0
                    while True:
                        if cancel_event.is_set():
                            raise ScoreboardOcrError(
                                "ocr_request_cancelled",
                                "scoreboard OCR request was cancelled during shutdown",
                                diagnostics={
                                    "worker_mode": "persistent",
                                    "stage": "shutdown",
                                },
                            )
                        read_remaining = deadline - time.monotonic()
                        if read_remaining <= 0:
                            raise socket.timeout("persistent OCR response timed out")
                        connection.settimeout(min(0.25, read_remaining))
                        try:
                            chunk = connection.recv(
                                min(65_536, 10_000_001 - response_size)
                            )
                        except socket.timeout:
                            continue
                        if not chunk:
                            break
                        chunks.append(chunk)
                        response_size += len(chunk)
                        if b"\n" in chunk or response_size >= 10_000_001:
                            break
                    response = b"".join(chunks)
            if response:
                break
            last_communication_error = OSError(
                "persistent worker closed the connection without a response"
            )
        except socket.timeout as exc:
            raise ScoreboardOcrError(
                "inference_timeout",
                f"scoreboard OCR exceeded the {timeout_seconds:g}s timeout",
                diagnostics={
                    "timeout_seconds": timeout_seconds,
                    "worker_mode": "persistent",
                },
            ) from exc
        except OSError as exc:
            last_communication_error = exc
        if attempt == 0:
            restart_deadline = min(deadline, time.monotonic() + 0.75)
            while time.monotonic() < restart_deadline:
                try:
                    probe = _connect_worker(socket_path, 0.1)
                except OSError:
                    break
                else:
                    probe.close()
                    time.sleep(0.05)
    if not response:
        if time.monotonic() >= deadline:
            raise ScoreboardOcrError(
                "inference_timeout",
                f"scoreboard OCR exceeded the {timeout_seconds:g}s timeout",
                diagnostics={
                    "timeout_seconds": timeout_seconds,
                    "worker_mode": "persistent",
                },
            ) from last_communication_error
        raise ScoreboardOcrError(
            "ocr_worker_failed",
            f"persistent scoreboard OCR communication failed: {last_communication_error}",
            diagnostics={"socket_path": str(socket_path)},
        ) from last_communication_error
    if len(response) > 10_000_000:
        raise ScoreboardOcrError(
            "ocr_worker_failed",
            "persistent scoreboard OCR worker returned an invalid response",
            diagnostics={"socket_path": str(socket_path)},
        )
    return subprocess.CompletedProcess(
        [python, str(worker.resolve()), "--serve-socket", str(socket_path)],
        0,
        stdout=response.decode("utf-8"),
        stderr="",
    )


def run_scoreboard_ocr(
    request: ScoreboardOcrRequest,
    *,
    python_executable: str | Path | None = None,
    worker_path: str | Path = DEFAULT_WORKER,
    timeout_seconds: float = 180.0,
    runner: Runner = subprocess.run,
    persistent: bool | None = None,
    cancel_event: Any = None,
) -> dict[str, Any]:
    """Run PaddleOCR in a child Python and return its structured result."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    payload = request.to_payload()
    if cancel_event is not None and cancel_event.is_set():
        raise ScoreboardOcrError(
            "ocr_request_cancelled",
            "scoreboard OCR request was cancelled during shutdown",
            diagnostics={"stage": "shutdown"},
        )
    worker = Path(worker_path)
    if not worker.is_file():
        raise ScoreboardOcrError(
            "ocr_model_unavailable",
            f"scoreboard OCR worker does not exist: {worker}",
        )
    python = str(python_executable or sys.executable)
    command: Sequence[str] = (python, str(worker.resolve()))
    started = time.perf_counter()
    worker_mode = "persistent" if _persistent_worker_enabled(runner, persistent) else "one_shot"
    if worker_mode == "persistent":
        completed = _run_persistent_worker(
            payload,
            worker=worker,
            python=python,
            timeout_seconds=timeout_seconds,
            cancel_event=cancel_event,
        )
    else:
        try:
            completed = runner(
                list(command),
                input=json.dumps(payload, ensure_ascii=False),
                check=False,
                text=True,
                capture_output=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise ScoreboardOcrError(
                "inference_timeout",
                f"scoreboard OCR exceeded the {timeout_seconds:g}s timeout",
                diagnostics={"timeout_seconds": timeout_seconds},
            ) from exc
        except OSError as exc:
            raise ScoreboardOcrError(
                "ocr_model_unavailable",
                f"cannot start scoreboard OCR worker: {exc}",
                diagnostics={"python_executable": python},
            ) from exc

    document = _decode_worker_document(completed)
    if document.get("ok") is not True:
        error = document.get("error")
        if not isinstance(error, dict):
            error = {}
        kind = str(error.get("kind") or "ocr_worker_failed")
        diagnostics = error.get("diagnostics")
        if not isinstance(diagnostics, dict):
            diagnostics = {}
        diagnostics.setdefault("return_code", completed.returncode)
        stderr = (completed.stderr or "").strip()
        if stderr:
            diagnostics.setdefault("worker_stderr", stderr[-2000:])
        raise ScoreboardOcrError(
            kind,
            str(error.get("message") or "scoreboard OCR worker failed"),
            diagnostics=diagnostics,
        )
    result = document.get("result")
    if completed.returncode != 0 or not isinstance(result, dict):
        raise ScoreboardOcrError(
            "ocr_worker_failed",
            "scoreboard OCR worker returned an invalid success document",
            diagnostics={"return_code": completed.returncode},
        )
    result = dict(result)
    diagnostics = result.get("diagnostics")
    if not isinstance(diagnostics, dict):
        diagnostics = {}
    diagnostics.setdefault(
        "worker_wall_seconds", round(time.perf_counter() - started, 3)
    )
    diagnostics.setdefault("worker_python", python)
    diagnostics.setdefault("worker_mode", worker_mode)
    result["diagnostics"] = diagnostics
    return result


_COARSE_SCAN_MISS_KINDS = frozenset(
    {
        "ocr_ambiguous",
        "ocr_clock_unreadable",
        "ocr_exact_second_not_found",
        "ocr_minute_boundary_not_found",
        "clock_profile_mismatch",
        "scoreboard_missing",
        "inference_timeout",
    }
)


def _remaining_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ScoreboardOcrError(
            "inference_timeout",
            "scoreboard OCR exhausted its timeout before the fine scan",
            diagnostics={"stage": "sampling_strategy"},
        )
    return remaining


def _coarse_timeout_budget(timeout_seconds: float) -> float:
    # Preserve time for one bounded recovery pass and the local one-second scan.
    return min(timeout_seconds, 60.0, max(5.0, timeout_seconds * 0.4))


def _minute_boundary_clock_seconds(event_minute: str | int | None) -> int | None:
    text = str(event_minute if event_minute is not None else "").strip().rstrip("'")
    if not text:
        return None
    base_text, separator, extra_text = text.partition("+")
    try:
        base = int(base_text)
        extra = int(extra_text) if separator else 0
    except ValueError:
        return None
    if base < 0 or extra < 0:
        return None
    return (base + extra) * 60


def _fine_scan_center(
    coarse_result: Mapping[str, Any], request: ScoreboardOcrRequest
) -> float | None:
    try:
        center = float(coarse_result.get("anchor_seconds"))
    except (TypeError, ValueError):
        return None
    if not (center >= 0 and center < float("inf")):
        return None
    if (
        request.event_second is not None
        and coarse_result.get("location_kind") == "match_clock_minute_boundary"
    ):
        boundary_second = _minute_boundary_clock_seconds(request.event_minute)
        if boundary_second is not None:
            center += request.event_second - boundary_second
    return center


def _sampling_result_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    diagnostics = result.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        diagnostics = {}
    return {
        "anchor_seconds": result.get("anchor_seconds"),
        "method": result.get("method"),
        "precision": result.get("precision"),
        "location_kind": result.get("location_kind"),
        "sampled_frame_count": diagnostics.get("sampled_frame_count"),
        "clock_readable_frame_count": diagnostics.get("clock_readable_frame_count"),
        "clock_readable_rate": diagnostics.get("clock_readable_rate"),
    }


def _attach_sampling_strategy(
    result: dict[str, Any],
    *,
    mode: str,
    coarse_interval: float,
    fine_interval: float,
    fine_window_radius: float,
    coarse_result: Mapping[str, Any] | None = None,
    coarse_error: ScoreboardOcrError | None = None,
    recovery_interval: float | None = None,
    recovery_result: Mapping[str, Any] | None = None,
    recovery_error: ScoreboardOcrError | None = None,
    local_fine_error: ScoreboardOcrError | None = None,
    fine_clip_start_seconds: float | None = None,
    final_anchor_source: str = "fine_scan",
) -> dict[str, Any]:
    diagnostics = result.get("diagnostics")
    if not isinstance(diagnostics, dict):
        diagnostics = {}
        result["diagnostics"] = diagnostics
    diagnostics["sampling_strategy"] = {
        "mode": mode,
        "coarse_sample_interval_seconds": coarse_interval,
        "fine_sample_interval_seconds": fine_interval,
        "fine_window_radius_seconds": fine_window_radius,
        "fine_clip_start_seconds": fine_clip_start_seconds,
        "coarse_scan": (
            _sampling_result_summary(coarse_result)
            if coarse_result is not None
            else None
        ),
        "coarse_error": (
            coarse_error.as_dict() if coarse_error is not None else None
        ),
        "recovery_sample_interval_seconds": recovery_interval,
        "recovery_scan": (
            _sampling_result_summary(recovery_result)
            if recovery_result is not None
            else None
        ),
        "recovery_error": (
            recovery_error.as_dict() if recovery_error is not None else None
        ),
        "local_fine_error": (
            local_fine_error.as_dict() if local_fine_error is not None else None
        ),
        "full_fine_scan_skipped": True,
        "final_anchor_source": final_anchor_source,
    }
    return result


def _materialize_fine_scan_clip(
    candidate_path: Path,
    output_path: Path,
    *,
    ffmpeg: str,
    start_seconds: float,
    duration_seconds: float,
    timeout_seconds: float,
    input_format: str | None = None,
    input_seek_seconds: float = 0.0,
) -> None:
    command = [
        ffmpeg,
        "-y",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
    ]
    if input_format == "ffconcat":
        command.extend(["-f", "concat", "-safe", "0"])
    command.extend(["-i", str(candidate_path)])
    effective_start = input_seek_seconds + start_seconds
    command.extend([
        "-ss",
        f"{effective_start:.6f}",
        "-t",
        f"{duration_seconds:.6f}",
        "-map",
        "0:v:0",
        "-an",
        "-c:v",
        "libx264",
        "-threads",
        "1",
        "-preset",
        "veryfast",
        "-crf",
        "20",
        "-pix_fmt",
        "yuv420p",
        str(output_path),
    ])
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise ScoreboardOcrError(
            "inference_timeout",
            "FFmpeg timed out while preparing the one-second OCR fine scan",
            diagnostics={"stage": "fine_scan_clip_extraction"},
        ) from exc
    except OSError as exc:
        raise ScoreboardOcrError(
            "ocr_frame_extraction_failed",
            f"cannot prepare the one-second OCR fine scan: {exc}",
            diagnostics={"stage": "fine_scan_clip_extraction"},
        ) from exc
    if completed.returncode != 0 or not output_path.is_file():
        raise ScoreboardOcrError(
            "ocr_frame_extraction_failed",
            "FFmpeg could not prepare the one-second OCR fine scan",
            diagnostics={
                "stage": "fine_scan_clip_extraction",
                "ffmpeg_stderr": (completed.stderr or "")[-2000:],
            },
        )


def _sampling_strategy_error(
    error: ScoreboardOcrError,
    *,
    mode: str,
    coarse_interval: float,
    fine_interval: float,
    fine_window_radius: float,
    coarse_result: Mapping[str, Any] | None = None,
    coarse_error: ScoreboardOcrError | None = None,
    recovery_interval: float | None = None,
    recovery_result: Mapping[str, Any] | None = None,
    recovery_error: ScoreboardOcrError | None = None,
    local_fine_error: ScoreboardOcrError | None = None,
) -> ScoreboardOcrError:
    strategy = {
        "mode": mode,
        "coarse_sample_interval_seconds": coarse_interval,
        "fine_sample_interval_seconds": fine_interval,
        "fine_window_radius_seconds": fine_window_radius,
        "coarse_scan": (
            _sampling_result_summary(coarse_result)
            if coarse_result is not None
            else None
        ),
        "coarse_error": coarse_error.as_dict() if coarse_error is not None else None,
        "recovery_sample_interval_seconds": recovery_interval,
        "recovery_scan": (
            _sampling_result_summary(recovery_result)
            if recovery_result is not None
            else None
        ),
        "recovery_error": (
            recovery_error.as_dict() if recovery_error is not None else None
        ),
        "local_fine_error": (
            local_fine_error.as_dict() if local_fine_error is not None else None
        ),
        "full_fine_scan_skipped": True,
        "final_anchor_source": None,
    }
    return ScoreboardOcrError(
        error.kind,
        error.message,
        diagnostics={**error.diagnostics, "sampling_strategy": strategy},
    )


def locate_scoreboard_event(
    candidate_path: str | Path,
    *,
    event_code: str,
    target_score: str | None = None,
    event_minute: str | int | None = None,
    event_second: int | None = None,
    candidate_start_seconds: float = 0.0,
    sample_interval_seconds: float = 1.0,
    anchor_lead_seconds: float = 3.0,
    stable_frames: int = 2,
    timeout_seconds: float = 180.0,
    python_executable: str | Path | None = None,
    worker_path: str | Path = DEFAULT_WORKER,
    runner: Runner = subprocess.run,
    scoreboard_profile: ScoreboardProfile | Mapping[str, Any] | str | None = None,
    clock_only: bool = False,
    coarse_sample_interval_seconds: float | None = DEFAULT_COARSE_SAMPLE_INTERVAL_SECONDS,
    fine_scan_radius_seconds: float = DEFAULT_FINE_SCAN_RADIUS_SECONDS,
    candidate_input_format: str | None = None,
    candidate_seek_seconds: float = 0.0,
    candidate_duration_seconds: float | None = None,
    cancel_event: Any = None,
) -> dict[str, Any]:
    """Locate an event, using a sparse clock scan before authoritative fine OCR."""
    request = ScoreboardOcrRequest(
        candidate_path=Path(candidate_path),
        event_code=event_code,
        target_score=target_score,
        event_minute=event_minute,
        event_second=event_second,
        candidate_start_seconds=candidate_start_seconds,
        sample_interval_seconds=sample_interval_seconds,
        anchor_lead_seconds=anchor_lead_seconds,
        stable_frames=stable_frames,
        scoreboard_profile=scoreboard_profile,
        clock_only=clock_only,
        candidate_input_format=candidate_input_format,
        candidate_seek_seconds=candidate_seek_seconds,
        candidate_duration_seconds=candidate_duration_seconds,
    )
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    if fine_scan_radius_seconds <= 0:
        raise ValueError("fine_scan_radius_seconds must be positive")
    if coarse_sample_interval_seconds is not None and coarse_sample_interval_seconds <= 0:
        raise ValueError("coarse_sample_interval_seconds must be positive or None")
    if (
        not clock_only
        or coarse_sample_interval_seconds is None
        or coarse_sample_interval_seconds <= sample_interval_seconds
    ):
        return run_scoreboard_ocr(
            request,
            python_executable=python_executable,
            worker_path=worker_path,
            timeout_seconds=timeout_seconds,
            runner=runner,
            cancel_event=cancel_event,
        )

    deadline = time.monotonic() + timeout_seconds
    coarse_interval = float(coarse_sample_interval_seconds)
    recovery_interval = min(
        DEFAULT_RECOVERY_SAMPLE_INTERVAL_SECONDS,
        coarse_interval / 2.0,
    )
    coarse_request = replace(
        request,
        sample_interval_seconds=coarse_interval,
    )
    coarse_result: dict[str, Any] | None = None
    coarse_error: ScoreboardOcrError | None = None
    recovery_result: dict[str, Any] | None = None
    recovery_error: ScoreboardOcrError | None = None
    try:
        coarse_result = run_scoreboard_ocr(
            coarse_request,
            python_executable=python_executable,
            worker_path=worker_path,
            timeout_seconds=_coarse_timeout_budget(timeout_seconds),
            runner=runner,
            cancel_event=cancel_event,
        )
    except ScoreboardOcrError as exc:
        if exc.kind not in _COARSE_SCAN_MISS_KINDS:
            raise
        coarse_error = exc

    fine_center = (
        _fine_scan_center(coarse_result, request)
        if coarse_result is not None
        else None
    )
    if fine_center is None:
        if coarse_error is None:
            coarse_error = ScoreboardOcrError(
                "ocr_no_target",
                "the coarse OCR scan returned no usable clock anchor",
                diagnostics={
                    "coarse_result": _sampling_result_summary(coarse_result or {})
                },
            )
        if recovery_interval <= request.sample_interval_seconds:
            raise _sampling_strategy_error(
                coarse_error,
                mode="coarse_recovery_unavailable",
                coarse_interval=coarse_interval,
                fine_interval=request.sample_interval_seconds,
                fine_window_radius=fine_scan_radius_seconds,
                coarse_result=coarse_result,
                coarse_error=coarse_error,
            ) from coarse_error
        recovery_request = replace(
            request,
            sample_interval_seconds=recovery_interval,
        )
        try:
            recovery_result = run_scoreboard_ocr(
                recovery_request,
                python_executable=python_executable,
                worker_path=worker_path,
                timeout_seconds=_coarse_timeout_budget(
                    _remaining_timeout(deadline)
                ),
                runner=runner,
                cancel_event=cancel_event,
            )
        except ScoreboardOcrError as exc:
            if exc.kind not in _COARSE_SCAN_MISS_KINDS:
                raise
            recovery_error = exc
            raise _sampling_strategy_error(
                exc,
                mode="coarse_recovery_failed",
                coarse_interval=coarse_interval,
                fine_interval=request.sample_interval_seconds,
                fine_window_radius=fine_scan_radius_seconds,
                coarse_result=coarse_result,
                coarse_error=coarse_error,
                recovery_interval=recovery_interval,
                recovery_error=recovery_error,
            ) from exc
        fine_center = _fine_scan_center(recovery_result, request)
        if fine_center is None:
            recovery_error = ScoreboardOcrError(
                "ocr_no_target",
                "the bounded recovery OCR scan returned no usable clock anchor",
                diagnostics={
                    "recovery_result": _sampling_result_summary(recovery_result)
                },
            )
            raise _sampling_strategy_error(
                recovery_error,
                mode="coarse_recovery_no_anchor",
                coarse_interval=coarse_interval,
                fine_interval=request.sample_interval_seconds,
                fine_window_radius=fine_scan_radius_seconds,
                coarse_result=coarse_result,
                coarse_error=coarse_error,
                recovery_interval=recovery_interval,
                recovery_result=recovery_result,
                recovery_error=recovery_error,
            ) from recovery_error

    assert fine_center is not None
    local_center = fine_center - request.candidate_start_seconds
    fine_start = max(0.0, local_center - fine_scan_radius_seconds)
    fine_duration = fine_scan_radius_seconds * 2.0
    try:
        if request.candidate_input_format == "ffconcat":
            available_duration = request.candidate_duration_seconds
            bounded_duration = (
                min(fine_duration, max(0.0, available_duration - fine_start))
                if available_duration is not None
                else fine_duration
            )
            fine_request = replace(
                request,
                candidate_start_seconds=(
                    request.candidate_start_seconds + fine_start
                ),
                candidate_seek_seconds=request.candidate_seek_seconds + fine_start,
                candidate_duration_seconds=bounded_duration,
            )
            fine_result = run_scoreboard_ocr(
                fine_request,
                python_executable=python_executable,
                worker_path=worker_path,
                timeout_seconds=_remaining_timeout(deadline),
                runner=runner,
                cancel_event=cancel_event,
            )
        else:
            with tempfile.TemporaryDirectory(prefix="scoreboard_ocr_fine_") as directory:
                fine_path = Path(directory) / "candidate_fine.mp4"
                _materialize_fine_scan_clip(
                    request.candidate_path,
                    fine_path,
                    ffmpeg=request.ffmpeg,
                    start_seconds=fine_start,
                    duration_seconds=fine_duration,
                    timeout_seconds=_remaining_timeout(deadline),
                )
                fine_request = replace(
                    request,
                    candidate_path=fine_path,
                    candidate_start_seconds=(
                        request.candidate_start_seconds + fine_start
                    ),
                )
                fine_result = run_scoreboard_ocr(
                    fine_request,
                    python_executable=python_executable,
                    worker_path=worker_path,
                    timeout_seconds=_remaining_timeout(deadline),
                    runner=runner,
                    cancel_event=cancel_event,
                )
    except ScoreboardOcrError as exc:
        if exc.kind not in _COARSE_SCAN_MISS_KINDS | {
            "ocr_frame_extraction_failed"
        }:
            raise
        raise _sampling_strategy_error(
            exc,
            mode="local_fine_failed",
            coarse_interval=coarse_interval,
            fine_interval=request.sample_interval_seconds,
            fine_window_radius=fine_scan_radius_seconds,
            coarse_result=coarse_result,
            coarse_error=coarse_error,
            recovery_interval=(
                recovery_interval if recovery_result is not None else None
            ),
            recovery_result=recovery_result,
            recovery_error=recovery_error,
            local_fine_error=exc,
        ) from exc
    return _attach_sampling_strategy(
        fine_result,
        mode=(
            "coarse_recovery_then_local_fine"
            if recovery_result is not None
            else "coarse_then_local_fine"
        ),
        coarse_interval=coarse_interval,
        fine_interval=request.sample_interval_seconds,
        fine_window_radius=fine_scan_radius_seconds,
        coarse_result=coarse_result,
        coarse_error=coarse_error,
        recovery_interval=(
            recovery_interval if recovery_result is not None else None
        ),
        recovery_result=recovery_result,
        fine_clip_start_seconds=round(
            request.candidate_start_seconds + fine_start, 3
        ),
    )


__all__ = [
    "ClockContinuityResult",
    "ClockContinuityStateMachine",
    "DEFAULT_COARSE_SAMPLE_INTERVAL_SECONDS",
    "DEFAULT_RECOVERY_SAMPLE_INTERVAL_SECONDS",
    "DEFAULT_FINE_SCAN_RADIUS_SECONDS",
    "DEFAULT_WORKER",
    "ParsedMatchClock",
    "ParsedScore",
    "ScoreboardProfile",
    "STRUCTURED_ERROR_KINDS",
    "ScoreboardOcrError",
    "ScoreboardOcrRequest",
    "locate_scoreboard_event",
    "parse_clock_texts",
    "parse_score_texts",
    "register_scoreboard_profile",
    "resolve_scoreboard_profile",
    "run_scoreboard_ocr",
]
