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
from concurrent.futures import (
    Future,
    InvalidStateError,
    TimeoutError as FutureTimeoutError,
)
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
GOAL_LIKE_EVENT_CODES = frozenset({"G", "OG", "PG"})
SUPPORTED_EVENT_CODES = GOAL_LIKE_EVENT_CODES | frozenset({"YC", "RC"})
MAX_REASONABLE_MATCH_MINUTE = 150
MAX_REASONABLE_SCORE = 20
OCR_CROP_KINDS = frozenset({"clock", "score"})
MIN_PROFILE_TRUSTED_CLOCK_FRAMES = 2
MIN_PROFILE_TRUSTED_CLOCK_RATE = 0.20
MIN_PROFILE_CLOCK_PROGRESSION_SECONDS = 1
AUTO_SEARCH_WIDTH_RATIO = 0.40
AUTO_SEARCH_HEIGHT_RATIO = 0.25
AUTO_SEARCH_SCALE = 3
AUTO_DISCOVERY_OBSERVATIONS = 3
AUTO_DISCOVERY_MAX_MISSES = 3
AUTO_REACQUIRE_MISSING_FRAMES = 8
AUTO_SEARCH_BATCH_FRAMES = 5
AUTO_MAXIMUM_ANALYSIS_SECONDS = 360.0
CLOCK_PREPROCESS_VARIANTS = ("gray_contrast_sharp", "binary_contrast")
INDEPENDENT_STOPPAGE_BASE_MINUTES = (45, 90)
INDEPENDENT_STOPPAGE_MAX_MINUTES = 15
INDEPENDENT_STOPPAGE_MIN_OBSERVATIONS = 3

_CLOCK_CHARACTER_TRANSLATION = str.maketrans(
    {
        "O": "0",
        "I": "1",
        "L": "1",
        "S": "5",
        "B": "8",
        "|": "1",
        "０": "0",
        "１": "1",
        "２": "2",
        "３": "3",
        "４": "4",
        "５": "5",
        "６": "6",
        "７": "7",
        "８": "8",
        "９": "9",
        "：": ":",
        "．": ".",
        "＋": "+",
    }
)
_ALLOWED_CLOCK_CHARACTERS = frozenset("0123456789:+. ")

BBox = tuple[float, float, float, float]


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
    observed_clock_seconds: int | None = None


@dataclass(frozen=True)
class _ClockSecondCandidate:
    frame_seconds: float
    method: str
    precision: str
    segment_index: int
    left: FrameReading
    right: FrameReading | None = None


@dataclass(frozen=True)
class _ClockSecondEstimate:
    frame_seconds: float
    segment_index: int
    nearest: FrameReading
    evidence: tuple[FrameReading, ...]
    direction: str
    clock_distance_seconds: int
    clock_video_slope: float


@dataclass(frozen=True)
class _ClockMappingProjection:
    frame_seconds: float
    evidence: tuple[FrameReading, ...]
    left: FrameReading
    right: FrameReading
    slope: float
    intercept: float
    maximum_residual_seconds: float
    error_bound_seconds: float
    mapping_kind: str
    projection_distance_seconds: int


# Keep nearby-frame evidence bounded while retaining enough consecutive OCR
# observations to verify normal clock progression.
NEAR_NEIGHBOR_MAX_DIRECT_READINGS = 5
NEAR_NEIGHBOR_MIN_CLOCK_VIDEO_SLOPE = 0.8
NEAR_NEIGHBOR_MAX_CLOCK_VIDEO_SLOPE = 1.2
# When the exact displayed second is unavailable, prefer a nearby frame that
# OCR actually read before projecting through a longer scoreboard occlusion.
# This applies to explicit goal seconds and to the requested minute boundary,
# with each result retaining its precision label.
NEARBY_OBSERVED_MAX_CLOCK_DISTANCE_SECONDS = 5
NEARBY_OBSERVED_MIN_DIRECT_READINGS = 2
CLOCK_MAPPING_MIN_DIRECT_READINGS = 4
CLOCK_MAPPING_MAX_OCCLUSION_SECONDS = 60.0
CLOCK_MAPPING_MAX_EXTRAPOLATION_SECONDS = 15
CLOCK_MAPPING_MAX_RESIDUAL_SECONDS = 1.5


@dataclass(frozen=True)
class DetectedText:
    """One OCR detection with its text, confidence, and quadrilateral bounds."""

    text: str
    confidence: float
    bbox: BBox


@dataclass(frozen=True)
class AutoClockDecision:
    status: str
    reason: str
    clock_seconds: int | None = None
    clock_roi: BBox | None = None
    score_roi: BBox | None = None
    score_rois: tuple[BBox, ...] = ()
    search_roi: BBox | None = None
    candidate_count: int = 0
    miss_count: int = 0
    expanded_search: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason": self.reason,
            "clock_seconds": self.clock_seconds,
            "clock_roi": list(self.clock_roi) if self.clock_roi else None,
            "score_roi": list(self.score_roi) if self.score_roi else None,
            "score_rois": [list(roi) for roi in self.score_rois],
            "search_roi": list(self.search_roi) if self.search_roi else None,
            "candidate_count": self.candidate_count,
            "miss_count": self.miss_count,
            "expanded_search": self.expanded_search,
        }


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
    backend_generation: int = 0

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
            "backend_generation": self.backend_generation,
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


def _as_sequence(value: Any) -> list[Any] | None:
    """Return JSON-like arrays, including NumPy arrays from Paddle results."""
    if isinstance(value, (str, bytes, bytearray)):
        return None
    if isinstance(value, Sequence):
        return list(value)
    to_list = getattr(value, "tolist", None)
    if callable(to_list):
        try:
            converted = to_list()
        except Exception:
            return None
        return _as_sequence(converted)
    return None


def _bbox_from_polygon(value: Any) -> BBox | None:
    points = _as_sequence(value)
    if not points:
        return None
    if len(points) == 4 and all(isinstance(item, (int, float)) for item in points):
        x1, y1, x2, y2 = (float(item) for item in points)
        if x2 > x1 and y2 > y1 and all(
            math.isfinite(item) for item in (x1, y1, x2, y2)
        ):
            return x1, y1, x2, y2
        return None
    coordinates: list[tuple[float, float]] = []
    for point in points:
        pair = _as_sequence(point)
        if pair is None or len(pair) < 2:
            return None
        try:
            x, y = float(pair[0]), float(pair[1])
        except (TypeError, ValueError):
            return None
        if not math.isfinite(x) or not math.isfinite(y):
            return None
        coordinates.append((x, y))
    if len(coordinates) < 2:
        return None
    xs, ys = zip(*coordinates)
    x1, y1, x2, y2 = min(xs), min(ys), max(xs), max(ys)
    if x2 <= x1 or y2 <= y1:
        return None
    return x1, y1, x2, y2


def _extract_detected_texts(value: Any) -> list[DetectedText]:
    """Normalize PaddleOCR v2/v3 text detections without losing bbox pairing."""
    extracted: list[DetectedText] = []
    visited: set[int] = set()

    def append(text: Any, confidence: Any, polygon: Any) -> None:
        bbox = _bbox_from_polygon(polygon)
        if bbox is None:
            return
        normalized = str(text).strip()
        if not normalized:
            return
        try:
            score = float(confidence)
        except (TypeError, ValueError):
            score = 1.0
        if not math.isfinite(score):
            return
        extracted.append(DetectedText(normalized, score, bbox))

    def visit(item: Any) -> None:
        if item is None:
            return
        if not isinstance(item, (str, int, float, bool)):
            identity = id(item)
            if identity in visited:
                return
            visited.add(identity)
        if isinstance(item, Mapping):
            texts = _as_sequence(item.get("rec_texts"))
            scores = _as_sequence(item.get("rec_scores")) or []
            polygons = (
                _as_sequence(item.get("rec_polys"))
                or _as_sequence(item.get("dt_polys"))
                or _as_sequence(item.get("rec_boxes"))
            )
            if texts is not None and polygons is not None:
                for index in range(min(len(texts), len(polygons))):
                    append(
                        texts[index],
                        scores[index] if index < len(scores) else 1.0,
                        polygons[index],
                    )
                return
            for nested in item.values():
                visit(nested)
            return

        values = _as_sequence(item)
        if values is not None:
            if len(values) == 2:
                recognized = _as_sequence(values[1])
                if (
                    _bbox_from_polygon(values[0]) is not None
                    and recognized is not None
                    and len(recognized) == 2
                    and isinstance(recognized[0], str)
                ):
                    append(values[1][0], values[1][1], values[0])
                    return
            for nested in values:
                visit(nested)
            return

        json_value = getattr(item, "json", None)
        if callable(json_value):
            try:
                json_value = json_value()
            except Exception:
                return
        if isinstance(json_value, str):
            try:
                json_value = json.loads(json_value)
            except json.JSONDecodeError:
                return
        if json_value is not None:
            visit(json_value)

    visit(value)
    deduplicated: list[DetectedText] = []
    seen: set[tuple[str, float, BBox]] = set()
    for detected in extracted:
        key = (
            detected.text,
            round(detected.confidence, 6),
            tuple(round(point, 3) for point in detected.bbox),
        )
        if key not in seen:
            seen.add(key)
            deduplicated.append(detected)
    return deduplicated


def _bbox_union(boxes: Iterable[BBox]) -> BBox | None:
    values = list(boxes)
    if not values:
        return None
    return (
        min(box[0] for box in values),
        min(box[1] for box in values),
        max(box[2] for box in values),
        max(box[3] for box in values),
    )


def _bbox_center(box: BBox) -> tuple[float, float]:
    return (box[0] + box[2]) / 2, (box[1] + box[3]) / 2


def _bbox_similar(left: BBox, right: BBox) -> bool:
    left_width, left_height = left[2] - left[0], left[3] - left[1]
    right_width, right_height = right[2] - right[0], right[3] - right[1]
    if min(left_width, left_height, right_width, right_height) <= 0:
        return False
    overlap_width = max(0.0, min(left[2], right[2]) - max(left[0], right[0]))
    overlap_height = max(0.0, min(left[3], right[3]) - max(left[1], right[1]))
    intersection = overlap_width * overlap_height
    union = left_width * left_height + right_width * right_height - intersection
    if union > 0 and intersection / union >= 0.25:
        return True
    left_center, right_center = _bbox_center(left), _bbox_center(right)
    return (
        abs(left_center[0] - right_center[0])
        <= max(left_width, right_width) * 0.6 + 5.0
        and abs(left_center[1] - right_center[1])
        <= max(left_height, right_height) * 0.5 + 4.0
    )


def _padded_roi(
    bbox: BBox,
    *,
    frame_width: int,
    frame_height: int,
    padding_ratio: float = 0.30,
) -> BBox:
    width, height = bbox[2] - bbox[0], bbox[3] - bbox[1]
    padding_x, padding_y = max(4.0, width * padding_ratio), max(4.0, height * padding_ratio)
    return (
        max(0.0, bbox[0] - padding_x),
        max(0.0, bbox[1] - padding_y),
        min(float(frame_width), bbox[2] + padding_x),
        min(float(frame_height), bbox[3] + padding_y),
    )


def _spatial_text_groups(detections: Sequence[DetectedText]) -> list[DetectedText]:
    """Include same-line joined tokens for clocks/scores split by detection."""
    rows: list[list[DetectedText]] = []
    for detected in sorted(detections, key=lambda item: _bbox_center(item.bbox)[1]):
        center_y = _bbox_center(detected.bbox)[1]
        for row in rows:
            row_center = sum(_bbox_center(item.bbox)[1] for item in row) / len(row)
            row_height = max(item.bbox[3] - item.bbox[1] for item in row)
            if abs(center_y - row_center) <= max(row_height, detected.bbox[3] - detected.bbox[1]):
                row.append(detected)
                break
        else:
            rows.append([detected])
    grouped = list(detections)
    for row in rows:
        ordered = sorted(row, key=lambda item: item.bbox[0])
        for start in range(len(ordered)):
            current: list[DetectedText] = []
            previous_x2: float | None = None
            for detected in ordered[start : start + 4]:
                width = detected.bbox[2] - detected.bbox[0]
                if previous_x2 is not None and detected.bbox[0] - previous_x2 > max(20.0, width * 2.0):
                    break
                current.append(detected)
                previous_x2 = detected.bbox[2]
                if len(current) > 1:
                    bbox = _bbox_union(item.bbox for item in current)
                    assert bbox is not None
                    grouped.append(
                        DetectedText(
                            "".join(item.text for item in current),
                            min(item.confidence for item in current),
                            bbox,
                        )
                    )
    return grouped


@dataclass(frozen=True)
class _AutoClockCandidate:
    clock_seconds: int
    bbox: BBox
    confidence: float


@dataclass
class _AutoClockTrack:
    bbox: BBox
    first_clock_seconds: int
    last_clock_seconds: int
    last_video_seconds: float
    boxes: list[BBox]
    observations: int = 1
    misses: int = 0


@dataclass
class _AutoScoreTrack:
    bbox: BBox
    boxes: list[BBox]
    observations: int = 1
    misses: int = 0


class AutoClockTracker:
    """Find one spatially stable match-clock ROI before normal OCR begins."""

    def __init__(
        self,
        frame_width: int,
        frame_height: int,
        *,
        stable_observations: int = AUTO_DISCOVERY_OBSERVATIONS,
        maximum_misses: int = AUTO_DISCOVERY_MAX_MISSES,
        clock_only: bool = False,
    ) -> None:
        if frame_width <= 0 or frame_height <= 0:
            raise ValueError("frame dimensions must be positive")
        if stable_observations < 2 or maximum_misses < 1:
            raise ValueError("invalid automatic clock tracker thresholds")
        self.frame_width = int(frame_width)
        self.frame_height = int(frame_height)
        self.stable_observations = int(stable_observations)
        self.maximum_misses = int(maximum_misses)
        if not isinstance(clock_only, bool):
            raise ValueError("clock_only must be a boolean")
        self.clock_only = clock_only
        self.expanded_search = False
        self.clock_roi: BBox | None = None
        self.score_roi: BBox | None = None
        self.score_rois: tuple[BBox, ...] = ()
        self._clock_tracks: list[_AutoClockTrack] = []
        self._score_tracks: list[_AutoScoreTrack] = []
        self._locked_clock_seconds: int | None = None
        self._locked_video_seconds: float | None = None
        self._locked_misses = 0
        self._timeline_discontinuities = 0

    @property
    def search_roi(self) -> BBox:
        width = self.frame_width if self.expanded_search else self.frame_width * AUTO_SEARCH_WIDTH_RATIO
        return 0.0, 0.0, float(width), self.frame_height * AUTO_SEARCH_HEIGHT_RATIO

    @staticmethod
    def _clock_candidates(detections: Sequence[DetectedText]) -> list[_AutoClockCandidate]:
        candidates: list[_AutoClockCandidate] = []
        for detected in _spatial_text_groups(detections):
            parsed = parse_clock_texts(detected.text)
            if parsed.clock_seconds is None or parsed.ambiguous:
                continue
            candidate = _AutoClockCandidate(
                parsed.clock_seconds,
                detected.bbox,
                detected.confidence,
            )
            duplicate_index = next(
                (
                    index
                    for index, existing in enumerate(candidates)
                    if existing.clock_seconds == candidate.clock_seconds
                    and _bbox_similar(existing.bbox, candidate.bbox)
                ),
                None,
            )
            if duplicate_index is None:
                candidates.append(candidate)
            else:
                existing = candidates[duplicate_index]
                existing_area = (existing.bbox[2] - existing.bbox[0]) * (
                    existing.bbox[3] - existing.bbox[1]
                )
                candidate_area = (candidate.bbox[2] - candidate.bbox[0]) * (
                    candidate.bbox[3] - candidate.bbox[1]
                )
                if candidate_area < existing_area:
                    candidates[duplicate_index] = candidate
        return candidates

    @staticmethod
    def _score_candidates(
        detections: Sequence[DetectedText],
    ) -> list[DetectedText]:
        candidates: list[DetectedText] = []
        for detected in _spatial_text_groups(detections):
            parsed_clock = parse_clock_texts(detected.text)
            parsed = parse_score_texts(detected.text)
            clock_like = (
                parsed_clock.clock_seconds is not None
                and any(separator in detected.text for separator in (":", "."))
            )
            if (
                parsed.score is not None
                and not parsed.ambiguous
                and not clock_like
            ):
                candidates.append(detected)
        # A full detector line such as ``0310`` can be a score separator
        # dropped by PaddleOCR. Keep this fallback to original boxes only;
        # applying it to joined same-line tokens would absorb team names.
        for detected in detections:
            parsed_clock = parse_clock_texts(detected.text)
            if (
                parsed_clock.clock_seconds is not None
                and any(separator in detected.text for separator in (":", "."))
            ):
                continue
            compact_numeric = (
                len(detected.text) <= 8
                and sum(character.isdigit() for character in detected.text)
                >= max(2, len(detected.text) // 2)
            )
            if compact_numeric:
                candidates.append(detected)
        unique: list[DetectedText] = []
        seen: set[tuple[str, BBox]] = set()
        for candidate in candidates:
            key = (candidate.text, candidate.bbox)
            if key not in seen:
                seen.add(key)
                unique.append(candidate)
        return unique

    @staticmethod
    def _is_continuous(track: _AutoClockTrack, candidate: _AutoClockCandidate, video_seconds: float) -> bool:
        elapsed = video_seconds - track.last_video_seconds
        if elapsed <= 0:
            return False
        clock_delta = candidate.clock_seconds - track.last_clock_seconds
        return 0 <= clock_delta <= max(4, round(elapsed * 2.0) + 2)

    def _best_clock_track(self, candidate: _AutoClockCandidate) -> _AutoClockTrack | None:
        matches = [track for track in self._clock_tracks if _bbox_similar(track.bbox, candidate.bbox)]
        if not matches:
            return None
        return min(
            matches,
            key=lambda track: math.dist(
                _bbox_center(track.bbox), _bbox_center(candidate.bbox)
            ),
        )

    def _update_score_tracks(self, detections: Sequence[DetectedText]) -> None:
        matched: set[int] = set()
        for candidate in self._score_candidates(detections):
            matches = [
                (index, track)
                for index, track in enumerate(self._score_tracks)
                if _bbox_similar(track.bbox, candidate.bbox)
            ]
            if matches:
                index, track = min(
                    matches,
                    key=lambda item: math.dist(
                        _bbox_center(item[1].bbox), _bbox_center(candidate.bbox)
                    ),
                )
                track.bbox = candidate.bbox
                track.boxes.append(candidate.bbox)
                track.observations += 1
                track.misses = 0
                matched.add(index)
            else:
                self._score_tracks.append(_AutoScoreTrack(candidate.bbox, [candidate.bbox]))
                matched.add(len(self._score_tracks) - 1)
        self._score_tracks = [
            track
            for index, track in enumerate(self._score_tracks)
            if index in matched or track.misses + 1 < self.stable_observations
        ]
        for index, track in enumerate(self._score_tracks):
            if index not in matched:
                track.misses += 1

    def _score_roi_for_clock(self, clock_bbox: BBox) -> BBox | None:
        qualified = [
            track for track in self._score_tracks if track.observations >= self.stable_observations
        ]
        if not qualified:
            return None
        clock_y = _bbox_center(clock_bbox)[1]
        clock_height = max(1.0, clock_bbox[3] - clock_bbox[1])
        aligned = [
            track
            for track in qualified
            if abs(_bbox_center(track.bbox)[1] - clock_y)
            <= max(clock_height * 1.5, self.frame_height * 0.04)
            and min(
                abs(_bbox_center(track.bbox)[0] - clock_bbox[0]),
                abs(_bbox_center(track.bbox)[0] - clock_bbox[2]),
            )
            <= self.frame_width * 0.30
        ]
        if not aligned:
            return None
        bbox = _bbox_union(
            box for track in aligned for box in track.boxes
        )
        assert bbox is not None
        return _padded_roi(
            bbox,
            frame_width=self.frame_width,
            frame_height=self.frame_height,
            padding_ratio=0.08,
        )

    def _score_roi_candidates(self, clock_bbox: BBox) -> tuple[BBox, ...]:
        detected = self._score_roi_for_clock(clock_bbox)
        clock_width = max(1.0, clock_bbox[2] - clock_bbox[0])
        clock_height = max(1.0, clock_bbox[3] - clock_bbox[1])
        span = max(clock_width * 3.0, self.frame_width * 0.12)
        y1 = max(0.0, clock_bbox[1] - clock_height * 0.45)
        y2 = min(float(self.frame_height), clock_bbox[3] + clock_height * 0.45)
        candidates = [
            detected,
            (
                max(0.0, clock_bbox[0] - span),
                y1,
                max(1.0, clock_bbox[0] - 1.0),
                y2,
            ),
            (
                min(float(self.frame_width - 1), clock_bbox[2] + 1.0),
                y1,
                min(float(self.frame_width), clock_bbox[2] + span),
                y2,
            ),
        ]
        unique: list[BBox] = []
        for candidate in candidates:
            if candidate is None or candidate[2] <= candidate[0] or candidate[3] <= candidate[1]:
                continue
            if not any(_bbox_similar(candidate, existing) for existing in unique):
                unique.append(candidate)
        return tuple(unique)

    def observe_search(
        self,
        frame_index: int,
        video_seconds: float,
        detections: Sequence[DetectedText],
    ) -> AutoClockDecision:
        del frame_index
        candidates = self._clock_candidates(detections)
        if not self.clock_only:
            self._update_score_tracks(detections)
        matched_tracks: set[int] = set()
        discontinuous = False
        for candidate in candidates:
            track = self._best_clock_track(candidate)
            if track is None:
                self._clock_tracks.append(
                    _AutoClockTrack(
                        candidate.bbox,
                        candidate.clock_seconds,
                        candidate.clock_seconds,
                        float(video_seconds),
                        [candidate.bbox],
                    )
                )
                matched_tracks.add(len(self._clock_tracks) - 1)
                continue
            track_index = self._clock_tracks.index(track)
            if not self._is_continuous(track, candidate, float(video_seconds)):
                discontinuous = True
                continue
            track.bbox = candidate.bbox
            track.last_clock_seconds = candidate.clock_seconds
            track.last_video_seconds = float(video_seconds)
            track.boxes.append(candidate.bbox)
            track.observations += 1
            track.misses = 0
            matched_tracks.add(track_index)
        retained: list[_AutoClockTrack] = []
        for index, track in enumerate(self._clock_tracks):
            if index not in matched_tracks:
                track.misses += 1
            # Discovery validation is deliberately consecutive. This prevents
            # a static score token misread as a compact clock every other frame
            # from accumulating enough observations to lock.
            if track.misses == 0:
                retained.append(track)
        self._clock_tracks = retained
        progressing = [
            track
            for track in self._clock_tracks
            if track.observations >= self.stable_observations
            and track.last_clock_seconds > track.first_clock_seconds
        ]
        # Discovery must prove that the candidate is a running match clock.
        # A paused clock is supported after the ROI is locked, but a static
        # token (often a score misread as compact ``MMSS``) must not establish
        # the initial lock on its own.
        qualified = progressing
        if len(qualified) > 1:
            return AutoClockDecision(
                "ambiguous",
                "ambiguous",
                search_roi=self.search_roi,
                candidate_count=len(qualified),
                expanded_search=self.expanded_search,
            )
        if len(qualified) == 1:
            track = qualified[0]
            bbox = _bbox_union(track.boxes)
            assert bbox is not None
            self.clock_roi = _padded_roi(
                bbox, frame_width=self.frame_width, frame_height=self.frame_height
            )
            if self.clock_only:
                self.score_rois = ()
                self.score_roi = None
            else:
                self.score_rois = self._score_roi_candidates(bbox)
                self.score_roi = self.score_rois[0] if self.score_rois else None
            self._locked_clock_seconds = track.last_clock_seconds
            self._locked_video_seconds = track.last_video_seconds
            self._locked_misses = 0
            return AutoClockDecision(
                "locked",
                "stable_clock_track",
                clock_seconds=track.last_clock_seconds,
                clock_roi=self.clock_roi,
                score_roi=self.score_roi,
                score_rois=self.score_rois,
                search_roi=self.search_roi,
                candidate_count=1,
                expanded_search=self.expanded_search,
            )
        if discontinuous:
            return AutoClockDecision(
                "searching",
                "timeline_discontinuous",
                search_roi=self.search_roi,
                candidate_count=len(candidates),
                expanded_search=self.expanded_search,
            )
        return AutoClockDecision(
            "searching",
            "searching",
            search_roi=self.search_roi,
            candidate_count=len(candidates),
            expanded_search=self.expanded_search,
        )

    def observe_locked(
        self, video_seconds: float, clock_texts: Sequence[str]
    ) -> AutoClockDecision:
        if self.clock_roi is None:
            return AutoClockDecision(
                "searching",
                "auto_search_failed",
                search_roi=self.search_roi,
                expanded_search=self.expanded_search,
            )
        parsed = parse_clock_texts(clock_texts)
        if parsed.clock_seconds is None or parsed.ambiguous:
            self._locked_misses += 1
            if self._locked_misses < self.maximum_misses:
                return AutoClockDecision(
                    "locked",
                    "temporarily_hidden",
                    clock_roi=self.clock_roi,
                    score_roi=self.score_roi,
                    score_rois=self.score_rois,
                    search_roi=self.search_roi,
                    miss_count=self._locked_misses,
                    expanded_search=self.expanded_search,
                )
            self.expanded_search = True
            self.clock_roi = None
            self.score_roi = None
            self.score_rois = ()
            return AutoClockDecision(
                "searching",
                "auto_search_failed",
                search_roi=self.search_roi,
                miss_count=self._locked_misses,
                expanded_search=True,
            )
        if (
            self._locked_clock_seconds is not None
            and self._locked_video_seconds is not None
            and (
                parsed.clock_seconds < self._locked_clock_seconds - 1
                or parsed.clock_seconds - self._locked_clock_seconds
                > max(4, round((video_seconds - self._locked_video_seconds) * 2.0) + 2)
            )
        ):
            self._timeline_discontinuities += 1
            return AutoClockDecision(
                "locked",
                "timeline_discontinuous",
                clock_roi=self.clock_roi,
                score_roi=self.score_roi,
                score_rois=self.score_rois,
                search_roi=self.search_roi,
                miss_count=self._locked_misses,
                expanded_search=self.expanded_search,
            )
        self._locked_clock_seconds = parsed.clock_seconds
        self._locked_video_seconds = float(video_seconds)
        self._locked_misses = 0
        self._timeline_discontinuities = 0
        return AutoClockDecision(
            "locked",
            "accepted",
            clock_seconds=parsed.clock_seconds,
            clock_roi=self.clock_roi,
            score_roi=self.score_roi,
            score_rois=self.score_rois,
            search_roi=self.search_roi,
            expanded_search=self.expanded_search,
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
        observed_clock_seconds=parsed.clock_seconds,
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
    clock_visible: bool | None = None,
    scoreboard_visible: bool | None = None,
) -> FrameReading:
    """Build one reading from independent clock and score recognition crops."""
    clock_values = tuple(str(text) for text in clock_texts if str(text).strip())
    score_values = tuple(str(text) for text in score_texts if str(text).strip())
    parsed_clock = parse_clock_texts(clock_values)
    parsed_score = parse_score_texts(score_values)
    clock_is_visible = bool(clock_values) if clock_visible is None else bool(clock_visible)
    visible = (
        bool(clock_values or score_values)
        if scoreboard_visible is None
        else bool(scoreboard_visible)
    )
    continuity = tracker.update(
        float(frame_seconds),
        clock_values,
        scoreboard_visible=clock_is_visible,
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
        scoreboard_visible=visible,
        observed_clock_seconds=continuity.observed_clock_seconds,
    )


def _clock_timeline_diagnostics(
    readings: Sequence[FrameReading],
) -> dict[str, Any]:
    """Expose raw observations and every continuity correction/jump."""
    observations: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []
    abnormal_jumps: list[dict[str, Any]] = []
    previous_effective: FrameReading | None = None
    for reading in readings:
        observed = reading.observed_clock_seconds
        effective = reading.clock_seconds
        observation = {
            "frame_index": reading.frame_index,
            "frame_seconds": round(reading.frame_seconds, 3),
            "clock_texts": list(reading.clock_texts),
            "observed_clock_seconds": observed,
            "observed_clock": _clock_text(observed),
            "effective_clock_seconds": effective,
            "effective_clock": _clock_text(effective),
            "continuity_status": reading.continuity_status,
            "continuity_reason": reading.continuity_reason,
        }
        observations.append(observation)
        if reading.continuity_status == "repaired":
            repairs.append(
                {
                    **observation,
                    "correction_seconds": (
                        effective - observed
                        if effective is not None and observed is not None
                        else None
                    ),
                }
            )
        if observed is not None and previous_effective is not None:
            previous_clock = previous_effective.clock_seconds
            if previous_clock is not None:
                video_delta = reading.frame_seconds - previous_effective.frame_seconds
                expected_clock = previous_clock + max(0, round(video_delta))
                deviation = observed - expected_clock
                maximum_deviation = max(3, round(max(0.0, video_delta) * 0.8) + 2)
                if (
                    video_delta <= 0
                    or abs(deviation) > maximum_deviation
                    or reading.continuity_reason == "clock_discontinuity"
                ):
                    abnormal_jumps.append(
                        {
                            "frame_index": reading.frame_index,
                            "frame_seconds": round(reading.frame_seconds, 3),
                            "previous_effective_clock_seconds": previous_clock,
                            "observed_clock_seconds": observed,
                            "expected_clock_seconds": expected_clock,
                            "video_delta_seconds": round(video_delta, 3),
                            "jump_deviation_seconds": deviation,
                            "continuity_status": reading.continuity_status,
                            "continuity_reason": reading.continuity_reason,
                        }
                    )
        if effective is not None:
            previous_effective = reading
    return {
        "clock_raw_observations": observations,
        "clock_continuity_repairs": repairs,
        "clock_continuity_repair_count": len(repairs),
        "abnormal_clock_jumps": abnormal_jumps,
        "abnormal_clock_jump_count": len(abnormal_jumps),
    }


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
                "observed_clock_seconds": reading.observed_clock_seconds,
                "observed_clock": _clock_text(reading.observed_clock_seconds),
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
        **_clock_timeline_diagnostics(readings),
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


def _trustworthy_clock_reading(reading: FrameReading) -> bool:
    """Return whether a continuity-cleaned clock can support second precision."""
    return (
        reading.clock_seconds is not None
        and math.isfinite(reading.frame_seconds)
        and not reading.ambiguous_clock
        and reading.scoreboard_visible
        and reading.continuity_status not in {"rejected", "repaired"}
    )


def _clock_segments(
    readings: Sequence[FrameReading], *, sample_interval_seconds: float
) -> list[list[FrameReading]]:
    trusted = [reading for reading in readings if _trustworthy_clock_reading(reading)]
    segments: list[list[FrameReading]] = []
    maximum_video_gap = max(5.0, sample_interval_seconds * 3.1)
    for reading in trusted:
        previous = segments[-1][-1] if segments else None
        starts_new_segment = previous is None
        if previous is not None:
            assert previous.clock_seconds is not None
            assert reading.clock_seconds is not None
            video_delta = reading.frame_seconds - previous.frame_seconds
            clock_delta = reading.clock_seconds - previous.clock_seconds
            maximum_clock_advance = video_delta + max(2.0, sample_interval_seconds)
            starts_new_segment = (
                video_delta <= 0
                or video_delta > maximum_video_gap
                or clock_delta < 0
                or clock_delta > maximum_clock_advance
                or reading.continuity_status == "resynchronized"
            )
        if starts_new_segment:
            segments.append([reading])
        else:
            segments[-1].append(reading)
    return segments


def _direct_estimate_clock_reading(reading: FrameReading) -> bool:
    """Return whether a raw OCR clock can support a bounded extrapolation."""
    return (
        _trustworthy_clock_reading(reading)
        and reading.observed_clock_seconds is not None
        and reading.observed_clock_seconds == reading.clock_seconds
        and reading.continuity_status not in {"repaired", "resynchronized"}
        and reading.continuity_reason
        not in {
            "clock_discontinuity",
            "continuous_observations_resynchronized",
            "period_boundary_resynchronized",
        }
    )


def _near_one_to_one_pair(left: FrameReading, right: FrameReading) -> bool:
    if (
        left.clock_seconds is None
        or right.clock_seconds is None
        or right.frame_index != left.frame_index + 1
    ):
        return False
    video_delta = right.frame_seconds - left.frame_seconds
    clock_delta = right.clock_seconds - left.clock_seconds
    if video_delta <= 0 or clock_delta <= 0:
        return False
    slope = clock_delta / video_delta
    return (
        NEAR_NEIGHBOR_MIN_CLOCK_VIDEO_SLOPE
        <= slope
        <= NEAR_NEIGHBOR_MAX_CLOCK_VIDEO_SLOPE
    )


def _direct_estimate_runs(
    segment: Sequence[FrameReading],
) -> list[list[FrameReading]]:
    """Split one continuity segment into gap-free direct 1:1 OCR runs."""
    runs: list[list[FrameReading]] = []
    for reading in segment:
        if not _direct_estimate_clock_reading(reading):
            continue
        if not runs or not _near_one_to_one_pair(runs[-1][-1], reading):
            runs.append([reading])
        else:
            runs[-1].append(reading)
    return runs


def _nearby_observed_clock_candidates(
    segments: Sequence[Sequence[FrameReading]],
    *,
    event_second: int,
) -> list[_ClockSecondEstimate]:
    """Return the closest real OCR frames from continuity-verified runs."""
    candidates: list[_ClockSecondEstimate] = []
    for segment_index, segment in enumerate(segments):
        if any(
            reading.continuity_status == "resynchronized"
            or reading.continuity_reason
            in {
                "continuous_observations_resynchronized",
                "period_boundary_resynchronized",
            }
            for reading in segment
        ):
            continue
        for run in _direct_estimate_runs(segment):
            if len(run) < NEARBY_OBSERVED_MIN_DIRECT_READINGS:
                continue
            eligible = [
                reading
                for reading in run
                if reading.clock_seconds is not None
                and 1
                <= abs(reading.clock_seconds - event_second)
                <= NEARBY_OBSERVED_MAX_CLOCK_DISTANCE_SECONDS
            ]
            if not eligible:
                continue
            minimum_distance = min(
                abs(int(reading.clock_seconds) - event_second)
                for reading in eligible
                if reading.clock_seconds is not None
            )
            nearest_readings = [
                reading
                for reading in eligible
                if reading.clock_seconds is not None
                and abs(reading.clock_seconds - event_second) == minimum_distance
            ]
            for nearest in nearest_readings:
                assert nearest.clock_seconds is not None
                nearest_index = run.index(nearest)
                evidence_start = max(
                    0,
                    min(
                        nearest_index - NEAR_NEIGHBOR_MAX_DIRECT_READINGS // 2,
                        len(run) - NEAR_NEIGHBOR_MAX_DIRECT_READINGS,
                    ),
                )
                evidence = tuple(
                    run[
                        evidence_start:
                        evidence_start + NEAR_NEIGHBOR_MAX_DIRECT_READINGS
                    ]
                )
                first = evidence[0]
                last = evidence[-1]
                assert first.clock_seconds is not None
                assert last.clock_seconds is not None
                video_span = last.frame_seconds - first.frame_seconds
                clock_span = last.clock_seconds - first.clock_seconds
                if video_span <= 0:
                    continue
                slope = clock_span / video_span
                if not (
                    NEAR_NEIGHBOR_MIN_CLOCK_VIDEO_SLOPE
                    <= slope
                    <= NEAR_NEIGHBOR_MAX_CLOCK_VIDEO_SLOPE
                ):
                    continue
                candidates.append(
                    _ClockSecondEstimate(
                        # Keep the real observed frame. Do not project into a
                        # missing target second between retained fragments.
                        frame_seconds=nearest.frame_seconds,
                        segment_index=segment_index,
                        nearest=nearest,
                        evidence=evidence,
                        direction=(
                            "preceding_observed_reading"
                            if nearest.clock_seconds < event_second
                            else "following_observed_reading"
                        ),
                        clock_distance_seconds=minimum_distance,
                        clock_video_slope=slope,
                    )
                )
    return candidates


def _stable_clock_mapping_projections(
    readings: Sequence[FrameReading],
    *,
    event_second: int,
    sample_interval_seconds: float,
) -> list[_ClockMappingProjection]:
    """Project an obscured target from bounded, stable direct OCR runs."""
    ordered = sorted(readings, key=lambda reading: reading.frame_index)
    direct = [
        reading for reading in ordered if _direct_estimate_clock_reading(reading)
    ]
    if len(direct) < CLOCK_MAPPING_MIN_DIRECT_READINGS:
        return []

    projections: list[_ClockMappingProjection] = []
    for index, (left, right) in enumerate(zip(direct, direct[1:])):
        assert left.clock_seconds is not None
        assert right.clock_seconds is not None
        if not left.clock_seconds < event_second < right.clock_seconds:
            continue
        if index < 1 or index + 2 >= len(direct):
            continue
        left_support = direct[index - 1]
        right_support = direct[index + 2]
        if not (
            _near_one_to_one_pair(left_support, left)
            and _near_one_to_one_pair(right, right_support)
        ):
            continue
        video_gap = right.frame_seconds - left.frame_seconds
        clock_gap = right.clock_seconds - left.clock_seconds
        if (
            video_gap <= 0
            or video_gap > CLOCK_MAPPING_MAX_OCCLUSION_SECONDS
            or clock_gap <= 0
        ):
            continue
        gap_slope = video_gap / clock_gap
        if not (
            NEAR_NEIGHBOR_MIN_CLOCK_VIDEO_SLOPE
            <= gap_slope
            <= NEAR_NEIGHBOR_MAX_CLOCK_VIDEO_SLOPE
        ):
            continue

        covered = [
            reading
            for reading in ordered
            if left.frame_index <= reading.frame_index <= right.frame_index
        ]
        expected_indices = list(range(left.frame_index, right.frame_index + 1))
        if [reading.frame_index for reading in covered] != expected_indices:
            continue
        maximum_frame_step = max(
            1.5,
            float(sample_interval_seconds) * 1.5,
        )
        if any(
            current.frame_seconds <= previous.frame_seconds
            or current.frame_seconds - previous.frame_seconds > maximum_frame_step
            for previous, current in zip(covered, covered[1:])
        ):
            continue
        if any(
            reading.continuity_status == "resynchronized"
            or reading.continuity_reason
            in {
                "clock_discontinuity",
                "continuous_observations_resynchronized",
                "period_boundary_resynchronized",
            }
            for reading in covered
        ):
            continue

        evidence = (left_support, left, right, right_support)
        clocks = [float(reading.clock_seconds) for reading in evidence]
        frames = [reading.frame_seconds for reading in evidence]
        mean_clock = sum(clocks) / len(clocks)
        mean_frame = sum(frames) / len(frames)
        denominator = sum((clock - mean_clock) ** 2 for clock in clocks)
        if denominator <= 0:
            continue
        slope = sum(
            (clock - mean_clock) * (frame - mean_frame)
            for clock, frame in zip(clocks, frames)
        ) / denominator
        if not (
            NEAR_NEIGHBOR_MIN_CLOCK_VIDEO_SLOPE
            <= slope
            <= NEAR_NEIGHBOR_MAX_CLOCK_VIDEO_SLOPE
        ):
            continue
        intercept = mean_frame - slope * mean_clock
        residuals = [
            abs(frame - (slope * clock + intercept))
            for clock, frame in zip(clocks, frames)
        ]
        maximum_residual = max(residuals)
        if maximum_residual > CLOCK_MAPPING_MAX_RESIDUAL_SECONDS:
            continue
        projected_frame = slope * event_second + intercept
        if not left.frame_seconds <= projected_frame <= right.frame_seconds:
            continue
        projections.append(
            _ClockMappingProjection(
                frame_seconds=projected_frame,
                evidence=evidence,
                left=left,
                right=right,
                slope=slope,
                intercept=intercept,
                maximum_residual_seconds=maximum_residual,
                error_bound_seconds=max(
                    float(sample_interval_seconds) / 2.0,
                    maximum_residual,
                ),
                mapping_kind="interpolation",
                projection_distance_seconds=0,
            )
        )
    if projections:
        return projections

    # If the clock becomes hidden immediately after a stable direct run, allow
    # a short one-sided projection only while sampled video itself remains
    # continuous through the projected target frame.
    for run in _direct_estimate_runs(ordered):
        if len(run) < CLOCK_MAPPING_MIN_DIRECT_READINGS:
            continue
        assert run[0].clock_seconds is not None
        assert run[-1].clock_seconds is not None
        if run[-1].clock_seconds < event_second:
            evidence = tuple(run[-CLOCK_MAPPING_MIN_DIRECT_READINGS:])
            nearest = evidence[-1]
            direction = "forward_extrapolation"
        elif run[0].clock_seconds > event_second:
            evidence = tuple(run[:CLOCK_MAPPING_MIN_DIRECT_READINGS])
            nearest = evidence[0]
            direction = "backward_extrapolation"
        else:
            continue
        assert nearest.clock_seconds is not None
        projection_distance = abs(event_second - nearest.clock_seconds)
        if not 1 <= projection_distance <= CLOCK_MAPPING_MAX_EXTRAPOLATION_SECONDS:
            continue

        clocks = [float(reading.clock_seconds) for reading in evidence]
        frames = [reading.frame_seconds for reading in evidence]
        mean_clock = sum(clocks) / len(clocks)
        mean_frame = sum(frames) / len(frames)
        denominator = sum((clock - mean_clock) ** 2 for clock in clocks)
        if denominator <= 0:
            continue
        slope = sum(
            (clock - mean_clock) * (frame - mean_frame)
            for clock, frame in zip(clocks, frames)
        ) / denominator
        if not (
            NEAR_NEIGHBOR_MIN_CLOCK_VIDEO_SLOPE
            <= slope
            <= NEAR_NEIGHBOR_MAX_CLOCK_VIDEO_SLOPE
        ):
            continue
        intercept = mean_frame - slope * mean_clock
        residuals = [
            abs(frame - (slope * clock + intercept))
            for clock, frame in zip(clocks, frames)
        ]
        maximum_residual = max(residuals)
        if maximum_residual > CLOCK_MAPPING_MAX_RESIDUAL_SECONDS:
            continue
        projected_frame = slope * event_second + intercept
        span_start = min(nearest.frame_seconds, projected_frame)
        span_end = max(nearest.frame_seconds, projected_frame)
        covered = [
            reading
            for reading in ordered
            if span_start - 1e-6 <= reading.frame_seconds <= span_end + 1e-6
        ]
        if (
            not covered
            or covered[0].frame_seconds > span_start + 1e-6
            or covered[-1].frame_seconds < span_end - 1e-6
        ):
            continue
        expected_indices = list(
            range(covered[0].frame_index, covered[-1].frame_index + 1)
        )
        if [reading.frame_index for reading in covered] != expected_indices:
            continue
        maximum_frame_step = max(1.5, float(sample_interval_seconds) * 1.5)
        if any(
            current.frame_seconds <= previous.frame_seconds
            or current.frame_seconds - previous.frame_seconds > maximum_frame_step
            for previous, current in zip(covered, covered[1:])
        ):
            continue
        if any(
            reading.continuity_status == "resynchronized"
            or reading.continuity_reason
            in {
                "clock_discontinuity",
                "continuous_observations_resynchronized",
                "period_boundary_resynchronized",
            }
            for reading in covered
        ):
            continue
        projections.append(
            _ClockMappingProjection(
                frame_seconds=projected_frame,
                evidence=evidence,
                left=evidence[0],
                right=evidence[-1],
                slope=slope,
                intercept=intercept,
                maximum_residual_seconds=maximum_residual,
                error_bound_seconds=max(
                    float(sample_interval_seconds) / 2.0,
                    maximum_residual + projection_distance * 0.1,
                ),
                mapping_kind=direction,
                projection_distance_seconds=projection_distance,
            )
        )
    return projections


def _locate_goal_second(
    readings: Sequence[FrameReading],
    *,
    event_second: int,
    candidate_start_seconds: float,
    sample_interval_seconds: float,
    diagnostics: dict[str, Any],
    allow_nearby_observed_clock: bool = False,
    minimum_confidence: float = 0.35,
) -> dict[str, Any]:
    target_clock = _clock_text(event_second)
    segments = _clock_segments(
        readings,
        sample_interval_seconds=sample_interval_seconds,
    )
    trusted_count = sum(len(segment) for segment in segments)
    base = {
        **diagnostics,
        "target_clock": target_clock,
        "target_clock_seconds": event_second,
        "trusted_clock_frame_count": trusted_count,
        "clock_continuity_segment_count": len(segments),
    }
    if not segments:
        raise WorkerError(
            "ocr_clock_unreadable",
            f"no trustworthy match-clock readings were available for {target_clock}",
            diagnostics={
                **base,
                "exact_second_failure_reason": "no_trustworthy_clock_readings",
                "exact_second_failure_cause": "unreadable",
            },
        )

    observed_candidates: list[_ClockSecondCandidate] = []
    interpolated_candidates: list[_ClockSecondCandidate] = []
    isolated_target_candidates: list[_ClockSecondCandidate] = []
    isolated_target_reading_count = 0
    for segment_index, segment in enumerate(segments):
        for reading in segment:
            if reading.clock_seconds == event_second:
                if len(segment) < 2:
                    isolated_target_reading_count += 1
                    if (
                        _direct_estimate_clock_reading(reading)
                        and reading.mean_confidence is not None
                        and reading.mean_confidence >= minimum_confidence
                    ):
                        isolated_target_candidates.append(
                            _ClockSecondCandidate(
                                reading.frame_seconds,
                                "paddleocr_single_frame_target",
                                "estimated_second",
                                segment_index,
                                reading,
                            )
                        )
                    continue
                observed_candidates.append(
                    _ClockSecondCandidate(
                        reading.frame_seconds,
                        "paddleocr_exact_clock",
                        "observed_second",
                        segment_index,
                        reading,
                    )
                )
        for left, right in zip(segment, segment[1:]):
            assert left.clock_seconds is not None
            assert right.clock_seconds is not None
            if not left.clock_seconds < event_second < right.clock_seconds:
                continue
            video_delta = right.frame_seconds - left.frame_seconds
            clock_delta = right.clock_seconds - left.clock_seconds
            interpolation_tolerance = max(1.5, sample_interval_seconds * 0.75)
            if (
                video_delta <= 0
                or abs(clock_delta - video_delta) > interpolation_tolerance
            ):
                continue
            fraction = (event_second - left.clock_seconds) / clock_delta
            interpolated_candidates.append(
                _ClockSecondCandidate(
                    left.frame_seconds + fraction * video_delta,
                    "paddleocr_interpolated_clock",
                    "interpolated_second",
                    segment_index,
                    left,
                    right,
                )
            )
    # Preserve the established exact contract: a direct observation wins, then
    # a local two-sided interpolation. Wider nearby/mapping recovery comes later.
    candidates = observed_candidates or interpolated_candidates
    accepted_isolated_target_reading_count = len(isolated_target_candidates)
    candidate_source = (
        "direct_observation"
        if observed_candidates
        else "two_sided_interpolation"
        if interpolated_candidates
        else None
    )
    matching_segments = sorted({candidate.segment_index for candidate in candidates})
    match_diagnostics = {
        **base,
        "isolated_target_reading_count": isolated_target_reading_count,
        "accepted_isolated_target_reading_count": (
            accepted_isolated_target_reading_count
        ),
        "exact_second_candidate_source": candidate_source,
        "direct_observation_candidate_count": len(observed_candidates),
        "two_sided_interpolation_candidate_count": len(interpolated_candidates),
    }
    if len(matching_segments) > 1:
        raise WorkerError(
            "ocr_ambiguous",
            f"the target match clock {target_clock} appeared in multiple disjoint intervals",
            diagnostics={
                **match_diagnostics,
                "exact_second_failure_reason": "multiple_disjoint_occurrences",
                "matching_occurrence_count": len(matching_segments),
                "matching_frame_seconds": [
                    round(candidate.frame_seconds, 3) for candidate in candidates
                ],
            },
        )
    if not candidates and isolated_target_candidates:
        isolated_occurrence_count = len(isolated_target_candidates)
        if isolated_occurrence_count != 1:
            raise WorkerError(
                "ocr_ambiguous",
                f"the isolated target match clock {target_clock} had conflicting OCR evidence",
                diagnostics={
                    **match_diagnostics,
                    "accepted_isolated_target_reading_count": (
                        accepted_isolated_target_reading_count
                    ),
                    "exact_second_failure_reason": (
                        "conflicting_isolated_target_observations"
                    ),
                    "matching_occurrence_count": isolated_occurrence_count,
                    "matching_frame_seconds": [
                        round(candidate.frame_seconds, 3)
                        for candidate in isolated_target_candidates
                    ],
                },
            )
        selected = isolated_target_candidates[0]
        anchor = candidate_start_seconds + selected.frame_seconds
        error_bound = round(max(0.5, sample_interval_seconds / 2.0), 3)
        degradation_reason = {
            "kind": "single_frame_target_observation",
            "message": (
                f"the target match clock {target_clock} was clearly read in one "
                "frame, but no adjacent clock reading was available to confirm it"
            ),
            "target_clock_directly_observed": True,
            "estimated_error_bound_seconds": error_bound,
        }
        diagnostics.update({
            **match_diagnostics,
            "accepted_isolated_target_reading_count": 1,
            "matching_occurrence_count": 1,
            "exact_second_candidate_source": (
                "single_frame_target_observation"
            ),
            "exact_second_method": selected.method,
            "exact_second_precision": selected.precision,
            "target_frame_seconds": round(selected.frame_seconds, 3),
            "target_clock_directly_observed": True,
            "single_frame_mean_confidence": round(
                float(selected.left.mean_confidence), 4
            ),
            "single_frame_minimum_confidence": minimum_confidence,
            "estimated_error_bound_seconds": error_bound,
            "interpolation_clock_bounds": None,
            "interpolation_frame_bounds": None,
        })
        return {
            "anchor_seconds": round(anchor, 3),
            "method": selected.method,
            "precision": selected.precision,
            "location_kind": "match_clock_second",
            "localization_quality": "estimated",
            "degraded": True,
            "degradation_mode": "single_frame_target_observation",
            "degradation_reason": degradation_reason,
            "target_clock_directly_observed": True,
            "estimated_error_bound_seconds": error_bound,
            "estimated_error_bound_label": f"+/-{error_bound:g}s",
            "target_clock": target_clock,
            "target_clock_seconds": event_second,
            "observed_clock": target_clock,
            "observed_clock_seconds": event_second,
            "observed_clock_delta_seconds": 0,
            "observed_clock_distance_seconds": 0,
            "requires_tdeed": False,
            "diagnostics": diagnostics,
        }
    nearby_observations: list[_ClockSecondEstimate] = []
    if (
        not candidates
        and isolated_target_reading_count == 0
        and allow_nearby_observed_clock
    ):
        nearby_observations = _nearby_observed_clock_candidates(
            segments,
            event_second=event_second,
        )
        if nearby_observations:
            minimum_distance = min(
                candidate.clock_distance_seconds
                for candidate in nearby_observations
            )
            nearby_observations = [
                candidate
                for candidate in nearby_observations
                if candidate.clock_distance_seconds == minimum_distance
            ]
            if len(nearby_observations) > 1:
                raise WorkerError(
                    "ocr_ambiguous",
                    f"the closest readable clock to {target_clock} appeared at multiple video positions",
                    diagnostics={
                        **match_diagnostics,
                        "exact_second_failure_reason": (
                            "multiple_equally_near_observed_clocks"
                        ),
                        "matching_occurrence_count": len(nearby_observations),
                        "nearby_observed_clock_distance_seconds": minimum_distance,
                        "nearby_observed_frame_seconds": [
                            round(candidate.frame_seconds, 3)
                            for candidate in nearby_observations
                        ],
                    },
                )
    if nearby_observations:
        selected_observation = nearby_observations[0]
        nearest = selected_observation.nearest
        assert nearest.clock_seconds is not None
        evidence = selected_observation.evidence
        anchor = candidate_start_seconds + nearest.frame_seconds
        signed_delta = nearest.clock_seconds - event_second
        distance = selected_observation.clock_distance_seconds
        degradation_reason = {
            "kind": "nearby_observed_clock",
            "message": (
                f"the exact target frame was unavailable; used the real OCR frame "
                f"at {_clock_text(nearest.clock_seconds)} ({signed_delta:+d}s from target)"
            ),
            "clock_difference_seconds": signed_delta,
            "accepted_tolerance_seconds": (
                NEARBY_OBSERVED_MAX_CLOCK_DISTANCE_SECONDS
            ),
        }
        diagnostics.update(
            {
                "target_clock": target_clock,
                "target_clock_seconds": event_second,
                "trusted_clock_frame_count": trusted_count,
                "clock_continuity_segment_count": len(segments),
                "isolated_target_reading_count": isolated_target_reading_count,
                "matching_occurrence_count": 1,
                "exact_second_candidate_source": "nearby_direct_observation",
                "direct_observation_candidate_count": len(observed_candidates),
                "two_sided_interpolation_candidate_count": len(
                    interpolated_candidates
                ),
                "exact_second_method": "paddleocr_nearby_clock_observation",
                "exact_second_precision": "estimated_second",
                "target_frame_seconds": round(nearest.frame_seconds, 3),
                "nearby_observed_clock": _clock_text(nearest.clock_seconds),
                "nearby_observed_clock_seconds": nearest.clock_seconds,
                "nearby_observed_frame_seconds": round(nearest.frame_seconds, 3),
                "nearby_observed_clock_delta_seconds": signed_delta,
                "nearby_observed_clock_distance_seconds": distance,
                "nearby_observed_direction": selected_observation.direction,
                "estimate_nearest_clock": _clock_text(nearest.clock_seconds),
                "estimate_clock_distance_seconds": distance,
                "nearby_observed_tolerance_seconds": (
                    NEARBY_OBSERVED_MAX_CLOCK_DISTANCE_SECONDS
                ),
                "nearby_observed_direct_reading_count": len(evidence),
                "nearby_observed_evidence_frame_indices": [
                    reading.frame_index for reading in evidence
                ],
                "nearby_observed_evidence_clock_bounds": [
                    _clock_text(evidence[0].clock_seconds),
                    _clock_text(evidence[-1].clock_seconds),
                ],
                "nearby_observed_clock_video_slope": round(
                    selected_observation.clock_video_slope, 6
                ),
                "interpolation_clock_bounds": None,
                "interpolation_frame_bounds": None,
            }
        )
        return {
            "anchor_seconds": round(anchor, 3),
            "method": "paddleocr_nearby_clock_observation",
            "precision": "estimated_second",
            "location_kind": "match_clock_second",
            "localization_quality": "estimated",
            "degraded": True,
            "degradation_mode": "nearby_observed_clock",
            "degradation_reason": degradation_reason,
            "estimated_error_bound_seconds": distance,
            "estimated_error_bound_label": f"+/-{distance}s",
            "target_clock": target_clock,
            "target_clock_seconds": event_second,
            "observed_clock": _clock_text(nearest.clock_seconds),
            "observed_clock_seconds": nearest.clock_seconds,
            "observed_clock_delta_seconds": signed_delta,
            "observed_clock_distance_seconds": distance,
            "estimate_clock_distance_seconds": distance,
            "accepted_clock_tolerance_seconds": (
                NEARBY_OBSERVED_MAX_CLOCK_DISTANCE_SECONDS
            ),
            "requires_tdeed": False,
            "diagnostics": diagnostics,
        }

    mapping_projections: list[_ClockMappingProjection] = []
    if not candidates and not nearby_observations and isolated_target_reading_count == 0:
        mapping_projections = _stable_clock_mapping_projections(
            readings,
            event_second=event_second,
            sample_interval_seconds=sample_interval_seconds,
        )
        if len(mapping_projections) > 1:
            raise WorkerError(
                "ocr_ambiguous",
                f"multiple stable clock mappings projected {target_clock} to different video positions",
                diagnostics={
                    **match_diagnostics,
                    "exact_second_failure_reason": (
                        "multiple_stable_mapping_projections"
                    ),
                    "matching_occurrence_count": len(mapping_projections),
                    "projected_frame_seconds": [
                        round(projection.frame_seconds, 3)
                        for projection in mapping_projections
                    ],
                },
            )

    if mapping_projections:
        projection = mapping_projections[0]
        anchor = candidate_start_seconds + projection.frame_seconds
        error_bound = round(projection.error_bound_seconds, 3)
        mapping_diagnostics = {
            "status": "stable",
            "sample_count": len(projection.evidence),
            "frame_span_seconds": round(
                projection.evidence[-1].frame_seconds
                - projection.evidence[0].frame_seconds,
                3,
            ),
            "clock_span_seconds": (
                int(projection.evidence[-1].clock_seconds)
                - int(projection.evidence[0].clock_seconds)
            ),
            "slope": round(projection.slope, 6),
            "intercept": round(projection.intercept, 6),
            "maximum_residual_seconds": round(
                projection.maximum_residual_seconds, 3
            ),
            "mapping_kind": projection.mapping_kind,
            "projection_distance_seconds": (
                projection.projection_distance_seconds
            ),
            "left_clock": _clock_text(projection.left.clock_seconds),
            "right_clock": _clock_text(projection.right.clock_seconds),
            "left_frame_seconds": round(projection.left.frame_seconds, 3),
            "right_frame_seconds": round(projection.right.frame_seconds, 3),
            "video_gap_checked": True,
            "clock_regression_checked": True,
            "resynchronization_checked": True,
        }
        mapping_basis = {
            "interpolation": "遮挡前后连续可读的比赛时钟",
            "forward_extrapolation": "遮挡前连续可读的比赛时钟",
            "backward_extrapolation": "遮挡后连续可读的比赛时钟",
        }.get(projection.mapping_kind, "连续可读的比赛时钟")
        degradation_reason = {
            "kind": "mapped_clock_projection",
            "message": (
                f"目标时钟所在画面被遮挡，已根据{mapping_basis}"
                f"估算画面位置，预计误差不超过 {error_bound:g} 秒"
            ),
            "estimated_error_bound_seconds": error_bound,
            "target_clock_directly_observed": False,
        }
        diagnostics.update(
            {
                "target_clock": target_clock,
                "target_clock_seconds": event_second,
                "trusted_clock_frame_count": trusted_count,
                "clock_continuity_segment_count": len(segments),
                "isolated_target_reading_count": isolated_target_reading_count,
                "matching_occurrence_count": 1,
                "exact_second_candidate_source": "stable_clock_video_mapping",
                "direct_observation_candidate_count": len(observed_candidates),
                "two_sided_interpolation_candidate_count": 0,
                "stable_mapping_projection_candidate_count": 1,
                "exact_second_method": "paddleocr_stable_clock_mapping",
                "exact_second_precision": "projected_second",
                "target_frame_seconds": round(projection.frame_seconds, 3),
                "target_clock_directly_observed": False,
                "estimated_error_bound_seconds": error_bound,
                "clock_video_mapping": mapping_diagnostics,
                "mapping_evidence_frame_indices": [
                    reading.frame_index for reading in projection.evidence
                ],
                "mapping_evidence_clocks": [
                    _clock_text(reading.clock_seconds)
                    for reading in projection.evidence
                ],
                "mapping_evidence_frame_seconds": [
                    round(reading.frame_seconds, 3)
                    for reading in projection.evidence
                ],
                "interpolation_clock_bounds": [
                    _clock_text(projection.left.clock_seconds),
                    _clock_text(projection.right.clock_seconds),
                ],
                "interpolation_frame_bounds": [
                    round(projection.left.frame_seconds, 3),
                    round(projection.right.frame_seconds, 3),
                ],
            }
        )
        return {
            "anchor_seconds": round(anchor, 3),
            "method": "paddleocr_stable_clock_mapping",
            "precision": "projected_second",
            "location_kind": "match_clock_second",
            "localization_quality": "projected",
            "degraded": True,
            "degradation_mode": "mapped_clock_projection",
            "degradation_reason": degradation_reason,
            "projection_status": "estimated",
            "target_clock_directly_observed": False,
            "estimated_error_bound_seconds": error_bound,
            "estimated_error_bound_label": f"+/-{error_bound:g}s",
            "target_clock": target_clock,
            "target_clock_seconds": event_second,
            "clock_video_mapping": mapping_diagnostics,
            "requires_tdeed": False,
            "diagnostics": diagnostics,
        }

    if not candidates and not nearby_observations:
        readable_clocks = [
            reading.clock_seconds
            for segment in segments
            for reading in segment
            if reading.clock_seconds is not None
        ]
        raise WorkerError(
            "ocr_exact_second_not_found",
            f"the target match clock {target_clock} was not directly observed and no safe fallback was available",
            diagnostics={
                **match_diagnostics,
                "exact_second_failure_reason": "target_clock_not_found",
                "exact_second_failure_cause": (
                    "isolated"
                    if isolated_target_reading_count
                    else "continuity"
                    if matching_segments == [] and len(readable_clocks) >= 2
                    else "unreadable"
                ),
                "trusted_clock_range": [
                    _clock_text(min(readable_clocks)),
                    _clock_text(max(readable_clocks)),
                ],
                "matching_occurrence_count": 0,
                "stable_mapping_projection_candidate_count": 0,
            },
        )

    selected = min(candidates, key=lambda candidate: candidate.frame_seconds)
    anchor = candidate_start_seconds + selected.frame_seconds
    diagnostics.update(
        {
            "target_clock": target_clock,
            "target_clock_seconds": event_second,
            "trusted_clock_frame_count": trusted_count,
            "clock_continuity_segment_count": len(segments),
            "isolated_target_reading_count": isolated_target_reading_count,
            "accepted_isolated_target_reading_count": (
                accepted_isolated_target_reading_count
            ),
            "matching_occurrence_count": 1,
            "exact_second_candidate_source": candidate_source,
            "direct_observation_candidate_count": len(observed_candidates),
            "two_sided_interpolation_candidate_count": len(
                interpolated_candidates
            ),
            "exact_second_method": selected.method,
            "exact_second_precision": selected.precision,
            "target_frame_seconds": round(selected.frame_seconds, 3),
            "interpolation_clock_bounds": (
                [
                    _clock_text(selected.left.clock_seconds),
                    _clock_text(selected.right.clock_seconds),
                ]
                if selected.right is not None
                else None
            ),
            "interpolation_frame_bounds": (
                [
                    round(selected.left.frame_seconds, 3),
                    round(selected.right.frame_seconds, 3),
                ]
                if selected.right is not None
                else None
            ),
        }
    )
    return {
        "anchor_seconds": round(anchor, 3),
        "method": selected.method,
        "precision": selected.precision,
        "location_kind": "match_clock_second",
        "localization_quality": "exact",
        "degraded": False,
        "degradation_mode": None,
        "degradation_reason": None,
        "target_clock": target_clock,
        "target_clock_seconds": event_second,
        "requires_tdeed": False,
        "diagnostics": diagnostics,
    }


def _locate_minute_boundary(
    readings: Sequence[FrameReading],
    *,
    event_minute: int,
    candidate_start_seconds: float,
    sample_interval_seconds: float,
    diagnostics: dict[str, Any],
    minimum_confidence: float = 0.35,
) -> dict[str, Any]:
    """Locate the end boundary of the API event minute.

    API minute M describes the running interval [(M-1):00, M:00]. The OCR
    artifact therefore ends at M:00 and contains exactly the preceding minute.
    """
    target_second = event_minute * 60
    try:
        located = _locate_goal_second(
            readings,
            event_second=target_second,
            candidate_start_seconds=candidate_start_seconds,
            sample_interval_seconds=sample_interval_seconds,
            diagnostics=diagnostics,
            allow_nearby_observed_clock=True,
            minimum_confidence=minimum_confidence,
        )
    except WorkerError as exc:
        if exc.kind == "ocr_invalid_request":
            raise
        raise WorkerError(
            "ocr_minute_boundary_not_found",
            f"the end boundary {_clock_text(target_second)} of API minute {event_minute} was not located",
            diagnostics={
                **exc.diagnostics,
                "api_event_minute": event_minute,
                "minute_window_start_clock": _clock_text(max(0, target_second - 60)),
                "minute_window_end_clock": _clock_text(target_second),
                "minute_boundary_failure": exc.as_dict(),
            },
        ) from exc
    estimated_boundary = located.get("precision") == "estimated_second"
    projected_boundary = located.get("precision") == "projected_second"
    located.update({
        "method": (
            "paddleocr_projected_minute_boundary"
            if projected_boundary
            else
            "paddleocr_estimated_minute_boundary"
            if estimated_boundary
            else "paddleocr_minute_boundary"
        ),
        "precision": (
            "projected_minute_boundary"
            if projected_boundary
            else
            "estimated_minute_boundary"
            if estimated_boundary
            else "minute_boundary"
        ),
        "location_kind": "match_clock_minute_boundary",
        "api_event_minute": event_minute,
        "minute_window_start_clock": _clock_text(max(0, target_second - 60)),
        "minute_window_end_clock": _clock_text(target_second),
    })
    return located


def _attach_exact_second_failure(
    result: dict[str, Any],
    error: WorkerError,
    *,
    degradation_mode: str,
) -> dict[str, Any]:
    failure = error.as_dict()
    result["exact_second_error"] = failure
    result["localization_quality"] = "degraded"
    result["degraded"] = True
    result["degradation_mode"] = degradation_mode
    result["degradation_reason"] = failure
    diagnostics = result.get("diagnostics")
    if isinstance(diagnostics, dict):
        diagnostics["exact_second_failure"] = failure
        diagnostics["target_clock"] = error.diagnostics.get("target_clock")
        diagnostics["exact_second_failure_reason"] = error.diagnostics.get(
            "exact_second_failure_reason"
        )
    return result


def _combined_goal_location_failure(
    score_error: WorkerError, exact_second_error: WorkerError
) -> WorkerError:
    preferred = (
        exact_second_error
        if exact_second_error.kind == "ocr_ambiguous"
        else score_error
    )
    return WorkerError(
        preferred.kind,
        preferred.message,
        diagnostics={
            **preferred.diagnostics,
            "exact_second_failure": exact_second_error.as_dict(),
            "score_transition_failure": score_error.as_dict(),
        },
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


def _event_minute_candidates(event_minute: int) -> tuple[int, ...]:
    """Accept the API minute and the preceding displayed minute."""
    return tuple(
        dict.fromkeys(
            value for value in (event_minute - 1, event_minute) if value >= 0
        )
    )


def _matching_clock_groups(
    readings: Sequence[FrameReading],
    *,
    event_minute: int,
    sample_interval_seconds: float,
) -> tuple[list[list[FrameReading]], list[dict[str, Any]]]:
    event_minutes = set(_event_minute_candidates(event_minute))
    matching = [
        reading
        for reading in readings
        if reading.clock_seconds is not None
        and reading.clock_seconds // 60 in event_minutes
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
                and item.clock_seconds // 60 not in event_minutes
                for item in intervening_readable
            )
        )
        if starts_new_group:
            groups.append([reading])
        else:
            groups[-1].append(reading)

    # A low-resolution clock can be unreadable for most of a minute while the
    # trusted observations on both sides still prove one continuous timeline.
    # Bridge only that narrow case; repeated overlays and isolated false clocks
    # must continue to produce separate groups.
    maximum_bridge_gap = 75.0
    maximum_projection_error = 3.0
    merged_groups: list[list[FrameReading]] = []
    bridged_gaps: list[dict[str, Any]] = []
    for group in groups:
        if not merged_groups:
            merged_groups.append(group)
            continue
        left_group = merged_groups[-1]
        left_anchor = next(
            (
                item
                for item in reversed(left_group)
                if _trustworthy_clock_reading(item)
            ),
            None,
        )
        right_anchor = next(
            (item for item in group if _trustworthy_clock_reading(item)),
            None,
        )
        boundary_gap = group[0].frame_seconds - left_group[-1].frame_seconds
        intervening_non_target = any(
            item.clock_seconds is not None
            and item.clock_seconds // 60 not in event_minutes
            and left_group[-1].frame_seconds
            < item.frame_seconds
            < group[0].frame_seconds
            for item in readings
        )
        if left_anchor is not None and right_anchor is not None:
            assert left_anchor.clock_seconds is not None
            assert right_anchor.clock_seconds is not None
            anchor_video_gap = (
                right_anchor.frame_seconds - left_anchor.frame_seconds
            )
            clock_advance = (
                right_anchor.clock_seconds - left_anchor.clock_seconds
            )
            projection_error = clock_advance - anchor_video_gap
        else:
            anchor_video_gap = math.inf
            clock_advance = -1
            projection_error = math.inf
        can_bridge = (
            boundary_gap > maximum_missing_gap
            and len(left_group) >= 2
            and len(group) >= 2
            and 0 < anchor_video_gap <= maximum_bridge_gap
            and clock_advance >= 0
            and abs(projection_error) <= maximum_projection_error
            and not intervening_non_target
        )
        if not can_bridge:
            merged_groups.append(group)
            continue
        bridged_gaps.append(
            {
                "left_frame_index": left_anchor.frame_index,
                "right_frame_index": right_anchor.frame_index,
                "left_frame_seconds": round(left_anchor.frame_seconds, 3),
                "right_frame_seconds": round(right_anchor.frame_seconds, 3),
                "left_clock": _clock_text(left_anchor.clock_seconds),
                "right_clock": _clock_text(right_anchor.clock_seconds),
                "boundary_gap_seconds": round(boundary_gap, 3),
                "anchor_video_gap_seconds": round(anchor_video_gap, 3),
                "clock_advance_seconds": clock_advance,
                "projection_error_seconds": round(projection_error, 3),
            }
        )
        left_group.extend(group)
    return merged_groups, bridged_gaps


def _locate_card_interval(
    readings: Sequence[FrameReading],
    *,
    event_minute: int,
    candidate_start_seconds: float,
    sample_interval_seconds: float,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    # A single ambiguous detector result is treated as a missing sample. The
    # continuity state machine may already have repaired the surrounding
    # timeline, and rejecting the whole interval here would turn a normal
    # low-resolution OCR glitch into a false fallback.
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
    groups, bridged_gaps = _matching_clock_groups(
        readings,
        event_minute=event_minute,
        sample_interval_seconds=sample_interval_seconds,
    )
    diagnostics["bridged_matching_clock_gaps"] = bridged_gaps
    event_minutes = set(_event_minute_candidates(event_minute))
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
                "api_event_minute_candidates": sorted(event_minutes),
                "clock_range": [
                    _clock_text(min(clocks)),
                    _clock_text(max(clocks)),
                ],
                "matching_interval_count": len(groups),
            },
        )
    group = groups[0]
    filtered_group: list[FrameReading] = []
    dropped_backward_outliers: list[dict[str, Any]] = []
    for index, current in enumerate(group):
        assert current.clock_seconds is not None
        previous = filtered_group[-1] if filtered_group else None
        if (
            previous is not None
            and previous.clock_seconds is not None
            and current.clock_seconds < previous.clock_seconds - 2
        ):
            next_clock = next(
                (
                    later.clock_seconds
                    for later in group[index + 1 :]
                    if later.clock_seconds is not None
                ),
                None,
            )
            if next_clock is not None and next_clock >= previous.clock_seconds - 2:
                dropped_backward_outliers.append({
                    "frame_index": current.frame_index,
                    "clock": _clock_text(current.clock_seconds),
                })
                continue
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
        filtered_group.append(current)
    group = filtered_group
    interval_start = candidate_start_seconds + group[0].frame_seconds
    interval_end = (
        candidate_start_seconds
        + group[-1].frame_seconds
        + sample_interval_seconds
    )
    diagnostics.update(
        {
            "api_event_minute": event_minute,
            "api_event_minute_candidates": sorted(event_minutes),
            "ocr_interval_clock_start": _clock_text(group[0].clock_seconds),
            "ocr_interval_clock_end": _clock_text(group[-1].clock_seconds),
            "dropped_backward_clock_outliers": dropped_backward_outliers,
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
    raw_clock_only = request.get("clock_only", False)
    if not isinstance(raw_clock_only, bool):
        raise WorkerError("ocr_invalid_request", "clock_only must be a boolean")
    clock_only = raw_clock_only
    sample_interval = float(request.get("sample_interval_seconds", 1.0))
    candidate_start = float(request.get("candidate_start_seconds", 0.0))
    minimum_confidence = float(request.get("minimum_confidence", 0.35))
    if (
        sample_interval <= 0
        or candidate_start < 0
        or not 0 <= minimum_confidence <= 1
    ):
        raise WorkerError("ocr_invalid_request", "invalid OCR timeline parameters")
    diagnostics = _base_diagnostics(
        ordered,
        sample_interval_seconds=sample_interval,
        candidate_start_seconds=candidate_start,
    )
    if clock_only:
        diagnostics.update({"clock_only": True, "score_ocr_skipped": True})
        raw_event_minute = request.get("event_minute")
        if raw_event_minute is None or not str(raw_event_minute).strip():
            raise WorkerError(
                "ocr_invalid_request",
                "event_minute is required when clock_only is enabled",
                diagnostics=diagnostics,
            )
    _raise_for_missing_scoreboard(ordered, diagnostics)
    exact_second_error: WorkerError | None = None
    event_second = request.get("event_second")
    if code in GOAL_LIKE_EVENT_CODES and event_second is not None:
        if isinstance(event_second, bool) or not isinstance(event_second, int):
            raise WorkerError(
                "ocr_invalid_request",
                "event_second must be a cumulative integer second",
                diagnostics={**diagnostics, "event_second": event_second},
            )
        if not 0 <= event_second <= MAX_REASONABLE_MATCH_MINUTE * 60 + 59:
            raise WorkerError(
                "ocr_invalid_request",
                "event_second is outside the supported match clock range",
                diagnostics={**diagnostics, "event_second": event_second},
            )
        try:
            exact = _locate_goal_second(
                ordered,
                event_second=event_second,
                candidate_start_seconds=candidate_start,
                sample_interval_seconds=sample_interval,
                diagnostics=dict(diagnostics),
                allow_nearby_observed_clock=True,
                minimum_confidence=minimum_confidence,
            )
            if clock_only:
                exact["clock_only"] = True
            return exact
        except WorkerError as exc:
            if exc.kind == "ocr_invalid_request":
                raise
            exact_second_error = exc
    if clock_only:
        try:
            boundary = _locate_minute_boundary(
                ordered,
                event_minute=parse_event_minute(raw_event_minute),
                candidate_start_seconds=candidate_start,
                sample_interval_seconds=sample_interval,
                diagnostics=diagnostics,
                minimum_confidence=minimum_confidence,
            )
        except WorkerError as boundary_error:
            if exact_second_error is None:
                raise
            raise WorkerError(
                boundary_error.kind,
                boundary_error.message,
                diagnostics={
                    **boundary_error.diagnostics,
                    "exact_second_failure": exact_second_error.as_dict(),
                    "minute_boundary_failure": boundary_error.as_dict(),
                },
            ) from boundary_error
        if exact_second_error is not None:
            _attach_exact_second_failure(
                boundary,
                exact_second_error,
                degradation_mode="minute_boundary_fallback",
            )
        boundary["clock_only"] = True
        return boundary
    if code in GOAL_LIKE_EVENT_CODES:
        stable_frames = int(request.get("stable_frames", 2))
        anchor_lead_seconds = float(request.get("anchor_lead_seconds", 3.0))
        if stable_frames < 2:
            raise WorkerError("ocr_invalid_request", "stable_frames must be at least 2")
        if anchor_lead_seconds < 0:
            raise WorkerError(
                "ocr_invalid_request",
                "anchor_lead_seconds must not be negative",
            )
        raw_target_score = request.get("target_score")
        if not str(raw_target_score or "").strip():
            event_minute = request.get("event_minute")
            if exact_second_error is None:
                raise WorkerError(
                    "ocr_invalid_request",
                    "target_score or event_second is required for goal-like events",
                    diagnostics=diagnostics,
                )
            if event_minute is None or not str(event_minute).strip():
                raise exact_second_error
            try:
                interval = _locate_card_interval(
                    ordered,
                    event_minute=parse_event_minute(event_minute),
                    candidate_start_seconds=candidate_start,
                    sample_interval_seconds=sample_interval,
                    diagnostics=dict(diagnostics),
                )
            except WorkerError as interval_error:
                raise WorkerError(
                    exact_second_error.kind,
                    exact_second_error.message,
                    diagnostics={
                        **exact_second_error.diagnostics,
                        "minute_interval_failure": interval_error.as_dict(),
                    },
                ) from interval_error
            interval["method"] = "paddleocr_goal_clock_interval"
            _attach_exact_second_failure(
                interval,
                exact_second_error,
                degradation_mode="minute_interval_fallback",
            )
            return interval
        target_score = parse_target_score(raw_target_score)
        try:
            score_result = _locate_goal(
                ordered,
                target_score=target_score,
                candidate_start_seconds=candidate_start,
                sample_interval_seconds=sample_interval,
                stable_frames=stable_frames,
                anchor_lead_seconds=anchor_lead_seconds,
                diagnostics=diagnostics,
            )
            if exact_second_error is not None:
                _attach_exact_second_failure(
                    score_result,
                    exact_second_error,
                    degradation_mode="score_transition_fallback",
                )
            return score_result
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
                if exact_second_error is None:
                    raise
                raise _combined_goal_location_failure(
                    score_error, exact_second_error
                ) from score_error
            try:
                interval = _locate_card_interval(
                    ordered,
                    event_minute=parse_event_minute(event_minute),
                    candidate_start_seconds=candidate_start,
                    sample_interval_seconds=sample_interval,
                    diagnostics=dict(diagnostics),
                )
            except WorkerError:
                if exact_second_error is None:
                    raise score_error
                raise _combined_goal_location_failure(
                    score_error, exact_second_error
                ) from score_error
            interval["method"] = "paddleocr_goal_clock_interval"
            interval["score_transition_error"] = score_error.as_dict()
            if exact_second_error is not None:
                _attach_exact_second_failure(
                    interval,
                    exact_second_error,
                    degradation_mode="minute_interval_fallback",
                )
            return interval
    interval = _locate_card_interval(
        ordered,
        event_minute=parse_event_minute(request.get("event_minute")),
        candidate_start_seconds=candidate_start,
        sample_interval_seconds=sample_interval,
        diagnostics=diagnostics,
    )
    return interval


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


_AUTO_DISCOVERY_ENGINE_LOCK = threading.RLock()
_AUTO_DISCOVERY_ENGINES: dict[str, Any] = {}


def load_auto_discovery_engine(
    language: str,
) -> Any:
    """Load a detection-capable PaddleOCR pipeline for a small search batch."""
    normalized = str(language or "en").strip() or "en"
    with _AUTO_DISCOVERY_ENGINE_LOCK:
        cached = _AUTO_DISCOVERY_ENGINES.get(normalized)
        if cached is not None:
            return cached
        try:
            with contextlib.redirect_stdout(sys.stderr):
                from paddleocr import PaddleOCR

                try:
                    engine = PaddleOCR(
                        lang=normalized,
                        text_detection_model_name=os.environ.get(
                            "GIF_OCR_DETECTION_MODEL", "PP-OCRv6_medium_det"
                        ),
                        text_recognition_model_name=os.environ.get(
                            "GIF_OCR_RECOGNITION_MODEL", "PP-OCRv6_medium_rec"
                        ),
                        use_doc_orientation_classify=False,
                        use_doc_unwarping=False,
                        use_textline_orientation=False,
                    )
                except (TypeError, ValueError):
                    # PaddleOCR v2 accepts the legacy constructor and still
                    # returns text boxes in its standard OCR output shape.
                    engine = PaddleOCR(
                        lang=normalized,
                        use_angle_cls=False,
                        show_log=False,
                    )
        except Exception as exc:
            raise WorkerError(
                "ocr_model_unavailable",
                f"PaddleOCR detection model could not be imported or initialized: {exc}",
            ) from exc
        _AUTO_DISCOVERY_ENGINES[normalized] = engine
        return engine


def detect_batch(
    engine: Any,
    frames: Sequence[Any],
    *,
    minimum_confidence: float,
) -> list[list[DetectedText]]:
    """Run the detection-capable engine once for a batch of search frames."""
    if not frames:
        return []
    inputs = [
        str(frame) if isinstance(frame, (PathLike, Path)) else frame
        for frame in frames
    ]
    try:
        with contextlib.redirect_stdout(sys.stderr):
            if hasattr(engine, "predict"):
                raw = engine.predict(inputs)
            elif hasattr(engine, "ocr"):
                try:
                    raw = engine.ocr(inputs, cls=False)
                except TypeError:
                    raw = engine.ocr(inputs)
            else:
                raise AttributeError("PaddleOCR detection engine has no predict/ocr method")
            per_frame = _split_batch_output(raw, len(inputs))
    except WorkerError:
        raise
    except Exception as exc:
        raise WorkerError(
            "ocr_inference_failed",
            f"PaddleOCR detection batch inference failed: {exc}",
            diagnostics={"batch_size": len(inputs), "stage": "auto_search"},
        ) from exc
    return [
        [
            detected
            for detected in _extract_detected_texts(raw_result)
            if detected.confidence >= minimum_confidence
        ]
        for raw_result in per_frame
    ]


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
        generation: int = 0,
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
        if int(generation) < 0:
            raise ValueError("generation must not be negative")

        self.language = str(language)
        self.max_batch_size = int(max_batch_size)
        self.batch_wait_seconds = float(batch_wait_seconds)
        self.queue_capacity = int(queue_capacity)
        self.generation = int(generation)
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
        self._active_lock = threading.Lock()
        self._active: list[_QueuedCrop] = []
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

    def invalidate_generation(self) -> None:
        """Reject queued/in-flight work without waiting for a stuck backend."""
        error = WorkerError(
            "ocr_backend_generation_invalidated",
            "the OCR backend generation was replaced after an inference timeout",
            diagnostics={
                "stage": "batch_inference",
                "backend_unhealthy": True,
                "backend_generation": self.generation,
            },
        )
        with self._state_lock:
            self._terminal_error = error
            self._accepting = False
            self._stop_requested.set()
        self._cancel_queued(error)
        with self._active_lock:
            active = list(self._active)
        for item in active:
            if not item.future.done():
                with contextlib.suppress(InvalidStateError):
                    item.future.set_exception(
                        self._error_for_request(error, item.request)
                    )

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
                        with contextlib.suppress(InvalidStateError):
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
        with self._active_lock:
            self._active.extend(active)
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
                if not item.future.done():
                    with contextlib.suppress(InvalidStateError):
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
                if not item.future.done():
                    with contextlib.suppress(InvalidStateError):
                        item.future.set_exception(
                            self._error_for_request(error, item.request)
                        )
            return
        else:
            inference_seconds = time.perf_counter() - started
            for item, (texts, confidences) in zip(active, normalized):
                request = item.request
                if not item.future.done():
                    with contextlib.suppress(InvalidStateError):
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
                                backend_generation=self.generation,
                            )
                        )
        finally:
            with self._active_lock:
                for item in active:
                    with contextlib.suppress(ValueError):
                        self._active.remove(item)


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
    input_format: str | None = None,
    input_seek_seconds: float = 0.0,
    input_duration_seconds: float | None = None,
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
    ]
    # The direct TS path is represented by a generated, trusted ffconcat
    # manifest.  Keep protocol handling explicit and never infer it from a
    # filename, so ordinary MP4 requests retain the legacy command exactly.
    if input_format == "ffconcat":
        command.extend(["-f", "concat", "-safe", "0"])
    command.append(str(candidate_path))
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


def _validate_ffconcat_manifest(path: Path) -> None:
    """Fail closed on direct-input manifests before passing them to FFmpeg."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise WorkerError(
            "ocr_invalid_request",
            f"cannot read ffconcat candidate manifest: {path}",
        ) from exc
    if not lines or lines[0].strip() != "ffconcat version 1.0":
        raise WorkerError("ocr_invalid_request", "invalid ffconcat candidate manifest header")
    entries = 0
    for raw in lines[1:]:
        line = raw.strip()
        if not line:
            continue
        if not (line.startswith("file '") and line.endswith("'")):
            raise WorkerError("ocr_invalid_request", "ffconcat manifest contains unsupported directives")
        encoded = line[6:-1]
        # The producer escapes embedded single quotes using ffconcat's
        # standard '\'' form. No other protocol or directive is accepted.
        value = encoded.replace("'\\''", "'")
        if not value or "://" in value or value.startswith(("pipe:", "subfile:")):
            raise WorkerError("ocr_invalid_request", "ffconcat manifest contains an unsafe media path")
        media = Path(value)
        try:
            available = media.is_file() and media.stat().st_size > 0
        except OSError:
            available = False
        if not available:
            raise WorkerError(
                "ocr_frame_extraction_failed",
                f"ffconcat media segment is unavailable: {media}",
                diagnostics={"stage": "ffconcat_validation", "path": str(media)},
            )
        entries += 1
    if not entries:
        raise WorkerError("ocr_invalid_request", "ffconcat candidate manifest has no media entries")


def extract_profile_clock_frames(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    sample_interval_seconds: float,
    profile: ScoreboardProfile,
    maximum_frames: int = 300,
    deadline_monotonic: float | None = None,
    input_format: str | None = None,
    input_seek_seconds: float = 0.0,
    input_duration_seconds: float | None = None,
) -> tuple[list[Path], dict[str, Any]]:
    """Extract only the clock ROI for an explicit clock-only request."""
    frame_width, frame_height = probe_video_dimensions(
        candidate_path,
        ffmpeg=ffmpeg,
        timeout_seconds=_remaining_seconds(
            deadline_monotonic,
            stage="ffprobe",
        ),
        input_format=input_format,
        input_seek_seconds=input_seek_seconds,
        input_duration_seconds=input_duration_seconds,
    )
    try:
        clock_x1, clock_y1, clock_x2, clock_y2 = profile.scaled_rois(
            frame_width, frame_height
        )["clock_roi"]
    except ScoreboardOcrError as exc:
        raise WorkerError(
            exc.kind,
            exc.message,
            diagnostics=exc.diagnostics,
        ) from exc
    frame_rate = 1.0 / sample_interval_seconds
    clock_pattern = output_dir / "clock_only_%06d.png"
    command = [
        ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
    ]
    if input_format == "ffconcat":
        command.extend(["-f", "concat", "-safe", "0"])
    command.extend([
        "-i",
        str(candidate_path),
    ])
    if input_seek_seconds > 0:
        command.extend(["-ss", f"{input_seek_seconds:.6f}"])
    if input_duration_seconds is not None:
        command.extend(["-t", f"{input_duration_seconds:.6f}"])
    command.extend([
        "-an",
        "-vf",
        (
            f"fps={frame_rate:.8f},"
            f"crop={clock_x2 - clock_x1}:{clock_y2 - clock_y1}:"
            f"{clock_x1}:{clock_y1},scale=iw*3:ih*3:flags=lanczos"
        ),
        "-frames:v",
        str(maximum_frames),
        str(clock_pattern),
    ])
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
            "FFmpeg timed out while extracting clock crops",
            diagnostics={"stage": "frame_extraction"},
        ) from exc
    except OSError as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            f"cannot start FFmpeg for clock extraction: {exc}",
        ) from exc
    if completed.returncode != 0:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFmpeg could not extract the configured clock ROI",
            diagnostics={"ffmpeg_stderr": (completed.stderr or "")[-2000:]},
        )
    clock_frames = sorted(output_dir.glob("clock_only_*.png"))
    if not clock_frames:
        raise WorkerError(
            "scoreboard_missing",
            "the candidate video did not produce configured clock crops",
        )
    return clock_frames, {
        "profile_id": profile.profile_id,
        "frame_resolution": [frame_width, frame_height],
        "clock_roi": [clock_x1, clock_y1, clock_x2, clock_y2],
        "score_roi": None,
        "clock_only": True,
        "score_ocr_skipped": True,
    }


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
    output_prefix: str = "scoreboard",
    start_seconds: float = 0.0,
) -> list[Path]:
    frame_rate = 1.0 / sample_interval_seconds
    crop = (
        f"crop=trunc(iw*{roi_width_ratio:.6f}/2)*2:"
        f"trunc(ih*{roi_height_ratio:.6f}/2)*2:0:0"
    )
    if not re.fullmatch(r"[A-Za-z0-9_-]+", output_prefix):
        raise ValueError("output_prefix contains unsupported characters")
    output_pattern = output_dir / f"{output_prefix}_%06d.png"
    if start_seconds < 0:
        raise ValueError("start_seconds must not be negative")
    command = [
        ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
    ]
    if start_seconds > 0:
        command.extend(["-ss", f"{start_seconds:.6f}"])
    command.extend(
        [
            "-i",
            str(candidate_path),
            "-an",
            "-vf",
            f"fps={frame_rate:.8f},{crop},"
            f"scale=iw*{AUTO_SEARCH_SCALE}:ih*{AUTO_SEARCH_SCALE}:flags=lanczos",
            "-frames:v",
            str(maximum_frames),
            str(output_pattern),
        ]
    )
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
    frames = sorted(output_dir.glob(f"{output_prefix}_*.png"))
    if not frames:
        raise WorkerError(
            "scoreboard_missing",
            "the candidate video did not produce any scoreboard frames",
        )
    return frames


def _integer_roi(bbox: BBox, frame_width: int, frame_height: int) -> tuple[int, int, int, int]:
    x1 = max(0, min(frame_width - 1, math.floor(bbox[0])))
    y1 = max(0, min(frame_height - 1, math.floor(bbox[1])))
    x2 = max(x1 + 1, min(frame_width, math.ceil(bbox[2])))
    y2 = max(y1 + 1, min(frame_height, math.ceil(bbox[3])))
    return x1, y1, x2, y2


def _independent_stoppage_base_minute(
    request: Mapping[str, Any],
) -> int | None:
    """Enable auxiliary stoppage-clock OCR only for an explicit 45+/90+ event."""
    raw_minute = str(request.get("event_minute") or "").strip()
    match = re.fullmatch(r"(\d{1,3})\s*\+\s*\d{1,2}", raw_minute)
    if match is None:
        return None
    base_minute = int(match.group(1))
    if base_minute not in INDEPENDENT_STOPPAGE_BASE_MINUTES:
        return None
    event_second = request.get("event_second")
    if event_second is not None:
        try:
            if int(event_second) <= base_minute * 60:
                return None
        except (TypeError, ValueError):
            return None
    return base_minute


def _independent_stoppage_candidate_rois(
    clock_roi: BBox,
    *,
    frame_width: int,
    frame_height: int,
) -> tuple[tuple[str, BBox], ...]:
    """Return narrow right/below crops without changing the primary clock ROI."""
    x1, y1, x2, y2 = clock_roi
    width = max(1.0, x2 - x1)
    height = max(1.0, y2 - y1)
    candidates = (
        (
            "right",
            (
                x2,
                max(0.0, y1 - height * 0.5),
                min(float(frame_width), x2 + width * 2.0),
                min(float(frame_height), y2 + height * 0.5),
            ),
        ),
        (
            "below",
            (
                max(0.0, x1 - width * 0.5),
                y2,
                min(float(frame_width), x2 + width * 1.5),
                min(float(frame_height), y2 + height * 2.0),
            ),
        ),
    )
    return tuple(
        (label, roi)
        for label, roi in candidates
        if roi[2] - roi[0] >= 2.0 and roi[3] - roi[1] >= 2.0
    )


def extract_independent_stoppage_frames(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    sample_interval_seconds: float,
    frame_width: int,
    frame_height: int,
    clock_roi: BBox,
    maximum_frames: int,
    deadline_monotonic: float | None,
    input_format: str | None = None,
    input_seek_seconds: float = 0.0,
    input_duration_seconds: float | None = None,
) -> tuple[list[Path], list[str], dict[str, Any]]:
    """Extract independent right/below timer candidates for stoppage events."""
    candidates = [
        (label, _integer_roi(roi, frame_width, frame_height))
        for label, roi in _independent_stoppage_candidate_rois(
            clock_roi,
            frame_width=frame_width,
            frame_height=frame_height,
        )
    ]
    if not candidates:
        return [], [], {"candidate_rois": {}, "frame_count": 0}
    frame_rate = 1.0 / sample_interval_seconds
    command = [ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error"]
    if input_format == "ffconcat":
        command.extend(["-f", "concat", "-safe", "0"])
    command.extend(["-i", str(candidate_path)])
    if input_seek_seconds > 0:
        command.extend(["-ss", f"{input_seek_seconds:.6f}"])
    if input_duration_seconds is not None:
        command.extend(["-t", f"{input_duration_seconds:.6f}"])
    labels = [label for label, _roi in candidates]
    split_sources = [f"stoppage_source_{index}" for index in range(len(candidates))]
    filter_graph = (
        f"[0:v]fps={frame_rate:.8f},split={len(candidates)}"
        + "".join(f"[{source}]" for source in split_sources)
        + ";"
    )
    for index, ((_label, (x1, y1, x2, y2)), source) in enumerate(
        zip(candidates, split_sources)
    ):
        filter_graph += (
            f"[{source}]crop={x2 - x1}:{y2 - y1}:{x1}:{y1},"
            f"scale=iw*3:ih*3:flags=lanczos[stoppage_{index}];"
        )
    command.extend(["-an", "-filter_complex", filter_graph.rstrip(";")])
    for index, label in enumerate(labels):
        pattern = output_dir / f"stoppage_{label}_%06d.png"
        command.extend([
            "-map",
            f"[stoppage_{index}]",
            "-frames:v",
            str(maximum_frames),
            str(pattern),
        ])
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=_remaining_seconds(
                deadline_monotonic, stage="stoppage_frame_extraction"
            ),
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkerError(
            "inference_timeout",
            "FFmpeg timed out while extracting independent stoppage timer crops",
            diagnostics={"stage": "stoppage_frame_extraction"},
        ) from exc
    except OSError as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            f"cannot start FFmpeg for stoppage timer extraction: {exc}",
        ) from exc
    if completed.returncode != 0:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFmpeg could not extract independent stoppage timer crops",
            diagnostics={"ffmpeg_stderr": (completed.stderr or "")[-2000:]},
        )
    streams = {
        label: sorted(output_dir.glob(f"stoppage_{label}_*.png"))
        for label in labels
    }
    frame_count = min((len(paths) for paths in streams.values()), default=0)
    paths: list[Path] = []
    path_labels: list[str] = []
    for frame_index in range(frame_count):
        for label in labels:
            paths.append(streams[label][frame_index])
            path_labels.append(label)
    return paths, path_labels, {
        "candidate_rois": {
            label: list(roi) for label, roi in candidates
        },
        "candidate_labels": labels,
        "frame_count": frame_count,
        "conditional": True,
    }


def extract_auto_roi_frames(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    sample_interval_seconds: float,
    frame_width: int,
    frame_height: int,
    clock_roi: BBox,
    score_roi: BBox | None,
    score_rois: Sequence[BBox] | None = None,
    maximum_frames: int,
    deadline_monotonic: float | None,
    output_prefix: str = "auto",
) -> tuple[list[Path], list[str], dict[str, Any]]:
    """Extract a locked automatic clock ROI and an optional independent score ROI."""
    clock = _integer_roi(clock_roi, frame_width, frame_height)
    raw_score_rois = list(score_rois or ())
    if not raw_score_rois and score_roi is not None:
        raw_score_rois = [score_roi]
    scores = [
        _integer_roi(roi, frame_width, frame_height)
        for roi in raw_score_rois
    ]
    frame_rate = 1.0 / sample_interval_seconds
    clock_x1, clock_y1, clock_x2, clock_y2 = clock
    if not re.fullmatch(r"[A-Za-z0-9_-]+", output_prefix):
        raise ValueError("output_prefix contains unsupported characters")
    clock_pattern = output_dir / f"{output_prefix}_clock_%06d.png"
    command = [
        ffmpeg,
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(candidate_path),
        "-an",
    ]
    if not scores:
        command.extend(
            [
                "-vf",
                (
                    f"fps={frame_rate:.8f},"
                    f"crop={clock_x2 - clock_x1}:{clock_y2 - clock_y1}:{clock_x1}:{clock_y1},"
                    "scale=iw*3:ih*3:flags=lanczos"
                ),
                "-frames:v",
                str(maximum_frames),
                str(clock_pattern),
            ]
        )
    else:
        split_labels = ["clock_source", *[f"score_source_{index}" for index in range(len(scores))]]
        filter_graph = (
            f"[0:v]fps={frame_rate:.8f},split={len(split_labels)}"
            + "".join(f"[{label}]" for label in split_labels)
            + ";"
            + f"[clock_source]crop={clock_x2 - clock_x1}:{clock_y2 - clock_y1}:"
            + f"{clock_x1}:{clock_y1},scale=iw*3:ih*3:flags=lanczos[clock];"
        )
        for index, (score_x1, score_y1, score_x2, score_y2) in enumerate(scores):
            filter_graph += (
                f"[score_source_{index}]crop={score_x2 - score_x1}:{score_y2 - score_y1}:"
                f"{score_x1}:{score_y1},scale=iw*3:ih*3:flags=lanczos[score_{index}];"
            )
        command.extend(["-filter_complex", filter_graph.rstrip(";")])
        command.extend(["-map", "[clock]", "-frames:v", str(maximum_frames), str(clock_pattern)])
        for index in range(len(scores)):
            score_pattern = output_dir / f"{output_prefix}_score_{index}_%06d.png"
            command.extend(["-map", f"[score_{index}]", "-frames:v", str(maximum_frames), str(score_pattern)])
    try:
        completed = subprocess.run(
            command,
            check=False,
            text=True,
            capture_output=True,
            timeout=_remaining_seconds(deadline_monotonic, stage="frame_extraction"),
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkerError(
            "inference_timeout",
            "FFmpeg timed out while extracting automatic clock crops",
            diagnostics={"stage": "frame_extraction"},
        ) from exc
    except OSError as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            f"cannot start FFmpeg for automatic scoreboard extraction: {exc}",
        ) from exc
    if completed.returncode != 0:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "FFmpeg could not extract the automatically discovered clock ROI",
            diagnostics={"ffmpeg_stderr": (completed.stderr or "")[-2000:]},
        )
    clock_frames = sorted(output_dir.glob(f"{output_prefix}_clock_*.png"))
    if not clock_frames:
        raise WorkerError(
            "scoreboard_missing",
            "the automatic clock ROI did not produce any frames",
        )
    if not scores:
        return clock_frames, ["clock"] * len(clock_frames), {
            "clock_roi": list(clock),
            "score_roi": None,
        }
    score_frames_by_roi = [
        sorted(output_dir.glob(f"{output_prefix}_score_{index}_*.png"))
        for index in range(len(scores))
    ]
    if any(len(score_frames) != len(clock_frames) for score_frames in score_frames_by_roi):
        raise WorkerError(
            "ocr_frame_extraction_failed",
            "automatic clock and score crops are not aligned",
            diagnostics={
                "clock_frame_count": len(clock_frames),
                "score_frame_counts": [len(items) for items in score_frames_by_roi],
            },
        )
    paths: list[Path] = []
    kinds: list[str] = []
    for frame_index, clock_frame in enumerate(clock_frames):
        paths.append(clock_frame)
        kinds.append("clock")
        for score_frames in score_frames_by_roi:
            paths.append(score_frames[frame_index])
            kinds.append("score")
    return paths, kinds, {
        "clock_roi": list(clock),
        "score_roi": list(scores[0]) if scores else None,
        "score_rois": [list(roi) for roi in scores],
    }


def _map_search_detections(
    detections: Sequence[DetectedText],
    *,
    search_roi: BBox,
    scale: float = AUTO_SEARCH_SCALE,
) -> list[DetectedText]:
    offset_x, offset_y = search_roi[0], search_roi[1]
    return [
        DetectedText(
            detected.text,
            detected.confidence,
            (
                offset_x + detected.bbox[0] / scale,
                offset_y + detected.bbox[1] / scale,
                offset_x + detected.bbox[2] / scale,
                offset_y + detected.bbox[3] / scale,
            ),
        )
        for detected in detections
    ]


def _search_auto_clock_frames(
    tracker: AutoClockTracker,
    frames: Sequence[Path],
    *,
    detector: Any,
    minimum_confidence: float,
    sample_interval_seconds: float,
    maximum_search_frames: int = 60,
    start_frame_index: int = 0,
) -> tuple[AutoClockDecision | None, list[dict[str, Any]]]:
    decisions: list[dict[str, Any]] = []
    limited = frames[:maximum_search_frames]
    for offset in range(0, len(limited), AUTO_SEARCH_BATCH_FRAMES):
        batch = limited[offset : offset + AUTO_SEARCH_BATCH_FRAMES]
        with _AUTO_DISCOVERY_ENGINE_LOCK:
            detected_batch = detect_batch(
                detector,
                batch,
                minimum_confidence=minimum_confidence,
            )
        for batch_index, detections in enumerate(detected_batch):
            frame_index = start_frame_index + offset + batch_index
            mapped = _map_search_detections(
                detections,
                search_roi=tracker.search_roi,
            )
            decision = tracker.observe_search(
                frame_index,
                frame_index * sample_interval_seconds,
                mapped,
            )
            decisions.append({"frame_index": frame_index, **decision.as_dict()})
            if decision.status in {"locked", "ambiguous"}:
                return decision, decisions
    return None, decisions


def discover_auto_clock(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    language: str,
    sample_interval_seconds: float,
    minimum_confidence: float,
    maximum_frames: int,
    deadline_monotonic: float | None,
    detector: Any | None = None,
    clock_only: bool = False,
) -> tuple[AutoClockTracker, dict[str, Any]]:
    """Discover and validate a clock bbox, expanding from top-left when needed."""
    frame_width, frame_height = probe_video_dimensions(
        candidate_path,
        ffmpeg=ffmpeg,
        timeout_seconds=_remaining_seconds(deadline_monotonic, stage="ffprobe"),
    )
    if detector is None:
        detector = load_auto_discovery_engine(language)

    attempts: list[dict[str, Any]] = []
    for expanded, width_ratio, prefix in (
        (False, AUTO_SEARCH_WIDTH_RATIO, "auto_search_left"),
        (True, 1.0, "auto_search_top"),
    ):
        frames = extract_scoreboard_frames(
            candidate_path,
            output_dir,
            ffmpeg=ffmpeg,
            sample_interval_seconds=sample_interval_seconds,
            roi_width_ratio=width_ratio,
            roi_height_ratio=AUTO_SEARCH_HEIGHT_RATIO,
            maximum_frames=min(maximum_frames, 60),
            timeout_seconds=_remaining_seconds(
                deadline_monotonic, stage="auto_search_extraction"
            ),
            output_prefix=prefix,
        )
        tracker = AutoClockTracker(frame_width, frame_height, clock_only=clock_only)
        tracker.expanded_search = expanded
        decision, frame_decisions = _search_auto_clock_frames(
            tracker,
            frames,
            detector=detector,
            minimum_confidence=minimum_confidence,
            sample_interval_seconds=sample_interval_seconds,
            maximum_search_frames=min(maximum_frames, 60),
        )
        attempts.append(
            {
                "expanded_search": expanded,
                "search_roi": list(tracker.search_roi),
                "searched_frame_count": len(frame_decisions),
                "decisions": frame_decisions[-10:],
            }
        )
        if decision is not None and decision.status == "ambiguous":
            raise WorkerError(
                "ocr_ambiguous",
                "automatic clock search found multiple stable clock candidates",
                diagnostics={
                    "auto_clock": {
                        **decision.as_dict(),
                        "attempts": attempts,
                    }
                },
            )
        if decision is not None and decision.status == "locked":
            return tracker, {
                **decision.as_dict(),
                "frame_resolution": [frame_width, frame_height],
                "attempts": attempts,
            }

    final_reason = "auto_search_failed"
    if any(
        item.get("reason") == "timeline_discontinuous"
        for attempt in attempts
        for item in attempt["decisions"]
    ):
        final_reason = "timeline_discontinuous"
    raise WorkerError(
        "ocr_clock_unreadable" if final_reason == "timeline_discontinuous" else "scoreboard_missing",
        "automatic clock search could not validate one stable match clock",
        diagnostics={
            "auto_clock": {
                "status": "failed",
                "reason": final_reason,
                "attempts": attempts,
            }
        },
    )


def _first_clock_missing_run(
    recognized: Sequence[tuple[list[str], list[float]]],
    kinds: Sequence[str],
    *,
    minimum_length: int = AUTO_REACQUIRE_MISSING_FRAMES,
) -> int | None:
    clock_indices = [index for index, kind in enumerate(kinds) if kind == "clock"]
    run_start: int | None = None
    run_length = 0
    for frame_index, result_index in enumerate(clock_indices):
        texts = recognized[result_index][0]
        missing = parse_clock_texts(texts).clock_seconds is None
        if missing:
            run_start = frame_index if run_start is None else run_start
            run_length += 1
            if run_length >= minimum_length:
                return run_start
        else:
            run_start = None
            run_length = 0
    return None


def _auto_results_by_frame(
    recognized: Sequence[tuple[list[str], list[float]]],
    kinds: Sequence[str],
) -> list[dict[str, tuple[list[str], list[float]]]]:
    if len(recognized) != len(kinds):
        raise ValueError("recognized results and kinds must have equal lengths")
    frames: list[dict[str, tuple[list[str], list[float]]]] = []
    current: dict[str, tuple[list[str], list[float]]] | None = None
    for kind, result in zip(kinds, recognized):
        if kind == "clock":
            current = {"clock": result}
            frames.append(current)
        elif kind == "score" and current is not None:
            previous = current.get("score")
            if previous is None:
                current["score"] = result
            else:
                current["score"] = (
                    [*previous[0], *result[0]],
                    [*previous[1], *result[1]],
                )
        else:
            raise ValueError("automatic OCR results are not clock/score aligned")
    return frames


def _flatten_auto_results(
    frames: Sequence[Mapping[str, tuple[list[str], list[float]]]],
) -> tuple[list[tuple[list[str], list[float]]], list[str]]:
    paired = any("score" in frame for frame in frames)
    recognized: list[tuple[list[str], list[float]]] = []
    kinds: list[str] = []
    for frame in frames:
        recognized.append(frame.get("clock", ([], [])))
        kinds.append("clock")
        if paired:
            recognized.append(frame.get("score", ([], [])))
            kinds.append("score")
    return recognized, kinds


def _merge_auto_results(
    primary: Sequence[Mapping[str, tuple[list[str], list[float]]]],
    recovered: Sequence[Mapping[str, tuple[list[str], list[float]]]],
) -> list[dict[str, tuple[list[str], list[float]]]]:
    """Use a re-acquired ROI only for frames the original ROI cannot parse.

    Scoreboards can move briefly during a goal/replay graphic and then return
    to their original position. A whole-tail ROI replacement would therefore
    turn valid readings before or after the graphic into false gaps.
    """
    if len(primary) != len(recovered):
        raise ValueError("automatic OCR recovery results must have equal lengths")

    def usable_clock(result: tuple[list[str], list[float]] | None) -> bool:
        if result is None:
            return False
        parsed = parse_clock_texts(result[0])
        return parsed.clock_seconds is not None and not parsed.ambiguous

    def usable_score(result: tuple[list[str], list[float]] | None) -> bool:
        if result is None:
            return False
        parsed = parse_score_texts(result[0])
        return parsed.score is not None and not parsed.ambiguous

    merged: list[dict[str, tuple[list[str], list[float]]]] = []
    for old, new in zip(primary, recovered):
        frame: dict[str, tuple[list[str], list[float]]] = {}
        old_clock = old.get("clock")
        new_clock = new.get("clock")
        frame["clock"] = (
            old_clock
            if usable_clock(old_clock) or not usable_clock(new_clock)
            else new_clock
        )
        old_score = old.get("score")
        new_score = new.get("score")
        if old_score is not None or new_score is not None:
            frame["score"] = (
                old_score
                if usable_score(old_score) or not usable_score(new_score)
                else (new_score or ([], []))
            )
        merged.append(frame)
    return merged


def _recover_auto_clock(
    candidate_path: Path,
    output_dir: Path,
    *,
    ffmpeg: str,
    language: str,
    sample_interval_seconds: float,
    minimum_confidence: float,
    maximum_frames: int,
    start_frame_index: int,
    frame_width: int,
    frame_height: int,
    deadline_monotonic: float | None,
    clock_only: bool = False,
) -> tuple[AutoClockTracker | None, dict[str, Any]]:
    """Re-search a missing run using the expanded top-of-frame region."""
    frames = extract_scoreboard_frames(
        candidate_path,
        output_dir,
        ffmpeg=ffmpeg,
        sample_interval_seconds=sample_interval_seconds,
        roi_width_ratio=1.0,
        roi_height_ratio=AUTO_SEARCH_HEIGHT_RATIO,
        maximum_frames=min(60, max(1, maximum_frames - start_frame_index)),
        timeout_seconds=_remaining_seconds(deadline_monotonic, stage="auto_research_extraction"),
        output_prefix="auto_research_top",
        start_seconds=start_frame_index * sample_interval_seconds,
    )
    tracker = AutoClockTracker(frame_width, frame_height, clock_only=clock_only)
    tracker.expanded_search = True
    detector = load_auto_discovery_engine(language)
    decision, decisions = _search_auto_clock_frames(
        tracker,
        frames,
        detector=detector,
        minimum_confidence=minimum_confidence,
        sample_interval_seconds=sample_interval_seconds,
        start_frame_index=start_frame_index,
    )
    diagnostics = {
        "start_frame_index": start_frame_index,
        "status": decision.status if decision is not None else "searching",
        "reason": decision.reason if decision is not None else "auto_search_failed",
        "decision_count": len(decisions),
        "decisions": decisions[-10:],
    }
    return (tracker if decision is not None and decision.status == "locked" else None), diagnostics


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


def _frame_indices_for_kinds(kinds: Sequence[str]) -> list[int]:
    """Map interleaved clock/score crops back to their sampled video frame."""
    frame_indices: list[int] = []
    frame_index = -1
    for kind in kinds:
        if kind == "clock":
            frame_index += 1
        frame_indices.append(max(0, frame_index))
    return frame_indices


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
    source_frame_indices: Sequence[int] | None = None,
) -> tuple[list[tuple[list[str], list[float]]], list[str]]:
    if len(paths) != len(kinds):
        raise ValueError("paths and kinds must have equal lengths")
    pending: list[tuple[int, Future[OcrCropResult]]] = []
    all_futures: list[Future[OcrCropResult]] = []
    recognized: list[tuple[list[str], list[float]]] = [
        ([], []) for _path in paths
    ]
    frame_indices = (
        [int(value) for value in source_frame_indices]
        if source_frame_indices is not None
        else _frame_indices_for_kinds(kinds)
    )
    if len(frame_indices) != len(paths) or any(value < 0 for value in frame_indices):
        raise ValueError("source frame indices must align with OCR paths")

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
            crop_frame_index = frame_indices[index]
            while True:
                try:
                    future = batch_worker.submit(
                        match_id=match_id,
                        video_pts=(
                            candidate_start_seconds + crop_frame_index * sample_interval
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
                "backend_generation": batch_worker.generation,
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
            "ocr_backend_generation_invalidated",
        } or (exc.kind == "inference_timeout" and error_stage == "batch_inference")
        raise WorkerError(
            exc.kind,
            exc.message,
            diagnostics={
                **exc.diagnostics,
                "stage": error_stage,
                "backend_unhealthy": backend_unhealthy,
                "backend_generation": exc.diagnostics.get(
                    "backend_generation", batch_worker.generation
                ),
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


def _independent_stoppage_results_by_frame(
    recognized: Sequence[tuple[list[str], list[float]]],
    labels: Sequence[str],
) -> list[dict[str, tuple[list[str], list[float]]]]:
    if len(recognized) != len(labels):
        raise ValueError("stoppage OCR results and labels must have equal lengths")
    if not labels:
        return []
    first_label = labels[0]
    frames: list[dict[str, tuple[list[str], list[float]]]] = []
    current: dict[str, tuple[list[str], list[float]]] | None = None
    for label, result in zip(labels, recognized):
        if label == first_label:
            current = {}
            frames.append(current)
        if current is None or label in current:
            raise ValueError("stoppage OCR candidate streams are not frame aligned")
        current[label] = result
    return frames


def _independent_stoppage_elapsed_seconds(texts: Sequence[str]) -> int | None:
    parsed = parse_clock_texts(texts)
    if (
        parsed.clock_seconds is None
        or parsed.ambiguous
        or parsed.precision != "second"
        or parsed.clock_format not in {"continuous", "compact"}
    ):
        return None
    minute, _second = divmod(parsed.clock_seconds, 60)
    if not 0 <= minute <= INDEPENDENT_STOPPAGE_MAX_MINUTES:
        return None
    return parsed.clock_seconds


def _confirmed_independent_stoppage_overrides(
    main_results: Sequence[tuple[list[str], list[float]]],
    candidate_frames: Sequence[
        Mapping[str, tuple[list[str], list[float]]]
    ],
    *,
    base_minute: int | None,
    sample_interval: float,
) -> tuple[dict[int, tuple[list[str], list[float]]], dict[str, Any]]:
    """Convert a separately running stoppage timer after three coherent frames."""
    diagnostics: dict[str, Any] = {
        "enabled": base_minute is not None,
        "base_minute": base_minute,
        "minimum_observations": INDEPENDENT_STOPPAGE_MIN_OBSERVATIONS,
        "confirmed": False,
        "confirmed_channels": [],
        "static_candidate_rejected": False,
    }
    if base_minute is None or not candidate_frames:
        return {}, diagnostics
    frame_count = min(len(main_results), len(candidate_frames))
    labels = sorted({label for frame in candidate_frames[:frame_count] for label in frame})
    confirmed: dict[str, list[tuple[int, int, float]]] = {}
    observed_by_label: dict[str, list[dict[str, Any]]] = {}
    for label in labels:
        observations: list[dict[str, Any]] = []
        runs: list[list[tuple[int, int, float]]] = []
        current_run: list[tuple[int, int, float]] = []
        static_run: list[tuple[int, int]] = []
        for frame_index in range(frame_count):
            main_texts, _main_confidences = main_results[frame_index]
            main_clock = parse_clock_texts(main_texts)
            result = candidate_frames[frame_index].get(label, ([], []))
            texts, confidences = result
            elapsed = _independent_stoppage_elapsed_seconds(texts)
            main_is_frozen_base = bool(
                main_clock.clock_seconds == base_minute * 60
                and not main_clock.ambiguous
            )
            observations.append({
                "frame_index": frame_index,
                "texts": list(texts),
                "elapsed_seconds": elapsed,
                "main_is_frozen_base": main_is_frozen_base,
            })
            confidence = min((float(value) for value in confidences), default=0.0)
            if elapsed is None or not main_is_frozen_base:
                if current_run:
                    runs.append(current_run)
                    current_run = []
                static_run = []
                continue
            if (
                static_run
                and frame_index == static_run[-1][0] + 1
                and elapsed == static_run[-1][1]
            ):
                static_run.append((frame_index, elapsed))
            else:
                static_run = [(frame_index, elapsed)]
            if len(static_run) >= INDEPENDENT_STOPPAGE_MIN_OBSERVATIONS:
                diagnostics["static_candidate_rejected"] = True
            if current_run:
                previous_index, previous_elapsed, _previous_confidence = current_run[-1]
                video_delta = (frame_index - previous_index) * sample_interval
                clock_delta = elapsed - previous_elapsed
                allowed_deviation = max(1.0, video_delta * 0.35)
                if (
                    frame_index != previous_index + 1
                    or clock_delta <= 0
                    or abs(clock_delta - video_delta) > allowed_deviation
                ):
                    runs.append(current_run)
                    current_run = []
            current_run.append((frame_index, elapsed, confidence))
        if current_run:
            runs.append(current_run)
        observed_by_label[label] = observations
        qualified = [
            run for run in runs
            if len(run) >= INDEPENDENT_STOPPAGE_MIN_OBSERVATIONS
        ]
        if qualified:
            confirmed[label] = max(qualified, key=len)
    diagnostics["observations"] = observed_by_label
    if not confirmed:
        return {}, diagnostics

    overrides: dict[int, tuple[list[str], list[float]]] = {}
    conflicts: list[int] = []
    for frame_index in range(frame_count):
        candidates = [
            (label, elapsed, confidence)
            for label, run in confirmed.items()
            for index, elapsed, confidence in run
            if index == frame_index
        ]
        elapsed_values = {elapsed for _label, elapsed, _confidence in candidates}
        if len(elapsed_values) != 1:
            if len(elapsed_values) > 1:
                conflicts.append(frame_index)
            continue
        elapsed = next(iter(elapsed_values))
        added_minute, second = divmod(elapsed, 60)
        confidence = max(confidence for _label, _elapsed, confidence in candidates)
        overrides[frame_index] = (
            [f"{base_minute}+{added_minute}:{second:02d}"],
            [confidence],
        )
    diagnostics.update({
        "confirmed": bool(overrides),
        "confirmed_channels": sorted(confirmed),
        "confirmed_frame_indices": sorted(overrides),
        "conflicting_frame_indices": conflicts,
        "method": (
            "independent_stoppage_stopwatch" if overrides else None
        ),
    })
    return overrides, diagnostics


def _independent_stoppage_reading(
    frame_index: int,
    frame_seconds: float,
    clock_texts: Iterable[str],
    score_texts: Iterable[str] = (),
    *,
    clock_confidences: Iterable[float] = (),
    score_confidences: Iterable[float] = (),
) -> FrameReading:
    """Build a confirmed independent-stopwatch reading without re-normalizing it."""
    clock_values = tuple(str(text) for text in clock_texts if str(text).strip())
    score_values = tuple(str(text) for text in score_texts if str(text).strip())
    parsed_clock = parse_clock_texts(clock_values)
    parsed_score = parse_score_texts(score_values)
    if parsed_clock.clock_seconds is None or parsed_clock.ambiguous:
        raise ValueError("confirmed stoppage override must contain one match clock")
    confidence_values = [
        *[float(value) for value in clock_confidences],
        *[float(value) for value in score_confidences],
    ]
    return FrameReading(
        frame_index=int(frame_index),
        frame_seconds=float(frame_seconds),
        texts=clock_values + score_values,
        clock_seconds=parsed_clock.clock_seconds,
        score=parsed_score.score,
        mean_confidence=(
            sum(confidence_values) / len(confidence_values)
            if confidence_values else None
        ),
        ambiguous_clock=False,
        ambiguous_score=parsed_score.ambiguous,
        clock_texts=clock_values,
        score_texts=score_values,
        continuity_status="accepted",
        continuity_reason="independent_stoppage_stopwatch",
        scoreboard_visible=True,
        observed_clock_seconds=parsed_clock.clock_seconds,
    )


def _profile_clock_readings(
    recognized: Sequence[tuple[list[str], list[float]]],
    *,
    profile: ScoreboardProfile,
    sample_interval: float,
    period: int | str | None,
    independent_stoppage_frames: Sequence[
        Mapping[str, tuple[list[str], list[float]]]
    ] = (),
    independent_stoppage_base_minute: int | None = None,
) -> tuple[list[FrameReading], list[dict[str, Any]]]:
    """Build one continuity-aware reading per clock crop, with no score OCR."""
    tracker = ClockContinuityStateMachine(profile)
    stoppage_overrides, stoppage_diagnostics = (
        _confirmed_independent_stoppage_overrides(
            recognized,
            independent_stoppage_frames,
            base_minute=independent_stoppage_base_minute,
            sample_interval=sample_interval,
        )
    )
    readings: list[FrameReading] = []
    continuity_diagnostics: list[dict[str, Any]] = []
    for frame_index, (clock_texts, clock_confidences) in enumerate(recognized):
        raw_clock_texts = list(clock_texts)
        if frame_index in stoppage_overrides:
            tracker.update(
                frame_index * sample_interval,
                raw_clock_texts,
                scoreboard_visible=True,
                period=period,
            )
            override_texts, override_confidences = stoppage_overrides[frame_index]
            reading = _independent_stoppage_reading(
                frame_index,
                frame_index * sample_interval,
                override_texts,
                clock_confidences=override_confidences,
            )
        else:
            reading = split_frame_reading(
                frame_index,
                frame_index * sample_interval,
                clock_texts,
                (),
                clock_confidences=clock_confidences,
                tracker=tracker,
                period=period,
                # The extracted crop exists even when OCR misses one frame. This
                # lets the continuity state machine repair a bounded short gap.
                clock_visible=True,
                scoreboard_visible=bool(clock_texts),
            )
        readings.append(reading)
        continuity_diagnostics.append(
            {
                "frame_index": reading.frame_index,
                "video_seconds": reading.frame_seconds,
                "observed_clock_seconds": reading.observed_clock_seconds,
                "clock_seconds": reading.clock_seconds,
                "status": reading.continuity_status,
                "reason": reading.continuity_reason,
                "clock_texts": list(reading.clock_texts),
                "raw_clock_texts": raw_clock_texts,
                "independent_stoppage_applied": frame_index in stoppage_overrides,
                "score_texts": [],
                "scoreboard_visible": reading.scoreboard_visible,
            }
        )
    if continuity_diagnostics:
        continuity_diagnostics[0]["independent_stoppage"] = stoppage_diagnostics
    return readings, continuity_diagnostics


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
                "observed_clock_seconds": reading.observed_clock_seconds,
                "clock_seconds": reading.clock_seconds,
                "status": reading.continuity_status,
                "reason": reading.continuity_reason,
                "clock_texts": list(reading.clock_texts),
                "score_texts": list(reading.score_texts),
                "scoreboard_visible": reading.scoreboard_visible,
            }
        )
    return readings, continuity_diagnostics


def _auto_readings(
    recognized: Sequence[tuple[list[str], list[float]]],
    *,
    kinds: Sequence[str],
    sample_interval: float,
    period: int | str | None,
    clock_only: bool = False,
    independent_stoppage_frames: Sequence[
        Mapping[str, tuple[list[str], list[float]]]
    ] = (),
    independent_stoppage_base_minute: int | None = None,
) -> tuple[list[FrameReading], list[dict[str, Any]]]:
    """Parse auto-discovered clock and score crops as independent streams."""
    if len(recognized) != len(kinds):
        raise ValueError("recognized results and crop kinds must have equal lengths")
    frames = _auto_results_by_frame(recognized, kinds)
    main_results = [frame.get("clock", ([], [])) for frame in frames]
    stoppage_overrides, stoppage_diagnostics = (
        _confirmed_independent_stoppage_overrides(
            main_results,
            independent_stoppage_frames,
            base_minute=independent_stoppage_base_minute,
            sample_interval=sample_interval,
        )
    )
    tracker = ClockContinuityStateMachine(second_half_clock_mode="auto")
    readings: list[FrameReading] = []
    diagnostics: list[dict[str, Any]] = []
    for frame_index, frame in enumerate(frames):
        clock_texts, clock_confidences = frame.get("clock", ([], []))
        raw_clock_texts = list(clock_texts)
        score_texts, score_confidences = frame.get("score", ([], []))
        if frame_index in stoppage_overrides:
            tracker.update(
                frame_index * sample_interval,
                raw_clock_texts,
                scoreboard_visible=(True if clock_only else bool(raw_clock_texts)),
                period=period,
            )
            override_texts, override_confidences = stoppage_overrides[frame_index]
            reading = _independent_stoppage_reading(
                frame_index,
                frame_index * sample_interval,
                override_texts,
                score_texts,
                clock_confidences=override_confidences,
                score_confidences=score_confidences,
            )
        else:
            reading = split_frame_reading(
                frame_index,
                frame_index * sample_interval,
                clock_texts,
                score_texts,
                clock_confidences=clock_confidences,
                score_confidences=score_confidences,
                tracker=tracker,
                period=period,
                clock_visible=(True if clock_only else bool(clock_texts)),
                scoreboard_visible=bool(clock_texts or score_texts),
            )
        readings.append(reading)
        diagnostics.append(
            {
                "frame_index": reading.frame_index,
                "video_seconds": reading.frame_seconds,
                "observed_clock_seconds": reading.observed_clock_seconds,
                "clock_seconds": reading.clock_seconds,
                "status": reading.continuity_status,
                "reason": reading.continuity_reason,
                "clock_texts": list(reading.clock_texts),
                "raw_clock_texts": raw_clock_texts,
                "independent_stoppage_applied": frame_index in stoppage_overrides,
                "score_texts": list(reading.score_texts),
                "scoreboard_visible": reading.scoreboard_visible,
            }
        )
    if diagnostics:
        diagnostics[0]["independent_stoppage"] = stoppage_diagnostics
    return readings, diagnostics


def _recognize_request_paths(
    frames: Sequence[Path],
    crop_kinds: Sequence[str],
    *,
    engine: Any | None,
    batch_worker: BatchOcrWorker | None,
    request: Mapping[str, Any],
    profile_id: str,
    sample_interval: float,
    minimum_confidence: float,
    inference_batch_size: int,
    deadline_monotonic: float | None,
    source_frame_indices: Sequence[int] | None = None,
) -> tuple[list[tuple[list[str], list[float]]], list[str]]:
    if batch_worker is not None:
        return _recognize_paths_shared(
            batch_worker,
            frames,
            kinds=crop_kinds,
            match_id=str(request.get("match_id") or Path(str(request["candidate_path"])).parent.name),
            profile_id=profile_id,
            candidate_start_seconds=float(request.get("candidate_start_seconds", 0.0)),
            sample_interval=sample_interval,
            minimum_confidence=minimum_confidence,
            deadline_monotonic=(
                deadline_monotonic
                if deadline_monotonic is not None
                else time.monotonic() + 3600.0
            ),
            source_frame_indices=source_frame_indices,
        )
    assert engine is not None
    return _recognize_paths(
        engine,
        frames,
        minimum_confidence=minimum_confidence,
        batch_size=inference_batch_size,
    )


def _canonicalize_clock_text(text: str) -> str:
    translated = str(text).upper().translate(_CLOCK_CHARACTER_TRANSLATION)
    filtered = "".join(
        character if character in _ALLOWED_CLOCK_CHARACTERS else " "
        for character in translated
    )
    return " ".join(filtered.split())


def _normalize_clock_recognition_results(
    recognized: Sequence[tuple[list[str], list[float]]],
    kinds: Sequence[str],
    *,
    source_frame_indices: Sequence[int] | None = None,
    source: str = "raw",
) -> tuple[list[tuple[list[str], list[float]]], list[dict[str, Any]]]:
    """Repair clock-like OCR characters only when the raw text is unusable."""
    if len(recognized) != len(kinds):
        raise ValueError("recognized results and crop kinds must have equal lengths")
    frame_indices = (
        [int(value) for value in source_frame_indices]
        if source_frame_indices is not None
        else _frame_indices_for_kinds(kinds)
    )
    if len(frame_indices) != len(recognized):
        raise ValueError("source frame indices must align with OCR results")
    normalized_results = [
        (list(texts), list(confidences)) for texts, confidences in recognized
    ]
    repairs: list[dict[str, Any]] = []
    for crop_index, ((texts, confidences), kind) in enumerate(
        zip(normalized_results, kinds)
    ):
        if kind != "clock":
            continue
        raw_clock = parse_clock_texts(texts)
        if raw_clock.clock_seconds is not None and not raw_clock.ambiguous:
            continue
        normalized_texts = [
            normalized
            for text in texts
            if (normalized := _canonicalize_clock_text(text))
        ]
        if not normalized_texts or normalized_texts == texts:
            continue
        normalized_clock = parse_clock_texts(normalized_texts)
        if normalized_clock.clock_seconds is None or normalized_clock.ambiguous:
            continue
        normalized_results[crop_index] = (normalized_texts, confidences)
        repairs.append(
            {
                "frame_index": frame_indices[crop_index],
                "crop_index": crop_index,
                "source": source,
                "raw_texts": list(texts),
                "normalized_texts": normalized_texts,
                "clock_seconds": normalized_clock.clock_seconds,
            }
        )
    return normalized_results, repairs


def _write_clock_preprocess_variant(
    source_path: Path,
    output_path: Path,
    variant: str,
) -> None:
    """Write one enhanced clock crop without importing OpenCV at module load."""
    try:
        import cv2
    except ImportError as exc:
        raise WorkerError(
            "ocr_model_unavailable",
            "OpenCV is unavailable for clock preprocessing",
            diagnostics={"stage": "clock_preprocessing"},
        ) from exc
    image = cv2.imread(str(source_path), cv2.IMREAD_GRAYSCALE)
    if image is None:
        raise WorkerError(
            "ocr_frame_extraction_failed",
            f"cannot read clock crop for preprocessing: {source_path.name}",
            diagnostics={"stage": "clock_preprocessing"},
        )
    if variant == "gray_contrast_sharp":
        enhanced = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(4, 4)).apply(image)
        blurred = cv2.GaussianBlur(enhanced, (0, 0), 1.0)
        processed = cv2.addWeighted(enhanced, 1.6, blurred, -0.6, 0)
    elif variant == "binary_contrast":
        enhanced = cv2.equalizeHist(image)
        _threshold, processed = cv2.threshold(
            enhanced,
            0,
            255,
            cv2.THRESH_BINARY + cv2.THRESH_OTSU,
        )
    else:
        raise ValueError(f"unsupported clock preprocessing variant: {variant}")
    if not cv2.imwrite(str(output_path), processed):
        raise WorkerError(
            "ocr_frame_extraction_failed",
            f"cannot write preprocessed clock crop: {output_path.name}",
            diagnostics={"stage": "clock_preprocessing"},
        )


def _prepare_clock_only_recognition(
    recognized: Sequence[tuple[list[str], list[float]]],
    frames: Sequence[Path],
    crop_kinds: Sequence[str],
    *,
    engine: Any | None,
    batch_worker: BatchOcrWorker | None,
    request: Mapping[str, Any],
    profile_id: str,
    sample_interval: float,
    minimum_confidence: float,
    inference_batch_size: int,
    deadline_monotonic: float | None,
) -> tuple[list[tuple[list[str], list[float]]], dict[str, Any]]:
    """Normalize raw OCR, then retry only unreadable clock frames in place."""
    frame_indices = _frame_indices_for_kinds(crop_kinds)
    prepared, character_repairs = _normalize_clock_recognition_results(
        recognized,
        crop_kinds,
        source_frame_indices=frame_indices,
    )
    remaining = {
        index
        for index, ((texts, _confidences), kind) in enumerate(
            zip(prepared, crop_kinds)
        )
        if kind == "clock"
        and (
            parse_clock_texts(texts).clock_seconds is None
            or parse_clock_texts(texts).ambiguous
        )
    }
    initial_unreadable_count = len(remaining)
    attempts: list[dict[str, Any]] = []
    selected_variants: list[dict[str, Any]] = []
    for variant in CLOCK_PREPROCESS_VARIANTS:
        if not remaining:
            break
        retry_paths: list[Path] = []
        retry_crop_indices: list[int] = []
        retry_frame_indices: list[int] = []
        for crop_index in sorted(remaining):
            source_path = Path(frames[crop_index])
            variant_path = source_path.with_name(
                f"{source_path.stem}_{variant}{source_path.suffix}"
            )
            try:
                _write_clock_preprocess_variant(source_path, variant_path, variant)
            except (OSError, ValueError, WorkerError) as exc:
                attempts.append(
                    {
                        "variant": variant,
                        "frame_index": frame_indices[crop_index],
                        "status": "preprocess_failed",
                        "error": str(exc),
                    }
                )
                continue
            retry_paths.append(variant_path)
            retry_crop_indices.append(crop_index)
            retry_frame_indices.append(frame_indices[crop_index])
        if not retry_paths:
            continue
        try:
            retry_results, retry_failures = _recognize_request_paths(
                retry_paths,
                ["clock"] * len(retry_paths),
                engine=engine,
                batch_worker=batch_worker,
                request=request,
                profile_id=f"{profile_id}:{variant}",
                sample_interval=sample_interval,
                minimum_confidence=minimum_confidence,
                inference_batch_size=inference_batch_size,
                deadline_monotonic=deadline_monotonic,
                source_frame_indices=retry_frame_indices,
            )
        except WorkerError as exc:
            attempts.append(
                {
                    "variant": variant,
                    "status": "ocr_failed",
                    "error_kind": exc.kind,
                    "error": exc.message,
                }
            )
            break
        retry_results, retry_repairs = _normalize_clock_recognition_results(
            retry_results,
            ["clock"] * len(retry_results),
            source_frame_indices=retry_frame_indices,
            source=variant,
        )
        character_repairs.extend(retry_repairs)
        attempts.append(
            {
                "variant": variant,
                "attempted_frame_count": len(retry_paths),
                "ocr_failed_frame_count": len(retry_failures),
            }
        )
        for retry_index, crop_index in enumerate(retry_crop_indices):
            texts, confidences = retry_results[retry_index]
            parsed = parse_clock_texts(texts)
            if parsed.clock_seconds is None or parsed.ambiguous:
                continue
            prepared[crop_index] = (list(texts), list(confidences))
            remaining.discard(crop_index)
            selected_variants.append(
                {
                    "frame_index": frame_indices[crop_index],
                    "variant": variant,
                    "clock_seconds": parsed.clock_seconds,
                }
            )
    return prepared, {
        "strategy": "raw_then_failed_frames_only",
        "initial_unreadable_frame_count": initial_unreadable_count,
        "recovered_frame_count": initial_unreadable_count - len(remaining),
        "remaining_unreadable_frame_count": len(remaining),
        "variant_attempts": attempts,
        "selected_variants": selected_variants,
        "character_normalizations": character_repairs,
        "character_normalization_count": len(character_repairs),
        "frame_identity_preserved": True,
    }


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
    clock_only = request.get("clock_only", False)
    candidate_input_format = request.get("candidate_input_format")
    candidate_seek_seconds = float(request.get("candidate_seek_seconds", 0.0))
    candidate_duration_seconds = request.get("candidate_duration_seconds")
    if candidate_duration_seconds is not None:
        candidate_duration_seconds = float(candidate_duration_seconds)
    maximum_frames = int(
        request.get(
            "maximum_frames",
            max(300, math.ceil(AUTO_MAXIMUM_ANALYSIS_SECONDS / sample_interval) + 2),
        )
    )
    if sample_interval <= 0 or (
        profile_value is None
        and (not 0 < roi_width <= 1 or not 0 < roi_height <= 1)
    ):
        raise WorkerError("ocr_invalid_request", "invalid frame-sampling parameters")
    if not 0 <= minimum_confidence <= 1:
        raise WorkerError("ocr_invalid_request", "invalid OCR confidence threshold")
    if not 1 <= inference_batch_size <= 64:
        raise WorkerError("ocr_invalid_request", "invalid OCR inference batch size")
    if maximum_frames < 1:
        raise WorkerError("ocr_invalid_request", "maximum_frames must be positive")
    if not isinstance(clock_only, bool):
        raise WorkerError("ocr_invalid_request", "clock_only must be a boolean")
    if candidate_input_format not in {None, "ffconcat"}:
        raise WorkerError("ocr_invalid_request", "candidate_input_format must be ffconcat or None")
    if candidate_seek_seconds < 0 or (
        candidate_duration_seconds is not None and candidate_duration_seconds <= 0
    ):
        raise WorkerError("ocr_invalid_request", "invalid ffconcat input bounds")
    if candidate_input_format == "ffconcat":
        if not clock_only or profile_value is None:
            raise WorkerError(
                "ocr_invalid_request",
                "direct ffconcat OCR requires clock_only and a scoreboard profile",
            )
        _validate_ffconcat_manifest(candidate_path)
    raw_event_minute = request.get("event_minute")
    if clock_only and (
        raw_event_minute is None or not str(raw_event_minute).strip()
    ):
        raise WorkerError(
            "ocr_invalid_request",
            "event_minute is required when clock_only is enabled",
        )
    if request_timeout_seconds is not None and request_timeout_seconds <= 0:
        raise WorkerError("ocr_invalid_request", "request timeout must be positive")
    independent_stoppage_base = (
        _independent_stoppage_base_minute(request) if clock_only else None
    )
    deadline_monotonic = (
        time.monotonic() + request_timeout_seconds
        if request_timeout_seconds is not None else None
    )

    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="scoreboard_ocr_") as directory:
        ffmpeg = str(request.get("ffmpeg") or "ffmpeg")
        profile: ScoreboardProfile | None = None
        profile_diagnostics: dict[str, Any] | None = None
        auto_diagnostics: dict[str, Any] | None = None
        auto_tracker: AutoClockTracker | None = None
        clock_preprocessing_diagnostics: dict[str, Any] | None = None
        independent_stoppage_frames: list[
            dict[str, tuple[list[str], list[float]]]
        ] = []
        independent_stoppage_diagnostics: dict[str, Any] = {
            "enabled": independent_stoppage_base is not None,
            "base_minute": independent_stoppage_base,
            "status": (
                "pending" if independent_stoppage_base is not None else "not_requested"
            ),
        }
        if profile_value is not None:
            profile = _resolve_worker_profile(profile_value)
            if not clock_only and profile.score_roi is None:
                raise WorkerError(
                    "ocr_invalid_request",
                    "scoreboard profile must define score_roi outside clock_only mode",
                )
            if clock_only:
                frames, profile_diagnostics = extract_profile_clock_frames(
                    candidate_path,
                    Path(directory),
                    ffmpeg=ffmpeg,
                    sample_interval_seconds=sample_interval,
                    profile=profile,
                    maximum_frames=maximum_frames,
                    deadline_monotonic=deadline_monotonic,
                    input_format=candidate_input_format,
                    input_seek_seconds=candidate_seek_seconds,
                    input_duration_seconds=candidate_duration_seconds,
                )
                crop_kinds = ["clock"] * len(frames)
            else:
                frame_pairs, profile_diagnostics = extract_profile_frames(
                    candidate_path,
                    Path(directory),
                    ffmpeg=ffmpeg,
                    sample_interval_seconds=sample_interval,
                    profile=profile,
                    maximum_frames=maximum_frames,
                    deadline_monotonic=deadline_monotonic,
                )
                frames = [path for pair in frame_pairs for path in pair]
                crop_kinds = [
                    kind for _pair in frame_pairs for kind in ("clock", "score")
                ]
        else:
            auto_tracker, auto_diagnostics = discover_auto_clock(
                candidate_path,
                Path(directory),
                ffmpeg=ffmpeg,
                language=str(request.get("language") or "en"),
                sample_interval_seconds=sample_interval,
                minimum_confidence=minimum_confidence,
                maximum_frames=maximum_frames,
                deadline_monotonic=deadline_monotonic,
                clock_only=clock_only,
            )
            assert auto_tracker.clock_roi is not None
            if clock_only:
                auto_diagnostics["score_roi"] = None
                auto_diagnostics["score_rois"] = []
            frame_width, frame_height = auto_diagnostics["frame_resolution"]
            frames, crop_kinds, locked_diagnostics = extract_auto_roi_frames(
                candidate_path,
                Path(directory),
                ffmpeg=ffmpeg,
                sample_interval_seconds=sample_interval,
                frame_width=frame_width,
                frame_height=frame_height,
                clock_roi=auto_tracker.clock_roi,
                score_roi=(None if clock_only else auto_tracker.score_roi),
                score_rois=(() if clock_only else auto_tracker.score_rois),
                maximum_frames=maximum_frames,
                deadline_monotonic=deadline_monotonic,
                output_prefix="auto_initial",
            )
            auto_diagnostics["locked_crops"] = locked_diagnostics
            auto_diagnostics["clock_score_separate"] = not clock_only
            auto_diagnostics["clock_only"] = clock_only
            auto_diagnostics["score_ocr_skipped"] = clock_only
        extraction_seconds = time.perf_counter() - started
        if batch_worker is None and engine is None:
            engine = load_ocr_engine(str(request.get("language") or "en"))
        inference_started = time.perf_counter()
        profile_id = profile.profile_id if profile is not None else "auto_discovered"
        recognized, failed_frames = _recognize_request_paths(
            frames,
            crop_kinds,
            engine=engine,
            batch_worker=batch_worker,
            request=request,
            profile_id=profile_id,
            sample_interval=sample_interval,
            minimum_confidence=minimum_confidence,
            inference_batch_size=inference_batch_size,
            deadline_monotonic=deadline_monotonic,
        )
        if failed_frames and len(failed_frames) == len(frames):
            raise WorkerError(
                "ocr_model_unavailable",
                "PaddleOCR failed on every extracted scoreboard frame",
                diagnostics={"failed_frame_count": len(failed_frames)},
            )
        if clock_only:
            recognized, clock_preprocessing_diagnostics = (
                _prepare_clock_only_recognition(
                    recognized,
                    frames,
                    crop_kinds,
                    engine=engine,
                    batch_worker=batch_worker,
                    request=request,
                    profile_id=profile_id,
                    sample_interval=sample_interval,
                    minimum_confidence=minimum_confidence,
                    inference_batch_size=inference_batch_size,
                    deadline_monotonic=deadline_monotonic,
                )
            )
        request_period = _request_period(request)
        if profile is None:
            assert auto_diagnostics is not None
            assert auto_tracker is not None
            assert auto_tracker.clock_roi is not None
            recovery_records: list[dict[str, Any]] = []
            missing_start = _first_clock_missing_run(recognized, crop_kinds)
            if missing_start is not None:
                recovered_tracker, recovery_diagnostics = _recover_auto_clock(
                    candidate_path,
                    Path(directory),
                    ffmpeg=ffmpeg,
                    language=str(request.get("language") or "en"),
                    sample_interval_seconds=sample_interval,
                    minimum_confidence=minimum_confidence,
                    maximum_frames=maximum_frames,
                    start_frame_index=missing_start,
                    frame_width=auto_tracker.frame_width,
                    frame_height=auto_tracker.frame_height,
                    deadline_monotonic=deadline_monotonic,
                    clock_only=clock_only,
                )
                recovery_diagnostics["start_frame_index"] = missing_start
                recovery_records.append(recovery_diagnostics)
                if recovered_tracker is not None and recovered_tracker.clock_roi is not None:
                    if _bbox_similar(auto_tracker.clock_roi, recovered_tracker.clock_roi):
                        recovery_diagnostics["applied"] = False
                        recovery_diagnostics["skip_reason"] = "known_clock_roi"
                    else:
                        previous_by_frame = _auto_results_by_frame(recognized, crop_kinds)
                        recovered_frames, recovered_kinds, recovered_crops = extract_auto_roi_frames(
                            candidate_path,
                            Path(directory),
                            ffmpeg=ffmpeg,
                            sample_interval_seconds=sample_interval,
                            frame_width=recovered_tracker.frame_width,
                            frame_height=recovered_tracker.frame_height,
                            clock_roi=recovered_tracker.clock_roi,
                            score_roi=(
                                None if clock_only else recovered_tracker.score_roi
                            ),
                            score_rois=(
                                () if clock_only else recovered_tracker.score_rois
                            ),
                            maximum_frames=maximum_frames,
                            deadline_monotonic=deadline_monotonic,
                            output_prefix="auto_recovered",
                        )
                        recovered_recognized, failed_frames = _recognize_request_paths(
                            recovered_frames,
                            recovered_kinds,
                            engine=engine,
                            batch_worker=batch_worker,
                            request=request,
                            profile_id="auto_rediscovered",
                            sample_interval=sample_interval,
                            minimum_confidence=minimum_confidence,
                            inference_batch_size=inference_batch_size,
                            deadline_monotonic=deadline_monotonic,
                        )
                        if clock_only:
                            recovered_recognized, recovered_preprocessing = (
                                _prepare_clock_only_recognition(
                                    recovered_recognized,
                                    recovered_frames,
                                    recovered_kinds,
                                    engine=engine,
                                    batch_worker=batch_worker,
                                    request=request,
                                    profile_id="auto_rediscovered",
                                    sample_interval=sample_interval,
                                    minimum_confidence=minimum_confidence,
                                    inference_batch_size=inference_batch_size,
                                    deadline_monotonic=deadline_monotonic,
                                )
                            )
                            assert clock_preprocessing_diagnostics is not None
                            clock_preprocessing_diagnostics.setdefault(
                                "recovery_passes", []
                            ).append(recovered_preprocessing)
                        recovered_by_frame = _auto_results_by_frame(
                            recovered_recognized,
                            recovered_kinds,
                        )
                        combined_by_frame = _merge_auto_results(
                            previous_by_frame,
                            recovered_by_frame,
                        )
                        recognized, crop_kinds = _flatten_auto_results(combined_by_frame)
                        auto_tracker = recovered_tracker
                        recovery_diagnostics["applied"] = True
                        recovery_diagnostics["recovered_crops"] = recovered_crops
                        auto_diagnostics["status"] = "relocked"
                        auto_diagnostics["reason"] = "stable_clock_track"
                        auto_diagnostics["clock_roi"] = list(recovered_tracker.clock_roi)
                        auto_diagnostics["score_roi"] = (
                            None
                            if clock_only
                            else (
                                list(recovered_tracker.score_roi)
                                if recovered_tracker.score_roi is not None
                                else None
                            )
                        )
            auto_diagnostics["re_search"] = recovery_records
            auto_diagnostics["re_search_attempt_count"] = len(recovery_records)
            remaining_missing_start = _first_clock_missing_run(recognized, crop_kinds)
            auto_diagnostics["remaining_missing_start"] = remaining_missing_start
        if independent_stoppage_base is not None:
            try:
                if profile is not None:
                    assert profile_diagnostics is not None
                    frame_width, frame_height = profile_diagnostics["frame_resolution"]
                    stoppage_clock_roi = tuple(profile_diagnostics["clock_roi"])
                else:
                    assert auto_tracker is not None
                    assert auto_tracker.clock_roi is not None
                    frame_width, frame_height = (
                        auto_tracker.frame_width,
                        auto_tracker.frame_height,
                    )
                    stoppage_clock_roi = auto_tracker.clock_roi
                stoppage_paths, stoppage_labels, extraction_diagnostics = (
                    extract_independent_stoppage_frames(
                        candidate_path,
                        Path(directory),
                        ffmpeg=ffmpeg,
                        sample_interval_seconds=sample_interval,
                        frame_width=int(frame_width),
                        frame_height=int(frame_height),
                        clock_roi=stoppage_clock_roi,
                        maximum_frames=maximum_frames,
                        deadline_monotonic=deadline_monotonic,
                        input_format=candidate_input_format,
                        input_seek_seconds=candidate_seek_seconds,
                        input_duration_seconds=candidate_duration_seconds,
                    )
                )
                stoppage_recognized, stoppage_failed_frames = (
                    _recognize_request_paths(
                        stoppage_paths,
                        ["clock"] * len(stoppage_paths),
                        engine=engine,
                        batch_worker=batch_worker,
                        request=request,
                        profile_id=f"{profile_id}:independent_stoppage",
                        sample_interval=sample_interval,
                        minimum_confidence=minimum_confidence,
                        inference_batch_size=inference_batch_size,
                        deadline_monotonic=deadline_monotonic,
                    )
                )
                stoppage_recognized, stoppage_repairs = (
                    _normalize_clock_recognition_results(
                        stoppage_recognized,
                        ["clock"] * len(stoppage_recognized),
                        source="independent_stoppage",
                    )
                )
                independent_stoppage_frames = (
                    _independent_stoppage_results_by_frame(
                        stoppage_recognized,
                        stoppage_labels,
                    )
                )
                independent_stoppage_diagnostics.update({
                    "status": "analyzed",
                    "extraction": extraction_diagnostics,
                    "failed_frame_count": len(stoppage_failed_frames),
                    "character_repairs": stoppage_repairs,
                })
            except (OSError, ValueError, WorkerError) as exc:
                error = (
                    exc.as_dict()
                    if isinstance(exc, WorkerError)
                    else {
                        "kind": "independent_stoppage_analysis_failed",
                        "message": str(exc),
                        "diagnostics": {},
                    }
                )
                independent_stoppage_diagnostics.update({
                    "status": "failed",
                    "fallback": "primary_clock",
                    "error": error,
                })
        continuity_diagnostics: list[dict[str, Any]] | None = None
        profile_quality: dict[str, Any] | None = None
        if profile is not None:
            if clock_only:
                readings, continuity_diagnostics = _profile_clock_readings(
                    recognized,
                    profile=profile,
                    sample_interval=sample_interval,
                    period=request_period,
                    independent_stoppage_frames=independent_stoppage_frames,
                    independent_stoppage_base_minute=independent_stoppage_base,
                )
            else:
                readings, continuity_diagnostics = _profile_readings(
                    recognized,
                    profile=profile,
                    sample_interval=sample_interval,
                    period=request_period,
                )
        else:
            readings, continuity_diagnostics = _auto_readings(
                recognized,
                kinds=crop_kinds,
                sample_interval=sample_interval,
                period=request_period,
                clock_only=clock_only,
                independent_stoppage_frames=independent_stoppage_frames,
                independent_stoppage_base_minute=independent_stoppage_base,
            )
            assert auto_diagnostics is not None
            auto_diagnostics["temporarily_hidden_frame_count"] = sum(
                item["reason"] == "scoreboard_temporarily_missing"
                for item in continuity_diagnostics
            )
            auto_diagnostics["timeline_discontinuous_frame_count"] = sum(
                item["reason"] == "clock_discontinuity"
                for item in continuity_diagnostics
            )
        if independent_stoppage_base is not None and continuity_diagnostics:
            confirmed = continuity_diagnostics[0].get("independent_stoppage")
            if isinstance(confirmed, Mapping):
                independent_stoppage_diagnostics["confirmation"] = dict(confirmed)
                if confirmed.get("confirmed"):
                    independent_stoppage_diagnostics["status"] = "confirmed"
                elif independent_stoppage_diagnostics["status"] != "failed":
                    independent_stoppage_diagnostics["status"] = "not_confirmed"
        if profile is not None:
            try:
                profile_quality = _validate_profile_content_quality(
                    readings,
                    profile_id=profile.profile_id,
                )
            except WorkerError as exc:
                if independent_stoppage_base is None:
                    raise
                raise WorkerError(
                    exc.kind,
                    exc.message,
                    diagnostics={
                        **exc.diagnostics,
                        "independent_stoppage": independent_stoppage_diagnostics,
                    },
                ) from exc
        inference_seconds = time.perf_counter() - inference_started
        try:
            result = locate_from_readings(readings, request)
        except WorkerError as exc:
            extra_diagnostics: dict[str, Any] = {}
            if auto_diagnostics is not None:
                extra_diagnostics["auto_clock"] = auto_diagnostics
            if clock_preprocessing_diagnostics is not None:
                extra_diagnostics["clock_preprocessing"] = (
                    clock_preprocessing_diagnostics
                )
            if independent_stoppage_base is not None:
                extra_diagnostics["independent_stoppage"] = (
                    independent_stoppage_diagnostics
                )
            if not extra_diagnostics:
                raise
            raise WorkerError(
                exc.kind,
                exc.message,
                diagnostics={**exc.diagnostics, **extra_diagnostics},
            ) from exc
        diagnostics = result["diagnostics"]
        diagnostics.update(
            {
                "candidate_path": str(candidate_path.resolve()),
                "candidate_input_format": candidate_input_format or "file",
                "candidate_seek_seconds": candidate_seek_seconds,
                "candidate_duration_seconds": candidate_duration_seconds,
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
            diagnostics["clock_score_separate"] = not clock_only
            diagnostics["continuity"] = continuity_diagnostics
        else:
            diagnostics["auto_clock"] = auto_diagnostics
        diagnostics["clock_only"] = clock_only
        diagnostics["score_ocr_skipped"] = clock_only
        if clock_preprocessing_diagnostics is not None:
            diagnostics["clock_preprocessing"] = clock_preprocessing_diagnostics
        if independent_stoppage_base is not None:
            diagnostics["independent_stoppage"] = independent_stoppage_diagnostics
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
        self._generation = 1
        self._closed = False

    @property
    def generation(self) -> int:
        with self._lock:
            return self._generation

    def worker_for(self, language: str) -> BatchOcrWorker:
        normalized = str(language or "en").strip() or "en"
        with self._lock:
            if self._closed:
                raise WorkerError(
                    "ocr_worker_closed",
                    "persistent OCR backend is closed",
                )
            worker = self._workers.get(normalized)
            if worker is None:
                worker = BatchOcrWorker(
                    language=normalized,
                    max_batch_size=self._max_batch_size,
                    batch_wait_seconds=self._batch_wait_seconds,
                    queue_capacity=self._queue_capacity,
                    generation=self._generation,
                    engine_factory=self._engine_factory,
                    batch_recognizer=self._batch_recognizer,
                )
                self._workers[normalized] = worker
            return worker

    def invalidate_generation(self, generation: int) -> dict[str, Any]:
        """Atomically replace one unhealthy generation and keep serving."""
        with self._lock:
            if self._closed or int(generation) != self._generation:
                return {
                    "ocr_backend_restarted": self._generation > int(generation),
                    "backend_generation_before": int(generation),
                    "backend_generation_after": self._generation,
                }
            previous_generation = self._generation
            self._generation += 1
            workers = list(self._workers.values())
            self._workers.clear()
        for worker in workers:
            worker.invalidate_generation()
            worker.close(wait=False)
        return {
            "ocr_backend_restarted": True,
            "backend_generation_before": previous_generation,
            "backend_generation_after": previous_generation + 1,
        }

    def mark_unhealthy(self) -> None:
        """Backward-compatible generation invalidation hook."""
        self.invalidate_generation(self.generation)

    def close(self, *, timeout: float = 2.0) -> None:
        with self._lock:
            self._closed = True
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


def _restartable_backend_generation(
    document: Mapping[str, Any],
) -> int | None:
    """Return the failed generation only for shared-backend failures."""
    error = document.get("error")
    if not isinstance(error, Mapping):
        return None
    diagnostics = error.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        return None
    kind = str(error.get("kind") or "")
    stage = str(diagnostics.get("stage") or "")
    restartable = kind == "ocr_backend_generation_invalidated" or (
        kind == "inference_timeout" and stage == "batch_inference"
    )
    if not restartable or not diagnostics.get("backend_unhealthy"):
        return None
    try:
        generation = int(diagnostics.get("backend_generation"))
    except (TypeError, ValueError):
        return None
    return generation if generation >= 0 else None


def _record_backend_restart(
    document: dict[str, Any],
    restart: Mapping[str, Any],
    *,
    retry_count: int,
) -> None:
    container_key = "result" if document.get("ok") else "error"
    container = document.get(container_key)
    if not isinstance(container, dict):
        return
    diagnostics = container.get("diagnostics")
    if not isinstance(diagnostics, dict):
        diagnostics = {}
        container["diagnostics"] = diagnostics
    diagnostics.update({
        **restart,
        "ocr_backend_restart_retry_count": int(retry_count),
    })


def _serve_socket_connection(
    connection: socket.socket,
    *,
    runtime: _SocketOcrRuntime,
    stop_requested: threading.Event,
    request_executor: Callable[..., tuple[dict[str, Any], int]],
) -> None:
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
                                request_deadline = (
                                    time.monotonic() + timeout_seconds
                                )
                                retry_reserve_seconds = min(
                                    15.0,
                                    max(0.05, timeout_seconds * 0.1),
                                )
                                restart_diagnostics: dict[str, Any] | None = None
                                retry_count = 0
                                while True:
                                    remaining = request_deadline - time.monotonic()
                                    if remaining <= 0:
                                        raise WorkerError(
                                            "inference_timeout",
                                            "OCR request budget ended during backend restart",
                                            diagnostics={
                                                "stage": "batch_inference",
                                                **(restart_diagnostics or {}),
                                            },
                                        )
                                    batch_worker = runtime.worker_for(
                                        str(request.get("language") or "en")
                                    )
                                    attempt_budget = remaining
                                    if retry_count == 0:
                                        attempt_budget = max(
                                            0.05,
                                            remaining - retry_reserve_seconds,
                                        )
                                    document, _return_code = request_executor(
                                        dict(request),
                                        batch_worker=batch_worker,
                                        request_timeout_seconds=min(
                                            remaining, attempt_budget
                                        ),
                                    )
                                    failed_generation = (
                                        _restartable_backend_generation(document)
                                    )
                                    if failed_generation is None:
                                        if restart_diagnostics is not None:
                                            _record_backend_restart(
                                                document,
                                                restart_diagnostics,
                                                retry_count=retry_count,
                                            )
                                        break
                                    restart_diagnostics = (
                                        runtime.invalidate_generation(
                                            failed_generation
                                        )
                                    )
                                    remaining = request_deadline - time.monotonic()
                                    if retry_count >= 1 or remaining <= 0.05:
                                        _record_backend_restart(
                                            document,
                                            restart_diagnostics,
                                            retry_count=retry_count,
                                        )
                                        break
                                    retry_count += 1
                            except (TypeError, ValueError, WorkerError) as exc:
                                error = (
                                    exc if isinstance(exc, WorkerError)
                                    else WorkerError("ocr_invalid_request", str(exc))
                                )
                                document = {"ok": False, "error": error.as_dict()}
                _write_socket_document(
                    connection if hasattr(connection, "sendall") else stream,
                    document,
                )
    except (OSError, ValueError, socket.timeout):
        # Client disconnects and stream teardown errors do not indicate a
        # damaged OCR engine. Drop the response and keep serving other jobs.
        return
    except Exception:
        # A handler failure is request-local. The backend is replaced only by
        # the generation-aware path above, never by generic protocol errors.
        return


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
