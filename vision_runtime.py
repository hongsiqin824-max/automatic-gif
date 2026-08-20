"""Shared contracts for optional visual refinement of event GIFs."""

from __future__ import annotations

import json
import math
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from artifact_naming import build_gif_filename
from live_goal_pipeline import (
    BufferNotReady,
    BufferUnavailable,
    CoverageStatus,
    PendingEvent,
    Segment,
    VideoCoverage,
    analyze_video_coverage,
    encode_gif,
)
from scoreboard_ocr import ScoreboardOcrError, locate_scoreboard_event
from vision_locator import (
    VisionBufferNotReady,
    VisionBufferUnavailable,
    VisionCandidateNotFound,
    VisionConfigurationError,
    VisionInferenceError,
    locate_candidate_video,
)


EVENT_LABELS = {
    "G": ("Goal",),
    "OG": ("Goal",),
    "PG": ("Goal",),
    "YC": ("Yellow card",),
    "RC": ("Red card", "Yellow->red card"),
}
GOAL_LIKE_EVENT_CODES = frozenset({"G", "OG", "PG"})


MIN_DEGRADED_CLIP_SECONDS = 2.0
OCR_EXACT_WINDOW_BEFORE_SECONDS = 30.0
OCR_EXACT_WINDOW_AFTER_SECONDS = 30.0
OCR_MINUTE_WINDOW_BEFORE_SECONDS = 60.0
OCR_MINUTE_WINDOW_AFTER_SECONDS = 0.0
OCR_PROGRESSIVE_INITIAL_LOOKBACK_SECONDS = 120.0
OCR_PROGRESSIVE_OVERLAP_SECONDS = 5.0
OCR_PROGRESSIVE_INCREMENT_SECONDS = 30.0
OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS = 0.25
OCR_TARGET_WAIT_INITIAL_SECONDS = 60.0
OCR_TARGET_WAIT_MARGIN_SECONDS = 20.0
OCR_EVENT_HARD_LIMIT_SECONDS = 180.0
OCR_POSTROLL_HARD_LIMIT_SECONDS = 60.0
OCR_FAR_TARGET_RETRY_MIN_SECONDS = 10.0
OCR_FAR_TARGET_RETRY_MAX_SECONDS = 30.0
OCR_PROGRESSIVE_SCAN_MISS_KINDS = frozenset({
    "buffer_gap",
    "ocr_clock_unreadable",
    "ocr_exact_second_not_found",
    "ocr_minute_boundary_not_found",
    "ocr_no_target",
    "ocr_target_localization_failed",
    "scoreboard_missing",
})
# A minute OCR reading identifies an interval ending at the requested minute.
# Use the same preceding-minute interval for the user-facing OCR artifact and
# the narrower T-DEED candidate search.
OCR_TDEED_MINUTE_BEFORE_SECONDS = 60.0
OCR_TDEED_MINUTE_AFTER_SECONDS = 0.0
# A minute-only location is intentionally emitted as a one-minute clip around
# the OCR-derived anchor.  It is a degraded location, but remains useful when
# T-DEED cannot select an action candidate.
OCR_MINUTE_FALLBACK_BEFORE_SECONDS = 30.0
OCR_MINUTE_FALLBACK_AFTER_SECONDS = 30.0
OCR_MINUTE_FALLBACK_WIDTH = 384
OCR_MINUTE_FALLBACK_FPS = 6.0
OCR_MINUTE_FALLBACK_COLORS = 160
OCR_MINUTE_FALLBACK_COMPLETE_RATIO = 0.9
OCR_PYTHON = Path(__file__).resolve().parent / "tmp" / "ocr_venv" / "bin" / "python"


class VisualLocationFailed(RuntimeError):
    """Both the OCR primary locator and T-DEED fallback failed."""

    def __init__(self, kind: str, message: str, diagnostics: dict[str, Any]) -> None:
        super().__init__(message)
        self.kind = kind
        self.diagnostics = diagnostics


def _event_minute(job: "VisionJob") -> str:
    minute = str(job.event_minute or "").strip()
    extra = str(job.event_minute_extra or "0").strip()
    if minute and extra not in {"", "0"} and "+" not in minute:
        return f"{minute}+{extra}"
    return minute


def _v1_clock_scope_error(job: "VisionJob") -> tuple[str, str] | None:
    """Reject inputs outside regular-time minute localization in V1."""
    minute = _event_minute(job).rstrip("'").strip()
    if not minute:
        # Keep recovered legacy tasks runnable. Current API-created tasks always
        # persist a minute and are therefore still scope-checked below.
        return None
    base_text, _, extra_text = minute.partition("+")
    try:
        base = float(base_text)
        extra = float(extra_text) if extra_text else 0.0
    except ValueError:
        return "event_minute_invalid", f"API event minute is invalid: {minute!r}"
    if (
        not math.isfinite(base)
        or not math.isfinite(extra)
        or base < 0
        or extra < 0
        or not base.is_integer()
        or not extra.is_integer()
    ):
        return "event_minute_invalid", f"API event minute is invalid: {minute!r}"
    if base > 90:
        return (
            "unsupported_extra_time_or_penalties_v1",
            "extra time and penalty shootouts are outside the V1 clock locator scope",
        )
    return None


def _profile_configuration_error(job: "VisionJob") -> tuple[str, str] | None:
    if job.require_scoreboard_profile and job.scoreboard_profile is None:
        return (
            "clock_profile_mismatch",
            "no scoreboard profile is configured for this broadcast layout",
        )
    return None


def _tdeed_error_kind(error: BaseException) -> str:
    if isinstance(error, VisionConfigurationError):
        return "tdeed_model_unavailable"
    if isinstance(error, VisionCandidateNotFound):
        return "tdeed_no_candidate"
    if isinstance(error, VisionInferenceError) and "exceeded" in str(error).lower():
        return "inference_timeout"
    return "tdeed_inference_failed"


def _normalized_buffer_error_kind(kind: str | None) -> str:
    if kind == "history_unavailable":
        return "buffer_history_missing"
    if kind in {"internal_video_gap", "anchor_gap"}:
        return "buffer_gap"
    return kind or "video_unavailable"


def _vision_failure_result(
    error_kind: str,
    message: str,
    *,
    failure_stage: str,
    **diagnostics: Any,
) -> dict[str, Any]:
    """Return the stable terminal contract for every visual failure path."""
    return {
        "stage": failure_stage,
        "error_kind": error_kind,
        "fallback_used": False,
        "minute_fallback": False,
        "precise_location": False,
        "default_gif_preserved": True,
        "fallback_generated": False,
        "output_kind": "failed",
        "failure_reason": {
            "kind": error_kind,
            "stage": failure_stage,
            "message": message,
        },
        **diagnostics,
    }


def _ocr_interval_bounds(
    ocr_result: dict[str, Any] | None,
    *,
    window_start: float,
    window_end: float,
) -> tuple[float, float] | None:
    if not ocr_result or not ocr_result.get("requires_tdeed"):
        return None
    try:
        interval_start = max(
            window_start,
            float(ocr_result["candidate_interval_start_seconds"]),
        )
        interval_end = min(
            window_end,
            float(ocr_result["candidate_interval_end_seconds"]),
        )
    except (KeyError, TypeError, ValueError):
        return None
    if (
        not math.isfinite(interval_start)
        or not math.isfinite(interval_end)
        or interval_end <= interval_start
    ):
        return None
    return interval_start, interval_end


def _ocr_anchor_or_interval(
    located: dict[str, Any],
    *,
    window_start: float,
    window_end: float,
) -> tuple[float | None, tuple[float, float] | None]:
    """Normalize OCR output without treating a valid interval as ``None``.

    The worker intentionally returns ``anchor_seconds=None`` for minute-only
    readings because no exact event second was established.  Callers still
    need a stable stream-time hint for the fallback clip and T-DEED window.
    """
    raw_anchor = located.get("anchor_seconds")
    try:
        anchor = float(raw_anchor)
    except (TypeError, ValueError):
        anchor = math.nan
    if math.isfinite(anchor) and window_start <= anchor <= window_end:
        return anchor, None
    interval = _ocr_interval_bounds(
        located,
        window_start=window_start,
        window_end=window_end,
    )
    if interval is not None:
        return (interval[0] + interval[1]) / 2.0, interval
    return None, None


def _ocr_minute_range_fallback(
    *,
    window_start: float,
    window_end: float,
    ocr_anchor: float,
    ocr_location_kind: str,
    locator_method: str,
    label: str,
    failure_kind: str,
    failure_stage: str,
    failure_message: str,
    ocr_result: dict[str, Any] | None = None,
    ocr_error: dict[str, Any] | None = None,
    tdeed_error_kind: str | None = None,
    tdeed_error: str | None = None,
    ocr_clock_only: bool = False,
) -> dict[str, Any]:
    """Create a 60-second fallback around a location found by OCR."""
    anchor = min(window_end, max(window_start, float(ocr_anchor)))
    upstream_degradation_reason = (ocr_result or {}).get("degradation_reason")
    if upstream_degradation_reason is None:
        upstream_degradation_reason = (ocr_result or {}).get(
            "exact_second_error"
        )
    degradation_reason = upstream_degradation_reason or {
        "kind": failure_kind,
        "stage": failure_stage,
        "message": failure_message,
    }
    return {
        "anchor_stream_time": anchor,
        "location_kind": (
            "match_clock_minute_interval"
            if ocr_location_kind == "clock_interval"
            else "score_transition"
        ),
        "confidence": None,
        "label": label,
        "model_name": "PaddleOCR",
        "model_version": "scoreboard-clock-v1",
        "checkpoint_sha256": None,
        "locator_wall_seconds": (
            ((ocr_result or {}).get("diagnostics") or {}).get(
                "worker_wall_seconds"
            )
        ),
        "locator_method": locator_method,
        "ocr": ocr_result,
        "ocr_error": ocr_error,
        "ocr_clock_only": ocr_clock_only,
        "fallback_used": True,
        "minute_fallback": True,
        "tdeed_error_kind": tdeed_error_kind,
        "tdeed_error": tdeed_error,
        "clip_before_seconds": OCR_MINUTE_FALLBACK_BEFORE_SECONDS,
        "clip_after_seconds": OCR_MINUTE_FALLBACK_AFTER_SECONDS,
        "output_kind": "minute_range_fallback",
        "precise_location": False,
        "localization_quality": "degraded",
        "degraded": True,
        "degradation_mode": "minute_range_fallback",
        "degradation_reason": degradation_reason,
        "error_kind": failure_kind,
        "failure_reason": {
            "kind": failure_kind,
            "stage": failure_stage,
            "message": failure_message,
            "ocr_anchor_found": True,
            "ocr_location_kind": ocr_location_kind,
            "ocr_minute_interval_found": ocr_location_kind == "clock_interval",
            "default_gif_preserved": True,
        },
    }


def locate_with_ocr_fallback(
    job: "VisionJob",
    analysis_path: Path,
    materialized: dict[str, Any],
    *,
    tdeed_python: Path,
    ocr_python: Path = OCR_PYTHON,
    ocr_timeout_seconds: float = 180.0,
    tdeed_timeout_seconds: float = 90.0,
) -> dict[str, Any]:
    """Locate by scoreboard first, then use T-DEED only when needed."""
    window_start = float(materialized["window_start_stream_time"])
    window_end = float(materialized["window_end_stream_time"])
    scope_error = _v1_clock_scope_error(job)
    if scope_error is not None:
        kind, message = scope_error
        raise VisualLocationFailed(
            kind,
            message,
            {
                "stage": "v1_scope",
                "event_minute": _event_minute(job) or None,
                "default_gif_preserved": True,
                "fallback_generated": False,
            },
        )
    ocr_result: dict[str, Any] | None = None
    ocr_error: dict[str, Any] | None = None
    profile_error = _profile_configuration_error(job)
    if profile_error is not None:
        kind, message = profile_error
        ocr_error = {
            "kind": kind,
            "message": message,
            "diagnostics": {},
        }
    else:
        try:
            if not Path(ocr_python).is_file():
                raise ScoreboardOcrError(
                    "ocr_model_unavailable",
                    f"isolated PaddleOCR Python is missing: {ocr_python}",
                )
            ocr_result = locate_scoreboard_event(
                analysis_path,
                event_code=job.code,
                target_score=job.target_score or None,
                event_minute=_event_minute(job) or None,
                event_second=(
                    job.event_second
                    if job.code.upper().strip() in GOAL_LIKE_EVENT_CODES
                    else None
                ),
                candidate_start_seconds=window_start,
                timeout_seconds=ocr_timeout_seconds,
                python_executable=ocr_python,
                scoreboard_profile=job.scoreboard_profile,
                clock_only=job.clock_only,
            )
        except ScoreboardOcrError as exc:
            ocr_error = exc.as_dict()
        except (OSError, TypeError, ValueError) as exc:
            ocr_error = {
                "kind": "ocr_invalid_request",
                "message": str(exc),
                "diagnostics": {},
            }

    if (
        ocr_result is not None
        and ocr_result.get("location_kind") == "match_clock_second"
    ):
        try:
            exact_anchor = float(ocr_result["anchor_seconds"])
        except (KeyError, TypeError, ValueError):
            exact_anchor = math.nan
        if math.isfinite(exact_anchor) and window_start <= exact_anchor <= window_end:
            return {
                "anchor_stream_time": exact_anchor,
                "confidence": None,
                "label": "scoreboard match clock second",
                "model_name": "PaddleOCR",
                "model_version": "scoreboard-clock-v2",
                "checkpoint_sha256": None,
                "locator_wall_seconds": (
                    (ocr_result.get("diagnostics") or {}).get("worker_wall_seconds")
                ),
                "locator_method": ocr_result.get("method") or "paddleocr_exact_clock",
                "location_kind": ocr_result.get("location_kind"),
                "precision": ocr_result.get("precision") or "observed_second",
                "localization_quality": (
                    ocr_result.get("localization_quality") or "exact"
                ),
                "degraded": bool(ocr_result.get("degraded")),
                "degradation_mode": ocr_result.get("degradation_mode"),
                "degradation_reason": ocr_result.get("degradation_reason"),
                "target_clock": ocr_result.get("target_clock"),
                "ocr": ocr_result,
                "ocr_error": None,
                "ocr_clock_only": job.clock_only,
                "fallback_used": False,
                "minute_fallback": False,
                "output_kind": "precise_refined",
                "precise_location": True,
            }
        ocr_error = {
            "kind": "ocr_invalid_result",
            "message": "exact-second OCR returned an anchor outside the search window",
            "diagnostics": {
                "anchor_seconds": ocr_result.get("anchor_seconds"),
                "window_start_stream_time": window_start,
                "window_end_stream_time": window_end,
            },
        }
        ocr_result = None

    expected_anchor = min(
        window_end,
        max(window_start, job.api_observed_stream_time),
    )
    maximum_distance = max(
        15.0,
        expected_anchor - window_start,
        window_end - expected_anchor,
    )
    method = "tdeed_fallback"
    score_transition_anchor: float | None = None
    if ocr_result is not None and not ocr_result.get("requires_tdeed"):
        try:
            score_transition_anchor = float(ocr_result["anchor_seconds"])
        except (KeyError, TypeError, ValueError):
            score_transition_anchor = None
        if score_transition_anchor is not None and math.isfinite(
            score_transition_anchor
        ):
            expected_anchor = min(
                window_end,
                max(window_start, score_transition_anchor),
            )
            # A broadcaster may update the score several seconds after the
            # actual goal. Use the transition as a search hint, never as the
            # final event timestamp.
            maximum_distance = min(30.0, max(1.0, window_end - window_start))
            method = "paddleocr_score_then_tdeed"
        else:
            score_transition_anchor = None
    ocr_interval = _ocr_interval_bounds(
        ocr_result,
        window_start=window_start,
        window_end=window_end,
    )
    if ocr_result is not None and ocr_interval is None:
        raw_anchor = ocr_result.get("anchor_seconds")
        try:
            parsed_anchor = float(raw_anchor)
        except (TypeError, ValueError):
            parsed_anchor = math.nan
        if not math.isfinite(parsed_anchor):
            ocr_error = {
                "kind": "ocr_no_target",
                "message": (
                    "OCR returned neither an exact anchor nor a valid candidate "
                    "interval"
                ),
                "diagnostics": {
                    **dict(ocr_result.get("diagnostics") or {}),
                    "anchor_seconds": raw_anchor,
                    "candidate_interval_start_seconds": ocr_result.get(
                        "candidate_interval_start_seconds"
                    ),
                    "candidate_interval_end_seconds": ocr_result.get(
                        "candidate_interval_end_seconds"
                    ),
                },
            }
            ocr_result = None
    if score_transition_anchor is None and ocr_interval is not None:
        interval_start, interval_end = ocr_interval
        expected_anchor = (interval_start + interval_end) / 2.0
        maximum_distance = max(1.0, (interval_end - interval_start) / 2.0)
        method = "paddleocr_clock_then_tdeed"

    started = time.perf_counter()
    try:
        located = locate_candidate_video(
            analysis_path,
            job.code,
            expected_offset_seconds=expected_anchor - window_start,
            threshold=0.2,
            max_anchor_distance_seconds=maximum_distance,
            timeout_seconds=tdeed_timeout_seconds,
            python_path=tdeed_python,
            candidate_window_start_seconds=window_start,
            candidate_window_end_seconds=window_end,
        )
    except (
        VisionCandidateNotFound,
        VisionConfigurationError,
        VisionInferenceError,
    ) as exc:
        kind = _tdeed_error_kind(exc)
        if ocr_interval is not None:
            return _ocr_minute_range_fallback(
                window_start=window_start,
                window_end=window_end,
                ocr_anchor=expected_anchor,
                ocr_location_kind="clock_interval",
                locator_method="paddleocr_clock_interval_fallback",
                label="scoreboard clock minute fallback",
                failure_kind=kind,
                failure_stage="event_second_localization",
                failure_message=str(exc),
                ocr_result=ocr_result,
                ocr_error=ocr_error,
                tdeed_error_kind=kind,
                tdeed_error=str(exc),
                ocr_clock_only=job.clock_only,
            )
        if score_transition_anchor is not None:
            return _ocr_minute_range_fallback(
                window_start=window_start,
                window_end=window_end,
                ocr_anchor=score_transition_anchor,
                ocr_location_kind="score_transition",
                locator_method="paddleocr_score_transition_fallback",
                label="scoreboard score transition fallback",
                failure_kind=kind,
                failure_stage="event_second_localization",
                failure_message=(
                    "the scoreboard score transition was found, but precise "
                    f"goal localization failed: {exc}"
                ),
                ocr_result=ocr_result,
                ocr_error=ocr_error,
                tdeed_error_kind=kind,
                tdeed_error=str(exc),
                ocr_clock_only=job.clock_only,
            )
        ocr_failure = (
            f"OCR failed ({ocr_error.get('kind')}): "
            f"{ocr_error.get('message') or 'no usable location'}"
            if ocr_error is not None
            else "OCR returned no usable event location"
        )
        raise VisualLocationFailed(
            kind,
            f"{ocr_failure}; T-DEED failed ({kind}): {exc}",
            {
                "stage": "event_localization",
                "ocr": ocr_result,
                "ocr_error": ocr_error,
                "ocr_clock_only": job.clock_only,
                "tdeed_error_kind": kind,
                "tdeed_error": str(exc),
                "fallback_used": True,
                "minute_fallback": False,
                "ocr_anchor_found": False,
                "precise_location": False,
                "default_gif_preserved": True,
                "fallback_generated": False,
                "output_kind": "failed",
            },
        )

    located["locator_wall_seconds"] = round(time.perf_counter() - started, 3)
    located["model_name"] = "T-DEED"
    located["locator_method"] = method
    located["ocr"] = ocr_result
    located["ocr_error"] = ocr_error
    located["ocr_clock_only"] = job.clock_only
    located["fallback_used"] = True
    return located


def _vision_coverage(
    segments: list[Segment],
    *,
    window_start: float,
    window_end: float,
    anchor: float,
    deadline_reached: bool = False,
    min_degraded_seconds: float = MIN_DEGRADED_CLIP_SECONDS,
) -> VideoCoverage:
    """Analyze visual coverage, allowing an anchor-side fallback at deadline."""
    return analyze_video_coverage(
        segments,
        window_start=window_start,
        window_end=window_end,
        anchor=anchor,
        allow_degraded=True,
        force_degraded=deadline_reached,
        min_degraded_seconds=min_degraded_seconds,
    )


@dataclass(frozen=True)
class VisionSearchComponent:
    """One continuous piece of the rolling buffer that can be OCR-scanned."""

    segments: tuple[Segment, ...]
    start: float
    end: float

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)


def _continuous_search_components(
    segments: list[Segment],
    *,
    window_start: float,
    window_end: float,
    gap_tolerance: float = 0.5,
    minimum_seconds: float = MIN_DEGRADED_CLIP_SECONDS,
) -> tuple[list[VisionSearchComponent], float | None]:
    """Split a search window into independently materializable components."""
    available = sorted(
        (
            segment for segment in segments
            if Path(segment.path).is_file()
            and float(segment.end) > window_start
            and float(segment.start) < window_end
        ),
        key=lambda segment: (float(segment.start), str(segment.path)),
    )
    latest_end = max((float(segment.end) for segment in segments), default=None)
    if not available:
        return [], latest_end
    groups: list[list[Segment]] = [[available[0]]]
    group_ends: list[float] = [float(available[0].end)]
    for segment in available[1:]:
        segment_start = float(segment.start)
        if segment_start - group_ends[-1] > gap_tolerance:
            groups.append([segment])
            group_ends.append(float(segment.end))
        else:
            groups[-1].append(segment)
            group_ends[-1] = max(group_ends[-1], float(segment.end))
    components: list[VisionSearchComponent] = []
    for group in groups:
        start = max(window_start, float(group[0].start))
        end = min(window_end, max(float(segment.end) for segment in group))
        if end - start >= minimum_seconds:
            components.append(VisionSearchComponent(tuple(group), start, end))
    return components, latest_end


def _component_coverage(component: VisionSearchComponent) -> VideoCoverage:
    """Adapt one continuous component to the existing materializer contract."""
    anchor = (component.start + component.end) / 2.0
    return VideoCoverage(
        status=CoverageStatus.READY_FULL,
        requested_start=component.start,
        requested_end=component.end,
        anchor=anchor,
        effective_start=component.start,
        effective_end=component.end,
        segments=component.segments,
        gaps=(),
        reason="scanning one continuous rolling-buffer component",
    )


def _clock_manifest_for_component(
    component: VisionSearchComponent,
    output_path: Path,
) -> dict[str, Any] | None:
    """Create a bounded ffconcat manifest for profile clock-only OCR.

    Segment timestamps are stream-time metadata, while concat input starts at
    zero.  ``input_seek_seconds`` carries the first-segment offset explicitly;
    the worker then samples only ``component.duration`` seconds and reports
    anchors against ``component.start``.  This preserves the existing stream
    time mapping without materializing a full-resolution MP4.
    """
    selected = list(component.segments)
    if not selected or any(Path(segment.path).suffix.lower() != ".ts" for segment in selected):
        return None
    if any(not Path(segment.path).is_file() for segment in selected):
        return None
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output_path.open("w", encoding="utf-8") as handle:
            handle.write("ffconcat version 1.0\n")
            for segment in selected:
                path = str(Path(segment.path).resolve()).replace("'", "'\\''")
                handle.write(f"file '{path}'\n")
    except OSError:
        output_path.unlink(missing_ok=True)
        return None
    first_start = float(selected[0].start)
    return {
        "path": str(output_path.resolve()),
        "window_start_stream_time": component.start,
        "window_end_stream_time": component.end,
        "requested_window_start_stream_time": component.start,
        "requested_window_end_stream_time": component.end,
        "selected_segment_count": len(selected),
        "bytes": output_path.stat().st_size,
        "coverage_status": CoverageStatus.READY_FULL.value,
        "coverage_reason": "direct clock ROI extraction from leased TS component",
        "input_format": "ffconcat",
        "input_seek_seconds": max(0.0, component.start - first_start),
        "input_duration_seconds": component.duration,
        "direct_clock_roi": True,
    }


def _locate_across_search_components(
    job: "VisionJob",
    segments: list[Segment],
    *,
    window_start: float,
    window_end: float,
    analysis_path: Path,
    ffmpeg: str,
    tdeed_python: Path,
    ocr_python: Path,
    ocr_timeout_seconds: float,
    tdeed_timeout_seconds: float,
    minimum_component_seconds: float = 3.0,
    components: list[VisionSearchComponent] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    """Run OCR/T-DEED independently on every usable continuous component."""
    latest_end: float | None = None
    if components is None:
        components, latest_end = _continuous_search_components(
            segments,
            window_start=window_start,
            window_end=window_end,
            minimum_seconds=minimum_component_seconds,
        )
    # The caller handles a live tail that has not reached the search window.
    if not components:
        raise VisualLocationFailed(
            "buffer_gap",
            "no continuous video component is long enough for OCR",
            {
                "stage": "fragmented_search",
                "latest_media_end": latest_end,
                "window_start": window_start,
                "window_end": window_end,
                "fragment_attempts": [],
                "default_gif_preserved": True,
                "fallback_generated": False,
            },
        )

    # Search the component containing the API observation first, then nearby
    # fragments. This minimizes latency while still checking the entire window.
    api_anchor = job.api_observed_stream_time

    def component_distance(component: VisionSearchComponent) -> tuple[int, float, float]:
        if component.start <= api_anchor <= component.end:
            distance = 0.0
            contains = 0
        else:
            distance = min(abs(api_anchor - component.start), abs(api_anchor - component.end))
            contains = 1
        return contains, distance, -component.end

    components = sorted(
        components,
        key=component_distance,
    )
    attempts: list[dict[str, Any]] = []
    fallback: tuple[dict[str, Any], dict[str, Any], float] | None = None
    secondary: tuple[dict[str, Any], dict[str, Any]] | None = None
    exact_matches: list[tuple[dict[str, Any], dict[str, Any]]] = []
    scan_all_for_exact_second = (
        job.code.upper().strip() in GOAL_LIKE_EVENT_CODES
        and job.event_second is not None
    )
    for index, component in enumerate(components):
        component_path = analysis_path.with_name(
            f"{analysis_path.stem}.part{index:03d}{analysis_path.suffix}"
        )
        try:
            materialized = materialize_analysis_clip(
                ffmpeg,
                list(segments),
                component_path,
                window_start=component.start,
                window_end=component.end,
                anchor=(component.start + component.end) / 2.0,
                coverage=_component_coverage(component),
            )
        except Exception as exc:
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "duration_seconds": component.duration,
                "error_kind": "fragment_materialize_failed",
                "error": str(exc),
            })
            continue
        try:
            located = locate_with_ocr_fallback(
                job,
                component_path,
                materialized,
                tdeed_python=tdeed_python,
                ocr_python=ocr_python,
                ocr_timeout_seconds=ocr_timeout_seconds,
                tdeed_timeout_seconds=tdeed_timeout_seconds,
            )
        except VisualLocationFailed as exc:
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "duration_seconds": component.duration,
                "error_kind": exc.kind,
                "error": str(exc),
                "diagnostics": exc.diagnostics,
            })
            continue
        except Exception as exc:
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "duration_seconds": component.duration,
                "error_kind": "fragment_locator_failed",
                "error": str(exc),
            })
            continue
        located = dict(located)
        located["fragment_index"] = index
        located["fragment_window"] = {
            "start_stream_time": component.start,
            "end_stream_time": component.end,
            "duration_seconds": component.duration,
        }
        located["search_window"] = materialized
        attempts.append({
            "component_index": index,
            "window_start": component.start,
            "window_end": component.end,
            "duration_seconds": component.duration,
            "result": located.get("output_kind", "precise_refined"),
            "precise_location": bool(located.get("precise_location", True)),
            "locator_method": located.get("locator_method"),
            "target_clock": located.get("target_clock"),
        })
        is_exact_second = (
            isinstance(located.get("ocr"), dict)
            and located["ocr"].get("location_kind") == "match_clock_second"
        )
        if is_exact_second:
            exact_matches.append((located, materialized))
            if scan_all_for_exact_second:
                continue
        if not located.get("minute_fallback"):
            if scan_all_for_exact_second:
                if secondary is None:
                    secondary = (located, materialized)
                continue
            located["fragment_attempts"] = attempts
            return located, materialized, [
                str(segment.path.resolve())
                for item in components
                for segment in item.segments
            ]
        distance = abs(
            float(located.get("anchor_stream_time", component.end))
            - job.api_observed_stream_time
        )
        if fallback is None or distance < fallback[2]:
            fallback = (located, materialized, distance)

    leased_paths = [
        str(segment.path.resolve())
        for item in components
        for segment in item.segments
    ]
    if scan_all_for_exact_second and len(exact_matches) > 1:
        minute, second = divmod(int(job.event_second), 60)
        target_clock = f"{minute:02d}:{second:02d}"
        ambiguity = {
            "kind": "ocr_ambiguous",
            "stage": "event_second_localization",
            "message": (
                f"the target match clock {target_clock} appeared in multiple "
                "video fragments"
            ),
            "target_clock": target_clock,
            "target_clock_seconds": job.event_second,
            "matching_fragment_count": len(exact_matches),
            "matching_fragment_windows": [
                located.get("fragment_window")
                for located, _materialized in exact_matches
            ],
        }
        # The exact clock is unsafe, but it can still be a useful signal that
        # leads into the established score/minute fallback chain. Re-run only
        # the API-nearest matching fragment without event_second.
        _exact, ambiguity_materialized = exact_matches[0]
        ambiguity_path = Path(str(ambiguity_materialized["path"]))
        try:
            downgraded = locate_with_ocr_fallback(
                replace(job, event_second=None),
                ambiguity_path,
                ambiguity_materialized,
                tdeed_python=tdeed_python,
                ocr_python=ocr_python,
                ocr_timeout_seconds=ocr_timeout_seconds,
                tdeed_timeout_seconds=tdeed_timeout_seconds,
            )
        except (VisualLocationFailed, OSError, TypeError, ValueError) as exc:
            attempts.append({
                "result": "exact_second_ambiguity_fallback_failed",
                "error_kind": getattr(exc, "kind", type(exc).__name__),
                "error": str(exc),
            })
        else:
            downgraded = dict(downgraded)
            downgraded["exact_second_error"] = ambiguity
            downgraded["fallback_used"] = True
            downgraded["fragment_attempts"] = attempts
            downgraded["search_window"] = ambiguity_materialized
            return downgraded, ambiguity_materialized, leased_paths
        raise VisualLocationFailed(
            "ocr_ambiguous",
            ambiguity["message"],
            {
                **ambiguity,
                "fragment_attempts": attempts,
                "default_gif_preserved": True,
                "fallback_generated": False,
            },
        )
    if exact_matches:
        located, materialized = exact_matches[0]
        located["fragment_attempts"] = attempts
        return located, materialized, leased_paths
    if secondary is not None:
        located, materialized = secondary
        located["fragment_attempts"] = attempts
        return located, materialized, leased_paths
    if fallback is not None:
        located, materialized, _distance = fallback
        located["fragment_attempts"] = attempts
        return located, materialized, leased_paths
    last = attempts[-1] if attempts else {}
    last_diagnostics = dict(last.get("diagnostics") or {})
    raise VisualLocationFailed(
        str(last.get("error_kind") or "tdeed_no_candidate"),
        str(last.get("error") or "no visual candidate found in continuous video components"),
        {
            **last_diagnostics,
            "stage": last_diagnostics.get("stage") or "fragmented_search",
            "search_stage": "fragmented_search",
            "fragment_attempts": attempts,
            "default_gif_preserved": True,
            "fallback_generated": False,
        },
    )


@dataclass
class VisionJob:
    event_key: str
    match_id: str
    code: str
    event_type: str
    default_anchor_stream_time: float
    default_anchor_source_time: float | None
    detected_at_unix: float
    observed_anchor_stream_time: float | None = None
    observed_anchor_source_time: float | None = None
    event_minute: str = ""
    event_minute_extra: str = "0"
    event_second: int | None = None
    target_score: str = ""
    scoreboard_profile: dict[str, Any] | str | None = None
    require_scoreboard_profile: bool = False
    clock_only: bool = False

    @property
    def api_observed_stream_time(self) -> float:
        if self.observed_anchor_stream_time is None:
            return self.default_anchor_stream_time
        return self.observed_anchor_stream_time

    @property
    def api_observed_source_time(self) -> float | None:
        if self.observed_anchor_source_time is None:
            return self.default_anchor_source_time
        return self.observed_anchor_source_time


def materialize_analysis_clip(
    ffmpeg: str,
    segments: list[Segment],
    output: Path,
    *,
    window_start: float,
    window_end: float,
    anchor: float,
    coverage: VideoCoverage | None = None,
    preserve_resolution: bool = False,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Create an OCR-quality or compact T-DEED clip from leased segments.

    ``timeout_seconds`` is optional for compatibility with existing callers.
    Direct-TS OCR fallback supplies its remaining deadline so a slow FFmpeg
    materialization cannot outlive the OCR subprocess budget.
    """
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive when provided")
    coverage = coverage or analyze_video_coverage(
        segments,
        window_start=window_start,
        window_end=window_end,
        anchor=anchor,
        allow_degraded=True,
    )
    coverage.validate_request(
        window_start=window_start,
        window_end=window_end,
        anchor=anchor,
    )
    if coverage.status == CoverageStatus.WAITING:
        raise BufferNotReady(coverage.reason)
    if coverage.status == CoverageStatus.UNAVAILABLE:
        raise BufferUnavailable(coverage.reason)
    if coverage.effective_start is None or coverage.effective_end is None:
        raise BufferUnavailable("vision coverage has no usable interval")
    selected = list(coverage.segments)
    window_start = coverage.effective_start
    window_end = coverage.effective_end

    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", prefix="vision_segments_", dir=output.parent,
        encoding="utf-8", delete=False,
    ) as handle:
        concat_path = Path(handle.name)
        for segment in selected:
            escaped = str(segment.path.resolve()).replace("'", "'\\''")
            handle.write(f"file '{escaped}'\n")
    try:
        # Remove a previous partial result so a failed FFmpeg run cannot be
        # mistaken for the newly materialized analysis clip.
        output.unlink(missing_ok=True)
        seek = max(0.0, window_start - selected[0].start)
        command = [
                ffmpeg, "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
                "-f", "concat", "-safe", "0", "-i", str(concat_path),
                "-ss", f"{seek:.3f}", "-t", f"{window_end - window_start:.3f}",
                "-an",
        ]
        if preserve_resolution:
            command.extend([
                "-c:v", "libx264", "-threads", "1", "-preset", "veryfast",
                "-crf", "20", "-pix_fmt", "yuv420p", str(output),
            ])
        else:
            command.extend([
                "-vf", "fps=25,scale=398:224:flags=lanczos", "-c:v", "libx264",
                "-threads", "1", "-preset", "veryfast", "-crf", "27",
                "-pix_fmt", "yuv420p", str(output),
            ])
        completed = subprocess.run(
            command,
            check=True,
            text=True,
            capture_output=True,
            **({"timeout": timeout_seconds} if timeout_seconds is not None else {}),
        )
        if not output.is_file():
            raise RuntimeError(
                f"FFmpeg did not create analysis video output: {output}"
            )
        output_bytes = output.stat().st_size
        if output_bytes <= 0:
            raise RuntimeError(
                f"FFmpeg created an empty analysis video output: {output}"
            )
    except Exception:
        # Do not leave an invalid partial clip for a later retry to consume.
        if output.is_file() or output.is_symlink():
            output.unlink(missing_ok=True)
        raise
    finally:
        concat_path.unlink(missing_ok=True)
    return {
        "path": str(output.resolve()),
        "window_start_stream_time": window_start,
        "window_end_stream_time": window_end,
        "requested_window_start_stream_time": coverage.requested_start,
        "requested_window_end_stream_time": coverage.requested_end,
        "selected_segment_count": len(selected),
        "bytes": output_bytes,
        "ffmpeg_stderr": completed.stderr[-2000:] if completed.stderr else "",
        "coverage_status": coverage.status.value,
        "coverage_reason": coverage.reason,
        **(
            {"coverage_error_kind": coverage.error_kind}
            if coverage.error_kind else {}
        ),
        **(
            {"skipped_gap_seconds": coverage.skipped_gap_seconds}
            if coverage.skipped_gap_seconds else {}
        ),
        **(
            {"degraded_reason": coverage.reason}
            if coverage.status == CoverageStatus.READY_DEGRADED else {}
        ),
    }


def _refine_event_job_legacy(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    search_before: float,
    search_after: float,
    refined_before: float,
    refined_after: float,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    python: Path,
    timeout_seconds: float,
    ocr_python: Path = OCR_PYTHON,
    ocr_timeout_seconds: float = 180.0,
    fallback_width: int = OCR_MINUTE_FALLBACK_WIDTH,
    fallback_fps: float = OCR_MINUTE_FALLBACK_FPS,
    fallback_colors: int = OCR_MINUTE_FALLBACK_COLORS,
    min_degraded_seconds: float = MIN_DEGRADED_CLIP_SECONDS,
    cancel_event: Any = None,
) -> bool:
    """Run the optional visual branch. Failures are persisted and never escape."""
    current = runtime.store.get_vision_task(job.event_key)
    if current is None:
        raise KeyError(f"unknown vision task: {job.event_key}")
    if time.time() < current.next_attempt_at_unix:
        return False
    lease_id: str | None = None
    analysis_path = output_dir / "vision_candidates" / f"{job.event_key.rsplit(':', 1)[-1][:8]}.mp4"
    result_path = analysis_path.with_suffix(".json")
    try:
        scope_error = _v1_clock_scope_error(job)
        if scope_error is not None:
            kind, message = scope_error
            raise VisualLocationFailed(
                kind,
                message,
                {
                    "stage": "v1_scope",
                    "event_minute": _event_minute(job) or None,
                    "default_gif_preserved": True,
                    "fallback_generated": False,
                },
            )
        if current.status == "located" and current.located_anchor_stream_time is not None:
            refined_anchor = current.located_anchor_stream_time
            anchor_source = current.located_anchor_source_time
            located = dict(current.result)
            # Normalize stored key names back to raw locator key names so that
            # the shared encoded.update() block below works correctly on both
            # the fresh-execution path and this resume-from-"located" path.
            located.setdefault("label", located.get("model_label"))
            located.setdefault("checkpoint_sha256", located.get("model_weights_sha256"))
            located.setdefault("locator_wall_seconds", located.get("inference_seconds"))
            located.setdefault("locator_method", located.get("locator_method"))
            materialized = located.get("search_window") or {}
            located_ocr = located.get("ocr")
            if not isinstance(located_ocr, dict):
                located_ocr = {}
            target_clock = located.get("target_clock") or located_ocr.get(
                "target_clock"
            )
            exact_second_error = located.get(
                "exact_second_error"
            ) or located_ocr.get("exact_second_error")
        else:
            window_start = max(
                0.0,
                current.search_start_stream_time,
            )
            window_end = current.search_end_stream_time
            search_anchor = job.api_observed_stream_time
            if not window_start <= search_anchor <= window_end:
                # Backward-compatible fallback for a malformed legacy row.
                window_start = max(
                    0.0, search_anchor - search_before
                )
                window_end = search_anchor + search_after
            segments = segment_reader()
            deadline_reached = time.time() >= current.deadline_at_unix
            components, latest_end = _continuous_search_components(
                segments,
                window_start=window_start,
                window_end=window_end,
                minimum_seconds=max(3.0, min_degraded_seconds),
            )
            if (
                latest_end is not None
                and latest_end < window_end - 0.25
                and not deadline_reached
            ):
                runtime.record_vision_readiness_wait(
                    job.event_key,
                    (
                        f"video tail has reached {latest_end:.3f}s but "
                        f"{window_end:.3f}s is required"
                    ),
                    error_kind="waiting_for_tail",
                )
                return False
            if not components:
                error_kind = (
                    "vision_deadline_exceeded" if deadline_reached else "buffer_gap"
                )
                error = (
                    "vision search video has no continuous component long enough "
                    f"for OCR in [{window_start:.3f}, {window_end:.3f}]"
                )
                diagnostics = {
                    "window_start_stream_time": window_start,
                    "window_end_stream_time": window_end,
                    "latest_media_end": latest_end,
                    "fragment_attempts": [],
                }
                runtime.transition_vision_task(
                    job.event_key,
                    "failed",
                    result=_vision_failure_result(
                        error_kind,
                        error,
                        failure_stage="fragmented_search",
                        **diagnostics,
                    ),
                    error=error,
                    error_kind=error_kind,
                )
                return True
            leased_paths = list(dict.fromkeys(
                str(segment.path.resolve())
                for component in components
                for segment in component.segments
            ))
            lease_id = runtime.store.acquire_segment_lease(
                job.event_key,
                leased_paths,
                owner="vision-locator",
                ttl_seconds=max(timeout_seconds + 60.0, 180.0),
            )
            runtime.transition_vision_task(job.event_key, "locating")
            located, materialized, _ = _locate_across_search_components(
                job,
                segments,
                window_start=window_start,
                window_end=window_end,
                analysis_path=analysis_path,
                ffmpeg=ffmpeg,
                tdeed_python=python,
                ocr_python=ocr_python,
                ocr_timeout_seconds=ocr_timeout_seconds,
                tdeed_timeout_seconds=timeout_seconds,
                minimum_component_seconds=max(3.0, min_degraded_seconds),
                components=components,
            )
            refined_anchor = float(located["anchor_stream_time"])
            located_ocr = located.get("ocr")
            if not isinstance(located_ocr, dict):
                located_ocr = {}
            target_clock = located.get("target_clock") or located_ocr.get(
                "target_clock"
            )
            exact_second_error = located.get(
                "exact_second_error"
            ) or located_ocr.get("exact_second_error")
            anchor_source = (
                job.api_observed_source_time
                + (refined_anchor - job.api_observed_stream_time)
                if job.api_observed_source_time is not None else None
            )
            locate_result = {
                "anchor_stream_time": refined_anchor,
                "anchor_source_time": anchor_source,
                "confidence": located.get("confidence"),
                "inference_seconds": located.get("locator_wall_seconds"),
                "model_name": located.get("model_name"),
                "model_version": located.get("model_version"),
                "model_weights_sha256": located.get("checkpoint_sha256"),
                "model_label": located.get("label"),
                "locator_method": located.get("locator_method"),
                "fallback_used": bool(located.get("fallback_used")),
                "minute_fallback": bool(located.get("minute_fallback")),
                "tdeed_error_kind": located.get("tdeed_error_kind"),
                "tdeed_error": located.get("tdeed_error"),
                "error_kind": located.get("error_kind"),
                "failure_reason": located.get("failure_reason"),
                "output_kind": located.get("output_kind", "precise_refined"),
                "precise_location": located.get("precise_location", True),
                "target_clock": target_clock,
                "exact_second_error": exact_second_error,
                "localization_quality": located.get("localization_quality"),
                "degraded": bool(located.get("degraded")),
                "degradation_mode": located.get("degradation_mode"),
                "degradation_reason": located.get("degradation_reason"),
                "clip_before_seconds": located.get(
                    "clip_before_seconds", refined_before
                ),
                "clip_after_seconds": located.get(
                    "clip_after_seconds", refined_after
                ),
                "ocr": located.get("ocr"),
                "ocr_error": located.get("ocr_error"),
                "ocr_clock_only": bool(located.get("ocr_clock_only")),
                "fragment_attempts": located.get("fragment_attempts", []),
                "fragment_window": located.get("fragment_window"),
                "stage": "located",
                "search_window": materialized,
            }
            (
                locate_result["clip_before_seconds"],
                locate_result["clip_after_seconds"],
            ) = _normalized_clip_window_values(
                locate_result,
                default_before=refined_before,
                default_after=refined_after,
                stage="legacy_location_persistence",
                error_kind="vision_invalid_clip_window",
                location_kind=str(located.get("location_kind") or "") or None,
            )
            runtime.transition_vision_task(job.event_key, "located", result=locate_result)
            try:
                result_path.write_text(
                    json.dumps(locate_result, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8",
                )
            except OSError:
                pass
            runtime.store.release_segment_lease(lease_id)
            lease_id = None

        effective_refined_before, effective_refined_after = (
            _normalized_clip_window_values(
                located,
                default_before=refined_before,
                default_after=refined_after,
                stage="legacy_output_window_validation",
                error_kind="vision_invalid_clip_window",
                location_kind=str(located.get("location_kind") or "") or None,
            )
        )
        segments = segment_reader()
        refined_start = max(0.0, refined_anchor - effective_refined_before)
        refined_end = refined_anchor + effective_refined_after
        current = runtime.store.get_vision_task(job.event_key)
        if current is None:
            raise KeyError(f"unknown vision task: {job.event_key}")
        deadline_reached = time.time() >= current.deadline_at_unix
        coverage = _vision_coverage(
            segments,
            window_start=refined_start,
            window_end=refined_end,
            anchor=refined_anchor,
            deadline_reached=deadline_reached,
            min_degraded_seconds=min_degraded_seconds,
        )
        if coverage.status == CoverageStatus.WAITING:
            if deadline_reached:
                error = (
                    "refined GIF video was not ready before the vision deadline: "
                    f"{coverage.reason}"
                )
                runtime.transition_vision_task(
                    job.event_key,
                    "failed",
                    result=_vision_failure_result(
                        "vision_deadline_exceeded",
                        error,
                        failure_stage="output_coverage",
                    ),
                    error=error,
                    error_kind="vision_deadline_exceeded",
                )
                return True
            runtime.record_vision_readiness_wait(
                job.event_key,
                coverage.reason,
                error_kind=coverage.error_kind or "waiting_for_video",
            )
            return False
        if coverage.status == CoverageStatus.UNAVAILABLE:
            error_kind = _normalized_buffer_error_kind(coverage.error_kind)
            runtime.transition_vision_task(
                job.event_key,
                "failed",
                result=_vision_failure_result(
                    error_kind,
                    coverage.reason,
                    failure_stage="output_coverage",
                ),
                error=coverage.reason,
                error_kind=error_kind,
            )
            return True
        available_fallback_seconds = None
        fallback_complete = None
        if located.get("minute_fallback"):
            if coverage.effective_start is not None and coverage.effective_end is not None:
                available_fallback_seconds = max(
                    0.0, float(coverage.effective_end) - float(coverage.effective_start)
                )
            fallback_complete = (
                available_fallback_seconds is not None
                and available_fallback_seconds
                >= (
                    OCR_MINUTE_FALLBACK_BEFORE_SECONDS
                    + OCR_MINUTE_FALLBACK_AFTER_SECONDS
                ) * OCR_MINUTE_FALLBACK_COMPLETE_RATIO
            )
            located["fallback_complete"] = bool(fallback_complete)
            located["fallback_label"] = (
                "60_second_fallback" if fallback_complete else "fragmented_clip"
            )
            # Coverage quality is reported separately. Keep one successful
            # fallback contract instead of inventing a fifth output outcome.
            located["output_kind"] = "minute_range_fallback"
            located["available_fallback_seconds"] = available_fallback_seconds
            located["requested_fallback_seconds"] = (
                OCR_MINUTE_FALLBACK_BEFORE_SECONDS
                + OCR_MINUTE_FALLBACK_AFTER_SECONDS
            )
        leased_paths = [
            str(segment.path.resolve())
            for segment in coverage.segments
        ]
        lease_id = runtime.store.acquire_segment_lease(
            job.event_key,
            leased_paths,
            owner="vision-encoder",
            ttl_seconds=max(timeout_seconds + 60.0, 180.0),
        )
        pending = PendingEvent(
            event_type=f"{job.event_type}_refined",
            stream_time=refined_anchor,
            source_time=anchor_source,
            detected_wall_time=job.detected_at_unix,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=refined_anchor + effective_refined_after,
            output_id=job.event_key.rsplit(":", 1)[-1][:8],
        )
        latest_task = runtime.store.get(job.event_key)
        if latest_task is None:
            raise KeyError(f"unknown event task: {job.event_key}")
        output_filename = build_gif_filename(
            match_id=latest_task.match_id,
            event_data=latest_task.event_data,
            variant=("fallback" if located.get("minute_fallback") else "ai"),
        )
        effective_width = fallback_width if located.get("minute_fallback") else width
        effective_fps = fallback_fps if located.get("minute_fallback") else fps
        effective_colors = (
            fallback_colors if located.get("minute_fallback") else colors
        )
        runtime.transition_vision_task(job.event_key, "encoding")
        encoded = encode_gif(
            ffmpeg, ffprobe, segments, pending, output_dir,
            before=effective_refined_before, after=effective_refined_after,
            width=effective_width, fps=effective_fps,
            colors=effective_colors, size_reference_bytes=size_reference_bytes,
            cancel_event=cancel_event,
            coverage=coverage,
            output_filename=output_filename,
        )
        encoded.update({
            "default_anchor_stream_time_sec": job.default_anchor_stream_time,
            "api_observed_stream_time_sec": job.api_observed_stream_time,
            "vision_anchor_stream_time_sec": round(refined_anchor, 3),
            "anchor_delta_seconds": round(refined_anchor - job.default_anchor_stream_time, 3),
            "confidence": located.get("confidence"),
            "model_label": located.get("label"),
            "model_version": located.get("model_version"),
            "checkpoint_sha256": located.get("checkpoint_sha256"),
            "locator_wall_seconds": located.get("locator_wall_seconds"),
            "locator_method": located.get("locator_method"),
            "fallback_used": bool(located.get("fallback_used")),
            "minute_fallback": bool(located.get("minute_fallback")),
            "tdeed_error_kind": located.get("tdeed_error_kind"),
            "tdeed_error": located.get("tdeed_error"),
            "error_kind": located.get("error_kind"),
            "failure_reason": located.get("failure_reason"),
            "output_kind": located.get("output_kind", "precise_refined"),
            "precise_location": located.get("precise_location", True),
            "target_clock": target_clock,
            "exact_second_error": exact_second_error,
            "localization_quality": located.get("localization_quality"),
            "degraded": bool(located.get("degraded")),
            "degradation_mode": located.get("degradation_mode"),
            "degradation_reason": located.get("degradation_reason"),
            "ocr_clock_only": bool(located.get("ocr_clock_only")),
            "default_gif_preserved": True,
            "fallback_generated": bool(located.get("minute_fallback")),
            "fallback_complete": located.get("fallback_complete"),
            "fallback_label": located.get("fallback_label"),
            "fragmented_fallback": bool(
                located.get("minute_fallback") and not located.get("fallback_complete", False)
            ),
            "available_fallback_seconds": located.get("available_fallback_seconds"),
            "requested_fallback_seconds": located.get("requested_fallback_seconds"),
            "clip_before_seconds": effective_refined_before,
            "clip_after_seconds": effective_refined_after,
            "output_width": effective_width,
            "output_fps": effective_fps,
            "output_colors": effective_colors,
            "ocr": located.get("ocr"),
            "ocr_error": located.get("ocr_error"),
            "stage": "encoded",
            "search_window": materialized,
            "fragment_attempts": located.get("fragment_attempts", []),
            "fragment_window": located.get("fragment_window"),
            "experimental": job.code in {"YC", "RC"},
        })
        runtime.transition_vision_task(job.event_key, "encoded", result=encoded)
        return True
    except VisualLocationFailed as exc:
        failure_result = {
            "stage": "failed",
            "error_kind": exc.kind,
            "locator_method": "ocr_then_tdeed",
            "fallback_used": bool(exc.diagnostics.get("fallback_used")),
            "minute_fallback": False,
            "precise_location": False,
            "default_gif_preserved": True,
            "fallback_generated": False,
            "output_kind": "failed",
            "failure_reason": {
                "kind": exc.kind,
                "stage": exc.diagnostics.get("stage", "event_localization"),
                "message": str(exc),
            },
            **exc.diagnostics,
        }
        try:
            result_path.parent.mkdir(parents=True, exist_ok=True)
            result_path.write_text(
                json.dumps(failure_result, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        except OSError:
            pass
        current = runtime.store.get_vision_task(job.event_key)
        if current is not None and current.status != "failed":
            runtime.transition_vision_task(
                job.event_key,
                "failed",
                result=failure_result,
                error=str(exc),
                error_kind=exc.kind,
            )
        return True
    except (BufferNotReady, VisionBufferNotReady) as exc:
        current = runtime.store.get_vision_task(job.event_key)
        if current is not None and current.status in {"locating", "located", "encoding"}:
            retry_status = "located" if current.status == "encoding" else "pending"
            runtime.transition_vision_task(
                job.event_key,
                retry_status,
                result={"last_buffer_error": str(exc)},
                reason="buffer_not_ready",
            )
            current = runtime.store.get_vision_task(job.event_key)
        if current is not None and current.status in {"pending", "located"}:
            if time.time() >= current.deadline_at_unix:
                runtime.transition_vision_task(
                    job.event_key,
                    "failed",
                    result=_vision_failure_result(
                        "vision_deadline_exceeded",
                        str(exc),
                        failure_stage="buffer",
                    ),
                    error=str(exc),
                    error_kind="vision_deadline_exceeded",
                )
                return True
            runtime.record_vision_readiness_wait(
                job.event_key,
                str(exc),
                error_kind="waiting_for_video",
            )
        return False
    except (BufferUnavailable, VisionBufferUnavailable) as exc:
        current = runtime.store.get_vision_task(job.event_key)
        if current is not None and current.status != "failed":
            message = str(exc)
            error_kind = (
                "buffer_gap" if "gap" in message.lower()
                else "buffer_history_missing"
                if "beginning" in message.lower() or "history" in message.lower()
                else "video_unavailable"
            )
            runtime.transition_vision_task(
                job.event_key,
                "failed",
                result=_vision_failure_result(
                    error_kind,
                    message,
                    failure_stage="buffer",
                ),
                error=message,
                error_kind=error_kind,
            )
        return True
    except Exception as exc:
        current = runtime.store.get_vision_task(job.event_key)
        if current is not None and current.status != "failed":
            # The deadline bounds video readiness only. Inference has its own
            # timeout, so processing failures after readiness are not deadline failures.
            error_kind = (
                "encode_failed" if current.status == "encoding"
                else "vision_processing_failed"
            )
            failure_stage = (
                "encoding" if current.status == "encoding" else "vision_processing"
            )
            runtime.transition_vision_task(
                job.event_key,
                "failed",
                result=_vision_failure_result(
                    error_kind,
                    str(exc),
                    failure_stage=failure_stage,
                ),
                error=str(exc),
                error_kind=error_kind,
            )
        return True
    finally:
        if lease_id:
            runtime.store.release_segment_lease(lease_id)
        # Candidate media and diagnostics are retained for at most 24 hours by
        # DiskLifecycleManager so failed OCR/T-DEED runs can be investigated.


def _artifact_task(runtime: Any, event_key: str, artifact_kind: str) -> Any:
    return runtime.store.get_vision_task(
        event_key,
        artifact_kind=artifact_kind,
    )


def _artifact_transition(
    runtime: Any,
    event_key: str,
    artifact_kind: str,
    status: str,
    **fields: Any,
) -> Any:
    return runtime.transition_vision_task(
        event_key,
        status,
        artifact_kind=artifact_kind,
        **fields,
    )


def _artifact_readiness_wait(
    runtime: Any,
    event_key: str,
    artifact_kind: str,
    error: str,
    *,
    error_kind: str,
    result: dict[str, Any] | None = None,
    window_metadata: dict[str, Any] | None = None,
    next_attempt_at_unix: float | None = None,
    deadline_at_unix: float | None = None,
    now: float | None = None,
) -> Any:
    return runtime.record_vision_readiness_wait(
        event_key,
        error,
        artifact_kind=artifact_kind,
        error_kind=error_kind,
        result=result,
        window_metadata=window_metadata,
        next_attempt_at_unix=next_attempt_at_unix,
        deadline_at_unix=deadline_at_unix,
        now=now,
    )


def _clock_text_from_seconds(value: int | float | None) -> str | None:
    if value is None:
        return None
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    minute, second = divmod(seconds, 60)
    return f"{minute:02d}:{second:02d}"


def _clock_seconds_from_text(value: Any) -> int | None:
    raw = str(value or "").strip()
    if not raw or ":" not in raw:
        return None
    minute_text, second_text = raw.split(":", 1)
    try:
        minute = int(minute_text)
        second = int(second_text)
    except ValueError:
        return None
    if minute < 0 or not 0 <= second < 60:
        return None
    return minute * 60 + second


def _latest_trusted_clock_seconds(value: Any) -> int | None:
    """Extract the newest continuity-approved clock from OCR diagnostics."""
    candidates: list[int] = []

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            trusted_range = item.get("trusted_clock_range")
            if isinstance(trusted_range, (list, tuple)):
                for clock in trusted_range:
                    parsed = _clock_seconds_from_text(clock)
                    if parsed is not None:
                        candidates.append(parsed)

            for key in ("clock_raw_observations", "readings"):
                readings = item.get(key)
                if not isinstance(readings, list):
                    continue
                for reading in readings:
                    if not isinstance(reading, dict):
                        continue
                    if reading.get("scoreboard_visible") is False:
                        continue
                    if reading.get("ambiguous_clock") is True:
                        continue
                    if str(reading.get("continuity_status") or "") in {
                        "rejected",
                        "repaired",
                    }:
                        continue
                    parsed: int | None = None
                    for field in (
                        "effective_clock_seconds",
                        "clock_seconds",
                        "clock",
                    ):
                        raw = reading.get(field)
                        if isinstance(raw, bool):
                            continue
                        if isinstance(raw, (int, float)) and float(raw).is_integer():
                            parsed = int(raw)
                        else:
                            parsed = _clock_seconds_from_text(raw)
                        if parsed is not None and parsed >= 0:
                            candidates.append(parsed)
                            break

            for key, nested in item.items():
                if key not in {
                    "target_clock",
                    "target_clock_seconds",
                    "minute_window_start_clock",
                    "minute_window_end_clock",
                    "requested_match_clock_window",
                    "clock_raw_observations",
                    "readings",
                    "trusted_clock_range",
                }:
                    visit(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested)

    visit(value)
    return max(candidates) if candidates else None


def _ocr_progressive_target_seconds(job: VisionJob) -> int | None:
    """Return the last clock boundary that can yield an OCR-authorized anchor."""
    minute_text = _event_minute(job).rstrip("'").strip()
    minute_target: int | None = None
    if minute_text:
        base_text, separator, extra_text = minute_text.partition("+")
        try:
            base = int(base_text)
            extra = int(extra_text) if separator else 0
        except ValueError:
            minute_target = None
        else:
            if base >= 0 and extra >= 0:
                minute_target = (base + extra) * 60
    if job.event_second is None:
        return minute_target
    if minute_target is None:
        return int(job.event_second)
    # Before this boundary the minute fallback is not yet observable. Continue
    # scanning even if an isolated OCR miss has already passed the exact second.
    return max(int(job.event_second), minute_target)


def _ocr_progressive_state(task: Any) -> dict[str, Any]:
    metadata = getattr(task, "window_metadata", None)
    if not isinstance(metadata, dict):
        return {}
    state = metadata.get("progressive_scan")
    return dict(state) if isinstance(state, dict) else {}


def _ocr_deadline_policy(
    task: Any,
    *,
    now_unix: float,
    target_clock_seconds: int | None,
    latest_trusted_clock_seconds: int | None,
) -> dict[str, Any]:
    """Return the persisted target-wait budget, excluding visual queue time."""
    state = _ocr_progressive_state(task)
    previous = state.get("deadline_policy")
    policy = dict(previous) if isinstance(previous, dict) else {}
    queue_timing = getattr(task, "window_metadata", {}).get("queue_timing") or {}
    slot_acquired = queue_timing.get("acquired_at_unix") is not None
    queue_wait = max(0.0, float(queue_timing.get("total_queue_wait_seconds") or 0.0))
    accounted_queue_wait = max(
        0.0,
        float(policy.get("queue_wait_accounted_seconds") or 0.0),
    )
    queue_extension = max(0.0, queue_wait - accounted_queue_wait)

    if "hard_deadline_at_unix" not in policy:
        initial_cap = float(task.created_at_unix) + OCR_TARGET_WAIT_INITIAL_SECONDS
        initial_deadline = min(float(task.deadline_at_unix), initial_cap)
        policy.update({
            "policy_version": 1,
            "phase": "target_wait",
            "initial_target_wait_seconds": OCR_TARGET_WAIT_INITIAL_SECONDS,
            "target_wait_margin_seconds": OCR_TARGET_WAIT_MARGIN_SECONDS,
            "event_hard_limit_seconds": OCR_EVENT_HARD_LIMIT_SECONDS,
            "initial_target_deadline_at_unix": initial_deadline,
            "target_deadline_at_unix": initial_deadline,
            "hard_deadline_at_unix": (
                float(task.created_at_unix) + OCR_EVENT_HARD_LIMIT_SECONDS
            ),
        })

    if queue_extension > 0:
        for field in (
            "initial_target_deadline_at_unix",
            "target_deadline_at_unix",
            "hard_deadline_at_unix",
            "postroll_deadline_at_unix",
            "postroll_hard_deadline_at_unix",
        ):
            if policy.get(field) is not None:
                policy[field] = float(policy[field]) + queue_extension
    policy["queue_wait_accounted_seconds"] = queue_wait
    policy["slot_acquired"] = slot_acquired

    gap_seconds: int | None = None
    if (
        target_clock_seconds is not None
        and latest_trusted_clock_seconds is not None
    ):
        gap_seconds = max(
            0,
            int(target_clock_seconds) - int(latest_trusted_clock_seconds),
        )
        proposed_deadline = (
            float(now_unix) + gap_seconds + OCR_TARGET_WAIT_MARGIN_SECONDS
        )
        policy["target_deadline_at_unix"] = min(
            float(policy["hard_deadline_at_unix"]),
            max(float(policy["target_deadline_at_unix"]), proposed_deadline),
        )
    policy.update({
        "phase": "target_wait",
        "target_clock_seconds": target_clock_seconds,
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "target_clock_gap_seconds": gap_seconds,
        "updated_at_unix": float(now_unix),
    })
    return policy


def _ocr_far_target_retry_at(
    *,
    now_unix: float,
    target_clock_seconds: int | None,
    latest_trusted_clock_seconds: int | None,
    target_deadline_at_unix: float,
) -> float | None:
    if target_clock_seconds is None or latest_trusted_clock_seconds is None:
        return None
    gap = max(0, int(target_clock_seconds) - int(latest_trusted_clock_seconds))
    if gap <= OCR_TARGET_WAIT_MARGIN_SECONDS:
        return None
    delay = min(
        OCR_FAR_TARGET_RETRY_MAX_SECONDS,
        max(
            OCR_FAR_TARGET_RETRY_MIN_SECONDS,
            gap - OCR_TARGET_WAIT_MARGIN_SECONDS,
        ),
    )
    return min(float(target_deadline_at_unix), float(now_unix) + delay)


def _ocr_postroll_deadline_policy(
    task: Any,
    *,
    now_unix: float,
    required_wait_seconds: float,
) -> dict[str, Any]:
    state = _ocr_progressive_state(task)
    target_clock = state.get("target_clock_seconds")
    latest_clock = state.get("latest_trusted_clock_seconds")
    policy = _ocr_deadline_policy(
        task,
        now_unix=now_unix,
        target_clock_seconds=(target_clock if isinstance(target_clock, int) else None),
        latest_trusted_clock_seconds=(
            latest_clock if isinstance(latest_clock, int) else None
        ),
    )
    postroll_hard_deadline = policy.get("postroll_hard_deadline_at_unix")
    if postroll_hard_deadline is None:
        postroll_hard_deadline = (
            float(now_unix) + OCR_POSTROLL_HARD_LIMIT_SECONDS
        )
    postroll_hard_deadline = float(postroll_hard_deadline)
    proposed = float(now_unix) + max(0.0, required_wait_seconds) + (
        OCR_TARGET_WAIT_MARGIN_SECONDS
    )
    previous = policy.get("postroll_deadline_at_unix")
    policy["postroll_deadline_at_unix"] = min(
        postroll_hard_deadline,
        max(float(previous) if previous is not None else 0.0, proposed),
    )
    policy["postroll_hard_limit_seconds"] = OCR_POSTROLL_HARD_LIMIT_SECONDS
    policy["postroll_hard_deadline_at_unix"] = postroll_hard_deadline
    policy["phase"] = "postroll_wait"
    policy["postroll_required_wait_seconds"] = max(0.0, required_wait_seconds)
    policy["updated_at_unix"] = float(now_unix)
    return policy


def _ocr_progressive_wait(
    runtime: Any,
    job: VisionJob,
    task: Any,
    *,
    wait_kind: str,
    message: str,
    scan_start: float,
    scan_end: float,
    latest_trusted_clock_seconds: int | None,
    history_evicted: bool,
    diagnostics: dict[str, Any] | None = None,
    now_unix: float | None = None,
) -> None:
    timestamp = time.time() if now_unix is None else float(now_unix)
    previous = _ocr_progressive_state(task)
    previous_latest = previous.get("latest_trusted_clock_seconds")
    if isinstance(previous_latest, int):
        latest_trusted_clock_seconds = max(
            previous_latest,
            latest_trusted_clock_seconds
            if latest_trusted_clock_seconds is not None
            else previous_latest,
        )
    target_clock_seconds = _ocr_progressive_target_seconds(job)
    deadline_policy = _ocr_deadline_policy(
        task,
        now_unix=timestamp,
        target_clock_seconds=target_clock_seconds,
        latest_trusted_clock_seconds=latest_trusted_clock_seconds,
    )
    target_deadline = float(deadline_policy["target_deadline_at_unix"])
    next_attempt_at = _ocr_far_target_retry_at(
        now_unix=timestamp,
        target_clock_seconds=target_clock_seconds,
        latest_trusted_clock_seconds=latest_trusted_clock_seconds,
        target_deadline_at_unix=target_deadline,
    )
    progress = {
        **previous,
        "state": wait_kind,
        "scan_attempt_count": int(previous.get("scan_attempt_count") or 0) + 1,
        "last_scan_start_stream_time": round(scan_start, 3),
        "last_scan_end_stream_time": round(scan_end, 3),
        "scan_cursor_stream_time": round(scan_end, 3),
        "overlap_seconds": OCR_PROGRESSIVE_OVERLAP_SECONDS,
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "latest_trusted_clock": _clock_text_from_seconds(
            latest_trusted_clock_seconds
        ),
        "target_clock_seconds": target_clock_seconds,
        "target_clock": _clock_text_from_seconds(
            target_clock_seconds
        ),
        "history_evicted": bool(history_evicted),
        "deadline_policy": deadline_policy,
    }
    if diagnostics:
        progress["last_scan_diagnostics"] = diagnostics
        progress["last_execution_completed_at_unix"] = timestamp
        progress["last_execution_error_kind"] = diagnostics.get("kind")
    current = _artifact_task(runtime, job.event_key, "ocr_window")
    if current is not None and current.status == "locating":
        _artifact_transition(
            runtime,
            job.event_key,
            "ocr_window",
            "pending",
            reason=wait_kind,
        )
    _artifact_readiness_wait(
        runtime,
        job.event_key,
        "ocr_window",
        message,
        error_kind=wait_kind,
        result={
            "artifact_kind": "ocr_window",
            "stage": wait_kind,
            "progressive_status": wait_kind,
            "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
            "latest_trusted_clock": _clock_text_from_seconds(
                latest_trusted_clock_seconds
            ),
            "default_gif_preserved": True,
        },
        window_metadata={"progressive_scan": progress},
        next_attempt_at_unix=next_attempt_at,
        deadline_at_unix=target_deadline,
        now=timestamp,
    )


def _ocr_output_shape(
    job: VisionJob,
    located: dict[str, Any],
) -> tuple[float, float, str]:
    location_kind = str(located.get("location_kind") or "")
    if location_kind == "match_clock_second":
        return (
            OCR_EXACT_WINDOW_BEFORE_SECONDS,
            OCR_EXACT_WINDOW_AFTER_SECONDS,
            "exact_second",
        )
    if location_kind in {
        "match_clock_minute_boundary",
        "match_clock_minute_interval",
    }:
        return (
            OCR_MINUTE_WINDOW_BEFORE_SECONDS,
            OCR_MINUTE_WINDOW_AFTER_SECONDS,
            "minute_boundary",
        )
    if not job.clock_only and location_kind in {
        "score_transition",
        "score_transition_interval",
        "",
    }:
        # Explicit legacy mode keeps the pre-clock-only score contract. The
        # production clock-only path never reaches this branch.
        return (
            OCR_EXACT_WINDOW_BEFORE_SECONDS,
            OCR_EXACT_WINDOW_AFTER_SECONDS,
            "score_transition",
        )
    raise VisualLocationFailed(
        "ocr_target_localization_failed",
        "OCR did not verify an exact match-clock second or minute boundary",
        {
            "stage": "ocr_target_localization",
            "location_kind": location_kind or None,
            "event_minute": _event_minute(job) or None,
            "event_second": job.event_second,
        },
    )


def _normalized_ocr_clip_window(
    job: VisionJob,
    located: dict[str, Any],
    *,
    stage: str,
) -> tuple[float, float]:
    """Validate a persisted OCR output window and recover legacy nulls.

    Older located rows may have been persisted before the output shape was
    written. Recover those rows from the verified localization kind, while
    keeping malformed non-null values visible as a structured task failure.
    """
    shape_source = dict(located)
    nested_ocr = located.get("ocr")
    if isinstance(nested_ocr, dict) and not shape_source.get("location_kind"):
        shape_source["location_kind"] = nested_ocr.get("location_kind")
    localization_source = str(located.get("localization_source") or "")
    if not shape_source.get("location_kind"):
        shape_source["location_kind"] = {
            "exact_second": "match_clock_second",
            "minute_boundary": "match_clock_minute_boundary",
            "score_transition": "score_transition",
        }.get(localization_source)

    default_before, default_after, _source = _ocr_output_shape(job, shape_source)
    return _normalized_clip_window_values(
        located,
        default_before=default_before,
        default_after=default_after,
        stage=stage,
        error_kind="ocr_invalid_clip_window",
        localization_source=localization_source,
        location_kind=shape_source.get("location_kind"),
    )


def _normalized_clip_window_values(
    located: dict[str, Any],
    *,
    default_before: float,
    default_after: float,
    stage: str,
    error_kind: str,
    localization_source: str | None = None,
    location_kind: str | None = None,
) -> tuple[float, float]:
    """Normalize a persisted two-sided window without truthiness checks."""
    defaults: dict[str, float] = {}
    invalid_defaults: dict[str, Any] = {}
    for field, raw_default in {
        "clip_before_seconds": default_before,
        "clip_after_seconds": default_after,
    }.items():
        if isinstance(raw_default, bool):
            invalid_defaults[field] = raw_default
            continue
        try:
            value = float(raw_default)
        except (TypeError, ValueError):
            invalid_defaults[field] = raw_default
            continue
        if not math.isfinite(value) or value < 0:
            invalid_defaults[field] = raw_default
            continue
        defaults[field] = value
    if invalid_defaults or sum(defaults.values()) <= 0:
        raise VisualLocationFailed(
            error_kind,
            "configured clip window defaults are invalid",
            {
                "stage": stage,
                "invalid_default_fields": invalid_defaults,
                "default_clip_window": {
                    "clip_before_seconds": default_before,
                    "clip_after_seconds": default_after,
                },
                "localization_source": localization_source or None,
                "location_kind": location_kind,
            },
        )
    raw_values = {
        "clip_before_seconds": located.get("clip_before_seconds"),
        "clip_after_seconds": located.get("clip_after_seconds"),
    }
    normalized: dict[str, float] = {}
    invalid_fields: dict[str, Any] = {}
    for field, raw_value in raw_values.items():
        if raw_value is None:
            normalized[field] = defaults[field]
            continue
        if isinstance(raw_value, bool):
            invalid_fields[field] = raw_value
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            invalid_fields[field] = raw_value
            continue
        if not math.isfinite(value) or value < 0:
            invalid_fields[field] = raw_value
            continue
        normalized[field] = value

    if invalid_fields or sum(normalized.values()) <= 0:
        raise VisualLocationFailed(
            error_kind,
            "persisted clip window contains invalid values",
            {
                "stage": stage,
                "invalid_fields": invalid_fields,
                "raw_clip_window": raw_values,
                "localization_source": localization_source or None,
                "location_kind": location_kind,
                "default_clip_window": defaults,
            },
        )
    return (
        normalized["clip_before_seconds"],
        normalized["clip_after_seconds"],
    )


def _locate_ocr_window_across_components(
    job: VisionJob,
    segments: list[Segment],
    *,
    window_start: float,
    window_end: float,
    analysis_path: Path,
    ffmpeg: str,
    ocr_python: Path,
    ocr_timeout_seconds: float,
    minimum_component_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    if not Path(ocr_python).is_file():
        raise VisualLocationFailed(
            "ocr_model_unavailable",
            f"isolated PaddleOCR Python is missing: {ocr_python}",
            {"stage": "ocr_clock_discovery"},
        )
    components, latest_end = _continuous_search_components(
        segments,
        window_start=window_start,
        window_end=window_end,
        minimum_seconds=minimum_component_seconds,
    )
    if not components:
        raise VisualLocationFailed(
            "buffer_gap",
            "no continuous source-video component is long enough for OCR",
            {
                "stage": "buffer_coverage",
                "latest_media_end": latest_end,
                "window_start_stream_time": window_start,
                "window_end_stream_time": window_end,
            },
        )

    attempts: list[dict[str, Any]] = []
    matches: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for index, component in enumerate(components):
        component_path = analysis_path.with_name(
            f"{analysis_path.stem}.part{index:03d}{analysis_path.suffix}"
        )
        manifest_path = component_path.with_suffix(".ffconcat")
        direct_input = (
            _clock_manifest_for_component(component, manifest_path)
            if job.clock_only and job.scoreboard_profile is not None
            else None
        )
        direct_deadline = (
            time.monotonic() + ocr_timeout_seconds
            if direct_input is not None
            else None
        )
        try:
            if direct_input is not None:
                materialized = direct_input
            else:
                materialized = materialize_analysis_clip(
                    ffmpeg,
                    segments,
                    component_path,
                    window_start=component.start,
                    window_end=component.end,
                    anchor=(component.start + component.end) / 2.0,
                    coverage=_component_coverage(component),
                    preserve_resolution=True,
                )
            located = locate_scoreboard_event(
                Path(str(materialized["path"])),
                event_code=job.code,
                target_score=(None if job.clock_only else job.target_score or None),
                event_minute=_event_minute(job) or None,
                event_second=(
                    job.event_second
                    if job.code.upper().strip() in GOAL_LIKE_EVENT_CODES
                    else None
                ),
                candidate_start_seconds=component.start,
                timeout_seconds=(
                    max(0.001, direct_deadline - time.monotonic())
                    if direct_deadline is not None
                    else ocr_timeout_seconds
                ),
                python_executable=ocr_python,
                scoreboard_profile=job.scoreboard_profile,
                clock_only=job.clock_only,
                candidate_input_format=materialized.get("input_format"),
                candidate_seek_seconds=float(materialized.get("input_seek_seconds", 0.0)),
                candidate_duration_seconds=materialized.get("input_duration_seconds"),
            )
            located = dict(located)
            anchor, interval = _ocr_anchor_or_interval(
                located,
                window_start=component.start,
                window_end=component.end,
            )
            if anchor is None:
                raise VisualLocationFailed(
                    "ocr_no_target",
                    (
                        "OCR returned neither an anchor nor a valid candidate "
                        "interval in this video component"
                    ),
                    {
                        "stage": "ocr_target_localization",
                        "anchor_stream_time": located.get("anchor_seconds"),
                        "candidate_interval_start_seconds": located.get(
                            "candidate_interval_start_seconds"
                        ),
                        "candidate_interval_end_seconds": located.get(
                            "candidate_interval_end_seconds"
                        ),
                        "ocr_result": located,
                    },
                )
            if interval is not None:
                located.setdefault("location_kind", "match_clock_minute_interval")
                located["anchor_seconds"] = round(anchor, 3)
                located["ocr_anchor_from_interval"] = True
            else:
                located["ocr_anchor_from_interval"] = False
            located["anchor_stream_time"] = anchor
            located["fragment_index"] = index
            located["fragment_window"] = {
                "start_stream_time": component.start,
                "end_stream_time": component.end,
            }
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "location_kind": located.get("location_kind"),
                "method": located.get("method"),
                "anchor_stream_time": anchor,
            })
            matches.append((located, materialized))
            if direct_input is not None:
                manifest_path.unlink(missing_ok=True)
        except (ScoreboardOcrError, VisualLocationFailed) as exc:
            if direct_input is not None:
                # A direct ROI failure is retried through the stable MP4 path;
                # its diagnostics remain attached to this fragment attempt.
                manifest_path.unlink(missing_ok=True)
                remaining = float(direct_deadline or 0.0) - time.monotonic()
                if remaining <= 0:
                    attempts.append({
                        "component_index": index,
                        "window_start": component.start,
                        "window_end": component.end,
                        "error_kind": "inference_timeout",
                        "error": "direct TS OCR exhausted the component timeout before MP4 fallback",
                        "direct_clock_roi_fallback": {
                            "status": "not_started",
                            "error_kind": exc.kind,
                            "error": str(exc),
                            "remaining_timeout_seconds": 0.0,
                        },
                    })
                    continue
                try:
                    materialized = materialize_analysis_clip(
                        ffmpeg,
                        segments,
                        component_path,
                        window_start=component.start,
                        window_end=component.end,
                        anchor=(component.start + component.end) / 2.0,
                        coverage=_component_coverage(component),
                        preserve_resolution=True,
                        timeout_seconds=remaining,
                    )
                    fallback_remaining = float(direct_deadline or 0.0) - time.monotonic()
                    if fallback_remaining <= 0:
                        raise ScoreboardOcrError(
                            "inference_timeout",
                            "MP4 preparation exhausted the remaining direct-OCR fallback timeout",
                            diagnostics={"stage": "direct_clock_roi_fallback"},
                        )
                    located = locate_scoreboard_event(
                        component_path,
                        event_code=job.code,
                        target_score=(None if job.clock_only else job.target_score or None),
                        event_minute=_event_minute(job) or None,
                        event_second=(
                            job.event_second
                            if job.code.upper().strip() in GOAL_LIKE_EVENT_CODES
                            else None
                        ),
                        candidate_start_seconds=component.start,
                        timeout_seconds=fallback_remaining,
                        python_executable=ocr_python,
                        scoreboard_profile=job.scoreboard_profile,
                        clock_only=job.clock_only,
                    )
                    located = dict(located)
                    direct_diagnostics = {
                        "direct_clock_roi": {"status": "fallback", "error_kind": exc.kind, "error": str(exc)},
                    }
                    located.setdefault("diagnostics", {}).update(direct_diagnostics)
                    anchor, interval = _ocr_anchor_or_interval(
                        located, window_start=component.start, window_end=component.end
                    )
                    if anchor is None:
                        raise VisualLocationFailed("ocr_no_target", "MP4 fallback returned no OCR anchor", direct_diagnostics)
                    if interval is not None:
                        located.setdefault("location_kind", "match_clock_minute_interval")
                        located["anchor_seconds"] = round(anchor, 3)
                        located["ocr_anchor_from_interval"] = True
                    else:
                        located["ocr_anchor_from_interval"] = False
                    located["anchor_stream_time"] = anchor
                    located["fragment_index"] = index
                    located["fragment_window"] = {"start_stream_time": component.start, "end_stream_time": component.end}
                    attempts.append({"component_index": index, "window_start": component.start, "window_end": component.end, "result": located.get("location_kind"), "direct_clock_roi_fallback": direct_diagnostics["direct_clock_roi"]})
                    matches.append((located, materialized))
                    continue
                except subprocess.TimeoutExpired as fallback_exc:
                    attempts.append({
                        "component_index": index,
                        "window_start": component.start,
                        "window_end": component.end,
                        "error_kind": "inference_timeout",
                        "error": "FFmpeg timed out while preparing the direct-OCR MP4 fallback",
                        "direct_clock_roi_fallback": {
                            "error_kind": exc.kind,
                            "error": str(exc),
                        },
                    })
                    continue
                except Exception as fallback_exc:
                    attempts.append({"component_index": index, "window_start": component.start, "window_end": component.end, "error_kind": getattr(fallback_exc, "kind", "ocr_component_failed"), "error": str(fallback_exc), "direct_clock_roi_fallback": {"error_kind": exc.kind, "error": str(exc)}})
                    continue
            diagnostics = (
                exc.as_dict()
                if isinstance(exc, ScoreboardOcrError)
                else {"kind": exc.kind, "diagnostics": exc.diagnostics}
            )
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "error_kind": diagnostics.get("kind"),
                "error": str(exc),
                "diagnostics": diagnostics.get("diagnostics"),
            })
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            if direct_input is not None:
                manifest_path.unlink(missing_ok=True)
                remaining = float(direct_deadline or 0.0) - time.monotonic()
                if remaining > 0:
                    try:
                        materialized = materialize_analysis_clip(
                            ffmpeg, segments, component_path,
                            window_start=component.start,
                            window_end=component.end,
                            anchor=(component.start + component.end) / 2.0,
                            coverage=_component_coverage(component),
                            preserve_resolution=True,
                            timeout_seconds=remaining,
                        )
                        fallback_remaining = float(direct_deadline or 0.0) - time.monotonic()
                        if fallback_remaining <= 0:
                            raise TimeoutError("MP4 preparation exhausted the fallback timeout")
                        located = locate_scoreboard_event(
                            component_path, event_code=job.code,
                            target_score=None,
                            event_minute=_event_minute(job) or None,
                            event_second=(
                                job.event_second
                                if job.code.upper().strip() in GOAL_LIKE_EVENT_CODES
                                else None
                            ),
                            candidate_start_seconds=component.start,
                            timeout_seconds=fallback_remaining,
                            python_executable=ocr_python,
                            scoreboard_profile=job.scoreboard_profile,
                            clock_only=True,
                        )
                        located = dict(located)
                        anchor, interval = _ocr_anchor_or_interval(
                            located, window_start=component.start, window_end=component.end
                        )
                        if anchor is None:
                            raise ValueError("MP4 fallback returned no OCR anchor")
                        located["anchor_stream_time"] = anchor
                        located["ocr_anchor_from_interval"] = interval is not None
                        located["fragment_index"] = index
                        located["fragment_window"] = {
                            "start_stream_time": component.start,
                            "end_stream_time": component.end,
                        }
                        located.setdefault("diagnostics", {})["direct_clock_roi"] = {
                            "status": "fallback",
                            "error_kind": "ocr_component_failed",
                            "error": str(exc),
                        }
                        attempts.append({
                            "component_index": index,
                            "window_start": component.start,
                            "window_end": component.end,
                            "result": located.get("location_kind"),
                            "direct_clock_roi_fallback": located["diagnostics"]["direct_clock_roi"],
                        })
                        matches.append((located, materialized))
                        continue
                    except subprocess.TimeoutExpired as fallback_exc:
                        attempts.append({
                            "component_index": index,
                            "window_start": component.start,
                            "window_end": component.end,
                            "error_kind": "inference_timeout",
                            "error": "FFmpeg timed out while preparing the direct-OCR MP4 fallback",
                            "direct_clock_roi_fallback": {
                                "error_kind": "ocr_component_failed",
                                "error": str(exc),
                            },
                        })
                        continue
                    except Exception as fallback_exc:
                        attempts.append({
                            "component_index": index,
                            "window_start": component.start,
                            "window_end": component.end,
                            "error_kind": getattr(fallback_exc, "kind", "ocr_component_failed"),
                            "error": str(fallback_exc),
                            "direct_clock_roi_fallback": {
                                "error_kind": "ocr_component_failed",
                                "error": str(exc),
                            },
                        })
                        continue
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "error_kind": "ocr_component_failed",
                "error": str(exc),
            })

    exact = [
        item for item in matches
        if item[0].get("location_kind") == "match_clock_second"
    ]
    selected_pool = exact or matches
    if len(selected_pool) > 1:
        target_clock = _clock_text_from_seconds(job.event_second)
        raise VisualLocationFailed(
            "ocr_ambiguous",
            "the requested match clock appeared in multiple video fragments",
            {
                "stage": "ocr_target_localization",
                "target_clock": target_clock,
                "matching_fragment_count": len(selected_pool),
                "fragment_attempts": attempts,
            },
        )
    if not selected_pool:
        last = attempts[-1] if attempts else {}
        diagnostics = dict(last.get("diagnostics") or {})
        raise VisualLocationFailed(
            str(last.get("error_kind") or "ocr_target_localization_failed"),
            str(last.get("error") or "OCR could not locate the requested match clock"),
            {
                **diagnostics,
                "stage": "ocr_target_localization",
                "fragment_attempts": attempts,
                "target_clock": _clock_text_from_seconds(job.event_second),
            },
        )
    located, materialized = selected_pool[0]
    located["fragment_attempts"] = attempts
    located["ocr_clock_only"] = job.clock_only
    return (
        located,
        materialized,
        [
            str(segment.path.resolve())
            for component in components
            for segment in component.segments
        ],
    )


def _fail_artifact(
    runtime: Any,
    job: VisionJob,
    artifact_kind: str,
    *,
    error_kind: str,
    stage: str,
    message: str,
    diagnostics: dict[str, Any] | None = None,
) -> None:
    current = _artifact_task(runtime, job.event_key, artifact_kind)
    if current is None or current.status in {"encoded", "failed"}:
        return
    result = _vision_failure_result(
        error_kind,
        message,
        failure_stage=stage,
        artifact_kind=artifact_kind,
        **(diagnostics or {}),
    )
    _artifact_transition(
        runtime,
        job.event_key,
        artifact_kind,
        "failed",
        result=result,
        error=message,
        error_kind=error_kind,
    )


def _process_ocr_window(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    search_before: float,
    search_after: float,
    size_reference_bytes: int,
    ocr_python: Path,
    ocr_timeout_seconds: float,
    width: int,
    fps: float,
    colors: int,
    min_degraded_seconds: float,
    cancel_event: Any,
    progressive_scan: bool = True,
) -> bool:
    artifact_kind = "ocr_window"
    current = _artifact_task(runtime, job.event_key, artifact_kind)
    if current is None or current.status in {"encoded", "failed"}:
        return True
    if time.time() < current.next_attempt_at_unix:
        return False
    lease_id: str | None = None
    try:
        scope_error = _v1_clock_scope_error(job)
        if scope_error is not None:
            kind, message = scope_error
            raise VisualLocationFailed(
                kind,
                message,
                {
                    "stage": "ocr_target_localization",
                    "event_minute": _event_minute(job) or None,
                },
            )
        if current.status == "located" and current.located_anchor_stream_time is not None:
            located = dict(current.result)
            anchor = float(current.located_anchor_stream_time)
            allowed_sources = {"exact_second", "minute_boundary"}
            if not job.clock_only:
                allowed_sources.add("score_transition")
            if str(located.get("localization_source") or "") not in allowed_sources:
                raise VisualLocationFailed(
                    "ocr_anchor_unverified",
                    "persisted OCR anchor was not verified by a match-clock second or minute boundary",
                    {
                        "stage": "ocr_anchor_validation",
                        "localization_source": located.get("localization_source"),
                        "location_kind": located.get("location_kind"),
                    },
                )
        else:
            segments = segment_reader()
            retained = sorted(
                (
                    segment
                    for segment in segments
                    if Path(segment.path).is_file()
                ),
                key=lambda segment: (float(segment.start), float(segment.end)),
            )
            earliest_start = (
                min(float(segment.start) for segment in retained)
                if retained
                else None
            )
            latest_end = (
                max(float(segment.end) for segment in retained)
                if retained
                else None
            )
            now_unix = time.time()
            state = _ocr_progressive_state(current)
            cursor = state.get("scan_cursor_stream_time")
            intended_initial_start = max(
                0.0,
                job.api_observed_stream_time
                - OCR_PROGRESSIVE_INITIAL_LOOKBACK_SECONDS,
            )
            first_scan_start = max(
                intended_initial_start,
                float(current.search_start_stream_time),
            )
            window_start = (
                max(
                    first_scan_start,
                    float(cursor) - OCR_PROGRESSIVE_OVERLAP_SECONDS,
                )
                if isinstance(cursor, (int, float))
                else first_scan_start
            )
            if latest_end is None:
                window_end = window_start
            elif isinstance(cursor, (int, float)):
                window_end = min(
                    float(latest_end),
                    float(cursor) + OCR_PROGRESSIVE_INCREMENT_SECONDS,
                )
            else:
                # The first pass is the persisted API observation window. Any
                # later media is picked up by cursor-based incremental passes.
                window_end = min(
                    float(latest_end),
                    float(current.search_end_stream_time),
                )
            history_evicted = bool(
                float(current.search_start_stream_time)
                > intended_initial_start + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                or (
                    earliest_start is not None
                    and earliest_start
                    > intended_initial_start + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                )
            )
            latest_trusted = state.get("latest_trusted_clock_seconds")
            if not isinstance(latest_trusted, int):
                latest_trusted = None
            target_clock_seconds = _ocr_progressive_target_seconds(job)
            deadline_policy = _ocr_deadline_policy(
                current,
                now_unix=now_unix,
                target_clock_seconds=target_clock_seconds,
                latest_trusted_clock_seconds=latest_trusted,
            )
            target_deadline = float(deadline_policy["target_deadline_at_unix"])
            deadline_reached = now_unix >= target_deadline
            final_scan_completed = bool(state.get("final_scan_completed_at_unix"))
            force_final_scan = False

            if latest_end is None:
                if deadline_reached:
                    raise VisualLocationFailed(
                        "ocr_buffer_never_available",
                        "no retained source-video segments became available before the OCR deadline",
                        {
                            "stage": "buffer_coverage",
                            "scan_start_stream_time": window_start,
                            "deadline_policy": deadline_policy,
                        },
                    )
                _ocr_progressive_wait(
                    runtime,
                    job,
                    current,
                    wait_kind="waiting_for_clock_target",
                    message="waiting for retained video before starting OCR clock scanning",
                    scan_start=window_start,
                    scan_end=window_end,
                    latest_trusted_clock_seconds=latest_trusted,
                    history_evicted=history_evicted,
                    now_unix=now_unix,
                )
                return False
            no_new_media = bool(
                isinstance(cursor, (int, float))
                and latest_end
                <= float(cursor) + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
            )
            no_scannable_window = bool(
                latest_end <= window_start + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
            )
            if (
                deadline_reached
                and not final_scan_completed
                and bool(deadline_policy.get("slot_acquired"))
                and retained
                and (no_new_media or no_scannable_window)
            ):
                final_scan_start = max(
                    float(earliest_start or 0.0),
                    float(latest_end) - max(3.0, OCR_PROGRESSIVE_OVERLAP_SECONDS),
                )
                if latest_end - final_scan_start >= 3.0:
                    window_start = final_scan_start
                    window_end = float(latest_end)
                    force_final_scan = True
                    no_new_media = False
                    no_scannable_window = False
            if no_new_media:
                if deadline_reached:
                    if latest_trusted is None:
                        error_kind = "ocr_no_trustworthy_clock_before_deadline"
                        message = "OCR never produced a trustworthy match-clock reading before the deadline"
                    elif history_evicted:
                        error_kind = "ocr_search_history_evicted"
                        message = "required OCR search history was evicted before the target clock was located"
                    elif (
                        target_clock_seconds is not None
                        and latest_trusted < target_clock_seconds
                    ):
                        error_kind = "ocr_clock_target_timeout"
                        message = "the retained video never reached the requested match clock before the deadline"
                    else:
                        error_kind = "ocr_clock_target_not_located"
                        message = "OCR passed the requested clock but could not verify a usable anchor"
                    raise VisualLocationFailed(
                        error_kind,
                        message,
                        {
                            "stage": "ocr_progressive_scan",
                            "latest_trusted_clock_seconds": latest_trusted,
                            "target_clock_seconds": target_clock_seconds,
                            "history_evicted": history_evicted,
                            "deadline_policy": deadline_policy,
                            "final_scan_completed": final_scan_completed,
                        },
                    )
                _ocr_progressive_wait(
                    runtime,
                    job,
                    current,
                    wait_kind="waiting_for_clock_target",
                    message="waiting for new video beyond the persisted OCR scan cursor",
                    scan_start=window_start,
                    scan_end=window_end,
                    latest_trusted_clock_seconds=latest_trusted,
                    history_evicted=history_evicted,
                    now_unix=now_unix,
                )
                return False
            if no_scannable_window:
                if deadline_reached:
                    raise VisualLocationFailed(
                        "ocr_buffer_never_available",
                        "the retained video did not cover the initial OCR search window before the deadline",
                        {
                            "stage": "buffer_coverage",
                            "scan_start_stream_time": window_start,
                            "latest_media_end": latest_end,
                            "deadline_policy": deadline_policy,
                            "final_scan_completed": final_scan_completed,
                        },
                    )
                _ocr_progressive_wait(
                    runtime,
                    job,
                    current,
                    wait_kind="waiting_for_clock_target",
                    message="waiting for retained video to cover the OCR search start",
                    scan_start=window_start,
                    scan_end=window_end,
                    latest_trusted_clock_seconds=latest_trusted,
                    history_evicted=history_evicted,
                    now_unix=now_unix,
                )
                return False
            leased_paths = [
                str(segment.path.resolve())
                for segment in retained
                if float(segment.end) > window_start
                and float(segment.start) < window_end
            ]
            if not leased_paths:
                raise VisualLocationFailed(
                    "buffer_history_missing",
                    "the OCR search window has no retained source-video segments",
                    {
                        "stage": "buffer_coverage",
                        "window_start_stream_time": window_start,
                        "window_end_stream_time": window_end,
                    },
                )
            lease_id = runtime.store.acquire_segment_lease(
                job.event_key,
                leased_paths,
                artifact_kind=artifact_kind,
                owner="ocr-window-locator",
                ttl_seconds=max(ocr_timeout_seconds + 60.0, 180.0),
            )
            scan_state = {
                **state,
                "state": "final_target_scan" if force_final_scan else "ocr_execution",
                "deadline_policy": deadline_policy,
                "execution_started_at_unix": now_unix,
            }
            if force_final_scan:
                scan_state["final_scan_started_at_unix"] = now_unix
                scan_state["final_scan_window"] = {
                    "start_stream_time": round(window_start, 3),
                    "end_stream_time": round(window_end, 3),
                }
            _artifact_transition(
                runtime,
                job.event_key,
                artifact_kind,
                "locating",
                window_metadata={"progressive_scan": scan_state},
            )
            analysis_path = (
                output_dir
                / "vision_candidates"
                / f"{job.event_key.rsplit(':', 1)[-1][:8]}.ocr.mp4"
            )
            try:
                located, materialized, _paths = _locate_ocr_window_across_components(
                    job,
                    segments,
                    window_start=window_start,
                    window_end=window_end,
                    analysis_path=analysis_path,
                    ffmpeg=ffmpeg,
                    ocr_python=ocr_python,
                    ocr_timeout_seconds=ocr_timeout_seconds,
                    minimum_component_seconds=max(3.0, min_degraded_seconds),
                )
            except VisualLocationFailed as exc:
                if (
                    not progressive_scan
                    or exc.kind not in OCR_PROGRESSIVE_SCAN_MISS_KINDS
                ):
                    raise
                scanned_trusted = _latest_trusted_clock_seconds(exc.diagnostics)
                if latest_trusted is not None:
                    scanned_trusted = max(
                        latest_trusted,
                        scanned_trusted
                        if scanned_trusted is not None
                        else latest_trusted,
                    )
                target_not_reached = bool(
                    target_clock_seconds is not None
                    and (
                        scanned_trusted is None
                        or scanned_trusted < target_clock_seconds
                    )
                )
                after_scan_unix = time.time()
                updated_deadline_policy = _ocr_deadline_policy(
                    current,
                    now_unix=after_scan_unix,
                    target_clock_seconds=target_clock_seconds,
                    latest_trusted_clock_seconds=scanned_trusted,
                )
                effective_deadline_reached = (
                    after_scan_unix
                    >= float(updated_deadline_policy["target_deadline_at_unix"])
                )
                if history_evicted and not target_not_reached:
                    raise VisualLocationFailed(
                        "ocr_search_history_evicted",
                        "required OCR search history was evicted before the target clock was located",
                        {
                            **exc.diagnostics,
                            "stage": "ocr_progressive_scan",
                            "last_scan_error_kind": exc.kind,
                            "latest_trusted_clock_seconds": scanned_trusted,
                            "target_clock_seconds": target_clock_seconds,
                            "history_evicted": True,
                        },
                    ) from exc
                if effective_deadline_reached:
                    if scanned_trusted is None:
                        error_kind = "ocr_no_trustworthy_clock_before_deadline"
                        message = "OCR never produced a trustworthy match-clock reading before the deadline"
                    elif target_not_reached:
                        error_kind = "ocr_clock_target_timeout"
                        message = "the retained video never reached the requested match clock before the deadline"
                    else:
                        error_kind = "ocr_clock_target_not_located"
                        message = "OCR passed the requested clock but could not verify a usable anchor"
                    raise VisualLocationFailed(
                        error_kind,
                        message,
                        {
                            **exc.diagnostics,
                            "stage": "ocr_progressive_scan",
                            "last_scan_error_kind": exc.kind,
                            "latest_trusted_clock_seconds": scanned_trusted,
                            "target_clock_seconds": target_clock_seconds,
                            "history_evicted": history_evicted,
                            "deadline_policy": updated_deadline_policy,
                            "final_scan_completed_at_unix": after_scan_unix,
                            "final_scan_was_forced": force_final_scan,
                        },
                    ) from exc
                _ocr_progressive_wait(
                    runtime,
                    job,
                    current,
                    wait_kind="waiting_for_clock_target",
                    message=(
                        "latest trustworthy OCR clock has not reached the target"
                        if target_not_reached
                        else "target clock has not yet yielded a verified OCR anchor"
                    ),
                    scan_start=window_start,
                    scan_end=window_end,
                    latest_trusted_clock_seconds=scanned_trusted,
                    history_evicted=history_evicted,
                    diagnostics={
                        "kind": exc.kind,
                        "message": str(exc),
                        **exc.diagnostics,
                    },
                    now_unix=after_scan_unix,
                )
                return False
            anchor = float(located["anchor_stream_time"])
            before, after, localization_source = _ocr_output_shape(job, located)
            anchor_source = (
                job.api_observed_source_time
                + (anchor - job.api_observed_stream_time)
                if job.api_observed_source_time is not None
                else None
            )
            locate_result = {
                "artifact_kind": artifact_kind,
                "stage": "ocr_target_localized",
                "anchor_stream_time": anchor,
                "anchor_source_time": anchor_source,
                "locator_method": located.get("method"),
                "localization_source": localization_source,
                "location_kind": located.get("location_kind"),
                "precision": located.get("precision"),
                "target_clock": located.get("target_clock"),
                "target_clock_seconds": located.get("target_clock_seconds"),
                "exact_second_error": located.get("exact_second_error"),
                "localization_quality": located.get("localization_quality"),
                "degraded": bool(located.get("degraded")),
                "degradation_mode": located.get("degradation_mode"),
                "degradation_reason": located.get("degradation_reason"),
                "clip_before_seconds": before,
                "clip_after_seconds": after,
                "ocr_clock_only": job.clock_only,
                "ocr": located,
                "search_window": materialized,
                "fragment_attempts": located.get("fragment_attempts", []),
                "anchor_provenance": "ocr_verified_match_clock",
                "progressive_status": "target_located",
                "default_gif_preserved": True,
            }
            before, after = _normalized_ocr_clip_window(
                job,
                locate_result,
                stage="ocr_location_persistence",
            )
            locate_result["clip_before_seconds"] = before
            locate_result["clip_after_seconds"] = after
            located_trusted = _latest_trusted_clock_seconds(located)
            progress = {
                **state,
                "state": "target_located",
                "scan_attempt_count": int(state.get("scan_attempt_count") or 0) + 1,
                "last_scan_start_stream_time": round(window_start, 3),
                "last_scan_end_stream_time": round(window_end, 3),
                "scan_cursor_stream_time": round(window_end, 3),
                "overlap_seconds": OCR_PROGRESSIVE_OVERLAP_SECONDS,
                "latest_trusted_clock_seconds": (
                    max(latest_trusted, located_trusted)
                    if latest_trusted is not None and located_trusted is not None
                    else latest_trusted
                    if latest_trusted is not None
                    else located_trusted
                ),
                "target_clock_seconds": target_clock_seconds,
                "anchor_stream_time": round(anchor, 3),
                "anchor_provenance": "ocr_verified_match_clock",
                "location_kind": located.get("location_kind"),
                "history_evicted": history_evicted,
                "deadline_policy": {
                    **deadline_policy,
                    "phase": "target_located",
                    "target_located_at_unix": time.time(),
                },
                "last_execution_completed_at_unix": time.time(),
                "last_execution_result": "target_located",
            }
            if force_final_scan:
                progress["final_scan_completed_at_unix"] = time.time()
                progress["final_scan_result"] = "target_located"
            _artifact_transition(
                runtime,
                job.event_key,
                artifact_kind,
                "located",
                result=locate_result,
                window_metadata={"progressive_scan": progress},
            )
            located = locate_result
            if lease_id:
                runtime.store.release_segment_lease(lease_id)
                lease_id = None

        before, after = _normalized_ocr_clip_window(
            job,
            located,
            stage="ocr_output_window_validation",
        )
        # Persist recovered legacy null/missing fields before encoding so a
        # second restart observes the same normalized window.
        located["clip_before_seconds"] = before
        located["clip_after_seconds"] = after
        segments = segment_reader()
        requested_start = max(0.0, anchor - before)
        requested_end = anchor + after
        current = _artifact_task(runtime, job.event_key, artifact_kind)
        assert current is not None
        now_unix = time.time()
        latest_media_end = max(
            (float(segment.end) for segment in segments),
            default=None,
        )
        required_postroll_wait = (
            max(0.0, requested_end - latest_media_end)
            if latest_media_end is not None
            else max(0.0, after)
        )
        postroll_policy = _ocr_postroll_deadline_policy(
            current,
            now_unix=now_unix,
            required_wait_seconds=required_postroll_wait,
        )
        postroll_deadline = float(postroll_policy["postroll_deadline_at_unix"])
        deadline_reached = now_unix >= postroll_deadline
        coverage = analyze_video_coverage(
            segments,
            window_start=requested_start,
            window_end=requested_end,
            anchor=anchor,
            allow_degraded=False,
            min_degraded_seconds=min_degraded_seconds,
        )
        if coverage.status == CoverageStatus.WAITING:
            wait_kind = (
                "waiting_for_postroll"
                if located.get("localization_source") == "exact_second"
                and after > 0
                else "waiting_for_ocr_output_window"
            )
            if deadline_reached:
                raise VisualLocationFailed(
                    (
                        "ocr_postroll_timeout"
                        if wait_kind == "waiting_for_postroll"
                        else "ocr_output_window_timeout"
                    ),
                    coverage.reason,
                    {
                        "stage": "ocr_output_coverage",
                        "anchor_stream_time": anchor,
                        "requested_start_stream_time": requested_start,
                        "requested_end_stream_time": requested_end,
                        "localization_source": located.get("localization_source"),
                        "deadline_policy": postroll_policy,
                    },
                )
            progress = {
                **_ocr_progressive_state(current),
                "state": wait_kind,
                "anchor_stream_time": round(anchor, 3),
                "anchor_provenance": "ocr_verified_match_clock",
                "requested_output_start_stream_time": round(requested_start, 3),
                "requested_output_end_stream_time": round(requested_end, 3),
                "deadline_policy": postroll_policy,
            }
            _artifact_readiness_wait(
                runtime,
                job.event_key,
                artifact_kind,
                coverage.reason,
                error_kind=wait_kind,
                result={
                    "stage": wait_kind,
                    "progressive_status": wait_kind,
                    "default_gif_preserved": True,
                },
                window_metadata={"progressive_scan": progress},
                deadline_at_unix=postroll_deadline,
                now=now_unix,
            )
            return False
        if coverage.status == CoverageStatus.UNAVAILABLE:
            unavailable_kind = _normalized_buffer_error_kind(coverage.error_kind)
            raise VisualLocationFailed(
                (
                    "ocr_output_history_evicted"
                    if unavailable_kind == "buffer_history_missing"
                    else "ocr_output_video_gap"
                    if unavailable_kind == "buffer_gap"
                    else unavailable_kind
                ),
                coverage.reason,
                {
                    "stage": "ocr_output_coverage",
                    "anchor_stream_time": anchor,
                    "requested_start_stream_time": requested_start,
                    "requested_end_stream_time": requested_end,
                    "coverage_error_kind": coverage.error_kind,
                },
            )
        lease_id = runtime.store.acquire_segment_lease(
            job.event_key,
            [str(segment.path.resolve()) for segment in coverage.segments],
            artifact_kind=artifact_kind,
            owner="ocr-window-encoder",
            ttl_seconds=max(ocr_timeout_seconds + 60.0, 180.0),
        )
        latest_task = runtime.store.get(job.event_key)
        if latest_task is None:
            raise KeyError(f"unknown event task: {job.event_key}")
        pending = PendingEvent(
            event_type=f"{job.event_type}_ocr_window",
            stream_time=anchor,
            source_time=located.get("anchor_source_time"),
            detected_wall_time=job.detected_at_unix,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=requested_end,
            output_id=job.event_key.rsplit(":", 1)[-1][:8],
        )
        _artifact_transition(
            runtime,
            job.event_key,
            artifact_kind,
            "encoding",
            result=located,
        )
        encoded = encode_gif(
            ffmpeg,
            ffprobe,
            segments,
            pending,
            output_dir,
            before=before,
            after=after,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            cancel_event=cancel_event,
            coverage=coverage,
            output_filename=build_gif_filename(
                match_id=latest_task.match_id,
                event_data=latest_task.event_data,
                variant="ocr",
            ),
        )
        encoded.update({
            **located,
            "artifact_kind": artifact_kind,
            "stage": "ocr_window_encoded",
            "progressive_status": "encoded",
            "output_kind": (
                "minute_range_fallback"
                if located.get("localization_source") == "minute_boundary"
                else "ocr_window"
            ),
            "minute_fallback": located.get("localization_source") == "minute_boundary",
            "fallback_generated": located.get("localization_source") == "minute_boundary",
            "precise_location": (
                located.get("localization_source") == "exact_second"
                and located.get("localization_quality") == "exact"
            ),
            "output_width": width,
            "output_fps": fps,
            "output_colors": colors,
            "requested_media_window": {
                "start_stream_time": requested_start,
                "end_stream_time": requested_end,
            },
            "requested_match_clock_window": {
                "start": _clock_text_from_seconds(
                    max(
                        0,
                        int(located.get("target_clock_seconds")) - int(before),
                    )
                ) if located.get("target_clock_seconds") is not None else None,
                "end": _clock_text_from_seconds(
                    int(located.get("target_clock_seconds")) + int(after)
                ) if located.get("target_clock_seconds") is not None else None,
            },
            "actual_media_window": {
                "start_stream_time": coverage.effective_start,
                "end_stream_time": coverage.effective_end,
                "coverage_status": coverage.status.value,
                "coverage_reason": coverage.reason,
            },
            "default_gif_preserved": True,
        })
        _artifact_transition(
            runtime,
            job.event_key,
            artifact_kind,
            "encoded",
            result=encoded,
            window_metadata={
                "progressive_scan": {
                    **_ocr_progressive_state(current),
                    "state": "encoded",
                    "requested_output_start_stream_time": round(
                        requested_start, 3
                    ),
                    "requested_output_end_stream_time": round(
                        requested_end, 3
                    ),
                }
            },
        )
        return True
    except VisualLocationFailed as exc:
        _fail_artifact(
            runtime,
            job,
            artifact_kind,
            error_kind=exc.kind,
            stage=str(exc.diagnostics.get("stage") or "ocr_target_localization"),
            message=str(exc),
            diagnostics=exc.diagnostics,
        )
        return True
    except Exception as exc:
        current = _artifact_task(runtime, job.event_key, artifact_kind)
        stage = "ocr_window_encoding" if current and current.status == "encoding" else "ocr_clock_discovery"
        _fail_artifact(
            runtime,
            job,
            artifact_kind,
            error_kind="ocr_window_encoding_failed" if stage == "ocr_window_encoding" else "ocr_processing_failed",
            stage=stage,
            message=str(exc),
        )
        return True
    finally:
        if lease_id:
            runtime.store.release_segment_lease(lease_id)


def _encode_tdeed_minute_fallback(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    ocr_task: Any,
    ocr_result: dict[str, Any],
    failure_kind: str,
    failure_stage: str,
    failure_message: str,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    timeout_seconds: float,
    min_degraded_seconds: float,
    cancel_event: Any,
) -> bool | None:
    """Encode the OCR-minute clip when T-DEED cannot select an action.

    ``None`` means this event has no usable OCR minute anchor and the caller
    should retain the normal terminal failure. ``False`` means video coverage
    is still being accumulated and the caller should retry later.
    """
    localization_source = str(ocr_result.get("localization_source") or "")
    location_kind = str(ocr_result.get("location_kind") or "")
    is_minute_result = localization_source == "minute_boundary" or location_kind in {
        "match_clock_minute_boundary",
        "match_clock_minute_interval",
    }
    if not is_minute_result:
        return None
    try:
        anchor = float(
            ocr_task.located_anchor_stream_time
            if ocr_task.located_anchor_stream_time is not None
            else ocr_result.get("anchor_stream_time")
        )
    except (TypeError, ValueError):
        return None
    if not math.isfinite(anchor):
        return None

    before = OCR_MINUTE_FALLBACK_BEFORE_SECONDS
    after = OCR_MINUTE_FALLBACK_AFTER_SECONDS
    requested_start = max(0.0, anchor - before)
    requested_end = anchor + after
    segments = segment_reader()
    current = _artifact_task(runtime, job.event_key, "tdeed_refined")
    if current is None:
        return None
    coverage = _vision_coverage(
        segments,
        window_start=requested_start,
        window_end=requested_end,
        anchor=anchor,
        deadline_reached=time.time() >= current.deadline_at_unix,
        min_degraded_seconds=min_degraded_seconds,
    )
    if coverage.status == CoverageStatus.WAITING:
        if current.status == "locating":
            _artifact_transition(
                runtime,
                job.event_key,
                "tdeed_refined",
                "pending",
                result={
                    "fallback_pending": True,
                    "fallback_pending_reason": coverage.reason,
                },
                reason="minute_fallback_waiting_for_video",
            )
        _artifact_readiness_wait(
            runtime,
            job.event_key,
            "tdeed_refined",
            coverage.reason,
            error_kind=coverage.error_kind or "waiting_for_video",
        )
        return False
    if coverage.status == CoverageStatus.UNAVAILABLE:
        return None

    anchor_source = (
        job.api_observed_source_time
        + (anchor - job.api_observed_stream_time)
        if job.api_observed_source_time is not None
        else None
    )
    available_seconds = None
    if coverage.effective_start is not None and coverage.effective_end is not None:
        available_seconds = max(
            0.0,
            float(coverage.effective_end) - float(coverage.effective_start),
        )
    requested_seconds = before + after
    fallback_complete = (
        available_seconds is not None
        and available_seconds >= requested_seconds * OCR_MINUTE_FALLBACK_COMPLETE_RATIO
    )
    failure_reason = {
        "kind": failure_kind,
        "stage": failure_stage,
        "message": failure_message,
        "ocr_anchor_found": True,
        "ocr_location_kind": location_kind or localization_source,
        "tdeed_failed": True,
        "default_gif_preserved": True,
    }
    located = {
        "artifact_kind": "tdeed_refined",
        "stage": "minute_fallback_ready",
        "anchor_stream_time": anchor,
        "anchor_source_time": anchor_source,
        "confidence": None,
        "model_name": "PaddleOCR",
        "model_version": "scoreboard-clock-v2",
        "model_weights_sha256": None,
        "model_label": "scoreboard clock minute fallback",
        "locator_method": "ocr_minute_fallback_after_tdeed",
        "fallback_used": True,
        "minute_fallback": True,
        "fallback_generated": True,
        "precise_location": False,
        "localization_source": "minute_boundary",
        "localization_quality": "degraded",
        "degraded": True,
        "degradation_mode": "minute_range_fallback",
        "degradation_reason": failure_reason,
        "error_kind": failure_kind,
        "failure_reason": failure_reason,
        "tdeed_error_kind": failure_kind,
        "tdeed_error": failure_message,
        "clip_before_seconds": before,
        "clip_after_seconds": after,
        "requested_fallback_seconds": requested_seconds,
        "available_fallback_seconds": available_seconds,
        "fallback_complete": bool(fallback_complete),
        "fallback_label": (
            "60_second_fallback" if fallback_complete else "fragmented_clip"
        ),
        "ocr": ocr_result.get("ocr") or ocr_result,
        "ocr_error": ocr_result.get("ocr_error"),
        "ocr_clock_only": bool(ocr_result.get("ocr_clock_only")),
        "source_ocr_artifact": {
            "output": ocr_task.output_path,
            "status": ocr_task.status,
            "anchor_stream_time": anchor,
            "localization_source": localization_source,
            "localization_quality": ocr_result.get("localization_quality"),
            "degraded": bool(ocr_result.get("degraded")),
        },
        "default_gif_preserved": True,
    }
    if current.status == "pending":
        _artifact_transition(
            runtime,
            job.event_key,
            "tdeed_refined",
            "locating",
            reason="minute_fallback_after_tdeed_failure",
        )
        current = _artifact_task(runtime, job.event_key, "tdeed_refined")
    if current is None:
        return None
    if current.status == "locating":
        _artifact_transition(
            runtime,
            job.event_key,
            "tdeed_refined",
            "located",
            result=located,
            reason="minute_fallback_anchor_ready",
        )
    elif current.status != "located":
        return None

    leased_paths = [str(segment.path.resolve()) for segment in coverage.segments]
    lease_id = runtime.store.acquire_segment_lease(
        job.event_key,
        leased_paths,
        artifact_kind="tdeed_refined",
        owner="tdeed-minute-fallback-encoder",
        ttl_seconds=max(timeout_seconds + 60.0, 180.0),
    )
    try:
        latest_task = runtime.store.get(job.event_key)
        if latest_task is None:
            raise KeyError(f"unknown vision task: {job.event_key}")
        pending = PendingEvent(
            event_type=f"{job.event_type}_tdeed_refined",
            stream_time=anchor,
            source_time=anchor_source,
            detected_wall_time=job.detected_at_unix,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=requested_end,
            output_id=job.event_key.rsplit(":", 1)[-1][:8],
        )
        _artifact_transition(
            runtime,
            job.event_key,
            "tdeed_refined",
            "encoding",
            result=located,
            reason="minute_fallback_encoding",
        )
        encoded = encode_gif(
            ffmpeg,
            ffprobe,
            segments,
            pending,
            output_dir,
            before=before,
            after=after,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            cancel_event=cancel_event,
            coverage=coverage,
            output_filename=build_gif_filename(
                match_id=latest_task.match_id,
                event_data=latest_task.event_data,
                variant="ai",
            ),
        )
        encoded.update({
            **located,
            "stage": "minute_fallback_encoded",
            "output_kind": "minute_range_fallback",
            "requested_media_window": {
                "start_stream_time": requested_start,
                "end_stream_time": requested_end,
            },
            "actual_media_window": {
                "start_stream_time": coverage.effective_start,
                "end_stream_time": coverage.effective_end,
                "coverage_status": coverage.status.value,
                "coverage_reason": coverage.reason,
            },
            "output_width": width,
            "output_fps": fps,
            "output_colors": colors,
        })
        _artifact_transition(
            runtime,
            job.event_key,
            "tdeed_refined",
            "encoded",
            result=encoded,
            reason="minute_fallback_encoded",
        )
        return True
    finally:
        runtime.store.release_segment_lease(lease_id)


def _process_tdeed_refined(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    search_before: float,
    refined_before: float,
    refined_after: float,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    python: Path,
    timeout_seconds: float,
    min_degraded_seconds: float,
    cancel_event: Any,
) -> bool:
    artifact_kind = "tdeed_refined"
    current = _artifact_task(runtime, job.event_key, artifact_kind)
    if current is None or current.status in {"encoded", "failed"}:
        return True
    if time.time() < current.next_attempt_at_unix:
        return False
    ocr_task = _artifact_task(runtime, job.event_key, "ocr_window")
    if ocr_task is None:
        return False
    if ocr_task.status not in {"encoded", "failed"}:
        return False

    upstream_ocr_failure = (
        dict(ocr_task.result.get("failure_reason") or {})
        if ocr_task.status == "failed"
        else None
    )
    if ocr_task.status == "failed" and not upstream_ocr_failure:
        upstream_ocr_failure = {
            "kind": ocr_task.last_error_kind or "ocr_processing_failed",
            "message": ocr_task.error or "OCR artifact failed",
            "stage": ocr_task.failure_stage or "ocr_processing",
        }
    if ocr_task.status == "failed":
        # OCR failure is a recoverable upstream signal. T-DEED gets one
        # independent attempt over the original persisted search interval.
        # Only a subsequent T-DEED failure makes the refined artifact terminal.
        pass

    ocr_result: dict[str, Any] = (
        dict(ocr_task.result) if ocr_task.status == "encoded" else {}
    )
    lease_id: str | None = None
    try:
        if current.status == "located" and current.located_anchor_stream_time is not None:
            refined_anchor = float(current.located_anchor_stream_time)
            located = dict(current.result)
        else:
            if ocr_task.status == "encoded":
                ocr_anchor = float(ocr_task.located_anchor_stream_time)
                ocr_before, ocr_after = _normalized_ocr_clip_window(
                    job,
                    ocr_result,
                    stage="tdeed_ocr_window_validation",
                )
                minute_based = ocr_result.get("localization_source") == "minute_boundary"
                # A minute reading and its OCR artifact both cover the minute
                # immediately before the OCR-derived boundary.
                candidate_before = (
                    OCR_TDEED_MINUTE_BEFORE_SECONDS if minute_based else ocr_before
                )
                candidate_after = (
                    OCR_TDEED_MINUTE_AFTER_SECONDS if minute_based else ocr_after
                )
                window_start = max(0.0, ocr_anchor - candidate_before)
                window_end = ocr_anchor + candidate_after
            else:
                window_start = max(0.0, float(current.search_start_stream_time))
                window_end = float(current.search_end_stream_time)
                if window_end <= window_start:
                    window_start = max(0.0, job.api_observed_stream_time - search_before)
                    window_end = job.api_observed_stream_time
                ocr_anchor = min(
                    window_end,
                    max(window_start, job.api_observed_stream_time),
                )
                minute_based = False
            segments = segment_reader()
            coverage = _vision_coverage(
                segments,
                window_start=window_start,
                window_end=window_end,
                anchor=ocr_anchor,
                deadline_reached=time.time() >= current.deadline_at_unix,
                min_degraded_seconds=min_degraded_seconds,
            )
            if coverage.status == CoverageStatus.WAITING:
                _artifact_readiness_wait(
                    runtime,
                    job.event_key,
                    artifact_kind,
                    coverage.reason,
                    error_kind=coverage.error_kind or "waiting_for_video",
                )
                return False
            if coverage.status == CoverageStatus.UNAVAILABLE:
                raise VisualLocationFailed(
                    _normalized_buffer_error_kind(coverage.error_kind),
                    coverage.reason,
                    {"stage": "buffer_coverage"},
                )
            lease_id = runtime.store.acquire_segment_lease(
                job.event_key,
                [str(segment.path.resolve()) for segment in coverage.segments],
                artifact_kind=artifact_kind,
                owner="tdeed-locator",
                ttl_seconds=max(timeout_seconds + 60.0, 180.0),
            )
            _artifact_transition(
                runtime,
                job.event_key,
                artifact_kind,
                "locating",
            )
            analysis_path = (
                output_dir
                / "vision_candidates"
                / f"{job.event_key.rsplit(':', 1)[-1][:8]}.tdeed.mp4"
            )
            materialized = materialize_analysis_clip(
                ffmpeg,
                segments,
                analysis_path,
                window_start=window_start,
                window_end=window_end,
                anchor=ocr_anchor,
                coverage=coverage,
            )
            actual_start = float(materialized["window_start_stream_time"])
            actual_end = float(materialized["window_end_stream_time"])
            expected_anchor = (
                (actual_start + actual_end) / 2.0 if minute_based else ocr_anchor
            )
            started = time.perf_counter()
            tdeed = locate_candidate_video(
                analysis_path,
                job.code,
                expected_offset_seconds=expected_anchor - actual_start,
                threshold=0.2,
                max_anchor_distance_seconds=max(
                    5.0,
                    (actual_end - actual_start) / 2.0 if minute_based else 15.0,
                ),
                timeout_seconds=timeout_seconds,
                python_path=python,
                candidate_window_start_seconds=actual_start,
                candidate_window_end_seconds=actual_end,
            )
            refined_anchor = float(tdeed["anchor_stream_time"])
            if not math.isfinite(refined_anchor) or not actual_start <= refined_anchor <= actual_end:
                raise VisualLocationFailed(
                    "tdeed_candidate_selection_failed",
                    "T-DEED returned an anchor outside the OCR 60-second window",
                    {"stage": "tdeed_candidate_selection"},
                )
            anchor_source = (
                job.api_observed_source_time
                + (refined_anchor - job.api_observed_stream_time)
                if job.api_observed_source_time is not None
                else None
            )
            located = {
                "artifact_kind": artifact_kind,
                "stage": "tdeed_candidate_selected",
                "anchor_stream_time": refined_anchor,
                "anchor_source_time": anchor_source,
                "confidence": tdeed.get("confidence"),
                "inference_seconds": round(time.perf_counter() - started, 3),
                "model_name": "T-DEED",
                "model_version": tdeed.get("model_version"),
                "model_weights_sha256": tdeed.get("checkpoint_sha256"),
                "model_label": tdeed.get("label"),
                "locator_method": (
                    "tdeed_within_ocr_window"
                    if ocr_task.status == "encoded"
                    else "tdeed_after_ocr_failure"
                ),
                "clip_before_seconds": refined_before,
                "clip_after_seconds": refined_after,
                "source_ocr_artifact": {
                    "output": ocr_task.output_path,
                    "status": ocr_task.status,
                    "failure_reason": upstream_ocr_failure,
                    "anchor_stream_time": ocr_anchor,
                    "localization_source": ocr_result.get("localization_source"),
                    "localization_quality": ocr_result.get(
                        "localization_quality"
                    ),
                    "degraded": bool(ocr_result.get("degraded")),
                    "degradation_mode": ocr_result.get("degradation_mode"),
                    "degradation_reason": ocr_result.get(
                        "degradation_reason"
                    ),
                    "exact_second_error": ocr_result.get(
                        "exact_second_error"
                    ),
                    "window_start_stream_time": actual_start,
                    "window_end_stream_time": actual_end,
                },
                "analysis_window": materialized,
                "upstream_ocr_failure": upstream_ocr_failure,
                "default_gif_preserved": True,
            }
            _artifact_transition(
                runtime,
                job.event_key,
                artifact_kind,
                "located",
                result=located,
            )
            if lease_id:
                runtime.store.release_segment_lease(lease_id)
                lease_id = None

        segments = segment_reader()
        requested_start = max(0.0, refined_anchor - refined_before)
        requested_end = refined_anchor + refined_after
        current = _artifact_task(runtime, job.event_key, artifact_kind)
        assert current is not None
        coverage = _vision_coverage(
            segments,
            window_start=requested_start,
            window_end=requested_end,
            anchor=refined_anchor,
            deadline_reached=time.time() >= current.deadline_at_unix,
            min_degraded_seconds=min_degraded_seconds,
        )
        if coverage.status == CoverageStatus.WAITING:
            _artifact_readiness_wait(
                runtime,
                job.event_key,
                artifact_kind,
                coverage.reason,
                error_kind=coverage.error_kind or "waiting_for_video",
            )
            return False
        if coverage.status == CoverageStatus.UNAVAILABLE:
            raise VisualLocationFailed(
                _normalized_buffer_error_kind(coverage.error_kind),
                coverage.reason,
                {"stage": "buffer_coverage"},
            )
        lease_id = runtime.store.acquire_segment_lease(
            job.event_key,
            [str(segment.path.resolve()) for segment in coverage.segments],
            artifact_kind=artifact_kind,
            owner="tdeed-encoder",
            ttl_seconds=max(timeout_seconds + 60.0, 180.0),
        )
        latest_task = runtime.store.get(job.event_key)
        if latest_task is None:
            raise KeyError(f"unknown event task: {job.event_key}")
        pending = PendingEvent(
            event_type=f"{job.event_type}_tdeed_refined",
            stream_time=refined_anchor,
            source_time=located.get("anchor_source_time"),
            detected_wall_time=job.detected_at_unix,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=requested_end,
            output_id=job.event_key.rsplit(":", 1)[-1][:8],
        )
        _artifact_transition(
            runtime,
            job.event_key,
            artifact_kind,
            "encoding",
        )
        encoded = encode_gif(
            ffmpeg,
            ffprobe,
            segments,
            pending,
            output_dir,
            before=refined_before,
            after=refined_after,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            cancel_event=cancel_event,
            coverage=coverage,
            output_filename=build_gif_filename(
                match_id=latest_task.match_id,
                event_data=latest_task.event_data,
                variant="ai",
            ),
        )
        encoded.update({
            **located,
            "artifact_kind": artifact_kind,
            "stage": "tdeed_output_encoded",
            "output_kind": "tdeed_refined_20_second",
            "clip_before_seconds": refined_before,
            "clip_after_seconds": refined_after,
            "output_width": width,
            "output_fps": fps,
            "output_colors": colors,
            "requested_media_window": {
                "start_stream_time": requested_start,
                "end_stream_time": requested_end,
            },
            "actual_media_window": {
                "start_stream_time": coverage.effective_start,
                "end_stream_time": coverage.effective_end,
                "coverage_status": coverage.status.value,
                "coverage_reason": coverage.reason,
            },
            "default_gif_preserved": True,
        })
        _artifact_transition(
            runtime,
            job.event_key,
            artifact_kind,
            "encoded",
            result=encoded,
        )
        return True
    except VisualLocationFailed as exc:
        diagnostics = dict(exc.diagnostics)
        if upstream_ocr_failure is not None:
            diagnostics["upstream_ocr_failure"] = upstream_ocr_failure
        fallback_result = _encode_tdeed_minute_fallback(
            job,
            runtime,
            segment_reader,
            ffmpeg,
            ffprobe,
            output_dir,
            ocr_task=ocr_task,
            ocr_result=ocr_result,
            failure_kind=exc.kind,
            failure_stage=str(
                exc.diagnostics.get("stage") or "tdeed_candidate_selection"
            ),
            failure_message=str(exc),
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            timeout_seconds=timeout_seconds,
            min_degraded_seconds=min_degraded_seconds,
            cancel_event=cancel_event,
        )
        if fallback_result is not None:
            return fallback_result
        _fail_artifact(
            runtime,
            job,
            artifact_kind,
            error_kind=exc.kind,
            stage=str(exc.diagnostics.get("stage") or "tdeed_candidate_selection"),
            message=str(exc),
            diagnostics=diagnostics,
        )
        return True
    except (VisionConfigurationError, VisionCandidateNotFound, VisionInferenceError) as exc:
        kind = _tdeed_error_kind(exc)
        stage = (
            "tdeed_model_unavailable"
            if kind == "tdeed_model_unavailable"
            else "tdeed_candidate_selection"
            if kind == "tdeed_no_candidate"
            else "tdeed_inference"
        )
        fallback_result = _encode_tdeed_minute_fallback(
            job,
            runtime,
            segment_reader,
            ffmpeg,
            ffprobe,
            output_dir,
            ocr_task=ocr_task,
            ocr_result=ocr_result,
            failure_kind=kind,
            failure_stage=stage,
            failure_message=str(exc),
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            timeout_seconds=timeout_seconds,
            min_degraded_seconds=min_degraded_seconds,
            cancel_event=cancel_event,
        )
        if fallback_result is not None:
            return fallback_result
        _fail_artifact(
            runtime,
            job,
            artifact_kind,
            error_kind=kind,
            stage=stage,
            message=str(exc),
            diagnostics=(
                {"upstream_ocr_failure": upstream_ocr_failure}
                if upstream_ocr_failure is not None
                else None
            ),
        )
        return True
    except Exception as exc:
        current = _artifact_task(runtime, job.event_key, artifact_kind)
        stage = "tdeed_output_encoding" if current and current.status == "encoding" else "tdeed_inference"
        _fail_artifact(
            runtime,
            job,
            artifact_kind,
            error_kind="tdeed_output_encoding_failed" if stage == "tdeed_output_encoding" else "tdeed_inference_failed",
            stage=stage,
            message=str(exc),
            diagnostics=(
                {"upstream_ocr_failure": upstream_ocr_failure}
                if upstream_ocr_failure is not None
                else None
            ),
        )
        return True
    finally:
        if lease_id:
            runtime.store.release_segment_lease(lease_id)


def process_vision_artifact(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    artifact_kind: str,
    search_before: float,
    search_after: float,
    refined_before: float,
    refined_after: float,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    python: Path,
    timeout_seconds: float,
    ocr_python: Path = OCR_PYTHON,
    ocr_timeout_seconds: float = 180.0,
    fallback_width: int = OCR_MINUTE_FALLBACK_WIDTH,
    fallback_fps: float = OCR_MINUTE_FALLBACK_FPS,
    fallback_colors: int = OCR_MINUTE_FALLBACK_COLORS,
    min_degraded_seconds: float = MIN_DEGRADED_CLIP_SECONDS,
    cancel_event: Any = None,
    progressive_ocr: bool = True,
) -> bool:
    """Advance exactly one independently persisted visual artifact."""
    if artifact_kind == "ocr_window":
        return _process_ocr_window(
            job,
            runtime,
            segment_reader,
            ffmpeg,
            ffprobe,
            output_dir,
            search_before=search_before,
            search_after=search_after,
            size_reference_bytes=size_reference_bytes,
            ocr_python=ocr_python,
            ocr_timeout_seconds=ocr_timeout_seconds,
            width=fallback_width,
            fps=fallback_fps,
            colors=fallback_colors,
            min_degraded_seconds=min_degraded_seconds,
            cancel_event=cancel_event,
            progressive_scan=progressive_ocr,
        )
    if artifact_kind == "tdeed_refined":
        return _process_tdeed_refined(
            job,
            runtime,
            segment_reader,
            ffmpeg,
            ffprobe,
            output_dir,
            search_before=search_before,
            refined_before=refined_before,
            refined_after=refined_after,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            python=python,
            timeout_seconds=timeout_seconds,
            min_degraded_seconds=min_degraded_seconds,
            cancel_event=cancel_event,
        )
    raise ValueError(f"unsupported vision artifact kind: {artifact_kind!r}")


def refine_event_job(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    search_before: float,
    search_after: float,
    refined_before: float,
    refined_after: float,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    python: Path,
    timeout_seconds: float,
    ocr_python: Path = OCR_PYTHON,
    ocr_timeout_seconds: float = 180.0,
    fallback_width: int = OCR_MINUTE_FALLBACK_WIDTH,
    fallback_fps: float = OCR_MINUTE_FALLBACK_FPS,
    fallback_colors: int = OCR_MINUTE_FALLBACK_COLORS,
    min_degraded_seconds: float = MIN_DEGRADED_CLIP_SECONDS,
    cancel_event: Any = None,
) -> bool:
    """Produce independent OCR-window and T-DEED artifacts for one event."""
    try:
        ocr_task = _artifact_task(runtime, job.event_key, "ocr_window")
    except TypeError:
        ocr_task = None
    if ocr_task is None:
        # Databases/tests created before multi-artifact support retain the
        # established single refined-task behavior until explicitly migrated.
        return _refine_event_job_legacy(
            job,
            runtime,
            segment_reader,
            ffmpeg,
            ffprobe,
            output_dir,
            search_before=search_before,
            search_after=search_after,
            refined_before=refined_before,
            refined_after=refined_after,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            python=python,
            timeout_seconds=timeout_seconds,
            ocr_python=ocr_python,
            ocr_timeout_seconds=ocr_timeout_seconds,
            fallback_width=fallback_width,
            fallback_fps=fallback_fps,
            fallback_colors=fallback_colors,
            min_degraded_seconds=min_degraded_seconds,
            cancel_event=cancel_event,
        )

    ocr_done = process_vision_artifact(
        job,
        runtime,
        segment_reader,
        ffmpeg,
        ffprobe,
        output_dir,
        artifact_kind="ocr_window",
        search_before=search_before,
        search_after=search_after,
        refined_before=refined_before,
        refined_after=refined_after,
        size_reference_bytes=size_reference_bytes,
        python=python,
        timeout_seconds=timeout_seconds,
        ocr_python=ocr_python,
        ocr_timeout_seconds=ocr_timeout_seconds,
        width=width,
        fps=fps,
        colors=colors,
        fallback_width=fallback_width,
        fallback_fps=fallback_fps,
        fallback_colors=fallback_colors,
        min_degraded_seconds=min_degraded_seconds,
        cancel_event=cancel_event,
        progressive_ocr=False,
    )
    if not ocr_done:
        return False
    return process_vision_artifact(
        job,
        runtime,
        segment_reader,
        ffmpeg,
        ffprobe,
        output_dir,
        artifact_kind="tdeed_refined",
        search_before=search_before,
        search_after=search_after,
        refined_before=refined_before,
        refined_after=refined_after,
        width=width,
        fps=fps,
        colors=colors,
        size_reference_bytes=size_reference_bytes,
        python=python,
        timeout_seconds=timeout_seconds,
        ocr_python=ocr_python,
        ocr_timeout_seconds=ocr_timeout_seconds,
        fallback_width=fallback_width,
        fallback_fps=fallback_fps,
        fallback_colors=fallback_colors,
        min_degraded_seconds=min_degraded_seconds,
        cancel_event=cancel_event,
    )


def find_python(root: Path) -> Path:
    candidate = root / "tmp" / "venv" / "bin" / "python"
    return candidate if candidate.exists() else Path(shutil.which("python3") or "python3")
