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
from scoreboard_ocr import (
    DEFAULT_COARSE_SAMPLE_INTERVAL_SECONDS,
    ScoreboardOcrError,
    locate_scoreboard_event,
)
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
OCR_API_RANGE_FALLBACK_SECONDS = 120.0
OCR_API_RANGE_FALLBACK_COMPLETE_RATIO = 0.90
OCR_PROGRESSIVE_INITIAL_LOOKBACK_SECONDS = 120.0
OCR_PROGRESSIVE_OVERLAP_SECONDS = 5.0
OCR_PROGRESSIVE_INCREMENT_SECONDS = 30.0
OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS = 0.25
# Before two progressing clock observations establish that the broadcast
# timer is usable, inspect only the newest media. Re-running OCR sooner than
# one full probe window would mostly revisit the same frames.
OCR_CLOCK_READINESS_PROBE_SECONDS = 15.0
OCR_CLOCK_READINESS_MIN_MEDIA_GROWTH_SECONDS = 15.0
# A progressive miss may happen after the match clock has already crossed the
# requested boundary.  Keep a wider, target-centred retry window so a transient
# unreadable frame cannot permanently move the cursor past the event.  The
# first target-local retry uses a +/-15s window; subsequent retries use the
# expanded +/-30s window below.
OCR_PROGRESSIVE_TARGET_RESCAN_MARGIN_SECONDS = 15.0
# Target-local retries use the normal one-second OCR cadence.  The recovery
# comes from revisiting a target-centred window, not from changing the default
# GIF path or making every OCR pass more expensive.
OCR_PROGRESSIVE_TARGET_RESCAN_SAMPLE_INTERVAL_SECONDS = 1.0
OCR_PROGRESSIVE_TARGET_RESCAN_EXPANDED_MARGIN_SECONDS = 30.0
OCR_PROGRESSIVE_TARGET_RESCAN_MAX_ATTEMPTS = 3
OCR_OUTPUT_WINDOW_LEASE_OWNER = "ocr-output-window-retention"
OCR_OUTPUT_MIN_DEGRADED_SECONDS = 10.0
# The OCR artifact is a 60-second best-effort output. If its verified/projected
# anchor falls inside a source outage, prefer the nearest retained boundary
# anywhere in that window and mark the result approximate instead of failing.
OCR_OUTPUT_MAX_ANCHOR_GAP_SECONDS = 60.0
OCR_OUTPUT_MAX_ANCHOR_SHIFT_SECONDS = 30.0
OCR_OUTPUT_WINDOW_LEASE_MIN_TTL_SECONDS = 180.0
# Retain cumulative timing as diagnostics only. A slow-but-progressing event
# must not lose its OCR upgrade merely because earlier passes used this much
# wall time. Individual FFmpeg/OCR subprocesses still have watchdogs.
OCR_ACTIVE_PROCESSING_BUDGET_SECONDS = 300.0
OCR_ENCODING_RESERVE_SECONDS = 30.0
OCR_FFMPEG_WATCHDOG_SECONDS = 900.0
OCR_ROI_CACHE_FAILURES_BEFORE_REDISCOVERY = 3
# When one OCR pass takes long enough for the live buffer to grow, the next
# progressive pass is explicitly recorded as a refresh of the newly appended
# tail instead of treating the old scan snapshot as final evidence.
OCR_LONG_SCAN_TAIL_RESCAN_SECONDS = 5.0
# A validated clock-to-video mapping is already a strong anchor. Scan a
# target-centred +/-30s window as soon as its centre is retained; this avoids
# waiting for a separate tail-growth signal and tolerates timestamp jitter.
OCR_PROGRESSIVE_MAPPED_TARGET_MARGIN_SECONDS = 30.0
# A clock-to-video mapping is only used after two trustworthy observations.
# The ratio is normally close to 1.0, but a small tolerance covers broadcast
# drift and timestamp jitter without accepting a paused/replayed stream.
OCR_CLOCK_MAPPING_MIN_SAMPLES = 2
OCR_CLOCK_MAPPING_MIN_RATE = 0.5
OCR_CLOCK_MAPPING_MAX_RATE = 2.0
OCR_CLOCK_MAPPING_MAX_SAMPLES = 32
# Do not project a target arbitrarily far beyond the observed clock range.
# Such a projection is useful for a short API/OCR lag, but unsafe after a
# halftime break, stream seek, or a long missing segment.
# A validated clock-to-video mapping can safely navigate a longer live delay;
# the actual scan still requires retained, readable TS components below.
OCR_CLOCK_MAPPING_MAX_EXTRAPOLATION_SECONDS = 300.0
# A second-half clock-to-video mapping omits the wall-clock halftime interval.
# Add the normal 15-minute break only for availability diagnostics that project
# back into the first half.  This is not used to accept an OCR anchor.
OCR_AVAILABILITY_HALFTIME_BREAK_SECONDS = 15.0 * 60.0
OCR_AVAILABILITY_TARGET_MARGIN_SECONDS = 30.0
OCR_TARGET_WAIT_INITIAL_SECONDS = 60.0
OCR_TARGET_WAIT_MARGIN_SECONDS = 20.0
# The target wait may be extended while the live tail is still advancing, but
# it must have a finite watchdog so a dead stream cannot occupy an OCR worker
# forever.  Keep this separate from the per-inference timeout.
OCR_EVENT_HARD_LIMIT_SECONDS = 300.0
# A live tail or a trusted match clock that stops changing is useful evidence
# that waiting will not produce a new target.  The timeout is deliberately
# shorter than the event watchdog, while still allowing normal short pauses.
OCR_MEDIA_STALL_TIMEOUT_SECONDS = 60.0
OCR_CLOCK_PAUSE_TIMEOUT_SECONDS = 60.0
OCR_POSTROLL_HARD_LIMIT_SECONDS = 60.0
# When OCR has not produced an accepted clock yet, a growing live tail is
# still useful evidence that the requested media may arrive.  Extend the
# readiness deadline in short increments, bounded by the event hard limit,
# instead of failing at the initial 60-second cap.
OCR_MEDIA_PROGRESS_EXTENSION_SECONDS = 20.0
OCR_FAR_TARGET_RETRY_MIN_SECONDS = 10.0
OCR_FAR_TARGET_RETRY_MAX_SECONDS = 30.0
OCR_PROGRESSIVE_SCAN_MISS_KINDS = frozenset({
    "buffer_gap",
    # A slow OCR inference is a scan miss, not proof that the target clock is
    # absent.  The next progressive pass must be allowed to inspect newer
    # media that entered the rolling buffer while this pass was running.
    "inference_timeout",
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


_OCR_CLOCK_PHASES = frozenset({
    "first_half",
    "first_half_stoppage",
    "second_half",
    "second_half_stoppage",
})


def _normalize_ocr_clock_phase(value: Any) -> str | None:
    phase = str(value or "").strip().lower()
    return phase if phase in _OCR_CLOCK_PHASES else None


def _ocr_clock_period(phase: Any) -> str | None:
    normalized = _normalize_ocr_clock_phase(phase)
    if normalized is None:
        return None
    return "first_half" if normalized.startswith("first_half") else "second_half"


def _ocr_clock_phases_compatible(left: Any, right: Any) -> bool:
    left_period = _ocr_clock_period(left)
    right_period = _ocr_clock_period(right)
    return bool(left_period and left_period == right_period)


def _ocr_job_clock_phase(job: "VisionJob") -> str | None:
    """Classify the API target without confusing first-half stoppage with 48'."""
    minute = _event_minute(job).rstrip("'").strip()
    if minute:
        base_text, separator, extra_text = minute.partition("+")
        try:
            base = int(base_text)
            extra = int(extra_text) if separator else 0
        except ValueError:
            base = -1
            extra = -1
        if base == 45 and separator and extra > 0:
            return "first_half_stoppage"
        if base == 90 and separator and extra > 0:
            return "second_half_stoppage"
        if 0 <= base <= 45:
            return "first_half"
        if 45 < base <= 90:
            return "second_half"

    target = _ocr_progressive_target_seconds(job)
    if target is None:
        return None
    return "first_half" if int(target) <= 45 * 60 else "second_half"


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


def _walk_diagnostic_dicts(value: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    stack = [value]
    visited: set[int] = set()
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            identity = id(item)
            if identity in visited:
                continue
            visited.add(identity)
            found.append(item)
            stack.extend(item.values())
        elif isinstance(item, (list, tuple)):
            stack.extend(item)
    return found


def _scoreboard_roi_profile_from_result(
    match_id: str,
    value: Any,
) -> tuple[dict[str, Any], float | None] | None:
    """Build a reusable fixed profile from successful auto-discovery output."""
    confidence: float | None = None
    for item in _walk_diagnostic_dicts(value):
        raw_rate = item.get("clock_readable_rate")
        if raw_rate is not None:
            try:
                parsed = float(raw_rate)
            except (TypeError, ValueError):
                pass
            else:
                if math.isfinite(parsed):
                    confidence = max(0.0, min(1.0, parsed))
        auto = item.get("auto_clock")
        if not isinstance(auto, dict):
            continue
        roi = auto.get("clock_roi")
        resolution = auto.get("frame_resolution")
        if not (
            isinstance(roi, (list, tuple))
            and len(roi) == 4
            and isinstance(resolution, (list, tuple))
            and len(resolution) == 2
        ):
            continue
        try:
            normalized_roi = [int(value) for value in roi]
            width, height = (int(value) for value in resolution)
        except (TypeError, ValueError):
            continue
        if width <= 0 or height <= 0:
            continue
        profile = {
            "profile_id": f"auto-cache-{str(match_id)}",
            "reference_resolution": [width, height],
            "clock_roi": normalized_roi,
            "score_roi": None,
            "second_half_clock_mode": "auto",
            "aspect_ratio_tolerance": 0.04,
        }
        return profile, confidence
    return None


def _scoreboard_result_frame_resolution(value: Any) -> tuple[int, int] | None:
    for item in _walk_diagnostic_dicts(value):
        resolution = item.get("frame_resolution")
        if not isinstance(resolution, (list, tuple)) or len(resolution) != 2:
            continue
        try:
            width, height = (int(part) for part in resolution)
        except (TypeError, ValueError):
            continue
        if width > 0 and height > 0:
            return width, height
    return None


def _job_with_cached_scoreboard_profile(
    runtime: Any,
    job: "VisionJob",
) -> tuple["VisionJob", Any | None]:
    if job.scoreboard_profile is not None:
        return job, None
    getter = getattr(runtime.store, "get_scoreboard_roi_cache", None)
    if not callable(getter):
        return job, None
    cached = getter(job.match_id)
    if (
        cached is None
        or int(getattr(cached, "failure_streak", 0))
        >= OCR_ROI_CACHE_FAILURES_BEFORE_REDISCOVERY
    ):
        return job, None
    profile = getattr(cached, "profile", None)
    if not isinstance(profile, dict):
        return job, None
    return replace(job, scoreboard_profile=dict(profile)), cached


def _record_scoreboard_roi_success(
    runtime: Any,
    job: "VisionJob",
    located: dict[str, Any],
    *,
    cached: Any | None,
) -> dict[str, Any] | None:
    saver = getattr(runtime.store, "save_scoreboard_roi_cache", None)
    if not callable(saver):
        return None
    candidate = _scoreboard_roi_profile_from_result(job.match_id, located)
    if candidate is not None:
        profile, confidence = candidate
    elif cached is not None and isinstance(job.scoreboard_profile, dict):
        profile = dict(job.scoreboard_profile)
        confidence = getattr(cached, "confidence", None)
    else:
        return None
    stored = saver(job.match_id, profile, confidence=confidence)
    current_resolution = _scoreboard_result_frame_resolution(located)
    reference = profile.get("reference_resolution")
    resolution_changed = bool(
        current_resolution is not None
        and isinstance(reference, (list, tuple))
        and len(reference) == 2
        and current_resolution != (int(reference[0]), int(reference[1]))
    )
    if resolution_changed:
        invalidator = getattr(runtime.store, "record_scoreboard_roi_failure", None)
        if callable(invalidator):
            stored = invalidator(job.match_id, invalidate=True) or stored
    return {
        "status": "rediscover_next_request" if resolution_changed else "reused" if cached else "discovered",
        "success_streak": int(getattr(stored, "success_streak", 0)),
        "failure_streak": int(getattr(stored, "failure_streak", 0)),
        "reference_resolution": list(profile.get("reference_resolution") or []),
        "current_resolution": list(current_resolution) if current_resolution else None,
    }


def _record_scoreboard_roi_failure(
    runtime: Any,
    job: "VisionJob",
    error_kind: str,
    *,
    cached: Any | None,
) -> None:
    if cached is None or error_kind not in {
        "scoreboard_missing",
        "ocr_clock_unreadable",
        "clock_profile_mismatch",
    }:
        return
    recorder = getattr(runtime.store, "record_scoreboard_roi_failure", None)
    if callable(recorder):
        recorder(
            job.match_id,
            invalidate=error_kind == "clock_profile_mismatch",
        )


def _ocr_recoverable_profile_mismatch(
    error_kind: str,
    diagnostics: Any,
    clock_samples: Any,
) -> bool:
    """Treat a short post-gap probe as insufficient media, not a bad ROI."""
    if (
        error_kind != "clock_profile_mismatch"
        or _ocr_progressive_clock_mapping(clock_samples).get("status") != "ready"
        or not isinstance(diagnostics, dict)
    ):
        return False
    attempts = diagnostics.get("fragment_attempts")
    if not isinstance(attempts, list) or not attempts:
        return False
    for attempt in attempts:
        if not isinstance(attempt, dict):
            return False
        attempt_kind = attempt.get("error_kind")
        direct_fallback = attempt.get("direct_clock_roi_fallback")
        fallback_kind = (
            direct_fallback.get("error_kind")
            if isinstance(direct_fallback, dict)
            else None
        )
        if (
            attempt_kind != "clock_profile_mismatch"
            and fallback_kind != "clock_profile_mismatch"
        ):
            return False
        try:
            start = float(attempt["window_start"])
            end = float(attempt["window_end"])
        except (KeyError, TypeError, ValueError):
            return False
        if (
            not math.isfinite(start)
            or not math.isfinite(end)
            or end <= start
            or end - start
            > OCR_CLOCK_READINESS_PROBE_SECONDS
            + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
        ):
            return False
        detail_roots = [attempt.get("diagnostics")]
        if isinstance(direct_fallback, dict):
            detail_roots.append(direct_fallback.get("diagnostics"))
        for detail in detail_roots:
            if not isinstance(detail, dict):
                continue
            # These fields prove that the configured ROI itself is invalid.
            # They are unrelated to having too few frames after a stream gap.
            if any(
                key in detail
                for key in (
                    "aspect_ratio_difference",
                    "aspect_ratio_tolerance",
                    "available_profile_ids",
                    "reference_resolution",
                )
            ):
                return False
    return True


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
    if kind in {
        "internal_video_gap",
        "anchor_gap",
        "anchor_gap_too_large",
        "anchor_shift_too_large",
        "anchor_unavailable",
    }:
        return "buffer_gap"
    return kind or "video_unavailable"


def _ocr_coverage_contract(coverage: VideoCoverage) -> dict[str, Any]:
    """Return the stable media-coverage fields for an OCR GIF result."""
    return {
        "coverage_status": coverage.status.value,
        "coverage_quality": coverage.coverage_quality,
        "coverage_reason": coverage.reason,
        "degraded": coverage.degraded,
        "stitched_across_gap": coverage.stitched_across_gap,
        "video_gap_count": coverage.video_gap_count,
        "skipped_gap_seconds": coverage.skipped_gap_seconds,
        "approximate": coverage.approximate,
        "anchor_adjusted": coverage.anchor_adjusted,
        "anchor_adjusted_to_stream_time": coverage.anchor_adjusted_to,
        "anchor_shift_seconds": coverage.anchor_shift_seconds,
        "event_frame_may_be_missing": coverage.event_frame_may_be_missing,
        **(
            {"coverage_error_kind": coverage.error_kind}
            if coverage.error_kind
            else {}
        ),
    }


def _ensure_ocr_output_window_lease(
    runtime: Any,
    *,
    event_key: str,
    artifact_kind: str,
    target_revision: int,
    segments: list[Segment],
    window_start: float,
    window_end: float,
    ttl_seconds: float,
    now_unix: float | None = None,
) -> dict[str, Any]:
    """Retain currently available segments in a located OCR output window."""
    if ttl_seconds <= 0:
        raise ValueError("OCR output window lease TTL must be positive")
    if window_end <= window_start:
        raise ValueError("OCR output lease window must have positive duration")
    owner = _ocr_output_window_lease_owner(target_revision)
    timestamp = time.time() if now_unix is None else float(now_unix)
    candidate_paths: list[str] = []
    for segment in segments:
        if float(segment.end) <= window_start or float(segment.start) >= window_end:
            continue
        path = Path(segment.path)
        try:
            if path.is_file() and path.stat().st_size > 0:
                candidate_paths.append(str(path.resolve()))
        except OSError:
            continue
    candidate_paths = list(dict.fromkeys(candidate_paths))

    active = [
        lease
        for lease in runtime.store.list_segment_leases(
            event_key=event_key,
            active_at=timestamp,
        )
        if lease.owner == owner
    ]
    renewed_ids = {
        lease_id
        for lease_id in {lease.lease_id for lease in active}
        if runtime.store.renew_segment_lease(
            lease_id,
            ttl_seconds=ttl_seconds,
            now=timestamp,
        )
    }
    already_leased_paths = {lease.segment_path for lease in active}
    missing_paths = [
        path for path in candidate_paths if path not in already_leased_paths
    ]
    lease_id = None
    if missing_paths:
        lease_id = runtime.store.acquire_segment_lease(
            event_key,
            missing_paths,
            artifact_kind=artifact_kind,
            owner=owner,
            ttl_seconds=ttl_seconds,
            now=timestamp,
        )
    return {
        "owner": owner,
        "target_revision": max(0, int(target_revision)),
        "lease_id": lease_id,
        "segment_count": len(candidate_paths),
        "new_segment_count": len(missing_paths),
        "renewed_lease_count": len(renewed_ids),
        "retention_start_stream_time": round(float(window_start), 3),
        "retention_end_stream_time": round(float(window_end), 3),
        "ttl_seconds": float(ttl_seconds),
    }


def _ocr_output_window_lease_owner(target_revision: int) -> str:
    """Keep legacy revision zero compatible while isolating later targets."""
    revision = max(0, int(target_revision))
    if revision == 0:
        return OCR_OUTPUT_WINDOW_LEASE_OWNER
    return f"{OCR_OUTPUT_WINDOW_LEASE_OWNER}:revision-{revision}"


def _release_ocr_output_window_leases(
    runtime: Any,
    event_key: str,
    *,
    target_revision: int,
) -> int:
    return runtime.store.release_segment_leases_for_event(
        event_key,
        owner=_ocr_output_window_lease_owner(target_revision),
    )


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
        "localization_source": "failed",
        "precision": "unverified",
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
        "localization_source": (
            "minute_boundary"
            if ocr_location_kind == "clock_interval"
            else "score_transition"
        ),
        "precision": (
            "minute_boundary"
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
            localization_source, precision = _ocr_localization_contract(ocr_result)
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
                "localization_source": localization_source,
                "localization_precision": precision,
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
                "precise_location": localization_source in {"exact", "interpolated"}
                and (ocr_result.get("localization_quality") or "exact") == "exact",
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
                "localization_source": "failed",
                "precision": "unverified",
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
    # Monotonic target identity.  A late shotmap second increments this once;
    # workers use it to reject writes produced for the prior minute target.
    target_revision: int = 0
    target_kind: str = "minute"
    target_source: str = "overview"

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


def ensure_ocr_target_revision(
    runtime: Any,
    job: VisionJob,
    *,
    now_unix: float | None = None,
) -> Any:
    """Make a late exact-second target restart OCR from the retained history.

    The event loop may discover an overview event first and receive the
    shotmap's cumulative ``second`` later.  OCR progress is durable, so merely
    mutating ``VisionJob.event_second`` would leave its cursor past the target.
    This helper records a monotonic revision, clears only the active scan
    cursor/anchor state, and requeues the OCR artifact immediately.  Existing
    default-GIF state and diagnostic history are untouched.

    It is safe to call on every polling iteration; identical revisions are a
    no-op.  The caller should increment ``job.target_revision`` exactly once
    when the authoritative target changes.
    """
    timestamp = time.time() if now_unix is None else float(now_unix)
    current = _artifact_task(runtime, job.event_key, "ocr_window")
    if current is None:
        return None
    revision = _vision_target_revision(job)
    source = _vision_target_source(job)
    previous = _ocr_progressive_state(current)
    recorded = previous.get("target_revision")
    try:
        recorded_revision = int(recorded) if recorded is not None else None
    except (TypeError, ValueError):
        recorded_revision = None
    # Existing rows created before target revisions were introduced must keep
    # their persisted located/encoding state on the first read.  Revision 0
    # is the legacy baseline; only a positive revision denotes a late target
    # that requires a rewind.
    if recorded_revision is None and revision == 0:
        return current
    if recorded_revision == revision:
        return current

    history = list(previous.get("target_revision_history") or [])
    history.append(
        {
            "from_revision": recorded_revision,
            "to_revision": revision,
            "target_source": source,
            "target_clock_seconds": job.event_second,
            "reset_at_unix": timestamp,
        }
    )
    reset_progress = {
        **previous,
        "target_revision": revision,
        "target_source": source,
        "target_clock_seconds": job.event_second,
        "target_clock": _clock_text_from_seconds(job.event_second),
        "target_revision_reset_at_unix": timestamp,
        "target_revision_reset_reason": "late_exact_second_target",
        "target_revision_history": history[-8:],
        # The old cursor and anchor belong to the old target.  Keep the scan
        # diagnostics/history above, but force the next pass to use the full
        # retained initial lookback window again.
        "scan_cursor_stream_time": None,
        "latest_trusted_clock_seconds": None,
        "final_scan_completed_at_unix": None,
        "final_scan_started_at_unix": None,
        "final_scan_window": None,
        "target_rescan_window": None,
        "target_rescan_started_at_unix": None,
        "target_rescan_completed_at_unix": None,
        "target_passed_without_anchor": False,
        "anchor_stream_time": None,
        "anchor_provenance": None,
        "state": "target_revision_reset",
    }

    # A worker may be in-flight while the API refreshes the event.  Moving it
    # back to pending invalidates its old result; the worker-side revision
    # check below prevents that stale result from being persisted afterward.
    if current.status in {"locating", "located", "encoding", "encoded", "failed"}:
        try:
            reset_result = {
                "stage": "target_revision_reset",
                "target_revision": revision,
                "target_source": source,
                "default_gif_preserved": True,
            }
            if current.status == "encoded":
                previous_output = current.result.get("output") or current.output_path
                if previous_output:
                    reset_result["superseded_output"] = previous_output
                reset_result["superseded_at_target_revision"] = revision
            _artifact_transition(
                runtime,
                job.event_key,
                "ocr_window",
                "pending",
                result=reset_result,
                window_metadata={"progressive_scan": reset_progress},
            )
        except ValueError:
            # Keep compatibility with an older state store that still treats
            # encoded visual rows as terminal; the upgrade remains visible in
            # diagnostics instead of corrupting that row.
            return current
        current = _artifact_task(runtime, job.event_key, "ocr_window")
        if current is None:
            return None

    if current.status == "pending":
        return _artifact_readiness_wait(
            runtime,
            job.event_key,
            "ocr_window",
            "OCR target revised; rescanning the retained initial history window",
            error_kind="ocr_target_revision_reset",
            result={
                "artifact_kind": "ocr_window",
                "stage": "target_revision_reset",
                "progressive_status": "target_revision_reset",
                "target_revision": revision,
                "target_source": source,
                "target_clock_seconds": job.event_second,
                "default_gif_preserved": True,
            },
            window_metadata={"progressive_scan": reset_progress},
            next_attempt_at_unix=timestamp,
            now=timestamp,
        )
    return current


def _ocr_target_revision_is_stale(
    runtime: Any,
    event_key: str,
    revision: int,
) -> bool:
    """Return whether a worker result belongs to an obsolete OCR target."""
    current = _artifact_task(runtime, event_key, "ocr_window")
    if current is None:
        return True
    state = _ocr_progressive_state(current)
    raw = state.get("target_revision")
    try:
        current_revision = int(raw) if raw is not None else 0
    except (TypeError, ValueError):
        current_revision = 0
    return current_revision != revision


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
    """Return the clock attached to the newest validated stream-time sample.

    Do not inspect arbitrary nested fields or clock-range summaries here. They
    may contain the API target, rejected OCR output, or an old maximum. Only
    explicit trusted sample containers are accepted by
    ``_ocr_progressive_clock_readings``.
    """
    readings = _ocr_progressive_clock_readings(value)
    if not readings:
        return None
    validated = _ocr_latest_continuous_clock_run(readings)
    return int(max(validated, key=lambda item: item[0])[1]) if validated else None


def _ocr_progressive_target_seconds(job: VisionJob) -> int | None:
    """Return the primary match-clock target for progressive OCR.

    An exact cumulative second is authoritative when available.  The previous
    implementation used ``max(event_second, minute_boundary)`` which silently
    moved a late shotmap target forward to the API minute and caused scans to
    miss the actual event.  Minute-boundary fallback remains represented by
    ``_ocr_minute_boundary_seconds`` and is no longer allowed to replace the
    exact target.
    """
    if job.event_second is not None:
        try:
            value = int(job.event_second)
        except (TypeError, ValueError):
            value = -1
        if value >= 0:
            return value
    return _ocr_minute_boundary_seconds(job)


def _ocr_minute_boundary_seconds(job: VisionJob) -> int | None:
    """Return the minute boundary used only for degraded fallback semantics."""
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
    return minute_target


def _ocr_progressive_fallback_horizon_seconds(job: VisionJob) -> int | None:
    """Return the minute boundary without overriding an exact target."""
    return _ocr_minute_boundary_seconds(job)


def _vision_target_revision(job: VisionJob) -> int:
    """Read the monotonic API-target revision from old and new job objects."""
    raw = getattr(job, "target_revision", 0)
    try:
        revision = int(raw)
    except (TypeError, ValueError):
        revision = 0
    return max(0, revision)


def _vision_target_source(job: VisionJob) -> str:
    source = str(getattr(job, "target_source", "overview") or "overview").strip()
    return source or "overview"


def _ocr_progressive_state(task: Any) -> dict[str, Any]:
    metadata = getattr(task, "window_metadata", None)
    if not isinstance(metadata, dict):
        return {}
    state = metadata.get("progressive_scan")
    if not isinstance(state, dict):
        return {}
    restored = dict(state)
    if "clock_samples" in restored or "latest_trusted_clock_seconds" in restored:
        raw_samples = restored.get("clock_samples")
        samples = _ocr_progressive_merge_clock_samples([], raw_samples)
        recovered_latest = _latest_trusted_clock_seconds(samples)
        raw_latest = restored.get("latest_trusted_clock_seconds")
        try:
            stored_latest = (
                int(raw_latest)
                if raw_latest is not None and not isinstance(raw_latest, bool)
                else None
            )
        except (TypeError, ValueError):
            stored_latest = None
        restored["clock_samples"] = samples
        restored["latest_trusted_clock_seconds"] = recovered_latest
        restored["latest_trusted_clock"] = _clock_text_from_seconds(
            recovered_latest
        )
        if stored_latest != recovered_latest:
            restored["clock_state_recovery"] = {
                "status": "invalid_persisted_latest_recomputed",
                "stored_latest_trusted_clock_seconds": stored_latest,
                "recovered_latest_trusted_clock_seconds": recovered_latest,
                "validated_sample_count": len(samples),
            }
    return restored


def _ocr_match_clock_readiness(
    runtime: Any,
    job: VisionJob,
    state: dict[str, Any],
) -> tuple[list[dict[str, float | int | str]], dict[str, Any]]:
    """Return this task's clock samples or a ready mapping from the match.

    Vision task metadata is already durable and indexed by match, so sharing a
    validated mapping does not require another database table. Only a mapping
    backed by two plausible progressing observations is shared; isolated OCR
    readings remain local to their event.
    """
    clock_phase = _ocr_job_clock_phase(job)
    clock_period = _ocr_clock_period(clock_phase)
    local_samples = _ocr_progressive_merge_clock_samples(
        [],
        state.get("clock_samples"),
        default_phase=clock_phase,
    )
    local_mapping = _ocr_progressive_clock_mapping(
        local_samples,
        clock_phase=clock_phase,
    )
    previous = state.get("clock_readiness")
    readiness = dict(previous) if isinstance(previous, dict) else {}
    previous_period = (
        _ocr_clock_period(readiness.get("clock_phase"))
        or str(readiness.get("clock_period") or "").strip()
        or None
    )
    if previous_period is not None and previous_period != clock_period:
        readiness = {}
    if local_mapping.get("status") == "ready":
        readiness.update({
            "status": "ready",
            "scope": "match",
            "source_event_key": str(
                readiness.get("source_event_key") or job.event_key
            ),
            "accepted_sample_count": int(
                local_mapping.get("accepted_sample_count") or 0
            ),
            "clock_mapping": local_mapping,
            "clock_phase": clock_phase,
            "clock_period": clock_period,
        })
        return local_samples, readiness

    list_tasks = getattr(runtime.store, "list_vision_tasks", None)
    if callable(list_tasks):
        candidates: list[tuple[float, str, list[dict[str, Any]], dict[str, Any]]] = []
        waiting_candidates: list[tuple[float, float, str, dict[str, Any]]] = []
        try:
            tasks = list_tasks(job.match_id, "ocr_window")
        except (OSError, RuntimeError, TypeError, ValueError):
            tasks = []
        for task in tasks:
            if str(getattr(task, "event_key", "")) == job.event_key:
                continue
            candidate_state = _ocr_progressive_state(task)
            candidate_readiness = candidate_state.get("clock_readiness")
            if isinstance(candidate_readiness, dict):
                candidate_readiness_period = (
                    _ocr_clock_period(candidate_readiness.get("clock_phase"))
                    or str(candidate_readiness.get("clock_period") or "").strip()
                    or None
                )
                try:
                    candidate_probe_tail = float(
                        candidate_readiness.get(
                            "last_probe_media_end_stream_time"
                        )
                    )
                except (TypeError, ValueError):
                    candidate_probe_tail = math.nan
                if (
                    candidate_readiness.get("status") == "waiting"
                    and candidate_readiness_period is not None
                    and candidate_readiness_period == clock_period
                    and math.isfinite(candidate_probe_tail)
                ):
                    waiting_candidates.append((
                        candidate_probe_tail,
                        float(getattr(task, "updated_at_unix", 0.0) or 0.0),
                        str(getattr(task, "event_key", "")),
                        dict(candidate_readiness),
                    ))
            candidate_samples = _ocr_progressive_merge_clock_samples(
                [], candidate_state.get("clock_samples")
            )
            candidate_sample_periods = [
                _ocr_clock_period(sample.get("clock_phase"))
                for sample in candidate_samples
            ]
            # Phase-less legacy samples are valid only inside their original
            # event. Cross-event reuse requires every source sample to prove
            # that it belongs to the target event's match period.
            if (
                not candidate_sample_periods
                or any(period is None for period in candidate_sample_periods)
                or any(period != clock_period for period in candidate_sample_periods)
            ):
                continue
            candidate_mapping = _ocr_progressive_clock_mapping(
                candidate_samples,
                clock_phase=clock_phase,
            )
            if candidate_mapping.get("status") != "ready":
                continue
            candidate_source_event_key = str(
                candidate_readiness.get("source_event_key")
                if isinstance(candidate_readiness, dict)
                and candidate_readiness.get("source_event_key")
                else getattr(task, "event_key", "")
            )
            candidates.append((
                float(getattr(task, "updated_at_unix", 0.0) or 0.0),
                candidate_source_event_key,
                candidate_samples,
                candidate_mapping,
            ))
        if candidates:
            _updated, source_event_key, shared_samples, shared_mapping = max(
                candidates, key=lambda item: (item[0], item[1])
            )
            readiness.update({
                "status": "ready",
                "scope": "match",
                "source_event_key": source_event_key,
                "accepted_sample_count": int(
                    shared_mapping.get("accepted_sample_count") or 0
                ),
                "clock_mapping": shared_mapping,
                "clock_phase": clock_phase,
                "clock_period": clock_period,
                "reused_from_match": True,
            })
            return shared_samples, readiness
        if waiting_candidates:
            probe_tail, _updated, source_event_key, shared_readiness = max(
                waiting_candidates, key=lambda item: (item[0], item[1], item[2])
            )
            source_event_key = str(
                shared_readiness.get("source_event_key") or source_event_key
            )
            try:
                local_probe_tail = float(
                    readiness.get("last_probe_media_end_stream_time")
                )
            except (TypeError, ValueError):
                local_probe_tail = math.nan
            if not math.isfinite(local_probe_tail) or probe_tail > local_probe_tail:
                readiness.update(shared_readiness)
                readiness.update({
                    "source_event_key": source_event_key,
                    "last_probe_media_end_stream_time": round(probe_tail, 3),
                    "clock_phase": clock_phase,
                    "clock_period": clock_period,
                    "reused_from_match": True,
                })

    readiness.update({
        "status": "waiting",
        "scope": "match",
        "source_event_key": str(
            readiness.get("source_event_key") or job.event_key
        ),
        "accepted_sample_count": int(
            local_mapping.get("accepted_sample_count") or 0
        ),
        "clock_mapping": local_mapping,
        "clock_phase": clock_phase,
        "clock_period": clock_period,
    })
    return local_samples, readiness


def _ocr_active_processing_budget(
    state: dict[str, Any],
    *,
    now_unix: float | None = None,
    account_open_execution: bool = False,
) -> dict[str, Any]:
    """Return the durable OCR/FFmpeg budget without charging media waits.

    Only intervals explicitly marked by ``execution_started_at_unix`` are
    charged. Queue time and the persisted pending/located waits therefore do
    not consume this budget. ``accounted_execution_started_at_unix`` makes the
    accounting idempotent across polling and process restarts.
    """
    raw = state.get("active_processing_budget")
    budget = dict(raw) if isinstance(raw, dict) else {}
    try:
        total = float(budget.get("total_seconds", OCR_ACTIVE_PROCESSING_BUDGET_SECONDS))
    except (TypeError, ValueError):
        total = OCR_ACTIVE_PROCESSING_BUDGET_SECONDS
    total = max(1.0, total)
    try:
        used = max(0.0, float(budget.get("used_seconds") or 0.0))
    except (TypeError, ValueError):
        used = 0.0

    if account_open_execution:
        started_raw = state.get("execution_started_at_unix")
        accounted_raw = budget.get("accounted_execution_started_at_unix")
        try:
            started = float(started_raw)
        except (TypeError, ValueError):
            started = math.nan
        try:
            accounted = float(accounted_raw)
        except (TypeError, ValueError):
            accounted = math.nan
        if math.isfinite(started) and (
            not math.isfinite(accounted) or abs(accounted - started) > 0.001
        ):
            completed = time.time() if now_unix is None else float(now_unix)
            elapsed = max(0.0, completed - started)
            used += elapsed
            budget["last_execution_seconds"] = round(elapsed, 3)
            budget["accounted_execution_started_at_unix"] = started
            budget["last_execution_completed_at_unix"] = completed

    used = min(total, used)
    budget.update({
        "policy_version": 2,
        "enforced": False,
        "total_seconds": total,
        "encoding_reserve_seconds": min(OCR_ENCODING_RESERVE_SECONDS, total),
        "used_seconds": round(used, 3),
        "remaining_seconds": round(max(0.0, total - used), 3),
    })
    return budget


def _ocr_budget_after_elapsed(
    budget: dict[str, Any],
    elapsed_seconds: float,
    *,
    phase: str,
) -> dict[str, Any]:
    updated = dict(budget)
    total = float(updated.get("total_seconds", OCR_ACTIVE_PROCESSING_BUDGET_SECONDS))
    used = max(0.0, float(updated.get("used_seconds") or 0.0))
    elapsed = max(0.0, float(elapsed_seconds))
    used = min(total, used + elapsed)
    updated.update({
        "used_seconds": round(used, 3),
        "remaining_seconds": round(max(0.0, total - used), 3),
        "last_phase": str(phase),
        "last_phase_seconds": round(elapsed, 3),
        "updated_at_unix": time.time(),
    })
    return updated


def _ocr_structured_diagnostic_roots(value: Any) -> list[dict[str, Any]]:
    """Return only documented OCR diagnostic containers, without recursion."""
    roots: list[dict[str, Any]] = []

    def add(item: Any) -> None:
        if isinstance(item, dict) and all(item is not root for root in roots):
            roots.append(item)

    add(value)
    if not isinstance(value, dict):
        return roots
    add(value.get("diagnostics"))
    ocr = value.get("ocr")
    add(ocr)
    if isinstance(ocr, dict):
        add(ocr.get("diagnostics"))
    exact_error = value.get("exact_second_error")
    if isinstance(exact_error, dict):
        add(exact_error.get("diagnostics"))
    for attempt in value.get("fragment_attempts") or ():
        if isinstance(attempt, dict):
            add(attempt.get("diagnostics"))
    return roots


def _ocr_progressive_clock_sample_records(
    value: Any,
    *,
    default_phase: str | None = None,
) -> list[dict[str, Any]]:
    """Extract unique trusted clock observations while preserving their phase.

    Worker failures preserve diagnostics at different nesting levels (for
    example ``readings`` and ``clock_raw_observations``).  Keeping this parser
    local to the progressive state avoids coupling the runtime to one worker
    response shape while still allowing a safe target-window retry.
    """
    normalized_default_phase = _normalize_ocr_clock_phase(default_phase)
    found: dict[tuple[float, int], str | None] = {}

    def add(stream: float, clock: int, phase: Any) -> None:
        normalized_phase = (
            _normalize_ocr_clock_phase(phase) or normalized_default_phase
        )
        key = (round(stream, 3), int(clock))
        existing = found.get(key)
        if existing is None or normalized_phase is not None:
            found[key] = normalized_phase

    # Durable samples have an absolute stream time and were already filtered
    # before persistence. They are still range-checked below and later passed
    # through the continuity-run validator during merge/recovery.
    if isinstance(value, list):
        for sample in value:
            if not isinstance(sample, dict):
                continue
            try:
                stream = float(sample.get("stream_time"))
                clock = int(sample.get("match_clock_seconds"))
            except (TypeError, ValueError):
                continue
            if (
                math.isfinite(stream)
                and stream >= 0
                and clock >= 0
                and not isinstance(sample.get("match_clock_seconds"), bool)
            ):
                add(stream, clock, sample.get("clock_phase"))
        return [
            {
                "stream_time": stream,
                "match_clock_seconds": clock,
                **({"clock_phase": phase} if phase is not None else {}),
            }
            for (stream, clock), phase in sorted(found.items())
        ]

    for root in _ocr_structured_diagnostic_roots(value):
        root_phase = _normalize_ocr_clock_phase(root.get("clock_phase"))
        try:
            candidate_start = float(root.get("candidate_start_seconds") or 0.0)
        except (TypeError, ValueError):
            candidate_start = 0.0
        if not math.isfinite(candidate_start):
            candidate_start = 0.0
        for key in ("clock_raw_observations", "readings"):
            readings = root.get(key)
            if not isinstance(readings, list):
                continue
            for reading in readings:
                if not isinstance(reading, dict):
                    continue
                status = str(
                    reading.get("continuity_status")
                    or reading.get("status")
                    or ""
                ).strip().lower()
                if status not in {"accepted", "resynchronized"}:
                    continue
                if (
                    reading.get("scoreboard_visible") is False
                    or reading.get("ambiguous_clock") is True
                ):
                    continue
                frame_raw = reading.get("frame_seconds")
                if frame_raw is None:
                    frame_raw = reading.get("video_seconds")
                try:
                    frame = float(frame_raw)
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(frame) or frame < 0:
                    continue
                clock_raw = (
                    reading.get("effective_clock_seconds")
                    if reading.get("effective_clock_seconds") is not None
                    else reading.get("clock_seconds")
                    if reading.get("clock_seconds") is not None
                    else reading.get("clock")
                )
                if isinstance(clock_raw, bool):
                    continue
                try:
                    clock = (
                        int(clock_raw)
                        if isinstance(clock_raw, (int, float))
                        and float(clock_raw).is_integer()
                        else _clock_seconds_from_text(clock_raw)
                    )
                except (TypeError, ValueError, OverflowError):
                    clock = None
                if clock is None or clock < 0:
                    continue
                add(
                    candidate_start + frame,
                    int(clock),
                    reading.get("clock_phase") or root_phase,
                )

    # Conflicting clocks at the same stream time are ambiguous even when both
    # records claim acceptance. Drop the whole timestamp rather than picking a
    # numerically larger value.
    clocks_by_stream: dict[float, set[int]] = {}
    for stream, clock in found:
        clocks_by_stream.setdefault(stream, set()).add(clock)
    records: list[dict[str, Any]] = []
    for (stream, clock), phase in sorted(found.items()):
        if len(clocks_by_stream.get(stream, ())) != 1:
            continue
        records.append({
            "stream_time": stream,
            "match_clock_seconds": clock,
            **({"clock_phase": phase} if phase is not None else {}),
        })
    return records


def _ocr_progressive_clock_readings(value: Any) -> list[tuple[float, int]]:
    """Return the legacy pair view of trusted clock sample records."""
    return [
        (float(sample["stream_time"]), int(sample["match_clock_seconds"]))
        for sample in _ocr_progressive_clock_sample_records(value)
    ]


def _ocr_latest_unpositioned_clock_seconds(value: Any) -> int | None:
    """Return one explicit accepted reading for display-only compatibility.

    Some older OCR backends omitted frame timestamps. Such a value may explain
    how far the worker appeared to get, but it cannot enter ``clock_samples``
    or prove that a target was passed.
    """
    candidates: list[int] = []
    for root in _ocr_structured_diagnostic_roots(value):
        for key in ("clock_raw_observations", "readings"):
            readings = root.get(key)
            if not isinstance(readings, list):
                continue
            for reading in readings:
                if not isinstance(reading, dict):
                    continue
                status = str(
                    reading.get("continuity_status")
                    or reading.get("status")
                    or ""
                ).strip().lower()
                if (
                    status not in {"accepted", "resynchronized"}
                    or reading.get("scoreboard_visible") is False
                    or reading.get("ambiguous_clock") is True
                    or reading.get("frame_seconds") is not None
                    or reading.get("video_seconds") is not None
                ):
                    continue
                raw = (
                    reading.get("effective_clock_seconds")
                    if reading.get("effective_clock_seconds") is not None
                    else reading.get("clock_seconds")
                    if reading.get("clock_seconds") is not None
                    else reading.get("clock")
                )
                if isinstance(raw, bool):
                    continue
                try:
                    parsed = (
                        int(raw)
                        if isinstance(raw, (int, float))
                        and float(raw).is_integer()
                        else _clock_seconds_from_text(raw)
                    )
                except (TypeError, ValueError, OverflowError):
                    parsed = None
                if parsed is not None and parsed >= 0:
                    candidates.append(parsed)
    return candidates[-1] if candidates else None


def _ocr_progressive_target_rescan_window(
    diagnostics: dict[str, Any] | None,
    *,
    target_clock_seconds: int | None,
    margin_seconds: float | None = None,
    rescan_attempt: int = 1,
) -> dict[str, Any] | None:
    """Estimate a stream-time window around a clock that was already crossed."""
    if target_clock_seconds is None or not isinstance(diagnostics, dict):
        return None
    readings = _ocr_progressive_clock_readings(diagnostics)
    if not readings:
        return None
    target = int(target_clock_seconds)
    samples = [
        {"stream_time": stream, "match_clock_seconds": clock}
        for stream, clock in readings
    ]
    if not _ocr_target_passed_with_continuous_evidence(
        samples,
        target_clock_seconds=target,
    ):
        return None
    exact = [frame for frame, clock in readings if clock == target]
    if exact:
        estimate = min(exact)
        method = "direct_clock_observation"
    else:
        before = [item for item in readings if item[1] < target]
        after = [item for item in readings if item[1] > target]
        estimate: float | None = None
        method = "nearest_clock_observation"
        if before and after:
            left_frame, left_clock = max(before, key=lambda item: item[1])
            right_frame, right_clock = min(after, key=lambda item: item[1])
            clock_delta = right_clock - left_clock
            video_delta = right_frame - left_frame
            if (
                clock_delta > 0
                and video_delta >= 0
                and 0.5 <= video_delta / clock_delta <= 1.5
            ):
                fraction = (target - left_clock) / (right_clock - left_clock)
                estimate = left_frame + fraction * (right_frame - left_frame)
                method = "two_sided_clock_interpolation"
        if estimate is None:
            nearest_frame, nearest_clock = min(
                readings,
                key=lambda item: (abs(item[1] - target), item[0]),
            )
            estimate = nearest_frame + (target - nearest_clock)
            method = "one_sided_clock_projection"
    try:
        attempt = max(1, int(rescan_attempt))
    except (TypeError, ValueError):
        attempt = 1
    margin = (
        OCR_PROGRESSIVE_TARGET_RESCAN_MARGIN_SECONDS
        if margin_seconds is None
        else max(1.0, float(margin_seconds))
    )
    return {
        "start_stream_time": round(max(0.0, float(estimate) - margin), 3),
        "end_stream_time": round(float(estimate) + margin, 3),
        "estimated_stream_time": round(float(estimate), 3),
        "target_clock_seconds": target,
        "method": method,
        "margin_seconds": margin,
        "rescan_attempt": attempt,
        "sample_interval_seconds": OCR_PROGRESSIVE_TARGET_RESCAN_SAMPLE_INTERVAL_SECONDS,
        "scan_mode": "target_centered_rescan",
    }


def _ocr_progressive_expand_target_rescan_window(
    previous: Any,
    *,
    target_clock_seconds: int | None,
    rescan_attempt: int,
    margin_seconds: float | None = None,
) -> dict[str, Any] | None:
    """Expand a persisted target retry around its previous stream-time centre."""
    if not isinstance(previous, dict):
        return None
    try:
        estimate = float(previous.get("estimated_stream_time"))
    except (TypeError, ValueError):
        return None
    if not math.isfinite(estimate):
        return None
    margin = (
        OCR_PROGRESSIVE_TARGET_RESCAN_EXPANDED_MARGIN_SECONDS
        if margin_seconds is None
        else max(1.0, float(margin_seconds))
    )
    return {
        "start_stream_time": round(max(0.0, estimate - margin), 3),
        "end_stream_time": round(estimate + margin, 3),
        "estimated_stream_time": round(estimate, 3),
        "target_clock_seconds": target_clock_seconds,
        "method": "expanded_previous_target_rescan",
        "margin_seconds": margin,
        "rescan_attempt": max(1, int(rescan_attempt)),
        "sample_interval_seconds": OCR_PROGRESSIVE_TARGET_RESCAN_SAMPLE_INTERVAL_SECONDS,
        "scan_mode": "target_centered_rescan",
    }


def _ocr_localization_contract(located: dict[str, Any]) -> tuple[str, str]:
    """Return the stable source/precision pair for a verified OCR result.

    The worker's historical ``location_kind`` remains the authorization
    boundary.  These additional values only make the evidence grade visible
    to persistence and the dashboard: a directly observed target second,
    two-sided interpolation, short one-sided estimate, or minute boundary.
    """
    location_kind = str(located.get("location_kind") or "")
    if location_kind == "match_clock_second":
        precision = str(located.get("precision") or "")
        method = str(located.get("method") or "")
        if precision == "observed_second" or method == "paddleocr_exact_clock":
            return "exact", "observed_second"
        if (
            precision == "interpolated_second"
            or method == "paddleocr_interpolated_clock"
        ):
            return "interpolated", "interpolated_second"
        if precision == "estimated_second" or method == "paddleocr_near_neighbor_estimate":
            return "estimated", "estimated_second"
        if (
            precision == "projected_second"
            or method == "paddleocr_stable_clock_mapping"
        ):
            return "projected", "projected_second"
        # Keep third-party/custom OCR worker implementations compatible while
        # remaining conservative about their claimed precision.
        return "exact", precision or "observed_second"
    if location_kind in {
        "match_clock_minute_boundary",
        "match_clock_minute_interval",
    }:
        precision = str(located.get("precision") or "minute_boundary")
        return "minute_boundary", precision
    if location_kind == "score_transition":
        return "score_transition", str(located.get("precision") or "score_transition")
    return "failed", "unverified"


def _ocr_result_locks_exact_target(located: dict[str, Any]) -> bool:
    """Whether this fragment has enough evidence to stop scanning later ones."""
    if str(located.get("location_kind") or "") != "match_clock_second":
        return False
    source, _precision = _ocr_localization_contract(located)
    return source in {"exact", "interpolated"}


def _ocr_localization_is_second_precision(source: Any) -> bool:
    """Whether a persisted source is authorized as a second-level anchor."""
    return str(source or "") in {
        # ``exact_second`` is retained for persisted rows created before the
        # evidence-grade contract was introduced.
        "exact_second",
        "exact",
        "interpolated",
        "estimated",
        "projected",
    }


def _ocr_latest_continuous_clock_run(
    samples: list[tuple[float, int]],
) -> list[tuple[float, int]]:
    """Keep the newest plausible clock/video run and discard isolated jumps."""
    ordered = sorted(set(samples))
    if len(ordered) <= 1:
        return ordered
    runs: list[list[tuple[float, int]]] = [[ordered[0]]]
    for sample in ordered[1:]:
        left_stream, left_clock = runs[-1][-1]
        right_stream, right_clock = sample
        stream_delta = right_stream - left_stream
        clock_delta = right_clock - left_clock
        same_displayed_second = clock_delta == 0 and 0 < stream_delta <= 3.0
        progressing = bool(
            clock_delta > 0
            and stream_delta > 0
            and OCR_CLOCK_MAPPING_MIN_RATE
            <= stream_delta / clock_delta
            <= OCR_CLOCK_MAPPING_MAX_RATE
        )
        if same_displayed_second or progressing:
            runs[-1].append(sample)
        else:
            runs.append([sample])
    continuous = [run for run in runs if len(run) >= OCR_CLOCK_MAPPING_MIN_SAMPLES]
    if continuous:
        return max(continuous, key=lambda run: run[-1][0])
    # With no established run, only a sole observation is displayable. Two or
    # more mutually inconsistent samples must not choose an arbitrary winner.
    return ordered if len(ordered) == 1 else []


def _ocr_progressive_merge_clock_samples(
    previous: Any,
    diagnostics: Any,
    *,
    limit: int = OCR_CLOCK_MAPPING_MAX_SAMPLES,
    default_phase: str | None = None,
) -> list[dict[str, float | int | str]]:
    """Merge trusted OCR clock readings into durable stream-time samples."""
    normalized_default_phase = _normalize_ocr_clock_phase(default_phase)
    target_period = _ocr_clock_period(normalized_default_phase)
    merged: dict[tuple[float, int], dict[str, float | int | str]] = {}

    def add(stream_time: Any, clock_seconds: Any, phase: Any = None) -> None:
        try:
            stream = float(stream_time)
            clock = int(clock_seconds)
        except (TypeError, ValueError):
            return
        if not math.isfinite(stream) or stream < 0 or clock < 0:
            return
        normalized_phase = (
            _normalize_ocr_clock_phase(phase) or normalized_default_phase
        )
        if (
            target_period is not None
            and _ocr_clock_period(normalized_phase) != target_period
        ):
            return
        key = (round(stream, 3), clock)
        existing = merged.get(key)
        if (
            existing is not None
            and existing.get("clock_phase") is not None
            and normalized_phase is None
        ):
            return
        merged[key] = {
            "stream_time": round(stream, 3),
            "match_clock_seconds": clock,
            **(
                {"clock_phase": normalized_phase}
                if normalized_phase is not None
                else {}
            ),
        }

    for sample in _ocr_progressive_clock_sample_records(
        previous,
        default_phase=normalized_default_phase,
    ):
        add(
            sample.get("stream_time"),
            sample.get("match_clock_seconds"),
            sample.get("clock_phase"),
        )
    for sample in _ocr_progressive_clock_sample_records(
        diagnostics,
        default_phase=normalized_default_phase,
    ):
        add(
            sample.get("stream_time"),
            sample.get("match_clock_seconds"),
            sample.get("clock_phase"),
        )
    ordered = sorted(merged)
    known_periods = {
        period
        for sample in merged.values()
        if (period := _ocr_clock_period(sample.get("clock_phase"))) is not None
    }
    continuity_candidates = [
        key
        for key in ordered
        if (
            not known_periods
            or len(known_periods) > 1
            or _ocr_clock_period(merged[key].get("clock_phase"))
            in known_periods
        )
    ]
    validated = (
        []
        if len(known_periods) > 1
        else _ocr_latest_continuous_clock_run(continuity_candidates)
    )
    # Keep an entirely inconsistent short set long enough for the mapping
    # diagnostics to explain pause/jump. Once a continuous run exists, discard
    # isolated outliers so they cannot poison subsequent state.
    durable = validated if validated else ordered
    bounded = durable[-max(1, min(int(limit), OCR_CLOCK_MAPPING_MAX_SAMPLES)) :]
    return [dict(merged[key]) for key in bounded]


def _ocr_target_passed_with_continuous_evidence(
    samples: Any,
    *,
    target_clock_seconds: int | None,
    clock_phase: str | None = None,
) -> bool:
    """Require a validated progressing run before declaring a target passed."""
    if target_clock_seconds is None:
        return False
    mapping = _ocr_progressive_clock_mapping(samples, clock_phase=clock_phase)
    return bool(
        mapping.get("status") == "ready"
        and int(mapping.get("right_match_clock_seconds", -1))
        >= int(target_clock_seconds)
    )


def _ocr_progressive_clock_mapping(
    samples: Any,
    *,
    clock_phase: str | None = None,
) -> dict[str, Any]:
    """Build a validated linear mapping from match-clock to stream time.

    At least two observations and one positive, sensible interval are
    required.  A replay, pause, or discontinuity therefore cannot silently
    become the localization anchor.
    """
    if not isinstance(samples, list):
        return {
            "status": "insufficient_samples",
            "reason": "samples_missing",
            "sample_count": 0,
            "accepted_sample_count": 0,
            "rejected_samples": [],
            "updated_at_unix": time.time(),
        }
    requested_phase = _normalize_ocr_clock_phase(clock_phase)
    requested_period = _ocr_clock_period(requested_phase)
    records = _ocr_progressive_clock_sample_records(samples)
    known_periods = {
        period
        for item in records
        if (period := _ocr_clock_period(item.get("clock_phase"))) is not None
    }
    if requested_period is not None:
        known_records = [
            item
            for item in records
            if _ocr_clock_period(item.get("clock_phase")) == requested_period
        ]
        # Legacy phase-less rows remain usable inside their own event. The
        # caller stamps those rows during local merge; never mix them with
        # explicitly phased samples here.
        if known_periods and not known_records:
            return {
                "status": "clock_phase_mismatch",
                "reason": "clock_samples_do_not_match_target_period",
                "sample_count": len(records),
                "accepted_sample_count": 0,
                "rejected_samples": [],
                "clock_periods": sorted(known_periods),
                "clock_phase": requested_phase,
                "clock_period": requested_period,
                "updated_at_unix": time.time(),
            }
        records = known_records if known_periods else records
    elif len(known_periods) > 1:
        return {
            "status": "clock_phase_mismatch",
            "reason": "clock_samples_span_multiple_match_periods",
            "sample_count": len(records),
            "accepted_sample_count": 0,
            "rejected_samples": [],
            "clock_periods": sorted(known_periods),
            "updated_at_unix": time.time(),
        }
    elif known_periods:
        records = [
            item
            for item in records
            if _ocr_clock_period(item.get("clock_phase")) in known_periods
        ]

    mapping_phase = requested_phase or next(
        (
            _normalize_ocr_clock_phase(item.get("clock_phase"))
            for item in reversed(records)
            if _normalize_ocr_clock_phase(item.get("clock_phase")) is not None
        ),
        None,
    )
    mapping_period = _ocr_clock_period(mapping_phase)
    parsed: list[tuple[float, int]] = []
    for item in records:
        try:
            stream = float(item.get("stream_time"))
            clock = int(item.get("match_clock_seconds"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(stream) and stream >= 0 and clock >= 0:
            parsed.append((stream, clock))
    parsed = sorted(set(parsed))[-OCR_CLOCK_MAPPING_MAX_SAMPLES:]

    def phase_fields() -> dict[str, str]:
        return {
            **({"clock_phase": mapping_phase} if mapping_phase else {}),
            **({"clock_period": mapping_period} if mapping_period else {}),
        }
    # Keep this initialized before the short-input guard; a one-sample history
    # should report insufficient data rather than raising an implementation
    # error while inspecting pause diagnostics.
    pause_runs: list[dict[str, Any]] = []
    # OCR usually emits several adjacent frames for the same displayed second.
    # Collapse those ordinary duplicate-clock readings before fitting.  Keep a
    # long same-clock run as an explicit pause diagnostic instead of treating
    # every duplicate frame as a discontinuity.
    collapsed: list[tuple[float, int]] = []
    index = 0
    while index < len(parsed):
        clock = parsed[index][1]
        run = []
        while index < len(parsed) and parsed[index][1] == clock:
            run.append(parsed[index])
            index += 1
        first_stream = run[0][0]
        last_stream = run[-1][0]
        if last_stream - first_stream > 3.0:
            pause_runs.append({
                "match_clock_seconds": clock,
                "stream_start": round(first_stream, 3),
                "stream_end": round(last_stream, 3),
                "reason": "match_clock_paused_or_video_continued",
            })
        # The midpoint reduces frame-boundary bias while retaining a stable
        # one-to-one clock sample for the linear fit.
        collapsed.append(((first_stream + last_stream) / 2.0, clock))
    parsed = collapsed
    if len(parsed) < OCR_CLOCK_MAPPING_MIN_SAMPLES:
        if pause_runs:
            return {
                "status": "rejected_pause",
                "reason": "match_clock_paused_or_video_continued",
                "sample_count": len(parsed),
                "accepted_sample_count": 0,
                "rejected_samples": pause_runs[-8:],
                **phase_fields(),
                "updated_at_unix": time.time(),
            }
        return {
            "status": "insufficient_samples",
            "reason": "at_least_two_distinct_clock_samples_required",
            "sample_count": len(parsed),
            "accepted_sample_count": len(parsed),
            "rejected_samples": pause_runs[-8:],
            **phase_fields(),
            "updated_at_unix": time.time(),
        }
    valid: list[tuple[float, int, float, int, float]] = []
    rejected: list[dict[str, Any]] = []
    for (left_stream, left_clock), (right_stream, right_clock) in zip(
        parsed, parsed[1:]
    ):
        clock_delta = right_clock - left_clock
        stream_delta = right_stream - left_stream
        if clock_delta == 0 and stream_delta > 0:
            rejected.append({
                "from_stream_time": round(left_stream, 3),
                "from_match_clock_seconds": left_clock,
                "to_stream_time": round(right_stream, 3),
                "to_match_clock_seconds": right_clock,
                "reason": "match_clock_paused_or_video_continued",
            })
            continue
        if clock_delta <= 0 or stream_delta <= 0:
            rejected.append({
                "from_stream_time": round(left_stream, 3),
                "from_match_clock_seconds": left_clock,
                "to_stream_time": round(right_stream, 3),
                "to_match_clock_seconds": right_clock,
                "reason": "clock_regressed_or_stream_rewound",
            })
            continue
        rate = stream_delta / clock_delta
        if OCR_CLOCK_MAPPING_MIN_RATE <= rate <= OCR_CLOCK_MAPPING_MAX_RATE:
            valid.append((left_stream, left_clock, right_stream, right_clock, rate))
        else:
            rejected.append({
                "from_stream_time": round(left_stream, 3),
                "from_match_clock_seconds": left_clock,
                "to_stream_time": round(right_stream, 3),
                "to_match_clock_seconds": right_clock,
                "reason": "clock_stream_rate_out_of_range",
                "rate": round(rate, 6),
            })
    if pause_runs:
        rejected.extend(pause_runs)
    if rejected:
        # A single pause or discontinuity makes one global linear mapping
        # unsafe.  Keep the reason in durable diagnostics and let the existing
        # progressive/target-rescan path handle localization.
        reason = (
            "match_clock_paused_or_video_continued"
            if any(item.get("reason") == "match_clock_paused_or_video_continued" for item in rejected)
            else "clock_stream_discontinuity"
        )
        return {
            "status": "rejected_pause" if reason == "match_clock_paused_or_video_continued" else "rejected_jump",
            "reason": reason,
            "sample_count": len(parsed),
            "accepted_sample_count": len(parsed) - len(rejected),
            "rejected_samples": rejected[-8:],
            **phase_fields(),
            "updated_at_unix": time.time(),
        }
    if not valid:
        return {
            "status": "rejected_jump",
            "reason": "no_valid_positive_clock_stream_interval",
            "sample_count": len(parsed),
            "accepted_sample_count": 0,
            "rejected_samples": [],
            **phase_fields(),
            "updated_at_unix": time.time(),
        }
    # Fit all trusted monotonic samples (rather than only the last pair) so
    # small frame timestamp jitter does not move the anchor unnecessarily.
    clocks = [clock for _, clock in parsed]
    streams = [stream for stream, _ in parsed]
    mean_clock = sum(clocks) / len(clocks)
    mean_stream = sum(streams) / len(streams)
    denominator = sum((clock - mean_clock) ** 2 for clock in clocks)
    slope = (
        sum((clock - mean_clock) * (stream - mean_stream) for stream, clock in parsed)
        / denominator
        if denominator > 0
        else valid[-1][4]
    )
    if not math.isfinite(slope) or not (
        OCR_CLOCK_MAPPING_MIN_RATE <= slope <= OCR_CLOCK_MAPPING_MAX_RATE
    ):
        return {
            "status": "rejected_jump",
            "reason": "fitted_clock_stream_rate_out_of_range",
            "sample_count": len(parsed),
            "accepted_sample_count": len(parsed),
            "rejected_samples": [],
            **phase_fields(),
            "updated_at_unix": time.time(),
        }
    intercept = mean_stream - slope * mean_clock
    left_stream, left_clock = parsed[0]
    right_stream, right_clock = parsed[-1]
    return {
        "status": "ready",
        "sample_count": len(parsed),
        "accepted_sample_count": len(parsed),
        "valid_interval_count": len(valid),
        "stream_time_per_match_second": round(slope, 6),
        "slope": round(slope, 6),
        "intercept": round(intercept, 6),
        "left_stream_time": round(left_stream, 3),
        "left_match_clock_seconds": left_clock,
        "right_stream_time": round(right_stream, 3),
        "right_match_clock_seconds": right_clock,
        **phase_fields(),
        "updated_at_unix": time.time(),
    }


def _ocr_progressive_mapped_target_window(
    samples: Any,
    *,
    target_clock_seconds: int | None,
    margin_seconds: float = OCR_PROGRESSIVE_MAPPED_TARGET_MARGIN_SECONDS,
    clock_phase: str | None = None,
) -> dict[str, Any] | None:
    """Estimate the retained-video window corresponding to a target clock."""
    if target_clock_seconds is None:
        return None
    mapping = _ocr_progressive_clock_mapping(samples, clock_phase=clock_phase)
    if mapping.get("status") != "ready":
        return None
    rate = float(mapping.get("slope") or mapping["stream_time_per_match_second"])
    intercept = float(mapping["intercept"])
    target = int(target_clock_seconds)
    left_clock = int(mapping["left_match_clock_seconds"])
    right_clock = int(mapping["right_match_clock_seconds"])
    halftime_boundary = 45 * 60
    if (
        right_clock <= halftime_boundary < target
        or target < halftime_boundary <= left_clock
    ):
        # A linear in-play clock mapping does not contain the wall-clock
        # halftime break. Wait for a trusted sample from the target half.
        return None
    outside_distance = max(left_clock - target, target - right_clock, 0)
    if outside_distance > OCR_CLOCK_MAPPING_MAX_EXTRAPOLATION_SECONDS:
        return None
    estimate = rate * target + intercept
    if not math.isfinite(estimate):
        return None
    margin = max(1.0, float(margin_seconds))
    return {
        "start_stream_time": round(max(0.0, estimate - margin), 3),
        "end_stream_time": round(estimate + margin, 3),
        "estimated_stream_time": round(estimate, 3),
        "target_clock_seconds": target,
        "margin_seconds": margin,
        "method": (
            "persistent_clock_video_interpolation"
            if left_clock <= target <= right_clock
            else "persistent_clock_video_extrapolation"
        ),
        "mapping": mapping,
    }


def _ocr_readable_mapped_target_scan_window(
    segments: list[Segment],
    target_window: Any,
    *,
    minimum_component_seconds: float = 3.0,
) -> tuple[float, float] | None:
    """Return the retained part of a mapped target window once its centre arrives."""
    if not isinstance(target_window, dict):
        return None
    try:
        mapped_start = float(target_window["start_stream_time"])
        mapped_end = float(target_window["end_stream_time"])
        estimated = float(target_window["estimated_stream_time"])
    except (KeyError, TypeError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (mapped_start, mapped_end, estimated)):
        return None
    if mapped_end <= mapped_start:
        return None
    components, _ = _continuous_search_components(
        segments,
        window_start=mapped_start,
        window_end=mapped_end,
        minimum_seconds=max(3.0, float(minimum_component_seconds)),
    )
    containing = next(
        (
            component
            for component in components
            if component.start <= estimated
            and component.end
            > estimated + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
        ),
        None,
    )
    if containing is None:
        return None
    return (
        max(mapped_start, containing.start),
        min(mapped_end, containing.end),
    )


def _ocr_has_scannable_media_after_cursor(
    segments: list[Segment],
    cursor_stream_time: Any,
    *,
    minimum_component_seconds: float = 3.0,
) -> bool:
    """Whether retained, readable video remains beyond the OCR cursor."""
    try:
        cursor = float(cursor_stream_time)
    except (TypeError, ValueError):
        return False
    if not math.isfinite(cursor):
        return False
    retained_ends = [
        float(segment.end)
        for segment in segments
        if Path(segment.path).is_file() and float(segment.end) > cursor
    ]
    if not retained_ends:
        return False
    components, _ = _continuous_search_components(
        segments,
        window_start=max(0.0, cursor),
        window_end=max(retained_ends),
        minimum_seconds=max(3.0, float(minimum_component_seconds)),
    )
    return bool(components)


def _ocr_target_media_availability(
    samples: Any,
    *,
    target_clock_seconds: int | None,
    earliest_retained_stream_time: float | None,
    clock_phase: str | None = None,
) -> dict[str, Any]:
    """Classify whether a far historical target could exist in this recording.

    The normal mapping deliberately refuses long extrapolation because it must
    not create an inaccurate anchor.  Availability needs a different, more
    conservative question: even after allowing for halftime, does the whole
    target window still fall before stream time zero?  Only that case is
    labelled as never recorded.
    """
    mapping = _ocr_progressive_clock_mapping(samples, clock_phase=clock_phase)
    if target_clock_seconds is None or mapping.get("status") != "ready":
        return {"status": "unknown", "reason": "clock_mapping_not_ready"}
    try:
        target = int(target_clock_seconds)
        slope = float(mapping.get("slope") or mapping["stream_time_per_match_second"])
        intercept = float(mapping["intercept"])
        left_clock = int(mapping["left_match_clock_seconds"])
        earliest = (
            float(earliest_retained_stream_time)
            if earliest_retained_stream_time is not None
            else None
        )
    except (KeyError, TypeError, ValueError):
        return {"status": "unknown", "reason": "clock_mapping_incomplete"}
    estimate = slope * target + intercept
    if not math.isfinite(estimate):
        return {"status": "unknown", "reason": "clock_mapping_non_finite"}
    halftime_adjustment = (
        OCR_AVAILABILITY_HALFTIME_BREAK_SECONDS
        if target <= 45 * 60 < left_clock
        else 0.0
    )
    adjusted_estimate = estimate + halftime_adjustment
    target_start = adjusted_estimate - OCR_AVAILABILITY_TARGET_MARGIN_SECONDS
    target_end = adjusted_estimate + OCR_AVAILABILITY_TARGET_MARGIN_SECONDS
    status = "retained_or_future"
    reason = "target_not_proven_missing"
    if target_end < 0.0:
        status = "before_recording"
        reason = "target_window_ends_before_stream_origin"
    elif earliest is not None and target_end <= earliest:
        status = "history_unavailable"
        reason = "target_window_ends_before_retained_head"
    return {
        "status": status,
        "reason": reason,
        "target_clock_seconds": target,
        "estimated_stream_time": round(adjusted_estimate, 3),
        "uncorrected_estimated_stream_time": round(estimate, 3),
        "halftime_adjustment_seconds": halftime_adjustment,
        "target_window_start_stream_time": round(target_start, 3),
        "target_window_end_stream_time": round(target_end, 3),
        "recording_origin_stream_time": 0.0,
        "earliest_retained_stream_time": (
            round(earliest, 3) if earliest is not None else None
        ),
        "clock_mapping": mapping,
    }


def _ocr_deadline_policy(
    task: Any,
    *,
    now_unix: float,
    target_clock_seconds: int | None,
    latest_trusted_clock_seconds: int | None,
    latest_media_end_stream_time: float | None = None,
    clock_samples: Any = None,
    has_scannable_media_after_cursor: bool | None = None,
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
        try:
            first_execution_started = float(
                queue_timing.get("execution_started_at_unix")
            )
        except (TypeError, ValueError):
            first_execution_started = math.nan
        pre_submission_wait = (
            max(
                0.0,
                first_execution_started
                - float(task.created_at_unix)
                - queue_wait,
            )
            if math.isfinite(first_execution_started)
            else 0.0
        )
        initial_cap = float(task.created_at_unix) + OCR_TARGET_WAIT_INITIAL_SECONDS
        initial_deadline = min(float(task.deadline_at_unix), initial_cap)
        policy.update({
            "policy_version": 2,
            "phase": "target_wait",
            "initial_target_wait_seconds": OCR_TARGET_WAIT_INITIAL_SECONDS,
            "target_wait_margin_seconds": OCR_TARGET_WAIT_MARGIN_SECONDS,
            "event_hard_limit_seconds": OCR_EVENT_HARD_LIMIT_SECONDS,
            "initial_target_deadline_at_unix": initial_deadline,
            "target_deadline_at_unix": initial_deadline,
            "hard_deadline_at_unix": (
                float(task.created_at_unix) + OCR_EVENT_HARD_LIMIT_SECONDS
            ),
            "first_execution_started_at_unix": (
                first_execution_started
                if math.isfinite(first_execution_started)
                else None
            ),
            "pre_submission_wait_accounted_seconds": pre_submission_wait,
        })
        if pre_submission_wait > 0:
            for field in (
                "initial_target_deadline_at_unix",
                "target_deadline_at_unix",
                "hard_deadline_at_unix",
            ):
                policy[field] = float(policy[field]) + pre_submission_wait

    # Policies are persisted in the task row.  Upgrade an in-flight task that
    # was created with the old 180-second watchdog without resetting its
    # already-accounted queue/OCR execution time.  If the old target deadline
    # had reached that watchdog, carry it forward as well; otherwise a process
    # restart would still fail at the old limit despite the new configuration.
    stored_limit_raw = policy.get("event_hard_limit_seconds")
    try:
        stored_limit = float(stored_limit_raw)
    except (TypeError, ValueError):
        stored_limit = OCR_EVENT_HARD_LIMIT_SECONDS
    if stored_limit < OCR_EVENT_HARD_LIMIT_SECONDS:
        old_hard_deadline = float(policy["hard_deadline_at_unix"])
        hard_extension = OCR_EVENT_HARD_LIMIT_SECONDS - stored_limit
        policy["hard_deadline_at_unix"] = old_hard_deadline + hard_extension
        target_deadline = policy.get("target_deadline_at_unix")
        if target_deadline is not None and float(target_deadline) >= old_hard_deadline - 0.001:
            policy["target_deadline_at_unix"] = float(target_deadline) + hard_extension
        policy["event_hard_limit_seconds"] = OCR_EVENT_HARD_LIMIT_SECONDS

    # A readiness check can create and persist the deadline policy before the
    # visual worker is submitted.  In that case the initial policy branch
    # above has no execution timestamp and cannot account for the time between
    # task creation and the first queue submission.  Apply that compensation
    # once as soon as the first execution timestamp becomes available.  Keep a
    # separate timestamp marker so later retries do not count the same wait a
    # second time.
    first_execution_started_raw = policy.get("first_execution_started_at_unix")
    try:
        first_execution_started = float(first_execution_started_raw)
    except (TypeError, ValueError):
        first_execution_started = math.nan
    queue_execution_started_raw = queue_timing.get("execution_started_at_unix")
    try:
        queue_execution_started = float(queue_execution_started_raw)
    except (TypeError, ValueError):
        queue_execution_started = math.nan
    if (
        math.isfinite(first_execution_started)
        and policy.get("pre_submission_wait_accounted_seconds") is None
    ):
        pre_submission_wait = max(
            0.0,
            first_execution_started
            - float(task.created_at_unix)
            - queue_wait,
        )
        policy["pre_submission_wait_accounted_seconds"] = pre_submission_wait
        if pre_submission_wait > 0:
            for field in (
                "initial_target_deadline_at_unix",
                "target_deadline_at_unix",
                "hard_deadline_at_unix",
            ):
                if policy.get(field) is not None:
                    policy[field] = float(policy[field]) + pre_submission_wait
    elif (
        not math.isfinite(first_execution_started)
        and math.isfinite(queue_execution_started)
    ):
        # Policies persisted before the first worker execution have no marker
        # at all.  This branch intentionally handles both missing legacy
        # fields and the explicit ``None`` marker from the initial policy.
        first_execution_started = queue_execution_started
        pre_submission_wait = max(
            0.0,
            first_execution_started
            - float(task.created_at_unix)
            - queue_wait,
        )
        policy["first_execution_started_at_unix"] = first_execution_started
        policy["pre_submission_wait_accounted_seconds"] = pre_submission_wait
        if pre_submission_wait > 0:
            for field in (
                "initial_target_deadline_at_unix",
                "target_deadline_at_unix",
                "hard_deadline_at_unix",
            ):
                if policy.get(field) is not None:
                    policy[field] = float(policy[field]) + pre_submission_wait
    try:
        policy_version = int(policy.get("policy_version") or 0)
    except (TypeError, ValueError):
        policy_version = 0
    policy["policy_version"] = max(2, policy_version)

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

    # A trusted OCR clock is not the only signal that live media is advancing.
    # In particular, a missing/temporary scoreboard ROI can leave
    # ``latest_trusted_clock_seconds`` unset while new TS segments continue to
    # arrive.  Persist the media tail and grant one bounded extension whenever
    # it advances since the previous readiness check.  The first observation
    # establishes a baseline; subsequent growth is what earns extra time.
    media_progress = False
    previous_media_end_raw = state.get("latest_media_end_stream_time")
    try:
        previous_media_end = float(previous_media_end_raw)
    except (TypeError, ValueError):
        previous_media_end = math.nan
    try:
        current_media_end = (
            float(latest_media_end_stream_time)
            if latest_media_end_stream_time is not None
            else math.nan
        )
    except (TypeError, ValueError):
        current_media_end = math.nan
    if math.isfinite(current_media_end):
        media_progress = bool(
            math.isfinite(previous_media_end)
            and current_media_end
            > previous_media_end + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
        )
        policy["latest_media_end_stream_time"] = round(current_media_end, 3)
    policy["media_progress_observed"] = bool(
        policy.get("media_progress_observed") or media_progress
    )
    policy["media_progress_current"] = media_progress
    if media_progress:
        policy["last_media_progress_at_unix"] = float(now_unix)
    elif math.isfinite(current_media_end):
        # The first observation is a baseline.  Subsequent unchanged tails
        # are measured from the last observed growth, not from task creation.
        policy.setdefault("last_media_progress_at_unix", float(now_unix))
    if has_scannable_media_after_cursor is None:
        has_scannable_media_after_cursor = bool(
            policy.get("has_scannable_media_after_cursor")
        )
    else:
        has_scannable_media_after_cursor = bool(
            has_scannable_media_after_cursor
        )
    policy["has_scannable_media_after_cursor"] = (
        has_scannable_media_after_cursor
    )
    terminal_wait_reason = policy.get("target_wait_terminal_reason")
    if (
        has_scannable_media_after_cursor
        and terminal_wait_reason in {
            "ocr_clock_paused_timeout",
            "ocr_target_media_stalled",
        }
    ):
        recovered_terminal_reason = str(terminal_wait_reason)
        policy.pop("target_wait_terminal_reason", None)
        terminal_wait_reason = None
        policy["terminal_wait_recovered_reason"] = recovered_terminal_reason
        policy["terminal_wait_recovered_at_unix"] = float(now_unix)
        policy["target_deadline_at_unix"] = min(
            float(policy["hard_deadline_at_unix"]),
            max(
                float(policy["target_deadline_at_unix"]),
                float(now_unix) + OCR_TARGET_WAIT_MARGIN_SECONDS,
            ),
        )
    if media_progress and latest_trusted_clock_seconds is None and not terminal_wait_reason:
        previous_deadline = float(policy["target_deadline_at_unix"])
        proposed_deadline = float(now_unix) + OCR_MEDIA_PROGRESS_EXTENSION_SECONDS
        hard_deadline = float(policy["hard_deadline_at_unix"])
        policy["target_deadline_at_unix"] = min(
            hard_deadline,
            max(previous_deadline, proposed_deadline),
        )
        policy["media_progress_extension_seconds"] = round(
            float(policy.get("media_progress_extension_seconds") or 0.0)
            + max(0.0, policy["target_deadline_at_unix"] - previous_deadline),
            3,
        )
        policy["last_media_progress_at_unix"] = float(now_unix)

    # A target wait is wall-clock time spent waiting for new live media, not
    # time spent materializing a scan window or running OCR.  A slow first
    # scan used to consume the whole target deadline; when it returned a
    # trustworthy 56:36 for a 57:00 target, the task failed immediately even
    # though several minutes of newer media had entered the rolling buffer.
    # Persist an incremental accounting cursor so each execution interval is
    # excluded exactly once, including across process restarts.
    execution_started_raw = state.get("execution_started_at_unix")
    execution_completed_raw = state.get("last_execution_completed_at_unix")
    try:
        execution_started = float(execution_started_raw)
    except (TypeError, ValueError):
        execution_started = math.nan
    try:
        execution_completed = float(execution_completed_raw)
    except (TypeError, ValueError):
        execution_completed = math.nan
    execution_extension = 0.0
    if math.isfinite(execution_started):
        execution_end = float(now_unix)
        if (
            math.isfinite(execution_completed)
            and execution_completed >= execution_started
        ):
            execution_end = min(execution_end, execution_completed)
        accounted_start_raw = policy.get("execution_accounted_start_at_unix")
        accounted_until_raw = policy.get("execution_accounted_until_unix")
        try:
            accounted_start = float(accounted_start_raw)
        except (TypeError, ValueError):
            accounted_start = math.nan
        try:
            accounted_until = float(accounted_until_raw)
        except (TypeError, ValueError):
            accounted_until = math.nan
        if (
            not math.isfinite(accounted_start)
            or abs(accounted_start - execution_started) > 0.001
        ):
            accounted_until = execution_started
        execution_extension = max(
            0.0,
            execution_end - max(execution_started, accounted_until),
        )
        if execution_extension > 0:
            for field in (
                "initial_target_deadline_at_unix",
                "target_deadline_at_unix",
                "hard_deadline_at_unix",
                "postroll_deadline_at_unix",
                "postroll_hard_deadline_at_unix",
            ):
                if policy.get(field) is not None:
                    policy[field] = float(policy[field]) + execution_extension
        policy["execution_accounted_start_at_unix"] = execution_started
        policy["execution_accounted_until_unix"] = max(
            execution_started,
            execution_end,
        )
    policy["ocr_execution_accounted_seconds"] = max(
        0.0,
        float(policy.get("ocr_execution_accounted_seconds") or 0.0)
        + execution_extension,
    )

    gap_seconds: int | None = None
    if (
        target_clock_seconds is not None
        and latest_trusted_clock_seconds is not None
    ):
        gap_seconds = max(
            0,
            int(target_clock_seconds) - int(latest_trusted_clock_seconds),
        )
        # A fresh OCR sample establishes how much live playback remains until
        # the target. A scan that ran while new source-video arrived is not a
        # true target-wait timeout: the source tail has advanced, but this
        # worker has only inspected its earlier snapshot. Permit that one
        # post-scan handoff to refresh the target wait. Stalled retries still
        # take the explicit final-scan/failure path.
        # Historical telemetry must not revive a later stalled retry.
        media_progress = bool(policy.get("media_progress_current"))
        if (
            not terminal_wait_reason
            and (
                float(now_unix) < float(policy["target_deadline_at_unix"])
                or media_progress
            )
        ):
            proposed_deadline = (
                float(now_unix) + gap_seconds + OCR_TARGET_WAIT_MARGIN_SECONDS
            )
            policy["target_deadline_at_unix"] = min(
                float(policy["hard_deadline_at_unix"]),
                max(float(policy["target_deadline_at_unix"]), proposed_deadline),
            )

    # Stop a readiness loop when its evidence has been stationary for long
    # enough.  This is checked after OCR execution accounting and target-gap
    # extensions so a slow scan cannot accidentally revive a stalled source.
    media_stall_since = policy.get("last_media_progress_at_unix")
    try:
        media_stall_elapsed = (
            max(0.0, float(now_unix) - float(media_stall_since))
            if media_stall_since is not None
            else 0.0
        )
    except (TypeError, ValueError):
        media_stall_elapsed = 0.0
    target_passed = _ocr_target_passed_with_continuous_evidence(
        state.get("clock_samples") if clock_samples is None else clock_samples,
        target_clock_seconds=target_clock_seconds,
    )
    target_still_future = not target_passed
    media_stalled = bool(
        target_still_future
        and math.isfinite(current_media_end)
        and media_stall_elapsed >= OCR_MEDIA_STALL_TIMEOUT_SECONDS
        and not has_scannable_media_after_cursor
    )
    previous_clock = state.get("latest_trusted_clock_seconds")
    clock_changed = bool(
        isinstance(previous_clock, int)
        and isinstance(latest_trusted_clock_seconds, int)
        and latest_trusted_clock_seconds > previous_clock
    )
    if clock_changed:
        policy["last_clock_progress_at_unix"] = float(now_unix)
    elif isinstance(latest_trusted_clock_seconds, int):
        policy.setdefault("last_clock_progress_at_unix", float(now_unix))
    clock_progress_at = policy.get("last_clock_progress_at_unix")
    try:
        clock_pause_elapsed = (
            max(0.0, float(now_unix) - float(clock_progress_at))
            if clock_progress_at is not None
            else 0.0
        )
    except (TypeError, ValueError):
        clock_pause_elapsed = 0.0
    clock_paused = bool(
        target_clock_seconds is not None
        and isinstance(latest_trusted_clock_seconds, int)
        and latest_trusted_clock_seconds < int(target_clock_seconds)
        and len(state.get("clock_samples") or []) >= 2
        and clock_pause_elapsed >= OCR_CLOCK_PAUSE_TIMEOUT_SECONDS
        and not has_scannable_media_after_cursor
    )
    policy["media_stall_elapsed_seconds"] = round(media_stall_elapsed, 3)
    policy["clock_pause_elapsed_seconds"] = round(clock_pause_elapsed, 3)
    policy["media_stalled"] = media_stalled
    policy["clock_paused"] = clock_paused
    if media_stalled or clock_paused:
        terminal_wait_reason = (
            "ocr_clock_paused_timeout" if clock_paused
            else "ocr_target_media_stalled"
        )
        policy["target_wait_terminal_reason"] = terminal_wait_reason
        policy["target_deadline_at_unix"] = min(
            float(policy["target_deadline_at_unix"]), float(now_unix)
        )
    policy.update({
        "phase": "target_wait",
        "target_clock_seconds": target_clock_seconds,
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "target_clock_gap_seconds": gap_seconds,
        "updated_at_unix": float(now_unix),
    })
    return policy


def _ocr_target_wait_failure(
    state: dict[str, Any],
    *,
    target_clock_seconds: int | None,
    latest_trusted_clock_seconds: int,
    latest_media_end_stream_time: float | None,
    diagnostics: Any = None,
) -> tuple[str, str, dict[str, Any]]:
    """Explain why live media failed to reach a still-future clock target."""
    gap_seconds = max(
        0,
        int(target_clock_seconds or 0) - int(latest_trusted_clock_seconds),
    )
    clock_samples = _ocr_progressive_merge_clock_samples(
        state.get("clock_samples"), diagnostics
    )
    clock_mapping = _ocr_progressive_clock_mapping(clock_samples)
    deadline_policy = state.get("deadline_policy")
    if not isinstance(deadline_policy, dict):
        deadline_policy = {}
    previous_media_end_raw = state.get("latest_media_end_stream_time")
    try:
        previous_media_end = float(previous_media_end_raw)
    except (TypeError, ValueError):
        previous_media_end = math.nan
    current_media_end = (
        float(latest_media_end_stream_time)
        if latest_media_end_stream_time is not None
        else math.nan
    )
    media_stalled = bool(
        deadline_policy.get("media_stalled")
        or (
        math.isfinite(previous_media_end)
        and math.isfinite(current_media_end)
        and current_media_end
        <= previous_media_end + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
        )
    )
    clock_paused = bool(
        deadline_policy.get("clock_paused")
        or clock_mapping.get("status") == "rejected_pause"
    )
    details = {
        "target_wait_outcome": (
            "clock_paused"
            if clock_paused
            else "media_stalled"
            if media_stalled
            else "target_media_not_arrived"
        ),
        "target_clock_seconds": target_clock_seconds,
        "target_clock": _clock_text_from_seconds(target_clock_seconds),
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "latest_trusted_clock": _clock_text_from_seconds(
            latest_trusted_clock_seconds
        ),
        "target_clock_gap_seconds": gap_seconds,
        "latest_media_end_stream_time": latest_media_end_stream_time,
        "previous_media_end_stream_time": (
            previous_media_end if math.isfinite(previous_media_end) else None
        ),
        "clock_mapping": clock_mapping,
    }
    if clock_paused:
        return (
            "ocr_clock_paused_timeout",
            (
                "the scoreboard clock stopped advancing before the requested "
                f"time (latest {_clock_text_from_seconds(latest_trusted_clock_seconds)}, "
                f"target {_clock_text_from_seconds(target_clock_seconds)}, "
                f"{gap_seconds} seconds remaining)"
            ),
            details,
        )
    if media_stalled:
        return (
            "ocr_target_media_stalled",
            (
                "the live video buffer stopped growing before the requested "
                f"match clock arrived (latest {_clock_text_from_seconds(latest_trusted_clock_seconds)}, "
                f"target {_clock_text_from_seconds(target_clock_seconds)}, "
                f"{gap_seconds} seconds remaining)"
            ),
            details,
        )
    return (
        "ocr_target_media_not_arrived",
        (
            "the requested match clock did not enter the retained live video "
            f"before the wait budget ended (latest {_clock_text_from_seconds(latest_trusted_clock_seconds)}, "
            f"target {_clock_text_from_seconds(target_clock_seconds)}, "
            f"{gap_seconds} seconds remaining)"
        ),
        details,
    )


def _ocr_target_not_located_diagnostics(
    diagnostics: Any,
    *,
    target_clock_seconds: int | None,
    latest_trusted_clock_seconds: int | None,
    coverage_diagnostics: dict[str, Any] | None = None,
    clock_samples: Any = None,
) -> dict[str, Any]:
    """Classify a crossed/unverified OCR target without changing its error kind.

    ``ocr_clock_target_not_located`` is intentionally kept as the stable
    terminal error for callers that already depend on it.  The information
    needed to explain that error is collected in several nested OCR payloads,
    so normalize it here into a small, durable contract for the worker and UI.
    """
    source = diagnostics if isinstance(diagnostics, dict) else {}
    coverage = (
        coverage_diagnostics
        if isinstance(coverage_diagnostics, dict)
        else source.get("coverage_diagnostics")
        if isinstance(source.get("coverage_diagnostics"), dict)
        else {}
    )

    # Prefer explicit OCR failure metadata, then inspect the nested payload
    # emitted by scoreboard_ocr_worker.
    exact_reason = str(
        source.get("exact_second_failure_cause")
        or source.get("target_failure_cause")
        or source.get("exact_second_failure_reason")
        or ""
    ).strip().lower()
    nested = source.get("ocr")
    if isinstance(nested, dict):
        nested_diag = nested.get("diagnostics")
        if isinstance(nested_diag, dict):
            exact_reason = str(
                nested_diag.get("exact_second_failure_cause")
                or nested_diag.get("target_failure_cause")
                or nested_diag.get("exact_second_failure_reason")
                or exact_reason
            ).strip().lower()

    coverage_class = str(coverage.get("coverage_class") or "").strip().lower()
    isolated_count = source.get("isolated_target_reading_count")
    try:
        isolated_count = int(isolated_count or 0)
    except (TypeError, ValueError):
        isolated_count = 0
    evidence_samples = _ocr_progressive_merge_clock_samples(
        clock_samples,
        diagnostics,
    )
    target_passed = _ocr_target_passed_with_continuous_evidence(
        evidence_samples,
        target_clock_seconds=target_clock_seconds,
    )

    if coverage_class in {"history_unavailable", "window_evicted"} or bool(
        coverage.get("target_history_fully_missing")
    ):
        cause = "window_evicted"
        explanation = "目标对应的视频窗口已经不在保留缓存中，无法再次扫描。"
    elif coverage_class in {"media_stalled", "video_stalled"} or bool(
        coverage.get("media_stalled")
    ):
        cause = "media_stalled"
        explanation = "缓存尾部在扫描期间没有继续增长，暂时没有新的画面可定位。"
    elif exact_reason in {
        "isolated_target_reading",
        "isolated_target",
        "single_frame_target",
    } or isolated_count > 0:
        cause = "isolated"
        explanation = "OCR 只在单帧读到了目标时间，缺少连续帧证据，因此没有把它当作可靠锚点。"
    elif exact_reason in {
        "continuity_rejected",
        "discontinuous_clock",
        "clock_mapping_unverified",
        "paused_or_accelerated_clock",
    } or coverage_class in {
        "video_gap",
        "clock_unreadable",
        "clock_target_unverified",
    }:
        cause = "continuity"
        explanation = "目标附近的时钟读数不连续或视频存在间断，无法安全插值到目标秒。"
    elif not target_passed and (
        coverage_class in {"clock_target_not_reached", "waiting_for_media"}
        or latest_trusted_clock_seconds is None
    ):
        cause = "target_not_reached"
        explanation = "最近一次可信时钟还没有达到 API 事件的目标时间。"
    elif target_passed:
        cause = "target_passed"
        explanation = "OCR 已经越过目标时间，但没有找到可验证的直接读数或连续插值锚点。"
    else:
        cause = "unreadable"
        explanation = "目标附近没有足够可读的时钟证据，无法确认目标秒。"

    outcome = {
        # Crossing the target is an observable fact independent of why the
        # anchor was rejected (isolated frame, discontinuity, etc.).
        "target_passed_without_anchor": target_passed,
        "target_failure_cause": cause,
        "target_failure_explanation": explanation,
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "latest_trusted_clock": _clock_text_from_seconds(
            latest_trusted_clock_seconds
        ),
        "target_clock_seconds": target_clock_seconds,
        "target_clock": _clock_text_from_seconds(target_clock_seconds),
        "target_wait_outcome": (
            "clock_passed_without_anchor"
            if target_passed
            else "media_stalled"
            if cause == "media_stalled"
            else "target_media_not_arrived"
            if cause == "target_not_reached"
            else None
        ),
    }
    if coverage:
        outcome["target_failure_coverage_class"] = coverage_class or None
        outcome["target_failure_scan_stage"] = (
            source.get("scan_stage")
            or source.get("stage")
            or coverage.get("scan_stage")
            or "ocr_progressive_scan"
        )
        outcome["target_history_fully_missing"] = bool(
            coverage.get("target_history_fully_missing")
        )
        outcome["media_stalled"] = bool(coverage.get("media_stalled"))
    else:
        outcome["target_failure_scan_stage"] = (
            source.get("scan_stage")
            or source.get("stage")
            or "ocr_progressive_scan"
        )
    if isolated_count:
        outcome["isolated_target_reading_count"] = isolated_count
    if exact_reason:
        outcome["exact_second_failure_reason"] = exact_reason
    return outcome


def _ocr_progressive_coverage_diagnostics(
    retained: Any,
    *,
    intended_initial_start: float,
    requested_start: float,
    requested_end: float,
    scan_start: float,
    scan_end: float,
    target_clock_seconds: int | None,
    latest_trusted_clock_seconds: int | None,
    target_passed_with_continuous_evidence: bool = False,
    previous_media_end_stream_time: float | None = None,
    target_window: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify the media/OCR coverage state before deciding to wait or fail.

    Progressive OCR used to expose only a boolean ``history_evicted`` flag.
    That flag is useful for retention diagnostics, but it cannot distinguish
    missing history from a live tail that has not reached the requested target
    or an input stream that stopped growing.  This helper deliberately avoids
    OCR-specific assumptions and reports the observable media bounds, gaps,
    and a conservative high-level classification for the caller/UI.
    """
    epsilon = OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
    spans: list[tuple[float, float]] = []
    for segment in retained or ():
        try:
            start = float(segment.start)
            end = float(segment.end)
        except (AttributeError, TypeError, ValueError):
            continue
        if not math.isfinite(start) or not math.isfinite(end) or end <= start:
            continue
        spans.append((start, end))
    spans.sort()
    earliest = spans[0][0] if spans else None
    latest = max((end for _, end in spans), default=None)
    gaps: list[dict[str, float]] = []
    if spans:
        component_end = spans[0][1]
        for start, end in spans[1:]:
            if start - component_end > 0.5:
                gap_start = max(component_end, float(requested_start))
                gap_end = min(start, float(requested_end))
                if gap_end > gap_start:
                    gaps.append({
                        "start_stream_time": round(gap_start, 3),
                        "end_stream_time": round(gap_end, 3),
                        "duration_seconds": round(gap_end - gap_start, 3),
                    })
            component_end = max(component_end, end)

    history_missing_seconds = 0.0
    if earliest is not None:
        history_missing_seconds = max(
            0.0,
            earliest - float(intended_initial_start),
        )
    requested_history_missing = bool(
        earliest is not None
        and earliest > float(requested_start) + epsilon
    )
    target_window_start: float | None = None
    target_window_end: float | None = None
    if isinstance(target_window, dict):
        try:
            candidate_start = float(target_window.get("start_stream_time"))
            candidate_end = float(target_window.get("end_stream_time"))
        except (TypeError, ValueError):
            candidate_start = candidate_end = math.nan
        if (
            math.isfinite(candidate_start)
            and math.isfinite(candidate_end)
            and candidate_end > candidate_start
        ):
            target_window_start = candidate_start
            target_window_end = candidate_end
    if target_window_start is None:
        target_window_start = float(scan_start)
        target_window_end = float(scan_end)

    # If the whole target window ends before the retained head, the target has
    # been evicted.  The previous overlap-only check missed the exact-boundary
    # case (target_end == earliest) and misreported it as an unreadable clock.
    target_history_missing = bool(
        earliest is not None
        and (
            (
                target_window_start < earliest - epsilon
                and target_window_end > earliest + epsilon
            )
            or target_window_end <= earliest + epsilon
        )
    )
    target_history_fully_missing = bool(
        earliest is not None
        and target_window_end <= earliest + epsilon
    )
    target_media_not_arrived = bool(
        latest is None or latest <= target_window_start + epsilon
    )
    media_stalled = bool(
        previous_media_end_stream_time is not None
        and latest is not None
        and latest <= float(previous_media_end_stream_time) + epsilon
    )
    gap_intersects_target = any(
        item["end_stream_time"] > target_window_start + epsilon
        and item["start_stream_time"] < target_window_end - epsilon
        for item in gaps
    )

    if latest is None:
        coverage_class = "waiting_for_media"
    elif target_history_missing:
        coverage_class = "history_unavailable"
    elif gap_intersects_target:
        coverage_class = "video_gap"
    elif target_media_not_arrived:
        coverage_class = "media_stalled" if media_stalled else "waiting_for_media"
    elif target_clock_seconds is not None and latest_trusted_clock_seconds is None:
        coverage_class = "clock_unreadable"
    elif (
        target_clock_seconds is not None
        and latest_trusted_clock_seconds is not None
        and int(latest_trusted_clock_seconds) < int(target_clock_seconds)
    ):
        coverage_class = "clock_target_not_reached"
    elif (
        target_clock_seconds is not None
        and latest_trusted_clock_seconds is not None
        and int(latest_trusted_clock_seconds) >= int(target_clock_seconds)
        and target_passed_with_continuous_evidence
    ):
        coverage_class = "clock_passed_without_anchor"
    elif (
        target_clock_seconds is not None
        and latest_trusted_clock_seconds is not None
        and int(latest_trusted_clock_seconds) >= int(target_clock_seconds)
    ):
        coverage_class = "clock_target_unverified"
    else:
        coverage_class = "covered"

    return {
        "coverage_class": coverage_class,
        "earliest_media_start_stream_time": (
            round(earliest, 3) if earliest is not None else None
        ),
        "latest_media_end_stream_time": (
            round(latest, 3) if latest is not None else None
        ),
        "intended_initial_start_stream_time": round(
            float(intended_initial_start), 3
        ),
        "requested_search_start_stream_time": round(float(requested_start), 3),
        "requested_search_end_stream_time": round(float(requested_end), 3),
        "scan_start_stream_time": round(float(scan_start), 3),
        "scan_end_stream_time": round(float(scan_end), 3),
        "target_window_start_stream_time": round(target_window_start, 3),
        "target_window_end_stream_time": round(target_window_end, 3),
        "history_missing_seconds": round(history_missing_seconds, 3),
        "requested_history_missing": requested_history_missing,
        "target_history_missing": target_history_missing,
        "target_history_fully_missing": target_history_fully_missing,
        "target_media_not_arrived": target_media_not_arrived,
        "media_stalled": media_stalled,
        "video_gaps": gaps,
        "target_clock_seconds": target_clock_seconds,
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "target_passed_with_continuous_evidence": bool(
            target_passed_with_continuous_evidence
        ),
    }


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
    latest_media_end_stream_time: float | None,
    history_evicted: bool,
    diagnostics: dict[str, Any] | None = None,
    target_rescan_attempted: bool = False,
    suppress_target_rescan: bool = False,
    next_scan_cursor_stream_time: float | None = None,
    has_scannable_media_after_cursor: bool | None = None,
    retry_immediately: bool = False,
    now_unix: float | None = None,
) -> None:
    timestamp = time.time() if now_unix is None else float(now_unix)
    clock_phase = _ocr_job_clock_phase(job)
    previous = _ocr_progressive_state(task)
    try:
        prior_rescan_attempt_count = max(
            0, int(previous.get("target_rescan_attempt_count") or 0)
        )
    except (TypeError, ValueError):
        prior_rescan_attempt_count = 0
    clock_samples = _ocr_progressive_merge_clock_samples(
        previous.get("clock_samples"),
        diagnostics,
        default_phase=clock_phase,
    )
    if isinstance(diagnostics, dict):
        clock_samples = _ocr_progressive_merge_clock_samples(
            clock_samples,
            diagnostics.get("clock_samples"),
            default_phase=clock_phase,
        )
    previous_readiness = previous.get("clock_readiness")
    readiness = (
        dict(previous_readiness)
        if isinstance(previous_readiness, dict)
        else {}
    )
    diagnostic_readiness = (
        diagnostics.get("clock_readiness")
        if isinstance(diagnostics, dict)
        else None
    )
    if isinstance(diagnostic_readiness, dict):
        readiness.update(diagnostic_readiness)
    if job.clock_only:
        clock_samples, readiness = _ocr_match_clock_readiness(
            runtime,
            job,
            {
                **previous,
                "clock_samples": clock_samples,
                "clock_readiness": readiness,
            },
        )
    clock_mapping = _ocr_progressive_clock_mapping(
        clock_samples,
        clock_phase=clock_phase,
    )
    # Derive state from the newest validated stream-time sample. Never retain
    # a numerically larger old clock merely because it was persisted first.
    validated_latest = _latest_trusted_clock_seconds(clock_samples)
    latest_trusted_clock_seconds = (
        validated_latest
        if validated_latest is not None
        else latest_trusted_clock_seconds
        if isinstance(latest_trusted_clock_seconds, int)
        else None
    )
    target_clock_seconds = _ocr_progressive_target_seconds(job)
    target_passed = _ocr_target_passed_with_continuous_evidence(
        clock_samples,
        target_clock_seconds=target_clock_seconds,
        clock_phase=clock_phase,
    )
    deadline_policy = _ocr_deadline_policy(
        task,
        now_unix=timestamp,
        target_clock_seconds=target_clock_seconds,
        latest_trusted_clock_seconds=latest_trusted_clock_seconds,
        latest_media_end_stream_time=latest_media_end_stream_time,
        clock_samples=clock_samples,
        has_scannable_media_after_cursor=has_scannable_media_after_cursor,
    )
    target_deadline = float(deadline_policy["target_deadline_at_unix"])
    next_attempt_at = _ocr_far_target_retry_at(
        now_unix=timestamp,
        target_clock_seconds=target_clock_seconds,
        latest_trusted_clock_seconds=latest_trusted_clock_seconds,
        target_deadline_at_unix=target_deadline,
    )
    if retry_immediately:
        next_attempt_at = timestamp
    active_processing_budget = _ocr_active_processing_budget(
        previous,
        now_unix=timestamp,
        account_open_execution=diagnostics is not None,
    )
    progress = {
        **previous,
        "state": wait_kind,
        "target_revision": _vision_target_revision(job),
        "target_source": _vision_target_source(job),
        "scan_attempt_count": int(previous.get("scan_attempt_count") or 0) + 1,
        "last_scan_start_stream_time": round(scan_start, 3),
        "last_scan_end_stream_time": round(scan_end, 3),
        "scan_cursor_stream_time": round(
            scan_end
            if next_scan_cursor_stream_time is None
            else next_scan_cursor_stream_time,
            3,
        ),
        "overlap_seconds": OCR_PROGRESSIVE_OVERLAP_SECONDS,
        "latest_trusted_clock_seconds": latest_trusted_clock_seconds,
        "latest_trusted_clock": _clock_text_from_seconds(
            latest_trusted_clock_seconds
        ),
        "latest_media_end_stream_time": (
            round(float(latest_media_end_stream_time), 3)
            if latest_media_end_stream_time is not None
            else None
        ),
        "target_clock_seconds": target_clock_seconds,
        "target_clock": _clock_text_from_seconds(
            target_clock_seconds
        ),
        "history_evicted": bool(history_evicted),
        "deadline_policy": deadline_policy,
        "active_processing_budget": active_processing_budget,
        "clock_samples": clock_samples,
        "clock_mapping": clock_mapping,
        "target_rescan_attempt_count": prior_rescan_attempt_count,
    }
    readiness.update({
        "status": "ready" if clock_mapping.get("status") == "ready" else "waiting",
        "scope": "match",
        "accepted_sample_count": int(
            clock_mapping.get("accepted_sample_count") or 0
        ),
        "clock_mapping": clock_mapping,
    })
    if readiness:
        progress["clock_readiness"] = readiness
    if diagnostics:
        progress["last_scan_diagnostics"] = diagnostics
        progress["last_execution_completed_at_unix"] = timestamp
        progress["last_execution_error_kind"] = diagnostics.get("kind")
        coverage_diagnostics = diagnostics.get("coverage_diagnostics")
        if isinstance(coverage_diagnostics, dict):
            progress["coverage_diagnostics"] = dict(coverage_diagnostics)
        if target_rescan_attempted:
            completed_attempt_count = prior_rescan_attempt_count + 1
            progress["target_rescan_attempt_count"] = completed_attempt_count
            progress["target_rescan_last_completed_at_unix"] = timestamp
            if (
                completed_attempt_count >= OCR_PROGRESSIVE_TARGET_RESCAN_MAX_ATTEMPTS
                or suppress_target_rescan
            ):
                # A small bounded retry budget is enough to recover transient
                # OCR misses. Mark exhaustion durably so polling/restarts
                # cannot keep rescanning the same historical window forever.
                progress["target_rescan_completed_at_unix"] = timestamp
                progress["target_rescan_exhausted"] = True
            else:
                next_attempt = completed_attempt_count + 1
                target_window = _ocr_progressive_target_rescan_window(
                    diagnostics,
                    target_clock_seconds=target_clock_seconds,
                    margin_seconds=OCR_PROGRESSIVE_TARGET_RESCAN_EXPANDED_MARGIN_SECONDS,
                    rescan_attempt=next_attempt,
                )
                if target_window is None and target_passed:
                    target_window = _ocr_progressive_expand_target_rescan_window(
                        previous.get("target_rescan_window"),
                        target_clock_seconds=target_clock_seconds,
                        rescan_attempt=next_attempt,
                    )
                if target_window is not None and target_passed:
                    progress["target_rescan_window"] = target_window
                    progress["target_passed_without_anchor"] = True
                    progress.pop("target_rescan_completed_at_unix", None)
                    progress["target_rescan_exhausted"] = False
        elif (
            not suppress_target_rescan
            and
            prior_rescan_attempt_count < OCR_PROGRESSIVE_TARGET_RESCAN_MAX_ATTEMPTS
            and not bool(previous.get("target_rescan_exhausted"))
        ):
            target_window = _ocr_progressive_target_rescan_window(
                diagnostics,
                target_clock_seconds=target_clock_seconds,
                rescan_attempt=prior_rescan_attempt_count + 1,
            )
            if (
                target_window is None
                and target_passed
                and scan_end > scan_start
            ):
                # Some OCR backends report the accepted clock but omit the
                # frame-relative timestamp.  Keep the recovery contract
                # usable by centring the first retry on the scanned window;
                # this is only a search window, never an accepted anchor.
                target_window = _ocr_progressive_expand_target_rescan_window(
                    {
                        "estimated_stream_time": (
                            float(scan_start) + float(scan_end)
                        )
                        / 2.0
                    },
                    target_clock_seconds=target_clock_seconds,
                    rescan_attempt=prior_rescan_attempt_count + 1,
                    margin_seconds=(
                        OCR_PROGRESSIVE_TARGET_RESCAN_MARGIN_SECONDS
                        if prior_rescan_attempt_count == 0
                        else OCR_PROGRESSIVE_TARGET_RESCAN_EXPANDED_MARGIN_SECONDS
                    ),
                )
            if target_window is not None and target_passed:
                # Keep this independently of the normal forward cursor.  The
                # next worker attempt must revisit the crossed target before
                # advancing through newer media, even when the miss was only
                # temporary.
                progress["target_rescan_window"] = target_window
                progress["target_passed_without_anchor"] = True
                progress.pop("target_rescan_completed_at_unix", None)
                progress["target_rescan_exhausted"] = False
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
            # Keep the tuple's historical third value for callers that use it
            # only to choose the 30/30-second window.  Persisted results use
            # the evidence-grade source from ``_ocr_localization_contract``.
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
            "exact": "match_clock_second",
            "interpolated": "match_clock_second",
            "estimated": "match_clock_second",
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
    sample_interval_seconds: float = 1.0,
    coarse_sample_interval_seconds: float | None = (
        DEFAULT_COARSE_SAMPLE_INTERVAL_SECONDS
    ),
    cancel_event: Any = None,
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
        component_stage = "ocr_video_preparation"
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
                    timeout_seconds=OCR_FFMPEG_WATCHDOG_SECONDS,
                )
            component_stage = "ocr_clock_discovery"
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
                sample_interval_seconds=sample_interval_seconds,
                coarse_sample_interval_seconds=coarse_sample_interval_seconds,
                candidate_input_format=materialized.get("input_format"),
                candidate_seek_seconds=float(materialized.get("input_seek_seconds", 0.0)),
                candidate_duration_seconds=materialized.get("input_duration_seconds"),
                cancel_event=cancel_event,
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
            # The output window is known once a continuity-verified target
            # second is found. Do not scan disconnected fragments after that
            # point.
            if _ocr_result_locks_exact_target(located):
                break
        except subprocess.TimeoutExpired as exc:
            preparation_timeout = component_stage == "ocr_video_preparation"
            attempts.append({
                "component_index": index,
                "window_start": component.start,
                "window_end": component.end,
                "error_kind": (
                    "ocr_video_preparation_timeout"
                    if preparation_timeout
                    else "ocr_inference_failed"
                ),
                "error": (
                    "FFmpeg did not finish preparing this OCR video component "
                    f"within {OCR_FFMPEG_WATCHDOG_SECONDS:.0f} seconds"
                    if preparation_timeout
                    else "The OCR subprocess exceeded its per-process timeout"
                ),
                "diagnostics": {
                    "stage": component_stage,
                    "watchdog_seconds": (
                        OCR_FFMPEG_WATCHDOG_SECONDS
                        if preparation_timeout
                        else ocr_timeout_seconds
                    ),
                    "timeout": str(exc),
                },
            })
            continue
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
                        sample_interval_seconds=sample_interval_seconds,
                        coarse_sample_interval_seconds=(
                            coarse_sample_interval_seconds
                        ),
                        cancel_event=cancel_event,
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
                    if _ocr_result_locks_exact_target(located):
                        break
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
                            clock_only=job.clock_only,
                            sample_interval_seconds=sample_interval_seconds,
                            coarse_sample_interval_seconds=(
                                coarse_sample_interval_seconds
                            ),
                            cancel_event=cancel_event,
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
                        if _ocr_result_locks_exact_target(located):
                            break
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
        if _ocr_result_locks_exact_target(item[0])
    ]
    selected_pool = exact or matches
    if not exact:
        nearby_observed: list[tuple[tuple[dict[str, Any], dict[str, Any]], int]] = []
        for item in matches:
            located_result = item[0]
            if (
                located_result.get("degradation_mode")
                != "nearby_observed_clock"
                or located_result.get("precision")
                not in {"estimated_second", "estimated_minute_boundary"}
            ):
                continue
            distance = located_result.get("observed_clock_distance_seconds")
            if isinstance(distance, bool):
                continue
            try:
                parsed_distance = int(distance)
            except (TypeError, ValueError):
                continue
            if parsed_distance >= 0:
                nearby_observed.append((item, parsed_distance))
        if nearby_observed:
            minimum_distance = min(distance for _item, distance in nearby_observed)
            selected_pool = [
                item
                for item, distance in nearby_observed
                if distance == minimum_distance
            ]
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
                "stage": str(
                    diagnostics.get("stage") or "ocr_target_localization"
                ),
                "fragment_attempts": attempts,
                "target_clock": _clock_text_from_seconds(job.event_second),
            },
        )
    located, materialized = selected_pool[0]
    located["fragment_attempts"] = attempts
    located["ocr_clock_only"] = job.clock_only
    located["exact_target_locked"] = bool(exact)
    located["unscanned_component_count"] = max(0, len(components) - len(attempts))
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


def _encode_ocr_api_range_fallback(
    job: VisionJob,
    runtime: Any,
    segment_reader: Callable[[], list[Segment]],
    ffmpeg: str,
    ffprobe: str,
    output_dir: Path,
    *,
    failure: VisualLocationFailed,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    timeout_seconds: float,
    min_degraded_seconds: float,
    cancel_event: Any,
) -> bool:
    """Encode the closest retained API-time range after OCR localization fails."""
    failure_stage = str(
        failure.diagnostics.get("stage") or "ocr_target_localization"
    )
    if failure_stage in {
        "ocr_anchor_validation",
        "ocr_output_coverage",
        "ocr_window_encoding",
    }:
        return False
    if _ocr_target_revision_is_stale(
        runtime, job.event_key, _vision_target_revision(job)
    ):
        return False

    retained = sorted(
        (
            segment
            for segment in segment_reader()
            if Path(segment.path).is_file()
            and float(segment.end) > float(segment.start)
        ),
        key=lambda segment: (float(segment.start), float(segment.end)),
    )
    if not retained:
        return False
    earliest = min(float(segment.start) for segment in retained)
    latest = max(float(segment.end) for segment in retained)
    if latest - earliest < max(2.0, min(float(min_degraded_seconds), 10.0)):
        return False

    api_anchor = float(job.api_observed_stream_time)
    api_anchor_history_missing = api_anchor < earliest
    if api_anchor_history_missing:
        fallback_end = min(latest, earliest + OCR_API_RANGE_FALLBACK_SECONDS)
    else:
        fallback_end = min(api_anchor, latest)
    fallback_start = max(earliest, fallback_end - OCR_API_RANGE_FALLBACK_SECONDS)
    requested_seconds = OCR_API_RANGE_FALLBACK_SECONDS
    if fallback_end - fallback_start < max(2.0, min(float(min_degraded_seconds), 10.0)):
        return False

    coverage = analyze_video_coverage(
        retained,
        window_start=fallback_start,
        window_end=fallback_end,
        anchor=fallback_end,
        allow_degraded=True,
        force_degraded=True,
        min_degraded_seconds=max(2.0, min(float(min_degraded_seconds), 10.0)),
        stitch_across_gaps=True,
        allow_anchor_adjustment=True,
        max_anchor_gap_seconds=OCR_OUTPUT_MAX_ANCHOR_GAP_SECONDS,
        max_anchor_shift_seconds=OCR_API_RANGE_FALLBACK_SECONDS,
    )
    if coverage.status not in {CoverageStatus.READY_FULL, CoverageStatus.READY_DEGRADED}:
        return False
    if coverage.effective_start is None or coverage.effective_end is None:
        return False
    fallback_span_seconds = max(
        0.0, float(coverage.effective_end) - float(coverage.effective_start)
    )
    # A stitched GIF compresses gaps out of its output timeline. Report the
    # actual amount of playable video instead of the wall-clock span.
    available_seconds = max(
        0.0, fallback_span_seconds - float(coverage.skipped_gap_seconds)
    )
    if available_seconds < max(2.0, min(float(min_degraded_seconds), 10.0)):
        return False
    fallback_complete = bool(
        not api_anchor_history_missing
        and available_seconds
        >= requested_seconds * OCR_API_RANGE_FALLBACK_COMPLETE_RATIO
    )
    failure_reason = {
        "kind": failure.kind,
        "stage": failure_stage,
        "message": str(failure),
        "ocr_verified": False,
        "default_gif_preserved": True,
    }
    if api_anchor_history_missing:
        explanation = (
            "接口事件到达时对应的历史视频已被清理；"
            f"已生成当前缓存中最接近的约 {available_seconds:.1f} 秒低清片段，"
            "可能不包含事件。"
        )
    elif fallback_complete:
        explanation = (
            "没有通过画面比赛时间完成二次定位；"
            "已保留接口到达前约 120 秒的低清范围片段。"
        )
    else:
        explanation = (
            "没有通过画面比赛时间完成二次定位；现有可播放视频不足 120 秒，"
            f"已保留约 {available_seconds:.1f} 秒残缺片段，可能不包含事件。"
        )
    if coverage.skipped_gap_seconds > 0:
        explanation += (
            f" 直播源中断部分已跳过，共约 {coverage.skipped_gap_seconds:.1f} 秒。"
        )
    anchor_source = (
        job.api_observed_source_time
        + (fallback_end - job.api_observed_stream_time)
        if job.api_observed_source_time is not None
        else None
    )
    located = {
        "artifact_kind": "ocr_window",
        "stage": "ocr_api_range_fallback_ready",
        "anchor_stream_time": fallback_end,
        "anchor_source_time": anchor_source,
        "anchor_provenance": "api_observation_range_unverified",
        "locator_method": "api_time_range_fallback",
        "location_kind": "api_observation_range",
        "localization_source": "api_time_range",
        "precision": "unverified_range",
        "localization_precision": "unverified_range",
        "localization_quality": "fallback",
        "precise_location": False,
        "ocr_verified": False,
        "target_clock": _clock_text_from_seconds(
            _ocr_progressive_target_seconds(job)
        ),
        "target_clock_seconds": _ocr_progressive_target_seconds(job),
        "degraded": True,
        "localization_degraded": True,
        "degradation_mode": "api_time_range_fallback",
        "degradation_reason": failure_reason,
        "failure_reason": failure_reason,
        "fallback_explanation": explanation,
        "fallback_used": True,
        "fallback_generated": True,
        "fallback_time_range_aligned": not api_anchor_history_missing,
        "fallback_anchor_history_missing": api_anchor_history_missing,
        "minute_fallback": False,
        "fragmented_fallback": not fallback_complete,
        "fallback_complete": fallback_complete,
        "fallback_label": (
            "120_second_fallback"
            if fallback_complete
            else "history_missing_nearest_clip"
            if api_anchor_history_missing
            else "fragmented_clip"
        ),
        "requested_fallback_seconds": requested_seconds,
        "available_fallback_seconds": available_seconds,
        "clip_before_seconds": fallback_end - fallback_start,
        "clip_after_seconds": 0.0,
        "requested_media_window": {
            "start_stream_time": fallback_start,
            "end_stream_time": fallback_end,
        },
        "actual_media_window": {
            "start_stream_time": coverage.effective_start,
            "end_stream_time": coverage.effective_end,
            **_ocr_coverage_contract(coverage),
        },
        "default_gif_preserved": True,
    }

    current = _artifact_task(runtime, job.event_key, "ocr_window")
    if current is None or current.status in {"encoded", "failed", "encoding"}:
        return False
    if current.status == "pending":
        _artifact_transition(
            runtime,
            job.event_key,
            "ocr_window",
            "locating",
            reason="api_time_range_fallback",
        )
        current = _artifact_task(runtime, job.event_key, "ocr_window")
    if current is None or current.status != "locating":
        return False
    _artifact_transition(
        runtime,
        job.event_key,
        "ocr_window",
        "located",
        result=located,
        reason="api_time_range_fallback_ready",
    )

    lease_id = runtime.store.acquire_segment_lease(
        job.event_key,
        [str(segment.path.resolve()) for segment in coverage.segments],
        artifact_kind="ocr_window",
        owner="ocr-api-range-fallback-encoder",
        ttl_seconds=max(timeout_seconds + 60.0, 180.0),
    )
    try:
        latest_task = runtime.store.get(job.event_key)
        if latest_task is None:
            return False
        pending = PendingEvent(
            event_type=f"{job.event_type}_ocr_range_fallback",
            stream_time=fallback_end,
            source_time=anchor_source,
            detected_wall_time=job.detected_at_unix,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=fallback_end,
            output_id=job.event_key.rsplit(":", 1)[-1][:8],
        )
        _artifact_transition(
            runtime,
            job.event_key,
            "ocr_window",
            "encoding",
            result=located,
            reason="api_time_range_fallback_encoding",
        )
        encoded = encode_gif(
            ffmpeg,
            ffprobe,
            retained,
            pending,
            output_dir,
            before=fallback_end - fallback_start,
            after=0.0,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            cancel_event=cancel_event,
            coverage=coverage,
            output_filename=build_gif_filename(
                match_id=latest_task.match_id,
                event_data=latest_task.event_data,
                variant="ocr-fallback",
            ),
            timeout_seconds=max(OCR_FFMPEG_WATCHDOG_SECONDS, timeout_seconds),
        )
        encoded.update(
            {
                **located,
                "stage": "ocr_api_range_fallback_encoded",
                "progressive_status": "ocr_range_fallback",
                "output_kind": "api_time_range_fallback",
                "output_width": width,
                "output_fps": fps,
                "output_colors": colors,
                **_ocr_coverage_contract(coverage),
                "coverage_degraded": coverage.degraded,
                "localization_degraded": True,
                "degraded": True,
            }
        )
        _artifact_transition(
            runtime,
            job.event_key,
            "ocr_window",
            "encoded",
            result=encoded,
            reason="api_time_range_fallback_encoded",
        )
        return True
    finally:
        runtime.store.release_segment_lease(lease_id)


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
    target_revision = _vision_target_revision(job)
    current = _artifact_task(runtime, job.event_key, artifact_kind)
    if current is None:
        _release_ocr_output_window_leases(
            runtime,
            job.event_key,
            target_revision=target_revision,
        )
        return True
    current = ensure_ocr_target_revision(runtime, job)
    if current is None or current.status in {"encoded", "failed"}:
        _release_ocr_output_window_leases(
            runtime,
            job.event_key,
            target_revision=target_revision,
        )
        return True
    if time.time() < current.next_attempt_at_unix:
        return False
    lease_id: str | None = None
    cached_scoreboard_roi: Any | None = None
    try:
        # Capture the revision before any expensive materialization.  The
        # event loop may advance the target while this worker is running.
        worker_target_revision = target_revision
        job, cached_scoreboard_roi = _job_with_cached_scoreboard_profile(
            runtime,
            job,
        )
        clock_phase = _ocr_job_clock_phase(job)
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
            allowed_sources = {
                "exact_second",  # legacy persisted rows
                "exact",
                "interpolated",
                "estimated",
                "projected",
                "minute_boundary",
            }
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
            if job.clock_only and progressive_scan:
                readiness_samples, clock_readiness = _ocr_match_clock_readiness(
                    runtime,
                    job,
                    state,
                )
                state = {
                    **state,
                    "clock_samples": readiness_samples,
                    "latest_trusted_clock_seconds": (
                        _latest_trusted_clock_seconds(readiness_samples)
                    ),
                    "clock_mapping": _ocr_progressive_clock_mapping(
                        readiness_samples,
                        clock_phase=clock_phase,
                    ),
                    "clock_readiness": clock_readiness,
                }
            active_processing_budget = _ocr_active_processing_budget(state)
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
            clock_samples = list(state.get("clock_samples") or [])
            target_passed = _ocr_target_passed_with_continuous_evidence(
                clock_samples,
                target_clock_seconds=target_clock_seconds,
                clock_phase=clock_phase,
            )
            mapped_target_window = _ocr_progressive_mapped_target_window(
                clock_samples,
                target_clock_seconds=target_clock_seconds,
                clock_phase=clock_phase,
            )
            target_rescan_window = state.get("target_rescan_window")
            try:
                target_rescan_attempt_count = max(
                    0, int(state.get("target_rescan_attempt_count") or 0)
                )
            except (TypeError, ValueError):
                target_rescan_attempt_count = 0
            target_rescan_pending = (
                isinstance(target_rescan_window, dict)
                and state.get("target_rescan_completed_at_unix") is None
                and not bool(state.get("target_rescan_exhausted"))
                and target_rescan_attempt_count < OCR_PROGRESSIVE_TARGET_RESCAN_MAX_ATTEMPTS
            )
            previous_media_end_raw = state.get("latest_media_end_stream_time")
            try:
                previous_media_end = float(previous_media_end_raw)
            except (TypeError, ValueError):
                previous_media_end = None
            has_scannable_media_after_cursor = (
                _ocr_has_scannable_media_after_cursor(
                    retained,
                    cursor,
                    minimum_component_seconds=max(3.0, min_degraded_seconds),
                )
            )
            deadline_policy = _ocr_deadline_policy(
                current,
                now_unix=now_unix,
                target_clock_seconds=target_clock_seconds,
                latest_trusted_clock_seconds=latest_trusted,
                latest_media_end_stream_time=latest_end,
                clock_samples=clock_samples,
                has_scannable_media_after_cursor=(
                    has_scannable_media_after_cursor
                ),
            )
            target_deadline = float(deadline_policy["target_deadline_at_unix"])
            deadline_reached = now_unix >= target_deadline
            final_scan_completed = bool(state.get("final_scan_completed_at_unix"))
            force_final_scan = False
            mapped_target_ready = False
            readiness = state.get("clock_readiness")
            readiness = dict(readiness) if isinstance(readiness, dict) else {}
            readiness_probe = bool(
                job.clock_only
                and progressive_scan
                and _ocr_progressive_clock_mapping(
                    clock_samples,
                    clock_phase=clock_phase,
                ).get("status")
                != "ready"
                and latest_end is not None
            )
            readiness_probe_growth: float | None = None
            if readiness_probe:
                try:
                    last_probe_tail = float(
                        readiness.get("last_probe_media_end_stream_time")
                    )
                except (TypeError, ValueError):
                    last_probe_tail = None
                if (
                    last_probe_tail is not None
                    and float(latest_end)
                    < last_probe_tail - OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                ):
                    readiness["shared_probe_ignored_reason"] = (
                        "media_timeline_rewound"
                    )
                    last_probe_tail = None
                if last_probe_tail is not None:
                    readiness_probe_growth = max(
                        0.0, float(latest_end) - last_probe_tail
                    )
                window_end = float(latest_end)
                window_start = max(
                    float(earliest_start or 0.0),
                    window_end - OCR_CLOCK_READINESS_PROBE_SECONDS,
                )
            coverage_diagnostics = _ocr_progressive_coverage_diagnostics(
                retained,
                intended_initial_start=intended_initial_start,
                requested_start=float(current.search_start_stream_time),
                requested_end=float(current.search_end_stream_time),
                scan_start=window_start,
                scan_end=window_end,
                target_clock_seconds=target_clock_seconds,
                latest_trusted_clock_seconds=latest_trusted,
                target_passed_with_continuous_evidence=target_passed,
                previous_media_end_stream_time=previous_media_end,
                target_window=(
                    target_rescan_window
                    if target_rescan_pending
                    else mapped_target_window
                    if isinstance(mapped_target_window, dict)
                    else target_rescan_window
                ),
            )
            target_media_availability = _ocr_target_media_availability(
                clock_samples,
                target_clock_seconds=target_clock_seconds,
                earliest_retained_stream_time=earliest_start,
                clock_phase=clock_phase,
            )
            coverage_diagnostics["target_media_availability"] = (
                target_media_availability
            )
            if target_media_availability.get("status") == "before_recording":
                raise VisualLocationFailed(
                    "ocr_target_before_recording",
                    "the requested match clock occurred before this recording began",
                    {
                        "stage": "ocr_target_media_availability",
                        "target_clock_seconds": target_clock_seconds,
                        "latest_trusted_clock_seconds": latest_trusted,
                        "target_media_availability": target_media_availability,
                        "coverage_diagnostics": coverage_diagnostics,
                        "default_gif_preserved": True,
                    },
                )
            if target_media_availability.get("status") == "history_unavailable":
                raise VisualLocationFailed(
                    "ocr_search_history_evicted",
                    "the requested match-clock window is older than the retained video",
                    {
                        "stage": "ocr_target_media_availability",
                        "target_clock_seconds": target_clock_seconds,
                        "latest_trusted_clock_seconds": latest_trusted,
                        "target_media_availability": target_media_availability,
                        "coverage_diagnostics": coverage_diagnostics,
                        "default_gif_preserved": True,
                    },
                )

            if (
                readiness_probe
                and readiness_probe_growth is not None
                and readiness_probe_growth
                < OCR_CLOCK_READINESS_MIN_MEDIA_GROWTH_SECONDS
                and not deadline_reached
            ):
                _ocr_progressive_wait(
                    runtime,
                    job,
                    current,
                    wait_kind="waiting_for_clock_readiness",
                    message="waiting for at least 15 seconds of new video before checking whether the match clock is visible again",
                    scan_start=window_start,
                    scan_end=window_end,
                    latest_trusted_clock_seconds=latest_trusted,
                    latest_media_end_stream_time=latest_end,
                    history_evicted=history_evicted,
                    diagnostics={
                        "stage": "ocr_clock_readiness",
                        "clock_samples": clock_samples,
                        "clock_readiness": {
                            **readiness,
                            "status": "waiting",
                            "current_media_end_stream_time": round(
                                float(latest_end), 3
                            ),
                            "media_growth_since_last_probe_seconds": round(
                                readiness_probe_growth, 3
                            ),
                            "required_media_growth_seconds": (
                                OCR_CLOCK_READINESS_MIN_MEDIA_GROWTH_SECONDS
                            ),
                        },
                        "coverage_diagnostics": coverage_diagnostics,
                    },
                    next_scan_cursor_stream_time=last_probe_tail,
                    has_scannable_media_after_cursor=(
                        has_scannable_media_after_cursor
                    ),
                    now_unix=now_unix,
                )
                return False

            # Once two continuous OCR observations establish a clock/video
            # mapping, avoid replaying the whole retained history.  A target
            # that is still ahead of the media tail only records a readiness
            # wait; the queue slot is released by the caller before retry.
            if mapped_target_window is not None and not target_rescan_pending:
                mapped_start = float(mapped_target_window["start_stream_time"])
                mapped_end = float(mapped_target_window["end_stream_time"])
                readable_mapped_window = _ocr_readable_mapped_target_scan_window(
                    retained,
                    mapped_target_window,
                    minimum_component_seconds=max(3.0, min_degraded_seconds),
                )
                if readable_mapped_window is None:
                    mapped_estimate = float(
                        mapped_target_window["estimated_stream_time"]
                    )
                    if deadline_reached:
                        if (
                            latest_end is None
                            or float(latest_end)
                            <= mapped_estimate
                            + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                        ):
                            raise VisualLocationFailed(
                                "ocr_target_media_not_arrived",
                                "the predicted match-clock centre did not enter the retained live buffer before the deadline",
                                {
                                    "stage": "ocr_target_window_mapping",
                                    "mapped_target_window": mapped_target_window,
                                    "latest_media_end_stream_time": latest_end,
                                    "deadline_policy": deadline_policy,
                                },
                            )
                        if (
                            earliest_start is not None
                            and float(earliest_start)
                            > mapped_estimate
                            + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                        ):
                            raise VisualLocationFailed(
                                "ocr_search_history_evicted",
                                "the predicted match-clock centre is older than the retained video",
                                {
                                    "stage": "ocr_target_window_mapping",
                                    "mapped_target_window": mapped_target_window,
                                    "earliest_retained_stream_time": earliest_start,
                                    "deadline_policy": deadline_policy,
                                },
                            )
                        raise VisualLocationFailed(
                            "buffer_gap",
                            "the predicted match-clock centre falls in a retained-video gap",
                            {
                                "stage": "ocr_target_window_mapping",
                                "mapped_target_window": mapped_target_window,
                                "earliest_retained_stream_time": earliest_start,
                                "latest_media_end_stream_time": latest_end,
                                "deadline_policy": deadline_policy,
                            },
                        )
                    _ocr_progressive_wait(
                        runtime,
                        job,
                        current,
                        wait_kind="waiting_for_target_media",
                        message="trusted OCR clock mapping is ready; waiting for target media to enter the retained buffer",
                        scan_start=mapped_start,
                        scan_end=mapped_end,
                        latest_trusted_clock_seconds=latest_trusted,
                        latest_media_end_stream_time=latest_end,
                        history_evicted=history_evicted,
                        has_scannable_media_after_cursor=(
                            has_scannable_media_after_cursor
                        ),
                        diagnostics={
                            "stage": "ocr_target_window_mapping",
                            "mapped_target_window": mapped_target_window,
                            "clock_mapping": _ocr_progressive_clock_mapping(
                                clock_samples,
                                clock_phase=clock_phase,
                            ),
                            "coverage_diagnostics": _ocr_progressive_coverage_diagnostics(
                                retained,
                                intended_initial_start=intended_initial_start,
                                requested_start=float(current.search_start_stream_time),
                                requested_end=float(current.search_end_stream_time),
                                scan_start=mapped_start,
                                scan_end=mapped_end,
                                target_clock_seconds=target_clock_seconds,
                                latest_trusted_clock_seconds=latest_trusted,
                                target_passed_with_continuous_evidence=target_passed,
                                previous_media_end_stream_time=previous_media_end,
                                target_window=mapped_target_window,
                            ),
                        },
                        now_unix=now_unix,
                    )
                    return False
                focused_start, focused_end = readable_mapped_window
                window_start = focused_start
                window_end = focused_end
                mapped_target_ready = True
                force_final_scan = True
                target_rescan_pending = False

            if latest_end is None:
                if deadline_reached:
                    raise VisualLocationFailed(
                        "ocr_buffer_never_available",
                        "no retained source-video segments became available before the OCR deadline",
                        {
                            "stage": "buffer_coverage",
                            "scan_start_stream_time": window_start,
                            "deadline_policy": deadline_policy,
                            "coverage_diagnostics": coverage_diagnostics,
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
                    latest_media_end_stream_time=latest_end,
                    history_evicted=history_evicted,
                    diagnostics={"coverage_diagnostics": coverage_diagnostics},
                    has_scannable_media_after_cursor=(
                        has_scannable_media_after_cursor
                    ),
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
            if mapped_target_ready:
                # The target window is intentionally historical when a late
                # shotmap second arrives; do not let the normal tail cursor
                # turn this focused rewind into a readiness wait.
                no_new_media = False
                no_scannable_window = False
            if target_rescan_pending and latest_end is not None:
                try:
                    retry_start = float(target_rescan_window["start_stream_time"])
                    retry_end = float(target_rescan_window["end_stream_time"])
                except (KeyError, TypeError, ValueError):
                    retry_start = retry_end = math.nan
                if (
                    math.isfinite(retry_start)
                    and math.isfinite(retry_end)
                    and retry_end > retry_start
                    and latest_end > retry_start + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                ):
                    # A target-local retry is historical media by design; it
                    # must run even when the normal cursor has reached the
                    # current media tail.
                    window_start = max(0.0, retry_start)
                    window_end = min(float(latest_end), retry_end)
                    if window_end - window_start >= 3.0:
                        no_new_media = False
                        no_scannable_window = False
                        if deadline_reached and bool(
                            deadline_policy.get("slot_acquired")
                        ):
                            force_final_scan = True
            if (
                deadline_reached
                and not final_scan_completed
                and bool(deadline_policy.get("slot_acquired"))
                and retained
                and (no_new_media or no_scannable_window)
            ):
                final_scan_start = None
                final_scan_end = float(latest_end)
                if isinstance(target_rescan_window, dict):
                    try:
                        candidate_start = float(
                            target_rescan_window["start_stream_time"]
                        )
                        candidate_end = float(
                            target_rescan_window["end_stream_time"]
                        )
                    except (KeyError, TypeError, ValueError):
                        candidate_start = candidate_end = math.nan
                    if (
                        math.isfinite(candidate_start)
                        and math.isfinite(candidate_end)
                        and candidate_end > candidate_start
                    ):
                        final_scan_start = max(
                            float(earliest_start or 0.0), candidate_start
                        )
                        final_scan_end = min(float(latest_end), candidate_end)
                if final_scan_start is None:
                    final_scan_start = max(
                        float(earliest_start or 0.0),
                        float(latest_end)
                        - max(3.0, OCR_PROGRESSIVE_OVERLAP_SECONDS),
                    )
                if final_scan_end - final_scan_start >= 3.0:
                    window_start = final_scan_start
                    window_end = final_scan_end
                    force_final_scan = True
                    no_new_media = False
                    no_scannable_window = False
            if no_new_media:
                if deadline_reached:
                    try:
                        persisted_rescan_count = max(
                            0, int(state.get("target_rescan_attempt_count") or 0)
                        )
                    except (TypeError, ValueError):
                        persisted_rescan_count = 0
                    legacy_rescan_completed = bool(
                        state.get("target_rescan_completed_at_unix") is not None
                        and persisted_rescan_count == 0
                        and not bool(state.get("target_rescan_exhausted"))
                    )
                    can_retry_without_scan = bool(
                        not coverage_diagnostics.get("target_history_fully_missing")
                        and target_passed
                        and persisted_rescan_count
                        < OCR_PROGRESSIVE_TARGET_RESCAN_MAX_ATTEMPTS
                        and not bool(state.get("target_rescan_exhausted"))
                        and not legacy_rescan_completed
                    )
                    if can_retry_without_scan:
                        _ocr_progressive_wait(
                            runtime,
                            job,
                            current,
                            wait_kind="waiting_for_target_rescan",
                            message="目标时钟已经经过，正在重扫目标附近画面",
                            scan_start=window_start,
                            scan_end=window_end,
                            latest_trusted_clock_seconds=latest_trusted,
                            latest_media_end_stream_time=latest_end,
                            history_evicted=history_evicted,
                            diagnostics={
                                "stage": "ocr_target_rescan_recovery",
                                "coverage_diagnostics": coverage_diagnostics,
                                **(
                                    state.get("last_scan_diagnostics")
                                    if isinstance(
                                        state.get("last_scan_diagnostics"), dict
                                    )
                                    else {}
                                ),
                            },
                            target_rescan_attempted=False,
                            has_scannable_media_after_cursor=(
                                has_scannable_media_after_cursor
                            ),
                            now_unix=now_unix,
                        )
                        return False
                    failure_details: dict[str, Any] = {
                        "coverage_diagnostics": coverage_diagnostics,
                    }
                    if coverage_diagnostics.get("target_history_fully_missing"):
                        error_kind = "ocr_search_history_evicted"
                        message = "required OCR search history was evicted before the target clock was located"
                    elif latest_trusted is None:
                        error_kind = "ocr_no_trustworthy_clock_before_deadline"
                        message = "OCR never produced a trustworthy match-clock reading before the deadline"
                    elif (
                        target_clock_seconds is not None
                        and latest_trusted < target_clock_seconds
                    ):
                        error_kind, message, target_wait_details = (
                            _ocr_target_wait_failure(
                                {**state, "deadline_policy": deadline_policy},
                                target_clock_seconds=target_clock_seconds,
                                latest_trusted_clock_seconds=latest_trusted,
                                latest_media_end_stream_time=latest_end,
                            )
                        )
                        failure_details.update(target_wait_details)
                    else:
                        error_kind = "ocr_clock_target_not_located"
                        message = "OCR passed the requested clock but could not verify a usable anchor"
                        target_failure_diagnostics = {}
                        last_scan_diagnostics = state.get("last_scan_diagnostics")
                        if isinstance(last_scan_diagnostics, dict):
                            target_failure_diagnostics.update(last_scan_diagnostics)
                        target_failure_diagnostics.update(failure_details)
                        failure_details.update(
                            _ocr_target_not_located_diagnostics(
                                target_failure_diagnostics,
                                target_clock_seconds=target_clock_seconds,
                                latest_trusted_clock_seconds=latest_trusted,
                                coverage_diagnostics=coverage_diagnostics,
                                clock_samples=clock_samples,
                            )
                        )
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
                            **failure_details,
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
                    latest_media_end_stream_time=latest_end,
                    history_evicted=history_evicted,
                    diagnostics={"coverage_diagnostics": coverage_diagnostics},
                    has_scannable_media_after_cursor=(
                        has_scannable_media_after_cursor
                    ),
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
                            "coverage_diagnostics": coverage_diagnostics,
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
                    latest_media_end_stream_time=latest_end,
                    history_evicted=history_evicted,
                    diagnostics={"coverage_diagnostics": coverage_diagnostics},
                    has_scannable_media_after_cursor=(
                        has_scannable_media_after_cursor
                    ),
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
                        "coverage_diagnostics": coverage_diagnostics,
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
                "target_revision": worker_target_revision,
                "target_source": _vision_target_source(job),
                "deadline_policy": deadline_policy,
                "active_processing_budget": active_processing_budget,
                # Keep the media-tail baseline with the in-flight scan.  The
                # post-scan policy can then tell whether new TS arrived while
                # OCR was running, even when no prior readiness-wait row exists.
                "latest_media_end_stream_time": (
                    round(float(latest_end), 3)
                    if latest_end is not None
                    else None
                ),
                "execution_started_at_unix": now_unix,
                "sample_interval_seconds": (
                    OCR_PROGRESSIVE_TARGET_RESCAN_SAMPLE_INTERVAL_SECONDS
                    if target_rescan_pending
                    or mapped_target_ready
                    or force_final_scan
                    else 1.0
                ),
                "scan_mode": (
                    "target_centered_dense_rescan"
                    if target_rescan_pending or mapped_target_ready or force_final_scan
                    else "clock_readiness_tail_probe"
                    if readiness_probe
                    else "progressive_forward_scan"
                ),
                "target_rescan_attempt_count": target_rescan_attempt_count,
            }
            if readiness_probe:
                try:
                    readiness_probe_count = max(
                        0, int(readiness.get("probe_count") or 0)
                    )
                except (TypeError, ValueError):
                    readiness_probe_count = 0
                scan_state["clock_readiness"] = {
                    **readiness,
                    "status": "waiting",
                    "scope": "match",
                    "clock_phase": clock_phase,
                    "clock_period": _ocr_clock_period(clock_phase),
                    "probe_count": readiness_probe_count + 1,
                    "last_probe_media_end_stream_time": round(
                        float(latest_end), 3
                    ),
                    "last_probe_window": {
                        "start_stream_time": round(window_start, 3),
                        "end_stream_time": round(window_end, 3),
                    },
                    "probe_window_seconds": OCR_CLOCK_READINESS_PROBE_SECONDS,
                    "required_media_growth_seconds": (
                        OCR_CLOCK_READINESS_MIN_MEDIA_GROWTH_SECONDS
                    ),
                }
            if target_rescan_pending:
                scan_state["target_rescan_started_at_unix"] = now_unix
                scan_state["target_rescan_window"] = dict(target_rescan_window)
            if force_final_scan:
                scan_state["final_scan_started_at_unix"] = now_unix
                scan_state["final_scan_window"] = {
                    "start_stream_time": round(window_start, 3),
                    "end_stream_time": round(window_end, 3),
                }
            if _ocr_target_revision_is_stale(
                runtime, job.event_key, worker_target_revision
            ):
                return False
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
                    sample_interval_seconds=(
                        OCR_PROGRESSIVE_TARGET_RESCAN_SAMPLE_INTERVAL_SECONDS
                        if target_rescan_pending
                        or mapped_target_ready
                        or force_final_scan
                        else 1.0
                    ),
                    # Target-centred scans are already bounded to the small
                    # predicted/retry window. Scan them densely from the
                    # first pass so a short retained component does not yield
                    # only one 10-second coarse sample.
                    coarse_sample_interval_seconds=(
                        None
                        if target_rescan_pending
                        or mapped_target_ready
                        or force_final_scan
                        else DEFAULT_COARSE_SAMPLE_INTERVAL_SECONDS
                    ),
                    cancel_event=cancel_event,
                )
            except VisualLocationFailed as exc:
                recoverable_profile_mismatch = bool(
                    job.clock_only
                    and progressive_scan
                    and _ocr_recoverable_profile_mismatch(
                        exc.kind,
                        exc.diagnostics,
                        clock_samples,
                    )
                )
                if not recoverable_profile_mismatch:
                    _record_scoreboard_roi_failure(
                        runtime,
                        job,
                        exc.kind,
                        cached=cached_scoreboard_roi,
                    )
                if _ocr_target_revision_is_stale(
                    runtime, job.event_key, worker_target_revision
                ):
                    return False
                if (
                    not progressive_scan
                    or (
                        exc.kind not in OCR_PROGRESSIVE_SCAN_MISS_KINDS
                        and not recoverable_profile_mismatch
                    )
                ):
                    raise
                scanned_clock_samples = _ocr_progressive_merge_clock_samples(
                    clock_samples,
                    exc.diagnostics,
                    default_phase=clock_phase,
                )
                scanned_trusted = _latest_trusted_clock_seconds(
                    scanned_clock_samples
                )
                if scanned_trusted is None:
                    scanned_trusted = _ocr_latest_unpositioned_clock_seconds(
                        exc.diagnostics
                    )
                scanned_target_passed = (
                    _ocr_target_passed_with_continuous_evidence(
                        scanned_clock_samples,
                        target_clock_seconds=target_clock_seconds,
                        clock_phase=clock_phase,
                    )
                )
                target_not_reached = bool(
                    target_clock_seconds is not None
                    and (
                        scanned_trusted is None
                        or scanned_trusted < target_clock_seconds
                    )
                )
                after_scan_unix = time.time()
                media_tail_before_scan = latest_end
                refreshed_segments = segment_reader()
                refreshed_retained = [
                    segment
                    for segment in refreshed_segments
                    if Path(segment.path).is_file()
                ]
                refreshed_latest_end = max(
                    (float(segment.end) for segment in refreshed_retained),
                    default=None,
                )
                latest_end = refreshed_latest_end
                media_tail_grew_during_scan = bool(
                    latest_end is not None
                    and (
                        media_tail_before_scan is None
                        or latest_end
                        > float(media_tail_before_scan)
                        + OCR_PROGRESSIVE_TAIL_EPSILON_SECONDS
                    )
                )
                # Re-read the transition persisted immediately before OCR.
                # It contains the execution start timestamp needed to exclude
                # this expensive scan from the live-media wait budget.
                refreshed = _artifact_task(runtime, job.event_key, artifact_kind)
                if refreshed is not None:
                    current = refreshed
                    state = _ocr_progressive_state(current)
                post_scan_cursor = (
                    float(cursor)
                    if target_rescan_pending and isinstance(cursor, (int, float))
                    else float(window_end)
                )
                post_scan_has_scannable_media = (
                    _ocr_has_scannable_media_after_cursor(
                        refreshed_retained,
                        post_scan_cursor,
                        minimum_component_seconds=max(
                            3.0, min_degraded_seconds
                        ),
                    )
                )
                long_scan_tail_rescan = bool(
                    media_tail_grew_during_scan
                    and post_scan_has_scannable_media
                    and after_scan_unix - now_unix
                    >= OCR_LONG_SCAN_TAIL_RESCAN_SECONDS
                )
                updated_deadline_policy = _ocr_deadline_policy(
                    current,
                    now_unix=after_scan_unix,
                    target_clock_seconds=target_clock_seconds,
                    latest_trusted_clock_seconds=scanned_trusted,
                    latest_media_end_stream_time=latest_end,
                    clock_samples=scanned_clock_samples,
                    has_scannable_media_after_cursor=(
                        post_scan_has_scannable_media
                    ),
                )
                effective_deadline_reached = (
                    after_scan_unix
                    >= float(updated_deadline_policy["target_deadline_at_unix"])
                )
                post_scan_clock_samples = _ocr_progressive_merge_clock_samples(
                    state.get("clock_samples"),
                    scanned_clock_samples,
                    default_phase=clock_phase,
                )
                scanned_target_passed = (
                    _ocr_target_passed_with_continuous_evidence(
                        post_scan_clock_samples,
                        target_clock_seconds=target_clock_seconds,
                        clock_phase=clock_phase,
                    )
                )
                post_scan_target_window = _ocr_progressive_mapped_target_window(
                    post_scan_clock_samples,
                    target_clock_seconds=target_clock_seconds,
                    clock_phase=clock_phase,
                )
                post_scan_mapping = _ocr_progressive_clock_mapping(
                    post_scan_clock_samples,
                    clock_phase=clock_phase,
                )
                readiness_roi_cache = None
                if (
                    job.clock_only
                    and post_scan_mapping.get("status") == "ready"
                    and _scoreboard_roi_profile_from_result(
                        job.match_id, exc.diagnostics
                    )
                    is not None
                ):
                    # Auto-discovery can establish a reusable ROI even when
                    # the requested target is still ahead. Persist it as soon
                    # as two progressing clock readings make the discovery
                    # trustworthy, so the next event can use direct TS OCR.
                    readiness_roi_cache = _record_scoreboard_roi_success(
                        runtime,
                        job,
                        exc.diagnostics,
                        cached=cached_scoreboard_roi,
                    )
                predicted_target_scan_window = (
                    _ocr_readable_mapped_target_scan_window(
                        refreshed_retained,
                        post_scan_target_window,
                        minimum_component_seconds=max(
                            3.0, min_degraded_seconds
                        ),
                    )
                )
                predicted_target_media_ready = bool(
                    predicted_target_scan_window is not None
                    and not scanned_target_passed
                    and not mapped_target_ready
                    and not target_rescan_pending
                    and not force_final_scan
                )
                post_scan_coverage = _ocr_progressive_coverage_diagnostics(
                    refreshed_retained,
                    intended_initial_start=intended_initial_start,
                    requested_start=float(current.search_start_stream_time),
                    requested_end=float(current.search_end_stream_time),
                    scan_start=window_start,
                    scan_end=window_end,
                    target_clock_seconds=target_clock_seconds,
                    latest_trusted_clock_seconds=scanned_trusted,
                    target_passed_with_continuous_evidence=scanned_target_passed,
                    previous_media_end_stream_time=media_tail_before_scan,
                    target_window=post_scan_target_window,
                )
                if post_scan_coverage.get("target_history_fully_missing"):
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
                            "coverage_diagnostics": post_scan_coverage,
                        },
                    ) from exc
                # A deadline reached immediately after a crossed-target scan
                # is not enough evidence to fail.  Give the bounded target
                # rescan policy its remaining attempt(s), including the first
                # target-centered pass when the forward scan was the one that
                # first observed the clock crossing.
                try:
                    persisted_rescan_count = max(
                        0, int(state.get("target_rescan_attempt_count") or 0)
                    )
                except (TypeError, ValueError):
                    persisted_rescan_count = 0
                # Rows written before the retry counter was introduced used
                # ``target_rescan_completed_at_unix`` as a terminal marker.
                # Treat those legacy rows as exhausted instead of silently
                # reopening an old final-scan decision after an upgrade.
                legacy_rescan_completed = bool(
                    state.get("target_rescan_completed_at_unix") is not None
                    and persisted_rescan_count == 0
                    and not bool(state.get("target_rescan_exhausted"))
                )
                can_retry_crossed_target = bool(
                    effective_deadline_reached
                    and not predicted_target_media_ready
                    and not post_scan_coverage.get("target_history_fully_missing")
                    and scanned_target_passed
                    and persisted_rescan_count
                    < OCR_PROGRESSIVE_TARGET_RESCAN_MAX_ATTEMPTS
                    and not bool(state.get("target_rescan_exhausted"))
                    and not legacy_rescan_completed
                )
                if can_retry_crossed_target:
                    _ocr_progressive_wait(
                        runtime,
                        job,
                        current,
                        wait_kind="waiting_for_target_rescan",
                        message="目标时钟已经经过，正在重扫目标附近画面",
                        scan_start=window_start,
                        scan_end=window_end,
                        latest_trusted_clock_seconds=scanned_trusted,
                        latest_media_end_stream_time=latest_end,
                        history_evicted=history_evicted,
                        diagnostics={
                            "kind": exc.kind,
                            "message": str(exc),
                            "scoreboard_roi_cache": readiness_roi_cache,
                            "post_scan_target_window": post_scan_target_window,
                            "coverage_diagnostics": post_scan_coverage,
                            **exc.diagnostics,
                        },
                        target_rescan_attempted=target_rescan_pending,
                        next_scan_cursor_stream_time=(
                            float(cursor)
                            if target_rescan_pending
                            and isinstance(cursor, (int, float))
                            else None
                        ),
                        has_scannable_media_after_cursor=(
                            post_scan_has_scannable_media
                        ),
                        now_unix=after_scan_unix,
                    )
                    return False
                if effective_deadline_reached and not predicted_target_media_ready:
                    if long_scan_tail_rescan:
                        _ocr_progressive_wait(
                            runtime,
                            job,
                            current,
                            wait_kind="waiting_for_latest_tail_rescan",
                            message="上一轮 OCR 用时较长，正在重新扫描新增视频尾部",
                            scan_start=window_start,
                            scan_end=window_end,
                            latest_trusted_clock_seconds=scanned_trusted,
                            latest_media_end_stream_time=latest_end,
                            history_evicted=history_evicted,
                            diagnostics={
                                "kind": exc.kind,
                                "message": str(exc),
                                "media_tail_before_scan": media_tail_before_scan,
                                "media_tail_after_scan": latest_end,
                                "media_tail_grew_during_scan": True,
                                "long_scan_tail_rescan": True,
                                "scoreboard_roi_cache": readiness_roi_cache,
                                "post_scan_target_window": post_scan_target_window,
                                "coverage_diagnostics": post_scan_coverage,
                                **exc.diagnostics,
                            },
                            target_rescan_attempted=target_rescan_pending,
                            next_scan_cursor_stream_time=(
                                float(cursor)
                                if target_rescan_pending
                                and isinstance(cursor, (int, float))
                                else None
                            ),
                            has_scannable_media_after_cursor=(
                                post_scan_has_scannable_media
                            ),
                            now_unix=after_scan_unix,
                        )
                        return False
                    failure_details: dict[str, Any] = {}
                    if post_scan_coverage.get("target_history_fully_missing"):
                        error_kind = "ocr_search_history_evicted"
                        message = "required OCR search history was evicted before the target clock was located"
                    elif scanned_trusted is None:
                        error_kind = "ocr_no_trustworthy_clock_before_deadline"
                        message = "OCR never produced a trustworthy match-clock reading before the deadline"
                    elif target_not_reached:
                        error_kind, message, target_wait_details = (
                            _ocr_target_wait_failure(
                                {
                                    **state,
                                    "deadline_policy": updated_deadline_policy,
                                },
                                target_clock_seconds=target_clock_seconds,
                                latest_trusted_clock_seconds=scanned_trusted,
                                latest_media_end_stream_time=latest_end,
                                diagnostics=exc.diagnostics,
                            )
                        )
                        failure_details.update(target_wait_details)
                    else:
                        error_kind = "ocr_clock_target_not_located"
                        message = "OCR passed the requested clock but could not verify a usable anchor"
                        failure_details.update(
                            _ocr_target_not_located_diagnostics(
                                exc.diagnostics,
                                target_clock_seconds=target_clock_seconds,
                                latest_trusted_clock_seconds=scanned_trusted,
                                coverage_diagnostics=post_scan_coverage,
                                clock_samples=post_scan_clock_samples,
                            )
                        )
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
                            "coverage_diagnostics": post_scan_coverage,
                            **failure_details,
                        },
                    ) from exc
                _ocr_progressive_wait(
                    runtime,
                    job,
                    current,
                    wait_kind=(
                        "waiting_for_latest_tail_rescan"
                        if long_scan_tail_rescan
                        else "waiting_for_clock_target"
                    ),
                    message=(
                        "上一轮 OCR 用时较长，正在重新扫描新增视频尾部"
                        if long_scan_tail_rescan
                        else (
                            "已读到 "
                            f"{_clock_text_from_seconds(scanned_trusted) or '有效比赛时间'}，"
                            "目标 "
                            f"{_clock_text_from_seconds(target_clock_seconds) or '接口时间'}；"
                            "直播中断后正在继续检查恢复画面"
                        )
                        if recoverable_profile_mismatch
                        else "latest trustworthy OCR clock has not reached the target"
                        if target_not_reached
                        else "target clock has not yet yielded a verified OCR anchor"
                    ),
                    scan_start=window_start,
                    scan_end=window_end,
                    latest_trusted_clock_seconds=scanned_trusted,
                    latest_media_end_stream_time=latest_end,
                    history_evicted=history_evicted,
                    diagnostics={
                        "kind": exc.kind,
                        "message": str(exc),
                        "media_tail_before_scan": media_tail_before_scan,
                        "media_tail_after_scan": latest_end,
                        "media_tail_grew_during_scan": media_tail_grew_during_scan,
                        "long_scan_tail_rescan": long_scan_tail_rescan,
                        "recoverable_profile_mismatch": (
                            recoverable_profile_mismatch
                        ),
                        "scoreboard_roi_cache": readiness_roi_cache,
                        "predicted_target_media_ready": predicted_target_media_ready,
                        "predicted_target_scan_window": (
                            {
                                "start_stream_time": round(
                                    predicted_target_scan_window[0], 3
                                ),
                                "end_stream_time": round(
                                    predicted_target_scan_window[1], 3
                                ),
                            }
                            if predicted_target_scan_window is not None
                            else None
                        ),
                        "post_scan_target_window": post_scan_target_window,
                        "coverage_diagnostics": post_scan_coverage,
                        **exc.diagnostics,
                    },
                    target_rescan_attempted=target_rescan_pending,
                    suppress_target_rescan=effective_deadline_reached,
                    next_scan_cursor_stream_time=(
                        float(cursor)
                        if target_rescan_pending
                        and isinstance(cursor, (int, float))
                        else None
                    ),
                    has_scannable_media_after_cursor=(
                        post_scan_has_scannable_media
                    ),
                    retry_immediately=predicted_target_media_ready,
                    now_unix=after_scan_unix,
                )
                return False
            if _ocr_target_revision_is_stale(
                runtime, job.event_key, worker_target_revision
            ):
                return False
            execution_completed_at = time.time()
            refreshed = _artifact_task(runtime, job.event_key, artifact_kind)
            if refreshed is not None:
                current = refreshed
                state = _ocr_progressive_state(current)
            active_processing_budget = _ocr_active_processing_budget(
                state,
                now_unix=execution_completed_at,
                account_open_execution=True,
            )
            scoreboard_roi_cache = _record_scoreboard_roi_success(
                runtime,
                job,
                located,
                cached=cached_scoreboard_roi,
            )
            anchor = float(located["anchor_stream_time"])
            before, after, _legacy_localization_source = _ocr_output_shape(job, located)
            localization_source, localization_precision = _ocr_localization_contract(
                located
            )
            anchor_provenance = (
                "trusted_clock_mapping_projection"
                if located.get("degradation_mode") == "mapped_clock_projection"
                or localization_source == "projected"
                else "ocr_verified_match_clock"
            )
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
                "localization_precision": localization_precision,
                "target_clock": located.get("target_clock"),
                "target_clock_seconds": located.get("target_clock_seconds"),
                "exact_second_error": located.get("exact_second_error"),
                "localization_quality": located.get("localization_quality"),
                "degraded": bool(located.get("degraded")),
                "degradation_mode": located.get("degradation_mode"),
                "degradation_reason": located.get("degradation_reason"),
                "estimated_error_bound_seconds": located.get(
                    "estimated_error_bound_seconds"
                ),
                "target_clock_directly_observed": located.get(
                    "target_clock_directly_observed"
                ),
                "clock_video_mapping": located.get("clock_video_mapping"),
                "clip_before_seconds": before,
                "clip_after_seconds": after,
                "ocr_clock_only": job.clock_only,
                "ocr": located,
                "search_window": materialized,
                "fragment_attempts": located.get("fragment_attempts", []),
                "anchor_provenance": anchor_provenance,
                "progressive_status": "target_located",
                "default_gif_preserved": True,
                "scoreboard_roi_cache": scoreboard_roi_cache,
            }
            before, after = _normalized_ocr_clip_window(
                job,
                locate_result,
                stage="ocr_location_persistence",
            )
            locate_result["clip_before_seconds"] = before
            locate_result["clip_after_seconds"] = after
            merged_clock_samples = _ocr_progressive_merge_clock_samples(
                state.get("clock_samples"),
                located.get("diagnostics") or located,
                default_phase=clock_phase,
            )
            effective_latest_trusted = _latest_trusted_clock_seconds(
                merged_clock_samples
            )
            deadline_policy = _ocr_deadline_policy(
                current,
                now_unix=execution_completed_at,
                target_clock_seconds=target_clock_seconds,
                latest_trusted_clock_seconds=effective_latest_trusted,
                clock_samples=merged_clock_samples,
            )
            progress = {
                **state,
                "state": "target_located",
                "scan_attempt_count": int(state.get("scan_attempt_count") or 0) + 1,
                "last_scan_start_stream_time": round(window_start, 3),
                "last_scan_end_stream_time": round(window_end, 3),
                "scan_cursor_stream_time": round(window_end, 3),
                "overlap_seconds": OCR_PROGRESSIVE_OVERLAP_SECONDS,
                "latest_trusted_clock_seconds": effective_latest_trusted,
                "target_clock_seconds": target_clock_seconds,
                "target_revision": worker_target_revision,
                "target_source": _vision_target_source(job),
                "anchor_stream_time": round(anchor, 3),
                "anchor_provenance": anchor_provenance,
                "location_kind": located.get("location_kind"),
                "history_evicted": history_evicted,
                "deadline_policy": {
                    **deadline_policy,
                    "phase": "target_located",
                    "target_located_at_unix": execution_completed_at,
                },
                "active_processing_budget": active_processing_budget,
                "last_execution_completed_at_unix": execution_completed_at,
                "last_execution_result": "target_located",
                "clock_samples": merged_clock_samples,
                "clock_mapping": _ocr_progressive_clock_mapping(
                    merged_clock_samples,
                    clock_phase=clock_phase,
                ),
            }
            if force_final_scan:
                progress["final_scan_completed_at_unix"] = time.time()
                progress["final_scan_result"] = "target_located"
            if _ocr_target_revision_is_stale(
                runtime, job.event_key, worker_target_revision
            ):
                return False
            _artifact_transition(
                runtime,
                job.event_key,
                artifact_kind,
                "located",
                result=locate_result,
                window_metadata={"progressive_scan": progress},
            )
            located = locate_result

        if _ocr_target_revision_is_stale(
            runtime, job.event_key, worker_target_revision
        ):
            return False
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
        output_lease = _ensure_ocr_output_window_lease(
            runtime,
            event_key=job.event_key,
            artifact_kind=artifact_kind,
            target_revision=worker_target_revision,
            segments=segments,
            window_start=requested_start,
            window_end=requested_end,
            ttl_seconds=max(
                OCR_OUTPUT_WINDOW_LEASE_MIN_TTL_SECONDS,
                ocr_timeout_seconds + OCR_POSTROLL_HARD_LIMIT_SECONDS + 60.0,
            ),
            now_unix=now_unix,
        )
        if output_lease["new_segment_count"]:
            runtime.logger.log(
                "ocr_output_window_leased",
                match_id=job.match_id,
                event_key=job.event_key,
                artifact_kind=artifact_kind,
                **output_lease,
            )
        # The durable output lease now owns retention across post-roll waits.
        if lease_id:
            runtime.store.release_segment_lease(lease_id)
            lease_id = None
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
            allow_degraded=True,
            force_degraded=deadline_reached,
            min_degraded_seconds=OCR_OUTPUT_MIN_DEGRADED_SECONDS,
            stitch_across_gaps=True,
            allow_anchor_adjustment=True,
            max_anchor_gap_seconds=OCR_OUTPUT_MAX_ANCHOR_GAP_SECONDS,
            max_anchor_shift_seconds=OCR_OUTPUT_MAX_ANCHOR_SHIFT_SECONDS,
        )
        if coverage.status == CoverageStatus.WAITING:
            wait_kind = (
                "waiting_for_postroll"
                if (
                    _ocr_localization_is_second_precision(
                        located.get("localization_source")
                    )
                    or located.get("location_kind") == "match_clock_second"
                )
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
                        **_ocr_coverage_contract(coverage),
                    },
                )
            progress = {
                **_ocr_progressive_state(current),
                "state": wait_kind,
                "anchor_stream_time": round(anchor, 3),
                "anchor_provenance": located.get(
                    "anchor_provenance", "ocr_verified_match_clock"
                ),
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
                    "output_window_lease": output_lease,
                    **_ocr_coverage_contract(coverage),
                },
                window_metadata={"progressive_scan": progress},
                deadline_at_unix=postroll_deadline,
                now=now_unix,
            )
            return False
        if coverage.status == CoverageStatus.UNAVAILABLE:
            event_frame_missing = coverage.error_kind in {
                "anchor_gap",
                "anchor_gap_too_large",
                "anchor_shift_too_large",
                "anchor_unavailable",
            }
            unavailable_kind = _normalized_buffer_error_kind(coverage.error_kind)
            raise VisualLocationFailed(
                (
                    "event_frame_missing"
                    if event_frame_missing
                    else "ocr_output_history_evicted"
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
                    "event_frame_missing": event_frame_missing,
                    **_ocr_coverage_contract(coverage),
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
        if _ocr_target_revision_is_stale(
            runtime, job.event_key, worker_target_revision
        ):
            return False
        encoding_state = _ocr_progressive_state(current)
        active_processing_budget = _ocr_active_processing_budget(encoding_state)
        encoding_watchdog_seconds = max(
            OCR_FFMPEG_WATCHDOG_SECONDS,
            float(ocr_timeout_seconds) * 3.0,
        )
        _artifact_transition(
            runtime,
            job.event_key,
            artifact_kind,
            "encoding",
            result=located,
        )
        if _ocr_target_revision_is_stale(
            runtime, job.event_key, worker_target_revision
        ):
            return False
        encoding_started_monotonic = time.monotonic()
        try:
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
                timeout_seconds=encoding_watchdog_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            raise VisualLocationFailed(
                "ocr_window_encoding_timeout",
                "FFmpeg did not finish generating the OCR GIF before its watchdog expired",
                {
                    "stage": "ocr_window_encoding",
                    "encoding_timeout_seconds": encoding_watchdog_seconds,
                    "active_processing_budget": active_processing_budget,
                },
            ) from exc
        active_processing_budget = _ocr_budget_after_elapsed(
            active_processing_budget,
            time.monotonic() - encoding_started_monotonic,
            phase="gif_encoding",
        )
        if _ocr_target_revision_is_stale(
            runtime, job.event_key, worker_target_revision
        ):
            # The encoded file is intentionally left to the normal lifecycle
            # cleanup; a newer revision owns the durable artifact row.
            return False
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
                located.get("localization_source")
                in {"exact_second", "exact", "interpolated"}
                and located.get("localization_quality") == "exact"
                and not coverage.event_frame_may_be_missing
            ),
            "output_width": width,
            "output_fps": fps,
            "output_colors": colors,
            "localization_degraded": bool(located.get("degraded")),
            **_ocr_coverage_contract(coverage),
            "coverage_degraded": coverage.degraded,
            "degraded": bool(located.get("degraded")) or coverage.degraded,
            "degradation_source": (
                "live_source_missing"
                if coverage.degraded
                else "ocr_localization_degraded"
                if bool(located.get("degraded"))
                else None
            ),
            "event_frame_present": not coverage.event_frame_may_be_missing,
            "active_processing_budget": active_processing_budget,
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
                **_ocr_coverage_contract(coverage),
            },
            "default_gif_preserved": True,
        })
        if _ocr_target_revision_is_stale(
            runtime, job.event_key, worker_target_revision
        ):
            # A late target revision may have reset the durable row while the
            # old encode was running. Do not let the stale artifact transition
            # back to encoded; the next worker will encode the new target.
            return False
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
                    "active_processing_budget": active_processing_budget,
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
        try:
            fallback_allowed = exc.kind != "ocr_target_before_recording"
            if (
                fallback_allowed
                and _encode_ocr_api_range_fallback(
                    job,
                    runtime,
                    segment_reader,
                    ffmpeg,
                    ffprobe,
                    output_dir,
                    failure=exc,
                    width=width,
                    fps=fps,
                    colors=colors,
                    size_reference_bytes=size_reference_bytes,
                    timeout_seconds=max(ocr_timeout_seconds * 3.0, 180.0),
                    min_degraded_seconds=min_degraded_seconds,
                    cancel_event=cancel_event,
                )
            ):
                return True
        except Exception as fallback_exc:
            exc.diagnostics["api_range_fallback_error"] = str(fallback_exc)
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
        terminal = _artifact_task(runtime, job.event_key, artifact_kind)
        worker_revision_is_stale = _ocr_target_revision_is_stale(
            runtime,
            job.event_key,
            worker_target_revision,
        )
        if (
            terminal is None
            or terminal.status in {"encoded", "failed"}
            or worker_revision_is_stale
        ):
            _release_ocr_output_window_leases(
                runtime,
                job.event_key,
                target_revision=worker_target_revision,
            )


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

    ocr_location_verified = bool(
        ocr_task.status == "encoded"
        and ocr_task.result.get("ocr_verified") is not False
        and ocr_task.result.get("output_kind") != "api_time_range_fallback"
        and ocr_task.result.get("localization_source") != "api_time_range"
    )
    upstream_ocr_failure = (
        dict(ocr_task.result.get("failure_reason") or {})
        if ocr_task.status == "failed" or not ocr_location_verified
        else None
    )
    if not ocr_location_verified and not upstream_ocr_failure:
        upstream_ocr_failure = {
            "kind": ocr_task.last_error_kind or "ocr_processing_failed",
            "message": ocr_task.error or "OCR artifact failed",
            "stage": ocr_task.failure_stage or "ocr_processing",
        }
    if not ocr_location_verified:
        # OCR failure is a recoverable upstream signal. T-DEED gets one
        # independent attempt over the original persisted search interval.
        # Only a subsequent T-DEED failure makes the refined artifact terminal.
        pass

    ocr_result: dict[str, Any] = (
        dict(ocr_task.result) if ocr_location_verified else {}
    )
    lease_id: str | None = None
    try:
        if current.status == "located" and current.located_anchor_stream_time is not None:
            refined_anchor = float(current.located_anchor_stream_time)
            located = dict(current.result)
        else:
            target_window_used = None
            if ocr_location_verified:
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
                # When OCR exhausted its target-local retries, preserve the
                # last focused retry window for T-DEED.  The old behavior
                # always fell back to the broad API interval, which could
                # make T-DEED search the wrong part of a rolling buffer even
                # though OCR had already narrowed the target location.
                progressive_state = _ocr_progressive_state(ocr_task)
                target_window = progressive_state.get("target_rescan_window")
                target_window_used = None
                if isinstance(target_window, dict):
                    try:
                        candidate_start = float(target_window["start_stream_time"])
                        candidate_end = float(target_window["end_stream_time"])
                        candidate_anchor = float(
                            target_window.get(
                                "estimated_stream_time",
                                (candidate_start + candidate_end) / 2.0,
                            )
                        )
                    except (KeyError, TypeError, ValueError):
                        candidate_start = candidate_end = candidate_anchor = math.nan
                    if (
                        math.isfinite(candidate_start)
                        and math.isfinite(candidate_end)
                        and math.isfinite(candidate_anchor)
                        and candidate_end > candidate_start
                    ):
                        window_start = max(0.0, candidate_start)
                        window_end = candidate_end
                        ocr_anchor = min(
                            window_end,
                            max(window_start, candidate_anchor),
                        )
                        target_window_used = dict(target_window)
                if target_window_used is None:
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
                    if ocr_location_verified
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
                    "target_rescan_window": target_window_used,
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
