"""Shared contracts for optional visual refinement of event GIFs."""

from __future__ import annotations

import json
import math
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
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
    "YC": ("Yellow card",),
    "RC": ("Red card", "Yellow->red card"),
}


MIN_DEGRADED_CLIP_SECONDS = 2.0
OCR_MINUTE_FALLBACK_BEFORE_SECONDS = 60.0
OCR_MINUTE_FALLBACK_AFTER_SECONDS = 60.0
OCR_MINUTE_FALLBACK_WIDTH = 384
OCR_MINUTE_FALLBACK_FPS = 6.0
OCR_MINUTE_FALLBACK_COLORS = 160
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
) -> dict[str, Any]:
    """Create a 120-second fallback only around a location found by OCR."""
    anchor = min(window_end, max(window_start, float(ocr_anchor)))
    return {
        "anchor_stream_time": anchor,
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
        "fallback_used": True,
        "minute_fallback": True,
        "tdeed_error_kind": tdeed_error_kind,
        "tdeed_error": tdeed_error,
        "clip_before_seconds": OCR_MINUTE_FALLBACK_BEFORE_SECONDS,
        "clip_after_seconds": OCR_MINUTE_FALLBACK_AFTER_SECONDS,
        "output_kind": "minute_range_fallback",
        "precise_location": False,
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
    ocr_timeout_seconds: float = 45.0,
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
                candidate_start_seconds=window_start,
                timeout_seconds=ocr_timeout_seconds,
                python_executable=ocr_python,
                scoreboard_profile=job.scoreboard_profile,
            )
        except ScoreboardOcrError as exc:
            ocr_error = exc.as_dict()
        except (OSError, TypeError, ValueError) as exc:
            ocr_error = {
                "kind": "ocr_invalid_request",
                "message": str(exc),
                "diagnostics": {},
            }

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
    target_score: str = ""
    scoreboard_profile: dict[str, Any] | str | None = None
    require_scoreboard_profile: bool = False

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
) -> dict[str, Any]:
    """Create a compact 25 FPS search clip from leased rolling-buffer segments."""
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
        completed = subprocess.run(
            [
                ffmpeg, "-y", "-nostdin", "-hide_banner", "-loglevel", "error",
                "-f", "concat", "-safe", "0", "-i", str(concat_path),
                "-ss", f"{seek:.3f}", "-t", f"{window_end - window_start:.3f}",
                "-an", "-vf", "fps=25,scale=398:224:flags=lanczos", "-c:v", "libx264",
                "-threads", "1", "-preset", "veryfast", "-crf", "27", "-pix_fmt", "yuv420p", str(output),
            ],
            check=True,
            text=True,
            capture_output=True,
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
    ocr_timeout_seconds: float = 45.0,
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
            coverage = _vision_coverage(
                segments,
                window_start=window_start,
                window_end=window_end,
                anchor=search_anchor,
                deadline_reached=deadline_reached,
                min_degraded_seconds=min_degraded_seconds,
            )
            if coverage.status == CoverageStatus.WAITING:
                if deadline_reached:
                    error = (
                        "vision search video was not ready before the vision deadline: "
                        f"{coverage.reason}"
                    )
                    runtime.transition_vision_task(
                        job.event_key,
                        "failed",
                        result={"error_kind": "vision_deadline_exceeded"},
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
                    result={"error_kind": error_kind},
                    error=coverage.reason,
                    error_kind=error_kind,
                )
                return True
            leased_paths = [
                str(segment.path.resolve())
                for segment in coverage.segments
            ]
            lease_id = runtime.store.acquire_segment_lease(
                job.event_key,
                leased_paths,
                owner="vision-locator",
                ttl_seconds=max(timeout_seconds + 60.0, 180.0),
            )
            runtime.transition_vision_task(job.event_key, "locating")
            materialized = materialize_analysis_clip(
                ffmpeg, segments, analysis_path,
                window_start=window_start,
                window_end=window_end,
                anchor=search_anchor,
                coverage=coverage,
            )
            located = locate_with_ocr_fallback(
                job,
                analysis_path,
                materialized,
                tdeed_python=python,
                ocr_python=ocr_python,
                ocr_timeout_seconds=ocr_timeout_seconds,
                tdeed_timeout_seconds=timeout_seconds,
            )
            refined_anchor = float(located["anchor_stream_time"])
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
                "clip_before_seconds": located.get(
                    "clip_before_seconds", refined_before
                ),
                "clip_after_seconds": located.get(
                    "clip_after_seconds", refined_after
                ),
                "ocr": located.get("ocr"),
                "ocr_error": located.get("ocr_error"),
                "stage": "located",
                "search_window": materialized,
            }
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

        effective_refined_before = float(
            located.get("clip_before_seconds", refined_before)
        )
        effective_refined_after = float(
            located.get("clip_after_seconds", refined_after)
        )
        if effective_refined_before < 0 or effective_refined_after <= 0:
            raise ValueError("effective vision output window must be valid")
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
                    result={"error_kind": "vision_deadline_exceeded"},
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
                result={"error_kind": error_kind},
                error=coverage.reason,
                error_kind=error_kind,
            )
            return True
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
            "default_gif_preserved": True,
            "fallback_generated": bool(located.get("minute_fallback")),
            "clip_before_seconds": effective_refined_before,
            "clip_after_seconds": effective_refined_after,
            "output_width": effective_width,
            "output_fps": effective_fps,
            "output_colors": effective_colors,
            "ocr": located.get("ocr"),
            "ocr_error": located.get("ocr_error"),
            "stage": "encoded",
            "search_window": materialized,
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
                    result={"error_kind": "vision_deadline_exceeded"},
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
                result={"stage": "buffer", "error_kind": error_kind},
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
            runtime.transition_vision_task(
                job.event_key,
                "failed",
                result={"error_kind": error_kind},
                error=str(exc),
                error_kind=error_kind,
            )
        return True
    finally:
        if lease_id:
            runtime.store.release_segment_lease(lease_id)
        # Candidate media and diagnostics are retained for at most 24 hours by
        # DiskLifecycleManager so failed OCR/T-DEED runs can be investigated.


def find_python(root: Path) -> Path:
    candidate = root / "tmp" / "venv" / "bin" / "python"
    return candidate if candidate.exists() else Path(shutil.which("python3") or "python3")
