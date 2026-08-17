#!/usr/bin/env python3
"""Prepare rolling video and locate a supported event with T-DEED.

This module deliberately has no Torch imports.  The live pipeline can import it
from its normal Python environment; T-DEED itself is launched with the isolated
``tmp/venv`` interpreter.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from live_goal_pipeline import (
    CoverageStatus,
    VideoCoverage,
    analyze_video_coverage,
)


ROOT = Path(__file__).resolve().parent
TDEED_ROOT = ROOT / "tmp" / "T-DEED"
TDEED_PYTHON = ROOT / "tmp" / "venv" / "bin" / "python"
INFERENCE_SCRIPT = TDEED_ROOT / "inference.py"
CHECKPOINT = (
    TDEED_ROOT
    / "checkpoints"
    / "SoccerNet"
    / "SoccerNet_small"
    / "checkpoint_best.pt"
)

MODEL_NAME = "T-DEED"
MODEL_VERSION = "SoccerNet_small"
ANALYSIS_FPS = 25.0
ANALYSIS_WIDTH = 398
ANALYSIS_HEIGHT = 224
DEFAULT_THRESHOLD = 0.20
DEFAULT_MAX_ANCHOR_DISTANCE_SECONDS = 15.0
EVENT_LABELS: dict[str, tuple[str, ...]] = {
    "G": ("Goal",),
    "OG": ("Goal",),
    "YC": ("Yellow card",),
    "RC": ("Red card", "Yellow->red card"),
}


class SegmentLike(Protocol):
    path: Path
    start: float
    end: float


Runner = Callable[..., subprocess.CompletedProcess[str]]


class VisionLocatorError(RuntimeError):
    """Base class for errors in the optional visual-refinement branch."""


class VisionConfigurationError(VisionLocatorError):
    """The local model installation or supplied configuration is invalid."""


class VisionBufferNotReady(VisionLocatorError):
    """The rolling buffer does not yet contain the complete search window."""


class VisionBufferUnavailable(VisionLocatorError):
    """The rolling buffer has a permanent gap in the search window."""


class VisionInferenceError(VisionLocatorError):
    """T-DEED did not produce a valid prediction document."""


class VisionCandidateNotFound(VisionLocatorError):
    """No model candidate satisfied the event class and anchor constraints."""


@dataclass(frozen=True)
class MaterializedCandidate:
    path: Path
    stream_start_seconds: float
    stream_end_seconds: float
    selected_segment_count: int
    selected_segment_paths: tuple[str, ...]
    bytes: int
    materialize_seconds: float
    coverage_status: str = CoverageStatus.READY_FULL.value
    skipped_gap_seconds: float = 0.0
    requested_stream_start_seconds: float | None = None
    requested_stream_end_seconds: float | None = None
    coverage_reason: str = ""
    coverage_error_kind: str | None = None

    @property
    def duration_seconds(self) -> float:
        return self.stream_end_seconds - self.stream_start_seconds

    def as_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path.resolve()),
            "stream_start_seconds": round(self.stream_start_seconds, 3),
            "stream_end_seconds": round(self.stream_end_seconds, 3),
            "requested_stream_start_seconds": round(
                self.requested_stream_start_seconds
                if self.requested_stream_start_seconds is not None
                else self.stream_start_seconds,
                3,
            ),
            "requested_stream_end_seconds": round(
                self.requested_stream_end_seconds
                if self.requested_stream_end_seconds is not None
                else self.stream_end_seconds,
                3,
            ),
            "duration_seconds": round(self.duration_seconds, 3),
            "selected_segment_count": self.selected_segment_count,
            "selected_segment_paths": list(self.selected_segment_paths),
            "bytes": self.bytes,
            "materialize_seconds": self.materialize_seconds,
            "coverage_status": self.coverage_status,
            "coverage_reason": self.coverage_reason,
            **(
                {"coverage_error_kind": self.coverage_error_kind}
                if self.coverage_error_kind else {}
            ),
            **(
                {"skipped_gap_seconds": self.skipped_gap_seconds}
                if self.skipped_gap_seconds else {}
            ),
        }


def _run(
    runner: Runner,
    command: Sequence[str],
    *,
    cwd: Path | None = None,
    timeout: float | None = None,
    env: Mapping[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        completed = runner(
            list(command),
            cwd=str(cwd) if cwd is not None else None,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:
        raise VisionInferenceError(
            f"T-DEED inference exceeded the {timeout:g}s timeout"
        ) from exc
    except OSError as exc:
        raise VisionInferenceError(f"cannot start visual inference: {exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "no process output").strip()
        raise VisionInferenceError(
            f"visual command exited with status {completed.returncode}: {detail[-2000:]}"
        )
    return completed


def _concat_escape(path: Path) -> str:
    return str(path.resolve()).replace("'", "'\\''")


def _select_segments(
    segments: Iterable[SegmentLike],
    window_start: float,
    window_end: float,
) -> VideoCoverage:
    return _analyze_segments(
        segments,
        window_start,
        window_end,
        anchor=(window_start + window_end) / 2.0,
    )


def _analyze_segments(
    segments: Iterable[SegmentLike],
    window_start: float,
    window_end: float,
    *,
    anchor: float,
    allow_degraded: bool = False,
    min_degraded_seconds: float = 2.0,
) -> VideoCoverage:
    coverage = analyze_video_coverage(
        segments,
        window_start=window_start,
        window_end=window_end,
        anchor=anchor,
        allow_degraded=allow_degraded,
        min_degraded_seconds=min_degraded_seconds,
    )
    if coverage.status == CoverageStatus.WAITING:
        raise VisionBufferNotReady(coverage.reason)
    if coverage.status == CoverageStatus.UNAVAILABLE:
        raise VisionBufferUnavailable(coverage.reason)
    return coverage


def materialize_candidate_video(
    segments: Iterable[SegmentLike],
    output: Path,
    *,
    window_start: float,
    window_end: float,
    anchor: float | None = None,
    ffmpeg: str = "ffmpeg",
    runner: Runner = subprocess.run,
    coverage: VideoCoverage | None = None,
    allow_degraded: bool = False,
    min_degraded_seconds: float = 2.0,
) -> MaterializedCandidate:
    """Join a stable segment set into the compact video consumed by T-DEED."""
    anchor = (window_start + window_end) / 2.0 if anchor is None else anchor
    coverage = coverage or _analyze_segments(
        segments,
        window_start,
        window_end,
        anchor=anchor,
        allow_degraded=allow_degraded,
        min_degraded_seconds=min_degraded_seconds,
    )
    coverage.validate_request(
        window_start=window_start,
        window_end=window_end,
        anchor=anchor,
    )
    if coverage.status == CoverageStatus.WAITING:
        raise VisionBufferNotReady(coverage.reason)
    if coverage.status == CoverageStatus.UNAVAILABLE:
        raise VisionBufferUnavailable(coverage.reason)
    selected = list(coverage.segments)
    if coverage.effective_start is None or coverage.effective_end is None:
        raise VisionBufferUnavailable("vision coverage has no usable interval")
    effective_start = coverage.effective_start
    effective_end = coverage.effective_end
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    concat_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".txt",
            prefix="vision_segments_",
            dir=output.parent,
            encoding="utf-8",
            delete=False,
        ) as handle:
            concat_path = Path(handle.name)
            for segment in selected:
                handle.write(f"file '{_concat_escape(Path(segment.path))}'\n")
        seek = max(0.0, effective_start - float(selected[0].start))
        command = [
            ffmpeg,
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_path),
            "-ss",
            f"{seek:.3f}",
            "-t",
            f"{effective_end - effective_start:.3f}",
            "-an",
            "-vf",
            f"fps={ANALYSIS_FPS:g},scale={ANALYSIS_WIDTH}:{ANALYSIS_HEIGHT}:flags=lanczos",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "27",
            "-pix_fmt",
            "yuv420p",
            str(output),
        ]
        _run(runner, command)
        if not output.is_file():
            raise VisionInferenceError("FFmpeg reported success but created no candidate video")
        if output.stat().st_size <= 0:
            raise VisionInferenceError("FFmpeg created an empty candidate video")
    finally:
        if concat_path is not None:
            concat_path.unlink(missing_ok=True)
    return MaterializedCandidate(
        path=output.resolve(),
        stream_start_seconds=effective_start,
        stream_end_seconds=effective_end,
        selected_segment_count=len(selected),
        selected_segment_paths=tuple(str(Path(item.path).resolve()) for item in selected),
        bytes=output.stat().st_size,
        materialize_seconds=round(time.perf_counter() - started, 3),
        coverage_status=coverage.status.value,
        skipped_gap_seconds=coverage.skipped_gap_seconds,
        requested_stream_start_seconds=coverage.requested_start,
        requested_stream_end_seconds=coverage.requested_end,
        coverage_reason=coverage.reason,
        coverage_error_kind=coverage.error_kind,
    )


def _validate_event_code(code: str) -> str:
    normalized = str(code).upper()
    if normalized not in EVENT_LABELS:
        supported = ", ".join(EVENT_LABELS)
        raise VisionConfigurationError(
            f"unsupported visual event code {code!r}; expected one of {supported}"
        )
    return normalized


def _validate_model_installation(
    python_path: Path,
    tdeed_root: Path,
    checkpoint: Path,
) -> None:
    missing = [
        path
        for path in (python_path, tdeed_root / "inference.py", checkpoint)
        if not path.exists()
    ]
    if missing:
        raise VisionConfigurationError(
            "T-DEED installation is incomplete; missing "
            + ", ".join(str(path) for path in missing)
        )


@lru_cache(maxsize=8)
def checkpoint_sha256(path: Path = CHECKPOINT) -> str:
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise VisionConfigurationError(f"cannot read T-DEED checkpoint: {exc}") from exc
    return digest.hexdigest()


def run_tdeed_inference(
    video: Path,
    output_dir: Path,
    *,
    threshold: float = DEFAULT_THRESHOLD,
    timeout_seconds: float = 180.0,
    python_path: Path = TDEED_PYTHON,
    tdeed_root: Path = TDEED_ROOT,
    checkpoint: Path = CHECKPOINT,
    runner: Runner = subprocess.run,
) -> tuple[list[dict[str, Any]], float]:
    """Run the validated official inference entry point in the Torch venv."""
    video = Path(video)
    if not video.is_file():
        raise VisionConfigurationError(f"candidate video does not exist: {video}")
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("vision confidence threshold must be between 0 and 1")
    if timeout_seconds <= 0:
        raise ValueError("vision timeout must be positive")
    python_path = Path(python_path)
    tdeed_root = Path(tdeed_root)
    checkpoint = Path(checkpoint)
    _validate_model_installation(python_path, tdeed_root, checkpoint)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    command = [
        str(python_path),
        str(tdeed_root / "inference.py"),
        "--model",
        MODEL_VERSION,
        "--video_path",
        str(video.resolve()),
        "--frame_width",
        str(ANALYSIS_WIDTH),
        "--frame_height",
        str(ANALYSIS_HEIGHT),
        "--inference_threshold",
        f"{threshold:g}",
        "--device",
        "cpu",
        "--output_dir",
        str(output_dir.resolve()),
    ]
    inference_environment = os.environ.copy()
    inference_environment.update({
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "VECLIB_MAXIMUM_THREADS": "1",
    })
    _run(
        runner,
        command,
        cwd=tdeed_root,
        timeout=timeout_seconds,
        env=inference_environment,
    )
    inference_seconds = round(time.perf_counter() - started, 3)
    result_path = output_dir / "results_inference.json"
    try:
        document = json.loads(result_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise VisionInferenceError(
            f"T-DEED created no readable result at {result_path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise VisionInferenceError(
            f"T-DEED result is not valid JSON at {result_path}: {exc}"
        ) from exc
    predictions = document.get("predictions") if isinstance(document, dict) else None
    if not isinstance(predictions, list):
        raise VisionInferenceError("T-DEED result must contain a predictions array")
    if not all(isinstance(item, dict) for item in predictions):
        raise VisionInferenceError("T-DEED predictions must be JSON objects")
    return predictions, inference_seconds


def select_event_candidate(
    predictions: Iterable[Mapping[str, Any]],
    code: str,
    *,
    expected_offset_seconds: float,
    threshold: float = DEFAULT_THRESHOLD,
    max_anchor_distance_seconds: float = DEFAULT_MAX_ANCHOR_DISTANCE_SECONDS,
    analysis_fps: float = ANALYSIS_FPS,
) -> dict[str, Any]:
    """Select the nearest above-threshold candidate of the API event's class."""
    normalized_code = _validate_event_code(code)
    if expected_offset_seconds < 0:
        raise ValueError("expected event offset must not be negative")
    if not 0.0 <= threshold <= 1.0:
        raise ValueError("vision confidence threshold must be between 0 and 1")
    if max_anchor_distance_seconds <= 0:
        raise ValueError("maximum anchor distance must be positive")
    if analysis_fps <= 0:
        raise ValueError("analysis FPS must be positive")

    wanted_labels = set(EVENT_LABELS[normalized_code])
    matching_label_count = 0
    above_threshold_count = 0
    eligible: list[dict[str, Any]] = []
    strongest_confidence: float | None = None
    for index, raw in enumerate(predictions):
        label = str(raw.get("label") or "")
        if label not in wanted_labels:
            continue
        matching_label_count += 1
        try:
            confidence = float(raw.get("confidence", raw.get("score")))
            if raw.get("offset_seconds") is not None:
                offset = float(raw["offset_seconds"])
            else:
                offset = float(raw["frame"]) / analysis_fps
        except (KeyError, TypeError, ValueError) as exc:
            raise VisionInferenceError(
                f"invalid T-DEED prediction at index {index}: {raw!r}"
            ) from exc
        strongest_confidence = (
            confidence
            if strongest_confidence is None
            else max(strongest_confidence, confidence)
        )
        if confidence < threshold:
            continue
        above_threshold_count += 1
        distance = abs(offset - expected_offset_seconds)
        if distance > max_anchor_distance_seconds:
            continue
        eligible.append(
            {
                "label": label,
                "anchor_seconds": offset,
                "confidence": confidence,
                "distance_from_expected_seconds": distance,
            }
        )

    if not eligible:
        labels = " / ".join(EVENT_LABELS[normalized_code])
        strongest = (
            f"{strongest_confidence:.6f}" if strongest_confidence is not None else "none"
        )
        raise VisionCandidateNotFound(
            f"no {labels} candidate within {max_anchor_distance_seconds:g}s of "
            f"the coarse anchor at {expected_offset_seconds:.3f}s "
            f"(threshold={threshold:g}, matching={matching_label_count}, "
            f"above_threshold={above_threshold_count}, strongest={strongest})"
        )

    best = min(
        eligible,
        key=lambda item: (
            item["distance_from_expected_seconds"],
            -item["confidence"],
            item["anchor_seconds"],
            item["label"],
        ),
    )
    return {
        "label": best["label"],
        "anchor_seconds": round(float(best["anchor_seconds"]), 3),
        "confidence": round(float(best["confidence"]), 6),
        "distance_from_expected_seconds": round(
            float(best["distance_from_expected_seconds"]), 3
        ),
        "matching_label_count": matching_label_count,
        "above_threshold_count": above_threshold_count,
        "eligible_candidate_count": len(eligible),
    }


def locate_candidate_video(
    video: Path,
    code: str,
    *,
    expected_offset_seconds: float,
    threshold: float = DEFAULT_THRESHOLD,
    max_anchor_distance_seconds: float = DEFAULT_MAX_ANCHOR_DISTANCE_SECONDS,
    timeout_seconds: float = 180.0,
    candidate_window_start_seconds: float = 0.0,
    candidate_window_end_seconds: float | None = None,
    python_path: Path = TDEED_PYTHON,
    tdeed_root: Path = TDEED_ROOT,
    checkpoint: Path = CHECKPOINT,
    runner: Runner = subprocess.run,
) -> dict[str, Any]:
    """Locate an event inside one already-materialized candidate video."""
    normalized_code = _validate_event_code(code)
    if candidate_window_end_seconds is not None and (
        candidate_window_end_seconds <= candidate_window_start_seconds
    ):
        raise ValueError("candidate window end must be after its start")
    with tempfile.TemporaryDirectory(prefix="tdeed_inference_") as directory:
        predictions, inference_seconds = run_tdeed_inference(
            Path(video),
            Path(directory),
            threshold=threshold,
            timeout_seconds=timeout_seconds,
            python_path=python_path,
            tdeed_root=tdeed_root,
            checkpoint=checkpoint,
            runner=runner,
        )
    selected = select_event_candidate(
        predictions,
        normalized_code,
        expected_offset_seconds=expected_offset_seconds,
        threshold=threshold,
        max_anchor_distance_seconds=max_anchor_distance_seconds,
    )
    anchor_seconds = float(selected["anchor_seconds"])
    candidate_window: dict[str, Any] = {
        "start_seconds": round(candidate_window_start_seconds, 3),
        "end_seconds": (
            round(candidate_window_end_seconds, 3)
            if candidate_window_end_seconds is not None
            else None
        ),
    }
    if candidate_window_end_seconds is not None:
        candidate_window["duration_seconds"] = round(
            candidate_window_end_seconds - candidate_window_start_seconds, 3
        )
    return {
        "status": "located",
        "code": normalized_code,
        **selected,
        "anchor_stream_time": round(candidate_window_start_seconds + anchor_seconds, 3),
        "threshold": threshold,
        "max_anchor_distance_seconds": max_anchor_distance_seconds,
        "model_name": MODEL_NAME,
        "model_version": MODEL_VERSION,
        "checkpoint_sha256": checkpoint_sha256(Path(checkpoint)),
        "inference_seconds": inference_seconds,
        "candidate_window": candidate_window,
        "experimental": normalized_code in {"YC", "RC"},
    }


def locate(
    video: Path,
    code: str,
    expected_offset: float,
    threshold: float = DEFAULT_THRESHOLD,
    **kwargs: Any,
) -> dict[str, Any]:
    """Backward-compatible short form used by early refinement integrations."""
    return locate_candidate_video(
        video,
        code,
        expected_offset_seconds=expected_offset,
        threshold=threshold,
        **kwargs,
    )


def locate_event(
    segments: Iterable[SegmentLike],
    code: str,
    coarse_anchor_stream_time: float,
    *,
    search_before_seconds: float,
    search_after_seconds: float,
    candidate_video: Path,
    ffmpeg: str = "ffmpeg",
    threshold: float = DEFAULT_THRESHOLD,
    max_anchor_distance_seconds: float = DEFAULT_MAX_ANCHOR_DISTANCE_SECONDS,
    timeout_seconds: float = 180.0,
    python_path: Path = TDEED_PYTHON,
    tdeed_root: Path = TDEED_ROOT,
    checkpoint: Path = CHECKPOINT,
    runner: Runner = subprocess.run,
    allow_degraded: bool = True,
    min_degraded_seconds: float = 2.0,
) -> dict[str, Any]:
    """Materialize rolling segments, run T-DEED, and return a stream anchor."""
    if coarse_anchor_stream_time < 0:
        raise ValueError("coarse stream anchor must not be negative")
    if search_before_seconds < 0 or search_after_seconds <= 0:
        raise ValueError("vision search durations must be non-negative and non-zero")
    window_start = max(0.0, coarse_anchor_stream_time - search_before_seconds)
    window_end = coarse_anchor_stream_time + search_after_seconds
    materialized = materialize_candidate_video(
        segments,
        candidate_video,
        window_start=window_start,
        window_end=window_end,
        anchor=coarse_anchor_stream_time,
        ffmpeg=ffmpeg,
        runner=runner,
        allow_degraded=allow_degraded,
        min_degraded_seconds=min_degraded_seconds,
    )
    result = locate_candidate_video(
        materialized.path,
        code,
        expected_offset_seconds=(
            coarse_anchor_stream_time - materialized.stream_start_seconds
        ),
        threshold=threshold,
        max_anchor_distance_seconds=max_anchor_distance_seconds,
        timeout_seconds=timeout_seconds,
        candidate_window_start_seconds=materialized.stream_start_seconds,
        candidate_window_end_seconds=materialized.stream_end_seconds,
        python_path=python_path,
        tdeed_root=tdeed_root,
        checkpoint=checkpoint,
        runner=runner,
    )
    result["candidate_window"].update(materialized.as_dict())
    result["total_seconds"] = round(
        materialized.materialize_seconds + float(result["inference_seconds"]), 3
    )
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--video", type=Path, required=True)
    parser.add_argument("--code", required=True)
    parser.add_argument("--expected-offset", type=float, required=True)
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument(
        "--max-anchor-distance",
        type=float,
        default=DEFAULT_MAX_ANCHOR_DISTANCE_SECONDS,
    )
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    parser.add_argument("--window-start", type=float, default=0.0)
    parser.add_argument("--window-end", type=float)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    try:
        result = locate_candidate_video(
            args.video,
            args.code,
            expected_offset_seconds=args.expected_offset,
            threshold=args.threshold,
            max_anchor_distance_seconds=args.max_anchor_distance,
            timeout_seconds=args.timeout_seconds,
            candidate_window_start_seconds=args.window_start,
            candidate_window_end_seconds=args.window_end,
        )
        exit_code = 0
    except VisionCandidateNotFound as exc:
        result = {
            "status": "not_found",
            "code": str(args.code).upper(),
            "error": str(exc),
        }
        exit_code = 2
    except Exception as exc:
        result = {
            "status": "failed",
            "code": str(args.code).upper(),
            "error": f"{type(exc).__name__}: {exc}",
        }
        exit_code = 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if exit_code:
        print(result["error"], file=sys.stderr)
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
