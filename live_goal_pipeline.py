#!/usr/bin/env python3
"""Run the goal-to-GIF pipeline against a live input or a live MP4 replay.

The supplied MP4 can be throttled to wall-clock speed with ``--simulate-live``.
The detector receives no event timestamps. For this first baseline it confirms
goals from a stable change in a configured broadcast scoreboard region.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Iterable

import numpy as np


@dataclass(frozen=True)
class Box:
    x1: int
    y1: int
    x2: int
    y2: int

    @property
    def width(self) -> int:
        return self.x2 - self.x1

    @property
    def height(self) -> int:
        return self.y2 - self.y1


@dataclass(frozen=True)
class Segment:
    path: Path
    start: float
    end: float


@dataclass
class PendingEvent:
    event_type: str
    stream_time: float
    source_time: float | None
    detected_wall_time: float
    change_fraction: float
    stability_fraction: float
    output_due_stream_time: float
    output_id: str | None = None
    status: str = "pending"
    result: dict = field(default_factory=dict)


class BufferNotReady(RuntimeError):
    """The rolling segment list has not closed enough post-roll data yet."""


class BufferUnavailable(RuntimeError):
    """The requested interval contains a permanent gap in received video."""


class CoverageStatus(str, Enum):
    READY_FULL = "ready_full"
    WAITING = "waiting"
    UNAVAILABLE = "unavailable"
    READY_DEGRADED = "ready_degraded"


@dataclass(frozen=True)
class VideoCoverage:
    """Pure coverage decision shared by default and visual GIF paths."""

    status: CoverageStatus
    requested_start: float
    requested_end: float
    anchor: float
    effective_start: float | None
    effective_end: float | None
    segments: tuple[Any, ...]
    gaps: tuple[tuple[float, float], ...]
    error_kind: str | None = None
    reason: str = ""

    @property
    def skipped_gap_seconds(self) -> float:
        return round(sum(end - start for start, end in self.gaps), 3)

    def validate_request(
        self,
        *,
        window_start: float,
        window_end: float,
        anchor: float,
    ) -> None:
        expected = (window_start, window_end, anchor)
        actual = (self.requested_start, self.requested_end, self.anchor)
        if any(abs(left - right) > 1e-6 for left, right in zip(actual, expected)):
            raise ValueError(
                "video coverage was computed for a different window or anchor"
            )


def analyze_video_coverage(
    segments: Iterable[Any],
    *,
    window_start: float,
    window_end: float,
    anchor: float,
    gap_tolerance: float = 0.5,
    edge_tolerance: float = 0.25,
    allow_degraded: bool = False,
    force_degraded: bool = False,
    min_degraded_seconds: float = 2.0,
) -> VideoCoverage:
    """Classify whether one requested interval can be materialized safely.

    Only a missing live tail is retryable. Missing history, internal gaps, and
    anchors outside all continuous components cannot be repaired by waiting.
    ``force_degraded`` is reserved for a caller that has reached its deadline:
    it permits using the anchor component even when the live tail is incomplete.
    """
    if window_start < 0:
        raise ValueError("video window start must not be negative")
    if window_end <= window_start:
        raise ValueError("video window end must be after its start")
    if not window_start <= anchor <= window_end:
        raise ValueError("video anchor must be inside the requested window")
    if gap_tolerance < 0 or edge_tolerance < 0:
        raise ValueError("video coverage tolerances must not be negative")
    if min_degraded_seconds < 0:
        raise ValueError("minimum degraded clip duration must not be negative")
    if force_degraded and not allow_degraded:
        raise ValueError("force_degraded requires allow_degraded")

    available = sorted(
        (
            segment
            for segment in segments
            if Path(segment.path).is_file()
        ),
        key=lambda segment: (float(segment.start), str(segment.path)),
    )
    selected = [
        segment
        for segment in available
        if float(segment.end) > window_start
        and float(segment.start) < window_end
    ]

    def decision(
        status: CoverageStatus,
        *,
        effective_start: float | None = None,
        effective_end: float | None = None,
        chosen: Iterable[Any] = (),
        gaps: Iterable[tuple[float, float]] = (),
        error_kind: str | None = None,
        reason: str,
    ) -> VideoCoverage:
        return VideoCoverage(
            status=status,
            requested_start=window_start,
            requested_end=window_end,
            anchor=anchor,
            effective_start=effective_start,
            effective_end=effective_end,
            segments=tuple(chosen),
            gaps=tuple(gaps),
            error_kind=error_kind,
            reason=reason,
        )

    if not selected:
        latest_end = max((float(item.end) for item in available), default=None)
        if latest_end is None or latest_end < window_end - edge_tolerance:
            return decision(
                CoverageStatus.WAITING,
                error_kind="waiting_for_video",
                reason=(
                    "no closed video segment covers the requested window yet"
                ),
            )
        return decision(
            CoverageStatus.UNAVAILABLE,
            error_kind="history_unavailable",
            reason="requested video history is no longer present in the buffer",
        )

    components: list[list[Any]] = [[selected[0]]]
    component_ends = [float(selected[0].end)]
    gaps: list[tuple[float, float]] = []
    for segment in selected[1:]:
        start = float(segment.start)
        previous_end = component_ends[-1]
        if start - previous_end > gap_tolerance:
            gap = (max(previous_end, window_start), min(start, window_end))
            if gap[1] > gap[0]:
                gaps.append(gap)
            components.append([segment])
            component_ends.append(float(segment.end))
        else:
            components[-1].append(segment)
            component_ends[-1] = max(previous_end, float(segment.end))

    component_bounds = [
        (float(items[0].start), component_ends[index])
        for index, items in enumerate(components)
    ]
    anchor_index = next(
        (
            index
            for index, (start, end) in enumerate(component_bounds)
            if start - edge_tolerance <= anchor <= end + edge_tolerance
        ),
        None,
    )

    latest_end = max(float(item.end) for item in available)
    if anchor_index is None:
        if latest_end < anchor - edge_tolerance:
            return decision(
                CoverageStatus.WAITING,
                gaps=gaps,
                error_kind="waiting_for_anchor",
                reason="the live buffer has not reached the event anchor yet",
            )
        return decision(
            CoverageStatus.UNAVAILABLE,
            gaps=gaps,
            error_kind="anchor_gap",
            reason="the event anchor falls inside missing video",
        )

    anchor_component = components[anchor_index]
    component_start, component_end = component_bounds[anchor_index]
    effective_start = max(window_start, component_start)
    effective_end = min(window_end, component_end)
    covers_start = component_start <= window_start + edge_tolerance
    covers_end = component_end >= window_end - edge_tolerance

    def degraded_decision(*, error_kind: str, reason: str) -> VideoCoverage:
        degraded_duration = effective_end - effective_start
        if degraded_duration < min_degraded_seconds:
            return decision(
                CoverageStatus.UNAVAILABLE,
                effective_start=effective_start,
                effective_end=effective_end,
                chosen=anchor_component,
                gaps=gaps,
                error_kind="degraded_clip_too_short",
                reason=(
                    "the anchor-side video component is too short for a degraded GIF: "
                    f"{degraded_duration:.3f}s < {min_degraded_seconds:.3f}s"
                ),
            )
        return decision(
            CoverageStatus.READY_DEGRADED,
            effective_start=effective_start,
            effective_end=effective_end,
            chosen=anchor_component,
            gaps=gaps,
            error_kind=error_kind,
            reason=reason,
        )

    if covers_start and covers_end:
        return decision(
            CoverageStatus.READY_FULL,
            effective_start=window_start,
            effective_end=window_end,
            chosen=anchor_component,
            gaps=gaps,
            reason="the requested window is continuously covered",
        )

    if not covers_start and not allow_degraded:
        error_kind = "internal_video_gap" if anchor_index > 0 else "history_unavailable"
        reason = (
            "the requested window contains a permanent video gap"
            if anchor_index > 0
            else "the beginning of the requested window is no longer available"
        )
        return decision(
            CoverageStatus.UNAVAILABLE,
            effective_start=effective_start,
            effective_end=effective_end,
            chosen=anchor_component,
            gaps=gaps,
            error_kind=error_kind,
            reason=reason,
        )

    if anchor_index < len(components) - 1 and not allow_degraded:
        return decision(
            CoverageStatus.UNAVAILABLE,
            effective_start=effective_start,
            effective_end=effective_end,
            chosen=anchor_component,
            gaps=gaps,
            error_kind="internal_video_gap",
            reason="the requested window contains a permanent video gap",
        )

    if force_degraded:
        return degraded_decision(
            error_kind="degraded_deadline",
            reason="using the anchor-side component at the video deadline",
        )

    if (
        not covers_end
        and anchor_index == len(components) - 1
        and latest_end < window_end - edge_tolerance
    ):
        return decision(
            CoverageStatus.WAITING,
            effective_start=effective_start,
            effective_end=effective_end,
            chosen=anchor_component,
            gaps=gaps,
            error_kind="waiting_for_tail",
            reason=(
                f"video tail has reached {latest_end:.3f}s but "
                f"{window_end:.3f}s is required"
            ),
        )

    if allow_degraded:
        return degraded_decision(
            error_kind="degraded_window",
            reason="using the continuous component that contains the event anchor",
        )

    return decision(
        CoverageStatus.UNAVAILABLE,
        effective_start=effective_start,
        effective_end=effective_end,
        chosen=anchor_component,
        gaps=gaps,
        error_kind="internal_video_gap",
        reason="the requested window contains a permanent video gap",
    )


def parse_box(value: str) -> Box:
    try:
        values = [int(part.strip()) for part in value.split(",")]
    except ValueError as exc:
        raise argparse.ArgumentTypeError("ROI must contain four integers") from exc
    if len(values) != 4:
        raise argparse.ArgumentTypeError("ROI must be x1,y1,x2,y2")
    box = Box(*values)
    if box.x1 < 0 or box.y1 < 0 or box.width <= 0 or box.height <= 0:
        raise argparse.ArgumentTypeError("ROI coordinates must define a positive box")
    return box


def run(
    command: list[str],
    *,
    cancel_event: threading.Event | None = None,
) -> subprocess.CompletedProcess[str]:
    if cancel_event is None:
        return subprocess.run(command, check=True, text=True, capture_output=True)

    process = subprocess.Popen(
        command,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    while True:
        if cancel_event.is_set():
            process.terminate()
            try:
                process.communicate(timeout=2.0)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
            raise RuntimeError("GIF encoding cancelled after graceful stop timeout")
        try:
            stdout, stderr = process.communicate(timeout=0.1)
            break
        except subprocess.TimeoutExpired:
            continue
    completed = subprocess.CompletedProcess(
        command,
        process.returncode,
        stdout=stdout,
        stderr=stderr,
    )
    completed.check_returncode()
    return completed


class ScoreboardGoalDetector:
    """Detect a persistent score glyph change without knowing score values."""

    def __init__(self, anchor: Box, score: Box, *, cooldown: float = 45.0) -> None:
        if not (
            anchor.x1 <= score.x1 < score.x2 <= anchor.x2
            and anchor.y1 <= score.y1 < score.y2 <= anchor.y2
        ):
            raise ValueError("score ROI must be contained in anchor ROI")
        if score.y1 != anchor.y1 or score.y2 != anchor.y2:
            raise ValueError("the current detector requires equal anchor and score heights")
        self.score_x1 = score.x1 - anchor.x1
        self.score_x2 = score.x2 - anchor.x1
        self.cooldown = cooldown
        self.baseline: np.ndarray | None = None
        self.bootstrap: list[np.ndarray] = []
        self.candidate: list[tuple[float, np.ndarray, float]] = []
        self.cooldown_until = 0.0
        self.present_samples = 0

    @staticmethod
    def _region_is_team_label(region: np.ndarray) -> bool:
        white_fraction = float(np.mean(region > 180))
        dark_fraction = float(np.mean(region < 80))
        return (
            float(np.std(region)) > 55.0
            and 0.07 < white_fraction < 0.20
            and dark_fraction > 0.65
        )

    def _scoreboard_is_present(self, frame: np.ndarray) -> bool:
        margin = 6
        left = frame[:, : max(1, self.score_x1 - margin)]
        right = frame[:, min(frame.shape[1] - 1, self.score_x2 + margin) :]
        score = frame[:, self.score_x1 : self.score_x2]
        score_white = float(np.mean(score > 180))
        score_dark = float(np.mean(score < 80))
        return (
            self._region_is_team_label(left)
            and self._region_is_team_label(right)
            and float(np.std(score)) > 60.0
            and 0.10 < score_white < 0.35
            and score_dark > 0.50
        )

    @staticmethod
    def _max_pairwise_difference(masks: list[np.ndarray]) -> float:
        return max(
            float(np.mean(masks[left] != masks[right]))
            for left in range(len(masks))
            for right in range(left + 1, len(masks))
        )

    @staticmethod
    def _median_mask(masks: list[np.ndarray]) -> np.ndarray:
        return np.median(np.stack(masks), axis=0) >= 0.5

    def process(self, frame: np.ndarray, stream_time: float) -> dict | None:
        if not self._scoreboard_is_present(frame):
            self.candidate.clear()
            self.bootstrap.clear()
            return None

        self.present_samples += 1
        score_mask = frame[:, self.score_x1 : self.score_x2] > 150
        if self.baseline is None:
            self.bootstrap.append(score_mask.copy())
            self.bootstrap = self.bootstrap[-3:]
            if (
                len(self.bootstrap) == 3
                and self._max_pairwise_difference(self.bootstrap) < 0.03
            ):
                self.baseline = self._median_mask(self.bootstrap)
            return None

        difference = float(np.mean(score_mask != self.baseline))
        if difference > 0.04 and stream_time >= self.cooldown_until:
            self.candidate.append((stream_time, score_mask.copy(), difference))
            self.candidate = self.candidate[-3:]
            if len(self.candidate) < 3:
                return None
            masks = [item[1] for item in self.candidate]
            stability = self._max_pairwise_difference(masks)
            if stability >= 0.03:
                return None
            first_time = self.candidate[0][0]
            self.baseline = self._median_mask(masks)
            self.cooldown_until = stream_time + self.cooldown
            self.candidate.clear()
            return {
                "stream_time": first_time,
                "change_fraction": difference,
                "stability_fraction": stability,
            }

        if difference <= 0.025:
            self.candidate.clear()
        return None


def read_segments(
    list_path: Path, buffer_dir: Path, time_offset: float = 0.0
) -> list[Segment]:
    if not list_path.exists():
        return []
    segments: list[Segment] = []
    try:
        with list_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.reader(handle):
                if len(row) < 3:
                    continue
                path = Path(row[0])
                if not path.is_absolute():
                    path = buffer_dir / path
                segments.append(
                    Segment(
                        path,
                        float(row[1]) + time_offset,
                        float(row[2]) + time_offset,
                    )
                )
    except (OSError, ValueError):
        # FFmpeg may be appending the last CSV row while it is being read.
        return []
    return segments


def concat_escape(path: Path) -> str:
    return str(path.resolve()).replace("'", "'\\''")


def encode_gif(
    ffmpeg: str,
    ffprobe: str,
    segments: list[Segment],
    event: PendingEvent,
    output_dir: Path,
    *,
    before: float,
    after: float,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    cancel_event: threading.Event | None = None,
    coverage: VideoCoverage | None = None,
    allow_degraded: bool = False,
    output_filename: str | None = None,
) -> dict:
    requested_start = max(0.0, event.stream_time - before)
    requested_end = event.stream_time + after
    coverage = coverage or analyze_video_coverage(
        segments,
        window_start=requested_start,
        window_end=requested_end,
        anchor=event.stream_time,
        allow_degraded=allow_degraded,
    )
    coverage.validate_request(
        window_start=requested_start,
        window_end=requested_end,
        anchor=event.stream_time,
    )
    if coverage.status == CoverageStatus.WAITING:
        raise BufferNotReady(coverage.reason)
    if coverage.status == CoverageStatus.UNAVAILABLE:
        raise BufferUnavailable(coverage.reason)
    if coverage.effective_start is None or coverage.effective_end is None:
        raise BufferUnavailable("video coverage has no usable interval")
    selected = list(coverage.segments)
    wanted_start = coverage.effective_start
    wanted_end = coverage.effective_end

    if output_filename is not None:
        requested_filename = str(output_filename)
        filename_path = Path(requested_filename)
        if (
            filename_path.is_absolute()
            or filename_path.name != requested_filename
            or "\\" in requested_filename
            or filename_path.suffix.lower() != ".gif"
        ):
            raise ValueError("output_filename must be a plain .gif filename")
        stem = filename_path.stem
    else:
        label_time = (
            event.source_time if event.source_time is not None else event.stream_time
        )
        stem = f"{event.event_type.lower()}_{label_time:09.3f}"
        if event.output_id:
            safe_output_id = "".join(
                character
                for character in event.output_id
                if character.isalnum() or character in ("-", "_")
            )
            if safe_output_id:
                stem = f"{stem}_{safe_output_id}"
    concat_path = output_dir / f"{stem}_segments.txt"
    concat_path.write_text(
        "".join(f"file '{concat_escape(segment.path)}'\n" for segment in selected),
        encoding="utf-8",
    )
    output = output_dir / (output_filename or f"{stem}.gif")
    try:
        seek = max(0.0, wanted_start - selected[0].start)
        wanted_duration = wanted_end - wanted_start
        encode_started = time.perf_counter()
        video_filter = (
            f"fps={fps:g},scale={width}:-2:flags=lanczos,split[s0][s1];"
            f"[s0]palettegen=max_colors={colors}:stats_mode=diff[p];"
            "[s1][p]paletteuse=dither=sierra2_4a"
        )
        completed = run(
            [
                ffmpeg,
                "-y",
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
                f"{wanted_duration:.3f}",
                "-vf",
                video_filter,
                "-loop",
                "0",
                str(output),
            ],
            cancel_event=cancel_event,
        )
        if not output.is_file():
            raise RuntimeError(f"FFmpeg did not create GIF output: {output}")
        size = output.stat().st_size
        if size <= 0:
            raise RuntimeError(f"FFmpeg created an empty GIF output: {output}")
        encoding = {
            "width": width,
            "fps": fps,
            "colors": colors,
            "bytes": size,
            "encode_seconds": round(time.perf_counter() - encode_started, 3),
        }
        if completed.stderr:
            encoding["ffmpeg_stderr"] = completed.stderr[-2000:]

        probe = json.loads(
            run(
                [
                    ffprobe,
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration,size:stream=width,height,r_frame_rate",
                    "-of",
                    "json",
                    str(output),
                ],
                cancel_event=cancel_event,
            ).stdout
        )
        stream = (probe.get("streams") or [{}])[0]
        fmt = probe.get("format") or {}
        return {
            "output": str(output.resolve()),
            "bytes": int(fmt.get("size", output.stat().st_size)),
            "duration_sec": float(fmt.get("duration", wanted_duration)),
            "width": stream.get("width"),
            "height": stream.get("height"),
            "fps": stream.get("r_frame_rate"),
            "clip_stream_start_sec": wanted_start,
            "clip_stream_end_sec": wanted_end,
            "requested_clip_stream_start_sec": requested_start,
            "requested_clip_stream_end_sec": requested_end,
            "buffer_coverage_sec": [selected[0].start, selected[-1].end],
            "selected_segment_count": len(selected),
            "encode_seconds": round(time.perf_counter() - encode_started, 3),
            "encoding": encoding,
            "size_reference_bytes": size_reference_bytes,
            "over_size_reference": size > size_reference_bytes,
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
        }
    finally:
        concat_path.unlink(missing_ok=True)


def prune_buffer(
    segments: list[Segment],
    stream_time: float,
    buffer_seconds: float,
    events: list[PendingEvent],
    before: float,
    protected_paths: set[str] | None = None,
    extra_cutoffs: list[float] | None = None,
) -> None:
    normal_cutoff = stream_time - buffer_seconds
    pending_starts = [
        max(0.0, event.stream_time - before)
        for event in events
        if event.status in ("pending", "encoding")
    ]
    retention_cutoffs = [*pending_starts, *(extra_cutoffs or [])]
    cutoff = min([normal_cutoff, *retention_cutoffs]) if retention_cutoffs else normal_cutoff
    protected = protected_paths or set()
    for segment in segments:
        if (
            segment.end < cutoff
            and str(segment.path.resolve()) not in protected
            and segment.path.exists()
        ):
            segment.path.unlink()


def source_is_local(source: str) -> bool:
    return "://" not in source


def build_ingest_command(
    ffmpeg: str,
    args: argparse.Namespace,
    anchor: Box,
    buffer_dir: Path,
    segment_list: Path,
) -> list[str]:
    command = [ffmpeg, "-y", "-nostdin", "-hide_banner", "-loglevel", "error"]
    if args.simulate_live:
        command.append("-re")
    if args.start > 0:
        command.extend(["-ss", f"{args.start:.3f}"])
    if args.duration is not None:
        command.extend(["-t", f"{args.duration:.3f}"])
    if args.source.lower().startswith("rtmp://"):
        command.extend(["-rw_timeout", "15000000", "-rtmp_live", "live"])
    command.extend(["-i", args.source])
    command.extend(
        [
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-bsf:v",
            "h264_mp4toannexb,dump_extra",
            "-f",
            "segment",
            "-segment_time",
            f"{args.segment_seconds:.3f}",
            "-reset_timestamps",
            "1",
            "-segment_list",
            str(segment_list),
            "-segment_list_type",
            "csv",
            str(buffer_dir / "segment_%06d.ts"),
            "-map",
            "0:v:0",
            "-vf",
            (
                f"fps={args.analysis_fps},"
                f"crop={anchor.width}:{anchor.height}:{anchor.x1}:{anchor.y1},"
                "format=gray"
            ),
            "-an",
            "-f",
            "rawvideo",
            "-pix_fmt",
            "gray",
            "pipe:1",
        ]
    )
    return command


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="RTMP URL or local MP4 path")
    parser.add_argument(
        "--simulate-live",
        action="store_true",
        help="read a local recording at 1x wall-clock speed",
    )
    parser.add_argument("--start", type=float, default=0.0)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--analysis-fps", type=float, default=2.0)
    parser.add_argument("--segment-seconds", type=float, default=2.0)
    parser.add_argument("--buffer-seconds", type=float, default=120.0)
    parser.add_argument("--before", type=float, default=12.0)
    parser.add_argument("--after", type=float, default=18.0)
    parser.add_argument("--segment-slack", type=float, default=7.0)
    parser.add_argument("--gif-width", type=int, default=768)
    parser.add_argument("--gif-fps", type=float, default=16.0)
    parser.add_argument("--gif-colors", type=int, default=256)
    parser.add_argument(
        "--gif-size-reference-mb",
        "--gif-max-mb",
        dest="gif_size_reference_mb",
        type=float,
        default=10.0,
        help="reporting threshold only; fixed GIF quality is never reduced",
    )
    parser.add_argument(
        "--anchor-roi",
        type=parse_box,
        default=parse_box("85,42,307,66"),
        help="full scoreboard x1,y1,x2,y2 for this broadcaster",
    )
    parser.add_argument(
        "--score-roi",
        type=parse_box,
        default=parse_box("178,42,214,66"),
        help="score-only x1,y1,x2,y2 for this broadcaster",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("output_gifs/live"))
    args = parser.parse_args()

    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    if not Path(ffmpeg).exists() or not Path(ffprobe).exists():
        raise SystemExit("ffmpeg and ffprobe are required")
    if source_is_local(args.source) and not Path(args.source).is_file():
        raise SystemExit(f"source does not exist: {args.source}")
    if args.simulate_live and not source_is_local(args.source):
        raise SystemExit("--simulate-live is only for a local recording")
    positive = (
        args.analysis_fps,
        args.segment_seconds,
        args.buffer_seconds,
        args.after,
        args.segment_slack,
        args.gif_width,
        args.gif_fps,
        args.gif_size_reference_mb,
    )
    if any(value <= 0 for value in positive) or args.before < 0 or args.start < 0:
        raise SystemExit("time, FPS, buffer, and size arguments must be positive")
    if not 2 <= args.gif_colors <= 256:
        raise SystemExit("--gif-colors must be between 2 and 256")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    buffer_dir = args.output_dir / "buffer"
    buffer_dir.mkdir(parents=True, exist_ok=True)
    segment_list = buffer_dir / "segments.csv"
    detector = ScoreboardGoalDetector(args.anchor_roi, args.score_roi)
    command = build_ingest_command(
        ffmpeg, args, args.anchor_roi, buffer_dir, segment_list
    )
    ffmpeg_log_path = args.output_dir / "ingest_ffmpeg.log"
    report_path = args.output_dir / "live_report.json"
    events: list[PendingEvent] = []
    frame_size = args.anchor_roi.width * args.anchor_roi.height
    frame_index = 0
    pipeline_started = time.perf_counter()
    last_prune_second = -1

    print(
        f"[ingest] source={args.source} simulate_live={args.simulate_live} "
        f"analysis_fps={args.analysis_fps:g}"
    )
    print(
        f"[buffer] segment={args.segment_seconds:g}s window={args.buffer_seconds:g}s "
        f"gif={args.gif_width}px/{args.gif_fps:g}fps/{args.gif_colors}colors "
        f"size_reference={args.gif_size_reference_mb:g}MB"
    )
    with ffmpeg_log_path.open("w", encoding="utf-8") as ffmpeg_log:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=ffmpeg_log,
        )
        assert process.stdout is not None
        while True:
            raw = process.stdout.read(frame_size)
            if len(raw) != frame_size:
                break
            stream_time = frame_index / args.analysis_fps
            frame_index += 1
            frame = np.frombuffer(raw, dtype=np.uint8).reshape(
                args.anchor_roi.height, args.anchor_roi.width
            )
            detection = detector.process(frame, stream_time)
            if detection is not None:
                event_stream_time = float(detection["stream_time"])
                source_time = (
                    args.start + event_stream_time
                    if source_is_local(args.source)
                    else None
                )
                event = PendingEvent(
                    event_type="goal",
                    stream_time=event_stream_time,
                    source_time=source_time,
                    detected_wall_time=time.time(),
                    change_fraction=float(detection["change_fraction"]),
                    stability_fraction=float(detection["stability_fraction"]),
                    output_due_stream_time=(
                        event_stream_time + args.after + args.segment_slack
                    ),
                )
                events.append(event)
                source_label = (
                    f" source={source_time:.1f}s" if source_time is not None else ""
                )
                print(
                    f"[goal] confirmed stream={event_stream_time:.1f}s{source_label} "
                    f"change={event.change_fraction:.3f}"
                )

            for event in events:
                if event.status != "pending" or stream_time < event.output_due_stream_time:
                    continue
                segments = read_segments(segment_list, buffer_dir)
                try:
                    encoded = encode_gif(
                        ffmpeg,
                        ffprobe,
                        segments,
                        event,
                        args.output_dir,
                        before=args.before,
                        after=args.after,
                        width=args.gif_width,
                        fps=args.gif_fps,
                        colors=args.gif_colors,
                        size_reference_bytes=int(
                            args.gif_size_reference_mb * 1_000_000
                        ),
                    )
                    event.status = "encoded"
                    event.result = encoded
                    ready_seconds = time.time() - event.detected_wall_time
                    event.result["seconds_after_confirmation"] = round(ready_seconds, 3)
                    size_notice = (
                        " above reference"
                        if encoded["over_size_reference"]
                        else ""
                    )
                    print(
                        f"[gif] {encoded['output']} {encoded['bytes'] / 1_000_000:.2f}MB "
                        f"ready={ready_seconds:.2f}s after confirmation{size_notice}"
                    )
                except BufferNotReady:
                    # Segment boundaries follow source keyframes, so the CSV can lag
                    # the analysis frames by several seconds. Retry on the next frame.
                    continue
                except Exception as exc:  # keep the live reader and report the event failure
                    event.status = "failed"
                    event.result = {"error": str(exc)}
                    print(f"[gif:error] {exc}")

            current_second = int(stream_time)
            if current_second != last_prune_second:
                prune_buffer(
                    read_segments(segment_list, buffer_dir),
                    stream_time,
                    args.buffer_seconds,
                    events,
                    args.before,
                )
                last_prune_second = current_second

        return_code = process.wait()

    # A short input may end just as the final segment closes. Try any due event once more.
    final_stream_time = frame_index / args.analysis_fps
    for event in events:
        if event.status != "pending" or final_stream_time < event.stream_time + args.after:
            continue
        try:
            encoded = encode_gif(
                ffmpeg,
                ffprobe,
                read_segments(segment_list, buffer_dir),
                event,
                args.output_dir,
                before=args.before,
                after=args.after,
                width=args.gif_width,
                fps=args.gif_fps,
                colors=args.gif_colors,
                size_reference_bytes=int(args.gif_size_reference_mb * 1_000_000),
            )
            event.status = "encoded"
            event.result = encoded
            ready_seconds = time.time() - event.detected_wall_time
            event.result["seconds_after_confirmation"] = round(ready_seconds, 3)
            size_notice = (
                " above reference" if encoded["over_size_reference"] else ""
            )
            print(
                f"[gif] {encoded['output']} {encoded['bytes'] / 1_000_000:.2f}MB "
                f"ready={ready_seconds:.2f}s after confirmation{size_notice}"
            )
        except Exception as exc:
            event.status = "failed"
            event.result = {"error": str(exc)}

    report = {
        "source": args.source,
        "mode": "1x_live_replay" if args.simulate_live else "unthrottled_stream_replay",
        "source_start_sec": args.start if source_is_local(args.source) else None,
        "requested_duration_sec": args.duration,
        "processed_stream_seconds": round(final_stream_time, 3),
        "processing_wall_seconds": round(time.perf_counter() - pipeline_started, 3),
        "ffmpeg_return_code": return_code,
        "detector": {
            "method": "stable scoreboard glyph transition",
            "anchor_roi": vars(args.anchor_roi),
            "score_roi": vars(args.score_roi),
            "analysis_fps": args.analysis_fps,
            "scoreboard_present_samples": detector.present_samples,
            "manual_event_timestamps_used": False,
        },
        "buffer": {
            "segment_seconds": args.segment_seconds,
            "window_seconds": args.buffer_seconds,
        },
        "gif": {
            "before_seconds": args.before,
            "after_seconds": args.after,
            "width": args.gif_width,
            "fps": args.gif_fps,
            "colors": args.gif_colors,
            "size_reference_bytes": int(args.gif_size_reference_mb * 1_000_000),
            "adaptive_quality_reduction": False,
        },
        "events": [
            {
                "type": event.event_type,
                "confirmation_stream_time_sec": event.stream_time,
                "confirmation_source_time_sec": event.source_time,
                "change_fraction": event.change_fraction,
                "stability_fraction": event.stability_fraction,
                "status": event.status,
                **event.result,
            }
            for event in events
        ],
        "limitations": [
            "This baseline requires the configured broadcaster scoreboard overlay.",
            "A scoreboard-free temporal goal action model is not integrated yet.",
            "RTMP reconnect supervision is not part of this validation run.",
        ],
    }
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"[report] {report_path.resolve()}")
    if return_code != 0:
        raise SystemExit(f"ingest ffmpeg exited with status {return_code}")


if __name__ == "__main__":
    main()
