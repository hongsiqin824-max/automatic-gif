#!/usr/bin/env python3
"""Validate the football highlight pipeline on a local recording.

This first-stage tool deliberately separates event timing from event detection.
Known event timestamps can be used to validate the rolling-window equivalent
(retroactive extraction), GIF encoding, and latency budget before a detector is
connected to the live RTMP reader.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Event:
    kind: str
    timestamp: float


def run(command: list[str], *, capture: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=capture,
    )


def media_info(path: Path) -> dict:
    result = run(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=filename,duration,size,format_name:"
            "stream=index,codec_name,codec_type,width,height,r_frame_rate,"
            "avg_frame_rate,sample_rate,channels",
            "-of",
            "json",
            str(path),
        ]
    )
    return json.loads(result.stdout)


def parse_events(value: str) -> list[Event]:
    """Parse ``goal@1046,goal@5039,goal@6566``."""
    events: list[Event] = []
    for item in value.split(","):
        kind, separator, timestamp = item.strip().partition("@")
        if not separator or not kind or not timestamp:
            raise ValueError(f"invalid event {item!r}; expected kind@seconds")
        events.append(Event(kind=kind, timestamp=float(timestamp)))
    return events


def extract_gif(
    source: Path,
    event: Event,
    output_dir: Path,
    before: float,
    after: float,
    fps: int,
    width: int,
) -> dict:
    start = max(0.0, event.timestamp - before)
    duration = before + after
    output = output_dir / f"{event.kind}_{event.timestamp:09.3f}.gif"
    started = time.perf_counter()

    # Two-pass palette generation avoids the severe banding produced by a
    # direct GIF conversion while keeping the source aspect ratio.
    vf = (
        f"fps={fps},scale={width}:-2:flags=lanczos,"
        "split[s0][s1];[s0]palettegen=max_colors=192:stats_mode=diff[p];"
        "[s1][p]paletteuse=dither=sierra2_4a"
    )
    run(
        [
            "ffmpeg",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-ss",
            f"{start:.3f}",
            "-t",
            f"{duration:.3f}",
            "-i",
            str(source),
            "-vf",
            vf,
            "-loop",
            "0",
            str(output),
        ]
    )
    elapsed = time.perf_counter() - started

    probe = json.loads(
        run(
            [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration,size:stream=width,height,r_frame_rate",
                "-of",
                "json",
                str(output),
            ]
        ).stdout
    )
    stream = (probe.get("streams") or [{}])[0]
    fmt = probe.get("format") or {}
    return {
        "event": event.kind,
        "event_timestamp_sec": event.timestamp,
        "clip_start_sec": start,
        "clip_duration_sec": float(fmt.get("duration", duration)),
        "fps": stream.get("r_frame_rate"),
        "width": stream.get("width"),
        "height": stream.get("height"),
        "bytes": int(fmt.get("size", output.stat().st_size)),
        "encode_seconds": round(elapsed, 3),
        "output": str(output),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument(
        "--events",
        default="goal@1046,goal@5039,goal@6566",
        help="comma-separated event labels and source timestamps",
    )
    parser.add_argument("--before", type=float, default=12.0)
    parser.add_argument("--after", type=float, default=24.0)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--output-dir", type=Path, default=Path("output_gifs"))
    args = parser.parse_args()

    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        raise SystemExit("ffmpeg and ffprobe are required")
    if not args.source.is_file():
        raise SystemExit(f"source does not exist: {args.source}")
    if args.before < 0 or args.after <= 0 or args.fps <= 0 or args.width <= 0:
        raise SystemExit("before/after/fps/width must be positive")

    info = media_info(args.source)
    events = parse_events(args.events)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    results = [
        extract_gif(
            args.source,
            event,
            args.output_dir,
            args.before,
            args.after,
            args.fps,
            args.width,
        )
        for event in events
    ]
    report = {
        "source": str(args.source),
        "source_media": info,
        "parameters": {
            "before_seconds": args.before,
            "after_seconds": args.after,
            "fps": args.fps,
            "width": args.width,
        },
        "events": results,
    }
    report_path = args.output_dir / "validation_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
