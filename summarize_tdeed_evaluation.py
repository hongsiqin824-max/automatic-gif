#!/usr/bin/env python3
"""Summarize T-DEED goal localization results from short video windows."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "tdeed_eval_manifest.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "tmp" / "verification" / "tdeed_evaluation_report.json",
    )
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    args = parse_args()
    manifest = json.loads(args.manifest.read_text())
    fps = float(manifest["analysis_fps"])
    threshold = float(manifest["threshold"])
    tolerance = float(manifest["tolerance_seconds"])

    samples: list[dict[str, Any]] = []
    true_positives = 0
    false_positives = 0
    positive_count = 0
    negative_count = 0
    wall_seconds: list[float] = []

    for sample in manifest["samples"]:
        result_path = ROOT / sample["result"]
        raw = json.loads(result_path.read_text())
        candidates = []
        for event in raw["predictions"]:
            if event["label"] != "Goal" or float(event["confidence"]) < threshold:
                continue
            clip_second = float(event["frame"]) / fps
            source_second = float(sample["source_start_sec"]) + clip_second
            candidates.append(
                {
                    "clip_second": round(clip_second, 3),
                    "source_second": round(source_second, 3),
                    "confidence": round(float(event["confidence"]), 6),
                }
            )

        expected = sample["expected_event_sec"]
        if expected is None:
            negative_count += 1
            detected = bool(candidates)
            false_positives += int(detected)
            best = None
        else:
            positive_count += 1
            best = min(
                candidates,
                key=lambda item: abs(item["source_second"] - float(expected)),
                default=None,
            )
            detected = bool(
                best
                and abs(best["source_second"] - float(expected)) <= tolerance
            )
            true_positives += int(detected)

        measured_wall = sample.get("wall_seconds")
        if measured_wall is not None:
            wall_seconds.append(float(measured_wall))

        samples.append(
            {
                "name": sample["name"],
                "source_start_sec": sample["source_start_sec"],
                "expected_event_sec": expected,
                "goal_candidates": candidates,
                "best_candidate": best,
                "localization_error_sec": (
                    round(best["source_second"] - float(expected), 3)
                    if expected is not None and best is not None
                    else None
                ),
                "detected": detected,
                "wall_seconds": measured_wall,
            }
        )

    false_negatives = positive_count - true_positives
    checkpoint = (
        ROOT
        / "tmp"
        / "T-DEED"
        / "checkpoints"
        / "SoccerNet"
        / "SoccerNet_small"
        / "checkpoint_best.pt"
    )
    actual_checkpoint_sha256 = sha256(checkpoint)
    expected_checkpoint_sha256 = manifest["checkpoint_sha256"]
    report = {
        "model": manifest["model"],
        "model_repository": manifest["model_repository"],
        "checkpoint": str(checkpoint.relative_to(ROOT)),
        "checkpoint_sha256": actual_checkpoint_sha256,
        "checkpoint_sha256_matches_manifest": (
            actual_checkpoint_sha256 == expected_checkpoint_sha256
        ),
        "input": {
            "analysis_fps": fps,
            "width": manifest["input_width"],
            "height": manifest["input_height"],
            "window_seconds": 120,
        },
        "evaluation": {
            "event_class": "Goal",
            "threshold": threshold,
            "tolerance_seconds": tolerance,
            "positive_windows": positive_count,
            "negative_windows": negative_count,
            "true_positives": true_positives,
            "false_positives": false_positives,
            "false_negatives": false_negatives,
            "recall_on_this_small_set": round(
                true_positives / positive_count, 4
            ) if positive_count else None,
            "precision_on_this_small_set": round(
                true_positives / (true_positives + false_positives), 4
            ) if true_positives + false_positives else None,
            "median_single_process_wall_seconds": round(
                statistics.median(wall_seconds), 3
            ) if wall_seconds else None,
        },
        "samples": samples,
        "limitations": [
            "Only one match and three positive windows were evaluated.",
            "Negative windows were selected samples, not a full-match false-positive scan.",
            "No verified yellow-card or red-card footage was available for evaluation.",
            "Manual event timestamps are approximate review anchors.",
            "This result does not establish a production accuracy guarantee.",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
