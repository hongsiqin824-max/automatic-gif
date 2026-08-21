from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from live_goal_pipeline import Segment
from pipeline_runtime import PipelineRuntime
from vision_runtime import (
    VisionJob,
    VisualLocationFailed,
    _ocr_target_not_located_diagnostics,
    _ocr_progressive_coverage_diagnostics,
    process_vision_artifact,
)


class OcrFailureModeTests(unittest.TestCase):
    """Regression coverage for OCR deadline and retained-history diagnostics."""

    def test_target_not_located_classifies_crossed_target(self):
        details = _ocr_target_not_located_diagnostics(
            {
                "exact_second_failure_reason": "target_clock_not_found",
                "isolated_target_reading_count": 0,
            },
            target_clock_seconds=455,
            latest_trusted_clock_seconds=457,
            coverage_diagnostics={
                "coverage_class": "clock_passed_without_anchor",
            },
        )
        self.assertEqual(details["target_failure_cause"], "target_passed")
        self.assertTrue(details["target_passed_without_anchor"])
        self.assertEqual(details["target_wait_outcome"], "clock_passed_without_anchor")

    def test_target_not_located_classifies_isolated_reading(self):
        details = _ocr_target_not_located_diagnostics(
            {
                "exact_second_failure_reason": "target_clock_not_found",
                "isolated_target_reading_count": 1,
            },
            target_clock_seconds=455,
            latest_trusted_clock_seconds=455,
        )
        self.assertEqual(details["target_failure_cause"], "isolated")
        self.assertTrue(details["target_passed_without_anchor"])
        self.assertIn("单帧", details["target_failure_explanation"])

    def test_target_not_located_classifies_evicted_window(self):
        details = _ocr_target_not_located_diagnostics(
            {},
            target_clock_seconds=455,
            latest_trusted_clock_seconds=460,
            coverage_diagnostics={
                "coverage_class": "history_unavailable",
                "target_history_fully_missing": True,
            },
        )
        self.assertEqual(details["target_failure_cause"], "window_evicted")
        self.assertTrue(details["target_history_fully_missing"])

    @staticmethod
    def _create_progressive_ocr_job(
        root: Path,
        suffix: str,
        *,
        observed_stream_time: float = 200.0,
        event_minute: str = "2",
        event_second: int | None = 120,
        search_start_stream_time: float = 80.0,
        deadline_at_unix: float | None = None,
    ) -> tuple[PipelineRuntime, VisionJob]:
        runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
        event_key = f"match:G:{suffix}"
        runtime.discover_task(
            match_id="match",
            event_data={
                "event_key": event_key,
                "code": "G",
                "event_type": "goal",
                "minute": event_minute,
                "minute_extra": "0",
                "second": event_second,
                "team": "teamA",
                "person": "A",
                "person_id": "1",
                "score": "1-0",
                "reason": "",
                "metadata": {},
            },
            observed_stream_time=observed_stream_time,
            observed_source_time=None,
            clip_anchor_stream_time=observed_stream_time,
            clip_anchor_source_time=None,
            output_due_stream_time=observed_stream_time + 30.0,
            detected_at_unix=time.time(),
        )
        runtime.enqueue_vision_task(
            event_key,
            artifact_kind="ocr_window",
            search_start_stream_time=search_start_stream_time,
            search_end_stream_time=observed_stream_time + 30.0,
            clip_before_seconds=30.0,
            clip_after_seconds=30.0,
            deadline_at_unix=(
                time.time() + 300.0
                if deadline_at_unix is None
                else deadline_at_unix
            ),
        )
        return runtime, VisionJob(
            event_key,
            "match",
            "G",
            "goal",
            observed_stream_time,
            None,
            time.time(),
            observed_anchor_stream_time=observed_stream_time,
            event_minute=event_minute,
            event_second=event_second,
            target_score="1-0",
            clock_only=True,
        )

    @staticmethod
    def _run_progressive_ocr(
        job: VisionJob,
        runtime: PipelineRuntime,
        segment_reader,
        root: Path,
    ) -> bool:
        return process_vision_artifact(
            job,
            runtime,
            segment_reader,
            "ffmpeg",
            "ffprobe",
            root,
            artifact_kind="ocr_window",
            search_before=120.0,
            search_after=30.0,
            refined_before=8.0,
            refined_after=12.0,
            width=768,
            fps=16.0,
            colors=256,
            size_reference_bytes=10_000_000,
            python=Path("python"),
            timeout_seconds=3.0,
            ocr_python=root / "ocr-python",
            ocr_timeout_seconds=3.0,
        )

    def test_no_clock_wait_continues_when_media_tail_advances(self):
        """A growing live tail must not be treated as a terminal no-clock miss."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "no-clock-tail-growth"
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertIsNotNone(task)
            created = task.created_at_unix
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")

            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "the clock ROI was unreadable in this scan",
                {"scoreboard_missing": True},
            )
            # First pass establishes a media-tail baseline while the initial
            # target budget is still open.
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ),
                patch("vision_runtime.time.time", return_value=created + 10.0),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 120.0)],
                        root,
                    )
                )

            # 100s is beyond the initial 60s target wait but below the hard
            # 180s event limit.  The refreshed tail grows from 120s to 190s,
            # so a no-clock miss must extend readiness instead of failing.
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ),
                patch("vision_runtime.time.time", return_value=created + 100.0),
            ):
                segment_sets = iter(
                    (
                        [Segment(segment_path, 80.0, 150.0)],
                        [Segment(segment_path, 80.0, 190.0)],
                    )
                )
                completed = self._run_progressive_ocr(
                    job,
                    runtime,
                    lambda: next(segment_sets),
                    root,
                )

            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertFalse(completed)
            self.assertEqual(waiting.status, "pending")
            self.assertEqual(waiting.last_error_kind, "waiting_for_clock_target")
            progress = waiting.window_metadata["progressive_scan"]
            self.assertTrue(progress["last_scan_diagnostics"]["media_tail_grew_during_scan"])
            self.assertIsNone(progress["latest_trusted_clock_seconds"])
            policy = progress["deadline_policy"]
            self.assertTrue(policy["media_progress_observed"])
            self.assertGreater(policy["target_deadline_at_unix"], created + 100.0)
            # The OCR branch is optional; a miss must never remove the default
            # artifact or turn it into a successful visual result.
            self.assertTrue(waiting.result["default_gif_preserved"])
            runtime.close()

    def test_coverage_classifies_evicted_target_window(self):
        """An absent target window is different from a merely short look-back."""
        target = _ocr_progressive_coverage_diagnostics(
            [Segment(Path("segment.ts"), 130.0, 230.0)],
            intended_initial_start=80.0,
            requested_start=80.0,
            requested_end=230.0,
            scan_start=80.0,
            scan_end=120.0,
            target_clock_seconds=120,
            latest_trusted_clock_seconds=None,
            target_window={
                "start_stream_time": 80.0,
                "end_stream_time": 120.0,
            },
        )
        self.assertEqual(target["coverage_class"], "history_unavailable")
        self.assertTrue(target["target_history_missing"])
        self.assertTrue(target["target_history_fully_missing"])
        self.assertEqual(target["history_missing_seconds"], 50.0)

    def test_coverage_classifies_waiting_and_stalled_media(self):
        """A tail before the target is waiting until it stops growing."""
        waiting = _ocr_progressive_coverage_diagnostics(
            [Segment(Path("segment.ts"), 80.0, 100.0)],
            intended_initial_start=80.0,
            requested_start=80.0,
            requested_end=150.0,
            scan_start=120.0,
            scan_end=150.0,
            target_clock_seconds=120,
            latest_trusted_clock_seconds=90,
            previous_media_end_stream_time=90.0,
            target_window={"start_stream_time": 120.0, "end_stream_time": 150.0},
        )
        self.assertEqual(waiting["coverage_class"], "waiting_for_media")
        self.assertTrue(waiting["target_media_not_arrived"])
        self.assertFalse(waiting["media_stalled"])

        stalled = _ocr_progressive_coverage_diagnostics(
            [Segment(Path("segment.ts"), 80.0, 100.0)],
            intended_initial_start=80.0,
            requested_start=80.0,
            requested_end=150.0,
            scan_start=120.0,
            scan_end=150.0,
            target_clock_seconds=120,
            latest_trusted_clock_seconds=90,
            previous_media_end_stream_time=100.0,
            target_window={"start_stream_time": 120.0, "end_stream_time": 150.0},
        )
        self.assertEqual(stalled["coverage_class"], "media_stalled")
        self.assertTrue(stalled["media_stalled"])

    def test_coverage_classifies_clock_unreadable_and_video_gap(self):
        """Covered media with no readable ROI differs from a physical gap."""
        unreadable = _ocr_progressive_coverage_diagnostics(
            [Segment(Path("segment.ts"), 80.0, 230.0)],
            intended_initial_start=80.0,
            requested_start=80.0,
            requested_end=230.0,
            scan_start=100.0,
            scan_end=130.0,
            target_clock_seconds=120,
            latest_trusted_clock_seconds=None,
        )
        self.assertEqual(unreadable["coverage_class"], "clock_unreadable")

        gap = _ocr_progressive_coverage_diagnostics(
            [
                Segment(Path("segment-a.ts"), 80.0, 100.0),
                Segment(Path("segment-b.ts"), 102.0, 230.0),
            ],
            intended_initial_start=80.0,
            requested_start=80.0,
            requested_end=230.0,
            scan_start=90.0,
            scan_end=110.0,
            target_clock_seconds=120,
            latest_trusted_clock_seconds=100,
        )
        self.assertEqual(gap["coverage_class"], "video_gap")
        self.assertEqual(gap["video_gaps"][0]["duration_seconds"], 2.0)


if __name__ == "__main__":
    unittest.main()
