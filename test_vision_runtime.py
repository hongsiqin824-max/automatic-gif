from __future__ import annotations

import json
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from live_goal_pipeline import CoverageStatus, PendingEvent, Segment, prune_buffer
from pipeline_runtime import PipelineRuntime
from scoreboard_ocr import ScoreboardOcrError
from vision_locator import VisionCandidateNotFound
from vision_runtime import (
    VisionJob,
    VisualLocationFailed,
    locate_with_ocr_fallback,
    materialize_analysis_clip,
    refine_event_job,
)


class VisionRuntimeTests(unittest.TestCase):
    def test_goal_score_transition_constrains_tdeed_instead_of_claiming_precision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            profile = {
                "profile_id": "feed-a",
                "reference_resolution": [1920, 1080],
                "clock_roi": [20, 20, 180, 70],
                "score_roi": [190, 20, 330, 70],
            }
            job = VisionJob(
                "match:G:ocr", "match", "G", "goal", 160.0, None, 1000.0,
                observed_anchor_stream_time=220.0,
                event_minute="34",
                target_score="1-0",
                scoreboard_profile=profile,
            )
            materialized = {
                "window_start_stream_time": 100.0,
                "window_end_stream_time": 220.0,
            }
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 147.0,
                        "method": "paddleocr_score_transition",
                        "diagnostics": {"worker_wall_seconds": 2.5},
                    },
                ) as ocr,
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={"anchor_stream_time": 139.0, "label": "Goal"},
                ) as tdeed,
            ):
                located = locate_with_ocr_fallback(
                    job, candidate, materialized,
                    tdeed_python=Path("tdeed-python"),
                    ocr_python=ocr_python,
                )

            self.assertEqual(located["anchor_stream_time"], 139.0)
            self.assertEqual(located["locator_method"], "paddleocr_score_then_tdeed")
            self.assertTrue(located["fallback_used"])
            self.assertEqual(ocr.call_args.kwargs["scoreboard_profile"], profile)
            self.assertEqual(tdeed.call_args.kwargs["expected_offset_seconds"], 47.0)
            self.assertEqual(tdeed.call_args.kwargs["max_anchor_distance_seconds"], 30.0)

    def test_goal_score_transition_and_tdeed_failure_uses_120_second_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:G:score-fallback", "match", "G", "goal",
                180.0, None, 1000.0,
                observed_anchor_stream_time=220.0,
                event_minute="34",
                target_score="1-0",
            )
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 147.0,
                        "method": "paddleocr_score_transition",
                        "diagnostics": {},
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no goal action"),
                ),
            ):
                located = locate_with_ocr_fallback(
                    job,
                    candidate,
                    {
                        "window_start_stream_time": 100.0,
                        "window_end_stream_time": 220.0,
                    },
                    tdeed_python=Path("tdeed-python"),
                    ocr_python=ocr_python,
                )

            self.assertTrue(located["minute_fallback"])
            self.assertFalse(located["precise_location"])
            self.assertEqual(located["anchor_stream_time"], 147.0)
            self.assertEqual(
                located["locator_method"],
                "paddleocr_score_transition_fallback",
            )
            self.assertEqual(located["clip_before_seconds"], 60.0)
            self.assertEqual(located["clip_after_seconds"], 60.0)
            self.assertEqual(
                located["failure_reason"]["stage"], "event_second_localization"
            )

    def test_profile_mismatch_skips_ocr_result_and_uses_tdeed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:G:profile", "match", "G", "goal", 160.0, None, 1000.0,
                event_minute="34", target_score="1-0",
                scoreboard_profile={"profile_id": "wrong"},
            )
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=ScoreboardOcrError(
                        "clock_profile_mismatch", "frame aspect ratio mismatch"
                    ),
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 139.0,
                        "confidence": 0.8,
                        "label": "Goal",
                    },
                ) as tdeed,
            ):
                located = locate_with_ocr_fallback(
                    job,
                    candidate,
                    {
                        "window_start_stream_time": 100.0,
                        "window_end_stream_time": 220.0,
                    },
                    tdeed_python=Path("tdeed-python"),
                    ocr_python=ocr_python,
                )

            self.assertEqual(located["anchor_stream_time"], 139.0)
            self.assertEqual(located["locator_method"], "tdeed_fallback")
            self.assertEqual(
                located["ocr_error"]["kind"], "clock_profile_mismatch"
            )
            self.assertFalse(located.get("minute_fallback", False))
            tdeed.assert_called_once()

    def test_required_profile_missing_skips_ocr_and_uses_tdeed(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            job = VisionJob(
                "match:G:no-profile", "match", "G", "goal",
                160.0, None, 1000.0,
                event_minute="34", target_score="1-0",
                require_scoreboard_profile=True,
            )
            with (
                patch("vision_runtime.locate_scoreboard_event") as ocr,
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 155.0,
                        "confidence": 0.75,
                        "label": "Goal",
                    },
                ) as tdeed,
            ):
                located = locate_with_ocr_fallback(
                    job,
                    candidate,
                    {
                        "window_start_stream_time": 100.0,
                        "window_end_stream_time": 220.0,
                    },
                    tdeed_python=Path("tdeed-python"),
                )

            self.assertEqual(located["anchor_stream_time"], 155.0)
            self.assertIn(
                "no scoreboard profile", located["ocr_error"]["message"]
            )
            self.assertFalse(located.get("minute_fallback", False))
            ocr.assert_not_called()
            tdeed.assert_called_once()

    def test_required_profile_missing_and_tdeed_failure_has_no_gif_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            job = VisionJob(
                "match:G:no-locator", "match", "G", "goal",
                160.0, None, 1000.0,
                event_minute="34", target_score="1-0",
                require_scoreboard_profile=True,
            )
            with (
                patch("vision_runtime.locate_scoreboard_event") as ocr,
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no goal"),
                ) as tdeed,
            ):
                with self.assertRaises(VisualLocationFailed) as raised:
                    locate_with_ocr_fallback(
                        job,
                        candidate,
                        {
                            "window_start_stream_time": 100.0,
                            "window_end_stream_time": 220.0,
                        },
                        tdeed_python=Path("tdeed-python"),
                    )

            self.assertEqual(raised.exception.kind, "tdeed_no_candidate")
            self.assertEqual(
                raised.exception.diagnostics["ocr_error"]["kind"],
                "clock_profile_mismatch",
            )
            self.assertFalse(raised.exception.diagnostics["minute_fallback"])
            self.assertFalse(raised.exception.diagnostics["fallback_generated"])
            ocr.assert_not_called()
            tdeed.assert_called_once()

    def test_v1_rejects_extra_time_without_running_ocr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            job = VisionJob(
                "match:G:extra", "match", "G", "goal", 160.0, None, 1000.0,
                event_minute="105", target_score="1-0",
            )
            with patch("vision_runtime.locate_scoreboard_event") as ocr:
                with self.assertRaises(VisualLocationFailed) as raised:
                    locate_with_ocr_fallback(
                        job,
                        candidate,
                        {
                            "window_start_stream_time": 100.0,
                            "window_end_stream_time": 220.0,
                        },
                        tdeed_python=Path("tdeed-python"),
                    )

            self.assertEqual(
                raised.exception.kind,
                "unsupported_extra_time_or_penalties_v1",
            )
            ocr.assert_not_called()

    def test_card_ocr_interval_constrains_tdeed_candidate_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:ocr", "match", "YC", "yellow_card", 160.0, None,
                1000.0, observed_anchor_stream_time=220.0, event_minute="34",
            )
            materialized = {
                "window_start_stream_time": 100.0,
                "window_end_stream_time": 220.0,
            }
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": None,
                        "candidate_interval_start_seconds": 150.0,
                        "candidate_interval_end_seconds": 180.0,
                        "method": "paddleocr_clock_interval",
                        "requires_tdeed": True,
                        "diagnostics": {},
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={"anchor_stream_time": 164.0, "label": "Yellow card"},
                ) as tdeed,
            ):
                located = locate_with_ocr_fallback(
                    job, candidate, materialized,
                    tdeed_python=Path("tdeed-python"),
                    ocr_python=ocr_python,
                )

            kwargs = tdeed.call_args.kwargs
            self.assertEqual(kwargs["expected_offset_seconds"], 65.0)
            self.assertEqual(kwargs["max_anchor_distance_seconds"], 15.0)
            self.assertEqual(located["locator_method"], "paddleocr_clock_then_tdeed")

    def test_ocr_and_tdeed_failure_keeps_structured_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:G:fail", "match", "G", "goal", 160.0, None, 1000.0,
                observed_anchor_stream_time=220.0, target_score="1-0",
            )
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=ScoreboardOcrError(
                        "ocr_no_score_transition", "no transition"
                    ),
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no goal"),
                ),
            ):
                with self.assertRaises(VisualLocationFailed) as raised:
                    locate_with_ocr_fallback(
                        job,
                        candidate,
                        {
                            "window_start_stream_time": 100.0,
                            "window_end_stream_time": 220.0,
                        },
                        tdeed_python=Path("tdeed-python"),
                        ocr_python=ocr_python,
                    )

            self.assertEqual(raised.exception.kind, "tdeed_no_candidate")
            self.assertEqual(
                raised.exception.diagnostics["ocr_error"]["kind"],
                "ocr_no_score_transition",
            )
            self.assertEqual(
                raised.exception.diagnostics["tdeed_error_kind"],
                "tdeed_no_candidate",
            )
            self.assertFalse(raised.exception.diagnostics["fallback_generated"])
            self.assertIn("OCR failed", str(raised.exception))

    def test_tdeed_failure_uses_ocr_minute_interval_as_two_minute_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:G:minute", "match", "G", "goal", 160.0, None,
                1000.0, observed_anchor_stream_time=220.0,
                event_minute="35", target_score="3-0",
            )
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": None,
                        "candidate_interval_start_seconds": 150.0,
                        "candidate_interval_end_seconds": 180.0,
                        "method": "paddleocr_goal_clock_interval",
                        "requires_tdeed": True,
                        "diagnostics": {"worker_wall_seconds": 4.2},
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no goal"),
                ),
            ):
                located = locate_with_ocr_fallback(
                    job,
                    candidate,
                    {
                        "window_start_stream_time": 100.0,
                        "window_end_stream_time": 220.0,
                    },
                    tdeed_python=Path("tdeed-python"),
                    ocr_python=ocr_python,
                )

            self.assertEqual(located["anchor_stream_time"], 165.0)
            self.assertEqual(
                located["locator_method"],
                "paddleocr_clock_interval_fallback",
            )
            self.assertTrue(located["minute_fallback"])
            self.assertEqual(located["tdeed_error_kind"], "tdeed_no_candidate")
            self.assertEqual(located["clip_before_seconds"], 60.0)
            self.assertEqual(located["clip_after_seconds"], 60.0)
            self.assertEqual(located["output_kind"], "minute_range_fallback")
            self.assertFalse(located["precise_location"])
            self.assertEqual(
                located["failure_reason"]["kind"], "tdeed_no_candidate"
            )

    def test_minute_fallback_encoding_overrides_normal_refined_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:minute-encode"
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key, "code": "G", "event_type": "goal",
                    "minute": "35", "minute_extra": "0", "team": "teamA",
                    "person": "A", "person_id": "1", "score": "3-0",
                    "reason": "", "metadata": {},
                },
                observed_stream_time=180.0,
                observed_source_time=None,
                clip_anchor_stream_time=120.0,
                clip_anchor_source_time=None,
                output_due_stream_time=140.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                search_start_stream_time=60.0,
                search_end_stream_time=180.0,
                clip_before_seconds=8.0,
                clip_after_seconds=12.0,
            )
            job = VisionJob(
                event_key, "match", "G", "goal", 120.0, None, 1000.0,
                observed_anchor_stream_time=180.0,
                event_minute="35", target_score="3-0",
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            output = root / "minute-fallback.gif"

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 60.0,
                        "window_end_stream_time": 180.0,
                        "selected_segment_count": 1,
                        "bytes": 100,
                    },
                ),
                patch(
                    "vision_runtime.locate_with_ocr_fallback",
                    return_value={
                        "anchor_stream_time": 120.0,
                        "confidence": None,
                        "label": "scoreboard clock minute fallback",
                        "model_name": "PaddleOCR",
                        "model_version": "scoreboard-clock-v1",
                        "locator_method": "paddleocr_clock_interval_fallback",
                        "fallback_used": True,
                        "minute_fallback": True,
                        "tdeed_error_kind": "tdeed_no_candidate",
                        "tdeed_error": "no goal",
                        "clip_before_seconds": 60.0,
                        "clip_after_seconds": 60.0,
                        "output_kind": "minute_range_fallback",
                        "precise_location": False,
                        "error_kind": "tdeed_no_candidate",
                        "failure_reason": {
                            "kind": "tdeed_no_candidate",
                            "stage": "event_second_localization",
                            "message": "no goal",
                        },
                    },
                ),
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(output), "bytes": 1234},
                ) as encode_mock,
            ):
                self.assertTrue(refine_event_job(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 0.0, 200.0)],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    search_before=120.0,
                    search_after=0.0,
                    refined_before=8.0,
                    refined_after=12.0,
                    width=768,
                    fps=16.0,
                    colors=256,
                    size_reference_bytes=10_000_000,
                    python=Path("python"),
                    timeout_seconds=3.0,
                ))

            self.assertEqual(encode_mock.call_args.kwargs["before"], 60.0)
            self.assertEqual(encode_mock.call_args.kwargs["after"], 60.0)
            self.assertEqual(encode_mock.call_args.kwargs["width"], 384)
            self.assertEqual(encode_mock.call_args.kwargs["fps"], 6.0)
            self.assertEqual(encode_mock.call_args.kwargs["colors"], 160)
            self.assertIn(
                "_fallback_", encode_mock.call_args.kwargs["output_filename"]
            )
            task = runtime.store.get_vision_task(event_key)
            self.assertEqual(task.status, "encoded")
            self.assertTrue(task.result["minute_fallback"])
            self.assertTrue(task.result["fallback_generated"])
            self.assertFalse(task.result["precise_location"])
            self.assertTrue(task.result["default_gif_preserved"])
            self.assertEqual(task.result["output_width"], 384)
            self.assertEqual(task.result["clip_before_seconds"], 60.0)
            self.assertEqual(task.result["clip_after_seconds"], 60.0)
            self.assertEqual(runtime.store.get(event_key).status, "pending")
            runtime.close()

    def test_ocr_and_tdeed_failure_marks_only_vision_task_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:no-visual-location"
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key, "code": "G", "event_type": "goal",
                    "minute": "35", "minute_extra": "0", "team": "teamA",
                    "person": "A", "person_id": "1", "score": "3-0",
                    "reason": "", "metadata": {},
                },
                observed_stream_time=180.0,
                observed_source_time=None,
                clip_anchor_stream_time=120.0,
                clip_anchor_source_time=None,
                output_due_stream_time=140.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                search_start_stream_time=60.0,
                search_end_stream_time=180.0,
                clip_before_seconds=8.0,
                clip_after_seconds=12.0,
            )
            job = VisionJob(
                event_key, "match", "G", "goal", 120.0, None, 1000.0,
                observed_anchor_stream_time=180.0,
                event_minute="35", target_score="3-0",
                require_scoreboard_profile=True,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 60.0,
                        "window_end_stream_time": 180.0,
                        "selected_segment_count": 1,
                        "bytes": 100,
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no goal"),
                ),
                patch("vision_runtime.encode_gif") as encode_mock,
            ):
                self.assertTrue(refine_event_job(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 0.0, 200.0)],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    search_before=120.0,
                    search_after=0.0,
                    refined_before=8.0,
                    refined_after=12.0,
                    width=768,
                    fps=16.0,
                    colors=256,
                    size_reference_bytes=10_000_000,
                    python=Path("python"),
                    timeout_seconds=3.0,
                ))

            encode_mock.assert_not_called()
            self.assertEqual(runtime.store.get(event_key).status, "pending")
            task = runtime.store.get_vision_task(event_key)
            self.assertEqual(task.status, "failed")
            self.assertIsNone(task.output_path)
            self.assertFalse(task.result["minute_fallback"])
            self.assertFalse(task.result["fallback_generated"])
            self.assertEqual(task.result["output_kind"], "failed")
            self.assertEqual(
                task.result["ocr_error"]["kind"], "clock_profile_mismatch"
            )
            self.assertEqual(task.result["tdeed_error_kind"], "tdeed_no_candidate")
            self.assertEqual(
                task.result["failure_reason"]["stage"], "event_localization"
            )
            runtime.close()

    def test_materialize_analysis_clip_validates_and_cleans_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            output = root / "analysis.mp4"

            def fake_run(command, **kwargs):
                Path(command[-1]).write_bytes(b"analysis")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("vision_runtime.subprocess.run", side_effect=fake_run):
                result = materialize_analysis_clip(
                    "ffmpeg", [Segment(segment_path, 0.0, 10.0)], output,
                    window_start=2.0, window_end=6.0, anchor=4.0,
                )

            self.assertEqual(result["bytes"], len(b"analysis"))
            self.assertTrue(output.is_file())
            self.assertEqual(list(root.glob("vision_segments_*.txt")), [])

    def test_materialize_analysis_clip_rejects_empty_output_and_removes_stale_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            output = root / "analysis.mp4"
            output.write_bytes(b"stale")

            def fake_run(command, **kwargs):
                Path(command[-1]).touch()
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("vision_runtime.subprocess.run", side_effect=fake_run):
                with self.assertRaisesRegex(RuntimeError, "empty analysis video"):
                    materialize_analysis_clip(
                        "ffmpeg", [Segment(segment_path, 0.0, 10.0)], output,
                        window_start=2.0, window_end=6.0, anchor=4.0,
                    )

            self.assertFalse(output.exists())
            self.assertEqual(list(root.glob("vision_segments_*.txt")), [])

    def _create_vision_job(
        self,
        root: Path,
        suffix: str,
        *,
        deadline_at_unix: float | None = None,
    ) -> tuple[PipelineRuntime, VisionJob]:
        runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
        event_key = f"match:G:{suffix}"
        runtime.discover_task(
            match_id="match",
            event_data={
                "event_key": event_key, "code": "G", "event_type": "goal",
                "minute": "18", "minute_extra": "0", "team": "teamA",
                "person": "A", "person_id": "1", "score": "1-0",
                "reason": "", "metadata": {},
            },
            observed_stream_time=60.0,
            observed_source_time=1060.0,
            clip_anchor_stream_time=60.0,
            clip_anchor_source_time=1060.0,
            output_due_stream_time=87.0,
            detected_at_unix=1000.0,
        )
        runtime.enqueue_vision_task(
            event_key,
            search_start_stream_time=40.0,
            search_end_stream_time=80.0,
            clip_before_seconds=8.0,
            clip_after_seconds=12.0,
            deadline_at_unix=deadline_at_unix,
        )
        return runtime, VisionJob(
            event_key, "match", "G", "goal", 60.0, 1060.0, 1000.0
        )

    def test_visual_success_encodes_refined_gif_without_changing_default_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:success"
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key, "code": "G", "event_type": "goal",
                    "minute": "18", "minute_extra": "0", "team": "teamA",
                    "person": "A", "person_id": "1", "score": "1-0",
                    "reason": "", "metadata": {},
                },
                observed_stream_time=60.0, observed_source_time=1060.0,
                clip_anchor_stream_time=60.0, clip_anchor_source_time=1060.0,
                output_due_stream_time=87.0, detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key, search_start_stream_time=0.0,
                search_end_stream_time=120.0, clip_before_seconds=8.0,
                clip_after_seconds=12.0,
            )
            job = VisionJob(event_key, "match", "G", "goal", 60.0, 1060.0, 1000.0)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            refined_path = root / "goal_refined.gif"

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 55.0,
                        "window_end_stream_time": 80.0,
                        "selected_segment_count": 1,
                        "bytes": 100,
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 62.0,
                        "confidence": 0.98,
                        "label": "Goal",
                        "model_version": "SoccerNet_small",
                        "checkpoint_sha256": "weight-hash",
                    },
                ) as locate_mock,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(refined_path),
                        "bytes": 1234,
                        "duration_sec": 20.0,
                        "encode_seconds": 0.2,
                    },
                ),
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: [Segment(segment_path, 0.0, 120.0)],
                    "ffmpeg", "ffprobe", root,
                    search_before=60.0, search_after=60.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=1.0,
                ))

            locate_kwargs = locate_mock.call_args.kwargs
            self.assertEqual(locate_kwargs["expected_offset_seconds"], 5.0)
            self.assertEqual(locate_kwargs["max_anchor_distance_seconds"], 20.0)
            self.assertEqual(locate_kwargs["candidate_window_start_seconds"], 55.0)
            self.assertEqual(locate_kwargs["candidate_window_end_seconds"], 80.0)

            default_task = runtime.store.get(event_key)
            vision_task = runtime.store.get_vision_task(event_key)
            self.assertEqual(default_task.status, "pending")
            self.assertIsNone(default_task.output_path)
            self.assertEqual(vision_task.status, "encoded")
            self.assertEqual(vision_task.output_path, str(refined_path))
            self.assertEqual(vision_task.located_anchor_stream_time, 62.0)
            self.assertEqual(vision_task.confidence, 0.98)
            self.assertEqual(vision_task.result["anchor_delta_seconds"], 2.0)
            self.assertEqual(runtime.store.list_segment_leases(event_key=event_key), [])

            records = [
                json.loads(line)
                for line in (root / "events.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            self.assertIn("refined_gif_ready", {record["event"] for record in records})
            runtime.close()

    def test_visual_gif_filename_uses_latest_event_data_and_ai_variant(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(root, "abcdef123456")
            self.assertTrue(runtime.update_task_event({
                "event_key": job.event_key,
                "minute": "19",
                "minute_extra": "2",
                "person": "Latest Player",
                "person_id": "99",
                "score": "2-0",
            }))
            runtime.transition_vision_task(job.event_key, "locating")
            runtime.transition_vision_task(
                job.event_key,
                "located",
                result={"anchor_stream_time": 60.0, "anchor_source_time": 1060.0},
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            expected_filename = (
                "match_m019+02_goal_Latest-Player_2-0_ai_abcdef.gif"
            )

            with patch(
                "vision_runtime.encode_gif",
                return_value={
                    "output": str(root / expected_filename),
                    "bytes": 1234,
                },
            ) as encode_mock:
                self.assertTrue(refine_event_job(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 52.0, 72.0)],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    search_before=20.0,
                    search_after=20.0,
                    refined_before=8.0,
                    refined_after=12.0,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"),
                    timeout_seconds=3.0,
                ))

            self.assertEqual(
                encode_mock.call_args.kwargs["output_filename"],
                expected_filename,
            )
            self.assertTrue(expected_filename.endswith("_ai_abcdef.gif"))
            self.assertNotIn("_A_", expected_filename)
            self.assertEqual(
                runtime.store.get_vision_task(job.event_key).output_path,
                str(root / expected_filename),
            )
            runtime.close()

    def test_prune_buffer_preserves_active_lease_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            protected = root / "protected.ts"
            disposable = root / "disposable.ts"
            protected.write_bytes(b"protected")
            disposable.write_bytes(b"old")
            segments = [
                Segment(protected, 0.0, 2.0),
                Segment(disposable, 2.0, 4.0),
            ]

            prune_buffer(
                segments,
                stream_time=100.0,
                buffer_seconds=30.0,
                events=[],
                before=8.0,
                protected_paths={str(protected.resolve())},
            )

            self.assertTrue(protected.exists())
            self.assertFalse(disposable.exists())

    def test_pending_default_event_still_extends_buffer_retention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "pending.ts"
            segment.write_bytes(b"pending")
            event = PendingEvent(
                event_type="goal",
                stream_time=25.0,
                source_time=None,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=50.0,
            )

            prune_buffer(
                [Segment(segment, 0.0, 2.0)],
                stream_time=100.0,
                buffer_seconds=30.0,
                events=[event],
                before=30.0,
            )

            self.assertTrue(segment.exists())

    def test_pending_vision_search_start_extends_buffer_retention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "vision-search.ts"
            segment.write_bytes(b"vision")

            prune_buffer(
                [Segment(segment, 20.0, 22.0)],
                stream_time=200.0,
                buffer_seconds=120.0,
                events=[],
                before=30.0,
                extra_cutoffs=[15.0],
            )

            self.assertTrue(segment.exists())

    def test_visual_failure_does_not_change_default_gif_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:one"
            event_data = {
                "event_key": event_key, "code": "G", "event_type": "goal",
                "minute": "18", "minute_extra": "0", "team": "teamA",
                "person": "A", "person_id": "1", "score": "1-0",
                "reason": "", "metadata": {},
            }
            runtime.discover_task(
                match_id="match", event_data=event_data,
                observed_stream_time=60.0, observed_source_time=None,
                clip_anchor_stream_time=60.0, clip_anchor_source_time=None,
                output_due_stream_time=87.0, detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key, search_start_stream_time=0.0,
                search_end_stream_time=120.0, clip_before_seconds=8.0,
                clip_after_seconds=12.0,
            )
            job = VisionJob(event_key, "match", "G", "goal", 60.0, None, 1000.0)

            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            with patch(
                "vision_runtime.materialize_analysis_clip",
                side_effect=RuntimeError("model input failed"),
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: [Segment(segment_path, 0.0, 120.0)],
                    "ffmpeg", "ffprobe", root,
                    search_before=60.0, search_after=60.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=1.0,
                ))

            self.assertEqual(runtime.store.get(event_key).status, "pending")
            self.assertEqual(runtime.store.get_vision_task(event_key).status, "failed")
            runtime.close()

    def test_visual_tail_wait_uses_readiness_counter_not_model_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:waiting"
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key, "code": "G", "event_type": "goal",
                    "minute": "18", "minute_extra": "0", "team": "teamA",
                    "person": "A", "person_id": "1", "score": "1-0",
                    "reason": "", "metadata": {},
                },
                observed_stream_time=60.0,
                observed_source_time=None,
                clip_anchor_stream_time=60.0,
                clip_anchor_source_time=None,
                output_due_stream_time=87.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                search_start_stream_time=40.0,
                search_end_stream_time=80.0,
                clip_before_seconds=8.0,
                clip_after_seconds=12.0,
            )
            segment_path = root / "partial.ts"
            segment_path.write_bytes(b"partial")
            job = VisionJob(event_key, "match", "G", "goal", 60.0, None, 1000.0)

            self.assertFalse(refine_event_job(
                job,
                runtime,
                lambda: [Segment(segment_path, 40.0, 70.0)],
                "ffmpeg",
                "ffprobe",
                root,
                search_before=20.0,
                search_after=20.0,
                refined_before=8.0,
                refined_after=12.0,
                width=384,
                fps=6.0,
                colors=160,
                size_reference_bytes=10_000_000,
                python=Path("python"),
                timeout_seconds=1.0,
            ))

            task = runtime.store.get_vision_task(event_key)
            self.assertEqual(task.status, "pending")
            self.assertEqual(task.locate_attempt_count, 0)
            self.assertEqual(task.encode_attempt_count, 0)
            self.assertEqual(task.readiness_check_count, 1)
            self.assertEqual(task.last_error_kind, "waiting_for_tail")
            self.assertEqual(runtime.store.list_segment_leases(event_key=event_key), [])
            runtime.close()

    def test_visual_search_and_refined_gif_use_anchor_side_of_known_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(root, "known-gaps")
            paths = [root / f"segment-{index}.ts" for index in range(4)]
            for path in paths:
                path.write_bytes(b"video")
            search_segments = [
                Segment(paths[0], 40.0, 50.0),
                Segment(paths[1], 52.0, 80.0),
            ]
            refined_segments = [
                Segment(paths[2], 54.0, 66.0),
                Segment(paths[3], 68.0, 80.0),
            ]
            batches = iter([search_segments, refined_segments])
            output = root / "known-gaps.gif"

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 52.0,
                        "window_end_stream_time": 80.0,
                        "selected_segment_count": 1,
                        "bytes": 100,
                        "coverage_status": CoverageStatus.READY_DEGRADED.value,
                    },
                ) as materialize_mock,
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 62.0,
                        "confidence": 0.9,
                        "label": "Goal",
                        "model_version": "SoccerNet_small",
                        "checkpoint_sha256": "weight-hash",
                    },
                ) as locate_mock,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(output), "bytes": 1234},
                ) as encode_mock,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: next(batches),
                    "ffmpeg", "ffprobe", root,
                    search_before=20.0, search_after=20.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            search_coverage = materialize_mock.call_args.kwargs["coverage"]
            self.assertEqual(search_coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(search_coverage.effective_start, 52.0)
            self.assertEqual(search_coverage.effective_end, 80.0)
            locate_kwargs = locate_mock.call_args.kwargs
            self.assertEqual(locate_kwargs["expected_offset_seconds"], 8.0)
            self.assertEqual(locate_kwargs["candidate_window_start_seconds"], 52.0)
            self.assertEqual(locate_kwargs["candidate_window_end_seconds"], 80.0)
            refined_coverage = encode_mock.call_args.kwargs["coverage"]
            self.assertEqual(refined_coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(refined_coverage.effective_start, 54.0)
            self.assertEqual(refined_coverage.effective_end, 66.0)
            self.assertEqual(runtime.store.get_vision_task(job.event_key).status, "encoded")
            self.assertEqual(runtime.store.list_segment_leases(event_key=job.event_key), [])
            runtime.close()

    def test_visual_known_gap_does_not_wait_for_unrelated_live_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(root, "known-gap-tail")
            first = root / "first.ts"
            second = root / "second.ts"
            final = root / "final.ts"
            for path in (first, second, final):
                path.write_bytes(b"video")
            batches = iter([
                [Segment(first, 40.0, 66.0), Segment(second, 68.0, 75.0)],
                [Segment(final, 54.0, 74.0)],
            ])
            output = root / "known-gap-tail.gif"

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 40.0,
                        "window_end_stream_time": 66.0,
                        "selected_segment_count": 1,
                        "bytes": 100,
                        "coverage_status": CoverageStatus.READY_DEGRADED.value,
                    },
                ) as materialize_mock,
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 62.0,
                        "confidence": 0.9,
                        "label": "Goal",
                        "model_version": "SoccerNet_small",
                        "checkpoint_sha256": "weight-hash",
                    },
                ) as locate_mock,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(output), "bytes": 1234},
                ) as encode_mock,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: next(batches),
                    "ffmpeg", "ffprobe", root,
                    search_before=20.0, search_after=20.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            coverage = materialize_mock.call_args.kwargs["coverage"]
            self.assertEqual(coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(coverage.error_kind, "degraded_window")
            self.assertEqual(coverage.effective_start, 40.0)
            self.assertEqual(coverage.effective_end, 66.0)
            self.assertEqual(locate_mock.call_args.kwargs["expected_offset_seconds"], 20.0)
            self.assertEqual(
                encode_mock.call_args.kwargs["coverage"].status,
                CoverageStatus.READY_FULL,
            )
            runtime.close()

    def test_visual_deadline_uses_existing_anchor_component_for_both_stages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(
                root, "deadline-degraded", deadline_at_unix=time.time() - 1.0
            )
            segment_path = root / "partial.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 40.0, 70.0)]
            output = root / "deadline-degraded.gif"

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 40.0,
                        "window_end_stream_time": 70.0,
                        "selected_segment_count": 1,
                        "bytes": 100,
                        "coverage_status": CoverageStatus.READY_DEGRADED.value,
                    },
                ) as materialize_mock,
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 60.0,
                        "confidence": 0.8,
                        "label": "Goal",
                        "model_version": "SoccerNet_small",
                        "checkpoint_sha256": "weight-hash",
                    },
                ) as locate_mock,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(output), "bytes": 321},
                ) as encode_mock,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: segments,
                    "ffmpeg", "ffprobe", root,
                    search_before=20.0, search_after=20.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            search_coverage = materialize_mock.call_args.kwargs["coverage"]
            self.assertEqual(search_coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(search_coverage.error_kind, "degraded_deadline")
            locate_kwargs = locate_mock.call_args.kwargs
            self.assertEqual(locate_kwargs["expected_offset_seconds"], 20.0)
            self.assertEqual(locate_kwargs["candidate_window_start_seconds"], 40.0)
            self.assertEqual(locate_kwargs["candidate_window_end_seconds"], 70.0)
            self.assertEqual(locate_kwargs["timeout_seconds"], 3.0)
            refined_coverage = encode_mock.call_args.kwargs["coverage"]
            self.assertEqual(refined_coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(refined_coverage.error_kind, "degraded_deadline")
            self.assertEqual(refined_coverage.effective_start, 52.0)
            self.assertEqual(refined_coverage.effective_end, 70.0)
            self.assertEqual(runtime.store.get_vision_task(job.event_key).status, "encoded")
            runtime.close()

    def test_visual_degraded_component_shorter_than_two_seconds_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(
                root, "too-short", deadline_at_unix=time.time() - 1.0
            )
            runtime.transition_vision_task(job.event_key, "locating")
            runtime.transition_vision_task(
                job.event_key,
                "located",
                result={"anchor_stream_time": 60.0, "anchor_source_time": 1060.0},
            )
            segment_path = root / "short.ts"
            segment_path.write_bytes(b"video")

            with patch("vision_runtime.encode_gif") as encode_mock:
                self.assertTrue(refine_event_job(
                    job, runtime,
                    lambda: [Segment(segment_path, 59.5, 60.5)],
                    "ffmpeg", "ffprobe", root,
                    search_before=20.0, search_after=20.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            encode_mock.assert_not_called()
            task = runtime.store.get_vision_task(job.event_key)
            self.assertEqual(task.status, "failed")
            self.assertEqual(task.last_error_kind, "degraded_clip_too_short")
            self.assertEqual(task.result["error_kind"], "degraded_clip_too_short")
            runtime.close()

    def test_visual_anchor_inside_gap_still_fails_at_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(
                root, "anchor-gap", deadline_at_unix=time.time() - 1.0
            )
            runtime.transition_vision_task(job.event_key, "locating")
            runtime.transition_vision_task(
                job.event_key,
                "located",
                result={"anchor_stream_time": 60.0, "anchor_source_time": 1060.0},
            )
            before = root / "before.ts"
            after = root / "after.ts"
            before.write_bytes(b"video")
            after.write_bytes(b"video")

            with patch("vision_runtime.encode_gif") as encode_mock:
                self.assertTrue(refine_event_job(
                    job, runtime,
                    lambda: [
                        Segment(before, 52.0, 59.0),
                        Segment(after, 61.0, 72.0),
                    ],
                    "ffmpeg", "ffprobe", root,
                    search_before=20.0, search_after=20.0,
                    refined_before=8.0, refined_after=12.0,
                    width=384, fps=6.0, colors=160,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            encode_mock.assert_not_called()
            task = runtime.store.get_vision_task(job.event_key)
            self.assertEqual(task.status, "failed")
            self.assertEqual(task.last_error_kind, "buffer_gap")
            runtime.close()


if __name__ == "__main__":
    unittest.main()
