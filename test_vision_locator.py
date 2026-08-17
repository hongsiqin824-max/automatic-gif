import json
import subprocess
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from vision_locator import (
    EVENT_LABELS,
    VisionBufferNotReady,
    VisionBufferUnavailable,
    VisionCandidateNotFound,
    VisionInferenceError,
    locate,
    locate_candidate_video,
    locate_event,
    materialize_candidate_video,
    run_tdeed_inference,
    select_event_candidate,
)


@dataclass(frozen=True)
class Segment:
    path: Path
    start: float
    end: float


class RecordingRunner:
    def __init__(self, callback=None):
        self.commands = []
        self.callback = callback

    def __call__(self, command, **kwargs):
        self.commands.append((list(command), kwargs))
        if self.callback is not None:
            self.callback(list(command), kwargs)
        return subprocess.CompletedProcess(command, 0, "", "")


class VisionCandidateSelectionTests(unittest.TestCase):
    def test_supported_codes_map_to_expected_model_labels(self):
        self.assertEqual(EVENT_LABELS["G"], ("Goal",))
        self.assertEqual(EVENT_LABELS["OG"], ("Goal",))
        self.assertEqual(EVENT_LABELS["YC"], ("Yellow card",))
        self.assertEqual(EVENT_LABELS["RC"], ("Red card", "Yellow->red card"))

    def test_selects_nearest_candidate_after_label_threshold_and_distance_filters(self):
        selected = select_event_candidate(
            [
                {"label": "Goal", "frame": 250, "confidence": 0.99},
                {"label": "Goal", "frame": 1425, "confidence": 0.31},
                {"label": "Goal", "frame": 1500, "confidence": 0.25},
                {"label": "Yellow card", "frame": 1475, "confidence": 0.99},
            ],
            "G",
            expected_offset_seconds=60.0,
            threshold=0.20,
            max_anchor_distance_seconds=15.0,
        )

        self.assertEqual(selected["anchor_seconds"], 60.0)
        self.assertEqual(selected["confidence"], 0.25)
        self.assertEqual(selected["eligible_candidate_count"], 2)

    def test_red_card_accepts_yellow_to_red_and_reports_actual_label(self):
        selected = select_event_candidate(
            [
                {
                    "label": "Yellow->red card",
                    "offset_seconds": 31.5,
                    "confidence": 0.72,
                }
            ],
            "RC",
            expected_offset_seconds=30.0,
        )
        self.assertEqual(selected["label"], "Yellow->red card")
        self.assertEqual(selected["distance_from_expected_seconds"], 1.5)

    def test_no_candidate_error_contains_filters_and_observed_strength(self):
        with self.assertRaisesRegex(
            VisionCandidateNotFound,
            r"Goal candidate within 15s.*threshold=0.2.*strongest=0.190000",
        ):
            select_event_candidate(
                [{"label": "Goal", "frame": 1500, "confidence": 0.19}],
                "OG",
                expected_offset_seconds=60.0,
            )


class CandidateMaterializationTests(unittest.TestCase):
    def test_materializes_ordered_segment_concat_at_fixed_analysis_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            late = root / "late.ts"
            early = root / "early.ts"
            early.write_bytes(b"early")
            late.write_bytes(b"late")
            output = root / "candidate.mp4"

            def create_output(command, _kwargs):
                Path(command[-1]).write_bytes(b"candidate")

            runner = RecordingRunner(create_output)
            result = materialize_candidate_video(
                [Segment(late, 2.0, 4.0), Segment(early, 0.0, 2.0)],
                output,
                window_start=0.0,
                window_end=4.0,
                runner=runner,
            )

            command, _ = runner.commands[0]
            self.assertIn("fps=25,scale=398:224:flags=lanczos", command)
            self.assertEqual(result.selected_segment_count, 2)
            self.assertEqual(result.selected_segment_paths[0], str(early.resolve()))
            self.assertEqual(result.bytes, len(b"candidate"))
            self.assertFalse(list(root.glob("vision_segments_*.txt")))

    def test_missing_window_history_is_permanently_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "one.ts"
            segment.write_bytes(b"video")
            with self.assertRaisesRegex(VisionBufferUnavailable, "beginning"):
                materialize_candidate_video(
                    [Segment(segment, 2.0, 4.0)],
                    root / "candidate.mp4",
                    window_start=0.0,
                    window_end=4.0,
                )

    def test_incomplete_live_tail_is_retryable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "one.ts"
            segment.write_bytes(b"video")
            with self.assertRaisesRegex(VisionBufferNotReady, "tail"):
                materialize_candidate_video(
                    [Segment(segment, 0.0, 2.0)],
                    root / "candidate.mp4",
                    window_start=0.0,
                    window_end=4.0,
                    anchor=1.0,
                )

    def test_anchor_inside_gap_is_permanently_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            with self.assertRaisesRegex(VisionBufferUnavailable, "anchor"):
                materialize_candidate_video(
                    [Segment(first, 0.0, 2.0), Segment(second, 3.0, 5.0)],
                    root / "candidate.mp4",
                    window_start=0.0,
                    window_end=5.0,
                    anchor=2.5,
                )

    def test_degraded_materialization_uses_only_anchor_component_and_real_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before_gap = root / "before-gap.ts"
            after_gap = root / "after-gap.ts"
            before_gap.write_bytes(b"before")
            after_gap.write_bytes(b"after")

            def create_output(command, _kwargs):
                Path(command[-1]).write_bytes(b"candidate")

            runner = RecordingRunner(create_output)
            result = materialize_candidate_video(
                [
                    Segment(before_gap, 0.0, 4.0),
                    Segment(after_gap, 6.0, 10.0),
                ],
                root / "candidate.mp4",
                window_start=0.0,
                window_end=10.0,
                anchor=3.0,
                runner=runner,
                allow_degraded=True,
            )

            self.assertEqual(result.coverage_status, "ready_degraded")
            self.assertEqual(result.stream_start_seconds, 0.0)
            self.assertEqual(result.stream_end_seconds, 4.0)
            self.assertEqual(result.selected_segment_paths, (str(before_gap.resolve()),))
            self.assertEqual(result.as_dict()["requested_stream_end_seconds"], 10.0)


class TdeedProcessTests(unittest.TestCase):
    def _model_installation(self, root: Path):
        python = root / "venv" / "python"
        tdeed = root / "T-DEED"
        checkpoint = tdeed / "checkpoints" / "checkpoint.pt"
        python.parent.mkdir(parents=True)
        python.write_bytes(b"python")
        (tdeed / "inference.py").parent.mkdir(parents=True)
        (tdeed / "inference.py").write_bytes(b"script")
        checkpoint.parent.mkdir(parents=True)
        checkpoint.write_bytes(b"weights")
        return python, tdeed, checkpoint

    def test_runner_uses_isolated_python_and_official_tdeed_script(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            video = root / "candidate.mp4"
            video.write_bytes(b"video")
            python, tdeed, checkpoint = self._model_installation(root)

            def write_result(command, _kwargs):
                output_dir = Path(command[command.index("--output_dir") + 1])
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / "results_inference.json").write_text(
                    json.dumps(
                        {
                            "predictions": [
                                {"label": "Goal", "frame": 100, "confidence": 0.8}
                            ]
                        }
                    ),
                    encoding="utf-8",
                )

            runner = RecordingRunner(write_result)
            predictions, elapsed = run_tdeed_inference(
                video,
                root / "results",
                python_path=python,
                tdeed_root=tdeed,
                checkpoint=checkpoint,
                runner=runner,
            )

            command, kwargs = runner.commands[0]
            self.assertEqual(command[0], str(python))
            self.assertEqual(command[1], str(tdeed / "inference.py"))
            self.assertEqual(kwargs["cwd"], str(tdeed))
            self.assertIn("--device", command)
            self.assertEqual(command[command.index("--device") + 1], "cpu")
            self.assertEqual(predictions[0]["label"], "Goal")
            self.assertGreaterEqual(elapsed, 0.0)

    def test_nonzero_model_exit_has_actionable_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            video = root / "candidate.mp4"
            video.write_bytes(b"video")
            python, tdeed, checkpoint = self._model_installation(root)

            def failed_runner(command, **kwargs):
                return subprocess.CompletedProcess(command, 9, "", "weights mismatch")

            with self.assertRaisesRegex(
                VisionInferenceError, "status 9: weights mismatch"
            ):
                run_tdeed_inference(
                    video,
                    root / "results",
                    python_path=python,
                    tdeed_root=tdeed,
                    checkpoint=checkpoint,
                    runner=failed_runner,
                )

    def test_high_level_api_returns_global_anchor_metadata_and_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segments = []
            for index in range(5):
                path = root / f"{index}.ts"
                path.write_bytes(b"segment")
                segments.append(Segment(path, index * 2.0, (index + 1) * 2.0))
            candidate = root / "candidate.mp4"
            python, tdeed, checkpoint = self._model_installation(root)

            def produce_outputs(command, _kwargs):
                if command[0] == "ffmpeg":
                    Path(command[-1]).write_bytes(b"candidate")
                    return
                output_dir = Path(command[command.index("--output_dir") + 1])
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / "results_inference.json").write_text(
                    json.dumps(
                        {
                            "predictions": [
                                {"label": "Yellow card", "frame": 100, "confidence": 0.65}
                            ]
                        }
                    ),
                    encoding="utf-8",
                )

            result = locate_event(
                segments,
                "YC",
                5.0,
                search_before_seconds=5.0,
                search_after_seconds=5.0,
                candidate_video=candidate,
                python_path=python,
                tdeed_root=tdeed,
                checkpoint=checkpoint,
                runner=RecordingRunner(produce_outputs),
            )

            self.assertEqual(result["anchor_seconds"], 4.0)
            self.assertEqual(result["anchor_stream_time"], 4.0)
            self.assertEqual(result["model_name"], "T-DEED")
            self.assertEqual(result["model_version"], "SoccerNet_small")
            self.assertEqual(len(result["checkpoint_sha256"]), 64)
            self.assertTrue(result["experimental"])
            self.assertEqual(result["candidate_window"]["stream_start_seconds"], 0.0)
            self.assertEqual(result["candidate_window"]["stream_end_seconds"], 10.0)
            self.assertEqual(result["candidate_window"]["selected_segment_count"], 5)

    def test_short_locate_api_preserves_existing_call_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            video = root / "candidate.mp4"
            video.write_bytes(b"video")
            python, tdeed, checkpoint = self._model_installation(root)

            def write_result(command, _kwargs):
                output_dir = Path(command[command.index("--output_dir") + 1])
                output_dir.mkdir(parents=True, exist_ok=True)
                (output_dir / "results_inference.json").write_text(
                    json.dumps(
                        {
                            "predictions": [
                                {"label": "Goal", "frame": 250, "confidence": 0.8}
                            ]
                        }
                    ),
                    encoding="utf-8",
                )

            result = locate(
                video,
                "G",
                10.0,
                0.2,
                python_path=python,
                tdeed_root=tdeed,
                checkpoint=checkpoint,
                runner=RecordingRunner(write_result),
            )

            self.assertEqual(result["status"], "located")
            self.assertEqual(result["anchor_seconds"], 10.0)


if __name__ == "__main__":
    unittest.main()
