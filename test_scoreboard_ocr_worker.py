from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from scoreboard_ocr import ClockContinuityStateMachine, ScoreboardProfile
from scoreboard_ocr_worker import (
    AutoClockTracker,
    BatchOcrWorker,
    DetectedText,
    WorkerError,
    _SocketOcrRuntime,
    _auto_results_by_frame,
    _auto_readings,
    _confirmed_independent_stoppage_overrides,
    _extract_detected_texts,
    _first_clock_missing_run,
    _merge_auto_results,
    _normalize_clock_recognition_results,
    _prepare_clock_only_recognition,
    _profile_clock_readings,
    _profile_readings,
    _recognize_paths_shared,
    _restartable_backend_generation,
    _validate_profile_content_quality,
    extract_auto_roi_frames,
    extract_independent_stoppage_frames,
    extract_profile_clock_frames,
    extract_profile_frames,
    extract_scoreboard_frames,
    frame_reading,
    locate_from_readings,
    probe_video_dimensions,
    recognize_batch,
    run_request,
    serve_socket,
    split_frame_reading,
)


class FakeEngine:
    def __init__(self) -> None:
        self.calls: list[list[object]] = []

    def predict(self, crops):
        self.calls.append(list(crops))
        return [
            {"rec_texts": [str(crop)], "rec_scores": [0.9]}
            for crop in crops
        ]


class BatchRecognitionTests(unittest.TestCase):
    def test_recognize_batch_calls_backend_once_and_keeps_alignment(self):
        engine = FakeEngine()

        results = recognize_batch(
            engine,
            ["44:59", "0-0", "45:00"],
            minimum_confidences=[0.0, 0.95, 0.0],
        )

        self.assertEqual(engine.calls, [["44:59", "0-0", "45:00"]])
        self.assertEqual(results[0], (["44:59"], [0.9]))
        self.assertEqual(results[1], ([], []))
        self.assertEqual(results[2], (["45:00"], [0.9]))

    def test_rejects_misaligned_backend_results(self):
        class BrokenEngine:
            def predict(self, _crops):
                return [{"rec_texts": ["45:00"], "rec_scores": [0.9]}]

        with self.assertRaises(WorkerError) as raised:
            recognize_batch(
                BrokenEngine(),
                ["clock-a", "clock-b"],
                minimum_confidences=[0.0, 0.0],
            )

        self.assertEqual(raised.exception.kind, "ocr_inference_failed")

    def test_clock_character_normalization_is_limited_to_unreadable_clock_crops(self):
        recognized, repairs = _normalize_clock_recognition_results(
            [(["7O：O1"], [0.9]), (["TEAM"], [0.8])],
            ["clock", "score"],
        )

        self.assertEqual(recognized[0][0], ["70:01"])
        self.assertEqual(recognized[1][0], ["TEAM"])
        self.assertEqual(repairs[0]["frame_index"], 0)
        self.assertEqual(repairs[0]["clock_seconds"], 70 * 60 + 1)

    def test_clock_preprocessing_retries_only_failed_frames_in_original_slots(self):
        with tempfile.TemporaryDirectory() as directory:
            frames = [Path(directory) / f"clock-{index}.png" for index in range(3)]
            for frame in frames:
                frame.write_bytes(b"crop")
            recognized = [
                (["12:00"], [0.9]),
                ([], []),
                (["12:02"], [0.9]),
            ]

            def write_variant(_source, output, _variant):
                output.write_bytes(b"enhanced")

            with (
                patch(
                    "scoreboard_ocr_worker._write_clock_preprocess_variant",
                    side_effect=write_variant,
                ),
                patch(
                    "scoreboard_ocr_worker._recognize_request_paths",
                    return_value=([(["12:01"], [0.95])], []),
                ) as retry,
            ):
                prepared, diagnostics = _prepare_clock_only_recognition(
                    recognized,
                    frames,
                    ["clock"] * 3,
                    engine=object(),
                    batch_worker=None,
                    request={"candidate_path": "candidate.mp4"},
                    profile_id="profile-a",
                    sample_interval=1.0,
                    minimum_confidence=0.35,
                    inference_batch_size=8,
                    deadline_monotonic=None,
                )

        self.assertEqual(
            [item[0] for item in prepared],
            [["12:00"], ["12:01"], ["12:02"]],
        )
        self.assertEqual(retry.call_args.kwargs["source_frame_indices"], [1])
        self.assertEqual(diagnostics["initial_unreadable_frame_count"], 1)
        self.assertEqual(diagnostics["recovered_frame_count"], 1)
        self.assertTrue(diagnostics["frame_identity_preserved"])


class AutoClockDiscoveryTests(unittest.TestCase):
    @staticmethod
    def _clock(text: str, x: float = 10, y: float = 8) -> DetectedText:
        return DetectedText(text, 0.97, (x, y, x + 60, y + 20))

    def test_extracts_v2_and_v3_bboxes_without_losing_alignment(self):
        clock_box = [[10, 8], [70, 8], [70, 28], [10, 28]]
        score_box = [[80, 8], [110, 8], [110, 28], [80, 28]]
        v2 = [[[clock_box, ("12:34", 0.97)], [score_box, ("1-0", 0.94)]]]

        class V3Result:
            json = {
                "res": {
                    "rec_texts": ["12:34", "1-0"],
                    "rec_scores": [0.97, 0.94],
                    "rec_polys": [clock_box, score_box],
                }
            }

        expected = [
            DetectedText("12:34", 0.97, (10.0, 8.0, 70.0, 28.0)),
            DetectedText("1-0", 0.94, (80.0, 8.0, 110.0, 28.0)),
        ]
        self.assertEqual(_extract_detected_texts(v2), expected)
        self.assertEqual(_extract_detected_texts(V3Result()), expected)

    def test_v3_uses_dt_polys_and_skips_text_without_a_bbox(self):
        detected = _extract_detected_texts(
            {
                "rec_texts": ["12:34", "orphan"],
                "rec_scores": [0.91, 0.99],
                "dt_polys": [[[2, 3], [12, 3], [12, 9], [2, 9]]],
            }
        )

        self.assertEqual(
            detected,
            [DetectedText("12:34", 0.91, (2.0, 3.0, 12.0, 9.0))],
        )

    def test_three_stable_frames_lock_padded_clamped_clock_and_score_rois(self):
        tracker = AutoClockTracker(1280, 720)
        decisions = []
        for index, text in enumerate(("12:34", "12:35", "12:36")):
            decisions.append(
                tracker.observe_search(
                    index,
                    index,
                    [
                        self._clock(text, x=2 + index, y=2),
                        DetectedText("1-0", 0.94, (72, 2, 102, 22)),
                    ],
                )
            )

        locked = decisions[-1]
        self.assertEqual(locked.status, "locked")
        self.assertEqual(locked.reason, "stable_clock_track")
        self.assertEqual(locked.clock_roi[0], 0)
        self.assertIsNotNone(locked.score_roi)
        self.assertLessEqual(locked.clock_roi[2], 1280)

    def test_two_stable_clock_tracks_are_ambiguous(self):
        tracker = AutoClockTracker(1280, 720)
        decision = None
        for index in range(3):
            decision = tracker.observe_search(
                index,
                index,
                [
                    self._clock(f"12:{34 + index:02d}", x=10),
                    self._clock(f"42:{10 + index:02d}", x=300),
                ],
            )

        self.assertIsNotNone(decision)
        self.assertEqual(decision.status, "ambiguous")
        self.assertEqual(decision.reason, "ambiguous")
        self.assertEqual(decision.candidate_count, 2)

    def test_static_clock_like_text_cannot_establish_initial_lock(self):
        tracker = AutoClockTracker(1280, 720)

        decisions = [
            tracker.observe_search(index, index, [self._clock("00:10")])
            for index in range(6)
        ]

        self.assertTrue(all(decision.status == "searching" for decision in decisions))

    def test_clock_pause_is_accepted_after_a_progressing_track_locks(self):
        tracker = AutoClockTracker(1280, 720)
        for index, text in enumerate(("12:34", "12:35", "12:36")):
            tracker.observe_search(index, index, [self._clock(text)])

        paused = tracker.observe_locked(3, ["12:36"])

        self.assertEqual(paused.status, "locked")
        self.assertEqual(paused.reason, "accepted")
        self.assertEqual(paused.clock_seconds, 12 * 60 + 36)

    def test_short_miss_keeps_roi_and_third_miss_expands_search(self):
        tracker = AutoClockTracker(1280, 720)
        for index, text in enumerate(("12:34", "12:35", "12:36")):
            tracker.observe_search(index, index, [self._clock(text)])

        first = tracker.observe_locked(3, [])
        second = tracker.observe_locked(4, [])
        third = tracker.observe_locked(5, [])

        self.assertEqual(first.reason, "temporarily_hidden")
        self.assertIsNone(first.clock_seconds)
        self.assertIsNotNone(first.clock_roi)
        self.assertEqual(second.reason, "temporarily_hidden")
        self.assertEqual(third.status, "searching")
        self.assertTrue(third.expanded_search)
        self.assertEqual(third.search_roi, (0.0, 0.0, 1280.0, 180.0))

    def test_expanded_search_can_relock_at_the_right(self):
        tracker = AutoClockTracker(1280, 720)
        for index, text in enumerate(("12:34", "12:35", "12:36")):
            tracker.observe_search(index, index, [self._clock(text)])
        for second in (3, 4, 5):
            tracker.observe_locked(second, [])

        decision = None
        for index, text in enumerate(("12:39", "12:40", "12:41"), start=6):
            decision = tracker.observe_search(
                index, index, [self._clock(text, x=900, y=10)]
            )

        self.assertEqual(decision.status, "locked")
        self.assertTrue(decision.expanded_search)
        self.assertGreater(decision.clock_roi[0], 800)

    def test_discontinuous_locked_value_does_not_replace_last_good_clock(self):
        tracker = AutoClockTracker(1280, 720)
        for index, text in enumerate(("12:34", "12:35", "12:36")):
            tracker.observe_search(index, index, [self._clock(text)])

        bad = tracker.observe_locked(3, ["40:00"])
        recovered = tracker.observe_locked(4, ["12:38"])

        self.assertEqual(bad.reason, "timeline_discontinuous")
        self.assertIsNone(bad.clock_seconds)
        self.assertEqual(recovered.reason, "accepted")
        self.assertEqual(recovered.clock_seconds, 12 * 60 + 38)

    def test_score_failure_does_not_block_clock_lock_or_clock_reading(self):
        tracker = AutoClockTracker(1280, 720)
        for index, text in enumerate(("12:34", "12:35", "12:36")):
            decision = tracker.observe_search(index, index, [self._clock(text)])
        self.assertEqual(decision.status, "locked")
        self.assertGreaterEqual(len(decision.score_rois), 1)
        self.assertEqual(decision.score_roi, decision.score_rois[0])

        readings, _diagnostics = _auto_readings(
            [(["12:34"], [0.9]), ([], []), (["12:36"], [0.9])],
            kinds=["clock", "clock", "clock"],
            sample_interval=1,
            period=1,
        )
        self.assertIsNone(readings[1].clock_seconds)
        self.assertEqual(readings[1].continuity_reason, "scoreboard_temporarily_missing")
        self.assertEqual(readings[2].clock_seconds, 12 * 60 + 36)

    def test_multiple_score_rois_merge_split_score_digits_per_frame(self):
        frames = _auto_results_by_frame(
            [
                (["12:34"], [0.9]), (["4"], [0.8]), (["0"], [0.85]),
                (["12:35"], [0.9]), (["4"], [0.8]), (["0"], [0.85]),
            ],
            ["clock", "score", "score", "clock", "score", "score"],
        )
        self.assertEqual(frames[0]["score"], (["4", "0"], [0.8, 0.85]))
        readings, _diagnostics = _auto_readings(
            [
                (["12:34"], [0.9]), (["4"], [0.8]), (["0"], [0.85]),
                (["12:35"], [0.9]), (["4"], [0.8]), (["0"], [0.85]),
            ],
            kinds=["clock", "score", "score", "clock", "score", "score"],
            sample_interval=1.0,
            period=1,
        )
        self.assertEqual([reading.score for reading in readings], [(4, 0), (4, 0)])

    def test_extract_auto_roi_frames_emits_all_score_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            candidate = output_dir / "candidate.mp4"
            candidate.write_bytes(b"video")

            def runner(command, **_kwargs):
                (output_dir / "multi_clock_000001.png").write_bytes(b"clock")
                (output_dir / "multi_score_0_000001.png").write_bytes(b"home")
                (output_dir / "multi_score_1_000001.png").write_bytes(b"away")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                paths, kinds, diagnostics = extract_auto_roi_frames(
                    candidate,
                    output_dir,
                    ffmpeg="ffmpeg",
                    sample_interval_seconds=1.0,
                    frame_width=1280,
                    frame_height=720,
                    clock_roi=(100, 20, 180, 60),
                    score_roi=None,
                    score_rois=((10, 20, 90, 60), (190, 20, 270, 60)),
                    maximum_frames=5,
                    deadline_monotonic=None,
                    output_prefix="multi",
                )

        self.assertEqual(kinds, ["clock", "score", "score"])
        self.assertEqual([path.name for path in paths], [
            "multi_clock_000001.png",
            "multi_score_0_000001.png",
            "multi_score_1_000001.png",
        ])
        self.assertEqual(len(diagnostics["score_rois"]), 2)

    def test_explicit_profile_never_calls_auto_discovery(self):
        profile = ScoreboardProfile(
            "source-a", 1280, 720, (10, 10, 80, 40), (80, 10, 130, 40)
        )
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            recognized = [
                (["12:34"], [0.9]),
                (["0-0"], [0.9]),
                (["12:35"], [0.9]),
                (["0-0"], [0.9]),
            ]
            located = {"diagnostics": {}}
            with (
                patch(
                    "scoreboard_ocr_worker.discover_auto_clock",
                    side_effect=AssertionError("auto discovery must not run"),
                ),
                patch(
                    "scoreboard_ocr_worker.extract_profile_frames",
                    return_value=([
                        (Path("clock-1"), Path("score-1")),
                        (Path("clock-2"), Path("score-2")),
                    ], {"profile_id": "source-a"}),
                ),
                patch(
                    "scoreboard_ocr_worker._recognize_paths",
                    return_value=(recognized, []),
                ),
                patch(
                    "scoreboard_ocr_worker._validate_profile_content_quality",
                    return_value={"trusted_clock_frame_count": 2},
                ),
                patch(
                    "scoreboard_ocr_worker.locate_from_readings",
                    return_value=located,
                ),
            ):
                result = run_request(
                    {
                        "candidate_path": str(candidate),
                        "event_code": "YC",
                        "event_minute": 12,
                        "scoreboard_profile": profile,
                    },
                    engine=object(),
                )

        self.assertIs(result, located)

    def test_recovery_only_replaces_frames_the_primary_roi_cannot_parse(self):
        primary = [
            {"clock": (["90:20"], [0.9]), "score": (["1-2"], [0.9])},
            {"clock": (["bad"], [0.9]), "score": (["1-2"], [0.9])},
            {"clock": (["90:22"], [0.9]), "score": (["1-2"], [0.9])},
        ]
        recovered = [
            {"clock": (["90:19"], [0.9]), "score": (["2-2"], [0.9])},
            {"clock": (["90:21"], [0.9]), "score": (["2-2"], [0.9])},
            {"clock": (["90:23"], [0.9]), "score": (["2-2"], [0.9])},
        ]

        merged = _merge_auto_results(primary, recovered)

        self.assertEqual(merged[0]["clock"][0], ["90:20"])
        self.assertEqual(merged[1]["clock"][0], ["90:21"])
        self.assertEqual(merged[2]["clock"][0], ["90:22"])
        self.assertEqual(merged[0]["score"][0], ["1-2"])

    def test_short_broadcast_graphic_does_not_trigger_full_reacquisition(self):
        seven_missing = [(["90:20"], [0.9]), *[([], []) for _ in range(7)]]
        eight_missing = [(["90:20"], [0.9]), *[([], []) for _ in range(8)]]

        self.assertIsNone(
            _first_clock_missing_run(seven_missing, ["clock"] * 8)
        )
        self.assertEqual(
            _first_clock_missing_run(eight_missing, ["clock"] * 9),
            1,
        )


class ProfileCropTests(unittest.TestCase):
    @staticmethod
    def _profile(**overrides):
        values = {
            "profile_id": "source-a",
            "reference_width": 1920,
            "reference_height": 1080,
            "clock_roi": (100, 40, 220, 90),
            "score_roi": (230, 40, 340, 90),
        }
        values.update(overrides)
        return ScoreboardProfile(**values)

    def test_extracts_aligned_clock_and_score_rois(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            candidate = output_dir / "candidate.mp4"
            candidate.write_bytes(b"video")
            commands = []

            def runner(command, **_kwargs):
                commands.append(command)
                if command[0] == "ffprobe":
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps(
                            {"streams": [{"width": 1280, "height": 720}]}
                        ),
                        stderr="",
                    )
                (output_dir / "clock_000001.png").write_bytes(b"clock")
                (output_dir / "score_000001.png").write_bytes(b"score")
                return subprocess.CompletedProcess(
                    command, 0, stdout="", stderr=""
                )

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                pairs, diagnostics = extract_profile_frames(
                    candidate,
                    output_dir,
                    ffmpeg="ffmpeg",
                    sample_interval_seconds=1,
                    profile=self._profile(),
                )

        self.assertEqual(len(pairs), 1)
        self.assertEqual(diagnostics["clock_roi"], [67, 27, 147, 60])
        self.assertEqual(diagnostics["score_roi"], [153, 27, 227, 60])
        filter_graph = commands[1][commands[1].index("-filter_complex") + 1]
        self.assertIn("crop=80:33:67:27", filter_graph)
        self.assertIn("crop=74:33:153:27", filter_graph)
        self.assertIn("split=2", filter_graph)

    def test_clock_only_profile_extracts_one_clock_crop_per_frame(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            candidate = output_dir / "candidate.mp4"
            candidate.write_bytes(b"video")
            commands = []

            def runner(command, **_kwargs):
                commands.append(command)
                if command[0] == "ffprobe":
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps(
                            {"streams": [{"width": 1280, "height": 720}]}
                        ),
                        stderr="",
                    )
                (output_dir / "clock_only_000001.png").write_bytes(b"clock")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                frames, diagnostics = extract_profile_clock_frames(
                    candidate,
                    output_dir,
                    ffmpeg="ffmpeg",
                    sample_interval_seconds=1,
                    profile=self._profile(),
                )

        self.assertEqual([frame.name for frame in frames], ["clock_only_000001.png"])
        self.assertTrue(diagnostics["clock_only"])
        self.assertTrue(diagnostics["score_ocr_skipped"])
        self.assertIsNone(diagnostics["score_roi"])
        ffmpeg_command = commands[1]
        self.assertIn("-vf", ffmpeg_command)
        self.assertNotIn("-filter_complex", ffmpeg_command)
        self.assertNotIn("[score]", ffmpeg_command)

    def test_clock_only_profile_ffconcat_applies_bounds_before_roi_filter(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            segment = output_dir / "segment.ts"
            segment.write_bytes(b"video")
            candidate = output_dir / "candidate.ffconcat"
            candidate.write_text(
                f"ffconcat version 1.0\nfile '{segment}'\n", encoding="utf-8"
            )
            commands = []

            def runner(command, **_kwargs):
                commands.append(command)
                if command[0] == "ffprobe":
                    return subprocess.CompletedProcess(
                        command, 0,
                        stdout=json.dumps({"streams": [{"width": 1280, "height": 720}]}),
                        stderr="",
                    )
                (output_dir / "clock_only_000001.png").write_bytes(b"clock")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                extract_profile_clock_frames(
                    candidate, output_dir, ffmpeg="ffmpeg",
                    sample_interval_seconds=10, profile=self._profile(),
                    input_format="ffconcat", input_seek_seconds=5.0,
                    input_duration_seconds=15.0,
                )

        ffmpeg_command = commands[1]
        self.assertEqual(ffmpeg_command[ffmpeg_command.index("-f") + 1], "concat")
        self.assertEqual(ffmpeg_command[ffmpeg_command.index("-ss") + 1], "5.000000")
        self.assertEqual(ffmpeg_command[ffmpeg_command.index("-t") + 1], "15.000000")
        self.assertIn("crop=", ffmpeg_command[ffmpeg_command.index("-vf") + 1])

    def test_independent_stoppage_extraction_uses_separate_right_and_below_rois(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            candidate = output_dir / "candidate.mp4"
            candidate.write_bytes(b"video")
            commands = []

            def runner(command, **_kwargs):
                commands.append(command)
                (output_dir / "stoppage_right_000001.png").write_bytes(b"right")
                (output_dir / "stoppage_below_000001.png").write_bytes(b"below")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                frames, labels, diagnostics = extract_independent_stoppage_frames(
                    candidate,
                    output_dir,
                    ffmpeg="ffmpeg",
                    sample_interval_seconds=1,
                    frame_width=1280,
                    frame_height=720,
                    clock_roi=(100, 20, 180, 60),
                    maximum_frames=10,
                    deadline_monotonic=None,
                )

        self.assertEqual(labels, ["right", "below"])
        self.assertEqual(
            [frame.name for frame in frames],
            ["stoppage_right_000001.png", "stoppage_below_000001.png"],
        )
        self.assertEqual(set(diagnostics["candidate_rois"]), {"right", "below"})
        filter_graph = commands[0][commands[0].index("-filter_complex") + 1]
        self.assertIn("split=2", filter_graph)
        self.assertIn("crop=160:80:180:0", filter_graph)
        self.assertIn("crop=240:80:60:60", filter_graph)

    def test_independent_stoppage_extraction_clamps_edge_rois(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            candidate = output_dir / "candidate.mp4"
            candidate.write_bytes(b"video")

            def runner(command, **_kwargs):
                (output_dir / "stoppage_below_000001.png").write_bytes(b"below")
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                _frames, labels, diagnostics = extract_independent_stoppage_frames(
                    candidate,
                    output_dir,
                    ffmpeg="ffmpeg",
                    sample_interval_seconds=1,
                    frame_width=1280,
                    frame_height=720,
                    clock_roi=(1220, 650, 1280, 690),
                    maximum_frames=10,
                    deadline_monotonic=None,
                )

        self.assertEqual(labels, ["below"])
        self.assertEqual(diagnostics["candidate_rois"]["below"], [1190, 690, 1280, 720])

    def test_profile_aspect_mismatch_fails_closed_before_extraction(self):
        probe = subprocess.CompletedProcess(
            ["ffprobe"],
            0,
            stdout=json.dumps({"streams": [{"width": 1024, "height": 768}]}),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "scoreboard_ocr_worker.subprocess.run", return_value=probe
            ) as runner:
                with self.assertRaises(WorkerError) as raised:
                    extract_profile_frames(
                        Path(directory) / "candidate.mp4",
                        Path(directory),
                        ffmpeg="ffmpeg",
                        sample_interval_seconds=1,
                        profile=self._profile(),
                    )

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(runner.call_count, 1)

    def test_separate_parsing_repairs_clock_without_using_score_text(self):
        recognized = [
            (["68:55"], [0.9]),
            (["1-0"], [0.9]),
            (["B:56"], [0.8]),
            (["1-0"], [0.9]),
            ([], []),
            ([], []),
            (["68:58"], [0.9]),
            (["1-0"], [0.9]),
        ]

        readings, continuity = _profile_readings(
            recognized,
            profile=self._profile(),
            sample_interval=1,
            period=2,
        )

        self.assertEqual(readings[0].clock_seconds, 68 * 60 + 55)
        self.assertEqual(readings[1].clock_seconds, 68 * 60 + 56)
        self.assertEqual(readings[1].score, (1, 0))
        self.assertEqual(continuity[1]["reason"], "ocr_character_repaired")
        self.assertIsNone(readings[2].clock_seconds)
        self.assertEqual(
            continuity[2]["reason"], "scoreboard_temporarily_missing"
        )
        self.assertEqual(readings[3].clock_seconds, 68 * 60 + 58)

    def test_clock_only_readings_keep_raw_repairs_and_jump_diagnostics(self):
        readings, continuity = _profile_clock_readings(
            [
                (["12:00"], [0.9]),
                ([], []),
                (["12:20"], [0.9]),
                (["35:00"], [0.9]),
                (["12:04"], [0.9]),
            ],
            profile=self._profile(),
            sample_interval=1,
            period=1,
        )

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_minute": 12,
                "clock_only": True,
                "sample_interval_seconds": 1,
            },
        )
        diagnostics = result["diagnostics"]

        self.assertEqual(
            [reading.frame_index for reading in readings], [0, 1, 2, 3, 4]
        )
        self.assertTrue(all(reading.score is None for reading in readings))
        self.assertTrue(all(not reading.score_texts for reading in readings))
        self.assertEqual(readings[1].clock_seconds, 12 * 60 + 1)
        self.assertEqual(readings[1].continuity_status, "repaired")
        self.assertEqual(readings[2].observed_clock_seconds, 12 * 60 + 20)
        self.assertEqual(readings[2].clock_seconds, 12 * 60 + 2)
        self.assertEqual(readings[2].continuity_status, "repaired")
        self.assertEqual(readings[3].observed_clock_seconds, 35 * 60)
        self.assertIsNone(readings[3].clock_seconds)
        self.assertEqual(len(diagnostics["clock_raw_observations"]), 5)
        self.assertEqual(diagnostics["clock_continuity_repair_count"], 2)
        self.assertEqual(
            diagnostics["clock_continuity_repairs"][0]["frame_index"], 1
        )
        self.assertEqual(diagnostics["abnormal_clock_jump_count"], 2)
        self.assertEqual(
            [item["frame_index"] for item in diagnostics["abnormal_clock_jumps"]],
            [2, 3],
        )

    def test_independent_first_half_stoppage_stopwatch_maps_to_match_clock(self):
        main = [(["45:00"], [0.9])] * 3
        auxiliary = [
            {"right": ([clock], [0.92])}
            for clock in ("2:16", "2:17", "2:18")
        ]

        readings, continuity = _profile_clock_readings(
            main,
            profile=self._profile(),
            sample_interval=1,
            period=1,
            independent_stoppage_frames=auxiliary,
            independent_stoppage_base_minute=45,
        )

        self.assertEqual(
            [reading.clock_seconds for reading in readings],
            [47 * 60 + 16, 47 * 60 + 17, 47 * 60 + 18],
        )
        self.assertTrue(
            all(reading.continuity_status == "accepted" for reading in readings)
        )
        self.assertTrue(
            all(
                reading.continuity_reason == "independent_stoppage_stopwatch"
                for reading in readings
            )
        )
        self.assertTrue(continuity[0]["independent_stoppage"]["confirmed"])

    def test_independent_second_half_stoppage_does_not_double_apply_period(self):
        main = [(["90:00"], [0.9])] * 3
        auxiliary = [
            {"below": ([clock], [0.93])}
            for clock in ("5:08", "5:09", "5:10")
        ]

        readings, _continuity = _profile_clock_readings(
            main,
            profile=self._profile(second_half_clock_mode="reset"),
            sample_interval=1,
            period=2,
            independent_stoppage_frames=auxiliary,
            independent_stoppage_base_minute=90,
        )

        self.assertEqual(
            [reading.clock_seconds for reading in readings],
            [95 * 60 + 8, 95 * 60 + 9, 95 * 60 + 10],
        )

    def test_static_or_insufficient_stoppage_candidate_is_rejected(self):
        main = [(["45:00"], [0.9])] * 3
        cases = {
            "static_minute": [
                {"right": (["+2"], [0.9])},
                {"right": (["+2"], [0.9])},
                {"right": (["+2"], [0.9])},
            ],
            "static_stopwatch": [
                {"right": (["2:17"], [0.9])},
                {"right": (["2:17"], [0.9])},
                {"right": (["2:17"], [0.9])},
            ],
            "two_advancing_frames": [
                {"right": (["2:17"], [0.9])},
                {"right": (["2:18"], [0.9])},
            ],
        }

        for name, auxiliary in cases.items():
            with self.subTest(name=name):
                overrides, diagnostics = _confirmed_independent_stoppage_overrides(
                    main,
                    auxiliary,
                    base_minute=45,
                    sample_interval=1,
                )
                self.assertEqual(overrides, {})
                self.assertFalse(diagnostics["confirmed"])
                if name == "static_stopwatch":
                    self.assertTrue(diagnostics["static_candidate_rejected"])

    def test_existing_combined_stoppage_formats_are_not_overridden(self):
        auxiliary = [
            {"right": ([clock], [0.9])}
            for clock in ("9:01", "9:02", "9:03")
        ]
        for main_texts, expected in (
            (("45+2:16", "45+2:17", "45+2:18"), 47 * 60 + 16),
            (("45:00+02:16", "45:00+02:17", "45:00+02:18"), 47 * 60 + 16),
        ):
            with self.subTest(main_texts=main_texts):
                readings, continuity = _profile_clock_readings(
                    [([text], [0.9]) for text in main_texts],
                    profile=self._profile(),
                    sample_interval=1,
                    period=1,
                    independent_stoppage_frames=auxiliary,
                    independent_stoppage_base_minute=45,
                )
                self.assertEqual(readings[0].clock_seconds, expected)
                self.assertFalse(
                    continuity[0]["independent_stoppage"]["confirmed"]
                )

    def test_clock_only_locates_exact_second_after_stream_gap_resynchronization(self):
        readings, _continuity = _profile_clock_readings(
            [
                (["42:52"], [0.9]),
                (["44:37"], [0.9]),
                (["44:39"], [0.9]),
                (["44:40"], [0.9]),
                (["44:41"], [0.9]),
            ],
            profile=self._profile(),
            sample_interval=1,
            period=1,
        )

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_minute": 44,
                "event_second": 44 * 60 + 40,
                "clock_only": True,
                "candidate_start_seconds": 100,
                "sample_interval_seconds": 1,
            },
        )

        self.assertEqual(readings[2].continuity_status, "resynchronized")
        self.assertEqual(readings[3].continuity_status, "accepted")
        self.assertEqual(readings[3].clock_seconds, 44 * 60 + 40)
        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(result["anchor_seconds"], 103)

    @staticmethod
    def _clock_readings():
        return [
            frame_reading(0, 0, ["58:59"], [0.9]),
            frame_reading(1, 1, ["59:00"], [0.9]),
            frame_reading(2, 2, ["59:01"], [0.9]),
        ]

    def test_clock_only_uses_goal_second_and_card_minute_boundary(self):
        for code in ("G", "OG", "PG", "YC", "RC"):
            with self.subTest(code=code):
                result = locate_from_readings(
                    self._clock_readings(),
                    {
                        "event_code": code,
                        "event_minute": 59,
                        "event_second": 58 * 60 + 59,
                        "target_score": "9-9",
                        "candidate_start_seconds": 100,
                        "sample_interval_seconds": 1,
                        "clock_only": True,
                    },
                )

                expected_anchor = 100 if code in {"G", "OG", "PG"} else 101
                self.assertEqual(result["anchor_seconds"], expected_anchor)
                self.assertEqual(
                    result["location_kind"],
                    "match_clock_second"
                    if code in {"G", "OG", "PG"}
                    else "match_clock_minute_boundary",
                )
                self.assertTrue(result["clock_only"])
                self.assertTrue(result["diagnostics"]["score_ocr_skipped"])
                self.assertEqual(result["localization_quality"], "exact")
                self.assertFalse(result["degraded"])
                self.assertIsNone(result["degradation_mode"])
                self.assertIsNone(result["degradation_reason"])

    @staticmethod
    def _direct_clock_run(
        start_clock_seconds: int,
        count: int,
        *,
        start_frame_index: int = 0,
        start_video_seconds: float = 0.0,
        video_step: float = 1.0,
    ):
        return [
            frame_reading(
                start_frame_index + index,
                start_video_seconds + index * video_step,
                [
                    f"{(start_clock_seconds + index) // 60:02d}:"
                    f"{(start_clock_seconds + index) % 60:02d}"
                ],
                [0.95],
            )
            for index in range(count)
        ]

    def test_goal_second_uses_closest_real_preceding_frame(self):
        readings = self._direct_clock_run(51 * 60 + 58, 2)

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 52 * 60,
                "candidate_start_seconds": 100.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 101.0)
        self.assertEqual(result["method"], "paddleocr_nearby_clock_observation")
        self.assertEqual(result["precision"], "estimated_second")
        self.assertEqual(result["localization_quality"], "estimated")
        self.assertTrue(result["degraded"])
        self.assertEqual(result["observed_clock"], "51:59")
        self.assertEqual(result["observed_clock_delta_seconds"], -1)
        self.assertEqual(result["observed_clock_distance_seconds"], 1)
        self.assertEqual(result["accepted_clock_tolerance_seconds"], 5)
        diagnostics = result["diagnostics"]
        self.assertEqual(
            diagnostics["exact_second_candidate_source"],
            "nearby_direct_observation",
        )
        self.assertEqual(diagnostics["nearby_observed_clock"], "51:59")
        self.assertEqual(diagnostics["nearby_observed_direct_reading_count"], 2)
        self.assertEqual(diagnostics["nearby_observed_evidence_frame_indices"], [0, 1])
        self.assertEqual(diagnostics["nearby_observed_clock_video_slope"], 1.0)

    def test_goal_second_accepts_real_following_frame_at_five_second_limit(self):
        readings = self._direct_clock_run(
            52 * 60 + 5,
            7,
            start_frame_index=2,
            start_video_seconds=2.0,
        )

        result = locate_from_readings(
            readings,
            {
                "event_code": "OG",
                "event_second": 52 * 60,
                "candidate_start_seconds": 100.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 102.0)
        self.assertEqual(result["observed_clock"], "52:05")
        self.assertEqual(result["observed_clock_distance_seconds"], 5)
        self.assertEqual(
            result["diagnostics"]["nearby_observed_clock_delta_seconds"],
            5,
        )
        self.assertEqual(
            result["diagnostics"]["nearby_observed_evidence_frame_indices"],
            [2, 3, 4, 5, 6],
        )

    def test_goal_second_rejects_equally_near_real_frames_at_different_positions(self):
        readings = [
            *self._direct_clock_run(51 * 60 + 58, 2),
            *self._direct_clock_run(
                52 * 60 + 1,
                2,
                start_frame_index=20,
                start_video_seconds=20.0,
            ),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {"event_code": "G", "event_second": 52 * 60},
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")
        self.assertEqual(
            raised.exception.diagnostics["exact_second_failure_reason"],
            "multiple_equally_near_observed_clocks",
        )
        self.assertEqual(
            raised.exception.diagnostics["nearby_observed_clock_distance_seconds"],
            1,
        )

    def test_card_minute_boundary_keeps_estimated_method_and_precision(self):
        readings = self._direct_clock_run(29 * 60 + 56, 3)

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 30,
                "candidate_start_seconds": 100.0,
                "clock_only": True,
            },
        )

        self.assertEqual(result["anchor_seconds"], 102.0)
        self.assertEqual(result["method"], "paddleocr_estimated_minute_boundary")
        self.assertEqual(result["precision"], "estimated_minute_boundary")
        self.assertEqual(result["localization_quality"], "estimated")
        self.assertEqual(result["estimated_error_bound_seconds"], 2)

    def test_minute_boundary_uses_real_5159_frame_when_5200_is_missing(self):
        readings = self._direct_clock_run(51 * 60 + 58, 2)

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 52,
                "candidate_start_seconds": 100.0,
                "clock_only": True,
            },
        )

        self.assertEqual(result["anchor_seconds"], 101.0)
        self.assertEqual(result["method"], "paddleocr_estimated_minute_boundary")
        self.assertEqual(result["precision"], "estimated_minute_boundary")
        self.assertEqual(result["location_kind"], "match_clock_minute_boundary")
        self.assertEqual(result["observed_clock"], "51:59")
        self.assertEqual(result["observed_clock_distance_seconds"], 1)
        self.assertTrue(result["degraded"])

    def test_goal_second_direct_observation_remains_ahead_of_estimate(self):
        readings = self._direct_clock_run(69 * 60 + 35, 4)

        result = locate_from_readings(
            readings,
            {"event_code": "G", "event_second": 69 * 60 + 37},
        )

        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(result["precision"], "observed_second")
        self.assertEqual(result["localization_quality"], "exact")

    def test_goal_second_accepts_clear_isolated_target_as_estimated(self):
        readings = [frame_reading(0, 7.0, ["69:37"], [0.95])]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 69 * 60 + 37,
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
                "minimum_confidence": 0.35,
            },
        )

        self.assertEqual(result["anchor_seconds"], 107.0)
        self.assertEqual(result["method"], "paddleocr_single_frame_target")
        self.assertEqual(result["precision"], "estimated_second")
        self.assertEqual(result["localization_quality"], "estimated")
        self.assertTrue(result["degraded"])
        self.assertEqual(
            result["degradation_mode"], "single_frame_target_observation"
        )
        self.assertTrue(result["target_clock_directly_observed"])
        self.assertEqual(result["estimated_error_bound_seconds"], 0.5)
        diagnostics = result["diagnostics"]
        self.assertEqual(diagnostics["isolated_target_reading_count"], 1)
        self.assertEqual(diagnostics["accepted_isolated_target_reading_count"], 1)
        self.assertEqual(diagnostics["direct_observation_candidate_count"], 0)
        self.assertEqual(
            diagnostics["exact_second_candidate_source"],
            "single_frame_target_observation",
        )

    def test_goal_second_rejects_two_isolated_target_positions_as_ambiguous(self):
        readings = [
            frame_reading(0, 7.0, ["69:37"], [0.95]),
            frame_reading(10, 17.0, ["69:37"], [0.96]),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "G",
                    "event_second": 69 * 60 + 37,
                    "sample_interval_seconds": 1.0,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")
        diagnostics = raised.exception.diagnostics
        self.assertEqual(
            diagnostics["exact_second_failure_reason"],
            "conflicting_isolated_target_observations",
        )
        self.assertEqual(diagnostics["matching_occurrence_count"], 2)
        self.assertEqual(diagnostics["matching_frame_seconds"], [7.0, 17.0])

    def test_goal_second_ignores_unrelated_ambiguous_frame_for_single_target(self):
        readings = [
            frame_reading(0, 7.0, ["69:37"], [0.95]),
            frame_reading(10, 17.0, ["69:52", "68:52"], [0.95, 0.95]),
        ]

        result = locate_from_readings(
            readings,
            {"event_code": "G", "event_second": 69 * 60 + 37},
        )

        self.assertEqual(result["anchor_seconds"], 7.0)
        self.assertEqual(result["method"], "paddleocr_single_frame_target")
        self.assertEqual(result["localization_quality"], "estimated")

    def test_goal_second_exact_candidate_precedes_isolated_estimate(self):
        readings = [
            frame_reading(0, 7.0, ["69:37"], [0.95]),
            frame_reading(10, 17.0, ["69:37"], [0.96]),
            frame_reading(11, 18.0, ["69:38"], [0.96]),
        ]

        result = locate_from_readings(
            readings,
            {"event_code": "G", "event_second": 69 * 60 + 37},
        )

        self.assertEqual(result["anchor_seconds"], 17.0)
        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(result["localization_quality"], "exact")

    def test_goal_second_rejects_unsafe_isolated_readings(self):
        target = 69 * 60 + 37
        cases = {
            "below_existing_confidence_threshold": (
                [frame_reading(0, 7.0, ["69:37"], [0.34])],
                {"minimum_confidence": 0.35},
            ),
            "ambiguous_clock": (
                [frame_reading(0, 7.0, ["69:37", "68:37"], [0.95, 0.95])],
                {},
            ),
            "non_target_clock": (
                [frame_reading(0, 7.0, ["69:36"], [0.95])],
                {},
            ),
            "resynchronized_target": (
                [
                    replace(
                        frame_reading(0, 7.0, ["69:37"], [0.95]),
                        continuity_status="resynchronized",
                    )
                ],
                {},
            ),
        }

        for name, (readings, options) in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(WorkerError) as raised:
                    locate_from_readings(
                        readings,
                        {
                            "event_code": "G",
                            "event_second": target,
                            **options,
                        },
                    )
                self.assertIn(
                    raised.exception.kind,
                    {"ocr_clock_unreadable", "ocr_exact_second_not_found"},
                )

    def test_goal_second_accepts_target_with_adjacent_continuity_reading(self):
        readings = [
            frame_reading(0, 7.0, ["69:37"], [0.95]),
            frame_reading(1, 8.0, ["69:38"], [0.95]),
        ]

        result = locate_from_readings(
            readings,
            {"event_code": "G", "event_second": 69 * 60 + 37},
        )

        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(result["precision"], "observed_second")
        self.assertEqual(result["anchor_seconds"], 7.0)

    def test_goal_second_direct_observation_precedes_disjoint_interpolation(self):
        readings = [
            frame_reading(0, 0.0, ["69:37"], [0.95]),
            frame_reading(1, 1.0, ["69:38"], [0.95]),
            frame_reading(100, 100.0, ["69:36"], [0.95]),
            frame_reading(102, 102.0, ["69:38"], [0.95]),
        ]

        result = locate_from_readings(
            readings,
            {"event_code": "G", "event_second": 69 * 60 + 37},
        )

        self.assertEqual(result["anchor_seconds"], 0.0)
        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(
            result["diagnostics"]["exact_second_candidate_source"],
            "direct_observation",
        )
        self.assertEqual(
            result["diagnostics"]["two_sided_interpolation_candidate_count"],
            1,
        )

    def test_goal_second_two_sided_interpolation_remains_ahead_of_estimate(self):
        readings = [
            frame_reading(0, 0.0, ["69:34"], [0.95]),
            frame_reading(1, 1.0, ["69:35"], [0.95]),
            frame_reading(4, 4.0, ["69:38"], [0.95]),
        ]

        result = locate_from_readings(
            readings,
            {"event_code": "PG", "event_second": 69 * 60 + 37},
        )

        self.assertEqual(result["method"], "paddleocr_interpolated_clock")
        self.assertEqual(result["precision"], "interpolated_second")
        self.assertEqual(result["localization_quality"], "exact")

    def test_goal_second_projects_from_stable_mapping_across_clock_occlusion(self):
        readings = [
            frame_reading(0, 0.0, ["84:45"], [0.95]),
            frame_reading(1, 1.0, ["84:46"], [0.95]),
            *[
                frame_reading(index, float(index), [], [])
                for index in range(2, 22)
            ],
            frame_reading(22, 22.0, ["85:07"], [0.95]),
            frame_reading(23, 23.0, ["85:08"], [0.95]),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 85 * 60,
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 115.0)
        self.assertEqual(result["method"], "paddleocr_stable_clock_mapping")
        self.assertEqual(result["precision"], "projected_second")
        self.assertEqual(result["localization_quality"], "projected")
        self.assertTrue(result["degraded"])
        self.assertEqual(result["degradation_mode"], "mapped_clock_projection")
        self.assertFalse(result["target_clock_directly_observed"])
        self.assertEqual(result["projection_status"], "estimated")
        self.assertEqual(result["estimated_error_bound_seconds"], 0.5)
        self.assertIn("目标时钟所在画面被遮挡", result["degradation_reason"]["message"])
        mapping = result["clock_video_mapping"]
        self.assertEqual(mapping["status"], "stable")
        self.assertEqual(mapping["mapping_kind"], "interpolation")
        self.assertEqual(mapping["sample_count"], 4)
        self.assertEqual(mapping["frame_span_seconds"], 23.0)
        self.assertEqual(mapping["clock_span_seconds"], 23)
        self.assertEqual(mapping["slope"], 1.0)
        self.assertEqual(mapping["maximum_residual_seconds"], 0.0)
        self.assertTrue(mapping["video_gap_checked"])
        self.assertTrue(mapping["clock_regression_checked"])
        self.assertTrue(mapping["resynchronization_checked"])

    def test_goal_second_projects_short_forward_distance_after_stable_run(self):
        readings = [
            *self._direct_clock_run(70 * 60 + 48, 4),
            *[
                frame_reading(index, float(index), [], [])
                for index in range(4, 13)
            ],
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "OG",
                "event_second": 71 * 60,
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 112.0)
        self.assertEqual(result["precision"], "projected_second")
        self.assertEqual(
            result["clock_video_mapping"]["mapping_kind"],
            "forward_extrapolation",
        )
        self.assertEqual(
            result["clock_video_mapping"]["projection_distance_seconds"],
            9,
        )
        self.assertEqual(result["estimated_error_bound_seconds"], 0.9)

    def test_minute_boundary_preserves_projected_mapping_status(self):
        readings = [
            *self._direct_clock_run(70 * 60 + 48, 4),
            *[
                frame_reading(index, float(index), [], [])
                for index in range(4, 13)
            ],
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 71,
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
                "clock_only": True,
            },
        )

        self.assertEqual(result["anchor_seconds"], 112.0)
        self.assertEqual(result["method"], "paddleocr_projected_minute_boundary")
        self.assertEqual(result["precision"], "projected_minute_boundary")
        self.assertEqual(result["location_kind"], "match_clock_minute_boundary")
        self.assertEqual(result["localization_quality"], "projected")
        self.assertEqual(result["degradation_mode"], "mapped_clock_projection")
        self.assertFalse(result["target_clock_directly_observed"])

    def test_goal_second_mapping_rejects_video_gap_resync_and_weak_evidence(self):
        stable = [
            *self._direct_clock_run(70 * 60 + 48, 4),
            *[
                frame_reading(index, float(index), [], [])
                for index in range(4, 13)
            ],
        ]
        cases = {
            "video_gap": [reading for reading in stable if reading.frame_index != 8],
            "resynchronized": [
                replace(
                    reading,
                    continuity_status=(
                        "resynchronized"
                        if reading.frame_index == 8
                        else reading.continuity_status
                    ),
                )
                for reading in stable
            ],
            "only_three_direct_readings": stable[1:],
        }

        for name, readings in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(WorkerError) as raised:
                    locate_from_readings(
                        readings,
                        {
                            "event_code": "G",
                            "event_second": 71 * 60,
                            "sample_interval_seconds": 1.0,
                        },
                    )
                self.assertEqual(
                    raised.exception.kind,
                    "ocr_exact_second_not_found",
                )

    def test_goal_second_nearby_observation_rejects_weak_evidence(self):
        valid = self._direct_clock_run(69 * 60 + 33, 3)
        cases = {
            "nearest_more_than_five_seconds": self._direct_clock_run(
                69 * 60 + 29, 3
            ),
            "fewer_than_two_direct_readings": self._direct_clock_run(
                69 * 60 + 36, 1
            ),
            "clock_video_slope_not_near_one": self._direct_clock_run(
                69 * 60 + 33, 3, video_step=2.0
            ),
            "frame_index_gap": [
                valid[0],
                replace(valid[1], frame_index=2),
                replace(valid[2], frame_index=4),
            ],
            "resynchronized_evidence": [
                replace(reading, continuity_status="resynchronized")
                for reading in valid
            ],
            "resync_before_otherwise_valid_run": [
                replace(
                    frame_reading(0, 0.0, ["69:32"], [0.95]),
                    continuity_status="resynchronized",
                ),
                *[
                    replace(
                        reading,
                        frame_index=reading.frame_index + 1,
                        frame_seconds=reading.frame_seconds + 1.0,
                    )
                    for reading in valid
                ],
            ],
            "repaired_evidence_only": [
                replace(reading, continuity_status="repaired")
                for reading in valid
            ],
        }

        for name, readings in cases.items():
            with self.subTest(name=name):
                with self.assertRaises(WorkerError) as raised:
                    locate_from_readings(
                        readings,
                        {
                            "event_code": "G",
                            "event_second": 69 * 60 + 37,
                        },
                    )
                self.assertIn(
                    raised.exception.kind,
                    {"ocr_clock_unreadable", "ocr_exact_second_not_found"},
                )

    def test_clock_only_goal_falls_back_from_missing_second_to_minute_boundary(self):
        result = locate_from_readings(
            self._clock_readings(),
            {
                "event_code": "G",
                "event_minute": 59,
                "event_second": 58 * 60 + 30,
                "candidate_start_seconds": 100,
                "sample_interval_seconds": 1,
                "clock_only": True,
            },
        )

        self.assertEqual(result["anchor_seconds"], 101)
        self.assertEqual(result["location_kind"], "match_clock_minute_boundary")
        self.assertEqual(result["method"], "paddleocr_minute_boundary")
        self.assertEqual(result["localization_quality"], "degraded")
        self.assertTrue(result["degraded"])
        self.assertEqual(
            result["degradation_mode"],
            "minute_boundary_fallback",
        )
        self.assertEqual(
            result["exact_second_error"]["kind"],
            "ocr_exact_second_not_found",
        )
        self.assertEqual(
            result["degradation_reason"],
            result["exact_second_error"],
        )
        self.assertEqual(result["minute_window_start_clock"], "58:00")
        self.assertEqual(result["minute_window_end_clock"], "59:00")

    def test_clock_only_penalty_goal_falls_back_from_missing_second(self):
        result = locate_from_readings(
            self._clock_readings(),
            {
                "event_code": "PG",
                "event_minute": 59,
                "event_second": 58 * 60 + 30,
                "candidate_start_seconds": 100,
                "sample_interval_seconds": 1,
                "clock_only": True,
            },
        )

        self.assertEqual(result["anchor_seconds"], 101)
        self.assertEqual(result["location_kind"], "match_clock_minute_boundary")
        self.assertEqual(result["degradation_mode"], "minute_boundary_fallback")
        self.assertEqual(result["exact_second_error"]["kind"], "ocr_exact_second_not_found")

    def test_clock_only_unreadable_ocr_still_fails(self):
        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                [frame_reading(0, 0, [], [])],
                {
                    "event_code": "G",
                    "event_minute": 59,
                    "event_second": 58 * 60 + 30,
                    "clock_only": True,
                },
            )

        self.assertEqual(raised.exception.kind, "scoreboard_missing")

    def test_clock_only_requires_minute_and_boolean_flag(self):
        for request in (
            {"event_code": "G", "clock_only": True},
            {"event_code": "G", "clock_only": "true", "event_minute": 59},
            {"event_code": "PM", "clock_only": True, "event_minute": 59},
        ):
            with self.subTest(request=request):
                with self.assertRaises(WorkerError) as raised:
                    locate_from_readings(self._clock_readings(), request)
                self.assertEqual(raised.exception.kind, "ocr_invalid_request")

    def test_default_goal_behavior_still_uses_score_transition(self):
        readings = [
            frame_reading(0, 0, ["59:05", "0-0"], [0.9, 0.9]),
            frame_reading(1, 1, ["59:06", "1-0"], [0.9, 0.9]),
            frame_reading(2, 2, ["59:07", "1-0"], [0.9, 0.9]),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_minute": 59,
                "target_score": "1-0",
            },
        )

        self.assertEqual(result["method"], "paddleocr_score_transition")
        self.assertNotIn("clock_only", result)

    def test_default_penalty_goal_behavior_still_uses_score_transition(self):
        readings = [
            frame_reading(0, 0, ["59:05", "0-0"], [0.9, 0.9]),
            frame_reading(1, 1, ["59:06", "1-0"], [0.9, 0.9]),
            frame_reading(2, 2, ["59:07", "1-0"], [0.9, 0.9]),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "PG",
                "event_minute": 59,
                "target_score": "1-0",
            },
        )

        self.assertEqual(result["method"], "paddleocr_score_transition")

    def test_profile_run_submits_only_clock_crops(self):
        profile = ProfileCropTests._profile()
        captured = {}
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            clock_paths = [Path(f"clock-{index}.png") for index in range(4)]

            def recognize(frames, crop_kinds, **_kwargs):
                captured["frames"] = list(frames)
                captured["crop_kinds"] = list(crop_kinds)
                return (
                    [
                        ([f"12:0{index}"], [0.9])
                        for index in range(len(frames))
                    ],
                    [],
                )

            with (
                patch(
                    "scoreboard_ocr_worker.extract_profile_clock_frames",
                    return_value=(
                        clock_paths,
                        {"profile_id": profile.profile_id, "clock_only": True},
                    ),
                ),
                patch(
                    "scoreboard_ocr_worker.extract_profile_frames",
                    side_effect=AssertionError("score crops must not be extracted"),
                ),
                patch(
                    "scoreboard_ocr_worker.extract_independent_stoppage_frames",
                    side_effect=AssertionError(
                        "ordinary events must not extract stoppage crops"
                    ),
                ),
                patch(
                    "scoreboard_ocr_worker._recognize_request_paths",
                    side_effect=recognize,
                ),
            ):
                result = run_request(
                    {
                        "candidate_path": str(candidate),
                        "event_code": "G",
                        "event_minute": 12,
                        "clock_only": True,
                        "scoreboard_profile": profile,
                    },
                    engine=object(),
                )

        self.assertEqual(captured["frames"], clock_paths)
        self.assertEqual(captured["crop_kinds"], ["clock"] * 4)
        self.assertTrue(result["clock_only"])
        self.assertTrue(result["diagnostics"]["score_ocr_skipped"])

    def test_profile_run_analyzes_independent_stopwatch_for_explicit_stoppage(self):
        profile = ProfileCropTests._profile()
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            main_paths = [Path(f"clock-{index}.png") for index in range(3)]
            auxiliary_paths = [Path(f"stoppage-{index}.png") for index in range(3)]

            def recognize(frames, _crop_kinds, **_kwargs):
                if list(frames) == main_paths:
                    return [(["45:00"], [0.9])] * 3, []
                if list(frames) == auxiliary_paths:
                    return (
                        [([clock], [0.92]) for clock in ("2:16", "2:17", "2:18")],
                        [],
                    )
                raise AssertionError(f"unexpected OCR paths: {frames!r}")

            with (
                patch(
                    "scoreboard_ocr_worker.extract_profile_clock_frames",
                    return_value=(
                        main_paths,
                        {
                            "profile_id": profile.profile_id,
                            "clock_only": True,
                            "frame_resolution": [1920, 1080],
                            "clock_roi": [100, 40, 220, 90],
                            "score_roi": None,
                        },
                    ),
                ),
                patch(
                    "scoreboard_ocr_worker.extract_independent_stoppage_frames",
                    return_value=(
                        auxiliary_paths,
                        ["right"] * 3,
                        {"candidate_labels": ["right"], "frame_count": 3},
                    ),
                ) as auxiliary_extraction,
                patch(
                    "scoreboard_ocr_worker._recognize_request_paths",
                    side_effect=recognize,
                ),
            ):
                result = run_request(
                    {
                        "candidate_path": str(candidate),
                        "event_code": "G",
                        "event_minute": "45+2",
                        "event_second": 47 * 60 + 17,
                        "clock_only": True,
                        "scoreboard_profile": profile,
                    },
                    engine=object(),
                )

        auxiliary_extraction.assert_called_once()
        self.assertEqual(result["anchor_seconds"], 1.0)
        self.assertEqual(result["method"], "paddleocr_exact_clock")
        independent = result["diagnostics"]["independent_stoppage"]
        self.assertEqual(independent["status"], "confirmed")
        self.assertEqual(
            independent["confirmation"]["method"],
            "independent_stoppage_stopwatch",
        )

    def test_stoppage_analysis_failure_falls_back_to_primary_clock(self):
        profile = ProfileCropTests._profile()
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            main_paths = [Path(f"clock-{index}.png") for index in range(3)]
            with (
                patch(
                    "scoreboard_ocr_worker.extract_profile_clock_frames",
                    return_value=(
                        main_paths,
                        {
                            "profile_id": profile.profile_id,
                            "clock_only": True,
                            "frame_resolution": [1920, 1080],
                            "clock_roi": [100, 40, 220, 90],
                            "score_roi": None,
                        },
                    ),
                ),
                patch(
                    "scoreboard_ocr_worker.extract_independent_stoppage_frames",
                    side_effect=WorkerError(
                        "ocr_frame_extraction_failed", "auxiliary crop failed"
                    ),
                ),
                patch(
                    "scoreboard_ocr_worker._recognize_request_paths",
                    return_value=(
                        [
                            (["47:16"], [0.9]),
                            (["47:17"], [0.9]),
                            (["47:18"], [0.9]),
                        ],
                        [],
                    ),
                ),
            ):
                result = run_request(
                    {
                        "candidate_path": str(candidate),
                        "event_code": "G",
                        "event_minute": "45+2",
                        "event_second": 47 * 60 + 17,
                        "clock_only": True,
                        "scoreboard_profile": profile,
                    },
                    engine=object(),
                )

        self.assertEqual(result["anchor_seconds"], 1.0)
        independent = result["diagnostics"]["independent_stoppage"]
        self.assertEqual(independent["status"], "failed")
        self.assertEqual(independent["fallback"], "primary_clock")
        self.assertEqual(
            independent["error"]["kind"], "ocr_frame_extraction_failed"
        )

    def test_auto_run_disables_discovered_score_rois(self):
        tracker = AutoClockTracker(1280, 720)
        tracker.clock_roi = (10, 10, 80, 40)
        tracker.score_roi = (80, 10, 120, 40)
        tracker.score_rois = ((80, 10, 120, 40),)
        captured = {}
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            clock_paths = [Path(f"auto-clock-{index}.png") for index in range(4)]

            def extract(*_args, **kwargs):
                captured.update(kwargs)
                return clock_paths, ["clock"] * 4, {"clock_roi": [10, 10, 80, 40]}

            with (
                patch(
                    "scoreboard_ocr_worker.discover_auto_clock",
                    return_value=(
                        tracker,
                        {
                            "frame_resolution": [1280, 720],
                            "score_roi": [80, 10, 120, 40],
                        },
                    ),
                ),
                patch(
                    "scoreboard_ocr_worker.extract_auto_roi_frames",
                    side_effect=extract,
                ),
                patch(
                    "scoreboard_ocr_worker._recognize_request_paths",
                    return_value=(
                        [
                            ([f"12:0{index}"], [0.9])
                            for index in range(4)
                        ],
                        [],
                    ),
                ),
            ):
                result = run_request(
                    {
                        "candidate_path": str(candidate),
                        "event_code": "RC",
                        "event_minute": 12,
                        "clock_only": True,
                    },
                    engine=object(),
                )

        self.assertIsNone(captured["score_roi"])
        self.assertEqual(captured["score_rois"], ())
        self.assertTrue(result["clock_only"])
        self.assertIsNone(result["diagnostics"]["auto_clock"]["score_roi"])

    def test_replay_scoreboard_gap_stays_in_one_minute_interval(self):
        readings = [
            frame_reading(0, 0, ["59:05"], [0.9]),
            *[
                frame_reading(index, index, [], [])
                for index in range(1, 11)
            ],
            frame_reading(11, 11, ["59:16"], [0.9]),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 59,
                "sample_interval_seconds": 1,
            },
        )

        self.assertEqual(result["candidate_interval_start_seconds"], 0)
        self.assertEqual(result["candidate_interval_end_seconds"], 12)

    def test_profile_content_rejects_one_random_clock_reading(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = [
            split_frame_reading(
                index,
                index,
                ["59:10"] if index == 4 else [],
                [],
                tracker=tracker,
                period=2,
            )
            for index in range(10)
        ]

        with self.assertRaises(WorkerError) as raised:
            _validate_profile_content_quality(readings)

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(raised.exception.diagnostics["trusted_clock_frame_count"], 1)
        self.assertEqual(raised.exception.diagnostics["minimum_trusted_clock_frames"], 2)
        self.assertEqual(raised.exception.diagnostics["minimum_trusted_clock_rate"], 0.2)

    def test_profile_content_allows_two_progressing_clock_readings_at_low_rate(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = [
            split_frame_reading(
                index,
                index,
                [f"59:{10 + index:02d}"] if index < 2 else [],
                [],
                tracker=tracker,
                period=2,
            )
            for index in range(20)
        ]

        diagnostics = _validate_profile_content_quality(readings)

        self.assertEqual(diagnostics["trusted_clock_frame_count"], 2)
        self.assertEqual(diagnostics["minimum_trusted_clock_frames"], 2)
        self.assertEqual(diagnostics["trusted_clock_rate"], 0.1)
        self.assertLess(
            diagnostics["trusted_clock_rate"],
            diagnostics["minimum_trusted_clock_rate"],
        )
        self.assertEqual(diagnostics["clock_progression_seconds"], 1)

    def test_profile_content_rejects_repeated_static_clock_like_text(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = [
            split_frame_reading(
                index,
                index,
                ["59:10"],
                ["1-0"],
                tracker=tracker,
                period=2,
            )
            for index in range(10)
        ]

        with self.assertRaises(WorkerError) as raised:
            _validate_profile_content_quality(readings)

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(raised.exception.diagnostics["clock_progression_seconds"], 0)
        self.assertEqual(
            raised.exception.diagnostics["minimum_clock_progression_seconds"], 1
        )

    def test_profile_content_allows_replay_gap_with_trusted_clock_around_it(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = []
        for index in range(15):
            if 5 <= index < 10:
                clock_texts = []
                score_texts = []
            else:
                clock_texts = [f"59:{index:02d}"]
                score_texts = ["1-0"]
            readings.append(
                split_frame_reading(
                    index,
                    index,
                    clock_texts,
                    score_texts,
                    tracker=tracker,
                    period=2,
                )
            )

        diagnostics = _validate_profile_content_quality(readings)

        self.assertEqual(diagnostics["trusted_clock_frame_count"], 10)
        self.assertEqual(diagnostics["scoreboard_missing_frame_count"], 5)
        self.assertGreater(diagnostics["trusted_clock_rate"], 0.6)


class BatchOcrWorkerTests(unittest.TestCase):
    def test_runtime_invalidates_only_failed_generation(self):
        release_first = threading.Event()
        first_started = threading.Event()
        calls = 0

        def recognizer(_engine, crops, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                release_first.wait(2)
            return [([str(crop)], [0.9]) for crop in crops]

        runtime = _SocketOcrRuntime(
            engine_factory=lambda _language: object(),
            batch_recognizer=recognizer,
            max_batch_size=1,
            batch_wait_seconds=0,
            queue_capacity=2,
        )
        first_worker = runtime.worker_for("en")
        pending = first_worker.submit(
            match_id="match-a",
            video_pts=1,
            kind="clock",
            profile="source-a",
            crop="10:00",
        )
        try:
            self.assertTrue(first_started.wait(1))
            restart = runtime.invalidate_generation(first_worker.generation)
            with self.assertRaises(WorkerError) as invalidated:
                pending.result(timeout=1)
            self.assertEqual(
                invalidated.exception.kind,
                "ocr_backend_generation_invalidated",
            )

            second_worker = runtime.worker_for("en")
            recovered = second_worker.submit(
                match_id="match-b",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            ).result(timeout=1)
        finally:
            release_first.set()
            runtime.close(timeout=1)

        self.assertTrue(restart["ocr_backend_restarted"])
        self.assertEqual(restart["backend_generation_before"], 1)
        self.assertEqual(restart["backend_generation_after"], 2)
        self.assertEqual(recovered.backend_generation, 2)

    def test_request_local_errors_do_not_select_backend_restart(self):
        for kind, stage in (
            ("scoreboard_missing", "profile_content_validation"),
            ("clock_profile_mismatch", "profile_validation"),
            ("ocr_exact_second_not_found", "target_localization"),
        ):
            with self.subTest(kind=kind):
                document = {
                    "ok": False,
                    "error": {
                        "kind": kind,
                        "diagnostics": {
                            "stage": stage,
                            # Even a malformed upstream diagnostic must not
                            # turn a request-local failure into a restart.
                            "backend_unhealthy": True,
                            "backend_generation": 1,
                        },
                    },
                }
                self.assertIsNone(_restartable_backend_generation(document))

    def test_shared_path_recognition_streams_more_crops_than_queue_capacity(self):
        def slow_recognizer(_engine, crops, **_kwargs):
            time.sleep(0.35)
            return [([str(crop)], [0.9]) for crop in crops]

        worker = BatchOcrWorker(
            engine_factory=lambda _language: object(),
            batch_recognizer=slow_recognizer,
            max_batch_size=4,
            batch_wait_seconds=0.02,
            queue_capacity=4,
        )
        paths = [Path(f"clock-{index}.png") for index in range(12)]
        try:
            recognized, failed = _recognize_paths_shared(
                worker,
                paths,
                kinds=["clock"] * len(paths),
                match_id="match-a",
                profile_id="source-a",
                candidate_start_seconds=0,
                sample_interval=1,
                minimum_confidence=0,
                deadline_monotonic=time.monotonic() + 4.0,
            )
        finally:
            worker.close(timeout=1)

        self.assertEqual(failed, [])
        self.assertEqual(
            [texts[0] for texts, _confidences in recognized],
            [str(path) for path in paths],
        )

    def test_batches_clock_and_score_crops_across_matches(self):
        factory_calls: list[str] = []
        engine = FakeEngine()

        def factory(language):
            factory_calls.append(language)
            return engine

        worker = BatchOcrWorker(
            engine_factory=factory,
            max_batch_size=4,
            batch_wait_seconds=0.1,
            queue_capacity=8,
        )
        try:
            futures = [
                worker.submit(
                    match_id="match-a",
                    video_pts=10.0,
                    kind="clock",
                    profile="source-a",
                    crop="44:59",
                ),
                worker.submit(
                    match_id="match-a",
                    video_pts=10.0,
                    kind="score",
                    profile="source-a",
                    crop="0-0",
                ),
                worker.submit(
                    match_id="match-b",
                    video_pts=28.5,
                    kind="clock",
                    profile="source-b",
                    crop="63:12",
                ),
                worker.submit(
                    match_id="match-b",
                    video_pts=28.5,
                    kind="score",
                    profile="source-b",
                    crop="2-1",
                ),
            ]
            results = [future.result(timeout=1) for future in futures]
        finally:
            worker.close(timeout=1)

        self.assertEqual(factory_calls, ["en"])
        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(results[0].match_id, "match-a")
        self.assertEqual(results[0].video_pts, 10.0)
        self.assertEqual(results[0].kind, "clock")
        self.assertEqual(results[0].profile, "source-a")
        self.assertEqual(results[0].texts, ("44:59",))
        self.assertTrue(all(result.batch_size == 4 for result in results))

    def test_reuses_one_engine_for_later_batches(self):
        factory_calls = 0
        engine = FakeEngine()

        def factory(_language):
            nonlocal factory_calls
            factory_calls += 1
            return engine

        worker = BatchOcrWorker(
            engine_factory=factory,
            max_batch_size=2,
            batch_wait_seconds=0,
            queue_capacity=4,
        )
        try:
            first = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile="source-a",
                crop="10:00",
            ).result(timeout=1)
            second = worker.submit(
                match_id="match-a",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            ).result(timeout=1)
        finally:
            worker.close(timeout=1)

        self.assertEqual(factory_calls, 1)
        self.assertEqual(len(engine.calls), 2)
        self.assertEqual(first.texts, ("10:00",))
        self.assertEqual(second.texts, ("10:01",))

    def test_queue_backpressure_is_structured(self):
        release_factory = threading.Event()

        def blocked_factory(_language):
            release_factory.wait(1)
            return FakeEngine()

        worker = BatchOcrWorker(
            engine_factory=blocked_factory,
            max_batch_size=1,
            queue_capacity=1,
        )
        try:
            first = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile="source-a",
                crop="10:00",
            )
            with self.assertRaises(WorkerError) as raised:
                worker.submit(
                    match_id="match-b",
                    video_pts=2,
                    kind="score",
                    profile="source-b",
                    crop="1-0",
                )
            self.assertEqual(raised.exception.kind, "ocr_queue_full")
            release_factory.set()
            self.assertEqual(first.result(timeout=1).texts, ("10:00",))
        finally:
            release_factory.set()
            worker.close(timeout=1)

    def test_batch_failure_isolated_with_request_diagnostics(self):
        class FailingEngine:
            def predict(self, _crops):
                raise RuntimeError("backend failed")

        worker = BatchOcrWorker(
            engine_factory=lambda _language: FailingEngine(),
            max_batch_size=2,
            batch_wait_seconds=0.1,
            queue_capacity=4,
        )
        try:
            clock = worker.submit(
                match_id="match-a",
                video_pts=42,
                kind="clock",
                profile="source-a",
                crop="bad-clock",
            )
            score = worker.submit(
                match_id="match-b",
                video_pts=99,
                kind="score",
                profile="source-b",
                crop="bad-score",
            )
            for future, match_id, kind in (
                (clock, "match-a", "clock"),
                (score, "match-b", "score"),
            ):
                with self.assertRaises(WorkerError) as raised:
                    future.result(timeout=1)
                self.assertEqual(raised.exception.kind, "ocr_inference_failed")
                self.assertEqual(raised.exception.diagnostics["match_id"], match_id)
                self.assertEqual(raised.exception.diagnostics["kind"], kind)
        finally:
            worker.close(timeout=1)

    def test_malformed_batch_result_does_not_kill_worker(self):
        calls = 0

        def recognizer(_engine, crops, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return [None for _crop in crops]
            return [([str(crop)], [0.9]) for crop in crops]

        worker = BatchOcrWorker(
            engine_factory=lambda _language: object(),
            batch_recognizer=recognizer,
            max_batch_size=1,
            batch_wait_seconds=0,
            queue_capacity=2,
        )
        try:
            malformed = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile="source-a",
                crop="bad",
            )
            with self.assertRaises(WorkerError) as raised:
                malformed.result(timeout=1)
            self.assertEqual(raised.exception.kind, "ocr_inference_failed")

            recovered = worker.submit(
                match_id="match-a",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            ).result(timeout=1)
            self.assertEqual(recovered.texts, ("10:01",))
            self.assertTrue(worker.is_alive)
        finally:
            worker.close(timeout=1)

    def test_accepts_profile_object_and_returns_profile_id(self):
        class Profile:
            profile_id = "source-profile"

        worker = BatchOcrWorker(
            engine_factory=lambda _language: FakeEngine(),
            max_batch_size=1,
            batch_wait_seconds=0,
            queue_capacity=1,
        )
        try:
            result = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile=Profile(),
                crop="10:00",
            ).result(timeout=1)
        finally:
            worker.close(timeout=1)

        self.assertEqual(result.profile, "source-profile")
        self.assertEqual(result.profile_id, "source-profile")
        self.assertEqual(result.as_dict()["profile_id"], "source-profile")

    def test_close_cancels_queued_work_and_rejects_new_submissions(self):
        release_factory = threading.Event()
        worker = BatchOcrWorker(
            engine_factory=lambda _language: (
                release_factory.wait(1) or FakeEngine()
            ),
            max_batch_size=1,
            queue_capacity=2,
        )
        pending = worker.submit(
            match_id="match-a",
            video_pts=1,
            kind="clock",
            profile="source-a",
            crop="10:00",
        )

        self.assertFalse(
            worker.close(wait=False, cancel_pending=True)
        )
        with self.assertRaises(WorkerError) as cancelled:
            pending.result(timeout=1)
        self.assertEqual(cancelled.exception.kind, "ocr_worker_closed")
        with self.assertRaises(WorkerError) as closed:
            worker.submit(
                match_id="match-a",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            )
        self.assertEqual(closed.exception.kind, "ocr_worker_closed")
        release_factory.set()
        self.assertTrue(worker.close(timeout=1))

    def test_engine_initialization_failure_is_terminal(self):
        def unavailable(_language):
            raise WorkerError("ocr_model_unavailable", "missing model")

        worker = BatchOcrWorker(engine_factory=unavailable)
        try:
            with self.assertRaises(WorkerError) as ready:
                worker.wait_until_ready(timeout=1)
            self.assertEqual(ready.exception.kind, "ocr_model_unavailable")
            with self.assertRaises(WorkerError) as submit:
                worker.submit(
                    match_id="match-a",
                    video_pts=1,
                    kind="clock",
                    profile="source-a",
                    crop="10:00",
                )
            self.assertEqual(submit.exception.kind, "ocr_model_unavailable")
        finally:
            worker.close(timeout=1)


class PersistentSocketTests(unittest.TestCase):
    @staticmethod
    def _wait_for_socket(socket_path: Path) -> None:
        deadline = time.monotonic() + 2.0
        while not socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not socket_path.exists():
            raise AssertionError("socket server did not start")

    @staticmethod
    def _exchange(socket_path: Path, request: dict) -> dict:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(3.0)
        connection.connect(str(socket_path))
        with connection, connection.makefile("rwb") as stream:
            stream.write(json.dumps(request).encode("utf-8") + b"\n")
            stream.flush()
            return json.loads(stream.readline().decode("utf-8"))

    def test_real_socket_batches_crops_from_concurrent_matches(self):
        engine = FakeEngine()
        factory_calls: list[str] = []
        rendezvous = threading.Barrier(2)

        def engine_factory(language):
            factory_calls.append(language)
            return engine

        def execute(request, *, batch_worker, request_timeout_seconds):
            rendezvous.wait(timeout=1)
            future = batch_worker.submit(
                match_id=request["match_id"],
                video_pts=request["video_pts"],
                kind=request["kind"],
                profile=request["profile"],
                crop=request["crop"],
            )
            result = future.result(timeout=request_timeout_seconds)
            return {"ok": True, "result": result.as_dict()}, 0

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            server = threading.Thread(
                target=serve_socket,
                args=(socket_path,),
                kwargs={
                    "engine_factory": engine_factory,
                    "request_executor": execute,
                },
            )
            server.start()
            self._wait_for_socket(socket_path)
            responses: list[dict] = []

            def send(request):
                responses.append(self._exchange(socket_path, request))

            clients = [
                threading.Thread(
                    target=send,
                    args=(
                        {
                            "match_id": match_id,
                            "video_pts": 10.0,
                            "kind": kind,
                            "profile": "source-a",
                            "crop": crop,
                            "_request_timeout_seconds": 2.0,
                        },
                    ),
                )
                for match_id, kind, crop in (
                    ("match-a", "clock", "44:59"),
                    ("match-b", "score", "1-0"),
                )
            ]
            for client in clients:
                client.start()
            for client in clients:
                client.join(timeout=2)
            self._exchange(socket_path, {"command": "shutdown"})
            server.join(timeout=2)

        self.assertFalse(server.is_alive())
        self.assertEqual(factory_calls, ["en"])
        self.assertEqual(len(responses), 2)
        self.assertTrue(all(response["ok"] for response in responses))
        self.assertEqual({response["result"]["batch_size"] for response in responses}, {2})
        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(set(engine.calls[0]), {"44:59", "1-0"})

    def test_disconnected_client_does_not_stop_real_socket_server(self):
        abandoned_started = threading.Event()
        release_abandoned = threading.Event()

        def execute(request, **_kwargs):
            if request.get("id") == "abandoned":
                abandoned_started.set()
                release_abandoned.wait(1)
            return {"ok": True, "result": {"id": request.get("id")}}, 0

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            server = threading.Thread(
                target=serve_socket,
                args=(socket_path,),
                kwargs={
                    "engine_factory": lambda _language: FakeEngine(),
                    "request_executor": execute,
                },
            )
            server.start()
            self._wait_for_socket(socket_path)

            abandoned = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            abandoned.connect(str(socket_path))
            abandoned.sendall(b'{"id":"abandoned"}\n')
            abandoned.close()
            self.assertTrue(abandoned_started.wait(1))
            release_abandoned.set()
            time.sleep(0.05)

            response = self._exchange(socket_path, {"id": "healthy"})
            self.assertEqual(response, {"ok": True, "result": {"id": "healthy"}})
            self.assertTrue(server.is_alive())
            self._exchange(socket_path, {"command": "shutdown"})
            server.join(timeout=2)

        self.assertFalse(server.is_alive())

    def test_backend_timeout_restarts_generation_and_requeues_once(self):
        release_backend = threading.Event()
        backend_calls = 0
        backend_lock = threading.Lock()

        def blocked_recognizer(_engine, crops, **_kwargs):
            nonlocal backend_calls
            with backend_lock:
                backend_calls += 1
                call = backend_calls
            if call == 1:
                release_backend.wait(2)
            return [([str(crop)], [0.9]) for crop in crops]

        def execute(request, *, batch_worker, request_timeout_seconds):
            try:
                recognized, _failed = _recognize_paths_shared(
                    batch_worker,
                    [Path("clock.png")],
                    kinds=["clock"],
                    match_id="match-a",
                    profile_id="source-a",
                    candidate_start_seconds=0,
                    sample_interval=1,
                    minimum_confidence=0,
                    deadline_monotonic=time.monotonic() + request_timeout_seconds,
                )
            except WorkerError as exc:
                return {"ok": False, "error": exc.as_dict()}, 2
            return {
                "ok": True,
                "result": {
                    "texts": recognized[0][0],
                    "diagnostics": {},
                },
            }, 0

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            server = threading.Thread(
                target=serve_socket,
                args=(socket_path,),
                kwargs={
                    "engine_factory": lambda _language: object(),
                    "batch_recognizer": blocked_recognizer,
                    "request_executor": execute,
                },
            )
            server.start()
            self._wait_for_socket(socket_path)
            response = self._exchange(
                socket_path,
                {"id": "slow", "_request_timeout_seconds": 0.8},
            )
            healthy = self._exchange(
                socket_path,
                {"id": "healthy", "_request_timeout_seconds": 0.8},
            )
            self._exchange(socket_path, {"command": "shutdown"})
            release_backend.set()
            server.join(timeout=2)

        self.assertTrue(response["ok"])
        restart = response["result"]["diagnostics"]
        self.assertTrue(restart["ocr_backend_restarted"])
        self.assertEqual(restart["backend_generation_before"], 1)
        self.assertEqual(restart["backend_generation_after"], 2)
        self.assertEqual(restart["ocr_backend_restart_retry_count"], 1)
        self.assertTrue(healthy["ok"])
        self.assertFalse(server.is_alive())

    def test_ffprobe_timeout_is_structured(self):
        with patch(
            "scoreboard_ocr_worker.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["ffprobe"], 0.1),
        ):
            with self.assertRaises(WorkerError) as raised:
                probe_video_dimensions(
                    Path("candidate.mp4"),
                    ffmpeg="ffmpeg",
                    timeout_seconds=0.1,
                )

        self.assertEqual(raised.exception.kind, "inference_timeout")
        self.assertEqual(raised.exception.diagnostics["stage"], "ffprobe")

    def test_ffmpeg_timeout_is_structured(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "scoreboard_ocr_worker.subprocess.run",
                side_effect=subprocess.TimeoutExpired(["ffmpeg"], 0.1),
            ):
                with self.assertRaises(WorkerError) as raised:
                    extract_scoreboard_frames(
                        Path("candidate.mp4"),
                        Path(directory),
                        ffmpeg="ffmpeg",
                        sample_interval_seconds=1,
                        roi_width_ratio=0.5,
                        roi_height_ratio=0.25,
                        timeout_seconds=0.1,
                    )

        self.assertEqual(raised.exception.kind, "inference_timeout")
        self.assertEqual(
            raised.exception.diagnostics["stage"],
            "frame_extraction",
        )

    def test_empty_readiness_probe_does_not_stop_server(self):
        writes: list[bytes] = []

        class FakeStream:
            def __init__(self, line: bytes) -> None:
                self.line = line

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def readline(self, _limit: int) -> bytes:
                return self.line

            def write(self, value: bytes) -> None:
                writes.append(value)

            def flush(self) -> None:
                pass

        class FakeConnection:
            def __init__(self, line: bytes) -> None:
                self.stream = FakeStream(line)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def settimeout(self, _timeout: float) -> None:
                pass

            def makefile(self, _mode: str) -> FakeStream:
                return self.stream

        class FakeServer:
            def __init__(self) -> None:
                self.connections = iter(
                    [
                        FakeConnection(b""),
                        FakeConnection(b'{"command":"shutdown"}\n'),
                    ]
                )

            def bind(self, _path: str) -> None:
                pass

            def listen(self, _backlog: int) -> None:
                pass

            def settimeout(self, _timeout: float) -> None:
                pass

            def accept(self):
                return next(self.connections), None

            def close(self) -> None:
                pass

        fake_server = FakeServer()
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            with patch(
                "scoreboard_ocr_worker.socket.socket", return_value=fake_server
            ), patch("scoreboard_ocr_worker.os.chmod"):
                result = serve_socket(socket_path)

        self.assertEqual(result, 0)
        self.assertEqual(
            [json.loads(value.decode("utf-8")) for value in writes],
            [{"ok": True, "result": {"shutdown": True}}],
        )


if __name__ == "__main__":
    unittest.main()
