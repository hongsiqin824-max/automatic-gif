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
    OCR_FFMPEG_WATCHDOG_SECONDS,
    VisionJob,
    VisualLocationFailed,
    _continuous_search_components,
    _encode_ocr_api_range_fallback,
    _locate_across_search_components,
    _locate_ocr_window_across_components,
    _normalized_ocr_clip_window,
    _ocr_deadline_policy,
    _ocr_target_media_availability,
    _ocr_target_wait_failure,
    _ocr_progressive_clock_mapping,
    _ocr_progressive_mapped_target_window,
    _ocr_readable_mapped_target_scan_window,
    _ocr_has_scannable_media_after_cursor,
    _ocr_match_clock_readiness,
    _ocr_progressive_merge_clock_samples,
    _latest_trusted_clock_seconds,
    _ocr_progressive_state,
    _ocr_progressive_target_rescan_window,
    _ocr_localization_contract,
    _ocr_progressive_target_seconds,
    _ocr_progressive_coverage_diagnostics,
    _ocr_output_shape,
    _vision_failure_result,
    locate_with_ocr_fallback,
    materialize_analysis_clip,
    process_vision_artifact,
    refine_event_job,
    ensure_ocr_target_revision,
)


class VisionRuntimeTests(unittest.TestCase):
    def test_progressive_coverage_diagnostics_classifies_media_states(self):
        with tempfile.TemporaryDirectory() as directory:
            segment_path = Path(directory) / "segment.ts"
            segment_path.write_bytes(b"video")
            segment = Segment(segment_path, 100.0, 160.0)
            history = _ocr_progressive_coverage_diagnostics(
                [segment],
                intended_initial_start=80.0,
                requested_start=80.0,
                requested_end=170.0,
                scan_start=80.0,
                scan_end=120.0,
                target_clock_seconds=60,
                latest_trusted_clock_seconds=60,
                target_window={
                    "start_stream_time": 70.0,
                    "end_stream_time": 90.0,
                },
            )
            self.assertEqual(history["coverage_class"], "history_unavailable")
            self.assertGreater(history["history_missing_seconds"], 0.0)

            stalled = _ocr_progressive_coverage_diagnostics(
                [segment],
                intended_initial_start=100.0,
                requested_start=100.0,
                requested_end=200.0,
                scan_start=160.0,
                scan_end=190.0,
                target_clock_seconds=180,
                latest_trusted_clock_seconds=120,
                previous_media_end_stream_time=160.0,
            )
            self.assertEqual(stalled["coverage_class"], "media_stalled")
            self.assertTrue(stalled["media_stalled"])

            unreadable = _ocr_progressive_coverage_diagnostics(
                [segment],
                intended_initial_start=100.0,
                requested_start=100.0,
                requested_end=150.0,
                scan_start=100.0,
                scan_end=150.0,
                target_clock_seconds=180,
                latest_trusted_clock_seconds=None,
            )
            self.assertEqual(unreadable["coverage_class"], "clock_unreadable")

    def test_clock_mapping_requires_two_progressing_samples(self):
        self.assertEqual(
            _ocr_progressive_clock_mapping(
                [{"stream_time": 100.0, "match_clock_seconds": 50}]
            )["status"],
            "insufficient_samples",
        )
        self.assertNotEqual(
            _ocr_progressive_clock_mapping(
                [
                    {"stream_time": 100.0, "match_clock_seconds": 50},
                    {"stream_time": 100.0, "match_clock_seconds": 51},
                ]
            )["status"],
            "ready",
        )

    def test_clock_samples_persist_event_phase_and_mapping_rejects_mixed_halves(self):
        first_half = _ocr_progressive_merge_clock_samples(
            [],
            [
                {"stream_time": 100.0, "match_clock_seconds": 600},
                {"stream_time": 110.0, "match_clock_seconds": 610},
            ],
            default_phase="first_half",
        )
        self.assertEqual(
            [sample["clock_phase"] for sample in first_half],
            ["first_half", "first_half"],
        )

        mixed = first_half + [
            {
                "stream_time": 120.0,
                "match_clock_seconds": 3010,
                "clock_phase": "second_half",
            }
        ]
        mapping = _ocr_progressive_clock_mapping(mixed)
        self.assertEqual(mapping["status"], "clock_phase_mismatch")
        self.assertEqual(
            mapping["clock_periods"], ["first_half", "second_half"]
        )

    def test_clock_mapping_predicts_focused_target_window(self):
        samples = [
            {"stream_time": 100.0, "match_clock_seconds": 50},
            {"stream_time": 120.0, "match_clock_seconds": 70},
        ]
        mapping = _ocr_progressive_clock_mapping(samples)
        self.assertEqual(mapping["status"], "ready")
        self.assertEqual(mapping["stream_time_per_match_second"], 1.0)
        window = _ocr_progressive_mapped_target_window(
            samples, target_clock_seconds=60, margin_seconds=15.0
        )
        self.assertEqual(window["method"], "persistent_clock_video_interpolation")
        self.assertEqual(window["estimated_stream_time"], 110.0)
        self.assertEqual(window["start_stream_time"], 95.0)
        self.assertEqual(window["end_stream_time"], 125.0)

    def test_target_media_availability_proves_first_half_target_precedes_recording(self):
        availability = _ocr_target_media_availability(
            [
                {"stream_time": 190.0, "match_clock_seconds": 3870},
                {"stream_time": 200.0, "match_clock_seconds": 3880},
            ],
            target_clock_seconds=2700,
            earliest_retained_stream_time=80.0,
        )

        self.assertEqual(availability["status"], "before_recording")
        self.assertEqual(availability["halftime_adjustment_seconds"], 900.0)
        self.assertLess(availability["target_window_end_stream_time"], 0.0)

    def test_target_media_availability_distinguishes_unavailable_history(self):
        availability = _ocr_target_media_availability(
            [
                {"stream_time": 190.0, "match_clock_seconds": 190},
                {"stream_time": 200.0, "match_clock_seconds": 200},
            ],
            target_clock_seconds=100,
            earliest_retained_stream_time=150.0,
        )

        self.assertEqual(availability["status"], "history_unavailable")
        self.assertGreaterEqual(availability["estimated_stream_time"], 0.0)
        self.assertLessEqual(
            availability["target_window_end_stream_time"],
            availability["earliest_retained_stream_time"],
        )

    def test_clock_mapping_rejects_a_paused_match_clock(self):
        samples = [
            {"stream_time": 100.0, "match_clock_seconds": 600},
            {"stream_time": 106.0, "match_clock_seconds": 600},
        ]
        mapping = _ocr_progressive_clock_mapping(samples)

        self.assertEqual(mapping["status"], "rejected_pause")
        self.assertEqual(
            mapping["reason"], "match_clock_paused_or_video_continued"
        )
        self.assertIsNone(
            _ocr_progressive_mapped_target_window(
                samples, target_clock_seconds=600
            )
        )

    def test_clock_mapping_rejects_regression_and_bounds_extrapolation(self):
        regression = _ocr_progressive_clock_mapping(
            [
                {"stream_time": 100.0, "match_clock_seconds": 610},
                {"stream_time": 110.0, "match_clock_seconds": 600},
            ]
        )
        self.assertEqual(regression["status"], "rejected_jump")

        samples = [
            {"stream_time": 100.0, "match_clock_seconds": 600},
            {"stream_time": 110.0, "match_clock_seconds": 610},
        ]
        self.assertIsNotNone(
            _ocr_progressive_mapped_target_window(samples, target_clock_seconds=620)
        )
        self.assertIsNotNone(
            _ocr_progressive_mapped_target_window(samples, target_clock_seconds=900)
        )
        self.assertIsNone(
            _ocr_progressive_mapped_target_window(samples, target_clock_seconds=911)
        )

    def test_clock_mapping_does_not_extrapolate_across_halftime(self):
        samples = [
            {"stream_time": 100.0, "match_clock_seconds": 2600},
            {"stream_time": 110.0, "match_clock_seconds": 2610},
        ]

        self.assertIsNone(
            _ocr_progressive_mapped_target_window(
                samples, target_clock_seconds=2800
            )
        )

    def test_mapped_target_scan_requires_one_component_covering_center(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before = root / "before.ts"
            after = root / "after.ts"
            before.write_bytes(b"video")
            after.write_bytes(b"video")
            target_window = {
                "start_stream_time": 90.0,
                "end_stream_time": 150.0,
                "estimated_stream_time": 120.0,
            }

            self.assertIsNone(
                _ocr_readable_mapped_target_scan_window(
                    [
                        Segment(before, 90.0, 119.0),
                        Segment(after, 121.0, 150.0),
                    ],
                    target_window,
                )
            )
            self.assertIsNone(
                _ocr_readable_mapped_target_scan_window(
                    [Segment(before, 90.0, 119.0)],
                    target_window,
                )
            )
            self.assertIsNone(
                _ocr_readable_mapped_target_scan_window(
                    [Segment(before, 90.0, 120.0)],
                    target_window,
                )
            )
            self.assertEqual(
                _ocr_readable_mapped_target_scan_window(
                    [Segment(before, 90.0, 120.5)],
                    target_window,
                ),
                (90.0, 120.5),
            )

    def test_clock_samples_merge_only_trusted_readings(self):
        merged = _ocr_progressive_merge_clock_samples(
            [{"stream_time": 100.0, "match_clock_seconds": 50}],
            {
                "candidate_start_seconds": 100.0,
                "clock_raw_observations": [
                    {
                        "frame_seconds": 20.0,
                        "effective_clock_seconds": 70,
                        "continuity_status": "accepted",
                    },
                    {
                        "frame_seconds": 30.0,
                        "effective_clock_seconds": 80,
                        "continuity_status": "rejected",
                    },
                ],
            },
        )
        self.assertEqual(
            merged,
            [
                {"stream_time": 100.0, "match_clock_seconds": 50},
                {"stream_time": 120.0, "match_clock_seconds": 70},
            ],
        )

    def test_progressive_target_rescan_window_uses_clock_diagnostics(self):
        window = _ocr_progressive_target_rescan_window(
            {
                "candidate_start_seconds": 80.0,
                "clock_raw_observations": [
                    {
                        "frame_seconds": 35.0,
                        "effective_clock_seconds": 115,
                        "continuity_status": "accepted",
                    },
                    {
                        "frame_seconds": 45.0,
                        "effective_clock_seconds": 125,
                        "continuity_status": "accepted",
                    }
                ]
            },
            target_clock_seconds=120,
        )
        self.assertEqual(window["method"], "two_sided_clock_interpolation")
        self.assertEqual(window["start_stream_time"], 105.0)
        self.assertEqual(window["end_stream_time"], 135.0)
        self.assertEqual(window["margin_seconds"], 15.0)
        self.assertEqual(window["sample_interval_seconds"], 1.0)
        self.assertEqual(window["scan_mode"], "target_centered_rescan")

    def test_single_clock_beyond_target_cannot_start_target_rescan(self):
        window = _ocr_progressive_target_rescan_window(
            {
                "candidate_start_seconds": 80.0,
                "clock_raw_observations": [
                    {
                        "frame_seconds": 45.0,
                        "effective_clock_seconds": 8294,
                        "continuity_status": "accepted",
                    }
                ],
            },
            target_clock_seconds=120,
        )
        self.assertIsNone(window)

    def test_latest_clock_uses_stream_order_and_ignores_nested_summaries(self):
        diagnostics = {
            "target_clock_seconds": 9999,
            "unrelated": {
                "clock_raw_observations": [
                    {
                        "frame_seconds": 999.0,
                        "effective_clock_seconds": 9999,
                        "continuity_status": "accepted",
                    }
                ]
            },
            "candidate_start_seconds": 100.0,
            "clock_raw_observations": [
                {
                    "frame_seconds": 20.0,
                    "effective_clock_seconds": 70,
                    "continuity_status": "accepted",
                },
                {
                    "frame_seconds": 10.0,
                    "effective_clock_seconds": 60,
                    "continuity_status": "accepted",
                },
            ],
        }
        self.assertEqual(_latest_trusted_clock_seconds(diagnostics), 70)

    def test_latest_clock_rejects_repaired_rejected_and_ambiguous_samples(self):
        diagnostics = {
            "candidate_start_seconds": 100.0,
            "clock_raw_observations": [
                {"frame_seconds": 0.0, "effective_clock_seconds": 50, "continuity_status": "accepted"},
                {"frame_seconds": 10.0, "effective_clock_seconds": 60, "continuity_status": "accepted"},
                {"frame_seconds": 20.0, "effective_clock_seconds": 5000, "continuity_status": "repaired"},
                {"frame_seconds": 21.0, "effective_clock_seconds": 5001, "continuity_status": "rejected"},
                {"frame_seconds": 22.0, "effective_clock_seconds": 5002, "continuity_status": "accepted", "ambiguous_clock": True},
            ],
        }
        self.assertEqual(_latest_trusted_clock_seconds(diagnostics), 60)

    def test_merge_drops_anomalous_latest_jump_from_durable_state(self):
        merged = _ocr_progressive_merge_clock_samples(
            [
                {"stream_time": 100.0, "match_clock_seconds": 50},
                {"stream_time": 110.0, "match_clock_seconds": 60},
            ],
            {
                "candidate_start_seconds": 100.0,
                "clock_raw_observations": [
                    {"frame_seconds": 20.0, "effective_clock_seconds": 5000, "continuity_status": "accepted"},
                ],
            },
        )
        self.assertEqual(
            merged,
            [
                {"stream_time": 100.0, "match_clock_seconds": 50},
                {"stream_time": 110.0, "match_clock_seconds": 60},
            ],
        )

    def test_sqlite_restored_latest_is_recomputed_from_valid_samples(self):
        class RestoredTask:
            window_metadata = {
                "progressive_scan": {
                    "latest_trusted_clock_seconds": 5000,
                    "clock_samples": [
                        {"stream_time": 100.0, "match_clock_seconds": 50},
                        {"stream_time": 110.0, "match_clock_seconds": 60},
                        {"stream_time": 120.0, "match_clock_seconds": 5000},
                    ],
                }
            }

        restored = _ocr_progressive_state(RestoredTask())
        self.assertEqual(restored["latest_trusted_clock_seconds"], 60)
        self.assertEqual(restored["clock_state_recovery"]["stored_latest_trusted_clock_seconds"], 5000)

    def test_sqlite_restored_latest_without_samples_is_cleared(self):
        class LegacyTask:
            window_metadata = {
                "progressive_scan": {
                    "latest_trusted_clock_seconds": 8294,
                }
            }

        restored = _ocr_progressive_state(LegacyTask())
        self.assertIsNone(restored["latest_trusted_clock_seconds"])
        self.assertEqual(restored["clock_samples"], [])

    def test_ocr_localization_contract_exposes_evidence_grade(self):
        self.assertEqual(
            _ocr_localization_contract(
                {
                    "location_kind": "match_clock_second",
                    "method": "paddleocr_exact_clock",
                    "precision": "observed_second",
                }
            ),
            ("exact", "observed_second"),
        )
        self.assertEqual(
            _ocr_localization_contract(
                {
                    "location_kind": "match_clock_second",
                    "method": "paddleocr_interpolated_clock",
                    "precision": "interpolated_second",
                }
            ),
            ("interpolated", "interpolated_second"),
        )
        self.assertEqual(
            _ocr_localization_contract(
                {
                    "location_kind": "match_clock_second",
                    "method": "paddleocr_near_neighbor_estimate",
                    "precision": "estimated_second",
                }
            ),
            ("estimated", "estimated_second"),
        )
        self.assertEqual(
            _ocr_localization_contract(
                {
                    "location_kind": "match_clock_second",
                    "method": "paddleocr_stable_clock_mapping",
                    "precision": "projected_second",
                }
            ),
            ("projected", "projected_second"),
        )
        self.assertEqual(
            _ocr_localization_contract(
                {
                    "location_kind": "match_clock_minute_boundary",
                    "precision": "minute_boundary",
                }
            ),
            ("minute_boundary", "minute_boundary"),
        )
        self.assertEqual(
            _ocr_localization_contract({"location_kind": "unknown"}),
            ("failed", "unverified"),
        )

    def test_progressive_ocr_prefers_exact_second_over_minute_boundary(self):
        job = VisionJob(
            "match:G:late-second",
            "match",
            "G",
            "goal",
            100.0,
            None,
            time.time(),
            event_minute="90",
            event_second=5353,
        )

        self.assertEqual(_ocr_progressive_target_seconds(job), 5353)

    def test_progressive_ocr_uses_minute_when_exact_second_is_missing(self):
        job = VisionJob(
            "match:G:minute-only",
            "match",
            "G",
            "goal",
            100.0,
            None,
            time.time(),
            event_minute="90",
            event_second=None,
        )

        self.assertEqual(_ocr_progressive_target_seconds(job), 5400)

    def test_late_target_revision_rewinds_persisted_ocr_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "revision")
            runtime.store.record_vision_readiness_wait(
                job.event_key,
                "old target pending",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "target_revision": 0,
                        "scan_cursor_stream_time": 250.0,
                        "latest_trusted_clock_seconds": 5400,
                    }
                },
                now=1000.0,
                next_attempt_at_unix=1000.0,
            )
            job.target_revision = 1
            job.target_source = "shotmap"
            job.event_second = 5353

            updated = ensure_ocr_target_revision(runtime, job, now_unix=1001.0)

            self.assertEqual(updated.status, "pending")
            progress = updated.window_metadata["progressive_scan"]
            self.assertEqual(progress["target_revision"], 1)
            self.assertEqual(progress["target_source"], "shotmap")
            self.assertIsNone(progress["scan_cursor_stream_time"])
            self.assertIsNone(progress["latest_trusted_clock_seconds"])
            runtime.close()

    def test_continuous_trusted_clock_samples_establish_a_target_mapping(self):
        samples = _ocr_progressive_merge_clock_samples(
            [],
            {
                "candidate_start_seconds": 100.0,
                "clock_raw_observations": [
                    {
                        "frame_seconds": 0.0,
                        "effective_clock_seconds": 600,
                        "continuity_status": "accepted",
                    },
                    {
                        "frame_seconds": 12.0,
                        "effective_clock_seconds": 612,
                        "continuity_status": "accepted",
                    },
                    {
                        "frame_seconds": 18.0,
                        "effective_clock_seconds": 660,
                        "continuity_status": "rejected",
                    },
                ],
            },
        )

        self.assertEqual(
            samples,
            [
                {"stream_time": 100.0, "match_clock_seconds": 600},
                {"stream_time": 112.0, "match_clock_seconds": 612},
            ],
        )
        mapping = _ocr_progressive_clock_mapping(samples)
        self.assertIsNotNone(mapping)
        self.assertEqual(mapping["status"], "ready")
        self.assertEqual(mapping["valid_interval_count"], 1)
        self.assertEqual(mapping["stream_time_per_match_second"], 1.0)
        window = _ocr_progressive_mapped_target_window(
            samples,
            target_clock_seconds=630,
        )
        self.assertEqual(window["method"], "persistent_clock_video_extrapolation")
        self.assertEqual(window["estimated_stream_time"], 130.0)
        self.assertEqual(window["start_stream_time"], 100.0)
        self.assertEqual(window["end_stream_time"], 160.0)

    def test_future_mapped_target_waits_without_starting_ocr(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "mapped-future-wait",
                event_second=120,
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertIsNotNone(task)
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persisted OCR samples",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 120.0, "match_clock_seconds": 110},
                        ],
                        "latest_trusted_clock_seconds": 110,
                    }
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components"
                ) as locate,
                patch(
                    "vision_runtime.time.time",
                    return_value=task.created_at_unix + 1.0,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 110.0)],
                        root,
                    )
                )

            locate.assert_not_called()
            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(waiting.status, "pending")
            self.assertEqual(waiting.last_error_kind, "waiting_for_target_media")
            progress = waiting.window_metadata["progressive_scan"]
            self.assertEqual(
                progress["clock_mapping"]["stream_time_per_match_second"], 1.0
            )
            self.assertEqual(progress["last_scan_start_stream_time"], 100.0)
            self.assertEqual(progress["last_scan_end_stream_time"], 160.0)
            runtime.close()

    def test_mapped_target_arrival_scans_only_predicted_plus_minus_thirty_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "mapped-target-arrived",
                event_second=120,
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertIsNotNone(task)
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persisted OCR samples",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 120.0, "match_clock_seconds": 110},
                        ],
                        "latest_trusted_clock_seconds": 110,
                    }
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target was not readable in its focused window",
                {},
            )
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch(
                    "vision_runtime.time.time",
                    return_value=task.created_at_unix + 1.0,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 240.0)],
                        root,
                    )
                )

            self.assertEqual(locate.call_count, 1)
            self.assertEqual(locate.call_args.kwargs["window_start"], 100.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 160.0)
            self.assertIsNone(
                locate.call_args.kwargs["coarse_sample_interval_seconds"]
            )
            runtime.close()

    def test_mapped_target_center_gap_fails_at_deadline_without_ocr_loop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "mapped-center-gap",
                event_second=120,
                deadline_at_unix=time.time() - 1.0,
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persisted OCR samples",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 120.0, "match_clock_seconds": 110},
                        ],
                        "latest_trusted_clock_seconds": 110,
                    }
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            before = root / "before-center.ts"
            after = root / "after-center.ts"
            before.write_bytes(b"video")
            after.write_bytes(b"video")
            with patch(
                "vision_runtime._locate_ocr_window_across_components"
            ) as locate:
                self.assertTrue(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [
                            Segment(before, 80.0, 129.0),
                            Segment(after, 131.0, 180.0),
                        ],
                        root,
                    )
                )

            locate.assert_not_called()
            failed = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(failed.status, "failed")
            self.assertEqual(failed.last_error_kind, "buffer_gap")
            runtime.close()

    def test_post_scan_mapping_schedules_target_window_without_tail_growth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "post-scan-mapped-target",
                event_second=120,
                deadline_at_unix=time.time() - 1.0,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 200.0)]
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target is ahead of the forward scan",
                {
                    "candidate_start_seconds": 80.0,
                    "clock_raw_observations": [
                        {
                            "frame_seconds": 0.0,
                            "clock": "00:50",
                            "continuity_status": "accepted",
                        },
                        {
                            "frame_seconds": 10.0,
                            "clock": "01:00",
                            "continuity_status": "accepted",
                        },
                    ],
                },
            )
            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=miss,
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(
                        job, runtime, lambda: segments, root
                    )
                )
                waiting = runtime.store.get_vision_task(
                    job.event_key, "ocr_window"
                )
                self.assertTrue(
                    waiting.window_metadata["progressive_scan"]
                    ["last_scan_diagnostics"]
                    ["predicted_target_media_ready"]
                )
                self.assertFalse(
                    waiting.window_metadata["progressive_scan"]
                    ["last_scan_diagnostics"]
                    ["media_tail_grew_during_scan"]
                )

                self.assertTrue(
                    self._run_progressive_ocr(
                        job, runtime, lambda: segments, root
                    )
                )

            self.assertEqual(locate.call_count, 2)
            self.assertEqual(locate.call_args.kwargs["window_start"], 120.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 180.0)
            runtime.close()

    def test_post_scan_fully_missing_target_history_skips_tail_rescan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "post-scan-target-history-missing",
                event_second=10,
                deadline_at_unix=time.time() - 1.0,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segment_sets = iter(
                (
                    [Segment(segment_path, 80.0, 120.0)],
                    [Segment(segment_path, 80.0, 190.0)],
                )
            )
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target predates the retained video",
                {
                    "candidate_start_seconds": 80.0,
                    "clock_raw_observations": [
                        {
                            "frame_seconds": 0.0,
                            "clock": "01:40",
                            "continuity_status": "accepted",
                        },
                        {
                            "frame_seconds": 10.0,
                            "clock": "01:50",
                            "continuity_status": "accepted",
                        },
                    ],
                },
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            timestamps = [task.created_at_unix, task.created_at_unix + 10.0]

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ),
                patch(
                    "vision_runtime.time.time",
                    side_effect=lambda: timestamps.pop(0)
                    if timestamps
                    else task.created_at_unix + 10.0,
                ),
            ):
                self.assertTrue(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: next(segment_sets),
                        root,
                    )
                )

            failed = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(failed.status, "failed")
            self.assertEqual(failed.last_error_kind, "ocr_search_history_evicted")
            coverage = failed.result["coverage_diagnostics"]
            self.assertTrue(coverage["target_history_fully_missing"])
            runtime.close()

    def test_post_scan_unscanned_tail_recovers_paused_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "post-scan-pause-recovery", event_second=120
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist paused mapped state",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "scan_cursor_stream_time": 200.0,
                        "latest_trusted_clock_seconds": 110,
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 120.0, "match_clock_seconds": 110},
                        ],
                        "deadline_policy": {
                            "policy_version": 2,
                            "event_hard_limit_seconds": 300.0,
                            "target_deadline_at_unix": created + 60.0,
                            "hard_deadline_at_unix": created + 300.0,
                            "last_clock_progress_at_unix": created,
                            "target_wait_terminal_reason": (
                                "ocr_clock_paused_timeout"
                            ),
                        },
                    }
                },
                now=created,
                next_attempt_at_unix=created,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segment_sets = iter(
                (
                    [Segment(segment_path, 80.0, 150.0)],
                    [Segment(segment_path, 80.0, 220.0)],
                )
            )
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "mapped target was temporarily unreadable",
                {},
            )
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch(
                    "vision_runtime.time.time",
                    return_value=created + 70.0,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: next(segment_sets),
                        root,
                    )
                )

            locate.assert_called_once()
            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            policy = waiting.window_metadata["progressive_scan"]["deadline_policy"]
            self.assertTrue(policy["has_scannable_media_after_cursor"])
            self.assertFalse(policy["clock_paused"])
            self.assertNotIn("target_wait_terminal_reason", policy)
            self.assertEqual(
                policy["terminal_wait_recovered_reason"],
                "ocr_clock_paused_timeout",
            )
            self.assertGreater(policy["target_deadline_at_unix"], created + 70.0)
            runtime.close()

    def test_late_second_target_reuses_persisted_mapping_without_touching_default_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "late-mapped-revision",
                event_second=None,
            )
            runtime.transition(job.event_key, "encoding")
            runtime.transition(
                job.event_key,
                "encoded",
                result={"output": str(root / "default.gif"), "source": "default"},
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertIsNotNone(task)
            runtime.record_vision_readiness_wait(
                job.event_key,
                "minute target scanned",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "target_revision": 0,
                        "scan_cursor_stream_time": 250.0,
                        "latest_trusted_clock_seconds": 110,
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 120.0, "match_clock_seconds": 110},
                        ],
                    }
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            job.event_second = 110
            job.target_revision = 1
            job.target_source = "shotmap"
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found", "focused retry missed", {}
            )
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch(
                    "vision_runtime.time.time",
                    return_value=task.created_at_unix + 1.0,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 160.0)],
                        root,
                    )
                )

            self.assertEqual(locate.call_args.kwargs["window_start"], 90.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 150.0)
            visual = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(visual.window_metadata["progressive_scan"]["target_revision"], 1)
            self.assertEqual(
                visual.window_metadata["progressive_scan"]["clock_mapping"][
                    "stream_time_per_match_second"
                ],
                1.0,
            )
            default_task = runtime.store.get(job.event_key)
            self.assertEqual(default_task.status, "encoded")
            self.assertEqual(default_task.result["source"], "default")
            runtime.close()

    def test_invalid_clock_mapping_falls_back_to_tail_readiness_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "mapping-invalid-fallback",
                event_second=120,
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertIsNotNone(task)
            runtime.record_vision_readiness_wait(
                job.event_key,
                "inconsistent OCR samples",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "scan_cursor_stream_time": 150.0,
                        "latest_trusted_clock_seconds": 110,
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 101.0, "match_clock_seconds": 110},
                        ],
                    }
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found", "fallback scan missed", {}
            )
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch(
                    "vision_runtime.time.time",
                    return_value=task.created_at_unix + 1.0,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 200.0)],
                        root,
                    )
                )

            self.assertEqual(locate.call_args.kwargs["window_start"], 185.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 200.0)
            visual = runtime.store.get_vision_task(job.event_key, "ocr_window")
            mapping = visual.window_metadata["progressive_scan"]["clock_mapping"]
            self.assertEqual(mapping["status"], "rejected_jump")
            self.assertEqual(mapping["reason"], "clock_stream_discontinuity")
            runtime.close()

    @staticmethod
    def _add_progressive_ocr_job(
        runtime: PipelineRuntime,
        suffix: str,
        *,
        observed_stream_time: float = 200.0,
        event_minute: str = "2",
        event_second: int | None = 120,
        search_start_stream_time: float = 80.0,
        deadline_at_unix: float | None = None,
    ) -> VisionJob:
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
        return VisionJob(
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
        job = VisionRuntimeTests._add_progressive_ocr_job(
            runtime,
            suffix,
            observed_stream_time=observed_stream_time,
            event_minute=event_minute,
            event_second=event_second,
            search_start_stream_time=search_start_stream_time,
            deadline_at_unix=deadline_at_unix,
        )
        return runtime, job

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

    def test_ocr_failure_generates_complete_unverified_120_second_range(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "range-fallback")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            failure = VisualLocationFailed(
                "ocr_clock_target_not_located",
                "target clock was hidden",
                {"stage": "ocr_progressive_scan"},
            )

            with patch(
                "vision_runtime.encode_gif",
                return_value={
                    "output": str(root / "range.gif"),
                    "bytes": 123,
                    "duration_sec": 120.0,
                },
            ) as encode:
                generated = _encode_ocr_api_range_fallback(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 80.0, 200.0)],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    failure=failure,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    timeout_seconds=300.0,
                    min_degraded_seconds=2.0,
                    cancel_event=None,
                )

            self.assertTrue(generated)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertEqual(encoded.result["output_kind"], "api_time_range_fallback")
            self.assertFalse(encoded.result["ocr_verified"])
            self.assertTrue(encoded.result["degraded"])
            self.assertTrue(encoded.result["fallback_complete"])
            self.assertEqual(encoded.result["fallback_label"], "120_second_fallback")
            self.assertEqual(encoded.result["requested_fallback_seconds"], 120.0)
            self.assertEqual(encoded.result["available_fallback_seconds"], 120.0)
            self.assertEqual(encode.call_args.kwargs["before"], 120.0)
            self.assertEqual(encode.call_args.kwargs["after"], 0.0)
            runtime.close()

    def test_ocr_failure_labels_short_range_as_fragmented_and_unverified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "short-range-fallback"
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            failure = VisualLocationFailed(
                "ocr_no_trustworthy_clock_before_deadline",
                "clock was unreadable",
                {"stage": "ocr_progressive_scan"},
            )

            with patch(
                "vision_runtime.encode_gif",
                return_value={
                    "output": str(root / "short-range.gif"),
                    "bytes": 45,
                    "duration_sec": 7.0,
                },
            ):
                generated = _encode_ocr_api_range_fallback(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 193.0, 200.0)],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    failure=failure,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    timeout_seconds=300.0,
                    min_degraded_seconds=2.0,
                    cancel_event=None,
                )

            self.assertTrue(generated)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertFalse(encoded.result["fallback_complete"])
            self.assertTrue(encoded.result["fragmented_fallback"])
            self.assertEqual(encoded.result["fallback_label"], "fragmented_clip")
            self.assertEqual(encoded.result["available_fallback_seconds"], 7.0)
            self.assertIn("可能不包含事件", encoded.result["fallback_explanation"])
            runtime.close()

    def test_ocr_failure_counts_only_playable_seconds_across_video_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "gapped-range-fallback"
            )
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"video")
            second.write_bytes(b"video")
            failure = VisualLocationFailed(
                "ocr_clock_unreadable",
                "clock was unreadable",
                {"stage": "ocr_progressive_scan"},
            )

            with patch(
                "vision_runtime.encode_gif",
                return_value={
                    "output": str(root / "gapped-range.gif"),
                    "bytes": 45,
                    "duration_sec": 80.0,
                },
            ):
                generated = _encode_ocr_api_range_fallback(
                    job,
                    runtime,
                    lambda: [
                        Segment(first, 80.0, 120.0),
                        Segment(second, 160.0, 200.0),
                    ],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    failure=failure,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    timeout_seconds=300.0,
                    min_degraded_seconds=2.0,
                    cancel_event=None,
                )

            self.assertTrue(generated)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertFalse(encoded.result["fallback_complete"])
            self.assertEqual(encoded.result["available_fallback_seconds"], 80.0)
            self.assertEqual(encoded.result["skipped_gap_seconds"], 40.0)
            self.assertIn("直播源中断部分已跳过", encoded.result["fallback_explanation"])
            runtime.close()

    def test_ocr_failure_does_not_call_nearest_video_a_complete_range(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "history-missing-range-fallback"
            )
            segment_path = root / "late-segment.ts"
            segment_path.write_bytes(b"video")
            failure = VisualLocationFailed(
                "ocr_target_history_evicted",
                "event history was removed",
                {"stage": "ocr_progressive_scan"},
            )

            with patch(
                "vision_runtime.encode_gif",
                return_value={
                    "output": str(root / "nearest-range.gif"),
                    "bytes": 45,
                    "duration_sec": 120.0,
                },
            ):
                generated = _encode_ocr_api_range_fallback(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 220.0, 340.0)],
                    "ffmpeg",
                    "ffprobe",
                    root,
                    failure=failure,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    timeout_seconds=300.0,
                    min_degraded_seconds=2.0,
                    cancel_event=None,
                )

            self.assertTrue(generated)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertFalse(encoded.result["fallback_complete"])
            self.assertTrue(encoded.result["fallback_anchor_history_missing"])
            self.assertEqual(
                encoded.result["fallback_label"], "history_missing_nearest_clip"
            )
            self.assertIn("历史视频已被清理", encoded.result["fallback_explanation"])
            runtime.close()

    def test_terminal_ocr_localization_failure_invokes_api_range_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "fallback-integration",
                event_minute="91",
                event_second=None,
            )

            with patch(
                "vision_runtime._encode_ocr_api_range_fallback",
                return_value=True,
            ) as fallback:
                completed = self._run_progressive_ocr(
                    job,
                    runtime,
                    lambda: [],
                    root,
                )

            self.assertTrue(completed)
            fallback.assert_called_once()
            self.assertEqual(
                fallback.call_args.kwargs["failure"].kind,
                "unsupported_extra_time_or_penalties_v1",
            )
            runtime.close()

    def test_target_before_recording_does_not_generate_unrelated_range_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "before-recording-no-fallback",
                event_minute="45",
                event_second=2700,
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            runtime.record_vision_readiness_wait(
                job.event_key,
                "second-half clock mapping ready",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {"stream_time": 190.0, "match_clock_seconds": 3870},
                            {"stream_time": 200.0, "match_clock_seconds": 3880},
                        ],
                    }
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")

            with (
                patch("vision_runtime._encode_ocr_api_range_fallback") as fallback,
                patch("vision_runtime._locate_ocr_window_across_components") as locate,
            ):
                completed = self._run_progressive_ocr(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 0.0, 220.0)],
                    root,
                )

            self.assertTrue(completed)
            fallback.assert_not_called()
            locate.assert_not_called()
            failed = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(failed.status, "failed")
            self.assertEqual(failed.last_error_kind, "ocr_target_before_recording")
            self.assertEqual(
                failed.result["target_media_availability"]["status"],
                "before_recording",
            )
            runtime.close()

    def test_process_vision_artifact_advances_only_requested_artifact(self):
        with (
            patch("vision_runtime._process_ocr_window", return_value=True) as ocr,
            patch("vision_runtime._process_tdeed_refined") as tdeed,
        ):
            completed = process_vision_artifact(
                object(),
                object(),
                lambda: [],
                "ffmpeg",
                "ffprobe",
                Path("/tmp"),
                artifact_kind="ocr_window",
                search_before=300.0,
                search_after=30.0,
                refined_before=8.0,
                refined_after=12.0,
                width=768,
                fps=16.0,
                colors=256,
                size_reference_bytes=10_000_000,
                python=Path("python"),
                timeout_seconds=60.0,
            )

        self.assertTrue(completed)
        ocr.assert_called_once()
        tdeed.assert_not_called()

    def test_progressive_ocr_persists_cursor_and_resumes_with_overlap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "progressive")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 220.0)]
            misses = [
                VisualLocationFailed(
                    "ocr_exact_second_not_found",
                    "target clock not reached",
                    {
                        "clock_raw_observations": [
                            {"clock": "01:30", "continuity_status": "accepted"}
                        ]
                    },
                ),
                VisualLocationFailed(
                    "ocr_exact_second_not_found",
                    "target clock still not reached",
                    {
                        "clock_raw_observations": [
                            {"clock": "01:50", "continuity_status": "accepted"}
                        ]
                    },
                ),
            ]

            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=misses,
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )
                first = runtime.store.get_vision_task(job.event_key, "ocr_window")
                first_progress = first.window_metadata["progressive_scan"]
                self.assertEqual(first.last_error_kind, "waiting_for_clock_target")
                self.assertEqual(first_progress["last_scan_start_stream_time"], 205.0)
                self.assertEqual(first_progress["scan_cursor_stream_time"], 220.0)
                self.assertEqual(first_progress["latest_trusted_clock_seconds"], 90)
                self.assertEqual(first_progress["scan_attempt_count"], 1)

                runtime.close()
                runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
                runtime.recover_incomplete_vision("match", "ocr_window")
                segments[:] = [Segment(segment_path, 80.0, 240.0)]
                retry_at = runtime.store.get_vision_task(
                    job.event_key, "ocr_window"
                ).next_attempt_at_unix
                with patch("vision_runtime.time.time", return_value=retry_at + 0.1):
                    self.assertFalse(
                        self._run_progressive_ocr(job, runtime, lambda: segments, root)
                    )

            second = runtime.store.get_vision_task(job.event_key, "ocr_window")
            second_progress = second.window_metadata["progressive_scan"]
            self.assertEqual(locate.call_args.kwargs["window_start"], 225.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 240.0)
            self.assertEqual(second_progress["latest_trusted_clock_seconds"], 110)
            self.assertEqual(second_progress["scan_attempt_count"], 2)
            runtime.close()

    def test_clock_readiness_probes_tail_and_waits_for_fifteen_seconds_growth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "readiness-tail")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 220.0)]
            miss = VisualLocationFailed(
                "ocr_clock_unreadable",
                "no trustworthy clock in the newest media",
                {},
            )

            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=[miss, miss],
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )
                first = runtime.store.get_vision_task(job.event_key, "ocr_window")
                first_progress = first.window_metadata["progressive_scan"]
                self.assertEqual(locate.call_args.kwargs["window_start"], 205.0)
                self.assertEqual(locate.call_args.kwargs["window_end"], 220.0)
                self.assertEqual(
                    first_progress["clock_readiness"][
                        "last_probe_media_end_stream_time"
                    ],
                    220.0,
                )
                self.assertEqual(
                    first_progress["clock_readiness"]["status"], "waiting"
                )

                segments[:] = [Segment(segment_path, 80.0, 234.9)]
                with patch(
                    "vision_runtime.time.time",
                    return_value=first.next_attempt_at_unix + 0.1,
                ):
                    self.assertFalse(
                        self._run_progressive_ocr(
                            job, runtime, lambda: segments, root
                        )
                    )
                self.assertEqual(locate.call_count, 1)
                waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
                self.assertEqual(
                    waiting.last_error_kind, "waiting_for_clock_readiness"
                )

                segments[:] = [Segment(segment_path, 80.0, 235.0)]
                with patch(
                    "vision_runtime.time.time",
                    return_value=waiting.next_attempt_at_unix + 0.1,
                ):
                    self.assertFalse(
                        self._run_progressive_ocr(
                            job, runtime, lambda: segments, root
                        )
                    )

            self.assertEqual(locate.call_count, 2)
            self.assertEqual(locate.call_args.kwargs["window_start"], 220.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 235.0)
            runtime.close()

    def test_clock_readiness_requires_two_progressing_readings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "readiness-two-samples",
                event_second=120,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 220.0)]
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock is ahead of the newest media",
                {
                    "candidate_start_seconds": 205.0,
                    "clock_readable_rate": 0.8,
                    "auto_clock": {
                        "clock_roi": [288, 129, 423, 186],
                        "frame_resolution": [1920, 1080],
                    },
                    "clock_raw_observations": [
                        {
                            "frame_seconds": 5.0,
                            "effective_clock_seconds": 90,
                            "continuity_status": "accepted",
                        },
                        {
                            "frame_seconds": 10.0,
                            "effective_clock_seconds": 95,
                            "continuity_status": "accepted",
                        },
                    ],
                },
            )

            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=miss,
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )

            self.assertEqual(locate.call_count, 1)
            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            readiness = waiting.window_metadata["progressive_scan"][
                "clock_readiness"
            ]
            self.assertEqual(readiness["status"], "ready")
            self.assertEqual(readiness["accepted_sample_count"], 2)
            self.assertEqual(readiness["last_probe_media_end_stream_time"], 220.0)
            cached = runtime.store.get_scoreboard_roi_cache(job.match_id)
            self.assertIsNotNone(cached)
            self.assertEqual(cached.profile["clock_roi"], [288, 129, 423, 186])
            runtime.close()

    def test_clock_readiness_shares_no_clock_probe_tail_with_same_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, source_job = self._create_progressive_ocr_job(
                root, "readiness-wait-source"
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 220.0)]
            miss = VisualLocationFailed(
                "ocr_clock_unreadable",
                "no trustworthy clock in the newest media",
                {},
            )
            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=miss,
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        source_job, runtime, lambda: segments, root
                    )
                )

            consumer_job = self._add_progressive_ocr_job(
                runtime, "readiness-wait-consumer"
            )
            with patch(
                "vision_runtime._locate_ocr_window_across_components"
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(
                        consumer_job, runtime, lambda: segments, root
                    )
                )

            locate.assert_not_called()
            consumer = runtime.store.get_vision_task(
                consumer_job.event_key, "ocr_window"
            )
            self.assertEqual(
                consumer.last_error_kind, "waiting_for_clock_readiness"
            )
            readiness = consumer.window_metadata["progressive_scan"][
                "clock_readiness"
            ]
            self.assertEqual(readiness["status"], "waiting")
            self.assertTrue(readiness["reused_from_match"])
            self.assertEqual(
                readiness["source_event_key"], source_job.event_key
            )
            self.assertEqual(
                readiness["last_probe_media_end_stream_time"], 220.0
            )
            runtime.close()

    def test_clock_readiness_reuses_ready_mapping_from_same_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, source_job = self._create_progressive_ocr_job(
                root,
                "readiness-source",
                event_second=120,
            )
            source = runtime.store.get_vision_task(
                source_job.event_key, "ocr_window"
            )
            runtime.record_vision_readiness_wait(
                source_job.event_key,
                "clock readiness established",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {
                                "stream_time": 190.0,
                                "match_clock_seconds": 90,
                                "clock_phase": "first_half",
                            },
                            {
                                "stream_time": 200.0,
                                "match_clock_seconds": 100,
                                "clock_phase": "first_half",
                            },
                        ],
                        "clock_readiness": {
                            "status": "ready",
                            "clock_phase": "first_half",
                            "clock_period": "first_half",
                        },
                    }
                },
                now=source.created_at_unix,
                next_attempt_at_unix=source.created_at_unix,
            )

            consumer_job = self._add_progressive_ocr_job(
                runtime,
                "readiness-consumer",
                event_second=120,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")

            with patch(
                "vision_runtime._locate_ocr_window_across_components"
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(
                        consumer_job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 200.0)],
                        root,
                    )
                )

            locate.assert_not_called()
            consumer = runtime.store.get_vision_task(
                consumer_job.event_key, "ocr_window"
            )
            self.assertEqual(
                consumer.last_error_kind, "waiting_for_target_media"
            )
            progress = consumer.window_metadata["progressive_scan"]
            self.assertEqual(progress["last_scan_start_stream_time"], 190.0)
            self.assertEqual(progress["last_scan_end_stream_time"], 250.0)
            self.assertEqual(len(progress["clock_samples"]), 2)
            readiness = progress["clock_readiness"]
            self.assertEqual(readiness["status"], "ready")
            self.assertEqual(readiness["source_event_key"], source_job.event_key)
            runtime.close()

    def test_clock_readiness_does_not_share_legacy_or_other_half_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, source_job = self._create_progressive_ocr_job(
                root,
                "phase-isolated-source",
                event_minute="2",
                event_second=120,
            )
            source = runtime.store.get_vision_task(
                source_job.event_key, "ocr_window"
            )
            runtime.record_vision_readiness_wait(
                source_job.event_key,
                "legacy phase-less mapping",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {"stream_time": 190.0, "match_clock_seconds": 90},
                            {"stream_time": 200.0, "match_clock_seconds": 100},
                        ],
                        "clock_readiness": {"status": "ready"},
                    }
                },
                now=source.created_at_unix,
                next_attempt_at_unix=source.created_at_unix,
            )

            legacy_consumer = self._add_progressive_ocr_job(
                runtime,
                "legacy-phase-consumer",
                event_minute="3",
                event_second=180,
            )
            samples, readiness = _ocr_match_clock_readiness(
                runtime, legacy_consumer, {}
            )
            self.assertEqual(samples, [])
            self.assertEqual(readiness["status"], "waiting")
            self.assertFalse(readiness.get("reused_from_match", False))

            phased_state = {
                "clock_samples": [
                    {
                        "stream_time": 190.0,
                        "match_clock_seconds": 90,
                        "clock_phase": "first_half",
                    },
                    {
                        "stream_time": 200.0,
                        "match_clock_seconds": 100,
                        "clock_phase": "first_half",
                    },
                ],
                "clock_readiness": {
                    "status": "ready",
                    "clock_phase": "first_half",
                    "clock_period": "first_half",
                },
            }
            runtime.record_vision_readiness_wait(
                source_job.event_key,
                "phase-aware mapping",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={"progressive_scan": phased_state},
                now=source.created_at_unix + 1.0,
                next_attempt_at_unix=source.created_at_unix + 1.0,
            )

            second_half_consumer = self._add_progressive_ocr_job(
                runtime,
                "second-half-consumer",
                event_minute="50",
                event_second=3000,
            )
            samples, readiness = _ocr_match_clock_readiness(
                runtime, second_half_consumer, {}
            )
            self.assertEqual(samples, [])
            self.assertEqual(readiness["clock_period"], "second_half")
            self.assertFalse(readiness.get("reused_from_match", False))

            stoppage_consumer = self._add_progressive_ocr_job(
                runtime,
                "first-half-stoppage-consumer",
                event_minute="45+2",
                event_second=None,
            )
            samples, readiness = _ocr_match_clock_readiness(
                runtime, stoppage_consumer, {}
            )
            self.assertEqual(len(samples), 2)
            self.assertEqual(readiness["clock_period"], "first_half")
            self.assertTrue(readiness["reused_from_match"])
            runtime.close()

    def test_progressive_ocr_rechecks_crossed_target_before_new_tail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "target-rescan")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 220.0)]
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock was unreadable after the boundary",
                {
                    "candidate_start_seconds": 80.0,
                    "clock_raw_observations": [
                        {
                            "frame_seconds": 35.0,
                            "effective_clock_seconds": 115,
                            "continuity_status": "accepted",
                        },
                        {
                            "frame_seconds": 45.0,
                            "effective_clock_seconds": 125,
                            "continuity_status": "accepted",
                        }
                    ]
                },
            )
            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=[miss, miss, miss, miss],
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )
                waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
                segments[:] = [Segment(segment_path, 80.0, 240.0)]
                with patch(
                    "vision_runtime.time.time",
                    return_value=waiting.next_attempt_at_unix + 0.1,
                ):
                    self.assertFalse(
                        self._run_progressive_ocr(
                            job, runtime, lambda: segments, root
                        )
                    )

                waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
                progress = waiting.window_metadata["progressive_scan"]
                self.assertEqual(progress["target_rescan_attempt_count"], 1)
                self.assertEqual(progress["target_rescan_window"]["margin_seconds"], 30.0)
                with patch(
                    "vision_runtime.time.time",
                    return_value=waiting.next_attempt_at_unix + 0.1,
                ):
                    self.assertFalse(
                        self._run_progressive_ocr(
                            job, runtime, lambda: segments, root
                        )
                    )

                waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
                with patch(
                    "vision_runtime.time.time",
                    return_value=waiting.next_attempt_at_unix + 0.1,
                ):
                    self.assertFalse(
                        self._run_progressive_ocr(
                            job, runtime, lambda: segments, root
                        )
                    )

            self.assertEqual(locate.call_count, 4)
            self.assertEqual(
                locate.call_args_list[0].kwargs["coarse_sample_interval_seconds"],
                10.0,
            )
            self.assertEqual(locate.call_args_list[1].kwargs["sample_interval_seconds"], 1.0)
            self.assertEqual(locate.call_args_list[2].kwargs["sample_interval_seconds"], 1.0)
            self.assertEqual(locate.call_args_list[3].kwargs["sample_interval_seconds"], 1.0)
            self.assertIsNone(
                locate.call_args_list[1].kwargs["coarse_sample_interval_seconds"]
            )
            self.assertIsNone(
                locate.call_args_list[2].kwargs["coarse_sample_interval_seconds"]
            )
            self.assertIsNone(
                locate.call_args_list[3].kwargs["coarse_sample_interval_seconds"]
            )
            self.assertEqual(locate.call_args_list[1].kwargs["window_start"], 105.0)
            self.assertEqual(locate.call_args_list[1].kwargs["window_end"], 135.0)
            self.assertEqual(locate.call_args_list[2].kwargs["window_start"], 90.0)
            self.assertEqual(locate.call_args_list[2].kwargs["window_end"], 150.0)
            self.assertEqual(locate.call_args_list[3].kwargs["window_start"], 90.0)
            self.assertEqual(locate.call_args_list[3].kwargs["window_end"], 150.0)
            progress = runtime.store.get_vision_task(
                job.event_key, "ocr_window"
            ).window_metadata["progressive_scan"]
            self.assertIsNotNone(progress.get("target_rescan_completed_at_unix"))
            self.assertTrue(progress.get("target_rescan_exhausted"))
            self.assertEqual(progress.get("target_rescan_attempt_count"), 3)
            self.assertEqual(progress["scan_cursor_stream_time"], 220.0)
            runtime.close()

    def test_target_rescan_attempt_counter_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "target-rescan-restart")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 240.0)]
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock was unreadable after the boundary",
                {
                    "candidate_start_seconds": 80.0,
                    "clock_raw_observations": [
                        {
                            "frame_seconds": 35.0,
                            "effective_clock_seconds": 115,
                            "continuity_status": "accepted",
                        },
                        {
                            "frame_seconds": 45.0,
                            "effective_clock_seconds": 125,
                            "continuity_status": "accepted",
                        }
                    ],
                },
            )
            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=[miss, miss, miss, miss],
            ) as locate:
                self.assertFalse(self._run_progressive_ocr(job, runtime, lambda: segments, root))
                first = runtime.store.get_vision_task(job.event_key, "ocr_window")
                self.assertEqual(
                    first.window_metadata["progressive_scan"]["target_rescan_attempt_count"],
                    0,
                )
                runtime.close()

                runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
                runtime.recover_incomplete_vision("match", "ocr_window")
                retry_at = runtime.store.get_vision_task(job.event_key, "ocr_window").next_attempt_at_unix
                with patch("vision_runtime.time.time", return_value=retry_at + 0.1):
                    self.assertFalse(self._run_progressive_ocr(job, runtime, lambda: segments, root))
                second = runtime.store.get_vision_task(job.event_key, "ocr_window")
                second_progress = second.window_metadata["progressive_scan"]
                self.assertEqual(second_progress["target_rescan_attempt_count"], 1)
                self.assertEqual(second_progress["target_rescan_window"]["margin_seconds"], 30.0)
                runtime.close()

                runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
                runtime.recover_incomplete_vision("match", "ocr_window")
                retry_at = runtime.store.get_vision_task(job.event_key, "ocr_window").next_attempt_at_unix
                with patch("vision_runtime.time.time", return_value=retry_at + 0.1):
                    self.assertFalse(self._run_progressive_ocr(job, runtime, lambda: segments, root))

                runtime.close()
                runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
                runtime.recover_incomplete_vision("match", "ocr_window")
                retry_at = runtime.store.get_vision_task(job.event_key, "ocr_window").next_attempt_at_unix
                with patch("vision_runtime.time.time", return_value=retry_at + 0.1):
                    self.assertFalse(self._run_progressive_ocr(job, runtime, lambda: segments, root))

            final = runtime.store.get_vision_task(job.event_key, "ocr_window")
            final_progress = final.window_metadata["progressive_scan"]
            self.assertEqual(final_progress["target_rescan_attempt_count"], 3)
            self.assertTrue(final_progress["target_rescan_exhausted"])
            self.assertEqual(locate.call_count, 4)
            runtime.close()

    def test_clock_mapping_waits_for_future_target_media_without_locator(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "mapping-wait")
            job.event_second = 150
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            runtime.store.record_vision_readiness_wait(
                job.event_key,
                "prior OCR observations",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "clock_samples": [
                            {"stream_time": 180.0, "match_clock_seconds": 80},
                            {"stream_time": 200.0, "match_clock_seconds": 100},
                        ],
                    }
                },
                now=time.time(),
                next_attempt_at_unix=time.time() - 1.0,
            )

            with patch("vision_runtime._locate_ocr_window_across_components") as locate:
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 150.0)],
                        root,
                    )
                )

            locate.assert_not_called()
            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(waiting.last_error_kind, "waiting_for_target_media")
            progress = waiting.window_metadata["progressive_scan"]
            self.assertEqual(progress["state"], "waiting_for_target_media")
            self.assertEqual(progress["last_scan_start_stream_time"], 220.0)
            self.assertEqual(progress["last_scan_end_stream_time"], 280.0)
            runtime.close()

    def test_clock_mapping_rewinds_late_target_to_focused_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "mapping-rewind")
            job.event_second = 60
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            runtime.store.record_vision_readiness_wait(
                job.event_key,
                "prior OCR observations",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "scan_cursor_stream_time": 220.0,
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 50},
                            {"stream_time": 120.0, "match_clock_seconds": 70},
                        ],
                    }
                },
                now=time.time(),
                next_attempt_at_unix=time.time() - 1.0,
            )
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target still unreadable",
                {"clock_raw_observations": []},
            )

            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                side_effect=miss,
            ) as locate:
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 220.0)],
                        root,
                    )
                )

            self.assertEqual(locate.call_args.kwargs["window_start"], 80.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 140.0)
            progress = runtime.store.get_vision_task(
                job.event_key, "ocr_window"
            ).window_metadata["progressive_scan"]
            self.assertEqual(progress["clock_mapping"]["status"], "ready")
            runtime.close()

    def test_target_clock_gap_extends_deadline_and_throttles_far_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "dynamic-wait")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            now_unix = task.created_at_unix + 40.0
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock is still ahead",
                {
                    "clock_raw_observations": [
                        {"clock": "00:30", "continuity_status": "accepted"}
                    ]
                },
            )

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch("vision_runtime.time.time", return_value=now_unix),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 220.0)],
                        root,
                    )
                )

            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            policy = waiting.window_metadata["progressive_scan"]["deadline_policy"]
            self.assertEqual(policy["target_clock_gap_seconds"], 90)
            self.assertAlmostEqual(
                waiting.deadline_at_unix,
                task.created_at_unix + 150.0,
                places=3,
            )
            self.assertAlmostEqual(
                policy["hard_deadline_at_unix"],
                task.created_at_unix + 300.0,
                places=3,
            )
            self.assertAlmostEqual(
                waiting.next_attempt_at_unix,
                now_unix + 30.0,
                places=3,
            )

            with patch("vision_runtime.time.time", return_value=now_unix + 1.0):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 225.0)],
                        root,
                    )
                )
            locate.assert_called_once()
            runtime.close()

    def test_postroll_starts_independent_sixty_second_phase_near_target_hard_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "late-postroll")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            late_now = task.created_at_unix + 175.0
            located = {
                "anchor_stream_time": 200.0,
                "anchor_seconds": 200.0,
                "location_kind": "match_clock_second",
                "method": "paddleocr_exact_clock",
                "precision": "observed_second",
                "localization_quality": "exact",
                "target_clock": "02:00",
                "target_clock_seconds": 120,
                "diagnostics": {},
            }

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        located,
                        {
                            "window_start_stream_time": 80.0,
                            "window_end_stream_time": 215.0,
                        },
                        [str(segment_path)],
                    ),
                ),
                patch("vision_runtime.time.time", return_value=late_now),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 215.0)],
                        root,
                    )
                )

            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            policy = waiting.window_metadata["progressive_scan"]["deadline_policy"]
            self.assertEqual(policy["phase"], "postroll_wait")
            self.assertAlmostEqual(
                policy["postroll_deadline_at_unix"],
                late_now + 35.0,
                places=3,
            )
            self.assertAlmostEqual(
                policy["postroll_hard_deadline_at_unix"],
                late_now + 60.0,
                places=3,
            )
            # The target watchdog is now 300 seconds; this late postroll is
            # covered by the target hard limit instead of extending beyond it.
            self.assertLess(
                policy["postroll_deadline_at_unix"],
                policy["hard_deadline_at_unix"],
            )
            self.assertEqual(waiting.last_error_kind, "waiting_for_postroll")
            runtime.close()

    def test_exact_ocr_anchor_waits_for_postroll_then_encodes_without_relocation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "postroll")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 80.0, 215.0)]
            located = {
                "anchor_stream_time": 200.0,
                "anchor_seconds": 200.0,
                "location_kind": "match_clock_second",
                "method": "paddleocr_exact_clock",
                "precision": "observed_second",
                "localization_quality": "exact",
                "target_clock": "02:00",
                "target_clock_seconds": 120,
                "diagnostics": {},
            }

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        located,
                        {
                            "window_start_stream_time": 80.0,
                            "window_end_stream_time": 215.0,
                        },
                        [str(segment_path)],
                    ),
                ) as locate,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(root / "ocr.gif"),
                        "bytes": 1234,
                        "duration_sec": 60.0,
                    },
                ) as encode,
            ):
                self.assertFalse(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )
                waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
                self.assertEqual(waiting.status, "located")
                self.assertEqual(waiting.last_error_kind, "waiting_for_postroll")
                self.assertEqual(
                    waiting.window_metadata["progressive_scan"]["state"],
                    "waiting_for_postroll",
                )
                self.assertEqual(
                    waiting.window_metadata["progressive_scan"][
                        "requested_output_end_stream_time"
                    ],
                    230.0,
                )
                encode.assert_not_called()

                segments[:] = [Segment(segment_path, 80.0, 240.0)]
                with patch(
                    "vision_runtime.time.time",
                    return_value=waiting.next_attempt_at_unix + 0.1,
                ):
                    self.assertTrue(
                        self._run_progressive_ocr(job, runtime, lambda: segments, root)
                    )

            locate.assert_called_once()
            encode.assert_called_once()
            self.assertEqual(
                (encode.call_args.kwargs["before"], encode.call_args.kwargs["after"]),
                (30.0, 30.0),
            )
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertEqual(encoded.result["anchor_provenance"], "ocr_verified_match_clock")
            self.assertEqual(encoded.result["progressive_status"], "encoded")
            self.assertEqual(
                encoded.window_metadata["progressive_scan"]["state"], "encoded"
            )
            runtime.close()

    def test_ocr_output_stitches_internal_video_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "stitched-output")
            before_gap = root / "before-gap.ts"
            after_gap = root / "after-gap.ts"
            before_gap.write_bytes(b"video")
            after_gap.write_bytes(b"video")
            segments = [
                Segment(before_gap, 170.0, 185.0),
                Segment(after_gap, 190.0, 235.0),
            ]
            located = {
                "anchor_stream_time": 200.0,
                "anchor_seconds": 200.0,
                "location_kind": "match_clock_second",
                "method": "paddleocr_exact_clock",
                "precision": "observed_second",
                "localization_quality": "exact",
                "target_clock": "02:00",
                "target_clock_seconds": 120,
                "diagnostics": {},
            }

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        located,
                        {
                            "window_start_stream_time": 80.0,
                            "window_end_stream_time": 235.0,
                        },
                        [str(before_gap), str(after_gap)],
                    ),
                ),
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(root / "ocr-stitched.gif"),
                        "bytes": 1234,
                        "duration_sec": 55.0,
                    },
                ) as encode,
            ):
                self.assertTrue(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )

            coverage = encode.call_args.kwargs["coverage"]
            self.assertEqual(coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertTrue(coverage.stitched_across_gap)
            self.assertEqual(coverage.coverage_quality, "stitched_across_gap")
            self.assertEqual(coverage.video_gap_count, 1)
            self.assertEqual(coverage.skipped_gap_seconds, 5.0)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertTrue(encoded.result["stitched_across_gap"])
            self.assertTrue(encoded.result["precise_location"])
            self.assertTrue(encoded.result["event_frame_present"])
            runtime.close()

    def test_ocr_output_uses_nearest_boundary_for_small_anchor_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "anchor-gap-output")
            before_gap = root / "before-anchor-gap.ts"
            after_gap = root / "after-anchor-gap.ts"
            before_gap.write_bytes(b"video")
            after_gap.write_bytes(b"video")
            segments = [
                Segment(before_gap, 170.0, 198.0),
                Segment(after_gap, 202.0, 235.0),
            ]
            located = {
                "anchor_stream_time": 200.0,
                "anchor_seconds": 200.0,
                "location_kind": "match_clock_second",
                "method": "paddleocr_exact_clock",
                "precision": "observed_second",
                "localization_quality": "exact",
                "target_clock": "02:00",
                "target_clock_seconds": 120,
                "diagnostics": {},
            }

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        located,
                        {
                            "window_start_stream_time": 80.0,
                            "window_end_stream_time": 235.0,
                        },
                        [str(before_gap), str(after_gap)],
                    ),
                ),
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(root / "ocr-anchor-adjusted.gif"),
                        "bytes": 1234,
                        "duration_sec": 56.0,
                    },
                ) as encode,
            ):
                self.assertTrue(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )

            coverage = encode.call_args.kwargs["coverage"]
            self.assertEqual(coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertTrue(coverage.stitched_across_gap)
            self.assertTrue(coverage.anchor_adjusted)
            self.assertEqual(coverage.anchor_adjusted_to, 198.0)
            self.assertTrue(coverage.event_frame_may_be_missing)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertTrue(encoded.result["approximate"])
            self.assertFalse(encoded.result["precise_location"])
            self.assertFalse(encoded.result["event_frame_present"])
            runtime.close()

    def test_ocr_output_stitches_large_anchor_gap_as_approximate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "large-anchor-gap-output"
            )
            before_gap = root / "before-large-anchor-gap.ts"
            after_gap = root / "after-large-anchor-gap.ts"
            before_gap.write_bytes(b"video")
            after_gap.write_bytes(b"video")
            segments = [
                Segment(before_gap, 170.0, 188.0),
                Segment(after_gap, 212.0, 235.0),
            ]
            located = {
                "anchor_stream_time": 200.0,
                "anchor_seconds": 200.0,
                "location_kind": "match_clock_second",
                "method": "paddleocr_exact_clock",
                "precision": "observed_second",
                "localization_quality": "exact",
                "target_clock": "02:00",
                "target_clock_seconds": 120,
                "diagnostics": {},
            }

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        located,
                        {
                            "window_start_stream_time": 80.0,
                            "window_end_stream_time": 235.0,
                        },
                        [str(before_gap), str(after_gap)],
                    ),
                ),
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(root / "ocr-large-anchor-gap.gif"),
                        "bytes": 1234,
                        "duration_sec": 36.0,
                    },
                ) as encode,
            ):
                self.assertTrue(
                    self._run_progressive_ocr(job, runtime, lambda: segments, root)
                )

            coverage = encode.call_args.kwargs["coverage"]
            self.assertEqual(coverage.status, CoverageStatus.READY_DEGRADED)
            self.assertTrue(coverage.stitched_across_gap)
            self.assertEqual(coverage.skipped_gap_seconds, 24.0)
            self.assertEqual(coverage.anchor_adjusted_to, 188.0)
            encoded = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertTrue(encoded.result["approximate"])
            self.assertFalse(encoded.result["precise_location"])
            self.assertFalse(encoded.result["event_frame_present"])
            runtime.close()

    def test_progressive_ocr_deadline_distinguishes_clock_failures(self):
        cases = (
            (
                "no-trusted-clock",
                80.0,
                {},
                "ocr_no_trustworthy_clock_before_deadline",
            ),
            (
                "target-timeout",
                80.0,
                {
                    "clock_raw_observations": [
                        {"clock": "01:30", "continuity_status": "accepted"}
                    ]
                },
                "ocr_target_media_stalled",
            ),
            (
                "history-evicted",
                100.0,
                {
                    "candidate_start_seconds": 80.0,
                    "clock_raw_observations": [
                        {"frame_seconds": 35.0, "clock": "01:55", "continuity_status": "accepted"},
                        {"frame_seconds": 45.0, "clock": "02:05", "continuity_status": "accepted"},
                    ]
                },
                "waiting_for_target_rescan",
            ),
        )
        for suffix, search_start, diagnostics, expected_kind in cases:
            with self.subTest(expected_kind=expected_kind):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    runtime, job = self._create_progressive_ocr_job(
                        root,
                        suffix,
                        search_start_stream_time=search_start,
                        deadline_at_unix=time.time() - 1.0,
                    )
                    created = runtime.store.get_vision_task(
                        job.event_key,
                        "ocr_window",
                    ).created_at_unix
                    runtime.record_vision_queue_phase(
                        job.event_key,
                        "queued",
                        artifact_kind="ocr_window",
                        now=created,
                    )
                    runtime.record_vision_queue_phase(
                        job.event_key,
                        "acquired",
                        artifact_kind="ocr_window",
                        queued_at_unix=created,
                        now=created + 1.0,
                    )
                    segment_path = root / "segment.ts"
                    segment_path.write_bytes(b"video")
                    with (
                        patch(
                            "vision_runtime._locate_ocr_window_across_components",
                            side_effect=VisualLocationFailed(
                                "ocr_exact_second_not_found",
                                "target clock was not located",
                                diagnostics,
                            ),
                        ),
                        patch(
                            "vision_runtime.time.time",
                            return_value=created + 182.0,
                        ),
                    ):
                        should_retry = expected_kind == "waiting_for_target_rescan"
                        self.assertEqual(
                            self._run_progressive_ocr(
                                job,
                                runtime,
                                lambda: [Segment(segment_path, search_start, 230.0)],
                                root,
                            ),
                            not should_retry,
                        )
                    failed = runtime.store.get_vision_task(
                        job.event_key, "ocr_window"
                    )
                    if should_retry:
                        self.assertEqual(failed.status, "pending")
                        self.assertEqual(
                            failed.last_error_kind, "waiting_for_target_rescan"
                        )
                        self.assertTrue(
                            failed.window_metadata["progressive_scan"]
                            .get("target_passed_without_anchor")
                        )
                    else:
                        self.assertEqual(failed.status, "failed")
                        self.assertEqual(failed.last_error_kind, expected_kind)
                        self.assertTrue(failed.result["default_gif_preserved"])
                    runtime.close()

    def test_ocr_execution_time_does_not_consume_target_wait_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "execution-budget")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            initial_policy = _ocr_deadline_policy(
                task,
                now_unix=created + 10.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=None,
            )
            runtime.record_vision_readiness_wait(
                job.event_key,
                "OCR execution in progress",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "state": "ocr_execution",
                        "execution_started_at_unix": created + 20.0,
                        "deadline_policy": initial_policy,
                    }
                },
                now=created + 20.0,
            )
            executing = runtime.store.get_vision_task(job.event_key, "ocr_window")

            after_execution = _ocr_deadline_policy(
                executing,
                now_unix=created + 140.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=90,
            )

            self.assertAlmostEqual(
                after_execution["ocr_execution_accounted_seconds"],
                120.0,
                places=3,
            )
            self.assertAlmostEqual(
                after_execution["hard_deadline_at_unix"],
                created + 420.0,
                places=3,
            )
            self.assertGreater(
                after_execution["target_deadline_at_unix"],
                created + 140.0,
            )
            runtime.close()

    def test_deadline_policy_ends_wait_after_media_tail_stalls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "media-stall-policy")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            first = _ocr_deadline_policy(
                task,
                now_unix=created,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=None,
                latest_media_end_stream_time=100.0,
            )
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist media baseline",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={"progressive_scan": {"deadline_policy": first}},
                now=created,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            stalled = _ocr_deadline_policy(
                persisted,
                now_unix=created + 60.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=None,
                latest_media_end_stream_time=100.0,
            )
            self.assertTrue(stalled["media_stalled"])
            self.assertEqual(
                stalled["target_wait_terminal_reason"],
                "ocr_target_media_stalled",
            )
            self.assertLessEqual(stalled["target_deadline_at_unix"], created + 60.0)
            runtime.close()

    def test_deadline_policy_ends_wait_after_clock_pause(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "clock-pause-policy")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            first = _ocr_deadline_policy(
                task,
                now_unix=created,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=90,
            )
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist paused clock baseline",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "deadline_policy": first,
                        "latest_trusted_clock_seconds": 90,
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 90},
                            {"stream_time": 110.0, "match_clock_seconds": 90},
                        ],
                    }
                },
                now=created,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            stalled = _ocr_deadline_policy(
                persisted,
                now_unix=created + 60.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=90,
            )
            self.assertTrue(stalled["clock_paused"])
            self.assertEqual(
                stalled["target_wait_terminal_reason"],
                "ocr_clock_paused_timeout",
            )
            self.assertLessEqual(stalled["target_deadline_at_unix"], created + 60.0)
            runtime.close()

    def test_deadline_policy_resumes_paused_wait_for_unscanned_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "pause-resumed")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            paused_policy = {
                "policy_version": 2,
                "event_hard_limit_seconds": 300.0,
                "target_deadline_at_unix": created + 60.0,
                "hard_deadline_at_unix": created + 300.0,
                "last_clock_progress_at_unix": created,
                "target_wait_terminal_reason": "ocr_clock_paused_timeout",
            }
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist paused state",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "deadline_policy": paused_policy,
                        "latest_trusted_clock_seconds": 90,
                        "clock_samples": [
                            {"stream_time": 100.0, "match_clock_seconds": 89},
                            {"stream_time": 101.0, "match_clock_seconds": 90},
                        ],
                    }
                },
                now=created,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            recovered = _ocr_deadline_policy(
                persisted,
                now_unix=created + 70.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=89,
                has_scannable_media_after_cursor=True,
            )

            self.assertFalse(recovered["clock_paused"])
            self.assertNotIn("target_wait_terminal_reason", recovered)
            self.assertEqual(
                recovered["terminal_wait_recovered_reason"],
                "ocr_clock_paused_timeout",
            )
            self.assertGreater(
                recovered["target_deadline_at_unix"], created + 70.0
            )
            # The regressed 89 must not reset the last-progress timestamp.
            self.assertEqual(recovered["last_clock_progress_at_unix"], created)
            runtime.close()

    def test_deadline_policy_does_not_report_media_stall_after_target_passed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "target-passed-policy"
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            passed_samples = [
                {"stream_time": 90.0, "match_clock_seconds": 115},
                {"stream_time": 100.0, "match_clock_seconds": 125},
            ]
            first = _ocr_deadline_policy(
                task,
                now_unix=created,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=125,
                latest_media_end_stream_time=100.0,
                clock_samples=passed_samples,
            )
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist target-passed media baseline",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={"progressive_scan": {
                    "deadline_policy": first,
                    "clock_samples": passed_samples,
                    "latest_trusted_clock_seconds": 125,
                }},
                now=created,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            updated = _ocr_deadline_policy(
                persisted,
                now_unix=created + 60.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=125,
                latest_media_end_stream_time=100.0,
                clock_samples=passed_samples,
            )

            self.assertFalse(updated["media_stalled"])
            self.assertNotIn("target_wait_terminal_reason", updated)
            runtime.close()

    def test_deadline_policy_upgrades_persisted_old_hard_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "hard-limit-upgrade")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            old = {
                "policy_version": 1,
                "event_hard_limit_seconds": 180.0,
                "target_deadline_at_unix": created + 180.0,
                "hard_deadline_at_unix": created + 180.0,
            }
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist old deadline policy",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={"progressive_scan": {"deadline_policy": old}},
                now=created,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            upgraded = _ocr_deadline_policy(
                persisted,
                now_unix=created + 181.0,
                target_clock_seconds=120,
                latest_trusted_clock_seconds=90,
            )
            self.assertEqual(upgraded["policy_version"], 2)
            self.assertEqual(upgraded["event_hard_limit_seconds"], 300.0)
            self.assertAlmostEqual(
                upgraded["hard_deadline_at_unix"], created + 300.0, places=3
            )
            self.assertAlmostEqual(
                upgraded["target_deadline_at_unix"], created + 300.0, places=3
            )
            runtime.close()

    def test_pre_submission_readiness_wait_does_not_expire_ocr_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "readiness-budget")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            queue_wait = 280.0
            execution_started = created + 460.0
            policy = _ocr_deadline_policy(
                task,
                now_unix=created + 1.0,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=None,
            )
            runtime.record_vision_readiness_wait(
                job.event_key,
                "OCR execution started after media readiness",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "queue_timing": {
                        "total_queue_wait_seconds": queue_wait,
                        "acquired_at_unix": execution_started,
                    },
                    "progressive_scan": {
                        "state": "ocr_execution",
                        "execution_started_at_unix": execution_started,
                        "deadline_policy": policy,
                    },
                },
                now=execution_started,
            )
            executing = runtime.store.get_vision_task(job.event_key, "ocr_window")
            updated = _ocr_deadline_policy(
                executing,
                now_unix=execution_started,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=3396,
            )

            self.assertAlmostEqual(
                updated["hard_deadline_at_unix"],
                created + 580.0,
                places=3,
            )
            self.assertLessEqual(updated["target_deadline_at_unix"], execution_started)
            runtime.close()

    def test_deadline_policy_accounts_pre_submission_wait_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "pre-submission-budget"
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            runtime.record_vision_queue_phase(
                job.event_key,
                "queued",
                artifact_kind="ocr_window",
                now=created + 230.0,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "acquired",
                artifact_kind="ocr_window",
                queued_at_unix=created + 230.0,
                now=created + 270.0,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "executing",
                artifact_kind="ocr_window",
                queued_at_unix=created + 230.0,
                now=created + 271.0,
            )
            executing = runtime.store.get_vision_task(
                job.event_key, "ocr_window"
            )

            first = _ocr_deadline_policy(
                executing,
                now_unix=created + 271.0,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=3409,
            )
            self.assertAlmostEqual(
                first["pre_submission_wait_accounted_seconds"],
                231.0,
                places=3,
            )
            self.assertGreater(first["target_deadline_at_unix"], created + 271.0)

            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist deadline policy",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={"progressive_scan": {"deadline_policy": first}},
                now=created + 271.0,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            second = _ocr_deadline_policy(
                persisted,
                now_unix=created + 272.0,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=3410,
            )
            self.assertEqual(
                second["pre_submission_wait_accounted_seconds"],
                first["pre_submission_wait_accounted_seconds"],
            )
            self.assertEqual(
                second["hard_deadline_at_unix"],
                first["hard_deadline_at_unix"],
            )
            runtime.close()

    def test_deadline_policy_accounts_pre_submission_wait_after_readiness_policy(self):
        """A policy created before queue submission is extended once at execution."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root, "pre-submission-after-readiness"
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix

            # The scheduler may persist a readiness policy before the visual
            # worker is submitted.  This used to permanently lose the later
            # pre-submission interval because the policy already had a hard
            # deadline by the time OCR acquired a slot.
            initial = _ocr_deadline_policy(
                task,
                now_unix=created + 1.0,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=None,
            )
            runtime.record_vision_readiness_wait(
                job.event_key,
                "waiting for visual queue",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "deadline_policy": initial,
                    }
                },
                now=created + 1.0,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "queued",
                artifact_kind="ocr_window",
                now=created + 230.0,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "acquired",
                artifact_kind="ocr_window",
                queued_at_unix=created + 230.0,
                now=created + 270.0,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "executing",
                artifact_kind="ocr_window",
                queued_at_unix=created + 230.0,
                now=created + 271.0,
            )

            executing = runtime.store.get_vision_task(job.event_key, "ocr_window")
            updated = _ocr_deadline_policy(
                executing,
                now_unix=created + 271.0,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=3409,
            )
            self.assertAlmostEqual(
                updated["pre_submission_wait_accounted_seconds"],
                231.0,
                places=3,
            )
            # 300s base + 231s pre-submission + 40s actual queue wait.
            self.assertAlmostEqual(
                updated["hard_deadline_at_unix"],
                created + 571.0,
                places=3,
            )

            runtime.record_vision_readiness_wait(
                job.event_key,
                "persist compensated policy",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={"progressive_scan": {"deadline_policy": updated}},
                now=created + 271.0,
            )
            persisted = runtime.store.get_vision_task(job.event_key, "ocr_window")
            retried = _ocr_deadline_policy(
                persisted,
                now_unix=created + 272.0,
                target_clock_seconds=3420,
                latest_trusted_clock_seconds=3410,
            )
            self.assertEqual(
                retried["hard_deadline_at_unix"],
                updated["hard_deadline_at_unix"],
            )
            runtime.close()

    def test_scan_miss_refreshes_media_tail_before_target_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "tail-refresh")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock is still ahead",
                {
                    "clock_raw_observations": [
                        {"clock": "00:56", "continuity_status": "accepted"},
                        {"clock": "00:57", "continuity_status": "accepted"},
                    ]
                },
            )
            segment_sets = iter(
                (
                    [Segment(segment_path, 80.0, 120.0)],
                    [Segment(segment_path, 80.0, 190.0)],
                )
            )
            clock_values = [created + 10.0, created + 140.0]

            def fake_time():
                return clock_values.pop(0) if clock_values else created + 140.0

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ),
                patch(
                    "vision_runtime.time.time",
                    side_effect=fake_time,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: next(segment_sets),
                        root,
                    )
                )
            waiting = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(waiting.status, "pending")
            self.assertEqual(waiting.last_error_kind, "waiting_for_clock_target")
            progress = waiting.window_metadata["progressive_scan"]
            self.assertTrue(
                progress["last_scan_diagnostics"]["media_tail_grew_during_scan"]
            )
            self.assertEqual(progress["latest_media_end_stream_time"], 190.0)
            runtime.close()

    def test_target_wait_failure_reports_stalled_media_and_clock_gap(self):
        kind, message, details = _ocr_target_wait_failure(
            {"latest_media_end_stream_time": 120.0},
            target_clock_seconds=3420,
            latest_trusted_clock_seconds=3396,
            latest_media_end_stream_time=120.0,
        )
        self.assertEqual(kind, "ocr_target_media_stalled")
        self.assertIn("56:36", message)
        self.assertEqual(details["target_clock_gap_seconds"], 24)

    def test_acquired_expired_target_wait_runs_one_final_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "final-scan",
                deadline_at_unix=time.time() - 1.0,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            runtime.record_vision_readiness_wait(
                job.event_key,
                "waiting for new media",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                deadline_at_unix=task.deadline_at_unix,
                window_metadata={
                    "progressive_scan": {
                        "state": "waiting_for_clock_target",
                        "scan_cursor_stream_time": 220.0,
                        "latest_trusted_clock_seconds": 90,
                    },
                },
                now=created,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "queued",
                artifact_kind="ocr_window",
                now=created,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "acquired",
                artifact_kind="ocr_window",
                queued_at_unix=created,
                now=created + 1.0,
            )
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock was not located",
                {
                    "candidate_start_seconds": 80.0,
                    "clock_raw_observations": [
                        {"frame_seconds": 35.0, "clock": "01:55", "continuity_status": "accepted"},
                        {"frame_seconds": 45.0, "clock": "02:05", "continuity_status": "accepted"},
                    ]
                },
            )
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch(
                    "vision_runtime.time.time",
                    return_value=created + 182.0,
                ),
            ):
                self.assertFalse(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 220.0)],
                        root,
                    )
                )

            locate.assert_called_once()
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(task.status, "pending")
            progress = task.window_metadata["progressive_scan"]
            self.assertEqual(progress["target_rescan_window"]["margin_seconds"], 15.0)
            self.assertEqual(progress["target_rescan_attempt_count"], 0)
            runtime.close()

    def test_expired_final_scan_revisits_inferred_target_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "target-final-scan",
                deadline_at_unix=time.time() - 1.0,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            created = task.created_at_unix
            runtime.record_vision_readiness_wait(
                job.event_key,
                "waiting after target retry",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                deadline_at_unix=task.deadline_at_unix,
                window_metadata={
                    "progressive_scan": {
                        "target_revision": 0,
                        "state": "waiting_for_clock_target",
                        "scan_cursor_stream_time": 220.0,
                        "latest_trusted_clock_seconds": 125,
                        "target_rescan_window": {
                            "start_stream_time": 105.0,
                            "end_stream_time": 135.0,
                        },
                        "target_rescan_completed_at_unix": created,
                    },
                },
                now=created,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "queued",
                artifact_kind="ocr_window",
                now=created,
            )
            runtime.record_vision_queue_phase(
                job.event_key,
                "acquired",
                artifact_kind="ocr_window",
                queued_at_unix=created,
                now=created + 1.0,
            )
            miss = VisualLocationFailed(
                "ocr_exact_second_not_found",
                "target clock was not located",
                {
                    "clock_raw_observations": [
                        {"clock": "02:05", "continuity_status": "accepted"}
                    ]
                },
            )
            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=miss,
                ) as locate,
                patch("vision_runtime.time.time", return_value=created + 182.0),
            ):
                self.assertTrue(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 80.0, 220.0)],
                        root,
                    )
                )

            self.assertEqual(locate.call_args.kwargs["window_start"], 105.0)
            self.assertEqual(locate.call_args.kwargs["window_end"], 135.0)
            failed = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertTrue(failed.result["final_scan_was_forced"])
            runtime.close()

    def test_progressive_ocr_preserves_non_scan_failure_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(root, "model-missing")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")

            self.assertTrue(
                self._run_progressive_ocr(
                    job,
                    runtime,
                    lambda: [Segment(segment_path, 80.0, 230.0)],
                    root,
                )
            )

            failed = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(failed.status, "failed")
            self.assertEqual(failed.last_error_kind, "ocr_model_unavailable")
            self.assertEqual(
                failed.result["failure_reason"]["kind"], "ocr_model_unavailable"
            )
            self.assertTrue(failed.result["default_gif_preserved"])
            runtime.close()

    def test_ocr_output_shape_accepts_only_verified_clock_anchors(self):
        job = VisionJob(
            "match:G:shape",
            "match",
            "G",
            "goal",
            100.0,
            None,
            time.time(),
            event_minute="2",
            event_second=120,
            clock_only=True,
        )
        self.assertEqual(
            _ocr_output_shape(job, {"location_kind": "match_clock_second"}),
            (30.0, 30.0, "exact_second"),
        )
        self.assertEqual(
            _ocr_output_shape(
                job, {"location_kind": "match_clock_minute_boundary"}
            ),
            (60.0, 0.0, "minute_boundary"),
        )
        with self.assertRaises(VisualLocationFailed) as raised:
            _ocr_output_shape(job, {"location_kind": "score_transition"})
        self.assertEqual(raised.exception.kind, "ocr_target_localization_failed")

    def test_normalized_ocr_clip_window_rejects_malformed_values(self):
        job = VisionJob(
            "match:G:invalid-window",
            "match",
            "G",
            "goal",
            100.0,
            None,
            time.time(),
            event_minute="2",
            event_second=120,
            clock_only=True,
        )
        for field, value in (
            ("clip_before_seconds", "not-a-number"),
            ("clip_after_seconds", -1),
            ("clip_before_seconds", float("nan")),
        ):
            with self.subTest(field=field, value=value):
                located = {
                    "location_kind": "match_clock_second",
                    "clip_before_seconds": 30.0,
                    "clip_after_seconds": 30.0,
                    field: value,
                }
                with self.assertRaises(VisualLocationFailed) as raised:
                    _normalized_ocr_clip_window(
                        job,
                        located,
                        stage="test_window_validation",
                    )
                self.assertEqual(raised.exception.kind, "ocr_invalid_clip_window")
                self.assertEqual(
                    raised.exception.diagnostics["stage"],
                    "test_window_validation",
                )
                self.assertIn(
                    field,
                    raised.exception.diagnostics["invalid_fields"],
                )

    def test_exact_second_located_restart_recovers_null_window_and_encodes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "restart-exact-null-window",
                observed_stream_time=120.0,
            )
            runtime.transition_vision_task(
                job.event_key,
                "locating",
                artifact_kind="ocr_window",
            )
            runtime.transition_vision_task(
                job.event_key,
                "located",
                artifact_kind="ocr_window",
                result={
                    "anchor_stream_time": 120.0,
                    "localization_source": "exact_second",
                    "location_kind": "match_clock_second",
                    "clip_after_seconds": None,
                    "target_clock_seconds": 120,
                },
            )
            runtime.close()

            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            recovered = runtime.recover_incomplete_vision("match", "ocr_window")
            self.assertEqual(recovered[0].status, "located")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            with patch(
                "vision_runtime.encode_gif",
                return_value={"output": str(root / "exact.gif"), "bytes": 1234},
            ) as encode:
                self.assertTrue(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 0.0, 180.0)],
                        root,
                    )
                )

            self.assertEqual(
                (encode.call_args.kwargs["before"], encode.call_args.kwargs["after"]),
                (30.0, 30.0),
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(task.status, "encoded")
            self.assertEqual(task.result["clip_before_seconds"], 30.0)
            self.assertEqual(task.result["clip_after_seconds"], 30.0)
            runtime.close()

    def test_minute_located_restart_accepts_string_before_and_recovers_zero_after(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "restart-minute-null-window",
                observed_stream_time=120.0,
                event_second=None,
            )
            runtime.transition_vision_task(
                job.event_key,
                "locating",
                artifact_kind="ocr_window",
            )
            runtime.transition_vision_task(
                job.event_key,
                "located",
                artifact_kind="ocr_window",
                result={
                    "anchor_stream_time": 120.0,
                    "localization_source": "minute_boundary",
                    "location_kind": "match_clock_minute_boundary",
                    "clip_before_seconds": "60",
                    "clip_after_seconds": None,
                    "target_clock_seconds": 120,
                },
            )
            runtime.close()

            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            runtime.recover_incomplete_vision("match", "ocr_window")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            with patch(
                "vision_runtime.encode_gif",
                return_value={"output": str(root / "minute.gif"), "bytes": 1234},
            ) as encode:
                self.assertTrue(
                    self._run_progressive_ocr(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 0.0, 180.0)],
                        root,
                    )
                )

            self.assertEqual(
                (encode.call_args.kwargs["before"], encode.call_args.kwargs["after"]),
                (60.0, 0.0),
            )
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(task.status, "encoded")
            self.assertEqual(task.result["clip_before_seconds"], 60.0)
            self.assertEqual(task.result["clip_after_seconds"], 0.0)
            self.assertEqual(
                task.result["requested_media_window"],
                {"start_stream_time": 60.0, "end_stream_time": 120.0},
            )
            runtime.close()

    def test_invalid_persisted_ocr_window_fails_with_structured_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_progressive_ocr_job(
                root,
                "restart-invalid-window",
                observed_stream_time=120.0,
            )
            runtime.transition_vision_task(
                job.event_key,
                "locating",
                artifact_kind="ocr_window",
            )
            runtime.transition_vision_task(
                job.event_key,
                "located",
                artifact_kind="ocr_window",
                result={
                    "anchor_stream_time": 120.0,
                    "localization_source": "exact_second",
                    "location_kind": "match_clock_second",
                    "clip_before_seconds": "invalid",
                    "clip_after_seconds": 30.0,
                },
            )

            with patch("vision_runtime.encode_gif") as encode:
                self.assertTrue(
                    self._run_progressive_ocr(job, runtime, lambda: [], root)
                )

            encode.assert_not_called()
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            self.assertEqual(task.status, "failed")
            self.assertEqual(task.last_error_kind, "ocr_invalid_clip_window")
            self.assertEqual(
                task.result["failure_reason"]["stage"],
                "ocr_output_window_validation",
            )
            self.assertEqual(
                task.result["invalid_fields"],
                {"clip_before_seconds": "invalid"},
            )
            self.assertTrue(task.result["default_gif_preserved"])
            runtime.close()

    def test_tdeed_consumes_encoded_minute_ocr_with_null_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:tdeed-null-ocr-window"
            self._discover_three_path_event(runtime, event_key, second=None)
            job = VisionJob(
                event_key,
                "match",
                "G",
                "goal",
                140.0,
                None,
                time.time(),
                observed_anchor_stream_time=150.0,
                event_minute="2",
                event_second=None,
                target_score="1-0",
                clock_only=True,
            )
            ocr_result = {
                "anchor_stream_time": 120.0,
                "localization_source": "minute_boundary",
                "location_kind": "match_clock_minute_boundary",
                "clip_before_seconds": None,
                "clip_after_seconds": None,
                "output": str(root / "ocr.gif"),
                "bytes": 1234,
            }
            for status in ("locating", "located", "encoding", "encoded"):
                runtime.transition_vision_task(
                    event_key,
                    status,
                    artifact_kind="ocr_window",
                    result=ocr_result,
                )

            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "candidate.mp4"),
                        "window_start_stream_time": 60.0,
                        "window_end_stream_time": 120.0,
                    },
                ) as materialize,
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 100.0,
                        "confidence": 0.9,
                        "label": "Goal",
                    },
                ) as locate,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(root / "ai.gif"), "bytes": 456},
                ),
            ):
                self.assertTrue(
                    process_vision_artifact(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 0.0, 180.0)],
                        "ffmpeg",
                        "ffprobe",
                        root,
                        artifact_kind="tdeed_refined",
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
                    )
                )

            self.assertEqual(materialize.call_args.kwargs["window_start"], 60.0)
            self.assertEqual(materialize.call_args.kwargs["window_end"], 120.0)
            self.assertEqual(
                locate.call_args.kwargs["candidate_window_start_seconds"],
                60.0,
            )
            self.assertEqual(
                locate.call_args.kwargs["candidate_window_end_seconds"],
                120.0,
            )
            refined = runtime.store.get_vision_task(event_key, "tdeed_refined")
            self.assertEqual(refined.status, "encoded")
            self.assertEqual(refined.result["locator_method"], "tdeed_within_ocr_window")
            runtime.close()

    def test_legacy_clock_only_false_reaches_score_ocr_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:G:legacy-clock", "match", "G", "goal", 100.0, None, 1.0,
                observed_anchor_stream_time=100.0,
                event_minute="1", target_score="1-0", clock_only=False,
            )
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "part.mp4"),
                        "window_start_stream_time": 0.0,
                        "window_end_stream_time": 10.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 5.0,
                        "location_kind": "score_transition",
                        "method": "paddleocr_score_transition",
                    },
                ) as ocr,
            ):
                located, _materialized, _paths = _locate_ocr_window_across_components(
                    job,
                    [Segment(segment_path, 0.0, 10.0)],
                    window_start=0.0,
                    window_end=10.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    ocr_python=ocr_python,
                    ocr_timeout_seconds=3.0,
                    minimum_component_seconds=3.0,
                )

            self.assertFalse(ocr.call_args.kwargs["clock_only"])
            self.assertEqual(ocr.call_args.kwargs["target_score"], "1-0")
            self.assertFalse(located["ocr_clock_only"])

    def test_ocr_minute_interval_without_anchor_uses_interval_midpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:minute-interval",
                "match",
                "YC",
                "yellow_card",
                100.0,
                None,
                1.0,
                event_minute="2",
                clock_only=True,
            )
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "part.mp4"),
                        "window_start_stream_time": 0.0,
                        "window_end_stream_time": 30.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": None,
                        "candidate_interval_start_seconds": 10.0,
                        "candidate_interval_end_seconds": 20.0,
                        "requires_tdeed": True,
                        "method": "paddleocr_clock_interval",
                    },
                ),
            ):
                located, _materialized, _paths = _locate_ocr_window_across_components(
                    job,
                    [Segment(segment_path, 0.0, 30.0)],
                    window_start=0.0,
                    window_end=30.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    ocr_python=ocr_python,
                    ocr_timeout_seconds=1.0,
                    minimum_component_seconds=3.0,
                )

            self.assertEqual(located["anchor_stream_time"], 15.0)
            self.assertEqual(located["anchor_seconds"], 15.0)
            self.assertEqual(located["location_kind"], "match_clock_minute_interval")
            self.assertTrue(located["ocr_anchor_from_interval"])

    def test_profile_clock_only_reads_leased_ts_without_materializing_full_mp4(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:direct-ts", "match", "YC", "yellow_card",
                125.0, None, 1.0, event_minute="2", clock_only=True,
                scoreboard_profile={
                    "profile_id": "feed-a",
                    "reference_resolution": [1920, 1080],
                    "clock_roi": [40, 20, 220, 90],
                },
            )
            segments = [Segment(first, 100.0, 110.0), Segment(second, 110.0, 120.0)]
            with (
                patch("vision_runtime.materialize_analysis_clip", side_effect=AssertionError("MP4 path used")),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 115.0,
                        "location_kind": "match_clock_minute_boundary",
                        "method": "paddleocr_clock_boundary",
                    },
                ) as ocr,
            ):
                located, materialized, _paths = _locate_ocr_window_across_components(
                    job, segments, window_start=105.0, window_end=120.0,
                    analysis_path=root / "candidate.mp4", ffmpeg="ffmpeg",
                    ocr_python=ocr_python, ocr_timeout_seconds=5.0,
                    minimum_component_seconds=3.0,
                )

            self.assertEqual(located["anchor_stream_time"], 115.0)
            self.assertTrue(materialized["direct_clock_roi"])
            self.assertEqual(ocr.call_args.kwargs["candidate_start_seconds"], 105.0)
            self.assertEqual(ocr.call_args.kwargs["candidate_seek_seconds"], 5.0)
            self.assertEqual(ocr.call_args.kwargs["candidate_duration_seconds"], 15.0)
            self.assertEqual(list(root.glob("*.ffconcat")), [])

    def test_target_rescan_passes_dense_sampling_to_clock_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "segment.ts"
            segment.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:dense-rescan", "match", "YC", "yellow_card",
                10.0, None, 1.0, event_minute="1", clock_only=True,
            )
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "part.mp4"),
                        "window_start_stream_time": 0.0,
                        "window_end_stream_time": 10.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 8.0,
                        "location_kind": "match_clock_minute_boundary",
                        "method": "paddleocr_minute_boundary",
                        "precision": "minute_boundary",
                    },
                ) as locate,
            ):
                _locate_ocr_window_across_components(
                    job,
                    [Segment(segment, 0.0, 10.0)],
                    window_start=0.0,
                    window_end=10.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    ocr_python=ocr_python,
                    ocr_timeout_seconds=5.0,
                    minimum_component_seconds=3.0,
                    sample_interval_seconds=1.0,
                    coarse_sample_interval_seconds=None,
                )

            self.assertEqual(locate.call_args.kwargs["sample_interval_seconds"], 1.0)
            self.assertIsNone(
                locate.call_args.kwargs["coarse_sample_interval_seconds"]
            )

    def test_slow_video_preparation_uses_independent_watchdog_and_clear_error(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "segment.ts"
            segment.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:prepare-timeout",
                "match",
                "YC",
                "yellow_card",
                10.0,
                None,
                1.0,
                event_minute="1",
                clock_only=True,
            )
            timeout = subprocess.TimeoutExpired(
                ["ffmpeg"], OCR_FFMPEG_WATCHDOG_SECONDS
            )
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=timeout,
                ) as materialize,
                patch("vision_runtime.locate_scoreboard_event") as locate,
                self.assertRaises(VisualLocationFailed) as caught,
            ):
                _locate_ocr_window_across_components(
                    job,
                    [Segment(segment, 0.0, 10.0)],
                    window_start=0.0,
                    window_end=10.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    ocr_python=ocr_python,
                    ocr_timeout_seconds=5.0,
                    minimum_component_seconds=3.0,
                )

            self.assertEqual(caught.exception.kind, "ocr_video_preparation_timeout")
            self.assertEqual(
                caught.exception.diagnostics["stage"],
                "ocr_video_preparation",
            )
            self.assertEqual(
                materialize.call_args.kwargs["timeout_seconds"],
                OCR_FFMPEG_WATCHDOG_SECONDS,
            )
            locate.assert_not_called()

    def test_direct_ts_ocr_failure_falls_back_to_existing_mp4_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "segment.ts"
            segment.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:direct-fallback", "match", "YC", "yellow_card",
                10.0, None, 1.0, event_minute="1", clock_only=True,
                scoreboard_profile={
                    "profile_id": "feed-a", "reference_resolution": [1920, 1080],
                    "clock_roi": [40, 20, 220, 90],
                },
            )
            mp4_result = {
                "path": str(root / "part.mp4"),
                "window_start_stream_time": 0.0,
                "window_end_stream_time": 10.0,
            }
            with (
                patch("vision_runtime.materialize_analysis_clip", return_value=mp4_result) as materialize,
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=[
                        ScoreboardOcrError("ocr_frame_extraction_failed", "direct failed"),
                        {"anchor_seconds": 8.0, "location_kind": "match_clock_minute_boundary"},
                    ],
                ) as ocr,
            ):
                located, materialized, _paths = _locate_ocr_window_across_components(
                    job, [Segment(segment, 0.0, 10.0)], window_start=0.0,
                    window_end=10.0, analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg", ocr_python=ocr_python,
                    ocr_timeout_seconds=5.0, minimum_component_seconds=3.0,
                )

            materialize.assert_called_once()
            self.assertGreater(
                materialize.call_args.kwargs["timeout_seconds"],
                0.0,
            )
            self.assertLessEqual(
                materialize.call_args.kwargs["timeout_seconds"],
                5.0,
            )
            self.assertEqual(materialized, mp4_result)
            self.assertEqual(located["anchor_stream_time"], 8.0)
            direct_timeout = ocr.call_args_list[0].kwargs["timeout_seconds"]
            fallback_timeout = ocr.call_args_list[1].kwargs["timeout_seconds"]
            self.assertGreater(direct_timeout, 0.0)
            self.assertGreater(fallback_timeout, 0.0)
            self.assertLessEqual(fallback_timeout, direct_timeout)
            self.assertEqual(
                located["diagnostics"]["direct_clock_roi"]["error_kind"],
                "ocr_frame_extraction_failed",
            )

    def test_direct_ts_mp4_fallback_timeout_is_reported_as_inference_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment = root / "segment.ts"
            segment.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:direct-fallback-timeout",
                "match",
                "YC",
                "yellow_card",
                10.0,
                None,
                1.0,
                event_minute="1",
                clock_only=True,
                scoreboard_profile={
                    "profile_id": "feed-a",
                    "reference_resolution": [1920, 1080],
                    "clock_roi": [40, 20, 220, 90],
                },
            )
            timeout = subprocess.TimeoutExpired(["ffmpeg"], 5.0)
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=timeout,
                ) as materialize,
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=ScoreboardOcrError(
                        "ocr_frame_extraction_failed",
                        "direct failed",
                    ),
                ),
            ):
                with self.assertRaises(VisualLocationFailed) as captured:
                    _locate_ocr_window_across_components(
                        job,
                        [Segment(segment, 0.0, 10.0)],
                        window_start=0.0,
                        window_end=10.0,
                        analysis_path=root / "candidate.mp4",
                        ffmpeg="ffmpeg",
                        ocr_python=ocr_python,
                        ocr_timeout_seconds=5.0,
                        minimum_component_seconds=3.0,
                    )

            self.assertEqual(captured.exception.kind, "inference_timeout")
            self.assertEqual(
                captured.exception.diagnostics["fragment_attempts"][-1][
                    "error_kind"
                ],
                "inference_timeout",
            )
            self.assertGreater(
                materialize.call_args.kwargs["timeout_seconds"],
                0.0,
            )

    def test_ocr_without_anchor_or_interval_is_explicit_no_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:YC:no-target",
                "match",
                "YC",
                "yellow_card",
                100.0,
                None,
                1.0,
                event_minute="2",
                clock_only=True,
            )
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "part.mp4"),
                        "window_start_stream_time": 0.0,
                        "window_end_stream_time": 30.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={"anchor_seconds": None},
                ),
            ):
                with self.assertRaises(VisualLocationFailed) as raised:
                    _locate_ocr_window_across_components(
                        job,
                        [Segment(segment_path, 0.0, 30.0)],
                        window_start=0.0,
                        window_end=30.0,
                        analysis_path=root / "candidate.mp4",
                        ffmpeg="ffmpeg",
                        ocr_python=ocr_python,
                        ocr_timeout_seconds=1.0,
                        minimum_component_seconds=3.0,
                    )

            self.assertEqual(raised.exception.kind, "ocr_no_target")
            self.assertEqual(
                raised.exception.diagnostics["fragment_attempts"][0]["error_kind"],
                "ocr_no_target",
            )

    @staticmethod
    def _discover_three_path_event(
        runtime: PipelineRuntime,
        event_key: str,
        *,
        second: int | None,
    ) -> None:
        runtime.discover_task(
            match_id="match",
            event_data={
                "event_key": event_key,
                "code": "G",
                "event_type": "goal",
                "minute": "2",
                "minute_extra": "0",
                "second": second,
                "team": "teamA",
                "person": "A",
                "person_id": "1",
                "score": "1-0",
                "reason": "",
                "metadata": {},
            },
            observed_stream_time=150.0,
            observed_source_time=None,
            clip_anchor_stream_time=140.0,
            clip_anchor_source_time=None,
            output_due_stream_time=160.0,
            detected_at_unix=time.time(),
        )
        for artifact_kind in ("ocr_window", "tdeed_refined"):
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind=artifact_kind,
                search_start_stream_time=60.0,
                search_end_stream_time=180.0,
                clip_before_seconds=(30.0 if artifact_kind == "ocr_window" else 8.0),
                clip_after_seconds=(30.0 if artifact_kind == "ocr_window" else 12.0),
                deadline_at_unix=time.time() + 300.0,
            )

    def test_three_path_exact_second_produces_independent_60_and_20_second_gifs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:three-path-exact"
            self._discover_three_path_event(runtime, event_key, second=120)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            job = VisionJob(
                event_key, "match", "G", "goal", 140.0, None, time.time(),
                observed_anchor_stream_time=150.0,
                event_minute="2",
                event_second=120,
                target_score="1-0",
                clock_only=True,
            )

            def fake_encode(*_args, **kwargs):
                output = root / kwargs["output_filename"]
                return {"output": str(output), "bytes": 1234, "duration_sec": 60.0}

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        {
                            "anchor_stream_time": 100.0,
                            "anchor_seconds": 100.0,
                            "location_kind": "match_clock_second",
                            "method": "paddleocr_exact_clock",
                            "precision": "observed_second",
                            "localization_quality": "exact",
                            "degraded": False,
                            "degradation_mode": None,
                            "degradation_reason": None,
                            "target_clock": "02:00",
                            "target_clock_seconds": 120,
                            "diagnostics": {},
                        },
                        {"window_start_stream_time": 60.0, "window_end_stream_time": 180.0},
                        [str(segment_path)],
                    ),
                ),
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "tdeed.mp4"),
                        "window_start_stream_time": 70.0,
                        "window_end_stream_time": 130.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={
                        "anchor_stream_time": 102.0,
                        "confidence": 0.9,
                        "label": "Goal",
                    },
                ) as tdeed,
                patch("vision_runtime.encode_gif", side_effect=fake_encode) as encode,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: [Segment(segment_path, 0.0, 200.0)],
                    "ffmpeg", "ffprobe", root,
                    search_before=120.0, search_after=30.0,
                    refined_before=8.0, refined_after=12.0,
                    width=768, fps=16.0, colors=256,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            self.assertEqual(encode.call_count, 2)
            ocr_call, refined_call = encode.call_args_list
            self.assertEqual(
                (ocr_call.kwargs["before"], ocr_call.kwargs["after"]),
                (30.0, 30.0),
            )
            self.assertEqual(
                (ocr_call.kwargs["width"], ocr_call.kwargs["fps"], ocr_call.kwargs["colors"]),
                (384, 6.0, 160),
            )
            self.assertEqual(
                (refined_call.kwargs["before"], refined_call.kwargs["after"]),
                (8.0, 12.0),
            )
            self.assertEqual(tdeed.call_args.kwargs["candidate_window_start_seconds"], 70.0)
            self.assertEqual(tdeed.call_args.kwargs["candidate_window_end_seconds"], 130.0)
            self.assertEqual(
                runtime.store.get_vision_task(event_key, "ocr_window").status,
                "encoded",
            )
            self.assertEqual(
                runtime.store.get_vision_task(event_key, "tdeed_refined").status,
                "encoded",
            )
            ocr_result = runtime.store.get_vision_task(
                event_key, "ocr_window"
            ).result
            self.assertEqual(ocr_result["localization_quality"], "exact")
            self.assertFalse(ocr_result["degraded"])
            refined_source = runtime.store.get_vision_task(
                event_key, "tdeed_refined"
            ).result["source_ocr_artifact"]
            self.assertEqual(refined_source["localization_quality"], "exact")
            self.assertFalse(refined_source["degraded"])
            self.assertEqual(runtime.store.get(event_key).status, "pending")
            runtime.close()

    def test_penalty_goal_ocr_window_passes_second_to_every_fragment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / "early.ts", root / "recent.ts"]
            for path in paths:
                path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            segments = [
                Segment(paths[0], 0.0, 20.0),
                Segment(paths[1], 80.0, 100.0),
            ]
            job = VisionJob(
                "match:PG:ocr-fragments",
                "match",
                "PG",
                "goal",
                95.0,
                None,
                1000.0,
                event_minute="69",
                event_second=4177,
                clock_only=True,
            )

            def materialize(_ffmpeg, _segments, output, **kwargs):
                return {
                    "path": str(output),
                    "window_start_stream_time": kwargs["window_start"],
                    "window_end_stream_time": kwargs["window_end"],
                }

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=materialize,
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=[
                        ScoreboardOcrError(
                            "ocr_clock_unreadable",
                            "early fragment has no readable clock",
                        ),
                        {
                            "anchor_seconds": 90.0,
                            "location_kind": "match_clock_second",
                            "method": "paddleocr_exact_clock",
                            "target_clock": "69:37",
                        },
                    ],
                ) as locate,
            ):
                located, _materialized, _leased = _locate_ocr_window_across_components(
                    job,
                    segments,
                    window_start=0.0,
                    window_end=100.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    ocr_python=ocr_python,
                    ocr_timeout_seconds=1.0,
                    minimum_component_seconds=3.0,
                )

            self.assertEqual(located["anchor_stream_time"], 90.0)
            self.assertEqual(located["location_kind"], "match_clock_second")
            self.assertEqual(locate.call_count, 2)
            self.assertEqual(
                [call.kwargs["event_code"] for call in locate.call_args_list],
                ["PG", "PG"],
            )
            self.assertEqual(
                [call.kwargs["event_second"] for call in locate.call_args_list],
                [4177, 4177],
            )

    def test_exact_ocr_target_stops_before_later_disconnected_components(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / f"part-{index}.ts" for index in range(3)]
            for path in paths:
                path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            segments = [
                Segment(paths[0], 0.0, 20.0),
                Segment(paths[1], 40.0, 60.0),
                Segment(paths[2], 80.0, 100.0),
            ]
            job = VisionJob(
                "match:G:early-exact",
                "match",
                "G",
                "goal",
                95.0,
                None,
                1000.0,
                event_minute="1",
                event_second=60,
                clock_only=True,
            )

            def materialize(_ffmpeg, _segments, output, **kwargs):
                return {
                    "path": str(output),
                    "window_start_stream_time": kwargs["window_start"],
                    "window_end_stream_time": kwargs["window_end"],
                }

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=materialize,
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 10.0,
                        "location_kind": "match_clock_second",
                        "method": "paddleocr_exact_clock",
                        "target_clock": "01:00",
                    },
                ) as locate,
            ):
                located, _materialized, _leased = (
                    _locate_ocr_window_across_components(
                        job,
                        segments,
                        window_start=0.0,
                        window_end=100.0,
                        analysis_path=root / "candidate.mp4",
                        ffmpeg="ffmpeg",
                        ocr_python=ocr_python,
                        ocr_timeout_seconds=10.0,
                        minimum_component_seconds=3.0,
                    )
                )

            locate.assert_called_once()
            self.assertTrue(located["exact_target_locked"])
            self.assertEqual(located["unscanned_component_count"], 2)
            self.assertEqual(len(located["fragment_attempts"]), 1)

    def test_estimated_second_does_not_hide_later_exact_fragment(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / f"part-{index}.ts" for index in range(3)]
            for path in paths:
                path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            segments = [
                Segment(paths[0], 0.0, 20.0),
                Segment(paths[1], 40.0, 60.0),
                Segment(paths[2], 80.0, 100.0),
            ]
            job = VisionJob(
                "match:G:estimated-before-exact",
                "match",
                "G",
                "goal",
                95.0,
                None,
                1000.0,
                event_minute="1",
                event_second=60,
                clock_only=True,
            )

            def materialize(_ffmpeg, _segments, output, **kwargs):
                return {
                    "path": str(output),
                    "window_start_stream_time": kwargs["window_start"],
                    "window_end_stream_time": kwargs["window_end"],
                }

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=materialize,
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=[
                        {
                            "anchor_seconds": 10.0,
                            "location_kind": "match_clock_second",
                            "method": "paddleocr_near_neighbor_estimate",
                            "precision": "estimated_second",
                            "localization_quality": "estimated",
                            "target_clock": "01:00",
                        },
                        {
                            "anchor_seconds": 50.0,
                            "location_kind": "match_clock_second",
                            "method": "paddleocr_exact_clock",
                            "precision": "observed_second",
                            "localization_quality": "exact",
                            "target_clock": "01:00",
                        },
                    ],
                ) as locate,
            ):
                located, _materialized, _leased = (
                    _locate_ocr_window_across_components(
                        job,
                        segments,
                        window_start=0.0,
                        window_end=100.0,
                        analysis_path=root / "candidate.mp4",
                        ffmpeg="ffmpeg",
                        ocr_python=ocr_python,
                        ocr_timeout_seconds=10.0,
                        minimum_component_seconds=3.0,
                    )
                )

            self.assertEqual(locate.call_count, 2)
            self.assertEqual(located["anchor_stream_time"], 50.0)
            self.assertEqual(located["localization_quality"], "exact")
            self.assertTrue(located["exact_target_locked"])
            self.assertEqual(located["unscanned_component_count"], 1)
            self.assertEqual(len(located["fragment_attempts"]), 2)

    def test_nearby_observations_across_fragments_choose_smallest_clock_distance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / f"part-{index}.ts" for index in range(2)]
            for path in paths:
                path.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            segments = [
                Segment(paths[0], 0.0, 20.0),
                Segment(paths[1], 40.0, 60.0),
            ]
            job = VisionJob(
                "match:G:nearest-across-fragments",
                "match",
                "G",
                "goal",
                55.0,
                None,
                1000.0,
                event_minute="52",
                event_second=None,
                clock_only=True,
            )

            def materialize(_ffmpeg, _segments, output, **kwargs):
                return {
                    "path": str(output),
                    "window_start_stream_time": kwargs["window_start"],
                    "window_end_stream_time": kwargs["window_end"],
                }

            def nearby(anchor, observed_clock, distance):
                return {
                    "anchor_seconds": anchor,
                    "location_kind": "match_clock_minute_boundary",
                    "method": "paddleocr_estimated_minute_boundary",
                    "precision": "estimated_minute_boundary",
                    "localization_quality": "estimated",
                    "degraded": True,
                    "degradation_mode": "nearby_observed_clock",
                    "target_clock": "52:00",
                    "target_clock_seconds": 52 * 60,
                    "observed_clock": observed_clock,
                    "observed_clock_distance_seconds": distance,
                }

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=materialize,
                ),
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    side_effect=[
                        nearby(19.0, "51:59", 1),
                        nearby(45.0, "52:05", 5),
                    ],
                ) as locate,
            ):
                located, _materialized, _leased = (
                    _locate_ocr_window_across_components(
                        job,
                        segments,
                        window_start=0.0,
                        window_end=60.0,
                        analysis_path=root / "candidate.mp4",
                        ffmpeg="ffmpeg",
                        ocr_python=ocr_python,
                        ocr_timeout_seconds=10.0,
                        minimum_component_seconds=3.0,
                    )
                )

            self.assertEqual(locate.call_count, 2)
            self.assertEqual(located["anchor_stream_time"], 19.0)
            self.assertEqual(located["observed_clock"], "51:59")
            self.assertEqual(located["observed_clock_distance_seconds"], 1)
            self.assertFalse(located["exact_target_locked"])
            self.assertEqual(len(located["fragment_attempts"]), 2)

    def test_minute_ocr_gif_survives_tdeed_failure_with_ai_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:three-path-minute"
            self._discover_three_path_event(runtime, event_key, second=90)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            job = VisionJob(
                event_key, "match", "G", "goal", 140.0, None, time.time(),
                observed_anchor_stream_time=150.0,
                event_minute="2",
                event_second=90,
                target_score="1-0",
                clock_only=True,
            )
            exact_second_error = {
                "kind": "ocr_exact_second_not_found",
                "message": "target second was not observed",
                "diagnostics": {"exact_second_failure_reason": "target_clock_not_found"},
            }

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    return_value=(
                        {
                            "anchor_stream_time": 120.0,
                            "anchor_seconds": 120.0,
                            "location_kind": "match_clock_minute_boundary",
                            "method": "paddleocr_minute_boundary",
                            "precision": "minute_boundary",
                            "localization_quality": "degraded",
                            "degraded": True,
                            "degradation_mode": "minute_boundary_fallback",
                            "degradation_reason": exact_second_error,
                            "exact_second_error": exact_second_error,
                            "target_clock": "02:00",
                            "target_clock_seconds": 120,
                            "diagnostics": {},
                        },
                        {"window_start_stream_time": 60.0, "window_end_stream_time": 180.0},
                        [str(segment_path)],
                    ),
                ),
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "tdeed.mp4"),
                        "window_start_stream_time": 60.0,
                        "window_end_stream_time": 120.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no candidate in OCR window"),
                ) as locate_candidate_video_mock,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(root / "ocr.gif"),
                        "bytes": 1234,
                        "duration_sec": 60.0,
                    },
                ) as encode,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: [Segment(segment_path, 0.0, 200.0)],
                    "ffmpeg", "ffprobe", root,
                    search_before=120.0, search_after=30.0,
                    refined_before=8.0, refined_after=12.0,
                    width=768, fps=16.0, colors=256,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            self.assertEqual(encode.call_count, 2)
            self.assertEqual(
                (encode.call_args_list[0].kwargs["before"], encode.call_args_list[0].kwargs["after"]),
                (60.0, 0.0),
            )
            self.assertEqual(
                (encode.call_args_list[1].kwargs["before"], encode.call_args_list[1].kwargs["after"]),
                (30.0, 30.0),
            )
            tdeed_kwargs = locate_candidate_video_mock.call_args.kwargs
            self.assertEqual(tdeed_kwargs["candidate_window_start_seconds"], 60.0)
            self.assertEqual(tdeed_kwargs["candidate_window_end_seconds"], 120.0)
            ocr_task = runtime.store.get_vision_task(event_key, "ocr_window")
            refined_task = runtime.store.get_vision_task(event_key, "tdeed_refined")
            self.assertEqual(ocr_task.status, "encoded")
            self.assertEqual(ocr_task.result["localization_source"], "minute_boundary")
            self.assertEqual(ocr_task.result["localization_quality"], "degraded")
            self.assertTrue(ocr_task.result["degraded"])
            self.assertEqual(ocr_task.result["clip_before_seconds"], 60.0)
            self.assertEqual(ocr_task.result["clip_after_seconds"], 0.0)
            self.assertEqual(
                ocr_task.result["requested_media_window"],
                {
                    "start_stream_time": 60.0,
                    "end_stream_time": 120.0,
                },
            )
            self.assertEqual(
                ocr_task.result["degradation_mode"],
                "minute_boundary_fallback",
            )
            self.assertEqual(
                ocr_task.result["degradation_reason"], exact_second_error
            )
            self.assertEqual(
                ocr_task.result["exact_second_error"], exact_second_error
            )
            self.assertEqual(refined_task.status, "encoded")
            self.assertEqual(refined_task.result["output_kind"], "minute_range_fallback")
            self.assertTrue(refined_task.result["fallback_generated"])
            self.assertEqual(refined_task.result["clip_before_seconds"], 30.0)
            self.assertEqual(refined_task.result["clip_after_seconds"], 30.0)
            self.assertEqual(refined_task.result["tdeed_error_kind"], "tdeed_no_candidate")
            self.assertEqual(runtime.store.get(event_key).status, "pending")
            runtime.close()

    def test_three_path_ocr_and_tdeed_failure_keeps_both_reasons(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:three-path-unreadable"
            self._discover_three_path_event(runtime, event_key, second=120)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            job = VisionJob(
                event_key, "match", "G", "goal", 140.0, None, time.time(),
                observed_anchor_stream_time=150.0,
                event_minute="2",
                event_second=120,
                target_score="1-0",
                clock_only=True,
            )

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=VisualLocationFailed(
                        "ocr_clock_unreadable",
                        "no trustworthy match-clock readings were available",
                        {"stage": "ocr_target_localization"},
                    ),
                ),
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "tdeed.mp4"),
                        "window_start_stream_time": 60.0,
                        "window_end_stream_time": 180.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    side_effect=VisionCandidateNotFound("no standalone candidate"),
                ) as tdeed,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(root / "ocr-range.gif"),
                        "bytes": 1234,
                    },
                ) as encode,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: [Segment(segment_path, 0.0, 200.0)],
                    "ffmpeg", "ffprobe", root,
                    search_before=120.0, search_after=30.0,
                    refined_before=8.0, refined_after=12.0,
                    width=768, fps=16.0, colors=256,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            encode.assert_called_once()
            ocr_task = runtime.store.get_vision_task(event_key, "ocr_window")
            refined_task = runtime.store.get_vision_task(
                event_key, "tdeed_refined"
            )
            self.assertEqual(ocr_task.status, "encoded")
            self.assertEqual(
                ocr_task.result["output_kind"], "api_time_range_fallback"
            )
            self.assertFalse(ocr_task.result["ocr_verified"])
            self.assertEqual(refined_task.status, "failed")
            self.assertEqual(refined_task.last_error_kind, "tdeed_no_candidate")
            self.assertEqual(
                refined_task.result["upstream_ocr_failure"]["kind"],
                "ocr_clock_unreadable",
            )
            tdeed.assert_called_once()
            self.assertEqual(runtime.store.get(event_key).status, "pending")
            runtime.close()

    def test_three_path_ocr_failure_still_allows_standalone_tdeed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:three-path-tdeed-after-ocr"
            self._discover_three_path_event(runtime, event_key, second=120)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            job = VisionJob(
                event_key, "match", "G", "goal", 140.0, None, time.time(),
                observed_anchor_stream_time=150.0,
                event_minute="2", event_second=120,
                target_score="1-0", clock_only=True,
            )

            with (
                patch(
                    "vision_runtime._locate_ocr_window_across_components",
                    side_effect=VisualLocationFailed(
                        "ocr_clock_unreadable", "clock unavailable", {"stage": "ocr"}
                    ),
                ),
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    return_value={
                        "path": str(root / "tdeed.mp4"),
                        "window_start_stream_time": 60.0,
                        "window_end_stream_time": 180.0,
                    },
                ),
                patch(
                    "vision_runtime.locate_candidate_video",
                    return_value={"anchor_stream_time": 132.0, "confidence": 0.8},
                ) as tdeed,
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(root / "tdeed.gif"), "bytes": 1234},
                ) as encode,
            ):
                self.assertTrue(refine_event_job(
                    job, runtime, lambda: [Segment(segment_path, 0.0, 200.0)],
                    "ffmpeg", "ffprobe", root,
                    search_before=120.0, search_after=30.0,
                    refined_before=8.0, refined_after=12.0,
                    width=768, fps=16.0, colors=256,
                    size_reference_bytes=10_000_000,
                    python=Path("python"), timeout_seconds=3.0,
                ))

            self.assertEqual(tdeed.call_args.kwargs["candidate_window_start_seconds"], 60.0)
            self.assertEqual(tdeed.call_args.kwargs["candidate_window_end_seconds"], 180.0)
            self.assertEqual(encode.call_count, 2)
            self.assertIn(
                "ocr-fallback", encode.call_args_list[0].kwargs["output_filename"]
            )
            refined = runtime.store.get_vision_task(event_key, "tdeed_refined")
            self.assertEqual(refined.status, "encoded")
            self.assertEqual(refined.result["locator_method"], "tdeed_after_ocr_failure")
            self.assertEqual(
                refined.result["upstream_ocr_failure"]["kind"],
                "ocr_clock_unreadable",
            )
            runtime.close()

    def test_visual_failure_result_uses_one_terminal_contract(self):
        result = _vision_failure_result(
            "buffer_gap",
            "video has a gap",
            failure_stage="buffer",
            fragment_attempts=[],
        )

        self.assertEqual(result["stage"], "buffer")
        self.assertEqual(result["output_kind"], "failed")
        self.assertFalse(result["precise_location"])
        self.assertFalse(result["fallback_generated"])
        self.assertTrue(result["default_gif_preserved"])
        self.assertEqual(result["failure_reason"], {
            "kind": "buffer_gap",
            "stage": "buffer",
            "message": "video has a gap",
        })

    def test_goal_exact_second_returns_clock_anchor_without_running_tdeed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate = root / "candidate.mp4"
            candidate.write_bytes(b"video")
            ocr_python = root / "ocr-python"
            ocr_python.write_bytes(b"python")
            job = VisionJob(
                "match:G:second",
                "match",
                "G",
                "goal",
                160.0,
                None,
                1000.0,
                observed_anchor_stream_time=220.0,
                event_minute="69",
                event_second=4177,
                target_score="1-0",
            )
            with (
                patch(
                    "vision_runtime.locate_scoreboard_event",
                    return_value={
                        "anchor_seconds": 147.25,
                        "method": "paddleocr_interpolated_clock",
                        "precision": "interpolated_second",
                        "location_kind": "match_clock_second",
                        "target_clock": "69:37",
                        "diagnostics": {"worker_wall_seconds": 2.5},
                    },
                ) as ocr,
                patch("vision_runtime.locate_candidate_video") as tdeed,
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

        self.assertEqual(located["anchor_stream_time"], 147.25)
        self.assertEqual(located["locator_method"], "paddleocr_interpolated_clock")
        self.assertEqual(located["precision"], "interpolated_second")
        self.assertEqual(located["target_clock"], "69:37")
        self.assertEqual(located["location_kind"], "match_clock_second")
        self.assertEqual(located["localization_quality"], "exact")
        self.assertFalse(located["degraded"])
        self.assertIsNone(located["degradation_mode"])
        self.assertIsNone(located["degradation_reason"])
        self.assertFalse(located["fallback_used"])
        self.assertTrue(located["precise_location"])
        self.assertEqual(ocr.call_args.kwargs["event_second"], 4177)
        tdeed.assert_not_called()

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

    def test_goal_score_transition_and_tdeed_failure_uses_60_second_fallback(self):
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
            self.assertEqual(located["clip_before_seconds"], 30.0)
            self.assertEqual(located["clip_after_seconds"], 30.0)
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
                clock_only=True,
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
                ) as ocr,
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
            self.assertTrue(ocr.call_args.kwargs["clock_only"])
            self.assertTrue(located["ocr_clock_only"])

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

    def test_tdeed_failure_uses_ocr_minute_interval_as_60_second_fallback(self):
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
            self.assertEqual(located["clip_before_seconds"], 30.0)
            self.assertEqual(located["clip_after_seconds"], 30.0)
            self.assertEqual(located["output_kind"], "minute_range_fallback")
            self.assertFalse(located["precise_location"])
            self.assertEqual(located["localization_quality"], "degraded")
            self.assertTrue(located["degraded"])
            self.assertEqual(
                located["degradation_mode"], "minute_range_fallback"
            )
            self.assertEqual(
                located["degradation_reason"]["kind"], "tdeed_no_candidate"
            )
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

    def test_materialize_analysis_clip_enforces_optional_timeout_and_cleans_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            output = root / "analysis.mp4"
            output.write_bytes(b"stale")

            def fake_run(command, **kwargs):
                self.assertEqual(kwargs["timeout"], 2.5)
                Path(command[-1]).write_bytes(b"partial")
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])

            with patch("vision_runtime.subprocess.run", side_effect=fake_run):
                with self.assertRaises(subprocess.TimeoutExpired):
                    materialize_analysis_clip(
                        "ffmpeg",
                        [Segment(segment_path, 0.0, 10.0)],
                        output,
                        window_start=2.0,
                        window_end=6.0,
                        anchor=4.0,
                        timeout_seconds=2.5,
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
            runtime.transition(
                event_key,
                "failed",
                error="default GIF encoding failed",
                reason="test_default_gif_failure",
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
            self.assertEqual(default_task.status, "failed")
            self.assertEqual(default_task.error, "default GIF encoding failed")
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

    def test_fragmented_minute_fallback_is_labeled_by_actual_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_vision_job(
                root, "fragmented-fallback", deadline_at_unix=time.time() - 1.0
            )
            segment_path = root / "short.ts"
            segment_path.write_bytes(b"video")
            segments = [Segment(segment_path, 57.0, 64.0)]
            output = root / "fragmented.gif"
            located = {
                "anchor_stream_time": 60.0,
                "locator_method": "paddleocr_clock_interval_fallback",
                "minute_fallback": True,
                "fallback_used": True,
                "precise_location": False,
                "clip_before_seconds": 60.0,
                "clip_after_seconds": 60.0,
                "output_kind": "minute_range_fallback",
            }
            with (
                patch(
                    "vision_runtime._locate_across_search_components",
                    return_value=(
                        located,
                        {
                            "path": str(root / "candidate.mp4"),
                            "window_start_stream_time": 57.0,
                            "window_end_stream_time": 64.0,
                        },
                        [str(segment_path.resolve())],
                    ),
                ),
                patch(
                    "vision_runtime.encode_gif",
                    return_value={"output": str(output), "bytes": 321},
                ),
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

            task = runtime.store.get_vision_task(job.event_key)
            self.assertEqual(task.status, "encoded")
            self.assertEqual(task.result["output_kind"], "minute_range_fallback")
            self.assertFalse(task.result["fallback_complete"])
            self.assertEqual(task.result["fallback_label"], "fragmented_clip")
            self.assertAlmostEqual(task.result["available_fallback_seconds"], 7.0)
            self.assertEqual(task.result["requested_fallback_seconds"], 60.0)
            runtime.close()

    def test_search_components_keep_disconnected_video_fragments_independent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / "a.ts", root / "b.ts", root / "c.ts"]
            for path in paths:
                path.write_bytes(b"video")
            components, latest_end = _continuous_search_components(
                [
                    Segment(paths[0], 0.0, 10.0),
                    Segment(paths[1], 10.2, 18.0),
                    Segment(paths[2], 25.0, 32.0),
                ],
                window_start=0.0,
                window_end=40.0,
                minimum_seconds=3.0,
            )
            self.assertEqual(latest_end, 32.0)
            self.assertEqual([(item.start, item.end) for item in components], [
                (0.0, 18.0), (25.0, 32.0)
            ])

    def test_penalty_goal_exact_second_scans_all_components_before_score_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / "early.ts", root / "recent.ts"]
            for path in paths:
                path.write_bytes(b"video")
            segments = [
                Segment(paths[0], 0.0, 20.0),
                Segment(paths[1], 80.0, 100.0),
            ]
            job = VisionJob(
                "match:PG:fragments",
                "match",
                "PG",
                "goal",
                95.0,
                None,
                1000.0,
                event_minute="69",
                event_second=4177,
                target_score="1-0",
            )
            score_result = {
                "anchor_stream_time": 92.0,
                "locator_method": "paddleocr_score_then_tdeed",
                "fallback_used": True,
                "minute_fallback": False,
                "output_kind": "precise_refined",
            }
            exact_result = {
                "anchor_stream_time": 12.0,
                "locator_method": "paddleocr_exact_clock",
                "target_clock": "69:37",
                "fallback_used": False,
                "minute_fallback": False,
                "output_kind": "precise_refined",
                "ocr": {"location_kind": "match_clock_second"},
            }

            def materialize(_ffmpeg, _segments, output, **kwargs):
                return {
                    "path": str(output),
                    "window_start_stream_time": kwargs["window_start"],
                    "window_end_stream_time": kwargs["window_end"],
                }

            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=materialize,
                ),
                patch(
                    "vision_runtime.locate_with_ocr_fallback",
                    side_effect=[score_result, exact_result],
                ) as locate,
            ):
                located, _materialized, _leased = _locate_across_search_components(
                    job,
                    segments,
                    window_start=0.0,
                    window_end=100.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    tdeed_python=Path("python"),
                    ocr_python=Path("ocr-python"),
                    ocr_timeout_seconds=1.0,
                    tdeed_timeout_seconds=1.0,
                )

        self.assertEqual(locate.call_count, 2)
        self.assertEqual(located["anchor_stream_time"], 12.0)
        self.assertEqual(located["locator_method"], "paddleocr_exact_clock")
        self.assertEqual(len(located["fragment_attempts"]), 2)

    def test_exact_second_ambiguity_downgrades_to_existing_locator_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / "early.ts", root / "recent.ts"]
            for path in paths:
                path.write_bytes(b"video")
            segments = [
                Segment(paths[0], 0.0, 20.0),
                Segment(paths[1], 80.0, 100.0),
            ]
            job = VisionJob(
                "match:G:ambiguous-fragments",
                "match",
                "G",
                "goal",
                95.0,
                None,
                1000.0,
                event_second=4177,
            )

            def materialize(_ffmpeg, _segments, output, **kwargs):
                return {
                    "path": str(output),
                    "window_start_stream_time": kwargs["window_start"],
                    "window_end_stream_time": kwargs["window_end"],
                }

            exact = {
                "anchor_stream_time": 90.0,
                "locator_method": "paddleocr_exact_clock",
                "target_clock": "69:37",
                "minute_fallback": False,
                "ocr": {"location_kind": "match_clock_second"},
            }
            score_fallback = {
                "anchor_stream_time": 92.0,
                "locator_method": "paddleocr_score_then_tdeed",
                "minute_fallback": False,
                "fallback_used": True,
                "ocr": {"method": "paddleocr_score_transition"},
            }
            with (
                patch(
                    "vision_runtime.materialize_analysis_clip",
                    side_effect=materialize,
                ),
                patch(
                    "vision_runtime.locate_with_ocr_fallback",
                    side_effect=[
                        exact,
                        {**exact, "anchor_stream_time": 10.0},
                        score_fallback,
                    ],
                ) as locate,
            ):
                located, _materialized, _leased = _locate_across_search_components(
                    job,
                    segments,
                    window_start=0.0,
                    window_end=100.0,
                    analysis_path=root / "candidate.mp4",
                    ffmpeg="ffmpeg",
                    tdeed_python=Path("python"),
                    ocr_python=Path("ocr-python"),
                    ocr_timeout_seconds=1.0,
                    tdeed_timeout_seconds=1.0,
                )

        self.assertEqual(located["locator_method"], "paddleocr_score_then_tdeed")
        self.assertEqual(
            located["exact_second_error"]["matching_fragment_count"], 2
        )
        self.assertIsNone(locate.call_args_list[2].args[0].event_second)

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
            self.assertEqual(search_coverage.status, CoverageStatus.READY_FULL)
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
                [Segment(first, 40.0, 66.0), Segment(second, 68.0, 80.0)],
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
            self.assertEqual(coverage.status, CoverageStatus.READY_FULL)
            self.assertIsNone(coverage.error_kind)
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
            self.assertEqual(search_coverage.status, CoverageStatus.READY_FULL)
            self.assertIsNone(search_coverage.error_kind)
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
            self.assertEqual(task.result["output_kind"], "failed")
            self.assertFalse(task.result["fallback_generated"])
            self.assertTrue(task.result["default_gif_preserved"])
            self.assertEqual(
                task.result["failure_reason"]["stage"], "output_coverage"
            )
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
            self.assertEqual(task.result["output_kind"], "failed")
            self.assertFalse(task.result["precise_location"])
            self.assertTrue(task.result["default_gif_preserved"])
            runtime.close()


if __name__ == "__main__":
    unittest.main()
