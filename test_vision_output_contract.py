from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from live_goal_pipeline import (
    CoverageStatus,
    PendingEvent,
    Segment,
    analyze_video_coverage,
    encode_gif,
)
from pipeline_runtime import PipelineRuntime
from vision_runtime import (
    OCR_OUTPUT_WINDOW_LEASE_OWNER,
    VisionJob,
    _ensure_ocr_output_window_lease,
    _release_ocr_output_window_leases,
    _ocr_progressive_clock_mapping,
    _ocr_progressive_merge_clock_samples,
    process_vision_artifact,
)


CONTRACT_FIELDS = {
    "coverage_quality",
    "degraded",
    "stitched_across_gap",
    "video_gap_count",
    "skipped_gap_seconds",
    "approximate",
    "anchor_adjusted",
    "anchor_adjusted_to_stream_time",
    "anchor_shift_seconds",
    "event_frame_may_be_missing",
}


class VisionOutputContractTests(unittest.TestCase):
    def test_stale_revision_release_keeps_new_output_window_lease(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:revision-output-lease"
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "2",
                    "minute_extra": "0",
                    "second": 120,
                    "team": "teamA",
                    "person": "A",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=130.0,
                observed_source_time=None,
                clip_anchor_stream_time=130.0,
                clip_anchor_source_time=None,
                output_due_stream_time=160.0,
                detected_at_unix=time.time(),
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=70.0,
                search_end_stream_time=130.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=time.time() + 300.0,
            )
            old_path = root / "old.ts"
            new_path = root / "new.ts"
            old_path.write_bytes(b"old")
            new_path.write_bytes(b"new")

            old_lease = _ensure_ocr_output_window_lease(
                runtime,
                event_key=event_key,
                artifact_kind="ocr_window",
                target_revision=0,
                segments=[Segment(old_path, 70.0, 100.0)],
                window_start=70.0,
                window_end=100.0,
                ttl_seconds=300.0,
            )
            new_lease = _ensure_ocr_output_window_lease(
                runtime,
                event_key=event_key,
                artifact_kind="ocr_window",
                target_revision=1,
                segments=[Segment(new_path, 100.0, 130.0)],
                window_start=100.0,
                window_end=130.0,
                ttl_seconds=300.0,
            )

            self.assertNotEqual(old_lease["owner"], new_lease["owner"])
            self.assertEqual(
                _release_ocr_output_window_leases(
                    runtime,
                    event_key,
                    target_revision=0,
                ),
                1,
            )
            remaining = runtime.store.list_segment_leases(event_key=event_key)
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0].owner, new_lease["owner"])
            self.assertEqual(remaining[0].segment_path, str(new_path.resolve()))

            runtime.close()

    def test_untrusted_ocr_observations_do_not_pollute_clock_mapping(self):
        samples = _ocr_progressive_merge_clock_samples(
            [
                {"stream_time": 100.0, "match_clock_seconds": 600},
                {"stream_time": 110.0, "match_clock_seconds": 610},
            ],
            {
                "candidate_start_seconds": 100.0,
                "target_clock_seconds": 9_999,
                "metadata": {
                    "clock_raw_observations": [
                        {
                            "frame_seconds": 25.0,
                            "effective_clock_seconds": 5_555,
                            "continuity_status": "accepted",
                        }
                    ]
                },
                "clock_raw_observations": [
                    {
                        "frame_seconds": 20.0,
                        "effective_clock_seconds": 620,
                        "continuity_status": "accepted",
                    },
                    {
                        "frame_seconds": 21.0,
                        "effective_clock_seconds": 9_999,
                        "continuity_status": "rejected",
                    },
                    {
                        "frame_seconds": 22.0,
                        "effective_clock_seconds": 8_888,
                        "continuity_status": "repaired",
                    },
                    {
                        "frame_seconds": 23.0,
                        "effective_clock_seconds": 7_777,
                        "ambiguous_clock": True,
                        "continuity_status": "accepted",
                    },
                    {
                        "frame_seconds": 24.0,
                        "effective_clock_seconds": 6_666,
                        "scoreboard_visible": False,
                        "continuity_status": "accepted",
                    },
                ],
            },
        )

        self.assertEqual(
            samples,
            [
                {"stream_time": 100.0, "match_clock_seconds": 600},
                {"stream_time": 110.0, "match_clock_seconds": 610},
                {"stream_time": 120.0, "match_clock_seconds": 620},
            ],
        )
        mapping = _ocr_progressive_clock_mapping(samples)
        self.assertEqual(mapping["status"], "ready")
        self.assertEqual(mapping["stream_time_per_match_second"], 1.0)

    def test_encode_result_labels_stitched_video_without_claiming_exact_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            segments = [Segment(first, 0.0, 4.0), Segment(second, 6.0, 10.0)]
            coverage = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=10.0,
                anchor=3.0,
                allow_degraded=True,
                stitch_across_gaps=True,
            )
            event = PendingEvent(
                event_type="goal",
                stream_time=3.0,
                source_time=None,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=10.0,
            )

            def fake_run(command, **_kwargs):
                if command[0] == "ffmpeg":
                    Path(command[-1]).write_bytes(b"GIF89a")
                    return SimpleNamespace(stderr="", stdout="")
                return SimpleNamespace(
                    stderr="",
                    stdout=(
                        '{"streams":[{"width":384,"height":216,'
                        '"r_frame_rate":"6/1"}],'
                        '"format":{"duration":"8.0","size":"6"}}'
                    ),
                )

            with patch("live_goal_pipeline.run", side_effect=fake_run):
                result = encode_gif(
                    "ffmpeg",
                    "ffprobe",
                    segments,
                    event,
                    root,
                    before=3.0,
                    after=7.0,
                    width=384,
                    fps=6,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    coverage=coverage,
                )

        self.assertTrue(CONTRACT_FIELDS.issubset(result))
        self.assertEqual(result["coverage_status"], CoverageStatus.READY_DEGRADED.value)
        self.assertEqual(result["coverage_quality"], "stitched_across_gap")
        self.assertTrue(result["degraded"])
        self.assertTrue(result["stitched_across_gap"])
        self.assertEqual(result["video_gap_count"], 1)
        self.assertEqual(result["skipped_gap_seconds"], 2.0)
        self.assertFalse(result["approximate"])
        self.assertFalse(result["anchor_adjusted"])
        self.assertIsNone(result["anchor_adjusted_to_stream_time"])
        self.assertEqual(result["anchor_shift_seconds"], 0.0)
        self.assertFalse(result["event_frame_may_be_missing"])

    def test_anchor_inside_allowed_gap_is_explicitly_approximate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")

            small_gap = analyze_video_coverage(
                [Segment(first, 0.0, 4.0), Segment(second, 6.0, 10.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=5.0,
                allow_degraded=True,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )

        self.assertEqual(small_gap.status, CoverageStatus.READY_DEGRADED)
        self.assertEqual(small_gap.coverage_quality, "approximate_anchor_boundary")
        self.assertTrue(small_gap.approximate)
        self.assertTrue(small_gap.anchor_adjusted)
        self.assertEqual(small_gap.anchor_adjusted_to, 4.0)
        self.assertEqual(small_gap.anchor_shift_seconds, -1.0)
        self.assertTrue(small_gap.event_frame_may_be_missing)

    def test_anchor_just_inside_gap_is_not_swallowed_by_edge_tolerance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")

            coverage = analyze_video_coverage(
                [Segment(first, 0.0, 4.0), Segment(second, 6.0, 10.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=4.2,
                allow_degraded=True,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )

        self.assertEqual(coverage.status, CoverageStatus.READY_DEGRADED)
        self.assertEqual(coverage.coverage_quality, "approximate_anchor_boundary")
        self.assertTrue(coverage.approximate)
        self.assertEqual(coverage.anchor_adjusted_to, 4.0)
        self.assertAlmostEqual(coverage.anchor_shift_seconds, -0.2)
        self.assertTrue(coverage.event_frame_may_be_missing)

    def test_anchor_gap_over_limit_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")

            coverage = analyze_video_coverage(
                [Segment(first, 0.0, 4.0), Segment(second, 14.1, 20.0)],
                window_start=0.0,
                window_end=20.0,
                anchor=9.0,
                allow_degraded=True,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )

        self.assertEqual(coverage.status, CoverageStatus.UNAVAILABLE)
        self.assertEqual(coverage.error_kind, "anchor_gap_too_large")
        self.assertFalse(coverage.approximate)

    def test_anchor_shift_over_configured_limit_is_unavailable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")

            coverage = analyze_video_coverage(
                [Segment(first, 0.0, 4.0), Segment(second, 6.0, 10.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=5.0,
                allow_degraded=True,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=0.5,
            )

        self.assertEqual(coverage.status, CoverageStatus.UNAVAILABLE)
        self.assertEqual(coverage.error_kind, "anchor_shift_too_large")
        self.assertFalse(coverage.approximate)

    def test_ordinary_internal_gap_can_be_stitched_regardless_of_size(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")

            coverage = analyze_video_coverage(
                [Segment(first, 0.0, 5.0), Segment(second, 25.0, 30.0)],
                window_start=0.0,
                window_end=30.0,
                anchor=2.0,
                allow_degraded=True,
                min_degraded_seconds=10.0,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )

        self.assertEqual(coverage.status, CoverageStatus.READY_DEGRADED)
        self.assertEqual(coverage.coverage_quality, "stitched_across_gap")
        self.assertEqual(coverage.segments, (
            Segment(first, 0.0, 5.0),
            Segment(second, 25.0, 30.0),
        ))
        self.assertEqual(coverage.skipped_gap_seconds, 20.0)
        self.assertTrue(coverage.stitched_across_gap)
        self.assertFalse(coverage.approximate)

    def test_ocr_minimum_degraded_duration_is_ten_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            short_path = root / "short.ts"
            boundary_path = root / "boundary.ts"
            short_path.write_bytes(b"short")
            boundary_path.write_bytes(b"boundary")

            too_short = analyze_video_coverage(
                [Segment(short_path, 0.0, 9.999)],
                window_start=0.0,
                window_end=20.0,
                anchor=5.0,
                allow_degraded=True,
                force_degraded=True,
                min_degraded_seconds=10.0,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )
            boundary = analyze_video_coverage(
                [Segment(boundary_path, 0.0, 10.0)],
                window_start=0.0,
                window_end=20.0,
                anchor=5.0,
                allow_degraded=True,
                force_degraded=True,
                min_degraded_seconds=10.0,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )

        self.assertEqual(too_short.status, CoverageStatus.UNAVAILABLE)
        self.assertEqual(too_short.error_kind, "degraded_clip_too_short")
        self.assertEqual(boundary.status, CoverageStatus.READY_DEGRADED)

    def test_shared_default_minimum_remains_two_seconds(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            short_path = root / "short.ts"
            boundary_path = root / "boundary.ts"
            short_path.write_bytes(b"short")
            boundary_path.write_bytes(b"boundary")

            too_short = analyze_video_coverage(
                [Segment(short_path, 0.0, 1.999)],
                window_start=0.0,
                window_end=10.0,
                anchor=1.0,
                allow_degraded=True,
                force_degraded=True,
            )
            boundary = analyze_video_coverage(
                [Segment(boundary_path, 0.0, 2.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=1.0,
                allow_degraded=True,
                force_degraded=True,
            )

        self.assertEqual(too_short.status, CoverageStatus.UNAVAILABLE)
        self.assertEqual(too_short.error_kind, "degraded_clip_too_short")
        self.assertEqual(boundary.status, CoverageStatus.READY_DEGRADED)

    def test_ocr_output_lease_survives_wait_and_restart_then_releases_on_encode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "state.sqlite3"
            event_log = root / "events.jsonl"
            event_key = "match:G:output-lease"
            runtime = PipelineRuntime(database, event_log)
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "2",
                    "minute_extra": "0",
                    "second": 120,
                    "team": "teamA",
                    "person": "A",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=130.0,
                observed_source_time=None,
                clip_anchor_stream_time=130.0,
                clip_anchor_source_time=None,
                output_due_stream_time=160.0,
                detected_at_unix=time.time(),
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=70.0,
                search_end_stream_time=130.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=time.time() + 300.0,
            )
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            initial_segments = [Segment(first, 70.0, 110.0)]
            job = VisionJob(
                event_key,
                "match",
                "G",
                "goal",
                130.0,
                None,
                time.time(),
                event_minute="2",
                event_second=120,
                target_score="1-0",
                clock_only=True,
            )
            located = {
                "anchor_stream_time": 100.0,
                "anchor_seconds": 100.0,
                "location_kind": "match_clock_second",
                "method": "paddleocr_exact_clock",
                "precision": "observed_second",
                "localization_quality": "exact",
                "degraded": False,
                "target_clock": "02:00",
                "target_clock_seconds": 120,
                "diagnostics": {},
            }

            with patch(
                "vision_runtime._locate_ocr_window_across_components",
                return_value=(
                    located,
                    {
                        "window_start_stream_time": 70.0,
                        "window_end_stream_time": 110.0,
                    },
                    [str(first)],
                ),
            ):
                completed = process_vision_artifact(
                    job,
                    runtime,
                    lambda: initial_segments,
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

            self.assertFalse(completed)
            waiting = runtime.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(waiting.status, "located")
            self.assertEqual(waiting.last_error_kind, "waiting_for_postroll")
            first_wait_leases = runtime.store.list_segment_leases(
                event_key=event_key
            )
            self.assertEqual(
                {lease.owner for lease in first_wait_leases},
                {OCR_OUTPUT_WINDOW_LEASE_OWNER},
            )
            self.assertEqual(
                {lease.segment_path for lease in first_wait_leases},
                {str(first.resolve())},
            )
            retry_at = waiting.next_attempt_at_unix
            runtime.close()

            reopened = PipelineRuntime(database, event_log)
            persisted = reopened.store.list_segment_leases(event_key=event_key)
            self.assertEqual(len(persisted), 1)
            full_segments = [
                Segment(first, 70.0, 110.0),
                Segment(second, 110.0, 130.0),
            ]
            output = root / "ocr.gif"
            with (
                patch("vision_runtime.time.time", return_value=retry_at + 1.0),
                patch(
                    "vision_runtime.encode_gif",
                    return_value={
                        "output": str(output),
                        "bytes": 456,
                        "duration_sec": 60.0,
                    },
                ) as encode_mock,
            ):
                completed = process_vision_artifact(
                    job,
                    reopened,
                    lambda: full_segments,
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

            self.assertTrue(completed)
            coverage = encode_mock.call_args.kwargs["coverage"]
            self.assertEqual(coverage.status, CoverageStatus.READY_FULL)
            encoded = reopened.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(encoded.status, "encoded")
            self.assertEqual(
                reopened.store.list_segment_leases(event_key=event_key), []
            )
            renewed_events = [
                record
                for record in (
                    json.loads(line)
                    for line in event_log.read_text(encoding="utf-8").splitlines()
                )
                if record.get("event") == "ocr_output_window_leased"
            ]
            self.assertEqual(len(renewed_events), 2)
            self.assertEqual(renewed_events[1]["renewed_lease_count"], 1)
            self.assertEqual(renewed_events[1]["new_segment_count"], 1)
            self.assertEqual(renewed_events[1]["segment_count"], 2)
            reopened.close()

    def test_ocr_output_lease_releases_on_terminal_coverage_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:output-lease-failure"
            runtime.discover_task(
                match_id="match",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "2",
                    "minute_extra": "0",
                    "second": 120,
                    "team": "teamA",
                    "person": "A",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=130.0,
                observed_source_time=None,
                clip_anchor_stream_time=130.0,
                clip_anchor_source_time=None,
                output_due_stream_time=160.0,
                detected_at_unix=time.time(),
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=70.0,
                search_end_stream_time=130.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=time.time() + 300.0,
            )
            located = {
                "anchor_stream_time": 100.0,
                "anchor_source_time": None,
                "localization_source": "exact",
                "location_kind": "match_clock_second",
                "localization_quality": "exact",
                "target_clock_seconds": 120,
                "clip_before_seconds": 30.0,
                "clip_after_seconds": 30.0,
            }
            runtime.transition_vision_task(
                event_key,
                "locating",
                artifact_kind="ocr_window",
            )
            runtime.transition_vision_task(
                event_key,
                "located",
                artifact_kind="ocr_window",
                result=located,
            )
            protected = root / "protected.ts"
            before_gap = root / "before.ts"
            after_gap = root / "after.ts"
            for path in (protected, before_gap, after_gap):
                path.write_bytes(b"video")
            runtime.store.acquire_segment_lease(
                event_key,
                [str(protected.resolve())],
                artifact_kind="ocr_window",
                owner=OCR_OUTPUT_WINDOW_LEASE_OWNER,
                ttl_seconds=300.0,
            )
            job = VisionJob(
                event_key,
                "match",
                "G",
                "goal",
                130.0,
                None,
                time.time(),
                event_minute="2",
                event_second=120,
                target_score="1-0",
                clock_only=True,
            )

            completed = process_vision_artifact(
                job,
                runtime,
                lambda: [
                    Segment(before_gap, 70.0, 90.0),
                    Segment(after_gap, 111.0, 130.0),
                ],
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

            self.assertTrue(completed)
            failed = runtime.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(failed.status, "failed")
            self.assertEqual(failed.last_error_kind, "event_frame_missing")
            self.assertEqual(
                failed.result["coverage_error_kind"], "anchor_gap_too_large"
            )
            self.assertEqual(
                runtime.store.list_segment_leases(event_key=event_key), []
            )
            runtime.close()

    def test_ocr_gap_output_stitches_all_available_components(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match:G:approximate-output"
            event_data = {
                "event_key": event_key,
                "code": "G",
                "event_type": "goal",
                "minute": "2",
                "minute_extra": "0",
                "second": 120,
                "team": "teamA",
                "person": "A",
                "person_id": "1",
                "score": "1-0",
                "reason": "",
                "metadata": {},
            }
            runtime.discover_task(
                match_id="match",
                event_data=event_data,
                observed_stream_time=130.0,
                observed_source_time=None,
                clip_anchor_stream_time=130.0,
                clip_anchor_source_time=None,
                output_due_stream_time=160.0,
                detected_at_unix=time.time(),
            )
            runtime.transition(event_key, "encoding")
            runtime.transition(
                event_key,
                "encoded",
                result={"output": str(root / "default.gif"), "bytes": 1234},
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=70.0,
                search_end_stream_time=130.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=time.time() + 300.0,
            )
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            segments = [Segment(first, 70.0, 95.0), Segment(second, 97.0, 130.0)]
            job = VisionJob(
                event_key,
                "match",
                "G",
                "goal",
                130.0,
                None,
                time.time(),
                event_minute="2",
                event_second=120,
                target_score="1-0",
                clock_only=True,
            )

            def fake_encode(*_args, **kwargs):
                coverage = kwargs["coverage"]
                return {
                    "output": str(root / "ocr.gif"),
                    "bytes": 456,
                    "duration_sec": 58.0,
                    "coverage_status": coverage.status.value,
                    "coverage_quality": coverage.coverage_quality,
                    "degraded": coverage.degraded,
                    "stitched_across_gap": coverage.stitched_across_gap,
                    "video_gap_count": coverage.video_gap_count,
                    "skipped_gap_seconds": coverage.skipped_gap_seconds,
                    "approximate": coverage.approximate,
                    "anchor_adjusted": coverage.anchor_adjusted,
                    "anchor_adjusted_to_stream_time": coverage.anchor_adjusted_to,
                    "anchor_shift_seconds": coverage.anchor_shift_seconds,
                    "event_frame_may_be_missing": coverage.event_frame_may_be_missing,
                }

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
                            "target_clock": "02:00",
                            "target_clock_seconds": 120,
                            "diagnostics": {},
                        },
                        {
                            "window_start_stream_time": 70.0,
                            "window_end_stream_time": 130.0,
                        },
                        [str(first), str(second)],
                    ),
                ),
                patch("vision_runtime.encode_gif", side_effect=fake_encode),
            ):
                completed = process_vision_artifact(
                    job,
                    runtime,
                    lambda: segments,
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

            self.assertTrue(completed)
            default_task = runtime.store.get(event_key)
            self.assertEqual(default_task.status, "encoded")
            self.assertEqual(default_task.output_path, str(root / "default.gif"))
            self.assertEqual(default_task.output_bytes, 1234)

            ocr_task = runtime.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(ocr_task.status, "encoded", ocr_task.result)
            result = ocr_task.result
            self.assertTrue(CONTRACT_FIELDS.issubset(result))
            self.assertTrue(result["default_gif_preserved"])
            self.assertEqual(result["coverage_status"], "ready_degraded")
            self.assertTrue(result["coverage_degraded"])
            self.assertFalse(result["approximate"])
            self.assertFalse(result["anchor_adjusted"])
            self.assertIsNone(result["anchor_adjusted_to_stream_time"])
            self.assertEqual(result["anchor_shift_seconds"], 0.0)
            self.assertTrue(result["stitched_across_gap"])
            self.assertEqual(result["video_gap_count"], 1)
            self.assertEqual(result["skipped_gap_seconds"], 2.0)
            self.assertEqual(result["degradation_source"], "live_source_missing")
            self.assertFalse(result["event_frame_may_be_missing"])
            self.assertTrue(result["event_frame_present"])
            actual = result["actual_media_window"]
            self.assertEqual(actual["start_stream_time"], 70.0)
            self.assertEqual(actual["end_stream_time"], 130.0)
            self.assertTrue(CONTRACT_FIELDS.issubset(actual))
            for key in CONTRACT_FIELDS:
                self.assertEqual(actual[key], result[key], key)
            runtime.close()


if __name__ == "__main__":
    unittest.main()
