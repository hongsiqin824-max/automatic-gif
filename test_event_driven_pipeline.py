import json
import signal
import tempfile
import unittest
import urllib.error
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from event_driven_pipeline import (
    EventJob,
    EventRevisionTracker,
    HttpShotmapGoalSource,
    HttpMatchEventSource,
    HttpShotmapSecondSource,
    MatchEvent,
    MockMatchEventSource,
    SegmentGeneration,
    associate_shotmap_second,
    build_ocr_draft_queue,
    cross_source_goal_incident,
    encode_event_job,
    enqueue_all_encoded_ocr_drafts,
    enqueue_encoded_ocr_draft,
    event_timing_diagnostics,
    exact_shotmap_stream_anchor,
    evict_terminal_runtime_jobs,
    protect_incomplete_vision_event_segments,
    maintain_segment_generations,
    merge_cross_source_goal,
    merge_observed_event_revision,
    main,
    load_scoreboard_profile,
    mark_incomplete_vision_tasks_on_shutdown,
    observe_segment_progress,
    observed_stream_time_from_wall,
    parse_match_start_play,
    parse_match_events,
    parse_cumulative_match_second,
    promote_shotmap_goal_candidates,
    normalize_shotmap_goal,
    overview_goal_fallback_status,
    select_cross_source_goal_incident,
    shotmap_goal_match_event,
    refresh_vision_job_event_data,
    reset_vision_artifact_for_target_upgrade,
    split_vision_pool_task_key,
    shutdown_task_pools,
    sync_completed_default_job,
    vision_artifact_ready_for_submission,
    vision_pool_task_key,
)
from live_goal_pipeline import PendingEvent, Segment
from live_runtime import BoundedTaskPool, ProcessExit
from segment_manifest import new_segment_manifest, save_segment_manifest, upsert_segment_generation
from vision_runtime import VisionJob


class FakeHttpResponse:
    def __init__(self, payload):
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.body


class EventVisualLeaseTests(unittest.TestCase):
    def test_worker_draft_queue_uses_shared_staging_directory(self):
        database = Path("/tmp/article-publish.sqlite3")
        staging = Path("/tmp/published-gifs")
        with patch("event_driven_pipeline.ArticleDraftQueue") as queue_class:
            queue = build_ocr_draft_queue(
                database,
                staging_directory=staging,
            )

        self.assertIs(queue, queue_class.return_value)
        queue_class.assert_called_once_with(
            database_path=database,
            staging_directory=staging,
        )

    def test_encoded_ocr_is_registered_but_pending_ocr_is_not(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "54478914:G:draft"
            runtime.discover_task(
                match_id="54478914",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "19",
                    "team": "teamA",
                    "person": "球员甲",
                    "score": "1-0",
                },
                observed_stream_time=100.0,
                observed_source_time=None,
                clip_anchor_stream_time=100.0,
                clip_anchor_source_time=None,
                output_due_stream_time=130.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=0.0,
                search_end_stream_time=100.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
            )
            draft_queue = Mock()
            draft_queue.enqueue.return_value = {
                "status": "queued",
                "article_id": None,
                "quality_label": "精确到秒",
            }

            self.assertFalse(
                enqueue_encoded_ocr_draft(
                    draft_queue,
                    runtime,
                    match_id="54478914",
                    event_key=event_key,
                    match_detail={},
                )
            )
            runtime.transition_vision_task(
                event_key, "locating", artifact_kind="ocr_window"
            )
            runtime.transition_vision_task(
                event_key,
                "located",
                artifact_kind="ocr_window",
                result={"anchor_stream_time": 80.0},
            )
            runtime.transition_vision_task(
                event_key, "encoding", artifact_kind="ocr_window"
            )
            runtime.transition_vision_task(
                event_key,
                "encoded",
                artifact_kind="ocr_window",
                result={
                    "output": str(root / "ocr.gif"),
                    "bytes": 100,
                    "visual_resolution": "ocr_second_exact",
                },
            )

            self.assertEqual(
                enqueue_all_encoded_ocr_drafts(
                    draft_queue,
                    runtime,
                    match_id="54478914",
                    match_detail={"team_A_name": "主队"},
                ),
                1,
            )
            called = draft_queue.enqueue.call_args.kwargs
            self.assertEqual(called["event"]["event_key"], event_key)
            self.assertEqual(called["event"]["status"], "encoded")
            self.assertEqual(called["source_path"], root / "ocr.gif")
            self.assertEqual(called["artifact_result"]["visual_resolution"], "ocr_second_exact")
            draft_queue.reset_mock()
            runtime.store.suppress_task(event_key, "54478914:G:canonical")
            self.assertFalse(
                enqueue_encoded_ocr_draft(
                    draft_queue,
                    runtime,
                    match_id="54478914",
                    event_key=event_key,
                    match_detail={},
                )
            )
            draft_queue.enqueue.assert_not_called()
            runtime.close()

    def test_shutdown_marks_ocr_incomplete_without_touching_default_gif(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match-1:G:shutdown-ocr"
            runtime.discover_task(
                match_id="match-1",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "2",
                    "minute_extra": "0",
                    "team": "teamA",
                    "person": "A",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=200.0,
                observed_source_time=None,
                clip_anchor_stream_time=200.0,
                clip_anchor_source_time=None,
                output_due_stream_time=230.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=100.0,
                search_end_stream_time=200.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=1400.0,
            )

            marked = mark_incomplete_vision_tasks_on_shutdown(
                runtime, "match-1", reason="graceful_stop_timeout"
            )

            self.assertEqual(marked, 1)
            ocr_task = runtime.store.get_vision_task(event_key, "ocr_window")
            default_task = runtime.store.get(event_key)
            self.assertEqual(ocr_task.status, "failed")
            self.assertEqual(ocr_task.last_error_kind, "vision_shutdown_timeout")
            self.assertEqual(ocr_task.result["completion_state"], "incomplete")
            self.assertEqual(default_task.status, "pending")
            runtime.close()

    def test_bounded_shutdown_prioritizes_default_and_fences_vision_state(self):
        import threading

        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match-1:G:bounded-shutdown"
            runtime.discover_task(
                match_id="match-1",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "2",
                    "minute_extra": "0",
                    "team": "teamA",
                    "person": "A",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=200.0,
                observed_source_time=None,
                clip_anchor_stream_time=200.0,
                clip_anchor_source_time=None,
                output_due_stream_time=230.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=100.0,
                search_end_stream_time=200.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=1400.0,
            )
            default_pool = BoundedTaskPool(1)
            vision_pool = BoundedTaskPool(1, prioritized=True)
            default_cancel = threading.Event()
            vision_cancel = threading.Event()
            vision_started = threading.Event()

            def finish_default_after_vision_is_cancelled():
                self.assertTrue(vision_cancel.wait(1.0))
                return default_cancel.is_set()

            def running_vision():
                vision_started.set()
                self.assertTrue(vision_cancel.wait(1.0))

            default_pool.submit("default", finish_default_after_vision_is_cancelled)
            vision_pool.submit("active", running_vision)
            self.assertTrue(vision_started.wait(1.0))
            vision_pool.submit("queued", lambda: None)

            summary = shutdown_task_pools(
                default_pool,
                vision_pool,
                default_cancel_event=default_cancel,
                vision_cancel_event=vision_cancel,
                runtime=runtime,
                match_id="match-1",
                terminal_vision_reason="match_played",
                default_drain_seconds=0.5,
                cancel_drain_seconds=0.5,
            )

            self.assertEqual(summary["cancelled_vision_keys"], ["queued"])
            self.assertTrue(summary["default_drained_before_cancel"])
            self.assertTrue(summary["default_drained"])
            self.assertTrue(summary["vision_drained"])
            self.assertFalse(default_cancel.is_set())
            self.assertTrue(vision_cancel.is_set())
            self.assertFalse(default_pool.collect_done()[0][1])
            task = runtime.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(task.status, "failed")
            self.assertEqual(task.last_error_kind, "vision_shutdown_timeout")
            with self.assertRaisesRegex(ValueError, "match-end shutdown"):
                runtime.transition_vision_task(
                    event_key,
                    "pending",
                    artifact_kind="ocr_window",
                )
            runtime.close()

    def test_progressive_scan_and_output_windows_extend_event_lease(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match-1:G:lease-window"
            runtime.discover_task(
                match_id="match-1",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "2",
                    "minute_extra": "0",
                    "team": "teamA",
                    "person": "A",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=200.0,
                observed_source_time=None,
                clip_anchor_stream_time=200.0,
                clip_anchor_source_time=None,
                output_due_stream_time=230.0,
                detected_at_unix=1000.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=100.0,
                search_end_stream_time=200.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
                deadline_at_unix=1400.0,
            )
            runtime.record_vision_readiness_wait(
                event_key,
                "waiting for progressive media",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "last_scan_start_stream_time": 180.0,
                        "scan_cursor_stream_time": 260.0,
                        "target_rescan_window": {
                            "start_stream_time": 130.0,
                            "end_stream_time": 160.0,
                        },
                        "requested_output_start_stream_time": 120.0,
                        "requested_output_end_stream_time": 280.0,
                    }
                },
                now=1001.0,
            )
            early_path = root / "early.ts"
            late_path = root / "late.ts"
            early_path.write_bytes(b"video")
            late_path.write_bytes(b"video")
            task = runtime.store.get_vision_task(event_key, "ocr_window")

            result = protect_incomplete_vision_event_segments(
                runtime,
                [task],
                [
                    Segment(early_path, 40.0, 70.0),
                    Segment(late_path, 250.0, 280.0),
                ],
                ocr_timeout_seconds=180.0,
                vision_timeout_seconds=90.0,
                graceful_stop_timeout_seconds=30.0,
                now_unix=1002.0,
            )[0]

            self.assertEqual(result["retention_start_stream_time"], 40.0)
            self.assertEqual(result["retention_end_stream_time"], 290.0)
            self.assertEqual(result["segment_count"], 2)
            self.assertEqual(
                runtime.store.protected_segment_paths(now=1002.0),
                {str(early_path.resolve()), str(late_path.resolve())},
            )
            runtime.close()


class ShotmapSecondEnrichmentTests(unittest.TestCase):
    @staticmethod
    def goal(**changes):
        values = {
            "event_key": "match-1:G:goal-1",
            "code": "G",
            "event_type": "goal",
            "minute": "69",
            "minute_extra": "0",
            "team": "teamA",
            "person": "Scorer",
            "person_id": "50000009",
            "score": "1-0",
            "reason": "",
        }
        values.update(changes)
        return MatchEvent(**values)

    def test_shotmap_second_is_cumulative_match_clock_seconds(self):
        for raw, expected in ((152, 152), ("4177", 4177), (5643.0, 5643)):
            with self.subTest(raw=raw):
                self.assertEqual(parse_cumulative_match_second(raw), expected)
        for raw in (None, "", -1, 12.5, True, "69:37"):
            with self.subTest(raw=raw):
                self.assertIsNone(parse_cumulative_match_second(raw))

    def test_shotmap_goal_normalizes_455_to_exact_clock_without_minute_flooring(self):
        goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "minute_extra": 0,
                "second": 455,
                "situation": "open_play",
                "start_x": 0.35001,
                "start_y": 0.5,
            }
        )
        self.assertIsNotNone(goal)
        self.assertEqual(goal["second"], 455)
        self.assertEqual(goal["minute"], 8)
        self.assertEqual(goal["start_x"], 0.35)

    def test_direct_shotmap_event_keeps_api_minute_and_exact_target_clock(self):
        goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "minute_extra": 0,
                "second": 455,
                "situation": "open_play",
                "start_x": 0.35,
                "start_y": 0.5,
            }
        )

        event = shotmap_goal_match_event(
            "match-1",
            goal,
            observed_at_unix=1000.0,
            request_diagnostics={"request_count": 2},
        )

        self.assertEqual(event.code, "G")
        self.assertEqual(event.minute, "8")
        self.assertEqual(event.second, 455)
        self.assertEqual(event.person_id, "50000009")
        self.assertEqual(event.metadata["target_clock"], "07:35")
        self.assertEqual(event.metadata["event_source"]["primary"], "shotmap")

    def test_shotmap_fingerprint_covers_required_incident_fields(self):
        base = {
            "outcome": "goal",
            "person_id": 9,
            "team_id": 2,
            "minute": 8,
            "minute_extra": 0,
            "second": 455,
            "situation": "open_play",
            "start_x": 0.35,
            "start_y": 0.5,
        }
        original = normalize_shotmap_goal(base)
        self.assertIsNotNone(original)

        for field, replacement in (
            ("person_id", 10),
            ("team_id", 3),
            ("minute", 9),
            ("second", 456),
            ("situation", "penalty"),
            ("start_x", 0.45),
        ):
            with self.subTest(field=field):
                changed = normalize_shotmap_goal({**base, field: replacement})
                self.assertIsNotNone(changed)
                self.assertNotEqual(changed["fingerprint"], original["fingerprint"])

    def test_background_response_time_maps_to_the_true_stream_observation(self):
        self.assertEqual(
            observed_stream_time_from_wall(
                120.0,
                processed_at_unix=1010.0,
                observed_at_unix=1004.0,
                stream_rate=1.0,
            ),
            114.0,
        )
        self.assertEqual(
            observed_stream_time_from_wall(
                3.0,
                processed_at_unix=1010.0,
                observed_at_unix=1004.0,
                stream_rate=1.0,
            ),
            0.0,
        )

    def test_independent_shotmap_source_baselines_then_emits_new_goals(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            source = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-1",
                None,
                runtime.store,
                timeout=1.0,
            )
            source.start = lambda: None
            baseline = {
                "shots": [
                    {
                        "outcome": "goal",
                        "person_id": 9,
                        "team_id": 2,
                        "minute": 8,
                        "minute_extra": 0,
                        "second": 455,
                        "situation": "open_play",
                    },
                    {"outcome": "save", "person_id": 10, "team_id": 2},
                ]
            }
            source._responses.put((baseline, {"http_status": 200}, 1000.0))
            self.assertEqual(source.poll(0.0, 0.0), [])
            self.assertTrue(source.initialized)

            updated = {
                "shots": [
                    *baseline["shots"],
                    {
                        "outcome": "goal",
                        "person_id": 11,
                        "team_id": 1,
                        "minute": 12,
                        "minute_extra": 0,
                        "second": 701,
                        "situation": "penalty",
                    },
                ]
            }
            source._responses.put((updated, {"http_status": 200}, 1005.0))
            emitted = source.poll(0.0, 0.0)
            self.assertEqual(len(emitted), 1)
            self.assertEqual(emitted[0].code, "PG")
            self.assertEqual(emitted[0].second, 701)
            self.assertEqual(emitted[0].metadata["event_source"]["primary"], "shotmap")

            runtime.close()
            reopened = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            restored = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-1",
                None,
                reopened.store,
                timeout=1.0,
            )
            restored.start = lambda: None
            self.assertFalse(restored.initialized)
            self.assertEqual(restored.last_shot_count, 3)
            self.assertEqual(restored.last_goal_count, 2)
            restored._responses.put((updated, {"http_status": 200}, 1010.0))
            self.assertEqual(restored.poll(0.0, 0.0), [])
            self.assertTrue(restored.initialized)
            reopened.close()

    def test_restart_first_response_is_always_a_fresh_process_baseline(self):
        from pipeline_runtime import PipelineRuntime

        def goal(person_id: int, second: int) -> dict[str, object]:
            return {
                "outcome": "goal",
                "person_id": person_id,
                "team_id": 2,
                "minute": (second + 59) // 60,
                "minute_extra": 0,
                "second": second,
                "situation": "open_play",
            }

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            source = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-restart-baseline",
                None,
                runtime.store,
                timeout=1.0,
            )
            source.start = lambda: None
            first = {"shots": [goal(1, 455)]}
            second = {"shots": [*first["shots"], goal(2, 701)]}
            source._responses.put((first, {"http_status": 200}, 1000.0))
            self.assertEqual(source.poll(0.0, 0.0), [])
            source._responses.put((second, {"http_status": 200}, 1005.0))
            emitted = source.poll(0.0, 0.0)
            self.assertEqual([event.second for event in emitted], [701])
            source.acknowledge(emitted)
            runtime.close()

            reopened = PipelineRuntime(
                root / "state.sqlite3", root / "events.jsonl"
            )
            restarted = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-restart-baseline",
                None,
                reopened.store,
                timeout=1.0,
            )
            restarted.start = lambda: None
            self.assertFalse(restarted.initialized)

            # This row appeared while the monitor was down. It remains durable
            # history but must not be promoted by the first process response.
            during_downtime = {
                "shots": [*second["shots"], goal(3, 900)]
            }
            restarted._responses.put(
                (during_downtime, {"http_status": 200}, 1010.0)
            )
            self.assertEqual(restarted.poll(0.0, 0.0), [])
            state = reopened.store.load_shotmap_state(
                "match-restart-baseline"
            )
            self.assertEqual(len(state.seen_fingerprints), 3)

            live_update = {
                "shots": [*during_downtime["shots"], goal(4, 960)]
            }
            restarted._responses.put(
                (live_update, {"http_status": 200}, 1015.0)
            )
            emitted = restarted.poll(0.0, 0.0)
            self.assertEqual([event.second for event in emitted], [960])
            reopened.close()

    def test_shotmap_candidate_promotes_on_unique_new_overview_event(self):
        from pipeline_runtime import TimelineState

        normalized = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "minute_extra": 0,
                "second": 455,
                "situation": "open_play",
            }
        )
        candidate = shotmap_goal_match_event(
            "match-1",
            normalized,
            observed_at_unix=1000.0,
            request_diagnostics={},
        )
        overview = self.goal(
            event_key="match-1:G:overview-live",
            minute="8",
            person_id="50000009",
            metadata={"team_id": "2", "source": "overview"},
        )
        timeline = TimelineState(
            match_id="match-1",
            timeline_origin_wall_unix=1000.0,
            match_start_at_unix=None,
        )

        promoted = promote_shotmap_goal_candidates(
            [candidate],
            [overview],
            [],
            timeline,
            stream_rate=1.0,
            before=10.0,
            after=20.0,
        )

        self.assertEqual(len(promoted), 1)
        self.assertEqual(
            promoted[0].metadata["shotmap_promotion"]["reason"],
            "new_overview_event",
        )
        self.assertEqual(
            promoted[0].metadata["shotmap_promotion"]["overview_event_key"],
            overview.event_key,
        )
        ambiguous = promote_shotmap_goal_candidates(
            [candidate, replace(candidate, event_key="match-1:G:shotmap-other")],
            [overview],
            [],
            timeline,
            stream_rate=1.0,
            before=10.0,
            after=20.0,
        )
        self.assertEqual(ambiguous, [])

    def test_shotmap_candidate_requires_complete_retained_video_without_overview(self):
        from pipeline_runtime import TimelineState

        normalized = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "minute_extra": 0,
                "second": 455,
                "situation": "open_play",
            }
        )
        candidate = shotmap_goal_match_event(
            "match-1",
            normalized,
            observed_at_unix=1000.0,
            request_diagnostics={},
        )
        timeline = TimelineState(
            match_id="match-1",
            timeline_origin_wall_unix=1000.0,
            timeline_origin_stream_time=0.0,
            match_start_at_unix=1000.0,
        )
        self.assertEqual(
            exact_shotmap_stream_anchor(candidate, timeline, stream_rate=1.0),
            455.0,
        )
        second_half_goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 10,
                "team_id": 2,
                "minute": 70,
                "minute_extra": 0,
                "second": 4177,
                "situation": "open_play",
            }
        )
        second_half = shotmap_goal_match_event(
            "match-1",
            second_half_goal,
            observed_at_unix=1000.0,
            request_diagnostics={},
        )
        self.assertEqual(
            exact_shotmap_stream_anchor(second_half, timeline, stream_rate=1.0),
            4177.0 + timeline.halftime_break_seconds,
        )
        inconsistent = replace(candidate, minute="20")
        self.assertIsNone(
            exact_shotmap_stream_anchor(
                inconsistent, timeline, stream_rate=1.0
            )
        )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            full_path = root / "full.ts"
            full_path.write_bytes(b"video")
            promoted = promote_shotmap_goal_candidates(
                [candidate],
                [],
                [Segment(full_path, 440.0, 480.0)],
                timeline,
                stream_rate=1.0,
                before=10.0,
                after=20.0,
            )
            self.assertEqual(len(promoted), 1)
            self.assertEqual(
                promoted[0].metadata["shotmap_promotion"]["reason"],
                "retained_video_coverage",
            )
            self.assertEqual(
                promoted[0].metadata["shotmap_promotion"]["anchor_stream_time"],
                455.0,
            )

            left = root / "left.ts"
            right = root / "right.ts"
            left.write_bytes(b"video")
            right.write_bytes(b"video")
            rejected_gap = promote_shotmap_goal_candidates(
                [candidate],
                [],
                [Segment(left, 440.0, 450.0), Segment(right, 460.0, 480.0)],
                timeline,
                stream_rate=1.0,
                before=10.0,
                after=20.0,
            )
            self.assertEqual(rejected_gap, [])

        uncalibrated = replace(timeline, match_start_at_unix=None)
        self.assertEqual(
            promote_shotmap_goal_candidates(
                [candidate],
                [],
                [],
                uncalibrated,
                stream_rate=1.0,
                before=10.0,
                after=20.0,
            ),
            [],
        )

    def test_empty_shotmap_response_establishes_an_empty_baseline(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            source = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-empty",
                None,
                runtime.store,
                timeout=1.0,
            )
            source.start = lambda: None
            source._responses.put(({"shots": []}, {"http_status": 200}, 1000.0))

            self.assertEqual(source.poll(0.0, 0.0), [])
            self.assertTrue(source.initialized)
            self.assertEqual(source.last_shot_count, 0)
            self.assertEqual(source.last_goal_count, 0)
            self.assertTrue(runtime.store.load_shotmap_state("match-empty").initialized)
            runtime.close()

    def test_non_goal_shot_after_baseline_does_not_emit_a_goal(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            source = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-non-goal",
                None,
                runtime.store,
                timeout=1.0,
            )
            source.start = lambda: None
            source._responses.put(({"shots": []}, {"http_status": 200}, 1000.0))
            self.assertEqual(source.poll(0.0, 0.0), [])
            source._responses.put(
                (
                    {
                        "shots": [
                            {
                                "outcome": "save",
                                "person_id": 9,
                                "team_id": 2,
                                "minute": 8,
                                "second": 455,
                            }
                        ]
                    },
                    {"http_status": 200},
                    1005.0,
                )
            )

            self.assertEqual(source.poll(0.0, 0.0), [])
            self.assertEqual(source.last_shot_count, 1)
            self.assertEqual(source.last_goal_count, 0)
            self.assertEqual(
                runtime.store.load_shotmap_state("match-non-goal").seen_fingerprints,
                frozenset(),
            )
            runtime.close()

    def test_goal_present_in_first_valid_response_is_historical_baseline_only(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            source = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-history",
                None,
                runtime.store,
                timeout=1.0,
            )
            source.start = lambda: None
            source._responses.put(
                (
                    {
                        "shots": [
                            {
                                "outcome": "goal",
                                "person_id": 9,
                                "team_id": 2,
                                "minute": 8,
                                "minute_extra": 0,
                                "second": 455,
                                "situation": "open_play",
                            }
                        ]
                    },
                    {"http_status": 200},
                    1000.0,
                )
            )

            self.assertEqual(source.poll(0.0, 0.0), [])
            self.assertTrue(source.initialized)
            self.assertEqual(source.last_goal_count, 1)
            self.assertEqual(
                len(runtime.store.load_shotmap_state("match-history").seen_fingerprints),
                1,
            )
            runtime.close()

    def test_invalid_shotmap_response_does_not_initialize_or_replace_baseline(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            source = HttpShotmapGoalSource(
                "https://example.test/shotmap",
                "match-invalid",
                None,
                runtime.store,
                timeout=1.0,
            )
            source.start = lambda: None
            baseline = {
                "shots": [
                    {
                        "outcome": "save",
                        "person_id": 9,
                        "team_id": 2,
                        "minute": 8,
                        "second": 455,
                    }
                ]
            }
            source._responses.put((baseline, {"http_status": 200}, 1000.0))
            self.assertEqual(source.poll(0.0, 0.0), [])
            before_invalid = runtime.store.load_shotmap_state("match-invalid")
            self.assertTrue(before_invalid.initialized)
            self.assertEqual(before_invalid.last_snapshot, baseline)

            source._responses.put((None, {"error": "timeout"}, 1005.0))

            self.assertEqual(source.poll(0.0, 0.0), [])
            self.assertTrue(source.initialized)
            after_invalid = runtime.store.load_shotmap_state("match-invalid")
            self.assertTrue(after_invalid.initialized)
            self.assertEqual(after_invalid.last_snapshot, baseline)
            runtime.close()

    def test_late_matching_shotmap_goal_merges_into_overview_incident(self):
        overview = self.goal(
            event_key="match-1:G:overview-goal",
            minute="8",
            person_id="50000009",
            metadata={"team_id": "2", "source": "overview"},
        )
        goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "minute_extra": 0,
                "second": 455,
                "situation": "open_play",
            }
        )
        self.assertIsNotNone(goal)
        shotmap = shotmap_goal_match_event(
            "match-1",
            goal,
            observed_at_unix=1005.0,
            request_diagnostics={"request_count": 2},
        )

        self.assertTrue(cross_source_goal_incident(overview, shotmap))
        merged = merge_cross_source_goal(overview.__dict__, shotmap)

        self.assertEqual(merged.event_key, overview.event_key)
        self.assertEqual(merged.second, 455)
        self.assertEqual(merged.metadata["event_source"]["primary"], "shotmap")
        self.assertTrue(merged.metadata["overview_merged"])

    def test_historical_shotmap_goal_does_not_absorb_a_new_overview_goal(self):
        historical_goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "minute_extra": 0,
                "second": 455,
                "situation": "open_play",
            }
        )
        self.assertIsNotNone(historical_goal)
        historical = shotmap_goal_match_event(
            "match-1",
            historical_goal,
            observed_at_unix=1000.0,
            request_diagnostics={"request_count": 1},
        )
        current_overview = self.goal(
            event_key="match-1:G:overview-current",
            minute="20",
            person_id="50000010",
            metadata={"team_id": "2", "source": "overview"},
        )

        self.assertFalse(cross_source_goal_incident(historical, current_overview))

    def test_overview_fallback_status_distinguishes_empty_and_non_goal_shotmap(self):
        self.assertEqual(overview_goal_fallback_status(None), "overview_fallback_no_match")
        self.assertEqual(
            overview_goal_fallback_status(
                SimpleNamespace(initialized=True, last_shot_count=0, last_goal_count=0)
            ),
            "overview_fallback_empty",
        )
        self.assertEqual(
            overview_goal_fallback_status(
                SimpleNamespace(initialized=True, last_shot_count=3, last_goal_count=0)
            ),
            "overview_fallback_no_goal",
        )
        self.assertEqual(
            overview_goal_fallback_status(
                SimpleNamespace(initialized=True, last_shot_count=3, last_goal_count=1)
            ),
            "overview_fallback_no_match",
        )

    def test_unique_cross_source_candidate_is_selected_for_one_default_task(self):
        overview = self.goal(
            event_key="match-1:G:overview-goal",
            minute="8",
            person_id="50000009",
            metadata={"team_id": "2", "source": "overview"},
        )
        incoming_goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "second": 455,
                "situation": "open_play",
            }
        )
        self.assertIsNotNone(incoming_goal)
        incoming = shotmap_goal_match_event(
            "match-1",
            incoming_goal,
            observed_at_unix=1005.0,
            request_diagnostics={},
        )

        selected, method, count = select_cross_source_goal_incident(
            [SimpleNamespace(event_data=overview.__dict__)], incoming
        )
        self.assertIsNotNone(selected)
        self.assertEqual(selected.event_data["event_key"], overview.event_key)
        self.assertEqual(method, "strong")
        self.assertEqual(count, 1)

    def test_ambiguous_cross_source_candidates_are_not_merged(self):
        first = self.goal(
            event_key="match-1:G:overview-1",
            minute="8",
            person_id="50000009",
            metadata={"team_id": "2", "source": "overview"},
        )
        second = self.goal(
            event_key="match-1:G:overview-2",
            minute="8",
            person_id="50000009",
            metadata={"team_id": "2", "source": "overview"},
        )
        incoming_goal = normalize_shotmap_goal(
            {
                "outcome": "goal",
                "person_id": 9,
                "team_id": 2,
                "minute": 8,
                "second": 455,
                "situation": "open_play",
            }
        )
        self.assertIsNotNone(incoming_goal)
        incoming = shotmap_goal_match_event(
            "match-1",
            incoming_goal,
            observed_at_unix=1005.0,
            request_diagnostics={},
        )

        selected, method, count = select_cross_source_goal_incident(
            [
                SimpleNamespace(event_data=first.__dict__),
                SimpleNamespace(event_data=second.__dict__),
            ],
            incoming,
        )
        self.assertIsNone(selected)
        self.assertEqual(method, "ambiguous")
        self.assertEqual(count, 2)

    def test_unique_goal_match_enriches_without_changing_event_key(self):
        event = self.goal()
        enriched = associate_shotmap_second(
            event,
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4177,
                    }
                ]
            },
        )

        self.assertEqual(enriched.event_key, event.event_key)
        self.assertEqual(enriched.second, 4177)
        self.assertEqual(enriched.metadata["second_source"], "shotmap")
        self.assertEqual(enriched.metadata["shotmap_match_status"], "matched")
        self.assertEqual(enriched.metadata["shotmap_candidate_count"], 1)
        self.assertEqual(
            enriched.metadata["shotmap_candidate_details"][0]["person_id"],
            "50000009",
        )
        self.assertEqual(
            enriched.metadata["shotmap_candidate_details"][0]["shotmap_person_id"],
            "9",
        )
        self.assertEqual(
            enriched.metadata["shotmap_candidate_details"][0]["outcome"],
            "goal",
        )
        self.assertIsNone(enriched.metadata["shotmap_required_situation"])

    def test_raw_exact_person_id_does_not_bypass_fixed_namespace_offset(self):
        enriched = associate_shotmap_second(
            self.goal(person_id="9"),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4177,
                    }
                ]
            },
        )

        self.assertIsNone(enriched.second)
        self.assertEqual(enriched.metadata["shotmap_person_candidate_count"], 0)
        self.assertEqual(
            enriched.metadata["shotmap_filter_failure_reason"],
            "person_id_mismatch",
        )

    def test_penalty_goal_requires_goal_outcome_and_penalty_situation(self):
        event = self.goal(
            event_key="match-1:PG:goal-1",
            code="PG",
        )
        enriched = associate_shotmap_second(
            event,
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "situation": "open_play",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4161,
                    },
                    {
                        "outcome": "miss",
                        "situation": "penalty",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4168,
                    },
                    {
                        "outcome": "goal",
                        "situation": "penalty",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4177,
                    },
                ]
            },
        )

        self.assertEqual(enriched.code, "PG")
        self.assertEqual(enriched.event_type, "goal")
        self.assertEqual(enriched.second, 4177)
        self.assertEqual(
            enriched.metadata["shotmap_match_method"],
            "outcome+situation+minute+minute_extra+person_id",
        )
        self.assertEqual(enriched.metadata["shotmap_required_outcome"], "goal")
        self.assertEqual(enriched.metadata["shotmap_required_situation"], "penalty")
        self.assertEqual(enriched.metadata["shotmap_raw_candidate_count"], 3)
        self.assertEqual(enriched.metadata["shotmap_outcome_candidate_count"], 2)
        self.assertEqual(enriched.metadata["shotmap_situation_candidate_count"], 1)
        self.assertEqual(
            enriched.metadata["shotmap_candidate_details"][0]["situation"],
            "penalty",
        )

    def test_penalty_goal_reports_missing_required_situation(self):
        enriched = associate_shotmap_second(
            self.goal(event_key="match-1:PG:goal-1", code="PG"),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "situation": "open_play",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4177,
                    }
                ]
            },
        )

        self.assertIsNone(enriched.second)
        self.assertEqual(enriched.metadata["shotmap_match_status"], "missing")
        self.assertEqual(
            enriched.metadata["shotmap_match_reason"],
            "required_situation_not_found",
        )
        self.assertEqual(enriched.metadata["shotmap_outcome_candidate_count"], 1)
        self.assertEqual(enriched.metadata["shotmap_situation_candidate_count"], 0)

    def test_person_and_team_ids_conservatively_narrow_same_minute_goals(self):
        event = self.goal(metadata={"team_id": "101"})
        enriched = associate_shotmap_second(
            event,
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 8,
                        "team_id": 101,
                        "second": 4161,
                    },
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "team_id": 101,
                        "second": 4177,
                    },
                ]
            },
        )

        self.assertEqual(enriched.second, 4177)
        self.assertEqual(
            enriched.metadata["shotmap_match_method"],
            "outcome+minute+minute_extra+person_id+team_id",
        )

    def test_same_player_on_other_team_is_not_associated(self):
        enriched = associate_shotmap_second(
            self.goal(metadata={"team_id": "101"}),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "team_id": 202,
                        "second": 4177,
                    }
                ]
            },
        )

        self.assertIsNone(enriched.second)
        self.assertEqual(enriched.metadata["shotmap_match_status"], "missing")
        self.assertEqual(enriched.metadata["shotmap_match_reason"], "no_candidate")
        self.assertEqual(enriched.metadata["shotmap_candidate_count"], 0)
        self.assertEqual(enriched.metadata["shotmap_clock_candidate_count"], 1)
        self.assertEqual(enriched.metadata["shotmap_person_candidate_count"], 1)
        self.assertEqual(enriched.metadata["shotmap_team_candidate_count"], 0)
        self.assertEqual(
            enriched.metadata["shotmap_clock_candidate_details"][0]["team_id"],
            "202",
        )

    def test_minute_and_stoppage_time_formats_are_normalized(self):
        enriched = associate_shotmap_second(
            self.goal(minute="90+6", minute_extra="0", metadata={"team_id": "101"}),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": "90'",
                        "minute_extra": "6'",
                        "person_id": 9,
                        "team_id": "101",
                        "second": 5763,
                    }
                ]
            },
        )

        self.assertEqual(enriched.second, 5763)
        self.assertEqual(enriched.metadata["shotmap_match_status"], "matched")

    def test_elapsed_minute_and_stoppage_minute_are_equivalent(self):
        enriched = associate_shotmap_second(
            self.goal(minute="46", minute_extra="0"),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": "45",
                        "minute_extra": "1",
                        "person_id": 9,
                        "second": 2758,
                    }
                ]
            },
        )

        self.assertEqual(enriched.second, 2758)
        self.assertEqual(enriched.metadata["shotmap_match_status"], "matched")

    def test_zero_and_integral_float_minutes_remain_valid(self):
        for event_minute, shot_minute in (("0", 0), (0, 0.0)):
            with self.subTest(event_minute=event_minute, shot_minute=shot_minute):
                enriched = associate_shotmap_second(
                    self.goal(minute=event_minute),
                    {
                        "shots": [
                            {
                                "outcome": "goal",
                                "minute": shot_minute,
                                "minute_extra": 0,
                                "person_id": 9,
                                "second": 32,
                            }
                        ]
                    },
                )

                self.assertEqual(enriched.second, 32)

    def test_known_person_id_does_not_match_a_shot_with_missing_person_id(self):
        enriched = associate_shotmap_second(
            self.goal(),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "second": 4177,
                    }
                ]
            },
        )

        self.assertIsNone(enriched.second)
        self.assertEqual(enriched.metadata["shotmap_match_status"], "missing")
        self.assertEqual(enriched.metadata["shotmap_candidate_count"], 0)

    def test_own_goal_requires_a_unique_clock_candidate(self):
        event = self.goal(
            code="OG",
            event_key="match-1:OG:goal-1",
            person_id="0",
            metadata={"team_id": "101"},
        )
        enriched = associate_shotmap_second(
            event,
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 8,
                        "team_id": 101,
                        "second": 4171,
                    },
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "team_id": 101,
                        "second": 4177,
                    },
                ]
            },
        )

        self.assertIsNone(enriched.second)
        self.assertEqual(enriched.metadata["shotmap_match_status"], "ambiguous")
        self.assertEqual(
            enriched.metadata["shotmap_match_reason"],
            "multiple_candidates",
        )
        self.assertEqual(enriched.metadata["shotmap_candidate_count"], 2)
        self.assertEqual(
            [
                candidate["person_id"]
                for candidate in enriched.metadata["shotmap_candidate_details"]
            ],
            ["50000008", "50000009"],
        )

    def test_missing_ambiguous_and_invalid_results_are_distinguishable(self):
        event = self.goal()
        missing = associate_shotmap_second(event, {"shots": []})
        invalid = associate_shotmap_second(
            event,
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": "not-a-second",
                    }
                ]
            },
        )
        malformed = associate_shotmap_second(event, {"shots": {}})

        self.assertEqual(missing.metadata["shotmap_match_status"], "missing")
        self.assertEqual(missing.metadata["shotmap_match_reason"], "no_candidate")
        self.assertEqual(missing.metadata["shotmap_candidate_details"], [])
        self.assertEqual(invalid.metadata["shotmap_match_status"], "invalid")
        self.assertEqual(invalid.metadata["shotmap_invalid_reason"], "invalid_second")
        self.assertEqual(malformed.metadata["shotmap_match_status"], "invalid")

    def test_cumulative_second_must_match_the_shot_minute(self):
        invalid = associate_shotmap_second(
            self.goal(),
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 152,
                    }
                ]
            },
        )

        self.assertIsNone(invalid.second)
        self.assertEqual(invalid.metadata["shotmap_match_status"], "invalid")
        self.assertEqual(
            invalid.metadata["shotmap_invalid_reason"],
            "second_minute_mismatch",
        )

    def test_source_retries_missing_goal_and_returns_later_second(self):
        source = HttpShotmapSecondSource(
            "https://example.test/shotmap",
            "match-1",
            "user@example.test",
            timeout=1,
            retry_interval=5,
            wait_seconds=40,
        )
        empty = FakeHttpResponse({"shots": []})
        matched = FakeHttpResponse(
            {
                "shots": [
                    {
                        "outcome": "goal",
                        "minute": 69,
                        "minute_extra": 0,
                        "person_id": 9,
                        "second": 4177,
                    }
                ]
            }
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            side_effect=[empty, matched],
        ) as urlopen:
            self.assertIsNone(source.poll(self.goal(), 0.0))
            self.assertIsNone(source.poll(self.goal(), 4.9))
            enriched = source.poll(self.goal(), 5.0)

        self.assertEqual(enriched.second, 4177)
        self.assertEqual(urlopen.call_count, 2)
        request_url = urlopen.call_args.args[0].full_url
        self.assertIn("match_id=match-1", request_url)
        self.assertIn("user=user%40example.test", request_url)

    def test_cached_second_does_not_restore_stale_overview_fields(self):
        source = HttpShotmapSecondSource(
            "https://example.test/shotmap",
            "match-1",
            None,
            timeout=1,
        )
        payload = {
            "shots": [
                {
                    "outcome": "goal",
                    "minute": 69,
                    "minute_extra": 0,
                    "person_id": 9,
                    "second": 4177,
                }
            ]
        }
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            return_value=FakeHttpResponse(payload),
        ):
            original = source.poll(self.goal(score="1-0"), 0.0)
            refreshed = source.poll(self.goal(score="2-0"), 0.1)

        self.assertEqual(original.second, 4177)
        self.assertEqual(refreshed.second, 4177)
        self.assertEqual(refreshed.score, "2-0")

    def test_overview_association_revision_does_not_reuse_stale_second(self):
        source = HttpShotmapSecondSource(
            "https://example.test/shotmap",
            "match-1",
            None,
            timeout=1,
            retry_interval=5,
        )
        first_payload = {
            "shots": [
                {
                    "outcome": "goal",
                    "minute": 69,
                    "minute_extra": 0,
                    "person_id": 9,
                    "second": 4177,
                }
            ]
        }
        revised_payload = {
            "shots": [
                {
                    "outcome": "goal",
                    "minute": 70,
                    "minute_extra": 0,
                    "person_id": 9,
                    "second": 4234,
                }
            ]
        }
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            side_effect=[
                FakeHttpResponse(first_payload),
                FakeHttpResponse(revised_payload),
            ],
        ):
            original = source.poll(self.goal(), 0.0)
            revised = source.poll(replace(original, minute="70"), 5.0)

        self.assertEqual(original.second, 4177)
        self.assertEqual(revised.second, 4234)
        self.assertEqual(
            revised.metadata["shotmap_association_signature"]["minute"],
            "70",
        )

    def test_source_releases_original_minute_path_after_wait_window(self):
        source = HttpShotmapSecondSource(
            "https://example.test/shotmap",
            "match-1",
            None,
            timeout=1,
            retry_interval=5,
            wait_seconds=40,
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            return_value=FakeHttpResponse({"shots": []}),
        ):
            self.assertIsNone(source.poll(self.goal(), 0.0))
            final = source.poll(self.goal(), 40.0)

        self.assertIsNone(final.second)
        self.assertEqual(final.metadata["shotmap_match_status"], "missing")

    def test_cards_bypass_shotmap_without_a_request(self):
        source = HttpShotmapSecondSource(
            "https://example.test/shotmap",
            "match-1",
            None,
            timeout=1,
        )
        card = self.goal(
            event_key="match-1:YC:card-1",
            code="YC",
            event_type="yellow_card",
        )
        with patch("event_driven_pipeline.urllib.request.urlopen") as urlopen:
            self.assertIs(source.poll(card, 0.0), card)
        urlopen.assert_not_called()


class ExitedProcess:
    def poll(self):
        return 1

    def wait(self):
        return 1


class OptionalVisionSchedulingTests(unittest.TestCase):
    def test_scoreboard_profile_file_is_normalized_for_ocr_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "scoreboard.json"
            path.write_text(json.dumps({
                "profile_id": "feed-a",
                "reference_resolution": [1920, 1080],
                "clock_roi": [30, 20, 180, 70],
                "score_roi": [190, 20, 330, 70],
                "second_half_clock_mode": "continuous",
            }), encoding="utf-8")

            profile = load_scoreboard_profile(path)

        self.assertEqual(profile["profile_id"], "feed-a")
        self.assertEqual(profile["clock_roi"], [30, 20, 180, 70])
        self.assertEqual(profile["score_roi"], [190, 20, 330, 70])

    def test_terminal_failed_encode_does_not_become_encoded_in_memory(self):
        event = MatchEvent(
            "match:G:failed", "G", "goal", "35", "0", "team", "", "", "1-0", ""
        )
        pending = PendingEvent(
            event_type="goal", stream_time=10.0, source_time=None,
            detected_wall_time=0.0, change_fraction=0.0,
            stability_fraction=0.0, output_due_stream_time=12.0,
        )
        job = EventJob(event, pending, 10.0, None)
        stored = SimpleNamespace(status="failed", result={"error_kind": "video_gap"})
        runtime = SimpleNamespace(store=SimpleNamespace(get=Mock(return_value=stored)))

        sync_completed_default_job(job, runtime, event.event_key, completed=True)

        self.assertEqual(job.pending.status, "failed")
        self.assertEqual(job.pending.result["error_kind"], "video_gap")

    def test_visual_job_uses_latest_event_revision_before_submission(self):
        vision_job = SimpleNamespace(
            code="G", event_type="goal",
            event_minute="35", event_minute_extra="0", target_score="4-0",
            observed_anchor_stream_time=100.0, observed_anchor_source_time=200.0,
        )
        default_task = SimpleNamespace(
            event_data={
                "code": "PG",
                "event_type": "goal",
                "minute": "41",
                "minute_extra": "0",
                "second": 2473,
                "score": "5-0",
            },
            observed_stream_time=120.0,
            observed_source_time=220.0,
        )

        refresh_vision_job_event_data(vision_job, default_task)

        self.assertEqual(vision_job.code, "PG")
        self.assertEqual(vision_job.event_type, "goal")
        self.assertEqual(vision_job.event_minute, "41")
        self.assertEqual(vision_job.event_second, 2473)
        self.assertEqual(vision_job.target_score, "5-0")
        self.assertEqual(vision_job.observed_anchor_stream_time, 120.0)

    def test_late_shotmap_second_increments_target_revision_once(self):
        vision_job = VisionJob(
            event_key="match-1:G:late",
            match_id="match-1",
            code="G",
            event_type="goal",
            default_anchor_stream_time=100.0,
            default_anchor_source_time=None,
            detected_at_unix=1.0,
            event_minute="90",
            event_second=None,
        )
        overview_task = SimpleNamespace(
            event_data={
                "code": "G",
                "event_type": "goal",
                "minute": "90",
                "minute_extra": "0",
                "second": None,
                "metadata": {"event_source": {"primary": "overview"}},
            },
            observed_stream_time=100.0,
            observed_source_time=None,
        )
        self.assertIsNone(refresh_vision_job_event_data(vision_job, overview_task))

        shotmap_task = SimpleNamespace(
            event_data={
                **overview_task.event_data,
                "second": 5353,
                "metadata": {"event_source": {"primary": "shotmap"}},
            },
            observed_stream_time=108.0,
            observed_source_time=None,
        )
        upgrade = refresh_vision_job_event_data(vision_job, shotmap_task)
        self.assertEqual(upgrade["target_revision"], 1)
        self.assertEqual(upgrade["target_clock_seconds"], 5353)
        self.assertEqual(vision_job.target_source, "shotmap")
        self.assertIsNone(refresh_vision_job_event_data(vision_job, shotmap_task))
        self.assertEqual(vision_job.target_revision, 1)

        changed_shotmap_task = SimpleNamespace(
            event_data={
                **shotmap_task.event_data,
                "second": 5354,
            },
            observed_stream_time=109.0,
            observed_source_time=None,
        )
        changed = refresh_vision_job_event_data(
            vision_job, changed_shotmap_task
        )
        self.assertEqual(changed["target_revision"], 2)
        self.assertEqual(changed["target_clock_seconds"], 5354)

    def test_direct_shotmap_target_does_not_count_as_late_upgrade(self):
        vision_job = VisionJob(
            event_key="match-1:G:direct",
            match_id="match-1",
            code="G",
            event_type="goal",
            default_anchor_stream_time=100.0,
            default_anchor_source_time=None,
            detected_at_unix=1.0,
            event_minute="90",
            event_second=5353,
            target_kind="exact_second",
            target_source="shotmap",
        )
        task = SimpleNamespace(
            event_data={
                "code": "G",
                "event_type": "goal",
                "minute": "90",
                "minute_extra": "0",
                "second": 5353,
                "metadata": {"event_source": {"primary": "shotmap"}},
            },
            observed_stream_time=100.0,
            observed_source_time=None,
        )
        self.assertIsNone(refresh_vision_job_event_data(vision_job, task))
        self.assertEqual(vision_job.target_revision, 0)

    def test_target_upgrade_resets_pending_artifact_and_persists_progress(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match-1:G:reset"
            runtime.discover_task(
                match_id="match-1",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "90",
                    "minute_extra": "0",
                    "team": "team",
                    "person": "player",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {"event_source": {"primary": "overview"}},
                },
                observed_stream_time=100.0,
                observed_source_time=None,
                clip_anchor_stream_time=90.0,
                clip_anchor_source_time=None,
                output_due_stream_time=110.0,
                detected_at_unix=1.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=0.0,
                search_end_stream_time=220.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
            )
            vision_job = VisionJob(
                event_key=event_key,
                match_id="match-1",
                code="G",
                event_type="goal",
                default_anchor_stream_time=90.0,
                default_anchor_source_time=None,
                detected_at_unix=1.0,
            )
            self.assertTrue(
                reset_vision_artifact_for_target_upgrade(
                    runtime,
                    vision_job,
                    "ocr_window",
                    {
                        "target_revision": 1,
                        "target_kind": "exact_second",
                        "target_source": "shotmap",
                        "target_clock_seconds": 5353,
                        "reason": "shotmap_second_upgrade",
                    },
                )
            )
            task = runtime.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(task.status, "pending")
            self.assertEqual(
                task.window_metadata["progressive_scan"]["target_revision"], 1
            )
            self.assertFalse(
                reset_vision_artifact_for_target_upgrade(
                    runtime,
                    vision_job,
                    "ocr_window",
                    {
                        "target_revision": 1,
                        "target_kind": "exact_second",
                        "target_source": "shotmap",
                        "target_clock_seconds": 5353,
                    },
                )
            )
            runtime.close()

    def test_target_upgrade_requeues_encoded_artifact_for_exact_second(self):
        from pipeline_runtime import PipelineRuntime

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            event_key = "match-1:G:encoded-reset"
            runtime.discover_task(
                match_id="match-1",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "90",
                    "minute_extra": "0",
                    "team": "team",
                    "person": "player",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {"event_source": {"primary": "overview"}},
                },
                observed_stream_time=100.0,
                observed_source_time=None,
                clip_anchor_stream_time=90.0,
                clip_anchor_source_time=None,
                output_due_stream_time=110.0,
                detected_at_unix=1.0,
            )
            runtime.enqueue_vision_task(
                event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=0.0,
                search_end_stream_time=220.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
            )
            runtime.transition_vision_task(event_key, "locating", artifact_kind="ocr_window")
            runtime.transition_vision_task(
                event_key,
                "located",
                artifact_kind="ocr_window",
                result={"anchor_stream_time": 90.0},
            )
            runtime.transition_vision_task(event_key, "encoding", artifact_kind="ocr_window")
            runtime.transition_vision_task(
                event_key,
                "encoded",
                artifact_kind="ocr_window",
                result={"output": "/tmp/minute.gif", "bytes": 10},
            )
            job = VisionJob(
                event_key=event_key,
                match_id="match-1",
                code="G",
                event_type="goal",
                default_anchor_stream_time=90.0,
                default_anchor_source_time=None,
                detected_at_unix=1.0,
            )
            self.assertTrue(
                reset_vision_artifact_for_target_upgrade(
                    runtime,
                    job,
                    "ocr_window",
                    {
                        "target_revision": 1,
                        "target_kind": "exact_second",
                        "target_source": "shotmap",
                        "target_clock_seconds": 5353,
                        "reason": "shotmap_second_upgrade",
                    },
                )
            )
            task = runtime.store.get_vision_task(event_key, "ocr_window")
            self.assertEqual(task.status, "pending")
            self.assertEqual(task.result["superseded_output"], "/tmp/minute.gif")
            runtime.close()


class ReconnectingSupervisor:
    last_kwargs = None

    def __init__(self, *args, **kwargs):
        ReconnectingSupervisor.last_kwargs = kwargs
        self.process = ExitedProcess()
        self.generation = -1
        self.restart_count = 0

    def start(self, now_monotonic=None):
        self.generation += 1
        return self.process

    def observe_exit(self):
        self.process = None
        self.restart_count += 1
        return ProcessExit(1, True, 30.0, 1)

    def terminate(self):
        pass

    def note_media_progress(self):
        pass

    def close(self):
        pass


class GracefullyStoppedProcess:
    def __init__(self):
        self.return_code = None

    def poll(self):
        return self.return_code

    def wait(self):
        return self.return_code


class GracefulStopSupervisor:
    instance = None

    def __init__(self, *args, **kwargs):
        del args, kwargs
        self.process = GracefullyStoppedProcess()
        self.generation = -1
        self.restart_count = 0
        self.reconnect = True
        self.terminated = False
        GracefulStopSupervisor.instance = self

    def start(self, now_monotonic=None):
        del now_monotonic
        self.generation += 1
        return self.process

    def observe_exit(self):
        if self.process is None or self.process.poll() is None:
            return None
        return_code = self.process.return_code
        self.process = None
        return ProcessExit(return_code, False, 0.0, 0)

    def terminate(self):
        self.terminated = True
        if self.process is not None:
            self.process.return_code = -15

    def note_media_progress(self):
        pass

    def close(self):
        pass


class EventParsingTests(unittest.TestCase):
    def test_visual_submission_is_independent_from_default_gif_state(self):
        pending_ocr = SimpleNamespace(
            status="pending",
            next_attempt_at_unix=0.0,
            search_end_stream_time=100.0,
            deadline_at_unix=2000.0,
        )
        self.assertTrue(
            vision_artifact_ready_for_submission(
                "ocr_window",
                pending_ocr,
                None,
                stream_time=103.0,
                segment_slack=2.0,
                now_unix=1000.0,
            )
        )

        pending_tdeed = SimpleNamespace(
            status="pending",
            next_attempt_at_unix=0.0,
        )
        self.assertFalse(
            vision_artifact_ready_for_submission(
                "tdeed_refined",
                pending_tdeed,
                pending_ocr,
                stream_time=103.0,
                segment_slack=2.0,
                now_unix=1000.0,
            )
        )
        for upstream_status in ("encoded", "failed"):
            with self.subTest(upstream_status=upstream_status):
                self.assertTrue(
                    vision_artifact_ready_for_submission(
                        "tdeed_refined",
                        pending_tdeed,
                        SimpleNamespace(status=upstream_status),
                        stream_time=103.0,
                        segment_slack=2.0,
                        now_unix=1000.0,
                    )
                )

    def test_visual_pool_keys_are_artifact_specific(self):
        event_key = "match-1:G:event"
        ocr_key = vision_pool_task_key(event_key, "ocr_window")
        tdeed_key = vision_pool_task_key(event_key, "tdeed_refined")

        self.assertNotEqual(ocr_key, tdeed_key)
        self.assertEqual(
            split_vision_pool_task_key(ocr_key),
            (event_key, "ocr_window"),
        )

    def test_default_gif_name_uses_latest_persisted_event_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            event_key = "match-42:OG:abcdef123456"
            old_event = MatchEvent(
                event_key=event_key,
                code="OG",
                event_type="goal",
                minute="89",
                minute_extra="0",
                team="teamA",
                person="Old name",
                person_id="17",
                score="1-1",
                reason="",
            )
            latest_event_data = {
                **old_event.__dict__,
                "minute": "90",
                "minute_extra": "3",
                "person": "王 伟",
                "score": "2-1",
            }
            stored = SimpleNamespace(
                next_attempt_at_unix=0.0,
                deadline_at_unix=10_000_000_000.0,
                match_id="match-42",
                event_data=latest_event_data,
            )
            runtime = SimpleNamespace(
                store=SimpleNamespace(get=Mock(return_value=stored)),
                transition=Mock(),
                record_readiness_wait=Mock(),
                logger=SimpleNamespace(log=Mock()),
            )
            pending = PendingEvent(
                event_type="goal",
                stream_time=5.0,
                source_time=5.0,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=8.0,
            )
            job = EventJob(
                match_event=old_event,
                pending=pending,
                observed_stream_time=5.0,
                observed_source_time=5.0,
            )
            encoded = {
                "output": str(root / "output.gif"),
                "bytes": 6,
                "duration_sec": 6.0,
                "encode_seconds": 0.1,
                "over_size_reference": False,
            }

            with patch("event_driven_pipeline.encode_gif", return_value=encoded) as encode:
                completed = encode_event_job(
                    job,
                    runtime,
                    "ffmpeg",
                    "ffprobe",
                    lambda: [Segment(segment_path, 0.0, 10.0)],
                    root,
                    before=3.0,
                    after=3.0,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                )

            self.assertTrue(completed)
            self.assertEqual(
                encode.call_args.kwargs["output_filename"],
                "match-42_m090+03_own-goal_王-伟_2-1_default_abcdef.gif",
            )

    def test_default_gif_uses_observed_anchor_not_match_clock_anchor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            event = MatchEvent(
                event_key="match-1:G:anchor",
                code="G",
                event_type="goal",
                minute="1",
                minute_extra="0",
                team="teamA",
                person="Scorer",
                person_id="9",
                score="1-0",
                reason="",
            )
            stored = SimpleNamespace(
                next_attempt_at_unix=0.0,
                deadline_at_unix=10_000_000_000.0,
                match_id="match-1",
                event_data=event.__dict__,
            )
            runtime = SimpleNamespace(
                store=SimpleNamespace(get=Mock(return_value=stored)),
                transition=Mock(),
                record_readiness_wait=Mock(),
                logger=SimpleNamespace(log=Mock()),
            )
            pending = PendingEvent(
                event_type="goal",
                stream_time=80.0,
                source_time=None,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=107.0,
            )
            job = EventJob(
                match_event=event,
                pending=pending,
                observed_stream_time=80.0,
                observed_source_time=None,
                match_clock_anchor_stream_time=5.0,
            )
            encoded = {
                "output": str(root / "output.gif"),
                "bytes": 6,
                "duration_sec": 30.0,
                "encode_seconds": 0.1,
                "over_size_reference": False,
            }

            with patch(
                "event_driven_pipeline.encode_gif", return_value=encoded
            ) as encode:
                completed = encode_event_job(
                    job,
                    runtime,
                    "ffmpeg",
                    "ffprobe",
                    lambda: [Segment(segment_path, 0.0, 120.0)],
                    root,
                    before=10.0,
                    after=20.0,
                    width=384,
                    fps=6.0,
                    colors=160,
                    size_reference_bytes=10_000_000,
                )

            self.assertTrue(completed)
            encoded_pending = encode.call_args.args[3]
            self.assertIs(encoded_pending, pending)
            self.assertEqual(encoded_pending.stream_time, 80.0)
            self.assertEqual(encode.call_args.kwargs["before"], 10.0)
            self.assertEqual(encode.call_args.kwargs["after"], 20.0)
            coverage = encode.call_args.kwargs["coverage"]
            self.assertEqual(coverage.requested_start, 70.0)
            self.assertEqual(coverage.requested_end, 100.0)
            self.assertEqual(coverage.anchor, 80.0)

    def test_event_timing_diagnostics_preserve_discovery_sample(self):
        sample = {
            "api_request_started_at_unix": 1000.0,
            "api_request_finished_at_unix": 1000.25,
            "api_request_duration_seconds": 0.25,
            "api_request_succeeded": True,
            "first_observed_wall_time_unix": 1000.3,
            "first_observed_stream_time_sec": 80.0,
            "media_tail_stream_time_sec": 77.5,
            "media_tail_lag_seconds": 2.5,
            "event_to_video_offset_seconds": -15.0,
            "clip_anchor_stream_time_sec": 65.0,
            "requested_clip_start_stream_time_sec": 20.0,
            "requested_clip_end_stream_time_sec": 80.0,
        }
        event = MatchEvent(
            event_key="match-1:G:timing",
            code="G",
            event_type="goal",
            minute="1",
            minute_extra="0",
            team="teamA",
            person="Scorer",
            person_id="9",
            score="1-0",
            reason="",
            metadata={"timing_diagnostics": sample},
        )
        pending = PendingEvent(
            event_type="goal",
            stream_time=65.0,
            source_time=None,
            detected_wall_time=1000.3,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=87.0,
        )
        job = EventJob(event, pending, 80.0, None)

        self.assertEqual(
            event_timing_diagnostics(job, before=10.0, after=20.0),
            sample,
        )

    def test_event_revision_keeps_first_observation_diagnostics(self):
        sample = {"first_observed_stream_time_sec": 80.0}
        current = MatchEvent(
            event_key="match:G:1",
            code="G",
            event_type="goal",
            minute="10",
            minute_extra="0",
            team="teamA",
            person="",
            person_id="0",
            score="1-0",
            reason="",
            metadata={"timing_diagnostics": sample},
        )
        update = replace(
            current,
            person="Scorer",
            person_id="7",
            metadata={"bucket": "10"},
        )

        merged = merge_observed_event_revision(current, update)

        self.assertEqual(merged.person, "Scorer")
        self.assertEqual(merged.person_id, "7")
        self.assertEqual(merged.metadata["bucket"], "10")
        self.assertEqual(merged.metadata["timing_diagnostics"], sample)

    def test_only_new_nonempty_segments_reset_ingest_backoff(self):
        class Supervisor:
            def __init__(self):
                self.progress_calls = 0

            def note_media_progress(self):
                self.progress_calls += 1

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_path = root / "old.ts"
            new_path = root / "new.ts"
            empty_path = root / "empty.ts"
            missing_path = root / "missing.ts"
            old_path.write_bytes(b"old media")
            new_path.write_bytes(b"new media")
            empty_path.write_bytes(b"")
            segments = [
                Segment(old_path, 0.0, 2.0),
                Segment(new_path, 2.0, 4.0),
                Segment(empty_path, 4.0, 6.0),
                Segment(missing_path, 6.0, 8.0),
            ]
            observed = {str(old_path.resolve())}
            supervisor = Supervisor()

            self.assertEqual(
                observe_segment_progress(supervisor, segments, observed),
                1,
            )
            self.assertEqual(supervisor.progress_calls, 1)
            self.assertIn(str(new_path.resolve()), observed)
            self.assertNotIn(str(empty_path.resolve()), observed)
            self.assertNotIn(str(missing_path.resolve()), observed)

            self.assertEqual(
                observe_segment_progress(supervisor, segments, observed),
                0,
            )
            self.assertEqual(supervisor.progress_calls, 1)

    def test_observed_segment_paths_are_bounded_to_recent_media(self):
        class Supervisor:
            def __init__(self):
                self.progress_calls = 0

            def note_media_progress(self):
                self.progress_calls += 1

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            segments = []
            for index in range(4):
                path = root / f"segment-{index}.ts"
                path.write_bytes(b"media")
                paths.append(path)
                segments.append(Segment(path, float(index), float(index + 1)))
            observed = set()
            supervisor = Supervisor()

            self.assertEqual(
                observe_segment_progress(
                    supervisor,
                    segments,
                    observed,
                    max_observed_paths=2,
                ),
                2,
            )
            self.assertEqual(len(observed), 2)
            self.assertEqual(
                observed,
                {str(paths[2].resolve()), str(paths[3].resolve())},
            )

    def test_closed_empty_generation_is_removed_and_csv_is_compacted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            buffer_dir = root / "buffer"
            buffer_dir.mkdir()
            valid_media = buffer_dir / "valid.ts"
            valid_media.write_bytes(b"media")
            compacted = buffer_dir / "closed.csv"
            compacted.write_text(
                "valid.ts,0,1\nmissing.ts,1,2\n",
                encoding="utf-8",
            )
            stale = buffer_dir / "stale.csv"
            stale.write_text("missing.ts,0,1\n", encoding="utf-8")
            manifest_path = buffer_dir / "segment_manifest.json"
            manifest = new_segment_manifest("match-1", "source", 1000.0)
            manifest = upsert_segment_generation(
                manifest,
                list_path=Path("closed.csv"),
                stream_offset=0.0,
                started_at_wall=1000.0,
            )
            manifest = upsert_segment_generation(
                manifest,
                list_path=Path("stale.csv"),
                stream_offset=1.0,
                started_at_wall=1001.0,
            )
            save_segment_manifest(manifest_path, manifest)
            generations = [
                SegmentGeneration(compacted, 0.0),
                SegmentGeneration(stale, 1.0),
            ]

            updated, removed, rows = maintain_segment_generations(
                generations,
                manifest,
                manifest_path=manifest_path,
                buffer_dir=buffer_dir,
                active_list_path=None,
            )

            self.assertEqual(removed, 1)
            self.assertEqual(rows, 1)
            self.assertEqual(len(generations), 1)
            self.assertEqual(len(updated.generations), 1)
            self.assertEqual(compacted.read_text(encoding="utf-8"), "valid.ts,0,1\n")
            self.assertFalse(stale.exists())

    def test_terminal_jobs_are_evicted_after_durable_completion(self):
        event = MatchEvent(
            event_key="match-1:G:terminal",
            code="G",
            event_type="goal",
            minute="1",
            minute_extra="0",
            team="teamA",
            person="Scorer",
            person_id="9",
            score="1-0",
            reason="",
        )
        pending = PendingEvent(
            event_type="goal",
            stream_time=10.0,
            source_time=None,
            detected_wall_time=100.0,
            change_fraction=0.0,
            stability_fraction=0.0,
            output_due_stream_time=12.0,
            status="encoded",
            result={"output": "/tmp/goal.gif"},
        )
        job = EventJob(event, pending, 10.0, None)
        vision_job = VisionJob(
            event_key=event.event_key,
            match_id="match-1",
            code="G",
            event_type="goal",
            default_anchor_stream_time=10.0,
            default_anchor_source_time=None,
            detected_at_unix=100.0,
        )
        visual_statuses = {
            "ocr_window": "encoded",
            "tdeed_refined": "pending",
        }
        runtime = SimpleNamespace(
            store=SimpleNamespace(
                get=lambda key: SimpleNamespace(status="encoded")
                if key == event.event_key else None,
                get_vision_task=lambda key, artifact_kind=None: SimpleNamespace(
                    status=visual_statuses[artifact_kind]
                )
                if key == event.event_key else None,
            )
        )
        jobs = [job]
        vision_jobs = {event.event_key: vision_job}
        contexts = {}

        self.assertEqual(
            evict_terminal_runtime_jobs(
                jobs,
                vision_jobs,
                runtime,
                contexts,
                before=10.0,
                after=20.0,
            ),
            (1, 0),
        )
        self.assertEqual(jobs, [])
        self.assertEqual(vision_jobs, {event.event_key: vision_job})
        self.assertEqual(
            contexts[event.event_key]["clip_anchor_stream_time_sec"],
            10.0,
        )

        visual_statuses["tdeed_refined"] = "failed"
        self.assertEqual(
            evict_terminal_runtime_jobs(
                jobs,
                vision_jobs,
                runtime,
                contexts,
                before=10.0,
                after=20.0,
            ),
            (0, 1),
        )
        self.assertEqual(vision_jobs, {})

    def test_match_start_play_defaults_naive_values_to_beijing(self):
        expected = parse_match_start_play("2026-05-20T11:00:00+08:00")
        self.assertEqual(
            parse_match_start_play("2026-05-20 11:00:00"),
            expected,
        )
        self.assertEqual(parse_match_start_play(str(expected)), expected)
        self.assertIsNone(parse_match_start_play(None))
        with self.assertRaisesRegex(ValueError, "match start"):
            parse_match_start_play("not-a-date")

    def test_match_start_play_supports_explicit_naive_timezone(self):
        expected_utc = parse_match_start_play("2026-05-20T11:00:00Z")
        self.assertEqual(
            parse_match_start_play(
                "2026-05-20 11:00:00",
                naive_timezone="utc",
            ),
            expected_utc,
        )
        self.assertEqual(
            parse_match_start_play(
                "2026-05-20T11:00:00+08:00",
                naive_timezone="utc",
            ),
            parse_match_start_play("2026-05-20T11:00:00+08:00"),
        )
        self.assertEqual(
            parse_match_start_play(str(expected_utc), naive_timezone="utc"),
            expected_utc,
        )
        with self.assertRaisesRegex(ValueError, "naive timezone"):
            parse_match_start_play(
                "2026-05-20 11:00:00",
                naive_timezone="local",
            )

    def test_goal_feed_revisions_keep_one_canonical_event_key(self):
        snapshots = [
            {
                "events": {
                    "5": {
                        "minute": "5",
                        "teamAEvents": [
                            {"code": "G", "person_id": "0", "score": "1-0"}
                        ],
                    }
                }
            },
            {
                "events": {
                    "5": {
                        "minute": "5",
                        "teamAEvents": [
                            {
                                "code": "G",
                                "person": "Miguel Murillo",
                                "person_id": "50895934",
                                "score": "1-0",
                            }
                        ],
                    }
                }
            },
            {
                "events": {
                    "4": {
                        "minute": "4",
                        "teamAEvents": [
                            {
                                "code": "G",
                                "person": "Miguel Murillo",
                                "person_id": "50895934",
                                "score": "1-0",
                            }
                        ],
                    }
                }
            },
        ]
        tracker = EventRevisionTracker()
        revisions = [
            tracker.reconcile(parse_match_events(snapshot, "54478922"))[0]
            for snapshot in snapshots
        ]
        self.assertEqual(len({event.event_key for event in revisions}), 1)
        self.assertEqual(revisions[-1].minute, "4")
        self.assertEqual(revisions[-1].person, "Miguel Murillo")

    def test_empty_yellow_card_replaced_by_player_keeps_canonical_key(self):
        empty = {
            "events": {
                "81": {
                    "minute": "81",
                    "teamAEvents": [{"code": "YC", "person_id": "0"}],
                }
            }
        }
        completed = {
            "events": {
                "81": {
                    "minute": "81",
                    "teamAEvents": [
                        {
                            "code": "YC",
                            "person": "Raul Torres",
                            "person_id": "50405792",
                        }
                    ],
                }
            }
        }
        tracker = EventRevisionTracker()
        original = tracker.reconcile(
            parse_match_events(empty, "54507611"),
            observed_at_unix=1000.0,
        )[0]
        revision = tracker.reconcile(
            parse_match_events(completed, "54507611"),
            observed_at_unix=1094.54,
        )[0]

        self.assertEqual(revision.event_key, original.event_key)
        self.assertEqual(revision.person, "Raul Torres")
        self.assertEqual(revision.person_id, "50405792")

    def test_yellow_card_replacement_requires_a_unique_new_candidate(self):
        tracker = EventRevisionTracker()
        original = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [{"code": "YC", "person_id": "0"}],
                    }
                }
            },
            "match-1",
        )[0]
        tracker.reconcile([original], observed_at_unix=1000.0)
        candidates = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {"code": "YC", "person": "A", "person_id": "1"},
                            {"code": "YC", "person": "B", "person_id": "2"},
                        ],
                    }
                }
            },
            "match-1",
        )
        reconciled = tracker.reconcile(candidates, observed_at_unix=1094.54)

        self.assertEqual(len({event.event_key for event in reconciled}), 2)
        self.assertNotIn(original.event_key, {event.event_key for event in reconciled})

    def test_yellow_card_replacement_rejects_ambiguous_old_occurrences(self):
        tracker = EventRevisionTracker()
        originals = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {"code": "YC", "person_id": "0"},
                            {"code": "YC", "person_id": "0"},
                        ],
                    }
                }
            },
            "match-1",
        )
        tracker.reconcile(originals, observed_at_unix=1000.0)
        current = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {"code": "YC", "person_id": "0"},
                            {"code": "YC", "person": "A", "person_id": "1"},
                        ],
                    }
                }
            },
            "match-1",
        )
        reconciled = tracker.reconcile(current, observed_at_unix=1094.54)

        self.assertEqual(
            {event.event_key for event in reconciled},
            {event.event_key for event in current},
        )

    def test_yellow_card_replacement_rejects_conflicting_event_ids(self):
        tracker = EventRevisionTracker()
        original = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {"id": "card-a", "code": "YC", "person_id": "0"}
                        ],
                    }
                }
            },
            "match-1",
        )[0]
        tracker.reconcile([original], observed_at_unix=1000.0)
        completed = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {
                                "id": "card-b",
                                "code": "YC",
                                "person": "A",
                                "person_id": "1",
                            }
                        ],
                    }
                }
            },
            "match-1",
        )[0]
        revision = tracker.reconcile([completed], observed_at_unix=1094.54)[0]

        self.assertNotEqual(revision.event_key, original.event_key)

    def test_yellow_card_replacement_requires_old_version_to_disappear(self):
        tracker = EventRevisionTracker()
        original = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [{"code": "YC", "person_id": "0"}],
                    }
                }
            },
            "match-1",
        )[0]
        tracker.reconcile([original], observed_at_unix=1000.0)
        complete = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {"code": "YC", "person_id": "0"},
                            {"code": "YC", "person": "A", "person_id": "1"},
                        ],
                    }
                }
            },
            "match-1",
        )
        reconciled = tracker.reconcile(complete, observed_at_unix=1094.54)

        self.assertEqual(len({event.event_key for event in reconciled}), 2)

    def test_yellow_card_replacement_expires_after_revision_window(self):
        tracker = EventRevisionTracker()
        original = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [{"code": "YC", "person_id": "0"}],
                    }
                }
            },
            "match-1",
        )[0]
        tracker.reconcile([original], observed_at_unix=1000.0)
        completed = parse_match_events(
            {
                "events": {
                    "81": {
                        "minute": "81",
                        "teamAEvents": [
                            {"code": "YC", "person": "A", "person_id": "1"}
                        ],
                    }
                }
            },
            "match-1",
        )[0]
        revision = tracker.reconcile([completed], observed_at_unix=1180.01)[0]

        self.assertNotEqual(revision.event_key, original.event_key)

    def test_same_minute_complete_yellow_cards_remain_separate(self):
        events = parse_match_events(
            {
                "events": {
                    "90": {
                        "minute": "90",
                        "teamAEvents": [
                            {
                                "code": "YC",
                                "minute_extra": "6",
                                "person": "A",
                                "person_id": "1",
                            },
                            {
                                "code": "YC",
                                "minute_extra": "6",
                                "person": "B",
                                "person_id": "2",
                            },
                        ],
                    }
                }
            },
            "match-1",
        )
        reconciled = EventRevisionTracker().reconcile(
            events,
            observed_at_unix=1000.0,
        )

        self.assertEqual(len({event.event_key for event in reconciled}), 2)

    def test_existing_goal_in_same_snapshot_is_not_merged_with_new_goal(self):
        tracker = EventRevisionTracker()
        first = parse_match_events(
            {
                "events": {
                    "5": {
                        "minute": "5",
                        "teamAEvents": [
                            {"code": "G", "person_id": "1", "score": "1-0"}
                        ],
                    }
                }
            },
            "match-1",
        )
        tracker.reconcile(first)
        second = parse_match_events(
            {
                "events": {
                    "5": {
                        "minute": "5",
                        "teamAEvents": [
                            {"code": "G", "person_id": "1", "score": "1-0"}
                        ],
                    },
                    "6": {
                        "minute": "6",
                        "teamAEvents": [
                            {"code": "G", "person_id": "2", "score": "2-0"}
                        ],
                    },
                }
            },
            "match-1",
        )
        reconciled = tracker.reconcile(second)
        self.assertEqual(len({event.event_key for event in reconciled}), 2)

    def test_adjacent_goal_seconds_never_reuse_the_previous_canonical_key(self):
        for later_score in ("1-0", ""):
            with self.subTest(later_score=later_score):
                tracker = EventRevisionTracker()
                first = MatchEvent(
                    event_key="match-1:G:first",
                    code="G",
                    event_type="goal",
                    minute="10",
                    minute_extra="0",
                    team="teamA",
                    person="",
                    person_id="0",
                    score="1-0",
                    reason="",
                    second=601,
                    metadata={"second_source": "shotmap"},
                )
                later = replace(
                    first,
                    event_key="match-1:G:later",
                    minute="11",
                    score=later_score,
                    second=659,
                )
                tracker.reconcile([first])
                reconciled = tracker.reconcile([later])

                self.assertEqual(reconciled[0].event_key, later.event_key)
                self.assertEqual(len(tracker.canonical_events), 2)

    def test_same_snapshot_goal_versions_are_merged_before_task_creation(self):
        tracker = EventRevisionTracker()
        snapshot = {
            "events": {
                "5": {
                    "minute": "5",
                    "teamAEvents": [
                        {"code": "G", "person_id": "0", "score": "1-0"},
                        {
                            "code": "G",
                            "person": "Miguel Murillo",
                            "person_id": "50895934",
                            "score": "1-0",
                        },
                    ],
                }
            }
        }
        reconciled = tracker.reconcile(parse_match_events(snapshot, "match-1"))
        self.assertEqual(len({event.event_key for event in reconciled}), 1)
        self.assertEqual(reconciled[-1].person, "Miguel Murillo")

    def test_goal_score_correction_updates_the_existing_incident(self):
        tracker = EventRevisionTracker()
        first = {
            "events": {
                "5": {
                    "minute": "5",
                    "teamAEvents": [
                        {
                            "code": "G",
                            "person": "Miguel Murillo",
                            "person_id": "50895934",
                            "score": "1-0",
                        }
                    ],
                }
            }
        }
        corrected = {
            "events": {
                "4": {
                    "minute": "4",
                    "teamAEvents": [
                        {
                            "code": "G",
                            "person": "Miguel Murillo",
                            "person_id": "50895934",
                            "score": "2-0",
                        }
                    ],
                }
            }
        }
        original = tracker.reconcile(parse_match_events(first, "match-1"))[0]
        revision = tracker.reconcile(parse_match_events(corrected, "match-1"))[0]
        self.assertEqual(revision.event_key, original.event_key)
        self.assertEqual(revision.minute, "4")
        self.assertEqual(revision.score, "2-0")

    def test_explicit_event_ids_do_not_depend_on_api_array_order(self):
        def payload(events):
            return {
                "events": {
                    "18": {
                        "minute": "18",
                        "teamAEvents": events,
                    }
                }
            }

        event_a = {"id": "goal-a", "code": "G", "person_id": "1", "score": "1-0"}
        event_b = {"id": "goal-b", "code": "G", "person_id": "2", "score": "2-0"}
        first = parse_match_events(payload([event_a, event_b]), "match-1")
        second = parse_match_events(payload([event_b, event_a]), "match-1")
        first_keys = {
            event.metadata["id"]: event.event_key for event in first
        }
        second_keys = {
            event.metadata["id"]: event.event_key for event in second
        }
        self.assertEqual(first_keys, second_keys)

        tracker = EventRevisionTracker()
        tracker.reconcile(first)
        reconciled = tracker.reconcile(second)
        self.assertEqual(
            {event.metadata["id"]: event.event_key for event in reconciled},
            first_keys,
        )

    def test_same_score_goals_far_apart_are_not_merged_without_event_id(self):
        tracker = EventRevisionTracker()
        first = parse_match_events(
            {
                "events": {
                    "5": {
                        "minute": "5",
                        "teamAEvents": [{"code": "G", "score": "1-0"}],
                    }
                }
            },
            "match-1",
        )
        later = parse_match_events(
            {
                "events": {
                    "42": {
                        "minute": "42",
                        "teamAEvents": [{"code": "G", "score": "1-0"}],
                    }
                }
            },
            "match-1",
        )
        tracker.reconcile(first)
        reconciled = tracker.reconcile(later)
        self.assertEqual(len(tracker.canonical_events), 2)
        self.assertNotEqual(reconciled[0].event_key, first[0].event_key)

    def test_goal_yellow_and_red_card_codes_are_supported(self):
        payload = {
            "events": {
                "18": {
                    "minute": "18",
                    "teamAEvents": [
                        {"code": "G", "person": "A", "person_id": "1"},
                        {"code": "YC", "person": "B", "person_id": "2"},
                    ],
                    "teamBEvents": [
                        {"code": "RC", "person": "C", "person_id": "3"}
                    ],
                }
            }
        }
        events = parse_match_events(payload, "match-1")
        self.assertEqual([event.code for event in events], ["G", "YC", "RC"])
        self.assertEqual(
            [event.event_type for event in events],
            ["goal", "yellow_card", "red_card"],
        )

    def test_own_goal_is_treated_as_a_goal(self):
        payload = {
            "events": {
                "42": {
                    "minute": "42",
                    "teamAEvents": [{"code": "OG", "person": "A", "person_id": "1"}],
                    "teamBEvents": [],
                }
            }
        }
        events = parse_match_events(payload, "match-1")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].code, "OG")
        self.assertEqual(events[0].event_type, "goal")

    def test_penalty_goal_is_preserved_as_pg_and_penalty_miss_is_excluded(self):
        payload = {
            "events": {
                "42": {
                    "minute": "42",
                    "teamAEvents": [
                        {"code": "PG", "person": "A", "person_id": "1"},
                        {"code": "PM", "person": "B", "person_id": "2"},
                    ],
                    "teamBEvents": [],
                }
            }
        }

        events = parse_match_events(payload, "match-1")

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].code, "PG")
        self.assertEqual(events[0].event_type, "goal")
        self.assertIn(":PG:", events[0].event_key)

    def test_mock_source_emits_once_after_configured_delay(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.json"
            path.write_text(
                json.dumps(
                    {
                        "events": [
                            {
                                "code": "RC",
                                "source_time_sec": 10,
                                "notification_delay_sec": 3,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            source = MockMatchEventSource(path, "match-1", 0)
            self.assertEqual(source.poll(12.9, 0), [])
            emitted = source.poll(13.0, 0)
            self.assertEqual(len(emitted), 1)
            self.assertEqual(emitted[0].code, "RC")
            self.assertEqual(source.poll(20, 0), [])

    def test_http_source_seeds_history_and_only_emits_new_events(self):
        first_payload = {
            "events": {
                "18": {
                    "minute": "18",
                    "teamAEvents": [
                        {"code": "G", "person_id": "1", "person": "A"}
                    ],
                    "teamBEvents": [],
                }
            }
        }
        second_payload = {
            "events": {
                **first_payload["events"],
                "28": {
                    "minute": "28",
                    "teamAEvents": [],
                    "teamBEvents": [
                        {"code": "RC", "person_id": "2", "person": "B"}
                    ],
                },
            }
        }
        source = HttpMatchEventSource(
            "https://example.test/match/{match_id}",
            "match-1",
            "user@example.test",
            poll_interval=5,
            emit_existing=False,
            timeout=1,
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            side_effect=[FakeHttpResponse(first_payload), FakeHttpResponse(second_payload)],
        ):
            self.assertEqual(source.poll(0, 0), [])
            emitted = source.poll(5, 5)
        self.assertEqual([event.code for event in emitted], ["RC"])
        self.assertEqual(source.seen, {
            event.event_key for event in parse_match_events(second_payload, "match-1")
        })

    def test_http_source_reports_completed_yellow_card_as_update_only(self):
        empty = {
            "events": {
                "81": {
                    "minute": "81",
                    "teamAEvents": [{"code": "YC", "person_id": "0"}],
                }
            }
        }
        completed = {
            "events": {
                "81": {
                    "minute": "81",
                    "teamAEvents": [
                        {
                            "code": "YC",
                            "person": "Raul Torres",
                            "person_id": "50405792",
                        }
                    ],
                }
            }
        }
        source = HttpMatchEventSource(
            "https://example.test/match/{match_id}",
            "54507611",
            None,
            poll_interval=5,
            emit_existing=True,
            timeout=1,
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            side_effect=[FakeHttpResponse(empty), FakeHttpResponse(completed)],
        ):
            original = source.poll(0, 0)
            revision = source.poll(5, 5)

        self.assertEqual(len(original), 1)
        self.assertEqual(revision, [])
        self.assertEqual(len(source.updated_events), 1)
        self.assertEqual(
            source.updated_events[0].event_key,
            original[0].event_key,
        )
        self.assertEqual(source.updated_events[0].person, "Raul Torres")

    def test_http_source_accepts_empty_event_array(self):
        source = HttpMatchEventSource(
            "https://example.test/match/{match_id}",
            "match-1",
            None,
            poll_interval=5,
            emit_existing=False,
            timeout=1,
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            return_value=FakeHttpResponse({"status": 0, "events": []}),
        ):
            self.assertEqual(source.poll(0, 0), [])
        self.assertTrue(source.initialized)
        self.assertEqual(source.error_count, 0)
        self.assertIsNone(source.last_error)

    def test_http_source_accepts_all_success_status_variants(self):
        for status_payload in (
            {"events": {}},
            {"status": None, "events": {}},
            {"status": 0, "events": {}},
            {"status": "0", "events": {}},
        ):
            with self.subTest(payload=status_payload):
                source = HttpMatchEventSource(
                    "https://example.test/match/{match_id}",
                    "match-1",
                    None,
                    poll_interval=5,
                    emit_existing=False,
                    timeout=1,
                )
                with patch(
                    "event_driven_pipeline.urllib.request.urlopen",
                    return_value=FakeHttpResponse(status_payload),
                ):
                    self.assertEqual(source.poll(0, 0), [])
                self.assertTrue(source.initialized)
                self.assertEqual(source.error_count, 0)
                self.assertIsNone(source.last_error)

    def test_http_source_reports_request_wall_time_and_duration(self):
        source = HttpMatchEventSource(
            "https://example.test/match/{match_id}",
            "match-1",
            None,
            poll_interval=5,
            emit_existing=False,
            timeout=1,
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            return_value=FakeHttpResponse({"status": 0, "events": {}}),
        ):
            source.poll(0, 0)

        report = source.report()
        self.assertTrue(report["last_request_succeeded"])
        self.assertIsNotNone(report["last_request_started_at_unix"])
        self.assertGreaterEqual(
            report["last_request_finished_at_unix"],
            report["last_request_started_at_unix"],
        )
        self.assertGreaterEqual(report["last_request_duration_seconds"], 0.0)

    def test_http_source_rejects_invalid_status_and_events_shape(self):
        invalid_payloads = [
            {"status": 1, "events": {}},
            {"status": "1", "events": {}},
            {"status": True, "events": {}},
            {"status": False, "events": {}},
            {"status": 0, "events": None},
            {"status": 0, "events": True},
            {"status": 0, "events": "not-an-object"},
            {"status": 0, "events": [{"code": "G"}]},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                source = HttpMatchEventSource(
                    "https://example.test/match/{match_id}",
                    "match-1",
                    None,
                    poll_interval=5,
                    emit_existing=False,
                    timeout=1,
                )
                with patch(
                    "event_driven_pipeline.urllib.request.urlopen",
                    return_value=FakeHttpResponse(payload),
                ):
                    self.assertEqual(source.poll(0, 0), [])
                self.assertFalse(source.initialized)
                self.assertEqual(source.error_count, 1)
                self.assertEqual(source.last_error_kind, "temporary")

    def test_http_source_resumes_from_durable_seen_keys(self):
        first_payload = {
            "events": {
                "18": {
                    "minute": "18",
                    "teamAEvents": [
                        {"code": "G", "person_id": "1", "person": "A"}
                    ],
                    "teamBEvents": [],
                }
            }
        }
        old_event = parse_match_events(first_payload, "match-1")[0]
        resumed_payload = {
            "events": {
                **first_payload["events"],
                "28": {
                    "minute": "28",
                    "teamAEvents": [],
                    "teamBEvents": [
                        {"code": "RC", "person_id": "2", "person": "B"}
                    ],
                },
            }
        }
        source = HttpMatchEventSource(
            "https://example.test/match/{match_id}",
            "match-1",
            None,
            poll_interval=5,
            emit_existing=False,
            timeout=1,
            initial_seen={old_event.event_key},
            initialized=True,
        )
        with patch(
            "event_driven_pipeline.urllib.request.urlopen",
            return_value=FakeHttpResponse(resumed_payload),
        ):
            emitted = source.poll(0, 0)
        self.assertEqual([event.code for event in emitted], ["RC"])

    def test_http_source_classifies_unauthorized_and_backs_off(self):
        source = HttpMatchEventSource(
            "https://example.test/match/{match_id}",
            "match-1",
            None,
            poll_interval=5,
            emit_existing=False,
            timeout=1,
        )
        error = urllib.error.HTTPError(
            source.url, 401, "Unauthorized", hdrs=None, fp=None
        )
        with patch("event_driven_pipeline.urllib.request.urlopen", side_effect=error):
            self.assertEqual(source.poll(0, 10), [])
        self.assertEqual(source.last_error_kind, "unauthorized")
        self.assertEqual(source.next_poll_monotonic, 15)

    def test_event_feed_is_polled_while_ffmpeg_waits_to_reconnect(self):
        class InterruptingEventSource:
            error_count = 0
            poll_count = 0
            last_error = None

            def __init__(self):
                self.polled = False

            def poll(self, stream_time, now_monotonic):
                self.polled = True
                raise KeyboardInterrupt

            def report(self):
                return {"type": "test", "poll_count": int(self.polled)}

        event_source = InterruptingEventSource()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            sys,
            "argv",
            [
                "event_driven_pipeline.py",
                "rtmp://example/live",
                "--event-url",
                "https://example.test/{match_id}",
                "--match-id",
                "match-1",
                "--output-dir",
                directory,
            ],
        ), patch(
            "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
        ), patch(
            "event_driven_pipeline.IngestSupervisor", ReconnectingSupervisor
        ), patch(
            "event_driven_pipeline.HttpMatchEventSource", return_value=event_source
        ):
            main()

        self.assertTrue(event_source.polled)
        self.assertEqual(
            ReconnectingSupervisor.last_kwargs["backoff_initial"],
            2.0,
        )
        self.assertEqual(
            ReconnectingSupervisor.last_kwargs["backoff_max"],
            5.0,
        )

    def test_worker_restart_restores_manifest_clock_instead_of_resetting(self):
        class InterruptingEventSource:
            error_count = 0
            poll_count = 0
            last_error = None

            def poll(self, stream_time, now_monotonic):
                del stream_time, now_monotonic
                self.poll_count += 1
                raise KeyboardInterrupt

            def report(self):
                return {"type": "test", "poll_count": self.poll_count}

        with tempfile.TemporaryDirectory() as directory:
            arguments = [
                "event_driven_pipeline.py",
                "rtmp://example/live",
                "--event-url",
                "https://example.test/{match_id}",
                "--match-id",
                "match-1",
                "--output-dir",
                directory,
            ]
            manifests = []
            reports = []
            for _ in range(2):
                with patch.object(sys, "argv", arguments), patch(
                    "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
                ), patch(
                    "event_driven_pipeline.IngestSupervisor", ReconnectingSupervisor
                ), patch(
                    "event_driven_pipeline.HttpMatchEventSource",
                    return_value=InterruptingEventSource(),
                ):
                    main()
                manifests.append(
                    json.loads(
                        (
                            Path(directory) / "buffer" / "segment_manifest.json"
                        ).read_text(encoding="utf-8")
                    )
                )
                reports.append(
                    json.loads(
                        (
                            Path(directory) / "event_pipeline_report.json"
                        ).read_text(encoding="utf-8")
                    )
                )

        self.assertEqual(
            reports[0]["timeline"]["timeline_origin_wall_unix"],
            reports[1]["timeline"]["timeline_origin_wall_unix"],
        )
        self.assertGreaterEqual(
            manifests[1]["generations"][0]["stream_offset"],
            manifests[0]["generations"][0]["stream_offset"],
        )

    @unittest.skipUnless(hasattr(signal, "SIGUSR1"), "SIGUSR1 is not available")
    def test_sigusr1_drains_and_reports_normal_match_end(self):
        installed_handlers = {}

        def fake_signal(signum, handler):
            previous = installed_handlers.get(signum, signal.SIG_DFL)
            installed_handlers[signum] = handler
            return previous

        class MatchEndingEventSource:
            error_count = 0
            poll_count = 0
            last_error = None
            snapshot_revision = 0
            seen = set()

            def poll(self, stream_time, now_monotonic):
                del stream_time, now_monotonic
                self.poll_count += 1
                if self.poll_count == 1:
                    installed_handlers[signal.SIGUSR1](signal.SIGUSR1, None)
                return []

            def report(self):
                return {"type": "test", "poll_count": self.poll_count}

        event_source = MatchEndingEventSource()
        with tempfile.TemporaryDirectory() as directory, patch.object(
            sys,
            "argv",
            [
                "event_driven_pipeline.py",
                "rtmp://example/live",
                "--event-url",
                "https://example.test/{match_id}",
                "--match-id",
                "match-1",
                "--match-start-play",
                "2026-05-20 11:00:00",
                "--match-start-naive-timezone",
                "utc",
                "--output-dir",
                directory,
                "--graceful-stop-grace-seconds",
                "0.01",
                "--graceful-stop-timeout-seconds",
                "1",
            ],
        ), patch(
            "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
        ), patch(
            "event_driven_pipeline.IngestSupervisor", GracefulStopSupervisor
        ), patch(
            "event_driven_pipeline.HttpMatchEventSource", return_value=event_source
        ), patch(
            "event_driven_pipeline.signal.signal", side_effect=fake_signal
        ):
            main()

            report = json.loads(
                (Path(directory) / "event_pipeline_report.json").read_text(
                    encoding="utf-8"
                )
            )
            event_log = [
                json.loads(line)
                for line in (
                    Path(directory) / "pipeline_events.jsonl"
                ).read_text(encoding="utf-8").splitlines()
            ]

        supervisor = GracefulStopSupervisor.instance
        self.assertIsNotNone(supervisor)
        self.assertTrue(supervisor.terminated)
        self.assertFalse(supervisor.reconnect)
        self.assertEqual(report["stop_reason"], "match_played")
        self.assertEqual(report["exit_reason"], "match_played")
        self.assertEqual(report["completion_state"], "completed")
        self.assertTrue(report["graceful_stop_requested"])
        self.assertFalse(report["graceful_stop_timed_out"])
        self.assertEqual(report["ffmpeg_return_code"], -15)
        expected_match_start = parse_match_start_play(
            "2026-05-20 11:00:00",
            naive_timezone="utc",
        )
        self.assertEqual(report["timeline"]["match_start_naive_timezone"], "utc")
        self.assertEqual(
            report["timeline"]["match_start_normalized_unix"],
            expected_match_start,
        )
        self.assertEqual(
            report["timeline"]["match_start_at_unix"],
            expected_match_start,
        )
        self.assertIn("graceful_stop_requested", {item["event"] for item in event_log})

    @unittest.skipUnless(hasattr(signal, "SIGUSR1"), "SIGUSR1 is not available")
    def test_sigusr1_timeout_reports_incomplete_final_video(self):
        installed_handlers = {}

        def fake_signal(signum, handler):
            previous = installed_handlers.get(signum, signal.SIG_DFL)
            installed_handlers[signum] = handler
            return previous

        event = MatchEvent(
            event_key="match-1:G:final",
            code="G",
            event_type="goal",
            minute="90",
            minute_extra="3",
            team="teamA",
            person="Final scorer",
            person_id="9",
            score="1-0",
            reason="",
        )

        class FinalEventSource:
            error_count = 0
            poll_count = 0
            last_error = None
            snapshot_revision = 0
            seen = {event.event_key}

            def poll(self, stream_time, now_monotonic):
                del stream_time, now_monotonic
                self.poll_count += 1
                if self.poll_count == 1:
                    installed_handlers[signal.SIGUSR1](signal.SIGUSR1, None)
                    return [event]
                return []

            def report(self):
                return {"type": "test", "poll_count": self.poll_count}

        with tempfile.TemporaryDirectory() as directory, patch.object(
            sys,
            "argv",
            [
                "event_driven_pipeline.py",
                "rtmp://example/live",
                "--event-url",
                "https://example.test/{match_id}",
                "--match-id",
                "match-1",
                "--output-dir",
                directory,
                "--graceful-stop-grace-seconds",
                "0.01",
                "--graceful-stop-timeout-seconds",
                "0.05",
            ],
        ), patch(
            "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
        ), patch(
            "event_driven_pipeline.IngestSupervisor", GracefulStopSupervisor
        ), patch(
            "event_driven_pipeline.HttpMatchEventSource",
            return_value=FinalEventSource(),
        ), patch(
            "event_driven_pipeline.signal.signal", side_effect=fake_signal
        ):
            main()
            report = json.loads(
                (Path(directory) / "event_pipeline_report.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertEqual(report["stop_reason"], "match_played_stream_incomplete")
        self.assertEqual(report["completion_state"], "completed_with_warnings")
        self.assertTrue(report["graceful_stop_timed_out"])
        self.assertEqual(report["events"][0]["status"], "failed")
        self.assertEqual(
            report["events"][0]["error_kind"], "graceful_stop_timeout"
        )
        timing = report["events"][0]["timing_diagnostics"]
        self.assertAlmostEqual(
            timing["first_observed_stream_time_sec"],
            report["events"][0]["observed_stream_time_sec"],
            delta=0.001,
        )
        self.assertEqual(
            timing["event_to_video_offset_seconds"],
            report["timeline"]["event_to_video_offset_seconds"],
        )
        self.assertEqual(
            timing["requested_clip_start_stream_time_sec"],
            max(
                0.0,
                timing["clip_anchor_stream_time_sec"]
                - report["gif"]["before_seconds"],
            ),
        )
        self.assertEqual(
            timing["requested_clip_end_stream_time_sec"],
            timing["clip_anchor_stream_time_sec"]
            + report["gif"]["after_seconds"],
        )
        self.assertIn("media_tail_stream_time_sec", timing)
        self.assertGreater(timing["first_observed_wall_time_unix"], 0)

    @unittest.skipUnless(hasattr(signal, "SIGUSR1"), "SIGUSR1 is not available")
    def test_sigusr1_keeps_final_event_polling_after_ingest_exits(self):
        installed_handlers = {}

        def fake_signal(signum, handler):
            previous = installed_handlers.get(signum, signal.SIG_DFL)
            installed_handlers[signum] = handler
            return previous

        event = MatchEvent(
            event_key="match-1:G:late",
            code="G",
            event_type="goal",
            minute="90",
            minute_extra="5",
            team="teamB",
            person="Late scorer",
            person_id="11",
            score="1-1",
            reason="",
        )

        class LateFinalEventSource:
            error_count = 0
            poll_count = 0
            last_error = None
            snapshot_revision = 0
            seen = {event.event_key}

            def poll(self, stream_time, now_monotonic):
                del stream_time, now_monotonic
                self.poll_count += 1
                if self.poll_count == 1:
                    installed_handlers[signal.SIGUSR1](signal.SIGUSR1, None)
                    return []
                if self.poll_count == 2:
                    supervisor = GracefulStopSupervisor.instance
                    supervisor.process.return_code = 1
                    return [event]
                return []

            def report(self):
                return {"type": "test", "poll_count": self.poll_count}

        with tempfile.TemporaryDirectory() as directory, patch.object(
            sys,
            "argv",
            [
                "event_driven_pipeline.py",
                "rtmp://example/live",
                "--event-url",
                "https://example.test/{match_id}",
                "--match-id",
                "match-1",
                "--output-dir",
                directory,
                "--graceful-stop-grace-seconds",
                "0.01",
                "--graceful-stop-timeout-seconds",
                "0.05",
            ],
        ), patch(
            "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
        ), patch(
            "event_driven_pipeline.IngestSupervisor", GracefulStopSupervisor
        ), patch(
            "event_driven_pipeline.HttpMatchEventSource",
            return_value=LateFinalEventSource(),
        ), patch(
            "event_driven_pipeline.signal.signal", side_effect=fake_signal
        ):
            main()
            report = json.loads(
                (Path(directory) / "event_pipeline_report.json").read_text(
                    encoding="utf-8"
                )
            )

        self.assertGreaterEqual(report["event_source"]["poll_count"], 2)
        self.assertEqual(len(report["events"]), 1)
        self.assertEqual(report["events"][0]["person"], "Late scorer")
        self.assertEqual(report["completion_state"], "completed_with_warnings")


if __name__ == "__main__":
    unittest.main()
