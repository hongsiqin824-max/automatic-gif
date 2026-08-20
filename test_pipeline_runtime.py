import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from event_driven_pipeline import (
    EVENT_VISUAL_WINDOW_LEASE_OWNER,
    EventRevisionTracker,
    MatchEvent,
    disable_incomplete_tdeed_tasks,
    enabled_vision_artifact_kinds,
    encode_event_job,
    protect_incomplete_vision_event_segments,
    parse_match_events,
    recovered_event_job,
    release_terminal_event_visual_window_leases,
)
from live_goal_pipeline import Segment
from pipeline_runtime import PipelineRuntime, TaskStateStore


def event_data(event_key="match-1:G:key"):
    return {
        "event_key": event_key,
        "code": "G",
        "event_type": "goal",
        "minute": "18",
        "minute_extra": "0",
        "team": "teamA",
        "person": "Player A",
        "person_id": "1",
        "score": "1-0",
        "reason": "",
        "metadata": {"bucket": "18"},
    }


def discover(runtime, event_key="match-1:G:key"):
    return runtime.discover_task(
        match_id="match-1",
        event_data=event_data(event_key),
        observed_stream_time=25.0,
        observed_source_time=125.0,
        clip_anchor_stream_time=24.0,
        clip_anchor_source_time=124.0,
        output_due_stream_time=51.0,
        detected_at_unix=1000.0,
        deadline_at_unix=time.time() + 600.0,
    )


def enqueue_vision(runtime, event_key="match-1:G:key"):
    return runtime.enqueue_vision_task(
        event_key,
        search_start_stream_time=0.0,
        search_end_stream_time=80.0,
        clip_before_seconds=8.0,
        clip_after_seconds=12.0,
        model_name="T-DEED",
        model_version="SoccerNet_small",
        model_weights_sha256="abc123",
    )


class PipelineRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary_directory.name)
        self.database_path = self.directory / "state.sqlite3"
        self.log_path = self.directory / "events.jsonl"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def test_encoded_event_is_deduplicated_after_reopen(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        runtime.transition("match-1:G:key", "encoding")
        runtime.transition(
            "match-1:G:key",
            "encoded",
            result={
                "output": "/tmp/goal.gif",
                "bytes": 1234,
                "duration_sec": 30.0,
                "encode_seconds": 1.2,
                "seconds_after_event_observed": 28.0,
            },
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        self.assertFalse(discover(reopened))
        task = reopened.store.get("match-1:G:key")
        self.assertEqual(task.status, "encoded")
        self.assertEqual(task.output_path, "/tmp/goal.gif")
        self.assertEqual(task.output_bytes, 1234)
        self.assertEqual(reopened.recover_incomplete("match-1"), [])
        reopened.close()

    def test_pending_and_interrupted_encoding_tasks_are_recovered(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime, "match-1:G:pending"))
        self.assertTrue(discover(runtime, "match-1:G:encoding"))
        runtime.transition("match-1:G:encoding", "encoding")
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        recovered = reopened.recover_incomplete("match-1")
        self.assertEqual(
            {task.event_key for task in recovered},
            {"match-1:G:pending", "match-1:G:encoding"},
        )
        self.assertTrue(all(task.status == "pending" for task in recovered))
        encoding = reopened.store.get("match-1:G:encoding")
        self.assertEqual(encoding.attempt_count, 1)
        self.assertEqual(encoding.clip_anchor_stream_time, 24.0)
        reopened.close()

    def test_recovery_keeps_identical_independent_tasks(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime, "match-1:G:first"))
        self.assertTrue(discover(runtime, "match-1:G:second"))

        recovered = runtime.recover_incomplete("match-1")

        self.assertEqual(
            {task.event_key for task in recovered},
            {"match-1:G:first", "match-1:G:second"},
        )
        self.assertIsNone(runtime.store.get("match-1:G:first").suppressed_by_event_key)
        self.assertIsNone(runtime.store.get("match-1:G:second").suppressed_by_event_key)
        runtime.close()

    def test_recovery_suppresses_duplicate_incomplete_event_versions(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        first = event_data("match-1:G:base")
        first.update({"minute": "5", "person": "", "person_id": "0", "score": "1-0"})
        updated = event_data("match-1:G:updated")
        updated.update(
            {
                "minute": "4",
                "person": "Miguel Murillo",
                "person_id": "50895934",
                "score": "1-0",
            }
        )
        for event in (first, updated):
            runtime.discover_task(
                match_id="match-1",
                event_data=event,
                observed_stream_time=25.0,
                observed_source_time=None,
                clip_anchor_stream_time=25.0,
                clip_anchor_source_time=None,
                output_due_stream_time=50.0,
                detected_at_unix=1000.0,
            )
        recovered = runtime.recover_incomplete("match-1")
        self.assertEqual([task.event_key for task in recovered], ["match-1:G:base"])
        primary = runtime.store.get("match-1:G:base")
        duplicate = runtime.store.get("match-1:G:updated")
        self.assertEqual(primary.event_data["person"], "Miguel Murillo")
        self.assertEqual(duplicate.status, "pending")
        self.assertEqual(duplicate.suppressed_by_event_key, "match-1:G:base")
        self.assertIn(
            "event_superseded",
            self.log_path.read_text(encoding="utf-8"),
        )
        runtime.close()

    def test_recovery_suppresses_new_version_when_original_is_encoded(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime, "match-1:G:base"))
        runtime.transition("match-1:G:base", "encoding")
        runtime.transition(
            "match-1:G:base",
            "encoded",
            result={"output": "/tmp/base.gif", "bytes": 1},
        )
        duplicate = event_data("match-1:G:updated")
        duplicate.update({"minute": "17", "person": "Player A", "person_id": "1"})
        runtime.discover_task(
            match_id="match-1",
            event_data=duplicate,
            observed_stream_time=25.0,
            observed_source_time=None,
            clip_anchor_stream_time=25.0,
            clip_anchor_source_time=None,
            output_due_stream_time=50.0,
            detected_at_unix=1000.0,
        )
        self.assertEqual(runtime.recover_incomplete("match-1"), [])
        duplicate_task = runtime.store.get("match-1:G:updated")
        self.assertEqual(duplicate_task.status, "pending")
        self.assertEqual(duplicate_task.suppressed_by_event_key, "match-1:G:base")
        runtime.close()

    def test_event_feed_cursor_survives_worker_restart(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        initialized, keys = runtime.store.load_event_cursor("match-1")
        self.assertFalse(initialized)
        self.assertEqual(keys, set())

        runtime.store.remember_event_snapshot(
            "match-1", {"match-1:G:old", "match-1:YC:old"}, now=1000.0
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        initialized, keys = reopened.store.load_event_cursor("match-1")
        self.assertTrue(initialized)
        self.assertEqual(keys, {"match-1:G:old", "match-1:YC:old"})
        reopened.store.remember_event_snapshot(
            "match-1", {*keys, "match-1:RC:new"}, now=1001.0
        )
        self.assertEqual(
            reopened.store.load_event_cursor("match-1")[1],
            {"match-1:G:old", "match-1:YC:old", "match-1:RC:new"},
        )
        reopened.close()

    def test_shotmap_cursor_only_initializes_from_valid_snapshot(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)

        self.assertFalse(
            runtime.store.upsert_shotmap_snapshot(
                "match-1",
                {"error": "upstream unavailable"},
                diagnostics={"http_status": 503},
                observed_at_unix=1000.0,
                now=1001.0,
            )
        )
        state = runtime.store.load_shotmap_state("match-1")
        self.assertFalse(state.initialized)
        self.assertIsNone(state.last_snapshot)
        self.assertEqual(state.last_response_at_unix, 1000.0)
        self.assertEqual(state.diagnostics["http_status"], 503)
        self.assertEqual(
            state.diagnostics["validation_error"],
            "shots_missing_or_not_list",
        )

        self.assertTrue(
            runtime.store.upsert_shotmap_snapshot(
                "match-1",
                {"shots": []},
                diagnostics={"http_status": 200, "age": 4},
                observed_at_unix=1010.0,
                now=1011.0,
            )
        )
        state = runtime.store.load_shotmap_state("match-1")
        self.assertTrue(state.initialized)
        self.assertEqual(state.initialized_at_unix, 1011.0)
        self.assertEqual(state.last_snapshot, {"shots": []})
        self.assertEqual(state.last_snapshot_at_unix, 1010.0)
        self.assertEqual(state.diagnostics["valid"], True)
        runtime.close()

    def test_shotmap_baseline_and_snapshot_survive_worker_restart(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        payload = {
            "shots": [
                {
                    "outcome": "goal",
                    "second": 455,
                    "minute": 8,
                    "playerId": 123,
                }
            ]
        }
        self.assertTrue(
            runtime.store.upsert_shotmap_snapshot(
                "match-1", payload, observed_at_unix=2000.0, now=2001.0
            )
        )
        self.assertEqual(
            runtime.store.mark_shotmap_seen(
                "match-1",
                {"goal-fingerprint"},
                events={"goal-fingerprint": payload["shots"][0]},
                now=2001.0,
            ),
            {"goal-fingerprint"},
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        initialized, fingerprints = reopened.store.load_shotmap_cursor("match-1")
        self.assertTrue(initialized)
        self.assertEqual(fingerprints, {"goal-fingerprint"})
        state = reopened.store.load_shotmap_state("match-1")
        self.assertEqual(state.last_snapshot, payload)
        self.assertEqual(state.seen_fingerprints, frozenset({"goal-fingerprint"}))
        self.assertEqual(
            reopened.store.mark_shotmap_seen(
                "match-1", {"goal-fingerprint"}, now=2002.0
            ),
            set(),
        )
        reopened.close()

    def test_invalid_shotmap_response_does_not_replace_valid_snapshot(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        valid_payload = {"shots": [{"outcome": "goal", "second": 455}]}
        runtime.store.upsert_shotmap_snapshot(
            "match-1", valid_payload, observed_at_unix=3000.0, now=3001.0
        )

        self.assertFalse(
            runtime.store.upsert_shotmap_snapshot(
                "match-1",
                [valid_payload],
                diagnostics={"error": "unexpected envelope"},
                observed_at_unix=3010.0,
                now=3011.0,
            )
        )
        state = runtime.store.load_shotmap_state("match-1")
        self.assertTrue(state.initialized)
        self.assertEqual(state.initialized_at_unix, 3001.0)
        self.assertEqual(state.last_snapshot, valid_payload)
        self.assertEqual(state.last_snapshot_at_unix, 3000.0)
        self.assertEqual(state.last_response_at_unix, 3010.0)
        self.assertEqual(state.diagnostics["valid"], False)
        runtime.close()

    def test_concurrent_shotmap_seen_updates_are_deduplicated(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        runtime.store.upsert_shotmap_snapshot(
            "match-1", {"shots": []}, now=4000.0
        )
        barrier = threading.Barrier(4)
        errors = []

        def mark_seen(index):
            try:
                barrier.wait()
                runtime.store.mark_shotmap_seen(
                    "match-1",
                    {"shared", f"goal-{index}"},
                    now=4001.0 + index,
                )
            except Exception as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=mark_seen, args=(index,)) for index in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(
            runtime.store.load_shotmap_fingerprints("match-1"),
            {"shared", "goal-0", "goal-1", "goal-2", "goal-3"},
        )
        runtime.close()

    def test_event_aliases_survive_worker_restart(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        original = event_data("match-1:G:original")
        runtime.store.remember_event_snapshot(
            "match-1",
            {"match-1:G:original"},
            aliases={"match-1:G:version": original},
            now=1000.0,
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        aliases = reopened.store.load_event_aliases("match-1")
        self.assertEqual(
            aliases["match-1:G:version"]["event_key"],
            "match-1:G:original",
        )
        self.assertEqual(aliases["match-1:G:version"]["person"], "Player A")
        reopened.close()

    def test_exact_event_snapshot_replaces_old_versions_and_keeps_first_seen(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        old = {
            **event_data("match-1:YC:old"),
            "code": "YC",
            "event_type": "yellow_card",
            "minute": "81",
            "person": "",
            "person_id": "0",
            "score": "",
        }
        new = {
            **old,
            "event_key": "match-1:YC:new",
            "person": "Raul Torres",
            "person_id": "50405792",
        }
        runtime.store.remember_event_snapshot(
            "match-1",
            {old["event_key"]},
            current_versions={old["event_key"]: old},
            now=1000.0,
        )
        runtime.store.remember_event_snapshot(
            "match-1",
            {old["event_key"]},
            current_versions={old["event_key"]: old},
            now=1050.0,
        )
        snapshot, first_seen = runtime.store.load_event_snapshot("match-1")
        self.assertEqual(set(snapshot), {old["event_key"]})
        self.assertEqual(first_seen[old["event_key"]], 1000.0)

        runtime.store.remember_event_snapshot(
            "match-1",
            {old["event_key"]},
            current_versions={new["event_key"]: new},
            now=1094.54,
        )
        snapshot, first_seen = runtime.store.load_event_snapshot("match-1")
        self.assertEqual(set(snapshot), {new["event_key"]})
        self.assertEqual(first_seen[new["event_key"]], 1094.54)
        self.assertEqual(
            runtime.store.load_event_cursor("match-1")[1],
            {old["event_key"]},
        )
        runtime.close()

    def test_yellow_card_replacement_survives_worker_restart(self):
        empty_payload = {
            "events": {
                "81": {
                    "minute": "81",
                    "teamAEvents": [{"code": "YC", "person_id": "0"}],
                }
            }
        }
        completed_payload = {
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
        runtime = PipelineRuntime(self.database_path, self.log_path)
        tracker = EventRevisionTracker()
        original = tracker.reconcile(
            parse_match_events(empty_payload, "match-1"),
            observed_at_unix=1000.0,
        )[0]
        runtime.store.remember_event_snapshot(
            "match-1",
            {original.event_key},
            aliases={
                key: vars(event) for key, event in tracker.snapshot().items()
            },
            current_versions={
                key: vars(event)
                for key, event in tracker.current_snapshot().items()
            },
            now=1000.0,
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        aliases = {
            key: MatchEvent(**value)
            for key, value in reopened.store.load_event_aliases("match-1").items()
        }
        snapshot_data, first_seen = reopened.store.load_event_snapshot("match-1")
        resumed = EventRevisionTracker(
            aliases,
            previous_versions={
                key: MatchEvent(**value) for key, value in snapshot_data.items()
            },
            first_seen_at=first_seen,
        )
        revision = resumed.reconcile(
            parse_match_events(completed_payload, "match-1"),
            observed_at_unix=1094.54,
        )[0]

        self.assertEqual(revision.event_key, original.event_key)
        self.assertEqual(revision.person, "Raul Torres")
        reopened.close()

    def test_mutable_event_metadata_updates_without_changing_task_status(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        updated = {
            **event_data(),
            "minute": "17",
            "person": "Updated Player",
            "person_id": "2",
        }
        self.assertTrue(runtime.update_task_event(updated))
        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "pending")
        self.assertEqual(task.event_data["minute"], "17")
        self.assertEqual(task.event_data["person"], "Updated Player")
        runtime.close()

    def test_nullable_enrichment_field_can_be_explicitly_cleared(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        enriched = {
            **event_data(),
            "second": 4177,
            "metadata": {
                "bucket": "18",
                "second_source": "shotmap",
                "shotmap_match_status": "matched",
            },
        }
        self.assertTrue(runtime.discover_task(
            match_id="match-1",
            event_data=enriched,
            observed_stream_time=25.0,
            observed_source_time=125.0,
            clip_anchor_stream_time=24.0,
            clip_anchor_source_time=124.0,
            output_due_stream_time=51.0,
            detected_at_unix=1000.0,
            deadline_at_unix=time.time() + 600.0,
        ))

        cleared = {
            **enriched,
            "second": None,
            "metadata": {
                **enriched["metadata"],
                "second_source": None,
                "shotmap_match_status": "missing",
            },
        }
        self.assertTrue(runtime.update_task_event(
            cleared,
            replace_fields={"second", "metadata"},
        ))

        task = runtime.store.get("match-1:G:key")
        self.assertIsNone(task.event_data["second"])
        self.assertIsNone(task.event_data["metadata"]["second_source"])
        self.assertEqual(task.event_data["metadata"]["shotmap_match_status"], "missing")
        runtime.close()

    def test_encoded_event_metadata_update_keeps_anchor_and_output(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        runtime.transition("match-1:G:key", "encoding")
        runtime.transition(
            "match-1:G:key",
            "encoded",
            result={"output": "/tmp/original.gif", "bytes": 1234},
        )

        self.assertTrue(runtime.update_task_event({
            **event_data(),
            "person": "Updated Player",
            "person_id": "2",
        }))
        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "encoded")
        self.assertEqual(task.clip_anchor_stream_time, 24.0)
        self.assertEqual(task.output_path, "/tmp/original.gif")
        self.assertEqual(task.output_bytes, 1234)
        runtime.close()

    def test_failure_saves_error_and_event_metadata(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        runtime.transition("match-1:G:key", "encoding")
        runtime.transition("match-1:G:key", "failed", error="ffmpeg failed")
        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.error, "ffmpeg failed")
        self.assertEqual(task.event_data["person"], "Player A")
        self.assertIsNotNone(task.failed_at_unix)
        runtime.close()

    def test_jsonl_records_discovery_transitions_api_error_and_gif_timing(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        runtime.log_api_error(
            match_id="match-1", error="temporary timeout", poll_count=2, error_count=1
        )
        runtime.log_ingest_restart(
            match_id="match-1", return_code=1, restart_count=1, delay_seconds=2.0
        )
        runtime.transition("match-1:G:key", "encoding")
        runtime.transition(
            "match-1:G:key",
            "encoded",
            result={
                "output": "/tmp/goal.gif",
                "bytes": 9876,
                "duration_sec": 30.0,
                "encode_seconds": 1.5,
                "seconds_after_event_observed": 26.5,
                "over_size_reference": False,
            },
        )
        runtime.close()

        records = [
            json.loads(line)
            for line in self.log_path.read_text(encoding="utf-8").splitlines()
        ]
        event_names = [record["event"] for record in records]
        self.assertIn("event_discovered", event_names)
        self.assertIn("api_error", event_names)
        self.assertIn("ingest_restart", event_names)
        ready = next(record for record in records if record["event"] == "gif_ready")
        self.assertEqual(ready["bytes"], 9876)
        self.assertEqual(ready["seconds_after_event_observed"], 26.5)
        self.assertTrue(all("timestamp" in record for record in records))

    def test_store_rejects_invalid_state_transition(self):
        with TaskStateStore(self.database_path) as store:
            store.discover(
                match_id="match-1",
                event_data=event_data(),
                observed_stream_time=25.0,
                observed_source_time=None,
                clip_anchor_stream_time=25.0,
                clip_anchor_source_time=None,
                output_due_stream_time=50.0,
                detected_at_unix=1000.0,
            )
            with self.assertRaisesRegex(ValueError, "discovered -> encoded"):
                store.transition("match-1:G:key", "encoded")

    def test_store_supports_transitions_from_worker_threads(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        errors = []

        def encode_on_worker():
            try:
                runtime.transition("match-1:G:key", "encoding")
                runtime.transition(
                    "match-1:G:key",
                    "encoded",
                    result={"output": "/tmp/thread.gif", "bytes": 42},
                )
            except Exception as exc:
                errors.append(exc)

        worker = threading.Thread(target=encode_on_worker)
        worker.start()
        worker.join()
        self.assertEqual(errors, [])
        self.assertEqual(runtime.store.get("match-1:G:key").status, "encoded")
        runtime.close()

    def test_default_vision_task_is_tdeed_artifact_and_does_not_change_default(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        default_before = runtime.store.get("match-1:G:key")

        self.assertTrue(enqueue_vision(runtime))
        self.assertFalse(enqueue_vision(runtime))

        refined = runtime.store.get_vision_task("match-1:G:key")
        default_after = runtime.store.get("match-1:G:key")
        self.assertEqual(refined.artifact_kind, "tdeed_refined")
        self.assertEqual(refined.status, "pending")
        self.assertEqual(refined.source_anchor_stream_time, 24.0)
        self.assertEqual(refined.search_end_stream_time, 80.0)
        self.assertEqual(refined.clip_before_seconds, 8.0)
        self.assertEqual(refined.model_name, "T-DEED")
        self.assertEqual(len(runtime.store.list_vision_tasks("match-1")), 1)
        self.assertEqual(default_after.status, default_before.status)
        self.assertEqual(default_after.output_path, default_before.output_path)
        self.assertEqual(default_after.result, default_before.result)
        runtime.close()

    def test_visual_artifact_types_persist_independent_results_and_metadata(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        self.assertTrue(enqueue_vision(runtime))
        self.assertTrue(runtime.enqueue_vision_task(
            "match-1:G:key",
            artifact_kind="ocr_window",
            search_start_stream_time=10.0,
            search_end_stream_time=40.0,
            clip_before_seconds=4.0,
            clip_after_seconds=6.0,
            model_name="PaddleOCR",
            window_metadata={"profile": "scoreboard-a"},
        ))

        runtime.transition_vision_task(
            "match-1:G:key", "locating", artifact_kind="ocr_window"
        )
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            artifact_kind="ocr_window",
            result={"anchor_stream_time": 26.0, "confidence": 0.93},
            location_metadata={"clock_text": "18:42"},
        )
        runtime.transition_vision_task(
            "match-1:G:key", "encoding", artifact_kind="ocr_window"
        )
        runtime.transition_vision_task(
            "match-1:G:key",
            "encoded",
            artifact_kind="ocr_window",
            result={"output": "/tmp/ocr-window.mp4", "bytes": 321},
        )

        runtime.transition_vision_task("match-1:G:key", "locating")
        runtime.transition_vision_task(
            "match-1:G:key",
            "failed",
            result={
                "failure_reason": {
                    "stage": "event_localization",
                    "message": "no T-DEED candidate",
                }
            },
            error="localization failed",
            error_kind="tdeed_no_candidate",
            window_metadata={"candidate_count": 0},
        )
        default_before = runtime.store.get("match-1:G:key")
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        ocr = reopened.store.get_vision_task("match-1:G:key", "ocr_window")
        tdeed = reopened.store.get_vision_task("match-1:G:key", "refined")
        self.assertEqual(ocr.status, "encoded")
        self.assertEqual(ocr.output_path, "/tmp/ocr-window.mp4")
        self.assertEqual(ocr.output_bytes, 321)
        self.assertEqual(ocr.location_metadata["clock_text"], "18:42")
        self.assertEqual(ocr.location_metadata["anchor_stream_time"], 26.0)
        self.assertEqual(ocr.window_metadata["profile"], "scoreboard-a")
        self.assertEqual(tdeed.artifact_kind, "tdeed_refined")
        self.assertEqual(tdeed.status, "failed")
        self.assertIsNone(tdeed.output_path)
        self.assertEqual(tdeed.failure_stage, "event_localization")
        self.assertEqual(tdeed.failure_reason, "no T-DEED candidate")
        self.assertEqual(tdeed.window_metadata["candidate_count"], 0)
        self.assertEqual(
            {
                (task.event_key, task.artifact_kind)
                for task in reopened.store.list_vision_tasks("match-1")
            },
            {
                ("match-1:G:key", "ocr_window"),
                ("match-1:G:key", "tdeed_refined"),
            },
        )
        default_after = reopened.store.get("match-1:G:key")
        self.assertEqual(default_after.status, default_before.status)
        self.assertEqual(default_after.output_path, default_before.output_path)
        reopened.close()

    def test_disabling_tdeed_keeps_ocr_runnable_and_makes_refinement_terminal(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        self.assertTrue(discover(runtime))
        self.assertTrue(enqueue_vision(runtime))
        self.assertTrue(runtime.enqueue_vision_task(
            "match-1:G:key",
            artifact_kind="ocr_window",
            search_start_stream_time=10.0,
            search_end_stream_time=40.0,
            clip_before_seconds=30.0,
            clip_after_seconds=30.0,
            model_name="PaddleOCR",
        ))

        self.assertEqual(enabled_vision_artifact_kinds(tdeed_enabled=False), ("ocr_window",))
        self.assertEqual(disable_incomplete_tdeed_tasks(runtime, "match-1"), 1)

        ocr = runtime.store.get_vision_task("match-1:G:key", "ocr_window")
        tdeed = runtime.store.get_vision_task("match-1:G:key", "tdeed_refined")
        self.assertEqual(ocr.status, "pending")
        self.assertEqual(tdeed.status, "failed")
        self.assertTrue(tdeed.result["disabled"])
        self.assertEqual(tdeed.last_error_kind, "tdeed_disabled")
        self.assertEqual(
            runtime.store.list_incomplete_vision_tasks("match-1"),
            [ocr],
        )
        runtime.close()

    def test_recovery_updates_each_visual_artifact_row_independently(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.enqueue_vision_task(
            "match-1:G:key",
            artifact_kind="ocr_window",
            search_start_stream_time=10.0,
            search_end_stream_time=40.0,
            clip_before_seconds=4.0,
            clip_after_seconds=6.0,
        )
        runtime.transition_vision_task("match-1:G:key", "locating")
        runtime.transition_vision_task(
            "match-1:G:key",
            "locating",
            artifact_kind="ocr_window",
        )
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            artifact_kind="ocr_window",
            result={"anchor_stream_time": 25.0},
        )
        runtime.transition_vision_task(
            "match-1:G:key",
            "encoding",
            artifact_kind="ocr_window",
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        recovered = reopened.recover_incomplete_vision("match-1")
        self.assertEqual(
            {(task.artifact_kind, task.status) for task in recovered},
            {("tdeed_refined", "pending"), ("ocr_window", "located")},
        )
        self.assertEqual(
            reopened.store.get_vision_task("match-1:G:key").status,
            "pending",
        )
        self.assertEqual(
            reopened.store.get_vision_task("match-1:G:key", "ocr_window").status,
            "located",
        )
        reopened.close()

    def test_vision_task_persists_location_and_refined_output(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        located = runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            result={
                "anchor_stream_time": 26.4,
                "anchor_source_time": 126.4,
                "confidence": 0.91,
                "inference_seconds": 15.2,
                "candidate_count": 2,
            },
        )
        self.assertEqual(located.locate_attempt_count, 1)
        self.assertEqual(located.located_anchor_stream_time, 26.4)
        self.assertEqual(located.confidence, 0.91)
        runtime.transition_vision_task("match-1:G:key", "encoding")
        encoded = runtime.transition_vision_task(
            "match-1:G:key",
            "encoded",
            result={"output": "/tmp/refined.gif", "bytes": 2468},
        )
        self.assertEqual(encoded.encode_attempt_count, 1)
        self.assertEqual(encoded.output_path, "/tmp/refined.gif")
        self.assertEqual(encoded.result["candidate_count"], 2)
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        persisted = reopened.store.get_vision_task("match-1:G:key")
        self.assertEqual(persisted.status, "encoded")
        self.assertEqual(persisted.output_bytes, 2468)
        self.assertEqual(reopened.recover_incomplete_vision("match-1"), [])
        default_task = reopened.store.get("match-1:G:key")
        self.assertEqual(default_task.status, "pending")
        self.assertIsNone(default_task.output_path)
        reopened.close()

    def test_vision_recovery_resumes_the_interrupted_stage(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        for suffix in ("pending", "locating", "located", "encoding", "encoded"):
            event_key = f"match-1:G:{suffix}"
            discover(runtime, event_key)
            enqueue_vision(runtime, event_key)
        runtime.transition_vision_task("match-1:G:locating", "locating")
        for suffix in ("located", "encoding", "encoded"):
            event_key = f"match-1:G:{suffix}"
            runtime.transition_vision_task(event_key, "locating")
            runtime.transition_vision_task(
                event_key,
                "located",
                result={"anchor_stream_time": 25.5, "confidence": 0.8},
            )
        for suffix in ("encoding", "encoded"):
            runtime.transition_vision_task(f"match-1:G:{suffix}", "encoding")
        runtime.transition_vision_task(
            "match-1:G:encoded",
            "encoded",
            result={"output": "/tmp/already.gif", "bytes": 100},
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        recovered = reopened.recover_incomplete_vision("match-1")
        self.assertEqual(
            {task.event_key: task.status for task in recovered},
            {
                "match-1:G:pending": "pending",
                "match-1:G:locating": "pending",
                "match-1:G:located": "located",
                "match-1:G:encoding": "located",
            },
        )
        self.assertEqual(
            reopened.store.get_vision_task("match-1:G:encoded").status,
            "encoded",
        )
        self.assertEqual(
            reopened.store.get_vision_task("match-1:G:locating").locate_attempt_count,
            1,
        )
        self.assertEqual(
            reopened.store.get_vision_task("match-1:G:encoding").encode_attempt_count,
            1,
        )
        reopened.close()

    def test_vision_failure_can_be_explicitly_retried(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        failed = runtime.transition_vision_task(
            "match-1:G:key", "failed", error="model process exited"
        )
        self.assertEqual(failed.error, "model process exited")
        retried = runtime.transition_vision_task(
            "match-1:G:key", "pending", reason="manual_retry"
        )
        self.assertEqual(retried.status, "pending")
        self.assertIsNone(retried.error)
        runtime.close()

    def test_combined_vision_runner_helpers_follow_the_durable_state_machine(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        started = runtime.start_vision("match-1:G:key")
        self.assertEqual(started.status, "locating")
        lease_id = runtime.acquire_segment_lease(
            "match-1:G:key",
            ["/tmp/helper-segment.ts"],
            expires_in_seconds=30.0,
        )
        self.assertEqual(runtime.release_segment_lease(lease_id), 1)
        completed = runtime.complete_vision(
            "match-1:G:key",
            {
                "vision_anchor_stream_time_sec": 26.0,
                "confidence": 0.88,
                "output": "/tmp/helper-refined.gif",
                "bytes": 55,
            },
        )
        self.assertEqual(completed.status, "encoded")
        self.assertEqual(completed.located_anchor_stream_time, 26.0)
        self.assertEqual(completed.output_path, "/tmp/helper-refined.gif")
        runtime.close()

    def test_complete_vision_preserves_existing_anchor_when_result_omits_it(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            result={"anchor_stream_time": 25.0},
        )
        runtime.transition_vision_task("match-1:G:key", "encoding")

        completed = runtime.complete_vision(
            "match-1:G:key",
            {
                "anchor_stream_time": None,
                "vision_anchor_stream_time_sec": None,
                "output": "/tmp/preserved-anchor.gif",
                "bytes": 55,
            },
        )

        self.assertEqual(completed.status, "encoded")
        self.assertEqual(completed.located_anchor_stream_time, 25.0)
        self.assertEqual(completed.result["anchor_stream_time"], 25.0)
        self.assertEqual(completed.location_metadata["anchor_stream_time"], 25.0)
        runtime.close()

    def test_complete_vision_accepts_zero_anchor(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.start_vision("match-1:G:key")

        completed = runtime.complete_vision(
            "match-1:G:key",
            {
                "anchor_stream_time": 0.0,
                "output": "/tmp/zero-anchor.gif",
                "bytes": 55,
            },
        )

        self.assertEqual(completed.status, "encoded")
        self.assertEqual(completed.located_anchor_stream_time, 0.0)
        self.assertEqual(completed.result["anchor_stream_time"], 0.0)
        runtime.close()

    def test_complete_vision_without_any_anchor_does_not_advance_state(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.start_vision("match-1:G:key")

        with self.assertRaisesRegex(ValueError, "must have an anchor"):
            runtime.complete_vision(
                "match-1:G:key",
                {"output": "/tmp/missing-anchor.gif", "bytes": 55},
            )

        task = runtime.store.get_vision_task("match-1:G:key")
        self.assertEqual(task.status, "locating")
        self.assertIsNone(task.located_anchor_stream_time)
        self.assertNotIn("anchor_stream_time", task.result)
        runtime.close()

    def test_complete_vision_rejects_invalid_anchor_before_advancing_state(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            result={"anchor_stream_time": 25.0},
        )

        with self.assertRaisesRegex(ValueError, "finite number"):
            runtime.complete_vision(
                "match-1:G:key",
                {
                    "anchor_stream_time": float("nan"),
                    "output": "/tmp/invalid-anchor.gif",
                    "bytes": 55,
                },
            )

        task = runtime.store.get_vision_task("match-1:G:key")
        self.assertEqual(task.status, "located")
        self.assertEqual(task.located_anchor_stream_time, 25.0)
        self.assertEqual(task.result["anchor_stream_time"], 25.0)
        runtime.close()

    def test_encoded_transition_preserves_anchor_when_result_reports_none(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            result={"anchor_stream_time": 25.0},
        )
        runtime.transition_vision_task("match-1:G:key", "encoding")

        encoded = runtime.transition_vision_task(
            "match-1:G:key",
            "encoded",
            result={
                "anchor_stream_time": None,
                "output": "/tmp/direct-encoded.gif",
                "bytes": 55,
            },
        )

        self.assertEqual(encoded.located_anchor_stream_time, 25.0)
        self.assertEqual(encoded.result["anchor_stream_time"], 25.0)
        self.assertEqual(encoded.location_metadata["anchor_stream_time"], 25.0)
        runtime.close()

    def test_vision_transition_rejects_invalid_anchor_without_advancing_state(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        for index, invalid_anchor in enumerate(
            (float("nan"), float("inf"), float("-inf"), -1.0, True)
        ):
            event_key = f"match-1:G:invalid-{index}"
            with self.subTest(anchor=invalid_anchor):
                discover(runtime, event_key)
                enqueue_vision(runtime, event_key)
                runtime.transition_vision_task(event_key, "locating")
                with self.assertRaisesRegex(
                    ValueError, "finite number|non-negative"
                ):
                    runtime.transition_vision_task(
                        event_key,
                        "located",
                        result={"anchor_stream_time": invalid_anchor},
                    )

                task = runtime.store.get_vision_task(event_key)
                self.assertEqual(task.status, "locating")
                self.assertIsNone(task.located_anchor_stream_time)
        runtime.close()

    def test_combined_vision_runner_retry_keeps_task_recoverable(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.start_vision("match-1:G:key")
        retried = runtime.retry_vision("match-1:G:key", "waiting for post-roll")
        self.assertEqual(retried.status, "pending")
        self.assertEqual(retried.error, "waiting for post-roll")
        runtime.start_vision("match-1:G:key")
        failed = runtime.fail_vision("match-1:G:key", "model exited")
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.error, "model exited")
        runtime.close()

    def test_vision_transition_requires_anchor_and_output(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        with self.assertRaisesRegex(ValueError, "must have an anchor"):
            runtime.transition_vision_task("match-1:G:key", "located")
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            result={"anchor_stream_time": 25.0},
        )
        runtime.transition_vision_task("match-1:G:key", "encoding")
        with self.assertRaisesRegex(ValueError, "must have an output path"):
            runtime.transition_vision_task("match-1:G:key", "encoded")
        runtime.close()

    def test_segment_lease_ttl_renewal_release_and_protected_paths(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        enqueue_vision(runtime)
        lease_id = runtime.store.acquire_segment_lease(
            "match-1:G:key",
            ["/tmp/segment-1.ts", "/tmp/segment-2.ts", "/tmp/segment-1.ts"],
            owner="vision-worker-1",
            ttl_seconds=30.0,
            now=1000.0,
        )
        self.assertEqual(
            runtime.store.protected_segment_paths(now=1001.0),
            {"/tmp/segment-1.ts", "/tmp/segment-2.ts"},
        )
        self.assertEqual(len(runtime.store.list_segment_leases(active_at=1001.0)), 2)
        self.assertTrue(
            runtime.store.renew_segment_lease(
                lease_id, ttl_seconds=50.0, now=1020.0
            )
        )
        runtime.close()

        reopened = PipelineRuntime(self.database_path, self.log_path)
        self.assertEqual(
            reopened.store.protected_segment_paths(now=1069.0),
            {"/tmp/segment-1.ts", "/tmp/segment-2.ts"},
        )
        self.assertEqual(reopened.store.protected_segment_paths(now=1070.0), set())
        self.assertFalse(
            reopened.store.renew_segment_lease(
                lease_id, ttl_seconds=30.0, now=1070.0
            )
        )
        self.assertEqual(reopened.store.purge_expired_segment_leases(now=1070.0), 2)
        self.assertEqual(reopened.store.release_segment_lease(lease_id), 0)

        second_lease = reopened.store.acquire_segment_lease(
            "match-1:G:key",
            ["/tmp/segment-3.ts"],
            owner="vision-worker-2",
            ttl_seconds=30.0,
            now=2000.0,
        )
        self.assertEqual(reopened.store.release_segment_lease(second_lease), 1)
        self.assertEqual(reopened.store.protected_segment_paths(now=2001.0), set())
        reopened.close()

    def test_event_visual_window_lease_is_early_renewable_and_terminally_released(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        runtime.enqueue_vision_task(
            "match-1:G:key",
            artifact_kind="ocr_window",
            search_start_stream_time=70.0,
            search_end_stream_time=100.0,
            clip_before_seconds=30.0,
            clip_after_seconds=30.0,
            deadline_at_unix=1060.0,
            now=1000.0,
        )
        runtime.enqueue_vision_task(
            "match-1:G:key",
            artifact_kind="tdeed_refined",
            search_start_stream_time=70.0,
            search_end_stream_time=100.0,
            clip_before_seconds=8.0,
            clip_after_seconds=12.0,
            deadline_at_unix=1060.0,
            now=1000.0,
        )
        old_path = self.directory / "old.ts"
        first_path = self.directory / "first.ts"
        added_path = self.directory / "added.ts"
        old_path.write_bytes(b"old")
        first_path.write_bytes(b"first")
        added_path.write_bytes(b"added")
        tasks = runtime.store.list_incomplete_vision_tasks("match-1")

        created = protect_incomplete_vision_event_segments(
            runtime,
            tasks,
            [
                Segment(old_path, 0.0, 5.0),
                Segment(first_path, 15.0, 25.0),
            ],
            ocr_timeout_seconds=30.0,
            vision_timeout_seconds=30.0,
            graceful_stop_timeout_seconds=30.0,
            now_unix=1000.0,
        )
        self.assertEqual(created[0]["new_segment_count"], 1)
        self.assertEqual(
            runtime.store.protected_segment_paths(now=1001.0),
            {str(first_path.resolve())},
        )
        runtime.close()

        runtime = PipelineRuntime(self.database_path, self.log_path)
        tasks = runtime.store.list_incomplete_vision_tasks("match-1")

        renewed = protect_incomplete_vision_event_segments(
            runtime,
            tasks,
            [
                Segment(first_path, 15.0, 25.0),
                Segment(added_path, 90.0, 100.0),
            ],
            ocr_timeout_seconds=30.0,
            vision_timeout_seconds=30.0,
            graceful_stop_timeout_seconds=30.0,
            now_unix=1010.0,
        )
        self.assertEqual(renewed[0]["new_segment_count"], 1)
        self.assertEqual(renewed[0]["renewed_lease_count"], 1)
        worker_lease = runtime.store.acquire_segment_lease(
            "match-1:G:key",
            [str(old_path.resolve())],
            owner="vision-worker",
            ttl_seconds=300.0,
            now=1010.0,
        )

        runtime.transition_vision_task(
            "match-1:G:key",
            "failed",
            artifact_kind="ocr_window",
            error="ocr failed",
        )
        self.assertEqual(release_terminal_event_visual_window_leases(runtime), {})
        runtime.transition_vision_task(
            "match-1:G:key",
            "failed",
            artifact_kind="tdeed_refined",
            error="upstream failed",
        )

        released = release_terminal_event_visual_window_leases(runtime)

        self.assertEqual(released, {"match-1:G:key": 2})
        remaining = runtime.store.list_segment_leases(event_key="match-1:G:key")
        self.assertEqual({lease.lease_id for lease in remaining}, {worker_lease})
        self.assertNotIn(
            EVENT_VISUAL_WINDOW_LEASE_OWNER,
            {lease.owner for lease in remaining},
        )
        runtime.close()

    def test_existing_database_is_migrated_without_changing_event_rows(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        runtime.transition("match-1:G:key", "encoding")
        runtime.transition(
            "match-1:G:key",
            "encoded",
            result={"output": "/tmp/default.gif", "bytes": 77},
        )
        runtime.close()

        connection = sqlite3.connect(self.database_path)
        connection.execute("DROP TABLE vision_tasks")
        connection.execute("DROP TABLE segment_leases")
        connection.commit()
        connection.close()

        migrated = PipelineRuntime(self.database_path, self.log_path)
        existing = migrated.store.get("match-1:G:key")
        self.assertEqual(existing.status, "encoded")
        self.assertEqual(existing.output_path, "/tmp/default.gif")
        self.assertTrue(enqueue_vision(migrated))
        self.assertEqual(
            migrated.store.get_vision_task("match-1:G:key").status, "pending"
        )
        migrated.close()

    def test_legacy_refined_row_migrates_to_tdeed_without_changing_default_gif(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        runtime.transition("match-1:G:key", "encoding")
        runtime.transition(
            "match-1:G:key",
            "encoded",
            result={"output": "/tmp/default.gif", "bytes": 77},
        )
        enqueue_vision(runtime)
        runtime.transition_vision_task("match-1:G:key", "locating")
        runtime.transition_vision_task(
            "match-1:G:key",
            "located",
            result={"anchor_stream_time": 26.0, "confidence": 0.88},
        )
        runtime.transition_vision_task("match-1:G:key", "encoding")
        runtime.transition_vision_task(
            "match-1:G:key",
            "encoded",
            result={"output": "/tmp/legacy-refined.gif", "bytes": 456},
        )
        runtime.close()

        connection = sqlite3.connect(self.database_path)
        connection.executescript(
            """
            DROP INDEX IF EXISTS vision_tasks_match_status;
            ALTER TABLE vision_tasks RENAME TO vision_tasks_current;
            CREATE TABLE vision_tasks AS
            SELECT
                event_key, match_id, code, event_type, artifact_kind, status,
                source_anchor_stream_time, source_anchor_source_time,
                search_start_stream_time, search_end_stream_time,
                clip_before_seconds, clip_after_seconds,
                model_name, model_version, model_weights_sha256,
                created_at_unix, updated_at_unix,
                locating_started_at_unix, located_at_unix,
                encoding_started_at_unix, encoded_at_unix, failed_at_unix,
                locate_attempt_count, encode_attempt_count,
                located_anchor_stream_time, located_anchor_source_time,
                confidence, inference_seconds, output_path, output_bytes,
                result_json, error
            FROM vision_tasks_current;
            UPDATE vision_tasks SET artifact_kind = 'refined';
            DROP TABLE vision_tasks_current;
            """
        )
        connection.close()

        migrated = PipelineRuntime(self.database_path, self.log_path)
        legacy_alias = migrated.store.get_vision_task("match-1:G:key", "refined")
        self.assertEqual(legacy_alias.artifact_kind, "tdeed_refined")
        self.assertEqual(legacy_alias.status, "encoded")
        self.assertEqual(legacy_alias.output_path, "/tmp/legacy-refined.gif")
        self.assertEqual(legacy_alias.output_bytes, 456)
        self.assertEqual(legacy_alias.location_metadata, {})
        self.assertEqual(legacy_alias.window_metadata, {})
        default_task = migrated.store.get("match-1:G:key")
        self.assertEqual(default_task.status, "encoded")
        self.assertEqual(default_task.output_path, "/tmp/default.gif")
        self.assertEqual(default_task.output_bytes, 77)
        migrated.close()

    def test_stored_task_rebuilds_the_pipeline_job(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        stored = runtime.store.get("match-1:G:key")
        job = recovered_event_job(stored)
        self.assertEqual(job.match_event.person, "Player A")
        self.assertEqual(job.pending.stream_time, 24.0)
        self.assertEqual(job.pending.source_time, 124.0)
        self.assertEqual(job.pending.status, "pending")
        runtime.close()

    def test_buffer_not_ready_returns_task_to_pending_then_can_encode(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        discover(runtime)
        job = recovered_event_job(runtime.store.get("match-1:G:key"))
        partial_path = self.directory / "partial.ts"
        complete_path = self.directory / "complete.ts"
        partial_path.write_bytes(b"partial")
        complete_path.write_bytes(b"complete")
        current_segments = [[
            type("TestSegment", (), {
                "path": partial_path,
                "start": 0.0,
                "end": 30.0,
            })()
        ]]
        encode_arguments = (
            job,
            runtime,
            "ffmpeg",
            "ffprobe",
            lambda: current_segments[0],
            self.directory,
        )
        encode_options = {
            "before": 12.0,
            "after": 18.0,
            "width": 384,
            "fps": 6.0,
            "colors": 160,
            "size_reference_bytes": 10_000_000,
            "allow_degraded": True,
        }
        self.assertFalse(encode_event_job(*encode_arguments, **encode_options))
        waiting = runtime.store.get("match-1:G:key")
        self.assertEqual(waiting.status, "pending")
        self.assertEqual(waiting.attempt_count, 0)
        self.assertEqual(waiting.readiness_check_count, 1)
        self.assertEqual(waiting.last_error_kind, "waiting_for_tail")

        current_segments[0] = [
            type("TestSegment", (), {
                "path": complete_path,
                "start": 0.0,
                "end": 100.0,
            })()
        ]
        runtime.store.connection.execute(
            "UPDATE event_tasks SET next_attempt_at_unix = 0 WHERE event_key = ?",
            ("match-1:G:key",),
        )
        runtime.store.connection.commit()

        result = {
            "output": "/tmp/goal.gif",
            "bytes": 5000,
            "duration_sec": 30.0,
            "encode_seconds": 1.0,
            "over_size_reference": False,
        }
        with patch("event_driven_pipeline.encode_gif", return_value=result):
            self.assertTrue(encode_event_job(*encode_arguments, **encode_options))
        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "encoded")
        self.assertEqual(task.attempt_count, 1)
        self.assertEqual(task.output_bytes, 5000)
        runtime.close()

    def test_readiness_backoff_and_deadline_survive_reopen(self):
        with TaskStateStore(self.database_path) as store:
            store.discover(
                match_id="match-1",
                event_data=event_data(),
                observed_stream_time=25.0,
                observed_source_time=None,
                clip_anchor_stream_time=24.0,
                clip_anchor_source_time=None,
                output_due_stream_time=51.0,
                detected_at_unix=1000.0,
                deadline_at_unix=1015.0,
                now=1000.0,
            )
            store.transition("match-1:G:key", "pending", now=1000.0)
            first = store.record_readiness_wait(
                "match-1:G:key",
                "tail missing",
                error_kind="waiting_for_tail",
                now=1000.0,
            )
            second = store.record_readiness_wait(
                "match-1:G:key",
                "tail missing",
                error_kind="waiting_for_tail",
                now=1002.0,
            )
            third = store.record_readiness_wait(
                "match-1:G:key",
                "tail missing",
                error_kind="waiting_for_tail",
                now=1006.0,
            )
            fourth = store.record_readiness_wait(
                "match-1:G:key",
                "tail missing",
                error_kind="waiting_for_tail",
                now=1014.0,
            )
            self.assertEqual(first.next_attempt_at_unix, 1002.0)
            self.assertEqual(second.next_attempt_at_unix, 1006.0)
            self.assertEqual(third.next_attempt_at_unix, 1014.0)
            self.assertEqual(fourth.next_attempt_at_unix, 1015.0)
            self.assertEqual(fourth.attempt_count, 0)
            self.assertEqual(fourth.readiness_check_count, 4)

        with TaskStateStore(self.database_path) as reopened:
            task = reopened.get("match-1:G:key")
            self.assertEqual(task.deadline_at_unix, 1015.0)
            self.assertEqual(task.next_attempt_at_unix, 1015.0)
            self.assertEqual(task.last_error_kind, "waiting_for_tail")

    def test_expired_buffer_wait_fails_without_encoding_attempt(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        runtime.discover_task(
            match_id="match-1",
            event_data=event_data(),
            observed_stream_time=25.0,
            observed_source_time=125.0,
            clip_anchor_stream_time=24.0,
            clip_anchor_source_time=124.0,
            output_due_stream_time=51.0,
            detected_at_unix=time.time() - 100.0,
            deadline_at_unix=time.time() - 1.0,
        )
        job = recovered_event_job(runtime.store.get("match-1:G:key"))
        partial = self.directory / "partial-deadline.ts"
        partial.write_bytes(b"partial")
        segment = type("TestSegment", (), {
            "path": partial,
            "start": 0.0,
            "end": 30.0,
        })()

        self.assertTrue(encode_event_job(
            job,
            runtime,
            "ffmpeg",
            "ffprobe",
            lambda: [segment],
            self.directory,
            before=12.0,
            after=18.0,
            width=384,
            fps=6.0,
            colors=160,
            size_reference_bytes=10_000_000,
        ))

        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.attempt_count, 0)
        self.assertEqual(task.readiness_check_count, 0)
        self.assertEqual(task.last_error_kind, "buffer_deadline_exceeded")
        self.assertEqual(task.result["error_kind"], "buffer_deadline_exceeded")
        runtime.close()

    def test_expired_tail_uses_anchor_component_when_degraded_is_enabled(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        runtime.discover_task(
            match_id="match-1",
            event_data=event_data(),
            observed_stream_time=25.0,
            observed_source_time=125.0,
            clip_anchor_stream_time=24.0,
            clip_anchor_source_time=124.0,
            output_due_stream_time=51.0,
            detected_at_unix=time.time() - 100.0,
            deadline_at_unix=time.time() - 1.0,
        )
        job = recovered_event_job(runtime.store.get("match-1:G:key"))
        partial = self.directory / "partial-degraded-deadline.ts"
        partial.write_bytes(b"partial")
        segment = type("TestSegment", (), {
            "path": partial,
            "start": 0.0,
            "end": 30.0,
        })()
        encoded = {
            "output": "/tmp/goal-degraded.gif",
            "bytes": 4000,
            "duration_sec": 18.0,
            "encode_seconds": 1.0,
            "over_size_reference": False,
            "coverage_status": "ready_degraded",
        }

        with patch("event_driven_pipeline.encode_gif", return_value=encoded) as encode:
            self.assertTrue(encode_event_job(
                job,
                runtime,
                "ffmpeg",
                "ffprobe",
                lambda: [segment],
                self.directory,
                before=12.0,
                after=18.0,
                width=384,
                fps=6.0,
                colors=160,
                size_reference_bytes=10_000_000,
                allow_degraded=True,
            ))

        coverage = encode.call_args.kwargs["coverage"]
        self.assertEqual(coverage.status.value, "ready_degraded")
        self.assertEqual(coverage.error_kind, "degraded_deadline")
        self.assertEqual(
            (coverage.effective_start, coverage.effective_end),
            (12.0, 30.0),
        )
        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "encoded")
        self.assertEqual(task.attempt_count, 1)
        runtime.close()

    def test_expired_tiny_anchor_component_fails_without_encoding(self):
        runtime = PipelineRuntime(self.database_path, self.log_path)
        runtime.discover_task(
            match_id="match-1",
            event_data=event_data(),
            observed_stream_time=25.0,
            observed_source_time=125.0,
            clip_anchor_stream_time=24.0,
            clip_anchor_source_time=124.0,
            output_due_stream_time=51.0,
            detected_at_unix=time.time() - 100.0,
            deadline_at_unix=time.time() - 1.0,
        )
        job = recovered_event_job(runtime.store.get("match-1:G:key"))
        tiny = self.directory / "tiny-degraded-deadline.ts"
        tiny.write_bytes(b"partial")
        segment = type("TestSegment", (), {
            "path": tiny,
            "start": 23.5,
            "end": 24.5,
        })()

        with patch("event_driven_pipeline.encode_gif") as encode:
            self.assertTrue(encode_event_job(
                job,
                runtime,
                "ffmpeg",
                "ffprobe",
                lambda: [segment],
                self.directory,
                before=12.0,
                after=18.0,
                width=384,
                fps=6.0,
                colors=160,
                size_reference_bytes=10_000_000,
                allow_degraded=True,
                min_degraded_seconds=2.0,
            ))

        encode.assert_not_called()
        task = runtime.store.get("match-1:G:key")
        self.assertEqual(task.status, "failed")
        self.assertEqual(task.last_error_kind, "degraded_clip_too_short")
        runtime.close()


class VisionQueueTimingTests(unittest.TestCase):
    def test_stage_wait_persists_custom_retry_and_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            try:
                discover(runtime)
                runtime.enqueue_vision_task(
                    "match-1:G:key",
                    artifact_kind="ocr_window",
                    search_start_stream_time=0.0,
                    search_end_stream_time=80.0,
                    clip_before_seconds=60.0,
                    clip_after_seconds=0.0,
                    deadline_at_unix=1060.0,
                    now=1000.0,
                )
                waited = runtime.record_vision_readiness_wait(
                    "match-1:G:key",
                    "target clock is still 70 seconds away",
                    artifact_kind="ocr_window",
                    error_kind="waiting_for_clock_target",
                    next_attempt_at_unix=1030.0,
                    deadline_at_unix=1090.0,
                    window_metadata={
                        "progressive_scan": {
                            "state": "waiting_for_clock_target",
                            "deadline_policy": {
                                "target_deadline_at_unix": 1090.0,
                            },
                        },
                    },
                    now=1000.0,
                )

                self.assertEqual(waited.next_attempt_at_unix, 1030.0)
                self.assertEqual(waited.deadline_at_unix, 1090.0)
                runtime.close()
                runtime = PipelineRuntime(
                    root / "state.sqlite3",
                    root / "events.jsonl",
                )
                reopened = runtime.store.get_vision_task(
                    "match-1:G:key",
                    "ocr_window",
                )
                self.assertEqual(reopened.next_attempt_at_unix, 1030.0)
                self.assertEqual(reopened.deadline_at_unix, 1090.0)
                self.assertEqual(
                    reopened.window_metadata["progressive_scan"][
                        "deadline_policy"
                    ]["target_deadline_at_unix"],
                    1090.0,
                )
            finally:
                runtime.close()

    def test_actual_queue_wait_extends_only_the_artifact_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            try:
                discover(runtime)
                runtime.enqueue_vision_task(
                    "match-1:G:key",
                    artifact_kind="ocr_window",
                    search_start_stream_time=0.0,
                    search_end_stream_time=80.0,
                    clip_before_seconds=60.0,
                    clip_after_seconds=0.0,
                    deadline_at_unix=1060.0,
                    now=1000.0,
                )
                runtime.enqueue_vision_task(
                    "match-1:G:key",
                    artifact_kind="tdeed_refined",
                    search_start_stream_time=0.0,
                    search_end_stream_time=80.0,
                    clip_before_seconds=8.0,
                    clip_after_seconds=12.0,
                    deadline_at_unix=1070.0,
                    now=1000.0,
                )
                runtime.record_vision_queue_phase(
                    "match-1:G:key",
                    "queued",
                    artifact_kind="ocr_window",
                    now=1010.0,
                )
                acquired = runtime.record_vision_queue_phase(
                    "match-1:G:key",
                    "acquired",
                    artifact_kind="ocr_window",
                    queued_at_unix=1010.0,
                    now=1035.0,
                )
                executing = runtime.record_vision_queue_phase(
                    "match-1:G:key",
                    "executing",
                    artifact_kind="ocr_window",
                    queued_at_unix=1010.0,
                    now=1036.0,
                )

                self.assertEqual(acquired.deadline_at_unix, 1085.0)
                self.assertEqual(executing.deadline_at_unix, 1085.0)
                self.assertEqual(
                    runtime.store.get_vision_task(
                        "match-1:G:key",
                        "tdeed_refined",
                    ).deadline_at_unix,
                    1070.0,
                )
                timing = executing.window_metadata["queue_timing"]
                self.assertEqual(timing["phase"], "executing")
                self.assertEqual(timing["queue_wait_seconds"], 25.0)
                self.assertEqual(timing["deadline_extension_seconds"], 25.0)
                self.assertEqual(timing["execution_started_at_unix"], 1036.0)
            finally:
                runtime.close()


if __name__ == "__main__":
    unittest.main()
