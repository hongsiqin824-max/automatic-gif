import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from event_driven_pipeline import encode_event_job, recovered_event_job
from live_goal_pipeline import BufferNotReady
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
        encode_arguments = (
            job,
            runtime,
            "ffmpeg",
            "ffprobe",
            lambda: [],
            self.directory,
        )
        encode_options = {
            "before": 12.0,
            "after": 18.0,
            "width": 384,
            "fps": 6.0,
            "colors": 160,
            "size_reference_bytes": 10_000_000,
        }
        with patch(
            "event_driven_pipeline.encode_gif",
            side_effect=BufferNotReady("waiting for post-roll"),
        ):
            self.assertFalse(encode_event_job(*encode_arguments, **encode_options))
        self.assertEqual(runtime.store.get("match-1:G:key").status, "pending")

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
        self.assertEqual(task.attempt_count, 2)
        self.assertEqual(task.output_bytes, 5000)
        runtime.close()


if __name__ == "__main__":
    unittest.main()
