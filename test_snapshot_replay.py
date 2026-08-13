import json
import tempfile
import unittest
from pathlib import Path

from event_driven_pipeline import parse_match_events
from event_snapshot_replay import SnapshotReplayEventSource


def payload(*events):
    buckets = {}
    for event in events:
        minute = str(event["minute"])
        bucket = buckets.setdefault(
            minute,
            {"minute": minute, "teamAEvents": [], "teamBEvents": []},
        )
        bucket[f"{event.get('team', 'teamA')}Events"].append(event)
    return {"status": 0, "match_status": "Playing", "events": buckets}


class SnapshotReplayEventSourceTests(unittest.TestCase):
    def make_source(self, steps, emit_existing=False):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        path = Path(temporary_directory.name) / "replay.json"
        path.write_text(json.dumps({"steps": steps}), encoding="utf-8")
        return SnapshotReplayEventSource(
            path,
            lambda response: parse_match_events(response, "match-1"),
            emit_existing,
        )

    def test_replays_full_snapshots_with_duplicates_failure_and_late_event(self):
        goal = {"code": "G", "minute": "18", "person_id": "1"}
        yellow = {
            "code": "YC",
            "minute": "28",
            "person_id": "2",
            "team": "teamB",
        }
        first_snapshot = payload(goal)
        later_snapshot = payload(goal, yellow)
        source = self.make_source(
            [
                {"at_stream_sec": 0, "payload": payload()},
                {"at_stream_sec": 5, "payload": first_snapshot},
                {"at_stream_sec": 6, "payload": first_snapshot},
                {"at_stream_sec": 8, "error": "simulated timeout"},
                {"at_stream_sec": 15, "payload": later_snapshot},
            ]
        )

        self.assertEqual(source.poll(0, 100), [])
        self.assertEqual(source.poll(4.99, 101), [])
        self.assertEqual([event.code for event in source.poll(5, 102)], ["G"])
        self.assertEqual(source.poll(6, 103), [])
        self.assertEqual(source.poll(8, 104), [])
        self.assertEqual(source.last_error, "simulated timeout")
        self.assertEqual([event.code for event in source.poll(15, 105)], ["YC"])
        self.assertIsNone(source.last_error)

        report = source.report()
        self.assertEqual(report["consumed_steps"], 5)
        self.assertEqual(report["response_count"], 4)
        self.assertEqual(report["error_count"], 1)

    def test_first_successful_snapshot_can_seed_existing_history(self):
        goal = {"code": "G", "minute": "18", "person_id": "1"}
        source = self.make_source(
            [
                {"at_stream_sec": 0, "error": "API unavailable"},
                {"at_stream_sec": 1, "payload": payload(goal)},
            ]
        )

        self.assertEqual(source.poll(0, 0), [])
        self.assertFalse(source.initialized)
        self.assertEqual(source.poll(1, 0), [])
        self.assertTrue(source.initialized)
        self.assertEqual(len(source.seen), 1)

    def test_emit_existing_returns_initial_snapshot_events(self):
        red = {"code": "RC", "minute": "90", "person_id": "3"}
        source = self.make_source(
            [{"at_stream_sec": 0, "payload": payload(red)}],
            emit_existing=True,
        )

        self.assertEqual([event.code for event in source.poll(0, 0)], ["RC"])
        self.assertEqual(source.poll(1, 0), [])

    def test_invalid_api_payload_is_counted_and_does_not_initialize(self):
        source = self.make_source(
            [{"at_stream_sec": 0, "payload": {"status": 500, "events": {}}}]
        )

        self.assertEqual(source.poll(0, 0), [])
        self.assertFalse(source.initialized)
        self.assertEqual(source.error_count, 1)
        self.assertIn("status=500", source.last_error)

    def test_rejects_ambiguous_step(self):
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.make_source(
                [{"at_stream_sec": 0, "payload": payload(), "error": "timeout"}]
            )

    def test_rejects_non_finite_schedule_time(self):
        with self.assertRaisesRegex(ValueError, "invalid at_stream_sec"):
            self.make_source([{"at_stream_sec": "NaN", "payload": payload()}])


if __name__ == "__main__":
    unittest.main()
