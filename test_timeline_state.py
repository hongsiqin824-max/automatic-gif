import json
import math
import tempfile
import unittest
from pathlib import Path

from pipeline_runtime import (
    DEFAULT_HALFTIME_BREAK_SECONDS,
    TaskStateStore,
    TimelineState,
    coarse_event_elapsed_seconds,
)


class TimelineStateTests(unittest.TestCase):
    def test_wall_and_stream_times_are_reversible(self):
        state = TimelineState(
            match_id="54478923",
            timeline_origin_wall_unix=1_000.0,
            timeline_origin_stream_time=120.0,
        )

        self.assertEqual(state.wall_to_stream_time(1_025.5), 145.5)
        self.assertEqual(state.stream_to_wall_time(145.5), 1_025.5)

    def test_event_minute_estimate_includes_halftime_and_broadcast_delay(self):
        state = TimelineState(
            match_id="54478923",
            timeline_origin_wall_unix=900.0,
            match_start_at_unix=1_000.0,
            broadcast_delay_seconds=8.0,
        )

        self.assertEqual(state.coarse_event_stream_time("18", "0"), 1_188.0)
        self.assertEqual(state.coarse_event_stream_time("63", "0"), 4_788.0)
        self.assertEqual(state.coarse_event_stream_time("90+2"), 6_528.0)

    def test_minute_helper_accepts_feed_strings_and_rejects_conflicts(self):
        self.assertEqual(coarse_event_elapsed_seconds("45'", "2"), 2_820.0)
        self.assertEqual(
            coarse_event_elapsed_seconds("46", "0"),
            46.0 * 60.0 + DEFAULT_HALFTIME_BREAK_SECONDS,
        )
        with self.assertRaisesRegex(ValueError, "conflicting"):
            coarse_event_elapsed_seconds("45+2", "3")
        with self.assertRaises(ValueError):
            coarse_event_elapsed_seconds("half time")

    def test_serialization_supplies_backward_compatible_defaults(self):
        state = TimelineState.from_mapping(
            {
                "match_id": "54478923",
                "timeline_origin_wall_unix": 1_000.0,
                "timeline_origin_stream_time": 25.0,
            }
        )

        self.assertEqual(state.broadcast_delay_seconds, 0.0)
        self.assertEqual(
            state.halftime_break_seconds, DEFAULT_HALFTIME_BREAK_SECONDS
        )
        self.assertEqual(state.last_stream_time, 25.0)
        restored = TimelineState.from_mapping(json.loads(json.dumps(state.to_dict())))
        self.assertEqual(restored, state)

    def test_state_validation_rejects_invalid_numbers(self):
        with self.assertRaises(ValueError):
            TimelineState(match_id="", timeline_origin_wall_unix=1.0)
        with self.assertRaises(ValueError):
            TimelineState(match_id="1", timeline_origin_wall_unix=math.inf)
        with self.assertRaises(ValueError):
            TimelineState(
                match_id="1",
                timeline_origin_wall_unix=1.0,
                broadcast_delay_seconds=-1.0,
            )


class TimelineStateStoreTests(unittest.TestCase):
    def test_state_and_monotonic_checkpoint_survive_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "state.sqlite3"
            state = TimelineState(
                match_id="54478923",
                timeline_origin_wall_unix=1_000.0,
                match_start_at_unix=1_100.0,
                broadcast_delay_seconds=8.0,
                last_stream_time=20.0,
            )
            with TaskStateStore(database_path) as store:
                saved = store.upsert_timeline_state(state, now=2_000.0)
                self.assertEqual(saved.created_at_unix, 2_000.0)
                advanced = store.checkpoint_timeline(
                    "54478923", 40.0, now=2_010.0
                )
                self.assertEqual(advanced.last_stream_time, 40.0)
                stale = store.checkpoint_timeline(
                    "54478923", 35.0, now=2_020.0
                )
                self.assertEqual(stale.last_stream_time, 40.0)
                self.assertEqual(stale.updated_at_unix, 2_010.0)

            with TaskStateStore(database_path) as reopened:
                restored = reopened.get_timeline_state("54478923")
                self.assertIsNotNone(restored)
                self.assertEqual(restored.last_stream_time, 40.0)
                self.assertEqual(restored.created_at_unix, 2_000.0)
                self.assertEqual(restored.updated_at_unix, 2_010.0)

    def test_upsert_updates_calibration_without_rewinding_checkpoint(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "state.sqlite3"
            with TaskStateStore(database_path) as store:
                first = TimelineState(
                    match_id="54478923",
                    timeline_origin_wall_unix=1_000.0,
                    last_stream_time=50.0,
                )
                store.upsert_timeline_state(first, now=2_000.0)
                recalibrated = TimelineState(
                    match_id="54478923",
                    timeline_origin_wall_unix=1_001.5,
                    match_start_at_unix=1_100.0,
                    broadcast_delay_seconds=6.0,
                    last_stream_time=10.0,
                )
                saved = store.upsert_timeline_state(recalibrated, now=2_100.0)

                self.assertEqual(saved.timeline_origin_wall_unix, 1_001.5)
                self.assertEqual(saved.broadcast_delay_seconds, 6.0)
                self.assertEqual(saved.last_stream_time, 50.0)
                self.assertEqual(saved.created_at_unix, 2_000.0)
                self.assertEqual(saved.updated_at_unix, 2_100.0)

    def test_checkpoint_requires_existing_timeline(self):
        with tempfile.TemporaryDirectory() as directory:
            database_path = Path(directory) / "state.sqlite3"
            with TaskStateStore(database_path) as store:
                with self.assertRaises(KeyError):
                    store.checkpoint_timeline("missing", 1.0, now=2.0)


if __name__ == "__main__":
    unittest.main()
