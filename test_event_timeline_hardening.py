import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from event_driven_pipeline import (
    manifest_for_active_source,
    main,
    resumed_stream_time,
    stream_rate_for_mode,
    timeline_calibration_mismatches,
    vision_deadline_at,
    vision_search_window,
)
from pipeline_runtime import TaskStateStore, TimelineState
from segment_manifest import (
    load_segment_manifest,
    new_segment_manifest,
    save_segment_manifest,
    upsert_segment_generation,
)


class _RunningProcess:
    def poll(self):
        return None

    def wait(self):
        return 0


class _OneRunSupervisor:
    def __init__(self, *args, **kwargs):
        del args, kwargs
        self.process = _RunningProcess()
        self.generation = -1
        self.restart_count = 0

    def start(self, now_monotonic=None):
        del now_monotonic
        self.generation += 1
        return self.process

    def observe_exit(self):
        return None

    def terminate(self):
        pass

    def close(self):
        pass


class _InterruptingEventSource:
    error_count = 0
    poll_count = 0
    last_error = None

    def poll(self, stream_time, now_monotonic):
        del stream_time, now_monotonic
        self.poll_count += 1
        raise KeyboardInterrupt

    def report(self):
        return {"type": "test", "poll_count": self.poll_count}


class EventTimelineHardeningTests(unittest.TestCase):
    def test_real_rtmp_ignores_replay_speed_but_simulation_uses_readrate(self):
        self.assertEqual(
            stream_rate_for_mode(simulate_live=False, replay_speed=8.0),
            1.0,
        )
        self.assertEqual(
            stream_rate_for_mode(simulate_live=True, replay_speed=4.0),
            4.0,
        )

    def test_simulated_restart_resumes_checkpoint_without_wall_clock_gap(self):
        timeline = TimelineState(
            match_id="match-1",
            timeline_origin_wall_unix=1_000.0,
            last_stream_time=42.0,
        )
        resumed = resumed_stream_time(
            timeline,
            pipeline_started_wall=2_000.0,
            simulate_live=True,
            replay_speed=4.0,
        )
        self.assertEqual(resumed, 42.0)

    def test_real_rtmp_restart_advances_from_wall_clock_at_one_x(self):
        timeline = TimelineState(
            match_id="match-1",
            timeline_origin_wall_unix=1_000.0,
            last_stream_time=42.0,
        )
        resumed = resumed_stream_time(
            timeline,
            pipeline_started_wall=1_050.0,
            simulate_live=False,
            replay_speed=4.0,
        )
        self.assertEqual(resumed, 50.0)

    def test_visual_window_uses_raw_api_observation_not_match_clock(self):
        start, end = vision_search_window(
            clip_anchor=100.0,
            match_clock_anchor=135.0,
            buffer_seconds=180.0,
            segment_slack=7.0,
            search_before=120.0,
            search_after=0.0,
            minute_uncertainty=60.0,
        )
        self.assertEqual(start, 0.0)
        self.assertEqual(end, 100.0)

    def test_visual_window_respects_retention_floor(self):
        start, end = vision_search_window(
            clip_anchor=200.0,
            match_clock_anchor=10.0,
            buffer_seconds=180.0,
            segment_slack=7.0,
            search_before=120.0,
            search_after=0.0,
            minute_uncertainty=60.0,
        )
        self.assertEqual(start, 80.0)
        self.assertEqual(end, 200.0)

    def test_vision_deadline_extends_to_complete_the_stream_window(self):
        deadline, wait_budget = vision_deadline_at(
            detected_at_unix=1_000.0,
            current_stream_time=100.0,
            search_end_stream_time=195.0,
            stream_rate=1.0,
            segment_slack=7.0,
            configured_deadline_seconds=60.0,
        )
        self.assertEqual(wait_budget, 102.0)
        self.assertEqual(deadline, 1_102.0)

    def test_vision_deadline_scales_wait_for_fast_local_replay(self):
        deadline, wait_budget = vision_deadline_at(
            detected_at_unix=1_000.0,
            current_stream_time=100.0,
            search_end_stream_time=195.0,
            stream_rate=4.0,
            segment_slack=7.0,
            configured_deadline_seconds=60.0,
        )
        self.assertEqual(wait_budget, 60.0)
        self.assertEqual(deadline, 1_060.0)

    def test_persisted_calibration_mismatch_is_reported(self):
        timeline = TimelineState(
            match_id="match-1",
            timeline_origin_wall_unix=1_000.0,
            match_start_at_unix=2_000.0,
            broadcast_delay_seconds=8.0,
            halftime_break_seconds=900.0,
        )
        mismatches = timeline_calibration_mismatches(
            timeline,
            match_start_at_unix=2_001.0,
            broadcast_delay_seconds=9.0,
            halftime_break_seconds=900.0,
        )
        self.assertEqual(
            set(mismatches),
            {"match_start_at_unix", "broadcast_delay_seconds"},
        )

    def test_identical_persisted_calibration_is_accepted(self):
        timeline = TimelineState(
            match_id="match-1",
            timeline_origin_wall_unix=1_000.0,
            match_start_at_unix=2_000.0,
            broadcast_delay_seconds=8.0,
            halftime_break_seconds=900.0,
        )
        self.assertEqual(
            timeline_calibration_mismatches(
                timeline,
                match_start_at_unix=2_000.0005,
                broadcast_delay_seconds=8.0005,
                halftime_break_seconds=900.0,
            ),
            {},
        )


class SourceIdentitySafetyTests(unittest.TestCase):
    def test_same_resource_keeps_existing_generations(self):
        manifest = new_segment_manifest(
            "match-1",
            "rtmp://example/live/match-1",
            1_000.0,
        )
        manifest = upsert_segment_generation(
            manifest,
            list_path=Path("segments.csv"),
            stream_offset=10.0,
            started_at_wall=1_010.0,
        )
        active, discarded = manifest_for_active_source(
            manifest,
            requested_source="rtmp://example/live/match-1",
            timeline_origin_wall=1_000.0,
        )
        self.assertIs(active, manifest)
        self.assertEqual(discarded, 0)
        self.assertEqual(len(active.generations), 1)

    def test_changed_source_detaches_old_generations_without_deleting_media(self):
        with tempfile.TemporaryDirectory() as directory:
            old_list = Path(directory) / "segments.csv"
            old_list.write_text("old.ts,0,2\n", encoding="utf-8")
            manifest = new_segment_manifest(
                "match-1",
                "rtmp://example/live/match-1?token=old",
                1_000.0,
            )
            manifest = upsert_segment_generation(
                manifest,
                list_path=old_list,
                stream_offset=10.0,
                started_at_wall=1_010.0,
            )

            active, discarded = manifest_for_active_source(
                manifest,
                requested_source="rtmp://example/live/match-1?token=new",
                timeline_origin_wall=1_000.0,
            )

            self.assertEqual(active.source, "rtmp://example/live/match-1?token=new")
            self.assertEqual(active.timeline_origin_wall, 1_000.0)
            self.assertEqual(active.generations, ())
            self.assertEqual(discarded, 1)
            self.assertTrue(old_list.exists())

    def test_worker_source_switch_starts_with_only_a_new_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            buffer_dir = output_dir / "buffer"
            buffer_dir.mkdir(parents=True)
            old_segment = buffer_dir / "old.ts"
            old_segment.write_bytes(b"old-media")
            old_list = buffer_dir / "segments_old.csv"
            old_list.write_text("old.ts,0,2\n", encoding="utf-8")
            origin = time.time()
            manifest_path = buffer_dir / "segment_manifest.json"
            manifest = new_segment_manifest(
                "match-1",
                "rtmp://example/live/old",
                origin,
            )
            manifest = upsert_segment_generation(
                manifest,
                list_path=old_list.relative_to(buffer_dir),
                stream_offset=0.0,
                started_at_wall=origin,
            )
            save_segment_manifest(manifest_path, manifest)
            with TaskStateStore(output_dir / "pipeline_state.sqlite3") as store:
                store.upsert_timeline_state(
                    TimelineState(
                        match_id="match-1",
                        timeline_origin_wall_unix=origin,
                    ),
                    now=origin,
                )

            with patch.object(
                sys,
                "argv",
                [
                    "event_driven_pipeline.py",
                    "rtmp://example/live/new",
                    "--event-url",
                    "https://example.test/{match_id}",
                    "--match-id",
                    "match-1",
                    "--output-dir",
                    str(output_dir),
                ],
            ), patch(
                "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
            ), patch(
                "event_driven_pipeline.IngestSupervisor", _OneRunSupervisor
            ), patch(
                "event_driven_pipeline.HttpMatchEventSource",
                return_value=_InterruptingEventSource(),
            ):
                main()

            active = load_segment_manifest(
                manifest_path,
                expected_match_id="match-1",
                expected_source="rtmp://example/live/new",
                drop_stale=False,
            )
            self.assertIsNotNone(active)
            self.assertEqual(len(active.generations), 1)
            self.assertNotEqual(
                active.generations[0].list_path,
                Path("segments_old.csv"),
            )
            self.assertTrue(old_list.exists())
            self.assertTrue(old_segment.exists())
            log_entries = [
                json.loads(line)
                for line in (output_dir / "pipeline_events.jsonl").read_text(
                    encoding="utf-8"
                ).splitlines()
            ]
            source_reset = next(
                item
                for item in log_entries
                if item["event"] == "segment_manifest_source_reset"
            )
            self.assertEqual(source_reset["discarded_generation_count"], 1)
            self.assertFalse(source_reset["old_media_deleted"])

    def test_main_rejects_changed_persisted_timeline_calibration(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            buffer_dir = output_dir / "buffer"
            buffer_dir.mkdir(parents=True)
            origin = time.time()
            save_segment_manifest(
                buffer_dir / "segment_manifest.json",
                new_segment_manifest(
                    "match-1",
                    "rtmp://example/live/match-1",
                    origin,
                ),
            )
            with TaskStateStore(output_dir / "pipeline_state.sqlite3") as store:
                store.upsert_timeline_state(
                    TimelineState(
                        match_id="match-1",
                        timeline_origin_wall_unix=origin,
                        match_start_at_unix=2_000.0,
                        broadcast_delay_seconds=8.0,
                    ),
                    now=origin,
                )

            coordinator = Mock()
            with patch.object(
                sys,
                "argv",
                [
                    "event_driven_pipeline.py",
                    "rtmp://example/live/match-1",
                    "--event-url",
                    "https://example.test/{match_id}",
                    "--match-id",
                    "match-1",
                    "--match-start-play",
                    "2001",
                    "--broadcast-delay-seconds",
                    "8",
                    "--output-dir",
                    str(output_dir),
                ],
            ), patch(
                "event_driven_pipeline.shutil.which", return_value="/usr/bin/true"
            ), patch(
                "event_driven_pipeline.HeavyTaskCoordinator.from_environment",
                return_value=coordinator,
            ):
                with self.assertRaisesRegex(SystemExit, "fresh --output-dir"):
                    main()
            coordinator.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
