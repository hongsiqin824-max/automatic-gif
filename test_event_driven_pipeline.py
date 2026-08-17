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
    HttpMatchEventSource,
    MatchEvent,
    MockMatchEventSource,
    encode_event_job,
    event_timing_diagnostics,
    heavy_snapshot_has_default_gif_work,
    merge_observed_event_revision,
    main,
    load_scoreboard_profile,
    observe_segment_progress,
    parse_match_start_play,
    parse_match_events,
)
from live_goal_pipeline import PendingEvent, Segment
from live_runtime import ProcessExit


class FakeHttpResponse:
    def __init__(self, payload):
        self.body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.body


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

    def test_default_gif_work_blocks_optional_vision_submission(self):
        snapshot = {
            "active": {"items": [{"task_kind": "vision"}]},
            "waiting": {"items": [{"task_kind": "gif"}]},
        }
        self.assertTrue(heavy_snapshot_has_default_gif_work(snapshot))

    def test_only_vision_work_does_not_report_default_gif_pressure(self):
        snapshot = {
            "active": {"items": [{"task_kind": "vision"}]},
            "waiting": {"items": []},
        }
        self.assertFalse(heavy_snapshot_has_default_gif_work(snapshot))


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
