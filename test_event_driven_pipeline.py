import json
import signal
import tempfile
import unittest
import urllib.error
import sys
from pathlib import Path
from unittest.mock import patch

from event_driven_pipeline import (
    EventRevisionTracker,
    HttpMatchEventSource,
    MatchEvent,
    MockMatchEventSource,
    main,
    parse_match_events,
)
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


class ReconnectingSupervisor:
    def __init__(self, *args, **kwargs):
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

    def close(self):
        pass


class EventParsingTests(unittest.TestCase):
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
