import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

import dashboard_server
from heavy_task_coordinator import HeavyTaskCoordinator
from pipeline_runtime import PipelineRuntime


class DashboardTests(unittest.TestCase):
    @staticmethod
    def _catalog_match(
        match_id,
        status,
        timestamp,
        *,
        cmp_type="soccer",
    ):
        return {
            "match_id": match_id,
            "status": status,
            "sort_timestamp": timestamp,
            "start_play": "2027-01-15 08:00:00",
            "cmp_type": cmp_type,
            "team_A_name": f"A-{match_id}",
            "team_B_name": f"B-{match_id}",
            "large_unused_payload": {"must_not_reach_browser": True},
        }

    def test_dotenv_loads_values_without_overriding_shell(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "FROM_FILE=loaded\nEXISTING=file-value\nQUOTED='quoted value'\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"EXISTING": "shell-value"}, clear=True):
                dashboard_server._load_dotenv(path)
                self.assertEqual(os.environ["FROM_FILE"], "loaded")
                self.assertEqual(os.environ["EXISTING"], "shell-value")
                self.assertEqual(os.environ["QUOTED"], "quoted value")

    def test_max_concurrent_matches_environment_setting_defaults_to_eight(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                dashboard_server._positive_environment_integer(
                    "GIF_MAX_CONCURRENT_MATCHES", 8
                ),
                8,
            )
        with patch.dict(
            os.environ, {"GIF_MAX_CONCURRENT_MATCHES": "7"}, clear=True
        ):
            self.assertEqual(
                dashboard_server._positive_environment_integer(
                    "GIF_MAX_CONCURRENT_MATCHES", 8
                ),
                7,
            )
        with patch.dict(
            os.environ, {"GIF_MAX_CONCURRENT_MATCHES": "0"}, clear=True
        ):
            with self.assertRaisesRegex(RuntimeError, "正整数"):
                dashboard_server._positive_environment_integer(
                    "GIF_MAX_CONCURRENT_MATCHES", 8
                )

    def test_flatten_events_keeps_only_supported_codes(self):
        payload = {
            "events": {
                "45+2": {
                    "minute": "45",
                    "teamAEvents": [
                        {"code": "G", "person": "A", "person_id": "1", "minute_extra": "2"},
                        {"code": "AS", "person": "B", "person_id": "2"},
                    ],
                    "teamBEvents": [{"code": "RC", "person": "C", "person_id": "3"}],
                }
            }
        }
        events = dashboard_server.flatten_events(payload)
        self.assertEqual([event["code"] for event in events], ["G", "RC"])
        self.assertEqual([event["label"] for event in events], ["进球", "红牌"])
        self.assertEqual(events[0]["minute_extra"], "2")

    def test_flatten_events_keeps_durable_event_id(self):
        payload = {
            "events": {
                "18": {
                    "minute": "18",
                    "teamAEvents": [{"id": "goal-18", "code": "G"}],
                }
            }
        }
        self.assertEqual(dashboard_server.flatten_events(payload)[0]["event_id"], "goal-18")

    def test_flatten_events_includes_own_goal(self):
        payload = {
            "events": {
                "42": {
                    "minute": "42",
                    "teamAEvents": [{"code": "OG", "person": "A", "person_id": "1"}],
                    "teamBEvents": [],
                }
            }
        }
        events = dashboard_server.flatten_events(payload)
        self.assertEqual(events[0]["code"], "OG")
        self.assertEqual(events[0]["label"], "乌龙球")

    def test_flatten_events_includes_penalty_goal(self):
        payload = {
            "events": {
                "67": {
                    "minute": "67",
                    "teamAEvents": [{"code": "PG", "person": "A", "person_id": "1"}],
                    "teamBEvents": [],
                }
            }
        }

        events = dashboard_server.flatten_events(payload)

        self.assertEqual(events[0]["code"], "PG")
        self.assertEqual(events[0]["label"], "点球进球")

    def test_query_events_accepts_empty_event_array(self):
        with patch(
            "dashboard_server._json_request",
            return_value={"status": 0, "events": []},
        ):
            payload = dashboard_server.query_events("54478923")
        self.assertEqual(payload["events"], {})

    def test_query_events_accepts_all_success_status_variants(self):
        for status_payload in (
            {"events": {}},
            {"status": None, "events": {}},
            {"status": 0, "events": {}},
            {"status": "0", "events": {}},
        ):
            with self.subTest(payload=status_payload), patch(
                "dashboard_server._json_request", return_value=status_payload
            ):
                payload = dashboard_server.query_events("54478923")
                self.assertEqual(payload["events"], {})

    def test_query_events_requires_success_status_and_valid_events_shape(self):
        for status in (1, "1", 500, True, False):
            with self.subTest(status=status), patch(
                "dashboard_server._json_request",
                return_value={"status": status, "events": {}},
            ):
                with self.assertRaisesRegex(ValueError, "status"):
                    dashboard_server.query_events("54478923")

        for events in (None, True, "not-an-object", [{"code": "G"}]):
            with self.subTest(events=events), patch(
                "dashboard_server._json_request",
                return_value={"status": 0, "events": events},
            ):
                with self.assertRaisesRegex(ValueError, "events"):
                    dashboard_server.query_events("54478923")

    def test_match_catalog_keeps_only_soccer(self):
        now = 1_800_000_000.0
        rows = [
            self._catalog_match("soccer-live", "Playing", now - 30),
            self._catalog_match(
                "basketball-live",
                "Playing",
                now - 10,
                cmp_type="basketball",
            ),
        ]
        catalog = dashboard_server.MatchCatalog()

        with patch("dashboard_server._json_request", return_value={"list": rows}), patch(
            "dashboard_server.time.time", return_value=now
        ):
            payload = catalog.snapshot()

        self.assertEqual(
            [item["match_id"] for item in payload["playing"]],
            ["soccer-live"],
        )
        self.assertEqual(payload["upcoming"], [])
        self.assertNotIn("large_unused_payload", payload["playing"][0])
        self.assertEqual(payload["health"]["source_count"], 2)
        self.assertEqual(payload["health"]["soccer_count"], 1)

    def test_match_catalog_sorts_and_limits_playing_and_upcoming(self):
        now = 1_800_000_000.0
        playing = [
            self._catalog_match(f"playing-{index:02d}", "Playing", now - index)
            for index in range(25)
        ]
        upcoming = [
            self._catalog_match(
                f"fixture-{index:02d}",
                "Fixture",
                now + 15 + index,
            )
            for index in range(25)
        ]
        rows = list(reversed(playing + upcoming))
        catalog = dashboard_server.MatchCatalog()

        with patch("dashboard_server._json_request", return_value={"list": rows}), patch(
            "dashboard_server.time.time", return_value=now
        ):
            payload = catalog.snapshot()

        self.assertEqual(len(payload["playing"]), 20)
        self.assertEqual(len(payload["upcoming"]), 20)
        self.assertEqual(
            [item["sort_timestamp"] for item in payload["playing"]],
            sorted(item["sort_timestamp"] for item in playing)[:20],
        )
        self.assertEqual(
            [item["sort_timestamp"] for item in payload["upcoming"]],
            sorted(item["sort_timestamp"] for item in upcoming)[:20],
        )

    def test_match_catalog_excludes_past_and_more_than_15_minute_fixtures(self):
        now = 1_800_000_000.0
        rows = [
            self._catalog_match("fixture-past", "Fixture", now - 0.001),
            self._catalog_match("fixture-now", "Fixture", now),
            self._catalog_match("fixture-boundary", "Fixture", now + 15 * 60),
            self._catalog_match("fixture-late", "Fixture", now + 15 * 60 + 0.001),
        ]
        catalog = dashboard_server.MatchCatalog()

        with patch("dashboard_server._json_request", return_value={"list": rows}), patch(
            "dashboard_server.time.time", return_value=now
        ):
            payload = catalog.snapshot()

        self.assertEqual(
            [item["match_id"] for item in payload["upcoming"]],
            ["fixture-now", "fixture-boundary"],
        )

    def test_empty_match_catalog_is_healthy(self):
        now = 1_800_000_000.0
        catalog = dashboard_server.MatchCatalog()

        with patch("dashboard_server._json_request", return_value={"list": []}), patch(
            "dashboard_server.time.time", return_value=now
        ):
            payload = catalog.snapshot()

        self.assertEqual(payload["playing"], [])
        self.assertEqual(payload["upcoming"], [])
        self.assertEqual(payload["health"]["state"], "healthy")
        self.assertEqual(payload["health"]["status"], "ok")
        self.assertEqual(payload["health"]["total_count"], 0)
        self.assertIsNone(payload["health"]["error"])

    def test_invalid_match_catalog_list_is_reported_as_unavailable(self):
        now = 1_800_000_000.0
        catalog = dashboard_server.MatchCatalog()

        with patch(
            "dashboard_server._json_request",
            return_value={"list": {"not": "an array"}},
        ), patch("dashboard_server.time.time", return_value=now):
            payload = catalog.snapshot()

        self.assertEqual(payload["playing"], [])
        self.assertEqual(payload["upcoming"], [])
        self.assertEqual(payload["health"]["state"], "error")
        self.assertEqual(payload["health"]["status"], "unavailable")
        self.assertEqual(payload["health"]["consecutive_failures"], 1)
        self.assertIn("list", payload["health"]["error"])
        self.assertEqual(payload["health"]["next_retry_at_unix"], now + 5)

    def test_match_catalog_failure_keeps_cache_and_accumulates_backoff(self):
        catalog = dashboard_server.MatchCatalog()
        cached_rows = [self._catalog_match("cached-live", "Playing", 90.0)]

        with patch(
            "dashboard_server._json_request", return_value={"list": cached_rows}
        ), patch("dashboard_server.time.time", return_value=100.0):
            healthy = catalog.snapshot()
        self.assertEqual(healthy["playing"][0]["match_id"], "cached-live")

        catalog.next_attempt_at = 0.0
        with patch(
            "dashboard_server._json_request", side_effect=OSError("catalog offline")
        ), patch("dashboard_server.time.time", return_value=120.0):
            first_failure = catalog.snapshot()

        self.assertEqual(first_failure["playing"], healthy["playing"])
        self.assertEqual(first_failure["health"]["state"], "stale")
        self.assertEqual(first_failure["health"]["status"], "degraded")
        self.assertEqual(first_failure["health"]["consecutive_failures"], 1)
        self.assertEqual(first_failure["health"]["next_retry_at_unix"], 125.0)
        self.assertEqual(first_failure["health"]["cache_age_seconds"], 20.0)

        catalog.next_attempt_at = 0.0
        with patch(
            "dashboard_server._json_request", side_effect=OSError("still offline")
        ), patch("dashboard_server.time.time", return_value=130.0):
            second_failure = catalog.snapshot()

        self.assertEqual(second_failure["playing"], healthy["playing"])
        self.assertEqual(second_failure["health"]["consecutive_failures"], 2)
        self.assertEqual(second_failure["health"]["next_retry_at_unix"], 140.0)
        self.assertIn("still offline", second_failure["health"]["error"])

    def test_match_catalog_refresh_interval_is_measured_from_request_start(self):
        catalog = dashboard_server.MatchCatalog()
        rows = [self._catalog_match("refresh-live", "Playing", 90.0)]

        with patch(
            "dashboard_server._json_request", return_value={"list": rows}
        ) as request_json, patch(
            "dashboard_server.time.time", side_effect=[100.0, 104.0, 130.0, 134.0]
        ):
            catalog.snapshot()
            catalog.snapshot()

        self.assertEqual(request_json.call_count, 2)

    def test_matches_endpoint_does_not_create_match_session(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        catalog = dashboard_server.MatchCatalog()

        with patch.object(dashboard_server, "dashboard", manager), patch.object(
            dashboard_server, "match_catalog", catalog
        ), patch("dashboard_server._json_request", return_value={"list": []}):
            response = dashboard_server.app.test_client().get("/api/matches")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(manager.sessions, {})
        self.assertFalse(response.get_json()["locked"])
        self.assertIsNone(response.get_json()["active_match_id"])

    def test_matches_endpoint_exposes_global_heavy_task_status(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        catalog = Mock()
        catalog.snapshot.return_value = {
            "playing": [],
            "upcoming": [],
            "health": {"state": "healthy"},
        }
        with tempfile.TemporaryDirectory() as directory:
            coordinator = HeavyTaskCoordinator(
                Path(directory) / "coordinator.sqlite3",
                max_heavy_tasks=2,
                max_vision_tasks=1,
            )
            lease = coordinator.acquire(
                "vision",
                match_id="heavy-status-match",
                event_key="heavy-status-match:G:1",
            )
            try:
                with patch.object(dashboard_server, "dashboard", manager), patch.object(
                    dashboard_server, "match_catalog", catalog
                ), patch.object(
                    dashboard_server, "heavy_task_monitor", coordinator
                ):
                    response = dashboard_server.app.test_client().get("/api/matches")

                self.assertEqual(response.status_code, 200)
                status = response.get_json()["heavy_tasks"]
                self.assertEqual(status["total_slots"], 2)
                self.assertEqual(status["vision_slots"], 1)
                self.assertEqual(status["occupied"], 1)
                self.assertEqual(status["vision_active"], 1)
                self.assertEqual(status["queued"], 0)
                self.assertEqual(
                    status["active_items"][0]["match_id"],
                    "heavy-status-match",
                )
                self.assertNotIn("owner_pid", status["active_items"][0])
                self.assertNotIn("database_path", status)
            finally:
                lease.release()
                coordinator.close()

    def test_event_api_failure_keeps_last_valid_payload(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("54478923")
        previous = {"status": 0, "events": {"18": {"minute": "18"}}}
        session.event_payload = previous
        session.last_detail_poll = 1e20
        session.last_source_poll = 1e20
        with patch("dashboard_server.query_events", side_effect=ValueError("接口异常")):
            manager.refresh(session)
        self.assertIs(session.event_payload, previous)
        self.assertEqual(session.event_error, "接口异常")

    def test_match_id_validation_blocks_paths(self):
        self.assertEqual(dashboard_server.validate_match_id("54154533"), "54154533")
        with self.assertRaises(ValueError):
            dashboard_server.validate_match_id("../../secret")

    def test_demo_session_is_offline_and_has_a_local_source(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        with patch("dashboard_server.query_detail") as detail:
            session = manager.get("demo-test")
            manager.refresh(session)
        detail.assert_not_called()
        self.assertEqual(session.status(), "Playing")
        self.assertTrue(session.source["resource"].endswith(".mp4"))

    def test_jsonl_is_folded_into_latest_task_status(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            rows = [
                {
                    "event": "event_discovered",
                    "event_key": "m:G:1",
                    "code": "G",
                    "event_type": "goal",
                    "minute": "18",
                    "minute_extra": "2",
                    "person": "A",
                    "person_id": "7",
                    "team": "teamA",
                    "score": "1-0",
                    "reason": "点球",
                },
                {"event": "task_transition", "event_key": "m:G:1", "to_status": "encoding"},
                {"event": "gif_ready", "event_key": "m:G:1", "output": "/tmp/a.gif", "bytes": 123},
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
            tasks = dashboard_server._tasks_from_log(path)
        self.assertEqual(tasks[0]["status"], "encoded")
        self.assertEqual(tasks[0]["bytes"], 123)
        self.assertEqual(tasks[0]["team"], "teamA")
        self.assertEqual(tasks[0]["score"], "1-0")
        self.assertEqual(tasks[0]["reason"], "点球")

    def test_database_task_restores_complete_event_details(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.execute(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                )
                """
            )
            event = {
                "event_key": "m:G:1",
                "code": "G",
                "event_type": "goal",
                "minute": "90",
                "minute_extra": "1",
                "team": "teamA",
                "person": "Melvin Urbina",
                "person_id": "51003685",
                "score": "4-1",
                "reason": "",
            }
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "m:G:1",
                    "G",
                    "goal",
                    json.dumps(event),
                    "encoded",
                    1,
                    2,
                    "/tmp/goal.gif",
                    100,
                    "{}",
                    None,
                ),
            )
            runtime.commit()
            runtime.close()
            tasks, aliases, suppressed_keys = dashboard_server._tasks_from_database(database)
        self.assertEqual(aliases, {})
        self.assertEqual(suppressed_keys, set())
        self.assertEqual(tasks[0]["team"], "teamA")
        self.assertEqual(tasks[0]["score"], "4-1")
        self.assertEqual(tasks[0]["minute_extra"], "1")
        self.assertEqual(tasks[0]["output"], "/tmp/goal.gif")

    def test_database_task_includes_independent_vision_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            runtime = dashboard_server.sqlite3.connect(output / "pipeline_state.sqlite3")
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:1", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:1", "G", "goal", json.dumps(event), "encoded", 1, 2,
                 "/tmp/default.gif", 10,
                 json.dumps({"coverage_status": "ready_degraded"}), None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:1", "encoded", 26.4, 0.91, 14.2, "T-DEED",
                 "SoccerNet_small", "/tmp/refined.gif", 8,
                 json.dumps({
                     "anchor_delta_seconds": 2.4,
                     "coverage_status": "ready_degraded",
                 }), None, 1),
            )
            runtime.commit(); runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(output / "pipeline_state.sqlite3")
        self.assertEqual(tasks[0]["output"], "/tmp/default.gif")
        self.assertEqual(tasks[0]["vision"]["output"], "/tmp/refined.gif")
        self.assertEqual(tasks[0]["vision"]["confidence"], 0.91)
        self.assertEqual(tasks[0]["vision"]["locator_method"], "tdeed")
        self.assertEqual(tasks[0]["coverage_status"], "ready_degraded")
        self.assertEqual(tasks[0]["vision"]["coverage_status"], "ready_degraded")

    def test_database_exposes_ocr_and_tdeed_artifacts_without_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "pipeline_state.sqlite3", root / "events.jsonl")
            event_key = "m:G:three-path"
            runtime.discover_task(
                match_id="m",
                event_data={
                    "event_key": event_key,
                    "code": "G",
                    "event_type": "goal",
                    "minute": "8",
                    "minute_extra": "0",
                    "team": "A",
                    "person": "Player",
                    "person_id": "50000001",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=100.0,
                observed_source_time=None,
                clip_anchor_stream_time=90.0,
                clip_anchor_source_time=None,
                output_due_stream_time=110.0,
                detected_at_unix=time.time(),
            )
            for artifact_kind, output in (
                ("ocr_window", "/tmp/ocr.gif"),
                ("tdeed_refined", "/tmp/tdeed.gif"),
            ):
                runtime.enqueue_vision_task(
                    event_key,
                    artifact_kind=artifact_kind,
                    search_start_stream_time=0.0,
                    search_end_stream_time=100.0,
                    clip_before_seconds=30.0,
                    clip_after_seconds=30.0,
                )
                runtime.transition_vision_task(
                    event_key, "locating", artifact_kind=artifact_kind
                )
                runtime.transition_vision_task(
                    event_key,
                    "located",
                    artifact_kind=artifact_kind,
                    result={"anchor_stream_time": 80.0},
                )
                runtime.transition_vision_task(
                    event_key, "encoding", artifact_kind=artifact_kind
                )
                runtime.transition_vision_task(
                    event_key,
                    "encoded",
                    artifact_kind=artifact_kind,
                    result={
                        "output": output,
                        "bytes": 10,
                        **(
                            {
                                "localization_quality": "degraded",
                                "degraded": True,
                                "degradation_mode": "minute_boundary_fallback",
                                "degradation_reason": {
                                    "kind": "ocr_exact_second_not_found",
                                    "message": "target second was not found",
                                },
                            }
                            if artifact_kind == "ocr_window"
                            else {}
                        ),
                    },
                )
            runtime.close()

            tasks, _, _ = dashboard_server._tasks_from_database(
                root / "pipeline_state.sqlite3"
            )

        self.assertEqual(tasks[0]["ocr_window"]["output"], "/tmp/ocr.gif")
        self.assertEqual(
            tasks[0]["ocr_window"]["localization_quality"],
            "degraded",
        )
        self.assertTrue(tasks[0]["ocr_window"]["degraded"])
        self.assertEqual(
            tasks[0]["ocr_window"]["degradation_mode"],
            "minute_boundary_fallback",
        )
        self.assertEqual(
            tasks[0]["ocr_window"]["degradation_reason"]["kind"],
            "ocr_exact_second_not_found",
        )
        self.assertEqual(tasks[0]["vision"]["output"], "/tmp/tdeed.gif")
        self.assertEqual(
            set(tasks[0]["vision_artifacts"]),
            {"ocr_window", "tdeed_refined"},
        )

    def test_legacy_minute_fallback_uses_duration_for_fragment_label(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            runtime = dashboard_server.sqlite3.connect(output / "pipeline_state.sqlite3")
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:legacy", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:legacy", "G", "goal", json.dumps(event), "failed", 1, 2,
                 None, None, json.dumps({"error_kind": "anchor_gap"}), "anchor gap"),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:legacy", "encoded", 10.0, None, 1.0, "PaddleOCR", "1",
                 "/tmp/legacy-fallback.gif", 7,
                 json.dumps({
                     "minute_fallback": True,
                     "fallback_generated": True,
                     "duration_sec": 7.0,
                 }), None, 1),
            )
            runtime.commit()
            runtime.close()

            tasks, _, _ = dashboard_server._tasks_from_database(
                output / "pipeline_state.sqlite3"
            )

        vision = tasks[0]["vision"]
        self.assertFalse(vision["fallback_complete"])
        self.assertEqual(vision["available_fallback_seconds"], 7.0)
        self.assertEqual(vision["requested_fallback_seconds"], 60.0)

    def test_database_vision_failure_exposes_structured_ocr_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, last_error_kind TEXT,
                    created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:ocr", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:ocr", "G", "goal", json.dumps(event), "encoded", 1, 2,
                 "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:ocr", "failed", None, None, 4.2, "scoreboard-clock-ocr",
                 "1", None, None, json.dumps({
                     "stage": "ocr",
                     "locator_method": "ocr",
                     "fallback_used": False,
                     "ocr": {
                         "target_clock": "69:37",
                         "exact_second_error": {
                             "kind": "ocr_exact_second_not_found",
                             "diagnostics": {
                                 "exact_second_failure_reason": (
                                     "target_clock_not_found"
                                 )
                             },
                         },
                     },
                     "ocr_diagnostics": {
                         "sampled_frames": 120,
                         "valid_clock_frames": 0,
                         "candidate_count": 0,
                     },
                 }), "no valid clock", "ocr_no_clock", 1),
            )
            runtime.commit(); runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)

        vision = tasks[0]["vision"]
        self.assertEqual(vision["error_kind"], "ocr_no_clock")
        self.assertEqual(vision["last_error_kind"], "ocr_no_clock")
        self.assertEqual(vision["locator_method"], "ocr")
        self.assertEqual(vision["stage"], "ocr")
        self.assertFalse(vision["fallback_used"])
        self.assertEqual(vision["ocr_diagnostics"]["sampled_frames"], 120)
        self.assertEqual(vision["target_clock"], "69:37")
        self.assertEqual(
            vision["ocr_diagnostics"]["exact_second_failure_reason"],
            "target_clock_not_found",
        )
        self.assertEqual(vision["error"], "no valid clock")

    def test_database_exposes_incremental_ocr_dashboard_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, last_error_kind TEXT,
                    failure_stage TEXT, failure_reason TEXT,
                    location_json TEXT, window_json TEXT,
                    next_attempt_at_unix REAL, deadline_at_unix REAL,
                    artifact_kind TEXT, created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:incremental-ocr", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:incremental-ocr", "G", "goal", json.dumps(event),
                 "encoded", 1, 2, "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:incremental-ocr", "pending", None, None, None,
                 "PaddleOCR", "1", None, None,
                 json.dumps({
                     "stage": "waiting_for_clock_target",
                 }), "waiting", "waiting_for_clock_target", None, None,
                 json.dumps({}),
                 json.dumps({
                     "progressive_scan": {
                         "state": "waiting_for_clock_target",
                         "scan_cursor_stream_time": 88.5,
                        "latest_trusted_clock": "07:35",
                        "latest_trusted_clock_seconds": 455,
                        "target_failure_cause": "target_passed",
                        "target_failure_explanation": "OCR 已经越过目标时间，但没有找到可验证的直接读数或连续插值锚点。",
                        "target_passed_without_anchor": True,
                        "target_failure_scan_stage": "ocr_progressive_scan",
                        "scan_attempt_count": 3,
                         "last_scan_start_stream_time": 20.0,
                         "last_scan_end_stream_time": 100.0,
                     },
                 }), 30.0, 120.0, "ocr_window", 1),
            )
            runtime.commit()
            runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)

        self.assertEqual(tasks[0]["status"], "encoded")
        ocr = tasks[0]["ocr_window"]
        self.assertEqual(ocr["status"], "pending")
        self.assertEqual(ocr["ocr_pipeline_status"], "waiting_for_clock_target")
        self.assertEqual(ocr["scan_cursor"], 88.5)
        self.assertEqual(ocr["last_trusted_clock"], "07:35")
        self.assertEqual(ocr["last_trusted_clock_seconds"], 455)
        self.assertEqual(ocr["target_failure_cause"], "target_passed")
        self.assertTrue(ocr["target_passed_without_anchor"])
        self.assertEqual(ocr["target_failure_scan_stage"], "ocr_progressive_scan")
        self.assertEqual(ocr["scan_attempt_count"], 3)
        self.assertEqual(ocr["next_attempt_at_unix"], 30.0)
        self.assertEqual(ocr["deadline_at_unix"], 120.0)
        self.assertEqual(ocr["scan_window"]["start_stream_time"], 20.0)

    def test_database_marks_pending_target_rescan_as_recoverable(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, last_error_kind TEXT,
                    failure_stage TEXT, failure_reason TEXT,
                    location_json TEXT, window_json TEXT,
                    next_attempt_at_unix REAL, deadline_at_unix REAL,
                    artifact_kind TEXT, created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:rescan", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:rescan", "G", "goal", json.dumps(event),
                 "encoded", 1, 2, "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:rescan", "pending", None, None, None, "PaddleOCR", "1",
                 None, None, json.dumps({"stage": "waiting_for_clock_target"}),
                 "waiting", "waiting_for_clock_target", None, None, json.dumps({}),
                 json.dumps({"progressive_scan": {
                     "state": "waiting_for_clock_target",
                     "target_rescan_window": {"start_stream_time": 90, "end_stream_time": 120},
                     "target_passed_without_anchor": True,
                 }}), 30.0, 120.0, "ocr_window", 1),
            )
            runtime.commit(); runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)
        ocr = tasks[0]["ocr_window"]
        self.assertEqual(ocr["ocr_pipeline_status"], "ocr_target_rescan")
        self.assertTrue(ocr["target_rescan_pending"])
        self.assertEqual(ocr["target_rescan_window"]["start_stream_time"], 90)

    def test_database_downgrades_stale_failure_for_pending_ocr_row(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, last_error_kind TEXT,
                    failure_stage TEXT, failure_reason TEXT,
                    location_json TEXT, window_json TEXT,
                    next_attempt_at_unix REAL, deadline_at_unix REAL,
                    artifact_kind TEXT, created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:stale", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:stale", "G", "goal", json.dumps(event), "encoded", 1, 2,
                 "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:stale", "pending", None, None, None, "PaddleOCR", "1",
                 None, None, json.dumps({"error_kind": "ocr_clock_target_not_located"}),
                 "stale", "ocr_clock_target_not_located", None, None, json.dumps({}),
                 json.dumps({"progressive_scan": {"state": "waiting_for_clock_target"}}),
                 30.0, 120.0, "ocr_window", 1),
            )
            runtime.commit(); runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)
        self.assertEqual(
            tasks[0]["ocr_window"]["ocr_pipeline_status"],
            "waiting_for_clock_target",
        )

    def test_database_normalizes_progressive_ocr_failure_kind(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, last_error_kind TEXT,
                    failure_stage TEXT, failure_reason TEXT,
                    artifact_kind TEXT, created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:ocr-timeout", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:ocr-timeout", "G", "goal", json.dumps(event),
                 "encoded", 1, 2, "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:ocr-timeout", "failed", None, None, None,
                 "PaddleOCR", "1", None, None,
                 json.dumps({
                     "error_kind": "ocr_clock_target_timeout",
                     "failure_reason": {
                         "kind": "ocr_clock_target_timeout",
                         "stage": "ocr_progressive_scan",
                         "message": "target not reached",
                     },
                 }), "target not reached", "ocr_clock_target_timeout",
                 "ocr_progressive_scan", "target not reached",
                 "ocr_window", 1),
            )
            runtime.commit()
            runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)

        self.assertEqual(
            tasks[0]["ocr_window"]["ocr_pipeline_status"],
            "ocr_target_timeout",
        )

    def test_database_normalizes_ocr_window_failure_families(self):
        mappings = {
            "ocr_output_video_gap": "ocr_window_evicted",
            "ocr_inference_failed": "ocr_dependency_unavailable",
            "ocr_processing_failed": "ocr_dependency_unavailable",
            "ocr_window_encoding_failed": "ocr_encode_failed",
        }
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, last_error_kind TEXT,
                    failure_stage TEXT, failure_reason TEXT,
                    artifact_kind TEXT, created_at_unix REAL
                );
                """
            )
            for index, error_kind in enumerate(mappings):
                event_key = f"m:G:ocr-family-{index}"
                event = {"event_key": event_key, "code": "G", "event_type": "goal"}
                runtime.execute(
                    "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (event_key, "G", "goal", json.dumps(event),
                     "encoded", index + 1, index + 2, "/tmp/default.gif", 10, "{}", None),
                )
                runtime.execute(
                    "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (event_key, "failed", None, None, None, "PaddleOCR", "1",
                     None, None, json.dumps({"error_kind": error_kind}),
                     error_kind, error_kind, "ocr_progressive_scan", error_kind,
                     "ocr_window", index + 1),
                )
            runtime.commit()
            runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)

        actual = {
            task["event_key"]: task["ocr_window"]["ocr_pipeline_status"]
            for task in tasks
        }
        for index, (error_kind, expected) in enumerate(mappings.items()):
            self.assertEqual(actual[f"m:G:ocr-family-{index}"], expected)

    def test_database_maps_ocr_resolution_and_final_clip_window(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, artifact_kind TEXT,
                    created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:exact-ocr", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:exact-ocr", "G", "goal", json.dumps(event),
                 "encoded", 1, 2, "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:exact-ocr", "encoded", 80.0, None, 1.0,
                 "PaddleOCR", "1", "/tmp/ocr.gif", 8,
                 json.dumps({
                     "stage": "encoded",
                     "visual_resolution": "ocr_second_exact",
                     "requested_media_window": {
                         "start_stream_time": 50.0,
                         "end_stream_time": 110.0,
                     },
                 }), None, "ocr_window", 1),
            )
            runtime.commit()
            runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)

        ocr = tasks[0]["ocr_window"]
        self.assertEqual(ocr["ocr_pipeline_status"], "ocr_second_exact")
        self.assertEqual(ocr["final_clip_window"]["start_stream_time"], 50.0)

    def test_database_marks_near_neighbor_ocr_as_estimated(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "pipeline_state.sqlite3"
            runtime = dashboard_server.sqlite3.connect(database)
            runtime.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT, code TEXT, event_type TEXT, event_json TEXT,
                    status TEXT, discovered_at_unix REAL, updated_at_unix REAL,
                    output_path TEXT, output_bytes INTEGER, result_json TEXT,
                    error TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, status TEXT, located_anchor_stream_time REAL,
                    confidence REAL, inference_seconds REAL, model_name TEXT,
                    model_version TEXT, output_path TEXT, output_bytes INTEGER,
                    result_json TEXT, error TEXT, artifact_kind TEXT,
                    created_at_unix REAL
                );
                """
            )
            event = {"event_key": "m:G:estimated-ocr", "code": "G", "event_type": "goal"}
            runtime.execute(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:estimated-ocr", "G", "goal", json.dumps(event),
                 "encoded", 1, 2, "/tmp/default.gif", 10, "{}", None),
            )
            runtime.execute(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ("m:G:estimated-ocr", "encoded", 80.0, None, 1.0,
                 "PaddleOCR", "1", "/tmp/ocr.gif", 8,
                 json.dumps({
                     "stage": "encoded",
                     "localization_source": "exact_second",
                     "localization_quality": "estimated",
                     "degraded": True,
                     "estimated_error_bound_seconds": 2,
                 }), None, "ocr_window", 1),
            )
            runtime.commit()
            runtime.close()
            tasks, _, _ = dashboard_server._tasks_from_database(database)

        ocr = tasks[0]["ocr_window"]
        self.assertEqual(ocr["ocr_pipeline_status"], "ocr_second_estimated")
        self.assertFalse(ocr["precise_location"])

    def test_new_session_defaults_to_calibrated_candidate_window(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("default-gif-only")

        self.assertEqual(session.before_seconds, 30.0)
        self.assertEqual(session.after_seconds, 20.0)
        self.assertEqual(session.event_to_video_offset_seconds, -10.0)
        self.assertEqual(session.gif_width, 768)
        self.assertEqual(session.gif_fps, 16.0)
        self.assertEqual(session.gif_colors, 256)
        self.assertTrue(session.vision_enabled)
        self.assertFalse(session.tdeed_enabled)
        payload = dashboard_server._session_json(session)
        self.assertEqual(payload["gif"]["before_seconds"], 30.0)
        self.assertEqual(payload["gif"]["after_seconds"], 20.0)
        self.assertEqual(payload["gif"]["event_to_video_offset_seconds"], -10.0)
        self.assertEqual(payload["gif"]["width"], 768)
        self.assertEqual(payload["gif"]["fps"], 16.0)
        self.assertEqual(payload["gif"]["colors"], 256)
        self.assertTrue(payload["vision"]["enabled"])
        self.assertFalse(payload["vision"]["worker_enabled"])
        self.assertFalse(payload["vision"]["tdeed_enabled"])
        self.assertFalse(payload["vision"]["worker_tdeed_enabled"])
        self.assertEqual(payload["vision"]["search_before_seconds"], 120.0)
        self.assertEqual(payload["vision"]["search_after_seconds"], 0.0)
        self.assertEqual(payload["vision"]["fallback_gif"]["duration_seconds"], 60.0)
        self.assertEqual(
            payload["vision"]["fallback_gif"]["exact_second_before_seconds"],
            30.0,
        )
        self.assertEqual(
            payload["vision"]["fallback_gif"]["minute_boundary_before_seconds"],
            60.0,
        )
        self.assertEqual(
            payload["vision"]["fallback_gif"]["minute_boundary_after_seconds"],
            0.0,
        )
        self.assertEqual(payload["vision"]["fallback_gif"]["width"], 384)
        self.assertEqual(payload["vision"]["fallback_gif"]["fps"], 6.0)

    def test_session_configuration_accepts_and_preserves_negative_event_offset(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        with patch.object(dashboard_server, "dashboard", manager):
            response = dashboard_server.app.test_client().post(
                "/api/session",
                json={
                    "match_id": "custom-event-offset",
                    "before_seconds": 30.5,
                    "after_seconds": 12.5,
                    "event_to_video_offset_seconds": -22.5,
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["gif"]["before_seconds"], 30.5)
        self.assertEqual(payload["gif"]["after_seconds"], 12.5)
        self.assertEqual(payload["gif"]["event_to_video_offset_seconds"], -22.5)
        session = manager.get("custom-event-offset")
        self.assertEqual(session.event_to_video_offset_seconds, -22.5)

    def test_session_configuration_rejects_non_finite_event_offset(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        with patch.object(dashboard_server, "dashboard", manager):
            response = dashboard_server.app.test_client().post(
                "/api/session",
                json={
                    "match_id": "invalid-event-offset",
                    "event_to_video_offset_seconds": "nan",
                },
            )

        self.assertEqual(response.status_code, 400)
        self.assertIn("必须是有限数字", response.get_json()["error"])

    def test_concurrent_matches_are_isolated_and_manual_stop_releases_one_slot(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            max_concurrent_matches=2,
        )
        first = manager.get("worker-slot-first")
        second = manager.get("worker-slot-second")
        third = manager.get("worker-slot-third")
        first.source = {"resource": "rtmp://example/first"}
        second.source = {"resource": "rtmp://example/second"}
        third.source = {"resource": "rtmp://example/third"}
        first_worker = Mock(pid=701, returncode=None)
        first_worker.poll.return_value = None
        second_worker = Mock(pid=702, returncode=None)
        second_worker.poll.return_value = None
        third_worker = Mock(pid=703, returncode=None)
        third_worker.poll.return_value = None

        with tempfile.TemporaryDirectory() as directory:
            first.output_dir = Path(directory) / "first"
            second.output_dir = Path(directory) / "second"
            third.output_dir = Path(directory) / "third"
            with patch(
                "dashboard_server.subprocess.Popen",
                side_effect=[first_worker, second_worker, third_worker],
            ) as popen:
                manager.start(first)
                manager.start(second)
                active_status = manager.worker_slot_status()
                self.assertEqual(
                    set(active_status["active_match_ids"]),
                    {first.match_id, second.match_id},
                )
                self.assertEqual(active_status["active_match_count"], 2)
                self.assertEqual(active_status["available_worker_slots"], 0)
                self.assertTrue(active_status["locked"])
                self.assertEqual(popen.call_count, 2)

                first_worker.poll.return_value = 0
                first_worker.returncode = 0
                with patch.object(
                    manager, "_cleanup_worker_group_blocking", return_value=True
                ):
                    manager.stop(first)

                released_status = manager.worker_slot_status()
                self.assertEqual(
                    released_status["active_match_ids"], [second.match_id]
                )
                self.assertEqual(released_status["available_worker_slots"], 1)
                self.assertFalse(released_status["locked"])
                self.assertTrue(second.worker_running())
                manager.start(third)

        self.assertEqual(popen.call_count, 3)
        self.assertEqual(
            set(manager.worker_slot_status()["active_match_ids"]),
            {second.match_id, third.match_id},
        )

    def test_terminal_worker_state_releases_a_worker_slot(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            max_concurrent_matches=1,
        )
        finished = manager.get("demo-worker-slot-finished")
        replacement = manager.get("demo-worker-slot-replacement")
        finished_worker = Mock(pid=711, returncode=None)
        finished_worker.poll.return_value = None
        replacement_worker = Mock(pid=712, returncode=None)
        replacement_worker.poll.return_value = None

        with tempfile.TemporaryDirectory() as directory:
            finished.output_dir = Path(directory) / "finished"
            replacement.output_dir = Path(directory) / "replacement"
            with patch(
                "dashboard_server.subprocess.Popen",
                side_effect=[finished_worker, replacement_worker],
            ):
                manager.start(finished, demo=True)
                self.assertEqual(
                    manager.worker_slot_status()["active_match_ids"],
                    [finished.match_id],
                )

                finished_worker.poll.return_value = 0
                finished_worker.returncode = 0
                manager.refresh(finished)

                self.assertEqual(finished.lifecycle_state, "completed")
                self.assertFalse(manager.worker_slot_status()["locked"])
                manager.start(replacement, demo=True)

        self.assertEqual(
            manager.worker_slot_status()["active_match_ids"],
            [replacement.match_id],
        )

    def test_capacity_counts_starting_recovery_finishing_stopping_and_cleanup(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            max_concurrent_matches=5,
        )
        starting = manager.get("capacity-starting")
        starting.lifecycle_state = "starting"

        recovering = manager.get("capacity-recovering")
        recovering.desired_running = True
        recovering.worker_mode = "live"
        recovering.worker_restart_due_at = 200.0

        finishing = manager.get("capacity-finishing")
        finishing.lifecycle_state = "finishing"

        stopping = manager.get("capacity-stopping")
        stopping.lifecycle_state = "stopping"

        cleaning = manager.get("capacity-cleaning")
        cleaning.worker_cleanup_process_group = 501

        status = manager.worker_slot_status()
        self.assertEqual(status["active_match_count"], 5)
        self.assertEqual(status["available_worker_slots"], 0)
        self.assertTrue(status["at_capacity"])
        self.assertEqual(
            {item["lifecycle_state"] for item in status["active_matches"]},
            {"idle", "starting", "finishing", "stopping"},
        )
        self.assertEqual(
            {item["state"] for item in status["active_matches"]},
            {"starting", "recovering", "finishing", "stopping", "cleaning"},
        )

        sixth = manager.get("capacity-sixth")
        sixth.source = {"resource": "rtmp://example/sixth"}
        with patch("dashboard_server.subprocess.Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "上限 5 场"):
                manager.start(sixth)
        popen.assert_not_called()

    def test_recovery_reuses_its_reserved_slot_at_capacity(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            max_concurrent_matches=1,
        )
        session = manager.get("capacity-recovery")
        session.source = {"resource": "rtmp://example/recovery"}
        session.desired_running = True
        session.worker_mode = "live"
        session.lifecycle_state = "playing"
        session.worker_restart_due_at = 100.0
        worker = Mock(pid=751, returncode=None)
        worker.poll.return_value = None

        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            with patch(
                "dashboard_server.subprocess.Popen", return_value=worker
            ) as popen:
                manager.start(session, recovery=True)

        popen.assert_called_once()
        status = manager.worker_slot_status()
        self.assertEqual(status["active_match_ids"], [session.match_id])
        self.assertEqual(status["active_matches"][0]["state"], "running")
        self.assertTrue(status["at_capacity"])

    def test_sixth_api_start_returns_409_and_catalog_exposes_active_summaries(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            max_concurrent_matches=5,
        )
        catalog = Mock()
        catalog.snapshot.return_value = {
            "playing": [],
            "upcoming": [],
            "health": {"state": "healthy"},
        }
        workers = []
        with tempfile.TemporaryDirectory() as directory:
            for index in range(1, 7):
                session = manager.get(f"parallel-{index}")
                session.source = {"resource": f"rtmp://example/{index}"}
                session.output_dir = Path(directory) / str(index)
                worker = Mock(pid=800 + index, returncode=None)
                worker.poll.return_value = None
                workers.append(worker)

            with patch.object(dashboard_server, "dashboard", manager), patch.object(
                dashboard_server, "match_catalog", catalog
            ), patch(
                "dashboard_server.subprocess.Popen", side_effect=workers[:5]
            ) as popen:
                client = dashboard_server.app.test_client()
                for index in range(1, 6):
                    response = client.post(
                        "/api/session/start",
                        json={"match_id": f"parallel-{index}"},
                    )
                    self.assertEqual(response.status_code, 200)

                rejected = client.post(
                    "/api/session/start", json={"match_id": "parallel-6"}
                )
                catalog_response = client.get("/api/matches")

        self.assertEqual(rejected.status_code, 409)
        self.assertIn("上限 5 场", rejected.get_json()["error"])
        self.assertEqual(popen.call_count, 5)
        capacity = catalog_response.get_json()
        self.assertEqual(capacity["active_match_count"], 5)
        self.assertEqual(capacity["max_concurrent_matches"], 5)
        self.assertEqual(capacity["available_worker_slots"], 0)
        self.assertTrue(capacity["at_capacity"])
        self.assertTrue(capacity["selection_locked"])
        self.assertIsNone(capacity["active_match_id"])
        self.assertEqual(
            set(capacity["active_match_ids"]),
            {f"parallel-{index}" for index in range(1, 6)},
        )
        self.assertEqual(len(capacity["active_matches"]), 5)

    def test_duplicate_start_for_same_match_returns_409_without_second_process(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("duplicate-start")
        session.source = {"resource": "rtmp://example/duplicate"}
        worker = Mock(pid=901, returncode=None)
        worker.poll.return_value = None

        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            with patch.object(dashboard_server, "dashboard", manager), patch(
                "dashboard_server.subprocess.Popen", return_value=worker
            ) as popen:
                client = dashboard_server.app.test_client()
                first = client.post(
                    "/api/session/start", json={"match_id": session.match_id}
                )
                duplicate = client.post(
                    "/api/session/start", json={"match_id": session.match_id}
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(duplicate.status_code, 409)
        self.assertIn("已在运行", duplicate.get_json()["error"])
        popen.assert_called_once()

    def test_dashboard_form_matches_backend_defaults(self):
        html = (dashboard_server.ROOT / "dashboard_static" / "index.html").read_text(
            encoding="utf-8"
        )

        self.assertIn('id="before" type="number" value="30"', html)
        self.assertIn('id="after" type="number" value="20"', html)
        self.assertIn('id="event-offset" type="number" value="-10"', html)
        self.assertIn('id="width" type="number" value="768"', html)
        self.assertIn('id="vision-enabled" type="checkbox" checked', html)
        self.assertIn('id="vision-state">默认开启', html)

    def test_direct_start_uses_backend_defaults_and_enables_vision(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        worker = Mock(pid=122, returncode=None)
        worker.poll.return_value = None

        def provide_source(session):
            session.source = {"resource": "rtmp://example/live"}

        with tempfile.TemporaryDirectory() as directory, patch.object(
            dashboard_server, "DEFAULT_OUTPUT", Path(directory)
        ), patch.object(
            dashboard_server, "dashboard", manager
        ), patch.object(
            manager, "refresh", side_effect=provide_source
        ), patch(
            "dashboard_server.subprocess.Popen", return_value=worker
        ) as popen:
            response = dashboard_server.app.test_client().post(
                "/api/session/start", json={"match_id": "direct-default-start"}
            )

        self.assertEqual(response.status_code, 200)
        command = popen.call_args.args[0]
        self.assertEqual(command[command.index("--before") + 1], "30.0")
        self.assertEqual(command[command.index("--after") + 1], "20.0")
        self.assertEqual(
            command[command.index("--event-to-video-offset") + 1],
            "-10.0",
        )
        self.assertEqual(
            command[command.index("--buffer-seconds") + 1],
            "360.0",
        )
        self.assertEqual(command[command.index("--gif-width") + 1], "768")
        self.assertEqual(command[command.index("--gif-fps") + 1], "16.0")
        self.assertEqual(command[command.index("--gif-colors") + 1], "256")
        self.assertEqual(
            float(command[command.index("--graceful-stop-grace-seconds") + 1]),
            dashboard_server.WORKER_FINISH_GRACE_SECONDS,
        )
        self.assertIn("--vision-enabled", command)
        self.assertNotIn("--tdeed-enabled", command)
        self.assertEqual(
            command[command.index("--vision-search-before") + 1],
            "120.0",
        )
        self.assertEqual(
            command[command.index("--vision-search-after") + 1],
            "0.0",
        )
        payload = response.get_json()
        self.assertTrue(payload["vision"]["enabled"])
        self.assertTrue(payload["vision"]["worker_enabled"])
        self.assertFalse(payload["vision"]["tdeed_enabled"])
        self.assertFalse(payload["vision"]["worker_tdeed_enabled"])

    def test_live_start_passes_match_start_play_to_worker(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("timeline-command")
        session.source = {"resource": "rtmp://example/live"}
        session.detail = {"start_play": "2026-05-20 11:00:00"}
        worker = Mock(pid=126, returncode=None)
        worker.poll.return_value = None

        with patch("dashboard_server.subprocess.Popen", return_value=worker) as popen:
            manager.start(session)

        command = popen.call_args.args[0]
        self.assertEqual(
            command[command.index("--match-start-play") + 1],
            "2026-05-20 11:00:00",
        )
        self.assertEqual(
            command[command.index("--match-start-naive-timezone") + 1],
            "utc",
        )
        self.assertIsNone(
            dashboard_server._usable_match_start_play({"start_play": "演示数据"})
        )

    def test_session_json_keeps_worker_vision_state_separate_from_configuration(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("vision-runtime-state")
        session.source = {"resource": "rtmp://example/live"}
        session.vision_enabled = True
        session.vision_clock_only = True
        worker = Mock(pid=125, returncode=None)
        worker.poll.return_value = None

        with patch("dashboard_server.subprocess.Popen", return_value=worker):
            manager.start(session)
        session.vision_enabled = False

        payload = dashboard_server._session_json(session)
        self.assertFalse(payload["vision"]["enabled"])
        self.assertTrue(payload["vision"]["worker_enabled"])
        self.assertTrue(payload["vision"]["clock_only"])
        self.assertTrue(payload["vision"]["worker_clock_only"])

    def test_dashboard_passes_vision_configuration_to_worker(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("vision-command")
        session.source = {"resource": "rtmp://example/live"}
        session.vision_enabled = True
        session.vision_clock_only = True
        session.vision_before_seconds = 7
        session.vision_after_seconds = 11
        worker = Mock(pid=123, returncode=None)
        worker.poll.return_value = None
        with patch("dashboard_server.subprocess.Popen", return_value=worker) as popen:
            manager.start(session)
        command = popen.call_args.args[0]
        self.assertIn("--vision-enabled", command)
        self.assertIn("--ocr-clock-only", command)
        self.assertNotIn("--tdeed-enabled", command)
        self.assertEqual(command[command.index("--vision-before") + 1], "7")
        self.assertEqual(command[command.index("--vision-after") + 1], "11")
        self.assertEqual(
            command[command.index("--vision-search-before") + 1],
            "120.0",
        )
        self.assertEqual(
            command[command.index("--vision-search-after") + 1],
            "0.0",
        )
        self.assertEqual(
            command[command.index("--buffer-seconds") + 1],
            "360.0",
        )
        self.assertEqual(command[command.index("--gif-width") + 1], "768")
        self.assertEqual(command[command.index("--gif-fps") + 1], "16.0")
        self.assertEqual(command[command.index("--gif-colors") + 1], "256")
        self.assertEqual(command[command.index("--fallback-gif-width") + 1], "384")
        self.assertEqual(command[command.index("--fallback-gif-fps") + 1], "6.0")

    def test_dashboard_can_disable_clock_only_without_disabling_ai(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("vision-legacy-command")
        session.source = {"resource": "rtmp://example/live"}
        session.vision_enabled = True
        session.vision_clock_only = False
        worker = Mock(pid=128, returncode=None)
        worker.poll.return_value = None

        with patch("dashboard_server.subprocess.Popen", return_value=worker) as popen:
            manager.start(session)

        command = popen.call_args.args[0]
        self.assertIn("--vision-enabled", command)
        self.assertNotIn("--ocr-clock-only", command)

    def test_dashboard_enables_clock_only_by_default(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("vision-clock-only-default")

        self.assertTrue(session.vision_enabled)
        self.assertTrue(session.vision_clock_only)

    def test_dashboard_passes_explicit_scoreboard_profile_path(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("profile-command")
        session.source = {"resource": "rtmp://example/live"}
        worker = Mock(pid=127, returncode=None)
        worker.poll.return_value = None
        with tempfile.TemporaryDirectory() as directory:
            profile_path = Path(directory) / "layout.json"
            profile_path.write_text("{}", encoding="utf-8")
            session.scoreboard_profile_path = str(profile_path)
            with patch("dashboard_server.subprocess.Popen", return_value=worker) as popen:
                manager.start(session)

        command = popen.call_args.args[0]
        self.assertEqual(
            command[command.index("--scoreboard-profile") + 1],
            str(profile_path.resolve()),
        )

    def test_demo_duration_covers_default_vision_post_search_window(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("demo-vision-duration")
        session.vision_enabled = True
        worker = Mock(pid=124, returncode=None)
        worker.poll.return_value = None

        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            with patch("dashboard_server.subprocess.Popen", return_value=worker) as popen:
                manager.start(session, demo=True)

        command = popen.call_args.args[0]
        duration = float(command[command.index("--duration") + 1])
        scenario_path = Path(command[command.index("--replay-events") + 1])
        scenario = json.loads(scenario_path.read_text(encoding="utf-8"))
        last_event_time = max(
            float(step["at_stream_sec"])
            for step in scenario["steps"]
            if step.get("payload", {}).get("events")
        )

        # event_driven_pipeline defaults: 60s post-search plus 7s to close
        # the final transport-stream segment before queuing T-DEED.
        self.assertGreaterEqual(duration, last_event_time + 60.0 + 7.0)
        self.assertIn("--vision-enabled", command)
        self.assertEqual(
            float(command[command.index("--graceful-stop-grace-seconds") + 1]),
            dashboard_server.WORKER_FINISH_GRACE_SECONDS,
        )
        self.assertEqual(
            float(command[command.index("--graceful-stop-timeout-seconds") + 1]),
            dashboard_server.WORKER_FINISH_TIMEOUT_SECONDS,
        )

    def test_goal_revisions_are_one_unique_dashboard_event(self):
        tasks = [
            {
                "event_key": "first",
                "code": "G",
                "minute": "5",
                "minute_extra": "0",
                "team": "teamA",
                "person": "",
                "person_id": "0",
                "score": "1-0",
                "status": "encoded",
                "output": "/tmp/first.gif",
                "discovered_at_unix": 1,
            },
            {
                "event_key": "second",
                "code": "G",
                "minute": "5",
                "minute_extra": "0",
                "team": "teamA",
                "person": "Miguel Murillo",
                "person_id": "50895934",
                "score": "1-0",
                "status": "encoded",
                "output": "/tmp/second.gif",
                "discovered_at_unix": 2,
            },
            {
                "event_key": "third",
                "code": "G",
                "minute": "4",
                "minute_extra": "0",
                "team": "teamA",
                "person": "Miguel Murillo",
                "person_id": "50895934",
                "score": "1-0",
                "status": "encoded",
                "output": "/tmp/third.gif",
                "discovered_at_unix": 3,
            },
        ]
        events = dashboard_server._canonicalize_event_rows(tasks, [])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["minute"], "4")
        self.assertEqual(events[0]["person"], "Miguel Murillo")
        self.assertEqual(events[0]["team"], "teamA")
        self.assertEqual(events[0]["score"], "1-0")
        self.assertEqual(events[0]["output"], "/tmp/first.gif")
        self.assertEqual(events[0]["duplicate_task_count"], 3)

    def test_adjacent_incomplete_goals_are_not_collapsed(self):
        tasks = [
            {
                "event_key": "first",
                "code": "G",
                "event_type": "goal",
                "minute": "10",
                "team": "teamA",
                "status": "encoded",
                "discovered_at_unix": 1,
            },
            {
                "event_key": "second",
                "code": "G",
                "event_type": "goal",
                "minute": "11",
                "team": "teamA",
                "status": "encoded",
                "discovered_at_unix": 2,
            },
        ]
        events = dashboard_server._canonicalize_event_rows(tasks, [])
        self.assertEqual(len(events), 2)

    def test_dashboard_merges_task_and_api_event_with_the_same_id(self):
        tasks = [
            {
                "event_key": "task",
                "code": "G",
                "event_type": "goal",
                "minute": "10",
                "team": "teamA",
                "metadata": {"id": "goal-10"},
                "status": "encoded",
                "discovered_at_unix": 1,
            }
        ]
        api_events = [
            {
                "code": "G",
                "minute": "12",
                "team": "teamA",
                "event_id": "goal-10",
                "person": "Player A",
            }
        ]
        events = dashboard_server._canonicalize_event_rows(tasks, api_events)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["person"], "Player A")

    def test_dashboard_merges_exact_task_and_api_event(self):
        task = {
            "event_key": "task",
            "code": "G",
            "event_type": "goal",
            "minute": "10",
            "team": "teamA",
            "person": "Player A",
            "person_id": "7",
            "score": "1-0",
            "status": "encoded",
            "discovered_at_unix": 1,
        }
        api_event = {
            "code": "G",
            "minute": "10",
            "team": "teamA",
            "person": "Player A",
            "person_id": "7",
            "score": "1-0",
        }
        events = dashboard_server._canonicalize_event_rows([task], [api_event])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["status"], "encoded")

    def test_session_counts_unique_events_instead_of_task_versions(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("unique-count-test")
        session.event_payload = {
            "events": {
                "12": {
                    "minute": "12",
                    "teamAEvents": [
                        {"code": "G", "person": "A", "person_id": "1", "score": "1-0"}
                    ],
                }
            }
        }
        payload = dashboard_server._session_json(session)
        self.assertEqual(payload["event_counts"], {
            "unique": 1,
            "encoded": 0,
            "processing": 0,
            "history": 1,
            "failed": 0,
        })

    def test_runtime_evidence_reports_fresh_worker_heartbeat(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("runtime-test")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            session.worker_started_at = dashboard_server.time.time() - 6
            session.worker = type(
                "RunningWorker",
                (),
                {"poll": lambda self: None, "pid": 4321},
            )()
            buffer_dir = session.output_dir / "buffer"
            buffer_dir.mkdir()
            (buffer_dir / "segment_000001.ts").write_bytes(b"video")
            (session.output_dir / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": dashboard_server.time.time(),
                        "event": "runtime_heartbeat",
                        "event_poll_count": 3,
                        "event_error_count": 0,
                        "ingest_running": True,
                        "ingest_restart_count": 2,
                        "ingest_reconnect_due_unix": None,
                        "buffer_segment_count": 1,
                        "buffer_coverage_seconds": 2,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            evidence = dashboard_server._runtime_evidence(session, {}, [])
        self.assertEqual(evidence["state"], "healthy")
        self.assertEqual(evidence["event_poll_count"], 3)
        self.assertEqual(evidence["buffer_segment_count"], 1)
        self.assertTrue(evidence["worker_running"])
        self.assertTrue(evidence["ingest_running"])
        self.assertEqual(evidence["ingest_restart_count"], 2)
        self.assertIsNone(evidence["ingest_reconnect_due_unix"])
        self.assertTrue(evidence["heartbeat_fresh"])
        self.assertTrue(evidence["segment_writing"])

    def test_runtime_evidence_reports_live_ingest_reconnect_and_current_error(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("runtime-reconnect")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            session.worker_started_at = 100.0
            session.worker = Mock(pid=4322, returncode=None)
            session.worker.poll.return_value = None
            (session.output_dir / "pipeline_events.jsonl").write_text(
                "\n".join(
                    json.dumps(row)
                    for row in (
                        {
                            "timestamp_unix": 90.0,
                            "event": "runtime_heartbeat",
                            "ingest_running": True,
                            "ingest_restart_count": 99,
                        },
                        {
                            "timestamp_unix": 199.0,
                            "event": "runtime_heartbeat",
                            "event_poll_count": 8,
                            "ingest_running": False,
                            "ingest_restart_count": 4,
                            "ingest_reconnect_due_unix": 205.0,
                        },
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            old_error = session.output_dir / "ingest_ffmpeg_old_g001.log"
            old_error.write_text("old run error\n", encoding="utf-8")
            os.utime(old_error, (90.0, 90.0))
            current_error = session.output_dir / "ingest_ffmpeg_current_g001.log"
            current_error.write_text(
                "server error: Input/output error\n",
                encoding="utf-8",
            )
            os.utime(current_error, (150.0, 150.0))
            current_retry = session.output_dir / "ingest_ffmpeg_current_g002.log"
            current_retry.write_text("", encoding="utf-8")
            os.utime(current_retry, (180.0, 180.0))

            with patch("dashboard_server.time.time", return_value=200.0):
                evidence = dashboard_server._runtime_evidence(session, {}, [])

        self.assertEqual(evidence["state"], "recovering")
        self.assertIn("等待重连", evidence["label"])
        self.assertTrue(evidence["worker_running"])
        self.assertFalse(evidence["ingest_running"])
        self.assertEqual(evidence["ingest_restart_count"], 4)
        self.assertEqual(evidence["ingest_reconnect_due_unix"], 205.0)
        self.assertEqual(
            evidence["last_ingest_error"],
            "server error: Input/output error",
        )
        self.assertEqual(evidence["exit_message"], evidence["last_ingest_error"])

    def test_runtime_evidence_detects_running_ingest_with_stale_segments(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("runtime-stale-segments")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            session.worker_started_at = 100.0
            session.worker = Mock(pid=4323, returncode=None)
            session.worker.poll.return_value = None
            buffer_dir = session.output_dir / "buffer"
            buffer_dir.mkdir()
            segment = buffer_dir / "segment_current.ts"
            segment.write_bytes(b"video")
            os.utime(segment, (180.0, 180.0))
            (session.output_dir / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": 199.0,
                        "event": "runtime_heartbeat",
                        "ingest_running": True,
                        "ingest_restart_count": 2,
                        "buffer_segment_count": 1,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch("dashboard_server.time.time", return_value=200.0):
                evidence = dashboard_server._runtime_evidence(session, {}, [])

        self.assertEqual(evidence["state"], "degraded")
        self.assertIn("视频分片未持续写入", evidence["label"])
        self.assertTrue(evidence["heartbeat_fresh"])
        self.assertTrue(evidence["ingest_running"])
        self.assertFalse(evidence["segment_writing"])
        self.assertEqual(evidence["latest_segment_age_seconds"], 20.0)

    def test_runtime_evidence_ignores_artifacts_from_previous_worker_run(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("runtime-current-run-only")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            session.worker_started_at = 150.0
            session.worker = Mock(pid=4324, returncode=None)
            session.worker.poll.return_value = None
            buffer_dir = session.output_dir / "buffer"
            buffer_dir.mkdir()
            old_segment = buffer_dir / "segment_old.ts"
            old_segment.write_bytes(b"old video")
            os.utime(old_segment, (145.0, 145.0))
            (session.output_dir / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": 140.0,
                        "event": "runtime_heartbeat",
                        "ingest_running": False,
                        "ingest_restart_count": 7,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            old_error = session.output_dir / "ingest_ffmpeg_old_g001.log"
            old_error.write_text("stale Input/output error\n", encoding="utf-8")
            os.utime(old_error, (145.0, 145.0))

            stale_report = {
                "started_at_unix": 149.9,
                "runtime": {"ingest_restart_count": 12},
                "ffmpeg_return_code": 1,
            }
            with patch("dashboard_server.time.time", return_value=160.0):
                evidence = dashboard_server._runtime_evidence(
                    session,
                    stale_report,
                    [],
                )

        self.assertEqual(evidence["state"], "starting")
        self.assertIsNone(evidence["heartbeat_unix"])
        self.assertIsNone(evidence["ingest_running"])
        self.assertEqual(evidence["ingest_restart_count"], 0)
        self.assertEqual(evidence["buffer_segment_count"], 0)
        self.assertIsNone(evidence["last_ingest_error"])
        self.assertIsNone(evidence["exit_message"])

    def test_api_only_event_is_labeled_as_history(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("history-test")
        session.event_payload = {
            "events": {
                "12": {
                    "minute": "12",
                    "teamAEvents": [
                        {"code": "G", "person": "A", "person_id": "1"}
                    ],
                }
            }
        }
        payload = dashboard_server._session_json(session)
        self.assertEqual(payload["events"][0]["status"], "history")

    def test_health_and_demo_api(self):
        client = dashboard_server.app.test_client()
        self.assertEqual(client.get("/api/health").status_code, 200)
        response = client.get("/api/session?match_id=demo-api-test")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status_label"], "进行中")
        self.assertEqual(client.get("/api/session?match_id=../../bad").status_code, 400)

    def test_empty_source_response_keeps_last_valid_resource(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("123")
        session.source = {"resource": "rtmp://valid", "updated_at": "one"}
        session.last_detail_poll = 1e20
        session.last_event_poll = 1e20
        with patch("dashboard_server.query_source", return_value={"errno": 0, "data": {}}):
            manager.refresh(session)
        self.assertEqual(session.source["resource"], "rtmp://valid")
        self.assertIn("继续使用", session.source_error)

    def test_unexpected_live_worker_exit_is_restarted_from_desired_state(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("worker-recovery")
        session.source = {"resource": "rtmp://example/live"}
        session.desired_running = True
        session.worker_mode = "live"
        session.worker_started_at = 100.0
        session.worker_process_group = 101
        exited = Mock(pid=101, returncode=1)
        exited.poll.return_value = 1
        session.worker = exited
        session.last_detail_poll = 1e20
        session.last_source_poll = 1e20
        session.last_event_poll = 1e20

        with patch("dashboard_server.time.time", return_value=105.0), patch(
            "dashboard_server.os.killpg"
        ) as killpg, patch.object(
            manager, "_process_group_exists", return_value=False
        ):
            manager.refresh(session)
        self.assertEqual(session.worker_restart_due_at, 106.0)
        killpg.assert_called_once_with(101, dashboard_server.signal.SIGTERM)

        restarted = Mock(pid=202, returncode=None)
        restarted.poll.return_value = None
        with patch("dashboard_server.time.time", return_value=106.0), patch(
            "dashboard_server.subprocess.Popen", return_value=restarted
        ) as popen:
            manager.refresh(session)

        self.assertIs(session.worker, restarted)
        self.assertTrue(session.desired_running)
        self.assertEqual(session.worker_restart_count, 1)
        self.assertNotIn("--emit-existing-events", popen.call_args.args[0])
        self.assertTrue(popen.call_args.kwargs["start_new_session"])

    def test_restart_waits_for_term_then_kill_cleanup_confirmation(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("worker-forced-cleanup")
        session.source = {"resource": "rtmp://example/live"}
        session.desired_running = True
        session.worker_mode = "live"
        session.worker_started_at = 100.0
        session.worker_process_group = 101
        session.worker = Mock(pid=101, returncode=1)
        session.worker.poll.return_value = 1
        session.last_detail_poll = 1e20
        session.last_source_poll = 1e20
        session.last_event_poll = 1e20

        restarted = Mock(pid=202, returncode=None)
        restarted.poll.return_value = None
        group_exists = Mock(side_effect=[True, True, True, False])
        with patch.object(manager, "_process_group_exists", group_exists), patch(
            "dashboard_server.os.killpg"
        ) as killpg, patch(
            "dashboard_server.subprocess.Popen", return_value=restarted
        ) as popen:
            with patch("dashboard_server.time.time", return_value=105.0):
                manager.refresh(session)
            with patch("dashboard_server.time.time", return_value=109.0):
                manager.refresh(session)
            self.assertIs(session.worker.pid, 101)
            popen.assert_not_called()

            with patch("dashboard_server.time.time", return_value=112.0):
                manager.refresh(session)

        self.assertEqual(
            killpg.call_args_list,
            [
                call(101, dashboard_server.signal.SIGTERM),
                call(101, dashboard_server.signal.SIGKILL),
            ],
        )
        self.assertIs(session.worker, restarted)
        self.assertIsNone(session.worker_cleanup_process_group)
        self.assertEqual(session.worker_restart_count, 1)

    def test_cleanup_failure_blocks_worker_restart(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("worker-cleanup-failure")
        session.source = {"resource": "rtmp://example/live"}
        session.desired_running = True
        session.worker_mode = "live"
        session.worker_started_at = 100.0
        session.worker_process_group = 101
        session.worker = Mock(pid=101, returncode=1)
        session.worker.poll.return_value = 1
        session.last_detail_poll = 1e20
        session.last_source_poll = 1e20
        session.last_event_poll = 1e20

        with patch.object(
            manager, "_process_group_exists", return_value=True
        ), patch("dashboard_server.os.killpg") as killpg, patch(
            "dashboard_server.subprocess.Popen"
        ) as popen:
            for now in (105.0, 109.0, 112.0):
                with patch("dashboard_server.time.time", return_value=now):
                    manager.refresh(session)

        self.assertEqual(session.worker_cleanup_stage, "failed")
        self.assertIn("SIGKILL 后进程组仍存在", session.worker_cleanup_failure)
        self.assertEqual(session.worker_restart_due_at, 106.0)
        self.assertEqual(killpg.call_count, 2)
        popen.assert_not_called()

    def test_start_refuses_while_previous_process_group_cleanup_is_pending(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("worker-cleanup-pending")
        session.source = {"resource": "rtmp://example/live"}
        session.worker_cleanup_process_group = 101
        session.worker_cleanup_stage = "term"

        with patch("dashboard_server.subprocess.Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "尚未确认清理"):
                manager.start(session)

        popen.assert_not_called()

    def test_start_cleans_exited_worker_group_before_replacing_worker_handle(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("worker-cleanup-before-start")
        session.source = {"resource": "rtmp://example/live"}
        session.worker = Mock(pid=101, returncode=1)
        session.worker.poll.return_value = 1
        session.worker_process_group = 101
        restarted = Mock(pid=202, returncode=None)
        restarted.poll.return_value = None

        with patch.object(
            manager, "_process_group_exists", return_value=False
        ), patch("dashboard_server.os.killpg") as killpg, patch(
            "dashboard_server.subprocess.Popen", return_value=restarted
        ) as popen:
            manager.start(session)

        killpg.assert_called_once_with(101, dashboard_server.signal.SIGTERM)
        popen.assert_called_once()
        self.assertIs(session.worker, restarted)
        self.assertEqual(session.worker_process_group, 202)

    def test_blocking_cleanup_escalates_and_reaps_real_process_group(self):
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import signal,time;"
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
                    "signal.signal(signal.SIGINT, signal.SIG_IGN);"
                    "print('ready', flush=True);"
                    "time.sleep(60)"
                ),
            ],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            assert process.stdout is not None
            self.assertEqual(process.stdout.readline().strip(), "ready")
            manager = dashboard_server.Dashboard(background_monitors=False)
            session = manager.get("real-process-group-cleanup")
            with tempfile.TemporaryDirectory() as directory:
                session.output_dir = Path(directory)
                session.worker = process
                session.worker_process_group = process.pid
                with patch.object(
                    dashboard_server, "WORKER_TERM_GRACE_SECONDS", 0.05
                ), patch.object(
                    dashboard_server, "WORKER_KILL_GRACE_SECONDS", 0.2
                ), patch.object(
                    dashboard_server, "WORKER_CLEANUP_POLL_SECONDS", 0.01
                ):
                    self.assertTrue(
                        manager._cleanup_worker_group_blocking(session, process.pid)
                    )
            self.assertIsNotNone(process.returncode)
            self.assertIsNone(session.worker_cleanup_process_group)
        finally:
            if process.stdout is not None:
                process.stdout.close()
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=2)

    def test_manual_stop_cancels_scheduled_worker_restart(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("manual-stop")
        session.desired_running = True
        session.worker_mode = "live"
        session.worker_restart_due_at = 123.0

        manager.stop(session)

        self.assertFalse(session.desired_running)
        self.assertIsNone(session.worker_restart_due_at)
        self.assertEqual(session.lifecycle_state, "stopped")
        self.assertEqual(session.exit_reason, "manual_stop")

    def test_manual_stop_reports_failed_when_process_group_cleanup_fails(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("manual-stop-cleanup-failure")
        session.worker_process_group = 111
        session.worker = Mock(pid=111, returncode=None)
        session.worker.poll.return_value = None
        session.worker.wait.side_effect = subprocess.TimeoutExpired("worker", 12)

        with patch("dashboard_server.os.killpg"), patch.object(
            manager,
            "_cleanup_worker_group_blocking",
            return_value=False,
        ):
            session.worker_cleanup_failure = "cleanup failed"
            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                manager.stop(session)

        self.assertFalse(session.desired_running)
        self.assertEqual(session.lifecycle_state, "failed")

    def test_manual_stop_accepts_process_group_exiting_before_sigint(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("manual-stop-exit-race")
        session.worker_process_group = 131
        session.worker = Mock(pid=131, returncode=None)
        session.worker.poll.side_effect = [None, None]
        session.worker.wait.return_value = 0
        session.worker.returncode = 0

        with patch(
            "dashboard_server.os.killpg", side_effect=ProcessLookupError
        ), patch.object(
            manager, "_process_group_exists", return_value=False
        ):
            manager.stop(session)

        self.assertEqual(session.lifecycle_state, "stopped")
        self.assertFalse(session.desired_running)

    def test_source_switch_start_failure_schedules_automatic_recovery(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("source-switch-recovery")
        session.source = {"resource": "rtmp://example/new"}
        session.source_changed = True
        session.desired_running = True
        session.worker_mode = "live"
        session.worker = Mock(pid=121, returncode=None)
        session.worker.poll.return_value = None
        session.last_detail_poll = 1e20
        session.last_source_poll = 1e20
        session.last_event_poll = 1e20

        with patch("dashboard_server.time.time", return_value=200.0), patch.object(
            manager, "stop"
        ), patch.object(
            manager, "start", side_effect=OSError("cannot restart")
        ):
            manager.refresh(session)

        self.assertTrue(session.desired_running)
        self.assertEqual(session.worker_restart_due_at, 201.0)

    def test_failed_initial_start_does_not_set_desired_running(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("failed-start")
        session.source = {"resource": "rtmp://example/live"}

        with patch(
            "dashboard_server.subprocess.Popen", side_effect=OSError("cannot start")
        ):
            with self.assertRaisesRegex(OSError, "cannot start"):
                manager.start(session)

        self.assertFalse(session.desired_running)

    def test_played_requires_two_consecutive_detail_confirmations(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("played-confirmation")
        session.worker = Mock(pid=301)
        session.worker.poll.return_value = None
        session.worker_mode = "live"
        session.worker_started_at = 90.0
        session.desired_running = True
        session.worker_restart_due_at = 999.0
        session.last_source_poll = 1e20

        with patch(
            "dashboard_server.query_detail",
            side_effect=[{"status": "Played"}, {"status": "Played"}],
        ), patch(
            "dashboard_server.time.time", side_effect=[100.0, 111.0, 111.0, 111.0]
        ), patch(
            "dashboard_server.os.kill"
        ) as kill:
            manager.refresh(session)
            self.assertEqual(session.played_confirmation_count, 1)
            self.assertNotEqual(session.lifecycle_state, "finishing")

            session.source_changed = True
            manager.refresh(session)

        self.assertEqual(session.lifecycle_state, "finishing")
        self.assertFalse(session.desired_running)
        self.assertIsNone(session.worker_restart_due_at)
        self.assertFalse(session.source_changed)
        self.assertEqual(session.finishing_deadline, 246.0)
        kill.assert_called_once_with(301, dashboard_server.signal.SIGUSR1)

    def test_played_confirmation_resets_when_status_returns_to_playing(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("played-jitter")
        session.last_source_poll = 1e20
        session.last_event_poll = 1e20

        with patch(
            "dashboard_server.query_detail",
            side_effect=[{"status": "Played"}, {"status": "Playing"}],
        ), patch("dashboard_server.time.time", side_effect=[100.0, 111.0]), patch(
            "dashboard_server.os.kill"
        ) as kill:
            manager.refresh(session)
            manager.refresh(session)

        self.assertEqual(session.played_confirmation_count, 0)
        self.assertEqual(session.lifecycle_state, "playing")
        kill.assert_not_called()

    def test_finishing_disables_external_polling_and_source_restart(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("finishing-no-restart")
        session.lifecycle_state = "finishing"
        session.finishing_deadline = 220.0
        session.finish_requested = True
        session.desired_running = False
        session.source_changed = True
        session.worker_mode = "live"
        session.worker = Mock(pid=401)
        session.worker.poll.return_value = None

        with patch("dashboard_server.time.time", return_value=110.0), patch(
            "dashboard_server.query_detail"
        ) as detail, patch("dashboard_server.query_source") as source, patch(
            "dashboard_server.query_events"
        ) as events, patch.object(manager, "start") as start, patch.object(
            manager, "stop"
        ) as stop:
            manager.refresh(session)

        detail.assert_not_called()
        source.assert_not_called()
        events.assert_not_called()
        start.assert_not_called()
        stop.assert_not_called()

    def test_finishing_timeout_escalates_process_group_cleanup(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("finishing-timeout")
        session.lifecycle_state = "finishing"
        session.finishing_deadline = 100.0
        session.finish_requested = True
        session.worker_process_group = 501
        session.worker = Mock(pid=501)
        session.worker.poll.return_value = None

        group_exists = Mock(side_effect=[True, True, False])
        with patch.object(manager, "_process_group_exists", group_exists), patch(
            "dashboard_server.os.killpg"
        ) as killpg:
            with patch("dashboard_server.time.time", return_value=101.0):
                manager.refresh(session)
            with patch("dashboard_server.time.time", return_value=105.0):
                manager.refresh(session)
            with patch("dashboard_server.time.time", return_value=108.0):
                manager.refresh(session)

        self.assertTrue(session.finish_timeout_signaled)
        self.assertEqual(session.exit_reason, "match_played_finish_timeout")
        self.assertEqual(
            killpg.call_args_list,
            [
                call(501, dashboard_server.signal.SIGTERM),
                call(501, dashboard_server.signal.SIGKILL),
            ],
        )
        self.assertIsNone(session.worker_cleanup_process_group)

    def test_worker_exit_during_finishing_uses_reported_completion_state(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("finishing-complete")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            (session.output_dir / "event_pipeline_report.json").write_text(
                json.dumps(
                    {
                        "started_at_unix": 90.0,
                        "completion_state": "completed",
                        "exit_reason": "match_played",
                    }
                ),
                encoding="utf-8",
            )
            session.lifecycle_state = "finishing"
            session.finish_reason = "match_played"
            session.desired_running = False
            session.worker_started_at = 90.0
            session.worker = Mock(pid=601, returncode=0)
            session.worker.poll.return_value = 0

            with patch("dashboard_server.time.time", return_value=100.0):
                manager.refresh(session)

        self.assertEqual(session.lifecycle_state, "completed")
        self.assertEqual(session.exit_reason, "match_played")
        self.assertIsNone(session.worker_restart_due_at)

    def test_stream_incomplete_report_finishes_with_warning(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("finishing-incomplete")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            (session.output_dir / "event_pipeline_report.json").write_text(
                json.dumps(
                    {
                        "started_at_unix": 90.0,
                        "stop_reason": "match_played_stream_incomplete",
                    }
                ),
                encoding="utf-8",
            )
            session.lifecycle_state = "finishing"
            session.finish_reason = "match_played"
            session.worker_started_at = 90.0
            session.worker = Mock(pid=602, returncode=0)
            session.worker.poll.return_value = 0

            with patch("dashboard_server.time.time", return_value=100.0):
                manager.refresh(session)

        self.assertEqual(session.lifecycle_state, "completed_with_warnings")
        self.assertEqual(session.exit_reason, "match_played_stream_incomplete")

    def test_finishing_ignores_stale_report_from_previous_worker_run(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("finishing-stale-report")
        with tempfile.TemporaryDirectory() as directory:
            session.output_dir = Path(directory)
            (session.output_dir / "event_pipeline_report.json").write_text(
                json.dumps(
                    {
                        "started_at_unix": 10.0,
                        "completion_state": "completed",
                        "exit_reason": "match_played",
                    }
                ),
                encoding="utf-8",
            )
            session.lifecycle_state = "finishing"
            session.finish_reason = "match_played"
            session.worker_started_at = 90.0
            session.worker = Mock(pid=603, returncode=1)
            session.worker.poll.return_value = 1

            with patch("dashboard_server.time.time", return_value=100.0):
                manager.refresh(session)

        self.assertEqual(session.lifecycle_state, "completed_with_warnings")
        self.assertEqual(session.exit_reason, "match_played")

    def test_played_match_cannot_be_started_as_live_again(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("already-played")
        session.detail = {"status": "Played"}
        session.source = {"resource": "rtmp://example/live"}

        with patch("dashboard_server.subprocess.Popen") as popen:
            with self.assertRaisesRegex(RuntimeError, "比赛已结束"):
                manager.start(session)

        popen.assert_not_called()
        self.assertEqual(session.lifecycle_state, "completed")
        self.assertEqual(session.played_confirmation_count, 2)

    def test_terminal_monitor_thread_exits_and_removes_its_registry_entry(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("terminal-monitor")

        def finish_match(current):
            current.lifecycle_state = "stopped"

        with patch.object(manager, "refresh", side_effect=finish_match):
            thread = threading.Thread(target=manager._monitor, args=(session,))
            manager.monitor_threads[session.match_id] = thread
            thread.start()
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertNotIn(session.match_id, manager.monitor_threads)
        self.assertIsNotNone(session.terminal_at)

    def test_start_reinstates_monitor_after_terminal_monitor_exit(self):
        manager = dashboard_server.Dashboard(background_monitors=True)
        try:
            with tempfile.TemporaryDirectory() as directory:
                with patch.object(manager, "_monitor") as monitor:
                    session = manager.get("restart-monitor")
                    session.output_dir = Path(directory) / session.match_id
                    first_thread = manager.monitor_threads.pop(session.match_id)
                    first_thread.join(timeout=2)
                    session.lifecycle_state = "stopped"

                    fake_worker = Mock(pid=1234, returncode=None)
                    fake_worker.poll.return_value = None
                    with patch.object(
                        dashboard_server.subprocess,
                        "Popen",
                        return_value=fake_worker,
                    ):
                        manager.start(session, demo=True)

                    self.assertIn(session.match_id, manager.monitor_threads)
                    monitor_thread = manager.monitor_threads.pop(session.match_id)
                    monitor_thread.join(timeout=2)
                    self.assertEqual(monitor.call_count, 2)
        finally:
            manager.close()

    def test_session_ttl_only_evicts_terminal_inactive_sessions(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            session_retention_seconds=24 * 60 * 60,
        )
        terminal_at = time.time()
        expired = manager.get("expired-session")
        expired.lifecycle_state = "completed"
        expired.terminal_at = terminal_at
        active = manager.get("active-session")
        active.lifecycle_state = "completed"
        active.terminal_at = terminal_at
        active.desired_running = True

        with manager.lock:
            removed = manager._prune_terminal_sessions_locked(
                terminal_at + 24 * 60 * 60
            )

        self.assertEqual(removed, [expired.match_id])
        self.assertNotIn(expired.match_id, manager.sessions)
        self.assertIn(active.match_id, manager.sessions)

    def test_idle_session_ttl_evicts_unstarted_session(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            session_retention_seconds=60,
        )
        session = manager.get("idle-session")
        session.last_access_at = 100.0

        with manager.lock:
            removed = manager._prune_idle_sessions_locked(161.0)

        self.assertEqual(removed, [session.match_id])
        self.assertNotIn(session.match_id, manager.sessions)

    def test_terminal_maintenance_cleans_manual_stop_buffer_after_lease_expires(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("manual-stop-cleanup")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / session.match_id
            buffer_dir = output / "buffer"
            buffer_dir.mkdir(parents=True)
            segment = buffer_dir / "old.ts"
            segment.write_bytes(b"segment")
            segment_list = buffer_dir / "segments.csv"
            segment_list.write_text("old.ts,0,2\n", encoding="utf-8")
            manifest = buffer_dir / "segment_manifest.json"
            manifest.write_text(
                json.dumps(
                    {"version": 1, "generations": [{"list_path": "segments.csv"}]}
                ),
                encoding="utf-8",
            )
            database = output / "pipeline_state.sqlite3"
            connection = dashboard_server.sqlite3.connect(database)
            connection.execute(
                "CREATE TABLE segment_leases "
                "(segment_path TEXT, expires_at_unix REAL)"
            )
            connection.execute(
                "INSERT INTO segment_leases VALUES (?, ?)",
                (str(segment), 200.0),
            )
            connection.commit()
            connection.close()
            session.output_dir = output
            session.lifecycle_state = "stopped"
            session.terminal_at = 90.0

            with patch.object(dashboard_server, "DEFAULT_OUTPUT", Path(directory)):
                manager.run_maintenance(now=100.0)
                self.assertTrue(segment.exists())
                self.assertTrue(segment_list.exists())
                self.assertFalse(session.terminal_cleanup_done)

                manager.run_maintenance(now=300.0)

            self.assertFalse(segment.exists())
            self.assertFalse(segment_list.exists())
            self.assertEqual(json.loads(manifest.read_text())["generations"], [])
            self.assertTrue(session.terminal_cleanup_done)

    def test_maintenance_never_cleans_an_active_worker_session(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("active-cleanup-guard")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / session.match_id
            buffer_dir = output / "buffer"
            buffer_dir.mkdir(parents=True)
            segment = buffer_dir / "active.ts"
            segment.write_bytes(b"active")
            session.output_dir = output
            session.lifecycle_state = "playing"
            session.desired_running = True
            session.worker = Mock(pid=991, returncode=None)
            session.worker.poll.return_value = None

            with patch.object(dashboard_server, "DEFAULT_OUTPUT", Path(directory)):
                manager.run_maintenance(now=time.time())

            self.assertTrue(segment.exists())
            self.assertFalse(session.terminal_cleanup_done)

    def test_maintenance_cleans_stale_orphan_output_without_session(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            orphan_cleanup_grace_seconds=60,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "orphan-match"
            buffer_dir = output / "buffer"
            buffer_dir.mkdir(parents=True)
            segment = buffer_dir / "orphan.ts"
            segment.write_bytes(b"segment")
            (buffer_dir / "segments.csv").write_text(
                "orphan.ts,0,2\n", encoding="utf-8"
            )
            (buffer_dir / "segment_manifest.json").write_text(
                json.dumps(
                    {"version": 1, "generations": [{"list_path": "segments.csv"}]}
                ),
                encoding="utf-8",
            )
            (output / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": 100.0,
                        "event": "worker_started",
                        "pid": 999999,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root):
                manager.run_maintenance(now=200.0)

            self.assertFalse(segment.exists())
            self.assertFalse((buffer_dir / "segments.csv").exists())
            self.assertEqual(
                json.loads((buffer_dir / "segment_manifest.json").read_text())["generations"],
                [],
            )

    def test_maintenance_keeps_fresh_orphan_output_for_later_pass(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            orphan_cleanup_grace_seconds=60,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "fresh-orphan"
            buffer_dir = output / "buffer"
            buffer_dir.mkdir(parents=True)
            segment = buffer_dir / "fresh.ts"
            segment.write_bytes(b"segment")
            (output / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": 190.0,
                        "event": "runtime_heartbeat",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root):
                manager.run_maintenance(now=200.0)

            self.assertTrue(segment.exists())

    def test_orphan_cleanup_never_touches_a_directory_with_a_session(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            orphan_cleanup_grace_seconds=60,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            session = manager.get("stopped-session")
            session.output_dir = root / session.match_id
            session.lifecycle_state = "stopped"
            session.terminal_at = 90.0
            session.output_dir.mkdir(parents=True)
            (session.output_dir / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": 100.0,
                        "event": "pipeline_stopped",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root):
                with patch.object(
                    dashboard_server.DiskLifecycleManager,
                    "cleanup_finished_match",
                ) as cleanup:
                    manager._prune_orphan_outputs(200.0)

            cleanup.assert_not_called()

    def test_maintenance_keeps_orphan_output_when_worker_pid_is_alive(self):
        manager = dashboard_server.Dashboard(
            background_monitors=False,
            orphan_cleanup_grace_seconds=60,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "live-orphan"
            buffer_dir = output / "buffer"
            buffer_dir.mkdir(parents=True)
            segment = buffer_dir / "live.ts"
            segment.write_bytes(b"segment")
            (output / "pipeline_events.jsonl").write_text(
                json.dumps(
                    {
                        "timestamp_unix": 100.0,
                        "event": "worker_started",
                        "pid": 1234,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root):
                with patch.object(dashboard_server.os, "kill", return_value=None):
                    manager.run_maintenance(now=200.0)

            self.assertTrue(segment.exists())

    def test_recovery_start_is_allowed_after_single_unconfirmed_played_response(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("played-recovery")
        session.detail = {"status": "Played"}
        session.lifecycle_state = "playing"
        session.source = {"resource": "rtmp://example/live"}
        worker = Mock(pid=701, returncode=None)
        worker.poll.return_value = None

        with patch("dashboard_server.subprocess.Popen", return_value=worker):
            manager.start(session, recovery=True)

        self.assertIs(session.worker, worker)
        self.assertEqual(session.lifecycle_state, "playing")


if __name__ == "__main__":
    unittest.main()
