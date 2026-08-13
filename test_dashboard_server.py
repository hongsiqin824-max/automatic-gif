import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, call, patch

import dashboard_server


class DashboardTests(unittest.TestCase):
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

    def test_query_events_accepts_empty_event_array(self):
        with patch(
            "dashboard_server._json_request",
            return_value={"status": 0, "events": []},
        ):
            payload = dashboard_server.query_events("54478923")
        self.assertEqual(payload["events"], {})

    def test_query_events_requires_success_status_and_valid_events_shape(self):
        with patch(
            "dashboard_server._json_request",
            return_value={"status": 500, "events": {}},
        ):
            with self.assertRaisesRegex(ValueError, "status=500"):
                dashboard_server.query_events("54478923")
        with patch(
            "dashboard_server._json_request",
            return_value={"status": 0, "events": [{"code": "G"}]},
        ):
            with self.assertRaisesRegex(ValueError, "events"):
                dashboard_server.query_events("54478923")

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
