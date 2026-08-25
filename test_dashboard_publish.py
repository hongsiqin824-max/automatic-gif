import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import dashboard_server
from article_publisher import ArticlePublishError, PublishedGifStore


class DashboardPublishRoutesTests(unittest.TestCase):
    def test_ocr_draft_success_uses_a_separate_admin_button(self):
        script = (
            Path(dashboard_server.ROOT) / "dashboard_static" / "app.js"
        ).read_text(encoding="utf-8")
        self.assertIn('class="draft-link"', script)
        self.assertIn("查看草稿", script)
        self.assertIn('target="_blank"', script)

    def test_imported_dashboard_does_not_start_draft_delivery_thread(self):
        self.assertIsNone(dashboard_server.article_draft_queue._thread)

    def test_dashboard_worker_receives_permanent_draft_staging_directory(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("54478914")
        session.source = {"resource": "rtmp://example/live"}
        session.detail = {"team_A_name": "主队", "team_B_name": "客队"}
        session.vision_enabled = True
        worker = Mock(pid=123, returncode=None)
        worker.poll.return_value = None

        with patch.object(
            dashboard_server.subprocess,
            "Popen",
            return_value=worker,
        ) as popen:
            manager.start(session)

        command = popen.call_args.args[0]
        self.assertEqual(
            command[command.index("--ocr-draft-staging-dir") + 1],
            str(dashboard_server.article_publisher.gif_store.directory),
        )

    def test_reconcile_registers_only_existing_encoded_unsuppressed_ocr(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        session = manager.get("54478914")
        session.detail = {"team_A_name": "主队", "team_B_name": "客队"}
        draft_queue = Mock()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / session.match_id
            output_dir.mkdir()
            encoded_gif = output_dir / "encoded.gif"
            encoded_gif.write_bytes(b"GIF89a")
            tasks = [
                {
                    "event_key": "goal-version",
                    "code": "G",
                    "event_type": "goal",
                    "minute": "19",
                    "person": "球员甲",
                    "ocr_window": {
                        "status": "encoded",
                        "output": str(encoded_gif),
                        "visual_resolution": "ocr_second_exact",
                    },
                },
                {
                    "event_key": "yellow-suppressed",
                    "code": "YC",
                    "event_type": "yellow_card",
                    "ocr_window": {
                        "status": "encoded",
                        "output": str(encoded_gif),
                    },
                },
                {
                    "event_key": "red-pending",
                    "code": "RC",
                    "event_type": "red_card",
                    "ocr_window": {
                        "status": "encoding",
                        "output": str(encoded_gif),
                    },
                },
            ]
            with patch.object(
                dashboard_server,
                "_tasks_from_database",
                return_value=(
                    tasks,
                    {"goal-version": "goal-canonical"},
                    {"yellow-suppressed"},
                ),
            ):
                count = manager.reconcile_ocr_drafts(
                    draft_queue=draft_queue,
                    output_root=root,
                )

        self.assertEqual(count, 1)
        called = draft_queue.enqueue.call_args.kwargs
        self.assertEqual(called["match_id"], "54478914")
        self.assertEqual(called["event"]["event_key"], "goal-canonical")
        self.assertEqual(called["match_detail"]["team_A_name"], "主队")
        self.assertEqual(called["source_path"], encoded_gif)

    def test_reconcile_failure_is_logged_and_does_not_stop_maintenance(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        draft_queue = Mock()
        draft_queue.enqueue.side_effect = RuntimeError("暂时无法写入队列")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "54478914"
            output_dir.mkdir()
            encoded_gif = output_dir / "encoded.gif"
            encoded_gif.write_bytes(b"GIF89a")
            task = {
                "event_key": "goal-19",
                "code": "G",
                "event_type": "goal",
                "ocr_window": {"status": "encoded", "output": str(encoded_gif)},
            }
            with patch.object(
                dashboard_server,
                "_tasks_from_database",
                return_value=([task], {}, set()),
            ):
                count = manager.reconcile_ocr_drafts(
                    draft_queue=draft_queue,
                    output_root=root,
                )

            records = [
                json.loads(line)
                for line in (output_dir / "pipeline_events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]

        self.assertEqual(count, 0)
        self.assertEqual(records[-1]["event"], "ocr_draft_reconcile_failed")
        self.assertIn("系统稍后会再试", records[-1]["message"])

    def test_reconcile_does_not_backfill_old_historical_ocr(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        draft_queue = Mock()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "54478914"
            output_dir.mkdir()
            encoded_gif = output_dir / "historical.gif"
            encoded_gif.write_bytes(b"GIF89a")
            os.utime(encoded_gif, (1.0, 1.0))
            task = {
                "event_key": "goal-old",
                "code": "G",
                "event_type": "goal",
                "ocr_window": {"status": "encoded", "output": str(encoded_gif)},
            }
            with patch.object(
                dashboard_server,
                "_tasks_from_database",
                return_value=([task], {}, set()),
            ):
                count = manager.reconcile_ocr_drafts(
                    draft_queue=draft_queue,
                    output_root=root,
                )

        self.assertEqual(count, 0)
        draft_queue.enqueue.assert_not_called()

    def test_explicit_dashboard_start_starts_queue_then_reconciles(self):
        draft_queue = Mock()
        manager = Mock()
        manager.reconcile_ocr_drafts.return_value = 2
        with patch.object(dashboard_server, "OCR_DRAFT_AUTO_CREATE", True), patch.object(
            dashboard_server, "article_draft_queue", draft_queue
        ), patch.object(dashboard_server, "dashboard", manager):
            recovered = dashboard_server.start_ocr_draft_delivery()

        draft_queue.start.assert_called_once_with()
        manager.reconcile_ocr_drafts.assert_called_once_with()
        self.assertEqual(recovered, 2)

    def test_maintenance_reconciles_before_pruning_gifs(self):
        manager = dashboard_server.Dashboard(background_monitors=False)
        calls = []
        with patch.object(dashboard_server, "OCR_DRAFT_AUTO_CREATE", True), patch.object(
            manager,
            "reconcile_ocr_drafts",
            side_effect=lambda: calls.append("reconcile"),
        ), patch.object(
            manager,
            "_prune_expired_gifs",
            side_effect=lambda: calls.append("prune"),
        ), patch.object(manager, "_prune_orphan_outputs", return_value=[]):
            manager.run_maintenance(now=100.0)

        self.assertEqual(calls, ["reconcile", "prune"])

    def test_session_keeps_default_publish_and_ocr_draft_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            session = dashboard_server.MatchSession(
                match_id="54478914",
                output_dir=Path(directory),
            )
            event = {
                "event_key": "goal-19",
                "status": "encoded",
                "code": "G",
                "ocr_window": {
                    "artifact_kind": "ocr_window",
                    "status": "encoded",
                    "output": str(Path(directory) / "ocr.gif"),
                },
            }
            default_record = {"status": "success", "article_id": "6000001"}
            draft_record = {
                "status": "success",
                "article_id": "6230049",
                "draft_url": (
                    "https://dadmin.dongqiudi.com/admin/archives/"
                    "articlePublish?articleId=6230049"
                ),
            }
            with patch.object(
                dashboard_server,
                "_tasks_from_database",
                return_value=([event], {}, set()),
            ), patch.object(
                dashboard_server.article_publisher,
                "records_for_match",
                return_value={"goal-19": default_record},
            ), patch.object(
                dashboard_server.article_draft_queue,
                "records_for_match",
                return_value={"goal-19": draft_record},
            ):
                payload = dashboard_server._session_json(session)

            self.assertEqual(payload["events"][0]["publish"], default_record)
            self.assertEqual(payload["events"][0]["ocr_draft"], draft_record)

    def test_default_gif_path_must_belong_to_match_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            match_dir = root / "54478914"
            match_dir.mkdir()
            valid = match_dir / "default.gif"
            valid.write_bytes(b"GIF89a")
            outside = root / "outside.gif"
            outside.write_bytes(b"GIF89a")
            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root):
                self.assertEqual(
                    dashboard_server._default_gif_source_path(
                        "54478914", {"output": str(valid)}
                    ),
                    valid.resolve(),
                )
                with self.assertRaisesRegex(ArticlePublishError, "不属于"):
                    dashboard_server._default_gif_source_path(
                        "54478914", {"output": str(outside)}
                    )

    def test_publish_route_passes_only_server_resolved_event(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            match_dir = root / "54478914"
            match_dir.mkdir()
            gif_path = match_dir / "default.gif"
            gif_path.write_bytes(b"GIF89a")
            fake_publisher = Mock()
            fake_publisher.publish.return_value = {
                "status": "success",
                "article_id": "3801234",
            }
            fake_session = Mock()
            session_payload = {
                "detail": {"team_A_name": "主队", "team_B_name": "客队"},
                "events": [
                    {
                        "event_key": "goal-19",
                        "status": "encoded",
                        "output": str(gif_path),
                        "code": "G",
                    }
                ],
            }
            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root), patch.object(
                dashboard_server.dashboard, "get", return_value=fake_session
            ), patch.object(
                dashboard_server, "_session_json", return_value=session_payload
            ), patch.object(
                dashboard_server, "article_publisher", fake_publisher
            ):
                response = dashboard_server.app.test_client().post(
                    "/api/article-publish",
                    json={
                        "match_id": "54478914",
                        "event_key": "goal-19",
                        "output": str(root / "attacker-controlled.gif"),
                    },
                )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()["publish"]["article_id"], "3801234")
            called = fake_publisher.publish.call_args.kwargs
            self.assertEqual(called["source_path"], gif_path.resolve())
            self.assertEqual(called["event"]["event_key"], "goal-19")

    def test_publish_route_reports_stage_and_oauth_url(self):
        fake_publisher = Mock()
        fake_publisher.publish.side_effect = ArticlePublishError(
            "请先授权",
            code="open_platform_auth_required",
            stage="authorization",
            status_code=401,
            auth_required=True,
        )
        event = {
            "event_key": "goal-19",
            "status": "encoded",
            "output": "/tmp/not-used.gif",
        }
        with patch.object(
            dashboard_server.dashboard, "get", return_value=Mock()
        ), patch.object(
            dashboard_server,
            "_session_json",
            return_value={"detail": {}, "events": [event]},
        ), patch.object(
            dashboard_server,
            "_default_gif_source_path",
            return_value=Path("/tmp/not-used.gif"),
        ), patch.object(
            dashboard_server, "article_publisher", fake_publisher
        ):
            response = dashboard_server.app.test_client().post(
                "/api/article-publish",
                json={"match_id": "54478914", "event_key": "goal-19"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 401)
        self.assertEqual(payload["stage"], "authorization")
        self.assertEqual(payload["oauth_url"], "/api/open-platform/oauth/start")

    def test_published_gif_route_serves_only_sha_named_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif_id = "a" * 64
            store = PublishedGifStore(root, "https://matchgif.aisportsapp.com")
            store.path_for(gif_id).write_bytes(b"GIF89a")
            fake_publisher = Mock(gif_store=store)
            with patch.object(dashboard_server, "article_publisher", fake_publisher):
                client = dashboard_server.app.test_client()
                response = client.get(f"/publish-gifs/{gif_id}.gif")
                invalid = client.get("/publish-gifs/not-a-sha.gif")
            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.content_type.startswith("image/gif"))
            self.assertEqual(invalid.status_code, 404)


if __name__ == "__main__":
    unittest.main()
