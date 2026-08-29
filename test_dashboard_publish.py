import json
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import dashboard_server
from article_publisher import (
    ArticlePublishError,
    ArticlePublisher,
    PublishedGifStore,
)


def animated_gif_bytes_for_tests():
    header = (
        b"GIF89a"
        b"\x01\x00\x01\x00"
        b"\x80\x00\x00"
        b"\x00\x00\x00\xff\xff\xff"
    )
    frame = (
        b"\x2c"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00"
        b"\x02\x02\x44\x01\x00"
    )
    return header + frame + frame + b"\x3b"


def publisher_for_tests(root):
    return ArticlePublisher(
        platform_client=Mock(),
        gif_store=PublishedGifStore(
            root / "published", "https://matchgif.aisportsapp.com"
        ),
        database_path=root / "publish.sqlite3",
        public_url_checker=lambda url: None,
    )


def trusted_ocr_artifact(output: str):
    return {
        "status": "encoded",
        "output": output,
        "output_kind": "ocr_window",
        "localization_source": "exact_second",
        "localization_quality": "exact",
        "ocr_verified": True,
        "event_frame_may_be_missing": False,
        "anchor_adjusted": False,
        "stitched_across_gap": False,
        "video_gap_count": 0,
        "anchor_stream_time": 130.0,
        "clip_before_seconds": 30.0,
        "clip_after_seconds": 30.0,
        "requested_media_window": {
            "start_stream_time": 100.0,
            "end_stream_time": 160.0,
        },
        "actual_media_window": {
            "start_stream_time": 100.0,
            "end_stream_time": 160.0,
        },
        "available_media_duration_seconds": 60.0,
        "duration_sec": 60.0,
    }


class DashboardPublishRoutesTests(unittest.TestCase):
    def test_upload_route_requires_token_and_returns_public_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            publisher = publisher_for_tests(root)
            with patch.object(dashboard_server, "GIF_UPLOAD_TOKEN", "upload-secret"), patch.object(
                dashboard_server, "article_publisher", publisher
            ):
                client = dashboard_server.app.test_client()
                unauthorized = client.post(
                    "/api/article-publish/upload",
                    data={
                        "match_id": "54478914",
                        "event_key": "goal-19",
                        "gif": (io.BytesIO(animated_gif_bytes_for_tests()), "goal.gif"),
                    },
                    content_type="multipart/form-data",
                )
                response = client.post(
                    "/api/article-publish/upload",
                    headers={"Authorization": "Bearer upload-secret"},
                    data={
                        "match_id": "54478914",
                        "event_key": "goal-19",
                        "artifact_kind": "ocr_window",
                        "gif": (io.BytesIO(animated_gif_bytes_for_tests()), "goal.gif"),
                    },
                    content_type="multipart/form-data",
                )
            self.assertEqual(unauthorized.status_code, 401)
            self.assertEqual(response.status_code, 200)
            uploaded_gif = response.get_json()["gif"]
            self.assertTrue(uploaded_gif["url"].startswith("https://"))
            self.assertTrue(uploaded_gif["cover_url"].startswith("https://"))
            self.assertTrue(uploaded_gif["cover_url"].endswith(".jpg"))
            publisher.platform_client.create_article.assert_not_called()

    def test_publish_route_uses_uploaded_gif_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            publisher = publisher_for_tests(root)
            uploaded = publisher.upload_gif(
                body=animated_gif_bytes_for_tests(),
                match_id="54478914",
                event_key="goal-19",
                artifact_kind="ocr_window",
            )
            publisher.publish = Mock(
                return_value={"status": "success", "article_id": "3801234"}
            )
            session_payload = {
                "detail": {"team_A_name": "主队", "team_B_name": "客队"},
                "events": [
                    {
                        "event_key": "goal-19",
                        "status": "failed",
                        "code": "G",
                        "ocr_window": trusted_ocr_artifact(
                            str(Path(uploaded["gif"]["path"]).resolve())
                        ),
                    }
                ],
            }
            with patch.object(
                dashboard_server.dashboard, "get", return_value=Mock()
            ), patch.object(
                dashboard_server, "_session_json", return_value=session_payload
            ), patch.object(dashboard_server, "article_publisher", publisher):
                response = dashboard_server.app.test_client().post(
                    "/api/article-publish",
                    json={
                        "match_id": "54478914",
                        "event_key": "goal-19",
                        "artifact_kind": "ocr_window",
                        "gif_id": uploaded["gif"]["gif_id"],
                    },
                )
            self.assertEqual(response.status_code, 200)
            called = publisher.publish.call_args.kwargs
            self.assertEqual(
                called["source_path"],
                Path(uploaded["gif"]["path"]).resolve(),
            )
            self.assertEqual(called["event"]["status"], "encoded")

    def test_publish_route_can_be_disabled_on_storage_only_server(self):
        with patch.object(dashboard_server, "ARTICLE_PUBLISH_ENABLED", False):
            response = dashboard_server.app.test_client().post(
                "/api/article-publish",
                json={"match_id": "54478914", "event_key": "goal-19"},
            )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.get_json()["code"], "article_publish_disabled")

    def test_dashboard_uses_automatic_ocr_article_status_without_publish_buttons(self):
        script = (
            Path(dashboard_server.ROOT) / "dashboard_static" / "app.js"
        ).read_text(encoding="utf-8")
        self.assertIn("function automaticArticleStatus(event, artifact)", script)
        self.assertIn("等待自动创建文章", script)
        self.assertIn("文章任务尚未登记", script)
        self.assertIn("草稿已创建 · 等待球员信息", script)
        self.assertIn("等待球员信息 · 尚未创建草稿", script)
        self.assertIn("等待接口补齐球员信息", script)
        self.assertIn("未获取球员，已使用球队名发布", script)
        self.assertIn("错误码", script)
        self.assertIn("未自动发布", script)
        self.assertIn("publication_gate", script)
        self.assertIn("publication_eligibility", script)
        self.assertIn("ocr_article_eligibility", script)
        self.assertIn("auto_publish_not_eligible", script)
        self.assertIn("已自动发布", script)
        self.assertNotIn("publish-button", script)
        self.assertNotIn("publishGif(button)", script)

    def test_successful_publish_exposes_article_admin_link(self):
        script = (
            Path(dashboard_server.ROOT) / "dashboard_static" / "app.js"
        ).read_text(encoding="utf-8")
        self.assertIn("function articleAdminUrl(articleId)", script)
        self.assertIn(
            "https://dadmin.dongqiudi.com/admin/archives/articlePublish?articleId=",
            script,
        )
        self.assertIn("查看文章", script)

    def test_latest_article_event_uses_fresh_matching_api_revision(self):
        current = {
            "event_key": "stable-goal",
            "code": "G",
            "minute": "19",
            "team": "teamA",
            "person": "",
            "person_id": "0",
            "score": "1-0",
            "metadata": {"event_id": "event-19"},
        }
        payload = {
            "events": {
                "19": {
                    "minute": "19",
                    "teamAEvents": [
                        {
                            "id": "event-19",
                            "code": "G",
                            "person": "接口补齐球员",
                            "person_id": "91",
                            "score": "1-0",
                        }
                    ],
                }
            }
        }
        with patch.object(
            dashboard_server, "_tasks_from_database", return_value=([], {}, set())
        ), patch.object(dashboard_server, "query_events", return_value=payload):
            latest = dashboard_server._latest_ocr_article_event(
                "54478914", "stable-goal", current
            )

        self.assertEqual(latest["event_key"], "stable-goal")
        self.assertEqual(latest["person"], "接口补齐球员")
        self.assertEqual(latest["person_id"], "91")

    def test_ocr_publish_route_resolves_server_owned_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            match_dir = root / "54478914"
            match_dir.mkdir()
            gif_path = match_dir / "ocr.gif"
            gif_path.write_bytes(b"GIF89a")
            fake_publisher = Mock()
            fake_publisher.publish.return_value = {
                "status": "success", "article_id": "3801234"
            }
            fake_session = Mock()
            session_payload = {
                "detail": {"team_A_name": "主队", "team_B_name": "客队"},
                "events": [{
                    "event_key": "goal-19", "status": "failed", "code": "G",
                    "ocr_window": trusted_ocr_artifact(str(gif_path)),
                }],
            }
            with patch.object(dashboard_server, "DEFAULT_OUTPUT", root), patch.object(
                dashboard_server.dashboard, "get", return_value=fake_session
            ), patch.object(
                dashboard_server, "_session_json", return_value=session_payload
            ), patch.object(dashboard_server, "article_publisher", fake_publisher):
                response = dashboard_server.app.test_client().post(
                    "/api/article-publish",
                    json={
                        "match_id": "54478914",
                        "event_key": "goal-19",
                        "artifact_kind": "ocr_window",
                    },
                )
        self.assertEqual(response.status_code, 200)
        called = fake_publisher.publish.call_args.kwargs
        self.assertEqual(called["source_path"], gif_path.resolve())
        self.assertEqual(called["event"]["status"], "encoded")

    def test_ocr_publish_route_rejects_unverified_artifact(self):
        fake_publisher = Mock()
        session_payload = {
            "detail": {"team_A_name": "主队", "team_B_name": "客队"},
            "events": [{
                "event_key": "goal-19",
                "status": "encoded",
                "code": "G",
                "ocr_window": {
                    "status": "encoded",
                    "output": "/tmp/not-used.gif",
                    "output_kind": "api_time_range_fallback",
                    "localization_source": "api_time_range",
                },
            }],
        }
        with patch.object(
            dashboard_server.dashboard, "get", return_value=Mock()
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
                    "artifact_kind": "ocr_window",
                },
            )

        payload = response.get_json()
        self.assertEqual(response.status_code, 409)
        self.assertEqual(payload["stage"], "publication_gate")
        self.assertEqual(payload["code"], "unverified_api_fallback")
        self.assertFalse(
            payload["diagnostics"]["publication_eligibility"]["eligible"]
        )
        fake_publisher.publish.assert_not_called()

    def test_ocr_draft_retry_route_remains_compatible_for_old_tasks(self):
        queue = Mock()
        queue.retry.return_value = {"match_id": "54478914", "event_key": "goal-19", "status": "queued"}
        with patch.object(dashboard_server, "article_draft_queue", queue):
            response = dashboard_server.app.test_client().post(
                "/api/article-draft/retry",
                json={"match_id": "54478914", "event_key": "goal-19"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["ocr_draft"]["status"], "queued")
        queue.retry.assert_called_once_with(
            match_id="54478914", event_key="goal-19"
        )

    def test_imported_dashboard_does_not_start_draft_delivery_thread(self):
        self.assertIsNone(dashboard_server.article_draft_queue._thread)

    def test_dashboard_worker_enables_automatic_ocr_article_delivery(self):
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
        self.assertIn("--ocr-draft-db", command)
        self.assertIn("--ocr-draft-staging-dir", command)
        self.assertIn("--ocr-draft-team-a-name", command)
        self.assertIn("--ocr-draft-team-b-name", command)

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
            platform_code=10007,
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
        self.assertEqual(payload["platform_code"], 10007)

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
