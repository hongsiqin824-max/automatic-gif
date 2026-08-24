import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import dashboard_server
from article_publisher import ArticlePublishError, PublishedGifStore


class DashboardPublishRoutesTests(unittest.TestCase):
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
