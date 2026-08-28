import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from article_publisher import (
    ArticlePublishError,
    ArticlePublisher,
    PublishedGifStore,
    RemoteGifUploadClient,
    build_article_fields,
    build_article_title,
    inspect_animated_gif,
)
from open_platform_client import OpenPlatformError


def animated_gif_bytes():
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


class FakePlatformClient:
    def __init__(self):
        self.calls = []

    def status(self):
        return {"configured": True, "authorized": True}

    def create_article(self, fields):
        self.calls.append(fields)
        return {"article_id": "3801234", "duplicate": False, "code": 0}


class ArticlePublisherTests(unittest.TestCase):
    def test_remote_upload_client_returns_server_public_url(self):
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps({
                    "ok": True,
                    "gif": {
                        "gif_id": expected_id,
                        "url": f"https://matchgif.aisportsapp.com/publish-gifs/{expected_id}.gif",
                    },
                }).encode()

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "goal.gif"
            source.write_bytes(animated_gif_bytes())
            import hashlib
            expected_id = hashlib.sha256(animated_gif_bytes()).hexdigest()
            client = RemoteGifUploadClient(
                "https://matchgif.aisportsapp.com/api/article-publish/upload",
                "upload-secret",
            )
            with patch("article_publisher.urllib.request.urlopen", return_value=Response()) as opened:
                result = client.upload(
                    source_path=source,
                    match_id="54478914",
                    event_key="goal-19",
                    artifact_kind="ocr_window",
                    max_bytes=1024,
                )

        request = opened.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer upload-secret")
        self.assertIn(b'name="match_id"', request.data)
        self.assertIn(b"54478914", request.data)
        self.assertEqual(result["gif_id"], expected_id)
        self.assertTrue(result["url"].startswith("https://"))

    def test_local_publish_uploads_then_calls_platform_locally(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "goal.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            remote = RemoteGifUploadClient("https://upload.example/api", "secret")
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
                remote_upload_client=remote,
            )
            gif_id = __import__("hashlib").sha256(animated_gif_bytes()).hexdigest()
            remote_result = {
                "gif_id": gif_id,
                "path": str(root / "published" / f"{gif_id}.gif"),
                "url": f"https://matchgif.aisportsapp.com/publish-gifs/{gif_id}.gif",
                "bytes": len(animated_gif_bytes()),
                "animated": True,
                "frame_count": 2,
                "header": "GIF89a",
            }
            event = {
                "event_key": "goal-19",
                "status": "encoded",
                "code": "G",
                "minute": "19",
            }
            with patch.object(remote, "upload", return_value=remote_result) as upload:
                first = publisher.publish(
                    match_id="54478914",
                    event=event,
                    match_detail={},
                    source_path=source,
                    artifact_kind="ocr_window",
                )
                second = publisher.publish(
                    match_id="54478914",
                    event=event,
                    match_detail={},
                    source_path=source,
                    artifact_kind="ocr_window",
                )

            upload.assert_called_once()
            self.assertEqual(len(platform.calls), 1)
            self.assertIn(remote_result["url"], platform.calls[0]["body"])
            self.assertEqual(first["status"], "success")
            self.assertTrue(second["idempotent_replay"])

    def test_automatic_article_uses_shared_remote_upload_and_reuses_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            remote = RemoteGifUploadClient("https://upload.example/api", "secret")
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
                remote_upload_client=remote,
            )
            gif_id = __import__("hashlib").sha256(animated_gif_bytes()).hexdigest()
            remote_result = {
                "gif_id": gif_id,
                "path": str(root / "published" / f"{gif_id}.gif"),
                "url": f"https://matchgif.aisportsapp.com/publish-gifs/{gif_id}.gif",
                "bytes": len(animated_gif_bytes()),
                "animated": True,
                "frame_count": 2,
                "header": "GIF89a",
            }
            event = {
                "event_key": "goal-19",
                "code": "G",
                "minute": "19",
            }

            with patch.object(remote, "upload", return_value=remote_result) as upload:
                first = publisher.create_or_update_draft(
                    match_id="54478914",
                    event=event,
                    match_detail={},
                    source_path=source,
                )
                second = publisher.create_or_update_draft(
                    match_id="54478914",
                    event=event,
                    match_detail={},
                    source_path=source,
                    archive_id=first["article_id"],
                )

            upload.assert_called_once()
            self.assertEqual(upload.call_args.kwargs["artifact_kind"], "ocr_window")
            self.assertEqual(first["gif"]["url"], remote_result["url"])
            self.assertEqual(second["gif"]["url"], remote_result["url"])
            self.assertIn(remote_result["url"], platform.calls[0]["body"])
            self.assertIn(remote_result["url"], platform.calls[1]["body"])
            uploaded = publisher.uploaded_gif_for(
                "54478914", "goal-19", "ocr_window"
            )
            self.assertEqual(uploaded["gif_id"], gif_id)

    def test_upload_gif_persists_and_associates_with_event(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda url: None,
            )
            result = publisher.upload_gif(
                body=animated_gif_bytes(),
                match_id="54478914",
                event_key="goal-19",
                artifact_kind="ocr_window",
            )
            uploaded = publisher.uploaded_gif_for(
                "54478914", "goal-19", "ocr_window"
            )

            self.assertEqual(result["gif"]["gif_id"], uploaded["gif_id"])
            self.assertTrue(Path(uploaded["path"]).is_file())
            self.assertTrue(uploaded["url"].startswith("https://"))

    def test_upload_gif_rejects_unknown_artifact_kind(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
            )
            with self.assertRaisesRegex(ArticlePublishError, "不支持"):
                publisher.upload_gif(
                    body=animated_gif_bytes(),
                    match_id="54478914",
                    event_key="goal-19",
                    artifact_kind="scoreboard",
                )

    def test_gif_validation_requires_animation(self):
        info = inspect_animated_gif(animated_gif_bytes())
        self.assertEqual(info["frame_count"], 2)
        with self.assertRaisesRegex(ArticlePublishError, "动画"):
            body = animated_gif_bytes().replace(
                animated_gif_bytes()[34:49], b"", 1
            )
            inspect_animated_gif(body)

    def test_fields_use_confirmed_gif_article_defaults(self):
        fields = build_article_fields(
            match_id="54478914",
            event={
                "code": "PG",
                "minute": "45",
                "minute_extra": "2",
                "score": "1:0",
            },
            gif_url="https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            title="自动标题",
        )
        self.assertEqual(fields["archive_level"], "B")
        self.assertEqual(fields["status"], 1)
        self.assertEqual(fields["add_to_tab"], 1)
        self.assertEqual(fields["type"], "article")
        self.assertEqual(fields["style"], "gif")
        self.assertEqual(fields["match_event"], 2)
        self.assertEqual(fields["match_time"], "45+2")
        self.assertEqual(fields["match_score"], "1-0")
        self.assertNotIn("user_id", fields)
        self.assertIn('src="https://matchgif.aisportsapp.com/', fields["body"])

    def test_draft_fields_disable_publish_and_include_archive_id(self):
        fields = build_article_fields(
            match_id="54478914",
            event={"code": "G", "minute": "19", "score": "1-0"},
            gif_url="https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            title="进球草稿",
            delivery_mode="draft",
            archive_id="3801234",
        )

        self.assertEqual(fields["status"], 0)
        self.assertEqual(fields["add_to_tab"], 1)
        self.assertEqual(fields["archive_id"], 3801234)

    def test_draft_fields_reject_invalid_delivery_mode_and_archive_id(self):
        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "title": "进球草稿",
        }
        with self.assertRaisesRegex(ArticlePublishError, "publish 或 draft"):
            build_article_fields(**common, delivery_mode="preview")
        with self.assertRaisesRegex(ArticlePublishError, "文章 ID"):
            build_article_fields(
                **common,
                delivery_mode="draft",
                archive_id="not-an-id",
            )

    def test_yellow_card_does_not_claim_red_card_event_type(self):
        fields = build_article_fields(
            match_id="54478914",
            event={"code": "YC", "minute": "20"},
            gif_url="https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            title="黄牌事件",
        )
        self.assertNotIn("match_event", fields)

    def test_formal_publish_rejects_http_gif_url(self):
        with self.assertRaisesRegex(ArticlePublishError, "HTTPS"):
            build_article_fields(
                match_id="54478914",
                event={"code": "G", "minute": "20"},
                gif_url="http://matchgif.aisportsapp.com/publish-gifs/a.gif",
                title="进球",
            )

    def test_title_is_generated_from_event_and_match(self):
        title = build_article_title(
            {"code": "G", "minute": "19", "person": "球员甲", "score": "2-1"},
            {"team_A_name": "主队", "team_B_name": "客队"},
        )
        self.assertEqual(title, "19分钟，球员甲进球，主队 2-1 客队")

    def test_publish_is_idempotent_and_persists_success(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "default.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published",
                    "https://matchgif.aisportsapp.com",
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda url: None,
            )
            event = {
                "event_key": "goal-19",
                "status": "encoded",
                "code": "G",
                "minute": "19",
                "person": "球员甲",
                "score": "1-0",
            }
            detail = {"team_A_name": "主队", "team_B_name": "客队"}

            first = publisher.publish(
                match_id="54478914",
                event=event,
                match_detail=detail,
                source_path=source,
            )
            second = publisher.publish(
                match_id="54478914",
                event=event,
                match_detail=detail,
                source_path=source,
            )

            self.assertEqual(first["status"], "success")
            self.assertEqual(first["article_id"], "3801234")
            self.assertTrue(second["idempotent_replay"])
            self.assertEqual(len(platform.calls), 1)
            self.assertEqual(platform.calls[0]["status"], 1)
            self.assertEqual(platform.calls[0]["add_to_tab"], 1)
            self.assertNotIn("archive_id", platform.calls[0])
            self.assertEqual(
                publisher.records_for_match("54478914")["goal-19"]["article_id"],
                "3801234",
            )
            self.assertTrue(Path(first["gif_url"].split("/")[-1]).suffix == ".gif")

    def test_public_url_failure_stops_before_platform_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "default.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()

            def fail(_url):
                raise ArticlePublishError(
                    "公网不可访问",
                    code="publish_gif_public_unreachable",
                    stage="public_url_check",
                    status_code=503,
                )

            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published",
                    "https://matchgif.aisportsapp.com",
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=fail,
            )
            with self.assertRaisesRegex(ArticlePublishError, "公网不可访问"):
                publisher.publish(
                    match_id="54478914",
                    event={
                        "event_key": "goal-20",
                        "status": "encoded",
                        "code": "G",
                        "minute": "20",
                    },
                    match_detail={},
                    source_path=source,
                )
            self.assertEqual(platform.calls, [])
            record = publisher.records_for_match("54478914")["goal-20"]
            self.assertEqual(record["status"], "failed")
            self.assertEqual(record["stage"], "public_url_check")

    def test_create_and_update_draft_without_publish_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            checked_urls = []
            database_path = root / "publish.sqlite3"
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published",
                    "https://matchgif.aisportsapp.com",
                ),
                database_path=database_path,
                public_url_checker=checked_urls.append,
            )
            event = {
                "event_key": "goal-19",
                "code": "G",
                "minute": "19",
                "person": "球员甲",
                "score": "1-0",
            }
            detail = {"team_A_name": "主队", "team_B_name": "客队"}

            created = publisher.create_or_update_draft(
                match_id="54478914",
                event=event,
                match_detail=detail,
                source_path=source,
            )
            updated = publisher.create_or_update_draft(
                match_id="54478914",
                event=event,
                match_detail=detail,
                source_path=source,
                archive_id=created["article_id"],
            )

            self.assertEqual(created["article_id"], "3801234")
            self.assertFalse(created["updated"])
            self.assertTrue(updated["updated"])
            self.assertEqual(created["gif"]["url"], checked_urls[0])
            self.assertEqual(len(platform.calls), 2)
            self.assertEqual(platform.calls[0]["status"], 0)
            self.assertEqual(platform.calls[0]["add_to_tab"], 1)
            self.assertNotIn("archive_id", platform.calls[0])
            self.assertEqual(platform.calls[1]["archive_id"], 3801234)
            self.assertFalse(database_path.exists())

    def test_draft_preserves_open_platform_error_classification(self):
        class FailingPlatformClient(FakePlatformClient):
            def create_article(self, fields):
                raise OpenPlatformError(
                    "授权服务暂时不可用",
                    code=50001,
                    status_code=503,
                    auth_required=True,
                    retriable=True,
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            publisher = ArticlePublisher(
                platform_client=FailingPlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published",
                    "https://matchgif.aisportsapp.com",
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
            )

            with self.assertRaises(ArticlePublishError) as caught:
                publisher.create_or_update_draft(
                    match_id="54478914",
                    event={"event_key": "goal-19", "code": "G", "minute": "19"},
                    match_detail={},
                    source_path=source,
                )

            self.assertEqual(caught.exception.stage, "authorization")
            self.assertTrue(caught.exception.auth_required)
            self.assertTrue(caught.exception.retriable)
            self.assertEqual(caught.exception.platform_code, 50001)
            self.assertEqual(caught.exception.diagnostics["gif_bytes"], len(animated_gif_bytes()))


if __name__ == "__main__":
    unittest.main()
