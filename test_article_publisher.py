import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from article_publisher import (
    ArticlePublishError,
    ArticlePublisher,
    PublishedGifStore,
    RemoteGifUploadClient,
    TeamChannelResolver,
    _check_public_cover_url,
    _official_gif_litpic,
    article_channels_source,
    article_litpic_source,
    build_article_fields,
    build_article_title,
    inspect_animated_gif,
    pick_team_channel,
    pick_team_tabs,
    team_tag_row_id,
)
from open_platform_client import ImageUploadResults, OpenPlatformError
from publish_account_pool import PublishAccountPool


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
        self.search_calls = []
        self.search_rows = {}

    def status(self):
        return {"configured": True, "authorized": True}

    def create_article(self, fields):
        self.calls.append(fields)
        return {"article_id": "3801234", "duplicate": False, "code": 0}

    def search_channels(self, keyword):
        self.search_calls.append(keyword)
        return list(self.search_rows.get(keyword, []))


class FakeOfficialImageClient:
    def __init__(self):
        self.calls = []

    def status(self):
        return {"configured": True, "authorized": True, "api_name": "image-uploadimage-ai"}

    def upload_images(self, gif_path, cover_path, *, max_bytes):
        self.calls.append((Path(gif_path), Path(cover_path), max_bytes))
        return ImageUploadResults(
            [
                {
                    "image_id": "gif-image",
                    "mime": "image/gif",
                    "url": "/fastdfs8/M00/gif-event.gif",
                    "img1_url": "https://img1.qunliao.info/fastdfs8/M00/gif-event.gif",
                    "file_name": "event.gif",
                    "width": 440,
                    "height": 248,
                },
                {
                    "image_id": "cover-image",
                    "mime": "image/jpeg",
                    "url": "/fastdfs8/M00/cover-event.jpg",
                    "img1_url": "https://img1.qunliao.info/fastdfs8/M00/cover-event.jpg",
                    "file_name": "event.jpg",
                },
            ],
            diagnostics={
                "lifecycle": [
                    {"event": "upload_started"},
                    {"event": "upload_succeeded"},
                ],
            },
        )


class FailingOfficialImageClient(FakeOfficialImageClient):
    def upload_images(self, gif_path, cover_path, *, max_bytes):
        raise OpenPlatformError(
            "Not login",
            code=300002,
            status_code=401,
            auth_required=True,
            diagnostics={
                "http_status": 200,
                "platform_code": 300002,
                "lifecycle": [
                    {"event": "upload_started", "at_ms": 0.1},
                    {"event": "token_acquired", "at_ms": 4.2},
                    {
                        "event": "platform_response_received",
                        "http_status": 200,
                        "platform_code": 300002,
                    },
                ],
            },
        )


class ArticlePublisherTests(unittest.TestCase):
    def test_official_upload_skips_public_head_probe_but_self_hosted_still_probes(self):
        # The HEAD probe guards our own nginx. A GIF already on the platform's
        # FastDFS was confirmed by the upload response; probing its CDN URL
        # right after upload only adds latency and spurious retriable failures.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            checked_urls = []
            official = FakeOfficialImageClient()
            publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                verify_public_url=True,
                public_url_checker=checked_urls.append,
                ocr_image_upload_client=official,
            )
            event = {"event_key": "goal-19", "code": "G", "minute": "19"}

            publisher.create_or_update_article(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                delivery_mode="publish",
            )
            # Second call reuses the persisted mapping (upload_source=official).
            publisher.create_or_update_article(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                delivery_mode="publish",
            )

            self.assertEqual(len(official.calls), 1)
            self.assertEqual(checked_urls, [])

            # Without the official client the self-hosted URL is still probed.
            local_publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published2", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish2.sqlite3",
                verify_public_url=True,
                public_url_checker=checked_urls.append,
            )
            local_publisher.create_or_update_article(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                delivery_mode="publish",
            )
            # GIF and its JPEG cover, both served from our own origin.
            self.assertEqual(len(checked_urls), 2)
            self.assertTrue(all(url.startswith("https://matchgif.aisportsapp.com/") for url in checked_urls))

    def test_ocr_uses_official_image_urls_and_reuses_upload_for_archive_update(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            official = FakeOfficialImageClient()
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                verify_public_url=False,
                ocr_image_upload_client=official,
            )
            event = {"event_key": "goal-19", "code": "G", "minute": "19"}

            created = publisher.create_or_update_draft(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
            )
            updated = publisher.create_or_update_draft(
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                archive_id=created["article_id"],
            )

            self.assertEqual(len(official.calls), 1)
            self.assertEqual(official.calls[0][2], 20 * 1024 * 1024)
            self.assertEqual(
                created["gif"]["url"],
                "https://img1.qunliao.info/fastdfs8/M00/gif-event.gif",
            )
            self.assertEqual(created["gif"]["cover_url"], "/fastdfs8/M00/cover-event.jpg")
            # The App opens the large image only from a bare FastDFS path plus
            # the upload's pixel size.
            self.assertEqual(
                platform.calls[0]["body"],
                '<p><img src="/fastdfs8/M00/gif-event.gif" data-width="440" data-height="248" /></p>',
            )
            # The reused mapping keeps no width/height: the path is derived from
            # img1_url and the size is read from the local GIF header (1x1).
            self.assertEqual(
                platform.calls[1]["body"],
                '<p><img src="/fastdfs8/M00/gif-event.gif" data-width="1" data-height="1" /></p>',
            )
            # The GIF's own FastDFS path is the cover; the second call reuses
            # the persisted mapping, which only keeps img1_url, so the path
            # must be derived from it.
            self.assertEqual(platform.calls[0]["litpic"], "/fastdfs8/M00/gif-event.gif")
            self.assertEqual(platform.calls[1]["litpic"], "/fastdfs8/M00/gif-event.gif")
            self.assertEqual(platform.calls[1]["archive_id"], 3801234)
            self.assertEqual(updated["gif"]["url"], created["gif"]["url"])
            self.assertEqual(
                updated["diagnostics"]["image_upload"]["lifecycle"][-1]["event"],
                "upload_succeeded",
            )
            uploaded = publisher.uploaded_gif_for(
                "54478914", "goal-19", "ocr_window"
            )
            self.assertEqual(
                uploaded["upload_diagnostics"]["lifecycle"][-1]["event"],
                "upload_succeeded",
            )

    def test_ocr_upload_failure_keeps_official_lifecycle_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
                ocr_image_upload_client=FailingOfficialImageClient(),
            )
            with self.assertRaises(ArticlePublishError) as context:
                publisher.create_or_update_draft(
                    match_id="54478914",
                    event={"event_key": "goal-19", "code": "G", "minute": "19"},
                    match_detail={},
                    source_path=source,
                )
            self.assertEqual(context.exception.code, "official_image_upload_failed")
            self.assertEqual(context.exception.platform_code, 300002)
            self.assertEqual(
                context.exception.diagnostics["lifecycle"][-1]["event"],
                "platform_response_received",
            )

    def test_ocr_upload_requires_complete_img1_urls(self):
        class MissingPublicUrlClient(FakeOfficialImageClient):
            def upload_images(self, gif_path, cover_path, *, max_bytes):
                return ImageUploadResults(
                    [
                        {
                            "image_id": "gif-image",
                            "mime": "image/gif",
                            "url": "/fastdfs8/M00/gif-event.gif",
                        },
                        {
                            "image_id": "cover-image",
                            "mime": "image/jpeg",
                            "url": "/fastdfs8/M00/cover-event.jpg",
                        },
                    ]
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                verify_public_url=False,
                ocr_image_upload_client=MissingPublicUrlClient(),
            )
            with self.assertRaises(ArticlePublishError) as context:
                publisher.create_or_update_draft(
                    match_id="54478914",
                    event={"event_key": "goal-19", "code": "G", "minute": "19"},
                    match_detail={},
                    source_path=source,
                )
            self.assertEqual(context.exception.code, "official_image_upload_invalid_response")
            self.assertIn("img1_url", str(context.exception))

    def test_self_hosted_does_not_reuse_official_upload_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            remote = RemoteGifUploadClient("https://upload.example/api", "secret")
            publisher = ArticlePublisher(
                platform_client=FakePlatformClient(),
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                verify_public_url=False,
                remote_upload_client=remote,
            )
            local = publisher.gif_store.create(source)
            publisher._save_uploaded_mapping(
                match_id="54478914",
                event_key="goal-19",
                artifact_kind="ocr_window",
                gif={
                    **local,
                    "url": "https://official.example/gif.gif",
                    "cover_url": "https://official.example/cover.jpg",
                    "upload_source": "official",
                },
            )
            remote_result = {
                **local,
                "url": "https://matchgif.aisportsapp.com/publish-gifs/event.gif",
                "cover_url": "https://matchgif.aisportsapp.com/publish-gif-covers/event.jpg",
            }
            with patch.object(remote, "upload", return_value=remote_result) as upload:
                result = publisher._prepare_public_gif(
                    source_path=source,
                    match_id="54478914",
                    event_key="goal-19",
                    artifact_kind="ocr_window",
                )

            upload.assert_called_once()
            self.assertEqual(result["url"], remote_result["url"])

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
                        "cover_url": f"https://matchgif.aisportsapp.com/publish-gif-covers/{expected_id}.jpg",
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
        self.assertTrue(result["cover_url"].endswith(f"/{expected_id}.jpg"))

    def test_remote_upload_requires_explicit_https_jpeg_cover_url(self):
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
            expected_id = __import__("hashlib").sha256(animated_gif_bytes()).hexdigest()
            client = RemoteGifUploadClient("https://upload.example/api", "secret")
            with patch("article_publisher.urllib.request.urlopen", return_value=Response()):
                with self.assertRaises(ArticlePublishError) as context:
                    client.upload(
                        source_path=source,
                        match_id="54478914",
                        event_key="goal-19",
                        artifact_kind="ocr_window",
                        max_bytes=1024,
                    )
        self.assertEqual(context.exception.code, "remote_gif_upload_invalid_response")

    def test_remote_upload_rejects_non_https_or_non_jpeg_cover_url(self):
        class Response:
            status = 200

            def __init__(self, cover_url):
                self.cover_url = cover_url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps({
                    "ok": True,
                    "gif": {
                        "gif_id": expected_id,
                        "url": f"https://cdn.example/publish-gifs/{expected_id}.gif",
                        "cover_url": self.cover_url,
                    },
                }).encode()

        invalid_urls = (
            "http://cdn.example/covers/a.jpg",
            "https://cdn.example/covers/a.png",
            "https://cdn.example/covers/a.gif",
            "https://cdn.example/covers/wrong-cover.jpg",
        )
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "goal.gif"
            source.write_bytes(animated_gif_bytes())
            expected_id = __import__("hashlib").sha256(animated_gif_bytes()).hexdigest()
            client = RemoteGifUploadClient("https://upload.example/api", "secret")
            for cover_url in invalid_urls:
                with self.subTest(cover_url=cover_url), patch(
                    "article_publisher.urllib.request.urlopen",
                    return_value=Response(cover_url),
                ):
                    with self.assertRaises(ArticlePublishError) as context:
                        client.upload(
                            source_path=source,
                            match_id="54478914",
                            event_key="goal-19",
                            artifact_kind="ocr_window",
                            max_bytes=1024,
                        )
                    self.assertEqual(
                        context.exception.code,
                        "remote_gif_upload_invalid_response",
                    )

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
                "cover_url": f"https://cdn.example/covers/{gif_id}.jpg",
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
                "cover_url": f"https://cdn.example/covers/{gif_id}.jpg",
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
            self.assertEqual(first["gif"]["cover_url"], remote_result["cover_url"])
            self.assertEqual(second["gif"]["cover_url"], remote_result["cover_url"])
            self.assertIn(remote_result["url"], platform.calls[0]["body"])
            self.assertIn(remote_result["url"], platform.calls[1]["body"])
            self.assertNotIn(remote_result["cover_url"], platform.calls[0]["body"])
            self.assertEqual(platform.calls[0]["litpic"], remote_result["cover_url"])
            self.assertEqual(platform.calls[1]["litpic"], remote_result["cover_url"])
            uploaded = publisher.uploaded_gif_for(
                "54478914", "goal-19", "ocr_window"
            )
            self.assertEqual(uploaded["gif_id"], gif_id)
            self.assertEqual(uploaded["cover_url"], remote_result["cover_url"])

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

    def test_litpic_source_cover_keeps_first_frame_jpeg(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                verify_public_url=False,
                ocr_image_upload_client=FakeOfficialImageClient(),
            )
            with patch.dict(os.environ, {"ARTICLE_LITPIC_SOURCE": "cover"}):
                publisher.create_or_update_draft(
                    match_id="54478914",
                    event={"event_key": "goal-19", "code": "G", "minute": "19"},
                    match_detail={},
                    source_path=source,
                )
            self.assertEqual(platform.calls[0]["litpic"], "/fastdfs8/M00/cover-event.jpg")

    def test_litpic_source_rejects_unknown_value(self):
        with patch.dict(os.environ, {"ARTICLE_LITPIC_SOURCE": "png"}):
            with self.assertRaisesRegex(RuntimeError, "ARTICLE_LITPIC_SOURCE"):
                article_litpic_source()
        with patch.dict(os.environ, {"ARTICLE_LITPIC_SOURCE": " GIF "}):
            self.assertEqual(article_litpic_source(), "gif")
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(article_litpic_source(), "gif")

    def test_official_gif_litpic_prefers_direct_path_then_derives_from_url(self):
        self.assertEqual(
            _official_gif_litpic(
                {
                    "official_gif_path": "/fastdfs8/M00/a.gif",
                    "url": "https://img1.qunliao.info/fastdfs8/M00/other.gif",
                }
            ),
            "/fastdfs8/M00/a.gif",
        )
        # A mapping reused from the database only carries img1_url.
        self.assertEqual(
            _official_gif_litpic(
                {"url": "https://img1.qunliao.info/fastdfs8/M00/b.gif?x=1"}
            ),
            "/fastdfs8/M00/b.gif",
        )
        self.assertEqual(_official_gif_litpic({"url": "/relative-only.gif"}), "")
        self.assertEqual(_official_gif_litpic({"url": "https://host/"}), "")
        self.assertEqual(_official_gif_litpic({}), "")

    def test_fields_keep_jpeg_cover_when_litpic_is_not_official(self):
        base = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19"},
            "title": "自动标题",
        }
        # Legacy HTTPS uploader: GIF path is never used as cover.
        fields = build_article_fields(
            **base,
            gif_url="https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            cover_url="https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            litpic_url="/publish-gifs/a.gif",
        )
        self.assertEqual(
            fields["litpic"],
            "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
        )
        # Official path without a usable GIF path falls back to the JPEG.
        fields = build_article_fields(
            **base,
            gif_url="https://img1.qunliao.info/fastdfs8/M00/a.gif",
            cover_url="/fastdfs8/M00/a.jpg",
            litpic_url="",
            allow_relative_media=True,
        )
        self.assertEqual(fields["litpic"], "/fastdfs8/M00/a.jpg")
        # Official path with the GIF path uses it.
        fields = build_article_fields(
            **base,
            gif_url="https://img1.qunliao.info/fastdfs8/M00/a.gif",
            cover_url="/fastdfs8/M00/a.jpg",
            litpic_url="/fastdfs8/M00/a.gif",
            allow_relative_media=True,
        )
        self.assertEqual(fields["litpic"], "/fastdfs8/M00/a.gif")

    def test_article_gif_src_and_size_fallbacks(self):
        import article_publisher

        with tempfile.TemporaryDirectory() as directory:
            gif_path = Path(directory) / "a.gif"
            gif_path.write_bytes(animated_gif_bytes())
            official = {
                "url": "https://img1.qunliao.info/fastdfs8/M00/a.gif",
                "official_upload": True,
                "official_gif_path": "/fastdfs8/M00/a.gif",
                "path": str(gif_path),
            }
            # Official upload: bare FastDFS path and the upload's size.
            self.assertEqual(
                article_publisher._article_gif_src(official, True), "/fastdfs8/M00/a.gif"
            )
            self.assertEqual(
                article_publisher._gif_size(
                    {**official, "official_gif_width": "440", "official_gif_height": 248}
                ),
                (440, 248),
            )
            # Missing or invalid upload size: read the GIF header instead.
            self.assertEqual(article_publisher._gif_size(official), (1, 1))
            self.assertEqual(
                article_publisher._gif_size(
                    {**official, "official_gif_width": 0, "official_gif_height": "x"}
                ),
                (1, 1),
            )
            # Self-hosted / legacy uploads keep their full HTTPS URL.
            legacy = {"url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif", "path": str(gif_path)}
            self.assertEqual(article_publisher._article_gif_src(legacy, False), legacy["url"])
            # No readable GIF: no size, the body simply omits data-width/height.
            self.assertIsNone(article_publisher._gif_size({"path": str(Path(directory) / "gone.gif")}))
            not_gif = Path(directory) / "x.gif"
            not_gif.write_bytes(b"not a gif at all")
            self.assertIsNone(article_publisher._gif_size({"path": str(not_gif)}))
            self.assertIsNone(article_publisher._gif_size({}))

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
            cover_url="https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            title="自动标题",
        )
        self.assertEqual(fields["archive_level"], "B")
        self.assertEqual(fields["status"], 1)
        self.assertEqual(fields["add_to_tab"], 0)
        self.assertEqual(fields["channelsnew"], "12039035")
        self.assertEqual(fields["type"], "article")
        self.assertEqual(fields["style"], "gif")
        self.assertEqual(fields["match_event"], 2)
        self.assertEqual(fields["match_time"], "45+2")
        self.assertEqual(fields["match_score"], "1-0")
        self.assertNotIn("user_id", fields)
        self.assertIn('src="https://matchgif.aisportsapp.com/', fields["body"])
        self.assertEqual(
            fields["litpic"],
            "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
        )
        self.assertNotIn(fields["litpic"], fields["body"])

    def test_draft_fields_disable_publish_and_include_archive_id(self):
        fields = build_article_fields(
            match_id="54478914",
            event={"code": "G", "minute": "19", "score": "1-0"},
            gif_url="https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            cover_url="https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            title="进球草稿",
            delivery_mode="draft",
            archive_id="3801234",
        )

        self.assertEqual(fields["status"], 0)
        self.assertEqual(fields["add_to_tab"], 0)
        self.assertEqual(fields["archive_id"], 3801234)

    def test_add_to_tab_and_channelsnew_follow_environment(self):
        import os

        import article_publisher

        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19", "score": "1-0"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "cover_url": "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            "title": "t",
        }
        # Values are read per call so a .env loaded after import still applies.
        with patch.dict(os.environ, {"ARTICLE_ADD_TO_TAB": "1", "ARTICLE_CHANNELSNEW": "1,22"}):
            fields = build_article_fields(**common)
        self.assertEqual(fields["add_to_tab"], 1)
        self.assertEqual(fields["channelsnew"], "1,22")
        # Empty channelsnew is omitted from the request, not sent as "".
        with patch.dict(os.environ, {"ARTICLE_CHANNELSNEW": ""}):
            fields = build_article_fields(**common)
        self.assertNotIn("channelsnew", fields)
        with patch.dict(os.environ, {"ARTICLE_ADD_TO_TAB": "2"}):
            with self.assertRaises(RuntimeError):
                article_publisher.article_add_to_tab()
        with patch.dict(os.environ, {"ARTICLE_CHANNELSNEW": "abc"}):
            with self.assertRaises(RuntimeError):
                article_publisher.article_channelsnew()

    def test_tabs_follow_environment_as_array_field(self):
        import os
        import urllib.parse

        import article_publisher

        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19", "score": "1-0"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "cover_url": "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            "title": "t",
        }
        # Default (unset / empty) sends no tabs at all.
        with patch.dict(os.environ, {"ARTICLE_TABS": ""}):
            fields = build_article_fields(**common)
        self.assertNotIn("tabs[]", fields)
        self.assertNotIn("tabs", fields)
        with patch.dict(os.environ, {"ARTICLE_TABS": " 1, 284 "}):
            fields = build_article_fields(**common)
        self.assertEqual(fields["tabs[]"], ["1", "284"])
        # The platform wants PHP-style ``tabs[]=1&tabs[]=284``; the client
        # encodes with doseq=True so each id becomes its own pair.
        body = urllib.parse.urlencode(fields, doseq=True)
        self.assertIn("tabs%5B%5D=1&tabs%5B%5D=284", body)
        with patch.dict(os.environ, {"ARTICLE_TABS": "1,x"}):
            with self.assertRaises(RuntimeError):
                article_publisher.article_tabs()
        # Per-team columns from playerteam follow the fixed ones, de-duplicated.
        with patch.dict(os.environ, {"ARTICLE_TABS": "1,284"}):
            fields = build_article_fields(**common, tabs=["57", 284, "253", "57"])
        self.assertEqual(fields["tabs[]"], ["1", "284", "57", "253"])
        with patch.dict(os.environ, {"ARTICLE_TABS": ""}):
            fields = build_article_fields(**common, tabs=["57"])
        self.assertEqual(fields["tabs[]"], ["57"])

    def test_body_footer_follows_competition_and_environment(self):
        import os

        import article_publisher

        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19", "score": "1-0"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "cover_url": "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            "title": "t",
        }
        gif_p = '<p><img src="https://matchgif.aisportsapp.com/publish-gifs/a.gif" /></p>'
        # Several angles: one <p><img> per GIF, main first, footer after the last.
        # Each GIF carries its own size; a missing size omits data-width/height.
        fields = build_article_fields(
            **common,
            extra_gif_urls=["https://matchgif.aisportsapp.com/publish-gifs/b.gif", "/fastdfs/c.gif"],
            allow_relative_media=True,
            body_footer="上咪咕独家看英超",
            gif_sizes=[(440, 248), None, (360, 202)],
        )
        self.assertEqual(
            fields["body"],
            '<p><img src="https://matchgif.aisportsapp.com/publish-gifs/a.gif"'
            ' data-width="440" data-height="248" /></p>'
            + '<p><img src="https://matchgif.aisportsapp.com/publish-gifs/b.gif" /></p>'
            + '<p><img src="/fastdfs/c.gif" data-width="360" data-height="202" /></p>'
            + "<p>上咪咕独家看英超</p>",
        )
        self.assertEqual(fields["litpic"], common["cover_url"])  # cover stays the main GIF's
        with self.assertRaises(ArticlePublishError) as ctx:
            build_article_fields(**common, extra_gif_urls=["http://insecure/b.gif"])
        self.assertEqual(ctx.exception.code, "publish_gif_https_required")
        # Default mapping: the three 咪咕 competitions, exact name match.
        self.assertEqual(
            article_publisher.article_body_footer({"competition_name": "英超"}),
            "上咪咕独家看英超",
        )
        self.assertEqual(
            article_publisher.article_body_footer({"competition_name": "西甲"}),
            "上咪咕独家看西甲",
        )
        self.assertEqual(
            article_publisher.article_body_footer({"competition_name": "中超"}),
            "独家看中超就在咪咕",
        )
        for detail in ({"competition_name": "中甲"}, {"competition_name": ""}, {}, None):
            self.assertEqual(article_publisher.article_body_footer(detail), "")
        # Footer lands as its own paragraph right after the GIF; none = GIF only.
        fields = build_article_fields(**common, body_footer="上咪咕独家看英超")
        self.assertEqual(fields["body"], gif_p + "<p>上咪咕独家看英超</p>")
        fields = build_article_fields(**common)
        self.assertEqual(fields["body"], gif_p)
        fields = build_article_fields(**common, body_footer="  a<b  ")
        self.assertEqual(fields["body"], gif_p + "<p>a&lt;b</p>")
        # Environment overrides the mapping; empty disables all; bad = none.
        with patch.dict(os.environ, {"ARTICLE_BODY_FOOTERS": " 中甲 = 看中甲 ; 英超=X "}):
            self.assertEqual(
                article_publisher.article_body_footers(), {"中甲": "看中甲", "英超": "X"}
            )
            self.assertEqual(
                article_publisher.article_body_footer({"competition_name": "中甲"}), "看中甲"
            )
            self.assertEqual(
                article_publisher.article_body_footer({"competition_name": "西甲"}), ""
            )
        with patch.dict(os.environ, {"ARTICLE_BODY_FOOTERS": ""}):
            self.assertEqual(article_publisher.article_body_footers(), {})
            self.assertEqual(
                article_publisher.article_body_footer({"competition_name": "英超"}), ""
            )
        with patch.dict(os.environ, {"ARTICLE_BODY_FOOTERS": "英超"}):
            with self.assertRaises(RuntimeError):
                article_publisher.article_body_footers()
            self.assertEqual(
                article_publisher.article_body_footer({"competition_name": "英超"}), ""
            )
        with patch.dict(os.environ, {"ARTICLE_BODY_FOOTERS": "英超="}):
            with self.assertRaises(RuntimeError):
                article_publisher.article_body_footers()

    def test_channels_field_validated_and_omitted_when_empty(self):
        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "cover_url": "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            "title": "t",
        }
        self.assertNotIn("channels", build_article_fields(**common))
        self.assertNotIn("channels", build_article_fields(**common, channels="  "))
        fields = build_article_fields(**common, channels="14284455,15498503")
        self.assertEqual(fields["channels"], "14284455,15498503")
        with self.assertRaisesRegex(ArticlePublishError, "channels"):
            build_article_fields(**common, channels="14284455,abc")

    def test_channels_source_env_accepts_teams_or_off(self):
        import os

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("ARTICLE_CHANNELS_SOURCE", None)
            self.assertEqual(article_channels_source(), "teams")
        with patch.dict(os.environ, {"ARTICLE_CHANNELS_SOURCE": "OFF"}):
            self.assertEqual(article_channels_source(), "off")
        with patch.dict(os.environ, {"ARTICLE_CHANNELS_SOURCE": "match"}):
            with self.assertRaises(RuntimeError):
                article_channels_source()

    def test_ocr_draft_sends_both_team_channels_from_match_detail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "ocr.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            platform.search_rows = {
                "上海海港B队": [
                    {"id": 125618, "value": 14284455, "name": "上海海港B队", "type": "球队"},
                ],
                "杭州临平吴越": [
                    {"id": 9, "value": 9, "name": "临平", "type": "球员"},
                    {"id": 129034, "value": 15498503, "name": "杭州临平吴越", "type": "球队"},
                ],
            }
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                verify_public_url=False,
                ocr_image_upload_client=FakeOfficialImageClient(),
            )
            # Shape of monitor_server._match_detail_for / dashboard session.detail.
            detail = {
                "team_A_id": "50125618",
                "team_A_name": "上海海港B队",
                "team_B_id": "50129034",
                "team_B_name": "杭州临平吴越",
            }
            event = {"event_key": "goal-19", "code": "G", "minute": "19"}
            publisher.create_or_update_draft(
                match_id="54591935", event=event, match_detail=detail, source_path=source
            )
            publisher.create_or_update_draft(
                match_id="54591935",
                event={"event_key": "goal-40", "code": "G", "minute": "40"},
                match_detail=detail,
                source_path=source,
            )
            self.assertEqual(platform.calls[0]["channels"], "14284455,15498503")
            self.assertEqual(platform.calls[1]["channels"], "14284455,15498503")
            # Second article of the same match hits the cache: one search per team.
            self.assertEqual(platform.search_calls, ["上海海港B队", "杭州临平吴越"])
            self.assertEqual(platform.calls[0]["channelsnew"], "12039035")
            # A bad switch value still publishes (without channels) but is
            # surfaced loudly through status(), like the other .env fields.
            with patch.dict(os.environ, {"ARTICLE_CHANNELS_SOURCE": "bogus"}):
                publisher.create_or_update_draft(
                    match_id="54591935",
                    event={"event_key": "goal-77", "code": "G", "minute": "77"},
                    match_detail=detail,
                    source_path=source,
                )
                self.assertNotIn("channels", platform.calls[2])
                with self.assertRaises(RuntimeError):
                    publisher.status()

    def test_draft_fields_reject_invalid_delivery_mode_and_archive_id(self):
        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "19"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "cover_url": "https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
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
            cover_url="https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
            title="黄牌事件",
        )
        self.assertNotIn("match_event", fields)

    def test_formal_publish_rejects_http_gif_url(self):
        with self.assertRaisesRegex(ArticlePublishError, "HTTPS"):
            build_article_fields(
                match_id="54478914",
                event={"code": "G", "minute": "20"},
                gif_url="http://matchgif.aisportsapp.com/publish-gifs/a.gif",
                cover_url="https://matchgif.aisportsapp.com/publish-gif-covers/a.jpg",
                title="进球",
            )

    def test_article_fields_reject_invalid_cover_urls(self):
        common = {
            "match_id": "54478914",
            "event": {"code": "G", "minute": "20"},
            "gif_url": "https://matchgif.aisportsapp.com/publish-gifs/a.gif",
            "title": "进球",
        }
        for cover_url in (
            "",
            "http://cdn.example/covers/a.jpg",
            "https://cdn.example/covers/a.png",
            "https://cdn.example/covers/a.gif",
        ):
            with self.subTest(cover_url=cover_url):
                with self.assertRaises(ArticlePublishError) as context:
                    build_article_fields(**common, cover_url=cover_url)
                self.assertEqual(context.exception.code, "publish_cover_url_invalid")

    def test_title_is_generated_from_event_and_match(self):
        title = build_article_title(
            {"code": "G", "minute": "19", "person": "球员甲", "score": "2-1"},
            {"team_A_name": "主队", "team_B_name": "客队"},
        )
        self.assertEqual(title, "19分钟，球员甲进球，主队 2-1 客队")

    def test_title_uses_team_for_placeholder_person(self):
        title = build_article_title(
            {
                "code": "YC",
                "minute": "30",
                "person": "未提供球员",
                "team": "A",
            },
            {"team_A_name": "主队", "team_B_name": "客队"},
        )
        self.assertEqual(title, "30分钟，主队黄牌")
        self.assertNotIn("未提供球员", title)

    def test_goal_event_titles_include_score_but_card_titles_omit_matchup(self):
        detail = {"team_A_name": "主队", "team_B_name": "客队"}
        for code, action in (("G", "进球"), ("PG", "点球破门"), ("OG", "乌龙球")):
            with self.subTest(code=code):
                title = build_article_title(
                    {
                        "code": code,
                        "minute": "45",
                        "person": "球员甲",
                        "score": "2:1",
                    },
                    detail,
                )
                self.assertEqual(title, f"45分钟，球员甲{action}，主队 2-1 客队")

        for code, action in (("YC", "黄牌"), ("RC", "红牌")):
            with self.subTest(code=code):
                title = build_article_title(
                    {
                        "code": code,
                        "minute": "71",
                        "person": "球员乙",
                        "score": "2:1",
                    },
                    detail,
                )
                self.assertEqual(title, f"71分钟，球员乙{action}")

    def test_match_account_is_shared_by_default_ocr_and_all_events(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "event.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()
            pool = PublishAccountPool(
                root / "publish_accounts.json",
                initial_accounts=[
                    {"user_id": 1001, "user_name": "账号甲", "enabled": True},
                    {"user_id": 1002, "user_name": "账号乙", "enabled": True},
                ],
            )
            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published", "https://matchgif.aisportsapp.com"
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
                account_pool=pool,
            )
            detail = {"team_A_name": "主队", "team_B_name": "客队"}
            publisher.publish(
                match_id="54478914",
                event={
                    "event_key": "goal-default",
                    "status": "encoded",
                    "code": "G",
                    "minute": "19",
                    "score": "1-0",
                },
                match_detail=detail,
                source_path=source,
            )
            publisher.create_or_update_article(
                match_id="54478914",
                event={
                    "event_key": "card-30",
                    "code": "YC",
                    "minute": "30",
                },
                match_detail=detail,
                source_path=source,
                delivery_mode="publish",
            )
            publisher.create_or_update_draft(
                match_id="54478914",
                event={
                    "event_key": "goal-draft",
                    "code": "OG",
                    "minute": "45",
                    "score": "2-0",
                },
                match_detail=detail,
                source_path=source,
            )

            assigned_ids = [call["user_id"] for call in platform.calls]
            assigned_names = [call["user_name"] for call in platform.calls]
            self.assertEqual(len(set(assigned_ids)), 1)
            self.assertEqual(len(set(assigned_names)), 1)
            with sqlite3.connect(root / "publish.sqlite3") as connection:
                keys = [
                    row[0]
                    for row in connection.execute(
                        "SELECT assignment_key FROM article_publish_account_assignments"
                    )
                ]
            self.assertEqual(keys, ["match:54478914"])

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
            self.assertEqual(first["cover_url"], platform.calls[0]["litpic"])
            self.assertTrue(second["idempotent_replay"])
            self.assertEqual(len(platform.calls), 1)
            self.assertEqual(platform.calls[0]["status"], 1)
            self.assertEqual(platform.calls[0]["add_to_tab"], 0)
            self.assertEqual(platform.calls[0]["channelsnew"], "12039035")
            self.assertNotIn("archive_id", platform.calls[0])
            # No competition in detail: GIF-only body.
            self.assertTrue(platform.calls[0]["body"].endswith("></p>"))
            self.assertNotIn("咪咕", platform.calls[0]["body"])

            third = publisher.publish(
                match_id="54478915",
                event=event,
                match_detail={**detail, "competition_name": "英超"},
                source_path=source,
            )
            self.assertEqual(third["status"], "success")
            self.assertTrue(
                platform.calls[1]["body"].endswith("></p><p>上咪咕独家看英超</p>")
            )
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

    def test_cover_url_failure_stops_before_platform_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "default.gif"
            source.write_bytes(animated_gif_bytes())
            platform = FakePlatformClient()

            def fail_cover(_url):
                raise ArticlePublishError(
                    "公网封面不可访问",
                    code="publish_cover_public_unreachable",
                    stage="cover_public_url_check",
                    status_code=503,
                )

            publisher = ArticlePublisher(
                platform_client=platform,
                gif_store=PublishedGifStore(
                    root / "published",
                    "https://matchgif.aisportsapp.com",
                ),
                database_path=root / "publish.sqlite3",
                public_url_checker=lambda _url: None,
                public_cover_url_checker=fail_cover,
            )
            with self.assertRaises(ArticlePublishError) as context:
                publisher.publish(
                    match_id="54478914",
                    event={
                        "event_key": "goal-21",
                        "status": "encoded",
                        "code": "G",
                        "minute": "21",
                    },
                    match_detail={},
                    source_path=source,
                )

            self.assertEqual(context.exception.code, "publish_cover_public_unreachable")
            self.assertEqual(platform.calls, [])
            record = publisher.records_for_match("54478914")["goal-21"]
            self.assertEqual(record["stage"], "cover_public_url_check")

    def test_default_cover_checker_requires_jpeg_content_type(self):
        class Response:
            status = 200
            headers = {"Content-Type": "image/png"}

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with patch(
            "article_publisher.urllib.request.urlopen",
            return_value=Response(),
        ):
            with self.assertRaises(ArticlePublishError) as context:
                _check_public_cover_url("https://cdn.example/covers/a.jpg")
        self.assertEqual(context.exception.code, "publish_cover_public_invalid")
        self.assertEqual(context.exception.stage, "cover_public_url_check")

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
            self.assertEqual(platform.calls[0]["add_to_tab"], 0)
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


class TeamChannelResolverTests(unittest.TestCase):
    # Real shape from admin-archive-channelsearch for 根特: a player first,
    # then the club, then a reserve side.
    ROWS = [
        {"id": 51240, "value": 51240, "name": "特尔施特根", "type": "球员"},
        {"id": 138, "value": 1652, "name": "根特", "type": "球队", "status": 0},
        {"id": 9289077, "value": 9289077, "name": "根特预备队", "type": "球队"},
    ]
    # Real shape for 胡志明市女足: two 球队 rows with the identical name.
    DUP_ROWS = [
        {"id": 14473, "value": 10728022, "name": "胡志明市女足", "type": "球队"},
        {"id": 71037, "value": 10732023, "name": "胡志明市女足二队", "type": "球队"},
        {"id": 37537, "value": 10753811, "name": "胡志明市女足", "type": "球队"},
    ]

    def test_team_tag_row_id_requires_directory_offset(self):
        self.assertEqual(team_tag_row_id("50014473"), 14473)
        self.assertEqual(team_tag_row_id(50000138), 138)
        self.assertIsNone(team_tag_row_id(None))
        self.assertIsNone(team_tag_row_id(""))
        self.assertIsNone(team_tag_row_id("abc"))
        self.assertIsNone(team_tag_row_id(138))  # already a row id, not a team id
        self.assertIsNone(team_tag_row_id(50000000))
        self.assertIsNone(team_tag_row_id(True))

    def test_pick_prefers_id_and_tolerates_name_drift(self):
        self.assertEqual(pick_team_channel(self.ROWS, "根特", "50000138"), (1652, "id"))
        # Directory name differs from the CMS label: id still resolves it.
        self.assertEqual(pick_team_channel(self.ROWS, "根特队", 50000138), (1652, "id"))
        # id never matches a non-球队 row.
        self.assertIsNone(pick_team_channel(self.ROWS, "特尔施特根", 50051240))

    def test_pick_with_duplicates_uses_id_or_gives_up(self):
        self.assertEqual(
            pick_team_channel(self.DUP_ROWS, "胡志明市女足", "50014473"), (10728022, "id")
        )
        self.assertEqual(
            pick_team_channel(self.DUP_ROWS, "胡志明市女足", "50037537"), (10753811, "id")
        )
        # No id, or an id that is not in the results: two same-name rows are
        # ambiguous, so neither is sent.
        self.assertIsNone(pick_team_channel(self.DUP_ROWS, "胡志明市女足"))
        self.assertIsNone(pick_team_channel(self.DUP_ROWS, "胡志明市女足", "50099999"))
        # The unique name still resolves.
        self.assertEqual(
            pick_team_channel(self.DUP_ROWS, "胡志明市女足二队"), (10732023, "name")
        )

    def test_pick_name_fallback_requires_exact_unique_team_row(self):
        self.assertEqual(pick_team_channel(self.ROWS, "根特"), (1652, "name"))
        self.assertEqual(pick_team_channel(self.ROWS, "根特", "bad-id"), (1652, "name"))
        self.assertEqual(pick_team_channel(self.ROWS, "根特", "50099999"), (1652, "name"))
        self.assertIsNone(pick_team_channel(self.ROWS, "根特U19"))
        self.assertIsNone(pick_team_channel(self.ROWS, "特尔施特根"))
        self.assertIsNone(pick_team_channel([], "根特"))
        self.assertIsNone(pick_team_channel(self.ROWS, ""))
        self.assertEqual(
            pick_team_channel(
                [{"id": "138", "value": "1652", "name": "根特", "type": "球队"}], "根特", "50000138"
            ),
            (1652, "id"),
        )
        self.assertIsNone(
            pick_team_channel([{"id": 1, "value": True, "name": "根特", "type": "球队"}], "根特")
        )

    def test_resolver_caches_by_team_id_then_name_and_never_caches_misses(self):
        clock = {"now": 100.0}
        search = Mock(return_value=self.ROWS)
        resolver = TeamChannelResolver(
            search, ttl_seconds=60, negative_ttl_seconds=0, clock=lambda: clock["now"]
        )
        self.assertEqual(resolver.resolve("根特", "50000138"), 1652)
        # Same team id, different display name: cache hit, no new search.
        self.assertEqual(resolver.resolve("根特队", "50000138"), 1652)
        self.assertEqual(search.call_count, 1)
        # No id is a separate cache entry keyed by name.
        self.assertEqual(resolver.resolve("根特"), 1652)
        self.assertEqual(search.call_count, 2)
        self.assertEqual(resolver.resolve("根特"), 1652)
        self.assertEqual(search.call_count, 2)
        clock["now"] = 161.0
        self.assertEqual(resolver.resolve("根特", "50000138"), 1652)
        self.assertEqual(search.call_count, 3)
        # negative_ttl=0: misses are not remembered.
        self.assertIsNone(resolver.resolve("斯图拉德", "50137800"))
        self.assertIsNone(resolver.resolve("斯图拉德", "50137800"))
        self.assertEqual(search.call_count, 5)

    def test_resolver_negative_cache_limits_retries_during_outage(self):
        clock = {"now": 1000.0}
        calls = {"n": 0}

        def failing(_keyword):
            calls["n"] += 1
            raise OpenPlatformError("无法连接开放平台", retriable=True)

        resolver = TeamChannelResolver(
            failing, negative_ttl_seconds=120, clock=lambda: clock["now"]
        )
        detail = {
            "team_A_id": "50014473",
            "team_A_name": "胡志明市女足",
            "team_B_id": "50014474",
            "team_B_name": "丰富府里女足",
        }
        # First article: one failed search per team.
        self.assertEqual(resolver.channels_for(detail), "")
        self.assertEqual(calls["n"], 2)
        # Next articles inside the window: no searches at all.
        clock["now"] = 1060.0
        self.assertEqual(resolver.channels_for(detail), "")
        self.assertEqual(resolver.channels_for(detail), "")
        self.assertEqual(calls["n"], 2)
        # Window over: retried once per team.
        clock["now"] = 1121.0
        self.assertEqual(resolver.channels_for(detail), "")
        self.assertEqual(calls["n"], 4)

        # not_found is also negatively cached, and a positive hit after the
        # window replaces it with the long TTL.
        rows = {"n": 0}

        def search(_keyword):
            rows["n"] += 1
            return [] if rows["n"] == 1 else self.ROWS

        clock["now"] = 2000.0
        resolver = TeamChannelResolver(
            search, ttl_seconds=3600, negative_ttl_seconds=120, clock=lambda: clock["now"]
        )
        self.assertIsNone(resolver.resolve("根特", "50000138"))
        self.assertIsNone(resolver.resolve("根特", "50000138"))
        self.assertEqual(rows["n"], 1)
        clock["now"] = 2121.0
        self.assertEqual(resolver.resolve("根特", "50000138"), 1652)
        clock["now"] = 2121.0 + 3000
        self.assertEqual(resolver.resolve("根特", "50000138"), 1652)
        self.assertEqual(rows["n"], 2)

    def test_resolver_swallows_search_errors_and_skips_off_switch(self):
        def failing(_keyword):
            raise OpenPlatformError("服务异常", code=5)

        resolver = TeamChannelResolver(failing)
        detail = {"team_A_id": "50000138", "team_A_name": "根特", "team_B_name": "波鸿"}
        self.assertEqual(resolver.channels_for(detail), "")
        self.assertIsNone(resolver.resolve("根特", "50000138"))

        search = Mock(return_value=self.ROWS)
        resolver = TeamChannelResolver(search)
        self.assertEqual(
            resolver.channels_for({"team_A_name": "根特", "team_B_name": "根特"}), "1652"
        )
        self.assertEqual(resolver.channels_for({"team_B_name": "根特"}), "1652")
        self.assertEqual(resolver.channels_for({}), "")
        with patch.dict(os.environ, {"ARTICLE_CHANNELS_SOURCE": "off"}):
            self.assertEqual(resolver.channels_for({"team_A_name": "根特"}), "")
        # A misspelt switch degrades to "no tags" instead of failing the article.
        search.reset_mock()
        with patch.dict(os.environ, {"ARTICLE_CHANNELS_SOURCE": "bogus"}):
            self.assertEqual(
                resolver.channels_for({"team_A_id": "50000138", "team_A_name": "根特"}), ""
            )
        search.assert_not_called()
        # Non-list payloads (e.g. a bare Mock client) are treated as no rows.
        resolver = TeamChannelResolver(Mock(return_value=Mock()))
        self.assertIsNone(resolver.resolve("根特"))

    def test_channels_for_passes_each_side_id_to_the_search(self):
        rows_by_keyword = {
            "胡志明市女足": self.DUP_ROWS,
            "丰富府里女足": [
                {"id": 14474, "value": 10728023, "name": "丰富府里女足", "type": "球队"},
                {"id": 41888, "value": 10758183, "name": "丰富府里女足", "type": "球队"},
            ],
        }
        resolver = TeamChannelResolver(lambda keyword: rows_by_keyword[keyword])
        detail = {
            "team_A_id": "50014473",
            "team_A_name": "胡志明市女足",
            "team_B_id": "50014474",
            "team_B_name": "丰富府里女足",
        }
        self.assertEqual(resolver.channels_for(detail), "10728022,10728023")
        # Without ids both teams are ambiguous and nothing is sent.
        bare = TeamChannelResolver(lambda keyword: rows_by_keyword[keyword])
        self.assertEqual(
            bare.channels_for({"team_A_name": "胡志明市女足", "team_B_name": "丰富府里女足"}),
            "",
        )

    def test_pick_team_tabs_prefers_new_table_id_then_table_id(self):
        self.assertEqual(
            pick_team_tabs({"table_id": 57, "new_table_id": [57, 253, "253", 0, "x"]}),
            [57, 253],
        )
        self.assertEqual(pick_team_tabs({"table_id": "57", "new_table_id": []}), [57])
        self.assertEqual(pick_team_tabs({"table_id": None}), [])
        self.assertEqual(pick_team_tabs({}), [])
        self.assertEqual(pick_team_tabs([]), [])
        self.assertEqual(pick_team_tabs(None), [])

    def test_tabs_for_resolves_each_team_once_and_merges_columns(self):
        rows_by_keyword = {
            "根特": self.ROWS,
            "丰富府里女足": [
                {"id": 14474, "value": 10728023, "name": "丰富府里女足", "type": "球队"},
            ],
        }
        tabs_by_channel = {
            1652: {"table_id": 57, "table_name": "五洲", "new_table_id": [57, 253]},
            10728023: {"table_id": 60, "new_table_id": [60, 253]},
        }
        search = Mock(side_effect=lambda keyword: rows_by_keyword[keyword])
        player_team = Mock(side_effect=lambda value: tabs_by_channel[value])
        resolver = TeamChannelResolver(search, player_team)
        detail = {
            "team_A_id": "50000138",
            "team_A_name": "根特",
            "team_B_id": "50014474",
            "team_B_name": "丰富府里女足",
        }
        self.assertEqual(resolver.channels_for(detail), "1652,10728023")
        self.assertEqual(resolver.tabs_for(detail), ["57", "253", "60"])
        # channelsearch is shared with channels_for via the cache; playerteam
        # once per team, then cached for the next article.
        self.assertEqual(search.call_count, 2)
        self.assertEqual(player_team.call_count, 2)
        self.assertEqual(resolver.tabs_for(detail), ["57", "253", "60"])
        self.assertEqual(search.call_count, 2)
        self.assertEqual(player_team.call_count, 2)
        # Unresolved team contributes nothing; no playerteam call for it.
        self.assertEqual(
            resolver.tabs_for({"team_A_id": "50000138", "team_A_name": "根特", "team_B_name": "波鸿"}),
            ["57", "253"],
        )
        self.assertEqual(player_team.call_count, 2)
        with patch.dict(os.environ, {"ARTICLE_TABS_SOURCE": "off"}):
            self.assertEqual(resolver.tabs_for(detail), [])
        with patch.dict(os.environ, {"ARTICLE_TABS_SOURCE": "bogus"}):
            self.assertEqual(resolver.tabs_for(detail), [])
        # tabs source stays independent of the channels switch.
        with patch.dict(os.environ, {"ARTICLE_CHANNELS_SOURCE": "off"}):
            self.assertEqual(resolver.tabs_for(detail), ["57", "253", "60"])

    def test_tabs_for_swallows_lookup_errors_and_negative_caches(self):
        clock = {"now": 100.0}
        calls = {"n": 0}

        def failing(_value):
            calls["n"] += 1
            raise OpenPlatformError("服务异常", code=5, retriable=True)

        resolver = TeamChannelResolver(
            Mock(return_value=self.ROWS),
            failing,
            negative_ttl_seconds=120,
            clock=lambda: clock["now"],
        )
        detail = {"team_A_id": "50000138", "team_A_name": "根特"}
        self.assertEqual(resolver.tabs_for(detail), [])
        self.assertEqual(resolver.tabs_for(detail), [])
        self.assertEqual(calls["n"], 1)
        clock["now"] = 221.0
        self.assertEqual(resolver.tabs_for(detail), [])
        self.assertEqual(calls["n"], 2)
        # No playerteam callable (older client): silently no columns.
        plain = TeamChannelResolver(Mock(return_value=self.ROWS))
        self.assertEqual(plain.tabs_for(detail), [])
        # Platform "no match" ([] -> {}) and non-dict payloads: no columns.
        empty = TeamChannelResolver(Mock(return_value=self.ROWS), Mock(return_value={}))
        self.assertEqual(empty.tabs_for(detail), [])
        weird = TeamChannelResolver(Mock(return_value=self.ROWS), Mock(return_value=Mock()))
        self.assertEqual(weird.tabs_for(detail), [])

    def test_monitor_catalog_snapshot_keeps_team_ids_for_channel_lookup(self):
        """8901 builds match_detail from match_catalog; without the ids in the
        card the resolver would silently fall back to name matching."""
        import match_catalog

        raw = {
            "match_id": "54476694",
            "cmp_type": "soccer",
            "status": "Playing",
            "sort_timestamp": 1_700_000_000,
            "start_play": "2026-10-23 16:00:00",
            "team_A_id": "50014473",
            "team_A_name": "胡志明市女足",
            "team_B_id": "50014474",
            "team_B_name": "丰富府里女足",
            "large_unused_payload": {"must_not_reach_browser": True},
        }
        catalog = match_catalog.MatchCatalog()
        with patch.object(match_catalog, "request_json", return_value={"list": [raw]}), patch.object(
            match_catalog, "match_user", return_value="u"
        ):
            playing = catalog.snapshot()["playing"]
        self.assertEqual(playing[0]["team_A_id"], "50014473")
        self.assertEqual(playing[0]["team_B_id"], "50014474")
        self.assertNotIn("large_unused_payload", playing[0])


if __name__ == "__main__":
    unittest.main()
