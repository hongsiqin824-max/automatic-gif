import json
import tempfile
import unittest
from pathlib import Path

from article_publisher import (
    ArticlePublishError,
    ArticlePublisher,
    PublishedGifStore,
    build_article_fields,
    build_article_title,
    inspect_animated_gif,
)


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
        self.assertEqual(fields["add_to_tab"], 1)
        self.assertEqual(fields["type"], "article")
        self.assertEqual(fields["style"], "gif")
        self.assertEqual(fields["match_event"], 2)
        self.assertEqual(fields["match_time"], "45+2")
        self.assertEqual(fields["match_score"], "1-0")
        self.assertNotIn("user_id", fields)
        self.assertIn('src="https://matchgif.aisportsapp.com/', fields["body"])

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


if __name__ == "__main__":
    unittest.main()
