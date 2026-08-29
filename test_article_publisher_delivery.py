import tempfile
import unittest
from pathlib import Path

from article_publisher import ArticlePublishError, ArticlePublisher, PublishedGifStore


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
        self.calls.append(dict(fields))
        return {
            "article_id": str(fields.get("archive_id") or "3801234"),
            "duplicate": False,
            "code": 0,
            "message": "ok",
        }


class ArticlePublisherDeliveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        root = Path(self.temporary_directory.name)
        self.source = root / "ocr.gif"
        self.source.write_bytes(animated_gif_bytes())
        self.platform = FakePlatformClient()
        self.publisher = ArticlePublisher(
            platform_client=self.platform,
            gif_store=PublishedGifStore(
                root / "published",
                "https://matchgif.aisportsapp.com",
            ),
            database_path=root / "publish.sqlite3",
            public_url_checker=lambda _url: None,
        )
        self.match_detail = {"team_A_name": "主队", "team_B_name": "客队"}

    def test_existing_draft_can_be_rebuilt_and_published_with_same_archive_id(self):
        draft_event = {
            "event_key": "goal-19",
            "code": "G",
            "minute": "19",
            "team": "A",
            "score": "1-0",
        }
        created = self.publisher.create_or_update_draft(
            match_id="54478914",
            event=draft_event,
            match_detail=self.match_detail,
            source_path=self.source,
        )

        final_event = {**draft_event, "person": "球员甲"}
        published = self.publisher.publish_draft(
            match_id="54478914",
            event=final_event,
            match_detail=self.match_detail,
            source_path=self.source,
            archive_id=created["article_id"],
        )

        self.assertEqual(len(self.platform.calls), 2)
        self.assertEqual(self.platform.calls[0]["status"], 0)
        self.assertNotIn("archive_id", self.platform.calls[0])
        self.assertEqual(self.platform.calls[1]["status"], 1)
        self.assertEqual(self.platform.calls[1]["archive_id"], 3801234)
        self.assertIn("球员甲进球", self.platform.calls[1]["title"])
        self.assertIn("球员甲进球", self.platform.calls[1]["body"])
        self.assertEqual(published["article_id"], created["article_id"])
        self.assertEqual(published["delivery_mode"], "publish")
        self.assertTrue(published["updated"])

    def test_new_article_can_be_created_as_published(self):
        result = self.publisher.create_or_update_article(
            match_id="54478914",
            event={
                "event_key": "goal-20",
                "code": "G",
                "minute": "20",
                "person": "球员乙",
                "score": "2-0",
            },
            match_detail=self.match_detail,
            source_path=self.source,
            delivery_mode="publish",
        )

        self.assertEqual(self.platform.calls[0]["status"], 1)
        self.assertNotIn("archive_id", self.platform.calls[0])
        self.assertEqual(result["delivery_mode"], "publish")
        self.assertFalse(result["updated"])

    def test_updating_draft_with_new_gif_updates_litpic(self):
        event = {
            "event_key": "goal-22",
            "code": "G",
            "minute": "22",
            "score": "1-0",
        }
        created = self.publisher.create_or_update_draft(
            match_id="54478914",
            event=event,
            match_detail=self.match_detail,
            source_path=self.source,
        )
        second_source = self.source.with_name("ocr-updated.gif")
        second_source.write_bytes(
            animated_gif_bytes().replace(b"\xff\xff\xff", b"\xff\x00\x00", 1)
        )

        updated = self.publisher.create_or_update_draft(
            match_id="54478914",
            event=event,
            match_detail=self.match_detail,
            source_path=second_source,
            archive_id=created["article_id"],
        )

        first_fields, second_fields = self.platform.calls
        self.assertEqual(second_fields["archive_id"], 3801234)
        self.assertNotEqual(first_fields["litpic"], second_fields["litpic"])
        self.assertEqual(second_fields["litpic"], updated["gif"]["cover_url"])
        self.assertNotIn(second_fields["litpic"], second_fields["body"])

    def test_publish_draft_requires_archive_id(self):
        with self.assertRaises(ArticlePublishError) as caught:
            self.publisher.publish_draft(
                match_id="54478914",
                event={"event_key": "goal-19", "code": "G", "minute": "19"},
                match_detail=self.match_detail,
                source_path=self.source,
                archive_id=None,
            )

        self.assertEqual(caught.exception.code, "publish_archive_id_missing")
        self.assertEqual(self.platform.calls, [])


if __name__ == "__main__":
    unittest.main()
