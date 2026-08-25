import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from article_draft_queue import ArticleDraftQueue, draft_admin_url
from article_publisher import ArticlePublisher, ArticlePublishError, PublishedGifStore
from open_platform_client import OpenPlatformError


def animated_gif_bytes(color: bytes = b"\xff\xff\xff") -> bytes:
    header = (
        b"GIF89a"
        b"\x01\x00\x01\x00"
        b"\x80\x00\x00"
        b"\x00\x00\x00" + color
    )
    frame = (
        b"\x2c"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00"
        b"\x02\x02\x44\x01\x00"
    )
    return header + frame + frame + b"\x3b"


class SequencePlatformClient:
    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.calls = []

    def status(self):
        return {"configured": True, "authorized": True}

    def create_article(self, fields):
        self.calls.append(dict(fields))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return {"article_id": "6230049", "duplicate": False, "code": 0}


class ArticleDraftQueueTests(unittest.TestCase):
    def make_queue(self, root, platform, **queue_options):
        publisher = ArticlePublisher(
            platform_client=platform,
            gif_store=PublishedGifStore(
                root / "published",
                "https://matchgif.aisportsapp.com",
            ),
            database_path=root / "delivery.sqlite3",
            public_url_checker=lambda _url: None,
        )
        queue = ArticleDraftQueue(
            database_path=root / "delivery.sqlite3",
            publisher=publisher,
            allowed_output_root=root / "output",
            start_worker=False,
            **queue_options,
        )
        return queue

    def write_source(self, root, body=None):
        match_dir = root / "output" / "54478914"
        match_dir.mkdir(parents=True, exist_ok=True)
        source = match_dir / "goal_ocr.gif"
        source.write_bytes(body or animated_gif_bytes())
        return source

    def event(self):
        return {
            "event_key": "54478914:G:goal-19",
            "status": "encoded",
            "code": "G",
            "minute": "19",
            "person": "球员甲",
            "score": "1-0",
        }

    def test_create_is_idempotent_and_exposes_admin_link(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)

            queued = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                source_path=source,
                artifact_result={"visual_resolution": "ocr_second_exact"},
            )
            self.assertEqual(queued["status"], "queued")
            self.assertTrue(queue.run_once())

            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "success")
            self.assertEqual(record["quality_label"], "精确到秒")
            self.assertEqual(record["article_id"], "6230049")
            self.assertEqual(record["draft_url"], draft_admin_url("6230049"))
            self.assertEqual(platform.calls[0]["status"], 0)
            self.assertEqual(platform.calls[0]["add_to_tab"], 1)

            replay = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertEqual(replay["status"], "success")
            self.assertFalse(queue.run_once())
            self.assertEqual(len(platform.calls), 1)

    def test_unchanged_historical_gif_reuses_staged_copy_without_reading_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            first = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            with patch.object(queue, "_stage_file", wraps=queue._stage_file) as stage:
                replay = queue.enqueue(
                    match_id="54478914",
                    event=self.event(),
                    match_detail={"team_A_name": "主队"},
                    source_path=source,
                )

            stage.assert_not_called()
            self.assertEqual(replay["task_key"], first["task_key"])
            self.assertEqual(replay["status"], "queued")

    def test_changed_ocr_gif_updates_existing_draft(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            stat = source.stat()
            os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
            changed = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
                artifact_result={"output_kind": "api_time_range_fallback"},
            )

            self.assertEqual(changed["status"], "queued")
            self.assertEqual(changed["article_id"], "6230049")
            queue.run_once()
            self.assertEqual(len(platform.calls), 2)
            self.assertEqual(platform.calls[1]["archive_id"], 6230049)
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "success")
            self.assertEqual(record["quality_label"], "残缺范围片段")

    def test_temporary_failure_waits_then_retries(self):
        temporary_error = OpenPlatformError(
            "平台暂时繁忙",
            code=50001,
            status_code=503,
            retriable=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient([temporary_error])
            queue = self.make_queue(root, platform, retry_delays_seconds=(1, 2, 3, 4))
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            self.assertTrue(queue.run_once(now=100.0))
            waiting = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(waiting["status"], "retry_wait")
            self.assertTrue(waiting["retriable"])
            self.assertEqual(waiting["next_attempt_at_unix"], 101.0)
            self.assertFalse(queue.run_once(now=100.5))
            self.assertTrue(queue.run_once(now=101.0))
            completed = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(completed["status"], "success")
            self.assertEqual(len(platform.calls), 2)

    def test_expired_creating_lease_recovers_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            queued = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.execute(
                    "UPDATE article_delivery_tasks SET status='creating', "
                    "lease_until_unix=50 WHERE task_key=?",
                    (queued["task_key"],),
                )

            reopened = self.make_queue(root, platform)
            self.assertTrue(reopened.run_once(now=51.0))
            record = reopened.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "success")

    def test_existing_queue_schema_adds_generation_and_lease_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "delivery.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE article_delivery_tasks (
                        task_key TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        next_attempt_at_unix REAL,
                        lease_until_unix REAL
                    )
                    """
                )
            queue = ArticleDraftQueue(database_path=database)

            queue.status()
            with sqlite3.connect(database) as connection:
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(article_delivery_tasks)"
                    )
                }
            self.assertTrue(
                {"generation", "lease_token", "previous_staged_path"}.issubset(columns)
            )

    def test_missing_or_outside_source_fails_with_plain_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            missing = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=root / "output" / "54478914" / "missing.gif",
            )
            self.assertEqual(missing["status"], "failed")
            self.assertIn("OCR GIF 文件不存在", missing["error"])

            outside = root / "outside.gif"
            outside.write_bytes(animated_gif_bytes())
            with self.assertRaisesRegex(ValueError, "不属于这场比赛"):
                queue.enqueue(
                    match_id="54478914",
                    event={**self.event(), "event_key": "outside"},
                    match_detail={},
                    source_path=outside,
                )

    def test_staged_gif_survives_source_cleanup_while_waiting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            source.unlink()
            self.assertTrue(queue.run_once())
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "success")
            self.assertEqual(len(platform.calls), 1)

    def test_late_old_generation_result_cannot_overwrite_new_ocr_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            old_claim = dict(queue._claim_due(100.0))

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            changed = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertEqual(changed["status"], "queued")

            queue._record_success(
                old_claim,
                {
                    "article_id": "7000001",
                    "gif": {},
                    "updated": False,
                    "duplicate": False,
                },
                now=101.0,
            )
            waiting = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(waiting["status"], "queued")
            self.assertEqual(waiting["article_id"], "7000001")

            self.assertTrue(queue.run_once(now=102.0))
            self.assertEqual(platform.calls[0]["archive_id"], 7000001)
            completed = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(completed["status"], "success")

    def test_expired_lease_old_token_cannot_change_current_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient(), lease_seconds=10)
            queued = queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            old_claim = dict(queue._claim_due(100.0))
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.execute(
                    "UPDATE article_delivery_tasks SET lease_until_unix=101 WHERE task_key=?",
                    (queued["task_key"],),
                )
            current_claim = dict(queue._claim_due(102.0))
            self.assertNotEqual(old_claim["lease_token"], current_claim["lease_token"])

            queue._record_failure(
                old_claim,
                ArticlePublishError(
                    "旧请求失败",
                    code="old_request_failed",
                    stage="platform_publish",
                ),
                now=103.0,
            )
            queue._record_success(
                old_claim,
                {
                    "article_id": "7000002",
                    "gif": {},
                    "updated": False,
                    "duplicate": False,
                },
                now=103.0,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    "SELECT * FROM article_delivery_tasks WHERE task_key=?",
                    (queued["task_key"],),
                ).fetchone()
            self.assertEqual(row["status"], "creating")
            self.assertEqual(row["lease_token"], current_claim["lease_token"])
            self.assertEqual(row["article_id"], "7000002")

    def test_updated_draft_removes_unreferenced_previous_staged_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                previous = Path(
                    connection.execute(
                        "SELECT staged_path FROM article_delivery_tasks"
                    ).fetchone()[0]
                )
            self.assertTrue(previous.exists())

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            self.assertFalse(previous.exists())

    def test_rapid_ocr_revisions_remove_the_unreferenced_oldest_staged_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                first_path = Path(
                    connection.execute(
                        "SELECT staged_path FROM article_delivery_tasks"
                    ).fetchone()[0]
                )

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertTrue(first_path.exists())

            source.write_bytes(animated_gif_bytes(b"\x00\xff\x00"))
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertFalse(first_path.exists())

    def test_previous_staged_gif_is_kept_when_formal_publish_references_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            queue.publisher.publish(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                previous = Path(
                    connection.execute(
                        "SELECT gif_path FROM article_publish_records"
                    ).fetchone()[0]
                )

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            queue.enqueue(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            self.assertTrue(previous.exists())


if __name__ == "__main__":
    unittest.main()
