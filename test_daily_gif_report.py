import json
import sqlite3
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from daily_gif_report import (
    _load_delivery_rows,
    collect_report,
    completed_report_window,
    format_report,
    run_report,
)


class DailyGifReportTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.output_root = self.root / "output_gifs" / "dashboard"
        self.output_root.mkdir(parents=True)
        self.article_db = self.root / "data" / "article_publish.sqlite3"
        self.article_db.parent.mkdir(parents=True)

    def tearDown(self):
        self.temp_dir.cleanup()

    def _create_state(self, match_id="12345"):
        match_root = self.output_root / match_id
        match_root.mkdir()
        database = match_root / "pipeline_state.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.executescript(
                """
                CREATE TABLE event_tasks (
                    event_key TEXT PRIMARY KEY, match_id TEXT, code TEXT,
                    event_type TEXT, event_json TEXT, status TEXT,
                    detected_at_unix REAL, discovered_at_unix REAL,
                    suppressed_by_event_key TEXT, error TEXT,
                    last_error_kind TEXT
                );
                CREATE TABLE vision_tasks (
                    event_key TEXT, artifact_kind TEXT, status TEXT,
                    failure_reason TEXT, error TEXT, last_error_kind TEXT
                );
                """
            )
            connection.executemany(
                "INSERT INTO event_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        f"{match_id}:G:1", match_id, "G", "goal",
                        json.dumps({"minute": 12, "person": "甲"}), "encoded",
                        100, 100, None, None, None,
                    ),
                    (
                        f"{match_id}:YC:2", match_id, "YC", "yellow_card",
                        json.dumps({"minute": 25}), "encoded",
                        100, 100, None, None, None,
                    ),
                    (
                        f"{match_id}:G:suppressed", match_id, "G", "goal",
                        json.dumps({"minute": 12}), "failed",
                        100, 100, f"{match_id}:G:1", "stale", "stale",
                    ),
                ],
            )
            connection.executemany(
                "INSERT INTO vision_tasks VALUES (?,?,?,?,?,?)",
                [
                    (f"{match_id}:G:1", "ocr_window", "encoded", None, None, None),
                    (f"{match_id}:YC:2", "ocr_window", "failed", "OCR超时", "timeout", "ocr_timeout"),
                ],
            )
        return database

    def _create_delivery_schema(self):
        with sqlite3.connect(self.article_db) as connection:
            connection.execute(
                """
                CREATE TABLE article_delivery_tasks (
                    task_key TEXT PRIMARY KEY, match_id TEXT, event_key TEXT,
                    artifact_kind TEXT, delivery_mode TEXT, status TEXT,
                    event_json TEXT, match_detail_json TEXT, updated_at_unix REAL,
                    created_at_unix REAL, published_at_unix REAL,
                    publish_reason TEXT, error_code TEXT, error TEXT
                )
                """
            )

    def test_window_uses_previous_completed_19_boundary(self):
        now = datetime(2026, 9, 8, 18, 59, tzinfo=ZoneInfo("Asia/Shanghai"))
        window = completed_report_window(now)
        self.assertEqual(window.start_local.isoformat(), "2026-09-06T19:00:00+08:00")
        self.assertEqual(window.end_local.isoformat(), "2026-09-07T19:00:00+08:00")
        now = datetime(2026, 9, 8, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        window = completed_report_window(now)
        self.assertEqual(window.start_local.isoformat(), "2026-09-07T19:00:00+08:00")
        self.assertEqual(window.end_local.isoformat(), "2026-09-08T19:00:00+08:00")

    def test_collects_distinct_events_and_separates_ocr_failure(self):
        self._create_state()
        self._create_delivery_schema()
        with sqlite3.connect(self.article_db) as connection:
            connection.execute(
                "INSERT INTO article_delivery_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "ocr-task", "12345", "12345:G:1", "ocr_window", "draft",
                    "published", "{}", json.dumps({"team_A_name": "主队", "team_B_name": "客队"}),
                    200, 100, 210, "person_available", None, None,
                ),
            )
            # This default-GIF row must never be used for the OCR report.
            connection.execute(
                "INSERT INTO article_delivery_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "default-task", "12345", "12345:YC:2", "default", "draft",
                    "published", "{}", "{}", 200, 100, 210, None, None, None,
                ),
            )
        start = completed_report_window(
            datetime.fromtimestamp(200, ZoneInfo("Asia/Shanghai"))
        )
        window = type(start)(0, 200, start.timezone_name, start.start_local, start.end_local)
        reports = collect_report(self.output_root, self.article_db, window)
        self.assertEqual(len(reports), 1)
        summary = reports[0]
        self.assertEqual(summary.expected, 2)
        self.assertEqual(summary.counts()["published"], 1)
        self.assertEqual(summary.counts()["ocr_failed"], 1)
        self.assertEqual(summary.team_a, "主队")
        self.assertIn("OCR失败", format_report(reports, window))

    def test_report_delivery_is_idempotent(self):
        self._create_state()
        self._create_delivery_schema()
        window = completed_report_window(
            datetime(2026, 9, 8, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        with patch("daily_gif_report.send_feishu") as send:
            text, skipped = run_report(
                output_root=self.output_root,
                article_database=self.article_db,
                webhook_url="https://example.invalid/hook",
                window=window,
            )
            self.assertFalse(skipped)
            self.assertTrue(text)
            self.assertEqual(send.call_count, 1)
            _, skipped = run_report(
                output_root=self.output_root,
                article_database=self.article_db,
                webhook_url="https://example.invalid/hook",
                window=window,
            )
            self.assertTrue(skipped)
            self.assertEqual(send.call_count, 1)

    def test_recent_in_progress_delivery_is_not_started_twice(self):
        self._create_state()
        self._create_delivery_schema()
        window = completed_report_window(
            datetime(2026, 9, 8, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        with patch("daily_gif_report.send_feishu") as send:
            with sqlite3.connect(self.article_db) as connection:
                connection.execute(
                    "CREATE TABLE daily_report_deliveries ("
                    "report_key TEXT PRIMARY KEY, window_start_unix REAL NOT NULL, "
                    "window_end_unix REAL NOT NULL, status TEXT NOT NULL, "
                    "payload_sha256 TEXT NOT NULL, sent_at_unix REAL, "
                    "updated_at_unix REAL NOT NULL, error TEXT)"
                )
                key = f"{window.start_unix:.6f}:{window.end_unix:.6f}"
                connection.execute(
                    "INSERT INTO daily_report_deliveries VALUES (?,?,?,?,?,?,?,?)",
                    (key, window.start_unix, window.end_unix, "sending", "x", None, __import__("time").time(), None),
                )
            _, skipped = run_report(
                output_root=self.output_root,
                article_database=self.article_db,
                webhook_url="https://example.invalid/hook",
                window=window,
            )
            self.assertTrue(skipped)
            send.assert_not_called()

    def test_feishu_status_code_is_checked(self):
        from daily_gif_report import send_feishu

        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return b'{"StatusCode": 1, "StatusMessage": "bad"}'

        with patch("daily_gif_report.urllib.request.urlopen", return_value=Response()):
            with self.assertRaises(RuntimeError):
                send_feishu("https://example.invalid/hook", "test")

    def test_report_keeps_totals_when_many_matches_make_reason_details_large(self):
        window = completed_report_window(
            datetime(2026, 9, 8, 19, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
        )
        from daily_gif_report import EventOutcome, MatchSummary

        summaries = []
        for index in range(400):
            summary = MatchSummary(match_id=str(10000000 + index))
            summary.outcomes.append(
                EventOutcome(
                    match_id=summary.match_id,
                    event_key=f"{summary.match_id}:G:1",
                    code="G",
                    event_time="45'",
                    status="ocr_failed",
                    reason=f"原因 {index}",
                )
            )
            summaries.append(summary)
        text = format_report(summaries, window)
        self.assertLessEqual(len(text.encode("utf-8")), 28_000)
        self.assertIn("总计：", text)
        self.assertIn("部分比赛行未展开", text)


if __name__ == "__main__":
    unittest.main()
