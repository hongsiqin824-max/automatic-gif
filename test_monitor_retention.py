import json
import os
import sqlite3
import tempfile
import time
import unittest
from collections import namedtuple
from pathlib import Path
from unittest import mock

import monitor_retention
from monitor_retention import (
    DiskAlarm,
    RetentionWorker,
    expire_clips,
    expire_published_copies,
    protected_paths,
    retention_days_env,
)

DAY = 86400.0
NOW = 1_800_000_000.0
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
Usage = namedtuple("Usage", "total used free")


def _touch(path: Path, age_days: float, body: bytes = b"GIF89a") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    stamp = NOW - age_days * DAY
    os.utime(path, (stamp, stamp))
    return path


def _make_db(path: Path, rows: list[dict]) -> Path:
    connection = sqlite3.connect(str(path))
    connection.execute(
        """
        CREATE TABLE article_delivery_tasks (
            task_key TEXT PRIMARY KEY, status TEXT NOT NULL, source_path TEXT NOT NULL,
            staged_path TEXT, previous_staged_path TEXT,
            extra_source_paths_json TEXT, extra_staged_paths_json TEXT
        )
        """
    )
    for index, row in enumerate(rows):
        connection.execute(
            "INSERT INTO article_delivery_tasks VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                str(index),
                row["status"],
                row.get("source_path", ""),
                row.get("staged_path"),
                row.get("previous_staged_path"),
                json.dumps(row["extras"]) if row.get("extras") else None,
                json.dumps(row["extra_staged"]) if row.get("extra_staged") else None,
            ),
        )
    connection.commit()
    connection.close()
    return path


class RetentionTestCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        self.clips = self.root / "output_gifs" / "monitor"
        self.published = self.root / "data" / "published_gifs"
        self.db = self.root / "data" / "article_publish.sqlite3"
        self.clips.mkdir(parents=True)
        self.published.mkdir(parents=True)

    def tearDown(self):
        self._tmp.cleanup()

    def clip(self, match_id: str, name: str, age_days: float) -> Path:
        gif = _touch(self.clips / match_id / f"clip_{name}.gif", age_days)
        _touch(gif.with_suffix(".json"), age_days, b"{}")
        return gif


class ExpireClipsTests(RetentionTestCase):
    def test_old_clips_go_with_their_sidecars_recent_ones_stay(self):
        old = self.clip("54493413", "20261101-120000_aaaaaa", 4)
        recent = self.clip("54493413", "20261108-120000_bbbbbb", 1)

        tally = expire_clips(self.clips, NOW - 3 * DAY, set())

        self.assertFalse(old.exists())
        self.assertFalse(old.with_suffix(".json").exists())
        self.assertTrue(recent.exists())
        self.assertTrue(recent.with_suffix(".json").exists())
        self.assertEqual(tally.files, 2)
        self.assertGreater(tally.bytes, 0)

    def test_empty_match_dir_is_removed_and_pins_are_never_touched(self):
        self.clip("54000001", "20261101-120000_aaaaaa", 5)
        pin = _touch(self.clips / "54000002" / ".pin_abc" / "seg_1.ts", 10)
        self.clip("54000002", "20261101-120000_cccccc", 5)

        expire_clips(self.clips, NOW - 3 * DAY, set())

        self.assertFalse((self.clips / "54000001").exists())
        self.assertTrue(pin.exists())
        self.assertTrue((self.clips / "54000002").is_dir())

    def test_clip_a_pending_task_reads_is_kept(self):
        main = self.clip("54000001", "20261101-120000_aaaaaa", 5)
        extra = self.clip("54000001", "20261101-120005_bbbbbb", 5)

        expire_clips(self.clips, NOW - 3 * DAY, {main.resolve(), extra.resolve()})

        self.assertTrue(main.exists() and main.with_suffix(".json").exists())
        self.assertTrue(extra.exists())

    def test_orphan_sidecar_and_unrelated_files(self):
        orphan = _touch(self.clips / "54000001" / "clip_20261101-120000_aaaaaa.json", 5, b"{}")
        other = _touch(self.clips / "54000001" / "notes.txt", 5, b"x")
        not_a_match = _touch(self.clips / "scratch" / "clip_20261101-120000_aaaaaa.gif", 5)

        expire_clips(self.clips, NOW - 3 * DAY, set())

        self.assertFalse(orphan.exists())
        self.assertTrue(other.exists())
        self.assertTrue(not_a_match.exists())

    def test_missing_root_is_harmless(self):
        tally = expire_clips(self.root / "nope", NOW, set())
        self.assertEqual(tally.files, 0)


class ExpirePublishedCopiesTests(RetentionTestCase):
    def test_old_copies_and_covers_go_protected_and_recent_stay(self):
        old = _touch(self.published / f"{SHA_A}.gif", 4)
        old_cover = _touch(self.published / "covers" / f"{SHA_A}.jpg", 4)
        kept = _touch(self.published / f"{SHA_B}.gif", 4)
        kept_cover = _touch(self.published / "covers" / f"{SHA_B}.jpg", 4)
        recent = _touch(self.published / f"{SHA_C}.gif", 0.5)
        temp = _touch(self.published / f".{SHA_A}.gif.123.tmp", 4)

        tally = expire_published_copies(self.published, NOW - 3 * DAY, {kept.resolve()})

        self.assertFalse(old.exists())
        self.assertFalse(old_cover.exists())
        self.assertTrue(kept.exists() and kept_cover.exists())
        self.assertTrue(recent.exists())
        self.assertTrue(temp.exists())
        self.assertEqual(tally.files, 2)


class ProtectedPathsTests(RetentionTestCase):
    def test_only_tasks_still_in_flight_protect_their_files(self):
        _make_db(
            self.db,
            [
                {"status": "published", "source_path": "/x/published.gif", "staged_path": "/x/p_staged.gif"},
                {"status": "drafted", "source_path": "/x/drafted.gif"},
                {"status": "failed", "source_path": "/x/failed.gif"},
                {
                    "status": "retry_wait",
                    "source_path": "/x/retry.gif",
                    "staged_path": "/x/retry_staged.gif",
                    "previous_staged_path": "/x/retry_prev.gif",
                    "extras": ["/x/retry_extra.gif"],
                    "extra_staged": ["/x/retry_extra_staged.gif"],
                },
                {"status": "queued", "source_path": "/x/queued.gif", "extras": "not-a-list"},
                {"status": "some_future_status", "source_path": "/x/future.gif"},
            ],
        )

        protected = {str(p) for p in protected_paths(self.db)}

        self.assertEqual(
            protected,
            {
                str(Path(p).resolve())
                for p in (
                    "/x/retry.gif",
                    "/x/retry_staged.gif",
                    "/x/retry_prev.gif",
                    "/x/retry_extra.gif",
                    "/x/retry_extra_staged.gif",
                    "/x/queued.gif",
                    "/x/future.gif",
                )
            },
        )

    def test_no_database_or_no_table_protects_nothing(self):
        self.assertEqual(protected_paths(self.db), set())
        sqlite3.connect(str(self.db)).close()
        self.assertEqual(protected_paths(self.db), set())

    def test_unreadable_database_raises(self):
        self.db.write_bytes(b"this is not sqlite" * 100)
        with self.assertRaises(sqlite3.Error):
            protected_paths(self.db)


class DiskAlarmTests(unittest.TestCase):
    def test_warns_below_line_repeats_hourly_and_rearms(self):
        sent = []
        alarm = DiskAlarm(15.0, lambda text: sent.append(text) or True, repeat_seconds=3600)
        total = 128 * 10**9

        self.assertFalse(alarm.check(total, 48 * 10**9, NOW))
        self.assertTrue(alarm.check(total, 10 * 10**9, NOW))
        self.assertFalse(alarm.check(total, 9 * 10**9, NOW + 1800))
        self.assertTrue(alarm.check(total, 9 * 10**9, NOW + 3600))
        self.assertFalse(alarm.check(total, 30 * 10**9, NOW + 3700))
        self.assertTrue(alarm.check(total, 10 * 10**9, NOW + 3800))
        self.assertEqual(len(sent), 3)
        self.assertIn("服务器硬盘快满了", sent[0])
        self.assertIn("10.0G / 128.0G", sent[0])
        self.assertIn("告警线 15%", sent[0])

    def test_off_and_failed_push(self):
        self.assertFalse(DiskAlarm(0, lambda text: True).check(100, 1, NOW))
        failing = DiskAlarm(15.0, lambda text: False)
        self.assertTrue(failing.check(100, 1, NOW))
        self.assertFalse(failing.check(100, 1, NOW + 60))


class RetentionWorkerTests(RetentionTestCase):
    def worker(self, **overrides):
        sent = overrides.pop("sent", [])
        options = dict(
            clip_root=self.clips,
            published_dir=self.published,
            database_path=self.db,
            notify=lambda text: sent.append(text) or True,
            clip_retention_days=3,
            published_retention_days=3,
            disk_alert_free_percent=15,
            disk_usage=lambda path: Usage(100 * 10**9, 50 * 10**9, 50 * 10**9),
            clock=lambda: NOW,
        )
        options.update(overrides)
        return RetentionWorker(**options)

    def test_sweeps_both_dirs_and_spares_pending_task_files(self):
        old = self.clip("54000001", "20261101-120000_aaaaaa", 4)
        pending = self.clip("54000001", "20261101-120005_bbbbbb", 4)
        staged = _touch(self.published / f"{SHA_A}.gif", 4)
        old_copy = _touch(self.published / f"{SHA_B}.gif", 4)
        _make_db(self.db, [{"status": "queued", "source_path": str(pending), "staged_path": str(staged)}])

        summary = self.worker().run_once()

        self.assertFalse(old.exists())
        self.assertFalse(old_copy.exists())
        self.assertTrue(pending.exists() and staged.exists())
        self.assertEqual(summary["deleted_clip_files"], 2)
        self.assertEqual(summary["deleted_published_files"], 1)
        self.assertEqual(summary["protected_files"], 2)
        self.assertEqual(summary["disk_free_percent"], 50.0)

    def test_unreadable_task_database_deletes_nothing_but_still_checks_disk(self):
        old = self.clip("54000001", "20261101-120000_aaaaaa", 30)
        self.db.write_bytes(b"this is not sqlite" * 100)
        sent = []

        summary = self.worker(
            sent=sent, disk_usage=lambda path: Usage(100 * 10**9, 95 * 10**9, 5 * 10**9)
        ).run_once()

        self.assertTrue(old.exists())
        self.assertIn("skipped", summary)
        self.assertEqual(len(sent), 1)

    def test_zero_retention_keeps_everything(self):
        old = self.clip("54000001", "20261101-120000_aaaaaa", 30)
        copy = _touch(self.published / f"{SHA_A}.gif", 30)

        summary = self.worker(clip_retention_days=0, published_retention_days=0).run_once()

        self.assertTrue(old.exists() and copy.exists())
        self.assertNotIn("deleted_clip_files", summary)

    def test_low_disk_alert_says_cleanup_already_ran(self):
        sent = []
        self.worker(sent=sent, disk_usage=lambda path: Usage(128 * 10**9, 118 * 10**9, 10 * 10**9)).run_once()
        self.assertEqual(len(sent), 1)
        self.assertIn("已自动删除 3 天前", sent[0])

    def test_background_thread_runs_and_stops(self):
        calls = []
        worker = self.worker(initial_delay_seconds=0, interval_seconds=3600)
        with mock.patch.object(worker, "run_once", side_effect=lambda: calls.append(1)):
            worker.start()
            deadline = time.time() + 2
            while not calls and time.time() < deadline:
                time.sleep(0.01)
            worker.stop()
        self.assertEqual(calls, [1])


class MonitorServerWiringTests(unittest.TestCase):
    def test_import_starts_nothing_and_worker_sweeps_the_queue_paths(self):
        import threading

        import monitor_server

        self.assertFalse(any(t.name == "monitor-retention" for t in threading.enumerate()))
        queue = mock.Mock(staging_directory=Path("/q/published"), database_path=Path("/q/tasks.sqlite3"))
        with mock.patch.object(monitor_server, "manual_draft_queue", queue), mock.patch.object(
            RetentionWorker, "start"
        ) as start, mock.patch("atexit.register"):
            worker = monitor_server._start_retention_worker()
        start.assert_called_once_with()
        self.assertEqual(worker.clip_root, monitor_server.CLIP_OUTPUT_ROOT)
        self.assertEqual(worker.published_dir, Path("/q/published"))
        self.assertEqual(worker.database_path, Path("/q/tasks.sqlite3"))


class RetentionEnvTests(unittest.TestCase):
    def test_defaults_zero_floor_and_invalid(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("X_DAYS", None)
            self.assertEqual(retention_days_env("X_DAYS", 3.0), 3.0)
        with mock.patch.dict(os.environ, {"X_DAYS": "0"}):
            self.assertEqual(retention_days_env("X_DAYS", 3.0), 0)
        with mock.patch.dict(os.environ, {"X_DAYS": "0.25"}):
            self.assertEqual(retention_days_env("X_DAYS", 3.0), 1.0)
        with mock.patch.dict(os.environ, {"X_DAYS": "7"}):
            self.assertEqual(retention_days_env("X_DAYS", 3.0), 7.0)
        for bad in ("-1", "abc", "nan", "inf"):
            with mock.patch.dict(os.environ, {"X_DAYS": bad}), self.assertRaises(RuntimeError):
                retention_days_env("X_DAYS", 3.0)

    def test_module_defaults(self):
        self.assertEqual(monitor_retention.MIN_RETENTION_DAYS, 1.0)
        self.assertEqual(monitor_retention.SETTLED_TASK_STATUSES, ("published", "drafted", "failed"))


if __name__ == "__main__":
    unittest.main()
