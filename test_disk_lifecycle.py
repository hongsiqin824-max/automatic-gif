import json
import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from disk_lifecycle import DiskLifecycleManager, DiskLifecyclePolicy


class DiskLifecycleTests(unittest.TestCase):
    def test_expired_vision_candidates_are_pruned_without_touching_gifs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidates = root / "vision_candidates"
            screenshots = candidates / "scoreboards"
            screenshots.mkdir(parents=True)
            expired = [
                candidates / "candidate.mp4",
                candidates / "candidate.json",
                screenshots / "scoreboard.png",
            ]
            recent = candidates / "recent.mp4"
            candidate_gif = candidates / "refined.gif"
            default_gif = root / "goal_default.gif"
            refined_gif = root / "goal_refined.gif"
            for path in [*expired, recent, candidate_gif, default_gif, refined_gif]:
                path.write_bytes(path.name.encode())
            old_time = time.time() - 25 * 60 * 60
            for path in expired:
                os.utime(path, (old_time, old_time))
            os.utime(candidate_gif, (old_time, old_time))

            summary = DiskLifecycleManager(root).prune_ingest_logs()

            self.assertTrue(all(not path.exists() for path in expired))
            self.assertTrue(recent.exists())
            self.assertTrue(candidate_gif.exists())
            self.assertTrue(default_gif.exists())
            self.assertTrue(refined_gif.exists())
            self.assertEqual(summary.deleted_files, 3)
            self.assertEqual(summary.status, "completed")
            self.assertIn("expired_vision_candidates_pruned", summary.actions)

    def test_vision_candidate_cleanup_skips_file_and_directory_symlinks(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            tempfile.TemporaryDirectory() as outside,
        ):
            root = Path(directory)
            candidates = root / "vision_candidates"
            candidates.mkdir()
            outside_root = Path(outside)
            external_file = outside_root / "external.mp4"
            external_file.write_bytes(b"external")
            external_dir = outside_root / "frames"
            external_dir.mkdir()
            external_frame = external_dir / "scoreboard.png"
            external_frame.write_bytes(b"frame")
            file_link = candidates / "linked.mp4"
            directory_link = candidates / "linked_frames"
            file_link.symlink_to(external_file)
            directory_link.symlink_to(external_dir, target_is_directory=True)
            old_time = time.time() - 25 * 60 * 60
            os.utime(external_file, (old_time, old_time))
            os.utime(external_frame, (old_time, old_time))

            summary = DiskLifecycleManager(root).prune_ingest_logs()

            self.assertTrue(file_link.is_symlink())
            self.assertTrue(directory_link.is_symlink())
            self.assertTrue(external_file.exists())
            self.assertTrue(external_frame.exists())
            self.assertEqual(summary.deleted_files, 0)
            self.assertEqual(summary.skipped_files, 2)

    def test_vision_candidate_root_symlink_outside_output_is_skipped(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            tempfile.TemporaryDirectory() as outside,
        ):
            root = Path(directory)
            outside_root = Path(outside)
            external = outside_root / "candidate.mp4"
            external.write_bytes(b"external")
            old_time = time.time() - 25 * 60 * 60
            os.utime(external, (old_time, old_time))
            (root / "vision_candidates").symlink_to(
                outside_root,
                target_is_directory=True,
            )

            summary = DiskLifecycleManager(root).prune_ingest_logs()

            self.assertTrue(external.exists())
            self.assertEqual(summary.deleted_files, 0)
            self.assertEqual(summary.skipped_files, 1)
            self.assertIn("vision_candidates_symlink_skipped", summary.actions)

    def test_vision_candidate_scan_failure_is_nonfatal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidates = root / "vision_candidates"
            candidates.mkdir()

            with patch(
                "disk_lifecycle.os.scandir",
                side_effect=PermissionError("denied"),
            ):
                summary = DiskLifecycleManager(root).prune_ingest_logs()

            self.assertEqual(summary.status, "completed_with_warnings")
            self.assertTrue(summary.errors)

    def test_vision_candidate_delete_failure_is_nonfatal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidates = root / "vision_candidates"
            candidates.mkdir()
            expired = candidates / "candidate.mp4"
            expired.write_bytes(b"candidate")
            old_time = time.time() - 25 * 60 * 60
            os.utime(expired, (old_time, old_time))

            with patch.object(
                Path,
                "unlink",
                side_effect=PermissionError("denied"),
            ):
                summary = DiskLifecycleManager(root).prune_ingest_logs()

            self.assertTrue(expired.exists())
            self.assertEqual(summary.status, "completed_with_warnings")
            self.assertTrue(summary.errors)

    def test_finished_cleanup_removes_media_and_clears_manifest_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            buffer_dir = root / "buffer"
            buffer_dir.mkdir()
            old_ts = buffer_dir / "segment_old.ts"
            old_ts.write_bytes(b"old")
            old_csv = buffer_dir / "segments_run_g000.csv"
            old_csv.write_text("segment_old.ts,0,2\n", encoding="utf-8")
            manifest_path = buffer_dir / "segment_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "match_id": "match-1",
                        "source": "rtmp://source",
                        "timeline_origin_wall": 1000.0,
                        "generations": [{"list_path": "segments_run_g000.csv"}],
                    }
                ),
                encoding="utf-8",
            )
            event_log = root / "pipeline_events.jsonl"
            event_log.write_text("event\n", encoding="utf-8")
            state_db = root / "pipeline_state.sqlite3"
            sqlite3.connect(state_db).close()

            summary = DiskLifecycleManager(root).cleanup_finished_match(
                buffer_dir=buffer_dir,
                manifest_path=manifest_path,
                event_log_path=event_log,
                state_db_path=state_db,
            )

            self.assertEqual(summary.status, "completed")
            self.assertFalse(old_ts.exists())
            self.assertFalse(old_csv.exists())
            self.assertEqual(json.loads(manifest_path.read_text())["generations"], [])
            self.assertIn("manifest_generations_cleared", summary.actions)

    def test_protected_media_and_recent_retention_are_not_deleted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            buffer_dir = root / "buffer"
            buffer_dir.mkdir()
            protected = buffer_dir / "protected.ts"
            recent = buffer_dir / "recent.ts"
            old = buffer_dir / "old.ts"
            for path in (protected, recent, old):
                path.write_bytes(path.name.encode())
            old_time = time.time() - 600
            os.utime(old, (old_time, old_time))
            (buffer_dir / "segments_keep.csv").write_text(
                "protected.ts,0,2\nrecent.ts,2,4\nold.ts,4,6\n", encoding="utf-8"
            )
            manifest = buffer_dir / "segment_manifest.json"
            manifest.write_text(
                json.dumps({"version": 1, "generations": [{"list_path": "segments_keep.csv"}]}),
                encoding="utf-8",
            )
            event_log = root / "pipeline_events.jsonl"
            state_db = root / "pipeline_state.sqlite3"
            sqlite3.connect(state_db).close()

            summary = DiskLifecycleManager(
                root,
                DiskLifecyclePolicy(post_match_buffer_seconds=300),
            ).cleanup_finished_match(
                buffer_dir=buffer_dir,
                manifest_path=manifest,
                event_log_path=event_log,
                state_db_path=state_db,
                protected_paths=[protected],
            )

            self.assertTrue(protected.exists())
            self.assertTrue(recent.exists())
            self.assertFalse(old.exists())
            self.assertTrue((buffer_dir / "segments_keep.csv").exists())
            self.assertEqual(summary.status, "completed")

    def test_malformed_segment_list_is_retained_with_warning(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            buffer_dir = root / "buffer"
            buffer_dir.mkdir()
            malformed = buffer_dir / "segments_bad.csv"
            malformed.write_text("../../outside.ts,0,2\n", encoding="utf-8")
            manifest = buffer_dir / "segment_manifest.json"
            manifest.write_text(
                json.dumps({"version": 1, "generations": [{"list_path": malformed.name}]}),
                encoding="utf-8",
            )
            event_log = root / "pipeline_events.jsonl"
            state_db = root / "pipeline_state.sqlite3"
            sqlite3.connect(state_db).close()

            summary = DiskLifecycleManager(root).cleanup_finished_match(
                buffer_dir=buffer_dir,
                manifest_path=manifest,
                event_log_path=event_log,
                state_db_path=state_db,
            )

            self.assertTrue(malformed.exists())
            self.assertEqual(summary.status, "completed_with_warnings")
            self.assertTrue(summary.errors)

    def test_active_log_history_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index in range(5):
                path = root / f"ingest_ffmpeg_run_g{index:03d}.log"
                path.write_text(str(index), encoding="utf-8")
                os.utime(path, (100 + index, 100 + index))

            summary = DiskLifecycleManager(
                root, DiskLifecyclePolicy(keep_ingest_logs=2)
            ).prune_ingest_logs()

            self.assertEqual(
                sorted(path.name for path in root.glob("ingest_ffmpeg_*.log")),
                ["ingest_ffmpeg_run_g003.log", "ingest_ffmpeg_run_g004.log"],
            )
            self.assertEqual(summary.deleted_files, 3)

    def test_large_event_log_is_rotated_and_archive_count_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            event_log = root / "pipeline_events.jsonl"
            event_log.write_bytes(b"x" * 32)
            (root / "pipeline_events.jsonl.1").write_bytes(b"old")
            state_db = root / "pipeline_state.sqlite3"
            sqlite3.connect(state_db).close()

            summary = DiskLifecycleManager(
                root,
                DiskLifecyclePolicy(event_log_max_bytes=8, event_log_archives=1),
            ).cleanup_finished_match(
                buffer_dir=root / "buffer",
                manifest_path=root / "buffer" / "segment_manifest.json",
                event_log_path=event_log,
                state_db_path=state_db,
            )

            self.assertTrue(event_log.exists())
            self.assertEqual(event_log.stat().st_size, 0)
            self.assertTrue((root / "pipeline_events.jsonl.1").exists())
            self.assertEqual(summary.rotated_files, 1)
            self.assertEqual(summary.deleted_files, 1)
