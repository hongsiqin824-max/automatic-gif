"""Deterministic tests for the Dashboard live-source HLS preview manager."""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from live_preview import LivePreviewManager


def _stub_command_factory(directory_playlist_writer):
    """Return a fake ``_build_command`` that runs a controllable child process."""

    def _build(resource, directory):
        playlist = directory / "index.m3u8"
        # A short Python child that behaves like ffmpeg: write the playlist and
        # then stay alive until terminated. This keeps the test free of a real
        # ffmpeg dependency while exercising the real process lifecycle.
        script = (
            "import pathlib, time, sys\n"
            f"pathlib.Path(r'{playlist}').write_text('#EXTM3U', encoding='utf-8')\n"
            "time.sleep(30)\n"
        )
        return [sys.executable, "-c", script]

    return _build


class LivePreviewClassificationTests(unittest.TestCase):
    def test_rtmp_is_not_local_file(self):
        self.assertFalse(LivePreviewManager._is_local_file("rtmp://host/app/stream"))
        self.assertFalse(LivePreviewManager._is_local_file("https://host/x.m3u8"))

    def test_existing_path_is_local_file(self):
        with TemporaryDirectory() as tmp:
            media = Path(tmp) / "clip.mp4"
            media.write_bytes(b"data")
            self.assertTrue(LivePreviewManager._is_local_file(str(media)))

    def test_missing_path_is_not_local_file(self):
        self.assertFalse(LivePreviewManager._is_local_file("/no/such/file.mp4"))


class LivePreviewCommandTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.manager = LivePreviewManager(root=self.root, background_reaper=False)
        self.addCleanup(self._tmp.cleanup)

    def test_rtmp_command_copies_video_and_bounds_read(self):
        command = self.manager._build_command("rtmp://host/app/s", self.root)
        self.assertIn("-rw_timeout", command)
        self.assertNotIn("-re", command)
        self.assertEqual(command[command.index("-i") + 1], "rtmp://host/app/s")
        # The preview re-encodes so every HLS segment carries in-band H.264
        # parameter sets; a plain copy leaves later segments undecodable.
        self.assertEqual(command[command.index("-c:v") + 1], "libx264")
        self.assertIn("-force_key_frames", command)
        self.assertEqual(command[command.index("-f") + 1], "hls")
        # PROGRAM-DATE-TIME must be emitted so a capture can anchor on the exact
        # displayed frame instead of the server's wall-clock receive time.
        self.assertIn("program_date_time", command[command.index("-hls_flags") + 1])

    def test_local_file_command_paces_with_re(self):
        with TemporaryDirectory() as tmp:
            media = Path(tmp) / "clip.mp4"
            media.write_bytes(b"data")
            command = self.manager._build_command(str(media), self.root)
            self.assertIn("-re", command)
            self.assertNotIn("-rw_timeout", command)


class LivePreviewLifecycleTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.manager = LivePreviewManager(root=self.root, background_reaper=False)
        self.manager._build_command = _stub_command_factory(None)
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.manager.shutdown()
        self._tmp.cleanup()

    def _wait_playlist(self, match_id, timeout=5.0):
        deadline = time.time() + timeout
        directory = self.manager.media_dir(match_id)
        while time.time() < deadline:
            if (directory / "index.m3u8").is_file():
                return True
            time.sleep(0.05)
        return False

    def test_status_without_preview_is_not_running(self):
        status = self.manager.status("match-x")
        self.assertFalse(status["running"])
        self.assertFalse(status["playlist_ready"])

    def test_start_reports_running_and_writes_playlist(self):
        self.manager.start("match-1", "rtmp://host/app/s")
        self.assertTrue(self._wait_playlist("match-1"))
        status = self.manager.status("match-1")
        self.assertTrue(status["running"])
        self.assertTrue(status["playlist_ready"])
        self.assertEqual(status["resource"], "rtmp://host/app/s")

    def test_start_is_idempotent_for_same_resource(self):
        self.manager.start("match-1", "rtmp://host/app/s")
        self.assertTrue(self._wait_playlist("match-1"))
        first_pid = self.manager.entries["match-1"].process.pid
        self.manager.start("match-1", "rtmp://host/app/s")
        self.assertEqual(self.manager.entries["match-1"].process.pid, first_pid)

    def test_start_restarts_on_resource_change(self):
        self.manager.start("match-1", "rtmp://host/app/s")
        self.assertTrue(self._wait_playlist("match-1"))
        first_pid = self.manager.entries["match-1"].process.pid
        self.manager.start("match-1", "rtmp://host/app/other")
        self.assertTrue(self._wait_playlist("match-1"))
        self.assertNotEqual(self.manager.entries["match-1"].process.pid, first_pid)
        self.assertEqual(
            self.manager.entries["match-1"].resource, "rtmp://host/app/other"
        )

    def test_stop_terminates_process_and_clears_directory(self):
        self.manager.start("match-1", "rtmp://host/app/s")
        self.assertTrue(self._wait_playlist("match-1"))
        directory = self.manager.media_dir("match-1")
        self.manager.stop("match-1")
        self.assertNotIn("match-1", self.manager.entries)
        self.assertFalse(directory.exists())
        self.assertFalse(self.manager.status("match-1")["running"])

    def test_start_rejects_empty_resource(self):
        with self.assertRaises(ValueError):
            self.manager.start("match-1", "   ")


def _dying_command_factory():
    """Return a fake ``_build_command`` whose child dies right after booting.

    It writes the playlist (so ``playlist_ready`` turns true) and then exits
    non-zero, emulating an RTMP source that drops immediately.
    """

    def _build(resource, directory):
        playlist = directory / "index.m3u8"
        script = (
            "import pathlib, sys\n"
            f"pathlib.Path(r'{playlist}').write_text('#EXTM3U', encoding='utf-8')\n"
            "sys.exit(1)\n"
        )
        return [sys.executable, "-c", script]

    return _build


class LivePreviewAutoRestartTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._cleanup)
        self.manager = None

    def _cleanup(self):
        if self.manager is not None:
            self.manager.shutdown()
        self._tmp.cleanup()

    def _make(self, **kwargs):
        params = dict(
            root=self.root,
            background_reaper=False,
            auto_restart=True,
            restart_base_delay=0.0,
            restart_max_delay=0.0,
            restart_max_retries=3,
            restart_reset_seconds=100.0,
        )
        params.update(kwargs)
        manager = LivePreviewManager(**params)
        manager._build_command = _dying_command_factory()
        self.manager = manager
        return manager

    def test_dead_source_is_restarted_while_watched(self):
        manager = self._make()
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        first_pid = entry.process.pid
        entry.process.wait(timeout=5)
        manager._reap_tick(time.time())
        restarted = manager.entries["m1"]
        self.assertNotEqual(restarted.process.pid, first_pid)
        self.assertEqual(restarted.restart_count, 1)

    def test_backoff_blocks_restart_until_due(self):
        manager = self._make(
            restart_base_delay=100.0,
            restart_max_delay=100.0,
            idle_timeout_seconds=10000.0,
        )
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        entry.process.wait(timeout=5)
        now = time.time()
        manager._reap_tick(now)
        self.assertEqual(entry.restart_count, 1)
        pid_after_first = entry.process.pid
        entry.process.wait(timeout=5)
        # Same instant: still inside the backoff window, so no second restart.
        manager._reap_tick(now)
        self.assertEqual(manager.entries["m1"].process.pid, pid_after_first)
        self.assertEqual(manager.entries["m1"].restart_count, 1)
        # Past the backoff window it restarts again.
        manager._reap_tick(now + 1000)
        self.assertEqual(manager.entries["m1"].restart_count, 2)

    def test_cooling_after_max_retries_keeps_probing(self):
        manager = self._make(restart_max_retries=3)
        manager.start("m1", "rtmp://host/s")
        for expected in (1, 2, 3):
            entry = manager.entries["m1"]
            entry.process.wait(timeout=5)
            manager._reap_tick(time.time())
            self.assertEqual(manager.entries["m1"].restart_count, expected)
        entry = manager.entries["m1"]
        # Dropped to low-frequency probing, but NOT abandoned: it keeps retrying.
        self.assertTrue(entry.cooling)
        entry.process.wait(timeout=5)
        prev_pid = entry.process.pid
        manager._reap_tick(time.time())
        self.assertEqual(manager.entries["m1"].restart_count, 4)
        self.assertNotEqual(manager.entries["m1"].process.pid, prev_pid)

    def test_disabled_auto_restart_leaves_dead_process(self):
        manager = self._make(auto_restart=False)
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        dead_pid = entry.process.pid
        entry.process.wait(timeout=5)
        manager._reap_tick(time.time())
        self.assertIn("m1", manager.entries)
        self.assertEqual(manager.entries["m1"].process.pid, dead_pid)
        self.assertEqual(manager.entries["m1"].restart_count, 0)

    def test_idle_dead_source_is_reaped_not_restarted(self):
        manager = self._make(idle_timeout_seconds=0.0)
        manager.start("m1", "rtmp://host/s")
        manager.entries["m1"].process.wait(timeout=5)
        manager._reap_tick(time.time() + 1)
        self.assertNotIn("m1", manager.entries)

    def test_stable_run_resets_retry_budget(self):
        manager = self._make()
        # Swap to a long-lived child so the entry stays "running" past the reset.
        manager._build_command = _stub_command_factory(None)
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        entry.restart_count = 2
        entry.cooling = True
        entry.started_at = time.time() - 1000
        manager._reap_tick(time.time())
        self.assertEqual(manager.entries["m1"].restart_count, 0)
        self.assertFalse(manager.entries["m1"].cooling)


if __name__ == "__main__":
    unittest.main()
