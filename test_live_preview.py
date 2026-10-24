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
        # 1s segments with a keyframe forced at every segment boundary: this is
        # the main lever on preview delay (see live_preview.PREVIEW_SEGMENT_SECONDS).
        self.assertEqual(command[command.index("-hls_time") + 1], "1")
        self.assertEqual(
            command[command.index("-force_key_frames") + 1], "expr:gte(t,n_forced*1)"
        )
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

    def test_rtsp_command_uses_tcp_interleaving(self):
        # Scheme H: ffmpeg reads the match back from mediamtx over loopback RTSP.
        command = self.manager._build_command("rtsp://127.0.0.1:8554/54477889", self.root)
        self.assertEqual(command[command.index("-rtsp_transport") + 1], "tcp")
        # The RTSP demuxer rejects -rw_timeout ("Option rw_timeout not found");
        # verified against a real mediamtx. Its read bound is -timeout.
        self.assertNotIn("-rw_timeout", command)
        self.assertEqual(command[command.index("-timeout") + 1], "40000000")
        self.assertNotIn("-re", command)
        self.assertEqual(command[command.index("-i") + 1], "rtsp://127.0.0.1:8554/54477889")
        # Input options must precede -i for ffmpeg to apply them to the source.
        self.assertLess(command.index("-rtsp_transport"), command.index("-i"))

    def test_on_stop_fires_for_explicit_stop_idle_reap_and_shutdown(self):
        stopped = []
        manager = LivePreviewManager(
            root=self.root, background_reaper=False, idle_timeout_seconds=1.0,
            on_stop=stopped.append,
        )
        manager._build_command = _stub_command_factory(None)
        manager.start("a", "rtmp://x/a")
        manager.start("b", "rtmp://x/b")
        manager.start("c", "rtmp://x/c")
        manager.stop("a")
        self.assertEqual(stopped, ["a"])
        # Idle reap tears b down and must release its sibling resource too.
        manager.entries["b"].last_access -= 10
        manager._reap_tick(time.time())
        self.assertEqual(stopped, ["a", "b"])
        manager.shutdown()
        self.assertEqual(sorted(stopped), ["a", "b", "c"])

    def test_start_replacing_an_entry_does_not_fire_on_stop(self):
        # Cross bug between the two hooks: ``preview/start`` registers the
        # mediamtx path *before* calling ``start()``; when ``start()`` had to
        # replace an existing entry (dead ffmpeg, or resource changed e.g.
        # rtmp -> rtsp after a fallback) it reused the teardown helper, which
        # fired ``on_stop`` and deleted the freshly registered path - the new
        # ffmpeg then read a path that no longer existed. Replacing is not a
        # stop; only explicit stop / idle reap / shutdown may release the
        # sibling resource.
        stopped = []
        manager = LivePreviewManager(
            root=self.root, background_reaper=False, on_stop=stopped.append,
        )
        manager._build_command = _stub_command_factory(None)
        manager.start("a", "rtmp://x/a")
        # Resource changed (fallback RTMP -> mediamtx RTSP): must not release.
        manager.start("a", "rtsp://127.0.0.1:8554/a")
        self.assertEqual(stopped, [])
        self.assertEqual(manager.entries["a"].resource, "rtsp://127.0.0.1:8554/a")
        # Dead entry re-started by the operator: must not release either.
        manager.entries["a"].process.terminate()
        manager.entries["a"].process.wait(timeout=5)
        manager.start("a", "rtsp://127.0.0.1:8554/a")
        self.assertEqual(stopped, [])
        # The explicit stop afterwards still releases it exactly once.
        manager.stop("a")
        self.assertEqual(stopped, ["a"])
        manager.shutdown()
        self.assertEqual(stopped, ["a"])

    def test_on_stop_error_never_blocks_teardown(self):
        def boom(_match_id):
            raise RuntimeError("gateway down")

        manager = LivePreviewManager(root=self.root, background_reaper=False, on_stop=boom)
        manager._build_command = _stub_command_factory(None)
        manager.start("a", "rtmp://x/a")
        manager.stop("a")  # must not raise
        self.assertFalse(manager.status("a")["running"])


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
        self.assertFalse(self.manager.match_root("match-1").exists())
        self.assertFalse(self.manager.status("match-1")["running"])

    def test_media_dir_without_entry_is_first_generation(self):
        # Request handlers resolve the served directory through media_dir();
        # with no entry it must still be deterministic so tests and the
        # clip endpoints keep a stable path.
        self.assertEqual(
            self.manager.media_dir("match-x"),
            self.manager.match_root("match-x") / "g1",
        )

    def test_start_rejects_empty_resource(self):
        with self.assertRaises(ValueError):
            self.manager.start("match-1", "   ")


def _dying_command_factory(stderr_text: str = "", write_playlist: bool = True):
    """Return a fake ``_build_command`` whose child dies right after booting.

    It writes the playlist (so ``playlist_ready`` turns true), optionally emits
    ``stderr_text`` (emulating ffmpeg's final error line) and then exits
    non-zero, emulating an RTMP source that drops immediately. With
    ``write_playlist=False`` it dies before producing anything, like ffmpeg
    failing to connect to a source that has stopped for good.
    """

    def _build(resource, directory):
        playlist = directory / "index.m3u8"
        write = (
            f"pathlib.Path(r'{playlist}').write_text('#EXTM3U', encoding='utf-8')\n"
            if write_playlist else ""
        )
        script = (
            "import pathlib, sys\n"
            + write
            + f"sys.stderr.write({stderr_text!r})\n"
            "sys.stderr.flush()\n"
            "sys.exit(1)\n"
        )
        return [sys.executable, "-c", script]

    return _build


def _silent_command_factory():
    """A long-lived child that never writes a playlist (ffmpeg still connecting)."""

    def _build(resource, directory):
        return [sys.executable, "-c", "import time\ntime.sleep(30)\n"]

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

    def test_on_restart_hook_can_repoint_resource(self):
        seen = []

        def hook(match_id, resource):
            seen.append((match_id, resource))
            return "rtmp://upstream/direct" if resource.startswith("rtsp://") else None

        manager = self._make(on_restart=hook)
        manager.start("m1", "rtsp://127.0.0.1:8554/m1")
        entry = manager.entries["m1"]
        entry.process.wait(timeout=5)
        manager._reap_tick(time.time())
        self.assertEqual(seen, [("m1", "rtsp://127.0.0.1:8554/m1")])
        # The relaunch (and every later status) uses the replacement.
        self.assertEqual(manager.entries["m1"].resource, "rtmp://upstream/direct")
        self.assertEqual(manager.status("m1")["resource"], "rtmp://upstream/direct")
        # ``_make`` rebinds ``self.manager``; shut the first one down explicitly
        # so its child process never outlives the test.
        manager.shutdown()
        # A hook that returns None keeps the resource; one that raises never blocks the restart.
        manager2 = self._make(on_restart=lambda m, r: (_ for _ in ()).throw(RuntimeError("boom")))
        manager2.start("m2", "rtmp://host/s")
        manager2.entries["m2"].process.wait(timeout=5)
        manager2._reap_tick(time.time())
        self.assertEqual(manager2.entries["m2"].restart_count, 1)
        self.assertEqual(manager2.entries["m2"].resource, "rtmp://host/s")
        manager2.shutdown()

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

    def test_restart_records_exit_reason_and_keeps_prev_log(self):
        manager = self._make()
        manager._build_command = _dying_command_factory(
            "[warn] some decode noise\n"
            "rtmp://host/s: Connection timed out\n"
        )
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        entry.process.wait(timeout=5)
        manager._reap_tick(time.time())
        status = manager.status("m1")
        # The last stderr line is surfaced as the human-readable reason.
        self.assertEqual(status["last_exit_reason"], "rtmp://host/s: Connection timed out")
        self.assertEqual(status["last_exit_code"], 1)
        self.assertIsNotNone(status["last_exit_at_unix"])
        self.assertEqual(status["restart_count_total"], 1)
        # The dying process's stderr is carried into the replacement's
        # generation directory (the dead run's own files are kept frozen).
        prev_log = manager.entries["m1"].directory / "ffmpeg.prev.log"
        self.assertTrue(prev_log.is_file())
        self.assertIn("Connection timed out", prev_log.read_text(encoding="utf-8"))

    def test_restart_total_survives_budget_reset(self):
        manager = self._make()
        manager.start("m1", "rtmp://host/s")
        for _ in range(2):
            manager.entries["m1"].process.wait(timeout=5)
            manager._reap_tick(time.time())
        entry = manager.entries["m1"]
        self.assertEqual(entry.restart_count, 2)
        self.assertEqual(entry.restart_count_total, 2)
        # Emulate a long healthy run: the fast-retry budget resets, but the
        # lifetime counter and last exit reason are kept for diagnostics.
        manager._build_command = _stub_command_factory(None)
        manager.entries["m1"].process.wait(timeout=5)
        manager._restart_entry_locked("m1", time.time())
        entry = manager.entries["m1"]
        entry.started_at = time.time() - 1000
        manager._reap_tick(time.time())
        status = manager.status("m1")
        self.assertEqual(status["restart_count_total"], 3)
        self.assertEqual(manager.entries["m1"].restart_count, 0)
        self.assertEqual(status["last_exit_code"], 1)

    def test_exit_reason_is_none_when_stderr_empty(self):
        manager = self._make()
        manager.start("m1", "rtmp://host/s")
        manager.entries["m1"].process.wait(timeout=5)
        manager._reap_tick(time.time())
        status = manager.status("m1")
        self.assertIsNone(status["last_exit_reason"])
        self.assertEqual(status["last_exit_code"], 1)
        self.assertFalse((manager.entries["m1"].directory / "ffmpeg.prev.log").exists())


class LivePreviewFrozenHistoryTests(unittest.TestCase):
    """A crashed ffmpeg must not take the ring buffer with it.

    Scenario: the source stops at full time (or drops mid-match). Before, the
    reaper wiped the media directory before relaunching, so the final minutes
    vanished within seconds - exactly when the operator wants to scrub back
    for a stoppage-time goal. Now the dead run's directory is served frozen
    until the replacement has produced a playlist of its own.
    """

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.addCleanup(self._cleanup)
        self.manager = LivePreviewManager(
            root=self.root,
            background_reaper=False,
            auto_restart=True,
            restart_base_delay=0.0,
            restart_max_delay=0.0,
            restart_max_retries=3,
            restart_reset_seconds=100.0,
            idle_timeout_seconds=10000.0,
        )

    def _cleanup(self):
        self.manager.shutdown()
        self._tmp.cleanup()

    def _start_and_crash_first_run(self):
        """g1 writes a playlist plus a segment, then its process dies."""
        manager = self.manager
        manager._build_command = _dying_command_factory()
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        entry.process.wait(timeout=5)
        g1 = entry.directory
        self.assertEqual(g1, manager.match_root("m1") / "g1")
        self.assertTrue((g1 / "index.m3u8").is_file())
        (g1 / "seg_00001.ts").write_bytes(b"ts")
        return g1

    def test_relaunch_keeps_serving_frozen_history(self):
        manager = self.manager
        g1 = self._start_and_crash_first_run()
        # The replacement is alive but has not produced anything yet.
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        entry = manager.entries["m1"]
        self.assertEqual(entry.directory, manager.match_root("m1") / "g2")
        self.assertEqual(manager.media_dir("m1"), g1)
        self.assertTrue((g1 / "index.m3u8").is_file())
        self.assertTrue((g1 / "seg_00001.ts").is_file())
        status = manager.status("m1")
        self.assertTrue(status["running"])
        self.assertTrue(status["history_frozen"])
        self.assertFalse(status["playlist_ready"])
        self.assertEqual(status["resource"], "rtmp://host/s")

    def test_promotion_once_replacement_has_playlist(self):
        manager = self.manager
        g1 = self._start_and_crash_first_run()
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        g2 = manager.entries["m1"].directory
        (g2 / "index.m3u8").write_text("#EXTM3U", encoding="utf-8")
        # status() promotes eagerly so the browser's keep-alive sees the
        # switch without waiting for the next reaper tick.
        status = manager.status("m1")
        self.assertFalse(status["history_frozen"])
        self.assertTrue(status["playlist_ready"])
        self.assertEqual(manager.media_dir("m1"), g2)
        self.assertFalse(g1.exists())

    def test_repeated_failed_relaunches_never_touch_history(self):
        manager = self.manager
        g1 = self._start_and_crash_first_run()
        # Every relaunch dies before writing a playlist: the source is gone.
        manager._build_command = _dying_command_factory(
            "rtmp://host/s: Connection refused\n", write_playlist=False
        )
        for generation in (2, 3, 4):
            manager._reap_tick(time.time())
            entry = manager.entries["m1"]
            self.assertEqual(entry.directory.name, f"g{generation}")
            entry.process.wait(timeout=5)
            self.assertEqual(manager.media_dir("m1"), g1)
            self.assertTrue((g1 / "seg_00001.ts").is_file())
            self.assertTrue(manager.status("m1")["history_frozen"])
        # Dead empty generations are dropped as they are replaced; only the
        # frozen history and the newest attempt remain on disk.
        names = sorted(p.name for p in manager.match_root("m1").iterdir())
        self.assertEqual(names, ["g1", "g4"])
        self.assertEqual(manager.status("m1")["last_exit_reason"], "rtmp://host/s: Connection refused")
        # An explicit stop removes everything, frozen history included.
        manager.stop("m1")
        self.assertFalse(manager.match_root("m1").exists())

    def test_operator_restart_wipes_all_generations(self):
        manager = self.manager
        g1 = self._start_and_crash_first_run()
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        self.assertTrue(g1.exists())
        # Same resource, replacement still alive: start() stays idempotent and
        # must not throw the frozen history away.
        manager.start("m1", "rtmp://host/s")
        self.assertEqual(manager.media_dir("m1"), g1)
        self.assertTrue((g1 / "seg_00001.ts").is_file())
        # Replacement dead too, operator re-issues preview/start: clean slate.
        manager.entries["m1"].process.terminate()
        manager.entries["m1"].process.wait(timeout=5)
        manager._build_command = _stub_command_factory(None)
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        self.assertEqual(entry.directory, manager.match_root("m1") / "g1")
        self.assertEqual(entry.serve_dir, entry.directory)
        self.assertEqual(entry.generation, 1)
        self.assertFalse((g1 / "seg_00001.ts").exists())
        self.assertFalse((manager.match_root("m1") / "g2").exists())


if __name__ == "__main__":
    unittest.main()
