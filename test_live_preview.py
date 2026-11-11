"""Deterministic tests for the Dashboard live-source HLS preview manager."""

from __future__ import annotations

import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import live_preview
from live_preview import LivePreviewManager, non_negative_env_float


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
    def test_exit_reason_summary(self):
        summarize = LivePreviewManager._summarize_exit_reason
        self.assertIsNone(summarize(None))
        self.assertEqual(summarize("noise\nrtmp://h/s: Operation not permitted\n"), "rtmp://h/s: Operation not permitted")
        self.assertEqual(summarize("warn A\nLast message repeated 3 times\n"), "warn A (x4)")
        # Nothing before the marker: keep the marker rather than invent a cause.
        self.assertEqual(summarize("Last message repeated 1 times"), "Last message repeated 1 times")
        self.assertEqual(
            LivePreviewManager._distinct_tail("a\nb\nb\nLast message repeated 9 times\nc\n", count=2),
            ["b", "c"],
        )

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
        # The dying process's stderr is kept next to the replacement's log in
        # the same directory (the dead run's playlist/segments stay in place).
        prev_log = manager.entries["m1"].directory / "ffmpeg.prev.log"
        self.assertTrue(prev_log.is_file())
        self.assertIn("Connection timed out", prev_log.read_text(encoding="utf-8"))

    def test_repeated_warning_is_reported_instead_of_the_repeat_marker(self):
        # The 54475663 black-screen night logged only "Last message repeated 16
        # times"; the warning it repeats is what tells the cause apart.
        manager = self._make()
        manager._build_command = _dying_command_factory(
            "[flv @ 0x1] Input #0 opened\n"
            "[flv @ 0x1] Video codec 12 not supported\n"
            "    Last message repeated 16 times\n"
        )
        manager.start("m1", "rtmp://host/s")
        manager.entries["m1"].process.wait(timeout=5)
        with mock.patch("live_preview._log_event") as logged:
            manager._reap_tick(time.time())
        self.assertEqual(
            manager.status("m1")["last_exit_reason"],
            "[flv @ 0x1] Video codec 12 not supported (x17)",
        )
        exited = [c.kwargs for c in logged.call_args_list if c.args[0] == "preview_ffmpeg_exited"]
        self.assertEqual(
            exited[0]["stderr_tail"],
            ["[flv @ 0x1] Input #0 opened", "[flv @ 0x1] Video codec 12 not supported"],
        )

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


PLAYLIST_WITH_SEGMENTS = (
    "#EXTM3U\n#EXT-X-VERSION:6\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n"
    "#EXTINF:1.000000,\n#EXT-X-PROGRAM-DATE-TIME:2026-10-26T11:50:03.374+0800\nseg_00000.ts\n"
    "#EXTINF:1.000000,\n#EXT-X-PROGRAM-DATE-TIME:2026-10-26T11:50:04.374+0800\nseg_00001.ts\n"
)


def _dying_with_segments_factory():
    """A child that writes a two-segment playlist and then dies (source dropped)."""

    def _build(resource, directory):
        playlist = directory / "index.m3u8"
        script = (
            "import pathlib, sys\n"
            f"pathlib.Path(r'{playlist}').write_text({PLAYLIST_WITH_SEGMENTS!r}, encoding='utf-8')\n"
            f"pathlib.Path(r'{directory / 'seg_00000.ts'}').write_bytes(b'ts')\n"
            f"pathlib.Path(r'{directory / 'seg_00001.ts'}').write_bytes(b'ts')\n"
            "sys.exit(1)\n"
        )
        return [sys.executable, "-c", script]

    return _build


class LivePreviewFrozenHistoryTests(unittest.TestCase):
    """A crashed ffmpeg must not take the ring buffer with it.

    Scenario: the source drops mid-match (or stops at full time). The
    replacement ffmpeg is relaunched into the *same* directory so its
    ``append_list`` continues the dead run's playlist: old segments, their
    PROGRAM-DATE-TIME and the sequence numbers are kept and the join is marked
    with EXT-X-DISCONTINUITY. The browser keeps one continuous timeline, the
    operator can still scrub back past the gap, and a start pinned before the
    drop stays clippable. Until the replacement appends a segment the history
    is reported ``history_frozen``.
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
        """The first run writes a playlist with two segments, then its process dies."""
        manager = self.manager
        manager._build_command = _dying_with_segments_factory()
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        entry.process.wait(timeout=5)
        media = entry.directory
        self.assertEqual(media, manager.match_root("m1") / "g1")
        self.assertTrue((media / "index.m3u8").is_file())
        self.assertTrue((media / "seg_00001.ts").is_file())
        return media

    def test_dead_run_before_relaunch_is_clippable_not_frozen(self):
        self._start_and_crash_first_run()
        status = self.manager.status("m1")
        self.assertFalse(status["running"])
        self.assertTrue(status["playlist_ready"])
        self.assertFalse(status["history_frozen"])

    def test_relaunch_reuses_directory_and_freezes_history(self):
        manager = self.manager
        media = self._start_and_crash_first_run()
        # The replacement is alive but has not appended anything yet.
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        entry = manager.entries["m1"]
        self.assertEqual(entry.directory, media)
        self.assertEqual(manager.media_dir("m1"), media)
        self.assertEqual(entry.frozen_tail, "seg_00001.ts")
        self.assertTrue((media / "index.m3u8").is_file())
        self.assertTrue((media / "seg_00000.ts").is_file())
        self.assertTrue((media / "seg_00001.ts").is_file())
        self.assertEqual(sorted(p.name for p in manager.match_root("m1").iterdir()), ["g1"])
        status = manager.status("m1")
        self.assertTrue(status["running"])
        self.assertTrue(status["history_frozen"])
        self.assertFalse(status["playlist_ready"])
        self.assertEqual(status["resource"], "rtmp://host/s")

    def test_history_resumes_once_replacement_appends_a_segment(self):
        manager = self.manager
        media = self._start_and_crash_first_run()
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        # A playlist rewrite that does not add a segment keeps it frozen.
        (media / "index.m3u8").write_text(PLAYLIST_WITH_SEGMENTS + "\n", encoding="utf-8")
        self.assertTrue(manager.status("m1")["history_frozen"])
        # ffmpeg appends the first new segment after an EXT-X-DISCONTINUITY.
        (media / "seg_00002.ts").write_bytes(b"ts")
        (media / "index.m3u8").write_text(
            PLAYLIST_WITH_SEGMENTS
            + "#EXT-X-DISCONTINUITY\n#EXTINF:1.000000,\n"
            "#EXT-X-PROGRAM-DATE-TIME:2026-10-26T11:50:23.405+0800\nseg_00002.ts\n",
            encoding="utf-8",
        )
        # status() refreshes eagerly so the browser's keep-alive sees the
        # switch without waiting for the next reaper tick.
        status = manager.status("m1")
        self.assertFalse(status["history_frozen"])
        self.assertTrue(status["playlist_ready"])
        self.assertIsNone(manager.entries["m1"].frozen_tail)
        # Old segments are untouched: the timeline is continuous.
        self.assertTrue((media / "seg_00000.ts").is_file())
        self.assertEqual(manager.media_dir("m1"), media)

    def test_pdt_drift_after_relaunch_is_logged(self):
        # An hlsenc build that re-stamps the old segments on append_list
        # would silently break PDT-anchored clips; the manager must shout.
        manager = self.manager
        media = self._start_and_crash_first_run()
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        entry = manager.entries["m1"]
        self.assertEqual(entry.frozen_tail, "seg_00001.ts")
        self.assertEqual(entry.frozen_tail_pdt, "2026-10-26T11:50:04.374+0800")
        shifted = PLAYLIST_WITH_SEGMENTS.replace("11:50:03.374", "11:50:40.000").replace("11:50:04.374", "11:50:41.000")
        (media / "seg_00002.ts").write_bytes(b"ts")
        (media / "index.m3u8").write_text(
            shifted + "#EXT-X-DISCONTINUITY\n#EXTINF:1.000000,\n"
            "#EXT-X-PROGRAM-DATE-TIME:2026-10-26T11:50:42.000+0800\nseg_00002.ts\n",
            encoding="utf-8",
        )
        with mock.patch("live_preview._log_event") as log_event:
            self.assertFalse(manager.status("m1")["history_frozen"])
        events = {call.args[0]: call.kwargs for call in log_event.call_args_list}
        self.assertIn("preview_history_resumed", events)
        self.assertEqual(
            events["preview_pdt_drift"],
            {"match_id": "m1", "segment": "seg_00001.ts",
             "before": "2026-10-26T11:50:04.374+0800", "after": "2026-10-26T11:50:41.000+0800"},
        )

    def test_unchanged_pdt_after_relaunch_logs_no_drift(self):
        manager = self.manager
        media = self._start_and_crash_first_run()
        manager._build_command = _silent_command_factory()
        manager._reap_tick(time.time())
        (media / "seg_00002.ts").write_bytes(b"ts")
        (media / "index.m3u8").write_text(
            PLAYLIST_WITH_SEGMENTS + "#EXT-X-DISCONTINUITY\n#EXTINF:1.000000,\n"
            "#EXT-X-PROGRAM-DATE-TIME:2026-10-26T11:50:23.405+0800\nseg_00002.ts\n",
            encoding="utf-8",
        )
        with mock.patch("live_preview._log_event") as log_event:
            manager.status("m1")
        names = [call.args[0] for call in log_event.call_args_list]
        self.assertIn("preview_history_resumed", names)
        self.assertNotIn("preview_pdt_drift", names)

    def test_repeated_failed_relaunches_never_touch_history(self):
        manager = self.manager
        media = self._start_and_crash_first_run()
        # Every relaunch dies before writing anything: the source is gone.
        manager._build_command = _dying_command_factory(
            "rtmp://host/s: Connection refused\n", write_playlist=False
        )
        for _ in range(3):
            manager._reap_tick(time.time())
            entry = manager.entries["m1"]
            self.assertEqual(entry.directory, media)
            entry.process.wait(timeout=5)
            self.assertEqual(manager.media_dir("m1"), media)
            self.assertTrue((media / "seg_00001.ts").is_file())
            self.assertTrue(manager.status("m1")["history_frozen"])
            self.assertEqual(entry.frozen_tail, "seg_00001.ts")
        self.assertEqual(manager.status("m1")["last_exit_reason"], "rtmp://host/s: Connection refused")
        self.assertTrue((media / "ffmpeg.prev.log").is_file())
        # An explicit stop removes everything, history included.
        manager.stop("m1")
        self.assertFalse(manager.match_root("m1").exists())

    def test_operator_start_on_dead_same_source_relaunches_in_place(self):
        manager = self.manager
        hook_calls = []
        manager.on_restart = lambda match_id, resource: hook_calls.append((match_id, resource)) or None
        media = self._start_and_crash_first_run()
        dead_pid = manager.entries["m1"].process.pid
        # The operator (or the browser's self-heal) re-issues preview/start
        # for the same source while ffmpeg is dead: relaunch without wiping,
        # so the buffered history and any pinned start survive.
        manager._build_command = _silent_command_factory()
        status = manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        self.assertNotEqual(entry.process.pid, dead_pid)
        self.assertEqual(entry.directory, media)
        self.assertTrue((media / "seg_00001.ts").is_file())
        self.assertTrue(status["running"])
        self.assertTrue(status["history_frozen"])
        self.assertEqual(status["restart_count_total"], 1)
        # An operator-driven relaunch is not a reaper retry: it neither
        # consumes the backoff budget nor re-runs the on_restart hook (the
        # caller has just resolved the resource itself).
        self.assertEqual(entry.restart_count, 0)
        self.assertEqual(entry.next_restart_at, 0.0)
        self.assertFalse(entry.cooling)
        self.assertEqual(hook_calls, [])
        # Same source, replacement alive: start() stays idempotent.
        pid = entry.process.pid
        manager.start("m1", "rtmp://host/s")
        self.assertEqual(manager.entries["m1"].process.pid, pid)
        self.assertTrue((media / "seg_00001.ts").is_file())

    def test_operator_start_does_not_touch_reaper_backoff(self):
        # Repeated manual refreshes while the source is dead must not push
        # the entry into the reaper's cooling state.
        manager = self.manager
        manager.restart_max_retries = 2
        self._start_and_crash_first_run()
        manager._build_command = _dying_command_factory(
            "rtmp://host/s: Connection refused\n", write_playlist=False
        )
        for _ in range(4):
            manager.entries["m1"].process.wait(timeout=5)
            manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        self.assertEqual(entry.restart_count_total, 4)
        self.assertEqual(entry.restart_count, 0)
        self.assertFalse(entry.cooling)
        self.assertFalse(manager.status("m1")["auto_restart_cooling"])

    def test_start_with_new_source_is_a_clean_slate(self):
        manager = self.manager
        media = self._start_and_crash_first_run()
        manager._build_command = _stub_command_factory(None)
        manager.start("m1", "rtmp://host/other")
        entry = manager.entries["m1"]
        self.assertEqual(entry.directory, media)
        self.assertIsNone(entry.frozen_tail)
        self.assertEqual(entry.restart_count_total, 0)
        self.assertFalse((media / "seg_00001.ts").exists())


class LivePreviewSourceBlackTests(unittest.TestCase):
    """How long the source has been sending no picture, from segment sizes.

    54494599 on 11-07: the pusher's feed went black for about 4 minutes. The
    player kept playing the black picture, nothing on the page looked broken
    and the operator reloaded the page 4 times. Real sizes from that night:
    black 4-10 KB per 1 s segment, normal play >= 100 KB (99% of 1.17M
    segments served 11-01..11-09).
    """

    NORMAL = 200 * 1024
    BLACK = 8 * 1024

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.manager = LivePreviewManager(root=self.root, background_reaper=False)
        self.manager._build_command = _stub_command_factory(None)
        self.addCleanup(self._cleanup)

    def _cleanup(self):
        self.manager.shutdown()
        self._tmp.cleanup()

    def _start(self, match_id: str = "m1") -> Path:
        """Start the stub ffmpeg and wait for its own playlist write.

        Our playlist is written after that, so the stub cannot overwrite it.
        """
        self.manager.start(match_id, "rtmp://host/app/s")
        directory = self.manager.media_dir(match_id)
        deadline = time.time() + 5
        while not (directory / "index.m3u8").is_file():
            self.assertLess(time.time(), deadline, "stub ffmpeg never wrote its playlist")
            time.sleep(0.05)
        return directory

    @staticmethod
    def _write(directory: Path, sizes, duration: float = 1.0, missing=()) -> None:
        lines = ["#EXTM3U", "#EXT-X-VERSION:6", f"#EXT-X-TARGETDURATION:{int(duration)}", "#EXT-X-MEDIA-SEQUENCE:0"]
        for index, size in enumerate(sizes):
            name = f"seg_{index:05d}.ts"
            lines += [f"#EXTINF:{duration:.6f},", name]
            if index not in missing:
                (directory / name).write_bytes(b"\0" * size)
        (directory / "index.m3u8").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def test_black_tail_is_reported_once_long_enough(self):
        directory = self._start()
        self._write(directory, [self.NORMAL] * 5 + [self.BLACK] * 12)
        self.assertEqual(self.manager.source_black_seconds("m1"), 12.0)

    def test_short_fade_to_black_is_not_reported(self):
        # 1-3 s fades between programmes are normal broadcast.
        directory = self._start()
        self._write(directory, [self.NORMAL] * 5 + [self.BLACK] * 3)
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)

    def test_picture_back_clears_it(self):
        directory = self._start()
        self._write(directory, [self.NORMAL] * 5 + [self.BLACK] * 30 + [self.NORMAL])
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)

    def test_black_with_sound_counts_and_quiet_play_does_not(self):
        directory = self._start()
        # Black picture with full 128k sound: ~16 KB/s audio + a tiny video.
        self._write(directory, [24 * 1024] * 10)
        self.assertEqual(self.manager.source_black_seconds("m1"), 10.0)
        # The low end of normal play seen 11-01..11-09.
        self._write(directory, [100 * 1024] * 20)
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)

    def test_threshold_is_per_second_of_media(self):
        # GIF_PREVIEW_SEGMENT_SECONDS=2: 50 KB per 2 s is 25 KB/s -> black.
        directory = self._start()
        self._write(directory, [50 * 1024] * 5, duration=2.0)
        self.assertEqual(self.manager.source_black_seconds("m1"), 10.0)
        self._write(directory, [70 * 1024] * 5, duration=2.0)
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)

    def test_missing_segment_or_bad_duration_ends_the_count(self):
        directory = self._start()
        # ffmpeg deleted an older segment while we were counting back.
        self._write(directory, [self.BLACK] * 20, missing={9})
        self.assertEqual(self.manager.source_black_seconds("m1"), 10.0)
        self._write(directory, [self.BLACK] * 20)
        # A separate case: forget the stretch the first one started (otherwise
        # the time since then would be reported, see
        # test_black_longer_than_the_playlist_keeps_counting).
        self.manager.entries["m1"].source_black_since = None
        playlist = directory / "index.m3u8"
        playlist.write_text(
            playlist.read_text(encoding="utf-8").replace(
                "#EXTINF:1.000000,\nseg_00010.ts", "#EXTINF:x,\nseg_00010.ts"
            ),
            encoding="utf-8",
        )
        self.assertEqual(self.manager.source_black_seconds("m1"), 9.0)

    def test_only_reported_while_ffmpeg_is_producing(self):
        self.assertEqual(self.manager.source_black_seconds("nobody"), 0.0)
        directory = self._start()
        self._write(directory, [self.BLACK] * 20)
        entry = self.manager.entries["m1"]
        # A relaunch that has not appended yet: status reports it as frozen.
        entry.frozen_tail = "seg_00019.ts"
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)
        entry.frozen_tail = None
        self.assertEqual(self.manager.source_black_seconds("m1"), 20.0)
        entry.process.terminate()
        entry.process.wait(timeout=5)
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)

    def test_start_and_end_of_a_black_stretch_are_logged_once(self):
        directory = self._start()
        with mock.patch("live_preview._log_event") as log:
            self._write(directory, [self.NORMAL] * 5 + [self.BLACK] * 10)
            self.manager.source_black_seconds("m1")
            self.manager.source_black_seconds("m1")
            self._write(directory, [self.NORMAL] * 5 + [self.BLACK] * 10 + [self.NORMAL])
            self.manager.source_black_seconds("m1")
            self.manager.source_black_seconds("m1")
        events = [c.kwargs for c in log.call_args_list if c.args[0] == "preview_source_black"]
        self.assertEqual([e["black"] for e in events], [True, False])
        self.assertEqual(events[0]["seconds"], 10.0)
        self.assertEqual(events[0]["match_id"], "m1")

    def test_black_longer_than_the_playlist_keeps_counting(self):
        # 11-09 review of ef6cb4e: the playlist holds the last 600 x 1 s, so a
        # count over it alone stopped at ~10 minutes and the stage kept saying
        # "已约 10 分钟" however long the source stayed black.
        directory = self._start()
        self._write(directory, [self.BLACK] * 30)
        real_time = time.time
        self.assertEqual(self.manager.source_black_seconds("m1"), 30.0)
        with mock.patch("live_preview.time.time", side_effect=lambda: real_time() + 700):
            self.assertGreaterEqual(self.manager.source_black_seconds("m1"), 729.0)
        # The picture comes back: the stretch is over and forgotten.
        self._write(directory, [self.BLACK] * 29 + [self.NORMAL])
        self.assertEqual(self.manager.source_black_seconds("m1"), 0.0)
        self.assertIsNone(self.manager.entries["m1"].source_black_since)
        # A new stretch that starts inside the playlist is measured from it,
        # not from the old one.
        self._write(directory, [self.NORMAL] + [self.BLACK] * 12)
        with mock.patch("live_preview.time.time", side_effect=lambda: real_time() + 900):
            self.assertEqual(self.manager.source_black_seconds("m1"), 12.0)


def _segment_playlist(count: int) -> str:
    """An HLS playlist listing seg_00000.ts .. seg_<count-1>.ts (1 s each)."""
    lines = ["#EXTM3U", "#EXT-X-VERSION:6", "#EXT-X-TARGETDURATION:1", "#EXT-X-MEDIA-SEQUENCE:0"]
    for index in range(count):
        lines += ["#EXTINF:1.000000,", f"seg_{index:05d}.ts"]
    return "\n".join(lines) + "\n"


class LivePreviewStallWatchdogTests(unittest.TestCase):
    """An ffmpeg that is alive but writes nothing is relaunched.

    11-07 54493413: the CDN kept the RTMP session open without media for about
    11 minutes. ffmpeg neither timed out nor exited, so the stage froze at 63'
    with no warning, and each of the operator's 12 reopens reused the same
    stuck process until the CDN finally dropped it.
    """

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
        # The production retry settings (monitor_server.py defaults).
        params = dict(
            root=self.root,
            background_reaper=False,
            auto_restart=True,
            restart_base_delay=5.0,
            restart_max_delay=60.0,
            restart_max_retries=6,
            restart_reset_seconds=30.0,
            idle_timeout_seconds=100000.0,
            stall_seconds=20.0,
            stall_min_run_seconds=60.0,
        )
        params.update(kwargs)
        manager = LivePreviewManager(**params)
        # A live child that writes nothing itself; the test writes the playlist.
        manager._build_command = _silent_command_factory()
        self.manager = manager
        return manager

    def _start(self, manager, t0):
        manager.start("m1", "rtmp://host/s")
        entry = manager.entries["m1"]
        entry.started_at = t0
        return entry

    @staticmethod
    def _write(entry, count):
        (entry.directory / "index.m3u8").write_text(_segment_playlist(count), encoding="utf-8")

    def _produce_then_stop(self, manager, t0):
        """A run that produced for 105 s and then stopped at seg_00003.ts."""
        entry = self._start(manager, t0)
        self._write(entry, 3)
        manager._reap_tick(t0 + 100)
        self._write(entry, 4)
        manager._reap_tick(t0 + 105)
        return entry

    def test_stalled_run_is_relaunched_with_history_kept(self):
        manager = self._make()
        t0 = time.time()
        entry = self._produce_then_stop(manager, t0)
        stuck = entry.process
        manager._reap_tick(t0 + 120)  # 15 s without a segment: still waiting
        self.assertIs(manager.entries["m1"].process, stuck)
        with mock.patch("live_preview._log_event") as log:
            manager._reap_tick(t0 + 126)  # 21 s: relaunch
        entry = manager.entries["m1"]
        self.assertIsNot(entry.process, stuck)
        self.assertIsNotNone(stuck.poll())  # the stuck ffmpeg is gone, not orphaned
        self.assertEqual(entry.restart_count_total, 1)
        self.assertEqual(entry.restart_count, 1)
        self.assertEqual(entry.resource, "rtmp://host/s")
        # Same directory, history kept and reported frozen until the
        # replacement appends - exactly like a source drop.
        self.assertEqual(entry.directory, manager.match_root("m1") / "g1")
        self.assertEqual(entry.frozen_tail, "seg_00003.ts")
        status = manager.status("m1")
        self.assertTrue(status["running"])
        self.assertTrue(status["history_frozen"])
        self.assertEqual(status["last_exit_reason"], "源站连接卡住，21 秒没有新画面")
        names = [c.args[0] for c in log.call_args_list]
        self.assertEqual(names[:3], ["preview_ffmpeg_stalled", "preview_ffmpeg_exited", "preview_ffmpeg_restarted"])
        stalled = log.call_args_list[0].kwargs
        self.assertEqual(stalled["match_id"], "m1")
        self.assertEqual(stalled["last_segment"], "seg_00003.ts")
        self.assertEqual(stalled["stalled_seconds"], 21.0)
        self.assertEqual(stalled["uptime_seconds"], 126.0)
        self.assertEqual(log.call_args_list[1].kwargs["reason"], "源站连接卡住，21 秒没有新画面")
        # The replacement has not produced yet: it is left alone (the CDN's
        # own ~60 s refusal handles a source nobody is pushing).
        manager._reap_tick(t0 + 150)
        self.assertEqual(manager.entries["m1"].restart_count_total, 1)
        # Its first segment lands, then nothing: still inside its first 60 s
        # of producing, so this is the CDN's business, not the watchdog's.
        self._write(entry, 5)
        manager._reap_tick(t0 + 155)
        manager._reap_tick(t0 + 180)
        self.assertEqual(manager.entries["m1"].restart_count_total, 1)
        # Producing well past a minute after its start and then stalling for
        # 20 s again: relaunched again.
        self._write(entry, 6)
        manager._reap_tick(t0 + 250)
        manager._reap_tick(t0 + 265)
        self.assertEqual(manager.entries["m1"].restart_count_total, 1)
        manager._reap_tick(t0 + 275)
        self.assertEqual(manager.entries["m1"].restart_count_total, 2)
        self.assertEqual(manager.entries["m1"].frozen_tail, "seg_00005.ts")

    def test_growing_playlist_is_never_touched(self):
        manager = self._make()
        t0 = time.time()
        entry = self._start(manager, t0)
        proc = entry.process
        # 15 s between ticks, but a new segment every time.
        for count, at in enumerate(range(100, 500, 15), start=1):
            self._write(entry, count)
            manager._reap_tick(t0 + at)
        self.assertIs(manager.entries["m1"].process, proc)
        self.assertEqual(manager.entries["m1"].restart_count_total, 0)

    def test_cached_burst_after_connect_is_left_to_the_cdn(self):
        # 11-08 54340581 / 54475673: a connect to a stream nobody pushes gets
        # a few seconds of cached picture, then nothing until the CDN refuses
        # the session at ~60 s. That cycle must stay exactly as it was.
        manager = self._make()
        t0 = time.time()
        entry = self._start(manager, t0)
        proc = entry.process
        self._write(entry, 8)
        manager._reap_tick(t0 + 5)
        manager._reap_tick(t0 + 55)
        manager._reap_tick(t0 + 300)
        self.assertIs(manager.entries["m1"].process, proc)
        self.assertEqual(manager.entries["m1"].restart_count_total, 0)

    def test_switched_off_or_without_auto_restart_does_nothing(self):
        for kwargs in ({"stall_seconds": 0.0}, {"auto_restart": False}):
            with self.subTest(**kwargs):
                manager = self._make(**kwargs)
                t0 = time.time()
                entry = self._produce_then_stop(manager, t0)
                proc = entry.process
                manager._reap_tick(t0 + 400)
                self.assertIs(manager.entries["m1"].process, proc)
                self.assertIsNone(proc.poll())
                self.assertEqual(manager.entries["m1"].restart_count_total, 0)
                manager.shutdown()

    def test_unreadable_playlist_is_no_evidence(self):
        manager = self._make()
        t0 = time.time()
        entry = self._produce_then_stop(manager, t0)
        proc = entry.process
        (entry.directory / "index.m3u8").unlink()
        manager._reap_tick(t0 + 400)
        self.assertIs(manager.entries["m1"].process, proc)
        self.assertEqual(manager.entries["m1"].restart_count_total, 0)

    def test_stall_seconds_env(self):
        name = "GIF_TEST_PREVIEW_STALL_SECONDS"
        self.assertEqual(non_negative_env_float(name + "_UNSET", 20.0), 20.0)
        for raw, expected in (("", 20.0), ("0", 0.0), ("25", 25.0), (" 7.5 ", 7.5)):
            with mock.patch.dict("os.environ", {name: raw}):
                self.assertEqual(non_negative_env_float(name, 20.0), expected)
        for raw in ("-1", "abc", "nan", "inf"):
            with mock.patch.dict("os.environ", {name: raw}):
                with self.assertRaises(RuntimeError):
                    non_negative_env_float(name, 20.0)


def _live_with_segments_factory():
    """A child that writes a two-segment playlist and keeps running (healthy source)."""

    def _build(resource, directory):
        playlist = directory / "index.m3u8"
        script = (
            "import pathlib, time\n"
            f"pathlib.Path(r'{directory / 'seg_00000.ts'}').write_bytes(b'ts')\n"
            f"pathlib.Path(r'{directory / 'seg_00001.ts'}').write_bytes(b'ts')\n"
            f"pathlib.Path(r'{playlist}').write_text({PLAYLIST_WITH_SEGMENTS!r}, encoding='utf-8')\n"
            "time.sleep(30)\n"
        )
        return [sys.executable, "-c", script]

    return _build


class LivePreviewSourceSwitchTests(unittest.TestCase):
    """switch_resource: another source, same directory, history kept (11-09)."""

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
        self.built = []

    def _cleanup(self):
        self.manager.shutdown()
        self._tmp.cleanup()

    def _use(self, factory):
        def _build(resource, directory):
            self.built.append(resource)
            return factory(resource, directory)

        self.manager._build_command = _build

    def _start_live(self, resource="rtmp://h/wyc_live/pt-a"):
        self._use(_live_with_segments_factory())
        self.manager.start("m1", resource)
        playlist = self.manager.media_dir("m1") / "index.m3u8"
        deadline = time.time() + 5
        while not playlist.is_file() and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(playlist.is_file())
        return self.manager.media_dir("m1")

    def test_switch_keeps_history_and_marks_the_boundary(self):
        media = self._start_live()
        self._use(_silent_command_factory())
        with mock.patch("live_preview._log_event") as log_event:
            status = self.manager.switch_resource("m1", "rtmp://h/wyc_live/sd-b")
        self.assertEqual(status["resource"], "rtmp://h/wyc_live/sd-b")
        self.assertTrue(status["running"])
        self.assertTrue(status["history_frozen"])
        self.assertTrue(status["switching"])
        self.assertEqual(status["drops_recent"], 0)
        self.assertIsNotNone(status["switched_at_unix"])
        self.assertEqual(self.built, ["rtmp://h/wyc_live/pt-a", "rtmp://h/wyc_live/sd-b"])
        # Same directory, old segments untouched: scrubbing back still works.
        self.assertEqual(self.manager.media_dir("m1"), media)
        self.assertTrue((media / "seg_00000.ts").is_file())
        self.assertTrue((media / "seg_00001.ts").is_file())
        self.assertEqual(self.manager.switch_boundaries("m1"), [1])
        events = {c.args[0]: c.kwargs for c in log_event.call_args_list}
        self.assertEqual(events["preview_ffmpeg_exited"]["reason"], live_preview.SWITCH_EXIT_REASON)
        self.assertEqual(events["preview_resource_switched"]["new"], "rtmp://h/wyc_live/sd-b")
        self.assertEqual(events["preview_resource_switched"]["boundary_segment"], 1)
        entry = self.manager.entries["m1"]
        self.assertEqual((entry.restart_count, entry.cooling, entry.next_restart_at), (0, False, 0.0))

    def test_switch_to_the_same_source_does_nothing(self):
        self._start_live()
        before = self.manager.status("m1")["restart_count_total"]
        status = self.manager.switch_resource("m1", "rtmp://h/wyc_live/pt-a")
        self.assertEqual(status["restart_count_total"], before)
        self.assertEqual(self.built, ["rtmp://h/wyc_live/pt-a"])
        self.assertEqual(self.manager.switch_boundaries("m1"), [])

    def test_switch_without_a_preview_returns_none(self):
        self.assertIsNone(self.manager.switch_resource("nope", "rtmp://h/x"))
        self.assertEqual(self.manager.switch_boundaries("nope"), [])
        with self.assertRaises(ValueError):
            self.manager.switch_resource("nope", "  ")

    def test_drops_count_picture_breaks_not_refails_and_reset_on_switch(self):
        manager = self.manager
        self._use(_dying_with_segments_factory())
        manager.start("m1", "rtmp://h/wyc_live/pt-a")
        manager.entries["m1"].process.wait(timeout=5)
        # The run had delivered two segments, then the source dropped: one break.
        self._use(_silent_command_factory())
        manager._reap_tick(time.time())
        self.assertEqual(manager.status("m1")["drops_recent"], 1)
        # The replacement dies before delivering anything: the same break.
        entry = manager.entries["m1"]
        entry.process.terminate()
        entry.process.wait(timeout=5)
        manager._reap_tick(time.time())
        self.assertEqual(manager.status("m1")["drops_recent"], 1)
        self.assertEqual(manager.status("m1")["drops_window_seconds"], 600)
        self.assertFalse(manager.status("m1")["switching"])
        # A source switch starts the count over for the new source.
        manager.switch_resource("m1", "rtmp://h/wyc_live/sd-b")
        self.assertEqual(manager.status("m1")["drops_recent"], 0)
        # Old breaks age out of the window.
        manager.entries["m1"].drop_times = [time.time() - 601]
        self.assertEqual(manager.status("m1")["drops_recent"], 0)

    def test_a_source_that_never_delivered_is_not_counted_as_breaking(self):
        # 2342dba review (11-09): before kick-off several sources are not
        # pushing yet; each retry of one that never gave a segment counted as
        # a break (12 retries -> drops_recent 12) and the workshop suggested
        # switching away from a source that had simply not started.
        manager = self.manager
        for write_playlist in (False, True):  # no playlist / empty playlist
            self._use(_dying_command_factory(write_playlist=write_playlist))
            manager.start("m1", f"rtmp://h/wyc_live/sd-{int(write_playlist)}")
            for _ in range(4):
                manager.entries["m1"].process.wait(timeout=5)
                manager._reap_tick(time.time())
            self.assertGreaterEqual(manager.status("m1")["restart_count_total"], 3)
            self.assertEqual(manager.status("m1")["drops_recent"], 0)
            manager.stop("m1")
        # Once it does deliver and then drops, that is one break.
        self._use(_dying_with_segments_factory())
        manager.start("m1", "rtmp://h/wyc_live/sd-2")
        manager.entries["m1"].process.wait(timeout=5)
        self._use(_dying_command_factory(write_playlist=False))
        for _ in range(3):
            manager._reap_tick(time.time())
            manager.entries["m1"].process.wait(timeout=5)
        self.assertEqual(manager.status("m1")["drops_recent"], 1)

if __name__ == "__main__":
    unittest.main()
