"""Deterministic tests for the manual monitor service (port 8901)."""

from __future__ import annotations

import os
import json
import shutil
import signal
import subprocess
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

# The monitor reads GIF_SOURCE_SECRET lazily; a placeholder keeps import clean.
os.environ.setdefault("GIF_SOURCE_SECRET", "test-secret")
# Keep tests local-only: do not build the publish stack or start a draft worker.
os.environ.setdefault("GIF_MONITOR_PUBLISH_ENABLED", "false")
# Do not touch the real alert database; persistence has its own temp-dir tests.
os.environ.setdefault("GIF_MONITOR_ALERT_DB_PATH", "")
# A developer .env with real Feishu credentials would turn on the login wall
# and 401 every endpoint test; feishu_auth has its own test module.
os.environ["GIF_MONITOR_FEISHU_APP_ID"] = ""
os.environ["GIF_MONITOR_FEISHU_APP_SECRET"] = ""

import monitor_server
import live_preview
from dqd_api import positive_env_int
from monitor_server import (
    _encode_clip_gif,
    _parse_pdt,
    _read_playlist_segments,
    _select_clip_window,
)


class PlaylistParsingTests(unittest.TestCase):
    def test_reads_segments_and_durations_in_order(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for name in ("seg_00000.ts", "seg_00001.ts"):
                (directory / name).write_bytes(b"x")
            (directory / "index.m3u8").write_text(
                "#EXTM3U\n#EXT-X-VERSION:6\n"
                "#EXTINF:2.000000,\nseg_00000.ts\n"
                "#EXTINF:1.500000,\nseg_00001.ts\n",
                encoding="utf-8",
            )
            segments = _read_playlist_segments(directory)
            self.assertEqual([p.name for p, _, _ in segments], ["seg_00000.ts", "seg_00001.ts"])
            self.assertAlmostEqual(segments[0][1], 2.0)
            self.assertAlmostEqual(segments[1][1], 1.5)
            self.assertIsNone(segments[0][2])

    def test_parses_program_date_time(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for name in ("seg_00000.ts", "seg_00001.ts"):
                (directory / name).write_bytes(b"x")
            (directory / "index.m3u8").write_text(
                "#EXTM3U\n#EXT-X-VERSION:6\n"
                "#EXT-X-PROGRAM-DATE-TIME:2026-10-14T20:00:00.000+08:00\n"
                "#EXTINF:2.000000,\nseg_00000.ts\n"
                "#EXT-X-PROGRAM-DATE-TIME:2026-10-14T20:00:02.000+08:00\n"
                "#EXTINF:2.000000,\nseg_00001.ts\n",
                encoding="utf-8",
            )
            segments = _read_playlist_segments(directory)
            expected = _parse_pdt("2026-10-14T20:00:00.000+08:00")
            self.assertAlmostEqual(segments[0][2], expected)
            self.assertAlmostEqual(segments[1][2], expected + 2.0)

    def test_skips_missing_segment_files(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / "seg_00000.ts").write_bytes(b"x")
            (directory / "index.m3u8").write_text(
                "#EXTM3U\n#EXTINF:2.0,\nseg_00000.ts\n#EXTINF:2.0,\nseg_99999.ts\n",
                encoding="utf-8",
            )
            segments = _read_playlist_segments(directory)
            self.assertEqual([p.name for p, _, _ in segments], ["seg_00000.ts"])

    def test_missing_playlist_returns_empty(self):
        with TemporaryDirectory() as tmp:
            self.assertEqual(_read_playlist_segments(Path(tmp)), [])


class PdtParsingTests(unittest.TestCase):
    def test_colonless_offset_matches_colon_offset(self):
        # ffmpeg writes a colon-less offset; it must parse identically to the
        # RFC form so Python < 3.11 does not silently drop every anchor.
        self.assertEqual(
            _parse_pdt("2026-10-14T16:02:14.193+0800"),
            _parse_pdt("2026-10-14T16:02:14.193+08:00"),
        )
        self.assertIsNotNone(_parse_pdt("2026-10-14T16:02:14.193+0800"))

    def test_zulu_offset_parses(self):
        self.assertAlmostEqual(
            _parse_pdt("2026-10-14T08:02:14.193Z"),
            _parse_pdt("2026-10-14T16:02:14.193+08:00"),
        )


class ClipWindowSelectionTests(unittest.TestCase):
    def _segments(self, base: float, count: int):
        # Ten 2-second segments on a PDT timeline starting at ``base``.
        return [(Path(f"seg_{i:05d}.ts"), 2.0, base + i * 2.0) for i in range(count)]

    def test_anchor_on_pdt_ignores_request_delay(self):
        base = 1_000_000.0
        segments = self._segments(base, 10)  # covers [base, base+20)
        # Operator saw the frame at base+16; requesting a 15s window.
        anchor = base + 16.0
        kept, start_offset, window = _select_clip_window(segments, 15.0, anchor_epoch=anchor)
        first_kept_pdt = kept[0][2]
        # The absolute start of the cut is want_start = anchor - 15 = base + 1.
        self.assertAlmostEqual(first_kept_pdt + start_offset, anchor - 15.0)
        self.assertAlmostEqual(window, 15.0)
        # Anchor must not pull in frames newer than the one the operator saw.
        self.assertLess(kept[0][2], anchor)

    def test_future_anchor_is_clamped_to_latest_frame(self):
        base = 1_000_000.0
        segments = self._segments(base, 10)  # latest end = base+20
        kept, start_offset, window = _select_clip_window(
            segments, 15.0, anchor_epoch=base + 999.0
        )
        self.assertLessEqual((kept[0][2] + start_offset) + window, base + 20.0 + 1e-6)

    def test_fallback_without_anchor_uses_buffer_tail(self):
        base = 1_000_000.0
        segments = self._segments(base, 10)
        kept, start_offset, window = _select_clip_window(segments, 15.0, anchor_epoch=None)
        self.assertAlmostEqual(window, 15.0)
        self.assertGreaterEqual(sum(s[1] for s in kept), 15.0)

    def test_explicit_start_overrides_before_seconds(self):
        base = 1_000_000.0
        segments = self._segments(base, 10)  # covers [base, base+20)
        start, end = base + 3.0, base + 11.5
        kept, start_offset, window = _select_clip_window(
            segments, 99.0, anchor_epoch=end, start_epoch=start
        )
        self.assertAlmostEqual(kept[0][2] + start_offset, start)
        self.assertAlmostEqual(window, 8.5)
        # Exactly the segments overlapping [start, end): seg 1..5.
        self.assertEqual([s[0].name for s in kept], [f"seg_{i:05d}.ts" for i in range(1, 6)])

    def test_explicit_start_after_anchor_collapses_to_tail(self):
        base = 1_000_000.0
        segments = self._segments(base, 10)
        kept, start_offset, window = _select_clip_window(
            segments, 10.0, anchor_epoch=base + 5.0, start_epoch=base + 9.0
        )
        # Degenerate range must still yield a positive, bounded window.
        self.assertGreater(window, 0)
        self.assertLessEqual(window, 10.0)


class ClipPinStoreTests(unittest.TestCase):
    def _segments(self, directory: Path, base: float, count: int):
        segments = []
        for i in range(count):
            path = directory / f"seg_{i:05d}.ts"
            path.write_bytes(b"x" * 16)
            segments.append((path, 2.0, base + i * 2.0))
        return segments

    def test_pin_hard_links_start_region_and_survives_deletion(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            base = 1_000_000.0
            segments = self._segments(directory, base, 20)  # [base, base+40)
            store = monitor_server._ClipPinStore(ttl_seconds=60.0, max_range_seconds=10.0)
            with mock.patch.object(monitor_server, "_clip_dir", return_value=directory / "clips"):
                pin = store.create("1", base + 6.0, segments)
            # Pinned: segments overlapping [start-1, start+max_range+2] = [5, 18] → seg 2..8.
            self.assertEqual(sorted(pin["segments"]), [f"seg_{i:05d}.ts" for i in range(2, 9)])
            for path, _d, _p in segments:
                path.unlink()
            merged = store.merged_segments(pin, [])
            self.assertEqual(len(merged), 7)
            for path, _d, _p in merged:
                self.assertTrue(path.is_file(), path)
            store.release(pin["pin_id"])
            self.assertFalse(pin["link_dir"].exists())

    def test_pin_absorbs_segments_that_appear_later(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            base = 1_000_000.0
            early = self._segments(directory, base, 4)  # [base, base+8)
            store = monitor_server._ClipPinStore(ttl_seconds=60.0, max_range_seconds=10.0)
            with mock.patch.object(monitor_server, "_clip_dir", return_value=directory / "clips"):
                pin = store.create("1", base + 6.0, early)
            self.assertEqual(sorted(pin["segments"]), ["seg_00002.ts", "seg_00003.ts"])
            later = self._segments(directory, base, 12)  # now [base, base+24)
            merged = store.merged_segments(pin, later)
            # Newly arrived seg 4..8 fall inside the pin window and are linked.
            self.assertIn("seg_00008.ts", pin["segments"])
            self.assertNotIn("seg_00009.ts", pin["segments"])
            self.assertEqual([s[2] for s in merged], [base + i * 2.0 for i in range(12)])
            store.shutdown()

    def test_pin_expires_after_ttl(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            segments = self._segments(directory, 1_000_000.0, 4)
            store = monitor_server._ClipPinStore(ttl_seconds=5.0, max_range_seconds=10.0)
            with mock.patch.object(monitor_server, "_clip_dir", return_value=directory / "clips"):
                pin = store.create("1", 1_000_002.0, segments)
            self.assertIsNotNone(store.get(pin["pin_id"], "1"))
            self.assertIsNone(store.get(pin["pin_id"], "other-match"))
            with mock.patch.object(monitor_server.time, "time", return_value=pin["created_at"] + 6.0):
                self.assertIsNone(store.get(pin["pin_id"], "1"))
            self.assertFalse(pin["link_dir"].exists())

    def test_reaper_expires_pin_without_any_request(self):
        # A start the operator abandons must be freed by the timer, not wait
        # for the next create/get or for process exit.
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            segments = self._segments(directory, 1_000_000.0, 4)
            store = monitor_server._ClipPinStore(ttl_seconds=0.05, max_range_seconds=10.0)
            with mock.patch.object(monitor_server, "_clip_dir", return_value=directory / "clips"):
                pin = store.create("1", 1_000_002.0, segments)
            self.assertTrue(pin["link_dir"].exists())
            store.start_reaper(interval_seconds=0.01)
            try:
                deadline = time.time() + 2.0
                while pin["link_dir"].exists() and time.time() < deadline:
                    time.sleep(0.01)
                self.assertFalse(pin["link_dir"].exists())
            finally:
                store.shutdown()

    def test_sweep_orphans_removes_leftover_pin_dirs_only(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            orphan = root / "54622319" / ".pin_deadbeef"
            orphan.mkdir(parents=True)
            (orphan / "seg_00001.ts").write_bytes(b"x")
            keep_gif = root / "54622319" / "clip.gif"
            keep_gif.write_bytes(b"GIF")
            other_dir = root / "54622319" / "not_a_pin"
            other_dir.mkdir()
            self.assertEqual(monitor_server._ClipPinStore.sweep_orphans(root), 1)
            self.assertFalse(orphan.exists())
            self.assertTrue(keep_gif.exists())
            self.assertTrue(other_dir.exists())
            self.assertEqual(monitor_server._ClipPinStore.sweep_orphans(root / "missing"), 0)


class ClipRangeEndpointTests(unittest.TestCase):
    """Pin + range clip over the Flask API with a fake playlist on disk."""

    def setUp(self):
        self.client = monitor_server.app.test_client()
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.base = 1_700_000_000.0
        self.segments = []
        lines = ["#EXTM3U", "#EXT-X-TARGETDURATION:2"]
        for i in range(10):
            path = self.dir / f"seg_{i:05d}.ts"
            path.write_bytes(b"x")
            pdt = self.base + i * 2.0
            iso = monitor_server.datetime.fromtimestamp(pdt).astimezone().isoformat(timespec="milliseconds")
            lines += [f"#EXT-X-PROGRAM-DATE-TIME:{iso}", "#EXTINF:2.000,", path.name]
        (self.dir / "index.m3u8").write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.store = monitor_server._ClipPinStore(60.0, 30.0)
        self._patches = [
            mock.patch.object(monitor_server, "clip_pins", self.store),
            mock.patch.object(monitor_server, "manual_draft_queue", None),
            mock.patch.object(monitor_server, "CLIP_RANGE_MAX_SECONDS", 30.0),
            mock.patch.object(monitor_server, "_clip_dir", return_value=self.dir / "clips"),
            mock.patch.object(monitor_server.live_preview, "status", return_value={"running": True}),
            mock.patch.object(monitor_server.live_preview, "media_dir", return_value=self.dir),
            mock.patch.object(monitor_server, "_encode_clip_gif", side_effect=self._fake_encode),
        ]
        for patch in self._patches:
            patch.start()
        self.encode_calls = []

    def tearDown(self):
        for patch in self._patches:
            patch.stop()
        self.store.shutdown()
        self.tmp.cleanup()

    def _fake_encode(self, segments, before_seconds, output_path, anchor_epoch=None, start_epoch=None):
        self.encode_calls.append((before_seconds, anchor_epoch, start_epoch))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"GIF")

    def _pin(self, start_offset: float):
        return self.client.post(
            "/api/monitor/clip/pin",
            json={"match_id": "54622319", "start_epoch_ms": round((self.base + start_offset) * 1000)},
        )

    def test_pin_then_range_clip_passes_start_and_end(self):
        pinned = self._pin(4.0)
        self.assertEqual(pinned.status_code, 200, pinned.get_json())
        pin_id = pinned.get_json()["pin_id"]
        response = self.client.post(
            "/api/monitor/clip",
            json={"match_id": "54622319", "pin_id": pin_id, "end_epoch_ms": round((self.base + 12.5) * 1000)},
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        before, anchor, start = self.encode_calls[-1]
        self.assertAlmostEqual(start, self.base + 4.0, places=3)
        self.assertAlmostEqual(anchor, self.base + 12.5, places=3)
        self.assertAlmostEqual(before, 8.5, places=3)
        # Pin is released after a successful clip.
        self.assertIsNone(self.store.get(pin_id, "54622319"))

    def test_range_longer_than_max_is_rejected(self):
        pin_id = self._pin(0.0).get_json()["pin_id"]
        response = self.client.post(
            "/api/monitor/clip",
            json={"match_id": "54622319", "pin_id": pin_id, "end_epoch_ms": round((self.base + 31.0) * 1000)},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "range_too_long")
        self.assertEqual(self.encode_calls, [])
        # Pin stays alive so the operator can pick a shorter end.
        self.assertIsNotNone(self.store.get(pin_id, "54622319"))

    def test_end_before_start_is_rejected(self):
        pin_id = self._pin(10.0).get_json()["pin_id"]
        response = self.client.post(
            "/api/monitor/clip",
            json={"match_id": "54622319", "pin_id": pin_id, "end_epoch_ms": round((self.base + 9.0) * 1000)},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "range_invalid")

    def test_unknown_pin_is_rejected(self):
        response = self.client.post(
            "/api/monitor/clip",
            json={"match_id": "54622319", "pin_id": "nope", "end_epoch_ms": round((self.base + 9.0) * 1000)},
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "pin_expired")

    def test_pin_start_outside_buffer_is_rejected(self):
        response = self._pin(-30.0)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "anchor_out_of_buffer")

    def test_delete_pin_releases_it(self):
        pin_id = self._pin(2.0).get_json()["pin_id"]
        response = self.client.delete(f"/api/monitor/clip/pin/{pin_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(self.store.get(pin_id, "54622319"))

    def test_plain_clip_without_pin_is_unchanged(self):
        response = self.client.post(
            "/api/monitor/clip",
            json={"match_id": "54622319", "anchor_epoch_ms": round((self.base + 15.0) * 1000), "before_seconds": 5},
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        before, anchor, start = self.encode_calls[-1]
        self.assertEqual(before, 5.0)
        self.assertIsNone(start)

    def _run_through_real_encoder(self, body: dict):
        """POST /api/monitor/clip with the *real* ``_encode_clip_gif``.

        Only ``subprocess.run`` (ffmpeg) is faked. This pins the call contract
        between the endpoint and the encoder: a signature drift between the two
        surfaces here as a 500, whereas every other test in this class mocks the
        encoder away and would stay green.
        """
        ffmpeg_cmds = []

        def fake_run(command, **kwargs):
            ffmpeg_cmds.append(command)
            Path(command[-1]).write_bytes(b"GIF89a")
            return subprocess.CompletedProcess(command, 0, stdout=b"", stderr=b"")

        self._patches[-1].stop()  # un-mock _encode_clip_gif for this request
        try:
            with mock.patch.object(monitor_server.subprocess, "run", side_effect=fake_run):
                response = self.client.post("/api/monitor/clip", json=body)
        finally:
            self._patches[-1].start()
        return response, ffmpeg_cmds

    def test_endpoint_reaches_real_encoder_in_plain_mode(self):
        response, cmds = self._run_through_real_encoder(
            {"match_id": "54622319", "anchor_epoch_ms": round((self.base + 15.0) * 1000), "before_seconds": 5},
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(cmds), 1)
        ss = float(cmds[0][cmds[0].index("-ss") + 1])
        t = float(cmds[0][cmds[0].index("-t") + 1])
        self.assertAlmostEqual(t, 5.0, places=2)
        # Window [base+10, base+15) starts at seg_00005 (pdt base+10) → offset 0.
        self.assertAlmostEqual(ss, 0.0, places=2)

    def test_endpoint_reaches_real_encoder_in_range_mode(self):
        pin_id = self._pin(4.0).get_json()["pin_id"]
        response, cmds = self._run_through_real_encoder(
            {"match_id": "54622319", "pin_id": pin_id, "end_epoch_ms": round((self.base + 12.5) * 1000)},
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(cmds), 1)
        ss = float(cmds[0][cmds[0].index("-ss") + 1])
        t = float(cmds[0][cmds[0].index("-t") + 1])
        # Start base+4 falls inside seg_00002 (pdt base+4) → offset 0; span 8.5s.
        self.assertAlmostEqual(ss, 0.0, places=2)
        self.assertAlmostEqual(t, 8.5, places=2)
        self.assertIsNone(self.store.get(pin_id, "54622319"))


class ClipListTests(unittest.TestCase):
    """``GET /api/monitor/clips``: newest-first GIFs joined to draft records."""

    def setUp(self):
        self.client = monitor_server.app.test_client()
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self._patches = [
            mock.patch.object(monitor_server, "_clip_dir", return_value=self.dir),
            mock.patch.object(monitor_server, "CLIP_LIST_LIMIT", 50),
        ]
        for patch in self._patches:
            patch.start()

    def tearDown(self):
        for patch in self._patches:
            patch.stop()
        self.tmp.cleanup()

    def _gif(self, name: str, mtime: float, size: int = 10) -> Path:
        path = self.dir / name
        path.write_bytes(b"G" * size)
        os.utime(path, (mtime, mtime))
        return path

    def test_lists_newest_first_and_skips_non_clips(self):
        self._gif("clip_a.gif", 1_000.0)
        self._gif("clip_b.gif", 3_000.0)
        self._gif("clip_c.gif", 2_000.0)
        (self.dir / "other.gif").write_bytes(b"x")          # not a clip
        (self.dir / "clip_d.txt").write_bytes(b"x")         # wrong suffix
        (self.dir / ".pin_abc").mkdir()                     # pin staging dir
        with mock.patch.object(monitor_server, "manual_draft_queue", None):
            response = self.client.get("/api/monitor/clips?match_id=54622319")
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertFalse(body["publish_enabled"])
        self.assertEqual([c["name"] for c in body["clips"]], ["clip_b.gif", "clip_c.gif", "clip_a.gif"])
        self.assertEqual(body["clips"][0]["clip_url"], "clips/54622319/clip_b.gif")
        self.assertIsNone(body["clips"][0]["event_key"])

    def test_joins_draft_records_by_gif_name(self):
        self._gif("clip_a.gif", 1_000.0)
        self._gif("clip_b.gif", 2_000.0)
        queue = mock.Mock()
        queue.records_by_source_name.return_value = {
            "clip_b.gif": {"event_key": "manual_42", "status": "drafted", "draft_url": "https://d/1", "error": None, "retriable": False},
        }
        with mock.patch.object(monitor_server, "manual_draft_queue", queue):
            response = self.client.get("/api/monitor/clips?match_id=54622319")
        body = response.get_json()
        self.assertTrue(body["publish_enabled"])
        by_name = {c["name"]: c for c in body["clips"]}
        self.assertEqual(by_name["clip_b.gif"]["event_key"], "manual_42")
        self.assertEqual(by_name["clip_b.gif"]["draft_status"], "drafted")
        self.assertEqual(by_name["clip_b.gif"]["draft_url"], "https://d/1")
        self.assertIsNone(by_name["clip_a.gif"]["event_key"])
        queue.records_by_source_name.assert_called_once_with(
            "54622319", artifact_kind=monitor_server.MANUAL_ARTIFACT_KIND
        )

    def test_limit_applies_after_sorting(self):
        for i in range(5):
            self._gif(f"clip_{i}.gif", 1_000.0 + i)
        with mock.patch.object(monitor_server, "CLIP_LIST_LIMIT", 2), \
             mock.patch.object(monitor_server, "manual_draft_queue", None):
            body = self.client.get("/api/monitor/clips?match_id=54622319").get_json()
        self.assertEqual([c["name"] for c in body["clips"]], ["clip_4.gif", "clip_3.gif"])

    def test_missing_directory_is_empty_list(self):
        with mock.patch.object(monitor_server, "_clip_dir", return_value=self.dir / "nope"), \
             mock.patch.object(monitor_server, "manual_draft_queue", None):
            body = self.client.get("/api/monitor/clips?match_id=54622319").get_json()
        self.assertEqual(body["clips"], [])

    def test_invalid_match_id_rejected(self):
        response = self.client.get("/api/monitor/clips?match_id=../etc")
        self.assertEqual(response.status_code, 400)


class ClipEncodingTests(unittest.TestCase):
    def _make_segments(self, directory: Path, count: int) -> list:
        """Produce real 2-second MPEG-TS segments with ffmpeg for a true clip."""
        ffmpeg = monitor_server.resolve_ffmpeg()
        segments = []
        for index in range(count):
            path = directory / f"seg_{index:05d}.ts"
            command = [
                ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                "-f", "lavfi", "-i", "color=c=green:s=320x180:d=2:r=10",
                "-c:v", "libx264", "-pix_fmt", "yuv420p", "-t", "2",
                "-f", "mpegts", str(path),
            ]
            result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            if result.returncode != 0:
                self.skipTest("本机 ffmpeg 无法生成测试 TS 片段")
            segments.append((path, 2.0, None))
        return segments

    def test_encode_clip_gif_produces_file(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            segments = self._make_segments(directory, 10)
            output = directory / "clip.gif"
            _encode_clip_gif(segments, before_seconds=15.0, output_path=output)
            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)
            # Neither the concat list nor the hard-link staging dir may linger.
            self.assertEqual(list(directory.glob(".concat_*.txt")), [])
            self.assertEqual(list(directory.glob(".src_*")), [])

    def test_encode_clip_gif_with_explicit_range(self):
        """Real ffmpeg run through the 5-argument (anchor + start) signature.

        Also asserts the operator-chosen start actually drives ``-ss``/``-t``
        rather than being silently ignored.
        """
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            base = 1_700_000_000.0
            segments = [
                (path, duration, base + index * 2.0)
                for index, (path, duration, _pdt) in enumerate(self._make_segments(directory, 6))
            ]
            output = directory / "range.gif"
            real_run = subprocess.run
            seen: list[list[str]] = []

            def spy_run(command, **kwargs):
                seen.append(command)
                return real_run(command, **kwargs)

            with mock.patch.object(subprocess, "run", spy_run):
                _encode_clip_gif(
                    segments, before_seconds=99.0, output_path=output,
                    anchor_epoch=base + 7.0, start_epoch=base + 3.0,
                )
            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)
            self.assertEqual(len(seen), 1)
            ss = float(seen[0][seen[0].index("-ss") + 1])
            t = float(seen[0][seen[0].index("-t") + 1])
            # kept = seg 1..3 (pdt base+2..base+6); start base+3 is 1s into seg 1.
            self.assertAlmostEqual(ss, 1.0, places=2)
            self.assertAlmostEqual(t, 4.0, places=2)

    def test_encode_survives_segments_deleted_mid_encode(self):
        """The preview ffmpeg deletes its oldest segments while we encode.

        ``_encode_clip_gif`` hard-links the chosen segments first, so unlinking
        the originals only drops a reference: the inode and its data stay alive
        until the staging links go away. This reproduces the delete race by
        removing every original *after* the links are made but *before* ffmpeg
        reads them, and asserts the GIF is still produced.
        """
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            segments = self._make_segments(directory, 10)
            output = directory / "clip.gif"
            real_run = subprocess.run
            deleted: list[Path] = []

            def run_after_deleting_originals(*args, **kwargs):
                for path, _duration, _pdt in segments:
                    if path.exists():
                        path.unlink()
                        deleted.append(path)
                return real_run(*args, **kwargs)

            with mock.patch.object(subprocess, "run", run_after_deleting_originals):
                _encode_clip_gif(segments, before_seconds=15.0, output_path=output)

            # Guard the guard: if nothing was deleted the test proves nothing.
            self.assertEqual(len(deleted), len(segments))
            for path in deleted:
                self.assertFalse(path.exists())
            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)
            self.assertEqual(list(directory.glob(".src_*")), [])

    def test_hard_link_outlives_original_unlink(self):
        """Pin the OS assumption the race fix rests on.

        If this ever fails on the deployment filesystem, hard-linking cannot
        protect the clip encoder and the fix needs a different mechanism.
        """
        with TemporaryDirectory() as tmp:
            directory = Path(tmp)
            original = directory / "original.ts"
            original.write_bytes(b"payload")
            link = directory / "link.ts"
            os.link(original, link)
            original.unlink()
            self.assertFalse(original.exists())
            self.assertEqual(link.read_bytes(), b"payload")


class EventAlertTests(unittest.TestCase):
    """Cover the goal/red-card alert ingest + polling + Feishu fan-out."""

    def setUp(self):
        self.client = monitor_server.app.test_client()
        self._patches = [
            mock.patch.object(monitor_server, "alert_store", monitor_server._AlertStore(50)),
            mock.patch.object(monitor_server, "ALERT_ENABLED", True),
            mock.patch.object(
                monitor_server, "ALERT_EVENT_TYPES", frozenset({"goal", "red_card"})
            ),
            mock.patch.object(monitor_server, "ALERT_INGEST_TOKEN", ""),
        ]
        for patch in self._patches:
            patch.start()
        self.catalog = mock.patch.object(
            monitor_server.match_catalog,
            "snapshot",
            return_value={
                "playing": [
                    {
                        "match_id": "54622319",
                        "team_A_name": "伊尔韦斯",
                        "team_B_name": "里耶卡",
                        "competition_name": "哥伦杯",
                    }
                ]
            },
        )
        self.catalog.start()
        self.feishu = mock.patch.object(monitor_server, "_send_feishu_text", return_value=True)
        self.feishu_mock = self.feishu.start()

    def tearDown(self):
        self.feishu.stop()
        self.catalog.stop()
        for patch in self._patches:
            patch.stop()

    def _ingest(self, **overrides):
        body = {
            "match_id": "54622319",
            "event_type": "goal",
            "team": "A",
            "team_id": "50002103",
            "from": 1,
            "to": 2,
            "score": "2-1",
            "minute": "1H 41",
            "dedup_key": "k1",
        }
        body.update(overrides)
        return self.client.post("/api/monitor/events/ingest", json=body)

    def test_accepts_goal_enriches_name_and_pushes_feishu(self):
        data = self._ingest().get_json()
        self.assertTrue(data["accepted"])
        self.assertEqual(data["seq"], 1)
        self.assertTrue(data["feishu_pushed"])
        text = self.feishu_mock.call_args[0][0]
        self.assertIn("进球", text)
        self.assertIn("伊尔韦斯", text)  # resolved by A/B side, not team_id
        self.assertIn("50002103", text)  # team_id still shown
        self.assertIn("1→2", text)

    def test_side_b_resolves_other_team_name(self):
        self._ingest(team="B", team_id="50000371", dedup_key="kb")
        text = self.feishu_mock.call_args[0][0]
        self.assertIn("里耶卡", text)

    def test_accepts_red_card(self):
        data = self._ingest(event_type="red_card", dedup_key="kr").get_json()
        self.assertTrue(data["accepted"])

    def test_filters_non_capturable_match(self):
        data = self._ingest(match_id="99999999", dedup_key="kx").get_json()
        self.assertFalse(data["accepted"])
        self.assertEqual(data["reason"], "match_not_capturable")
        # Not even in the full Playing set: traceable as "not_in_catalog".
        self.assertEqual(data["detail_reason"], "not_in_catalog")
        self.feishu_mock.assert_not_called()

    def test_drop_reason_beyond_catalog_limit(self):
        # A match present in the full set but absent from the shown playing list
        # (e.g. a transient playing/all_playing skew) is still traceable.
        all_playing = [
            {"match_id": "77777777", "team_A_name": "甲队",
             "team_B_name": "乙队", "competition_name": "哥伦杯"}
        ]
        with mock.patch.object(monitor_server.match_catalog, "all_playing", all_playing):
            data = self._ingest(match_id="77777777", dedup_key="kbc").get_json()
        self.assertFalse(data["accepted"])
        self.assertEqual(data["detail_reason"], "beyond_catalog_limit")
        self.feishu_mock.assert_not_called()

    def test_filters_unwanted_event_type(self):
        data = self._ingest(event_type="corner", dedup_key="kc").get_json()
        self.assertFalse(data["accepted"])
        self.assertEqual(data["reason"], "event_type_filtered")
        self.feishu_mock.assert_not_called()

    def test_deduplicates_repeated_event(self):
        self.assertTrue(self._ingest(dedup_key="dup").get_json()["accepted"])
        second = self._ingest(dedup_key="dup").get_json()
        self.assertFalse(second["accepted"])
        self.assertEqual(second["reason"], "duplicate")
        self.assertEqual(self.feishu_mock.call_count, 1)

    def test_events_poll_respects_cursor(self):
        self._ingest(dedup_key="a")
        self._ingest(event_type="red_card", dedup_key="b")
        full = self.client.get("/api/monitor/events?since=0").get_json()
        self.assertEqual([e["seq"] for e in full["events"]], [1, 2])
        self.assertEqual(full["seq"], 2)
        partial = self.client.get("/api/monitor/events?since=1").get_json()
        self.assertEqual([e["seq"] for e in partial["events"]], [2])

    def test_events_poll_exposes_boot_id(self):
        # The browser uses boot_id to detect an 8901 restart (seq reset to 0) and
        # re-baseline its cursor, so an already-open page keeps getting alerts.
        data = self.client.get("/api/monitor/events?since=0").get_json()
        self.assertEqual(data["boot_id"], monitor_server.BOOT_ID)
        self.assertTrue(data["boot_id"])

    def test_feishu_failure_still_accepts(self):
        self.feishu_mock.return_value = False
        data = self._ingest(dedup_key="f").get_json()
        self.assertTrue(data["accepted"])
        self.assertFalse(data["feishu_pushed"])

    def test_alerts_disabled_rejects(self):
        with mock.patch.object(monitor_server, "ALERT_ENABLED", False):
            data = self._ingest(dedup_key="d").get_json()
        self.assertFalse(data["accepted"])
        self.assertEqual(data["reason"], "alerts_disabled")

    def test_ingest_token_enforced(self):
        with mock.patch.object(monitor_server, "ALERT_INGEST_TOKEN", "secret"):
            unauthorized = self._ingest(dedup_key="t1")
            self.assertEqual(unauthorized.status_code, 401)
            ok = self.client.post(
                "/api/monitor/events/ingest",
                json={"match_id": "54622319", "event_type": "goal", "team": "A", "dedup_key": "t2"},
                headers={"X-Monitor-Token": "secret"},
            )
            self.assertTrue(ok.get_json()["accepted"])


class AlertStorePersistenceTests(unittest.TestCase):
    """``_AlertStore`` survives a restart via its SQLite mirror."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.db = Path(self.tmp.name) / "alerts.sqlite3"

    def tearDown(self):
        self.tmp.cleanup()

    def _event(self, key: str, match_id: str = "54622319", **extra):
        body = {"match_id": match_id, "event_type": "goal", "team": "A", "score": "1-0",
                "minute": "1H 23", "dedup_key": key}
        body.update(extra)
        return body

    def test_reload_restores_events_seq_and_dedup(self):
        store = monitor_server._AlertStore(50, self.db)
        store.add(self._event("a"))
        store.add(self._event("b", score="2-0", minute="2H 61"))
        store.close()

        reloaded = monitor_server._AlertStore(50, self.db)
        events, seq = reloaded.since(0)
        self.assertEqual([e["seq"] for e in events], [1, 2])
        self.assertEqual(seq, 2)
        self.assertEqual(events[1]["score"], "2-0")
        # Dedup keys came back too, so a monitor retry after restart is still rejected.
        self.assertIsNone(reloaded.add(self._event("a")))
        # And new events continue the sequence instead of restarting at 1.
        self.assertEqual(reloaded.add(self._event("c"))["seq"], 3)
        reloaded.close()

    def test_reload_keeps_only_newest_capacity(self):
        store = monitor_server._AlertStore(10, self.db)
        for i in range(10):
            store.add(self._event(f"k{i}"))
        store.close()
        reloaded = monitor_server._AlertStore(3, self.db)
        events, seq = reloaded.since(0)
        self.assertEqual([e["seq"] for e in events], [8, 9, 10])
        self.assertEqual(seq, 10)
        reloaded.close()

    def test_memory_only_when_no_db_path(self):
        store = monitor_server._AlertStore(5, None)
        store.add(self._event("x"))
        self.assertEqual(store.since(0)[1], 1)
        self.assertFalse(self.db.exists())

    def test_unwritable_db_degrades_to_memory(self):
        bad = Path(self.tmp.name) / "not-a-dir"
        bad.write_bytes(b"file, not a directory")
        store = monitor_server._AlertStore(5, bad / "alerts.sqlite3")
        self.assertIsNotNone(store.add(self._event("y")))
        self.assertEqual(store.since(0)[1], 1)

    def test_for_match_filters_by_match_and_window(self):
        store = monitor_server._AlertStore(50, None)
        with mock.patch.object(monitor_server.time, "time", side_effect=[100.0, 200.0, 300.0]):
            store.add(self._event("m1"))
            store.add(self._event("m2", match_id="99999999"))
            store.add(self._event("m3"))
        picked = store.for_match("54622319", 150.0, 350.0)
        self.assertEqual([e["seq"] for e in picked], [3])
        self.assertEqual([e["seq"] for e in store.for_match("54622319", 0.0, 1000.0)], [1, 3])


class ClipAlertMatchTests(unittest.TestCase):
    """Which goal/red card a clip is tied to, and how that reaches the draft."""

    def setUp(self):
        self.store = monitor_server._AlertStore(50, None)
        self._patches = [
            mock.patch.object(monitor_server, "alert_store", self.store),
            mock.patch.object(monitor_server, "ALERT_MATCH_LOOKBACK_SECONDS", 600.0),
            mock.patch.object(monitor_server, "ALERT_MATCH_LOOKAHEAD_SECONDS", 180.0),
            mock.patch.object(monitor_server, "ALERT_MATCH_EXPECTED_OFFSET_SECONDS", 0.0),
            mock.patch.object(monitor_server, "ALERT_MATCH_AMBIGUOUS_SECONDS", 180.0),
            mock.patch.object(monitor_server, "ALERT_MATCH_WEAK_SECONDS", 240.0),
            mock.patch.object(
                monitor_server.match_catalog,
                "snapshot",
                return_value={
                    "playing": [
                        {"match_id": "54622319", "team_A_name": "伊尔韦斯", "team_B_name": "里耶卡",
                         "fs_A": 2, "fs_B": 1, "minute": "67", "minute_extra": "0"}
                    ]
                },
            ),
        ]
        for patch in self._patches:
            patch.start()

    def tearDown(self):
        for patch in self._patches:
            patch.stop()

    def _add(self, ts: float, **fields):
        body = {"match_id": "54622319", "event_type": "goal", "team": "A", "score": "1-0",
                "minute": "1H 23", "dedup_key": f"k{ts}"}
        body.update(fields)
        with mock.patch.object(monitor_server.time, "time", return_value=ts):
            return self.store.add(body)

    def test_minute_parsing(self):
        parts = monitor_server._alert_minute_parts
        self.assertEqual(parts("2H 68"), ("68", ""))
        self.assertEqual(parts("1H 45+2"), ("45", "2"))
        self.assertEqual(parts("45+2'"), ("45", "2"))
        self.assertEqual(parts("90"), ("90", ""))
        self.assertEqual(parts("HT"), ("", ""))
        self.assertEqual(parts(None), ("", ""))

    def test_picks_alert_closest_to_anchor(self):
        self._add(1000.0, score="1-0", minute="1H 23")
        self._add(1300.0, score="2-0", minute="2H 55")
        # Operator clips ~90s after the second push. Both goals are inside the
        # 600s lookback; the one closest to the frame wins and the runner-up is
        # 300s farther, so it is neither ambiguous nor weak.
        result = monitor_server._match_clip_alert("54622319", 1390.0)
        self.assertEqual(result["mode"], "alert")
        self.assertEqual(result["score"], "2-0")
        self.assertEqual(result["minute"], "55")
        self.assertEqual(result["alert_seq"], 2)
        self.assertEqual(result["candidates"], 2)
        self.assertFalse(result["ambiguous"])
        self.assertFalse(result["weak"])
        self.assertEqual(result["delta_seconds"], -90.0)
        self.assertEqual(result["candidate_deltas"], [-390.0, -90.0])

    def test_closest_beats_newest_when_push_lands_after_frame(self):
        # Push for the clipped goal arrived 50s before the frame; a later goal's
        # push is 150s after it. "Newest" would pick the later goal wrongly.
        self._add(1000.0, score="1-0", minute="1H 23")
        self._add(1200.0, score="2-0", minute="1H 27")
        result = monitor_server._match_clip_alert("54622319", 1050.0)
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["alert_seq"], 1)
        self.assertEqual(result["candidates"], 2)
        self.assertTrue(result["ambiguous"])

    def test_push_slower_than_frame_is_still_matched(self):
        # Push delay exceeds the live delay: the alert lands 120s after the
        # frame's PDT. The old +30s lookahead would have dropped it.
        self._add(1120.0, score="1-0", minute="1H 23")
        result = monitor_server._match_clip_alert("54622319", 1000.0)
        self.assertEqual(result["mode"], "alert")
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["delta_seconds"], 120.0)
        self.assertFalse(result["weak"])

    def test_only_stale_previous_goal_is_flagged_weak(self):
        # The push for the goal being clipped has not arrived; the only
        # candidate is the previous goal 400s earlier. We still fill it (no
        # waiting) but flag it so the operator verifies.
        self._add(1000.0, score="1-0", minute="1H 23")
        result = monitor_server._match_clip_alert("54622319", 1400.0)
        self.assertEqual(result["mode"], "alert")
        self.assertEqual(result["score"], "1-0")
        self.assertTrue(result["weak"])
        self.assertFalse(result["ambiguous"])
        self.assertEqual(result["delta_seconds"], -400.0)

    def test_expected_offset_shifts_the_pick(self):
        self._add(1000.0, score="1-0", minute="1H 23")
        self._add(1100.0, score="2-0", minute="1H 25")
        with mock.patch.object(monitor_server, "ALERT_MATCH_EXPECTED_OFFSET_SECONDS", -100.0):
            result = monitor_server._match_clip_alert("54622319", 1100.0)
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["delta_seconds"], -100.0)

    def test_rewinding_to_earlier_goal_after_later_push_landed(self):
        # Operator rewinds the buffer to clip goal A while goal B's push has
        # already arrived (A at -90s, B at +150s from the frame). With a measured
        # offset of -90 A is unambiguously closer; "newest" would pick B.
        self._add(910.0, score="1-0", minute="1H 23")
        self._add(1150.0, score="2-0", minute="1H 27")
        with mock.patch.object(monitor_server, "ALERT_MATCH_EXPECTED_OFFSET_SECONDS", -90.0):
            result = monitor_server._match_clip_alert("54622319", 1000.0)
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["alert_seq"], 1)
        self.assertEqual(result["candidates"], 2)
        self.assertFalse(result["ambiguous"])
        self.assertFalse(result["weak"])

    def test_first_goal_clip_ignores_later_goal(self):
        self._add(1000.0, score="1-0", minute="1H 23")
        self._add(1600.0, score="2-0", minute="2H 55")
        # Anchor is the frame of the first goal (+80s delay); the second goal has
        # not happened yet at that wall-clock, so it must not be picked.
        result = monitor_server._match_clip_alert("54622319", 1080.0)
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["alert_seq"], 1)
        self.assertEqual(result["candidates"], 1)

    def test_two_close_alerts_are_flagged_ambiguous(self):
        self._add(1000.0, event_type="red_card", score="0-0", minute="1H 30")
        self._add(1060.0, score="1-0", minute="1H 31")
        result = monitor_server._match_clip_alert("54622319", 1150.0)
        self.assertEqual(result["mode"], "alert")
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["code"], "G")
        self.assertTrue(result["ambiguous"])

    def test_exact_tie_picks_older_alert_and_is_ambiguous(self):
        # Both candidates sit exactly 100s from the expected offset. for_match
        # returns oldest first and min() keeps the first minimum, so the older
        # alert wins deterministically; the zero gap must be flagged ambiguous.
        self._add(1000.0, score="1-0", minute="1H 23")
        self._add(1200.0, score="2-0", minute="1H 27")
        result = monitor_server._match_clip_alert("54622319", 1100.0)
        self.assertEqual(result["candidate_deltas"], [-100.0, 100.0])
        self.assertEqual(result["alert_seq"], 1)
        self.assertEqual(result["score"], "1-0")
        self.assertEqual(result["delta_seconds"], -100.0)
        self.assertTrue(result["ambiguous"])
        self.assertFalse(result["weak"])

    def test_red_card_alert_sets_rc_code(self):
        self._add(1000.0, event_type="red_card", score="0-0", minute="1H 30")
        result = monitor_server._match_clip_alert("54622319", 1050.0)
        self.assertEqual(result["code"], "RC")

    def test_alert_too_old_falls_back_to_catalog(self):
        self._add(1000.0, score="1-0")
        result = monitor_server._match_clip_alert("54622319", 1000.0 + 601.0)
        self.assertEqual(result["mode"], "catalog")
        self.assertEqual(result["score"], "2-1")
        self.assertEqual(result["minute"], "67")
        self.assertEqual(result["candidates"], 0)

    def test_other_match_alert_is_ignored(self):
        self._add(1000.0, match_id="99999999", score="5-5")
        result = monitor_server._match_clip_alert("54622319", 1050.0)
        self.assertEqual(result["mode"], "catalog")
        self.assertEqual(result["score"], "2-1")

    def test_no_alert_and_no_catalog_is_none(self):
        with mock.patch.object(monitor_server.match_catalog, "snapshot", return_value={"playing": []}):
            result = monitor_server._match_clip_alert("54622319", 1050.0)
        self.assertEqual(result["mode"], "none")
        self.assertEqual(result["minute"], "")
        self.assertEqual(result["score"], "")


class ClipEndpointAlertMatchTests(unittest.TestCase):
    """``POST /api/monitor/clip`` forwards the matched alert into the draft event."""

    def setUp(self):
        self.client = monitor_server.app.test_client()
        self.tmp = TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.base = 1_700_000_000.0
        lines = ["#EXTM3U", "#EXT-X-TARGETDURATION:2"]
        for i in range(10):
            path = self.dir / f"seg_{i:05d}.ts"
            path.write_bytes(b"x")
            iso = monitor_server.datetime.fromtimestamp(self.base + i * 2.0).astimezone().isoformat(timespec="milliseconds")
            lines += [f"#EXT-X-PROGRAM-DATE-TIME:{iso}", "#EXTINF:2.000,", path.name]
        (self.dir / "index.m3u8").write_text("\n".join(lines) + "\n", encoding="utf-8")
        self.store = monitor_server._AlertStore(50, None)
        self.queue = mock.Mock()
        self.queue.enqueue.return_value = {"status": "queued"}
        self._patches = [
            mock.patch.object(monitor_server, "alert_store", self.store),
            mock.patch.object(monitor_server, "manual_draft_queue", self.queue),
            mock.patch.object(monitor_server, "_clip_dir", return_value=self.dir / "clips"),
            mock.patch.object(monitor_server.live_preview, "status", return_value={"running": True}),
            mock.patch.object(monitor_server.live_preview, "media_dir", return_value=self.dir),
            mock.patch.object(monitor_server, "_encode_clip_gif", side_effect=self._fake_encode),
            mock.patch.object(
                monitor_server.match_catalog,
                "snapshot",
                return_value={"playing": [{"match_id": "54622319", "team_A_name": "甲", "team_B_name": "乙",
                                           "fs_A": 1, "fs_B": 1, "minute": "70", "minute_extra": "0"}]},
            ),
        ]
        for patch in self._patches:
            patch.start()

    def tearDown(self):
        for patch in self._patches:
            patch.stop()
        self.tmp.cleanup()

    def _fake_encode(self, segments, before_seconds, output_path, anchor_epoch=None, start_epoch=None):
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"GIF")

    def _clip(self, anchor_offset: float):
        return self.client.post(
            "/api/monitor/clip",
            json={"match_id": "54622319", "anchor_epoch_ms": round((self.base + anchor_offset) * 1000), "before_seconds": 5},
        )

    def _clip_and_publish(self, anchor_offset: float, **extra):
        """Capture, then publish: the queue is only reached by the second call."""
        response = self._clip(anchor_offset)
        self.assertEqual(response.status_code, 200, response.get_json())
        self.queue.enqueue.assert_not_called()
        name = response.get_json()["name"]
        extra.setdefault("title", "人工标题")
        published = self.client.post(
            "/api/monitor/clip/publish", json={"match_id": "54622319", "name": name, **extra}
        )
        self.assertEqual(published.status_code, 200, published.get_json())
        return response

    def test_matched_alert_prefills_form_and_publishes_its_fields(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            stored = self.store.add({"match_id": "54622319", "event_type": "goal", "team": "B",
                                     "score": "1-1", "minute": "2H 68", "dedup_key": "g1"})
        response = self._clip_and_publish(10.0)
        data = response.get_json()
        self.assertEqual(data["alert_match"]["mode"], "alert")
        self.assertEqual(data["alert_match"]["match_time"], "68")
        self.assertEqual(data["alert_match"]["score"], "1-1")
        self.assertFalse(data["alert_match"]["weak"])
        # The form is pre-filled from the matched push: goal → type 1, clock, score, no title.
        self.assertEqual(
            data["form"],
            {"alert_seq": stored["seq"], "match_event": 1, "match_time": "68", "match_score": "1-1", "title": ""},
        )
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual(event["minute"], "68")
        self.assertEqual(event["score"], "1-1")
        self.assertEqual(event["code"], "G")
        self.assertEqual(event["team"], "B")
        self.assertEqual(event["match_event"], 1)
        self.assertEqual(event["title_override"], "人工标题")
        self.assertNotIn("manual_match_event", event)
        self.assertEqual(event["alert_match_mode"], "alert")

    def test_red_card_push_prefills_type_4_without_score(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            self.store.add({"match_id": "54622319", "event_type": "red_card", "team": "A",
                            "score": "1-0", "minute": "1H 30", "dedup_key": "r1"})
        data = self._clip_and_publish(10.0).get_json()
        # Only 进球/点球 carry 比分; the push's score is not pre-filled for a red card.
        self.assertEqual((data["form"]["match_event"], data["form"]["match_score"]), (4, ""))
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual((event["match_event"], event["code"], event["minute"], event["score"]), (4, "RC", "30", ""))

    def test_score_is_dropped_for_types_outside_goal_and_penalty(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            self.store.add({"match_id": "54622319", "event_type": "goal", "team": "B",
                            "score": "1-1", "minute": "2H 68", "dedup_key": "g1"})
        name = self._clip(10.0).get_json()["name"]
        for match_event, expected in ((0, ""), (3, ""), (4, ""), (5, ""), (6, ""), (1, "1-1"), (2, "1-1")):
            self.queue.enqueue.reset_mock()
            response = self.client.post(
                "/api/monitor/clip/publish",
                json={"match_id": "54622319", "name": name, "match_event": match_event,
                      "match_score": "1-1", "title": "t"},
            )
            self.assertEqual(response.status_code, 200, response.get_json())
            self.assertEqual(self.queue.enqueue.call_args.kwargs["event"]["score"], expected, match_event)

    def test_score_event_types_are_configurable(self):
        parse = monitor_server._parse_score_event_types
        self.assertEqual(parse("1,2"), frozenset({1, 2}))
        self.assertEqual(parse(" 1, 2 ,5,"), frozenset({1, 2, 5}))
        self.assertEqual(parse(""), frozenset())
        for bad in ("7", "a", "1;2", "-1"):
            with self.assertRaises(RuntimeError, msg=bad):
                parse(bad)
        # The override drives both the pre-fill and the publish filter, and is
        # reported to the page so the 比分 lock follows the same rule.
        with mock.patch.object(monitor_server, "SCORE_EVENT_TYPES", frozenset({4})):
            with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
                self.store.add({"match_id": "54622319", "event_type": "red_card", "team": "A",
                                "score": "2-0", "minute": "1H 30", "dedup_key": "r1"})
            data = self._clip(10.0).get_json()
            self.assertEqual(data["form"]["match_score"], "2-0")
            self.queue.records_by_source_name.return_value = {}
            listing = self.client.get("/api/monitor/clips?match_id=54622319").get_json()
            self.assertEqual(listing["score_event_types"], [4])
            response = self.client.post(
                "/api/monitor/clip/publish",
                json={"match_id": "54622319", "name": data["name"], "match_event": 1, "match_score": "2-0", "title": "t"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(self.queue.enqueue.call_args.kwargs["event"]["score"], "")

    def test_unresolved_side_is_not_forwarded(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            self.store.add({"match_id": "54622319", "event_type": "goal", "team": "?",
                            "score": "1-1", "minute": "2H 68", "dedup_key": "g?"})
        self._clip_and_publish(10.0)
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual(event["team"], "")

    def test_stoppage_time_is_rendered_with_plus(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            self.store.add({"match_id": "54622319", "event_type": "goal", "team": "A",
                            "score": "2-1", "minute": "1H 45+3", "dedup_key": "g2"})
        data = self._clip_and_publish(10.0).get_json()
        self.assertEqual(data["alert_match"]["match_time"], "45+3")
        self.assertEqual(data["form"]["match_time"], "45+3")
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual((event["minute"], event["minute_extra"]), ("45", "3"))

    def test_no_alert_defaults_to_skip_with_empty_clock(self):
        # Without a push the catalog clock is still reported for information,
        # but the form starts as 跳过: type 0 and nothing pre-filled.
        data = self._clip_and_publish(10.0).get_json()
        self.assertEqual(data["alert_match"]["mode"], "catalog")
        self.assertEqual(data["alert_match"]["match_time"], "70")
        self.assertEqual(
            data["form"], {"alert_seq": None, "match_event": 0, "match_time": "", "match_score": "", "title": ""}
        )
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual((event["match_event"], event["minute"], event["score"]), (0, "", ""))
        self.assertEqual(event["alert_match_mode"], "catalog")

    def test_publisher_sends_explicit_match_event_including_zero(self):
        import article_publisher

        for match_event in (0, 2, 6):
            fields = article_publisher.build_article_fields(
                match_id="54622319",
                event={"code": "G", "match_event": match_event, "minute": "68", "score": "1-1"},
                gif_url="https://example.com/a.gif",
                cover_url="https://example.com/a.jpg",
                title="t",
                delivery_mode="draft",
            )
            self.assertEqual(fields["match_event"], match_event)
        self.assertEqual(fields["match_time"], "68")
        self.assertEqual(fields["match_score"], "1-1")
        self.assertEqual(fields["style"], "gif")
        # Legacy manual flag still leaves the field out; OCR events still derive it.
        legacy = article_publisher.build_article_fields(
            match_id="54622319",
            event={"code": "G", "manual_match_event": True, "minute": "68"},
            gif_url="https://example.com/a.gif",
            cover_url="https://example.com/a.jpg",
            title="t",
            delivery_mode="draft",
        )
        self.assertNotIn("match_event", legacy)
        auto = article_publisher.build_article_fields(
            match_id="54622319",
            event={"code": "G", "minute": "68", "score": "1-1"},
            gif_url="https://example.com/a.gif",
            cover_url="https://example.com/a.jpg",
            title="t",
            delivery_mode="draft",
        )
        self.assertEqual(auto["match_event"], 1)


class ClipPublishEndpointTests(ClipEndpointAlertMatchTests):
    """Capture and publish are two steps; ``/api/monitor/clip/publish`` is the only path to the queue."""

    def test_capture_writes_sidecar_and_does_not_enqueue(self):
        data = self._clip(10.0).get_json()
        self.queue.enqueue.assert_not_called()
        self.assertNotIn("draft", data)
        self.assertTrue(data["event_key"].startswith("manual_"))
        meta = json.loads((self.dir / "clips" / data["name"]).with_suffix(".json").read_text(encoding="utf-8"))
        self.assertEqual(meta["event_key"], data["event_key"])
        self.assertEqual(meta["alert_match"]["mode"], "catalog")
        self.assertEqual(meta["form"]["match_event"], 0)
        self.assertEqual(meta["match_detail"]["team_A_name"], "甲")

    def test_publish_backfills_team_ids_into_old_clip_match_detail(self):
        """Clips captured before the catalog carried team ids get them from the
        live directory at publish time so the publisher can pick the exact tag."""
        name = self._clip(10.0).get_json()["name"]
        meta_path = (self.dir / "clips" / name).with_suffix(".json")
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        self.assertIsNone(meta["match_detail"].get("team_A_id"))
        with mock.patch.object(
            monitor_server.match_catalog,
            "snapshot",
            return_value={"playing": [{"match_id": "54622319", "team_A_name": "甲", "team_B_name": "乙",
                                       "team_A_id": "50000001", "team_B_id": "50000002"}]},
        ):
            published = self.client.post(
                "/api/monitor/clip/publish", json={"match_id": "54622319", "name": name, "title": "t"}
            )
        self.assertEqual(published.status_code, 200, published.get_json())
        detail = self.queue.enqueue.call_args.kwargs["match_detail"]
        self.assertEqual(detail["team_A_id"], "50000001")
        self.assertEqual(detail["team_B_id"], "50000002")
        self.assertEqual(detail["team_A_name"], "甲")
        # The sidecar itself is not rewritten with the ids; only the publish payload.
        self.assertIsNone(json.loads(meta_path.read_text(encoding="utf-8"))["match_detail"].get("team_A_id"))

    def test_list_returns_form_and_candidates_for_unpublished_rows(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 300.0):
            first = self.store.add({"match_id": "54622319", "event_type": "goal", "team": "A",
                                    "score": "1-0", "minute": "1H 12", "dedup_key": "c1", "team_name": "甲"})
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            second = self.store.add({"match_id": "54622319", "event_type": "red_card", "team": "B",
                                     "score": "1-0", "minute": "2H 68", "dedup_key": "c2", "team_name": "乙"})
        self.store.add({"match_id": "99999999", "event_type": "goal", "team": "A", "score": "1-0",
                        "minute": "1H 1", "dedup_key": "other"})
        name = self._clip(10.0).get_json()["name"]
        self.queue.records_by_source_name.return_value = {}
        body = self.client.get("/api/monitor/clips?match_id=54622319").get_json()
        row = next(c for c in body["clips"] if c["name"] == name)
        self.assertEqual(row["form"]["alert_seq"], second["seq"])
        self.assertEqual(row["form"]["match_event"], 4)
        self.assertNotIn("person", row)
        # Newest first, only this match, each with the values the page needs to pre-fill.
        self.assertEqual([c["seq"] for c in row["candidates"]], [second["seq"], first["seq"]])
        self.assertEqual(
            {k: row["candidates"][1][k] for k in ("event_type", "team_name", "match_event", "match_time", "score")},
            {"event_type": "goal", "team_name": "甲", "match_event": 1, "match_time": "12", "score": "1-0"},
        )

    def test_published_rows_carry_no_candidates(self):
        name = self._clip(10.0).get_json()["name"]
        self.queue.records_by_source_name.return_value = {
            name: {"event_key": "manual_1", "status": "published", "draft_url": "https://d/1", "error": None, "retriable": False},
        }
        row = self.client.get("/api/monitor/clips?match_id=54622319").get_json()["clips"][0]
        self.assertNotIn("candidates", row)
        self.assertIn("form", row)

    def test_publish_uses_the_edited_form_and_saves_it(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            stored = self.store.add({"match_id": "54622319", "event_type": "goal", "team": "B",
                                     "score": "1-1", "minute": "2H 68", "dedup_key": "g1"})
        name = self._clip(10.0).get_json()["name"]
        # Operator keeps the push but changes the type to penalty and fixes the score.
        published = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": name, "match_event": "2", "match_score": "2:1",
                  "match_time": "68+1'", "title": "  点球破门 "},
        )
        self.assertEqual(published.status_code, 200, published.get_json())
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual(event["match_event"], 2)
        self.assertEqual((event["minute"], event["minute_extra"], event["score"]), ("68", "1", "2-1"))
        self.assertEqual(event["title_override"], "点球破门")
        self.assertEqual(event["alert_seq"], stored["seq"])
        self.assertEqual(self.queue.enqueue.call_args.kwargs["source_path"], self.dir / "clips" / name)
        meta = json.loads((self.dir / "clips" / name).with_suffix(".json").read_text(encoding="utf-8"))
        self.assertEqual(
            meta["form"],
            {"alert_seq": stored["seq"], "match_event": 2, "match_time": "68+1", "match_score": "2-1", "title": "点球破门"},
        )

    def test_publish_skip_sends_type_zero_and_empty_clock(self):
        with mock.patch.object(monitor_server.time, "time", return_value=self.base - 60.0):
            self.store.add({"match_id": "54622319", "event_type": "goal", "team": "B",
                            "score": "1-1", "minute": "2H 68", "dedup_key": "g1"})
        name = self._clip(10.0).get_json()["name"]
        published = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": name, "alert_seq": None, "match_event": 0,
                  "match_time": "", "match_score": "", "title": "精彩过人"},
        )
        self.assertEqual(published.status_code, 200, published.get_json())
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual((event["match_event"], event["minute"], event["score"], event["alert_seq"]), (0, "", "", None))

    def test_publish_rejects_empty_title_and_bad_fields(self):
        name = self._clip(10.0).get_json()["name"]
        empty = self.client.post("/api/monitor/clip/publish", json={"match_id": "54622319", "name": name})
        self.assertEqual(empty.status_code, 400)
        self.assertEqual(empty.get_json()["code"], "title_required")
        for bad in ({"match_event": 7}, {"match_event": "x"}, {"match_time": "abc"}, {"match_score": "3"}):
            response = self.client.post(
                "/api/monitor/clip/publish", json={"match_id": "54622319", "name": name, "title": "t", **bad}
            )
            self.assertEqual(response.status_code, 400, bad)
            self.assertEqual(response.get_json()["code"], "form_invalid", bad)
        self.queue.enqueue.assert_not_called()

    def test_publish_twice_reuses_the_same_event_key(self):
        name = self._clip(10.0).get_json()["name"]
        for _ in range(2):
            self.client.post("/api/monitor/clip/publish", json={"match_id": "54622319", "name": name, "title": "t"})
        keys = {call.kwargs["event"]["event_key"] for call in self.queue.enqueue.call_args_list}
        self.assertEqual(len(keys), 1)

    def test_publish_warns_when_match_time_already_published_unless_forced(self):
        # 2026-10-22 02:08 on 54493251: two clips with match_time=5 eight seconds
        # apart, the CMS rejected the second with code=3. Surface it before sending.
        name = self._clip(10.0).get_json()["name"]
        self.queue.records_for_match.return_value = {
            "manual_1": {"event_key": "manual_1", "status": "published", "article_id": "6529989",
                         "draft_url": "https://d/6529989",
                         "final_event": {"minute": "5", "minute_extra": ""}},
            "manual_2": {"event_key": "manual_2", "status": "failed", "article_id": None,
                         "final_event": {"minute": "7", "minute_extra": ""}},
        }
        clash = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": name, "match_time": "5", "match_event": 1, "title": "t"},
        )
        self.assertEqual(clash.status_code, 409, clash.get_json())
        body = clash.get_json()
        self.assertEqual(body["code"], "duplicate_match_time")
        self.assertEqual((body["article_id"], body["match_time"], body["draft_url"]), ("6529989", "5", "https://d/6529989"))
        self.queue.enqueue.assert_not_called()
        # A failed record does not count; a different minute does not clash.
        ok = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": name, "match_time": "7", "match_event": 1, "title": "t"},
        )
        self.assertEqual(ok.status_code, 200, ok.get_json())
        # ``force`` publishes despite the clash (same-minute events are real).
        forced = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": name, "match_time": "5", "match_event": 1, "title": "t", "force": True},
        )
        self.assertEqual(forced.status_code, 200, forced.get_json())
        self.assertEqual(self.queue.enqueue.call_args.kwargs["event"]["minute"], "5")
        # Empty match_time never clashes; a broken records call never blocks.
        self.queue.records_for_match.side_effect = RuntimeError("db")
        skip = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": name, "match_time": "5", "match_event": 1, "title": "t"},
        )
        self.assertEqual(skip.status_code, 200, skip.get_json())

    def test_code_3_rejection_message_explains_duplicate(self):
        from article_draft_queue import _friendly_draft_error

        text = _friendly_draft_error("platform_publish", "创建失败", platform_code=3, publishing=True)
        self.assertIn("被平台拒绝", text)
        self.assertIn("相同比赛时间", text)
        other = _friendly_draft_error("platform_publish", "boom", platform_code=5, publishing=True)
        self.assertIn("没有接受请求", other)

    def test_publish_rejects_missing_or_malformed_clip(self):
        missing = self.client.post(
            "/api/monitor/clip/publish",
            json={"match_id": "54622319", "name": "clip_20260101-000000_abcdef.gif", "title": "t"},
        )
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.get_json()["code"], "clip_missing")
        bad = self.client.post("/api/monitor/clip/publish", json={"match_id": "54622319", "name": "../x.gif"})
        self.assertEqual(bad.status_code, 400)
        self.queue.enqueue.assert_not_called()

    def test_publish_without_queue_is_409(self):
        name = self._clip(10.0).get_json()["name"]
        with mock.patch.object(monitor_server, "manual_draft_queue", None):
            response = self.client.post(
                "/api/monitor/clip/publish", json={"match_id": "54622319", "name": name, "title": "t"}
            )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "publish_disabled")

    def test_publish_surfaces_queue_error(self):
        name = self._clip(10.0).get_json()["name"]
        self.queue.enqueue.side_effect = RuntimeError("DB locked")
        response = self.client.post(
            "/api/monitor/clip/publish", json={"match_id": "54622319", "name": name, "title": "t"}
        )
        self.assertEqual(response.status_code, 500)
        self.assertIn("DB locked", response.get_json()["error"])

    def test_publish_pre_sidecar_clip_falls_back_to_mtime(self):
        clips = self.dir / "clips"
        clips.mkdir(parents=True, exist_ok=True)
        legacy = clips / "clip_20260101-000000_abcdef.gif"
        legacy.write_bytes(b"GIF")
        os.utime(legacy, (self.base + 10.0, self.base + 10.0))
        response = self.client.post(
            "/api/monitor/clip/publish", json={"match_id": "54622319", "name": legacy.name, "title": "t"}
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        event = self.queue.enqueue.call_args.kwargs["event"]
        self.assertEqual(event["event_key"], f"manual_{int(self.base + 10.0)}")
        self.assertEqual(event["alert_match_mode"], "catalog")
        self.assertEqual(event["match_event"], 0)


class SigtermShutdownTests(unittest.TestCase):
    def test_handler_raises_system_exit_so_atexit_runs(self):
        with self.assertRaises(SystemExit) as ctx:
            monitor_server._exit_on_sigterm(15, None)
        self.assertEqual(ctx.exception.code, 0)

    def test_real_process_exits_cleanly_on_sigterm(self):
        # Default SIGTERM would report returncode -15 and skip atexit; with the
        # handler installed the server exits 0 and its atexit hooks (preview
        # shutdown) get to run.
        import socket
        import sys
        import urllib.error
        import urllib.request

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        env = dict(os.environ)
        env.update(
            {
                "GIF_MONITOR_PORT": str(port),
                "GIF_MONITOR_PUBLISH_ENABLED": "false",
                "GIF_MONITOR_ALERT_DB_PATH": "",
                "GIF_MONITOR_ALERT_ENABLED": "false",
                "GIF_SOURCE_SECRET": "test-secret",
            }
        )
        proc = subprocess.Popen(
            [sys.executable, "monitor_server.py"],
            cwd=Path(__file__).resolve().parent,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.time() + 15.0
            ready = False
            while time.time() < deadline:
                if proc.poll() is not None:
                    self.fail(f"server exited early with {proc.returncode}")
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1).read()
                    ready = True
                    break
                except (urllib.error.URLError, ConnectionError, OSError):
                    time.sleep(0.2)
            self.assertTrue(ready, "server did not become healthy")
            proc.send_signal(signal.SIGTERM)
            self.assertEqual(proc.wait(timeout=10), 0)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)


class PreviewStartWebRTCTests(unittest.TestCase):
    """Scheme H wiring: tile gets WHEP only when mediamtx registered the path."""

    def setUp(self):
        self.app = monitor_server.app.test_client()
        self.started = []

        def fake_start(match_id, resource):
            self.started.append((match_id, resource))
            return {"running": True, "resource": resource}

        self._patches = [
            mock.patch.object(monitor_server, "_resolve_source", return_value="rtmp://up/live/1"),
            mock.patch.object(monitor_server.live_preview, "start", side_effect=fake_start),
            mock.patch.object(
                monitor_server.live_preview, "status",
                side_effect=lambda m: {"running": True, "resource": self.started[-1][1]},
            ),
            mock.patch.object(monitor_server, "_wait_playlist_ready", return_value=True),
            mock.patch.object(monitor_server.live_preview, "playlist_url", return_value="preview/1/index.m3u8"),
        ]
        for patch in self._patches:
            patch.start()

    def tearDown(self):
        for patch in self._patches:
            patch.stop()

    def _start(self):
        resp = self.app.post("/api/monitor/preview/start", json={"match_id": "54477889"})
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        return resp.get_json()

    def test_disabled_gateway_keeps_direct_rtmp_and_no_whep(self):
        with mock.patch.object(monitor_server.webrtc_gateway, "enabled", False):
            data = self._start()
        self.assertEqual(self.started, [("54477889", "rtmp://up/live/1")])
        self.assertFalse(data["webrtc"])
        self.assertIsNone(data["whep_url"])
        self.assertEqual(data["playlist_url"], "preview/1/index.m3u8")

    def test_restart_hook_reregisters_or_falls_back(self):
        gw = monitor_server.webrtc_gateway
        rtsp = "rtsp://127.0.0.1:8554/54477889"
        # Switch off, or a plain RTMP resource: hook stays out of the way.
        with mock.patch.object(gw, "enabled", False):
            self.assertIsNone(monitor_server._before_preview_restart("54477889", rtsp))
        with mock.patch.object(gw, "enabled", True):
            self.assertIsNone(monitor_server._before_preview_restart("54477889", "rtmp://up/live/1"))
        # mediamtx restarted but is back: path re-created, keep reading RTSP.
        with mock.patch.object(gw, "enabled", True), \
             mock.patch.object(gw, "reensure_path", return_value=True) as re:
            self.assertIsNone(monitor_server._before_preview_restart("54477889", rtsp))
        re.assert_called_once_with("54477889")
        # mediamtx really gone: hand ffmpeg the remembered upstream and clean up.
        with mock.patch.object(gw, "enabled", True), \
             mock.patch.object(gw, "reensure_path", return_value=False), \
             mock.patch.object(gw, "upstream_for", return_value="rtmp://up/live/1"), \
             mock.patch.object(gw, "remove_path") as remove:
            self.assertEqual(monitor_server._before_preview_restart("54477889", rtsp), "rtmp://up/live/1")
        remove.assert_called_once_with("54477889")

    def test_enabled_gateway_routes_ffmpeg_via_rtsp_and_returns_whep(self):
        gw = monitor_server.webrtc_gateway
        with mock.patch.object(gw, "enabled", True), \
             mock.patch.object(gw, "ensure_path", return_value=True) as ensure, \
             mock.patch.object(gw, "remove_path") as remove:
            data = self._start()
        ensure.assert_called_once_with("54477889", "rtmp://up/live/1")
        remove.assert_not_called()
        self.assertEqual(self.started, [("54477889", "rtsp://127.0.0.1:8554/54477889")])
        self.assertTrue(data["webrtc"])
        self.assertEqual(data["whep_url"], "whep/54477889/whep")
        # HLS is still produced for the stage and for capture.
        self.assertEqual(data["playlist_url"], "preview/1/index.m3u8")

    def test_gateway_failure_falls_back_to_direct_rtmp(self):
        gw = monitor_server.webrtc_gateway
        with mock.patch.object(gw, "enabled", True), \
             mock.patch.object(gw, "ensure_path", return_value=False), \
             mock.patch.object(gw, "remove_path") as remove:
            data = self._start()
        # Half-registered path is cleaned so the upstream is not pulled twice.
        remove.assert_called_once_with("54477889")
        self.assertEqual(self.started, [("54477889", "rtmp://up/live/1")])
        self.assertFalse(data["webrtc"])
        self.assertIsNone(data["whep_url"])

    def test_status_reports_webrtc_from_running_resource(self):
        gw = monitor_server.webrtc_gateway
        with mock.patch.object(gw, "enabled", True), \
             mock.patch.object(monitor_server.live_preview, "touch"), \
             mock.patch.object(
                 monitor_server.live_preview, "status",
                 return_value={"running": True, "resource": "rtsp://127.0.0.1:8554/54477889"},
             ):
            data = self.app.get("/api/monitor/preview/status?match_id=54477889").get_json()
        self.assertTrue(data["webrtc"])
        self.assertEqual(data["whep_url"], "whep/54477889/whep")
        with mock.patch.object(gw, "enabled", True), \
             mock.patch.object(monitor_server.live_preview, "touch"), \
             mock.patch.object(
                 monitor_server.live_preview, "status",
                 return_value={"running": True, "resource": "rtmp://up/live/1"},
             ):
            data = self.app.get("/api/monitor/preview/status?match_id=54477889").get_json()
        self.assertFalse(data["webrtc"])
        self.assertIsNone(data["whep_url"])


class LatencyKillSwitchTests(unittest.TestCase):
    """Operator switches that restore pre-optimisation behaviour without a code change."""

    def test_matches_advertises_catchup_switch(self):
        app = monitor_server.app.test_client()
        with mock.patch.object(monitor_server.match_catalog, "snapshot", return_value={"playing": []}):
            with mock.patch.object(monitor_server, "CATCHUP_ENABLED", True):
                self.assertTrue(app.get("/api/monitor/matches").get_json()["catchup_enabled"])
            with mock.patch.object(monitor_server, "CATCHUP_ENABLED", False):
                self.assertFalse(app.get("/api/monitor/matches").get_json()["catchup_enabled"])

    def test_segment_seconds_env_restores_two_second_segments(self):
        with mock.patch.dict(os.environ, {"GIF_PREVIEW_SEGMENT_SECONDS": "2"}):
            self.assertEqual(positive_env_int("GIF_PREVIEW_SEGMENT_SECONDS", 1), 2)
        with TemporaryDirectory() as tmp:
            manager = live_preview.LivePreviewManager(
                root=Path(tmp), background_reaper=False, segment_seconds=2
            )
            command = manager._build_command("rtmp://host/app/s", Path(tmp))
        self.assertEqual(command[command.index("-hls_time") + 1], "2")
        self.assertEqual(
            command[command.index("-force_key_frames") + 1], "expr:gte(t,n_forced*2)"
        )

    def test_dotenv_segment_seconds_reaches_live_preview_module(self):
        # Regression: live_preview reads GIF_PREVIEW_SEGMENT_SECONDS at import
        # time, so .env must be loaded before the project imports or the value
        # silently stays at the default (the server then slices 1s segments
        # while the browser is told a 2s target).
        repo = Path(__file__).resolve().parent
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for source in repo.glob("*.py"):
                shutil.copy(source, root / source.name)
            (root / ".env").write_text("GIF_PREVIEW_SEGMENT_SECONDS=2\n", encoding="utf-8")
            env = {
                k: v for k, v in os.environ.items()
                if k != "GIF_PREVIEW_SEGMENT_SECONDS"
            }
            env.update({
                "GIF_SOURCE_SECRET": "test-secret",
                "GIF_MONITOR_PUBLISH_ENABLED": "false",
                "GIF_MONITOR_ALERT_DB_PATH": "",
            })
            completed = subprocess.run(
                [
                    "python3", "-c",
                    "import monitor_server, live_preview;"
                    "print(live_preview.PREVIEW_SEGMENT_SECONDS,"
                    " monitor_server.live_preview.segment_seconds)",
                ],
                cwd=root, env=env, capture_output=True, text=True, timeout=60,
            )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.split(), ["2", "2"])

    def test_matches_advertises_latency_config(self):
        app = monitor_server.app.test_client()
        with mock.patch.object(monitor_server.match_catalog, "snapshot", return_value={"playing": []}):
            latency = app.get("/api/monitor/matches").get_json()["latency"]
        for key in (
            "segment_seconds", "preview_segments", "history_seconds", "x264_preset",
            "hls_live_sync_count", "hls_target_seconds", "catchup_enabled",
            "catchup_rate_enabled", "catchup_rate", "catchup_min_lag", "tile_seek_lag",
            "stage_auto_seek_lag", "stage_user_grace_seconds",
            "stage_seek_lag", "hard_seek_lag", "adjusted",
        ):
            self.assertIn(key, latency)
        self.assertEqual(
            latency["hls_target_seconds"], latency["segment_seconds"] * latency["hls_live_sync_count"]
        )

    LATENCY_ENV_KEYS = (
        "GIF_MONITOR_HLS_LIVE_SYNC_COUNT", "GIF_MONITOR_CATCHUP_RATE_ENABLED",
        "GIF_MONITOR_CATCHUP_RATE", "GIF_MONITOR_CATCHUP_MIN_LAG", "GIF_MONITOR_TILE_SEEK_LAG",
        "GIF_MONITOR_STAGE_AUTO_SEEK_LAG", "GIF_MONITOR_STAGE_USER_GRACE_SECONDS",
        "GIF_MONITOR_STAGE_SEEK_LAG", "GIF_MONITOR_HARD_SEEK_LAG",
    )

    def _without_latency_env(self):
        """The local .env may carry experimental values; defaults tests must not see them."""
        env = {k: v for k, v in os.environ.items() if k not in self.LATENCY_ENV_KEYS}
        return mock.patch.dict(os.environ, env, clear=True)

    def test_latency_defaults_are_unchanged_and_unadjusted(self):
        with self._without_latency_env():
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["hls_live_sync_count"], 3)
        self.assertTrue(cfg["catchup_rate_enabled"])
        self.assertEqual(cfg["catchup_rate"], 1.5)
        self.assertEqual(cfg["catchup_min_lag"], 12.0)
        self.assertEqual(cfg["tile_seek_lag"], 0.0)   # off unless the operator opts in
        self.assertEqual(cfg["stage_auto_seek_lag"], 0.0)
        self.assertEqual(cfg["stage_user_grace_seconds"], 60.0)
        self.assertEqual(cfg["stage_seek_lag"], 30.0)
        self.assertEqual(cfg["hard_seek_lag"], 150.0)
        self.assertEqual(cfg["adjusted"], [])

    def test_latency_config_bounds_unsafe_values(self):
        env = {
            "GIF_MONITOR_HLS_LIVE_SYNC_COUNT": "1",   # <2 stalls forever
            "GIF_MONITOR_CATCHUP_RATE": "5",          # browsers mute/skip above ~2x
            "GIF_MONITOR_CATCHUP_MIN_LAG": "1",       # below target → constant toggling
            "GIF_MONITOR_HARD_SEEK_LAG": "9999",      # beyond the ring buffer
            "GIF_MONITOR_TILE_SEEK_LAG": "1",         # below target+2 → jumps every tick
        }
        with mock.patch.dict(os.environ, env):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["hls_live_sync_count"], 2)
        self.assertEqual(cfg["catchup_rate"], 2.0)
        self.assertGreaterEqual(cfg["catchup_min_lag"], cfg["hls_target_seconds"] + 2)
        self.assertLessEqual(cfg["hard_seek_lag"], cfg["history_seconds"] - 10)
        self.assertGreaterEqual(cfg["hard_seek_lag"], cfg["stage_seek_lag"])
        self.assertEqual(cfg["tile_seek_lag"], cfg["hls_target_seconds"] + 2)
        self.assertEqual(len(cfg["adjusted"]), 5)

    def test_tile_seek_lag_accepts_value_and_off_spellings(self):
        with mock.patch.dict(os.environ, {"GIF_MONITOR_TILE_SEEK_LAG": "6"}):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["tile_seek_lag"], 6.0)
        self.assertNotIn("tile_seek_lag=6.0->6.0", cfg["adjusted"])
        for off in ("0", "off", "false", ""):
            with mock.patch.dict(os.environ, {"GIF_MONITOR_TILE_SEEK_LAG": off}):
                self.assertEqual(monitor_server._build_latency_config()["tile_seek_lag"], 0.0, off)
        with mock.patch.dict(os.environ, {"GIF_MONITOR_TILE_SEEK_LAG": "abc"}):
            with self.assertRaises(RuntimeError):
                monitor_server._build_latency_config()

    def test_tile_seek_lag_is_capped_by_hard_seek_lag(self):
        env = {"GIF_MONITOR_HARD_SEEK_LAG": "40", "GIF_MONITOR_TILE_SEEK_LAG": "100"}
        with mock.patch.dict(os.environ, env):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["tile_seek_lag"], 40.0)
        self.assertIn("tile_seek_lag=100.0->40.0", cfg["adjusted"])

    def test_stage_auto_seek_lag_and_grace_are_bounded(self):
        with mock.patch.dict(os.environ, {"GIF_MONITOR_STAGE_AUTO_SEEK_LAG": "8",
                                          "GIF_MONITOR_STAGE_USER_GRACE_SECONDS": "60"}):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["stage_auto_seek_lag"], 8.0)
        self.assertEqual(cfg["stage_user_grace_seconds"], 60.0)
        self.assertEqual(cfg["adjusted"], [])
        # off spellings
        for off in ("0", "off"):
            with mock.patch.dict(os.environ, {"GIF_MONITOR_STAGE_AUTO_SEEK_LAG": off}):
                self.assertEqual(monitor_server._build_latency_config()["stage_auto_seek_lag"], 0.0, off)
        # below target+2 → raised; above hard limit → capped
        with mock.patch.dict(os.environ, {"GIF_MONITOR_STAGE_AUTO_SEEK_LAG": "1"}):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["stage_auto_seek_lag"], cfg["hls_target_seconds"] + 2)
        with mock.patch.dict(os.environ, {"GIF_MONITOR_HARD_SEEK_LAG": "40",
                                          "GIF_MONITOR_STAGE_AUTO_SEEK_LAG": "100"}):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["stage_auto_seek_lag"], 40.0)
        # grace: 0 allowed (= no grace), huge values capped at 600
        with mock.patch.dict(os.environ, {"GIF_MONITOR_STAGE_USER_GRACE_SECONDS": "0"}):
            self.assertEqual(monitor_server._build_latency_config()["stage_user_grace_seconds"], 0.0)
        with mock.patch.dict(os.environ, {"GIF_MONITOR_STAGE_USER_GRACE_SECONDS": "5000"}):
            cfg = monitor_server._build_latency_config()
        self.assertEqual(cfg["stage_user_grace_seconds"], 600.0)
        self.assertIn("stage_user_grace_seconds=5000.0->600.0", cfg["adjusted"])

    def test_catchup_rate_switch_is_independent_of_master(self):
        with mock.patch.dict(os.environ, {"GIF_MONITOR_CATCHUP_RATE_ENABLED": "false"}):
            cfg = monitor_server._build_latency_config()
        self.assertFalse(cfg["catchup_rate_enabled"])
        self.assertEqual(cfg["adjusted"], [])
        self.assertTrue(monitor_server.env_flag("GIF_MONITOR_NOPE", True))
        with mock.patch.dict(os.environ, {"GIF_MONITOR_NOPE": "  "}):
            self.assertTrue(monitor_server.env_flag("GIF_MONITOR_NOPE", True))
        for off in ("0", "False", "NO", "off"):
            with mock.patch.dict(os.environ, {"GIF_MONITOR_NOPE": off}):
                self.assertFalse(monitor_server.env_flag("GIF_MONITOR_NOPE", True), off)

    def test_x264_preset_env_is_validated(self):
        with mock.patch.dict(os.environ, {"GIF_PREVIEW_X264_PRESET": "ultrafast"}):
            self.assertEqual(live_preview.x264_preset_from_env(), "ultrafast")
        with mock.patch.dict(os.environ, {"GIF_PREVIEW_X264_PRESET": "placebo"}):
            self.assertEqual(live_preview.x264_preset_from_env(), "veryfast")
        with TemporaryDirectory() as tmp:
            manager = live_preview.LivePreviewManager(
                root=Path(tmp), background_reaper=False, x264_preset="superfast"
            )
            command = manager._build_command("rtmp://host/app/s", Path(tmp))
        self.assertEqual(command[command.index("-preset") + 1], "superfast")


class FeishuAuthIntegrationTests(unittest.TestCase):
    """The login wall ships disabled: no credentials in the test env."""

    def test_auth_disabled_without_credentials(self):
        self.assertFalse(monitor_server.FEISHU_AUTH.enabled)
        client = monitor_server.app.test_client()
        me = client.get("/api/auth/me").get_json()
        self.assertEqual(me, {"enabled": False, "user": None})
        # Core routes stay reachable exactly as before the wall existed.
        health = client.get("/api/health").get_json()
        self.assertTrue(health["ok"])
        self.assertEqual(client.get("/api/auth/feishu/login").status_code, 404)


if __name__ == "__main__":
    unittest.main()
