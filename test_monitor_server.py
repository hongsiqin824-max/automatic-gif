"""Deterministic tests for the manual monitor service (port 8901)."""

from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

# The monitor reads GIF_SOURCE_SECRET lazily; a placeholder keeps import clean.
os.environ.setdefault("GIF_SOURCE_SECRET", "test-secret")
# Keep tests local-only: do not build the publish stack or start a draft worker.
os.environ.setdefault("GIF_MONITOR_PUBLISH_ENABLED", "false")

import monitor_server
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

    def test_drop_reason_big_league_excluded(self):
        # In all_playing (full set) under an excluded league, but absent from the
        # shown/capturable playing list: the drop is a deliberate big-league skip.
        all_playing = [
            {"match_id": "88888888", "team_A_name": "曼城",
             "team_B_name": "阿森纳", "competition_name": "英超"}
        ]
        with mock.patch.object(monitor_server.match_catalog, "all_playing", all_playing):
            data = self._ingest(match_id="88888888", dedup_key="kbl").get_json()
        self.assertFalse(data["accepted"])
        self.assertEqual(data["detail_reason"], "big_league_excluded")
        self.feishu_mock.assert_not_called()

    def test_drop_reason_beyond_catalog_limit(self):
        # A non-excluded league present in the full set but past the display cap.
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


if __name__ == "__main__":
    unittest.main()
