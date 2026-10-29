"""Tests for the feature-flagged browser-extension clip ingest."""

from __future__ import annotations

import io
import json
import re
import subprocess
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from flask import Flask

import monitor_extension
from monitor_extension import (
    EBML_MAGIC,
    ExtSourceStore,
    ExtensionHooks,
    compute_trim,
    encode_from_webm,
    install_extension_routes,
    is_webm_head,
    parse_segments_json,
)

CLIP_NAME_RE = re.compile(r"clip_\d{8}-\d{6}_[0-9a-f]{6}\.gif")


class PureHelperTests(unittest.TestCase):
    def test_webm_head(self):
        self.assertTrue(is_webm_head(EBML_MAGIC + b"\x00"))
        self.assertFalse(is_webm_head(b"GIF89a"))
        self.assertFalse(is_webm_head(b""))

    def test_parse_segments_ok(self):
        raw = json.dumps([{"start_epoch_ms": 1000, "duration_ms": 30000}, {"start_epoch_ms": 31000, "duration_ms": 30000}])
        parsed = parse_segments_json(raw, 2)
        self.assertEqual([s["start_epoch_ms"] for s in parsed], [1000.0, 31000.0])

    def test_parse_segments_rejects_bad_input(self):
        with self.assertRaises(ValueError):
            parse_segments_json("not json", 1)
        with self.assertRaises(ValueError):
            parse_segments_json("[]", 0)
        with self.assertRaises(ValueError):
            parse_segments_json(json.dumps([{"start_epoch_ms": 1, "duration_ms": 1}]), 2)
        with self.assertRaises(ValueError):
            parse_segments_json(json.dumps([{"start_epoch_ms": 1}]), 1)
        with self.assertRaises(ValueError):
            parse_segments_json(json.dumps([{"start_epoch_ms": 1, "duration_ms": 0}]), 1)
        with self.assertRaises(ValueError):
            parse_segments_json(
                json.dumps([{"start_epoch_ms": 5000, "duration_ms": 1}, {"start_epoch_ms": 1000, "duration_ms": 1}]), 2
            )

    def test_compute_trim_inside_one_segment(self):
        segments = [{"start_epoch_ms": 10_000, "duration_ms": 30_000}]
        self.assertEqual(compute_trim(segments, 12_500, 20_000), (2.5, 7.5))

    def test_compute_trim_across_two_segments(self):
        segments = [
            {"start_epoch_ms": 10_000, "duration_ms": 30_000},
            {"start_epoch_ms": 40_000, "duration_ms": 30_000},
        ]
        self.assertEqual(compute_trim(segments, 35_000, 45_000), (25.0, 10.0))

    def test_compute_trim_tolerates_one_second_slop_then_refuses(self):
        segments = [{"start_epoch_ms": 10_000, "duration_ms": 30_000}]
        self.assertEqual(compute_trim(segments, 9_200, 15_000), (0.0, 5.8))
        with self.assertRaises(ValueError):
            compute_trim(segments, 8_000, 15_000)
        with self.assertRaises(ValueError):
            compute_trim(segments, 30_000, 42_000)


class ExtSourceStoreTests(unittest.TestCase):
    def test_register_expire_removes_directory(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp) / "54622319" / ".ext_src_abc"
            directory.mkdir(parents=True)
            store = ExtSourceStore(ttl_seconds=100.0)
            store.register(directory, now=1000.0)
            self.assertTrue(store.alive(directory))
            self.assertEqual(store.expire(now=1050.0), 0)
            self.assertTrue(directory.is_dir())
            self.assertEqual(store.expire(now=1100.0), 1)
            self.assertFalse(directory.exists())
            self.assertFalse(store.alive(directory))

    def test_sweep_orphans_keeps_gifs(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            match = root / "54622319"
            (match / ".ext_src_old").mkdir(parents=True)
            (match / ".pin_other").mkdir()
            gif = match / "clip_20261027-120000_abcdef.gif"
            gif.write_bytes(b"GIF")
            self.assertEqual(ExtSourceStore.sweep_orphans(root), 1)
            self.assertFalse((match / ".ext_src_old").exists())
            self.assertTrue((match / ".pin_other").is_dir())
            self.assertTrue(gif.is_file())

    def test_shutdown_drops_everything(self):
        with TemporaryDirectory() as tmp:
            directory = Path(tmp) / ".ext_src_x"
            directory.mkdir()
            store = ExtSourceStore(ttl_seconds=999.0)
            store.register(directory)
            store.shutdown()
            self.assertFalse(directory.exists())


class RouteTests(unittest.TestCase):
    """The routes over a bare Flask app with fake hooks and a fake encoder."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.app = Flask("ext-test")
        self.store = ExtSourceStore(ttl_seconds=600.0)
        self.meta: dict[Path, dict] = {}
        self.alert_match = {
            "mode": "alert", "candidates": 1, "ambiguous": False, "weak": False,
            "delta_seconds": -3.0, "candidate_deltas": [-3.0], "alert_seq": 7,
            "alert_minute": "2H 68", "minute": "68", "minute_extra": "", "score": "1-1",
            "event_type": "goal",
        }
        self.alert_calls = []

        def match_clip_alert(match_id, anchor):
            self.alert_calls.append((match_id, anchor))
            return dict(self.alert_match)

        self.hooks = ExtensionHooks(
            clip_dir=lambda match_id: self.root / match_id,
            clip_video_filter=lambda crop: f"fps=16.6667,{'crop' if crop else 'full'}",
            parse_clip_crop=self._parse_crop,
            write_clip_meta=lambda p, m: p.with_suffix(".json").write_text(json.dumps(m), encoding="utf-8"),
            read_clip_meta=lambda p: json.loads(p.with_suffix(".json").read_text(encoding="utf-8")),
            match_clip_alert=match_clip_alert,
            default_clip_form=lambda am, match_id: {"alert_seq": am.get("alert_seq"), "match_event": 1, "match_time": "68", "match_score": "1-1", "title": ""},
            alert_match_public=lambda am: {"mode": am.get("mode"), "alert_seq": am.get("alert_seq")},
            match_detail_for=lambda match_id: {"team_A_name": "甲", "team_B_name": "乙"},
            validate_match_id=self._validate_match_id,
            clip_name_valid=lambda name: CLIP_NAME_RE.fullmatch(name) is not None,
            resolve_ffmpeg=lambda: "ffmpeg",
            clip_range_max_seconds=30.0,
            publish_enabled=lambda: True,
            delivery_mode=lambda: "publish",
        )
        install_extension_routes(self.app, self.hooks, clip_output_root=self.root, source_store=self.store, start_reaper=False)
        self.client = self.app.test_client()
        self.encode_calls = []
        self.encode_patch = mock.patch.object(monitor_extension, "encode_from_webm", side_effect=self._fake_encode)
        self.encode_patch.start()

    def tearDown(self):
        self.encode_patch.stop()
        self.store.shutdown()
        self.tmp.cleanup()

    @staticmethod
    def _validate_match_id(value):
        if not re.fullmatch(r"\d{6,12}", value):
            raise ValueError("match_id 无效")
        return value

    @staticmethod
    def _parse_crop(raw):
        if raw is None:
            return None
        if not isinstance(raw, dict) or raw.get("w", 0) <= 0:
            raise ValueError("crop 无效")
        return {k: float(raw[k]) for k in ("x", "y", "w", "h")}

    def _fake_encode(self, ffmpeg, sources, ss, window, video_filter, output_path, timeout=120.0):
        self.encode_calls.append({"sources": list(sources), "ss": ss, "window": window, "filter": video_filter, "output": output_path})
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"GIF89a-fake")

    def _upload(self, *, files=None, segments=None, start=12_500, end=20_000, extra=None, match_id="54622319"):
        if segments is None:
            segments = [{"start_epoch_ms": 10_000, "duration_ms": 30_000}]
        if files is None:
            files = [EBML_MAGIC + b"\x00" * 64] * len(segments)
        data = {
            "match_id": match_id,
            "start_epoch_ms": str(start),
            "end_epoch_ms": str(end),
            "segments_json": json.dumps(segments),
            "source_host": "live.example.com",
            "capture_mode": "captureStream",
        }
        if extra:
            data.update(extra)
        data["file"] = [(io.BytesIO(body), f"seg_{i}.webm") for i, body in enumerate(files)]
        return self.client.post("/api/monitor/ext/clip", data=data, content_type="multipart/form-data")

    def test_config_reports_server_knobs(self):
        response = self.client.get("/api/monitor/ext/config")
        self.assertEqual(response.status_code, 200)
        body = response.get_json()
        self.assertTrue(body["enabled"])
        self.assertEqual(body["clip_range_max_seconds"], 30.0)
        self.assertEqual(body["buffer_seconds"], monitor_extension.EXT_BUFFER_SECONDS)
        self.assertEqual(body["max_files"], 4)
        self.assertEqual(body["delivery_mode"], "publish")
        self.assertTrue(body["publish_enabled"])

    def test_clip_success_writes_gif_sidecar_and_keeps_source(self):
        response = self._upload()
        self.assertEqual(response.status_code, 200, response.get_json())
        body = response.get_json()
        self.assertTrue(body["ok"])
        self.assertEqual(body["source"], "extension")
        self.assertTrue(CLIP_NAME_RE.fullmatch(body["name"]), body["name"])
        self.assertEqual(body["clip_url"], f"clips/54622319/{body['name']}")
        self.assertEqual(body["before_seconds"], 7.5)
        self.assertEqual(body["event_key"], "manual_20")
        self.assertEqual(body["form"]["match_time"], "68")
        self.assertEqual(body["alert_match"]["alert_seq"], 7)
        # ffmpeg got the trimmed window and the full-frame filter.
        self.assertEqual(len(self.encode_calls), 1)
        call = self.encode_calls[0]
        self.assertEqual((call["ss"], call["window"]), (2.5, 7.5))
        self.assertEqual(call["filter"], "fps=16.6667,full")
        self.assertEqual([p.name for p in call["sources"]], ["seg_000.webm"])
        # Alert match anchored at the end frame (no offset).
        self.assertEqual(self.alert_calls, [("54622319", 20.0)])
        # Sidecar carries everything publish needs plus the extension provenance.
        gif = self.root / "54622319" / body["name"]
        meta = json.loads(gif.with_suffix(".json").read_text(encoding="utf-8"))
        self.assertEqual(meta["source"], "extension")
        self.assertEqual(meta["event_key"], "manual_20")
        self.assertTrue(meta["anchored"])
        self.assertEqual(meta["anchor_epoch_ms"], 20_000)
        self.assertEqual(meta["ext_source"]["source_host"], "live.example.com")
        self.assertEqual(meta["ext_source"]["segments"], [{"start_epoch_ms": 10_000.0, "duration_ms": 30_000.0}])
        self.assertEqual(meta["match_detail"]["team_A_name"], "甲")
        self.assertIsNone(meta["crop"])
        # Raw source retained for re-crop.
        source_dir = gif.parent / meta["ext_source"]["dir"]
        self.assertTrue(source_dir.name.startswith(".ext_src_"))
        self.assertTrue(self.store.alive(source_dir))
        self.assertTrue((source_dir / "seg_000.webm").is_file())

    def test_clip_applies_source_offset_and_crop(self):
        response = self._upload(extra={"source_offset_seconds": "-4.5", "crop": json.dumps({"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5})})
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(self.alert_calls, [("54622319", 15.5)])
        self.assertEqual(self.encode_calls[0]["filter"], "fps=16.6667,crop")
        self.assertEqual(response.get_json()["crop"], {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5})
        self.assertEqual(response.get_json()["event_key"], "manual_15")

    def test_clip_two_segments_in_order(self):
        segments = [
            {"start_epoch_ms": 10_000, "duration_ms": 30_000},
            {"start_epoch_ms": 40_000, "duration_ms": 30_000},
        ]
        response = self._upload(segments=segments, start=35_000, end=45_000)
        self.assertEqual(response.status_code, 200, response.get_json())
        call = self.encode_calls[0]
        self.assertEqual([p.name for p in call["sources"]], ["seg_000.webm", "seg_001.webm"])
        self.assertEqual((call["ss"], call["window"]), (25.0, 10.0))

    def test_clip_rejects_bad_range(self):
        self.assertEqual(self._upload(start=20_000, end=20_100).status_code, 400)
        self.assertEqual(self._upload(start=10_000, end=50_000).get_json()["code"], "range_too_long")
        self.assertEqual(self._upload(start=5_000, end=15_000).get_json()["code"], "range_out_of_buffer")
        self.assertEqual(self.encode_calls, [])

    def test_clip_rejects_non_webm_and_cleans_up(self):
        response = self._upload(files=[b"GIF89a" + b"\x00" * 64])
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["code"], "file_invalid")
        self.assertEqual(list((self.root / "54622319").glob(".ext_src_*")) if (self.root / "54622319").exists() else [], [])
        self.assertEqual(self.encode_calls, [])

    def test_clip_rejects_segment_count_mismatch_and_too_many(self):
        response = self._upload(files=[EBML_MAGIC + b"\x00" * 8] * 2)
        self.assertEqual(response.get_json()["code"], "segments_invalid")
        segments = [{"start_epoch_ms": 10_000 + i * 1000, "duration_ms": 1000} for i in range(5)]
        response = self._upload(segments=segments, start=10_500, end=12_000)
        self.assertEqual(response.get_json()["code"], "too_many_files")

    def test_clip_rejects_oversized_upload(self):
        with mock.patch.object(monitor_extension, "EXT_UPLOAD_MAX_BYTES", 100):
            response = self._upload(files=[EBML_MAGIC + b"\x00" * 200])
        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.get_json()["code"], "upload_too_large")
        self.assertEqual(self.encode_calls, [])

    def test_clip_rejects_bad_match_id_and_missing_fields(self):
        self.assertEqual(self._upload(match_id="abc").status_code, 400)
        response = self.client.post(
            "/api/monitor/ext/clip",
            data={"match_id": "54622319", "end_epoch_ms": "1"},
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("start_epoch_ms", response.get_json()["error"])

    def test_clip_encode_failure_returns_500_and_drops_source(self):
        self.encode_patch.stop()
        with mock.patch.object(monitor_extension, "encode_from_webm", side_effect=RuntimeError("GIF 编码失败：boom")):
            response = self._upload()
        self.encode_patch.start()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.get_json()["code"], "encode_failed")
        self.assertEqual(list((self.root / "54622319").glob(".ext_src_*")), [])
        self.assertEqual(list((self.root / "54622319").glob("clip_*.gif")), [])

    def _recrop(self, name, crop, match_id="54622319"):
        return self.client.post("/api/monitor/ext/clip/recrop", json={"match_id": match_id, "name": name, "crop": crop})

    def test_recrop_reencodes_in_place_and_updates_sidecar(self):
        name = self._upload().get_json()["name"]
        response = self._recrop(name, {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4})
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()["name"], name)
        self.assertEqual(len(self.encode_calls), 2)
        call = self.encode_calls[1]
        self.assertEqual(call["filter"], "fps=16.6667,crop")
        self.assertEqual((call["ss"], call["window"]), (2.5, 7.5))
        self.assertEqual([p.name for p in call["sources"]], ["seg_000.webm"])
        gif = self.root / "54622319" / name
        self.assertTrue(gif.is_file())
        meta = json.loads(gif.with_suffix(".json").read_text(encoding="utf-8"))
        self.assertEqual(meta["crop"], {"x": 0.2, "y": 0.2, "w": 0.4, "h": 0.4})
        self.assertEqual(meta["event_key"], "manual_20")
        self.assertEqual(list(gif.parent.glob(".recrop_*")), [])

    def test_recrop_after_expiry_is_409(self):
        name = self._upload().get_json()["name"]
        self.store.expire(now=time.time() + 10_000)
        response = self._recrop(name, {"x": 0, "y": 0, "w": 0.5, "h": 0.5})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "source_expired")

    def test_recrop_refuses_non_extension_clip_and_bad_name(self):
        directory = self.root / "54622319"
        directory.mkdir(parents=True)
        gif = directory / "clip_20261027-120000_abcdef.gif"
        gif.write_bytes(b"GIF")
        gif.with_suffix(".json").write_text(json.dumps({"event_key": "manual_1"}), encoding="utf-8")
        response = self._recrop(gif.name, {"x": 0, "y": 0, "w": 0.5, "h": 0.5})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.get_json()["code"], "not_extension")
        self.assertEqual(self._recrop("../evil.gif", {}).status_code, 400)
        self.assertEqual(self._recrop("clip_20261027-120000_000000.gif", {}).get_json()["code"], "clip_missing")


class FlagOffTests(unittest.TestCase):
    def test_routes_absent_when_disabled(self):
        import monitor_server

        if monitor_server.EXTENSION_ENABLED:
            self.skipTest("本机 .env 打开了 GIF_MONITOR_EXTENSION_ENABLED")
        rules = [str(rule) for rule in monitor_server.app.url_map.iter_rules()]
        self.assertFalse(any("/api/monitor/ext/" in rule for rule in rules), rules)
        self.assertIsNone(monitor_server.ext_source_store)


class RealFfmpegTests(unittest.TestCase):
    """End-to-end encode with real ffmpeg on two synthetic webm segments."""

    def _make_webm(self, path: Path, seconds: int, color: str) -> None:
        import monitor_server

        ffmpeg = monitor_server.resolve_ffmpeg()
        command = [
            ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", f"color=c={color}:s=320x180:d={seconds}:r=15",
            "-c:v", "libvpx", "-b:v", "300k", "-t", str(seconds), str(path),
        ]
        result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if result.returncode != 0:
            self.skipTest("本机 ffmpeg 无法生成 webm 测试片段")

    def test_encode_from_two_webm_segments(self):
        import monitor_server

        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "seg_000.webm"
            second = root / "seg_001.webm"
            self._make_webm(first, 3, "green")
            self._make_webm(second, 3, "blue")
            self.assertTrue(is_webm_head(first.read_bytes()[:4]))
            output = root / "clip.gif"
            encode_from_webm(
                monitor_server.resolve_ffmpeg(), [first, second], ss=2.0, window=2.0,
                video_filter=monitor_server._clip_video_filter(None), output_path=output,
            )
            self.assertTrue(output.is_file())
            self.assertGreater(output.stat().st_size, 0)
            self.assertEqual(list(root.glob(".concat_*")), [])

    # Three 2 s segments, red / lime / blue, so a frame's colour says where
    # on the concatenated timeline it came from.
    COLOURS = ("red", "lime", "blue")

    def _encode_rgb(self, root: Path, ss: float, window: float) -> Path:
        import monitor_server

        sources = []
        for index, colour in enumerate(self.COLOURS):
            path = root / f"seg_{index:03d}.webm"
            self._make_webm(path, 2, colour)
            sources.append(path)
        output = root / "clip.gif"
        encode_from_webm(
            monitor_server.resolve_ffmpeg(), sources, ss=ss, window=window,
            video_filter=monitor_server._clip_video_filter(None), output_path=output,
        )
        return output

    @staticmethod
    def _colour(rgb: tuple[int, ...]) -> str:
        r, g, b = rgb
        if r > 180 and g < 80 and b < 80:
            return "red"
        if g > 180 and r < 80 and b < 80:
            return "lime"
        if b > 180 and r < 80 and g < 80:
            return "blue"
        return "other"

    def _frame_colours(self, gif: Path) -> list[str]:
        import monitor_server

        raw = subprocess.run(
            [monitor_server.resolve_ffmpeg(), "-nostdin", "-loglevel", "error", "-i", str(gif),
             "-vf", "scale=1:1", "-f", "rawvideo", "-pix_fmt", "rgb24", "-"],
            capture_output=True, check=True,
        ).stdout
        return [self._colour(tuple(raw[i:i + 3])) for i in range(0, len(raw), 3)]

    def _palette_colours(self, gif: Path) -> set[str]:
        """Every entry of the GIF's global and local colour tables, classified."""
        data = gif.read_bytes()
        entries: list[tuple[int, ...]] = []

        def table(pos: int, packed: int) -> int:
            size = 3 * (2 << (packed & 7))
            entries.extend(tuple(data[i:i + 3]) for i in range(pos, pos + size, 3))
            return pos + size

        def skip_sub_blocks(pos: int) -> int:
            while data[pos]:
                pos += data[pos] + 1
            return pos + 1

        pos = 13
        if data[10] & 0x80:
            pos = table(pos, data[10])
        while pos < len(data) and data[pos] != 0x3B:
            if data[pos] == 0x21:  # extension block
                pos = skip_sub_blocks(pos + 2)
            elif data[pos] == 0x2C:  # image descriptor
                packed = data[pos + 9]
                pos += 10
                if packed & 0x80:
                    pos = table(pos, packed)
                pos = skip_sub_blocks(pos + 1)
            else:
                self.fail(f"unexpected GIF block 0x{data[pos]:02x}")
        return {self._colour(rgb) for rgb in entries}

    def test_cut_lands_on_the_requested_moment(self):
        # [1.5 s, 2.5 s]: half red, half lime, switching mid-clip.
        with TemporaryDirectory() as tmp:
            frames = self._frame_colours(self._encode_rgb(Path(tmp), ss=1.5, window=1.0))
        self.assertIn(len(frames), (16, 17, 18))  # 1 s at 16.67 fps
        self.assertEqual(frames[0], "red")
        self.assertEqual(frames[-1], "lime")
        self.assertEqual(set(frames), {"red", "lime"})
        switch = frames.index("lime")
        self.assertLessEqual(abs(switch - len(frames) / 2), 1.5, frames)

    def test_only_the_clip_is_encoded(self):
        # [2.4 s, 3.6 s] lies inside the lime segment. Footage outside the
        # window must not reach palettegen: the old output-side -ss/-t ran
        # the whole upload through the graph and put red and blue into the
        # palette (and made the encode cost scale with the upload size).
        with TemporaryDirectory() as tmp:
            output = self._encode_rgb(Path(tmp), ss=2.4, window=1.2)
            frames = self._frame_colours(output)
            palette = self._palette_colours(output)
        self.assertIn(len(frames), (19, 20, 21))
        self.assertEqual(set(frames), {"lime"})
        self.assertIn("lime", palette)
        self.assertNotIn("red", palette)
        self.assertNotIn("blue", palette)


class ExtensionSelectSegmentsTests(unittest.TestCase):
    """``selectSegments`` in extension/gif-clipper/content/recorder.js, run under node."""

    @classmethod
    def setUpClass(cls):
        import shutil

        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        cls.script = Path(__file__).parent / "extension" / "gif-clipper" / "content" / "recorder.js"
        if not cls.script.is_file():
            raise unittest.SkipTest("recorder.js missing")

    def run_js(self, start: int, end: int, segments=None):
        segments = segments if segments is not None else [
            {"startEpochMs": 0, "durationMs": 30000},
            {"startEpochMs": 30000, "durationMs": 30000},
            {"startEpochMs": 60000, "durationMs": 30000},
        ]
        script = (
            f"const {{selectSegments}} = require({json.dumps(str(self.script))});"
            f"process.stdout.write(JSON.stringify(selectSegments({json.dumps(segments)}, {start}, {end}).map(s => s.startEpochMs)));"
        )
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_inside_one_segment(self):
        self.assertEqual(self.run_js(5000, 20000), [0])

    def test_across_two_segments(self):
        self.assertEqual(self.run_js(25000, 40000), [0, 30000])
        self.assertEqual(self.run_js(59500, 85000), [30000, 60000])

    def test_start_within_one_second_before_buffer_is_tolerated(self):
        self.assertEqual(self.run_js(-500, 5000), [0])

    def test_start_fell_off_buffer_is_empty(self):
        self.assertEqual(self.run_js(-2000, 5000), [])
        self.assertEqual(self.run_js(5000, 20000, segments=[{"startEpochMs": 30000, "durationMs": 30000}]), [])


class ExtensionPickSourceTests(unittest.TestCase):
    """``pickSource`` in extension/gif-clipper/content/finder.js, run under node.

    The recording source must stick to its <video> while it is in the DOM
    (pause / stall must not drop the buffer); it switches only when the
    element is gone or a rival keeps playing for CHALLENGE_MS.
    """

    CHALLENGE_MS = 8000

    @classmethod
    def setUpClass(cls):
        import shutil

        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        cls.script = Path(__file__).parent / "extension" / "gif-clipper" / "content" / "finder.js"
        if not cls.script.is_file():
            raise unittest.SkipTest("finder.js missing")

    def pick(self, current, candidates, challenger=None, now=100_000, review_open=False):
        script = (
            f"const {{pickSource, CHALLENGE_MS}} = require({json.dumps(str(self.script))});"
            f"const r = pickSource({json.dumps(current)}, {json.dumps(candidates)}, "
            f"{json.dumps(challenger)}, {now}, {json.dumps(review_open)});"
            "process.stdout.write(JSON.stringify({r, CHALLENGE_MS}));"
        )
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        data = json.loads(result.stdout)
        self.assertEqual(data["CHALLENGE_MS"], self.CHALLENGE_MS)
        return data["r"]

    def test_first_pick_takes_largest_playing(self):
        r = self.pick(None, [{"el": "a", "area": 100}, {"el": "b", "area": 300}])
        self.assertEqual((r["next"], r["reason"], r["challenger"]), ("b", "none", None))

    def test_nothing_to_pick(self):
        r = self.pick(None, [])
        self.assertEqual((r["next"], r["reason"]), (None, "none"))

    def test_paused_current_alone_is_kept(self):
        cur = {"el": "a", "connected": True, "playing": False, "area": 300}
        r = self.pick(cur, [])
        self.assertEqual((r["next"], r["reason"], r["challenger"]), ("a", "keep", None))

    def test_removed_current_hard_switches(self):
        cur = {"el": "a", "connected": False, "playing": False, "area": 300}
        r = self.pick(cur, [{"el": "b", "area": 50}])
        self.assertEqual((r["next"], r["reason"]), ("b", "hard"))
        r = self.pick(cur, [])
        self.assertEqual((r["next"], r["reason"]), (None, "hard"))

    def test_smaller_rival_never_challenges_playing_current(self):
        cur = {"el": "a", "connected": True, "playing": True, "area": 300}
        r = self.pick(cur, [{"el": "b", "area": 100}])
        self.assertEqual((r["next"], r["reason"], r["challenger"]), ("a", "keep", None))

    def test_rival_starts_challenge_then_switches_after_bar(self):
        cur = {"el": "a", "connected": True, "playing": False, "area": 300}
        r = self.pick(cur, [{"el": "b", "area": 100}], now=1000)
        self.assertEqual((r["next"], r["reason"]), ("a", "keep"))
        self.assertEqual(r["challenger"], {"el": "b", "since": 1000})
        r = self.pick(cur, [{"el": "b", "area": 100}], challenger=r["challenger"], now=1000 + self.CHALLENGE_MS - 1)
        self.assertEqual((r["next"], r["reason"]), ("a", "keep"))
        self.assertEqual(r["challenger"], {"el": "b", "since": 1000})
        r = self.pick(cur, [{"el": "b", "area": 100}], challenger=r["challenger"], now=1000 + self.CHALLENGE_MS)
        self.assertEqual((r["next"], r["reason"], r["challenger"]), ("b", "soft", None))

    def test_bigger_rival_challenges_even_while_current_plays(self):
        cur = {"el": "a", "connected": True, "playing": True, "area": 100}
        r = self.pick(cur, [{"el": "b", "area": 300}], now=1000)
        self.assertEqual(r["challenger"], {"el": "b", "since": 1000})

    def test_challenger_change_restarts_timer(self):
        cur = {"el": "a", "connected": True, "playing": False, "area": 300}
        r = self.pick(cur, [{"el": "c", "area": 500}], challenger={"el": "b", "since": 0}, now=50_000)
        self.assertEqual((r["next"], r["reason"]), ("a", "keep"))
        self.assertEqual(r["challenger"], {"el": "c", "since": 50_000})

    def test_review_open_blocks_soft_switch_only(self):
        cur = {"el": "a", "connected": True, "playing": False, "area": 300}
        r = self.pick(cur, [{"el": "b", "area": 900}], challenger={"el": "b", "since": 0}, now=50_000, review_open=True)
        self.assertEqual((r["next"], r["reason"], r["challenger"]), ("a", "keep", None))
        gone = dict(cur, connected=False)
        r = self.pick(gone, [{"el": "b", "area": 900}], review_open=True)
        self.assertEqual((r["next"], r["reason"]), ("b", "hard"))


class ExtensionCropForClipTests(unittest.TestCase):
    """``cropForClip`` in extension/gif-clipper/content/finder.js, run under node.

    The match's saved crop box applies to every clip, but a box drawn on a
    differently shaped stream is withheld (full frame) and flagged.
    """

    BOX = {"x": 0.1, "y": 0.2, "w": 0.5, "h": 0.4}

    @classmethod
    def setUpClass(cls):
        import shutil

        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        cls.script = Path(__file__).parent / "extension" / "gif-clipper" / "content" / "finder.js"
        if not cls.script.is_file():
            raise unittest.SkipTest("finder.js missing")

    def crop_for(self, entry, aspect):
        script = (
            f"const {{cropForClip}} = require({json.dumps(str(self.script))});"
            f"process.stdout.write(JSON.stringify(cropForClip({json.dumps(entry)}, {json.dumps(aspect)})));"
        )
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_no_entry_or_full_frame_is_uncropped(self):
        for entry in (None, {}, {"crop": None, "last": self.BOX, "aspect": 16 / 9}):
            self.assertEqual(self.crop_for(entry, 16 / 9), {"crop": None, "aspectChanged": False})

    def test_saved_box_applies_on_same_shape(self):
        entry = {"crop": self.BOX, "aspect": 16 / 9}
        self.assertEqual(self.crop_for(entry, 1920 / 1080), {"crop": self.BOX, "aspectChanged": False})
        # Small rounding differences (e.g. 854x480) stay within tolerance.
        self.assertEqual(self.crop_for(entry, 854 / 480)["crop"], self.BOX)

    def test_changed_shape_withholds_box(self):
        entry = {"crop": self.BOX, "aspect": 16 / 9}
        self.assertEqual(self.crop_for(entry, 4 / 3), {"crop": None, "aspectChanged": True})

    def test_unknown_aspect_keeps_box(self):
        self.assertEqual(self.crop_for({"crop": self.BOX, "aspect": None}, 4 / 3)["crop"], self.BOX)
        self.assertEqual(self.crop_for({"crop": self.BOX, "aspect": 16 / 9}, 0)["crop"], self.BOX)


class ExtensionUnstickTargetTests(unittest.TestCase):
    """``unstickTarget`` in extension/gif-clipper/content/review.js, run under node.

    The review player calls it on every "seeking" / "waiting" event and seeks
    to the result, which fires "seeking" again. It must therefore settle: a
    seek near the end of the buffer once looped ~160k times per second.
    """

    @classmethod
    def setUpClass(cls):
        import shutil

        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        cls.script = Path(__file__).parent / "extension" / "gif-clipper" / "content" / "review.js"
        if not cls.script.is_file():
            raise unittest.SkipTest("review.js missing")

    def settle(self, ranges, t, stalled=False):
        """Positions visited until the helper says stay (the browser's loop)."""
        script = (
            f"const {{unstickTarget}} = require({json.dumps(str(self.script))});"
            f"const ranges = {json.dumps(ranges)}; let t = {t}; let stalled = {json.dumps(stalled)};"
            "const seen = [];"
            "for (let i = 0; i < 10; i++) {"
            "  const next = unstickTarget(ranges, t, stalled);"
            "  if (next == null) break;"
            "  seen.push(next); t = next; stalled = false;"  # our own seek fires "seeking"
            "}"
            "process.stdout.write(JSON.stringify(seen));"
        )
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_inside_a_range_stays(self):
        self.assertEqual(self.settle([[0, 10]], 5), [])
        self.assertEqual(self.settle([[0, 10]], 5, stalled=True), [])

    def test_seek_into_hole_jumps_to_next_range(self):
        self.assertEqual(self.settle([[0, 5], [6, 10]], 5.5), [6.01])
        self.assertEqual(self.settle([[1, 5]], 0.2), [1.01])

    def test_stall_at_tail_crosses_small_gap_once(self):
        self.assertEqual(self.settle([[0, 5], [5.08, 10]], 4.95, stalled=True), [5.09])

    def test_seek_near_tail_is_not_bounced(self):
        # Frame-stepping up to a gap must not be thrown into the next range.
        self.assertEqual(self.settle([[0, 5], [5.08, 10]], 4.95), [])

    def test_seek_in_end_zone_does_not_loop(self):
        # Real numbers from the 2026-10-27 probe: buffer ends at 17.386 s and
        # dragging the timeline to the right seeks to total - 0.05 = 17.362 s.
        self.assertEqual(self.settle([[0, 17.386]], 17.362), [])
        self.assertEqual(self.settle([[0, 17.386]], 17.336), [])

    def test_past_the_end_parks_once(self):
        seen = self.settle([[0, 17.386]], 18.0)
        self.assertEqual(len(seen), 1)
        self.assertAlmostEqual(seen[0], 17.336, places=6)

    def test_always_settles(self):
        layouts = [
            [[0, 5], [5.02, 5.03], [5.1, 10]],   # tiny range between holes
            [[0, 0.004]],                          # degenerate single range
            [[0, 5], [5.05, 10]],
        ]
        for ranges in layouts:
            for t in (-1, 0, 4.9, 5.0, 5.025, 5.06, 9.9, 10, 11):
                for stalled in (False, True):
                    with self.subTest(ranges=ranges, t=t, stalled=stalled):
                        self.assertLessEqual(len(self.settle(ranges, t, stalled)), 3)


class ExtensionBackgroundUiTests(unittest.TestCase):
    """extension/gif-clipper/background.js under node with a fake ``chrome``.

    The bar and the side panel stay hidden until the toolbar icon or
    Alt+1/2/3; recording runs regardless, except on 8901's own pages. Chrome
    only lets ``sidePanel.open`` through while the click / key press is being
    handled, so it must be called before the handler's first await.
    """

    HARNESS = r"""
const vm = require("vm");
const fs = require("fs");
function load(session = {}) {
  const calls = [];
  const listeners = {};
  const store = { session: JSON.parse(JSON.stringify(session)), local: {} };
  const tick = (fn) => new Promise((r) => setTimeout(() => r(fn()), 0));
  const area = (name) => ({
    get: (keys) => tick(() => {
      const data = store[name];
      if (keys == null) return JSON.parse(JSON.stringify(data));
      const out = {};
      for (const k of Array.isArray(keys) ? keys : [keys]) {
        if (k in data) out[k] = JSON.parse(JSON.stringify(data[k]));
      }
      return out;
    }),
    set: (obj) => tick(() => { Object.assign(store[name], JSON.parse(JSON.stringify(obj))); }),
    remove: (k) => tick(() => { delete store[name][k]; }),
  });
  const event = (name) => ({ addListener: (fn) => { listeners[name] = fn; } });
  const rec = (name) => (...args) => { calls.push([name, ...args]); return Promise.resolve(null); };
  const chrome = {
    storage: { session: area("session"), local: area("local") },
    tabs: {
      sendMessage: rec("tabs.sendMessage"), create: rec("tabs.create"),
      onRemoved: event("tabs.onRemoved"), onActivated: event("tabs.onActivated"),
    },
    sidePanel: {
      open: rec("sidePanel.open"), setOptions: rec("sidePanel.setOptions"),
      setPanelBehavior: rec("sidePanel.setPanelBehavior"),
    },
    action: { onClicked: event("action.onClicked") },
    commands: { onCommand: event("commands.onCommand") },
    runtime: { onMessage: event("runtime.onMessage"), onConnect: event("runtime.onConnect"), sendMessage: rec("runtime.sendMessage") },
    webNavigation: { onCommitted: event("webNavigation.onCommitted") },
  };
  const fetch = async () => ({ ok: true, status: 200, json: async () => ({ buffer_seconds: 300, bitrate: 2000000 }) });
  const ctx = vm.createContext({ chrome, fetch, console, URL, URLSearchParams, setTimeout, clearTimeout });
  vm.runInContext(fs.readFileSync(SCRIPT, "utf8"), ctx);
  const send = (msg, sender) => new Promise((r) => listeners["runtime.onMessage"](msg, sender, r));
  const named = (name) => calls.filter((c) => c[0] === name);
  const messages = (tabId) => named("tabs.sendMessage").filter((c) => c[1] === tabId)
    .map((c) => ({ msg: c[2], frameId: c[3] ? c[3].frameId : null }));
  return { ctx, calls, listeners, store, send, named, messages };
}
const settle = () => new Promise((r) => setTimeout(r, 50));
const out = (value) => process.stdout.write(JSON.stringify(value));
(async () => {
__BODY__
})().catch((err) => { console.error(err); process.exit(1); });
"""

    @classmethod
    def setUpClass(cls):
        import shutil

        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        cls.script = Path(__file__).parent / "extension" / "gif-clipper" / "background.js"
        if not cls.script.is_file():
            raise unittest.SkipTest("background.js missing")

    def run_js(self, body: str):
        script = f"const SCRIPT = {json.dumps(str(self.script))};" + self.HARNESS.replace("__BODY__", body)
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_icon_opens_panel_in_the_click_and_shows_the_bar(self):
        r = self.run_js(
            """
const h = load();
await settle();
h.listeners["action.onClicked"]({ id: 7 });
const syncOpen = h.named("sidePanel.open").map((c) => c[1]);
await settle();
out({ syncOpen, behavior: h.named("sidePanel.setPanelBehavior").map((c) => c[1]),
      state: h.store.session["tab:7"], messages: h.messages(7) });
"""
        )
        self.assertEqual(r["syncOpen"], [{"tabId": 7}])
        self.assertEqual(r["behavior"], [{"openPanelOnActionClick": False}])
        self.assertTrue(r["state"]["shown"])
        # To every frame (no frameId): the bar shows, the recorder may toast.
        self.assertEqual(r["messages"], [{"msg": {"type": "ui-shown", "shown": True}, "frameId": None}])

    def test_new_tab_starts_hidden(self):
        r = self.run_js(
            """
const h = load();
out(await h.send({ type: "get-tab-state" }, { tab: { id: 4, url: "https://live.example/" }, frameId: 0 }));
"""
        )
        self.assertFalse(r["shown"])

    def test_shortcut_in_hidden_tab_opens_panel_then_answers_in_the_bar(self):
        r = self.run_js(
            """
const h = load();
await settle();
h.listeners["commands.onCommand"]("finish", { id: 3 });
const syncOpen = h.named("sidePanel.open").map((c) => c[1]);
await settle();
out({ syncOpen, state: h.store.session["tab:3"], messages: h.messages(3) });
"""
        )
        self.assertEqual(r["syncOpen"], [{"tabId": 3}])
        self.assertTrue(r["state"]["shown"])
        # The bar is told to show before the answer to the key press arrives.
        self.assertEqual(
            r["messages"],
            [
                {"msg": {"type": "ui-shown", "shown": True}, "frameId": None},
                {"msg": {"type": "bar-status", "text": "没有找到正在播放的视频", "level": "fail"}, "frameId": 0},
            ],
        )

    def test_shortcut_in_shown_tab_leaves_panel_alone(self):
        r = self.run_js(
            """
const h = load({ "tab:5": { matchId: null, mainFrameId: null, shown: true } });
await settle(); // worker restart: shown state comes back from storage.session
h.listeners["commands.onCommand"]("set-start", { id: 5 });
const syncOpen = h.named("sidePanel.open").length;
await settle();
out({ syncOpen, uiShown: h.messages(5).filter((m) => m.msg.type === "ui-shown").length });
"""
        )
        self.assertEqual(r, {"syncOpen": 0, "uiShown": 0})

    def test_alt0_toggles_bar_without_panel(self):
        r = self.run_js(
            """
const h = load();
await settle();
h.listeners["commands.onCommand"]("toggle-bar", { id: 2 });
await settle();
const first = h.store.session["tab:2"].shown;
h.listeners["commands.onCommand"]("toggle-bar", { id: 2 });
await settle();
out({ first, second: h.store.session["tab:2"].shown, open: h.named("sidePanel.open").length,
      shown: h.messages(2).map((m) => m.msg.shown) });
"""
        )
        self.assertEqual(r, {"first": True, "second": False, "open": 0, "shown": [True, False]})

    def test_bar_close_hides_everywhere_in_the_tab(self):
        r = self.run_js(
            """
const h = load();
await settle();
h.listeners["action.onClicked"]({ id: 7 });
await settle();
await h.send({ type: "set-shown", shown: false }, { tab: { id: 7, url: "https://live.example/" }, frameId: 0 });
await settle();
h.listeners["commands.onCommand"]("review", { id: 7 });
out({ state: h.store.session["tab:7"].shown, last: h.messages(7)[1], reopened: h.named("sidePanel.open").length });
"""
        )
        self.assertFalse(r["state"])
        self.assertEqual(r["last"], {"msg": {"type": "ui-shown", "shown": False}, "frameId": None})
        # Hidden again, so the next shortcut brings the panel back.
        self.assertEqual(r["reopened"], 2)

    def test_monitor_page_is_never_recorded(self):
        r = self.run_js(
            """
const h = load();
await settle();
const report = { type: "frame-report", playing: true, area: 1280 * 720 };
await h.send(report, { tab: { id: 1, url: "https://matchgif.aisportsapp.com/gif-monitor/?match_id=1" }, frameId: 0 });
await h.send(report, { tab: { id: 2, url: "https://live.example/watch" }, frameId: 4 });
await settle();
const starts = h.named("tabs.sendMessage").filter((c) => c[2].type === "recorder-start").map((c) => [c[1], c[3].frameId]);
out({ starts, monitor: h.store.session["tab:1"].mainFrameId, live: h.store.session["tab:2"].mainFrameId });
"""
        )
        self.assertEqual(r, {"starts": [[2, 4]], "monitor": None, "live": 4})

    def test_is_monitor_page(self):
        r = self.run_js(
            """
const h = load();
const prod = "https://matchgif.aisportsapp.com/gif-monitor/";
const local = "http://127.0.0.1:8902/";
out([
  h.ctx.isMonitorPage("https://matchgif.aisportsapp.com/gif-monitor/?mode=panel", prod),
  h.ctx.isMonitorPage("https://matchgif.aisportsapp.com/gif-monitor", prod),
  h.ctx.isMonitorPage("https://matchgif.aisportsapp.com/gif-monitor/workshop?x=1", prod),
  h.ctx.isMonitorPage("https://matchgif.aisportsapp.com/gif-monitor-old/", prod),
  h.ctx.isMonitorPage("https://matchgif.aisportsapp.com/", prod),
  h.ctx.isMonitorPage("https://live.example/gif-monitor/", prod),
  h.ctx.isMonitorPage("http://127.0.0.1:8902/anything", local),
  h.ctx.isMonitorPage("http://127.0.0.1:8901/", local),
  h.ctx.isMonitorPage(undefined, prod),
]);
"""
        )
        self.assertEqual(r, [True, True, True, False, False, False, True, False, False])

    def test_concurrent_tab_state_patches_all_land(self):
        r = self.run_js(
            """
const h = load();
await Promise.all([
  h.ctx.setTabState(9, { matchId: "123" }),
  h.ctx.setTabState(9, { mainFrameId: 2 }),
  h.ctx.setTabState(9, { shown: true }),
]);
out(h.store.session["tab:9"]);
"""
        )
        self.assertEqual(r, {"matchId": "123", "mainFrameId": 2, "shown": True})


class ExtensionClusterSliceTests(unittest.TestCase):
    """``indexClusters`` / ``sliceRange`` in recorder.js, run under node.

    The extension uploads only the webm clusters covering the clip instead of
    whole 30 s segments. Anything that cannot be indexed must return null so
    the caller falls back to uploading the segment unchanged.
    """

    @classmethod
    def setUpClass(cls):
        import shutil

        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        cls.script = Path(__file__).parent / "extension" / "gif-clipper" / "content" / "recorder.js"
        if not cls.script.is_file():
            raise unittest.SkipTest("recorder.js missing")

    def run_js(self, body: str):
        script = (
            f"const {{ indexClusters, sliceRange, buildCut }} = require({json.dumps(str(self.script))});"
            "const fs = require('fs');"
            "const out = (v) => process.stdout.write(JSON.stringify(v));"
            + body
        )
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def _make_webm(
        self, path: Path, seconds: int, keyframe_seconds: int = 2, pattern: str = "testsrc"
    ) -> None:
        import monitor_server

        command = [
            monitor_server.resolve_ffmpeg(), "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "lavfi", "-i", f"{pattern}=s=320x180:d={seconds}:r=30",
            "-c:v", "libvpx", "-b:v", "500k", "-g", str(keyframe_seconds * 30),
            "-t", str(seconds), str(path),
        ]
        result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        if result.returncode != 0:
            self.skipTest("本机 ffmpeg 无法生成 webm 测试片段")

    def test_garbage_and_truncated_input_fall_back(self):
        r = self.run_js(
            """
const cases = {
  empty: indexClusters(new Uint8Array(0)),
  garbage: indexClusters(new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])),
  headerOnly: indexClusters(new Uint8Array([0x1a, 0x45, 0xdf, 0xa3, 0x84, 0, 0, 0, 0])),
};
out(Object.fromEntries(Object.entries(cases).map(([k, v]) => [k, v === null])));
"""
        )
        self.assertEqual(r, {"empty": True, "garbage": True, "headerOnly": True})
        # sliceRange must tolerate a null index (that is the fallback path).
        self.assertTrue(self.run_js("out(sliceRange(null, 0, 30000, 1000, 2000) === null);"))

    def test_indexes_real_webm_clusters(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "seg.webm"
            self._make_webm(path, 10, keyframe_seconds=2)
            r = self.run_js(
                f"""
const bytes = new Uint8Array(fs.readFileSync({json.dumps(str(path))}));
const index = indexClusters(bytes);
out({{ clusters: index.clusters.length, headerEnd: index.headerEnd,
       timecodes: index.clusters.map((c) => c.timecodeMs),
       ordered: index.clusters.every((c, i, a) => i === 0 || c.offset > a[i - 1].offset) }});
"""
            )
        # 10 s at a 2 s keyframe interval: a cluster every 2 s.
        self.assertEqual(r["clusters"], 5)
        self.assertEqual(r["timecodes"], [0, 2000, 4000, 6000, 8000])
        self.assertGreater(r["headerEnd"], 0)
        self.assertTrue(r["ordered"])

    def test_slice_keeps_only_the_clusters_the_clip_needs(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "seg.webm"
            self._make_webm(path, 10, keyframe_seconds=2)
            size = path.stat().st_size
            r = self.run_js(
                f"""
const bytes = new Uint8Array(fs.readFileSync({json.dumps(str(path))}));
const index = indexClusters(bytes);
// Segment ran from epoch 100000 for 10 s; ask for 1 s starting at +5.0 s.
const r = sliceRange(index, 100000, 10000, 105000, 106000);
out({{ startEpochMs: r.startEpochMs, durationMs: r.durationMs,
       cut: r.indexHeaderEnd + (r.end - r.start), whole: bytes.length }});
"""
            )
        # The keyframe at +4 s is the one before the clip, so that is where the
        # upload starts, and it runs to the next keyframe after the end (+6 s).
        self.assertEqual(r["startEpochMs"], 104000)
        self.assertEqual(r["durationMs"], 2000)
        self.assertLess(r["cut"], size // 2)

    def test_whole_segment_request_falls_back(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "seg.webm"
            self._make_webm(path, 10, keyframe_seconds=2)
            r = self.run_js(
                f"""
const bytes = new Uint8Array(fs.readFileSync({json.dumps(str(path))}));
const index = indexClusters(bytes);
// Asking for the entire segment cannot shrink anything: expect null so the
// caller uploads the original blob untouched.
out(sliceRange(index, 100000, 10000, 100000, 110000) === null);
"""
            )
        self.assertTrue(r)

    def test_sliced_upload_encodes_the_same_frames_as_the_whole_segment(self):
        """End-to-end: the GIF from a sliced upload is identical to the whole one."""
        import monitor_server

        ffmpeg = monitor_server.resolve_ffmpeg()
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            whole_src = root / "seg.webm"
            self._make_webm(whole_src, 12, keyframe_seconds=2)
            seg_start, seg_duration = 100_000, 12_000
            want_start = seg_start + 5_300          # mid-clip, not on a keyframe
            want_end = want_start + 3_000
            cut_src = root / "cut.webm"
            cut = self.run_js(
                f"""
const bytes = new Uint8Array(fs.readFileSync({json.dumps(str(whole_src))}));
const index = indexClusters(bytes);
const r = sliceRange(index, {seg_start}, {seg_duration}, {want_start}, {want_end});
fs.writeFileSync({json.dumps(str(cut_src))}, buildCut(bytes, index, r));
out({{ startEpochMs: r.startEpochMs, durationMs: r.durationMs }});
"""
            )
            self.assertLess(cut_src.stat().st_size, whole_src.stat().st_size)

            def encode(source: Path, segments: list[dict], name: str) -> Path:
                ss, window = compute_trim(segments, want_start, want_end)
                output = root / name
                encode_from_webm(
                    ffmpeg, [source], ss, window,
                    monitor_server._clip_video_filter(None), output,
                )
                return output

            def frame_hashes(gif: Path) -> list[str]:
                raw = subprocess.run(
                    [ffmpeg, "-nostdin", "-v", "error", "-i", str(gif), "-f", "framehash", "-"],
                    capture_output=True, text=True,
                )
                return [l for l in raw.stdout.splitlines() if l and not l.startswith("#")]

            whole_gif = encode(
                whole_src,
                [{"start_epoch_ms": seg_start, "duration_ms": seg_duration}],
                "whole.gif",
            )
            cut_gif = encode(
                cut_src,
                [{"start_epoch_ms": cut["startEpochMs"], "duration_ms": cut["durationMs"]}],
                "cut.gif",
            )
            whole_frames, cut_frames = frame_hashes(whole_gif), frame_hashes(cut_gif)

        self.assertGreater(len(whole_frames), 0)
        self.assertEqual(whole_frames, cut_frames)

    def test_clip_across_the_seam_encodes_the_same_frames(self):
        """A clip spanning two segments: tail of the first + head of the second."""
        import monitor_server

        ffmpeg = monitor_server.resolve_ffmpeg()
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            # Different patterns, so footage landing in the wrong place shows up.
            sources = [root / "seg_a.webm", root / "seg_b.webm"]
            self._make_webm(sources[0], 12, keyframe_seconds=2, pattern="testsrc")
            self._make_webm(sources[1], 12, keyframe_seconds=2, pattern="testsrc2")
            seg_meta = [
                {"start_epoch_ms": 100_000, "duration_ms": 12_000},
                {"start_epoch_ms": 112_000, "duration_ms": 12_000},
            ]
            want_start = 109_300   # 2.7 s before the seam
            want_end = 114_500     # 2.5 s after it

            cuts = []
            for index, (src, meta) in enumerate(zip(sources, seg_meta)):
                cut_path = root / f"cut_{index}.webm"
                r = self.run_js(
                    f"""
const bytes = new Uint8Array(fs.readFileSync({json.dumps(str(src))}));
const index = indexClusters(bytes);
const r = sliceRange(index, {meta["start_epoch_ms"]}, {meta["duration_ms"]}, {want_start}, {want_end});
fs.writeFileSync({json.dumps(str(cut_path))}, buildCut(bytes, index, r));
out({{ start_epoch_ms: r.startEpochMs, duration_ms: r.durationMs }});
"""
                )
                self.assertLess(cut_path.stat().st_size, src.stat().st_size)
                cuts.append((cut_path, r))

            def encode(files: list[Path], segments: list[dict], name: str) -> list[str]:
                ss, window = compute_trim(segments, want_start, want_end)
                output = root / name
                encode_from_webm(
                    ffmpeg, files, ss, window,
                    monitor_server._clip_video_filter(None), output,
                )
                raw = subprocess.run(
                    [ffmpeg, "-nostdin", "-v", "error", "-i", str(output), "-f", "framehash", "-"],
                    capture_output=True, text=True,
                )
                return [l for l in raw.stdout.splitlines() if l and not l.startswith("#")]

            whole_frames = encode(sources, seg_meta, "whole.gif")
            cut_frames = encode([c[0] for c in cuts], [c[1] for c in cuts], "cut.gif")

        self.assertGreater(len(whole_frames), 0)
        self.assertEqual(len(whole_frames), len(cut_frames))
        self.assertEqual(whole_frames, cut_frames)


if __name__ == "__main__":
    unittest.main()
