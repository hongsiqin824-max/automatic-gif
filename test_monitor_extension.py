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
            default_clip_form=lambda am: {"alert_seq": am.get("alert_seq"), "match_event": 1, "match_time": "68", "match_score": "1-1", "title": ""},
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


if __name__ == "__main__":
    unittest.main()
