"""Stage hint for a black source picture (54494599 on 11-07).

The pusher's feed went black for about 4 minutes. The player kept playing the
black picture, so nothing on the page looked broken, and the operator reloaded
the page 4 times - which cannot help when the source itself is black. The
preview status endpoint now reports it (detection: live_preview
``source_black_seconds``, tested in test_live_preview.py) and the stage shows
a banner saying it is the pusher and that reloading does not help.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest import mock

# Same import guards as test_monitor_server.py.
os.environ.setdefault("GIF_SOURCE_SECRET", "test-secret")
os.environ.setdefault("GIF_MONITOR_PUBLISH_ENABLED", "false")
os.environ.setdefault("GIF_MONITOR_ALERT_DB_PATH", "")
os.environ["GIF_MONITOR_FEISHU_APP_ID"] = ""
os.environ["GIF_MONITOR_FEISHU_APP_SECRET"] = ""

import monitor_server


class PreviewStatusSourceBlackTests(unittest.TestCase):
    def setUp(self):
        self.app = monitor_server.app.test_client()

    def _status(self, black_seconds: float) -> dict:
        preview = monitor_server.live_preview
        with mock.patch.object(preview, "touch"), \
             mock.patch.object(preview, "status", return_value={"running": True, "resource": "rtmp://up/live/1"}), \
             mock.patch.object(preview, "source_black_seconds", return_value=black_seconds) as black:
            data = self.app.get("/api/monitor/preview/status?match_id=54494599").get_json()
        black.assert_called_once_with("54494599")
        return data

    def test_black_source_is_reported(self):
        data = self._status(37.4)
        self.assertTrue(data["source_black"])
        self.assertEqual(data["source_black_seconds"], 37)
        # The usual fields are still there.
        self.assertTrue(data["running"])
        self.assertEqual(data["match_id"], "54494599")

    def test_source_with_picture_is_not_reported(self):
        data = self._status(0.0)
        self.assertFalse(data["source_black"])
        self.assertEqual(data["source_black_seconds"], 0)

    def test_no_preview_reports_no_black(self):
        # Real manager, no entry for this match.
        with mock.patch.object(monitor_server.live_preview, "touch"):
            data = self.app.get("/api/monitor/preview/status?match_id=54494599").get_json()
        self.assertFalse(data["running"])
        self.assertFalse(data["source_black"])


class StageSourceBlackTextTests(unittest.TestCase):
    """``sourceBlackText`` from monitor_static/app.js, run under node."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        root = Path(monitor_server.__file__).parent / "monitor_static"
        source = (root / "app.js").read_text(encoding="utf-8")
        func = re.search(r"^function sourceBlackText\([^)]*\) \{.*?^\}", source, re.M | re.S)
        if func is None:
            raise AssertionError("sourceBlackText missing from app.js")
        cls.prelude = func.group(0)
        # The banner the text goes into must exist in the page, hidden at first.
        html = (root / "index.html").read_text(encoding="utf-8")
        cls.banner = re.search(r'<div id="stage-source-black" class="([^"]*)"', html)

    def run_js(self, expr: str):
        script = f"{self.prelude}\nprocess.stdout.write(JSON.stringify({expr}));"
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_no_black_gives_no_text(self):
        for value in ("0", "null", "undefined", "-3"):
            self.assertEqual(self.run_js(f"sourceBlackText({value})"), "", value)

    def test_text_names_the_pusher_and_says_reloading_does_not_help(self):
        text = self.run_js("sourceBlackText(12)")
        self.assertIn("12 秒", text)
        self.assertIn("推流方", text)
        self.assertIn("刷新没用", text)

    def test_long_black_is_shown_in_minutes(self):
        self.assertIn("1 分钟", self.run_js("sourceBlackText(75)"))
        self.assertIn("4 分钟", self.run_js("sourceBlackText(262)"))

    def test_without_another_source_it_says_wait_for_the_pusher(self):
        for switch_to in ("", "undefined", "null"):
            text = self.run_js(f"sourceBlackText(12, {switch_to or repr('')})")
            self.assertIn("等推流方恢复", text, switch_to)
            self.assertNotIn("换一路", text, switch_to)

    def test_with_another_source_it_points_at_the_source_button(self):
        # Since 2342dba the operator can switch the source from the stage, so
        # "just wait" is wrong advice when the match has another source.
        text = self.run_js("sourceBlackText(12, 'dqd 高清')")
        self.assertIn("12 秒", text)
        self.assertIn("推流方", text)
        self.assertIn("刷新没用", text)
        self.assertIn("「源」", text)
        self.assertIn("换一路", text)
        self.assertIn("推荐「dqd 高清」", text)
        self.assertNotIn("等推流方恢复", text)

    def test_banner_exists_and_starts_hidden(self):
        self.assertIsNotNone(self.banner, "stage-source-black missing from index.html")
        self.assertIn("hidden", self.banner.group(1).split())


class StageSourceBlackBannerTests(unittest.TestCase):
    """``renderStageSourceBlack`` + ``sourceAlternatives`` from app.js under node.

    The banner picks its advice from the stage tile's source list: another
    switchable source -> point at 「源」, otherwise -> wait for the pusher.
    """

    FUNCTIONS = ("sourceBlackText", "sourceAlternatives", "renderStageSourceBlack")

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        root = Path(monitor_server.__file__).parent / "monitor_static"
        source = (root / "app.js").read_text(encoding="utf-8")
        parts = []
        for name in cls.FUNCTIONS:
            func = re.search(rf"^function {name}\([^)]*\) \{{.*?^\}}", source, re.M | re.S)
            if func is None:
                raise AssertionError(f"{name} missing from app.js")
            parts.append(func.group(0))
        cls.functions = "\n".join(parts)
        # refreshSources must re-render the banner when the list arrives,
        # otherwise the advice stays stale until the next status poll.
        refresh = re.search(r"^async function refreshSources\([^)]*\) \{.*?^\}", source, re.M | re.S)
        cls.refresh_body = refresh.group(0) if refresh else ""

    def render(self, tile: dict | None, stage: str | None = "54494599") -> dict:
        script = f"""
const banner = {{textContent: 'old', hidden: true,
  classList: {{toggle(name, on) {{ if (name === 'hidden') banner.hidden = on; }}}}}};
const $ = (id) => (id === 'stage-source-black' ? banner : null);
const state = {{stageMatchId: {json.dumps(stage)}, tiles: new Map()}};
const tile = {json.dumps(tile)};
if (tile) state.tiles.set('54494599', tile);
{self.functions}
renderStageSourceBlack();
process.stdout.write(JSON.stringify({{text: banner.textContent, hidden: banner.hidden}}));
"""
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    @staticmethod
    def tile(black: int, sources=None, switchable=True) -> dict:
        tile = {"sourceBlackSeconds": black}
        if sources is not None:
            tile["sources"] = {"switchable": switchable, "sources": sources}
        return tile

    TWO = [
        {"source_id": "ls-1", "label": "ls 英文高清", "current": True},
        {"source_id": "dqd-1", "label": "dqd 高清", "current": False},
        {"source_id": "pt-1", "label": "pt 线路", "current": False},
    ]

    def test_other_source_recommends_the_first_alternative(self):
        out = self.render(self.tile(40, self.TWO))
        self.assertFalse(out["hidden"])
        self.assertIn("推荐「dqd 高清」", out["text"])
        self.assertNotIn("等推流方恢复", out["text"])

    def test_only_the_current_source_says_wait(self):
        out = self.render(self.tile(40, self.TWO[:1]))
        self.assertFalse(out["hidden"])
        self.assertIn("等推流方恢复", out["text"])

    def test_not_switchable_says_wait(self):
        # WebRTC previews report switchable=false: the menu cannot switch them.
        out = self.render(self.tile(40, self.TWO, switchable=False))
        self.assertIn("等推流方恢复", out["text"])
        self.assertNotIn("换一路", out["text"])

    def test_source_list_not_loaded_yet_says_wait(self):
        out = self.render(self.tile(40))
        self.assertIn("等推流方恢复", out["text"])

    def test_picture_back_hides_the_banner(self):
        out = self.render(self.tile(0, self.TWO))
        self.assertTrue(out["hidden"])
        self.assertEqual(out["text"], "")

    def test_closed_stage_hides_the_banner(self):
        out = self.render(self.tile(40, self.TWO), stage=None)
        self.assertTrue(out["hidden"])

    def test_refreshed_source_list_re_renders_the_banner(self):
        self.assertIn("renderStageSourceBlack()", self.refresh_body)


if __name__ == "__main__":
    unittest.main()
