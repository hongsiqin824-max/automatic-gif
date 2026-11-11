"""Where the time before 发布 goes (11-11).

Baseline from the server, 10-29..11-09: a GIF is ready a median 38 s before
发布 and every one of 3044 titles was typed by hand, but nothing showed how
those seconds are spent. The workshop now records, per clip, when the row was
first shown, the first edit, the span the title was typed over and which
fields changed, and sends it with 发布; the server only logs it
(``manual_draft_enqueue`` → ``timing``; the endpoint test is in
test_monitor_server.ClipPublishEndpointTests).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import unittest
from pathlib import Path

# Same import guards as test_monitor_server.py.
os.environ.setdefault("GIF_SOURCE_SECRET", "test-secret")
os.environ.setdefault("GIF_MONITOR_PUBLISH_ENABLED", "false")
os.environ.setdefault("GIF_MONITOR_ALERT_DB_PATH", "")
os.environ["GIF_MONITOR_FEISHU_APP_ID"] = ""
os.environ["GIF_MONITOR_FEISHU_APP_SECRET"] = ""

import monitor_server

APP_JS = Path(monitor_server.__file__).parent / "monitor_static" / "app.js"


class FormTimingJsTests(unittest.TestCase):
    """``formTimingFor`` / ``noteFormEdit`` / ``formTimingPayload`` under node."""

    @classmethod
    def setUpClass(cls):
        cls.node = shutil.which("node")
        if not cls.node:
            raise unittest.SkipTest("node 不可用")
        source = APP_JS.read_text(encoding="utf-8")
        block = re.search(
            r"^// ---- publish form timing ----\n.*?(?=^function buildClipForm\()", source, re.M | re.S
        )
        if block is None:
            raise AssertionError("publish form timing block missing from app.js")
        cls.prelude = block.group(0)

    def run_js(self, body: str):
        script = f"{self.prelude}\nconst result = (() => {{ {body} }})();\nprocess.stdout.write(JSON.stringify(result));"
        result = subprocess.run([self.node, "-e", script], capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_payload_breaks_the_wait_down(self):
        payload = self.run_js(
            """
            formTimingFor('a.gif', 1000);
            noteFormEdit('a.gif', 'match_time', 'input', 5000);
            noteFormEdit('a.gif', 'title', 'input', 6000);
            noteFormEdit('a.gif', 'title', 'input', 7000);
            noteFormEdit('a.gif', 'title', 'input', 9000);
            noteFormEdit('a.gif', 'title', 'change', 9500);
            return formTimingPayload('a.gif', 12000);
            """
        )
        self.assertEqual(payload, {
            "shown_to_publish_ms": 11000,
            "shown_to_first_edit_ms": 4000,
            "title_first_input_ms": 5000,
            "title_last_input_ms": 8000,
            # The blur 'change' is not a keystroke.
            "title_inputs": 3,
            "edited": ["match_time", "title"],
        })

    def test_a_rebuilt_form_keeps_the_first_shown_time(self):
        # The stage list rebuilds its forms (new pushes, player info) while
        # the operator types: the clock must not restart.
        payload = self.run_js(
            """
            formTimingFor('a.gif', 1000);
            formTimingFor('a.gif', 20000);
            return formTimingPayload('a.gif', 30000);
            """
        )
        self.assertEqual(payload["shown_to_publish_ms"], 29000)
        self.assertIsNone(payload["shown_to_first_edit_ms"])
        self.assertIsNone(payload["title_first_input_ms"])
        self.assertEqual(payload["title_inputs"], 0)
        self.assertEqual(payload["edited"], [])

    def test_unknown_clip_sends_null_and_the_map_stays_bounded(self):
        result = self.run_js(
            """
            for (let i = 0; i < FORM_TIMING_MAX + 50; i += 1) formTimingFor(`c${i}.gif`, i);
            return {unknown: formTimingPayload('never.gif', 5), size: formTimings.size,
                    oldestGone: !formTimings.has('c0.gif'), newestKept: formTimings.has(`c${FORM_TIMING_MAX + 49}.gif`)};
            """
        )
        self.assertIsNone(result["unknown"])
        self.assertEqual(result["size"], 200)
        self.assertTrue(result["oldestGone"])
        self.assertTrue(result["newestKept"])

    def test_the_form_and_publish_are_wired_to_it(self):
        source = APP_JS.read_text(encoding="utf-8")
        form = re.search(r"^function buildClipForm\(item, row\) \{.*?^\}", source, re.M | re.S).group(0)
        self.assertIn("formTimingFor(item.name);", form)
        self.assertIn("noteFormEdit(item.name, key, ev.type)", form)
        self.assertIn("noteFormEdit(item.name, 'alert_seq', 'change')", form)
        publish = re.search(r"^async function publishClip\(.*?^\}", source, re.M | re.S).group(0)
        self.assertIn("timing: formTimingPayload(name)", publish)
        self.assertIn("formTimings.delete(name)", publish)


class PublishTimingCleanTests(unittest.TestCase):
    """``monitor_server._publish_timing``: only known keys, sane numbers."""

    def test_absent_or_not_an_object(self):
        for raw in (None, "x", 3, [1, 2]):
            self.assertIsNone(monitor_server._publish_timing(raw), raw)

    def test_numbers_are_clamped_and_junk_dropped(self):
        timing = monitor_server._publish_timing({
            "shown_to_publish_ms": 10 ** 12,
            "shown_to_first_edit_ms": -5,
            "title_first_input_ms": True,
            "title_last_input_ms": float("nan"),
            "title_inputs": "12",
            "edited": ["title", "title", "<script>", 7],
            "extra": "dropped",
        })
        self.assertEqual(timing, {
            "shown_to_publish_ms": 86_400_000,
            "shown_to_first_edit_ms": 0,
            "title_first_input_ms": None,
            "title_last_input_ms": None,
            "title_inputs": None,
            "edited": ["title"],
        })

    def test_edited_must_be_a_list(self):
        self.assertEqual(monitor_server._publish_timing({"edited": "title"})["edited"], [])


if __name__ == "__main__":
    unittest.main()
