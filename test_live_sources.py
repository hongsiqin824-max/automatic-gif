"""Deterministic tests for live_sources: the 8901 preview's source catalog."""

from __future__ import annotations

import io
import json
import subprocess
import unittest
from unittest import mock

import live_sources
from live_sources import (
    PullAddressClient,
    RecordCache,
    SourceCatalog,
    SourceChoices,
    describe,
    kind_of,
    parse_source_order,
)

HD = "rtmp://h/wyc_live/hd-wi6h"
PT = "rtmp://h/wyc_live/pt-a9nz"
HDEN = "rtmp://h/wyc_live/hd-en-6Mx"
SD = "rtmp://h/wyc_live/sd-abc"
AI = "rtmp://h/wyc_live/ai-3a3d"
ZB = "rtmp://h/wyc_live/zb-d9bvb"
EMPTY = {"errno": 0, "message": "无数据", "data": {}}


def _record(url):
    return {"errno": 0, "message": "ok", "data": {"resource": url}}


class Clock:
    def __init__(self, now=1000.0):
        self.now = now

    def __call__(self):
        return self.now


class KindAndOrderTests(unittest.TestCase):
    def test_kind_from_stream_name_prefix(self):
        self.assertEqual(kind_of(HD), "hd")
        self.assertEqual(kind_of(HDEN), "hd-en")
        self.assertEqual(kind_of(PT), "pt")
        self.assertEqual(kind_of(SD), "sd")
        self.assertEqual(kind_of(AI), "ai")
        self.assertEqual(kind_of(ZB), "zb")
        self.assertEqual(kind_of("rtmp://h/wyc_live/xx-1"), "other")
        self.assertEqual(kind_of("rtmp://hd-host/live/abc"), "other")
        self.assertEqual(kind_of(""), "other")

    def test_describe_never_carries_the_url(self):
        info = describe(PT)
        self.assertEqual(info["kind"], "pt")
        self.assertEqual(info["name"], "pt-a9nz")
        self.assertEqual(info["source_id"], live_sources.source_id_of(PT))
        self.assertNotIn("rtmp://", json.dumps(info))

    def test_order_setting(self):
        self.assertEqual(parse_source_order(None), live_sources.DEFAULT_SOURCE_ORDER)
        self.assertEqual(parse_source_order(" hd , hd-en,sd,pt,ai,zb "), live_sources.DEFAULT_SOURCE_ORDER)
        # Listed kinds first; the rest keep their default order; junk ignored.
        self.assertEqual(parse_source_order("pt-, bogus, hd"), ("pt", "hd", "hd-en", "sd", "ai", "zb"))
        self.assertEqual(parse_source_order("bogus"), live_sources.DEFAULT_SOURCE_ORDER)


class PullAddressTests(unittest.TestCase):
    def test_fetch_maps_dqd_id_to_match_id_and_filters_junk(self):
        body = {
            "items": [
                {"dqd_id": "4477923", "pull_urls": [PT, ZB, PT, "", "not a url"]},
                {"dqd_id": "abc", "pull_urls": [SD]},
                {"dqd_id": "4477924", "pull_urls": []},
                "junk",
            ]
        }
        response = mock.MagicMock()
        response.read.return_value = json.dumps(body).encode("utf-8")
        response.__enter__.return_value = response
        with mock.patch.object(live_sources.urllib.request, "urlopen", return_value=response) as urlopen:
            result = live_sources.fetch_pull_addresses("https://x/stream/pull-addresses", "secret-key", 3.0)
        self.assertEqual(result, {"54477923": [PT, ZB]})
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("X-stream-api-key"), "secret-key")
        self.assertEqual(request.get_method(), "GET")

    def test_fetch_rejects_a_body_without_items(self):
        response = mock.MagicMock()
        response.read.return_value = b'{"detail": "no"}'
        response.__enter__.return_value = response
        with mock.patch.object(live_sources.urllib.request, "urlopen", return_value=response):
            with self.assertRaises(ValueError):
                live_sources.fetch_pull_addresses("u", "k", 1.0)

    def test_disabled_without_key(self):
        fetch = mock.Mock()
        client = PullAddressClient(api_key="  ", fetch=fetch)
        self.assertFalse(client.enabled)
        self.assertEqual(client.urls_for("1"), [])
        fetch.assert_not_called()

    def test_one_request_per_ttl_for_all_matches(self):
        clock = Clock()
        fetch = mock.Mock(return_value={"1": [ZB], "2": [PT]})
        client = PullAddressClient(api_key="k", ttl_seconds=45, fetch=fetch, clock=clock)
        self.assertEqual(client.urls_for("1"), [ZB])
        self.assertEqual(client.urls_for("2"), [PT])
        self.assertEqual(client.urls_for("3"), [])
        self.assertEqual(fetch.call_count, 1)
        clock.now += 46
        client.urls_for("1")
        self.assertEqual(fetch.call_count, 2)

    def test_failure_keeps_recent_answer_backs_off_and_expires(self):
        clock = Clock()
        answers = [{"1": [ZB]}, RuntimeError("timeout"), RuntimeError("timeout"), {"1": [PT]}]

        def fetch(url, key, timeout):
            value = answers.pop(0)
            if isinstance(value, Exception):
                raise value
            return value

        client = PullAddressClient(api_key="k", ttl_seconds=45, retry_seconds=30, fetch=fetch, clock=clock)
        with mock.patch("builtins.print") as printed:
            self.assertEqual(client.urls_for("1"), [ZB])
            clock.now += 50          # stale -> refresh fails, previous answer kept
            self.assertEqual(client.urls_for("1"), [ZB])
            clock.now += 10          # inside the back-off: no new request
            self.assertEqual(client.urls_for("1"), [ZB])
            self.assertEqual(len(answers), 2)
            clock.now += 90          # 150 s old (> 3 TTL) and failing again: offer nothing
            self.assertEqual(client.urls_for("1"), [])
            clock.now += 31
            self.assertEqual(client.urls_for("1"), [PT])
        events = [json.loads(c.args[0])["event"] for c in printed.call_args_list]
        # One failure log per outage, one recovery log; never the key.
        self.assertEqual(events, ["stream_pull_failed", "stream_pull_recovered"])
        self.assertNotIn('"k"', "".join(c.args[0] for c in printed.call_args_list))


class RecordCacheAndChoiceTests(unittest.TestCase):
    def test_only_found_records_are_cached(self):
        clock = Clock()
        replies = {"dqd": _record(PT), "ls": {"errno": 1, "message": "查询失败", "data": {}}}
        query = mock.Mock(side_effect=lambda match_id, live_type=None: replies[live_type])
        cache = RecordCache(query, ttl_seconds=30, clock=clock)
        self.assertEqual(cache.get("1", "dqd").state, "available")
        self.assertEqual(cache.get("1", "dqd").state, "available")
        self.assertEqual(cache.get("1", "ls").state, "error")
        self.assertEqual(cache.get("1", "ls").state, "error")
        self.assertEqual(query.call_count, 3)
        # "No record" is asked again: a stream added after kick-off shows up
        # on the very next retry.
        replies["ls"] = EMPTY
        self.assertEqual(cache.get("1", "ls").state, "no_source")
        replies["ls"] = _record(SD)
        self.assertEqual(cache.get("1", "ls").state, "available")
        self.assertEqual(query.call_count, 5)
        clock.now += 31
        cache.get("1", "dqd")
        self.assertEqual(query.call_count, 6)

    def test_choice_expires(self):
        clock = Clock()
        choices = SourceChoices(ttl_seconds=100, clock=clock)
        self.assertIsNone(choices.get("1"))
        choices.remember("1", SD, "孙鹏")
        self.assertEqual(choices.get("1")["by"], "孙鹏")
        clock.now += 101
        self.assertIsNone(choices.get("1"))


class CatalogTests(unittest.TestCase):
    def _catalog(self, records, pull=(), live=None):
        self.probed = []

        def probe(url, timeout_seconds=0):
            self.probed.append(url)
            return {"ok": live is None or url in live}

        return SourceCatalog(
            records=RecordCache(lambda m, live_type=None: records.get(live_type, EMPTY), ttl_seconds=0),
            pull=PullAddressClient(api_key="k", fetch=lambda u, k, t: {"1": list(pull)}),
            choices=SourceChoices(),
            probe=probe,
        )

    def test_merges_same_url_and_ranks_by_measured_stability(self):
        catalog = self._catalog({"dqd": _record(PT), "ls": _record(SD)}, pull=[ZB, PT, HDEN])
        sources, errors = catalog.collect("1")
        self.assertEqual(errors, [])
        self.assertEqual([s["name"] for s in sources], ["hd-en-6Mx", "sd-abc", "pt-a9nz", "zb-d9bvb"])
        pt = sources[2]
        self.assertEqual(pt["origins"], ["dqd", "pull"])
        self.assertTrue(pt["pushing"])
        self.assertFalse(sources[1]["pushing"])
        self.assertEqual(pt["label"], "懂球帝·网站抓取 pt")

    def test_unknown_kind_goes_last_and_errors_are_reported(self):
        catalog = self._catalog({"dqd": {"errno": 1, "message": "boom", "data": {}}, "ls": _record(AI)}, pull=["rtmp://h/x/yy-1"])
        sources, errors = catalog.collect("1")
        self.assertEqual([s["kind"] for s in sources], ["ai", "other"])
        self.assertEqual(len(errors), 1)
        self.assertIn("dqd", errors[0])

    def test_pick_single_candidate_without_probing(self):
        catalog = self._catalog({"ls": _record(SD)})
        sources, _ = catalog.collect("1")
        picked, probe = catalog.pick_startable(sources)
        self.assertEqual(picked["url"], SD)
        self.assertIsNone(probe)
        self.assertEqual(self.probed, [])

    def test_pick_first_live_in_preference_then_rank_order(self):
        catalog = self._catalog({"dqd": _record(HD), "ls": _record(SD)}, pull=[ZB], live={SD, ZB})
        sources, _ = catalog.collect("1")
        picked, probe = catalog.pick_startable(sources)
        self.assertEqual(picked["url"], SD)
        self.assertTrue(probe["ok"])
        picked, _ = catalog.pick_startable(sources, preferred_url=ZB)
        self.assertEqual(picked["url"], ZB)
        with self.assertRaises(ValueError):
            catalog.pick_startable([])

    def test_pick_nothing_live_keeps_the_first(self):
        catalog = self._catalog({"dqd": _record(HD), "ls": _record(SD)}, live=set())
        sources, _ = catalog.collect("1")
        picked, probe = catalog.pick_startable(sources)
        self.assertEqual(picked["url"], HD)
        self.assertIsNone(probe)


class ProbeTests(unittest.TestCase):
    def _run(self, stdout=b"", stderr=b"", side_effect=None):
        completed = subprocess.CompletedProcess([], 0, stdout=stdout, stderr=stderr)
        with mock.patch.object(live_sources.subprocess, "run", return_value=completed, side_effect=side_effect) as run:
            result = live_sources.probe_source(PT, timeout_seconds=3.0, ffprobe="ffprobe")
        return result, run

    def test_video_stream_found(self):
        out = json.dumps({"streams": [
            {"codec_type": "audio", "codec_name": "aac"},
            {"codec_type": "video", "codec_name": "h264", "width": 1920, "height": 1080},
        ]}).encode()
        result, run = self._run(stdout=out)
        self.assertTrue(result["ok"])
        self.assertEqual((result["codec"], result["width"], result["height"]), ("h264", 1920, 1080))
        command = run.call_args.args[0]
        self.assertEqual(command[command.index("-rw_timeout") + 1], "3000000")

    def test_not_pushing_reports_reason_without_url(self):
        result, _ = self._run(stdout=b"{}", stderr=f"{PT}: Input/output error\n".encode())
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "pt-a9nz: Input/output error")

    def test_unnamed_video_codec_is_unusable(self):
        out = json.dumps({"streams": [{"codec_type": "video"}]}).encode()
        result, _ = self._run(stdout=out)
        self.assertFalse(result["ok"])

    def test_timeout_and_missing_binary(self):
        result, _ = self._run(side_effect=subprocess.TimeoutExpired("ffprobe", 7))
        self.assertEqual(result, {"ok": False, "error": "连接超时", "seconds": result["seconds"]})
        result, _ = self._run(side_effect=FileNotFoundError("ffprobe"))
        self.assertFalse(result["ok"])
        self.assertIn("ffprobe", result["error"])


if __name__ == "__main__":
    unittest.main()
