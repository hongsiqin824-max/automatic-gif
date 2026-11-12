import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import roster_precheck as rp
from roster_precheck import (
    BEIJING,
    Precheck,
    RosterRow,
    compose,
    due_stage,
    find_fixture,
    parse_dadmin_list,
    parse_roster,
    same_team,
)


def bj(text):
    return datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=BEIJING).timestamp()


SHEET = [
    ["11月12日", "赛事", "星期四", "手动战报", "自动战报预编辑", "GIF", "分级", "产出形式"],
    ["19:35", "熊猫杯", "中国U19vs越南U19", "/", "王振", "", "A+", "首发+GIF+战报"],
    ["23:00", "英冠第16轮", "西布朗vs米尔沃尔", "/", "/", "老北京", "B", "简易合集+战报"],
    ["", "", "", "", "", "", "", ""],
    ["11月13日", "赛事", "星期五", None, None, None, None, None],
    ["3:45", "欧国联A级联赛", "意大利vs法国", "/", "a", "b", "A+", "首发+GIF+战报"],
    ["备注", "不是比赛"],
]


class RosterParsingTest(unittest.TestCase):
    def test_rows(self):
        rows = parse_roster(SHEET, 2026, 11)
        self.assertEqual([(r.home, r.away) for r in rows],
                         [("中国U19", "越南U19"), ("西布朗", "米尔沃尔"), ("意大利", "法国")])
        self.assertEqual(rows[0].kickoff, bj("2026-11-12 19:35"))
        self.assertEqual(rows[2].kickoff, bj("2026-11-13 03:45"))
        self.assertEqual(rows[0].staff, "王振")  # F empty -> E
        self.assertEqual(rows[1].staff, "老北京")
        self.assertEqual((rows[0].grade, rows[0].form), ("A+", "首发+GIF+战报"))

    def test_december_sheet_rolls_january_into_next_year(self):
        rows = parse_roster([["1月1日"], ["2:00", "英超", "AvsB"]], 2026, 12)
        self.assertEqual(rows[0].kickoff, bj("2027-01-01 02:00"))

    def test_same_team(self):
        self.assertTrue(same_team("热刺", "托特纳姆热刺"))
        self.assertTrue(same_team("拜仁慕尼黑", "拜仁慕尼黑"))
        self.assertFalse(same_team("曼联", "曼城"))
        self.assertFalse(same_team("", "曼城"))

    def test_find_fixture(self):
        row = RosterRow(bj("2026-11-07 23:00"), "挪超", "克里斯蒂安松", "瓦勒伦加")
        near = {"match_id": "1", "kickoff": bj("2026-11-07 23:00"), "home": "瓦勒伦加", "away": "克里斯蒂安松"}
        moved = {"match_id": "2", "kickoff": bj("2026-11-10 02:00"), "home": "克里斯蒂安松", "away": "瓦勒伦加"}
        other = {"match_id": "3", "kickoff": bj("2026-11-07 23:00"), "home": "莫尔德", "away": "瓦勒伦加"}
        self.assertEqual(find_fixture(row, [other, moved, near]), (near, False))
        self.assertEqual(find_fixture(row, [other, moved]), (moved, True))
        self.assertEqual(find_fixture(row, [other]), (None, False))

    def test_dadmin_list(self):
        page = (
            '<table><tr class="match-row" match_id="4634850" data-x="1">'
            "<td>54634850</td><td>U17女足世界杯 2026 决赛</td><td>2026-11-08 02:30:00</td>"
            "<td>日本女足U17 VS 尼日利亚女足U17</td><td>1 : 2 已结束</td></tr>"
            '<tr class="match-row" match_id="1"><td>x</td></tr></table>'
        )
        [row] = parse_dadmin_list(page)
        self.assertEqual(row["match_id"], "54634850")
        self.assertEqual((row["home"], row["away"]), ("日本女足U17", "尼日利亚女足U17"))
        self.assertEqual(row["kickoff"], bj("2026-11-08 02:30"))
        self.assertEqual(row["competition"], "U17女足世界杯 2026 决赛")

    def test_due_stage(self):
        ko = bj("2026-11-13 03:45")
        self.assertIsNone(due_stage(ko, ko - 3 * 3600, {}))
        self.assertEqual(due_stage(ko, ko - 2 * 3600, {}), "t2h")
        self.assertIsNone(due_stage(ko, ko - 3600, {"t2h": 1}))
        self.assertEqual(due_stage(ko, ko - 10 * 60, {"t2h": 1}), "t30")
        self.assertEqual(due_stage(ko, ko + 6 * 60, {}), "live")
        self.assertIsNone(due_stage(ko, ko + 40 * 60, {}))

    def test_compose(self):
        self.assertIsNone(compose({}))
        self.assertEqual(compose({"_summary": []}, "头"), "头")
        text = compose({"t2h": ["· a"], "t30": ["· b"]})
        self.assertLess(text.index("30 分钟"), text.index("2 小时"))
        self.assertIn("（1 场）", text)


class FakeWorld:
    """Clock, roster, directory, back office and sources for one test."""

    def __init__(self, tmp):
        self.now = bj("2026-11-12 16:00")
        self.rows = [
            RosterRow(bj("2026-11-13 03:45"), "欧国联A级联赛", "意大利", "法国", "王振", "A+", "首发+GIF+战报"),
        ]
        self.catalog = [{"match_id": "54419539", "kickoff": bj("2026-11-13 03:45"), "home": "意大利", "away": "法国"}]
        self.dadmin = []
        self.dadmin_calls = 0
        self.sources = {}
        self.probe_ok = {}
        self.source_error = False
        self.sent = []
        self.state_path = Path(tmp) / "state.json"
        self.extra_path = Path(tmp) / "extra.json"

    def collect(self, match_id):
        if self.source_error:
            raise RuntimeError("接口挂了")
        return self.sources.get(match_id, [])

    def search(self, day, team):
        self.dadmin_calls += 1
        return [d for d in self.dadmin if rp.fmt_bj(d["kickoff"], "%Y-%m-%d") == rp.fmt_bj(day, "%Y-%m-%d")]

    def build(self, dadmin=True):
        return Precheck(
            read_roster=lambda now: list(self.rows),
            fetch_catalog=lambda now: list(self.catalog),
            dadmin_search=self.search if dadmin else None,
            collect_sources=self.collect,
            probe=lambda url: {"ok": self.probe_ok.get(url, False), "error": "Input/output error"},
            send=self.sent.append,
            state_path=self.state_path,
            extra_path=self.extra_path,
            clock=lambda: self.now,
        )


class PrecheckFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.w = FakeWorld(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def at(self, text, check=None):
        self.w.now = bj(text)
        return (check or self.check).run_once()

    def test_no_source_before_kickoff_then_fixed_then_live(self):
        self.check = self.w.build()
        self.assertIsNone(self.at("2026-11-12 16:00"))
        summary = self.at("2026-11-12 20:00")
        self.assertIn("接下来 24 小时排班表 1 场，8901 列表能看到 1 场", summary)
        self.assertIsNone(self.at("2026-11-12 20:05"))  # summary once a day
        text = self.at("2026-11-13 01:46")
        self.assertIn("开球前 2 小时还没有直播源", text)
        self.assertIn("意大利 vs 法国（A+｜首发+GIF+战报｜王振） 比赛ID 54419539", text)
        self.assertIsNone(self.at("2026-11-13 02:30"))  # same stage not repeated
        self.w.sources["54419539"] = [{"url": "rtmp://x/sd-1", "kind": "sd"}]
        text = self.at("2026-11-13 03:20")
        self.assertIn("已补上直播源", text)
        self.assertIn("sd", text)
        self.w.probe_ok["rtmp://x/sd-1"] = True
        self.assertIsNone(self.at("2026-11-13 03:51"))

    def test_registered_but_nothing_delivers(self):
        self.check = self.w.build()
        self.w.sources["54419539"] = [{"url": "rtmp://x/pt-1", "kind": "pt"}, {"url": "rtmp://x/sd-1", "kind": "sd"}]
        self.assertIsNone(self.at("2026-11-13 01:46"))
        self.assertIsNone(self.at("2026-11-13 03:20"))
        text = self.at("2026-11-13 03:51")
        self.assertIn("登记的直播源都没有画面", text)
        self.assertIn("pt Input/output error；sd Input/output error", text)

    def test_one_delivering_source_is_enough(self):
        self.check = self.w.build()
        self.w.sources["54419539"] = [{"url": "rtmp://x/pt-1", "kind": "pt"}, {"url": "rtmp://x/sd-1", "kind": "sd"}]
        self.w.probe_ok["rtmp://x/sd-1"] = True
        self.assertIsNone(self.at("2026-11-13 03:51"))

    def test_directory_missed_match_is_found_in_the_back_office(self):
        self.w.catalog = []
        self.w.dadmin = [{"match_id": "54634850", "kickoff": bj("2026-11-13 03:45"), "home": "意大利",
                          "away": "法国", "competition": "欧国联"}]
        self.check = self.w.build()
        self.assertIsNone(self.at("2026-11-12 16:00"))
        calls = self.w.dadmin_calls
        summary = self.at("2026-11-12 20:00")
        self.assertIn("8901 列表能看到 0 场", summary)
        self.assertIn("8901 列表里看不到（赛事目录漏收，后台有这场）（1 场）", summary)
        self.assertIn("比赛ID 54634850", summary)
        self.assertEqual(self.w.dadmin_calls, calls)  # found once, not searched again
        extra = json.loads(self.w.extra_path.read_text())
        self.assertEqual(extra[0]["match_id"], "54634850")
        self.assertEqual(extra[0]["team_A_name"], "意大利")
        text = self.at("2026-11-13 01:46")  # still checked for sources by its id
        self.assertIn("开球前 2 小时还没有直播源", text)
        self.assertNotIn("看不到", text)  # reported once

    def test_rescheduled_and_missing(self):
        self.w.rows.append(RosterRow(bj("2026-11-13 01:00"), "沙特联", "哈森姆", "利雅得胜利"))
        self.w.catalog = [{"match_id": "54419539", "kickoff": bj("2026-11-14 03:45"), "home": "意大利", "away": "法国"}]
        self.check = self.w.build()
        summary = self.at("2026-11-12 20:00")
        self.assertIn("排班表时间和实际开球时间不一样", summary)
        self.assertIn("11-14 03:45 欧国联A级联赛 意大利 vs 法国", summary)
        self.assertIn("（排班表写 11-13 03:45）", summary)
        self.assertIn("都找不到这场", summary)
        self.assertIn("哈森姆 vs 利雅得胜利", summary)
        # The moved match's stages follow its real kick-off.
        self.assertIsNone(self.at("2026-11-13 01:46"))
        self.assertIn("开球前 2 小时", self.at("2026-11-14 01:46"))

    def test_back_office_unavailable(self):
        self.w.catalog = []
        self.check = self.w.build(dadmin=False)
        summary = self.at("2026-11-12 20:00")
        self.assertIn("懂球帝后台暂时查不了", summary)

    def test_source_lookup_failure_is_retried_not_alerted(self):
        self.check = self.w.build()
        self.w.source_error = True
        self.assertIsNone(self.at("2026-11-13 01:46"))
        self.w.source_error = False
        self.assertIn("开球前 2 小时", self.at("2026-11-13 01:47"))

    def test_a_failing_directory_is_asked_once_per_run(self):
        calls = []

        def broken(now):
            calls.append(now)
            raise RuntimeError("目录接口 502")

        self.w.rows.append(RosterRow(bj("2026-11-13 01:00"), "欧国联", "土耳其", "比利时"))
        self.check = self.w.build()
        self.check.fetch_catalog = broken
        self.assertIsNone(self.at("2026-11-12 16:00"))
        self.assertEqual(len(calls), 1)
        self.at("2026-11-12 16:02")
        self.assertEqual(len(calls), 1)  # still inside the retry pause
        self.at("2026-11-12 16:06")
        self.assertEqual(len(calls), 2)

    def test_state_survives_a_restart(self):
        self.check = self.w.build()
        self.at("2026-11-12 20:00")
        self.at("2026-11-13 01:46")
        self.check = self.w.build()
        self.assertIsNone(self.at("2026-11-13 01:50"))
        self.assertIsNone(self.at("2026-11-12 20:30"))


class MainGuardTest(unittest.TestCase):
    def test_empty_webhook_never_sends(self):
        env = {"GIF_ROSTER_SPREADSHEET_TOKEN": "t", "GIF_MONITOR_FEISHU_APP_ID": "a",
               "GIF_MONITOR_FEISHU_APP_SECRET": "s", "GIF_ROSTER_PRECHECK_WEBHOOK": ""}
        with mock.patch.dict(os.environ, env), mock.patch.object(rp, "send_webhook") as send, \
                mock.patch.object(rp, "_load_dotenv"):
            self.assertEqual(rp.main(["--once"]), 2)
        send.assert_not_called()

    def test_now_needs_dry_run(self):
        env = {"GIF_ROSTER_SPREADSHEET_TOKEN": "t", "GIF_MONITOR_FEISHU_APP_ID": "a",
               "GIF_MONITOR_FEISHU_APP_SECRET": "s"}
        with mock.patch.dict(os.environ, env), mock.patch.object(rp, "_load_dotenv"), \
                self.assertRaises(SystemExit):
            rp.main(["--once", "--now", "2026-11-12 20:00"])


if __name__ == "__main__":
    unittest.main()
