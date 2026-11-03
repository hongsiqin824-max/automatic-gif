import unittest

import match_players
from match_players import MatchPlayerLookup, parse_overview_events, resolve_player


def _payload():
    """Shaped like overview/match/54627168 (中美洲杯) as fetched on 2026-11-02."""
    return {
        "status": 1,
        "events": {
            "19": {"minute": "19", "teamAEvents": [
                {"code": "G", "person": "阿尼奥尔", "person_id": "50116664", "score": "1-0", "minute_extra": "0"},
            ]},
            "43": {"minute": "43", "teamAEvents": [
                {"code": "G", "person": "克赖切尔-佩雷斯", "person_id": "50748092", "score": "2-0"},
                {"code": "AS", "person": "肯耶尔-米歇尔", "person_id": "50811438", "score": ""},
            ]},
            "74": {"minute": "74", "teamBEvents": [
                {"code": "OG", "person": "A-甘博亚", "person_id": "50450691", "score": "2-1"},
            ]},
            "90+2": {"minute": "90", "teamAEvents": [
                {"code": "Y2C", "person": "萨尔迪瓦", "person_id": "50188698", "score": ""},
                {"code": "YC", "person": "某某", "person_id": "1", "score": ""},
            ]},
        },
    }


class ParseOverviewTests(unittest.TestCase):
    def test_keeps_goals_and_red_cards_only(self):
        events = parse_overview_events(_payload())
        self.assertEqual(
            sorted((e["side"], e["code"], e["minute"], e["score"], e["person"]) for e in events),
            [
                ("A", "G", 19, "1-0", "阿尼奥尔"),
                ("A", "G", 43, "2-0", "克赖切尔-佩雷斯"),
                ("A", "Y2C", 90, "", "萨尔迪瓦"),
                ("B", "OG", 74, "2-1", "A-甘博亚"),
            ],
        )

    def test_bad_payload_is_empty(self):
        self.assertEqual(parse_overview_events({}), [])
        self.assertEqual(parse_overview_events({"events": []}), [])

    def test_stoppage_time_comes_from_the_bucket_key(self):
        times = {e["person"]: e["match_time"] for e in parse_overview_events(_payload())}
        self.assertEqual(times["萨尔迪瓦"], "90+2")
        self.assertEqual(times["阿尼奥尔"], "")
        # Odd spacing / a trailing ' still normalise; a key without "+" never invents one.
        payload = {"events": {
            "45 + 1'": {"minute": "45", "teamAEvents": [{"code": "G", "person": "甲", "score": "1-0"}]},
            "x": {"minute": "90+4", "teamBEvents": [{"code": "RC", "person": "乙", "score": ""}]},
            "88": {"minute": "88", "teamBEvents": [{"code": "G", "person": "丙", "score": "1-1"}]},
        }}
        times = {e["person"]: e["match_time"] for e in parse_overview_events(payload)}
        self.assertEqual(times, {"甲": "45+1", "乙": "90+4", "丙": ""})


class StoppageTimeTests(unittest.TestCase):
    """The 54610084 (厄瓜杯半决赛) case of 2026-11: both late goals were pushed as
    "2H 90", the CMS refused the second article for the same match_time."""

    def setUp(self):
        self.events = parse_overview_events({"events": {
            "14": {"minute": "14", "teamAEvents": [{"code": "G", "person": "法哈多", "score": "1-0"}]},
            "90+6": {"minute": "90", "teamBEvents": [{"code": "G", "person": "卢卡斯-罗德里格斯", "score": "1-2"}]},
            "90+13": {"minute": "90", "teamAEvents": [{"code": "G", "person": "隆多尼奥", "score": "2-2"}]},
        }})

    def test_two_capped_pushes_get_their_own_full_time(self):
        first = resolve_player({"event_type": "goal", "team": "B", "score": "1-2", "minute": "2H 90"}, self.events)
        second = resolve_player({"event_type": "goal", "team": "A", "score": "2-2", "minute": "2H 90"}, self.events)
        self.assertEqual((first["person"], first["match_time"]), ("卢卡斯-罗德里格斯", "90+6"))
        self.assertEqual((second["person"], second["match_time"]), ("隆多尼奥", "90+13"))

    def test_regular_time_has_no_match_time(self):
        found = resolve_player({"event_type": "goal", "team": "A", "score": "1-0", "minute": "1H 14"}, self.events)
        self.assertEqual(found["match_time"], "")


class ResolvePlayerTests(unittest.TestCase):
    def setUp(self):
        self.events = parse_overview_events(_payload())

    def test_goal_pairs_by_side_and_score_not_minute(self):
        found = resolve_player({"event_type": "goal", "team": "A", "score": "2-0", "minute": "1H 45"}, self.events)
        self.assertEqual(found["person"], "克赖切尔-佩雷斯")
        self.assertEqual((found["event_cn"], found["match_event"]), ("进球", 1))

    def test_own_goal_pairs_with_the_benefiting_side_push(self):
        # The push said 75' 客队进球 2-1; overview lists it at 74' as OG.
        found = resolve_player({"event_type": "goal", "team": "B", "score": "2-1", "minute": "2H 75"}, self.events)
        self.assertEqual((found["person"], found["event_cn"], found["match_event"]), ("A-甘博亚", "乌龙", 5))

    def test_wrong_side_or_unknown_score_finds_nothing(self):
        self.assertIsNone(resolve_player({"event_type": "goal", "team": "B", "score": "2-0", "minute": "43"}, self.events))
        self.assertIsNone(resolve_player({"event_type": "goal", "team": "A", "score": "3-0", "minute": "80"}, self.events))
        self.assertIsNone(resolve_player({"event_type": "goal", "team": "?", "score": "1-0", "minute": "19"}, self.events))

    def test_goal_without_score_falls_back_to_minute(self):
        found = resolve_player({"event_type": "goal", "team": "A", "score": "", "minute": "20"}, self.events)
        self.assertEqual(found["person"], "阿尼奥尔")

    def test_red_card_within_three_minutes(self):
        found = resolve_player({"event_type": "red_card", "team": "A", "score": "2-1", "minute": "2H 88"}, self.events)
        self.assertEqual((found["person"], found["event_cn"], found["match_event"]), ("萨尔迪瓦", "两黄变红", 4))
        # The 54499922 case: a push at 38' with the only red at 61' stays unresolved.
        self.assertIsNone(resolve_player({"event_type": "red_card", "team": "A", "minute": "1H 38"}, self.events))

    def test_two_reds_equally_close_is_not_guessed(self):
        events = [
            {"side": "A", "code": "RC", "minute": 30, "score": "", "person": "甲", "person_id": "1"},
            {"side": "A", "code": "RC", "minute": 34, "score": "", "person": "乙", "person_id": "2"},
        ]
        self.assertIsNone(resolve_player({"event_type": "red_card", "team": "A", "minute": "32"}, events))
        self.assertEqual(resolve_player({"event_type": "red_card", "team": "A", "minute": "33"}, events)["person"], "乙")


class LookupCacheTests(unittest.TestCase):
    def test_fetches_once_per_window_and_keeps_last_good_on_error(self):
        now = [100.0]
        calls = []

        def fetch(match_id):
            calls.append(match_id)
            if len(calls) == 2:
                raise OSError("timeout")
            return _payload()

        lookup = MatchPlayerLookup(fetch, cache_seconds=10.0, clock=lambda: now[0])
        self.assertEqual(len(lookup.events_for("1")), 4)
        now[0] += 5
        self.assertEqual(len(lookup.events_for("1")), 4)
        self.assertEqual(len(calls), 1)
        now[0] += 6
        self.assertEqual(len(lookup.events_for("1")), 4)  # fetch failed: last good kept
        self.assertEqual(lookup.last_error, "timeout")
        self.assertEqual(len(calls), 2)

    def test_failure_without_history_is_empty(self):
        def fetch(match_id):
            raise OSError("down")

        self.assertEqual(MatchPlayerLookup(fetch).events_for("1"), [])

    def test_default_url_uses_match_id(self):
        self.assertIn("{match_id}", match_players.OVERVIEW_URL)


if __name__ == "__main__":
    unittest.main()
