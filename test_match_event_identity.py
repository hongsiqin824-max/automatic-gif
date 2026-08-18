import unittest

from match_event_identity import events_represent_same_incident


class MatchEventIdentityTests(unittest.TestCase):
    @staticmethod
    def goal(**changes):
        event = {
            "event_key": "match-1:G:first",
            "code": "G",
            "event_type": "goal",
            "minute": "10",
            "minute_extra": "0",
            "team": "teamA",
            "person": "",
            "person_id": "0",
            "score": "1-0",
            "reason": "",
            "second": None,
            "metadata": {},
        }
        event.update(changes)
        return event

    def test_different_seconds_keep_adjacent_goals_separate(self):
        cases = (
            ("same score", "1-0", "1-0"),
            ("right score missing", "1-0", ""),
            ("both scores missing", "", ""),
        )
        for label, left_score, right_score in cases:
            with self.subTest(label=label):
                left = self.goal(second=601, score=left_score)
                right = self.goal(
                    event_key="match-1:G:second",
                    minute="11",
                    second=659,
                    score=right_score,
                )
                self.assertFalse(events_represent_same_incident(left, right))

    def test_same_minute_and_score_do_not_override_different_seconds(self):
        left = self.goal(second=601)
        right = self.goal(event_key="match-1:G:second", second=620)
        self.assertFalse(events_represent_same_incident(left, right))

    def test_matched_shotmap_metadata_is_used_when_top_level_second_is_absent(self):
        def shotmap_metadata(second):
            return {
                "second_source": "shotmap",
                "shotmap_match_status": "matched",
                "shotmap_candidate_details": [{"second": second}],
            }

        left = self.goal(metadata=shotmap_metadata(601))
        right = self.goal(
            event_key="match-1:G:second",
            minute="11",
            metadata=shotmap_metadata(659),
        )
        self.assertFalse(events_represent_same_incident(left, right))

    def test_same_second_is_stable_revision_evidence(self):
        left = self.goal(second=601, score="", person_id="0")
        right = self.goal(
            event_key="match-1:G:revision",
            minute="11",
            second=601,
            score="1-0",
            person="Scorer",
            person_id="17",
        )
        self.assertTrue(events_represent_same_incident(left, right))

    def test_missing_score_and_player_are_not_revision_evidence(self):
        left = self.goal(score="1-0")
        right = self.goal(
            event_key="match-1:G:second",
            minute="11",
            score="",
        )
        self.assertFalse(events_represent_same_incident(left, right))

    def test_stable_event_id_has_priority_over_mutable_fields(self):
        left = self.goal(metadata={"event_id": "goal-17"})
        revision = self.goal(
            event_key="match-1:G:revision",
            minute="11",
            score="2-0",
            person="Scorer",
            person_id="17",
            metadata={"event_id": "goal-17"},
        )
        other = {**revision, "metadata": {"event_id": "goal-18"}}
        self.assertTrue(events_represent_same_incident(left, revision))
        self.assertFalse(events_represent_same_incident(left, other))

    def test_same_score_still_allows_incomplete_api_revision(self):
        incomplete = self.goal(minute="5", score="1-0")
        completed = self.goal(
            event_key="match-1:G:revision",
            minute="4",
            score="1-0",
            person="Miguel Murillo",
            person_id="50895934",
        )
        self.assertTrue(events_represent_same_incident(incomplete, completed))

    def test_stable_player_allows_score_and_minute_correction(self):
        left = self.goal(
            minute="5",
            score="1-0",
            person="Miguel Murillo",
            person_id="50895934",
        )
        corrected = self.goal(
            event_key="match-1:G:revision",
            minute="4",
            score="2-0",
            person="Miguel Murillo corrected",
            person_id="50895934",
        )
        self.assertTrue(events_represent_same_incident(left, corrected))

    def test_penalty_goal_and_goal_share_the_goal_event_family(self):
        penalty_goal = self.goal(
            event_key="match-1:PG:first",
            code="PG",
        )
        revised_goal = self.goal(
            event_key="match-1:G:revision",
            code="G",
        )

        self.assertTrue(events_represent_same_incident(penalty_goal, revised_goal))

    def test_penalty_miss_does_not_share_the_goal_event_family(self):
        penalty_goal = self.goal(
            event_key="match-1:PG:first",
            code="PG",
        )
        penalty_miss = self.goal(
            event_key="match-1:PM:first",
            code="PM",
        )

        self.assertFalse(events_represent_same_incident(penalty_goal, penalty_miss))


if __name__ == "__main__":
    unittest.main()
