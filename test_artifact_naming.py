import unittest

from artifact_naming import EVENT_TYPE_BY_CODE, MAX_FILENAME_BYTES, build_gif_filename


class ArtifactNamingTests(unittest.TestCase):
    def event_data(self, **updates):
        event = {
            "event_key": "match-42:G:abcdef123456",
            "code": "G",
            "minute": "90",
            "minute_extra": "3",
            "person": "张三",
            "person_id": "17",
            "score": "2-1",
        }
        event.update(updates)
        return event

    def test_event_codes_have_exact_artifact_labels(self):
        for code, label in EVENT_TYPE_BY_CODE.items():
            with self.subTest(code=code):
                filename = build_gif_filename(
                    match_id="match-42",
                    event_data=self.event_data(code=code),
                )
                self.assertEqual(
                    filename,
                    f"match-42_m090+03_{label}_张三_2-1_default_abcdef.gif",
                )

    def test_unicode_is_nfkc_normalized_and_unsafe_runs_become_hyphens(self):
        filename = build_gif_filename(
            match_id="Ｍ／42",
            event_data=self.event_data(
                minute="９０＋３",
                minute_extra="0",
                person=" Ａｌｉ／王 伟 ",
                score="２：１",
                event_key="match:G:ＡＢＣ１２３tail",
            ),
        )
        self.assertEqual(
            filename,
            "M-42_m090+03_goal_Ali-王-伟_2-1_default_ABC123.gif",
        )

    def test_player_id_and_unknown_are_used_when_person_is_missing(self):
        player_id = build_gif_filename(
            match_id="match-42",
            event_data=self.event_data(person="", person_id="50895934", score=""),
        )
        self.assertEqual(
            player_id,
            "match-42_m090+03_goal_player-50895934_default_abcdef.gif",
        )

        unknown = build_gif_filename(
            match_id="match-42",
            event_data=self.event_data(person="", person_id="0", score=""),
        )
        self.assertEqual(
            unknown,
            "match-42_m090+03_goal_unknown_default_abcdef.gif",
        )

    def test_missing_minute_is_not_mislabeled_as_minute_zero(self):
        filename = build_gif_filename(
            match_id="match-42",
            event_data=self.event_data(minute="", minute_extra="0"),
        )
        self.assertEqual(
            filename,
            "match-42_mUNK_goal_张三_2-1_default_abcdef.gif",
        )

    def test_variant_and_event_suffix_prevent_artifact_collisions(self):
        default = build_gif_filename(
            match_id="match-42",
            event_data=self.event_data(),
            variant="default",
        )
        ai = build_gif_filename(
            match_id="match-42",
            event_data=self.event_data(),
            variant="ai",
        )
        adjacent = build_gif_filename(
            match_id="match-42",
            event_data=self.event_data(event_key="match-42:G:654321fedcba"),
            variant="default",
        )

        self.assertEqual(len({default, ai, adjacent}), 3)
        self.assertTrue(default.endswith("_default_abcdef.gif"))
        self.assertTrue(ai.endswith("_ai_abcdef.gif"))
        self.assertTrue(adjacent.endswith("_default_654321.gif"))

    def test_filename_stays_within_utf8_limit_and_preserves_stable_suffix(self):
        filename = build_gif_filename(
            match_id="比赛" * 100,
            event_data=self.event_data(person="超长球员姓名" * 100),
        )
        self.assertLessEqual(len(filename.encode("utf-8")), MAX_FILENAME_BYTES)
        self.assertTrue(filename.endswith("_2-1_default_abcdef.gif"))

    def test_unsupported_event_code_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "unsupported event code"):
            build_gif_filename(
                match_id="match-42",
                event_data=self.event_data(code="SUB"),
            )


if __name__ == "__main__":
    unittest.main()
