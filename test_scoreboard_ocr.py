from __future__ import annotations

import ast
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scoreboard_ocr import (
    ClockContinuityStateMachine,
    ParsedMatchClock,
    STRUCTURED_ERROR_KINDS,
    ScoreboardOcrError,
    ScoreboardOcrRequest,
    ScoreboardProfile,
    _run_persistent_worker,
    locate_scoreboard_event,
    parse_clock_texts,
    parse_score_texts,
    resolve_scoreboard_profile,
    run_scoreboard_ocr,
)
from scoreboard_ocr_worker import (
    WorkerError,
    _extract_text_confidences,
    frame_reading,
    load_ocr_engine,
    locate_from_readings,
    parse_scoreboard_texts,
)


class ScoreboardParsingTests(unittest.TestCase):
    def test_parses_split_clock_and_score_tokens(self):
        parsed = parse_scoreboard_texts(["59", ":", "49", "1", "-", "0"])

        self.assertEqual(parsed.clock_seconds, 59 * 60 + 49)
        self.assertEqual(parsed.score, (1, 0))
        self.assertFalse(parsed.ambiguous_clock)
        self.assertFalse(parsed.ambiguous_score)

    def test_accepts_colon_score_without_treating_it_as_clock(self):
        parsed = parse_scoreboard_texts(["62:13", "1:1"])

        self.assertEqual(parsed.clock_seconds, 62 * 60 + 13)
        self.assertEqual(parsed.score, (1, 1))

    def test_accepts_compact_clock_when_paddle_drops_separator(self):
        parsed = parse_scoreboard_texts(["330", "3502"])

        self.assertEqual(parsed.clock_seconds, 35 * 60 + 2)
        self.assertIsNone(parsed.score)

    def test_marks_conflicting_scores_as_ambiguous(self):
        parsed = parse_scoreboard_texts(["59:10", "0-0", "1-0"])

        self.assertIsNone(parsed.score)
        self.assertTrue(parsed.ambiguous_score)

    def test_extracts_paddle_v2_and_v3_text_shapes(self):
        v2 = [[[[0, 0], [1, 0]], ("59:49", 0.98)]]
        v3 = {"rec_texts": ["1-0"], "rec_scores": [0.91]}
        recognition_only = {"res": {"rec_text": "59:50", "rec_score": 0.93}}

        self.assertEqual(_extract_text_confidences(v2), [("59:49", 0.98)])
        self.assertEqual(_extract_text_confidences(v3), [("1-0", 0.91)])
        self.assertEqual(
            _extract_text_confidences(recognition_only), [("59:50", 0.93)]
        )

    def test_missing_paddleocr_is_a_structured_model_error(self):
        with patch.dict(sys.modules, {"paddleocr": None}):
            with self.assertRaises(WorkerError) as raised:
                load_ocr_engine("en")

        self.assertEqual(raised.exception.kind, "ocr_model_unavailable")


class SplitScoreboardParsingTests(unittest.TestCase):
    def test_clock_parser_supports_continuous_and_stoppage_formats(self):
        cases = [
            (["47:18"], 47 * 60 + 18, "continuous", "second"),
            (["45", "+", "2"], 47 * 60, "stoppage", "minute"),
            (["45+2:17"], 47 * 60 + 17, "stoppage", "second"),
            (["90+5"], 95 * 60, "stoppage", "minute"),
            (["90:00", "+00:28"], 90 * 60 + 28, "added_stopwatch", "second"),
            (["45:00 +02:17"], 47 * 60 + 17, "added_stopwatch", "second"),
        ]
        for texts, expected, clock_format, precision in cases:
            with self.subTest(texts=texts):
                parsed = parse_clock_texts(texts)
                self.assertEqual(parsed.clock_seconds, expected)
                self.assertEqual(parsed.clock_format, clock_format)
                self.assertEqual(parsed.precision, precision)
                self.assertFalse(parsed.ambiguous)

    def test_clock_and_score_are_parsed_independently(self):
        self.assertEqual(parse_clock_texts(["62:13"]).clock_seconds, 62 * 60 + 13)
        self.assertIsNone(parse_clock_texts(["1:1"]).clock_seconds)
        self.assertEqual(parse_score_texts(["1", "-", "0"]).score, (1, 0))
        self.assertIsNone(parse_score_texts(["62:13"]).score)

    def test_conflicting_clock_or_score_is_ambiguous(self):
        clock = parse_clock_texts(["59:10", "59:11"])
        score = parse_score_texts(["0-0", "1-0"])

        self.assertTrue(clock.ambiguous)
        self.assertEqual(clock.candidates, (59 * 60 + 10, 59 * 60 + 11))
        self.assertTrue(score.ambiguous)
        self.assertEqual(score.candidates, ((0, 0), (1, 0)))

    def test_parses_j_league_logo_joined_into_score_text(self):
        parsed = parse_score_texts(["0.10"])
        self.assertEqual(parsed.score, (0, 0))
        parsed = parse_score_texts(["0.11"])
        self.assertEqual(parsed.score, (0, 1))

    def test_parses_score_digits_from_separate_ocr_crops(self):
        self.assertEqual(parse_score_texts(["4", "0"]).score, (4, 0))
        self.assertIsNone(parse_score_texts(["62", "13"]).score)


class ScoreboardProfileTests(unittest.TestCase):
    @staticmethod
    def _profile(**overrides):
        values = {
            "profile_id": "source-a",
            "reference_width": 1920,
            "reference_height": 1080,
            "clock_roi": (40, 30, 180, 82),
            "score_roi": (180, 30, 360, 82),
        }
        values.update(overrides)
        return ScoreboardProfile(**values)

    def test_scales_pixel_rois_from_reference_resolution(self):
        rois = self._profile().scaled_rois(1280, 720)

        self.assertEqual(rois["clock_roi"], (27, 20, 120, 55))
        self.assertEqual(rois["score_roi"], (120, 20, 240, 55))

    def test_rejects_wrong_aspect_ratio_with_structured_reason(self):
        with self.assertRaises(ScoreboardOcrError) as raised:
            self._profile().scaled_rois(1024, 768)

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(raised.exception.diagnostics["profile_id"], "source-a")

    def test_unknown_named_profile_fails_closed(self):
        with self.assertRaises(ScoreboardOcrError) as raised:
            resolve_scoreboard_profile("not-configured")

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")

    def test_mapping_profile_serializes_as_pixel_rois(self):
        profile = {
            "profile_id": "source-a",
            "reference_resolution": [1920, 1080],
            "clock_roi": [40, 30, 180, 82],
            "score_roi": [180, 30, 360, 82],
            "second_half_clock_mode": "auto",
        }
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            payload = ScoreboardOcrRequest(
                candidate,
                "YC",
                event_minute=59,
                scoreboard_profile=profile,
            ).to_payload()

        self.assertEqual(payload["scoreboard_profile"]["clock_roi"], [40, 30, 180, 82])
        self.assertEqual(payload["scoreboard_profile"]["score_roi"], [180, 30, 360, 82])
        self.assertEqual(payload["scoreboard_profile"]["reference_resolution"], [1920, 1080])

    def test_goal_request_serializes_cumulative_second_without_target_score(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            payload = ScoreboardOcrRequest(
                candidate,
                "G",
                event_minute=69,
                event_second=4177,
            ).to_payload()

        self.assertEqual(payload["event_second"], 4177)
        self.assertIsNone(payload["target_score"])

    def test_penalty_goal_request_serializes_cumulative_second(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            payload = ScoreboardOcrRequest(
                candidate,
                "PG",
                event_minute=69,
                event_second=4177,
            ).to_payload()

        self.assertEqual(payload["event_code"], "PG")
        self.assertEqual(payload["event_second"], 4177)
        self.assertIsNone(payload["target_score"])
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            with self.assertRaisesRegex(ValueError, "unsupported event code"):
                ScoreboardOcrRequest(candidate, "PM", event_minute=69).to_payload()


class ClockContinuityTests(unittest.TestCase):
    def test_repairs_b56_and_short_backwards_digit_outliers(self):
        tracker = ClockContinuityStateMachine()
        readings = [
            tracker.update(0.0, "68:59"),
            tracker.update(1.0, "69:00"),
            tracker.update(2.0, "60:07"),
            tracker.update(3.0, "60:02"),
            tracker.update(4.0, "69:03"),
        ]

        self.assertEqual(
            [reading.clock_seconds for reading in readings],
            [68 * 60 + 59, 69 * 60, 69 * 60 + 1, 69 * 60 + 2, 69 * 60 + 3],
        )
        self.assertEqual(readings[2].status, "repaired")
        self.assertEqual(readings[3].status, "repaired")

        tracker.reset()
        tracker.update(0.0, "68:55")
        repaired = tracker.update(1.0, "B:56")
        self.assertEqual(repaired.clock_seconds, 68 * 60 + 56)
        self.assertEqual(repaired.reason, "ocr_character_repaired")

    def test_character_repair_cannot_move_clock_away_from_expected_time(self):
        tracker = ClockContinuityStateMachine()
        tracker.update(0.0, "79:18")

        repaired = tracker.update(1.0, "79:I1")

        self.assertEqual(repaired.clock_seconds, 79 * 60 + 19)
        self.assertEqual(repaired.status, "repaired")
        self.assertEqual(repaired.reason, "continuity_outlier_repaired")

    def test_accepts_pause_and_does_not_invent_clock_while_scoreboard_missing(self):
        tracker = ClockContinuityStateMachine()
        first = tracker.update(0.0, "32:10")
        paused = tracker.update(2.0, "32:10")
        missing = tracker.update(3.0, None, scoreboard_visible=False)
        resumed = tracker.update(5.0, "32:13")

        self.assertEqual(first.status, "accepted")
        self.assertEqual(paused.reason, "clock_paused")
        self.assertIsNone(missing.clock_seconds)
        self.assertEqual(missing.reason, "scoreboard_temporarily_missing")
        self.assertEqual(resumed.clock_seconds, 32 * 60 + 13)

    def test_repairs_short_ocr_miss_when_scoreboard_is_still_visible(self):
        tracker = ClockContinuityStateMachine()
        tracker.update(0.0, "12:20")

        inferred = tracker.update(1.0, None)
        resumed = tracker.update(2.0, "12:22")

        self.assertEqual(inferred.clock_seconds, 12 * 60 + 21)
        self.assertEqual(inferred.status, "repaired")
        self.assertEqual(inferred.reason, "single_frame_clock_missing")
        self.assertEqual(resumed.status, "accepted")

    def test_normalizes_reset_second_half_clock_and_accepts_half_transition(self):
        profile = ScoreboardProfile(
            "reset-clock",
            1920,
            1080,
            (40, 30, 180, 82),
            (180, 30, 360, 82),
            second_half_clock_mode="reset",
        )
        tracker = ClockContinuityStateMachine(profile)
        tracker.update(0.0, "45+2:10", period="first_half")
        second_half = tracker.update(900.0, "00:03", period="second_half")

        self.assertEqual(second_half.clock_seconds, 45 * 60 + 3)
        self.assertEqual(second_half.reason, "second_half_started")

    def test_reset_second_half_maps_local_45_and_stoppage_to_match_clock(self):
        profile = ScoreboardProfile(
            "reset-clock",
            1920,
            1080,
            (40, 30, 180, 82),
            (180, 30, 360, 82),
            second_half_clock_mode="reset",
        )

        at_ninety = ClockContinuityStateMachine(profile).update(
            0.0, "45:00", period="second_half"
        )
        stoppage = ClockContinuityStateMachine(profile).update(
            0.0, "45+2:17", period="second_half"
        )

        self.assertEqual(at_ninety.clock_seconds, 90 * 60)
        self.assertEqual(stoppage.clock_seconds, 92 * 60 + 17)

        tracker = ClockContinuityStateMachine(profile)
        boundary = [
            tracker.update(index, text, period="second_half")
            for index, text in enumerate(["44:59", "45+1", "45+1", "45+2"])
        ]
        self.assertEqual(
            [result.clock_seconds for result in boundary],
            [89 * 60 + 59, 91 * 60, 91 * 60, 92 * 60],
        )
        self.assertEqual(boundary[1].reason, "coarse_stoppage_started")
        self.assertEqual(boundary[3].reason, "coarse_stoppage_advanced")

    def test_minute_only_stoppage_label_advance_does_not_lose_lock(self):
        tracker = ClockContinuityStateMachine()
        sequence = ["45+1", "45+1", "45+2", "45+2", "45+2"]

        results = [tracker.update(index, text, period=1) for index, text in enumerate(sequence)]

        self.assertEqual(
            [result.clock_seconds for result in results],
            [46 * 60, 46 * 60, 47 * 60, 47 * 60, 47 * 60],
        )
        self.assertEqual(results[2].status, "accepted")
        self.assertEqual(results[2].reason, "coarse_stoppage_advanced")

    def test_resynchronizes_after_four_continuous_observations_follow_bad_first_frame(self):
        tracker = ClockContinuityStateMachine()
        tracker.update(0.0, "8:55", period=2)

        candidates = [
            tracker.update(index, text, period=2)
            for index, text in enumerate(
                ["68:56", "68:57", "68:58", "68:59"], start=1
            )
        ]
        following = tracker.update(5.0, "69:00", period=2)

        self.assertEqual(candidates[-1].clock_seconds, 68 * 60 + 59)
        self.assertEqual(candidates[-1].status, "resynchronized")
        self.assertEqual(
            candidates[-1].reason, "continuous_observations_resynchronized"
        )
        self.assertEqual(following.status, "accepted")
        self.assertEqual(following.clock_seconds, 69 * 60)

    def test_stops_repairing_a_long_unreadable_run(self):
        tracker = ClockContinuityStateMachine(maximum_consecutive_repairs=2)
        tracker.update(0.0, "10:00")
        self.assertEqual(tracker.update(1.0, "20:00").status, "repaired")
        self.assertEqual(tracker.update(2.0, "20:01").status, "repaired")
        rejected = tracker.update(3.0, "20:02")
        still_rejected = tracker.update(4.0, "20:03")

        self.assertEqual(rejected.status, "rejected")
        self.assertIsNone(rejected.clock_seconds)
        self.assertEqual(still_rejected.status, "rejected")


class ScoreboardLocationTests(unittest.TestCase):
    @staticmethod
    def _reading(index, seconds, text):
        return frame_reading(index, seconds, [text], [0.95])

    def test_goal_uses_first_stable_target_score_and_leads_by_three_seconds(self):
        readings = [
            self._reading(0, 8.0, "59:08 0-0"),
            self._reading(1, 9.0, "59:09 0-0"),
            self._reading(2, 10.0, "59:10 1-0"),
            self._reading(3, 11.0, "59:11 1-0"),
            self._reading(4, 12.0, "59:12 1-0"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "target_score": "1:0",
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
                "stable_frames": 2,
                "anchor_lead_seconds": 3.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 107.0)
        self.assertEqual(result["method"], "paddleocr_score_transition")
        self.assertEqual(result["target_score"], "1-0")
        self.assertEqual(result["diagnostics"]["previous_score"], "0-0")
        self.assertEqual(result["diagnostics"]["transition_clock"], "59:10")

    def test_clock_only_goal_ignores_score_transition_and_uses_minute_boundary(self):
        readings = [
            self._reading(0, 0.0, "58:58 0-0"),
            self._reading(1, 1.0, "58:59 0-0"),
            self._reading(2, 2.0, "59:00 1-0"),
            self._reading(3, 3.0, "59:01 1-0"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_minute": "59",
                "target_score": "1-0",
                "clock_only": True,
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["method"], "paddleocr_minute_boundary")
        self.assertEqual(result["anchor_seconds"], 102.0)
        self.assertEqual(result["location_kind"], "match_clock_minute_boundary")
        self.assertEqual(result["minute_window_start_clock"], "58:00")
        self.assertEqual(result["minute_window_end_clock"], "59:00")
        self.assertTrue(result["diagnostics"]["clock_only"])
        self.assertNotIn("score_transition_error", result)

    def test_clock_only_rejects_non_boolean_mode(self):
        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                [self._reading(0, 0.0, "59:08")],
                {
                    "event_code": "YC",
                    "event_minute": "59",
                    "clock_only": "true",
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_invalid_request")

    def test_goal_exact_second_uses_observed_clock_as_anchor(self):
        readings = [
            self._reading(0, 8.0, "69:36 0-0"),
            self._reading(1, 9.0, "69:37 0-0"),
            self._reading(2, 10.0, "69:38 0-0"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 4177,
                "target_score": "1-0",
                "candidate_start_seconds": 100.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 109.0)
        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(result["precision"], "observed_second")
        self.assertEqual(result["target_clock"], "69:37")
        self.assertEqual(result["location_kind"], "match_clock_second")

    def test_goal_cumulative_second_152_targets_02_32_without_score(self):
        readings = [
            self._reading(0, 0.0, "02:31"),
            self._reading(1, 1.0, "02:32"),
            self._reading(2, 2.0, "02:33"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 152,
                "candidate_start_seconds": 40.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 41.0)
        self.assertEqual(result["target_clock"], "02:32")

    def test_goal_second_interpolates_between_trustworthy_adjacent_clocks(self):
        readings = [
            self._reading(0, 8.0, "69:36 0-0"),
            self._reading(2, 10.0, "69:38 0-0"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "OG",
                "event_second": 4177,
                "target_score": "1-0",
                "candidate_start_seconds": 100.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 109.0)
        self.assertEqual(result["method"], "paddleocr_interpolated_clock")
        self.assertEqual(result["precision"], "interpolated_second")
        self.assertEqual(
            result["diagnostics"]["interpolation_clock_bounds"],
            ["69:36", "69:38"],
        )

    def test_goal_second_uses_two_readings_for_degraded_one_sided_estimate(self):
        readings = [
            self._reading(0, 0.0, "69:33"),
            self._reading(1, 1.0, "69:34"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 4177,
            },
        )

        self.assertEqual(result["method"], "paddleocr_near_neighbor_estimate")
        self.assertEqual(result["precision"], "estimated_second")
        self.assertEqual(result["localization_quality"], "estimated")
        self.assertTrue(result["degraded"])
        self.assertEqual(result["anchor_seconds"], 4.0)
        self.assertEqual(result["estimated_error_bound_seconds"], 4)
        self.assertEqual(result["estimated_error_bound_label"], "+/-4s")
        self.assertEqual(result["diagnostics"]["estimate_direct_reading_count"], 2)

    def test_goal_second_rejects_multiple_disjoint_clock_occurrences(self):
        readings = [
            self._reading(0, 0.0, "69:37"),
            self._reading(1, 1.0, "69:38"),
            self._reading(100, 100.0, "69:37"),
            self._reading(101, 101.0, "69:38"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "G",
                    "event_second": 4177,
                    "target_score": "1-0",
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")
        self.assertEqual(
            raised.exception.diagnostics["exact_second_failure_reason"],
            "multiple_disjoint_occurrences",
        )
        self.assertEqual(
            raised.exception.diagnostics["matching_occurrence_count"], 2
        )

    def test_goal_second_pause_prefers_earliest_frame_in_continuous_occurrence(self):
        readings = [
            self._reading(0, 0.0, "69:36"),
            self._reading(1, 1.0, "69:37"),
            self._reading(2, 2.0, "69:37"),
            self._reading(3, 3.0, "69:37"),
            self._reading(4, 4.0, "69:38"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 4177,
                "candidate_start_seconds": 100.0,
            },
        )

        self.assertEqual(result["anchor_seconds"], 101.0)
        self.assertEqual(result["method"], "paddleocr_exact_clock")

    def test_goal_second_uses_one_isolated_clock_reading(self):
        readings = [self._reading(0, 7.0, "69:37")]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 4177,
            },
        )

        self.assertEqual(result["anchor_seconds"], 7.0)
        self.assertEqual(result["method"], "paddleocr_exact_clock")
        self.assertEqual(result["precision"], "observed_second")
        self.assertEqual(result["localization_quality"], "exact")
        self.assertEqual(
            result["diagnostics"]["isolated_target_reading_count"], 1
        )
        self.assertEqual(
            result["diagnostics"]["accepted_isolated_target_reading_count"],
            1,
        )

    def test_goal_second_failure_downgrades_to_score_transition(self):
        readings = [
            self._reading(0, 8.0, "59:08 0-0"),
            self._reading(1, 9.0, "59:09 0-0"),
            self._reading(2, 10.0, "59:10 1-0"),
            self._reading(3, 11.0, "59:11 1-0"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 4177,
                "target_score": "1-0",
                "candidate_start_seconds": 100.0,
                "stable_frames": 2,
            },
        )

        self.assertEqual(result["method"], "paddleocr_score_transition")
        self.assertEqual(
            result["exact_second_error"]["kind"],
            "ocr_exact_second_not_found",
        )
        self.assertEqual(
            result["diagnostics"]["exact_second_failure_reason"],
            "target_clock_not_found",
        )

    def test_goal_second_failure_without_score_downgrades_to_minute_interval(self):
        readings = [
            self._reading(0, 0.0, "34:59"),
            self._reading(1, 1.0, "35:00"),
            self._reading(2, 2.0, "35:01"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "event_second": 4177,
                "event_minute": "35",
            },
        )

        self.assertEqual(result["method"], "paddleocr_goal_clock_interval")
        self.assertTrue(result["requires_tdeed"])
        self.assertEqual(
            result["exact_second_error"]["kind"],
            "ocr_exact_second_not_found",
        )

    def test_goal_does_not_claim_transition_when_clip_starts_at_target_score(self):
        readings = [
            self._reading(0, 0.0, "59:20 1-0"),
            self._reading(1, 1.0, "59:21 1-0"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "OG",
                    "target_score": "1-0",
                    "stable_frames": 2,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_no_score_transition")

    def test_goal_uses_clock_interval_when_score_is_unreadable(self):
        readings = [
            self._reading(0, 0.0, "34:59"),
            self._reading(1, 1.0, "3500"),
            self._reading(2, 2.0, "3501"),
            self._reading(3, 3.0, "3502"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "G",
                "target_score": "3-0",
                "event_minute": "35",
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["candidate_interval_start_seconds"], 100.0)
        self.assertEqual(result["candidate_interval_end_seconds"], 104.0)
        self.assertEqual(result["method"], "paddleocr_goal_clock_interval")
        self.assertTrue(result["requires_tdeed"])
        self.assertEqual(
            result["score_transition_error"]["kind"],
            "ocr_score_unreadable",
        )

    def test_goal_rejects_target_old_target_sequence_as_ambiguous(self):
        readings = [
            self._reading(0, 0.0, "59:20 1-0"),
            self._reading(1, 1.0, "59:21 0-0"),
            self._reading(2, 2.0, "59:22 1-0"),
            self._reading(3, 3.0, "59:23 1-0"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "G",
                    "target_score": "1-0",
                    "stable_frames": 2,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")

    def test_card_returns_only_ocr_minute_interval_for_tdeed(self):
        readings = [
            self._reading(0, 0.0, "59:02 0-0"),
            self._reading(1, 10.0, "59:12 0-0"),
            self._reading(2, 20.0, "60:02 0-0"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": "59",
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 10.0,
            },
        )

        self.assertIsNone(result["anchor_seconds"])
        self.assertEqual(result["candidate_interval_start_seconds"], 100.0)
        self.assertEqual(result["candidate_interval_end_seconds"], 120.0)
        self.assertEqual(result["method"], "paddleocr_clock_interval")
        self.assertEqual(result["precision"], "interval_only")
        self.assertTrue(result["requires_tdeed"])

    def test_card_interval_tolerates_one_ambiguous_clock_sample(self):
        readings = [
            self._reading(0, 0.0, "90:20"),
            self._reading(1, 1.0, "10:00+00:15"),
            self._reading(2, 2.0, "90:22"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": "90",
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["candidate_interval_start_seconds"], 0.0)
        self.assertEqual(result["candidate_interval_end_seconds"], 3.0)

    def test_card_bridges_long_gap_when_clock_projection_is_consistent(self):
        readings = [
            self._reading(35, 35.0, "38:00"),
            self._reading(38, 38.0, "38:03"),
            self._reading(94, 94.0, "39:00"),
            self._reading(98, 98.0, "39:04"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 39,
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["candidate_interval_start_seconds"], 35.0)
        self.assertEqual(result["candidate_interval_end_seconds"], 99.0)
        self.assertEqual(
            result["diagnostics"]["bridged_matching_clock_gaps"],
            [
                {
                    "left_frame_index": 38,
                    "right_frame_index": 94,
                    "left_frame_seconds": 38.0,
                    "right_frame_seconds": 94.0,
                    "left_clock": "38:03",
                    "right_clock": "39:00",
                    "boundary_gap_seconds": 56.0,
                    "anchor_video_gap_seconds": 56.0,
                    "clock_advance_seconds": 57,
                    "projection_error_seconds": 1.0,
                }
            ],
        )

    def test_card_does_not_bridge_projection_inconsistent_long_gap(self):
        readings = [
            self._reading(0, 0.0, "38:00"),
            self._reading(1, 1.0, "38:01"),
            self._reading(60, 60.0, "38:10"),
            self._reading(61, 61.0, "38:11"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "YC",
                    "event_minute": 39,
                    "sample_interval_seconds": 1.0,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")
        self.assertEqual(
            raised.exception.diagnostics["bridged_matching_clock_gaps"], []
        )

    def test_card_does_not_bridge_across_readable_non_target_minute(self):
        readings = [
            self._reading(0, 0.0, "38:00"),
            self._reading(1, 1.0, "38:01"),
            self._reading(30, 30.0, "40:00"),
            self._reading(60, 60.0, "39:00"),
            self._reading(61, 61.0, "39:01"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "RC",
                    "event_minute": 39,
                    "sample_interval_seconds": 1.0,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")
        self.assertEqual(
            raised.exception.diagnostics["bridged_matching_clock_gaps"], []
        )

    def test_card_does_not_bridge_beyond_hard_gap_limit(self):
        readings = [
            self._reading(0, 0.0, "38:00"),
            self._reading(1, 1.0, "38:01"),
            self._reading(82, 82.0, "39:22"),
            self._reading(83, 83.0, "39:23"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "YC",
                    "event_minute": 39,
                    "sample_interval_seconds": 1.0,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")
        self.assertEqual(
            raised.exception.diagnostics["bridged_matching_clock_gaps"], []
        )

    def test_card_rejects_repeated_disjoint_minute_intervals(self):
        readings = [
            self._reading(0, 0.0, "59:02 0-0"),
            self._reading(1, 1.0, "60:02 0-0"),
            self._reading(10, 10.0, "59:12 0-0"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "RC",
                    "event_minute": 59,
                    "sample_interval_seconds": 1.0,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")

    def test_card_rejects_backwards_ocr_clock(self):
        readings = [
            self._reading(0, 0.0, "59:20 0-0"),
            self._reading(1, 1.0, "59:10 0-0"),
        ]

        with self.assertRaises(WorkerError) as raised:
            locate_from_readings(
                readings,
                {
                    "event_code": "YC",
                    "event_minute": 59,
                    "sample_interval_seconds": 1.0,
                },
            )

        self.assertEqual(raised.exception.kind, "ocr_ambiguous")

    def test_clock_interval_ignores_outlier_outside_target_minute(self):
        readings = [
            self._reading(0, 0.0, "34:52"),
            self._reading(1, 1.0, "34:59"),
            self._reading(2, 2.0, "3409"),
            self._reading(3, 3.0, "3500"),
            self._reading(4, 4.0, "3501"),
            self._reading(5, 5.0, "3502"),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 35,
                "candidate_start_seconds": 100.0,
                "sample_interval_seconds": 1.0,
            },
        )

        self.assertEqual(result["candidate_interval_start_seconds"], 100.0)
        self.assertEqual(result["candidate_interval_end_seconds"], 106.0)

    def test_structured_readability_errors(self):
        cases = [
            (
                [],
                {"event_code": "G", "target_score": "1-0"},
                "scoreboard_missing",
            ),
            (
                [self._reading(0, 0.0, "TEAM A TEAM B")],
                {"event_code": "YC", "event_minute": 59},
                "ocr_clock_unreadable",
            ),
            (
                [self._reading(0, 0.0, "59:10")],
                {"event_code": "G", "target_score": "1-0"},
                "ocr_score_unreadable",
            ),
            (
                [self._reading(0, 0.0, "59:10 0-0 1-0")],
                {"event_code": "G", "target_score": "1-0"},
                "ocr_ambiguous",
            ),
        ]
        for readings, request, expected_kind in cases:
            with self.subTest(expected_kind=expected_kind):
                with self.assertRaises(WorkerError) as raised:
                    locate_from_readings(readings, request)
                self.assertEqual(raised.exception.kind, expected_kind)


class ScoreboardClientTests(unittest.TestCase):
    def test_clock_only_contract_requires_minute_and_omits_legacy_false_flag(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")

            legacy = ScoreboardOcrRequest(
                candidate, "G", target_score="1-0"
            ).to_payload()
            clock_only = ScoreboardOcrRequest(
                candidate,
                "G",
                event_minute="35",
                clock_only=True,
            ).to_payload()

            self.assertNotIn("clock_only", legacy)
            self.assertTrue(clock_only["clock_only"])
            self.assertIsNone(clock_only["target_score"])
            with self.assertRaisesRegex(ValueError, "event_minute"):
                ScoreboardOcrRequest(
                    candidate,
                    "G",
                    clock_only=True,
                ).to_payload()

    def test_clock_only_uses_coarse_scan_then_authoritative_local_fine_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            coarse = {
                "anchor_seconds": 150.0,
                "method": "paddleocr_minute_boundary",
                "precision": "minute_boundary",
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {"sampled_frame_count": 12},
            }
            fine = {
                "anchor_seconds": 149.0,
                "method": "paddleocr_minute_boundary",
                "precision": "minute_boundary",
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {"sampled_frame_count": 30},
            }
            with (
                patch(
                    "scoreboard_ocr.run_scoreboard_ocr",
                    side_effect=[coarse, fine],
                ) as run,
                patch("scoreboard_ocr._materialize_fine_scan_clip") as materialize,
            ):
                result = locate_scoreboard_event(
                    candidate,
                    event_code="YC",
                    event_minute=30,
                    candidate_start_seconds=100.0,
                    clock_only=True,
                )

        requests = [call.args[0] for call in run.call_args_list]
        self.assertEqual(
            [request.sample_interval_seconds for request in requests],
            [10.0, 1.0],
        )
        self.assertEqual(requests[1].candidate_start_seconds, 135.0)
        self.assertEqual(result["anchor_seconds"], 149.0)
        strategy = result["diagnostics"]["sampling_strategy"]
        self.assertEqual(strategy["mode"], "coarse_then_local_fine")
        self.assertEqual(strategy["final_anchor_source"], "fine_scan")
        self.assertEqual(strategy["coarse_scan"]["anchor_seconds"], 150.0)
        self.assertEqual(materialize.call_args.kwargs["start_seconds"], 35.0)

    def test_goal_second_projects_coarse_minute_boundary_before_fine_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            coarse = {
                "anchor_seconds": 150.0,
                "method": "paddleocr_minute_boundary",
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {},
            }
            fine = {
                "anchor_seconds": 125.0,
                "method": "paddleocr_exact_clock",
                "location_kind": "match_clock_second",
                "diagnostics": {},
            }
            with (
                patch(
                    "scoreboard_ocr.run_scoreboard_ocr",
                    side_effect=[coarse, fine],
                ) as run,
                patch("scoreboard_ocr._materialize_fine_scan_clip") as materialize,
            ):
                result = locate_scoreboard_event(
                    candidate,
                    event_code="G",
                    event_minute=8,
                    event_second=455,
                    candidate_start_seconds=100.0,
                    clock_only=True,
                )

        fine_request = run.call_args_list[1].args[0]
        self.assertEqual(fine_request.candidate_start_seconds, 110.0)
        self.assertEqual(materialize.call_args.kwargs["start_seconds"], 10.0)
        self.assertEqual(result["anchor_seconds"], 125.0)

    def test_ffconcat_local_fine_scan_reuses_manifest_without_mp4(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.ffconcat"
            candidate.write_text("ffconcat version 1.0\n", encoding="utf-8")
            coarse = {
                "anchor_seconds": 150.0,
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {},
            }
            fine = {
                "anchor_seconds": 149.0,
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {},
            }
            with (
                patch("scoreboard_ocr.run_scoreboard_ocr", side_effect=[coarse, fine]) as run,
                patch(
                    "scoreboard_ocr._materialize_fine_scan_clip",
                    side_effect=AssertionError("direct ffconcat fine scan must not make MP4"),
                ),
            ):
                result = locate_scoreboard_event(
                    candidate, event_code="YC", event_minute=30,
                    candidate_start_seconds=100.0, clock_only=True,
                    candidate_input_format="ffconcat",
                    candidate_seek_seconds=5.0,
                    candidate_duration_seconds=80.0,
                )

        fine_request = run.call_args_list[1].args[0]
        self.assertEqual(fine_request.candidate_path, candidate)
        self.assertEqual(fine_request.candidate_start_seconds, 135.0)
        self.assertEqual(fine_request.candidate_seek_seconds, 40.0)
        self.assertEqual(fine_request.candidate_duration_seconds, 30.0)
        self.assertEqual(result["anchor_seconds"], 149.0)

    def test_coarse_miss_falls_back_to_full_one_second_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            coarse_error = ScoreboardOcrError(
                "ocr_clock_unreadable", "coarse clock was unreadable"
            )
            fine = {
                "anchor_seconds": 150.0,
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {},
            }
            with patch(
                "scoreboard_ocr.run_scoreboard_ocr",
                side_effect=[coarse_error, fine],
            ) as run:
                result = locate_scoreboard_event(
                    candidate,
                    event_code="YC",
                    event_minute=30,
                    clock_only=True,
                )

        requests = [call.args[0] for call in run.call_args_list]
        self.assertEqual(
            [request.sample_interval_seconds for request in requests],
            [10.0, 1.0],
        )
        self.assertEqual(requests[1].candidate_path, candidate)
        strategy = result["diagnostics"]["sampling_strategy"]
        self.assertEqual(strategy["mode"], "full_fine_fallback")
        self.assertEqual(strategy["coarse_error"]["kind"], "ocr_clock_unreadable")

    def test_local_fine_miss_falls_back_without_using_coarse_anchor(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            coarse = {
                "anchor_seconds": 150.0,
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {},
            }
            local_error = ScoreboardOcrError(
                "ocr_minute_boundary_not_found", "local fine scan missed"
            )
            full_fine = {
                "anchor_seconds": 152.0,
                "location_kind": "match_clock_minute_boundary",
                "diagnostics": {},
            }
            with (
                patch(
                    "scoreboard_ocr.run_scoreboard_ocr",
                    side_effect=[coarse, local_error, full_fine],
                ) as run,
                patch("scoreboard_ocr._materialize_fine_scan_clip"),
            ):
                result = locate_scoreboard_event(
                    candidate,
                    event_code="RC",
                    event_minute=30,
                    candidate_start_seconds=100.0,
                    clock_only=True,
                )

        self.assertEqual(result["anchor_seconds"], 152.0)
        self.assertEqual(run.call_args_list[2].args[0].candidate_path, candidate)
        strategy = result["diagnostics"]["sampling_strategy"]
        self.assertEqual(strategy["final_anchor_source"], "fine_scan")
        self.assertEqual(
            strategy["local_fine_error"]["kind"],
            "ocr_minute_boundary_not_found",
        )

    def test_client_module_has_no_model_framework_imports(self):
        source = Path(__file__).with_name("scoreboard_ocr.py").read_text(
            encoding="utf-8"
        )
        imported_roots = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.Import):
                imported_roots.update(
                    alias.name.split(".", 1)[0] for alias in node.names
                )
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported_roots.add(node.module.split(".", 1)[0])

        self.assertNotIn("paddle", imported_roots)
        self.assertNotIn("paddleocr", imported_roots)
        self.assertNotIn("torch", imported_roots)

    def test_client_runs_worker_in_child_python_and_returns_diagnostics(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            worker_result = {
                "ok": True,
                "result": {
                    "anchor_seconds": 107.0,
                    "method": "paddleocr_score_transition",
                    "diagnostics": {"sampled_frame_count": 120},
                },
            }
            runner = Mock(
                return_value=subprocess.CompletedProcess(
                    ["ocr-python"],
                    0,
                    stdout=json.dumps(worker_result),
                    stderr="",
                )
            )

            result = locate_scoreboard_event(
                candidate,
                event_code="G",
                target_score="1-0",
                event_second=4177,
                candidate_start_seconds=100.0,
                python_executable="ocr-python",
                runner=runner,
            )

        self.assertEqual(result["anchor_seconds"], 107.0)
        command = runner.call_args.args[0]
        self.assertEqual(command[0], "ocr-python")
        self.assertTrue(command[1].endswith("scoreboard_ocr_worker.py"))
        request = json.loads(runner.call_args.kwargs["input"])
        self.assertEqual(request["candidate_start_seconds"], 100.0)
        self.assertEqual(request["target_score"], "1-0")
        self.assertEqual(request["event_second"], 4177)
        self.assertEqual(result["diagnostics"]["worker_python"], "ocr-python")

    def test_client_uses_persistent_worker_protocol_when_enabled(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            response = subprocess.CompletedProcess(
                ["ocr-python"],
                0,
                stdout=json.dumps(
                    {
                        "ok": True,
                        "result": {
                            "anchor_seconds": 12.0,
                            "method": "paddleocr_score_transition",
                            "diagnostics": {},
                        },
                    }
                ),
                stderr="",
            )
            request = ScoreboardOcrRequest(candidate, "G", target_score="1-0")
            with patch(
                "scoreboard_ocr._run_persistent_worker", return_value=response
            ) as persistent_worker:
                result = run_scoreboard_ocr(
                    request,
                    python_executable="ocr-python",
                    persistent=True,
                )

        persistent_worker.assert_called_once()
        self.assertEqual(result["anchor_seconds"], 12.0)
        self.assertEqual(result["diagnostics"]["worker_mode"], "persistent")

    def test_persistent_client_retries_once_after_daemon_disconnect(self):
        class FakeStream:
            def __init__(self, response: bytes) -> None:
                self.response = response
                self.writes: list[bytes] = []

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def write(self, value: bytes) -> None:
                self.writes.append(value)

            def flush(self) -> None:
                pass

            def readline(self, _limit: int) -> bytes:
                return self.response

        class FakeConnection:
            def __init__(self, response: bytes) -> None:
                self.stream = FakeStream(response)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def close(self) -> None:
                pass

            def makefile(self, _mode: str) -> FakeStream:
                return self.stream

        response = json.dumps(
            {"ok": True, "result": {"anchor_seconds": 12.0}}
        ).encode("utf-8") + b"\n"
        disconnected = FakeConnection(b"")
        recovered = FakeConnection(response)
        with tempfile.TemporaryDirectory() as directory:
            worker = Path(directory) / "worker.py"
            worker.write_text("# worker", encoding="utf-8")
            with patch("scoreboard_ocr._ensure_persistent_worker") as ensure, patch(
                "scoreboard_ocr._connect_worker",
                side_effect=[
                    disconnected,
                    OSError("daemon exited"),
                    recovered,
                ],
            ):
                completed = _run_persistent_worker(
                    {"candidate_path": "candidate.mp4"},
                    worker=worker,
                    python="ocr-python",
                    timeout_seconds=2.0,
                )

        self.assertEqual(ensure.call_count, 2)
        self.assertEqual(json.loads(completed.stdout)["result"]["anchor_seconds"], 12.0)
        request = json.loads(recovered.stream.writes[0])
        self.assertGreater(request["_request_timeout_seconds"], 0)
        self.assertLessEqual(request["_request_timeout_seconds"], 2.0)

    def test_client_accepts_json_after_worker_dependency_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            response = {
                "ok": True,
                "result": {
                    "anchor_seconds": 7.0,
                    "method": "paddleocr_score_transition",
                    "diagnostics": {},
                },
            }
            runner = Mock(
                return_value=subprocess.CompletedProcess(
                    ["python"],
                    0,
                    stdout="dependency banner\n" + json.dumps(response),
                    stderr="",
                )
            )

            result = locate_scoreboard_event(
                candidate,
                event_code="G",
                target_score="1-0",
                runner=runner,
            )

        self.assertEqual(result["anchor_seconds"], 7.0)

    def test_client_maps_subprocess_timeout(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")

            def timeout(*args, **kwargs):
                raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

            request = ScoreboardOcrRequest(candidate, "G", target_score="1-0")
            with self.assertRaises(ScoreboardOcrError) as raised:
                run_scoreboard_ocr(request, timeout_seconds=0.5, runner=timeout)

        self.assertEqual(raised.exception.kind, "inference_timeout")

    def test_client_preserves_worker_structured_error(self):
        with tempfile.TemporaryDirectory() as directory:
            candidate = Path(directory) / "candidate.mp4"
            candidate.write_bytes(b"video")
            failure = {
                "ok": False,
                "error": {
                    "kind": "ocr_model_unavailable",
                    "message": "PaddleOCR is not installed",
                    "diagnostics": {"package": "paddleocr"},
                },
            }
            runner = Mock(
                return_value=subprocess.CompletedProcess(
                    ["python"], 2, stdout=json.dumps(failure), stderr="model log"
                )
            )

            with self.assertRaises(ScoreboardOcrError) as raised:
                locate_scoreboard_event(
                    candidate,
                    event_code="G",
                    target_score="1-0",
                    runner=runner,
                )

        self.assertEqual(raised.exception.kind, "ocr_model_unavailable")
        self.assertEqual(raised.exception.diagnostics["package"], "paddleocr")
        self.assertIn("model log", raised.exception.diagnostics["worker_stderr"])

    def test_required_structured_error_contract_is_exposed(self):
        self.assertTrue(
            {
                "scoreboard_missing",
                "ocr_clock_unreadable",
                "ocr_score_unreadable",
                "ocr_no_score_transition",
                "ocr_ambiguous",
                "inference_timeout",
                "ocr_model_unavailable",
            }.issubset(STRUCTURED_ERROR_KINDS)
        )


if __name__ == "__main__":
    unittest.main()
