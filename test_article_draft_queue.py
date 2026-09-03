import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from article_draft_queue import (
    ArticleDraftQueue,
    _event_with_team_fallback,
    draft_admin_url,
    has_reliable_person,
    ocr_publication_eligibility,
)
from article_publisher import ArticlePublisher, ArticlePublishError, PublishedGifStore
from open_platform_client import OpenPlatformError


def animated_gif_bytes(color: bytes = b"\xff\xff\xff") -> bytes:
    header = (
        b"GIF89a"
        b"\x01\x00\x01\x00"
        b"\x80\x00\x00"
        b"\x00\x00\x00" + color
    )
    frame = (
        b"\x2c"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00"
        b"\x02\x02\x44\x01\x00"
    )
    return header + frame + frame + b"\x3b"


class SequencePlatformClient:
    def __init__(self, outcomes=None):
        self.outcomes = list(outcomes or [])
        self.calls = []

    def status(self):
        return {"configured": True, "authorized": True}

    def create_article(self, fields):
        self.calls.append(dict(fields))
        if self.outcomes:
            outcome = self.outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        return {"article_id": "6230049", "duplicate": False, "code": 0}


class ArticleDraftQueueTests(unittest.TestCase):
    def make_queue(self, root, platform, **queue_options):
        publisher = ArticlePublisher(
            platform_client=platform,
            gif_store=PublishedGifStore(
                root / "published",
                "https://matchgif.aisportsapp.com",
            ),
            database_path=root / "delivery.sqlite3",
            public_url_checker=lambda _url: None,
        )
        queue = ArticleDraftQueue(
            database_path=root / "delivery.sqlite3",
            publisher=publisher,
            allowed_output_root=root / "output",
            start_worker=False,
            **queue_options,
        )
        return queue

    def write_source(self, root, body=None):
        match_dir = root / "output" / "54478914"
        match_dir.mkdir(parents=True, exist_ok=True)
        source = match_dir / "goal_ocr.gif"
        source.write_bytes(body or animated_gif_bytes())
        return source

    def event(self):
        return {
            "event_key": "54478914:G:goal-19",
            "status": "encoded",
            "code": "G",
            "minute": "19",
            "person": "球员甲",
            "score": "1-0",
        }

    def publication_result(self, **overrides):
        """Build a complete, trusted 60-second OCR result for gate tests."""
        result = {
            "output_kind": "ocr_window",
            "localization_source": "exact_second",
            "localization_quality": "exact",
            "ocr_verified": True,
            "event_frame_may_be_missing": False,
            "anchor_adjusted": False,
            "stitched_across_gap": False,
            "video_gap_count": 0,
            "anchor_stream_time": 130.0,
            "clip_before_seconds": 30.0,
            "clip_after_seconds": 30.0,
            "requested_media_window": {
                "start_stream_time": 100.0,
                "end_stream_time": 160.0,
            },
            "actual_media_window": {
                "start_stream_time": 100.0,
                "end_stream_time": 160.0,
            },
            "available_media_duration_seconds": 60.0,
            "duration_sec": 60.0,
        }
        result.update(overrides)
        return result

    def enqueue(self, queue, **kwargs):
        """Use a trusted OCR artifact unless a test explicitly overrides it."""
        kwargs.setdefault("artifact_result", self.publication_result())
        return queue.enqueue(**kwargs)

    def test_publication_gate_allows_complete_trusted_ocr(self):
        decision = ocr_publication_eligibility(self.publication_result())

        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["reason_code"], "trusted_ocr_complete")
        self.assertTrue(decision["reason"])

    def test_publication_gate_allows_small_leading_edge_truncation(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                actual_media_window={
                    "start_stream_time": 105.0,
                    "end_stream_time": 160.0,
                },
                available_media_duration_seconds=55.0,
                duration_sec=55.0,
            )
        )

        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["reason_code"], "trusted_ocr_edge_truncated")
        self.assertIn("自动发布", decision["reason"])

    def test_publication_gate_allows_small_internal_gap_at_boundaries(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                stitched_across_gap=True,
                video_gap_count=1,
                skipped_gap_seconds=8.0,
                anchor_stream_time=126.0,
                actual_media_window={
                    "start_stream_time": 100.0,
                    "end_stream_time": 152.0,
                },
                available_media_duration_seconds=52.0,
                duration_sec=52.0,
            )
        )

        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["reason_code"], "trusted_ocr_small_internal_gap")
        self.assertEqual(decision["skipped_gap_seconds"], 8.0)
        self.assertIn("允许自动发布", decision["reason"])

    def test_publication_gate_allows_typical_three_second_gap(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                stitched_across_gap=True,
                video_gap_count=1,
                skipped_gap_seconds=3.1,
                anchor_stream_time=130.0,
                actual_media_window={
                    "start_stream_time": 100.0,
                    "end_stream_time": 156.9,
                },
                available_media_duration_seconds=56.8,
                duration_sec=56.8,
            )
        )

        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["reason_code"], "trusted_ocr_small_internal_gap")
        self.assertAlmostEqual(decision["skipped_gap_seconds"], 3.1)

    def test_publication_gate_rejects_internal_gap_over_eight_seconds(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                stitched_across_gap=True,
                video_gap_count=1,
                skipped_gap_seconds=8.1,
            )
        )

        self.assertFalse(decision["eligible"])
        self.assertEqual(decision["reason_code"], "internal_gap")
        self.assertEqual(decision["skipped_gap_seconds"], 8.1)

    def test_publication_gate_rejects_clip_below_52_seconds(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                actual_media_window={
                    "start_stream_time": 100.0,
                    "end_stream_time": 151.9,
                },
                anchor_stream_time=126.0,
                available_media_duration_seconds=51.9,
                duration_sec=51.9,
            )
        )

        self.assertFalse(decision["eligible"])
        self.assertEqual(decision["reason_code"], "insufficient_coverage")

    def test_small_internal_gap_enters_queue_and_publishes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            artifact = self.publication_result(
                stitched_across_gap=True,
                video_gap_count=1,
                skipped_gap_seconds=3.1,
                available_media_duration_seconds=56.8,
                duration_sec=56.8,
                actual_media_window={
                    "start_stream_time": 100.0,
                    "end_stream_time": 156.9,
                },
            )

            queued = self.enqueue(
                queue,
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队"},
                source_path=source,
                artifact_result=artifact,
            )
            self.assertEqual(queued["status"], "queued")
            self.assertTrue(queue.run_once())
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "published")
            self.assertEqual(record["error_code"], None)
            self.assertEqual(len(platform.calls), 1)

    def test_publication_gate_rejects_missing_event_side_even_when_duration_is_55(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                actual_media_window={
                    "start_stream_time": 100.0,
                    "end_stream_time": 155.0,
                },
                anchor_stream_time=132.0,
                available_media_duration_seconds=55.0,
                duration_sec=55.0,
            )
        )

        self.assertFalse(decision["eligible"])
        self.assertEqual(
            decision["reason_code"], "anchor_trailing_coverage_insufficient"
        )
        self.assertTrue(decision["reason"])

    def test_publication_gate_allows_minute_boundary_clip_missing_only_leading_edge(self):
        decision = ocr_publication_eligibility(
            self.publication_result(
                localization_source="minute_boundary",
                actual_media_window={
                    "start_stream_time": 105.0,
                    "end_stream_time": 160.0,
                },
                anchor_stream_time=160.0,
                clip_before_seconds=60.0,
                clip_after_seconds=0.0,
                available_media_duration_seconds=55.0,
                duration_sec=55.0,
                requested_media_window={
                    "start_stream_time": 100.0,
                    "end_stream_time": 160.0,
                },
            )
        )

        self.assertTrue(decision["eligible"])
        self.assertEqual(decision["reason_code"], "trusted_ocr_edge_truncated")

    def test_publication_gate_rejects_unverified_and_unsafe_metadata(self):
        cases = (
            (
                "ocr_unverified",
                {"localization_source": "", "ocr_verified": None},
            ),
            (
                "ocr_unverified",
                {"ocr_verified": False},
            ),
            (
                "event_frame_risk",
                {"event_frame_may_be_missing": True},
            ),
            (
                "anchor_adjusted",
                {"anchor_adjusted": True},
            ),
            (
                "internal_gap",
                {"stitched_across_gap": True, "video_gap_count": 1},
            ),
            (
                "internal_gap",
                {"video_gap_count": 1},
            ),
            (
                "internal_gap",
                {
                    "stitched_across_gap": True,
                    "video_gap_count": 1,
                    "skipped_gap_seconds": 0.0,
                },
            ),
            (
                "unverified_api_fallback",
                {
                    "output_kind": "api_time_range_fallback",
                    "localization_source": "api_time_range",
                },
            ),
            (
                "history_misaligned",
                {"fallback_label": "history_missing_nearest_clip"},
            ),
            (
                "metadata_missing",
                {"requested_media_window": None},
            ),
        )
        for expected_code, overrides in cases:
            with self.subTest(expected_code=expected_code, overrides=overrides):
                decision = ocr_publication_eligibility(
                    self.publication_result(**overrides)
                )
                self.assertFalse(decision["eligible"])
                self.assertEqual(decision["reason_code"], expected_code)
                self.assertTrue(decision["reason"])

    def test_publication_gate_rejects_short_clip_with_stable_reason(self):
        result = self.publication_result(
            actual_media_window={
                "start_stream_time": 100.0,
                "end_stream_time": 154.0,
            },
            available_media_duration_seconds=53.9,
            duration_sec=53.9,
        )

        first = ocr_publication_eligibility(result)
        second = ocr_publication_eligibility(dict(result))
        self.assertFalse(first["eligible"])
        self.assertEqual(first["reason_code"], "insufficient_coverage")
        self.assertEqual(first["reason"], second["reason"])

    def test_queued_ineligible_task_is_rechecked_before_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            queued = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队"},
                source_path=source,
                artifact_result=self.publication_result(),
            )
            self.assertEqual(queued["status"], "queued")

            # Simulate a persisted result becoming unsafe after enqueue. The
            # final delivery gate must not trust the old queued status.
            held = ocr_publication_eligibility(
                self.publication_result(
                    event_frame_may_be_missing=True,
                )
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.execute(
                    "UPDATE article_delivery_tasks SET eligibility_json=?, "
                    "status='queued', stage='queued' WHERE task_key=?",
                    (json.dumps(held, ensure_ascii=False, sort_keys=True), queued["task_key"]),
                )
                connection.commit()

            self.assertTrue(queue.run_once(now=100.0))
            self.assertEqual(platform.calls, [])
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "held")
            self.assertEqual(record["error_code"], "event_frame_risk")
            self.assertIn("事件画面", record["error"])
            self.assertEqual(
                record["publication_eligibility"]["reason_code"],
                "event_frame_risk",
            )
            self.assertEqual(
                record["ocr_article_eligibility"]["reason_code"],
                "event_frame_risk",
            )

    def test_api_fallback_is_staged_but_held_and_retry_cannot_bypass_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)

            held = self.enqueue(
                queue,
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队"},
                source_path=source,
                artifact_result={
                    "output_kind": "api_time_range_fallback",
                    "localization_source": "api_time_range",
                    "fallback_complete": True,
                },
            )

            self.assertEqual(held["status"], "held")
            self.assertEqual(held["error_code"], "unverified_api_fallback")
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                staged_path = connection.execute(
                    "SELECT staged_path FROM article_delivery_tasks WHERE task_key=?",
                    (held["task_key"],),
                ).fetchone()[0]
            self.assertTrue(Path(staged_path).is_file())

            retried = queue.retry(
                match_id="54478914", event_key=self.event()["event_key"]
            )
            self.assertEqual(retried["status"], "held")
            self.assertEqual(retried["error_code"], "unverified_api_fallback")
            self.assertFalse(queue.run_once(now=100.0))
            self.assertEqual(platform.calls, [])
            self.assertEqual(
                retried["publication_eligibility"]["reason_code"],
                "unverified_api_fallback",
            )

    def test_create_is_idempotent_and_exposes_admin_link(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)

            queued = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                source_path=source,
                artifact_result=self.publication_result(
                    visual_resolution="ocr_second_exact"
                ),
            )
            self.assertEqual(queued["status"], "queued")
            self.assertTrue(queue.run_once())

            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "published")
            self.assertEqual(record["quality_label"], "精确到秒")
            self.assertEqual(record["article_id"], "6230049")
            self.assertEqual(record["draft_url"], draft_admin_url("6230049"))
            self.assertEqual(platform.calls[0]["status"], 1)
            self.assertEqual(platform.calls[0]["add_to_tab"], 1)

            replay = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertEqual(replay["status"], "published")
            self.assertFalse(queue.run_once())
            self.assertEqual(len(platform.calls), 1)

    def test_unchanged_historical_gif_reuses_staged_copy_without_reading_again(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            first = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            with patch.object(queue, "_stage_file", wraps=queue._stage_file) as stage:
                replay = self.enqueue(queue,
                    match_id="54478914",
                    event=self.event(),
                    match_detail={"team_A_name": "主队"},
                    source_path=source,
                )

            stage.assert_not_called()
            self.assertEqual(replay["task_key"], first["task_key"])
            self.assertEqual(replay["status"], "queued")

    def test_missing_person_waits_locally_then_publishes_when_name_arrives(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform, person_wait_seconds=60)
            missing = {**self.event(), "person": ""}
            self.enqueue(queue,
                match_id="54478914",
                event=missing,
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                source_path=source,
            )

            self.assertTrue(queue.run_once(now=100.0))
            waiting = queue.records_for_match("54478914")[missing["event_key"]]
            self.assertEqual(waiting["status"], "waiting_person")
            self.assertEqual(waiting["person_deadline_at_unix"], 160.0)
            self.assertIsNone(waiting["article_id"])
            self.assertIsNone(waiting["draft_created_at_unix"])
            self.assertEqual(platform.calls, [])

            refreshed = queue.refresh_event(
                match_id="54478914",
                event={**missing, "person": "补齐球员"},
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                now=120.0,
            )
            self.assertEqual(refreshed["status"], "waiting_person")
            self.assertTrue(queue.run_once(now=120.0))

            published = queue.records_for_match("54478914")[missing["event_key"]]
            self.assertEqual(published["status"], "published")
            self.assertEqual(published["publish_reason"], "person_available")
            self.assertEqual(published["final_event"]["person"], "补齐球员")
            self.assertEqual(platform.calls[0]["status"], 1)
            self.assertNotIn("archive_id", platform.calls[0])
            self.assertIn("补齐球员进球", platform.calls[0]["title"])

    def test_missing_person_publishes_with_team_fallback_at_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform, person_wait_seconds=60)
            missing = {**self.event(), "person": "", "team": "A"}
            self.enqueue(queue,
                match_id="54478914",
                event=missing,
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                source_path=source,
            )

            self.assertTrue(queue.run_once(now=100.0))
            self.assertFalse(queue.run_once(now=159.9))
            self.assertTrue(queue.run_once(now=160.0))

            published = queue.records_for_match("54478914")[missing["event_key"]]
            self.assertEqual(published["status"], "published")
            self.assertEqual(published["publish_reason"], "team_fallback")
            self.assertEqual(len(platform.calls), 1)
            self.assertEqual(platform.calls[0]["status"], 1)
            self.assertNotIn("archive_id", platform.calls[0])
            self.assertIn("主队进球", platform.calls[0]["title"])
            self.assertEqual(published["final_event"]["person"], "主队")
            self.assertTrue(published["final_event"]["person_fallback"])

    def test_waiting_person_deadline_recovers_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform, person_wait_seconds=60)
            missing = {**self.event(), "person": ""}
            self.enqueue(queue,
                match_id="54478914",
                event=missing,
                match_detail={"team_A_name": "主队"},
                source_path=source,
            )
            self.assertTrue(queue.run_once(now=100.0))

            reopened = self.make_queue(root, platform, person_wait_seconds=60)
            self.assertFalse(reopened.run_once(now=159.0))
            self.assertTrue(reopened.run_once(now=160.0))
            record = reopened.records_for_match("54478914")[missing["event_key"]]
            self.assertEqual(record["status"], "published")
            self.assertEqual(len(platform.calls), 1)
            self.assertEqual(platform.calls[0]["status"], 1)

    def test_latest_event_loader_can_publish_before_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            missing = {**self.event(), "person": ""}
            loader_calls = []

            def load_latest(match_id, event_key, current):
                loader_calls.append((match_id, event_key, dict(current)))
                return {**current, "person": "接口补齐球员"}

            queue = self.make_queue(
                root,
                platform,
                person_wait_seconds=60,
                latest_event_loader=load_latest,
            )
            self.enqueue(queue,
                match_id="54478914",
                event=missing,
                match_detail={"team_A_name": "主队"},
                source_path=source,
            )
            self.assertTrue(queue.run_once(now=100.0))
            self.assertFalse(queue.run_once(now=129.9))
            self.assertTrue(queue.run_once(now=130.0))

            record = queue.records_for_match("54478914")[missing["event_key"]]
            self.assertEqual(record["status"], "published")
            self.assertEqual(record["final_event"]["person"], "接口补齐球员")
            self.assertEqual(len(loader_calls), 1)
            self.assertEqual(platform.calls[0]["status"], 1)

    def test_published_task_ignores_later_name_and_gif_revisions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertTrue(queue.run_once(now=100.0))

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            unchanged = self.enqueue(queue,
                match_id="54478914",
                event={**self.event(), "person": "后来更正"},
                match_detail={},
                source_path=source,
            )
            queue.refresh_event(
                match_id="54478914",
                event={**self.event(), "person": "再次更正"},
                now=120.0,
            )

            self.assertEqual(unchanged["status"], "published")
            self.assertFalse(queue.run_once(now=120.0))
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["final_event"]["person"], "球员甲")
            self.assertEqual(len(platform.calls), 1)

    def test_reliable_person_rejects_placeholder_values(self):
        for value in ("", "0", "unknown", "NONE", "null", "未提供球员", "未知球员"):
            with self.subTest(value=value):
                self.assertFalse(has_reliable_person({"person": value, "person_id": "9"}))
        self.assertTrue(has_reliable_person({"person": "真实球员"}))

    def test_team_fallback_resolves_symbolic_and_numeric_team_values(self):
        detail = {
            "team_A_id": "500001",
            "team_A_name": "主队",
            "team_B_id": "500002",
            "team_B_name": "客队",
        }
        self.assertEqual(
            _event_with_team_fallback({"team": "teamA", "person": ""}, detail)["person"],
            "主队",
        )
        self.assertEqual(
            _event_with_team_fallback(
                {"team": "500002", "metadata": {"team_id": "500002"}, "person": ""},
                detail,
            )["person"],
            "客队",
        )

    def test_changed_ocr_gif_updates_existing_draft(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            event = {**self.event(), "person": ""}
            self.enqueue(queue,
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
            )
            queue.run_once()

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            stat = source.stat()
            os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
            changed = self.enqueue(queue,
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
                artifact_result=self.publication_result(),
            )

            self.assertEqual(changed["status"], "queued")
            self.assertIsNone(changed["article_id"])
            queue.run_once()
            self.assertEqual(len(platform.calls), 0)
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "waiting_person")
            self.assertEqual(record["quality_label"], "精确到秒")

    def test_temporary_failure_waits_then_retries(self):
        temporary_error = OpenPlatformError(
            "平台暂时繁忙",
            code=50001,
            status_code=503,
            retriable=True,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient([temporary_error])
            queue = self.make_queue(root, platform, retry_delays_seconds=(1, 2, 3, 4))
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            self.assertTrue(queue.run_once(now=100.0))
            waiting = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(waiting["status"], "retry_wait")
            self.assertTrue(waiting["retriable"])
            self.assertEqual(waiting["next_attempt_at_unix"], 101.0)
            refreshed = self.enqueue(
                queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertEqual(refreshed["status"], "retry_wait")
            self.assertEqual(refreshed["error_code"], waiting["error_code"])
            self.assertEqual(refreshed["error"], waiting["error"])
            self.assertEqual(
                refreshed["next_attempt_at_unix"], waiting["next_attempt_at_unix"]
            )
            self.assertEqual(refreshed["diagnostics"], waiting["diagnostics"])
            self.assertFalse(queue.run_once(now=100.5))
            self.assertTrue(queue.run_once(now=101.0))
            completed = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(completed["status"], "published")
            self.assertEqual(len(platform.calls), 2)

    def test_retry_wait_preserves_deadline_when_refresh_has_no_ocr_metadata(self):
        temporary_error = OpenPlatformError(
            "平台暂时繁忙", code=50001, status_code=503, retriable=True
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(
                root,
                SequencePlatformClient([temporary_error]),
                retry_delays_seconds=(10, 20, 30, 40),
            )
            self.enqueue(queue, match_id="54478914", event=self.event(), match_detail={}, source_path=source)
            self.assertTrue(queue.run_once(now=100.0))
            before = queue.records_for_match("54478914")[self.event()["event_key"]]
            refreshed = self.enqueue(
                queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
                artifact_result={},
            )
            self.assertEqual(refreshed["status"], "retry_wait")
            self.assertEqual(refreshed["next_attempt_at_unix"], before["next_attempt_at_unix"])
            self.assertEqual(refreshed["error_code"], before["error_code"])
            self.assertFalse(queue.run_once(now=105.0))

    def test_manual_retry_exposes_platform_code_and_diagnostics(self):
        rejected = OpenPlatformError(
            "服务异常",
            code=5,
            status_code=502,
            retriable=False,
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient([rejected])
            queue = self.make_queue(root, platform)
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                source_path=source,
            )

            self.assertTrue(queue.run_once(now=100.0))
            failed = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(failed["platform_code"], "5")
            self.assertIn("code=5", failed["error"])
            self.assertGreater(failed["diagnostics"]["gif_bytes"], 0)
            self.assertIn("match_id", failed["diagnostics"]["request_summary"])

            refreshed = self.enqueue(
                queue,
                match_id="54478914",
                event=self.event(),
                match_detail={"team_A_name": "主队", "team_B_name": "客队"},
                source_path=source,
            )
            self.assertEqual(refreshed["status"], "failed")
            self.assertEqual(refreshed["error_code"], failed["error_code"])
            self.assertEqual(refreshed["error"], failed["error"])
            self.assertEqual(refreshed["platform_code"], failed["platform_code"])
            self.assertEqual(refreshed["diagnostics"], failed["diagnostics"])

            queued = queue.retry(
                match_id="54478914", event_key=self.event()["event_key"]
            )
            self.assertEqual(queued["status"], "queued")
            self.assertTrue(queue.run_once(now=101.0))
            completed = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(completed["status"], "published")

    def test_expired_creating_lease_recovers_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            queued = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.execute(
                    "UPDATE article_delivery_tasks SET status='creating', "
                    "lease_until_unix=50 WHERE task_key=?",
                    (queued["task_key"],),
                )

            reopened = self.make_queue(root, platform)
            self.assertTrue(reopened.run_once(now=51.0))
            record = reopened.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "published")

    def test_existing_queue_schema_adds_generation_and_lease_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "delivery.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE article_delivery_tasks (
                        task_key TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        next_attempt_at_unix REAL,
                        lease_until_unix REAL
                    )
                    """
                )
            queue = ArticleDraftQueue(database_path=database)

            queue.status()
            with sqlite3.connect(database) as connection:
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(article_delivery_tasks)"
                    )
                }
            self.assertTrue(
                {
                    "generation",
                    "eligibility_json",
                    "lease_token",
                    "previous_staged_path",
                }.issubset(columns)
            )

    def test_legacy_queued_task_without_eligibility_is_migrated_and_held(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            stat = source.stat()
            source_signature = f"{stat.st_size}:{stat.st_mtime_ns}"
            database = root / "delivery.sqlite3"
            event = self.event()
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE article_delivery_tasks (
                        task_key TEXT PRIMARY KEY,
                        match_id TEXT NOT NULL,
                        event_key TEXT NOT NULL,
                        artifact_kind TEXT NOT NULL,
                        delivery_mode TEXT NOT NULL,
                        source_path TEXT NOT NULL,
                        source_signature TEXT NOT NULL,
                        event_json TEXT NOT NULL,
                        match_detail_json TEXT NOT NULL,
                        quality_label TEXT,
                        status TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        article_id TEXT,
                        artifact_sha256 TEXT,
                        staged_path TEXT,
                        gif_url TEXT,
                        platform_code TEXT,
                        duplicate INTEGER NOT NULL DEFAULT 0,
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        next_attempt_at_unix REAL,
                        lease_until_unix REAL,
                        retriable INTEGER NOT NULL DEFAULT 0,
                        auth_required INTEGER NOT NULL DEFAULT 0,
                        error_code TEXT,
                        error TEXT,
                        created_at_unix REAL NOT NULL,
                        updated_at_unix REAL NOT NULL,
                        completed_at_unix REAL,
                        UNIQUE(match_id, event_key, artifact_kind, delivery_mode)
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO article_delivery_tasks (
                        task_key, match_id, event_key, artifact_kind, delivery_mode,
                        source_path, source_signature, event_json, match_detail_json,
                        quality_label, status, stage, created_at_unix, updated_at_unix
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "legacy-task",
                        "54478914",
                        event["event_key"],
                        "ocr_window",
                        "draft",
                        str(source),
                        source_signature,
                        json.dumps(event, ensure_ascii=False),
                        json.dumps({"team_A_name": "主队"}, ensure_ascii=False),
                        "OCR GIF 已生成",
                        "queued",
                        "queued",
                        10.0,
                        10.0,
                    ),
                )

            platform = SequencePlatformClient()
            queue = self.make_queue(
                root,
                platform,
                auto_publish_after_unix=0.0,
            )
            self.assertTrue(queue.run_once(now=100.0))

            with sqlite3.connect(database) as connection:
                columns = {
                    row[1]
                    for row in connection.execute(
                        "PRAGMA table_info(article_delivery_tasks)"
                    )
                }
            self.assertIn("eligibility_json", columns)
            record = queue.records_for_match("54478914")[event["event_key"]]
            self.assertEqual(record["status"], "held")
            self.assertEqual(record["error_code"], "metadata_missing")
            self.assertEqual(platform.calls, [])

    def test_missing_or_outside_source_fails_with_plain_reason(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            missing = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=root / "output" / "54478914" / "missing.gif",
            )
            self.assertEqual(missing["status"], "failed")
            self.assertIn("OCR GIF 文件不存在", missing["error"])

            outside = root / "outside.gif"
            outside.write_bytes(animated_gif_bytes())
            with self.assertRaisesRegex(ValueError, "不属于这场比赛"):
                self.enqueue(queue,
                    match_id="54478914",
                    event={**self.event(), "event_key": "outside"},
                    match_detail={},
                    source_path=outside,
                )

    def test_staged_gif_survives_source_cleanup_while_waiting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )

            source.unlink()
            self.assertTrue(queue.run_once())
            record = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(record["status"], "published")
            self.assertEqual(len(platform.calls), 1)

    def test_late_old_generation_result_cannot_overwrite_new_ocr_task(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            platform = SequencePlatformClient()
            queue = self.make_queue(root, platform)
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            old_claim = dict(queue._claim_due(100.0))

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            changed = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertEqual(changed["status"], "queued")

            queue._record_success(
                old_claim,
                {
                    "article_id": "7000001",
                    "gif": {},
                    "updated": False,
                    "duplicate": False,
                },
                now=101.0,
            )
            waiting = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(waiting["status"], "queued")
            self.assertEqual(waiting["article_id"], "7000001")

            self.assertTrue(queue.run_once(now=102.0))
            self.assertEqual(platform.calls[0]["archive_id"], 7000001)
            completed = queue.records_for_match("54478914")[self.event()["event_key"]]
            self.assertEqual(completed["status"], "published")

    def test_expired_lease_old_token_cannot_change_current_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient(), lease_seconds=10)
            queued = self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            old_claim = dict(queue._claim_due(100.0))
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.execute(
                    "UPDATE article_delivery_tasks SET lease_until_unix=101 WHERE task_key=?",
                    (queued["task_key"],),
                )
            current_claim = dict(queue._claim_due(102.0))
            self.assertNotEqual(old_claim["lease_token"], current_claim["lease_token"])

            queue._record_failure(
                old_claim,
                ArticlePublishError(
                    "旧请求失败",
                    code="old_request_failed",
                    stage="platform_publish",
                ),
                now=103.0,
            )
            queue._record_success(
                old_claim,
                {
                    "article_id": "7000002",
                    "gif": {},
                    "updated": False,
                    "duplicate": False,
                },
                now=103.0,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                connection.row_factory = sqlite3.Row
                row = connection.execute(
                    "SELECT * FROM article_delivery_tasks WHERE task_key=?",
                    (queued["task_key"],),
                ).fetchone()
            self.assertEqual(row["status"], "creating")
            self.assertEqual(row["lease_token"], current_claim["lease_token"])
            self.assertEqual(row["article_id"], "7000002")

    def test_updated_draft_removes_unreferenced_previous_staged_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            event = {**self.event(), "person": ""}
            self.enqueue(queue,
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                previous = Path(
                    connection.execute(
                        "SELECT staged_path FROM article_delivery_tasks"
                    ).fetchone()[0]
                )
            self.assertTrue(previous.exists())

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            self.enqueue(queue,
                match_id="54478914",
                event=event,
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            self.assertFalse(previous.exists())

    def test_rapid_ocr_revisions_remove_the_unreferenced_oldest_staged_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                first_path = Path(
                    connection.execute(
                        "SELECT staged_path FROM article_delivery_tasks"
                    ).fetchone()[0]
                )

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertTrue(first_path.exists())

            source.write_bytes(animated_gif_bytes(b"\x00\xff\x00"))
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            self.assertFalse(first_path.exists())

    def test_previous_staged_gif_is_kept_when_formal_publish_references_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = self.write_source(root)
            queue = self.make_queue(root, SequencePlatformClient())
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            queue.publisher.publish(
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            with sqlite3.connect(root / "delivery.sqlite3") as connection:
                previous = Path(
                    connection.execute(
                        "SELECT gif_path FROM article_publish_records"
                    ).fetchone()[0]
                )

            source.write_bytes(animated_gif_bytes(b"\xff\x00\x00"))
            self.enqueue(queue,
                match_id="54478914",
                event=self.event(),
                match_detail={},
                source_path=source,
            )
            queue.run_once()
            self.assertTrue(previous.exists())


if __name__ == "__main__":
    unittest.main()
