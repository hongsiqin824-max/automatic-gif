from __future__ import annotations

from dataclasses import replace
import tempfile
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from live_goal_pipeline import Segment
from pipeline_runtime import PipelineRuntime
from vision_runtime import (
    OCR_FFMPEG_WATCHDOG_SECONDS,
    VisionJob,
    VisualLocationFailed,
    _job_with_cached_scoreboard_profile,
    _locate_ocr_window_with_cached_profile_recovery,
    _ocr_active_processing_budget,
    _ocr_budget_after_elapsed,
    _record_scoreboard_roi_failure,
    _record_scoreboard_roi_success,
    process_vision_artifact,
)


class OcrProcessingWatchdogTests(unittest.TestCase):
    def test_cumulative_processing_time_is_diagnostic_only(self):
        waiting_state = {
            "state": "waiting_for_target_media",
            "active_processing_budget": {
                "total_seconds": 180.0,
                "used_seconds": 20.0,
            },
            "last_execution_completed_at_unix": 100.0,
        }

        after_wait = _ocr_active_processing_budget(
            waiting_state,
            now_unix=10_000.0,
            account_open_execution=False,
        )
        self.assertEqual(after_wait["used_seconds"], 20.0)
        self.assertEqual(after_wait["remaining_seconds"], 160.0)
        self.assertEqual(after_wait["encoding_reserve_seconds"], 30.0)
        self.assertFalse(after_wait["enforced"])

        first_run = _ocr_active_processing_budget(
            {
                "execution_started_at_unix": 200.0,
                "active_processing_budget": after_wait,
            },
            now_unix=240.0,
            account_open_execution=True,
        )
        second_run = _ocr_active_processing_budget(
            {
                "execution_started_at_unix": 300.0,
                "active_processing_budget": first_run,
            },
            now_unix=350.0,
            account_open_execution=True,
        )

        self.assertEqual(first_run["used_seconds"], 60.0)
        self.assertEqual(second_run["used_seconds"], 110.0)
        self.assertEqual(second_run["remaining_seconds"], 70.0)
        self.assertEqual(second_run["total_seconds"], 180.0)
        repeated_accounting = _ocr_active_processing_budget(
            {
                "execution_started_at_unix": 300.0,
                "active_processing_budget": second_run,
            },
            now_unix=500.0,
            account_open_execution=True,
        )
        self.assertEqual(repeated_accounting["used_seconds"], 110.0)
        self.assertEqual(
            _ocr_budget_after_elapsed(
                second_run,
                15.0,
                phase="gif_encoding",
            )["used_seconds"],
            125.0,
        )

    def test_exhausted_legacy_budget_does_not_block_another_ocr_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_job(root, "budget-exhausted")
            task = runtime.store.get_vision_task(job.event_key, "ocr_window")
            runtime.record_vision_readiness_wait(
                job.event_key,
                "persisted processing budget",
                artifact_kind="ocr_window",
                error_kind="waiting_for_clock_target",
                window_metadata={
                    "progressive_scan": {
                        "state": "waiting_for_clock_target",
                        "active_processing_budget": {
                            "total_seconds": 180.0,
                            "used_seconds": 150.0,
                            "remaining_seconds": 30.0,
                            "encoding_reserve_seconds": 30.0,
                        },
                    },
                },
                now=task.created_at_unix,
                next_attempt_at_unix=task.created_at_unix,
            )
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            try:
                miss = RuntimeError("scan attempted")
                with (
                    patch(
                        "vision_runtime._locate_ocr_window_across_components",
                        side_effect=miss,
                    ) as locate,
                    patch("vision_runtime.time.time", return_value=1001.0),
                ):
                    completed = self._run_ocr(
                        job,
                        runtime,
                        root,
                        [Segment(segment_path, 0.0, 240.0)],
                    )
                self.assertTrue(completed)
                failed = runtime.store.get_vision_task(
                    job.event_key, "ocr_window"
                )
                self.assertEqual(failed.status, "failed")
                self.assertEqual(failed.last_error_kind, "ocr_processing_failed")
                locate.assert_called_once()
                self.assertNotIn(
                    "processing_deadline_monotonic",
                    locate.call_args.kwargs,
                )
            finally:
                runtime.close()

    def test_gif_encoding_uses_independent_watchdog_not_remaining_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._create_job(root, "encoding-timeout")
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            runtime.transition_vision_task(
                job.event_key,
                "locating",
                artifact_kind="ocr_window",
            )
            runtime.transition_vision_task(
                job.event_key,
                "located",
                artifact_kind="ocr_window",
                result={
                    "anchor_stream_time": 200.0,
                    "localization_source": "exact",
                    "location_kind": "match_clock_second",
                    "localization_quality": "exact",
                    "clip_before_seconds": 30.0,
                    "clip_after_seconds": 30.0,
                    "target_clock_seconds": 120,
                },
                window_metadata={
                    "progressive_scan": {
                        "active_processing_budget": {
                            "total_seconds": 180.0,
                            "used_seconds": 160.0,
                            "remaining_seconds": 20.0,
                            "encoding_reserve_seconds": 30.0,
                        },
                    },
                },
            )
            timeout = subprocess.TimeoutExpired(
                ["ffmpeg"], OCR_FFMPEG_WATCHDOG_SECONDS
            )
            try:
                with patch("vision_runtime.encode_gif", side_effect=timeout) as encode:
                    completed = process_vision_artifact(
                        job,
                        runtime,
                        lambda: [Segment(segment_path, 0.0, 240.0)],
                        "ffmpeg",
                        "ffprobe",
                        root,
                        artifact_kind="ocr_window",
                        search_before=120.0,
                        search_after=30.0,
                        refined_before=8.0,
                        refined_after=12.0,
                        width=768,
                        fps=16.0,
                        colors=256,
                        size_reference_bytes=10_000_000,
                        python=Path("python"),
                        timeout_seconds=3.0,
                        ocr_python=root / "ocr-python",
                        ocr_timeout_seconds=3.0,
                    )
                self.assertTrue(completed)
                failed = runtime.store.get_vision_task(
                    job.event_key, "ocr_window"
                )
                self.assertEqual(failed.status, "failed")
                self.assertEqual(
                    failed.last_error_kind,
                    "ocr_window_encoding_timeout",
                )
                self.assertEqual(failed.failure_stage, "ocr_window_encoding")
                self.assertEqual(
                    failed.result["encoding_timeout_seconds"],
                    OCR_FFMPEG_WATCHDOG_SECONDS,
                )
                self.assertEqual(
                    encode.call_args.kwargs["timeout_seconds"],
                    OCR_FFMPEG_WATCHDOG_SECONDS,
                )
            finally:
                runtime.close()

    @staticmethod
    def _create_job(
        root: Path,
        suffix: str,
    ) -> tuple[PipelineRuntime, VisionJob]:
        runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
        event_key = f"match:G:{suffix}"
        runtime.discover_task(
            match_id="match",
            event_data={
                "event_key": event_key,
                "code": "G",
                "event_type": "goal",
                "minute": "2",
                "minute_extra": "0",
                "second": 120,
                "team": "teamA",
                "person": "A",
                "person_id": "1",
                "score": "1-0",
                "reason": "",
                "metadata": {},
            },
            observed_stream_time=200.0,
            observed_source_time=None,
            clip_anchor_stream_time=200.0,
            clip_anchor_source_time=None,
            output_due_stream_time=230.0,
            detected_at_unix=1000.0,
        )
        runtime.enqueue_vision_task(
            event_key,
            artifact_kind="ocr_window",
            search_start_stream_time=80.0,
            search_end_stream_time=230.0,
            clip_before_seconds=30.0,
            clip_after_seconds=30.0,
            deadline_at_unix=1300.0,
            now=1000.0,
        )
        job = VisionJob(
            event_key,
            "match",
            "G",
            "goal",
            200.0,
            None,
            1000.0,
            observed_anchor_stream_time=200.0,
            event_minute="2",
            event_second=120,
            target_score="1-0",
            clock_only=True,
        )
        return runtime, job

    @staticmethod
    def _run_ocr(
        job: VisionJob,
        runtime: PipelineRuntime,
        root: Path,
        segments: list[Segment] | None = None,
    ) -> bool:
        return process_vision_artifact(
            job,
            runtime,
            lambda: list(segments or []),
            "ffmpeg",
            "ffprobe",
            root,
            artifact_kind="ocr_window",
            search_before=120.0,
            search_after=30.0,
            refined_before=8.0,
            refined_after=12.0,
            width=768,
            fps=16.0,
            colors=256,
            size_reference_bytes=10_000_000,
            python=Path("python"),
            timeout_seconds=3.0,
            ocr_python=root / "ocr-python",
            ocr_timeout_seconds=3.0,
        )


class ScoreboardRoiCacheTests(unittest.TestCase):
    @staticmethod
    def _job(match_id: str = "match-roi") -> VisionJob:
        return VisionJob(
            f"{match_id}:G:roi",
            match_id,
            "G",
            "goal",
            100.0,
            None,
            1000.0,
            event_minute="2",
            event_second=120,
            clock_only=True,
        )

    @staticmethod
    def _profile() -> dict:
        return {
            "profile_id": "cached",
            "reference_resolution": [1920, 1080],
            "clock_roi": [40, 30, 180, 82],
        }

    def test_auto_discovery_persists_and_is_reused_after_sqlite_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "state.sqlite3"
            event_log = root / "events.jsonl"
            runtime = PipelineRuntime(database, event_log)
            job = self._job()
            located = {
                "diagnostics": {
                    "clock_readable_rate": 0.88,
                    "auto_clock": {
                        "clock_roi": [40, 30, 180, 82],
                        "frame_resolution": [1920, 1080],
                    },
                },
            }
            saved = _record_scoreboard_roi_success(
                runtime,
                job,
                located,
                cached=None,
            )
            self.assertEqual(saved["status"], "discovered")
            cached_job, cached = _job_with_cached_scoreboard_profile(
                runtime, job
            )
            self.assertIsNotNone(cached)
            self.assertEqual(
                cached_job.scoreboard_profile["clock_roi"],
                [40, 30, 180, 82],
            )
            runtime.close()

            reopened = PipelineRuntime(database, event_log)
            try:
                cached_job, cached = _job_with_cached_scoreboard_profile(
                    reopened, job
                )
                self.assertIsNotNone(cached)
                self.assertEqual(cached.confidence, 0.88)
                self.assertEqual(
                    cached_job.scoreboard_profile["clock_roi"],
                    [40, 30, 180, 82],
                )
                self.assertEqual(
                    cached_job.scoreboard_profile["reference_resolution"],
                    [1920, 1080],
                )
            finally:
                reopened.close()

    def test_cache_is_not_reused_after_three_consecutive_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-failures")
            runtime.store.save_scoreboard_roi_cache(job.match_id, self._profile())
            try:
                for expected_streak, error_kind in (
                    (1, "scoreboard_missing"),
                    (2, "scoreboard_missing"),
                    (3, "ocr_clock_unreadable"),
                ):
                    _, cached = _job_with_cached_scoreboard_profile(runtime, job)
                    _record_scoreboard_roi_failure(
                        runtime,
                        job,
                        error_kind,
                        cached=cached,
                    )
                    self.assertEqual(
                        runtime.store.get_scoreboard_roi_cache(
                            job.match_id
                        ).failure_streak,
                        expected_streak,
                    )
                uncached_job, cached = _job_with_cached_scoreboard_profile(
                    runtime, job
                )
                self.assertIsNone(cached)
                self.assertIsNone(uncached_job.scoreboard_profile)
            finally:
                runtime.close()

    def test_profile_mismatch_invalidates_cache_immediately(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-mismatch")
            runtime.store.save_scoreboard_roi_cache(job.match_id, self._profile())
            try:
                _, cached = _job_with_cached_scoreboard_profile(runtime, job)
                _record_scoreboard_roi_failure(
                    runtime,
                    job,
                    "clock_profile_mismatch",
                    cached=cached,
                )
                uncached_job, cached = _job_with_cached_scoreboard_profile(
                    runtime, job
                )
                self.assertIsNone(cached)
                self.assertIsNone(uncached_job.scoreboard_profile)
                self.assertEqual(
                    runtime.store.get_scoreboard_roi_cache(
                        job.match_id
                    ).failure_streak,
                    3,
                )
            finally:
                runtime.close()

    def test_persisted_short_probe_mismatch_bypasses_cache_on_next_attempt(self):
        job = self._job("match-roi-short-probe")
        cache_reads: list[str] = []

        class Store:
            @staticmethod
            def get_vision_task(event_key, *, artifact_kind):
                self.assertEqual(event_key, job.event_key)
                self.assertEqual(artifact_kind, "ocr_window")
                return SimpleNamespace(
                    window_metadata={
                        "progressive_scan": {
                            "scoreboard_roi_cache_bypass": True,
                        }
                    }
                )

            @staticmethod
            def get_scoreboard_roi_cache(match_id):
                cache_reads.append(match_id)
                return object()

        effective_job, cached = _job_with_cached_scoreboard_profile(
            SimpleNamespace(store=Store()),
            job,
        )

        self.assertIs(effective_job, job)
        self.assertIsNone(cached)
        self.assertEqual(cache_reads, [])

    def test_explicit_profile_is_preserved_even_when_cache_bypass_is_persisted(self):
        profile = self._profile()
        job = replace(
            self._job("match-explicit-profile"),
            scoreboard_profile=profile,
        )

        class Store:
            @staticmethod
            def get_vision_task(*_args, **_kwargs):
                raise AssertionError("explicit profiles must not inspect cache state")

        effective_job, cached = _job_with_cached_scoreboard_profile(
            SimpleNamespace(store=Store()),
            job,
        )

        self.assertIs(effective_job, job)
        self.assertEqual(effective_job.scoreboard_profile, profile)
        self.assertIsNone(cached)

    def test_profile_mismatch_rediscovery_retries_current_event_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-recovery")
            runtime.store.save_scoreboard_roi_cache(job.match_id, self._profile())
            cached_job, cached = _job_with_cached_scoreboard_profile(runtime, job)
            calls: list[dict | None] = []

            def locate(candidate: VisionJob):
                calls.append(candidate.scoreboard_profile)
                if len(calls) == 1:
                    raise VisualLocationFailed(
                        "clock_profile_mismatch",
                        "cached ROI does not match this video",
                        {"stage": "ocr_clock_discovery"},
                    )
                return (
                    {
                        "anchor_stream_time": 101.0,
                        "diagnostics": {"auto_clock": {"clock_roi": [40, 30, 180, 82]}},
                    },
                    {"path": "candidate.mp4"},
                    ["segment.ts"],
                )

            try:
                effective_job, located, _materialized, _paths, retried = (
                    _locate_ocr_window_with_cached_profile_recovery(
                        cached_job,
                        runtime,
                        cached,
                        locate,
                    )
                )
                self.assertTrue(retried)
                self.assertIsNone(effective_job.scoreboard_profile)
                self.assertEqual(len(calls), 2)
                self.assertIsNotNone(calls[0])
                self.assertIsNone(calls[1])
                self.assertEqual(
                    located["scoreboard_roi_cache_recovery"]["status"],
                    "rediscovered",
                )
                self.assertEqual(
                    runtime.store.get_scoreboard_roi_cache(job.match_id).failure_streak,
                    3,
                )
            finally:
                runtime.close()

    def test_profile_mismatch_rediscovery_failure_is_reported_without_retry_loop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-recovery-failed")
            runtime.store.save_scoreboard_roi_cache(job.match_id, self._profile())
            cached_job, cached = _job_with_cached_scoreboard_profile(runtime, job)
            calls: list[dict | None] = []

            def locate(candidate: VisionJob):
                calls.append(candidate.scoreboard_profile)
                raise VisualLocationFailed(
                    "clock_profile_mismatch" if len(calls) == 1 else "scoreboard_missing",
                    "OCR could not use this layout",
                    {"stage": "ocr_clock_discovery"},
                )

            try:
                with self.assertRaises(VisualLocationFailed) as raised:
                    _locate_ocr_window_with_cached_profile_recovery(
                        cached_job,
                        runtime,
                        cached,
                        locate,
                    )
                self.assertEqual(calls, [self._profile(), None])
                recovery = raised.exception.diagnostics[
                    "scoreboard_roi_cache_recovery"
                ]
                self.assertEqual(recovery["status"], "rediscovery_failed")
                self.assertEqual(recovery["rediscovery_error_kind"], "scoreboard_missing")
            finally:
                runtime.close()


if __name__ == "__main__":
    unittest.main()
