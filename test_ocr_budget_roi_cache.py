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
    _scoreboard_layout_mode,
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

    def test_layout_mode_is_conservative_at_period_boundaries(self):
        job = self._job("match-layout")
        cases = (
            ("2", "0", "normal"),
            ("44", "0", "normal"),
            ("45", "0", None),
            ("90", "0", None),
            ("45+2", "0", "stoppage"),
            ("90+5", "0", "stoppage"),
            ("90", "5", "stoppage"),
            ("45+2", "3", None),
            ("invalid", "0", None),
        )
        for minute, extra, expected in cases:
            with self.subTest(minute=minute, extra=extra):
                self.assertEqual(
                    _scoreboard_layout_mode(
                        replace(
                            job,
                            event_minute=minute,
                            event_minute_extra=extra,
                        )
                    ),
                    expected,
                )

    def test_layout_and_resolution_cache_keys_do_not_overwrite_each_other(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            normal = self._profile()
            stoppage = {**self._profile(), "clock_roi": [100, 60, 300, 120]}
            smaller = {**self._profile(), "reference_resolution": [1280, 720]}
            try:
                runtime.store.save_scoreboard_roi_cache_v2(
                    "match-isolated", "normal", "1920x1080", normal
                )
                runtime.store.save_scoreboard_roi_cache_v2(
                    "match-isolated", "stoppage", "1920x1080", stoppage
                )
                runtime.store.save_scoreboard_roi_cache_v2(
                    "match-isolated", "normal", "1280x720", smaller
                )

                self.assertEqual(
                    runtime.store.get_scoreboard_roi_cache_v2(
                        "match-isolated", "normal", "1920x1080"
                    ).profile["clock_roi"],
                    normal["clock_roi"],
                )
                self.assertEqual(
                    runtime.store.get_scoreboard_roi_cache_v2(
                        "match-isolated", "stoppage", "1920x1080"
                    ).profile["clock_roi"],
                    stoppage["clock_roi"],
                )
                self.assertEqual(
                    runtime.store.get_scoreboard_roi_cache_v2(
                        "match-isolated", "normal", "1280x720"
                    ).profile["reference_resolution"],
                    [1280, 720],
                )
            finally:
                runtime.close()

    def test_legacy_cache_is_preserved_but_not_migrated_or_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "state.sqlite3"
            event_log = root / "events.jsonl"
            runtime = PipelineRuntime(database, event_log)
            runtime.store.save_scoreboard_roi_cache("legacy-match", self._profile())
            runtime.close()

            reopened = PipelineRuntime(database, event_log)
            try:
                self.assertIsNotNone(
                    reopened.store.get_scoreboard_roi_cache("legacy-match")
                )
                self.assertIsNone(
                    reopened.store.get_scoreboard_roi_cache_v2(
                        "legacy-match", "normal", "1920x1080"
                    )
                )
                effective, cached = _job_with_cached_scoreboard_profile(
                    reopened, self._job("legacy-match")
                )
                self.assertIsNone(cached)
                self.assertIsNone(effective.scoreboard_profile)
            finally:
                reopened.close()

    def test_stale_success_and_failure_cannot_replace_new_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            old_profile = self._profile()
            new_profile = {**self._profile(), "clock_roi": [80, 40, 260, 96]}
            try:
                old = runtime.store.save_scoreboard_roi_cache_v2(
                    "match-cas", "normal", "1920x1080", old_profile
                )
                current = runtime.store.save_scoreboard_roi_cache_v2(
                    "match-cas",
                    "normal",
                    "1920x1080",
                    new_profile,
                    expected_profile_fingerprint=old.profile_fingerprint,
                )
                self.assertIsNone(
                    runtime.store.record_scoreboard_roi_failure_v2(
                        "match-cas",
                        "normal",
                        "1920x1080",
                        invalidate=True,
                        profile_fingerprint=old.profile_fingerprint,
                    )
                )
                retained = runtime.store.save_scoreboard_roi_cache_v2(
                    "match-cas",
                    "normal",
                    "1920x1080",
                    old_profile,
                    expected_profile_fingerprint=old.profile_fingerprint,
                )
                self.assertEqual(retained.profile, current.profile)
                self.assertEqual(retained.profile_fingerprint, current.profile_fingerprint)
                self.assertEqual(retained.failure_streak, 0)
                rediscovered_late = runtime.store.save_scoreboard_roi_cache_v2(
                    "match-cas", "normal", "1920x1080", old_profile
                )
                self.assertEqual(
                    rediscovered_late.profile_fingerprint,
                    current.profile_fingerprint,
                )
            finally:
                runtime.close()

    def test_cache_version_guards_are_checked_for_success_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            profile = self._profile()
            try:
                cached = runtime.store.save_scoreboard_roi_cache_v2(
                    "match-version-guard",
                    "normal",
                    "1920x1080",
                    profile,
                    now=100.0,
                )
                stale = runtime.store.save_scoreboard_roi_cache_v2(
                    "match-version-guard",
                    "normal",
                    "1920x1080",
                    {**profile, "clock_roi": [80, 40, 260, 96]},
                    expected_profile_fingerprint=cached.profile_fingerprint,
                    expected_updated_at_unix=99.0,
                    expected_failure_streak=cached.failure_streak,
                    now=101.0,
                )
                self.assertEqual(stale.profile, cached.profile)
                self.assertEqual(stale.updated_at_unix, cached.updated_at_unix)

                self.assertIsNone(
                    runtime.store.record_scoreboard_roi_failure_v2(
                        "match-version-guard",
                        "normal",
                        "1920x1080",
                        profile_fingerprint=cached.profile_fingerprint,
                        expected_updated_at_unix=99.0,
                        expected_failure_streak=cached.failure_streak,
                        now=102.0,
                    )
                )
                retained = runtime.store.get_scoreboard_roi_cache_v2(
                    "match-version-guard", "normal", "1920x1080"
                )
                self.assertIsNotNone(retained)
                self.assertEqual(retained.failure_streak, 0)

                updated = runtime.store.record_scoreboard_roi_failure_v2(
                    "match-version-guard",
                    "normal",
                    "1920x1080",
                    profile_fingerprint=cached.profile_fingerprint,
                    expected_updated_at_unix=cached.updated_at_unix,
                    expected_failure_streak=cached.failure_streak,
                    now=103.0,
                )
                self.assertIsNotNone(updated)
                self.assertEqual(updated.failure_streak, 1)
            finally:
                runtime.close()

    def test_explicit_profile_is_never_written_to_automatic_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = replace(self._job("match-explicit-no-cache"), scoreboard_profile=self._profile())
            located = {
                "diagnostics": {
                    "auto_clock": {
                        "clock_roi": [40, 30, 180, 82],
                        "frame_resolution": [1920, 1080],
                    }
                }
            }
            try:
                self.assertIsNone(
                    _record_scoreboard_roi_success(
                        runtime, job, located, cached=None
                    )
                )
                self.assertIsNone(
                    runtime.store.get_scoreboard_roi_cache_v2(
                        job.match_id, "normal", "1920x1080"
                    )
                )
            finally:
                runtime.close()

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
            self.assertEqual(saved["success_streak"], 1)
            self.assertEqual(
                runtime.store.get_scoreboard_roi_cache_v2(
                    job.match_id, "normal", "1920x1080"
                ).success_streak,
                1,
            )
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

    def test_resolution_change_invalidates_only_old_cached_key(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-resolution-change")
            old_cached = runtime.store.save_scoreboard_roi_cache_v2(
                job.match_id, "normal", "1920x1080", self._profile()
            )
            cached_job = replace(job, scoreboard_profile=dict(old_cached.profile))
            located = {
                "diagnostics": {
                    "clock_readable_rate": 0.9,
                    "scoreboard_profile": {
                        "clock_roi": [26, 20, 120, 55],
                        "frame_resolution": [1280, 720],
                    },
                }
            }
            try:
                saved = _record_scoreboard_roi_success(
                    runtime,
                    cached_job,
                    located,
                    cached=old_cached,
                )
                old_entry = runtime.store.get_scoreboard_roi_cache_v2(
                    job.match_id, "normal", "1920x1080"
                )
                new_entry = runtime.store.get_scoreboard_roi_cache_v2(
                    job.match_id, "normal", "1280x720"
                )
                self.assertEqual(saved["resolution_key"], "1280x720")
                self.assertEqual(old_entry.failure_streak, 3)
                self.assertEqual(new_entry.failure_streak, 0)
                self.assertEqual(new_entry.success_streak, 1)
                self.assertEqual(new_entry.profile["clock_roi"], [26, 20, 120, 55])
                self.assertEqual(
                    new_entry.profile["reference_resolution"], [1280, 720]
                )
            finally:
                runtime.close()

    def test_cache_is_not_reused_after_three_consecutive_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-failures")
            runtime.store.save_scoreboard_roi_cache_v2(
                job.match_id, "normal", "1920x1080", self._profile()
            )
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
                        runtime.store.get_scoreboard_roi_cache_v2(
                            job.match_id, "normal", "1920x1080"
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
            runtime.store.save_scoreboard_roi_cache_v2(
                job.match_id, "normal", "1920x1080", self._profile()
            )
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
                    runtime.store.get_scoreboard_roi_cache_v2(
                        job.match_id, "normal", "1920x1080"
                    ).failure_streak,
                    3,
                )
                rediscovered = runtime.store.save_scoreboard_roi_cache_v2(
                    job.match_id,
                    "normal",
                    "1920x1080",
                    {**self._profile(), "clock_roi": [80, 40, 260, 96]},
                )
                self.assertEqual(rediscovered.failure_streak, 0)
                self.assertEqual(
                    rediscovered.profile["clock_roi"], [80, 40, 260, 96]
                )
            finally:
                runtime.close()

    def test_persisted_short_probe_mismatch_bypasses_cache_on_next_attempt(self):
        job = self._job("match-roi-short-probe")
        cache_reads: list[tuple[str, str, str | None]] = []

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
            def get_scoreboard_roi_cache_v2(match_id, layout_mode, resolution_key):
                cache_reads.append((match_id, layout_mode, resolution_key))
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
            runtime.store.save_scoreboard_roi_cache_v2(
                job.match_id, "normal", "1920x1080", self._profile()
            )
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
                    runtime.store.get_scoreboard_roi_cache_v2(
                        job.match_id, "normal", "1920x1080"
                    ).failure_streak,
                    3,
                )
            finally:
                runtime.close()

    def test_profile_mismatch_rediscovery_failure_is_reported_without_retry_loop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
            job = self._job("match-roi-recovery-failed")
            runtime.store.save_scoreboard_roi_cache_v2(
                job.match_id, "normal", "1920x1080", self._profile()
            )
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

    def test_rediscovery_attempt_is_durable_without_inflating_locate_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "state.sqlite3"
            event_log = root / "events.jsonl"
            runtime = PipelineRuntime(database, event_log)
            job = self._job("match-roi-durable-recovery")
            runtime.discover_task(
                match_id=job.match_id,
                event_data={
                    "event_key": job.event_key,
                    "code": job.code,
                    "event_type": job.event_type,
                    "minute": job.event_minute,
                    "minute_extra": job.event_minute_extra,
                    "second": job.event_second,
                    "team": "teamA",
                    "person": "Player",
                    "person_id": "1",
                    "score": "1-0",
                    "reason": "",
                    "metadata": {},
                },
                observed_stream_time=job.default_anchor_stream_time,
                observed_source_time=None,
                clip_anchor_stream_time=job.default_anchor_stream_time,
                clip_anchor_source_time=None,
                output_due_stream_time=job.default_anchor_stream_time + 30.0,
                detected_at_unix=job.detected_at_unix,
            )
            runtime.enqueue_vision_task(
                job.event_key,
                artifact_kind="ocr_window",
                search_start_stream_time=0.0,
                search_end_stream_time=130.0,
                clip_before_seconds=30.0,
                clip_after_seconds=30.0,
            )
            runtime.transition_vision_task(
                job.event_key, "locating", artifact_kind="ocr_window"
            )
            cached = runtime.store.save_scoreboard_roi_cache_v2(
                job.match_id, "normal", "1920x1080", self._profile()
            )
            cached_job = replace(job, scoreboard_profile=dict(cached.profile))

            def failed_rediscovery(_candidate: VisionJob):
                kind = (
                    "clock_profile_mismatch"
                    if failed_rediscovery.calls == 0
                    else "scoreboard_missing"
                )
                failed_rediscovery.calls += 1
                raise VisualLocationFailed(kind, "cannot locate clock", {})

            failed_rediscovery.calls = 0
            try:
                with self.assertRaises(VisualLocationFailed):
                    _locate_ocr_window_with_cached_profile_recovery(
                        cached_job, runtime, cached, failed_rediscovery
                    )
                stored = runtime.store.get_vision_task(
                    job.event_key, artifact_kind="ocr_window"
                )
                self.assertEqual(stored.locate_attempt_count, 1)
                self.assertEqual(
                    stored.window_metadata["progressive_scan"][
                        "roi_rediscovery_attempt_count"
                    ],
                    1,
                )
                self.assertEqual(
                    stored.window_metadata["progressive_scan"][
                        "roi_rediscovery_status"
                    ],
                    "failed",
                )
                upgraded = runtime.store.merge_vision_task_window_metadata(
                    job.event_key,
                    {
                        "progressive_scan": {
                            "target_revision": 1,
                            "scan_cursor_stream_time": 125.0,
                        }
                    },
                    artifact_kind="ocr_window",
                    expected_statuses=("locating",),
                    expected_progressive_target_revision=0,
                )
                self.assertIsNotNone(upgraded)
                stale_update = runtime.store.merge_vision_task_window_metadata(
                    job.event_key,
                    {"progressive_scan": {"roi_rediscovery_status": "running"}},
                    artifact_kind="ocr_window",
                    expected_statuses=("locating",),
                    expected_progressive_target_revision=0,
                )
                self.assertIsNone(stale_update)
                stored = runtime.store.get_vision_task(
                    job.event_key, artifact_kind="ocr_window"
                )
                self.assertEqual(
                    stored.window_metadata["progressive_scan"]["target_revision"],
                    1,
                )
                self.assertEqual(
                    stored.window_metadata["progressive_scan"][
                        "scan_cursor_stream_time"
                    ],
                    125.0,
                )
                self.assertEqual(
                    stored.window_metadata["progressive_scan"][
                        "roi_rediscovery_status"
                    ],
                    "failed",
                )
            finally:
                runtime.close()

            reopened = PipelineRuntime(database, event_log)
            calls = []

            def mismatch_again(candidate: VisionJob):
                calls.append(candidate.scoreboard_profile)
                raise VisualLocationFailed(
                    "clock_profile_mismatch", "stale cached job retried", {}
                )

            try:
                with self.assertRaises(VisualLocationFailed):
                    _locate_ocr_window_with_cached_profile_recovery(
                        cached_job, reopened, cached, mismatch_again
                    )
                self.assertEqual(len(calls), 1)
                stored = reopened.store.get_vision_task(
                    job.event_key, artifact_kind="ocr_window"
                )
                self.assertEqual(stored.locate_attempt_count, 1)
            finally:
                reopened.close()


if __name__ == "__main__":
    unittest.main()
