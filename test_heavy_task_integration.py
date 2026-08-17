from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from event_driven_pipeline import encode_event_job, recovered_event_job
from heavy_task_coordinator import HeavyTaskCoordinator
from pipeline_runtime import PipelineRuntime


def _event_data() -> dict:
    return {
        "event_key": "match-1:G:goal-1",
        "code": "G",
        "event_type": "goal",
        "minute": "10",
        "minute_extra": "0",
        "team": "teamA",
        "person": "Player",
        "person_id": "1",
        "score": "1-0",
        "reason": "",
        "metadata": {},
    }


class HeavyTaskIntegrationTests(unittest.TestCase):
    def _runtime_job(self, root: Path):
        runtime = PipelineRuntime(root / "state.sqlite3", root / "events.jsonl")
        runtime.discover_task(
            match_id="match-1",
            event_data=_event_data(),
            observed_stream_time=20.0,
            observed_source_time=None,
            clip_anchor_stream_time=20.0,
            clip_anchor_source_time=None,
            output_due_stream_time=20.0,
            detected_at_unix=1000.0,
            deadline_at_unix=2000.0,
        )
        return runtime, recovered_event_job(runtime.store.get("match-1:G:goal-1"))

    def _options(self, coordinator):
        return {
            "before": 5.0,
            "after": 5.0,
            "width": 384,
            "fps": 6.0,
            "colors": 160,
            "size_reference_bytes": 10_000_000,
            "allow_degraded": True,
            "heavy_task_coordinator": coordinator,
        }

    def test_gif_encoding_holds_and_releases_one_heavy_slot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._runtime_job(root)
            media = root / "segment.ts"
            media.write_bytes(b"video")
            coordinator = HeavyTaskCoordinator(
                root / "coordinator.sqlite3",
                max_heavy_tasks=1,
                max_vision_tasks=1,
            )
            encoded = {
                "output": str(root / "goal.gif"),
                "bytes": 123,
                "duration_sec": 10.0,
                "encode_seconds": 0.1,
            }
            try:
                with patch("event_driven_pipeline.encode_gif", return_value=encoded):
                    self.assertTrue(
                        encode_event_job(
                            job,
                            runtime,
                            "ffmpeg",
                            "ffprobe",
                            lambda: [
                                type(
                                    "Segment",
                                    (),
                                    {"path": media, "start": 0.0, "end": 100.0},
                                )()
                            ],
                            root,
                            **self._options(coordinator),
                        )
                    )
                self.assertEqual(coordinator.snapshot()["active"]["heavy"], 0)
                self.assertEqual(runtime.store.get(job.match_event.event_key).status, "encoded")
            finally:
                coordinator.close()
                runtime.close()

    def test_gif_waiting_for_global_slot_stays_pending(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._runtime_job(root)
            media = root / "segment.ts"
            media.write_bytes(b"video")
            coordinator = HeavyTaskCoordinator(
                root / "coordinator.sqlite3",
                max_heavy_tasks=1,
                max_vision_tasks=1,
            )
            holder = coordinator.acquire("gif", match_id="other", event_key="g0")
            try:
                with patch("event_driven_pipeline.encode_gif") as encode:
                    self.assertFalse(
                        encode_event_job(
                            job,
                            runtime,
                            "ffmpeg",
                            "ffprobe",
                            lambda: [
                                type(
                                    "Segment",
                                    (),
                                    {"path": media, "start": 0.0, "end": 100.0},
                                )()
                            ],
                            root,
                            wait_for_heavy_slot=False,
                            **self._options(coordinator),
                        )
                    )
                    encode.assert_not_called()
                self.assertEqual(runtime.store.get(job.match_event.event_key).status, "pending")
                self.assertEqual(coordinator.snapshot()["active"]["heavy"], 1)
            finally:
                holder.release()
                coordinator.close()
                runtime.close()

    def test_blocking_gif_wait_stays_pending_until_slot_is_acquired(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, job = self._runtime_job(root)
            media = root / "segment.ts"
            media.write_bytes(b"video")
            coordinator = HeavyTaskCoordinator(
                root / "coordinator.sqlite3",
                max_heavy_tasks=1,
                max_vision_tasks=1,
                poll_seconds=0.01,
            )
            holder = coordinator.acquire("gif", match_id="other", event_key="g0")
            encoded = {
                "output": str(root / "goal.gif"),
                "bytes": 123,
                "duration_sec": 10.0,
                "encode_seconds": 0.1,
            }
            outcome = []

            def run_encode():
                try:
                    outcome.append(
                        encode_event_job(
                            job,
                            runtime,
                            "ffmpeg",
                            "ffprobe",
                            lambda: [
                                type(
                                    "Segment",
                                    (),
                                    {"path": media, "start": 0.0, "end": 100.0},
                                )()
                            ],
                            root,
                            **self._options(coordinator),
                        )
                    )
                except BaseException as exc:
                    outcome.append(exc)

            thread = threading.Thread(target=run_encode)
            with patch("event_driven_pipeline.encode_gif", return_value=encoded):
                thread.start()
                try:
                    deadline = time.time() + 2.0
                    while coordinator.snapshot()["waiting"]["tasks"] != 1:
                        self.assertLess(time.time(), deadline)
                        time.sleep(0.01)
                    self.assertEqual(job.pending.status, "pending")
                    self.assertEqual(
                        runtime.store.get(job.match_event.event_key).status,
                        "pending",
                    )
                    holder.release()
                    thread.join(timeout=2.0)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(outcome, [True])
                    self.assertEqual(job.pending.status, "encoded")
                finally:
                    holder.release()
                    thread.join(timeout=2.0)
            coordinator.close()
            runtime.close()


if __name__ == "__main__":
    unittest.main()
