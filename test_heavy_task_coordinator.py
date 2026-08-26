from __future__ import annotations

import json
import multiprocessing
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from heavy_task_coordinator import (
    DEFAULT_MAX_CONCURRENT_HEAVY_TASKS,
    DEFAULT_MAX_CONCURRENT_VISION_TASKS,
    HeavyTaskCancelled,
    HeavyTaskCoordinator,
    HeavyTaskCoordinatorError,
    HeavyTaskUnavailable,
    configured_limits,
    run_with_task_slot,
)


def _hold_gif_slot_in_process(database_path, acquired, release):
    coordinator = HeavyTaskCoordinator(
        Path(database_path),
        max_heavy_tasks=1,
        max_vision_tasks=1,
    )
    try:
        with coordinator.acquire("gif", match_id="child", event_key="g1"):
            acquired.set()
            release.wait(5.0)
    finally:
        coordinator.close()


def _crash_after_acquiring_gif_slot(database_path, acquired):
    coordinator = HeavyTaskCoordinator(
        Path(database_path),
        max_heavy_tasks=1,
        max_vision_tasks=1,
        lease_seconds=0.2,
        poll_seconds=0.01,
    )
    coordinator.acquire("gif", match_id="crashed", event_key="g1")
    acquired.set()
    time.sleep(0.05)
    os._exit(17)


class HeavyTaskCoordinatorTests(unittest.TestCase):
    def make_coordinator(
        self,
        root: Path,
        *,
        heavy: int = 2,
        vision: int = 1,
        lease_seconds: float = 2.0,
    ) -> HeavyTaskCoordinator:
        return HeavyTaskCoordinator(
            root / "coordinator.sqlite3",
            max_heavy_tasks=heavy,
            max_vision_tasks=vision,
            lease_seconds=lease_seconds,
            poll_seconds=0.01,
        )

    def test_environment_defaults_and_validation(self):
        with patch.dict(
            "os.environ",
            {},
            clear=True,
        ):
            self.assertEqual(
                configured_limits(),
                (
                    DEFAULT_MAX_CONCURRENT_HEAVY_TASKS,
                    DEFAULT_MAX_CONCURRENT_VISION_TASKS,
                ),
            )
        with patch.dict(
            "os.environ",
            {"GIF_MAX_CONCURRENT_HEAVY_TASKS": "0"},
            clear=True,
        ):
            with self.assertRaisesRegex(HeavyTaskCoordinatorError, "positive integer"):
                configured_limits()
        with patch.dict(
            "os.environ",
            {
                "GIF_MAX_CONCURRENT_HEAVY_TASKS": "5",
                "GIF_VISION_WORKERS": "4",
            },
            clear=True,
        ):
            self.assertEqual(configured_limits(), (5, 4))
        with patch.dict(
            "os.environ",
            {
                "GIF_MAX_CONCURRENT_HEAVY_TASKS": "5",
                "GIF_VISION_WORKERS": "4",
                "GIF_MAX_CONCURRENT_VISION_TASKS": "3",
            },
            clear=True,
        ):
            self.assertEqual(configured_limits(), (5, 3))

    def test_limits_apply_across_coordinator_instances(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.make_coordinator(root)
            second = self.make_coordinator(root)
            try:
                gif = first.acquire("gif", match_id="m1", event_key="g1")
                vision = second.acquire("vision", match_id="m2", event_key="v1")
                with self.assertRaises(HeavyTaskUnavailable):
                    first.acquire(
                        "gif", match_id="m3", event_key="g2", wait=False
                    )
                with self.assertRaises(HeavyTaskUnavailable):
                    second.acquire(
                        "vision", match_id="m4", event_key="v2", wait=False
                    )
                snapshot = first.snapshot()
                self.assertEqual(snapshot["active"]["heavy"], 2)
                self.assertEqual(snapshot["active"]["vision"], 1)
                self.assertEqual(
                    {item["match_id"] for item in snapshot["active"]["items"]},
                    {"m1", "m2"},
                )
                vision.release()
                gif.release()
            finally:
                first.close()
                second.close()

    def test_single_slot_compatibility_still_allows_vision(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            coordinator = self.make_coordinator(root, heavy=1, vision=1)
            try:
                with coordinator.acquire(
                    "vision",
                    match_id="m1",
                    event_key="v1",
                    wait=False,
                ):
                    snapshot = coordinator.snapshot()
                    self.assertEqual(snapshot["active"]["heavy"], 1)
                    self.assertEqual(snapshot["active"]["vision"], 1)
            finally:
                coordinator.close()

    def test_default_gif_can_start_while_the_single_vision_slot_is_active(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            coordinator = self.make_coordinator(root, heavy=2, vision=1)
            try:
                with coordinator.acquire("vision", match_id="m1", event_key="v1"):
                    with coordinator.acquire(
                        "gif",
                        match_id="m2",
                        event_key="g1",
                        wait=False,
                    ):
                        snapshot = coordinator.snapshot()
                        self.assertEqual(snapshot["active"]["heavy"], 2)
                        self.assertEqual(snapshot["active"]["vision"], 1)
            finally:
                coordinator.close()

    def test_limits_apply_across_worker_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = multiprocessing.get_context("spawn")
            acquired = context.Event()
            release = context.Event()
            process = context.Process(
                target=_hold_gif_slot_in_process,
                args=(str(root / "coordinator.sqlite3"), acquired, release),
            )
            process.start()
            parent = self.make_coordinator(root, heavy=1, vision=1)
            try:
                self.assertTrue(acquired.wait(5.0))
                with self.assertRaises(HeavyTaskUnavailable):
                    parent.acquire(
                        "gif",
                        match_id="parent",
                        event_key="g2",
                        wait=False,
                    )
            finally:
                release.set()
                process.join(timeout=5.0)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=2.0)
                parent.close()
            self.assertEqual(process.exitcode, 0)

    def test_worker_process_crash_releases_slot_after_lease_expiry(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database_path = root / "coordinator.sqlite3"
            context = multiprocessing.get_context("spawn")
            acquired = context.Event()
            process = context.Process(
                target=_crash_after_acquiring_gif_slot,
                args=(str(database_path), acquired),
            )
            process.start()
            self.assertTrue(acquired.wait(5.0))
            process.join(timeout=5.0)
            self.assertEqual(process.exitcode, 17)

            survivor = HeavyTaskCoordinator(
                database_path,
                max_heavy_tasks=1,
                max_vision_tasks=1,
                lease_seconds=0.2,
                poll_seconds=0.01,
            )
            try:
                time.sleep(0.25)
                replacement = survivor.acquire(
                    "gif",
                    match_id="survivor",
                    event_key="g2",
                    wait=False,
                )
                replacement.release()
                self.assertEqual(survivor.snapshot()["active"]["tasks"], 0)
            finally:
                survivor.close()

    def test_existing_instance_uses_latest_idle_global_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = self.make_coordinator(root, heavy=2, vision=1)
            second = self.make_coordinator(root, heavy=1, vision=1)
            first_lease = first.acquire("gif", match_id="m1", event_key="g1")
            try:
                with self.assertRaises(HeavyTaskUnavailable):
                    first.acquire(
                        "gif",
                        match_id="m2",
                        event_key="g2",
                        wait=False,
                    )
                snapshot = second.snapshot()
                self.assertEqual(snapshot["limits"], {"heavy": 1, "vision": 1})
                self.assertEqual(snapshot["active"]["heavy"], 1)
            finally:
                first_lease.release()
                first.close()
                second.close()

    def test_waiting_request_acquires_after_release_without_being_dropped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            waiter = self.make_coordinator(root, heavy=1, vision=1)
            acquired = threading.Event()
            finished = threading.Event()
            errors = []
            first_lease = holder.acquire("gif", match_id="m1", event_key="g1")

            def wait_for_slot():
                try:
                    with waiter.acquire("gif", match_id="m2", event_key="g2"):
                        acquired.set()
                except BaseException as exc:
                    errors.append(exc)
                finally:
                    finished.set()

            thread = threading.Thread(target=wait_for_slot)
            thread.start()
            try:
                deadline = time.time() + 2.0
                while waiter.snapshot()["waiting"]["tasks"] != 1:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                self.assertFalse(acquired.is_set())
                first_lease.release()
                self.assertTrue(acquired.wait(2.0))
                self.assertTrue(finished.wait(2.0))
                self.assertEqual(errors, [])
                self.assertEqual(waiter.snapshot()["waiting"]["tasks"], 0)
            finally:
                first_lease.release()
                thread.join(timeout=2.0)
                holder.close()
                waiter.close()

    def test_expired_waiting_request_is_requeued_then_acquires(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            waiter = self.make_coordinator(root, heavy=1, vision=1)
            acquired = threading.Event()
            errors: list[BaseException] = []
            holder_lease = holder.acquire("gif", match_id="holder", event_key="g0")

            def wait_for_slot():
                try:
                    with waiter.acquire("gif", match_id="m1", event_key="g1"):
                        acquired.set()
                except BaseException as exc:
                    errors.append(exc)

            thread = threading.Thread(target=wait_for_slot)
            thread.start()
            try:
                deadline = time.monotonic() + 2.0
                original_request = None
                while original_request is None:
                    with sqlite3.connect(waiter.database_path) as connection:
                        original_request = connection.execute(
                            "SELECT request_id, requested_at_unix "
                            "FROM task_slot_requests WHERE event_key = ?",
                            ("g1",),
                        ).fetchone()
                    self.assertLess(time.monotonic(), deadline)

                with sqlite3.connect(waiter.database_path) as connection:
                    connection.execute(
                        "UPDATE task_slot_requests SET expires_at_unix = 0 "
                        "WHERE request_id = ?",
                        (original_request[0],),
                    )
                    waiter._purge_expired(connection, time.time())

                restored_request = None
                while restored_request is None:
                    with sqlite3.connect(waiter.database_path) as connection:
                        restored_request = connection.execute(
                            "SELECT request_id, requested_at_unix "
                            "FROM task_slot_requests WHERE event_key = ?",
                            ("g1",),
                        ).fetchone()
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)

                self.assertEqual(restored_request[0], original_request[0])
                self.assertEqual(restored_request[1], original_request[1])
                holder_lease.release()
                self.assertTrue(acquired.wait(2.0))
                thread.join(timeout=2.0)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(waiter.snapshot()["waiting"]["tasks"], 0)
            finally:
                holder_lease.release()
                thread.join(timeout=2.0)
                holder.close()
                waiter.close()

    def test_cancelled_requeued_request_is_removed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            waiter = self.make_coordinator(root, heavy=1, vision=1)
            cancelled = threading.Event()
            outcome: list[BaseException] = []
            holder_lease = holder.acquire("gif", match_id="holder", event_key="g0")

            def wait_for_slot():
                try:
                    waiter.acquire(
                        "gif",
                        match_id="m1",
                        event_key="g1",
                        cancel_event=cancelled,
                    )
                except BaseException as exc:
                    outcome.append(exc)

            thread = threading.Thread(target=wait_for_slot)
            thread.start()
            try:
                deadline = time.monotonic() + 2.0
                request_id = None
                while request_id is None:
                    with sqlite3.connect(waiter.database_path) as connection:
                        row = connection.execute(
                            "SELECT request_id FROM task_slot_requests "
                            "WHERE event_key = ?",
                            ("g1",),
                        ).fetchone()
                    request_id = row[0] if row is not None else None
                    self.assertLess(time.monotonic(), deadline)

                with sqlite3.connect(waiter.database_path) as connection:
                    connection.execute(
                        "UPDATE task_slot_requests SET expires_at_unix = 0 "
                        "WHERE request_id = ?",
                        (request_id,),
                    )
                    waiter._purge_expired(connection, time.time())

                restored = False
                while not restored:
                    with sqlite3.connect(waiter.database_path) as connection:
                        restored = connection.execute(
                            "SELECT 1 FROM task_slot_requests WHERE request_id = ?",
                            (request_id,),
                        ).fetchone() is not None
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)

                cancelled.set()
                thread.join(timeout=2.0)
                self.assertFalse(thread.is_alive())
                self.assertEqual(len(outcome), 1)
                self.assertIsInstance(outcome[0], HeavyTaskCancelled)
                snapshot = waiter.snapshot()
                self.assertEqual(snapshot["waiting"]["tasks"], 0)
                self.assertEqual(snapshot["active"]["tasks"], 1)
            finally:
                cancelled.set()
                holder_lease.release()
                thread.join(timeout=2.0)
                holder.close()
                waiter.close()

    def test_gif_waiters_have_priority_over_older_vision_waiters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            vision_coordinator = self.make_coordinator(root, heavy=1, vision=1)
            gif_coordinator = self.make_coordinator(root, heavy=1, vision=1)
            holder_lease = holder.acquire("gif", match_id="holder", event_key="g0")
            order: list[str] = []
            gif_acquired = threading.Event()
            gif_release = threading.Event()
            errors: list[BaseException] = []

            def run_vision():
                try:
                    with vision_coordinator.acquire(
                        "vision", match_id="m1", event_key="vision-1"
                    ):
                        order.append("vision")
                except BaseException as exc:
                    errors.append(exc)

            def run_gif():
                try:
                    with gif_coordinator.acquire("gif", match_id="m2", event_key="gif-1"):
                        order.append("gif")
                        gif_acquired.set()
                        gif_release.wait(2.0)
                except BaseException as exc:
                    errors.append(exc)

            vision_thread = threading.Thread(target=run_vision)
            gif_thread = threading.Thread(target=run_gif)
            vision_thread.start()
            try:
                deadline = time.time() + 2.0
                while vision_coordinator.snapshot()["waiting"]["tasks"] != 1:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                gif_thread.start()
                while gif_coordinator.snapshot()["waiting"]["tasks"] != 2:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                holder_lease.release()
                self.assertTrue(gif_acquired.wait(2.0))
                self.assertEqual(order, ["gif"])
                gif_release.set()
                gif_thread.join(timeout=2.0)
                vision_thread.join(timeout=2.0)
                self.assertEqual(order, ["gif", "vision"])
                self.assertEqual(errors, [])
            finally:
                gif_release.set()
                holder_lease.release()
                vision_thread.join(timeout=2.0)
                gif_thread.join(timeout=2.0)
                holder.close()
                vision_coordinator.close()
                gif_coordinator.close()

    def test_ocr_waiter_has_priority_over_older_tdeed_waiter(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            tdeed = self.make_coordinator(root, heavy=1, vision=1)
            ocr = self.make_coordinator(root, heavy=1, vision=1)
            holder_lease = holder.acquire("gif", match_id="holder", event_key="g0")
            order = []
            errors = []

            def wait(coordinator, task_kind, name):
                try:
                    with coordinator.acquire(
                        task_kind, match_id="m", event_key=name
                    ):
                        order.append(name)
                except BaseException as exc:
                    errors.append(exc)

            tdeed_thread = threading.Thread(
                target=wait,
                args=(tdeed, "vision_tdeed", "tdeed"),
            )
            ocr_thread = threading.Thread(
                target=wait,
                args=(ocr, "vision_ocr", "ocr"),
            )
            tdeed_thread.start()
            try:
                deadline = time.time() + 2.0
                while tdeed.snapshot()["waiting"]["tasks"] != 1:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                ocr_thread.start()
                while ocr.snapshot()["waiting"]["tasks"] != 2:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                holder_lease.release()
                tdeed_thread.join(timeout=2.0)
                ocr_thread.join(timeout=2.0)
                self.assertEqual(order, ["ocr", "tdeed"])
                self.assertEqual(errors, [])
            finally:
                holder_lease.release()
                if not ocr_thread.ident:
                    ocr_thread.start()
                tdeed_thread.join(timeout=2.0)
                ocr_thread.join(timeout=2.0)
                holder.close()
                tdeed.close()
                ocr.close()

    def test_task_runner_reports_acquired_before_executing(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator = self.make_coordinator(Path(directory), heavy=1, vision=1)
            phases = []
            try:
                result = run_with_task_slot(
                    coordinator,
                    "vision_ocr",
                    "m1",
                    "v1",
                    lambda: "done",
                    on_state_change=lambda phase, timestamp: phases.append(
                        (phase, timestamp)
                    ),
                )
                self.assertEqual(result, "done")
                self.assertEqual(
                    [phase for phase, _ in phases],
                    ["acquired", "executing"],
                )
                self.assertLessEqual(phases[0][1], phases[1][1])
            finally:
                coordinator.close()

    def test_same_kind_waiters_keep_fifo_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            first = self.make_coordinator(root, heavy=1, vision=1)
            second = self.make_coordinator(root, heavy=1, vision=1)
            holder_lease = holder.acquire("gif", match_id="holder", event_key="g0")
            order: list[str] = []
            errors: list[BaseException] = []

            def wait(coordinator, event_key):
                try:
                    with coordinator.acquire("gif", match_id="m", event_key=event_key):
                        order.append(event_key)
                except BaseException as exc:
                    errors.append(exc)

            first_thread = threading.Thread(target=wait, args=(first, "g1"))
            second_thread = threading.Thread(target=wait, args=(second, "g2"))
            first_thread.start()
            try:
                deadline = time.monotonic() + 2.0
                while first.snapshot()["waiting"]["tasks"] != 1:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
                second_thread.start()
                while second.snapshot()["waiting"]["tasks"] != 2:
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.01)
            finally:
                if not second_thread.is_alive():
                    second_thread.start()
            try:
                holder_lease.release()
                first_thread.join(timeout=2.0)
                second_thread.join(timeout=2.0)
                self.assertEqual(order, ["g1", "g2"])
                self.assertEqual(errors, [])
            finally:
                holder_lease.release()
                first_thread.join(timeout=2.0)
                second_thread.join(timeout=2.0)
                holder.close()
                first.close()
                second.close()

    def test_context_manager_releases_after_task_exception(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator = self.make_coordinator(Path(directory), heavy=1, vision=1)
            try:
                with self.assertRaisesRegex(RuntimeError, "task failed"):
                    with coordinator.acquire(
                        "vision", match_id="m1", event_key="v1"
                    ):
                        raise RuntimeError("task failed")
                snapshot = coordinator.snapshot()
                self.assertEqual(snapshot["active"]["tasks"], 0)
                self.assertEqual(snapshot["waiting"]["tasks"], 0)
            finally:
                coordinator.close()

    def test_task_runner_releases_when_callable_raises(self):
        with tempfile.TemporaryDirectory() as directory:
            coordinator = self.make_coordinator(Path(directory), heavy=1, vision=1)

            def fail(value):
                raise RuntimeError(value)

            try:
                with self.assertRaisesRegex(RuntimeError, "failed inside worker"):
                    run_with_task_slot(
                        coordinator,
                        "vision",
                        "m1",
                        "v1",
                        fail,
                        "failed inside worker",
                    )
                self.assertEqual(coordinator.snapshot()["active"]["tasks"], 0)
            finally:
                coordinator.close()

    def test_expired_lease_is_reclaimed_after_owner_disappears(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crashed = self.make_coordinator(
                root, heavy=1, vision=1, lease_seconds=0.15
            )
            survivor = self.make_coordinator(
                root, heavy=1, vision=1, lease_seconds=0.15
            )
            abandoned = crashed.acquire("gif", match_id="m1", event_key="g1")
            abandoned._stop_heartbeat.set()
            abandoned._heartbeat.join(timeout=1.0)
            try:
                time.sleep(0.2)
                replacement = survivor.acquire(
                    "gif", match_id="m2", event_key="g2", wait=False
                )
                replacement.release()
                self.assertEqual(survivor.snapshot()["active"]["tasks"], 0)
            finally:
                abandoned.release()
                crashed.close()
                survivor.close()

    def test_cancelled_wait_is_removed_without_consuming_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            holder = self.make_coordinator(root, heavy=1, vision=1)
            waiter = self.make_coordinator(root, heavy=1, vision=1)
            lease = holder.acquire("gif", match_id="m1", event_key="g1")
            cancelled = threading.Event()
            outcome = []

            def wait_for_slot():
                try:
                    waiter.acquire(
                        "gif",
                        match_id="m2",
                        event_key="g2",
                        cancel_event=cancelled,
                    )
                except BaseException as exc:
                    outcome.append(exc)

            thread = threading.Thread(target=wait_for_slot)
            thread.start()
            try:
                deadline = time.time() + 2.0
                while waiter.snapshot()["waiting"]["tasks"] != 1:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                cancelled.set()
                thread.join(timeout=2.0)
                self.assertEqual(len(outcome), 1)
                self.assertIsInstance(outcome[0], HeavyTaskCancelled)
                snapshot = waiter.snapshot()
                self.assertEqual(snapshot["waiting"]["tasks"], 0)
                self.assertEqual(snapshot["active"]["tasks"], 1)
            finally:
                lease.release()
                holder.close()
                waiter.close()

    def test_snapshot_file_is_dashboard_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            coordinator = self.make_coordinator(root)
            try:
                with coordinator.acquire("vision", match_id="m1", event_key="v1"):
                    payload = json.loads(
                        coordinator.snapshot_path.read_text(encoding="utf-8")
                    )
                    self.assertEqual(payload["limits"], {"heavy": 2, "vision": 1})
                    self.assertEqual(payload["active"]["tasks"], 1)
                    self.assertEqual(payload["active"]["items"][0]["match_id"], "m1")
            finally:
                coordinator.close()

    def test_snapshot_file_is_refreshed_when_active_lease_renews(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            coordinator = self.make_coordinator(
                root,
                lease_seconds=0.3,
            )
            try:
                with coordinator.acquire("gif", match_id="m1", event_key="g1"):
                    initial = json.loads(
                        coordinator.snapshot_path.read_text(encoding="utf-8")
                    )
                    initial_heartbeat = initial["active"]["items"][0][
                        "heartbeat_at_unix"
                    ]
                    deadline = time.time() + 2.0
                    while True:
                        refreshed = json.loads(
                            coordinator.snapshot_path.read_text(encoding="utf-8")
                        )
                        refreshed_item = refreshed["active"]["items"][0]
                        if refreshed_item["heartbeat_at_unix"] > initial_heartbeat:
                            break
                        self.assertLess(time.time(), deadline)
                        time.sleep(0.02)
                    self.assertGreater(
                        refreshed_item["expires_at_unix"],
                        initial["active"]["items"][0]["expires_at_unix"],
                    )
            finally:
                coordinator.close()


if __name__ == "__main__":
    unittest.main()
