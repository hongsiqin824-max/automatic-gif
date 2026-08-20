import io
import threading
import time
import unittest
from unittest.mock import Mock

from live_runtime import BoundedTaskPool, IngestSupervisor


class FakeProcess:
    def __init__(self, return_code=None):
        self.return_code = return_code
        self.terminated = False

    def poll(self):
        return self.return_code

    def terminate(self):
        self.terminated = True
        self.return_code = -15


class LiveRuntimeTests(unittest.TestCase):
    def test_pool_starts_higher_priority_job_before_queued_lower_priority_job(self):
        pool = BoundedTaskPool(1, prioritized=True)
        release = threading.Event()
        started = threading.Event()
        order = []

        def hold():
            started.set()
            release.wait(2.0)
            order.append("holder")
            return "holder"

        def run(name):
            order.append(name)
            return name

        try:
            self.assertTrue(pool.submit("holder", hold, task_priority=0))
            self.assertTrue(started.wait(2.0))
            self.assertTrue(pool.submit("tdeed", run, "tdeed", task_priority=2))
            self.assertTrue(pool.submit("ocr", run, "ocr", task_priority=1))
            release.set()
            deadline = time.time() + 2.0
            while len(order) < 3:
                self.assertLess(time.time(), deadline)
                time.sleep(0.01)
            self.assertEqual(order, ["holder", "ocr", "tdeed"])
        finally:
            release.set()
            pool.shutdown(wait=True)

    def test_non_waiting_shutdown_cancels_jobs_that_have_not_started(self):
        pool = BoundedTaskPool(1, prioritized=True)
        release = threading.Event()
        started = threading.Event()

        def hold():
            started.set()
            release.wait(2.0)

        self.assertTrue(pool.submit("holder", hold))
        self.assertTrue(started.wait(2.0))
        self.assertTrue(pool.submit("pending", lambda: None))
        pool.shutdown(wait=False)
        try:
            self.assertTrue(pool.futures["pending"].cancelled())
        finally:
            release.set()

    def test_supervisor_restarts_with_two_three_five_backoff(self):
        processes = [FakeProcess(1), FakeProcess(1), FakeProcess(1), FakeProcess(1)]
        popen = Mock(side_effect=processes)
        logs = []

        def log_factory(generation):
            handle = io.StringIO()
            logs.append((generation, handle))
            return handle

        supervisor = IngestSupervisor(
            lambda generation: ["ffmpeg", str(generation)],
            log_factory,
            reconnect=True,
            max_reconnects=3,
            backoff_initial=2,
            backoff_max=5,
            popen=popen,
        )
        supervisor.start(0)
        first = supervisor.observe_exit(1)
        self.assertTrue(first.restart)
        self.assertEqual(first.restart_delay_seconds, 2)
        supervisor.start(3)
        second = supervisor.observe_exit(4)
        self.assertTrue(second.restart)
        self.assertEqual(second.restart_delay_seconds, 3)
        supervisor.start(7)
        third = supervisor.observe_exit(8)
        self.assertTrue(third.restart)
        self.assertEqual(third.restart_delay_seconds, 5)
        supervisor.start(13)
        fourth = supervisor.observe_exit(14)
        self.assertFalse(fourth.restart)
        self.assertEqual(
            [call.args[0][-1] for call in popen.call_args_list],
            ["0", "1", "2", "3"],
        )

    def test_successful_media_progress_resets_reconnect_backoff(self):
        supervisor = IngestSupervisor(
            lambda generation: ["ffmpeg", str(generation)],
            lambda generation: io.StringIO(),
            reconnect=True,
            max_reconnects=None,
            backoff_initial=2,
            backoff_max=5,
            popen=Mock(side_effect=[FakeProcess(1), FakeProcess(1), FakeProcess(1)]),
        )

        supervisor.start(0)
        self.assertEqual(supervisor.observe_exit(1).restart_delay_seconds, 2)
        supervisor.start(3)
        self.assertEqual(supervisor.observe_exit(4).restart_delay_seconds, 3)

        supervisor.start(7)
        supervisor.note_media_progress()
        recovered_exit = supervisor.observe_exit(8)

        self.assertEqual(recovered_exit.restart_delay_seconds, 2)
        self.assertEqual(recovered_exit.consecutive_failures, 1)
        self.assertEqual(supervisor.restart_count, 3)

    def test_clean_rtmp_exit_is_restarted(self):
        supervisor = IngestSupervisor(
            lambda generation: ["ffmpeg"],
            lambda generation: io.StringIO(),
            reconnect=True,
            max_reconnects=3,
            backoff_initial=1,
            backoff_max=4,
            popen=Mock(return_value=FakeProcess(0)),
        )
        supervisor.start(0)
        self.assertTrue(supervisor.observe_exit(1).restart)

    def test_unlimited_rtmp_reconnects_never_exhaust_a_retry_budget(self):
        processes = [FakeProcess(1) for _ in range(26)]
        supervisor = IngestSupervisor(
            lambda generation: ["ffmpeg", str(generation)],
            lambda generation: io.StringIO(),
            reconnect=True,
            max_reconnects=None,
            backoff_initial=1,
            backoff_max=30,
            popen=Mock(side_effect=processes),
        )

        for generation in range(25):
            supervisor.start(generation * 2)
            result = supervisor.observe_exit(generation * 2 + 1)
            self.assertTrue(result.restart)

        self.assertEqual(supervisor.restart_count, 25)
        self.assertEqual(result.restart_delay_seconds, 30)

        supervisor.consecutive_failures = 10_000
        supervisor.start(100)
        result = supervisor.observe_exit(101)
        self.assertTrue(result.restart)
        self.assertEqual(result.restart_delay_seconds, 30)

    def test_clean_file_exit_is_not_restarted(self):
        supervisor = IngestSupervisor(
            lambda generation: ["ffmpeg"],
            lambda generation: io.StringIO(),
            reconnect=False,
            max_reconnects=3,
            backoff_initial=1,
            backoff_max=4,
            popen=Mock(return_value=FakeProcess(0)),
        )
        supervisor.start(0)
        self.assertFalse(supervisor.observe_exit(1).restart)

    def test_pool_rejects_duplicate_inflight_key_and_collects_result(self):
        pool = BoundedTaskPool(1)
        self.assertTrue(pool.submit("event-1", lambda: 42))
        self.assertFalse(pool.submit("event-1", lambda: 99))
        pool.shutdown(wait=True)
        self.assertEqual(pool.collect_done(), [("event-1", 42, None)])


if __name__ == "__main__":
    unittest.main()
