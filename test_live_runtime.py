import io
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
    def test_supervisor_restarts_with_capped_exponential_backoff(self):
        processes = [FakeProcess(1), FakeProcess(1), FakeProcess(1)]
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
            max_reconnects=2,
            backoff_initial=1,
            backoff_max=2,
            popen=popen,
        )
        supervisor.start(0)
        first = supervisor.observe_exit(1)
        self.assertTrue(first.restart)
        self.assertEqual(first.restart_delay_seconds, 1)
        supervisor.start(2)
        second = supervisor.observe_exit(3)
        self.assertTrue(second.restart)
        self.assertEqual(second.restart_delay_seconds, 2)
        supervisor.start(5)
        third = supervisor.observe_exit(6)
        self.assertFalse(third.restart)
        self.assertEqual([call.args[0][-1] for call in popen.call_args_list], ["0", "1", "2"])

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
