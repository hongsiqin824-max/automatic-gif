"""Process supervision and bounded background work for the live pipeline."""

from __future__ import annotations

import heapq
import itertools
import subprocess
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class ProcessExit:
    return_code: int
    restart: bool
    restart_delay_seconds: float
    consecutive_failures: int


class IngestSupervisor:
    """Restart an ingest process with capped progressive backoff."""

    def __init__(
        self,
        command_factory: Callable[[int], list[str]],
        log_factory: Callable[[int], Any],
        *,
        reconnect: bool,
        max_reconnects: int | None,
        backoff_initial: float,
        backoff_max: float,
        stable_run_seconds: float = 30.0,
        popen: Callable[..., subprocess.Popen[Any]] = subprocess.Popen,
    ) -> None:
        self.command_factory = command_factory
        self.log_factory = log_factory
        self.reconnect = reconnect
        self.max_reconnects = max_reconnects
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.stable_run_seconds = stable_run_seconds
        self.popen = popen
        self.generation = -1
        self.restart_count = 0
        self.consecutive_failures = 0
        self.process: subprocess.Popen[Any] | None = None
        self.log_handle: Any = None
        self.started_monotonic = 0.0

    def _restart_delay(self) -> float:
        """Return a short Fibonacci-style delay capped at ``backoff_max``."""
        first = min(self.backoff_initial, self.backoff_max)
        if self.consecutive_failures <= 1 or first >= self.backoff_max:
            return first
        second = min(self.backoff_initial * 1.5, self.backoff_max)
        if self.consecutive_failures == 2 or second >= self.backoff_max:
            return second
        previous, current = first, second
        for _ in range(3, self.consecutive_failures + 1):
            previous, current = current, min(previous + current, self.backoff_max)
            if current >= self.backoff_max:
                break
        return current

    def note_media_progress(self) -> None:
        """Reset failure backoff after FFmpeg closes a non-empty segment."""
        self.consecutive_failures = 0

    def start(self, now_monotonic: float | None = None) -> subprocess.Popen[Any]:
        if self.process is not None and self.process.poll() is None:
            raise RuntimeError("ingest process is already running")
        self.generation += 1
        self.log_handle = self.log_factory(self.generation)
        self.started_monotonic = (
            time.monotonic() if now_monotonic is None else now_monotonic
        )
        self.process = self.popen(
            self.command_factory(self.generation),
            stdout=subprocess.DEVNULL,
            stderr=self.log_handle,
        )
        return self.process

    def observe_exit(self, now_monotonic: float | None = None) -> ProcessExit | None:
        if self.process is None:
            return None
        return_code = self.process.poll()
        if return_code is None:
            return None
        now = time.monotonic() if now_monotonic is None else now_monotonic
        runtime = max(0.0, now - self.started_monotonic)
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None
        self.process = None
        if runtime >= self.stable_run_seconds:
            self.consecutive_failures = 0
        self.consecutive_failures += 1
        # A live RTMP server may close the FLV stream with a clean FFmpeg exit.
        # It is still a dropped live input. Production RTMP ingest uses no retry
        # limit; a finite limit remains available for deterministic tests and
        # explicitly bounded command-line runs.
        should_restart = self.reconnect and (
            self.max_reconnects is None
            or self.restart_count < self.max_reconnects
        )
        delay = 0.0
        if should_restart:
            delay = self._restart_delay()
            self.restart_count += 1
        return ProcessExit(
            return_code=return_code,
            restart=should_restart,
            restart_delay_seconds=delay,
            consecutive_failures=self.consecutive_failures,
        )

    def terminate(self) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()

    def close(self) -> None:
        if self.log_handle is not None:
            self.log_handle.close()
            self.log_handle = None


class BoundedTaskPool:
    """Run bounded keyed jobs, starting the lowest-priority value first."""

    def __init__(self, workers: int, *, prioritized: bool = False) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self.executor = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="gif-encoder"
        )
        self.workers = workers
        self.prioritized = prioritized
        self.futures: dict[str, Future[Any]] = {}
        self._pending: list[
            tuple[int, int, str, Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ] = []
        self._sequence = itertools.count()
        self._active_count = 0
        self._accepting = True
        self._condition = threading.Condition()

    def submit(
        self,
        key: str,
        function: Callable[..., Any],
        *args: Any,
        task_priority: int = 0,
        **kwargs: Any,
    ) -> bool:
        if not self.prioritized:
            if key in self.futures:
                return False
            self.futures[key] = self.executor.submit(function, *args, **kwargs)
            return True
        with self._condition:
            if not self._accepting:
                raise RuntimeError("task pool is shutting down")
            if key in self.futures:
                return False
            self.futures[key] = Future()
            heapq.heappush(
                self._pending,
                (
                    int(task_priority),
                    next(self._sequence),
                    key,
                    function,
                    args,
                    kwargs,
                ),
            )
            self._start_pending_locked()
            return True

    def _start_pending_locked(self) -> None:
        while self._pending and self._active_count < self.workers:
            _priority, _sequence, key, function, args, kwargs = heapq.heappop(
                self._pending
            )
            public_future = self.futures[key]
            if not public_future.set_running_or_notify_cancel():
                continue
            self._active_count += 1
            try:
                worker_future = self.executor.submit(function, *args, **kwargs)
            except BaseException as exc:
                self._active_count -= 1
                public_future.set_exception(exc)
                continue
            worker_future.add_done_callback(
                lambda completed, task_key=key: self._worker_done(
                    task_key, completed
                )
            )

    def _worker_done(self, key: str, worker_future: Future[Any]) -> None:
        with self._condition:
            public_future = self.futures.get(key)
            if public_future is not None:
                try:
                    public_future.set_result(worker_future.result())
                except BaseException as exc:
                    public_future.set_exception(exc)
            self._active_count -= 1
            self._start_pending_locked()
            self._condition.notify_all()

    def collect_done(self) -> list[tuple[str, Any, BaseException | None]]:
        completed: list[tuple[str, Any, BaseException | None]] = []
        with self._condition:
            done = [
                (key, future)
                for key, future in self.futures.items()
                if future.done()
            ]
            for key, _future in done:
                del self.futures[key]
        for key, future in done:
            try:
                completed.append((key, future.result(), None))
            except BaseException as exc:
                completed.append((key, None, exc))
        return completed

    def shutdown(self, *, wait: bool = True) -> None:
        if not self.prioritized:
            self.executor.shutdown(wait=wait)
            return
        with self._condition:
            self._accepting = False
            if wait:
                while self._pending or self._active_count:
                    self._condition.wait()
            else:
                for pending in self._pending:
                    key = pending[2]
                    self.futures[key].cancel()
                self._pending.clear()
        self.executor.shutdown(wait=wait)
