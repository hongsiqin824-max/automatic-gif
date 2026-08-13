"""Process supervision and bounded background work for the live pipeline."""

from __future__ import annotations

import subprocess
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
    """Restart an ingest process with capped exponential backoff."""

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
            exponent = min(self.consecutive_failures - 1, 60)
            delay = min(self.backoff_initial * (2**exponent), self.backoff_max)
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
    """Run at most ``workers`` jobs without submitting the same key twice."""

    def __init__(self, workers: int) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self.executor = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="gif-encoder"
        )
        self.futures: dict[str, Future[Any]] = {}

    def submit(
        self,
        key: str,
        function: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> bool:
        if key in self.futures:
            return False
        self.futures[key] = self.executor.submit(function, *args, **kwargs)
        return True

    def collect_done(self) -> list[tuple[str, Any, BaseException | None]]:
        completed: list[tuple[str, Any, BaseException | None]] = []
        for key, future in list(self.futures.items()):
            if not future.done():
                continue
            del self.futures[key]
            try:
                completed.append((key, future.result(), None))
            except BaseException as exc:
                completed.append((key, None, exc))
        return completed

    def shutdown(self, *, wait: bool = True) -> None:
        self.executor.shutdown(wait=wait)
