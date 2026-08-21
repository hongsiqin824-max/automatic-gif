"""Cross-process concurrency limits for CPU/GPU-heavy pipeline work."""

from __future__ import annotations

import atexit
import json
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable


# Keep one shared heavy slot available for the default GIF path while allowing
# four OCR jobs to run concurrently on the deployment machine.  Both limits
# remain overridable through GIF_MAX_CONCURRENT_* for smaller servers.
DEFAULT_MAX_CONCURRENT_HEAVY_TASKS = 5
DEFAULT_MAX_CONCURRENT_VISION_TASKS = 4
DEFAULT_RESERVED_GIF_SLOTS = 1
DEFAULT_LEASE_SECONDS = 15.0
DEFAULT_POLL_SECONDS = 0.1
DEFAULT_DATABASE_PATH = (
    Path(__file__).resolve().parent
    / "output_gifs"
    / "dashboard"
    / "heavy_task_coordinator.sqlite3"
)


class HeavyTaskCoordinatorError(RuntimeError):
    """The shared task coordinator cannot safely serve a request."""


class HeavyTaskUnavailable(HeavyTaskCoordinatorError):
    """No matching slot is currently available for a non-blocking request."""


class HeavyTaskCancelled(HeavyTaskCoordinatorError):
    """A waiting request was cancelled before it acquired a slot."""


def _positive_environment_integer(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or not raw_value.strip():
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise HeavyTaskCoordinatorError(
            f"{name} must be a positive integer, got {raw_value!r}"
        ) from exc
    if value < 1:
        raise HeavyTaskCoordinatorError(
            f"{name} must be a positive integer, got {raw_value!r}"
        )
    return value


def configured_limits() -> tuple[int, int]:
    """Return heavy/vision limits from the process environment."""
    return (
        _positive_environment_integer(
            "GIF_MAX_CONCURRENT_HEAVY_TASKS",
            DEFAULT_MAX_CONCURRENT_HEAVY_TASKS,
        ),
        _positive_environment_integer(
            "GIF_MAX_CONCURRENT_VISION_TASKS",
            DEFAULT_MAX_CONCURRENT_VISION_TASKS,
        ),
    )


class HeavyTaskLease:
    """A renewable cross-process slot lease released by a context manager."""

    def __init__(
        self,
        coordinator: "HeavyTaskCoordinator",
        lease_id: str,
        *,
        task_kind: str,
        match_id: str,
        event_key: str,
    ) -> None:
        self.coordinator = coordinator
        self.lease_id = lease_id
        self.task_kind = task_kind
        self.match_id = match_id
        self.event_key = event_key
        self._stop_heartbeat = threading.Event()
        self._released = False
        self._heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            name=f"heavy-slot-{lease_id[:8]}",
            daemon=True,
        )
        self._heartbeat.start()

    def _heartbeat_loop(self) -> None:
        interval = min(5.0, max(0.05, self.coordinator.lease_seconds / 3.0))
        while not self._stop_heartbeat.wait(interval):
            try:
                if not self.coordinator._renew(self.lease_id):
                    return
            except HeavyTaskCoordinatorError:
                # A transient SQLite failure is retried until the lease expires.
                continue

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        self._stop_heartbeat.set()
        if self._heartbeat is not threading.current_thread():
            self._heartbeat.join(timeout=max(0.2, self.coordinator.poll_seconds * 2.0))
        self.coordinator._release(self.lease_id)

    def __enter__(self) -> "HeavyTaskLease":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> bool:
        self.release()
        return False


class HeavyTaskCoordinator:
    """Coordinate heavy work across every Worker using one SQLite database."""

    def __init__(
        self,
        database_path: Path,
        *,
        max_heavy_tasks: int,
        max_vision_tasks: int,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        snapshot_path: Path | None = None,
    ) -> None:
        if max_heavy_tasks < 1 or max_vision_tasks < 1:
            raise HeavyTaskCoordinatorError("task concurrency limits must be positive")
        if max_vision_tasks > max_heavy_tasks:
            raise HeavyTaskCoordinatorError(
                "vision task limit cannot exceed the shared heavy task limit"
            )
        if lease_seconds <= 0 or poll_seconds <= 0:
            raise HeavyTaskCoordinatorError("lease and polling durations must be positive")
        self.database_path = Path(database_path).resolve()
        self.snapshot_path = (
            Path(snapshot_path).resolve()
            if snapshot_path is not None
            else self.database_path.with_suffix(".json")
        )
        self.max_heavy_tasks = int(max_heavy_tasks)
        self.max_vision_tasks = int(max_vision_tasks)
        self.lease_seconds = float(lease_seconds)
        self.poll_seconds = float(poll_seconds)
        self.owner_id = f"{os.getpid()}:{uuid.uuid4().hex}"
        self._closed = False
        self._owned_lease_ids: set[str] = set()
        self._owned_request_ids: set[str] = set()
        self._local_lock = threading.Lock()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()
        atexit.register(self._close_at_exit)

    @classmethod
    def from_environment(
        cls,
        database_path: Path | None = None,
        *,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
    ) -> "HeavyTaskCoordinator":
        max_heavy_tasks, max_vision_tasks = configured_limits()
        configured_path = os.environ.get("GIF_HEAVY_TASK_COORDINATOR_DB", "").strip()
        path = (
            Path(configured_path).expanduser()
            if configured_path
            else database_path or DEFAULT_DATABASE_PATH
        )
        return cls(
            path,
            max_heavy_tasks=max_heavy_tasks,
            max_vision_tasks=max_vision_tasks,
            lease_seconds=lease_seconds,
            poll_seconds=poll_seconds,
        )

    def _connect(self) -> sqlite3.Connection:
        try:
            connection = sqlite3.connect(
                self.database_path,
                timeout=10.0,
                isolation_level=None,
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 10000")
            return connection
        except sqlite3.Error as exc:
            raise HeavyTaskCoordinatorError(
                f"cannot open heavy task coordinator {self.database_path}: {exc}"
            ) from exc

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = FULL;
                CREATE TABLE IF NOT EXISTS coordinator_config (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    max_heavy_tasks INTEGER NOT NULL,
                    max_vision_tasks INTEGER NOT NULL,
                    updated_at_unix REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS task_slot_requests (
                    request_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_pid INTEGER NOT NULL,
                    task_kind TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    heavy_units INTEGER NOT NULL,
                    vision_units INTEGER NOT NULL,
                    requested_at_unix REAL NOT NULL,
                    heartbeat_at_unix REAL NOT NULL,
                    expires_at_unix REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS task_slot_requests_order
                    ON task_slot_requests(task_kind, requested_at_unix, request_id);
                CREATE TABLE IF NOT EXISTS task_slot_leases (
                    lease_id TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    owner_pid INTEGER NOT NULL,
                    task_kind TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    event_key TEXT NOT NULL,
                    heavy_units INTEGER NOT NULL,
                    vision_units INTEGER NOT NULL,
                    acquired_at_unix REAL NOT NULL,
                    heartbeat_at_unix REAL NOT NULL,
                    expires_at_unix REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS task_slot_leases_expiry
                    ON task_slot_leases(expires_at_unix);
                """
            )
            now = time.time()
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now)
            row = connection.execute(
                "SELECT max_heavy_tasks, max_vision_tasks "
                "FROM coordinator_config WHERE singleton = 1"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO coordinator_config VALUES (1, ?, ?, ?)",
                    (self.max_heavy_tasks, self.max_vision_tasks, now),
                )
            elif (
                row["max_heavy_tasks"] != self.max_heavy_tasks
                or row["max_vision_tasks"] != self.max_vision_tasks
            ):
                active_count = connection.execute(
                    "SELECT "
                    "(SELECT COUNT(*) FROM task_slot_leases) + "
                    "(SELECT COUNT(*) FROM task_slot_requests)"
                ).fetchone()[0]
                if active_count:
                    raise HeavyTaskCoordinatorError(
                        "configured heavy task limits differ from another active Worker"
                    )
                connection.execute(
                    "UPDATE coordinator_config SET max_heavy_tasks = ?, "
                    "max_vision_tasks = ?, updated_at_unix = ? WHERE singleton = 1",
                    (self.max_heavy_tasks, self.max_vision_tasks, now),
                )
            connection.commit()
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
        self._publish_snapshot()

    @staticmethod
    def _requirements(task_kind: str) -> tuple[int, int]:
        if task_kind == "gif":
            return 1, 0
        if task_kind in {"vision", "vision_ocr", "vision_tdeed"}:
            return 1, 1
        raise ValueError(f"unsupported heavy task kind: {task_kind!r}")

    @staticmethod
    def _validate_identity(match_id: str, event_key: str) -> tuple[str, str]:
        match = str(match_id).strip()
        event = str(event_key).strip()
        if not match or not event:
            raise ValueError("match_id and event_key must not be empty")
        return match, event

    def acquire(
        self,
        task_kind: str,
        *,
        match_id: str,
        event_key: str,
        cancel_event: threading.Event | None = None,
        wait: bool = True,
    ) -> HeavyTaskLease:
        """Wait fairly for a slot and return a renewable context-managed lease."""
        if self._closed:
            raise HeavyTaskCoordinatorError("heavy task coordinator is closed")
        heavy_units, vision_units = self._requirements(task_kind)
        match, event = self._validate_identity(match_id, event_key)
        request_id = uuid.uuid4().hex
        requested_at = time.time()
        self._insert_request(
            request_id,
            task_kind=task_kind,
            match_id=match,
            event_key=event,
            heavy_units=heavy_units,
            vision_units=vision_units,
            requested_at=requested_at,
        )
        with self._local_lock:
            self._owned_request_ids.add(request_id)
        try:
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    raise HeavyTaskCancelled(
                        f"cancelled while waiting for a {task_kind} task slot"
                    )
                lease_id = self._try_promote_request(request_id)
                if lease_id is not None:
                    with self._local_lock:
                        self._owned_request_ids.discard(request_id)
                        self._owned_lease_ids.add(lease_id)
                    self._publish_snapshot()
                    return HeavyTaskLease(
                        self,
                        lease_id,
                        task_kind=task_kind,
                        match_id=match,
                        event_key=event,
                    )
                if not wait:
                    raise HeavyTaskUnavailable(
                        f"no {task_kind} task slot is currently available"
                    )
                if cancel_event is not None:
                    cancel_event.wait(self.poll_seconds)
                else:
                    time.sleep(self.poll_seconds)
        except BaseException:
            self._delete_request(request_id)
            with self._local_lock:
                self._owned_request_ids.discard(request_id)
            self._publish_snapshot()
            raise

    def _insert_request(
        self,
        request_id: str,
        *,
        task_kind: str,
        match_id: str,
        event_key: str,
        heavy_units: int,
        vision_units: int,
        requested_at: float,
    ) -> None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, requested_at)
            connection.execute(
                """
                INSERT INTO task_slot_requests (
                    request_id, owner_id, owner_pid, task_kind, match_id,
                    event_key, heavy_units, vision_units, requested_at_unix,
                    heartbeat_at_unix, expires_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    self.owner_id,
                    os.getpid(),
                    task_kind,
                    match_id,
                    event_key,
                    heavy_units,
                    vision_units,
                    requested_at,
                    requested_at,
                    requested_at + self.lease_seconds,
                ),
            )
            connection.commit()
        except sqlite3.Error as exc:
            if connection.in_transaction:
                connection.rollback()
            raise HeavyTaskCoordinatorError(
                f"cannot queue heavy task request: {exc}"
            ) from exc
        finally:
            connection.close()
        self._publish_snapshot()

    def _try_promote_request(self, request_id: str) -> str | None:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now)
            request = connection.execute(
                "SELECT * FROM task_slot_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if request is None:
                raise HeavyTaskCoordinatorError(
                    "heavy task request expired before it could be renewed"
                )
            connection.execute(
                "UPDATE task_slot_requests SET heartbeat_at_unix = ?, "
                "expires_at_unix = ? WHERE request_id = ?",
                (now, now + self.lease_seconds, request_id),
            )
            config = connection.execute(
                "SELECT max_heavy_tasks, max_vision_tasks FROM coordinator_config "
                "WHERE singleton = 1"
            ).fetchone()
            if config is None:
                raise HeavyTaskCoordinatorError(
                    "heavy task coordinator configuration is missing"
                )
            usage = connection.execute(
                "SELECT COALESCE(SUM(heavy_units), 0), "
                "COALESCE(SUM(vision_units), 0) FROM task_slot_leases"
            ).fetchone()
            heavy_available = int(config["max_heavy_tasks"]) - int(usage[0])
            vision_available = int(config["max_vision_tasks"]) - int(usage[1])
            maximum_active_vision = max(
                1,
                int(config["max_heavy_tasks"]) - DEFAULT_RESERVED_GIF_SLOTS,
            )
            eligible = False
            for queued in connection.execute(
                "SELECT request_id, task_kind, heavy_units, vision_units "
                "FROM task_slot_requests ORDER BY "
                "CASE task_kind "
                "WHEN 'gif' THEN 0 "
                "WHEN 'vision_ocr' THEN 1 "
                "WHEN 'vision' THEN 1 "
                "WHEN 'vision_tdeed' THEN 2 "
                "ELSE 3 END, "
                "requested_at_unix, rowid"
            ):
                fits = (
                    queued["heavy_units"] <= heavy_available
                    and queued["vision_units"] <= vision_available
                    and (
                        queued["task_kind"] not in {
                            "vision",
                            "vision_ocr",
                            "vision_tdeed",
                        }
                        or int(usage[1]) + int(queued["vision_units"])
                        <= maximum_active_vision
                    )
                )
                if queued["request_id"] == request_id:
                    eligible = fits
                    break
                if fits:
                    # An older runnable request gets the capacity first.
                    break
            if not eligible:
                connection.commit()
                return None
            lease_id = uuid.uuid4().hex
            connection.execute(
                """
                INSERT INTO task_slot_leases (
                    lease_id, owner_id, owner_pid, task_kind, match_id,
                    event_key, heavy_units, vision_units, acquired_at_unix,
                    heartbeat_at_unix, expires_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    request["owner_id"],
                    request["owner_pid"],
                    request["task_kind"],
                    request["match_id"],
                    request["event_key"],
                    request["heavy_units"],
                    request["vision_units"],
                    now,
                    now,
                    now + self.lease_seconds,
                ),
            )
            connection.execute(
                "DELETE FROM task_slot_requests WHERE request_id = ?",
                (request_id,),
            )
            connection.commit()
            return lease_id
        except HeavyTaskCoordinatorError:
            if connection.in_transaction:
                connection.rollback()
            raise
        except sqlite3.Error as exc:
            if connection.in_transaction:
                connection.rollback()
            raise HeavyTaskCoordinatorError(
                f"cannot acquire heavy task slot: {exc}"
            ) from exc
        finally:
            connection.close()

    def _renew(self, lease_id: str) -> bool:
        now = time.time()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now)
            cursor = connection.execute(
                "UPDATE task_slot_leases SET heartbeat_at_unix = ?, "
                "expires_at_unix = ? WHERE lease_id = ?",
                (now, now + self.lease_seconds, lease_id),
            )
            connection.commit()
            renewed = cursor.rowcount == 1
        except sqlite3.Error as exc:
            if connection.in_transaction:
                connection.rollback()
            raise HeavyTaskCoordinatorError(
                f"cannot renew heavy task lease: {exc}"
            ) from exc
        finally:
            connection.close()
        if renewed:
            self._publish_snapshot()
        return renewed

    def _release(self, lease_id: str) -> None:
        connection = self._connect()
        try:
            connection.execute(
                "DELETE FROM task_slot_leases WHERE lease_id = ? AND owner_id = ?",
                (lease_id, self.owner_id),
            )
        except sqlite3.Error as exc:
            raise HeavyTaskCoordinatorError(
                f"cannot release heavy task lease: {exc}"
            ) from exc
        finally:
            connection.close()
        with self._local_lock:
            self._owned_lease_ids.discard(lease_id)
        self._publish_snapshot()

    def _delete_request(self, request_id: str) -> None:
        connection = self._connect()
        try:
            connection.execute(
                "DELETE FROM task_slot_requests WHERE request_id = ? AND owner_id = ?",
                (request_id, self.owner_id),
            )
        except sqlite3.Error as exc:
            raise HeavyTaskCoordinatorError(
                f"cannot remove heavy task request: {exc}"
            ) from exc
        finally:
            connection.close()

    @staticmethod
    def _purge_expired(connection: sqlite3.Connection, now: float) -> None:
        connection.execute(
            "DELETE FROM task_slot_leases WHERE expires_at_unix <= ?", (now,)
        )
        connection.execute(
            "DELETE FROM task_slot_requests WHERE expires_at_unix <= ?", (now,)
        )

    def snapshot(self) -> dict[str, Any]:
        """Return a Dashboard-friendly view of global active and waiting work."""
        now = time.time()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._purge_expired(connection, now)
            config = connection.execute(
                "SELECT max_heavy_tasks, max_vision_tasks FROM coordinator_config "
                "WHERE singleton = 1"
            ).fetchone()
            leases = [
                dict(row)
                for row in connection.execute(
                    "SELECT task_kind, match_id, event_key, owner_pid, "
                    "acquired_at_unix, heartbeat_at_unix, expires_at_unix, "
                    "heavy_units, vision_units FROM task_slot_leases "
                    "ORDER BY acquired_at_unix, lease_id"
                )
            ]
            requests = [
                dict(row)
                for row in connection.execute(
                    "SELECT task_kind, match_id, event_key, owner_pid, "
                    "requested_at_unix, heartbeat_at_unix, expires_at_unix, "
                    "heavy_units, vision_units FROM task_slot_requests "
                    "ORDER BY CASE task_kind WHEN 'gif' THEN 0 ELSE 1 END, "
                    "requested_at_unix, rowid"
                )
            ]
            connection.commit()
        except sqlite3.Error as exc:
            if connection.in_transaction:
                connection.rollback()
            raise HeavyTaskCoordinatorError(
                f"cannot read heavy task coordinator status: {exc}"
            ) from exc
        finally:
            connection.close()
        return {
            "updated_at_unix": now,
            "database_path": str(self.database_path),
            "snapshot_path": str(self.snapshot_path),
            "limits": {
                "heavy": int(config["max_heavy_tasks"]),
                "vision": int(config["max_vision_tasks"]),
            },
            "active": {
                "tasks": len(leases),
                "heavy": sum(int(item["heavy_units"]) for item in leases),
                "vision": sum(int(item["vision_units"]) for item in leases),
                "items": leases,
            },
            "waiting": {
                "tasks": len(requests),
                "items": requests,
            },
        }

    def _publish_snapshot(self) -> None:
        try:
            snapshot = self.snapshot()
            self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            temporary_path: Path | None = None
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    prefix=f".{self.snapshot_path.name}.",
                    suffix=".tmp",
                    dir=self.snapshot_path.parent,
                    delete=False,
                ) as temporary:
                    temporary_path = Path(temporary.name)
                    json.dump(snapshot, temporary, ensure_ascii=True, indent=2)
                    temporary.write("\n")
                    temporary.flush()
                    os.fsync(temporary.fileno())
                os.replace(temporary_path, self.snapshot_path)
                temporary_path = None
            finally:
                if temporary_path is not None:
                    temporary_path.unlink(missing_ok=True)
        except (OSError, HeavyTaskCoordinatorError):
            # SQLite is authoritative; the JSON file is only an observability aid.
            return

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "DELETE FROM task_slot_leases WHERE owner_id = ?", (self.owner_id,)
            )
            connection.execute(
                "DELETE FROM task_slot_requests WHERE owner_id = ?", (self.owner_id,)
            )
            connection.commit()
        except sqlite3.Error as exc:
            if connection.in_transaction:
                connection.rollback()
            raise HeavyTaskCoordinatorError(
                f"cannot close heavy task coordinator: {exc}"
            ) from exc
        finally:
            connection.close()
        with self._local_lock:
            self._owned_lease_ids.clear()
            self._owned_request_ids.clear()
        self._publish_snapshot()

    def _close_at_exit(self) -> None:
        try:
            self.close()
        except HeavyTaskCoordinatorError:
            # Temporary test/output directories may already be gone at exit.
            pass


def run_with_task_slot(
    coordinator: HeavyTaskCoordinator,
    task_kind: str,
    match_id: str,
    event_key: str,
    function: Callable[..., Any],
    *args: Any,
    cancel_event: threading.Event | None = None,
    function_kwargs: dict[str, Any] | None = None,
    on_state_change: Callable[[str, float], None] | None = None,
) -> Any:
    """Run a callable while holding the requested cross-process slot."""
    with coordinator.acquire(
        task_kind,
        match_id=match_id,
        event_key=event_key,
        cancel_event=cancel_event,
    ):
        if on_state_change is not None:
            on_state_change("acquired", time.time())
            on_state_change("executing", time.time())
        return function(*args, **(function_kwargs or {}))
