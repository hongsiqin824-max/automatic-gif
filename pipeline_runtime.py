"""Durable task state and structured runtime logging for the GIF pipeline."""

from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from match_event_identity import events_represent_same_incident, merge_event_metadata


TASK_STATUSES = ("discovered", "pending", "encoding", "encoded", "failed")
INCOMPLETE_STATUSES = ("discovered", "pending", "encoding")
_ALLOWED_TRANSITIONS = {
    "discovered": {"pending", "failed"},
    "pending": {"encoding", "failed"},
    "encoding": {"pending", "encoded", "failed"},
    "encoded": set(),
    "failed": set(),
}

VISION_TASK_STATUSES = (
    "pending",
    "locating",
    "located",
    "encoding",
    "encoded",
    "failed",
)
VISION_INCOMPLETE_STATUSES = ("pending", "locating", "located", "encoding")
DEFAULT_GIF_DEADLINE_SECONDS = 55.0
DEFAULT_VISION_DEADLINE_SECONDS = 60.0
READINESS_RETRY_DELAYS_SECONDS = (2.0, 4.0, 8.0)
DEFAULT_HALFTIME_BREAK_SECONDS = 15.0 * 60.0
_ALLOWED_VISION_TRANSITIONS = {
    "pending": {"locating", "failed"},
    "locating": {"pending", "located", "failed"},
    "located": {"pending", "encoding", "failed"},
    "encoding": {"pending", "located", "encoded", "failed"},
    "encoded": set(),
    "failed": {"pending"},
}


def _readiness_retry_delay(check_count: int) -> float:
    index = min(max(check_count, 0), len(READINESS_RETRY_DELAYS_SECONDS) - 1)
    return READINESS_RETRY_DELAYS_SECONDS[index]


def _finite_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a finite number") from exc
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be a finite number")
    return result


def _minute_parts(
    minute: str | int | float,
    minute_extra: str | int | float | None,
) -> tuple[float, float]:
    raw_minute = str(minute).strip().rstrip("'").strip()
    if not raw_minute:
        raise ValueError("event minute must not be empty")

    embedded_extra: float | None = None
    if "+" in raw_minute:
        parts = [part.strip() for part in raw_minute.split("+")]
        if len(parts) != 2 or not all(parts):
            raise ValueError("event minute must use the form minute or minute+extra")
        raw_minute, raw_embedded_extra = parts
        embedded_extra = _finite_float(raw_embedded_extra, "event minute extra")

    base = _finite_float(raw_minute, "event minute")
    explicit_extra = (
        0.0
        if minute_extra is None or str(minute_extra).strip() == ""
        else _finite_float(minute_extra, "event minute extra")
    )
    if embedded_extra is not None:
        if explicit_extra not in (0.0, embedded_extra):
            raise ValueError("conflicting embedded and explicit event minute extra")
        explicit_extra = embedded_extra
    if base < 0 or explicit_extra < 0:
        raise ValueError("event minute values must be non-negative")
    return base, explicit_extra


def coarse_event_elapsed_seconds(
    minute: str | int | float,
    minute_extra: str | int | float | None = 0,
    *,
    halftime_break_seconds: float = DEFAULT_HALFTIME_BREAK_SECONDS,
) -> float:
    """Estimate wall-clock elapsed time since kickoff from a feed minute.

    The result deliberately remains a coarse estimate: feed minutes do not
    encode seconds, pauses, or the exact halftime duration. A halftime break
    is included for regular-time events strictly after minute 45.
    """
    halftime = _finite_float(halftime_break_seconds, "halftime break")
    if halftime < 0:
        raise ValueError("halftime break must be non-negative")
    base, extra = _minute_parts(minute, minute_extra)
    elapsed = (base + extra) * 60.0
    if base > 45.0:
        elapsed += halftime
    return elapsed


def _has_event_revision_evidence(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> bool:
    """Return whether two task payloads differ beyond their version keys.

    A task key identifies the feed revision, so it must not by itself be
    treated as evidence that two rows are revisions of one real-world event.
    Any other payload change is evidence that the feed supplied a later
    version, subject to the incident matcher used by recovery.
    """
    left_payload = {key: value for key, value in left.items() if key != "event_key"}
    right_payload = {key: value for key, value in right.items() if key != "event_key"}
    return left_payload != right_payload


@dataclass(frozen=True)
class StoredTask:
    event_key: str
    match_id: str
    code: str
    event_type: str
    event_data: dict[str, Any]
    status: str
    observed_stream_time: float
    observed_source_time: float | None
    clip_anchor_stream_time: float
    clip_anchor_source_time: float | None
    output_due_stream_time: float
    detected_at_unix: float
    discovered_at_unix: float
    updated_at_unix: float
    encoding_started_at_unix: float | None
    encoded_at_unix: float | None
    failed_at_unix: float | None
    attempt_count: int
    output_path: str | None
    output_bytes: int | None
    result: dict[str, Any]
    error: str | None
    suppressed_by_event_key: str | None
    next_attempt_at_unix: float
    deadline_at_unix: float
    readiness_check_count: int
    last_error_kind: str | None


@dataclass(frozen=True)
class StoredVisionTask:
    event_key: str
    match_id: str
    code: str
    event_type: str
    artifact_kind: str
    status: str
    source_anchor_stream_time: float
    source_anchor_source_time: float | None
    search_start_stream_time: float
    search_end_stream_time: float
    clip_before_seconds: float
    clip_after_seconds: float
    model_name: str | None
    model_version: str | None
    model_weights_sha256: str | None
    created_at_unix: float
    updated_at_unix: float
    locating_started_at_unix: float | None
    located_at_unix: float | None
    encoding_started_at_unix: float | None
    encoded_at_unix: float | None
    failed_at_unix: float | None
    locate_attempt_count: int
    encode_attempt_count: int
    located_anchor_stream_time: float | None
    located_anchor_source_time: float | None
    confidence: float | None
    inference_seconds: float | None
    output_path: str | None
    output_bytes: int | None
    result: dict[str, Any]
    error: str | None
    next_attempt_at_unix: float
    deadline_at_unix: float
    readiness_check_count: int
    last_error_kind: str | None


@dataclass(frozen=True)
class StoredSegmentLease:
    lease_id: str
    event_key: str
    owner: str
    segment_path: str
    acquired_at_unix: float
    renewed_at_unix: float
    expires_at_unix: float


@dataclass(frozen=True)
class TimelineState:
    """Persistent mapping between wall-clock, match-clock, and stream time."""

    match_id: str
    timeline_origin_wall_unix: float
    timeline_origin_stream_time: float = 0.0
    match_start_at_unix: float | None = None
    broadcast_delay_seconds: float = 0.0
    halftime_break_seconds: float = DEFAULT_HALFTIME_BREAK_SECONDS
    last_stream_time: float = 0.0
    created_at_unix: float = 0.0
    updated_at_unix: float = 0.0

    def __post_init__(self) -> None:
        match_id = str(self.match_id).strip()
        if not match_id:
            raise ValueError("timeline match_id must not be empty")
        object.__setattr__(self, "match_id", match_id)

        for field_name in (
            "timeline_origin_wall_unix",
            "timeline_origin_stream_time",
            "broadcast_delay_seconds",
            "halftime_break_seconds",
            "last_stream_time",
            "created_at_unix",
            "updated_at_unix",
        ):
            value = _finite_float(getattr(self, field_name), field_name)
            if value < 0:
                raise ValueError(f"{field_name} must be non-negative")
            object.__setattr__(self, field_name, value)
        if self.match_start_at_unix is not None:
            match_start = _finite_float(
                self.match_start_at_unix, "match_start_at_unix"
            )
            if match_start < 0:
                raise ValueError("match_start_at_unix must be non-negative")
            object.__setattr__(self, "match_start_at_unix", match_start)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "match_id": self.match_id,
            "timeline_origin_wall_unix": self.timeline_origin_wall_unix,
            "timeline_origin_stream_time": self.timeline_origin_stream_time,
            "match_start_at_unix": self.match_start_at_unix,
            "broadcast_delay_seconds": self.broadcast_delay_seconds,
            "halftime_break_seconds": self.halftime_break_seconds,
            "last_stream_time": self.last_stream_time,
            "created_at_unix": self.created_at_unix,
            "updated_at_unix": self.updated_at_unix,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> TimelineState:
        version = int(value.get("version", 1))
        if version != 1:
            raise ValueError(f"unsupported timeline state version: {version}")
        origin_stream_time = value.get("timeline_origin_stream_time", 0.0)
        return cls(
            match_id=str(value["match_id"]),
            timeline_origin_wall_unix=value["timeline_origin_wall_unix"],
            timeline_origin_stream_time=origin_stream_time,
            match_start_at_unix=value.get("match_start_at_unix"),
            broadcast_delay_seconds=value.get("broadcast_delay_seconds", 0.0),
            halftime_break_seconds=value.get(
                "halftime_break_seconds", DEFAULT_HALFTIME_BREAK_SECONDS
            ),
            last_stream_time=value.get("last_stream_time", origin_stream_time),
            created_at_unix=value.get("created_at_unix", 0.0),
            updated_at_unix=value.get("updated_at_unix", 0.0),
        )

    def wall_to_stream_time(self, wall_time_unix: float) -> float:
        wall_time = _finite_float(wall_time_unix, "wall_time_unix")
        return self.timeline_origin_stream_time + (
            wall_time - self.timeline_origin_wall_unix
        )

    def stream_to_wall_time(self, stream_time: float) -> float:
        absolute_stream_time = _finite_float(stream_time, "stream_time")
        return self.timeline_origin_wall_unix + (
            absolute_stream_time - self.timeline_origin_stream_time
        )

    def coarse_event_stream_time(
        self,
        minute: str | int | float,
        minute_extra: str | int | float | None = 0,
    ) -> float:
        if self.match_start_at_unix is None:
            raise ValueError("match_start_at_unix is required for an event estimate")
        elapsed = coarse_event_elapsed_seconds(
            minute,
            minute_extra,
            halftime_break_seconds=self.halftime_break_seconds,
        )
        event_visible_at_wall = (
            self.match_start_at_unix + elapsed + self.broadcast_delay_seconds
        )
        return self.wall_to_stream_time(event_visible_at_wall)


class JsonlEventLogger:
    """Append one complete JSON object per line and make it visible immediately."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def log(self, event: str, **fields: Any) -> None:
        now = time.time()
        record = {
            "timestamp": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "timestamp_unix": now,
            "event": event,
            **fields,
        }
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()


class TaskStateStore:
    """SQLite-backed event task store keyed by the feed's stable event key."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join(f"'{status}'" for status in TASK_STATUSES)
        vision_statuses = ",".join(
            f"'{status}'" for status in VISION_TASK_STATUSES
        )
        with self._lock:
            self.connection.executescript(
                f"""
            CREATE TABLE IF NOT EXISTS event_tasks (
                event_key TEXT PRIMARY KEY,
                match_id TEXT NOT NULL,
                code TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ({statuses})),
                observed_stream_time REAL NOT NULL,
                observed_source_time REAL,
                clip_anchor_stream_time REAL NOT NULL,
                clip_anchor_source_time REAL,
                output_due_stream_time REAL NOT NULL,
                detected_at_unix REAL NOT NULL,
                discovered_at_unix REAL NOT NULL,
                updated_at_unix REAL NOT NULL,
                encoding_started_at_unix REAL,
                encoded_at_unix REAL,
                failed_at_unix REAL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                output_path TEXT,
                output_bytes INTEGER,
                result_json TEXT NOT NULL DEFAULT '{{}}',
                error TEXT,
                suppressed_by_event_key TEXT,
                next_attempt_at_unix REAL,
                deadline_at_unix REAL,
                readiness_check_count INTEGER NOT NULL DEFAULT 0,
                last_error_kind TEXT
            );
            CREATE INDEX IF NOT EXISTS event_tasks_match_status
                ON event_tasks(match_id, status);
            CREATE TABLE IF NOT EXISTS event_feed_state (
                match_id TEXT PRIMARY KEY,
                initialized_at_unix REAL NOT NULL,
                updated_at_unix REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS event_feed_events (
                match_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                first_seen_at_unix REAL NOT NULL,
                PRIMARY KEY (match_id, event_key)
            );
            CREATE TABLE IF NOT EXISTS event_feed_aliases (
                match_id TEXT NOT NULL,
                version_key TEXT NOT NULL,
                canonical_key TEXT NOT NULL,
                event_json TEXT NOT NULL,
                updated_at_unix REAL NOT NULL,
                PRIMARY KEY (match_id, version_key)
            );
            CREATE TABLE IF NOT EXISTS event_feed_snapshot_events (
                match_id TEXT NOT NULL,
                version_key TEXT NOT NULL,
                event_json TEXT NOT NULL,
                first_seen_at_unix REAL NOT NULL,
                last_seen_at_unix REAL NOT NULL,
                PRIMARY KEY (match_id, version_key)
            );
            CREATE TABLE IF NOT EXISTS timeline_states (
                match_id TEXT PRIMARY KEY,
                timeline_origin_wall_unix REAL NOT NULL,
                timeline_origin_stream_time REAL NOT NULL DEFAULT 0,
                match_start_at_unix REAL,
                broadcast_delay_seconds REAL NOT NULL DEFAULT 0,
                halftime_break_seconds REAL NOT NULL DEFAULT 900,
                last_stream_time REAL NOT NULL DEFAULT 0,
                created_at_unix REAL NOT NULL,
                updated_at_unix REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS vision_tasks (
                event_key TEXT PRIMARY KEY,
                match_id TEXT NOT NULL,
                code TEXT NOT NULL,
                event_type TEXT NOT NULL,
                artifact_kind TEXT NOT NULL DEFAULT 'refined'
                    CHECK (artifact_kind = 'refined'),
                status TEXT NOT NULL CHECK (status IN ({vision_statuses})),
                source_anchor_stream_time REAL NOT NULL,
                source_anchor_source_time REAL,
                search_start_stream_time REAL NOT NULL,
                search_end_stream_time REAL NOT NULL,
                clip_before_seconds REAL NOT NULL,
                clip_after_seconds REAL NOT NULL,
                model_name TEXT,
                model_version TEXT,
                model_weights_sha256 TEXT,
                created_at_unix REAL NOT NULL,
                updated_at_unix REAL NOT NULL,
                locating_started_at_unix REAL,
                located_at_unix REAL,
                encoding_started_at_unix REAL,
                encoded_at_unix REAL,
                failed_at_unix REAL,
                locate_attempt_count INTEGER NOT NULL DEFAULT 0,
                encode_attempt_count INTEGER NOT NULL DEFAULT 0,
                located_anchor_stream_time REAL,
                located_anchor_source_time REAL,
                confidence REAL,
                inference_seconds REAL,
                output_path TEXT,
                output_bytes INTEGER,
                result_json TEXT NOT NULL DEFAULT '{{}}',
                error TEXT,
                next_attempt_at_unix REAL,
                deadline_at_unix REAL,
                readiness_check_count INTEGER NOT NULL DEFAULT 0,
                last_error_kind TEXT
            );
            CREATE INDEX IF NOT EXISTS vision_tasks_match_status
                ON vision_tasks(match_id, status);
            CREATE TABLE IF NOT EXISTS segment_leases (
                lease_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                owner TEXT NOT NULL,
                segment_path TEXT NOT NULL,
                acquired_at_unix REAL NOT NULL,
                renewed_at_unix REAL NOT NULL,
                expires_at_unix REAL NOT NULL,
                PRIMARY KEY (lease_id, segment_path)
            );
            CREATE INDEX IF NOT EXISTS segment_leases_path_expiry
                ON segment_leases(segment_path, expires_at_unix);
            CREATE INDEX IF NOT EXISTS segment_leases_event_expiry
                ON segment_leases(event_key, expires_at_unix);
            """
            )
            columns = {
                str(row["name"])
                for row in self.connection.execute("PRAGMA table_info(event_tasks)")
            }
            if "suppressed_by_event_key" not in columns:
                self.connection.execute(
                    """
                    ALTER TABLE event_tasks
                    ADD COLUMN suppressed_by_event_key TEXT
                    """
                )
            event_migrations = {
                "next_attempt_at_unix": "REAL",
                "deadline_at_unix": "REAL",
                "readiness_check_count": "INTEGER NOT NULL DEFAULT 0",
                "last_error_kind": "TEXT",
            }
            for name, definition in event_migrations.items():
                if name not in columns:
                    self.connection.execute(
                        f"ALTER TABLE event_tasks ADD COLUMN {name} {definition}"
                    )
            vision_columns = {
                str(row["name"])
                for row in self.connection.execute("PRAGMA table_info(vision_tasks)")
            }
            vision_migrations = {
                "next_attempt_at_unix": "REAL",
                "deadline_at_unix": "REAL",
                "readiness_check_count": "INTEGER NOT NULL DEFAULT 0",
                "last_error_kind": "TEXT",
            }
            for name, definition in vision_migrations.items():
                if name not in vision_columns:
                    self.connection.execute(
                        f"ALTER TABLE vision_tasks ADD COLUMN {name} {definition}"
                    )
            self.connection.execute(
                """
                UPDATE event_tasks SET
                    next_attempt_at_unix = COALESCE(
                        next_attempt_at_unix, discovered_at_unix
                    ),
                    deadline_at_unix = COALESCE(
                        deadline_at_unix,
                        detected_at_unix + ?
                    )
                """,
                (DEFAULT_GIF_DEADLINE_SECONDS,),
            )
            self.connection.execute(
                """
                UPDATE vision_tasks SET
                    next_attempt_at_unix = COALESCE(
                        next_attempt_at_unix, created_at_unix
                    ),
                    deadline_at_unix = COALESCE(
                        deadline_at_unix,
                        created_at_unix + ?
                    )
                """,
                (DEFAULT_VISION_DEADLINE_SECONDS,),
            )
            self.connection.commit()

    def close(self) -> None:
        with self._lock:
            self.connection.close()

    def __enter__(self) -> TaskStateStore:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    @staticmethod
    def _decode_row(row: sqlite3.Row) -> StoredTask:
        return StoredTask(
            event_key=row["event_key"],
            match_id=row["match_id"],
            code=row["code"],
            event_type=row["event_type"],
            event_data=json.loads(row["event_json"]),
            status=row["status"],
            observed_stream_time=row["observed_stream_time"],
            observed_source_time=row["observed_source_time"],
            clip_anchor_stream_time=row["clip_anchor_stream_time"],
            clip_anchor_source_time=row["clip_anchor_source_time"],
            output_due_stream_time=row["output_due_stream_time"],
            detected_at_unix=row["detected_at_unix"],
            discovered_at_unix=row["discovered_at_unix"],
            updated_at_unix=row["updated_at_unix"],
            encoding_started_at_unix=row["encoding_started_at_unix"],
            encoded_at_unix=row["encoded_at_unix"],
            failed_at_unix=row["failed_at_unix"],
            attempt_count=row["attempt_count"],
            output_path=row["output_path"],
            output_bytes=row["output_bytes"],
            result=json.loads(row["result_json"]),
            error=row["error"],
            suppressed_by_event_key=row["suppressed_by_event_key"],
            next_attempt_at_unix=row["next_attempt_at_unix"],
            deadline_at_unix=row["deadline_at_unix"],
            readiness_check_count=row["readiness_check_count"],
            last_error_kind=row["last_error_kind"],
        )

    @staticmethod
    def _decode_vision_row(row: sqlite3.Row) -> StoredVisionTask:
        return StoredVisionTask(
            event_key=row["event_key"],
            match_id=row["match_id"],
            code=row["code"],
            event_type=row["event_type"],
            artifact_kind=row["artifact_kind"],
            status=row["status"],
            source_anchor_stream_time=row["source_anchor_stream_time"],
            source_anchor_source_time=row["source_anchor_source_time"],
            search_start_stream_time=row["search_start_stream_time"],
            search_end_stream_time=row["search_end_stream_time"],
            clip_before_seconds=row["clip_before_seconds"],
            clip_after_seconds=row["clip_after_seconds"],
            model_name=row["model_name"],
            model_version=row["model_version"],
            model_weights_sha256=row["model_weights_sha256"],
            created_at_unix=row["created_at_unix"],
            updated_at_unix=row["updated_at_unix"],
            locating_started_at_unix=row["locating_started_at_unix"],
            located_at_unix=row["located_at_unix"],
            encoding_started_at_unix=row["encoding_started_at_unix"],
            encoded_at_unix=row["encoded_at_unix"],
            failed_at_unix=row["failed_at_unix"],
            locate_attempt_count=row["locate_attempt_count"],
            encode_attempt_count=row["encode_attempt_count"],
            located_anchor_stream_time=row["located_anchor_stream_time"],
            located_anchor_source_time=row["located_anchor_source_time"],
            confidence=row["confidence"],
            inference_seconds=row["inference_seconds"],
            output_path=row["output_path"],
            output_bytes=row["output_bytes"],
            result=json.loads(row["result_json"]),
            error=row["error"],
            next_attempt_at_unix=row["next_attempt_at_unix"],
            deadline_at_unix=row["deadline_at_unix"],
            readiness_check_count=row["readiness_check_count"],
            last_error_kind=row["last_error_kind"],
        )

    @staticmethod
    def _decode_segment_lease_row(row: sqlite3.Row) -> StoredSegmentLease:
        return StoredSegmentLease(
            lease_id=row["lease_id"],
            event_key=row["event_key"],
            owner=row["owner"],
            segment_path=row["segment_path"],
            acquired_at_unix=row["acquired_at_unix"],
            renewed_at_unix=row["renewed_at_unix"],
            expires_at_unix=row["expires_at_unix"],
        )

    @staticmethod
    def _decode_timeline_state(row: sqlite3.Row) -> TimelineState:
        return TimelineState(
            match_id=row["match_id"],
            timeline_origin_wall_unix=row["timeline_origin_wall_unix"],
            timeline_origin_stream_time=row["timeline_origin_stream_time"],
            match_start_at_unix=row["match_start_at_unix"],
            broadcast_delay_seconds=row["broadcast_delay_seconds"],
            halftime_break_seconds=row["halftime_break_seconds"],
            last_stream_time=row["last_stream_time"],
            created_at_unix=row["created_at_unix"],
            updated_at_unix=row["updated_at_unix"],
        )

    def get_timeline_state(self, match_id: str) -> TimelineState | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM timeline_states WHERE match_id = ?",
                (str(match_id),),
            ).fetchone()
        return self._decode_timeline_state(row) if row is not None else None

    def upsert_timeline_state(
        self,
        state: TimelineState,
        *,
        now: float | None = None,
    ) -> TimelineState:
        """Atomically save a match timeline without regressing its checkpoint."""
        timestamp = _finite_float(
            time.time() if now is None else now, "timeline updated_at_unix"
        )
        if timestamp < 0:
            raise ValueError("timeline updated_at_unix must be non-negative")
        created_at = state.created_at_unix or timestamp
        with self._lock, self.connection:
            self.connection.execute(
                """
                INSERT INTO timeline_states (
                    match_id, timeline_origin_wall_unix,
                    timeline_origin_stream_time, match_start_at_unix,
                    broadcast_delay_seconds, halftime_break_seconds,
                    last_stream_time, created_at_unix, updated_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET
                    timeline_origin_wall_unix = excluded.timeline_origin_wall_unix,
                    timeline_origin_stream_time = excluded.timeline_origin_stream_time,
                    match_start_at_unix = excluded.match_start_at_unix,
                    broadcast_delay_seconds = excluded.broadcast_delay_seconds,
                    halftime_break_seconds = excluded.halftime_break_seconds,
                    last_stream_time = MAX(
                        timeline_states.last_stream_time,
                        excluded.last_stream_time
                    ),
                    updated_at_unix = MAX(
                        timeline_states.updated_at_unix,
                        excluded.updated_at_unix
                    )
                """,
                (
                    state.match_id,
                    state.timeline_origin_wall_unix,
                    state.timeline_origin_stream_time,
                    state.match_start_at_unix,
                    state.broadcast_delay_seconds,
                    state.halftime_break_seconds,
                    state.last_stream_time,
                    created_at,
                    timestamp,
                ),
            )
        saved = self.get_timeline_state(state.match_id)
        assert saved is not None
        return saved

    def checkpoint_timeline(
        self,
        match_id: str,
        last_stream_time: float,
        *,
        now: float | None = None,
    ) -> TimelineState:
        """Advance the durable stream checkpoint; stale updates never rewind it."""
        stream_time = _finite_float(last_stream_time, "last_stream_time")
        timestamp = _finite_float(
            time.time() if now is None else now, "timeline updated_at_unix"
        )
        if stream_time < 0:
            raise ValueError("last_stream_time must be non-negative")
        if timestamp < 0:
            raise ValueError("timeline updated_at_unix must be non-negative")
        with self._lock, self.connection:
            cursor = self.connection.execute(
                """
                UPDATE timeline_states SET
                    last_stream_time = MAX(last_stream_time, ?),
                    updated_at_unix = CASE
                        WHEN ? > last_stream_time THEN ?
                        ELSE updated_at_unix
                    END
                WHERE match_id = ?
                """,
                (stream_time, stream_time, timestamp, str(match_id)),
            )
        if cursor.rowcount != 1:
            raise KeyError(f"unknown timeline state: {match_id}")
        saved = self.get_timeline_state(str(match_id))
        assert saved is not None
        return saved

    def get(self, event_key: str) -> StoredTask | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM event_tasks WHERE event_key = ?", (event_key,)
            ).fetchone()
        return self._decode_row(row) if row is not None else None

    def update_event_data(
        self,
        event_data: Mapping[str, Any],
        *,
        now: float | None = None,
    ) -> StoredTask | None:
        """Update mutable feed metadata without changing task processing state."""
        event_key = str(event_data["event_key"])
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get(event_key)
            if current is None:
                return None
            merged = merge_event_metadata(current.event_data, event_data)
            if merged == current.event_data:
                return current
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE event_tasks
                    SET code = ?, event_type = ?, event_json = ?, updated_at_unix = ?
                    WHERE event_key = ?
                    """,
                    (
                        str(merged["code"]),
                        str(merged["event_type"]),
                        json.dumps(merged, ensure_ascii=False, separators=(",", ":")),
                        timestamp,
                        event_key,
                    ),
                )
            return self.get(event_key)

    def discover(
        self,
        *,
        match_id: str,
        event_data: Mapping[str, Any],
        observed_stream_time: float,
        observed_source_time: float | None,
        clip_anchor_stream_time: float,
        clip_anchor_source_time: float | None,
        output_due_stream_time: float,
        detected_at_unix: float,
        deadline_at_unix: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Persist a newly observed event. Return False for any existing task."""
        timestamp = time.time() if now is None else now
        event_key = str(event_data["event_key"])
        deadline = (
            detected_at_unix + DEFAULT_GIF_DEADLINE_SECONDS
            if deadline_at_unix is None
            else deadline_at_unix
        )
        with self._lock, self.connection:
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO event_tasks (
                    event_key, match_id, code, event_type, event_json, status,
                    observed_stream_time, observed_source_time,
                    clip_anchor_stream_time, clip_anchor_source_time,
                    output_due_stream_time, detected_at_unix,
                    discovered_at_unix, updated_at_unix,
                    next_attempt_at_unix, deadline_at_unix
                ) VALUES (?, ?, ?, ?, ?, 'discovered', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    match_id,
                    str(event_data["code"]),
                    str(event_data["event_type"]),
                    json.dumps(dict(event_data), ensure_ascii=False, separators=(",", ":")),
                    observed_stream_time,
                    observed_source_time,
                    clip_anchor_stream_time,
                    clip_anchor_source_time,
                    output_due_stream_time,
                    detected_at_unix,
                    timestamp,
                    timestamp,
                    timestamp,
                    deadline,
                ),
            )
        return cursor.rowcount == 1

    def list_incomplete(self, match_id: str) -> list[StoredTask]:
        placeholders = ",".join("?" for _ in INCOMPLETE_STATUSES)
        with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT * FROM event_tasks
                WHERE match_id = ? AND status IN ({placeholders})
                  AND suppressed_by_event_key IS NULL
                ORDER BY discovered_at_unix, event_key
                """,
                (match_id, *INCOMPLETE_STATUSES),
            ).fetchall()
        return [self._decode_row(row) for row in rows]

    def suppress_task(
        self,
        event_key: str,
        canonical_event_key: str,
        *,
        now: float | None = None,
    ) -> StoredTask | None:
        """Prevent a stale task revision from being recovered or encoded."""
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get(event_key)
            if current is None:
                return None
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE event_tasks
                    SET suppressed_by_event_key = ?, updated_at_unix = ?
                    WHERE event_key = ?
                    """,
                    (canonical_event_key, timestamp, event_key),
                )
            return self.get(event_key)

    def list_for_match(self, match_id: str) -> list[StoredTask]:
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT * FROM event_tasks
                WHERE match_id = ?
                ORDER BY discovered_at_unix, event_key
                """,
                (match_id,),
            ).fetchall()
        return [self._decode_row(row) for row in rows]

    def load_event_cursor(self, match_id: str) -> tuple[bool, set[str]]:
        """Return whether the feed was initialized and all previously seen keys."""
        with self._lock:
            initialized = self.connection.execute(
                "SELECT 1 FROM event_feed_state WHERE match_id = ?", (match_id,)
            ).fetchone() is not None
            rows = self.connection.execute(
                "SELECT event_key FROM event_feed_events WHERE match_id = ?",
                (match_id,),
            ).fetchall()
        return initialized, {str(row["event_key"]) for row in rows}

    def load_event_aliases(self, match_id: str) -> dict[str, dict[str, Any]]:
        """Load persisted feed versions and migrate existing task variants."""
        with self._lock:
            alias_rows = self.connection.execute(
                """
                SELECT version_key, canonical_key, event_json
                FROM event_feed_aliases WHERE match_id = ?
                ORDER BY updated_at_unix, version_key
                """,
                (match_id,),
            ).fetchall()
            task_rows = self.connection.execute(
                """
                SELECT event_key, event_json FROM event_tasks
                WHERE match_id = ? ORDER BY discovered_at_unix, event_key
                """,
                (match_id,),
            ).fetchall()

        aliases: dict[str, dict[str, Any]] = {}
        canonicals: dict[str, dict[str, Any]] = {}
        for row in alias_rows:
            event_data = json.loads(row["event_json"])
            event_data["event_key"] = str(row["canonical_key"])
            aliases[str(row["version_key"])] = event_data
            canonical_key = event_data["event_key"]
            if canonical_key in canonicals:
                canonicals[canonical_key] = merge_event_metadata(
                    canonicals[canonical_key], event_data
                )
            else:
                canonicals[canonical_key] = dict(event_data)

        for row in task_rows:
            version_key = str(row["event_key"])
            event_data = json.loads(row["event_json"])
            existing = aliases.get(version_key)
            if existing is not None:
                canonical_key = str(existing["event_key"])
            else:
                candidates = [
                    key
                    for key, candidate in canonicals.items()
                    if events_represent_same_incident(
                        candidate, event_data, allow_exact_match=False
                    )
                ]
                canonical_key = candidates[0] if len(candidates) == 1 else version_key
            event_data["event_key"] = canonical_key
            if canonical_key in canonicals:
                canonicals[canonical_key] = merge_event_metadata(
                    canonicals[canonical_key], event_data
                )
            else:
                canonicals[canonical_key] = dict(event_data)
            canonicals[canonical_key]["event_key"] = canonical_key
            aliases[version_key] = canonicals[canonical_key]

        return {
            version_key: dict(canonicals[str(event_data["event_key"])])
            for version_key, event_data in aliases.items()
        }

    def load_event_snapshot(
        self,
        match_id: str,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
        """Load the last successful raw feed snapshot and version first-seen times."""
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT version_key, event_json, first_seen_at_unix
                FROM event_feed_snapshot_events
                WHERE match_id = ?
                ORDER BY version_key
                """,
                (match_id,),
            ).fetchall()
        events: dict[str, dict[str, Any]] = {}
        first_seen: dict[str, float] = {}
        for row in rows:
            version_key = str(row["version_key"])
            event_data = json.loads(row["event_json"])
            event_data["event_key"] = version_key
            events[version_key] = event_data
            first_seen[version_key] = float(row["first_seen_at_unix"])
        return events, first_seen

    def remember_event_snapshot(
        self,
        match_id: str,
        event_keys: set[str],
        *,
        aliases: Mapping[str, Mapping[str, Any]] | None = None,
        current_versions: Mapping[str, Mapping[str, Any]] | None = None,
        now: float | None = None,
    ) -> None:
        """Durably advance the feed cursor after its events became tasks."""
        timestamp = time.time() if now is None else now
        with self._lock, self.connection:
            self.connection.execute(
                """
                INSERT INTO event_feed_state (
                    match_id, initialized_at_unix, updated_at_unix
                ) VALUES (?, ?, ?)
                ON CONFLICT(match_id) DO UPDATE SET updated_at_unix = excluded.updated_at_unix
                """,
                (match_id, timestamp, timestamp),
            )
            self.connection.executemany(
                """
                INSERT OR IGNORE INTO event_feed_events (
                    match_id, event_key, first_seen_at_unix
                ) VALUES (?, ?, ?)
                """,
                [
                    (match_id, event_key, timestamp)
                    for event_key in event_keys
                ],
            )
            if aliases:
                self.connection.executemany(
                    """
                    INSERT INTO event_feed_aliases (
                        match_id, version_key, canonical_key, event_json,
                        updated_at_unix
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(match_id, version_key) DO UPDATE SET
                        canonical_key = excluded.canonical_key,
                        event_json = excluded.event_json,
                        updated_at_unix = excluded.updated_at_unix
                    """,
                    [
                        (
                            match_id,
                            version_key,
                            str(event_data["event_key"]),
                            json.dumps(
                                dict(event_data),
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            timestamp,
                        )
                        for version_key, event_data in aliases.items()
                    ],
                )
            if current_versions is not None:
                previous_first_seen = {
                    str(row["version_key"]): float(row["first_seen_at_unix"])
                    for row in self.connection.execute(
                        """
                        SELECT version_key, first_seen_at_unix
                        FROM event_feed_snapshot_events
                        WHERE match_id = ?
                        """,
                        (match_id,),
                    ).fetchall()
                }
                self.connection.execute(
                    "DELETE FROM event_feed_snapshot_events WHERE match_id = ?",
                    (match_id,),
                )
                self.connection.executemany(
                    """
                    INSERT INTO event_feed_snapshot_events (
                        match_id, version_key, event_json,
                        first_seen_at_unix, last_seen_at_unix
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            match_id,
                            version_key,
                            json.dumps(
                                {**dict(event_data), "event_key": version_key},
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            previous_first_seen.get(version_key, timestamp),
                            timestamp,
                        )
                        for version_key, event_data in current_versions.items()
                    ],
                )

    def transition(
        self,
        event_key: str,
        new_status: str,
        *,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        now: float | None = None,
    ) -> StoredTask:
        if new_status not in TASK_STATUSES:
            raise ValueError(f"unknown task status: {new_status}")
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get(event_key)
            if current is None:
                raise KeyError(f"unknown event task: {event_key}")
            if new_status not in _ALLOWED_TRANSITIONS[current.status]:
                raise ValueError(
                    f"invalid task transition: {current.status} -> {new_status}"
                )

            assignments = ["status = ?", "updated_at_unix = ?"]
            values: list[Any] = [new_status, timestamp]
            if new_status == "encoding":
                assignments.extend(
                    [
                        "encoding_started_at_unix = ?",
                        "attempt_count = attempt_count + 1",
                    ]
                )
                values.append(timestamp)
            elif new_status == "encoded":
                assignments.append("encoded_at_unix = ?")
                values.append(timestamp)
            elif new_status == "failed":
                assignments.append("failed_at_unix = ?")
                values.append(timestamp)
            if result is not None:
                normalized = dict(result)
                assignments.extend(
                    ["result_json = ?", "output_path = ?", "output_bytes = ?"]
                )
                values.extend(
                    [
                        json.dumps(
                            normalized,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        normalized.get("output"),
                        normalized.get("bytes"),
                    ]
                )
            if error is not None:
                assignments.append("error = ?")
                values.append(error)
            elif new_status in ("pending", "encoding", "encoded"):
                assignments.append("error = NULL")
            if error_kind is not None:
                assignments.append("last_error_kind = ?")
                values.append(error_kind)
            elif new_status == "encoded":
                assignments.append("last_error_kind = NULL")
            values.append(event_key)
            with self.connection:
                self.connection.execute(
                    f"UPDATE event_tasks SET {', '.join(assignments)} WHERE event_key = ?",
                    values,
                )
            updated = self.get(event_key)
        assert updated is not None
        return updated

    def record_readiness_wait(
        self,
        event_key: str,
        error: str,
        *,
        error_kind: str,
        now: float | None = None,
    ) -> StoredTask:
        """Persist a cheap coverage check without counting an encode attempt."""
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get(event_key)
            if current is None:
                raise KeyError(f"unknown event task: {event_key}")
            if current.status != "pending":
                raise ValueError(
                    f"cannot schedule readiness retry from {current.status}"
                )
            delay = _readiness_retry_delay(current.readiness_check_count)
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE event_tasks SET
                        updated_at_unix = ?,
                        next_attempt_at_unix = ?,
                        readiness_check_count = readiness_check_count + 1,
                        last_error_kind = ?,
                        error = ?
                    WHERE event_key = ?
                    """,
                    (
                        timestamp,
                        min(timestamp + delay, current.deadline_at_unix),
                        error_kind,
                        error,
                        event_key,
                    ),
                )
            updated = self.get(event_key)
        assert updated is not None
        return updated

    def enqueue_vision_task(
        self,
        event_key: str,
        *,
        search_start_stream_time: float,
        search_end_stream_time: float,
        clip_before_seconds: float,
        clip_after_seconds: float,
        model_name: str | None = None,
        model_version: str | None = None,
        model_weights_sha256: str | None = None,
        deadline_at_unix: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Create the event's sole refined-artifact task without changing its default task."""
        if search_end_stream_time < search_start_stream_time:
            raise ValueError("vision search end must not precede search start")
        if clip_before_seconds < 0 or clip_after_seconds < 0:
            raise ValueError("vision clip durations must be non-negative")
        timestamp = time.time() if now is None else now
        with self._lock, self.connection:
            event = self.connection.execute(
                "SELECT * FROM event_tasks WHERE event_key = ?", (event_key,)
            ).fetchone()
            if event is None:
                raise KeyError(f"unknown event task: {event_key}")
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO vision_tasks (
                    event_key, match_id, code, event_type, artifact_kind, status,
                    source_anchor_stream_time, source_anchor_source_time,
                    search_start_stream_time, search_end_stream_time,
                    clip_before_seconds, clip_after_seconds,
                    model_name, model_version, model_weights_sha256,
                    created_at_unix, updated_at_unix,
                    next_attempt_at_unix, deadline_at_unix
                ) VALUES (?, ?, ?, ?, 'refined', 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    event["match_id"],
                    event["code"],
                    event["event_type"],
                    event["clip_anchor_stream_time"],
                    event["clip_anchor_source_time"],
                    search_start_stream_time,
                    search_end_stream_time,
                    clip_before_seconds,
                    clip_after_seconds,
                    model_name,
                    model_version,
                    model_weights_sha256,
                    timestamp,
                    timestamp,
                    timestamp,
                    (
                        timestamp + DEFAULT_VISION_DEADLINE_SECONDS
                        if deadline_at_unix is None
                        else deadline_at_unix
                    ),
                ),
            )
        return cursor.rowcount == 1

    def get_vision_task(self, event_key: str) -> StoredVisionTask | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT * FROM vision_tasks WHERE event_key = ?", (event_key,)
            ).fetchone()
        return self._decode_vision_row(row) if row is not None else None

    def list_vision_tasks(self, match_id: str) -> list[StoredVisionTask]:
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT * FROM vision_tasks
                WHERE match_id = ? ORDER BY created_at_unix, event_key
                """,
                (match_id,),
            ).fetchall()
        return [self._decode_vision_row(row) for row in rows]

    def list_incomplete_vision_tasks(self, match_id: str) -> list[StoredVisionTask]:
        placeholders = ",".join("?" for _ in VISION_INCOMPLETE_STATUSES)
        with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT * FROM vision_tasks
                WHERE match_id = ? AND status IN ({placeholders})
                ORDER BY created_at_unix, event_key
                """,
                (match_id, *VISION_INCOMPLETE_STATUSES),
            ).fetchall()
        return [self._decode_vision_row(row) for row in rows]

    def transition_vision_task(
        self,
        event_key: str,
        new_status: str,
        *,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        now: float | None = None,
    ) -> StoredVisionTask:
        """Durably advance one refined task while preserving earlier stage results."""
        if new_status not in VISION_TASK_STATUSES:
            raise ValueError(f"unknown vision task status: {new_status}")
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get_vision_task(event_key)
            if current is None:
                raise KeyError(f"unknown vision task: {event_key}")
            if new_status not in _ALLOWED_VISION_TRANSITIONS[current.status]:
                raise ValueError(
                    f"invalid vision task transition: {current.status} -> {new_status}"
                )

            merged_result = dict(current.result)
            if result is not None:
                merged_result.update(result)
            located_anchor = merged_result.get(
                "anchor_stream_time", current.located_anchor_stream_time
            )
            if new_status in ("located", "encoding", "encoded") and located_anchor is None:
                raise ValueError(
                    f"vision task must have an anchor before entering {new_status}"
                )
            if new_status == "encoded" and not (
                merged_result.get("output") or current.output_path
            ):
                raise ValueError("encoded vision task must have an output path")

            assignments = [
                "status = ?",
                "updated_at_unix = ?",
                "result_json = ?",
            ]
            values: list[Any] = [
                new_status,
                timestamp,
                json.dumps(merged_result, ensure_ascii=False, separators=(",", ":")),
            ]
            if new_status == "locating":
                assignments.extend(
                    [
                        "locating_started_at_unix = ?",
                        "locate_attempt_count = locate_attempt_count + 1",
                    ]
                )
                values.append(timestamp)
            elif new_status == "located":
                assignments.extend(
                    [
                        "located_at_unix = ?",
                        "located_anchor_stream_time = ?",
                        "located_anchor_source_time = ?",
                        "confidence = ?",
                        "inference_seconds = ?",
                        "model_name = COALESCE(?, model_name)",
                        "model_version = COALESCE(?, model_version)",
                        "model_weights_sha256 = COALESCE(?, model_weights_sha256)",
                    ]
                )
                values.extend(
                    [
                        timestamp,
                        located_anchor,
                        merged_result.get(
                            "anchor_source_time", current.located_anchor_source_time
                        ),
                        merged_result.get("confidence", current.confidence),
                        merged_result.get(
                            "inference_seconds", current.inference_seconds
                        ),
                        merged_result.get("model_name"),
                        merged_result.get("model_version"),
                        merged_result.get("model_weights_sha256"),
                    ]
                )
            elif new_status == "encoding":
                assignments.extend(
                    [
                        "encoding_started_at_unix = ?",
                        "encode_attempt_count = encode_attempt_count + 1",
                    ]
                )
                values.append(timestamp)
            elif new_status == "encoded":
                assignments.extend(
                    [
                        "encoded_at_unix = ?",
                        "output_path = ?",
                        "output_bytes = ?",
                    ]
                )
                values.extend(
                    [
                        timestamp,
                        merged_result.get("output", current.output_path),
                        merged_result.get("bytes", current.output_bytes),
                    ]
                )
            elif new_status == "failed":
                assignments.append("failed_at_unix = ?")
                values.append(timestamp)
            if error is not None:
                assignments.append("error = ?")
                values.append(error)
            elif new_status in ("pending", "locating", "located", "encoding", "encoded"):
                assignments.append("error = NULL")
            if error_kind is not None:
                assignments.append("last_error_kind = ?")
                values.append(error_kind)
            elif new_status == "encoded":
                assignments.append("last_error_kind = NULL")
            values.append(event_key)
            with self.connection:
                self.connection.execute(
                    f"UPDATE vision_tasks SET {', '.join(assignments)} WHERE event_key = ?",
                    values,
                )
            updated = self.get_vision_task(event_key)
        assert updated is not None
        return updated

    def record_vision_readiness_wait(
        self,
        event_key: str,
        error: str,
        *,
        error_kind: str,
        now: float | None = None,
    ) -> StoredVisionTask:
        """Schedule visual input rechecks independently from model/encode attempts."""
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get_vision_task(event_key)
            if current is None:
                raise KeyError(f"unknown vision task: {event_key}")
            if current.status not in {"pending", "located"}:
                raise ValueError(
                    f"cannot schedule visual readiness retry from {current.status}"
                )
            delay = _readiness_retry_delay(current.readiness_check_count)
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE vision_tasks SET
                        updated_at_unix = ?,
                        next_attempt_at_unix = ?,
                        readiness_check_count = readiness_check_count + 1,
                        last_error_kind = ?,
                        error = ?
                    WHERE event_key = ?
                    """,
                    (
                        timestamp,
                        min(timestamp + delay, current.deadline_at_unix),
                        error_kind,
                        error,
                        event_key,
                    ),
                )
            updated = self.get_vision_task(event_key)
        assert updated is not None
        return updated

    def acquire_segment_lease(
        self,
        event_key: str,
        segment_paths: list[str] | tuple[str, ...] | set[str],
        *,
        owner: str,
        ttl_seconds: float,
        now: float | None = None,
    ) -> str:
        """Protect a stable set of segment paths until release or TTL expiry."""
        if ttl_seconds <= 0:
            raise ValueError("segment lease TTL must be positive")
        paths = tuple(dict.fromkeys(str(path) for path in segment_paths))
        if not paths or any(not path for path in paths):
            raise ValueError("segment lease requires at least one non-empty path")
        if not owner:
            raise ValueError("segment lease owner must not be empty")
        if self.get_vision_task(event_key) is None:
            raise KeyError(f"unknown vision task: {event_key}")
        timestamp = time.time() if now is None else now
        expires_at = timestamp + ttl_seconds
        lease_id = uuid.uuid4().hex
        with self._lock, self.connection:
            self.connection.executemany(
                """
                INSERT INTO segment_leases (
                    lease_id, event_key, owner, segment_path,
                    acquired_at_unix, renewed_at_unix, expires_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        lease_id,
                        event_key,
                        owner,
                        path,
                        timestamp,
                        timestamp,
                        expires_at,
                    )
                    for path in paths
                ],
            )
        return lease_id

    def renew_segment_lease(
        self,
        lease_id: str,
        *,
        ttl_seconds: float,
        now: float | None = None,
    ) -> bool:
        """Extend an active lease; expired leases cannot be resurrected."""
        if ttl_seconds <= 0:
            raise ValueError("segment lease TTL must be positive")
        timestamp = time.time() if now is None else now
        with self._lock, self.connection:
            cursor = self.connection.execute(
                """
                UPDATE segment_leases
                SET renewed_at_unix = ?, expires_at_unix = ?
                WHERE lease_id = ? AND expires_at_unix > ?
                """,
                (timestamp, timestamp + ttl_seconds, lease_id, timestamp),
            )
        return cursor.rowcount > 0

    def release_segment_lease(self, lease_id: str) -> int:
        with self._lock, self.connection:
            cursor = self.connection.execute(
                "DELETE FROM segment_leases WHERE lease_id = ?", (lease_id,)
            )
        return cursor.rowcount

    def list_segment_leases(
        self,
        *,
        event_key: str | None = None,
        active_at: float | None = None,
    ) -> list[StoredSegmentLease]:
        clauses: list[str] = []
        values: list[Any] = []
        if event_key is not None:
            clauses.append("event_key = ?")
            values.append(event_key)
        if active_at is not None:
            clauses.append("expires_at_unix > ?")
            values.append(active_at)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            rows = self.connection.execute(
                f"""
                SELECT * FROM segment_leases {where}
                ORDER BY acquired_at_unix, lease_id, segment_path
                """,
                values,
            ).fetchall()
        return [self._decode_segment_lease_row(row) for row in rows]

    def protected_segment_paths(self, *, now: float | None = None) -> set[str]:
        timestamp = time.time() if now is None else now
        with self._lock:
            rows = self.connection.execute(
                """
                SELECT DISTINCT segment_path FROM segment_leases
                WHERE expires_at_unix > ?
                """,
                (timestamp,),
            ).fetchall()
        return {str(row["segment_path"]) for row in rows}

    def purge_expired_segment_leases(self, *, now: float | None = None) -> int:
        timestamp = time.time() if now is None else now
        with self._lock, self.connection:
            cursor = self.connection.execute(
                "DELETE FROM segment_leases WHERE expires_at_unix <= ?", (timestamp,)
            )
        return cursor.rowcount


class PipelineRuntime:
    """Coordinate durable state transitions with matching structured logs."""

    def __init__(self, database_path: Path, log_path: Path) -> None:
        self.store = TaskStateStore(database_path)
        self.logger = JsonlEventLogger(log_path)

    def close(self) -> None:
        self.store.close()

    def discover_task(self, **fields: Any) -> bool:
        event_data = fields["event_data"]
        event_key = str(event_data["event_key"])
        inserted = self.store.discover(**fields)
        if not inserted:
            existing = self.store.get(event_key)
            self.logger.log(
                "event_duplicate",
                event_key=event_key,
                match_id=fields["match_id"],
                existing_status=existing.status if existing else None,
            )
            return False
        self.logger.log(
            "event_discovered",
            event_key=event_key,
            match_id=fields["match_id"],
            code=event_data["code"],
            event_type=event_data["event_type"],
            minute=event_data.get("minute", ""),
            minute_extra=event_data.get("minute_extra", ""),
            person=event_data.get("person", ""),
            person_id=event_data.get("person_id", ""),
            team=event_data.get("team", ""),
            score=event_data.get("score", ""),
            reason=event_data.get("reason", ""),
            event_id=(event_data.get("metadata") or {}).get("event_id")
            or (event_data.get("metadata") or {}).get("eventId")
            or (event_data.get("metadata") or {}).get("id"),
            observed_stream_time_sec=fields["observed_stream_time"],
            clip_anchor_stream_time_sec=fields["clip_anchor_stream_time"],
        )
        self.transition(event_key, "pending", reason="event_accepted")
        return True

    def update_task_event(self, event_data: Mapping[str, Any]) -> bool:
        previous = self.store.get(str(event_data["event_key"]))
        if previous is None:
            return False
        updated = self.store.update_event_data(event_data)
        if updated is None or updated.event_data == previous.event_data:
            return False
        self.logger.log(
            "event_updated",
            event_key=updated.event_key,
            match_id=updated.match_id,
            code=updated.code,
            minute=updated.event_data.get("minute", ""),
            person=updated.event_data.get("person", ""),
            team=updated.event_data.get("team", ""),
            score=updated.event_data.get("score", ""),
        )
        return True

    def transition(
        self,
        event_key: str,
        new_status: str,
        *,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        reason: str | None = None,
    ) -> StoredTask:
        previous = self.store.get(event_key)
        if previous is None:
            raise KeyError(f"unknown event task: {event_key}")
        updated = self.store.transition(
            event_key,
            new_status,
            result=result,
            error=error,
            error_kind=error_kind,
        )
        self.logger.log(
            "task_transition",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            from_status=previous.status,
            to_status=new_status,
            attempt_count=updated.attempt_count,
            reason=reason,
            error=error,
            error_kind=error_kind,
        )
        if new_status == "encoded":
            encoded_result = dict(result or {})
            self.logger.log(
                "gif_ready",
                event_key=event_key,
                match_id=updated.match_id,
                code=updated.code,
                output=encoded_result.get("output"),
                bytes=encoded_result.get("bytes"),
                duration_sec=encoded_result.get("duration_sec"),
                encode_seconds=encoded_result.get("encode_seconds"),
                seconds_after_event_observed=encoded_result.get(
                    "seconds_after_event_observed"
                ),
                over_size_reference=encoded_result.get("over_size_reference"),
            )
        return updated

    def record_readiness_wait(
        self, event_key: str, error: str, *, error_kind: str
    ) -> StoredTask:
        updated = self.store.record_readiness_wait(
            event_key, error, error_kind=error_kind
        )
        self.logger.log(
            "buffer_readiness_wait",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            readiness_check_count=updated.readiness_check_count,
            next_attempt_at_unix=updated.next_attempt_at_unix,
            deadline_at_unix=updated.deadline_at_unix,
            error_kind=error_kind,
            error=error,
        )
        return updated

    def enqueue_vision_task(self, event_key: str, **fields: Any) -> bool:
        inserted = self.store.enqueue_vision_task(event_key, **fields)
        task = self.store.get_vision_task(event_key)
        assert task is not None
        self.logger.log(
            "vision_task_enqueued" if inserted else "vision_task_duplicate",
            event_key=event_key,
            match_id=task.match_id,
            code=task.code,
            status=task.status,
            artifact_kind=task.artifact_kind,
        )
        return inserted

    def start_vision(self, event_key: str) -> StoredVisionTask:
        """Compatibility helper for the single-worker visual refinement runner."""
        current = self.store.get_vision_task(event_key)
        if current is None:
            raise KeyError(f"unknown vision task: {event_key}")
        if current.status == "pending":
            return self.transition_vision_task(event_key, "locating")
        if current.status == "located":
            return self.transition_vision_task(event_key, "encoding")
        raise ValueError(f"vision task cannot start from {current.status}")

    def complete_vision(
        self, event_key: str, result: Mapping[str, Any]
    ) -> StoredVisionTask:
        """Persist a located anchor and refined GIF returned by a combined runner."""
        current = self.store.get_vision_task(event_key)
        if current is None:
            raise KeyError(f"unknown vision task: {event_key}")
        normalized = dict(result)
        if "anchor_stream_time" not in normalized:
            normalized["anchor_stream_time"] = normalized.get(
                "vision_anchor_stream_time_sec"
            )
        if current.status == "locating":
            current = self.transition_vision_task(
                event_key, "located", result=normalized
            )
        if current.status == "located":
            current = self.transition_vision_task(event_key, "encoding")
        if current.status != "encoding":
            raise ValueError(f"vision task cannot complete from {current.status}")
        return self.transition_vision_task(event_key, "encoded", result=normalized)

    def retry_vision(self, event_key: str, error: str) -> StoredVisionTask:
        """Return transient visual work to the nearest restartable stage."""
        current = self.store.get_vision_task(event_key)
        if current is None:
            raise KeyError(f"unknown vision task: {event_key}")
        target = "located" if current.status == "encoding" else "pending"
        return self.transition_vision_task(
            event_key, target, error=error, reason="visual_input_not_ready"
        )

    def fail_vision(self, event_key: str, error: str) -> StoredVisionTask:
        return self.transition_vision_task(
            event_key, "failed", error=error, reason="visual_refinement_failed"
        )

    def acquire_segment_lease(
        self,
        event_key: str,
        segment_paths: list[str] | tuple[str, ...] | set[str],
        *,
        owner: str = "vision-worker",
        expires_in_seconds: float,
    ) -> str:
        return self.store.acquire_segment_lease(
            event_key,
            segment_paths,
            owner=owner,
            ttl_seconds=expires_in_seconds,
        )

    def renew_segment_lease(
        self, lease_id: str, *, expires_in_seconds: float
    ) -> bool:
        return self.store.renew_segment_lease(
            lease_id, ttl_seconds=expires_in_seconds
        )

    def release_segment_lease(self, lease_id: str) -> int:
        return self.store.release_segment_lease(lease_id)

    def transition_vision_task(
        self,
        event_key: str,
        new_status: str,
        *,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        reason: str | None = None,
    ) -> StoredVisionTask:
        previous = self.store.get_vision_task(event_key)
        if previous is None:
            raise KeyError(f"unknown vision task: {event_key}")
        updated = self.store.transition_vision_task(
            event_key,
            new_status,
            result=result,
            error=error,
            error_kind=error_kind,
        )
        self.logger.log(
            "vision_task_transition",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            from_status=previous.status,
            to_status=new_status,
            locate_attempt_count=updated.locate_attempt_count,
            encode_attempt_count=updated.encode_attempt_count,
            reason=reason,
            error=error,
            error_kind=error_kind,
        )
        if new_status == "encoded":
            self.logger.log(
                "refined_gif_ready",
                event_key=event_key,
                match_id=updated.match_id,
                code=updated.code,
                output=updated.output_path,
                bytes=updated.output_bytes,
                anchor_stream_time=updated.located_anchor_stream_time,
                confidence=updated.confidence,
                model_name=updated.model_name,
                model_version=updated.model_version,
                model_weights_sha256=updated.model_weights_sha256,
            )
        return updated

    def record_vision_readiness_wait(
        self, event_key: str, error: str, *, error_kind: str
    ) -> StoredVisionTask:
        updated = self.store.record_vision_readiness_wait(
            event_key, error, error_kind=error_kind
        )
        self.logger.log(
            "vision_buffer_readiness_wait",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            status=updated.status,
            readiness_check_count=updated.readiness_check_count,
            next_attempt_at_unix=updated.next_attempt_at_unix,
            deadline_at_unix=updated.deadline_at_unix,
            error_kind=error_kind,
            error=error,
        )
        return updated

    def recover_incomplete_vision(self, match_id: str) -> list[StoredVisionTask]:
        """Make interrupted visual stages runnable without duplicating artifacts."""
        recovered: list[StoredVisionTask] = []
        for task in self.store.list_incomplete_vision_tasks(match_id):
            previous_status = task.status
            if task.status == "locating":
                task = self.transition_vision_task(
                    task.event_key,
                    "pending",
                    reason="process_restart_recovery",
                )
            elif task.status == "encoding":
                task = self.transition_vision_task(
                    task.event_key,
                    "located",
                    reason="process_restart_recovery",
                )
            self.logger.log(
                "vision_task_recovered",
                event_key=task.event_key,
                match_id=task.match_id,
                code=task.code,
                previous_status=previous_status,
                status=task.status,
                locate_attempt_count=task.locate_attempt_count,
                encode_attempt_count=task.encode_attempt_count,
            )
            recovered.append(task)
        return recovered

    def recover_incomplete(self, match_id: str) -> list[StoredTask]:
        all_tasks = self.store.list_for_match(match_id)
        canonical: list[StoredTask] = []
        duplicate_keys: dict[str, str] = {}
        for task in all_tasks:
            if task.suppressed_by_event_key:
                continue
            candidates = [
                item
                for item in canonical
                if _has_event_revision_evidence(item.event_data, task.event_data)
                and events_represent_same_incident(item.event_data, task.event_data)
            ]
            if len(candidates) != 1:
                canonical.append(task)
                continue
            primary = candidates[0]
            if task.event_key == primary.event_key:
                continue
            merged_event = merge_event_metadata(primary.event_data, task.event_data)
            merged_event["event_key"] = primary.event_key
            updated_primary = self.store.update_event_data(merged_event)
            if updated_primary is not None:
                canonical[canonical.index(primary)] = updated_primary
            if task.status in INCOMPLETE_STATUSES:
                duplicate_keys[task.event_key] = primary.event_key

        for duplicate_key, primary_key in duplicate_keys.items():
            duplicate = self.store.get(duplicate_key)
            if duplicate is None or duplicate.status not in INCOMPLETE_STATUSES:
                continue
            updated = self.store.suppress_task(duplicate_key, primary_key)
            if updated is None:
                continue
            self.logger.log(
                "event_superseded",
                event_key=duplicate_key,
                match_id=updated.match_id,
                code=updated.code,
                canonical_event_key=primary_key,
                reason="same_incident_recovery",
            )

        recovered: list[StoredTask] = []
        for task in self.store.list_incomplete(match_id):
            previous_status = task.status
            if task.status in ("discovered", "encoding"):
                task = self.transition(
                    task.event_key,
                    "pending",
                    reason="process_restart_recovery",
                )
            self.logger.log(
                "task_recovered",
                event_key=task.event_key,
                match_id=task.match_id,
                code=task.code,
                previous_status=previous_status,
                status=task.status,
                attempt_count=task.attempt_count,
            )
            recovered.append(task)
        return recovered

    def log_api_error(
        self, *, match_id: str, error: str, poll_count: int, error_count: int
    ) -> None:
        self.logger.log(
            "api_error",
            match_id=match_id,
            error=error,
            poll_count=poll_count,
            error_count=error_count,
        )

    def log_ingest_restart(
        self,
        *,
        match_id: str,
        return_code: int,
        restart_count: int,
        delay_seconds: float,
    ) -> None:
        self.logger.log(
            "ingest_restart",
            match_id=match_id,
            return_code=return_code,
            restart_count=restart_count,
            delay_seconds=delay_seconds,
        )
