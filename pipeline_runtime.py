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
from typing import Any, Collection, Mapping

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
VISION_ARTIFACT_KINDS = ("ocr_window", "tdeed_refined")
DEFAULT_VISION_ARTIFACT_KIND = "tdeed_refined"
LEGACY_VISION_ARTIFACT_KIND = "refined"
DEFAULT_GIF_DEADLINE_SECONDS = 55.0
DEFAULT_VISION_DEADLINE_SECONDS = 60.0
READINESS_RETRY_DELAYS_SECONDS = (2.0, 4.0, 8.0)
DEFAULT_HALFTIME_BREAK_SECONDS = 15.0 * 60.0
_ALLOWED_VISION_TRANSITIONS = {
    "pending": {"locating", "failed"},
    "locating": {"pending", "located", "failed"},
    "located": {"pending", "encoding", "failed"},
    "encoding": {"pending", "located", "encoded", "failed"},
    # A late authoritative shotmap second may supersede a minute-only visual
    # artifact. The upgrade path records the prior result and requeues only
    # the visual artifact; the default GIF task remains terminal/independent.
    "encoded": {"pending"},
    "failed": {"pending"},
}


def normalize_vision_artifact_kind(value: str | None) -> str:
    """Return the durable artifact kind while accepting the legacy name."""
    normalized = str(value or DEFAULT_VISION_ARTIFACT_KIND).strip().lower()
    if normalized == LEGACY_VISION_ARTIFACT_KIND:
        return DEFAULT_VISION_ARTIFACT_KIND
    if normalized not in VISION_ARTIFACT_KINDS:
        supported = ", ".join(VISION_ARTIFACT_KINDS)
        raise ValueError(f"vision artifact kind must be one of: {supported}")
    return normalized


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


def _validated_vision_anchor(value: Any) -> float:
    anchor = _finite_float(value, "vision anchor_stream_time")
    if anchor < 0:
        raise ValueError("vision anchor_stream_time must be non-negative")
    return anchor


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
    failure_stage: str | None
    failure_reason: str | None
    location_metadata: dict[str, Any]
    window_metadata: dict[str, Any]


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
class StoredShotmapState:
    match_id: str
    initialized: bool
    initialized_at_unix: float | None
    updated_at_unix: float | None
    last_snapshot: dict[str, Any] | None
    last_snapshot_at_unix: float | None
    last_response_at_unix: float | None
    diagnostics: dict[str, Any]
    seen_fingerprints: frozenset[str]


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

    @staticmethod
    def _vision_tasks_table_sql(
        table_name: str,
        vision_statuses: str,
        *,
        if_not_exists: bool,
    ) -> str:
        if table_name not in {"vision_tasks", "vision_tasks_legacy"}:
            raise ValueError("unsupported vision task table name")
        create_guard = "IF NOT EXISTS " if if_not_exists else ""
        artifact_kinds = ",".join(f"'{kind}'" for kind in VISION_ARTIFACT_KINDS)
        return f"""
            CREATE TABLE {create_guard}{table_name} (
                event_key TEXT NOT NULL,
                match_id TEXT NOT NULL,
                code TEXT NOT NULL,
                event_type TEXT NOT NULL,
                artifact_kind TEXT NOT NULL
                    CHECK (artifact_kind IN ({artifact_kinds})),
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
                last_error_kind TEXT,
                failure_stage TEXT,
                failure_reason TEXT,
                location_json TEXT NOT NULL DEFAULT '{{}}',
                window_json TEXT NOT NULL DEFAULT '{{}}',
                PRIMARY KEY (event_key, artifact_kind)
            );
        """

    def _create_schema(self) -> None:
        statuses = ",".join(f"'{status}'" for status in TASK_STATUSES)
        vision_statuses = ",".join(
            f"'{status}'" for status in VISION_TASK_STATUSES
        )
        vision_tasks_sql = self._vision_tasks_table_sql(
            "vision_tasks", vision_statuses, if_not_exists=True
        )
        with self._lock, self.connection:
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
            CREATE TABLE IF NOT EXISTS shotmap_feed_state (
                match_id TEXT PRIMARY KEY,
                initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1)),
                initialized_at_unix REAL,
                updated_at_unix REAL NOT NULL,
                last_valid_response_at_unix REAL,
                last_snapshot_json TEXT,
                last_snapshot_at_unix REAL,
                last_response_at_unix REAL,
                diagnostics_json TEXT NOT NULL DEFAULT '{{}}'
            );
            CREATE TABLE IF NOT EXISTS shotmap_feed_events (
                match_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL,
                event_json TEXT NOT NULL DEFAULT '{{}}',
                first_seen_at_unix REAL NOT NULL,
                last_seen_at_unix REAL NOT NULL,
                PRIMARY KEY (match_id, fingerprint)
            );
            CREATE INDEX IF NOT EXISTS shotmap_feed_events_match_seen
                ON shotmap_feed_events(match_id, first_seen_at_unix);
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
            {vision_tasks_sql}
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
                "artifact_kind": "TEXT NOT NULL DEFAULT 'refined'",
                "next_attempt_at_unix": "REAL",
                "deadline_at_unix": "REAL",
                "readiness_check_count": "INTEGER NOT NULL DEFAULT 0",
                "last_error_kind": "TEXT",
                "failure_stage": "TEXT",
                "failure_reason": "TEXT",
                "location_json": "TEXT NOT NULL DEFAULT '{}'",
                "window_json": "TEXT NOT NULL DEFAULT '{}'",
            }
            for name, definition in vision_migrations.items():
                if name not in vision_columns:
                    self.connection.execute(
                        f"ALTER TABLE vision_tasks ADD COLUMN {name} {definition}"
                    )
            vision_table = self.connection.execute(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
                ("vision_tasks",),
            ).fetchone()
            vision_info = self.connection.execute(
                "PRAGMA table_info(vision_tasks)"
            ).fetchall()
            vision_primary_key = [
                str(row["name"])
                for row in sorted(vision_info, key=lambda row: int(row["pk"]))
                if int(row["pk"]) > 0
            ]
            vision_table_sql = str(vision_table["sql"] or "") if vision_table else ""
            if (
                vision_primary_key != ["event_key", "artifact_kind"]
                or "tdeed_refined" not in vision_table_sql
                or "ocr_window" not in vision_table_sql
            ):
                self.connection.execute("DROP INDEX IF EXISTS vision_tasks_match_status")
                self.connection.execute(
                    "ALTER TABLE vision_tasks RENAME TO vision_tasks_legacy"
                )
                self.connection.execute(
                    self._vision_tasks_table_sql(
                        "vision_tasks", vision_statuses, if_not_exists=False
                    )
                )
                self.connection.execute(
                    """
                    INSERT INTO vision_tasks (
                        event_key, match_id, code, event_type, artifact_kind, status,
                        source_anchor_stream_time, source_anchor_source_time,
                        search_start_stream_time, search_end_stream_time,
                        clip_before_seconds, clip_after_seconds,
                        model_name, model_version, model_weights_sha256,
                        created_at_unix, updated_at_unix,
                        locating_started_at_unix, located_at_unix,
                        encoding_started_at_unix, encoded_at_unix, failed_at_unix,
                        locate_attempt_count, encode_attempt_count,
                        located_anchor_stream_time, located_anchor_source_time,
                        confidence, inference_seconds, output_path, output_bytes,
                        result_json, error, next_attempt_at_unix, deadline_at_unix,
                        readiness_check_count, last_error_kind,
                        failure_stage, failure_reason, location_json, window_json
                    )
                    SELECT
                        event_key, match_id, code, event_type,
                        CASE artifact_kind
                            WHEN 'refined' THEN 'tdeed_refined'
                            ELSE artifact_kind
                        END,
                        status, source_anchor_stream_time, source_anchor_source_time,
                        search_start_stream_time, search_end_stream_time,
                        clip_before_seconds, clip_after_seconds,
                        model_name, model_version, model_weights_sha256,
                        created_at_unix, updated_at_unix,
                        locating_started_at_unix, located_at_unix,
                        encoding_started_at_unix, encoded_at_unix, failed_at_unix,
                        locate_attempt_count, encode_attempt_count,
                        located_anchor_stream_time, located_anchor_source_time,
                        confidence, inference_seconds, output_path, output_bytes,
                        result_json, error, next_attempt_at_unix, deadline_at_unix,
                        readiness_check_count, last_error_kind,
                        failure_stage, failure_reason, location_json, window_json
                    FROM vision_tasks_legacy
                    """
                )
                self.connection.execute("DROP TABLE vision_tasks_legacy")
            self.connection.execute(
                """
                CREATE INDEX IF NOT EXISTS vision_tasks_match_status
                ON vision_tasks(match_id, status)
                """
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
            artifact_kind=normalize_vision_artifact_kind(row["artifact_kind"]),
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
            failure_stage=row["failure_stage"],
            failure_reason=row["failure_reason"],
            location_metadata=json.loads(row["location_json"] or "{}"),
            window_metadata=json.loads(row["window_json"] or "{}"),
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
        replace_fields: Collection[str] = (),
    ) -> StoredTask | None:
        """Update feed metadata, optionally replacing selected nullable fields."""
        event_key = str(event_data["event_key"])
        replacement_keys = {str(key) for key in replace_fields}
        if "event_key" in replacement_keys:
            raise ValueError("event_key cannot be replaced")
        missing_replacements = replacement_keys.difference(event_data)
        if missing_replacements:
            missing = ", ".join(sorted(missing_replacements))
            raise ValueError(f"replacement fields are missing from event data: {missing}")
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get(event_key)
            if current is None:
                return None
            merged = merge_event_metadata(current.event_data, event_data)
            for key in replacement_keys:
                merged[key] = event_data[key]
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

    @staticmethod
    def is_valid_shotmap_payload(payload: Any) -> bool:
        """Return whether *payload* is a decoded, usable shotmap response.

        The feed cursor is deliberately stricter than a generic HTTP success:
        only a JSON object with a list-valued ``shots`` member is valid. An
        empty list is valid and is allowed to establish an empty baseline.
        """
        if not isinstance(payload, Mapping):
            return False
        return isinstance(payload.get("shots"), list)

    def load_shotmap_cursor(self, match_id: str) -> tuple[bool, set[str]]:
        """Return shotmap initialization state and durable fingerprints."""
        normalized_match_id = str(match_id)
        with self._lock:
            state = self.connection.execute(
                "SELECT initialized FROM shotmap_feed_state WHERE match_id = ?",
                (normalized_match_id,),
            ).fetchone()
            rows = self.connection.execute(
                """
                SELECT fingerprint FROM shotmap_feed_events
                WHERE match_id = ? ORDER BY fingerprint
                """,
                (normalized_match_id,),
            ).fetchall()
        return bool(state and int(state["initialized"])), {
            str(row["fingerprint"]) for row in rows
        }

    def load_shotmap_fingerprints(self, match_id: str) -> set[str]:
        """Load only the fingerprints remembered for one match."""
        return self.load_shotmap_cursor(match_id)[1]

    @staticmethod
    def _decode_json_object(raw: str | None) -> dict[str, Any]:
        if not raw:
            return {}
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return {}
        return dict(value) if isinstance(value, Mapping) else {}

    def load_shotmap_state(self, match_id: str) -> StoredShotmapState:
        """Load the persisted snapshot, diagnostics and cursor for a match."""
        normalized_match_id = str(match_id)
        with self._lock:
            row = self.connection.execute(
                """
                SELECT initialized, initialized_at_unix, updated_at_unix,
                       last_snapshot_json, last_snapshot_at_unix,
                       last_response_at_unix, diagnostics_json
                FROM shotmap_feed_state WHERE match_id = ?
                """,
                (normalized_match_id,),
            ).fetchone()
            fingerprint_rows = self.connection.execute(
                """
                SELECT fingerprint FROM shotmap_feed_events
                WHERE match_id = ? ORDER BY fingerprint
                """,
                (normalized_match_id,),
            ).fetchall()
        snapshot: dict[str, Any] | None = None
        if row is not None and row["last_snapshot_json"]:
            try:
                decoded = json.loads(row["last_snapshot_json"])
            except (TypeError, ValueError):
                decoded = None
            if isinstance(decoded, Mapping):
                snapshot = dict(decoded)
        return StoredShotmapState(
            match_id=normalized_match_id,
            initialized=bool(row and int(row["initialized"])),
            initialized_at_unix=(
                float(row["initialized_at_unix"])
                if row is not None and row["initialized_at_unix"] is not None
                else None
            ),
            updated_at_unix=(
                float(row["updated_at_unix"])
                if row is not None and row["updated_at_unix"] is not None
                else None
            ),
            last_snapshot=snapshot,
            last_snapshot_at_unix=(
                float(row["last_snapshot_at_unix"])
                if row is not None and row["last_snapshot_at_unix"] is not None
                else None
            ),
            last_response_at_unix=(
                float(row["last_response_at_unix"])
                if row is not None and row["last_response_at_unix"] is not None
                else None
            ),
            diagnostics=(
                self._decode_json_object(row["diagnostics_json"])
                if row is not None
                else {}
            ),
            seen_fingerprints=frozenset(
                str(item["fingerprint"]) for item in fingerprint_rows
            ),
        )

    def load_shotmap_snapshot(
        self, match_id: str
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """Load the last valid shotmap snapshot and latest diagnostics."""
        state = self.load_shotmap_state(match_id)
        return state.last_snapshot, state.diagnostics

    def upsert_shotmap_snapshot(
        self,
        match_id: str,
        payload: Any,
        *,
        diagnostics: Mapping[str, Any] | None = None,
        observed_at_unix: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Persist one shotmap response and return whether it was valid.

        Invalid responses update only response diagnostics and timestamps. In
        particular they cannot create an initialized cursor or overwrite the
        last valid snapshot. Call :meth:`mark_shotmap_seen` separately after
        comparing the response with the durable fingerprint cursor.
        """
        normalized_match_id = str(match_id)
        timestamp = time.time() if now is None else float(now)
        response_at = timestamp if observed_at_unix is None else float(observed_at_unix)
        valid = self.is_valid_shotmap_payload(payload)
        encoded_payload: str | None = None
        validation_error: str | None = None
        if valid:
            try:
                encoded_payload = json.dumps(
                    dict(payload), ensure_ascii=False, separators=(",", ":")
                )
            except (TypeError, ValueError):
                valid = False
                validation_error = "payload_not_json_serializable"
        elif not isinstance(payload, Mapping):
            validation_error = "payload_not_json_object"
        else:
            validation_error = "shots_missing_or_not_list"

        diagnostic_value: dict[str, Any] = dict(diagnostics or {})
        diagnostic_value["valid"] = valid
        if validation_error:
            diagnostic_value["validation_error"] = validation_error
        try:
            encoded_diagnostics = json.dumps(
                diagnostic_value, ensure_ascii=False, separators=(",", ":")
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("shotmap diagnostics must be JSON serializable") from exc

        with self._lock, self.connection:
            row = self.connection.execute(
                """
                SELECT initialized, initialized_at_unix
                FROM shotmap_feed_state WHERE match_id = ?
                """,
                (normalized_match_id,),
            ).fetchone()
            initialized = bool(row and int(row["initialized"]))
            initialized_at = (
                float(row["initialized_at_unix"])
                if row is not None and row["initialized_at_unix"] is not None
                else None
            )
            if valid and not initialized:
                initialized = True
                initialized_at = timestamp
            if row is None:
                self.connection.execute(
                    """
                    INSERT INTO shotmap_feed_state (
                        match_id, initialized, initialized_at_unix,
                        updated_at_unix, last_snapshot_json,
                        last_snapshot_at_unix, last_response_at_unix,
                        diagnostics_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized_match_id,
                        int(initialized),
                        initialized_at,
                        timestamp,
                        encoded_payload if valid else None,
                        response_at if valid else None,
                        response_at,
                        encoded_diagnostics,
                    ),
                )
            elif valid:
                self.connection.execute(
                    """
                    UPDATE shotmap_feed_state
                    SET initialized = ?, initialized_at_unix = ?,
                        updated_at_unix = ?, last_snapshot_json = ?,
                        last_snapshot_at_unix = ?, last_response_at_unix = ?,
                        diagnostics_json = ?
                    WHERE match_id = ?
                    """,
                    (
                        int(initialized),
                        initialized_at,
                        timestamp,
                        encoded_payload,
                        response_at,
                        response_at,
                        encoded_diagnostics,
                        normalized_match_id,
                    ),
                )
            else:
                self.connection.execute(
                    """
                    UPDATE shotmap_feed_state
                    SET updated_at_unix = ?, last_response_at_unix = ?,
                        diagnostics_json = ?
                    WHERE match_id = ?
                    """,
                    (
                        timestamp,
                        response_at,
                        encoded_diagnostics,
                        normalized_match_id,
                    ),
                )
        return valid

    def mark_shotmap_seen(
        self,
        match_id: str,
        fingerprints: Collection[str],
        *,
        events: Mapping[str, Mapping[str, Any]] | None = None,
        now: float | None = None,
    ) -> set[str]:
        """Durably add shotmap fingerprints and return newly inserted keys.

        This method never initializes the feed. Call it after a valid
        :meth:`upsert_shotmap_snapshot`; accidental calls before initialization
        leave the cursor uninitialized while retaining the fingerprints.
        """
        normalized_match_id = str(match_id)
        timestamp = time.time() if now is None else float(now)
        unique = {str(value) for value in fingerprints if str(value)}
        if not unique:
            return set()
        event_values: list[tuple[str, str]] = []
        for fingerprint in sorted(unique):
            event_data = (events or {}).get(fingerprint, {})
            try:
                encoded = json.dumps(
                    dict(event_data), ensure_ascii=False, separators=(",", ":")
                )
            except (TypeError, ValueError) as exc:
                raise ValueError("shotmap event data must be JSON serializable") from exc
            event_values.append((fingerprint, encoded))
        inserted: set[str] = set()
        with self._lock, self.connection:
            self.connection.execute(
                """
                INSERT OR IGNORE INTO shotmap_feed_state (
                    match_id, initialized, initialized_at_unix,
                    updated_at_unix, diagnostics_json
                ) VALUES (?, 0, NULL, ?, '{}')
                """,
                (normalized_match_id, timestamp),
            )
            existing = {
                str(row["fingerprint"])
                for row in self.connection.execute(
                    """
                    SELECT fingerprint FROM shotmap_feed_events
                    WHERE match_id = ? AND fingerprint IN (%s)
                    """ % ",".join("?" for _ in unique),
                    (normalized_match_id, *sorted(unique)),
                ).fetchall()
            }
            self.connection.executemany(
                """
                INSERT INTO shotmap_feed_events (
                    match_id, fingerprint, event_json,
                    first_seen_at_unix, last_seen_at_unix
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(match_id, fingerprint) DO UPDATE SET
                    event_json = CASE
                        WHEN excluded.event_json != '{}' THEN excluded.event_json
                        ELSE shotmap_feed_events.event_json
                    END,
                    last_seen_at_unix = excluded.last_seen_at_unix
                """,
                [
                    (
                        normalized_match_id,
                        fingerprint,
                        encoded,
                        timestamp,
                        timestamp,
                    )
                    for fingerprint, encoded in event_values
                ],
            )
            inserted = unique - existing
            self.connection.execute(
                """
                UPDATE shotmap_feed_state SET updated_at_unix = ?
                WHERE match_id = ?
                """,
                (timestamp, normalized_match_id),
            )
        return inserted

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
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        search_start_stream_time: float,
        search_end_stream_time: float,
        clip_before_seconds: float,
        clip_after_seconds: float,
        model_name: str | None = None,
        model_version: str | None = None,
        model_weights_sha256: str | None = None,
        location_metadata: Mapping[str, Any] | None = None,
        window_metadata: Mapping[str, Any] | None = None,
        deadline_at_unix: float | None = None,
        now: float | None = None,
    ) -> bool:
        """Create one independently recoverable visual artifact task."""
        if search_end_stream_time < search_start_stream_time:
            raise ValueError("vision search end must not precede search start")
        if clip_before_seconds < 0 or clip_after_seconds < 0:
            raise ValueError("vision clip durations must be non-negative")
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        initial_location = dict(location_metadata or {})
        initial_window = {
            "search_start_stream_time": search_start_stream_time,
            "search_end_stream_time": search_end_stream_time,
            "clip_before_seconds": clip_before_seconds,
            "clip_after_seconds": clip_after_seconds,
            **dict(window_metadata or {}),
        }
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
                    next_attempt_at_unix, deadline_at_unix,
                    location_json, window_json
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    event["match_id"],
                    event["code"],
                    event["event_type"],
                    normalized_artifact_kind,
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
                    json.dumps(
                        initial_location, ensure_ascii=False, separators=(",", ":")
                    ),
                    json.dumps(
                        initial_window, ensure_ascii=False, separators=(",", ":")
                    ),
                ),
            )
        return cursor.rowcount == 1

    def get_vision_task(
        self,
        event_key: str,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
    ) -> StoredVisionTask | None:
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        with self._lock:
            row = self.connection.execute(
                """
                SELECT * FROM vision_tasks
                WHERE event_key = ? AND artifact_kind = ?
                """,
                (event_key, normalized_artifact_kind),
            ).fetchone()
        return self._decode_vision_row(row) if row is not None else None

    def list_vision_tasks(
        self,
        match_id: str,
        artifact_kind: str | None = None,
    ) -> list[StoredVisionTask]:
        normalized_artifact_kind = (
            normalize_vision_artifact_kind(artifact_kind)
            if artifact_kind is not None
            else None
        )
        with self._lock:
            if normalized_artifact_kind is None:
                rows = self.connection.execute(
                    """
                    SELECT * FROM vision_tasks
                    WHERE match_id = ?
                    ORDER BY created_at_unix, event_key, artifact_kind
                    """,
                    (match_id,),
                ).fetchall()
            else:
                rows = self.connection.execute(
                    """
                    SELECT * FROM vision_tasks
                    WHERE match_id = ? AND artifact_kind = ?
                    ORDER BY created_at_unix, event_key, artifact_kind
                    """,
                    (match_id, normalized_artifact_kind),
                ).fetchall()
        return [self._decode_vision_row(row) for row in rows]

    def list_incomplete_vision_tasks(
        self,
        match_id: str,
        artifact_kind: str | None = None,
    ) -> list[StoredVisionTask]:
        placeholders = ",".join("?" for _ in VISION_INCOMPLETE_STATUSES)
        normalized_artifact_kind = (
            normalize_vision_artifact_kind(artifact_kind)
            if artifact_kind is not None
            else None
        )
        with self._lock:
            artifact_filter = (
                " AND artifact_kind = ?" if normalized_artifact_kind is not None else ""
            )
            parameters: tuple[Any, ...] = (
                match_id,
                *VISION_INCOMPLETE_STATUSES,
                *((normalized_artifact_kind,) if normalized_artifact_kind else ()),
            )
            rows = self.connection.execute(
                f"""
                SELECT * FROM vision_tasks
                WHERE match_id = ? AND status IN ({placeholders}){artifact_filter}
                ORDER BY created_at_unix, event_key, artifact_kind
                """,
                parameters,
            ).fetchall()
        return [self._decode_vision_row(row) for row in rows]

    def transition_vision_task(
        self,
        event_key: str,
        new_status: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        failure_stage: str | None = None,
        failure_reason: str | None = None,
        location_metadata: Mapping[str, Any] | None = None,
        window_metadata: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> StoredVisionTask:
        """Durably advance one artifact without touching sibling artifact rows."""
        if new_status not in VISION_TASK_STATUSES:
            raise ValueError(f"unknown vision task status: {new_status}")
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get_vision_task(event_key, normalized_artifact_kind)
            if current is None:
                raise KeyError(
                    f"unknown vision task: {event_key} ({normalized_artifact_kind})"
                )
            if new_status not in _ALLOWED_VISION_TRANSITIONS[current.status]:
                raise ValueError(
                    f"invalid vision task transition: {current.status} -> {new_status}"
                )

            merged_result = dict(current.result)
            if result is not None:
                merged_result.update(result)
            merged_location = dict(current.location_metadata)
            result_location = merged_result.get("location_metadata")
            if isinstance(result_location, Mapping):
                merged_location.update(result_location)
            if location_metadata is not None:
                merged_location.update(location_metadata)
            if new_status in ("located", "encoding", "encoded"):
                for key in (
                    "anchor_stream_time",
                    "anchor_source_time",
                    "confidence",
                    "inference_seconds",
                    "locator_method",
                    "model_name",
                    "model_version",
                    "model_weights_sha256",
                ):
                    value = merged_result.get(key)
                    if value is not None:
                        merged_location[key] = value
            merged_window = dict(current.window_metadata)
            result_window = merged_result.get("window_metadata")
            if isinstance(result_window, Mapping):
                merged_window.update(result_window)
            search_window = merged_result.get("search_window")
            if isinstance(search_window, Mapping):
                merged_window["search_window"] = dict(search_window)
            fragment_window = merged_result.get("fragment_window")
            if isinstance(fragment_window, Mapping):
                merged_window["fragment_window"] = dict(fragment_window)
            if window_metadata is not None:
                merged_window.update(window_metadata)

            structured_failure = (
                result.get("failure_reason") if result is not None else None
            )
            derived_failure_stage: str | None = None
            derived_failure_reason: str | None = None
            if isinstance(structured_failure, Mapping):
                if structured_failure.get("stage") is not None:
                    derived_failure_stage = str(structured_failure["stage"])
                if structured_failure.get("message") is not None:
                    derived_failure_reason = str(structured_failure["message"])
            elif structured_failure is not None:
                derived_failure_reason = str(structured_failure)
            has_failure_details = any(
                value is not None
                for value in (
                    failure_stage,
                    failure_reason,
                    derived_failure_stage,
                    derived_failure_reason,
                )
            )
            if new_status == "failed" or has_failure_details:
                stored_failure_stage = (
                    failure_stage or derived_failure_stage or error_kind
                )
                stored_failure_reason = (
                    failure_reason or derived_failure_reason or error
                )
            elif current.status == "failed" or new_status == "encoded":
                stored_failure_stage = None
                stored_failure_reason = None
            else:
                stored_failure_stage = current.failure_stage
                stored_failure_reason = current.failure_reason
            located_anchor = merged_result.get("anchor_stream_time")
            if located_anchor is None:
                located_anchor = current.located_anchor_stream_time
            if new_status in ("located", "encoding", "encoded"):
                if located_anchor is None:
                    raise ValueError(
                        f"vision task must have an anchor before entering {new_status}"
                    )
                located_anchor = _validated_vision_anchor(located_anchor)
                # Keep JSON and the dedicated column aligned when a later result
                # omits the anchor or explicitly reports it as null.
                merged_result["anchor_stream_time"] = located_anchor
                merged_location["anchor_stream_time"] = located_anchor
            if new_status == "encoded" and not (
                merged_result.get("output") or current.output_path
            ):
                raise ValueError("encoded vision task must have an output path")

            assignments = [
                "status = ?",
                "updated_at_unix = ?",
                "result_json = ?",
                "location_json = ?",
                "window_json = ?",
                "failure_stage = ?",
                "failure_reason = ?",
            ]
            values: list[Any] = [
                new_status,
                timestamp,
                json.dumps(merged_result, ensure_ascii=False, separators=(",", ":")),
                json.dumps(
                    merged_location, ensure_ascii=False, separators=(",", ":")
                ),
                json.dumps(merged_window, ensure_ascii=False, separators=(",", ":")),
                stored_failure_stage,
                stored_failure_reason,
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
            values.extend((event_key, normalized_artifact_kind))
            with self.connection:
                self.connection.execute(
                    f"""
                    UPDATE vision_tasks SET {', '.join(assignments)}
                    WHERE event_key = ? AND artifact_kind = ?
                    """,
                    values,
                )
            updated = self.get_vision_task(event_key, normalized_artifact_kind)
        assert updated is not None
        return updated

    def record_vision_readiness_wait(
        self,
        event_key: str,
        error: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        error_kind: str,
        result: Mapping[str, Any] | None = None,
        window_metadata: Mapping[str, Any] | None = None,
        next_attempt_at_unix: float | None = None,
        deadline_at_unix: float | None = None,
        now: float | None = None,
    ) -> StoredVisionTask:
        """Schedule visual input rechecks independently from model/encode attempts."""
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        timestamp = time.time() if now is None else now
        with self._lock:
            current = self.get_vision_task(event_key, normalized_artifact_kind)
            if current is None:
                raise KeyError(
                    f"unknown vision task: {event_key} ({normalized_artifact_kind})"
                )
            if current.status not in {"pending", "located"}:
                raise ValueError(
                    f"cannot schedule visual readiness retry from {current.status}"
                )
            delay = _readiness_retry_delay(current.readiness_check_count)
            merged_result = dict(current.result)
            if result is not None:
                merged_result.update(result)
            merged_window = dict(current.window_metadata)
            if window_metadata is not None:
                merged_window.update(window_metadata)
            effective_deadline = (
                current.deadline_at_unix
                if deadline_at_unix is None
                else _finite_float(deadline_at_unix, "vision wait deadline")
            )
            scheduled_attempt = (
                timestamp + delay
                if next_attempt_at_unix is None
                else _finite_float(
                    next_attempt_at_unix,
                    "vision wait next attempt",
                )
            )
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE vision_tasks SET
                        updated_at_unix = ?,
                        next_attempt_at_unix = ?,
                        deadline_at_unix = ?,
                        readiness_check_count = readiness_check_count + 1,
                        last_error_kind = ?,
                        error = ?,
                        result_json = ?,
                        window_json = ?
                    WHERE event_key = ? AND artifact_kind = ?
                    """,
                    (
                        timestamp,
                        min(scheduled_attempt, effective_deadline),
                        effective_deadline,
                        error_kind,
                        error,
                        json.dumps(
                            merged_result, ensure_ascii=False, separators=(",", ":")
                        ),
                        json.dumps(
                            merged_window, ensure_ascii=False, separators=(",", ":")
                        ),
                        event_key,
                        normalized_artifact_kind,
                    ),
                )
            updated = self.get_vision_task(event_key, normalized_artifact_kind)
        assert updated is not None
        return updated

    def record_vision_queue_phase(
        self,
        event_key: str,
        phase: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        queued_at_unix: float | None = None,
        now: float | None = None,
    ) -> StoredVisionTask:
        """Persist queue timing and exclude actual queue wait from the deadline."""
        if phase not in {"queued", "acquired", "executing"}:
            raise ValueError(f"unknown vision queue phase: {phase}")
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        timestamp = (
            time.time()
            if now is None
            else _finite_float(now, "queue phase time")
        )
        with self._lock:
            current = self.get_vision_task(event_key, normalized_artifact_kind)
            if current is None:
                raise KeyError(
                    f"unknown vision task: {event_key} ({normalized_artifact_kind})"
                )
            queue_timing = dict(current.window_metadata.get("queue_timing") or {})
            deadline = current.deadline_at_unix
            if phase == "queued":
                total_queue_wait = float(
                    queue_timing.get("total_queue_wait_seconds") or 0.0
                )
                queue_timing = {
                    "phase": phase,
                    "queued_at_unix": timestamp,
                    "queue_attempt_count": int(
                        queue_timing.get("queue_attempt_count") or 0
                    ) + 1,
                    "deadline_before_queue_wait": deadline,
                    "total_queue_wait_seconds": total_queue_wait,
                }
            else:
                submitted_at = (
                    _finite_float(queued_at_unix, "queued_at_unix")
                    if queued_at_unix is not None
                    else float(queue_timing.get("queued_at_unix") or timestamp)
                )
                queue_wait = max(0.0, timestamp - submitted_at)
                queue_timing.update(
                    {
                        "phase": phase,
                        "queued_at_unix": submitted_at,
                    }
                )
                if phase == "acquired":
                    already_acquired = "acquired_at_unix" in queue_timing
                    deadline_extension = 0.0 if already_acquired else queue_wait
                    deadline += deadline_extension
                    total_queue_wait = float(
                        queue_timing.get("total_queue_wait_seconds") or 0.0
                    ) + deadline_extension
                    queue_timing.update(
                        {
                            "acquired_at_unix": timestamp,
                            "queue_wait_seconds": queue_wait,
                            "deadline_extension_seconds": deadline_extension,
                            "total_queue_wait_seconds": total_queue_wait,
                            "deadline_after_queue_wait": deadline,
                        }
                    )
                else:
                    queue_timing["execution_started_at_unix"] = timestamp
            merged_window = dict(current.window_metadata)
            merged_window["queue_timing"] = queue_timing
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE vision_tasks SET
                        updated_at_unix = ?, deadline_at_unix = ?, window_json = ?
                    WHERE event_key = ? AND artifact_kind = ?
                    """,
                    (
                        timestamp,
                        deadline,
                        json.dumps(
                            merged_window, ensure_ascii=False, separators=(",", ":")
                        ),
                        event_key,
                        normalized_artifact_kind,
                    ),
                )
            updated = self.get_vision_task(event_key, normalized_artifact_kind)
        assert updated is not None
        return updated

    def acquire_segment_lease(
        self,
        event_key: str,
        segment_paths: list[str] | tuple[str, ...] | set[str],
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
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
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        if self.get_vision_task(event_key, normalized_artifact_kind) is None:
            raise KeyError(
                f"unknown vision task: {event_key} ({normalized_artifact_kind})"
            )
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

    def release_segment_leases_for_event(
        self,
        event_key: str,
        *,
        owner: str | None = None,
    ) -> int:
        """Release one event's leases, optionally limited to a named owner."""
        clauses = ["event_key = ?"]
        values: list[Any] = [event_key]
        if owner is not None:
            clauses.append("owner = ?")
            values.append(owner)
        with self._lock, self.connection:
            cursor = self.connection.execute(
                f"DELETE FROM segment_leases WHERE {' AND '.join(clauses)}",
                values,
            )
        return cursor.rowcount

    def has_incomplete_vision_tasks(self, event_key: str) -> bool:
        placeholders = ",".join("?" for _ in VISION_INCOMPLETE_STATUSES)
        with self._lock:
            row = self.connection.execute(
                f"""
                SELECT 1 FROM vision_tasks
                WHERE event_key = ? AND status IN ({placeholders})
                LIMIT 1
                """,
                (event_key, *VISION_INCOMPLETE_STATUSES),
            ).fetchone()
        return row is not None

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
        metadata = event_data.get("metadata")
        metadata = metadata if isinstance(metadata, Mapping) else {}
        event_source = metadata.get("event_source")
        if isinstance(event_source, Mapping):
            event_source = event_source.get("primary")
        event_source = str(event_source or metadata.get("source") or "overview")
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
            event_source=event_source,
            second=event_data.get("second"),
            goal_route_status=metadata.get("goal_route_status"),
            shotmap_route_diagnostics=metadata.get("shotmap_route_diagnostics"),
        )
        self.transition(event_key, "pending", reason="event_accepted")
        return True

    def update_task_event(
        self,
        event_data: Mapping[str, Any],
        *,
        replace_fields: Collection[str] = (),
    ) -> bool:
        previous = self.store.get(str(event_data["event_key"]))
        if previous is None:
            return False
        updated = self.store.update_event_data(
            event_data,
            replace_fields=replace_fields,
        )
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
        artifact_kind = normalize_vision_artifact_kind(fields.get("artifact_kind"))
        fields["artifact_kind"] = artifact_kind
        inserted = self.store.enqueue_vision_task(event_key, **fields)
        task = self.store.get_vision_task(event_key, artifact_kind)
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

    def start_vision(
        self,
        event_key: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
    ) -> StoredVisionTask:
        """Compatibility helper for the single-worker visual refinement runner."""
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        current = self.store.get_vision_task(event_key, normalized_artifact_kind)
        if current is None:
            raise KeyError(
                f"unknown vision task: {event_key} ({normalized_artifact_kind})"
            )
        if current.status == "pending":
            return self.transition_vision_task(
                event_key, "locating", artifact_kind=normalized_artifact_kind
            )
        if current.status == "located":
            return self.transition_vision_task(
                event_key, "encoding", artifact_kind=normalized_artifact_kind
            )
        raise ValueError(f"vision task cannot start from {current.status}")

    def complete_vision(
        self,
        event_key: str,
        result: Mapping[str, Any],
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
    ) -> StoredVisionTask:
        """Persist a located anchor and refined GIF returned by a combined runner."""
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        current = self.store.get_vision_task(event_key, normalized_artifact_kind)
        if current is None:
            raise KeyError(
                f"unknown vision task: {event_key} ({normalized_artifact_kind})"
            )
        normalized = dict(result)
        effective_anchor = normalized.get("anchor_stream_time")
        if effective_anchor is None:
            effective_anchor = normalized.get("vision_anchor_stream_time_sec")
        if effective_anchor is None:
            effective_anchor = current.located_anchor_stream_time
        if effective_anchor is None:
            effective_anchor = current.result.get("anchor_stream_time")
        if effective_anchor is None:
            normalized.pop("anchor_stream_time", None)
        else:
            # Validate before any transition so malformed completion results
            # cannot leave a located task partially advanced to encoding.
            normalized["anchor_stream_time"] = _validated_vision_anchor(
                effective_anchor
            )
        if current.status == "locating":
            current = self.transition_vision_task(
                event_key,
                "located",
                artifact_kind=normalized_artifact_kind,
                result=normalized,
            )
        if current.status == "located":
            current = self.transition_vision_task(
                event_key, "encoding", artifact_kind=normalized_artifact_kind
            )
        if current.status != "encoding":
            raise ValueError(f"vision task cannot complete from {current.status}")
        return self.transition_vision_task(
            event_key,
            "encoded",
            artifact_kind=normalized_artifact_kind,
            result=normalized,
        )

    def retry_vision(
        self,
        event_key: str,
        error: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
    ) -> StoredVisionTask:
        """Return transient visual work to the nearest restartable stage."""
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        current = self.store.get_vision_task(event_key, normalized_artifact_kind)
        if current is None:
            raise KeyError(
                f"unknown vision task: {event_key} ({normalized_artifact_kind})"
            )
        target = "located" if current.status == "encoding" else "pending"
        return self.transition_vision_task(
            event_key,
            target,
            artifact_kind=normalized_artifact_kind,
            error=error,
            reason="visual_input_not_ready",
        )

    def fail_vision(
        self,
        event_key: str,
        error: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        failure_stage: str | None = None,
        failure_reason: str | None = None,
    ) -> StoredVisionTask:
        return self.transition_vision_task(
            event_key,
            "failed",
            artifact_kind=artifact_kind,
            error=error,
            failure_stage=failure_stage,
            failure_reason=failure_reason,
            reason="visual_refinement_failed",
        )

    def acquire_segment_lease(
        self,
        event_key: str,
        segment_paths: list[str] | tuple[str, ...] | set[str],
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        owner: str = "vision-worker",
        expires_in_seconds: float,
    ) -> str:
        return self.store.acquire_segment_lease(
            event_key,
            segment_paths,
            artifact_kind=artifact_kind,
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

    def release_segment_leases_for_event(
        self,
        event_key: str,
        *,
        owner: str | None = None,
    ) -> int:
        return self.store.release_segment_leases_for_event(
            event_key,
            owner=owner,
        )

    def transition_vision_task(
        self,
        event_key: str,
        new_status: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
        error_kind: str | None = None,
        failure_stage: str | None = None,
        failure_reason: str | None = None,
        location_metadata: Mapping[str, Any] | None = None,
        window_metadata: Mapping[str, Any] | None = None,
        reason: str | None = None,
    ) -> StoredVisionTask:
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        previous = self.store.get_vision_task(event_key, normalized_artifact_kind)
        if previous is None:
            raise KeyError(
                f"unknown vision task: {event_key} ({normalized_artifact_kind})"
            )
        updated = self.store.transition_vision_task(
            event_key,
            new_status,
            artifact_kind=normalized_artifact_kind,
            result=result,
            error=error,
            error_kind=error_kind,
            failure_stage=failure_stage,
            failure_reason=failure_reason,
            location_metadata=location_metadata,
            window_metadata=window_metadata,
        )
        self.logger.log(
            "vision_task_transition",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            artifact_kind=updated.artifact_kind,
            from_status=previous.status,
            to_status=new_status,
            locate_attempt_count=updated.locate_attempt_count,
            encode_attempt_count=updated.encode_attempt_count,
            reason=reason,
            error=error,
            error_kind=error_kind,
            failure_stage=updated.failure_stage,
            failure_reason=updated.failure_reason,
        )
        if new_status == "encoded":
            self.logger.log(
                "refined_gif_ready",
                event_key=event_key,
                match_id=updated.match_id,
                code=updated.code,
                artifact_kind=updated.artifact_kind,
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
        self,
        event_key: str,
        error: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        error_kind: str,
        result: Mapping[str, Any] | None = None,
        window_metadata: Mapping[str, Any] | None = None,
        next_attempt_at_unix: float | None = None,
        deadline_at_unix: float | None = None,
        now: float | None = None,
    ) -> StoredVisionTask:
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        updated = self.store.record_vision_readiness_wait(
            event_key,
            error,
            artifact_kind=normalized_artifact_kind,
            error_kind=error_kind,
            result=result,
            window_metadata=window_metadata,
            next_attempt_at_unix=next_attempt_at_unix,
            deadline_at_unix=deadline_at_unix,
            now=now,
        )
        self.logger.log(
            "vision_buffer_readiness_wait",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            artifact_kind=updated.artifact_kind,
            status=updated.status,
            readiness_check_count=updated.readiness_check_count,
            next_attempt_at_unix=updated.next_attempt_at_unix,
            deadline_at_unix=updated.deadline_at_unix,
            error_kind=error_kind,
            error=error,
        )
        return updated

    def record_vision_queue_phase(
        self,
        event_key: str,
        phase: str,
        *,
        artifact_kind: str = DEFAULT_VISION_ARTIFACT_KIND,
        queued_at_unix: float | None = None,
        now: float | None = None,
    ) -> StoredVisionTask:
        normalized_artifact_kind = normalize_vision_artifact_kind(artifact_kind)
        previous = self.store.get_vision_task(event_key, normalized_artifact_kind)
        if previous is None:
            raise KeyError(
                f"unknown vision task: {event_key} ({normalized_artifact_kind})"
            )
        updated = self.store.record_vision_queue_phase(
            event_key,
            phase,
            artifact_kind=normalized_artifact_kind,
            queued_at_unix=queued_at_unix,
            now=now,
        )
        queue_timing = updated.window_metadata.get("queue_timing") or {}
        self.logger.log(
            "vision_task_queue_phase",
            event_key=event_key,
            match_id=updated.match_id,
            code=updated.code,
            artifact_kind=updated.artifact_kind,
            status=updated.status,
            phase=phase,
            queue_wait_seconds=queue_timing.get("queue_wait_seconds"),
            deadline_extension_seconds=queue_timing.get(
                "deadline_extension_seconds"
            ),
            deadline_at_unix=updated.deadline_at_unix,
        )
        return updated

    def recover_incomplete_vision(
        self,
        match_id: str,
        artifact_kind: str | None = None,
    ) -> list[StoredVisionTask]:
        """Make interrupted visual stages runnable without duplicating artifacts."""
        recovered: list[StoredVisionTask] = []
        for task in self.store.list_incomplete_vision_tasks(match_id, artifact_kind):
            previous_status = task.status
            if task.status == "locating":
                task = self.transition_vision_task(
                    task.event_key,
                    "pending",
                    artifact_kind=task.artifact_kind,
                    reason="process_restart_recovery",
                )
            elif task.status == "encoding":
                task = self.transition_vision_task(
                    task.event_key,
                    "located",
                    artifact_kind=task.artifact_kind,
                    reason="process_restart_recovery",
                )
            self.logger.log(
                "vision_task_recovered",
                event_key=task.event_key,
                match_id=task.match_id,
                code=task.code,
                artifact_kind=task.artifact_kind,
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
