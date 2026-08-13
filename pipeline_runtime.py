"""Durable task state and structured runtime logging for the GIF pipeline."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
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
                suppressed_by_event_key TEXT
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
        )

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
        now: float | None = None,
    ) -> bool:
        """Persist a newly observed event. Return False for any existing task."""
        timestamp = time.time() if now is None else now
        event_key = str(event_data["event_key"])
        with self._lock, self.connection:
            cursor = self.connection.execute(
                """
                INSERT OR IGNORE INTO event_tasks (
                    event_key, match_id, code, event_type, event_json, status,
                    observed_stream_time, observed_source_time,
                    clip_anchor_stream_time, clip_anchor_source_time,
                    output_due_stream_time, detected_at_unix,
                    discovered_at_unix, updated_at_unix
                ) VALUES (?, ?, ?, ?, ?, 'discovered', ?, ?, ?, ?, ?, ?, ?, ?)
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
            canonicals[event_data["event_key"]] = merge_event_metadata(
                canonicals.get(event_data["event_key"], {}), event_data
            )

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
            canonicals[canonical_key] = merge_event_metadata(
                canonicals.get(canonical_key, {}), event_data
            )
            canonicals[canonical_key]["event_key"] = canonical_key
            aliases[version_key] = canonicals[canonical_key]

        return {
            version_key: dict(canonicals[str(event_data["event_key"])])
            for version_key, event_data in aliases.items()
        }

    def remember_event_snapshot(
        self,
        match_id: str,
        event_keys: set[str],
        *,
        aliases: Mapping[str, Mapping[str, Any]] | None = None,
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
                [(match_id, event_key, timestamp) for event_key in event_keys],
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

    def transition(
        self,
        event_key: str,
        new_status: str,
        *,
        result: Mapping[str, Any] | None = None,
        error: str | None = None,
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
            values.append(event_key)
            with self.connection:
                self.connection.execute(
                    f"UPDATE event_tasks SET {', '.join(assignments)} WHERE event_key = ?",
                    values,
                )
            updated = self.get(event_key)
        assert updated is not None
        return updated


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
        reason: str | None = None,
    ) -> StoredTask:
        previous = self.store.get(event_key)
        if previous is None:
            raise KeyError(f"unknown event task: {event_key}")
        updated = self.store.transition(
            event_key, new_status, result=result, error=error
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
