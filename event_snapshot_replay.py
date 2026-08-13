"""Replay cumulative match-event API snapshots on a stream-time schedule."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Callable


class SnapshotReplayEventSource:
    """Expose new events from scheduled, cumulative API response snapshots.

    The replay uses stream-relative time so the same scenario works at any
    local MP4 start offset. A failed step records an error but does not alter
    the last successful snapshot or the deduplication set.
    """

    def __init__(
        self,
        path: Path,
        parse_events: Callable[[dict[str, Any]], list[Any]],
        emit_existing: bool,
    ) -> None:
        document = json.loads(path.read_text(encoding="utf-8"))
        raw_steps = document.get("steps") if isinstance(document, dict) else None
        if not isinstance(raw_steps, list) or not raw_steps:
            raise ValueError("snapshot replay file must contain a non-empty steps array")

        self.path = path
        self.parse_events = parse_events
        self.emit_existing = emit_existing
        self.steps: list[dict[str, Any]] = []
        previous_time = -1.0
        for index, raw_step in enumerate(raw_steps):
            if not isinstance(raw_step, dict):
                raise ValueError(f"snapshot replay step #{index + 1} must be an object")
            try:
                at_stream_sec = float(raw_step["at_stream_sec"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"snapshot replay step #{index + 1} needs a numeric at_stream_sec"
                ) from exc
            if not math.isfinite(at_stream_sec) or at_stream_sec < 0:
                raise ValueError(
                    f"snapshot replay step #{index + 1} has an invalid at_stream_sec"
                )
            if at_stream_sec < previous_time:
                raise ValueError("snapshot replay steps must be ordered by at_stream_sec")
            previous_time = at_stream_sec

            has_payload = "payload" in raw_step
            has_error = "error" in raw_step
            if has_payload == has_error:
                raise ValueError(
                    f"snapshot replay step #{index + 1} needs exactly one of payload or error"
                )
            if has_payload and not isinstance(raw_step["payload"], dict):
                raise ValueError(
                    f"snapshot replay step #{index + 1} payload must be an object"
                )
            if has_error and not isinstance(raw_step["error"], str):
                raise ValueError(
                    f"snapshot replay step #{index + 1} error must be a string"
                )
            self.steps.append({**raw_step, "at_stream_sec": at_stream_sec})

        self.next_step_index = 0
        self.seen: set[str] = set()
        self.initialized = False
        self.poll_count = 0
        self.response_count = 0
        self.error_count = 0
        self.last_error: str | None = None
        self.latest_events: dict[str, Any] = {}
        self.updated_events: list[Any] = []

    def _read_payload(self, payload: dict[str, Any]) -> list[Any]:
        if payload.get("status") not in (None, 0, "0"):
            raise ValueError(f"event API returned status={payload.get('status')!r}")
        if not isinstance(payload.get("events"), dict):
            raise ValueError("event API response does not contain an events object")
        return self.parse_events(payload)

    def poll(self, stream_time: float, now_monotonic: float) -> list[Any]:
        del now_monotonic
        self.updated_events = []
        self.poll_count += 1
        emitted: list[Any] = []
        while self.next_step_index < len(self.steps):
            step = self.steps[self.next_step_index]
            if step["at_stream_sec"] > stream_time:
                break
            self.next_step_index += 1

            if "error" in step:
                self.error_count += 1
                self.last_error = step["error"]
                print(f"[events:replay:error] {self.last_error}")
                continue

            self.response_count += 1
            try:
                current = self._read_payload(step["payload"])
            except ValueError as exc:
                self.error_count += 1
                self.last_error = str(exc)
                print(f"[events:replay:error] {exc}")
                continue

            current_keys = {event.event_key for event in current}
            if not self.initialized:
                self.initialized = True
                if not self.emit_existing:
                    self.seen.update(current_keys)
                    self.latest_events = {event.event_key: event for event in current}
                    self.last_error = None
                    print(
                        f"[events:replay] seeded {len(current_keys)} existing "
                        "supported events"
                    )
                    continue
            emitted.extend(event for event in current if event.event_key not in self.seen)
            self.updated_events.extend(
                event
                for event in current
                if event.event_key in self.seen
                and self.latest_events.get(event.event_key) != event
            )
            self.seen.update(current_keys)
            self.latest_events.update({event.event_key: event for event in current})
            self.last_error = None
        return emitted

    def report(self) -> dict[str, Any]:
        return {
            "type": "snapshot_replay",
            "path": str(self.path.resolve()),
            "scheduled_steps": len(self.steps),
            "consumed_steps": self.next_step_index,
            "poll_count": self.poll_count,
            "response_count": self.response_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "emit_existing": self.emit_existing,
        }
