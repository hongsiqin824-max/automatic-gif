#!/usr/bin/env python3
"""Generate football event GIFs from a live buffer and a match-event feed.

The video side never inspects a scoreboard. It continuously keeps a rolling
MPEG-TS buffer. New G/OG, YC, and RC records from the event source create GIF
jobs that read video from that buffer. OG is the feed's own code for an
own-goal and is handled as a goal event.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import signal
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import Any, Callable

from event_snapshot_replay import SnapshotReplayEventSource
from live_runtime import BoundedTaskPool, IngestSupervisor
from live_goal_pipeline import (
    BufferNotReady,
    BufferUnavailable,
    PendingEvent,
    encode_gif,
    prune_buffer,
    read_segments,
    source_is_local,
)
from pipeline_runtime import PipelineRuntime, StoredTask
from match_event_identity import (
    events_represent_same_incident,
    explicit_event_id,
    merge_event_metadata,
)


SUPPORTED_EVENT_CODES = {
    "G": "goal",
    "OG": "goal",
    "YC": "yellow_card",
    "RC": "red_card",
}


@dataclass(frozen=True)
class MatchEvent:
    event_key: str
    code: str
    event_type: str
    minute: str
    minute_extra: str
    team: str
    person: str
    person_id: str
    score: str
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventJob:
    match_event: MatchEvent
    pending: PendingEvent
    observed_stream_time: float
    observed_source_time: float | None


def recovered_event_job(task: StoredTask) -> EventJob:
    """Rebuild an in-memory job from durable state after a process restart."""
    match_event = MatchEvent(**task.event_data)
    pending = PendingEvent(
        event_type=task.event_type,
        stream_time=task.clip_anchor_stream_time,
        source_time=task.clip_anchor_source_time,
        detected_wall_time=task.detected_at_unix,
        change_fraction=0.0,
        stability_fraction=0.0,
        output_due_stream_time=task.output_due_stream_time,
        output_id=task.event_key.rsplit(":", 1)[-1][:8],
        status="pending",
        result=task.result,
    )
    return EventJob(
        match_event=match_event,
        pending=pending,
        observed_stream_time=task.observed_stream_time,
        observed_source_time=task.observed_source_time,
    )


def encode_event_job(
    job: EventJob,
    runtime: PipelineRuntime,
    ffmpeg: str,
    ffprobe: str,
    segment_reader: Callable[[], list[Any]],
    output_dir: Path,
    *,
    before: float,
    after: float,
    width: int,
    fps: float,
    colors: int,
    size_reference_bytes: int,
    state_lock: threading.Lock | None = None,
    cancel_event: threading.Event | None = None,
) -> bool:
    """Try one durable encoding attempt; return False while the buffer is incomplete."""
    pending = job.pending
    lock = state_lock or threading.Lock()
    with lock:
        runtime.transition(job.match_event.event_key, "encoding")
    try:
        encoded = encode_gif(
            ffmpeg,
            ffprobe,
            segment_reader(),
            pending,
            output_dir,
            before=before,
            after=after,
            width=width,
            fps=fps,
            colors=colors,
            size_reference_bytes=size_reference_bytes,
            cancel_event=cancel_event,
        )
        pending.status = "encoded"
        pending.result = encoded
        ready_seconds = time.time() - pending.detected_wall_time
        pending.result["seconds_after_event_observed"] = round(ready_seconds, 3)
        with lock:
            runtime.transition(
                job.match_event.event_key,
                "encoded",
                result=pending.result,
            )
        print(
            f"[gif:ready] code={job.match_event.code} "
            f"{encoded['bytes'] / 1_000_000:.2f}MB "
            f"ready={ready_seconds:.2f}s output={encoded['output']}"
        )
        return True
    except BufferNotReady:
        with lock:
            runtime.transition(
                job.match_event.event_key,
                "pending",
                reason="buffer_not_ready",
            )
        return False
    except BufferUnavailable as exc:
        pending.status = "failed"
        pending.result = {"error": str(exc), "error_kind": "video_gap"}
        with lock:
            runtime.transition(
                job.match_event.event_key,
                "failed",
                result=pending.result,
                error=str(exc),
            )
        print(f"[gif:error:video_gap] code={job.match_event.code} {exc}")
        return True
    except Exception as exc:
        pending.status = "failed"
        pending.result = {"error": str(exc)}
        with lock:
            runtime.transition(
                job.match_event.event_key,
                "failed",
                result=pending.result,
                error=str(exc),
            )
        print(f"[gif:error] code={job.match_event.code} {exc}")
        return True


def stable_event_key(
    match_id: str,
    code: str,
    minute: str,
    minute_extra: str,
    team: str,
    person_id: str,
    occurrence: int,
    *,
    event_id: str = "",
) -> str:
    if event_id:
        raw = "|".join([match_id, "event_id", event_id])
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
        return f"{match_id}:{code}:{digest}"
    raw = "|".join(
        [match_id, code, minute, minute_extra, team, person_id, str(occurrence)]
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]
    return f"{match_id}:{code}:{digest}"


def parse_match_events(payload: dict[str, Any], match_id: str) -> list[MatchEvent]:
    """Flatten the API's minute/team event buckets into supported events."""
    buckets = payload.get("events")
    if not isinstance(buckets, dict):
        return []

    parsed: list[MatchEvent] = []
    occurrences: dict[tuple[str, ...], int] = {}
    for bucket_key, bucket_value in buckets.items():
        if not isinstance(bucket_value, dict):
            continue
        bucket_minute = str(bucket_value.get("minute") or bucket_key)
        for team_key, event_values in bucket_value.items():
            if not team_key.endswith("Events") or not isinstance(event_values, list):
                continue
            team = team_key[: -len("Events")]
            for raw_event in event_values:
                if not isinstance(raw_event, dict):
                    continue
                code = str(raw_event.get("code") or "").upper()
                if code not in SUPPORTED_EVENT_CODES:
                    continue
                minute = str(raw_event.get("minute") or bucket_minute)
                minute_extra = str(raw_event.get("minute_extra") or "0")
                person_id = str(raw_event.get("person_id") or "0")
                metadata = {
                    "bucket": str(bucket_key),
                    **{
                        key: raw_event.get(key)
                        for key in ("event_id", "eventId", "id")
                        if raw_event.get(key) not in (None, "", "0", 0)
                    },
                }
                event_id = explicit_event_id({"metadata": metadata})
                identity = (code, minute, minute_extra, team, person_id)
                occurrence = occurrences.get(identity, 0)
                occurrences[identity] = occurrence + 1
                parsed.append(
                    MatchEvent(
                        event_key=stable_event_key(
                            match_id,
                            code,
                            minute,
                            minute_extra,
                            team,
                            person_id,
                            occurrence,
                            event_id=event_id,
                        ),
                        code=code,
                        event_type=SUPPORTED_EVENT_CODES[code],
                        minute=minute,
                        minute_extra=minute_extra,
                        team=team,
                        person=str(raw_event.get("person") or ""),
                        person_id=person_id,
                        score=str(raw_event.get("score") or ""),
                        reason=str(raw_event.get("reason") or ""),
                        metadata=metadata,
                    )
                )
    return parsed


class EventRevisionTracker:
    """Map changing API records onto one durable real-world event."""

    def __init__(self, aliases: dict[str, MatchEvent] | None = None) -> None:
        self.aliases = dict(aliases or {})
        self.canonical_events: dict[str, MatchEvent] = {}
        for event in self.aliases.values():
            current = self.canonical_events.get(event.event_key)
            if current is None:
                self.canonical_events[event.event_key] = event
            else:
                merged = merge_event_metadata(asdict(current), asdict(event))
                self.canonical_events[event.event_key] = MatchEvent(**merged)

    def reconcile(self, current: list[MatchEvent]) -> list[MatchEvent]:
        reconciled: list[MatchEvent] = []
        matched_canonical: set[str] = set()
        unmatched: list[MatchEvent] = []

        for version in current:
            known = self.aliases.get(version.event_key)
            if known is None or not events_represent_same_incident(
                asdict(known), asdict(version)
            ):
                unmatched.append(version)
                continue
            canonical_key = known.event_key
            merged = merge_event_metadata(
                asdict(self.canonical_events.get(canonical_key, known)),
                asdict(version),
            )
            merged["event_key"] = canonical_key
            canonical = MatchEvent(**merged)
            self.aliases[version.event_key] = canonical
            self.canonical_events[canonical_key] = canonical
            matched_canonical.add(canonical_key)
            reconciled.append(canonical)

        for version in unmatched:
            candidates = [
                event
                for key, event in self.canonical_events.items()
                if events_represent_same_incident(
                    asdict(event), asdict(version), allow_exact_match=False
                )
                and (
                    key not in matched_canonical
                    or self._can_merge_with_seen_version(event, version)
                )
            ]
            if len(candidates) == 1:
                canonical_key = candidates[0].event_key
                merged = merge_event_metadata(asdict(candidates[0]), asdict(version))
                merged["event_key"] = canonical_key
                canonical = MatchEvent(**merged)
            else:
                canonical = version
                canonical_key = version.event_key
            self.aliases[version.event_key] = canonical
            self.canonical_events[canonical_key] = canonical
            matched_canonical.add(canonical_key)
            reconciled.append(canonical)
        return reconciled

    @staticmethod
    def _can_merge_with_seen_version(
        canonical: MatchEvent, version: MatchEvent
    ) -> bool:
        """Avoid absorbing a new score into an event already in this snapshot."""
        canonical_score = canonical.score.replace(" ", "")
        version_score = version.score.replace(" ", "")
        return not (
            canonical_score
            and version_score
            and canonical_score != version_score
        )

    def snapshot(self) -> dict[str, MatchEvent]:
        return dict(self.aliases)


class HttpMatchEventSource:
    def __init__(
        self,
        url: str,
        match_id: str,
        user: str | None,
        poll_interval: float,
        emit_existing: bool,
        timeout: float,
        *,
        initial_seen: set[str] | None = None,
        initialized: bool = False,
        initial_aliases: dict[str, MatchEvent] | None = None,
    ) -> None:
        self.url = url.format(match_id=urllib.parse.quote(match_id, safe=""))
        self.match_id = match_id
        self.user = user
        self.poll_interval = poll_interval
        self.emit_existing = emit_existing
        self.timeout = timeout
        self.seen = set(initial_seen or ())
        self.initialized = initialized
        self.snapshot_revision = 0
        self.next_poll_monotonic = 0.0
        self.poll_count = 0
        self.error_count = 0
        self.last_error: str | None = None
        self.consecutive_errors = 0
        self.last_error_kind: str | None = None
        self.revisions = EventRevisionTracker(initial_aliases)
        self.latest_events: dict[str, MatchEvent] = dict(
            self.revisions.canonical_events
        )
        self.updated_events: list[MatchEvent] = []

    def _request_url(self) -> str:
        if not self.user:
            return self.url
        parts = urllib.parse.urlsplit(self.url)
        query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
        query["user"] = self.user
        return urllib.parse.urlunsplit(
            (
                parts.scheme,
                parts.netloc,
                parts.path,
                urllib.parse.urlencode(query),
                parts.fragment,
            )
        )

    def poll(self, stream_time: float, now_monotonic: float) -> list[MatchEvent]:
        del stream_time
        self.updated_events = []
        if now_monotonic < self.next_poll_monotonic:
            return []
        self.next_poll_monotonic = now_monotonic + self.poll_interval
        self.poll_count += 1
        try:
            request = urllib.request.Request(
                self._request_url(),
                headers={"Accept": "application/json", "User-Agent": "football-gif-pipeline/1.0"},
            )
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("event API response must be a JSON object")
            if payload.get("status") not in (None, 0, "0"):
                raise ValueError(f"event API returned status={payload.get('status')!r}")
            events = payload.get("events")
            if events == []:
                payload["events"] = {}
            elif not isinstance(events, dict):
                raise ValueError("event API response does not contain an events object")
            current = self.revisions.reconcile(parse_match_events(payload, self.match_id))
            current_keys = {event.event_key for event in current}
            self.snapshot_revision += 1
            if not self.initialized:
                self.initialized = True
                if not self.emit_existing:
                    self.seen.update(current_keys)
                    self.latest_events = {event.event_key: event for event in current}
                    print(f"[events] seeded {len(current_keys)} existing supported events")
                    return []
            new_events = [event for event in current if event.event_key not in self.seen]
            self.updated_events = [
                event
                for event in current
                if event.event_key in self.seen
                and self.latest_events.get(event.event_key) != event
            ]
            self.seen.update(current_keys)
            self.latest_events.update({event.event_key: event for event in current})
            self.last_error = None
            self.last_error_kind = None
            self.consecutive_errors = 0
            return new_events
        except urllib.error.HTTPError as exc:
            self.error_count += 1
            self.consecutive_errors += 1
            self.last_error_kind = "unauthorized" if exc.code in (401, 403) else "http"
            self.last_error = f"HTTP {exc.code}: {exc.reason}"
            backoff = min(
                self.poll_interval * (2 ** (self.consecutive_errors - 1)), 30.0
            )
            self.next_poll_monotonic = now_monotonic + backoff
            print(f"[events:error:{self.last_error_kind}] {self.last_error}")
            return []
        except (OSError, ValueError, json.JSONDecodeError, urllib.error.URLError) as exc:
            self.error_count += 1
            self.consecutive_errors += 1
            self.last_error_kind = "temporary"
            self.last_error = str(exc)
            backoff = min(
                self.poll_interval * (2 ** (self.consecutive_errors - 1)), 30.0
            )
            self.next_poll_monotonic = now_monotonic + backoff
            print(f"[events:error:temporary] {exc}")
            return []

    def report(self) -> dict[str, Any]:
        return {
            "type": "http",
            "url": self.url,
            "poll_interval_seconds": self.poll_interval,
            "poll_count": self.poll_count,
            "error_count": self.error_count,
            "last_error": self.last_error,
            "last_error_kind": self.last_error_kind,
            "consecutive_errors": self.consecutive_errors,
            "emit_existing": self.emit_existing,
        }


class MockMatchEventSource:
    def __init__(
        self,
        path: Path,
        match_id: str,
        source_start: float,
    ) -> None:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw_events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(raw_events, list):
            raise ValueError("mock event file must contain an events array")
        self.path = path
        self.match_id = match_id
        self.source_start = source_start
        self.schedule: list[tuple[float, MatchEvent]] = []
        for index, raw_event in enumerate(raw_events):
            if not isinstance(raw_event, dict):
                raise ValueError(f"mock event #{index + 1} must be an object")
            code = str(raw_event.get("code") or "").upper()
            if code not in SUPPORTED_EVENT_CODES:
                raise ValueError(f"mock event #{index + 1} has unsupported code {code!r}")
            source_event_time = float(raw_event["source_time_sec"])
            notification_delay = float(raw_event.get("notification_delay_sec", 0.0))
            emit_stream_time = source_event_time - source_start + notification_delay
            minute = str(raw_event.get("minute") or "")
            minute_extra = str(raw_event.get("minute_extra") or "0")
            person_id = str(raw_event.get("person_id") or index + 1)
            team = str(raw_event.get("team") or "teamA")
            event = MatchEvent(
                event_key=stable_event_key(
                    match_id, code, minute, minute_extra, team, person_id, index
                ),
                code=code,
                event_type=SUPPORTED_EVENT_CODES[code],
                minute=minute,
                minute_extra=minute_extra,
                team=team,
                person=str(raw_event.get("person") or ""),
                person_id=person_id,
                score=str(raw_event.get("score") or ""),
                reason=str(raw_event.get("reason") or ""),
                metadata={
                    "mock_source_event_time_sec": source_event_time,
                    "mock_notification_delay_sec": notification_delay,
                },
            )
            self.schedule.append((emit_stream_time, event))
        self.schedule.sort(key=lambda item: item[0])
        self.emitted: set[str] = set()

    def poll(self, stream_time: float, now_monotonic: float) -> list[MatchEvent]:
        del now_monotonic
        new_events = [
            event
            for emit_time, event in self.schedule
            if emit_time <= stream_time and event.event_key not in self.emitted
        ]
        self.emitted.update(event.event_key for event in new_events)
        return new_events

    def report(self) -> dict[str, Any]:
        return {
            "type": "mock",
            "path": str(self.path.resolve()),
            "scheduled_events": len(self.schedule),
            "emitted_events": len(self.emitted),
        }


def build_ingest_command(
    ffmpeg: str,
    source: str,
    simulate_live: bool,
    replay_speed: float,
    source_start: float,
    duration: float | None,
    segment_seconds: float,
    buffer_dir: Path,
    segment_list: Path,
    segment_prefix: str = "segment",
) -> list[str]:
    command = [ffmpeg, "-y", "-nostdin", "-hide_banner", "-loglevel", "error"]
    if simulate_live:
        command.extend(["-readrate", f"{replay_speed:g}"])
    if source_start > 0:
        command.extend(["-ss", f"{source_start:.3f}"])
    if duration is not None:
        command.extend(["-t", f"{duration:.3f}"])
    if source.lower().startswith("rtmp://"):
        command.extend(["-rw_timeout", "15000000", "-rtmp_live", "live"])
    command.extend(
        [
            "-i",
            source,
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-c",
            "copy",
            "-bsf:v",
            "h264_mp4toannexb,dump_extra",
            "-f",
            "segment",
            "-segment_time",
            f"{segment_seconds:.3f}",
            "-reset_timestamps",
            "1",
            "-segment_list",
            str(segment_list),
            "-segment_list_type",
            "csv",
            str(buffer_dir / f"{segment_prefix}_%06d.ts"),
        ]
    )
    return command


@dataclass(frozen=True)
class SegmentGeneration:
    list_path: Path
    stream_offset: float


def read_all_segments(
    generations: list[SegmentGeneration], buffer_dir: Path
) -> list[Any]:
    segments = []
    for generation in generations:
        segments.extend(
            read_segments(
                generation.list_path,
                buffer_dir,
                time_offset=generation.stream_offset,
            )
        )
    return sorted(segments, key=lambda segment: (segment.start, str(segment.path)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="RTMP URL or local MP4 path")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--event-url", help="event API URL; may contain {match_id}")
    source_group.add_argument("--mock-events", type=Path, help="local simulated event feed")
    source_group.add_argument(
        "--replay-events",
        type=Path,
        help="scheduled cumulative event API snapshots for deterministic tests",
    )
    parser.add_argument("--match-id", required=True)
    parser.add_argument("--event-user")
    parser.add_argument("--event-poll-seconds", type=float, default=5.0)
    parser.add_argument("--event-timeout-seconds", type=float, default=8.0)
    parser.add_argument("--emit-existing-events", action="store_true")
    parser.add_argument(
        "--event-to-video-offset",
        type=float,
        default=0.0,
        help="video event time minus the event's first-observed stream time",
    )
    parser.add_argument("--simulate-live", action="store_true")
    parser.add_argument("--replay-speed", type=float, default=1.0)
    parser.add_argument("--start", type=float, default=0.0)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--segment-seconds", type=float, default=2.0)
    parser.add_argument("--buffer-seconds", type=float, default=120.0)
    parser.add_argument("--before", type=float, default=30.0)
    parser.add_argument("--after", type=float, default=20.0)
    parser.add_argument("--segment-slack", type=float, default=7.0)
    parser.add_argument("--gif-width", type=int, default=384)
    parser.add_argument("--gif-fps", type=float, default=6.0)
    parser.add_argument("--gif-colors", type=int, default=160)
    parser.add_argument("--gif-size-reference-mb", type=float, default=10.0)
    parser.add_argument("--output-dir", type=Path, default=Path("output_gifs/events"))
    parser.add_argument(
        "--state-db",
        type=Path,
        help="durable task database (default: OUTPUT_DIR/pipeline_state.sqlite3)",
    )
    parser.add_argument(
        "--event-log",
        type=Path,
        help="real-time JSONL log (default: OUTPUT_DIR/pipeline_events.jsonl)",
    )
    parser.add_argument(
        "--gif-workers",
        type=int,
        default=2,
        help="maximum simultaneous GIF encodes (default: 2)",
    )
    parser.add_argument(
        "--rtmp-max-reconnects",
        type=int,
        default=None,
        help="maximum FFmpeg restarts after RTMP errors (default: unlimited)",
    )
    parser.add_argument("--rtmp-reconnect-initial-seconds", type=float, default=1.0)
    parser.add_argument("--rtmp-reconnect-max-seconds", type=float, default=30.0)
    parser.add_argument(
        "--graceful-stop-grace-seconds",
        type=float,
        help=(
            "seconds to keep ingesting after SIGUSR1 before normal shutdown "
            "(default: after + segment slack)"
        ),
    )
    parser.add_argument(
        "--graceful-stop-timeout-seconds",
        type=float,
        default=120.0,
        help="hard limit for SIGUSR1 shutdown (default: 120 seconds)",
    )
    args = parser.parse_args()

    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    ffprobe = shutil.which("ffprobe") or "/opt/homebrew/bin/ffprobe"
    if not Path(ffmpeg).exists() or not Path(ffprobe).exists():
        raise SystemExit("ffmpeg and ffprobe are required")
    if source_is_local(args.source) and not Path(args.source).is_file():
        raise SystemExit(f"source does not exist: {args.source}")
    if source_is_local(args.source) and not args.simulate_live:
        raise SystemExit("local recordings require --simulate-live in this event-driven tool")
    if args.simulate_live and not source_is_local(args.source):
        raise SystemExit("--simulate-live is only for local recordings")
    positive = (
        args.event_poll_seconds,
        args.event_timeout_seconds,
        args.replay_speed,
        args.segment_seconds,
        args.buffer_seconds,
        args.after,
        args.segment_slack,
        args.gif_width,
        args.gif_fps,
        args.gif_size_reference_mb,
        args.gif_workers,
        args.rtmp_reconnect_initial_seconds,
        args.rtmp_reconnect_max_seconds,
        args.graceful_stop_timeout_seconds,
    )
    if any(value <= 0 for value in positive) or args.before < 0 or args.start < 0:
        raise SystemExit("time, FPS, buffer, and size arguments must be positive")
    if not 2 <= args.gif_colors <= 256:
        raise SystemExit("--gif-colors must be between 2 and 256")
    if args.rtmp_max_reconnects is not None and args.rtmp_max_reconnects < 0:
        raise SystemExit("--rtmp-max-reconnects cannot be negative")
    if (
        args.graceful_stop_grace_seconds is not None
        and args.graceful_stop_grace_seconds <= 0
    ):
        raise SystemExit("--graceful-stop-grace-seconds must be positive")
    if args.before + args.segment_slack >= args.buffer_seconds:
        raise SystemExit("buffer must be longer than before + segment slack")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    buffer_dir = args.output_dir / "buffer"
    buffer_dir.mkdir(parents=True, exist_ok=True)
    segment_list = buffer_dir / "segments.csv"
    state_db_path = args.state_db or args.output_dir / "pipeline_state.sqlite3"
    event_log_path = args.event_log or args.output_dir / "pipeline_events.jsonl"
    runtime = PipelineRuntime(state_db_path, event_log_path)
    if args.mock_events:
        event_source: (
            HttpMatchEventSource | MockMatchEventSource | SnapshotReplayEventSource
        ) = MockMatchEventSource(
            args.mock_events, args.match_id, args.start
        )
    elif args.replay_events:
        event_source = SnapshotReplayEventSource(
            args.replay_events,
            lambda payload: parse_match_events(payload, args.match_id),
            args.emit_existing_events,
        )
    else:
        assert args.event_url
        cursor_initialized, cursor_keys = runtime.store.load_event_cursor(args.match_id)
        initial_aliases = {
            version_key: MatchEvent(**event_data)
            for version_key, event_data in runtime.store.load_event_aliases(
                args.match_id
            ).items()
        }
        event_source = HttpMatchEventSource(
            args.event_url,
            args.match_id,
            args.event_user,
            args.event_poll_seconds,
            args.emit_existing_events,
            args.event_timeout_seconds,
            initial_seen=cursor_keys,
            initialized=cursor_initialized,
            initial_aliases=initial_aliases,
        )

    report_path = args.output_dir / "event_pipeline_report.json"
    jobs = [recovered_event_job(task) for task in runtime.recover_incomplete(args.match_id)]
    recovered_jobs = len(jobs)
    pipeline_started_wall = time.time()
    pipeline_started_mono = time.monotonic()
    run_id = (
        time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(pipeline_started_wall))
        + f"_{uuid.uuid4().hex[:8]}"
    )
    run_report_path = args.output_dir / f"event_pipeline_report_{run_id}.json"
    last_prune_second = -1
    last_heartbeat_monotonic = 0.0
    persisted_snapshot_revision = 0
    stopped_by_user = False
    graceful_stop_requested = threading.Event()
    graceful_stop_cancel_encodes = threading.Event()
    graceful_stop_started_monotonic: float | None = None
    graceful_stop_ingest_stopped = False
    graceful_stop_timed_out = False
    graceful_stop_stream_incomplete = False
    stop_reason = "ingest_exit"
    state_lock = threading.Lock()
    task_pool = BoundedTaskPool(args.gif_workers)
    segment_generations: list[SegmentGeneration] = []
    if segment_list.exists():
        segment_generations.append(SegmentGeneration(segment_list, 0.0))

    def segment_reader() -> list[Any]:
        return read_all_segments(segment_generations, buffer_dir)

    def ingest_command(generation: int) -> list[str]:
        return build_ingest_command(
            ffmpeg,
            args.source,
            args.simulate_live,
            args.replay_speed,
            args.start,
            args.duration,
            args.segment_seconds,
            buffer_dir,
            buffer_dir / f"segments_{run_id}_g{generation:03d}.csv",
            segment_prefix=f"segment_{run_id}_g{generation:03d}",
        )

    def ingest_log(generation: int) -> Any:
        return (
            args.output_dir / f"ingest_ffmpeg_{run_id}_g{generation:03d}.log"
        ).open("w", encoding="utf-8")

    supervisor = IngestSupervisor(
        ingest_command,
        ingest_log,
        reconnect=args.source.lower().startswith("rtmp://"),
        max_reconnects=args.rtmp_max_reconnects,
        backoff_initial=args.rtmp_reconnect_initial_seconds,
        backoff_max=args.rtmp_reconnect_max_seconds,
    )

    graceful_signal = getattr(signal, "SIGUSR1", None)
    previous_graceful_handler: Any = None

    def request_graceful_stop(signum: int, frame: Any) -> None:
        # Signal handlers only set state; the main loop performs all cleanup.
        graceful_stop_requested.set()

    if graceful_signal is not None:
        previous_graceful_handler = signal.signal(
            graceful_signal, request_graceful_stop
        )

    print(
        f"[ingest] source={args.source} match={args.match_id} "
        f"simulate_live={args.simulate_live} speed={args.replay_speed:g}x"
    )
    print(
        f"[buffer] segment={args.segment_seconds:g}s window={args.buffer_seconds:g}s "
        f"clip=-{args.before:g}/+{args.after:g}s"
    )
    print(
        f"[gif] fixed={args.gif_width}px/{args.gif_fps:g}fps/{args.gif_colors}colors "
        f"size_reference={args.gif_size_reference_mb:g}MB adaptive_reduction=false"
    )
    print(
        f"[runtime] recovered={len(jobs)} state={state_db_path.resolve()} "
        f"log={event_log_path.resolve()}"
    )

    # A crash may leave enough closed buffer segments to finish an interrupted job.
    # Try those before the new ingest process starts overwriting segment filenames.
    if segment_generations:
        for job in jobs:
            encode_event_job(
                job,
                runtime,
                ffmpeg,
                ffprobe,
                segment_reader,
                args.output_dir,
                before=args.before,
                after=args.after,
                width=args.gif_width,
                fps=args.gif_fps,
                colors=args.gif_colors,
                size_reference_bytes=int(args.gif_size_reference_mb * 1_000_000),
                state_lock=state_lock,
                cancel_event=graceful_stop_cancel_encodes,
            )

    process = supervisor.start(pipeline_started_mono)
    reconnect_due_monotonic: float | None = None
    segment_generations.append(
        SegmentGeneration(
            buffer_dir
            / f"segments_{run_id}_g{supervisor.generation:03d}.csv",
            0.0,
        )
    )
    try:
        while True:
            now_monotonic = time.monotonic()
            if graceful_stop_requested.is_set() and graceful_stop_started_monotonic is None:
                graceful_stop_started_monotonic = now_monotonic
                # A normal match ending must never cause the RTMP process to be
                # restarted while we finish the final video window.
                supervisor.reconnect = False
                reconnect_due_monotonic = None
                runtime.logger.log(
                    "graceful_stop_requested",
                    match_id=args.match_id,
                    reason="match_played",
                )
                print(
                    "[shutdown] graceful stop requested; draining final GIF jobs",
                    flush=True,
                )

            process_exit = supervisor.observe_exit()
            if process_exit is not None:
                return_code = process_exit.return_code
                if graceful_stop_started_monotonic is not None:
                    if graceful_stop_ingest_stopped:
                        graceful_stop_stream_incomplete = (
                            graceful_stop_timed_out
                            and any(
                                job.pending.status in ("pending", "encoding")
                                for job in jobs
                            )
                        )
                        stop_reason = (
                            "match_played_stream_incomplete"
                            if graceful_stop_timed_out
                            else "match_played"
                        )
                        break
                    # Keep polling the event feed through the grace window even
                    # if the broadcaster closes the video stream immediately.
                    # A late final event is then recorded and reported as an
                    # incomplete clip rather than silently missed.
                    reconnect_due_monotonic = None
                    continue
                if not process_exit.restart:
                    break
                runtime.logger.log(
                    "ingest_restart",
                    match_id=args.match_id,
                    return_code=process_exit.return_code,
                    restart_count=supervisor.restart_count,
                    delay_seconds=process_exit.restart_delay_seconds,
                )
                reconnect_due_monotonic = (
                    time.monotonic() + process_exit.restart_delay_seconds
                )

            if (
                graceful_stop_started_monotonic is not None
                and now_monotonic - graceful_stop_started_monotonic
                >= args.graceful_stop_timeout_seconds
            ):
                graceful_stop_timed_out = True
                graceful_stop_stream_incomplete = any(
                    job.pending.status in ("pending", "encoding") for job in jobs
                )
                graceful_stop_cancel_encodes.set()
                graceful_stop_ingest_stopped = True
                supervisor.terminate()
                stop_reason = "match_played_stream_incomplete"
                runtime.logger.log(
                    "graceful_stop_ingest",
                    match_id=args.match_id,
                    timed_out=True,
                    elapsed_seconds=round(
                        now_monotonic - graceful_stop_started_monotonic, 3
                    ),
                    pending_count=sum(
                        job.pending.status == "pending" for job in jobs
                    ),
                )
                if supervisor.process is None:
                    return_code = process.wait()
                    break
                time.sleep(0.1)
                continue

            if (
                graceful_stop_started_monotonic is None
                and
                supervisor.process is None
                and reconnect_due_monotonic is not None
                and time.monotonic() >= reconnect_due_monotonic
            ):
                generation_offset = (
                    time.monotonic() - pipeline_started_mono
                ) * (args.replay_speed if args.simulate_live else 1.0)
                process = supervisor.start()
                reconnect_due_monotonic = None
                segment_generations.append(
                    SegmentGeneration(
                        buffer_dir
                        / f"segments_{run_id}_g{supervisor.generation:03d}.csv",
                        generation_offset,
                    )
                )

            # Keep the event feed alive while FFmpeg is waiting to reconnect.
            # The worker records events immediately and waits for usable video
            # before it submits their GIF jobs.
            if (
                not graceful_stop_ingest_stopped
                and (
                    supervisor.process is None
                    or supervisor.process.poll() is None
                )
            ):
                elapsed_wall = now_monotonic - pipeline_started_mono
                stream_time = elapsed_wall * (args.replay_speed if args.simulate_live else 1.0)

                previous_error_count = getattr(event_source, "error_count", 0)
                new_events = event_source.poll(stream_time, now_monotonic)
                current_error_count = getattr(event_source, "error_count", 0)
                if current_error_count > previous_error_count:
                    runtime.log_api_error(
                        match_id=args.match_id,
                        error=getattr(event_source, "last_error", None)
                        or "unknown event source error",
                        poll_count=getattr(event_source, "poll_count", 0),
                        error_count=current_error_count,
                    )
                if new_events:
                    observed_monotonic = time.monotonic()
                    stream_time = (
                        observed_monotonic - pipeline_started_mono
                    ) * (args.replay_speed if args.simulate_live else 1.0)
                updated_events = list(getattr(event_source, "updated_events", []))
                for updated_event in updated_events:
                    if runtime.update_task_event(asdict(updated_event)):
                        for job in jobs:
                            if job.match_event.event_key == updated_event.event_key:
                                job.match_event = replace(updated_event)
                        print(
                            f"[event:update] code={updated_event.code} "
                            f"minute={updated_event.minute} "
                            f"person={updated_event.person or '-'}"
                        )
                for match_event in new_events:
                    clip_anchor = max(0.0, stream_time + args.event_to_video_offset)
                    source_time = (
                        args.start + clip_anchor if source_is_local(args.source) else None
                    )
                    detected_wall_time = time.time()
                    observed_source_time = (
                        args.start + stream_time
                        if source_is_local(args.source)
                        else None
                    )
                    pending = PendingEvent(
                        event_type=match_event.event_type,
                        stream_time=clip_anchor,
                        source_time=source_time,
                        detected_wall_time=detected_wall_time,
                        change_fraction=0.0,
                        stability_fraction=0.0,
                        output_due_stream_time=(
                            clip_anchor + args.after + args.segment_slack
                        ),
                        output_id=match_event.event_key.rsplit(":", 1)[-1][:8],
                    )
                    if not runtime.discover_task(
                        match_id=args.match_id,
                        event_data=asdict(match_event),
                        observed_stream_time=stream_time,
                        observed_source_time=observed_source_time,
                        clip_anchor_stream_time=clip_anchor,
                        clip_anchor_source_time=source_time,
                        output_due_stream_time=pending.output_due_stream_time,
                        detected_at_unix=detected_wall_time,
                    ):
                        print(
                            f"[event:duplicate] code={match_event.code} "
                            f"key={match_event.event_key}"
                        )
                        continue
                    jobs.append(
                        EventJob(
                            match_event=match_event,
                            pending=pending,
                            observed_stream_time=stream_time,
                            observed_source_time=observed_source_time,
                        )
                    )
                    print(
                        f"[event] code={match_event.code} type={match_event.event_type} "
                        f"minute={match_event.minute} person={match_event.person or '-'} "
                        f"observed_stream={stream_time:.2f}s clip_anchor={clip_anchor:.2f}s"
                    )

                snapshot_revision = getattr(event_source, "snapshot_revision", 0)
                if snapshot_revision > persisted_snapshot_revision:
                    runtime.store.remember_event_snapshot(
                        args.match_id,
                        set(getattr(event_source, "seen", set())),
                        aliases={
                            key: asdict(event)
                            for key, event in getattr(
                                getattr(event_source, "revisions", None),
                                "snapshot",
                                lambda: {},
                            )().items()
                        },
                    )
                    persisted_snapshot_revision = snapshot_revision

                for job in jobs:
                    pending = job.pending
                    ingest_running = (
                        supervisor.process is not None
                        and supervisor.process.poll() is None
                    )
                    if (
                        not ingest_running
                        or pending.status != "pending"
                        or stream_time < pending.output_due_stream_time
                    ):
                        continue
                    submitted = task_pool.submit(
                        job.match_event.event_key,
                        encode_event_job,
                        job,
                        runtime,
                        ffmpeg,
                        ffprobe,
                        segment_reader,
                        args.output_dir,
                        before=args.before,
                        after=args.after,
                        width=args.gif_width,
                        fps=args.gif_fps,
                        colors=args.gif_colors,
                        size_reference_bytes=int(args.gif_size_reference_mb * 1_000_000),
                        state_lock=state_lock,
                        cancel_event=graceful_stop_cancel_encodes,
                    )
                    if submitted:
                        pending.status = "encoding"

                for event_key, completed, error in task_pool.collect_done():
                    job = next(
                        item for item in jobs if item.match_event.event_key == event_key
                    )
                    if error is not None:
                        job.pending.status = "failed"
                        job.pending.result = {"error": str(error)}
                        with state_lock:
                            stored = runtime.store.get(event_key)
                            if stored is not None and stored.status == "encoding":
                                runtime.transition(
                                    event_key,
                                    "failed",
                                    result=job.pending.result,
                                    error=str(error),
                                )
                        print(f"[gif:worker:error] key={event_key} {error}")
                    elif completed:
                        job.pending.status = "encoded"
                    else:
                        job.pending.status = "pending"
                        job.pending.output_due_stream_time = (
                            stream_time + args.segment_seconds
                        )

                if now_monotonic - last_heartbeat_monotonic >= 3.0:
                    heartbeat_segments = segment_reader()
                    status_counts = {
                        status: sum(job.pending.status == status for job in jobs)
                        for status in ("pending", "encoding", "encoded", "failed")
                    }
                    coverage_seconds = 0.0
                    if heartbeat_segments:
                        coverage_seconds = max(
                            0.0,
                            heartbeat_segments[-1].end - heartbeat_segments[0].start,
                        )
                    runtime.logger.log(
                        "runtime_heartbeat",
                        match_id=args.match_id,
                        stream_time_sec=round(stream_time, 3),
                        event_poll_count=getattr(event_source, "poll_count", 0),
                        event_error_count=getattr(event_source, "error_count", 0),
                        last_event_error=getattr(event_source, "last_error", None),
                        ingest_running=(
                            supervisor.process is not None
                            and supervisor.process.poll() is None
                        ),
                        ingest_restart_count=supervisor.restart_count,
                        ingest_reconnect_due_unix=(
                            time.time()
                            + max(0.0, reconnect_due_monotonic - now_monotonic)
                            if reconnect_due_monotonic is not None
                            else None
                        ),
                        buffer_segment_count=len(heartbeat_segments),
                        buffer_coverage_seconds=round(coverage_seconds, 3),
                        **{f"{status}_count": count for status, count in status_counts.items()},
                    )
                    last_heartbeat_monotonic = now_monotonic

                if graceful_stop_started_monotonic is not None:
                    grace_seconds = (
                        args.graceful_stop_grace_seconds
                        if args.graceful_stop_grace_seconds is not None
                        else args.after + args.segment_slack
                    )
                    stop_elapsed = now_monotonic - graceful_stop_started_monotonic
                    pending_due = [
                        job.pending.output_due_stream_time
                        for job in jobs
                        if job.pending.status == "pending"
                    ]
                    encodes_active = bool(task_pool.futures)
                    drain_ready = (
                        stop_elapsed >= grace_seconds
                        and not encodes_active
                        and not pending_due
                    )
                    timed_out = stop_elapsed >= args.graceful_stop_timeout_seconds
                    if (drain_ready or timed_out) and not graceful_stop_ingest_stopped:
                        graceful_stop_timed_out = timed_out and not drain_ready
                        if graceful_stop_timed_out:
                            graceful_stop_cancel_encodes.set()
                        graceful_stop_ingest_stopped = True
                        supervisor.terminate()
                        runtime.logger.log(
                            "graceful_stop_ingest",
                            match_id=args.match_id,
                            timed_out=graceful_stop_timed_out,
                            elapsed_seconds=round(stop_elapsed, 3),
                            pending_count=len(pending_due),
                        )
                        print(
                            "[shutdown] stopping ingest; waiting for GIF jobs"
                            + (" (timeout)" if graceful_stop_timed_out else ""),
                            flush=True,
                        )
                        if supervisor.process is None:
                            # We may already be between reconnect attempts. In
                            # that state there is no child process whose exit
                            # observe_exit() could collect.
                            return_code = process.wait()
                            graceful_stop_stream_incomplete = (
                                graceful_stop_timed_out
                                and any(
                                    job.pending.status in ("pending", "encoding")
                                    for job in jobs
                                )
                            )
                            stop_reason = (
                                "match_played_stream_incomplete"
                                if graceful_stop_timed_out
                                else "match_played"
                            )
                            break

                if graceful_stop_ingest_stopped:
                    # Wait for observe_exit() to collect FFmpeg's return code.
                    time.sleep(0.1)
                    continue

                current_second = int(stream_time)
                if current_second != last_prune_second:
                    prune_buffer(
                        segment_reader(),
                        stream_time,
                        args.buffer_seconds,
                        [job.pending for job in jobs],
                        args.before,
                    )
                    last_prune_second = current_second
                time.sleep(0.1)
    except KeyboardInterrupt:
        stopped_by_user = True
        stop_reason = "manual_stop"
        supervisor.terminate()
        return_code = process.wait()
    finally:
        if (
            graceful_stop_started_monotonic is not None
            and not graceful_stop_ingest_stopped
        ):
            graceful_stop_cancel_encodes.set()
        supervisor.close()
        task_pool.shutdown(wait=True)
        if graceful_signal is not None and previous_graceful_handler is not None:
            signal.signal(graceful_signal, previous_graceful_handler)

    for event_key, completed, error in task_pool.collect_done():
        job = next(item for item in jobs if item.match_event.event_key == event_key)
        if error is not None:
            job.pending.status = "failed"
            job.pending.result = {"error": str(error)}
            with state_lock:
                stored = runtime.store.get(event_key)
                if stored is not None and stored.status == "encoding":
                    runtime.transition(
                        event_key,
                        "failed",
                        result=job.pending.result,
                        error=str(error),
                    )
        elif completed:
            job.pending.status = "encoded"
        else:
            job.pending.status = "pending"

    final_elapsed_wall = time.monotonic() - pipeline_started_mono
    final_stream_time = final_elapsed_wall * (
        args.replay_speed if args.simulate_live else 1.0
    )
    for job in jobs:
        pending = job.pending
        if pending.status != "pending" or final_stream_time < pending.stream_time + args.after:
            continue
        encode_event_job(
            job,
            runtime,
            ffmpeg,
            ffprobe,
            segment_reader,
            args.output_dir,
            before=args.before,
            after=args.after,
            width=args.gif_width,
            fps=args.gif_fps,
            colors=args.gif_colors,
            size_reference_bytes=int(args.gif_size_reference_mb * 1_000_000),
            state_lock=state_lock,
            cancel_event=graceful_stop_cancel_encodes,
        )

    if graceful_stop_timed_out:
        for job in jobs:
            if job.pending.status != "pending":
                continue
            job.pending.status = "failed"
            job.pending.result = {
                "error": "graceful stop timeout before the final video window was available",
                "error_kind": "graceful_stop_timeout",
            }
            with state_lock:
                stored = runtime.store.get(job.match_event.event_key)
                if stored is not None and stored.status == "pending":
                    runtime.transition(
                        job.match_event.event_key,
                        "failed",
                        result=job.pending.result,
                        error=job.pending.result["error"],
                    )

    if graceful_stop_stream_incomplete:
        for job in jobs:
            if job.pending.status != "pending":
                continue
            job.pending.status = "failed"
            job.pending.result = {
                "error": "live stream ended before the final video window was available",
                "error_kind": "video_gap",
            }
            with state_lock:
                stored = runtime.store.get(job.match_event.event_key)
                if stored is not None and stored.status == "pending":
                    runtime.transition(
                        job.match_event.event_key,
                        "failed",
                        result=job.pending.result,
                        error=job.pending.result["error"],
                    )

    if stop_reason == "ingest_exit":
        stop_reason = "ingest_completed" if return_code == 0 else "ingest_error"
    completion_state = None
    if stop_reason == "match_played":
        completion_state = (
            "completed_with_warnings"
            if any(job.pending.status == "failed" for job in jobs)
            else "completed"
        )
    elif stop_reason == "match_played_stream_incomplete":
        completion_state = "completed_with_warnings"

    report = {
        "source": args.source,
        "match_id": args.match_id,
        "run_id": run_id,
        "started_at_unix": pipeline_started_wall,
        "processing_wall_seconds": round(final_elapsed_wall, 3),
        "processed_stream_seconds": round(final_stream_time, 3),
        "ffmpeg_return_code": return_code,
        "stopped_by_user": stopped_by_user,
        "stop_reason": stop_reason,
        "exit_reason": stop_reason,
        "completion_state": completion_state,
        "graceful_stop_requested": graceful_stop_started_monotonic is not None,
        "graceful_stop_timed_out": graceful_stop_timed_out,
        "event_source": event_source.report(),
        "runtime": {
            "state_database": str(state_db_path.resolve()),
            "event_log": str(event_log_path.resolve()),
            "recovered_task_count": recovered_jobs,
            "gif_workers": args.gif_workers,
            "ingest_restart_count": supervisor.restart_count,
        },
        "supported_event_codes": SUPPORTED_EVENT_CODES,
        "timeline": {
            "source_start_seconds": args.start if source_is_local(args.source) else None,
            "event_to_video_offset_seconds": args.event_to_video_offset,
        },
        "buffer": {
            "segment_seconds": args.segment_seconds,
            "window_seconds": args.buffer_seconds,
        },
        "gif": {
            "before_seconds": args.before,
            "after_seconds": args.after,
            "width": args.gif_width,
            "fps": args.gif_fps,
            "colors": args.gif_colors,
            "size_reference_bytes": int(args.gif_size_reference_mb * 1_000_000),
            "adaptive_quality_reduction": False,
        },
        "events": [
            {
                **asdict(job.match_event),
                "observed_stream_time_sec": round(job.observed_stream_time, 3),
                "observed_source_time_sec": (
                    round(job.observed_source_time, 3)
                    if job.observed_source_time is not None
                    else None
                ),
                "clip_anchor_stream_time_sec": round(job.pending.stream_time, 3),
                "clip_anchor_source_time_sec": job.pending.source_time,
                "status": job.pending.status,
                **job.pending.result,
            }
            for job in jobs
        ],
    }
    serialized_report = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    run_report_path.write_text(serialized_report, encoding="utf-8")
    report_path.write_text(serialized_report, encoding="utf-8")
    runtime.logger.log(
        "pipeline_stopped",
        match_id=args.match_id,
        ffmpeg_return_code=return_code,
        stopped_by_user=stopped_by_user,
        stop_reason=stop_reason,
        graceful_stop_requested=graceful_stop_started_monotonic is not None,
        graceful_stop_timed_out=graceful_stop_timed_out,
        processing_wall_seconds=round(final_elapsed_wall, 3),
        event_poll_count=getattr(event_source, "poll_count", 0),
        event_error_count=getattr(event_source, "error_count", 0),
    )
    print(f"[report] {run_report_path.resolve()}")
    print(f"[report:latest] {report_path.resolve()}")
    runtime.close()
    if (
        return_code != 0
        and not stopped_by_user
        and stop_reason not in ("match_played", "match_played_stream_incomplete")
    ):
        raise SystemExit(f"ingest ffmpeg exited with status {return_code}")


if __name__ == "__main__":
    main()
