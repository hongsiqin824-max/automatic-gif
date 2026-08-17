#!/usr/bin/env python3
"""Generate football event GIFs from a live buffer and a match-event feed.

The default path continuously keeps a rolling MPEG-TS buffer and never depends
on visual inference. New G/OG, YC, and RC records create a fast default GIF;
an optional second path can inspect the scoreboard and then fall back to
T-DEED without changing the default artifact.
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
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from artifact_naming import build_gif_filename
from event_snapshot_replay import SnapshotReplayEventSource
from heavy_task_coordinator import (
    DEFAULT_DATABASE_PATH as DEFAULT_HEAVY_TASK_COORDINATOR_DATABASE,
    HeavyTaskCancelled,
    HeavyTaskCoordinator,
    HeavyTaskCoordinatorError,
    HeavyTaskUnavailable,
    run_with_task_slot,
)
from live_runtime import BoundedTaskPool, IngestSupervisor
from live_goal_pipeline import (
    BufferNotReady,
    BufferUnavailable,
    CoverageStatus,
    PendingEvent,
    analyze_video_coverage,
    encode_gif,
    prune_buffer,
    read_segments,
    source_is_local,
)
from event_api_response import normalize_event_api_response
from disk_lifecycle import CleanupSummary, DiskLifecycleManager, DiskLifecyclePolicy
from pipeline_runtime import (
    PipelineRuntime,
    StoredTask,
    TimelineState,
    coarse_event_elapsed_seconds,
)
from segment_manifest import (
    SegmentManifest,
    SegmentManifestError,
    load_segment_manifest,
    new_segment_manifest,
    resolve_generation_path,
    save_segment_manifest,
    upsert_segment_generation,
)
from scoreboard_ocr import ScoreboardOcrError, resolve_scoreboard_profile
from vision_runtime import (
    OCR_MINUTE_FALLBACK_AFTER_SECONDS,
    OCR_MINUTE_FALLBACK_COLORS,
    OCR_MINUTE_FALLBACK_BEFORE_SECONDS,
    OCR_MINUTE_FALLBACK_FPS,
    OCR_MINUTE_FALLBACK_WIDTH,
    OCR_PYTHON,
    VisionJob,
    find_python,
    refine_event_job,
)
from match_event_identity import (
    events_represent_same_incident,
    explicit_event_id,
    meaningful_id,
    merge_event_metadata,
)


SUPPORTED_EVENT_CODES = {
    "G": "goal",
    "OG": "goal",
    "YC": "yellow_card",
    "RC": "red_card",
}
YELLOW_CARD_REVISION_WINDOW_SECONDS = 180.0


def load_scoreboard_profile(path: Path | None) -> dict[str, Any] | None:
    """Load and normalize one explicit per-layout scoreboard profile."""
    if path is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"cannot read scoreboard profile {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"scoreboard profile is not valid JSON: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError("scoreboard profile JSON must be an object")
    try:
        return resolve_scoreboard_profile(payload).to_payload()
    except ScoreboardOcrError as exc:
        raise ValueError(str(exc)) from exc


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
    match_clock_anchor_stream_time: float | None = None
    vision_search_start_stream_time: float | None = None
    vision_search_end_stream_time: float | None = None


BEIJING = ZoneInfo("Asia/Shanghai")
MATCH_START_NAIVE_TIMEZONES = {
    "beijing": BEIJING,
    "utc": ZoneInfo("UTC"),
}
TIMELINE_FLOAT_TOLERANCE_SECONDS = 1e-3


def latest_media_tail_stream_time(segments: list[Any]) -> float | None:
    """Return the latest buffered media timestamp available at observation."""
    return max((float(segment.end) for segment in segments), default=None)


def event_timing_diagnostics(
    job: EventJob,
    *,
    before: float,
    after: float,
) -> dict[str, Any]:
    """Return a stable timing sample, including fallbacks for older tasks."""
    stored = job.match_event.metadata.get("timing_diagnostics")
    if not isinstance(stored, dict):
        stored = {}
    defaults = {
        "api_request_started_at_unix": None,
        "api_request_finished_at_unix": None,
        "api_request_duration_seconds": None,
        "api_request_succeeded": None,
        "first_observed_wall_time_unix": job.pending.detected_wall_time,
        "first_observed_stream_time_sec": job.observed_stream_time,
        "media_tail_stream_time_sec": None,
        "media_tail_lag_seconds": None,
        "event_to_video_offset_seconds": (
            job.pending.stream_time - job.observed_stream_time
        ),
        "clip_anchor_stream_time_sec": job.pending.stream_time,
        "requested_clip_start_stream_time_sec": max(
            0.0, job.pending.stream_time - before
        ),
        "requested_clip_end_stream_time_sec": job.pending.stream_time + after,
    }
    return {**defaults, **stored}


def merge_observed_event_revision(
    current: MatchEvent,
    update: MatchEvent,
) -> MatchEvent:
    """Apply feed enrichment without losing first-observation diagnostics."""
    merged = merge_event_metadata(asdict(current), asdict(update))
    merged["event_key"] = current.event_key
    return MatchEvent(**merged)


def heavy_task_coordinator_database(output_dir: Path) -> Path:
    """Use one production database while keeping external test runs isolated."""
    project_output = Path(__file__).resolve().parent / "output_gifs"
    resolved_output = Path(output_dir).resolve()
    try:
        resolved_output.relative_to(project_output)
    except ValueError:
        return resolved_output / "heavy_task_coordinator.sqlite3"
    return DEFAULT_HEAVY_TASK_COORDINATOR_DATABASE


def heavy_snapshot_has_default_gif_work(snapshot: dict[str, Any]) -> bool:
    """Return whether any Worker is running or waiting to run a default GIF."""
    active = (snapshot.get("active") or {}).get("items") or []
    waiting = (snapshot.get("waiting") or {}).get("items") or []
    return any(
        str(item.get("task_kind") or "") == "gif"
        for item in [*active, *waiting]
        if isinstance(item, dict)
    )


def stream_rate_for_mode(*, simulate_live: bool, replay_speed: float) -> float:
    """Return the stream-clock rate used by both ingest and wall-clock math.

    Local recordings are throttled by FFmpeg's ``-readrate`` option, so their
    synthetic stream clock advances at ``replay_speed``.  A real RTMP source
    has no replay-speed option and must remain a one-second-per-second clock;
    passing a non-default replay speed must never distort its event anchors.
    """
    rate = float(replay_speed) if simulate_live else 1.0
    if rate <= 0:
        raise ValueError("stream rate must be positive")
    return rate


def resumed_stream_time(
    timeline: TimelineState,
    *,
    pipeline_started_wall: float,
    simulate_live: bool,
    replay_speed: float,
) -> float:
    """Choose a restart clock without inventing media during local downtime.

    An RTMP stream continues advancing while this process is down, whereas a
    local ``--simulate-live`` file is paused.  The latter must resume from the
    durable checkpoint; otherwise the wall-clock gap would make the new FFmpeg
    generation claim stream timestamps for content it never consumed.
    """
    rate = stream_rate_for_mode(
        simulate_live=simulate_live,
        replay_speed=replay_speed,
    )
    if simulate_live:
        return max(timeline.last_stream_time, timeline.timeline_origin_stream_time)
    return max(
        timeline.last_stream_time,
        timeline.timeline_origin_stream_time
        + (float(pipeline_started_wall) - timeline.timeline_origin_wall_unix) * rate,
    )


def vision_search_window(
    *,
    clip_anchor: float,
    match_clock_anchor: float | None,
    buffer_seconds: float,
    segment_slack: float,
    search_before: float,
    search_after: float,
    minute_uncertainty: float,
) -> tuple[float, float]:
    """Return a retained window before the raw API observation timestamp.

    ``clip_anchor`` is intentionally the unshifted first-observed stream time
    for this helper.  The default GIF may use a negative offset, but OCR must
    search backwards from the raw API observation instead of subtracting that
    offset twice.  Match-minute estimates are retained as diagnostic inputs
    only and never alter this search window.
    """
    if buffer_seconds <= segment_slack:
        raise ValueError("buffer_seconds must exceed segment_slack")
    if search_before < 0 or search_after < 0 or minute_uncertainty < 0:
        raise ValueError("vision search durations must not be negative")
    del match_clock_anchor, minute_uncertainty
    clip_anchor = max(0.0, float(clip_anchor))
    api_start = clip_anchor - float(search_before)
    retention_floor = max(
        0.0,
        clip_anchor - (float(buffer_seconds) - float(segment_slack)),
    )
    search_start = max(retention_floor, api_start)
    search_end = clip_anchor + float(search_after)
    return search_start, search_end


def vision_deadline_at(
    *,
    detected_at_unix: float,
    current_stream_time: float,
    search_end_stream_time: float,
    stream_rate: float,
    segment_slack: float,
    configured_deadline_seconds: float,
) -> tuple[float, float]:
    """Return an SLA deadline that cannot expire before the requested window.

    ``configured_deadline_seconds`` is the minimum readiness budget.  If the
    match-clock uncertainty extends the search farther into a live stream,
    the deadline is extended by the equivalent wall-clock wait; otherwise the
    worker would submit a model search before the promised window exists.
    """
    rate = float(stream_rate)
    if rate <= 0:
        raise ValueError("stream rate must be positive")
    if segment_slack < 0 or configured_deadline_seconds <= 0:
        raise ValueError("vision deadline and segment slack must be valid")
    required_wait = max(
        0.0,
        (
            float(search_end_stream_time)
            + float(segment_slack)
            - float(current_stream_time)
        )
        / rate,
    )
    wait_budget = max(float(configured_deadline_seconds), required_wait)
    return float(detected_at_unix) + wait_budget, wait_budget


def timeline_calibration_mismatches(
    timeline: TimelineState,
    *,
    match_start_at_unix: float | None,
    broadcast_delay_seconds: float,
    halftime_break_seconds: float,
) -> dict[str, tuple[Any, Any]]:
    """Describe requested calibration changes for an already-persisted clock."""
    requested = {
        "match_start_at_unix": match_start_at_unix,
        "broadcast_delay_seconds": float(broadcast_delay_seconds),
        "halftime_break_seconds": float(halftime_break_seconds),
    }
    stored = {
        "match_start_at_unix": timeline.match_start_at_unix,
        "broadcast_delay_seconds": timeline.broadcast_delay_seconds,
        "halftime_break_seconds": timeline.halftime_break_seconds,
    }
    mismatches: dict[str, tuple[Any, Any]] = {}
    for field_name, stored_value in stored.items():
        requested_value = requested[field_name]
        if stored_value is None or requested_value is None:
            if stored_value != requested_value:
                mismatches[field_name] = (stored_value, requested_value)
            continue
        if (
            abs(float(stored_value) - float(requested_value))
            > TIMELINE_FLOAT_TOLERANCE_SECONDS
        ):
            mismatches[field_name] = (stored_value, requested_value)
    return mismatches


def manifest_for_active_source(
    manifest: SegmentManifest,
    *,
    requested_source: str,
    timeline_origin_wall: float,
) -> tuple[SegmentManifest, int]:
    """Detach old media generations when a match moves to a new live source.

    Event/GIF task state remains in SQLite, but media from an unverifiable URL
    must not be mixed with the replacement stream.  The old files are left on
    disk for audit/recovery; replacing the manifest merely removes them from
    the active segment reader.
    """
    if manifest.source == requested_source:
        return manifest, 0
    discarded_generation_count = (
        len(manifest.generations) + len(manifest.stale_list_paths)
    )
    return (
        new_segment_manifest(
            manifest.match_id,
            requested_source,
            timeline_origin_wall,
        ),
        discarded_generation_count,
    )


def parse_match_start_play(
    value: str | None,
    *,
    naive_timezone: str = "beijing",
) -> float | None:
    """Parse start_play, applying an explicit timezone to naive date-times."""
    if value is None or not str(value).strip():
        return None
    raw = str(value).strip()
    try:
        numeric = float(raw)
    except ValueError:
        numeric = None
    if numeric is not None:
        if numeric < 0:
            raise ValueError("match start timestamp must not be negative")
        return numeric
    normalized = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            "match start must be a Unix timestamp or ISO date-time"
        ) from exc
    if parsed.tzinfo is None:
        try:
            timezone = MATCH_START_NAIVE_TIMEZONES[naive_timezone]
        except KeyError as exc:
            supported = ", ".join(MATCH_START_NAIVE_TIMEZONES)
            raise ValueError(
                f"match start naive timezone must be one of: {supported}"
            ) from exc
        parsed = parsed.replace(tzinfo=timezone)
    return parsed.timestamp()


def recovered_event_job(task: StoredTask) -> EventJob:
    """Rebuild an in-memory job from durable state after a process restart."""
    match_event = MatchEvent(**task.event_data)
    timeline_metadata = (
        task.event_data.get("metadata")
        if isinstance(task.event_data.get("metadata"), dict)
        else {}
    )
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
        match_clock_anchor_stream_time=(
            timeline_metadata.get("match_clock_anchor_stream_time")
        ),
        vision_search_start_stream_time=(
            timeline_metadata.get("vision_search_start_stream_time")
        ),
        vision_search_end_stream_time=(
            timeline_metadata.get("vision_search_end_stream_time")
        ),
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
    allow_degraded: bool = False,
    min_degraded_seconds: float = 2.0,
    heavy_task_coordinator: HeavyTaskCoordinator | None = None,
    wait_for_heavy_slot: bool = True,
) -> bool:
    """Check coverage, then perform at most one real encoding attempt."""
    pending = job.pending
    lock = state_lock or threading.Lock()
    now = time.time()
    current = runtime.store.get(job.match_event.event_key)
    if current is None:
        raise KeyError(f"unknown event task: {job.match_event.event_key}")
    if now < current.next_attempt_at_unix:
        pending.status = "pending"
        return False
    segments = segment_reader()
    coverage = analyze_video_coverage(
        segments,
        window_start=max(0.0, pending.stream_time - before),
        window_end=pending.stream_time + after,
        anchor=pending.stream_time,
        allow_degraded=allow_degraded,
        min_degraded_seconds=min_degraded_seconds,
    )
    if coverage.status == CoverageStatus.WAITING:
        if now >= current.deadline_at_unix:
            coverage = analyze_video_coverage(
                segments,
                window_start=max(0.0, pending.stream_time - before),
                window_end=pending.stream_time + after,
                anchor=pending.stream_time,
                allow_degraded=allow_degraded,
                force_degraded=allow_degraded,
                min_degraded_seconds=min_degraded_seconds,
            )
            if coverage.status != CoverageStatus.READY_DEGRADED:
                error = (
                    "video window was not ready before the GIF deadline: "
                    f"{coverage.reason}"
                )
                error_kind = (
                    coverage.error_kind
                    if coverage.status == CoverageStatus.UNAVAILABLE
                    else "buffer_deadline_exceeded"
                )
                pending.status = "failed"
                pending.result = {
                    "error": error,
                    "error_kind": error_kind,
                }
                with lock:
                    runtime.transition(
                        job.match_event.event_key,
                        "failed",
                        result=pending.result,
                        error=error,
                        error_kind=error_kind,
                    )
                return True
        pending.status = "pending"
        if coverage.status == CoverageStatus.WAITING:
            runtime.record_readiness_wait(
                job.match_event.event_key,
                coverage.reason,
                error_kind=coverage.error_kind or "waiting_for_video",
            )
            return False
    if coverage.status == CoverageStatus.UNAVAILABLE:
        error_kind = coverage.error_kind or "video_unavailable"
        pending.status = "failed"
        pending.result = {"error": coverage.reason, "error_kind": error_kind}
        with lock:
            runtime.transition(
                job.match_event.event_key,
                "failed",
                result=pending.result,
                error=coverage.reason,
                error_kind=error_kind,
            )
        print(f"[gif:error:{error_kind}] code={job.match_event.code} {coverage.reason}")
        return True

    try:
        slot = (
            heavy_task_coordinator.acquire(
                "gif",
                match_id=job.match_event.metadata.get("match_id")
                or job.match_event.event_key.split(":", 1)[0],
                event_key=job.match_event.event_key,
                cancel_event=cancel_event,
                wait=wait_for_heavy_slot,
            )
            if heavy_task_coordinator is not None
            else nullcontext()
        )
        with slot:
            latest_task = runtime.store.get(job.match_event.event_key)
            if latest_task is None:
                raise KeyError(f"unknown event task: {job.match_event.event_key}")
            output_filename = build_gif_filename(
                match_id=latest_task.match_id,
                event_data=latest_task.event_data,
                variant="default",
            )
            pending.status = "encoding"
            with lock:
                runtime.transition(job.match_event.event_key, "encoding")
            encoded = encode_gif(
                ffmpeg,
                ffprobe,
                segments,
                pending,
                output_dir,
                before=before,
                after=after,
                width=width,
                fps=fps,
                colors=colors,
                size_reference_bytes=size_reference_bytes,
                cancel_event=cancel_event,
                coverage=coverage,
                allow_degraded=allow_degraded,
                output_filename=output_filename,
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
    except (HeavyTaskCancelled, HeavyTaskUnavailable):
        pending.status = "pending"
        return False
    except HeavyTaskCoordinatorError as exc:
        pending.status = "pending"
        runtime.logger.log(
            "heavy_task_coordinator_error",
            match_id=job.match_event.event_key.split(":", 1)[0],
            event_key=job.match_event.event_key,
            task_kind="gif",
            error=str(exc),
        )
        return False
    except BufferNotReady as exc:
        with lock:
            runtime.transition(
                job.match_event.event_key,
                "pending",
                reason="buffer_not_ready",
            )
        pending.status = "pending"
        current = runtime.store.get(job.match_event.event_key)
        if current is not None and time.time() >= current.deadline_at_unix:
            error = f"video window was not ready before the GIF deadline: {exc}"
            pending.status = "failed"
            pending.result = {
                "error": error,
                "error_kind": "buffer_deadline_exceeded",
            }
            with lock:
                runtime.transition(
                    job.match_event.event_key,
                    "failed",
                    result=pending.result,
                    error=error,
                    error_kind="buffer_deadline_exceeded",
                )
            return True
        runtime.record_readiness_wait(
            job.match_event.event_key,
            str(exc),
            error_kind="waiting_for_video",
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
                error_kind="video_gap",
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

    def __init__(
        self,
        aliases: dict[str, MatchEvent] | None = None,
        *,
        previous_versions: dict[str, MatchEvent] | None = None,
        first_seen_at: dict[str, float] | None = None,
        yellow_card_revision_window_seconds: float = (
            YELLOW_CARD_REVISION_WINDOW_SECONDS
        ),
    ) -> None:
        self.aliases = dict(aliases or {})
        self.previous_versions = dict(previous_versions or {})
        self.first_seen_at = {
            str(key): float(value) for key, value in (first_seen_at or {}).items()
        }
        self.yellow_card_revision_window_seconds = float(
            yellow_card_revision_window_seconds
        )
        if self.yellow_card_revision_window_seconds < 0:
            raise ValueError("yellow-card revision window must not be negative")
        self.canonical_events: dict[str, MatchEvent] = {}
        for event in self.aliases.values():
            current = self.canonical_events.get(event.event_key)
            if current is None:
                self.canonical_events[event.event_key] = event
            else:
                merged = merge_event_metadata(asdict(current), asdict(event))
                self.canonical_events[event.event_key] = MatchEvent(**merged)

    def reconcile(
        self,
        current: list[MatchEvent],
        *,
        observed_at_unix: float | None = None,
    ) -> list[MatchEvent]:
        observed_at = time.time() if observed_at_unix is None else observed_at_unix
        current_versions = {event.event_key: event for event in current}
        for version_key in current_versions:
            self.first_seen_at.setdefault(version_key, observed_at)
        self._reconcile_yellow_card_replacements(current_versions, observed_at)

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
        self.previous_versions = current_versions
        return reconciled

    def _reconcile_yellow_card_replacements(
        self,
        current_versions: dict[str, MatchEvent],
        observed_at_unix: float,
    ) -> None:
        """Alias a unique empty-player YC replacement to its earlier version."""
        removed = [
            event
            for key, event in self.previous_versions.items()
            if key not in current_versions
        ]
        added = [
            event
            for key, event in current_versions.items()
            if key not in self.previous_versions
        ]
        old_candidates: dict[str, list[MatchEvent]] = {}
        new_candidates: dict[str, list[MatchEvent]] = {}
        for old in removed:
            for new in added:
                if not self._is_yellow_card_replacement(
                    old,
                    new,
                    observed_at_unix,
                ):
                    continue
                old_candidates.setdefault(old.event_key, []).append(new)
                new_candidates.setdefault(new.event_key, []).append(old)

        for old in removed:
            possible_new = old_candidates.get(old.event_key, [])
            if len(possible_new) != 1:
                continue
            new = possible_new[0]
            if len(new_candidates.get(new.event_key, [])) != 1:
                continue
            known = self.aliases.get(old.event_key)
            if known is None:
                continue
            existing_new = self.aliases.get(new.event_key)
            if (
                existing_new is not None
                and existing_new.event_key != known.event_key
            ):
                continue
            canonical_key = known.event_key
            merged = merge_event_metadata(
                asdict(self.canonical_events.get(canonical_key, known)),
                asdict(new),
            )
            merged["event_key"] = canonical_key
            canonical = MatchEvent(**merged)
            self.aliases[new.event_key] = canonical
            self.canonical_events[canonical_key] = canonical

    def _is_yellow_card_replacement(
        self,
        old: MatchEvent,
        new: MatchEvent,
        observed_at_unix: float,
    ) -> bool:
        if old.code != "YC" or new.code != "YC":
            return False
        if meaningful_id(old.person_id) or not meaningful_id(new.person_id):
            return False
        old_group = (old.code, old.team, old.minute, old.minute_extra)
        incomplete_in_group = [
            event
            for event in self.previous_versions.values()
            if (event.code, event.team, event.minute, event.minute_extra)
            == old_group
            and not meaningful_id(event.person_id)
        ]
        if len(incomplete_in_group) != 1:
            return False
        if (
            old.team,
            old.minute,
            old.minute_extra,
        ) != (
            new.team,
            new.minute,
            new.minute_extra,
        ):
            return False
        old_event_id = explicit_event_id(asdict(old))
        new_event_id = explicit_event_id(asdict(new))
        if old_event_id and new_event_id and old_event_id != new_event_id:
            return False
        old_person = old.person.strip()
        new_person = new.person.strip()
        if old_person and new_person and old_person != new_person:
            return False
        first_seen = self.first_seen_at.get(old.event_key)
        if first_seen is None:
            return False
        age = observed_at_unix - first_seen
        return 0.0 <= age <= self.yellow_card_revision_window_seconds

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

    def current_snapshot(self) -> dict[str, MatchEvent]:
        return dict(self.previous_versions)


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
        initial_snapshot: dict[str, MatchEvent] | None = None,
        initial_first_seen: dict[str, float] | None = None,
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
        self.last_request_started_at_unix: float | None = None
        self.last_request_finished_at_unix: float | None = None
        self.last_request_duration_seconds: float | None = None
        self.last_request_succeeded: bool | None = None
        self.revisions = EventRevisionTracker(
            initial_aliases,
            previous_versions=initial_snapshot,
            first_seen_at=initial_first_seen,
        )
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
        self.last_request_started_at_unix = time.time()
        request_started_monotonic = time.monotonic()
        self.last_request_finished_at_unix = None
        self.last_request_duration_seconds = None
        self.last_request_succeeded = None
        try:
            request = urllib.request.Request(
                self._request_url(),
                headers={"Accept": "application/json", "User-Agent": "football-gif-pipeline/1.0"},
            )
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                response_body = response.read()
            self._finish_request(request_started_monotonic, succeeded=True)
            payload = json.loads(response_body.decode("utf-8"))
            payload = normalize_event_api_response(payload)
            current = self.revisions.reconcile(
                parse_match_events(payload, self.match_id),
                observed_at_unix=self.last_request_finished_at_unix,
            )
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
            self.latest_events = {event.event_key: event for event in current}
            self.last_error = None
            self.last_error_kind = None
            self.consecutive_errors = 0
            return new_events
        except urllib.error.HTTPError as exc:
            self._finish_request(request_started_monotonic, succeeded=False)
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
            self._finish_request(request_started_monotonic, succeeded=False)
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

    def _finish_request(self, started_monotonic: float, *, succeeded: bool) -> None:
        if self.last_request_finished_at_unix is None:
            self.last_request_finished_at_unix = time.time()
            self.last_request_duration_seconds = max(
                0.0, time.monotonic() - started_monotonic
            )
        self.last_request_succeeded = succeeded

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
            "last_request_started_at_unix": self.last_request_started_at_unix,
            "last_request_finished_at_unix": self.last_request_finished_at_unix,
            "last_request_duration_seconds": self.last_request_duration_seconds,
            "last_request_succeeded": self.last_request_succeeded,
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


def successful_segment_paths(segments: list[Any]) -> set[str]:
    """Return closed segment paths that exist and contain media bytes."""
    successful: set[str] = set()
    for segment in segments:
        path = Path(segment.path)
        try:
            if path.is_file() and path.stat().st_size > 0:
                successful.add(str(path.resolve()))
        except OSError:
            continue
    return successful


def observe_segment_progress(
    supervisor: IngestSupervisor,
    segments: list[Any],
    observed_paths: set[str],
) -> int:
    """Reset reconnect backoff when FFmpeg publishes new usable segments."""
    successful = successful_segment_paths(segments)
    new_paths = successful - observed_paths
    observed_paths.update(successful)
    if new_paths:
        supervisor.note_media_progress()
    return len(new_paths)


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
        default=-60.0,
        help="video event time minus the event's first-observed stream time",
    )
    parser.add_argument(
        "--match-start-play",
        help="match detail start_play as a Unix timestamp or ISO date-time",
    )
    parser.add_argument(
        "--match-start-naive-timezone",
        choices=tuple(MATCH_START_NAIVE_TIMEZONES),
        default="beijing",
        help=(
            "timezone for a start_play value without an offset "
            "(default: beijing for backward compatibility)"
        ),
    )
    parser.add_argument(
        "--broadcast-delay-seconds",
        type=float,
        default=0.0,
        help="estimated live broadcast delay used for coarse event anchors",
    )
    parser.add_argument(
        "--halftime-break-seconds",
        type=float,
        default=900.0,
        help="regular-time halftime duration used for coarse event anchors",
    )
    parser.add_argument(
        "--match-minute-uncertainty-seconds",
        type=float,
        default=60.0,
        help="backward search allowance for minute-resolution event times",
    )
    parser.add_argument("--simulate-live", action="store_true")
    parser.add_argument("--replay-speed", type=float, default=1.0)
    parser.add_argument("--start", type=float, default=0.0)
    parser.add_argument("--duration", type=float)
    parser.add_argument("--segment-seconds", type=float, default=2.0)
    parser.add_argument("--buffer-seconds", type=float, default=360.0)
    parser.add_argument("--before", type=float, default=10.0)
    parser.add_argument("--after", type=float, default=20.0)
    parser.add_argument("--segment-slack", type=float, default=7.0)
    parser.add_argument("--gif-width", type=int, default=768)
    parser.add_argument("--gif-fps", type=float, default=16.0)
    parser.add_argument("--gif-colors", type=int, default=256)
    parser.add_argument("--gif-size-reference-mb", type=float, default=10.0)
    parser.add_argument("--gif-deadline-seconds", type=float, default=55.0)
    parser.add_argument("--min-degraded-gif-seconds", type=float, default=2.0)
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
    parser.add_argument("--vision-enabled", action="store_true")
    parser.add_argument("--vision-search-before", type=float, default=300.0)
    parser.add_argument("--vision-search-after", type=float, default=0.0)
    parser.add_argument("--vision-before", type=float, default=8.0)
    parser.add_argument("--vision-after", type=float, default=12.0)
    parser.add_argument("--vision-workers", type=int, default=1)
    parser.add_argument("--vision-timeout-seconds", type=float, default=90.0)
    parser.add_argument("--ocr-python", type=Path, default=OCR_PYTHON)
    parser.add_argument("--ocr-timeout-seconds", type=float, default=45.0)
    parser.add_argument(
        "--scoreboard-profile",
        type=Path,
        help="JSON file containing reference resolution, clock_roi, and score_roi",
    )
    parser.add_argument(
        "--fallback-gif-width", type=int, default=OCR_MINUTE_FALLBACK_WIDTH
    )
    parser.add_argument(
        "--fallback-gif-fps", type=float, default=OCR_MINUTE_FALLBACK_FPS
    )
    parser.add_argument(
        "--fallback-gif-colors", type=int, default=OCR_MINUTE_FALLBACK_COLORS
    )
    parser.add_argument(
        "--vision-deadline-seconds",
        type=float,
        default=60.0,
        help=(
            "minimum wall-clock budget for vision video readiness; extended "
            "when a calibrated search window needs more live video"
        ),
    )
    parser.add_argument(
        "--rtmp-max-reconnects",
        type=int,
        default=None,
        help="maximum FFmpeg restarts after RTMP errors (default: unlimited)",
    )
    parser.add_argument("--rtmp-reconnect-initial-seconds", type=float, default=2.0)
    parser.add_argument("--rtmp-reconnect-max-seconds", type=float, default=5.0)
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
    parser.add_argument(
        "--lifecycle-keep-ingest-logs",
        type=int,
        default=8,
        help="number of newest FFmpeg ingest logs to retain per match",
    )
    parser.add_argument(
        "--lifecycle-keep-run-reports",
        type=int,
        default=20,
        help="number of immutable run reports to retain per match",
    )
    parser.add_argument(
        "--lifecycle-event-log-max-mb",
        type=float,
        default=20.0,
        help="rotate pipeline_events.jsonl above this size after match completion",
    )
    parser.add_argument(
        "--lifecycle-event-log-archives",
        type=int,
        default=3,
        help="number of rotated event-log archives to retain",
    )
    parser.add_argument(
        "--post-match-buffer-retention-seconds",
        type=float,
        default=0.0,
        help="retain recent unprotected TS after a successful match (default: 0)",
    )
    args = parser.parse_args()

    try:
        scoreboard_profile = load_scoreboard_profile(args.scoreboard_profile)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    try:
        match_start_at_unix = parse_match_start_play(
            args.match_start_play,
            naive_timezone=args.match_start_naive_timezone,
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

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
        args.fallback_gif_width,
        args.fallback_gif_fps,
        args.gif_size_reference_mb,
        args.gif_workers,
        args.gif_deadline_seconds,
        args.min_degraded_gif_seconds,
        args.rtmp_reconnect_initial_seconds,
        args.rtmp_reconnect_max_seconds,
        args.graceful_stop_timeout_seconds,
    )
    if any(value <= 0 for value in positive) or args.before < 0 or args.start < 0:
        raise SystemExit("time, FPS, buffer, and size arguments must be positive")
    if (
        args.broadcast_delay_seconds < 0
        or args.halftime_break_seconds < 0
        or args.match_minute_uncertainty_seconds < 0
    ):
        raise SystemExit(
            "broadcast delay, halftime duration, and minute uncertainty "
            "must not be negative"
        )
    if not 2 <= args.gif_colors <= 256:
        raise SystemExit("--gif-colors must be between 2 and 256")
    if not 2 <= args.fallback_gif_colors <= 256:
        raise SystemExit("--fallback-gif-colors must be between 2 and 256")
    if args.rtmp_max_reconnects is not None and args.rtmp_max_reconnects < 0:
        raise SystemExit("--rtmp-max-reconnects cannot be negative")
    if (
        args.graceful_stop_grace_seconds is not None
        and args.graceful_stop_grace_seconds <= 0
    ):
        raise SystemExit("--graceful-stop-grace-seconds must be positive")
    if args.before + args.segment_slack >= args.buffer_seconds:
        raise SystemExit("buffer must be longer than before + segment slack")
    if args.vision_workers < 1 or args.vision_search_before < 0 or args.vision_search_after < 0:
        raise SystemExit("vision worker and search windows must be valid")
    if (
        args.vision_before < 0
        or args.vision_after <= 0
        or args.vision_timeout_seconds <= 0
        or args.ocr_timeout_seconds <= 0
        or args.vision_deadline_seconds <= 0
    ):
        raise SystemExit("vision output windows and timeout must be valid")
    if args.lifecycle_keep_ingest_logs < 1 or args.lifecycle_keep_run_reports < 1:
        raise SystemExit("lifecycle log and report retention counts must be positive")
    if args.lifecycle_event_log_max_mb <= 0 or args.lifecycle_event_log_archives < 0:
        raise SystemExit("lifecycle event-log limits must be valid")
    if args.post_match_buffer_retention_seconds < 0:
        raise SystemExit("post-match buffer retention cannot be negative")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    lifecycle = DiskLifecycleManager(
        args.output_dir,
        DiskLifecyclePolicy(
            keep_ingest_logs=args.lifecycle_keep_ingest_logs,
            keep_run_reports=args.lifecycle_keep_run_reports,
            event_log_max_bytes=int(args.lifecycle_event_log_max_mb * 1_000_000),
            event_log_archives=args.lifecycle_event_log_archives,
            post_match_buffer_seconds=args.post_match_buffer_retention_seconds,
        ),
    )
    buffer_dir = args.output_dir / "buffer"
    buffer_dir.mkdir(parents=True, exist_ok=True)
    segment_list = buffer_dir / "segments.csv"
    state_db_path = args.state_db or args.output_dir / "pipeline_state.sqlite3"
    event_log_path = args.event_log or args.output_dir / "pipeline_events.jsonl"
    runtime = PipelineRuntime(state_db_path, event_log_path)
    try:
        heavy_task_coordinator = HeavyTaskCoordinator.from_environment(
            heavy_task_coordinator_database(args.output_dir)
        )
    except HeavyTaskCoordinatorError as exc:
        runtime.close()
        raise SystemExit(f"cannot initialize heavy task coordinator: {exc}") from exc
    pipeline_started_wall = time.time()
    pipeline_started_mono = time.monotonic()

    manifest_path = buffer_dir / "segment_manifest.json"
    try:
        manifest = load_segment_manifest(
            manifest_path,
            expected_match_id=args.match_id,
            expected_source=None,
        )
    except SegmentManifestError as exc:
        heavy_task_coordinator.close()
        runtime.close()
        raise SystemExit(f"cannot safely restore video segment manifest: {exc}") from exc
    if manifest is not None:
        for health in manifest.generation_health:
            runtime.logger.log(
                "segment_manifest_generation_health",
                match_id=args.match_id,
                list_path=str(health.list_path),
                status=health.status,
                listed_segment_count=health.listed_segment_count,
                available_segment_count=health.available_segment_count,
                missing_media_paths=[str(path) for path in health.missing_media_paths],
                available_start_stream_time=health.available_start_stream_time,
                available_end_stream_time=health.available_end_stream_time,
            )
    stored_timeline = runtime.store.get_timeline_state(args.match_id)
    if stored_timeline is None:
        timeline_origin_wall = (
            manifest.timeline_origin_wall if manifest is not None else pipeline_started_wall
        )
        timeline = TimelineState(
            match_id=args.match_id,
            timeline_origin_wall_unix=timeline_origin_wall,
            match_start_at_unix=match_start_at_unix,
            broadcast_delay_seconds=args.broadcast_delay_seconds,
            halftime_break_seconds=args.halftime_break_seconds,
            created_at_unix=pipeline_started_wall,
            updated_at_unix=pipeline_started_wall,
        )
        runtime.store.upsert_timeline_state(timeline, now=pipeline_started_wall)
    else:
        timeline = stored_timeline
        calibration_mismatches = timeline_calibration_mismatches(
            timeline,
            match_start_at_unix=match_start_at_unix,
            broadcast_delay_seconds=args.broadcast_delay_seconds,
            halftime_break_seconds=args.halftime_break_seconds,
        )
        if calibration_mismatches:
            detail = ", ".join(
                f"{field_name}: stored={stored_value!r}, requested={requested_value!r}"
                for field_name, (stored_value, requested_value)
                in calibration_mismatches.items()
            )
            heavy_task_coordinator.close()
            runtime.close()
            raise SystemExit(
                "timeline calibration differs from the persisted match clock "
                f"({detail}); refusing to reinterpret buffered video. Use a fresh "
                "--output-dir (and a fresh --state-db when explicitly configured) "
                "to recalibrate this match."
            )

    if manifest is not None and abs(
        manifest.timeline_origin_wall - timeline.timeline_origin_wall_unix
    ) > 1e-3:
        heavy_task_coordinator.close()
        runtime.close()
        raise SystemExit(
            "timeline state and segment manifest have different origins; "
            "refusing to mix video clocks"
        )
    if manifest is None:
        manifest = new_segment_manifest(
            args.match_id,
            args.source,
            timeline.timeline_origin_wall_unix,
        )
        if segment_list.exists():
            manifest = upsert_segment_generation(
                manifest,
                list_path=segment_list.relative_to(buffer_dir),
                stream_offset=timeline.timeline_origin_stream_time,
                started_at_wall=timeline.timeline_origin_wall_unix,
            )
        save_segment_manifest(manifest_path, manifest)
    elif manifest.source != args.source:
        previous_source = manifest.source
        manifest, discarded_generation_count = manifest_for_active_source(
            manifest,
            requested_source=args.source,
            timeline_origin_wall=timeline.timeline_origin_wall_unix,
        )
        save_segment_manifest(manifest_path, manifest)
        runtime.logger.log(
            "segment_manifest_source_reset",
            match_id=args.match_id,
            previous_source=previous_source,
            source=args.source,
            discarded_generation_count=discarded_generation_count,
            old_media_deleted=False,
        )
    elif manifest.stale_list_paths:
        # Keep the durable file aligned after normal rolling-buffer pruning.
        save_segment_manifest(manifest_path, manifest)

    stream_rate = stream_rate_for_mode(
        simulate_live=args.simulate_live,
        replay_speed=args.replay_speed,
    )
    run_start_stream_time = resumed_stream_time(
        timeline,
        pipeline_started_wall=pipeline_started_wall,
        simulate_live=args.simulate_live,
        replay_speed=args.replay_speed,
    )
    replay_source_start = args.start
    replay_duration = args.duration
    if args.simulate_live:
        replay_consumed = max(
            0.0,
            run_start_stream_time - timeline.timeline_origin_stream_time,
        )
        replay_source_start += replay_consumed
        if replay_duration is not None:
            replay_duration -= replay_consumed
            if replay_duration <= 0:
                heavy_task_coordinator.close()
                runtime.close()
                raise SystemExit(
                    "the persisted simulated-live timeline already consumed the "
                    "configured --duration; use a fresh --output-dir to replay it "
                    "from the beginning"
                )

    def current_stream_time() -> float:
        return run_start_stream_time + (
            time.monotonic() - pipeline_started_mono
        ) * stream_rate

    def checkpoint_stream_time(stream_time: float) -> None:
        nonlocal timeline
        timeline = runtime.store.checkpoint_timeline(args.match_id, stream_time)

    def estimate_match_clock_anchor(match_event: MatchEvent) -> float | None:
        """Return a coarse match-clock anchor without replacing discovery time."""
        if timeline.match_start_at_unix is not None:
            try:
                elapsed = coarse_event_elapsed_seconds(
                    match_event.minute,
                    match_event.minute_extra,
                    halftime_break_seconds=timeline.halftime_break_seconds,
                )
                visible_wall = (
                    timeline.match_start_at_unix
                    + elapsed
                    + timeline.broadcast_delay_seconds
                )
                estimated = timeline.timeline_origin_stream_time + (
                    visible_wall - timeline.timeline_origin_wall_unix
                ) * stream_rate
                return max(0.0, estimated)
            except ValueError as exc:
                runtime.logger.log(
                    "event_timeline_estimate_failed",
                    match_id=args.match_id,
                    event_key=match_event.event_key,
                    minute=match_event.minute,
                    minute_extra=match_event.minute_extra,
                    error=str(exc),
                )
        return None

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
        snapshot_events, snapshot_first_seen = runtime.store.load_event_snapshot(
            args.match_id
        )
        initial_snapshot = {
            version_key: MatchEvent(**event_data)
            for version_key, event_data in snapshot_events.items()
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
            initial_snapshot=initial_snapshot,
            initial_first_seen=snapshot_first_seen,
        )

    report_path = args.output_dir / "event_pipeline_report.json"
    jobs = [recovered_event_job(task) for task in runtime.recover_incomplete(args.match_id)]
    vision_jobs: dict[str, VisionJob] = {}
    if args.vision_enabled:
        for task in runtime.recover_incomplete_vision(args.match_id):
            default_task = runtime.store.get(task.event_key)
            event_data = default_task.event_data if default_task is not None else {}
            vision_jobs[task.event_key] = VisionJob(
                event_key=task.event_key,
                match_id=task.match_id,
                code=task.code,
                event_type=task.event_type,
                default_anchor_stream_time=task.source_anchor_stream_time,
                default_anchor_source_time=task.source_anchor_source_time,
                detected_at_unix=task.created_at_unix,
                observed_anchor_stream_time=(
                    default_task.observed_stream_time
                    if default_task is not None else task.search_end_stream_time
                ),
                observed_anchor_source_time=(
                    default_task.observed_source_time
                    if default_task is not None else None
                ),
                event_minute=str(event_data.get("minute") or ""),
                event_minute_extra=str(event_data.get("minute_extra") or "0"),
                target_score=str(event_data.get("score") or ""),
                scoreboard_profile=scoreboard_profile,
                require_scoreboard_profile=True,
            )
    recovered_jobs = len(jobs)
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
    vision_pool = BoundedTaskPool(args.vision_workers) if args.vision_enabled else None
    segment_generations: list[SegmentGeneration] = []
    for generation in manifest.generations:
        segment_generations.append(
            SegmentGeneration(
                resolve_generation_path(manifest_path, generation.list_path),
                generation.stream_offset,
            )
        )

    def segment_reader() -> list[Any]:
        return read_all_segments(segment_generations, buffer_dir)

    def register_generation(
        list_path: Path,
        *,
        stream_offset: float,
        started_at_wall: float,
    ) -> None:
        nonlocal manifest
        relative_path = list_path
        try:
            relative_path = list_path.relative_to(buffer_dir)
        except ValueError:
            pass
        manifest = upsert_segment_generation(
            manifest,
            list_path=relative_path,
            stream_offset=stream_offset,
            started_at_wall=started_at_wall,
        )
        save_segment_manifest(manifest_path, manifest)
        segment_generations.append(
            SegmentGeneration(list_path, stream_offset)
        )

    def ingest_command(generation: int) -> list[str]:
        return build_ingest_command(
            ffmpeg,
            args.source,
            args.simulate_live,
            args.replay_speed,
            replay_source_start,
            replay_duration,
            args.segment_seconds,
            buffer_dir,
            buffer_dir / f"segments_{run_id}_g{generation:03d}.csv",
            segment_prefix=f"segment_{run_id}_g{generation:03d}",
        )

    def ingest_log(generation: int) -> Any:
        # Previous FFmpeg logs are closed before a reconnect creates the next
        # one, so bounding them here is safe during a long-running stream.
        lifecycle.prune_ingest_logs()
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
                allow_degraded=True,
                min_degraded_seconds=args.min_degraded_gif_seconds,
                heavy_task_coordinator=heavy_task_coordinator,
                wait_for_heavy_slot=False,
            )

    observed_segment_paths = successful_segment_paths(segment_reader())
    reconnect_due_monotonic: float | None = None
    initial_generation_path = (
        buffer_dir / f"segments_{run_id}_g{supervisor.generation + 1:03d}.csv"
    )
    initial_generation_wall = time.time()
    register_generation(
        initial_generation_path,
        stream_offset=current_stream_time(),
        started_at_wall=initial_generation_wall,
    )
    process = supervisor.start(pipeline_started_mono)
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

            if (
                supervisor.process is not None
                and supervisor.process.poll() is not None
            ):
                observe_segment_progress(
                    supervisor,
                    segment_reader(),
                    observed_segment_paths,
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
                generation_started_wall = time.time()
                register_generation(
                    buffer_dir
                    / f"segments_{run_id}_g{supervisor.generation + 1:03d}.csv",
                    stream_offset=current_stream_time(),
                    started_at_wall=generation_started_wall,
                )
                process = supervisor.start()
                reconnect_due_monotonic = None

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
                stream_time = current_stream_time()

                previous_error_count = getattr(event_source, "error_count", 0)
                new_events = event_source.poll(stream_time, now_monotonic)
                first_observed_wall_time = time.time() if new_events else None
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
                    stream_time = current_stream_time()
                    media_tail_stream_time = latest_media_tail_stream_time(
                        segment_reader()
                    )
                else:
                    media_tail_stream_time = None
                updated_events = list(getattr(event_source, "updated_events", []))
                for updated_event in updated_events:
                    if runtime.update_task_event(asdict(updated_event)):
                        for job in jobs:
                            if job.match_event.event_key == updated_event.event_key:
                                job.match_event = merge_observed_event_revision(
                                    job.match_event,
                                    updated_event,
                                )
                        print(
                            f"[event:update] code={updated_event.code} "
                            f"minute={updated_event.minute} "
                            f"person={updated_event.person or '-'}"
                        )
                for match_event in new_events:
                    clip_anchor = max(
                        0.0, stream_time + args.event_to_video_offset
                    )
                    match_clock_anchor = estimate_match_clock_anchor(match_event)
                    vision_search_start, vision_search_end = vision_search_window(
                        clip_anchor=stream_time,
                        match_clock_anchor=None,
                        buffer_seconds=args.buffer_seconds,
                        segment_slack=args.segment_slack,
                        search_before=args.vision_search_before,
                        search_after=args.vision_search_after,
                        minute_uncertainty=(
                            args.match_minute_uncertainty_seconds
                        ),
                    )
                    assert first_observed_wall_time is not None
                    detected_wall_time = first_observed_wall_time
                    (
                        effective_vision_deadline_at,
                        effective_vision_wait_seconds,
                    ) = vision_deadline_at(
                        detected_at_unix=detected_wall_time,
                        current_stream_time=stream_time,
                        search_end_stream_time=vision_search_end,
                        stream_rate=stream_rate,
                        segment_slack=args.segment_slack,
                        configured_deadline_seconds=(
                            args.vision_deadline_seconds
                        ),
                    )
                    timing_diagnostics = {
                        "api_request_started_at_unix": getattr(
                            event_source, "last_request_started_at_unix", None
                        ),
                        "api_request_finished_at_unix": getattr(
                            event_source, "last_request_finished_at_unix", None
                        ),
                        "api_request_duration_seconds": getattr(
                            event_source, "last_request_duration_seconds", None
                        ),
                        "api_request_succeeded": getattr(
                            event_source, "last_request_succeeded", None
                        ),
                        "first_observed_wall_time_unix": detected_wall_time,
                        "first_observed_stream_time_sec": stream_time,
                        "media_tail_stream_time_sec": media_tail_stream_time,
                        "media_tail_lag_seconds": (
                            stream_time - media_tail_stream_time
                            if media_tail_stream_time is not None else None
                        ),
                        "event_to_video_offset_seconds": (
                            args.event_to_video_offset
                        ),
                        "clip_anchor_stream_time_sec": clip_anchor,
                        "requested_clip_start_stream_time_sec": max(
                            0.0, clip_anchor - args.before
                        ),
                        "requested_clip_end_stream_time_sec": (
                            clip_anchor + args.after
                        ),
                    }
                    match_event = replace(
                        match_event,
                        metadata={
                            **match_event.metadata,
                            "anchor_strategy": "api_first_observed",
                            "match_clock_anchor_stream_time": match_clock_anchor,
                            "vision_search_start_stream_time": vision_search_start,
                            "vision_search_end_stream_time": vision_search_end,
                            "vision_wait_budget_seconds": (
                                effective_vision_wait_seconds
                            ),
                            "timing_diagnostics": timing_diagnostics,
                        },
                    )
                    source_time = (
                        args.start + clip_anchor if source_is_local(args.source) else None
                    )
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
                        deadline_at_unix=(
                            detected_wall_time + args.gif_deadline_seconds
                        ),
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
                            match_clock_anchor_stream_time=match_clock_anchor,
                            vision_search_start_stream_time=vision_search_start,
                            vision_search_end_stream_time=vision_search_end,
                        )
                    )
                    runtime.logger.log(
                        "event_timeline_anchor",
                        match_id=args.match_id,
                        event_key=match_event.event_key,
                        observed_stream_time_sec=round(stream_time, 3),
                        clip_anchor_stream_time_sec=round(clip_anchor, 3),
                        match_clock_anchor_stream_time_sec=(
                            round(match_clock_anchor, 3)
                            if match_clock_anchor is not None else None
                        ),
                        vision_search_start_stream_time_sec=round(
                            vision_search_start, 3
                        ),
                        vision_search_end_stream_time_sec=round(
                            vision_search_end, 3
                        ),
                        vision_wait_budget_seconds=round(
                            effective_vision_wait_seconds, 3
                        ),
                        timing_diagnostics=timing_diagnostics,
                    )
                    if args.vision_enabled:
                        runtime.enqueue_vision_task(
                            match_event.event_key,
                            search_start_stream_time=vision_search_start,
                            search_end_stream_time=vision_search_end,
                            clip_before_seconds=args.vision_before,
                            clip_after_seconds=args.vision_after,
                            model_name="PaddleOCR -> T-DEED",
                            model_version="scoreboard-clock-v1",
                            deadline_at_unix=(
                                effective_vision_deadline_at
                            ),
                        )
                        vision_jobs[match_event.event_key] = VisionJob(
                            event_key=match_event.event_key,
                            match_id=args.match_id,
                            code=match_event.code,
                            event_type=match_event.event_type,
                            default_anchor_stream_time=clip_anchor,
                            default_anchor_source_time=source_time,
                            detected_at_unix=detected_wall_time,
                            observed_anchor_stream_time=stream_time,
                            observed_anchor_source_time=observed_source_time,
                            event_minute=match_event.minute,
                            event_minute_extra=match_event.minute_extra,
                            target_score=match_event.score,
                            scoreboard_profile=scoreboard_profile,
                            require_scoreboard_profile=True,
                        )
                    print(
                        f"[event] code={match_event.code} type={match_event.event_type} "
                        f"minute={match_event.minute} person={match_event.person or '-'} "
                        f"observed_stream={stream_time:.2f}s clip_anchor={clip_anchor:.2f}s "
                        f"match_clock_anchor="
                        f"{match_clock_anchor if match_clock_anchor is not None else '-'}"
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
                        current_versions={
                            key: asdict(event)
                            for key, event in getattr(
                                getattr(event_source, "revisions", None),
                                "current_snapshot",
                                lambda: {},
                            )().items()
                        },
                    )
                    persisted_snapshot_revision = snapshot_revision

                for job in jobs:
                    pending = job.pending
                    stored_task = runtime.store.get(job.match_event.event_key)
                    now_unix = time.time()
                    ingest_running = (
                        supervisor.process is not None
                        and supervisor.process.poll() is None
                    )
                    if (
                        stored_task is None
                        or pending.status != "pending"
                        or (
                            not ingest_running
                            and now_unix < stored_task.deadline_at_unix
                        )
                        or (
                            stream_time < pending.output_due_stream_time
                            and now_unix < stored_task.deadline_at_unix
                        )
                        or now_unix < stored_task.next_attempt_at_unix
                    ):
                        continue
                    task_pool.submit(
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
                        allow_degraded=True,
                        min_degraded_seconds=args.min_degraded_gif_seconds,
                        heavy_task_coordinator=heavy_task_coordinator,
                    )

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

                if vision_pool is not None:
                    heavy_snapshot = heavy_task_coordinator.snapshot()
                    default_gif_work_active = (
                        bool(task_pool.futures)
                        or heavy_snapshot_has_default_gif_work(heavy_snapshot)
                    )
                    for event_key, vision_job in vision_jobs.items():
                        vision_task = runtime.store.get_vision_task(event_key)
                        if vision_task is None or vision_task.status not in {"pending", "located"}:
                            continue
                        default_task = runtime.store.get(event_key)
                        if default_task is None:
                            continue
                        if default_task.status == "failed":
                            runtime.transition_vision_task(
                                event_key,
                                "failed",
                                result={
                                    "stage": "waiting_for_default_gif",
                                    "error_kind": "default_gif_failed",
                                    "locator_method": None,
                                },
                                error=(
                                    "精剪未运行：默认 GIF 生成失败，已停止后续视觉任务"
                                ),
                                error_kind="default_gif_failed",
                            )
                            continue
                        if default_task.status != "encoded":
                            # The optional branch must never race the default GIF.
                            continue
                        if default_gif_work_active:
                            # Across all matches, keep visual work behind default GIFs.
                            continue
                        now_unix = time.time()
                        if now_unix < vision_task.next_attempt_at_unix:
                            continue
                        if (
                            stream_time
                            < vision_task.search_end_stream_time + args.segment_slack
                            and now_unix < vision_task.deadline_at_unix
                        ):
                            continue
                        if vision_pool.submit(
                            event_key,
                            run_with_task_slot,
                            heavy_task_coordinator,
                            "vision",
                            args.match_id,
                            event_key,
                            refine_event_job,
                            vision_job,
                            runtime,
                            segment_reader,
                            ffmpeg,
                            ffprobe,
                            args.output_dir,
                            cancel_event=graceful_stop_cancel_encodes,
                            function_kwargs={
                                "search_before": args.vision_search_before,
                                "search_after": args.vision_search_after,
                                "refined_before": args.vision_before,
                                "refined_after": args.vision_after,
                                "width": args.gif_width,
                                "fps": args.gif_fps,
                                "colors": args.gif_colors,
                                "size_reference_bytes": int(
                                    args.gif_size_reference_mb * 1_000_000
                                ),
                                "python": find_python(Path(__file__).resolve().parent),
                                "timeout_seconds": args.vision_timeout_seconds,
                                "ocr_python": args.ocr_python,
                                "ocr_timeout_seconds": args.ocr_timeout_seconds,
                                "fallback_width": args.fallback_gif_width,
                                "fallback_fps": args.fallback_gif_fps,
                                "fallback_colors": args.fallback_gif_colors,
                                "min_degraded_seconds": args.min_degraded_gif_seconds,
                                "cancel_event": graceful_stop_cancel_encodes,
                            },
                        ):
                            print(f"[vision] queued code={vision_job.code} key={event_key}")
                    for event_key, completed, error in vision_pool.collect_done():
                        if error is not None:
                            print(f"[vision:worker:error] key={event_key} {error}")

                if now_monotonic - last_heartbeat_monotonic >= 3.0:
                    heartbeat_segments = segment_reader()
                    heavy_task_status = heavy_task_coordinator.snapshot()
                    checkpoint_stream_time(stream_time)
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
                        heavy_task_active=heavy_task_status["active"]["heavy"],
                        vision_task_active=heavy_task_status["active"]["vision"],
                        heavy_task_waiting=heavy_task_status["waiting"]["tasks"],
                        **{f"{status}_count": count for status, count in status_counts.items()},
                    )
                    last_heartbeat_monotonic = now_monotonic

                if graceful_stop_started_monotonic is not None:
                    grace_seconds = (
                        args.graceful_stop_grace_seconds
                        if args.graceful_stop_grace_seconds is not None
                        else max(
                            args.after,
                            args.vision_search_after if args.vision_enabled else 0.0,
                        ) + args.segment_slack
                    )
                    stop_elapsed = now_monotonic - graceful_stop_started_monotonic
                    pending_due = [
                        job.pending.output_due_stream_time
                        for job in jobs
                        if job.pending.status == "pending"
                    ]
                    encodes_active = bool(task_pool.futures) or bool(
                        vision_pool and vision_pool.futures
                    )
                    vision_pending = bool(
                        args.vision_enabled
                        and runtime.store.list_incomplete_vision_tasks(args.match_id)
                    )
                    drain_ready = (
                        stop_elapsed >= grace_seconds
                        and not encodes_active
                        and not pending_due
                        and not vision_pending
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
                    current_segments = segment_reader()
                    observe_segment_progress(
                        supervisor,
                        current_segments,
                        observed_segment_paths,
                    )
                    prune_buffer(
                        current_segments,
                        stream_time,
                        args.buffer_seconds,
                        [job.pending for job in jobs],
                        args.before,
                        protected_paths=(
                            runtime.store.protected_segment_paths()
                            if args.vision_enabled else None
                        ),
                        extra_cutoffs=(
                            [
                                max(
                                    0.0,
                                    task.search_start_stream_time
                                    - OCR_MINUTE_FALLBACK_BEFORE_SECONDS,
                                )
                                for task in runtime.store.list_incomplete_vision_tasks(args.match_id)
                            ]
                            if args.vision_enabled else None
                        ),
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
        if vision_pool is not None:
            vision_pool.shutdown(wait=True)
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
    final_stream_time = current_stream_time()
    checkpoint_stream_time(final_stream_time)
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
            allow_degraded=True,
            min_degraded_seconds=args.min_degraded_gif_seconds,
            heavy_task_coordinator=heavy_task_coordinator,
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

    stored_default_tasks = {
        task.event_key: task for task in runtime.store.list_for_match(args.match_id)
    }

    def schedule_fields(event_key: str) -> dict[str, Any]:
        task = stored_default_tasks.get(event_key)
        if task is None:
            return {}
        return {
            "attempt_count": task.attempt_count,
            "readiness_check_count": task.readiness_check_count,
            "next_attempt_at_unix": task.next_attempt_at_unix,
            "deadline_at_unix": task.deadline_at_unix,
            "last_error_kind": task.last_error_kind,
        }

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
            "vision_enabled": args.vision_enabled,
            "vision_workers": args.vision_workers if args.vision_enabled else 0,
            "ocr_python": str(args.ocr_python) if args.vision_enabled else None,
            "ocr_timeout_seconds": (
                args.ocr_timeout_seconds if args.vision_enabled else None
            ),
            "scoreboard_profile_path": (
                str(args.scoreboard_profile.resolve())
                if args.vision_enabled and args.scoreboard_profile is not None
                else None
            ),
            "scoreboard_profile_id": (
                scoreboard_profile.get("profile_id")
                if args.vision_enabled and scoreboard_profile is not None
                else None
            ),
            "heavy_task_coordinator": heavy_task_coordinator.snapshot(),
        },
        "supported_event_codes": SUPPORTED_EVENT_CODES,
        "timeline": {
            "source_start_seconds": args.start if source_is_local(args.source) else None,
            "event_to_video_offset_seconds": args.event_to_video_offset,
            "match_start_play": args.match_start_play,
            "match_start_naive_timezone": args.match_start_naive_timezone,
            "match_start_normalized_unix": match_start_at_unix,
            "match_start_at_unix": timeline.match_start_at_unix,
            "timeline_origin_wall_unix": timeline.timeline_origin_wall_unix,
            "timeline_origin_stream_time": timeline.timeline_origin_stream_time,
            "broadcast_delay_seconds": timeline.broadcast_delay_seconds,
            "halftime_break_seconds": timeline.halftime_break_seconds,
            "match_minute_uncertainty_seconds": (
                args.match_minute_uncertainty_seconds
            ),
            "last_stream_time": timeline.last_stream_time,
            "segment_manifest": str(manifest_path.resolve()),
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
            "deadline_seconds": args.gif_deadline_seconds,
            "allow_degraded": True,
            "min_degraded_seconds": args.min_degraded_gif_seconds,
            "size_reference_bytes": int(args.gif_size_reference_mb * 1_000_000),
            "adaptive_quality_reduction": False,
        },
        "fallback_gif": {
            "before_seconds": OCR_MINUTE_FALLBACK_BEFORE_SECONDS,
            "after_seconds": OCR_MINUTE_FALLBACK_AFTER_SECONDS,
            "width": args.fallback_gif_width,
            "fps": args.fallback_gif_fps,
            "colors": args.fallback_gif_colors,
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
                "timing_diagnostics": event_timing_diagnostics(
                    job,
                    before=args.before,
                    after=args.after,
                ),
                "match_clock_anchor_stream_time_sec": (
                    round(job.match_clock_anchor_stream_time, 3)
                    if job.match_clock_anchor_stream_time is not None else None
                ),
                "vision_search_start_stream_time_sec": (
                    round(job.vision_search_start_stream_time, 3)
                    if job.vision_search_start_stream_time is not None else None
                ),
                "vision_search_end_stream_time_sec": (
                    round(job.vision_search_end_stream_time, 3)
                    if job.vision_search_end_stream_time is not None else None
                ),
                "status": job.pending.status,
                **schedule_fields(job.match_event.event_key),
                **job.pending.result,
            }
            for job in jobs
        ],
        "vision": [
            {
                "event_key": task.event_key,
                "code": task.code,
                "status": task.status,
                "artifact_kind": task.artifact_kind,
                "default_anchor_stream_time_sec": task.source_anchor_stream_time,
                "vision_anchor_stream_time_sec": task.located_anchor_stream_time,
                "confidence": task.confidence,
                "inference_seconds": task.inference_seconds,
                "locate_attempt_count": task.locate_attempt_count,
                "encode_attempt_count": task.encode_attempt_count,
                "readiness_check_count": task.readiness_check_count,
                "next_attempt_at_unix": task.next_attempt_at_unix,
                "deadline_at_unix": task.deadline_at_unix,
                "last_error_kind": task.last_error_kind,
                "model_name": task.model_name,
                "model_version": task.model_version,
                "model_weights_sha256": task.model_weights_sha256,
                "output": task.output_path,
                "bytes": task.output_bytes,
                "error": task.error,
                **task.result,
            }
            for task in runtime.store.list_vision_tasks(args.match_id)
        ] if args.vision_enabled else [],
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
    protected_segment_paths: set[str] = set()
    lease_query_ok = True
    try:
        runtime.store.purge_expired_segment_leases()
        protected_segment_paths = runtime.store.protected_segment_paths()
    except Exception:
        # Do not delete terminal media when the final lease check itself is
        # unavailable. The next confirmed shutdown can retry the cleanup.
        lease_query_ok = False
    runtime.close()
    heavy_task_coordinator.close()
    terminal_stop = lease_query_ok and stop_reason in {
        "match_played",
        "match_played_stream_incomplete",
        "ingest_completed",
    }
    if terminal_stop:
        lifecycle_summary = lifecycle.cleanup_finished_match(
            buffer_dir=buffer_dir,
            manifest_path=manifest_path,
            event_log_path=event_log_path,
            state_db_path=state_db_path,
            protected_paths=protected_segment_paths,
        )
    else:
        lifecycle_summary = CleanupSummary(
            phase="finished_match",
            status="deferred",
            actions=[
                (
                    "terminal_cleanup_deferred_until_lease_check_succeeds"
                    if not lease_query_ok
                    else "terminal_cleanup_deferred_until_confirmed_match_end"
                )
            ],
        )
    report["disk_lifecycle"] = {
        "policy": lifecycle.policy_dict(),
        **lifecycle_summary.to_dict(),
    }
    serialized_report = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    run_report_path.write_text(serialized_report, encoding="utf-8")
    report_path.write_text(serialized_report, encoding="utf-8")
    print(
        f"[disk] phase={lifecycle_summary.phase} status={lifecycle_summary.status} "
        f"deleted={lifecycle_summary.deleted_files} "
        f"bytes={lifecycle_summary.deleted_bytes}"
    )
    if (
        return_code != 0
        and not stopped_by_user
        and stop_reason not in ("match_played", "match_played_stream_incomplete")
    ):
        raise SystemExit(f"ingest ffmpeg exited with status {return_code}")


if __name__ == "__main__":
    main()
