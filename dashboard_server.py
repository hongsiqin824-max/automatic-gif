#!/usr/bin/env python3
"""Small Flask control plane for the football GIF pipeline.

The dashboard deliberately keeps network credentials on the server side. It
provides a read-only view of the three match APIs and starts the existing
event-driven worker only after a live source has been found.
"""

from __future__ import annotations
import sys
import errno
import html
import json
import math
import os
import re
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from flask import Flask, abort, jsonify, redirect, request, send_from_directory

from article_publisher import (
    ArticlePublishError,
    ArticlePublisher,
    PublishedGifStore,
    environment_boolean,
)
from disk_lifecycle import DiskLifecycleManager, DiskLifecyclePolicy
from event_api_response import EventApiResponseError, normalize_event_api_response
from heavy_task_coordinator import HeavyTaskCoordinator, HeavyTaskCoordinatorError
from match_event_identity import events_represent_same_incident
from open_platform_client import (
    OpenPlatformClient,
    OpenPlatformConfig,
    OpenPlatformError,
)


ROOT = Path(__file__).resolve().parent


def _load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE settings without overriding shell variables."""
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not key or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


_load_dotenv(ROOT / ".env")
HOST = os.environ.get("GIF_DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.environ.get("GIF_DASHBOARD_PORT", "8899"))


def _positive_environment_integer(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} 必须是正整数") from exc
    if value < 1:
        raise RuntimeError(f"{name} 必须是正整数")
    return value


def _positive_environment_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} 必须是正数") from exc
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(f"{name} 必须是正数")
    return value


MAX_CONCURRENT_MATCHES = _positive_environment_integer(
    "GIF_MAX_CONCURRENT_MATCHES", 8
)
SESSION_RETENTION_SECONDS = _positive_environment_float(
    "GIF_SESSION_RETENTION_SECONDS", 24 * 60 * 60
)
DISK_CLEANUP_INTERVAL_SECONDS = _positive_environment_float(
    "GIF_DISK_CLEANUP_INTERVAL_SECONDS", 5 * 60
)
FINAL_GIF_RETENTION_SECONDS = _positive_environment_float(
    "GIF_FINAL_GIF_RETENTION_SECONDS", 24 * 60 * 60
)
ORPHAN_CLEANUP_GRACE_SECONDS = _positive_environment_float(
    "GIF_ORPHAN_CLEANUP_GRACE_SECONDS", 15 * 60
)
DEFAULT_OUTPUT = ROOT / "output_gifs" / "dashboard"
DEFAULT_PUBLISHED_GIF_DIR = ROOT / "data" / "published_gifs"
DEFAULT_ARTICLE_PUBLISH_DATABASE = ROOT / "data" / "article_publish.sqlite3"
DEFAULT_EVENT_URL = (
    "https://openapi.dongqiudi.com/internal/api/data/overview/match/{match_id}"
)
DEFAULT_DETAIL_URL = (
    "https://openapi.dongqiudi.com/internal/api/data/detail/match/{match_id}"
)
DEFAULT_SOURCE_URL = (
    "https://openapi.dongqiudi.com/internal/sport-data/inner/tool/"
    "match/live_source/query"
)
MATCH_CATALOG_URL = "https://api.dongqiudi.com/data/tab/new/lotzc"
MATCH_CATALOG_REFRESH_SECONDS = 120.0
MATCH_CATALOG_BACKOFF_SECONDS = (5.0, 10.0, 20.0, 30.0)
MATCH_CATALOG_LIMIT = 20
MATCH_CATALOG_UPCOMING_SECONDS = 15 * 60
MATCH_CATALOG_CARD_FIELDS = (
    "match_id", "team_A_name", "team_A_logo", "team_B_name", "team_B_logo",
    "competition_name", "round_name", "status", "start_play", "sort_timestamp",
    "fs_A", "fs_B", "minute", "minute_extra", "minute_period", "cmp_type",
)
BEIJING_TIMEZONE = timezone(timedelta(hours=8))
MATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
MATCH_STATUS_LABELS = {
    "Cancelled": "已取消",
    "Fixture": "未开始",
    "Played": "已结束",
    "Playing": "进行中",
    "Postponed": "已推迟",
    "Suspended": "暂停",
    "Uncertain": "未知",
}
WORKER_TERM_GRACE_SECONDS = float(
    os.environ.get("GIF_WORKER_TERM_GRACE_SECONDS", "3")
)
WORKER_KILL_GRACE_SECONDS = float(
    os.environ.get("GIF_WORKER_KILL_GRACE_SECONDS", "2")
)
WORKER_CLEANUP_POLL_SECONDS = 0.1
PLAYED_CONFIRMATIONS_REQUIRED = 2
WORKER_FINISH_GRACE_SECONDS = 90.0
WORKER_FINISH_TIMEOUT_SECONDS = _positive_environment_float(
    "GIF_WORKER_FINISH_TIMEOUT_SECONDS", 600.0
)
DASHBOARD_BUFFER_SECONDS = 900.0
VISION_WORKERS = _positive_environment_integer("GIF_VISION_WORKERS", 2)
OCR_TIMEOUT_SECONDS = _positive_environment_float(
    "GIF_OCR_TIMEOUT_SECONDS", 300.0
)
VISION_SEARCH_BEFORE_SECONDS = _positive_environment_float(
    "GIF_VISION_SEARCH_BEFORE_SECONDS", 120.0
)
VISION_SEARCH_AFTER_SECONDS = 0.0
FALLBACK_GIF_WIDTH = 384
FALLBACK_GIF_FPS = 6.0
FALLBACK_GIF_COLORS = 160
DEFAULT_DEMO_SCOREBOARD_PROFILE = (
    ROOT / "scoreboard_profiles" / "demo_wiesbaden_1280x720.json"
)
# Leave the worker time to flush its task pool and write the final report before
# the dashboard escalates to process-group cleanup.
FINISHING_TIMEOUT_SECONDS = WORKER_FINISH_TIMEOUT_SECONDS + 15.0
TERMINAL_LIFECYCLE_STATES = {
    "completed",
    "completed_with_warnings",
    "stopping",
    "stopped",
    "failed",
}


def validate_match_id(value: str) -> str:
    match_id = value.strip()
    if not MATCH_ID_PATTERN.fullmatch(match_id):
        raise ValueError("match_id 只能包含字母、数字、下划线和连字符，长度不超过 80")
    return match_id


def _usable_match_start_play(detail: dict[str, Any]) -> str | None:
    """Return a real start_play value, excluding demo/placeholder text."""
    value = str(detail.get("start_play") or "").strip()
    if not value or value in {"演示数据", "未知", "--"}:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", value):
        return value
    if re.match(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}", value):
        return value
    return None


def _demo_detail(match_id: str) -> dict[str, Any]:
    return {
        "match_id": match_id,
        "competition_name": "离线链路验证",
        "match_title": "MP4 模拟 RTMP",
        "status": "Playing",
        "minute": "18",
        "minute_period": "2H",
        "start_play": "演示数据",
        "team_A_name": "威斯巴登",
        "team_B_name": "拜仁慕尼黑",
        "fs_A": "0",
        "fs_B": "1",
    }


@dataclass
class MatchSession:
    match_id: str
    output_dir: Path = DEFAULT_OUTPUT
    event_poll_seconds: float = 3.0
    shotmap_poll_seconds: float = 5.0
    source_poll_seconds: float = 10.0
    detail_poll_seconds: float = 10.0
    before_seconds: float = 30.0
    after_seconds: float = 20.0
    event_to_video_offset_seconds: float = -10.0
    shotmap_offset_seconds: float = 0.0
    gif_width: int = 768
    gif_fps: float = 16.0
    gif_colors: int = 256
    vision_enabled: bool = True
    tdeed_enabled: bool = False
    vision_clock_only: bool = True
    vision_before_seconds: float = 8.0
    vision_after_seconds: float = 12.0
    scoreboard_profile_path: str = field(
        default_factory=lambda: os.environ.get("GIF_SCOREBOARD_PROFILE", "").strip()
    )
    source: dict[str, Any] = field(default_factory=dict)
    detail: dict[str, Any] = field(default_factory=dict)
    event_payload: dict[str, Any] = field(default_factory=dict)
    event_error: str | None = None
    source_error: str | None = None
    detail_error: str | None = None
    worker: subprocess.Popen[str] | None = None
    worker_process_group: int | None = None
    worker_command: list[str] = field(default_factory=list)
    worker_started_at: float | None = None
    worker_exit_logged_pid: int | None = None
    last_source_poll: float | None = None
    last_detail_poll: float | None = None
    last_event_poll: float | None = None
    source_changed: bool = False
    source_change_message: str | None = None
    worker_mode: str | None = None
    desired_running: bool = False
    worker_restart_due_at: float | None = None
    worker_restart_count: int = 0
    worker_consecutive_failures: int = 0
    worker_cleanup_process_group: int | None = None
    worker_cleanup_stage: str | None = None
    worker_cleanup_due_at: float | None = None
    worker_cleanup_failure: str | None = None
    # Match lifecycle is deliberately separate from the API's display status.
    # A match can be reported as Played while its worker is still finishing GIFs.
    lifecycle_state: str = "idle"
    played_confirmation_count: int = 0
    played_confirmed_at: float | None = None
    finishing_started_at: float | None = None
    finishing_deadline: float | None = None
    finish_requested: bool = False
    finish_timeout_signaled: bool = False
    finish_reason: str | None = None
    exit_reason: str | None = None
    created_at: float = field(default_factory=time.time)
    last_access_at: float = field(default_factory=time.time)
    terminal_at: float | None = None
    terminal_cleanup_done: bool = False
    terminal_cleanup_last_attempt_at: float | None = None
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def status(self) -> str:
        status = str(self.detail.get("status") or "Uncertain")
        return status

    def worker_running(self) -> bool:
        return self.worker is not None and self.worker.poll() is None

    def output_report(self) -> dict[str, Any]:
        report_path = self.output_dir / "event_pipeline_report.json"
        if not report_path.exists():
            return {}
        try:
            return json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}


def _json_request(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float = 8.0,
) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        method=method,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "football-gif-dashboard/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("API response must be a JSON object")
    return value


def _user() -> str:
    return os.environ.get("GIF_MATCH_USER", "hongsiqin@dongqiudi.com")


def query_detail(match_id: str) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {"platform": "iphone", "version": "855", "user": _user()}
    )
    return _json_request(DEFAULT_DETAIL_URL.format(match_id=urllib.parse.quote(match_id)) + "?" + query)


def query_events(match_id: str) -> dict[str, Any]:
    url = DEFAULT_EVENT_URL.format(match_id=urllib.parse.quote(match_id))
    payload = _json_request(url + "?" + urllib.parse.urlencode({"user": _user()}))
    try:
        return normalize_event_api_response(payload)
    except EventApiResponseError as exc:
        if exc.reason == "status":
            raise ValueError(f"事件接口返回异常状态 status={exc.status!r}") from exc
        if exc.reason == "events":
            raise ValueError("事件接口返回缺少有效的 events 对象") from exc
        raise


def query_source(match_id: str) -> dict[str, Any]:
    source_user = os.environ.get("GIF_SOURCE_USER", "xuxinan@dongqiudi.com")
    secret = os.environ.get("GIF_SOURCE_SECRET", "")
    if not secret:
        raise RuntimeError("未设置 GIF_SOURCE_SECRET，暂不调用直播源鉴权接口")
    query = urllib.parse.urlencode({"user": source_user, "match_id": match_id})
    return _json_request(
        DEFAULT_SOURCE_URL + "?" + query,
        method="POST",
        body={"secret": secret},
    )


def _catalog_match_timestamp(item: dict[str, Any]) -> float:
    raw_timestamp = item.get("sort_timestamp")
    try:
        timestamp = float(raw_timestamp)
    except (TypeError, ValueError):
        timestamp = 0.0
    if timestamp > 0:
        return timestamp

    start_play = str(item.get("start_play") or "").strip()
    try:
        parsed = datetime.strptime(start_play, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ValueError(
            f"比赛 {item.get('match_id')!r} 缺少有效的 sort_timestamp/start_play"
        ) from exc
    return parsed.replace(tzinfo=BEIJING_TIMEZONE).timestamp()


def _catalog_required_text(item: dict[str, Any], field_name: str, index: int) -> str:
    value = item.get(field_name)
    if isinstance(value, (str, int)):
        text = str(value).strip()
        if text:
            return text
    raise ValueError(f"赛事目录第 {index + 1} 项缺少有效字段 {field_name}")


class MatchCatalog:
    """Small, independent cache for the global soccer match directory."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.playing: list[dict[str, Any]] = []
        self.upcoming: list[dict[str, Any]] = []
        self.last_success_at: float | None = None
        self.last_attempt_at: float | None = None
        self.next_attempt_at = 0.0
        self.last_latency_ms: float | None = None
        self.last_error: str | None = None
        self.last_query_start: str | None = None
        self.consecutive_failures = 0
        self.source_count = 0
        self.soccer_count = 0

    @staticmethod
    def _iso_beijing(timestamp: float | None) -> str | None:
        if timestamp is None:
            return None
        return datetime.fromtimestamp(timestamp, BEIJING_TIMEZONE).isoformat()

    def _fetch(
        self,
        now_unix: float,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, int, str]:
        query_start = datetime.fromtimestamp(now_unix, BEIJING_TIMEZONE).strftime(
            "%Y-%m-%d%H:%M:%S"
        )
        self.last_query_start = query_start
        url = MATCH_CATALOG_URL + "?" + urllib.parse.urlencode(
            {"start": query_start, "init": "1", "user": _user()}
        )
        payload = _json_request(url)
        rows = payload.get("list")
        if not isinstance(rows, list):
            raise ValueError("赛事目录响应缺少有效的 list 数组")

        candidates: dict[str, tuple[dict[str, Any], float]] = {}
        soccer_count = 0
        for index, raw_item in enumerate(rows):
            if not isinstance(raw_item, dict):
                raise ValueError(f"赛事目录第 {index + 1} 项不是 JSON 对象")
            if str(raw_item.get("cmp_type") or "").strip() != "soccer":
                continue
            soccer_count += 1
            match_id = validate_match_id(
                _catalog_required_text(raw_item, "match_id", index)
            )
            _catalog_required_text(raw_item, "team_A_name", index)
            _catalog_required_text(raw_item, "team_B_name", index)
            status = _catalog_required_text(raw_item, "status", index)
            if status not in {"Playing", "Fixture"}:
                continue
            item = {
                field_name: raw_item.get(field_name)
                for field_name in MATCH_CATALOG_CARD_FIELDS
            }
            item["match_id"] = match_id
            match_timestamp = _catalog_match_timestamp(item)
            existing = candidates.get(match_id)
            if existing is None:
                candidates[match_id] = (item, match_timestamp)
                continue
            existing_status = str(existing[0].get("status") or "")
            if (
                status == "Playing" and existing_status != "Playing"
            ) or (
                status == existing_status and match_timestamp < existing[1]
            ):
                candidates[match_id] = (item, match_timestamp)

        playing_rows: list[tuple[dict[str, Any], float]] = []
        upcoming_rows: list[tuple[dict[str, Any], float]] = []
        upcoming_deadline = now_unix + MATCH_CATALOG_UPCOMING_SECONDS
        for item, match_timestamp in candidates.values():
            if item.get("status") == "Playing":
                playing_rows.append((item, match_timestamp))
            elif now_unix <= match_timestamp <= upcoming_deadline:
                upcoming_rows.append((item, match_timestamp))

        playing_rows.sort(key=lambda entry: (entry[1], entry[0]["match_id"]))
        upcoming_rows.sort(key=lambda entry: (entry[1], entry[0]["match_id"]))
        return (
            [item for item, _ in playing_rows[:MATCH_CATALOG_LIMIT]],
            [item for item, _ in upcoming_rows[:MATCH_CATALOG_LIMIT]],
            len(rows),
            soccer_count,
            query_start,
        )

    def _refresh_if_due(self, now_unix: float) -> None:
        if now_unix < self.next_attempt_at:
            return

        self.last_attempt_at = now_unix
        started_monotonic = time.monotonic()
        try:
            playing, upcoming, source_count, soccer_count, query_start = self._fetch(
                now_unix
            )
        except Exception as exc:
            completed_at = time.time()
            self.last_latency_ms = round(
                (time.monotonic() - started_monotonic) * 1000, 1
            )
            self.consecutive_failures += 1
            backoff = MATCH_CATALOG_BACKOFF_SECONDS[
                min(
                    self.consecutive_failures - 1,
                    len(MATCH_CATALOG_BACKOFF_SECONDS) - 1,
                )
            ]
            self.next_attempt_at = completed_at + backoff
            self.last_error = str(exc)
            return

        completed_at = time.time()
        self.playing = playing
        self.upcoming = upcoming
        self.source_count = source_count
        self.soccer_count = soccer_count
        self.last_query_start = query_start
        self.last_success_at = completed_at
        self.last_latency_ms = round(
            (time.monotonic() - started_monotonic) * 1000, 1
        )
        self.consecutive_failures = 0
        self.last_error = None
        self.next_attempt_at = max(
            completed_at,
            now_unix + MATCH_CATALOG_REFRESH_SECONDS,
        )

    def snapshot(self) -> dict[str, Any]:
        now_unix = time.time()
        with self.lock:
            self._refresh_if_due(now_unix)
            cache_age = (
                max(0.0, now_unix - self.last_success_at)
                if self.last_success_at is not None
                else None
            )
            has_cache = self.last_success_at is not None
            if self.last_error and has_cache:
                state, status = "stale", "degraded"
            elif self.last_error:
                state, status = "error", "unavailable"
            else:
                state, status = "healthy", "ok"
            total_count = len(self.playing) + len(self.upcoming)
            from_cache = self.last_error is not None and has_cache
            health_label = {
                "healthy": "接口正常",
                "stale": "接口异常 · 缓存可用",
                "error": "接口异常",
            }[state]
            return {
                "playing": [dict(item) for item in self.playing],
                "upcoming": [dict(item) for item in self.upcoming],
                "health": {
                    "state": state,
                    "status": status,
                    "label": health_label,
                    "from_cache": from_cache,
                    "last_success_at_unix": self.last_success_at,
                    "last_success_at": self._iso_beijing(self.last_success_at),
                    "last_attempt_at_unix": self.last_attempt_at,
                    "last_attempt_at": self._iso_beijing(self.last_attempt_at),
                    "latency_ms": self.last_latency_ms,
                    "cache_age_seconds": (
                        round(cache_age, 1) if cache_age is not None else None
                    ),
                    "consecutive_failures": self.consecutive_failures,
                    "source_count": self.source_count,
                    "soccer_count": self.soccer_count,
                    "total_count": total_count,
                    "error": self.last_error,
                    "next_retry_at_unix": (
                        self.next_attempt_at if self.next_attempt_at > 0 else None
                    ),
                    "next_retry_at": (
                        self._iso_beijing(self.next_attempt_at)
                        if self.next_attempt_at > 0
                        else None
                    ),
                    "refresh_interval_seconds": MATCH_CATALOG_REFRESH_SECONDS,
                    "query_start": self.last_query_start,
                },
            }


def flatten_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    buckets = payload.get("events")
    if not isinstance(buckets, dict):
        return result
    for bucket_key, bucket in buckets.items():
        if not isinstance(bucket, dict):
            continue
        minute = str(bucket.get("minute") or bucket_key)
        for team_key, values in bucket.items():
            if not team_key.endswith("Events") or not isinstance(values, list):
                continue
            team = team_key.removesuffix("Events")
            for event in values:
                if not isinstance(event, dict) or event.get("code") not in {"G", "OG", "PG", "YC", "RC"}:
                    continue
                code = str(event.get("code"))
                result.append(
                    {
                        "code": code,
                        "label": {"G": "进球", "OG": "乌龙球", "PG": "点球进球", "YC": "黄牌", "RC": "红牌"}[code],
                        "minute": minute,
                        "minute_extra": str(event.get("minute_extra") or "0"),
                        "team": team,
                        "person": str(event.get("person") or ""),
                        "person_id": str(event.get("person_id") or ""),
                        "score": str(event.get("score") or ""),
                        "reason": str(event.get("reason") or ""),
                        "event_id": str(
                            event.get("event_id")
                            or event.get("eventId")
                            or event.get("id")
                            or ""
                        ),
                    }
                )
    return result


def _read_log(path: Path, limit: int = 80) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]
    output: list[dict[str, Any]] = []
    for line in reversed(lines):
        try:
            output.append(json.loads(line))
        except json.JSONDecodeError:
            output.append({"event": "raw", "message": line})
    return output


def _tasks_from_log(path: Path, limit: int = 1000) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: dict[str, dict[str, Any]] = {}
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]
    for line in lines:
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        event_key = item.get("event_key")
        if not event_key:
            continue
        if item.get("event") == "event_discovered":
            records[event_key] = {
                "event_key": event_key,
                "code": item.get("code"),
                "event_type": item.get("event_type"),
                "minute": item.get("minute"),
                "minute_extra": item.get("minute_extra"),
                "person": item.get("person"),
                "person_id": item.get("person_id"),
                "team": item.get("team"),
                "score": item.get("score"),
                "reason": item.get("reason"),
                "event_id": item.get("event_id") or item.get("eventId") or item.get("id"),
                "status": "pending",
            }
        elif item.get("event") == "task_transition" and event_key in records:
            records[event_key]["status"] = item.get("to_status")
            if item.get("error"):
                records[event_key]["error"] = item["error"]
        elif item.get("event") == "gif_ready" and event_key in records:
            records[event_key].update(
                status="encoded",
                output=item.get("output"),
                bytes=item.get("bytes"),
                duration_sec=item.get("duration_sec"),
                seconds_after_event_observed=item.get("seconds_after_event_observed"),
            )
    return list(reversed(records.values()))


COVERAGE_CONTRACT_FIELDS = (
    "coverage_quality",
    "stitched_across_gap",
    "video_gap_count",
    "skipped_gap_seconds",
    "approximate",
    "anchor_adjusted",
    "anchor_adjusted_to_stream_time",
    "anchor_shift_seconds",
    "event_frame_may_be_missing",
    "requested_anchor_offset_seconds",
    "estimated_encoded_anchor_offset_seconds",
    "timeline_compression_before_anchor_seconds",
    "anchor_offset_mapping_basis",
)


def _coverage_contract(result: dict[str, Any]) -> dict[str, Any]:
    """Expose coverage evidence from current and legacy encoded results."""
    actual = result.get("actual_media_window")
    if not isinstance(actual, dict):
        actual = {}
    return {
        field: (
            result.get(field)
            if result.get(field) is not None
            else actual.get(field)
        )
        for field in COVERAGE_CONTRACT_FIELDS
    }


def _tasks_from_database(
    path: Path,
) -> tuple[list[dict[str, Any]], dict[str, str], set[str]]:
    """Read complete task records without mutating the worker's SQLite state."""
    if not path.exists():
        return [], {}, set()
    try:
        connection = sqlite3.connect(
            f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=2.0
        )
        connection.row_factory = sqlite3.Row
        try:
            task_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(event_tasks)")
            }
            suppression_field = (
                "suppressed_by_event_key"
                if "suppressed_by_event_key" in task_columns
                else "NULL AS suppressed_by_event_key"
            )
            task_attempt_count_field = (
                "attempt_count" if "attempt_count" in task_columns
                else "NULL AS attempt_count"
            )
            task_readiness_count_field = (
                "readiness_check_count" if "readiness_check_count" in task_columns
                else "NULL AS readiness_check_count"
            )
            task_next_attempt_field = (
                "next_attempt_at_unix" if "next_attempt_at_unix" in task_columns
                else "NULL AS next_attempt_at_unix"
            )
            task_deadline_field = (
                "deadline_at_unix" if "deadline_at_unix" in task_columns
                else "NULL AS deadline_at_unix"
            )
            task_error_kind_field = (
                "last_error_kind" if "last_error_kind" in task_columns
                else "NULL AS last_error_kind"
            )
            rows = connection.execute(
                """
                SELECT event_key, code, event_type, event_json, status,
                       discovered_at_unix, updated_at_unix, output_path,
                       output_bytes, result_json, error, """
                + suppression_field
                + ", "
                + task_attempt_count_field
                + ", "
                + task_readiness_count_field
                + ", "
                + task_next_attempt_field
                + ", "
                + task_deadline_field
                + ", "
                + task_error_kind_field
                + """
                FROM event_tasks ORDER BY discovered_at_unix, event_key
                """
            ).fetchall()
            has_aliases = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'event_feed_aliases'
                """
            ).fetchone()
            alias_rows = (
                connection.execute(
                    """
                    SELECT version_key, canonical_key FROM event_feed_aliases
                    ORDER BY updated_at_unix, version_key
                    """
                ).fetchall()
                if has_aliases
                else []
            )
            has_vision = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'vision_tasks'"
            ).fetchone()
            vision_columns = (
                {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(vision_tasks)")
                }
                if has_vision
                else set()
            )
            vision_error_kind_field = (
                "last_error_kind"
                if "last_error_kind" in vision_columns
                else "NULL AS last_error_kind"
            )
            vision_artifact_kind_field = (
                "artifact_kind"
                if "artifact_kind" in vision_columns
                else "'refined' AS artifact_kind"
            )
            vision_failure_stage_field = (
                "failure_stage"
                if "failure_stage" in vision_columns
                else "NULL AS failure_stage"
            )
            vision_failure_reason_field = (
                "failure_reason AS persisted_failure_reason"
                if "failure_reason" in vision_columns
                else "NULL AS persisted_failure_reason"
            )
            vision_location_field = (
                "location_json"
                if "location_json" in vision_columns
                else "'{}' AS location_json"
            )
            vision_window_field = (
                "window_json"
                if "window_json" in vision_columns
                else "'{}' AS window_json"
            )
            vision_next_attempt_field = (
                "next_attempt_at_unix"
                if "next_attempt_at_unix" in vision_columns
                else "NULL AS next_attempt_at_unix"
            )
            vision_deadline_field = (
                "deadline_at_unix"
                if "deadline_at_unix" in vision_columns
                else "NULL AS deadline_at_unix"
            )
            vision_rows = (
                connection.execute(
                    """
                    SELECT event_key, status, located_anchor_stream_time,
                           confidence, inference_seconds, model_name,
                           model_version, output_path, output_bytes,
                           result_json, error, """
                    + vision_error_kind_field
                    + ", "
                    + vision_artifact_kind_field
                    + ", "
                    + vision_failure_stage_field
                    + ", "
                    + vision_failure_reason_field
                    + ", "
                    + vision_location_field
                    + ", "
                    + vision_window_field
                    + ", "
                    + vision_next_attempt_field
                    + ", "
                    + vision_deadline_field
                    + """
                    FROM vision_tasks
                    ORDER BY created_at_unix, event_key, artifact_kind
                    """
                ).fetchall()
                if has_vision else []
            )
        finally:
            connection.close()
    except (OSError, sqlite3.Error, json.JSONDecodeError):
        return [], {}, set()

    tasks: list[dict[str, Any]] = []
    suppressed_keys: set[str] = set()
    for row in rows:
        if row["suppressed_by_event_key"]:
            suppressed_keys.add(str(row["event_key"]))
            continue
        try:
            event_data = json.loads(row["event_json"])
            result = json.loads(row["result_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(event_data, dict) or not isinstance(result, dict):
            continue
        tasks.append(
            {
                **event_data,
                "event_key": str(row["event_key"]),
                "code": str(row["code"]),
                "event_type": str(row["event_type"]),
                "status": str(row["status"]),
                "discovered_at_unix": row["discovered_at_unix"],
                "updated_at_unix": row["updated_at_unix"],
                "output": row["output_path"] or result.get("output"),
                "bytes": row["output_bytes"] or result.get("bytes"),
                "duration_sec": result.get("duration_sec"),
                "coverage_status": result.get("coverage_status"),
                "coverage_reason": result.get("coverage_reason"),
                **_coverage_contract(result),
                "seconds_after_event_observed": result.get(
                    "seconds_after_event_observed"
                ),
                "attempt_count": row["attempt_count"],
                "readiness_check_count": row["readiness_check_count"],
                "next_attempt_at_unix": row["next_attempt_at_unix"],
                "deadline_at_unix": row["deadline_at_unix"],
                "last_error_kind": row["last_error_kind"],
                "error": row["error"],
            }
        )
    aliases = {
        str(row["version_key"]): str(row["canonical_key"])
        for row in alias_rows
    }
    vision_by_key: dict[str, dict[str, dict[str, Any]]] = {}
    for row in vision_rows:
        try:
            vision_result = json.loads(row["result_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            vision_result = {}
        if not isinstance(vision_result, dict):
            vision_result = {}
        try:
            location_metadata = json.loads(row["location_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            location_metadata = {}
        if not isinstance(location_metadata, dict):
            location_metadata = {}
        try:
            window_metadata = json.loads(row["window_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            window_metadata = {}
        if not isinstance(window_metadata, dict):
            window_metadata = {}
        progressive_scan = window_metadata.get("progressive_scan")
        if not isinstance(progressive_scan, dict):
            progressive_scan = {}
        error_kind = vision_result.get("error_kind") or row["last_error_kind"]
        locator_method = (
            vision_result.get("locator_method")
            or vision_result.get("location_method")
        )
        if not locator_method and "t-deed" in str(row["model_name"] or "").lower():
            locator_method = "tdeed"
        ocr_payload = vision_result.get("ocr")
        if not isinstance(ocr_payload, dict):
            ocr_payload = {}
        ocr_error = vision_result.get("ocr_error")
        nested_diagnostics = ocr_payload.get("diagnostics")
        if not isinstance(nested_diagnostics, dict) and isinstance(ocr_error, dict):
            nested_diagnostics = ocr_error.get("diagnostics")
        if not isinstance(nested_diagnostics, dict):
            nested_diagnostics = {}
        exact_second_error = (
            vision_result.get("exact_second_error")
            or ocr_payload.get("exact_second_error")
            or nested_diagnostics.get("exact_second_failure")
        )
        if not isinstance(exact_second_error, dict):
            exact_second_error = None
        exact_error_diagnostics = (
            exact_second_error.get("diagnostics")
            if exact_second_error is not None else None
        )
        if not isinstance(exact_error_diagnostics, dict):
            exact_error_diagnostics = {}
        target_clock = (
            vision_result.get("target_clock")
            or ocr_payload.get("target_clock")
            or nested_diagnostics.get("target_clock")
        )
        ocr_diagnostics = (
            vision_result.get("ocr_diagnostics")
            or vision_result.get("ocr_diagnostics_summary")
        )
        if not isinstance(ocr_diagnostics, dict):
            if nested_diagnostics:
                ocr_diagnostics = {
                    "sampled_frames": nested_diagnostics.get("sampled_frame_count"),
                    "clock_readable_frames": nested_diagnostics.get(
                        "clock_readable_frame_count"
                    ),
                    "clock_readable_rate": nested_diagnostics.get(
                        "clock_readable_rate"
                    ),
                    "score_readable_frames": nested_diagnostics.get(
                        "score_readable_frame_count"
                    ),
                    "score_readable_rate": nested_diagnostics.get(
                        "score_readable_rate"
                    ),
                    "clock_repaired_frames": nested_diagnostics.get(
                        "clock_repaired_frame_count"
                    ),
                    "scoreboard_missing_frames": nested_diagnostics.get(
                        "scoreboard_missing_frame_count"
                    ),
                    "ambiguous_frames": nested_diagnostics.get(
                        "ambiguous_frame_count"
                    ),
                    "worker_wall_seconds": nested_diagnostics.get(
                        "worker_wall_seconds"
                    ),
                    "inference_seconds": nested_diagnostics.get(
                        "inference_seconds"
                    ),
                    "worker_mode": nested_diagnostics.get("worker_mode"),
                }
        if isinstance(ocr_diagnostics, dict):
            ocr_diagnostics = dict(ocr_diagnostics)
            ocr_diagnostics.setdefault("target_clock", target_clock)
            ocr_diagnostics.setdefault(
                "exact_second_failure_reason",
                exact_error_diagnostics.get("exact_second_failure_reason")
                or nested_diagnostics.get("exact_second_failure_reason"),
            )
        ocr_pipeline_statuses = {
            "waiting_for_clock_target",
            "waiting_for_latest_tail_rescan",
            "ocr_target_rescan",
            "waiting_for_postroll",
            "ocr_second_exact",
            "ocr_second_interpolated",
            "ocr_second_estimated",
            "ocr_second_projected",
            "ocr_minute_fallback",
            "ocr_range_fallback",
            "ocr_no_clock_detected",
            "ocr_target_timeout",
            "ocr_target_media_not_arrived",
            "ocr_target_media_stalled",
            "ocr_clock_paused_timeout",
            "ocr_window_evicted",
            "ocr_discontinuous_clock",
            "ocr_preparation_timeout",
            "ocr_encode_failed",
            "ocr_dependency_unavailable",
            "ocr_incomplete",
        }

        def first_ocr_value(*keys: str) -> Any:
            for source in (
                vision_result,
                ocr_payload,
                nested_diagnostics,
                ocr_diagnostics if isinstance(ocr_diagnostics, dict) else {},
                location_metadata,
                progressive_scan,
                window_metadata,
            ):
                for key in keys:
                    value = source.get(key)
                    if value not in (None, ""):
                        return value
            return None

        ocr_pipeline_status = next(
            (
                candidate
                for key in (
                    "ocr_pipeline_status",
                    "ocr_status",
                    "workflow_status",
                    "pipeline_status",
                    "stage",
                    "visual_resolution",
                )
                if (
                    candidate := str(first_ocr_value(key) or "").strip()
                ) in ocr_pipeline_statuses
            ),
            "",
        )
        # A crossed target is retried from a short historical window.  The
        # durable row remains pending while that rewind is waiting for a
        # worker slot, so expose it as a distinct recoverable state instead of
        # presenting the old terminal timeout label.
        target_rescan_window = progressive_scan.get("target_rescan_window")
        target_rescan_pending = bool(
            str(row["status"] or "").strip().lower() != "failed"
            and isinstance(target_rescan_window, dict)
            and progressive_scan.get("target_rescan_completed_at_unix") is None
        )
        if target_rescan_pending and ocr_pipeline_status not in {
            "ocr_second_exact",
            "ocr_second_interpolated",
            "ocr_second_estimated",
            "ocr_second_projected",
            "ocr_minute_fallback",
            "ocr_range_fallback",
        }:
            ocr_pipeline_status = "ocr_target_rescan"
        # A previous attempt's terminal error can remain in result_json while
        # the durable row is requeued. Never expose that stale error as a
        # failed badge for a recoverable row.
        recoverable_statuses = {
            "waiting_for_latest_tail_rescan",
            "ocr_clock_target_not_located",
            "ocr_no_clock_detected",
            "ocr_target_timeout",
            "ocr_target_media_not_arrived",
            "ocr_target_media_stalled",
            "ocr_clock_paused_timeout",
            "ocr_window_evicted",
            "ocr_discontinuous_clock",
            "ocr_encode_failed",
            "ocr_dependency_unavailable",
        }
        if (
            str(row["status"] or "").strip().lower() != "failed"
            and ocr_pipeline_status in recoverable_statuses
        ):
            ocr_pipeline_status = (
                "ocr_target_rescan"
                if target_rescan_pending
                else "waiting_for_clock_target"
            )
        # Successful retries merge into the durable result JSON. Older failure
        # details may remain for diagnostics, but they must not override a real
        # encoded output and make the dashboard report a timeout after success.
        encoded_source = str(vision_result.get("localization_source") or "")
        encoded_precision = str(vision_result.get("precision") or "")
        encoded_has_location = bool(
            encoded_source
            in {"exact_second", "exact", "interpolated", "estimated", "projected"}
            or encoded_source == "minute_boundary"
            or encoded_precision
            in {
                "observed_second",
                "interpolated_second",
                "estimated_second",
                "projected_second",
                "minute_boundary",
                "estimated_minute_boundary",
                "projected_minute_boundary",
            }
        )
        if (
            str(row["status"] or "").strip().lower() == "encoded"
            and bool(row["output_path"] or vision_result.get("output"))
            and (
                encoded_has_location
                or vision_result.get("output_kind") == "api_time_range_fallback"
            )
        ):
            if vision_result.get("output_kind") == "api_time_range_fallback":
                ocr_pipeline_status = "ocr_range_fallback"
            elif (
                encoded_source == "projected"
                or encoded_precision == "projected_second"
                or vision_result.get("degradation_mode")
                == "mapped_clock_projection"
            ):
                ocr_pipeline_status = "ocr_second_projected"
            elif encoded_source == "minute_boundary":
                ocr_pipeline_status = "ocr_minute_fallback"
            elif (
                encoded_source == "estimated"
                or encoded_precision == "estimated_second"
                or vision_result.get("localization_quality") == "estimated"
            ):
                ocr_pipeline_status = "ocr_second_estimated"
            elif (
                encoded_source == "interpolated"
                or encoded_precision == "interpolated_second"
            ):
                ocr_pipeline_status = "ocr_second_interpolated"
            else:
                ocr_pipeline_status = "ocr_second_exact"
        if ocr_pipeline_status not in ocr_pipeline_statuses:
            structured_failure = vision_result.get("failure_reason")
            failure_kind = str(
                (
                    structured_failure.get("kind")
                    if isinstance(structured_failure, dict)
                    else structured_failure
                    if isinstance(structured_failure, str)
                    else ""
                )
                or error_kind
                or row["last_error_kind"]
                or ""
            ).strip()
            normalized_failure_status = {
                "ocr_no_trustworthy_clock_before_deadline": "ocr_no_clock_detected",
                "ocr_clock_unreadable": "ocr_no_clock_detected",
                "scoreboard_missing": "ocr_no_clock_detected",
                "ocr_clock_target_timeout": "ocr_target_timeout",
                "ocr_target_media_not_arrived": "ocr_target_media_not_arrived",
                "ocr_target_media_stalled": "ocr_target_media_stalled",
                "ocr_clock_paused_timeout": "ocr_clock_paused_timeout",
                "ocr_clock_target_not_located": "ocr_target_timeout",
                "ocr_postroll_timeout": "ocr_target_timeout",
                "ocr_output_window_timeout": "ocr_target_timeout",
                "ocr_search_history_evicted": "ocr_window_evicted",
                "ocr_output_history_evicted": "ocr_window_evicted",
                "ocr_buffer_never_available": "ocr_window_evicted",
                "buffer_history_missing": "ocr_window_evicted",
                "ocr_output_video_gap": "ocr_window_evicted",
                "buffer_gap": "ocr_discontinuous_clock",
                "ocr_ambiguous": "ocr_discontinuous_clock",
                "ocr_video_preparation_timeout": "ocr_preparation_timeout",
                "ocr_window_encoding_failed": "ocr_encode_failed",
                "ocr_window_encoding_timeout": "ocr_encode_failed",
                "ocr_model_unavailable": "ocr_dependency_unavailable",
                "ocr_inference_failed": "ocr_dependency_unavailable",
                "ocr_processing_failed": "ocr_dependency_unavailable",
                "vision_shutdown_timeout": "ocr_incomplete",
            }.get(failure_kind)
            if failure_kind in ocr_pipeline_statuses:
                ocr_pipeline_status = failure_kind
            elif normalized_failure_status is not None:
                ocr_pipeline_status = normalized_failure_status
            elif vision_result.get("localization_source") in {
                "exact_second",
                "exact",
                "interpolated",
                "estimated",
                "projected",
            }:
                localization_source = str(
                    vision_result.get("localization_source") or ""
                )
                precision = str(vision_result.get("precision") or "")
                is_estimated = (
                    localization_source == "estimated"
                    or precision == "estimated_second"
                    or vision_result.get("localization_quality") == "estimated"
                    or vision_result.get("degraded") is True
                )
                is_projected = (
                    localization_source == "projected"
                    or precision == "projected_second"
                    or vision_result.get("degradation_mode")
                    == "mapped_clock_projection"
                )
                is_interpolated = (
                    localization_source == "interpolated"
                    or precision == "interpolated_second"
                )
                ocr_pipeline_status = (
                    "ocr_second_projected"
                    if is_projected
                    else "ocr_second_estimated"
                    if is_estimated
                    else "ocr_second_interpolated"
                    if is_interpolated
                    else "ocr_second_exact"
                )
            elif (
                vision_result.get("localization_source") == "minute_boundary"
                or vision_result.get("minute_fallback") is True
            ):
                ocr_pipeline_status = "ocr_minute_fallback"
            else:
                ocr_pipeline_status = ""
        if (
            str(row["status"] or "").strip().lower() != "failed"
            and ocr_pipeline_status in recoverable_statuses
        ):
            ocr_pipeline_status = (
                "ocr_target_rescan"
                if target_rescan_pending
                else "waiting_for_clock_target"
            )
        scan_window = first_ocr_value("scan_window", "scanning_window")
        if not isinstance(scan_window, dict):
            scan_window = window_metadata.get("search_window")
        if not isinstance(scan_window, dict):
            scan_window = vision_result.get("search_window")
        if not isinstance(scan_window, dict) and (
            progressive_scan.get("last_scan_start_stream_time") is not None
            or progressive_scan.get("last_scan_end_stream_time") is not None
        ):
            scan_window = {
                "start_stream_time": progressive_scan.get(
                    "last_scan_start_stream_time"
                ),
                "end_stream_time": progressive_scan.get(
                    "last_scan_end_stream_time"
                ),
            }
        if not isinstance(scan_window, dict):
            scan_window = None
        final_clip_window = first_ocr_value("final_clip_window", "clip_window")
        if not isinstance(final_clip_window, dict):
            final_clip_window = vision_result.get("actual_media_window")
        if not isinstance(final_clip_window, dict):
            final_clip_window = vision_result.get("requested_media_window")
        if not isinstance(final_clip_window, dict) and (
            progressive_scan.get("requested_output_start_stream_time") is not None
            or progressive_scan.get("requested_output_end_stream_time") is not None
        ):
            final_clip_window = {
                "start_stream_time": progressive_scan.get(
                    "requested_output_start_stream_time"
                ),
                "end_stream_time": progressive_scan.get(
                    "requested_output_end_stream_time"
                ),
            }
        if not isinstance(final_clip_window, dict):
            final_clip_window = None
        minute_fallback = bool(vision_result.get("minute_fallback"))
        fallback_requested_seconds = vision_result.get(
            "requested_fallback_seconds"
        )
        try:
            fallback_requested_seconds = float(fallback_requested_seconds)
            if not math.isfinite(fallback_requested_seconds) or fallback_requested_seconds <= 0:
                raise ValueError
        except (TypeError, ValueError):
            fallback_requested_seconds = 60.0
        fallback_available_seconds = vision_result.get(
            "available_fallback_seconds"
        )
        try:
            fallback_available_seconds = float(fallback_available_seconds)
            if not math.isfinite(fallback_available_seconds) or fallback_available_seconds < 0:
                raise ValueError
        except (TypeError, ValueError):
            # Older workers did not persist coverage metadata. The encoded
            # duration is the best durable approximation of actual coverage.
            fallback_available_seconds = vision_result.get("duration_sec")
            try:
                fallback_available_seconds = float(fallback_available_seconds)
                if not math.isfinite(fallback_available_seconds) or fallback_available_seconds < 0:
                    raise ValueError
            except (TypeError, ValueError):
                fallback_available_seconds = None
        fallback_complete = vision_result.get("fallback_complete")
        if minute_fallback or vision_result.get("fallback_generated") is True:
            if fallback_complete is None:
                fallback_complete = bool(
                    fallback_available_seconds is not None
                    and fallback_available_seconds
                    >= fallback_requested_seconds * 0.9
                )
        else:
            fallback_complete = None

        raw_artifact_kind = str(row["artifact_kind"] or "refined")
        artifact_kind = (
            "tdeed_refined" if raw_artifact_kind == "refined" else raw_artifact_kind
        )
        target_wait_outcome = first_ocr_value("target_wait_outcome")
        coverage_diagnostics = first_ocr_value("coverage_diagnostics")
        if not isinstance(coverage_diagnostics, dict):
            coverage_diagnostics = {}
        target_failure_cause = first_ocr_value(
            "target_failure_cause", "ocr_target_failure_cause"
        )
        target_passed_without_anchor = first_ocr_value(
            "target_passed_without_anchor"
        )
        target_failure_coverage_class = first_ocr_value(
            "target_failure_coverage_class", "coverage_class"
        ) or coverage_diagnostics.get("coverage_class")
        target_failure_scan_stage = first_ocr_value(
            "target_failure_scan_stage", "scan_stage"
        )
        target_clock_gap_seconds = first_ocr_value("target_clock_gap_seconds")
        latest_media_end_stream_time = first_ocr_value(
            "latest_media_end_stream_time"
        ) or coverage_diagnostics.get("latest_media_end_stream_time")
        previous_media_end_stream_time = first_ocr_value(
            "previous_media_end_stream_time"
        )
        wait_explanations = {
            "target_media_not_arrived": (
                "目标比赛时钟对应的画面还没有进入当前缓存，OCR 目前只读到了较早时间。"
            ),
            "media_stalled": (
                "缓存尾部在等待期间没有继续增长，暂时没有新画面可供 OCR 扫描。"
            ),
            "clock_paused": (
                "比分牌时钟在画面继续播放时没有推进，可能是比赛暂停或转播回放。"
            ),
        }
        failure_explanation = first_ocr_value(
            "failure_explanation",
            "target_failure_explanation",
            "target_wait_explanation",
        )
        if not failure_explanation and target_wait_outcome:
            failure_explanation = wait_explanations.get(target_wait_outcome)
        next_actions = {
            "target_media_not_arrived": "继续等待缓存进入目标时间；若直播已停止，再保留默认 GIF。",
            "media_stalled": "检查直播源或缓存录制是否断流；恢复增长后会继续目标附近精扫。",
            "clock_paused": "等待比赛时钟恢复推进；不要把暂停期间的画面当作目标时间。",
        }
        failure_next_action = first_ocr_value(
            "failure_next_action", "target_wait_next_action"
        )
        if not failure_next_action and target_wait_outcome:
            failure_next_action = next_actions.get(target_wait_outcome)
        artifact = {
            "artifact_kind": artifact_kind,
            "status": str(row["status"]),
            "anchor_stream_time_sec": row["located_anchor_stream_time"],
            "confidence": row["confidence"],
            "inference_seconds": row["inference_seconds"],
            "model_name": row["model_name"],
            "model_version": row["model_version"],
            "output": row["output_path"] or vision_result.get("output"),
            "bytes": row["output_bytes"] or vision_result.get("bytes"),
            "duration_sec": vision_result.get("duration_sec"),
            "anchor_delta_seconds": vision_result.get("anchor_delta_seconds"),
            "coverage_status": vision_result.get("coverage_status"),
            "coverage_reason": vision_result.get("coverage_reason"),
            **_coverage_contract(vision_result),
            "experimental": bool(vision_result.get("experimental")),
            "disabled": bool(vision_result.get("disabled")),
            "ocr_pipeline_status": ocr_pipeline_status or None,
            "target_rescan_pending": target_rescan_pending,
            "target_rescan_window": target_rescan_window
            if isinstance(target_rescan_window, dict)
            else None,
            "target_rescan_attempt_count": first_ocr_value(
                "target_rescan_attempt_count"
            ),
            "target_rescan_exhausted": bool(
                progressive_scan.get("target_rescan_exhausted")
            ),
            "target_rescan_sample_interval_seconds": first_ocr_value(
                "sample_interval_seconds"
            ),
            "scan_window": scan_window,
            "scan_cursor": first_ocr_value(
                "scan_cursor",
                "scan_cursor_stream_time",
                "cursor_stream_time",
                "last_scanned_stream_time",
            ),
            "last_trusted_clock": first_ocr_value(
                "last_trusted_clock",
                "latest_trusted_clock",
                "last_trustworthy_clock",
                "trusted_clock",
                "last_clock_reading",
            ),
            "last_trusted_clock_seconds": first_ocr_value(
                "last_trusted_clock_seconds",
                "latest_trusted_clock_seconds",
            ),
            "target_clock_seconds": first_ocr_value("target_clock_seconds"),
            "target_wait_outcome": target_wait_outcome,
            "target_failure_cause": target_failure_cause,
            "target_failure_coverage_class": target_failure_coverage_class,
            "target_failure_scan_stage": target_failure_scan_stage,
            "target_passed_without_anchor": bool(target_passed_without_anchor),
            "target_failure_explanation": first_ocr_value(
                "target_failure_explanation"
            ),
            "target_clock_gap_seconds": target_clock_gap_seconds,
            "latest_media_end_stream_time": latest_media_end_stream_time,
            "previous_media_end_stream_time": previous_media_end_stream_time,
            "history_missing_seconds": coverage_diagnostics.get(
                "history_missing_seconds"
            ),
            "target_history_missing": bool(
                coverage_diagnostics.get("target_history_missing")
            ),
            "target_history_fully_missing": bool(
                coverage_diagnostics.get("target_history_fully_missing")
            ),
            "earliest_media_start_stream_time": coverage_diagnostics.get(
                "earliest_media_start_stream_time"
            ),
            "video_gaps": coverage_diagnostics.get("video_gaps", []),
            "failure_explanation": failure_explanation,
            "failure_next_action": failure_next_action,
            "scan_attempt_count": first_ocr_value("scan_attempt_count"),
            "next_attempt_at_unix": (
                first_ocr_value("next_attempt_at_unix")
                or row["next_attempt_at_unix"]
            ),
            "deadline_at_unix": (
                first_ocr_value("deadline_at_unix") or row["deadline_at_unix"]
            ),
            "final_clip_window": final_clip_window,
            "location_metadata": location_metadata,
            "window_metadata": window_metadata,
            "error_kind": error_kind,
            "last_error_kind": row["last_error_kind"],
            "locator_method": locator_method,
            "stage": (
                vision_result.get("stage")
                or vision_result.get("failed_stage")
                or row["failure_stage"]
            ),
            "progressive_status": first_ocr_value("progressive_status", "state"),
            "fallback_used": vision_result.get("fallback_used"),
            "minute_fallback": minute_fallback,
            "fallback_generated": bool(vision_result.get("fallback_generated")),
            "fallback_complete": fallback_complete,
            "fallback_label": vision_result.get("fallback_label"),
            "fragmented_fallback": bool(vision_result.get("fragmented_fallback")),
            "fallback_explanation": vision_result.get("fallback_explanation"),
            "ocr_verified": vision_result.get("ocr_verified"),
            "available_fallback_seconds": fallback_available_seconds,
            "requested_fallback_seconds": fallback_requested_seconds,
            "default_gif_preserved": vision_result.get("default_gif_preserved"),
            "output_kind": vision_result.get("output_kind"),
            "precise_location": vision_result.get("precise_location"),
            "target_clock": target_clock,
            "exact_second_error": exact_second_error,
            "failure_reason": (
                vision_result.get("failure_reason")
                or (
                    {
                        "kind": error_kind,
                        "stage": row["failure_stage"],
                        "message": row["persisted_failure_reason"],
                    }
                    if row["persisted_failure_reason"]
                    else None
                )
            ),
            "localization_source": vision_result.get("localization_source"),
            "localization_precision": vision_result.get(
                "localization_precision"
            ),
            "precision": vision_result.get("precision"),
            "localization_quality": vision_result.get("localization_quality"),
            "degraded": bool(vision_result.get("degraded")),
            "localization_degraded": vision_result.get("localization_degraded"),
            "coverage_degraded": vision_result.get("coverage_degraded"),
            "degradation_mode": vision_result.get("degradation_mode"),
            "degradation_reason": vision_result.get("degradation_reason"),
            "estimated_error_bound_seconds": first_ocr_value(
                "estimated_error_bound_seconds"
            ),
            "target_clock_directly_observed": first_ocr_value(
                "target_clock_directly_observed"
            ),
            "clock_video_mapping": first_ocr_value("clock_video_mapping"),
            "requested_media_window": vision_result.get("requested_media_window"),
            "actual_media_window": vision_result.get("actual_media_window"),
            "source_ocr_artifact": vision_result.get("source_ocr_artifact"),
            "clip_before_seconds": vision_result.get("clip_before_seconds"),
            "clip_after_seconds": vision_result.get("clip_after_seconds"),
            "output_width": vision_result.get("output_width"),
            "output_fps": vision_result.get("output_fps"),
            "output_colors": vision_result.get("output_colors"),
            "tdeed_error_kind": vision_result.get("tdeed_error_kind"),
            "ocr_diagnostics": ocr_diagnostics,
            "fragment_attempts": vision_result.get("fragment_attempts", []),
            "fragment_window": vision_result.get("fragment_window"),
            "error": row["error"] or vision_result.get("error"),
        }
        vision_by_key.setdefault(str(row["event_key"]), {})[artifact_kind] = artifact
    for task in tasks:
        artifacts = vision_by_key.get(str(task.get("event_key")), {})
        task["vision_artifacts"] = artifacts
        task["ocr_window"] = artifacts.get("ocr_window")
        # Keep the old field during the dashboard/API transition.
        task["vision"] = artifacts.get("tdeed_refined")
    return tasks, aliases, suppressed_keys


EVENT_DETAIL_FIELDS = (
    "code",
    "event_type",
    "label",
    "minute",
    "minute_extra",
    "team",
    "person",
    "person_id",
    "score",
    "reason",
    "metadata",
)
TASK_STATUS_RANK = {
    "history": 0,
    "failed": 1,
    "discovered": 2,
    "pending": 3,
    "encoding": 4,
    "encoded": 5,
}


def _merge_event_details(
    target: dict[str, Any], update: dict[str, Any]
) -> dict[str, Any]:
    merged = dict(target)
    for key in EVENT_DETAIL_FIELDS:
        value = update.get(key)
        if key == "metadata" and isinstance(value, dict):
            current = merged.get(key)
            merged[key] = {
                **(current if isinstance(current, dict) else {}),
                **value,
            }
        elif value not in (None, "", "0") or key == "minute_extra":
            merged[key] = value
    return merged


def _merge_task_rows(
    target: dict[str, Any], update: dict[str, Any]
) -> dict[str, Any]:
    merged = _merge_event_details(target, update)
    target_rank = TASK_STATUS_RANK.get(str(target.get("status")), 0)
    update_rank = TASK_STATUS_RANK.get(str(update.get("status")), 0)
    if update_rank > target_rank:
        for key in (
            "status",
            "output",
            "bytes",
            "duration_sec",
            "seconds_after_event_observed",
            "error",
        ):
            if update.get(key) is not None:
                merged[key] = update[key]
    merged["event_key"] = str(target.get("event_key") or update.get("event_key"))
    merged["duplicate_task_count"] = int(target.get("duplicate_task_count") or 1) + int(
        update.get("duplicate_task_count") or 1
    )
    return merged


def _canonicalize_event_rows(
    tasks: list[dict[str, Any]],
    api_events: list[dict[str, Any]],
    aliases: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Return one dashboard row per real incident while preserving task state."""
    canonical_rows: list[dict[str, Any]] = []
    by_key: dict[str, dict[str, Any]] = {}
    aliases = aliases or {}

    for task in sorted(
        tasks,
        key=lambda item: (
            float(item.get("discovered_at_unix") or 0),
            str(item.get("event_key") or ""),
        ),
    ):
        task = dict(task)
        task.setdefault("duplicate_task_count", 1)
        canonical_key = aliases.get(str(task.get("event_key") or ""), "")
        target = by_key.get(canonical_key) if canonical_key else None
        if target is None:
            candidates = [
                row
                for row in canonical_rows
                if events_represent_same_incident(row, task, allow_exact_match=False)
            ]
            target = candidates[0] if len(candidates) == 1 else None
        if target is None:
            if canonical_key:
                task["event_key"] = canonical_key
            canonical_rows.append(task)
            by_key[str(task.get("event_key") or "")] = task
            continue
        merged = _merge_task_rows(target, task)
        target.clear()
        target.update(merged)
        by_key[str(target.get("event_key") or "")] = target

    for index, api_event in enumerate(api_events):
        candidates = [
            row
            for row in canonical_rows
            if events_represent_same_incident(row, api_event)
        ]
        if len(candidates) == 1:
            merged = _merge_event_details(candidates[0], api_event)
            candidates[0].clear()
            candidates[0].update(merged)
            continue
        history = {
            **api_event,
            "event_key": (
                f"api:{api_event.get('event_id')}"
                if api_event.get("event_id")
                else f"api:{api_event.get('code')}:{api_event.get('minute')}:"
                f"{api_event.get('person_id')}:{index}"
            ),
            "status": "history",
            "duplicate_task_count": 0,
        }
        canonical_rows.append(history)

    return list(reversed(canonical_rows))


def _latest_ingest_message(
    output_dir: Path,
    *,
    started_at_unix: float | None = None,
) -> str | None:
    paths: list[tuple[float, Path]] = []
    try:
        for path in output_dir.glob("ingest_ffmpeg_*.log"):
            try:
                modified_at = path.stat().st_mtime
            except OSError:
                continue
            if started_at_unix is None or modified_at >= started_at_unix:
                paths.append((modified_at, path))
    except OSError:
        pass
    paths.sort(key=lambda item: item[0], reverse=True)
    for _, path in paths:
        try:
            lines = [
                line.strip()
                for line in path.read_text(
                    encoding="utf-8",
                    errors="replace",
                ).splitlines()
                if line.strip()
            ]
        except OSError:
            continue
        if lines:
            # FFmpeg runs with -loglevel error, so every non-empty line is an
            # ingest error. A newer, empty log means the current retry is alive;
            # retain the preceding error so the reconnect remains explainable.
            return lines[-1]
    return None


def _row_is_from_current_run(
    row: dict[str, Any],
    started_at_unix: float | None,
) -> bool:
    if started_at_unix is None:
        return True
    try:
        return float(row.get("timestamp_unix") or 0) >= started_at_unix
    except (TypeError, ValueError):
        return False


def _runtime_evidence(
    session: MatchSession,
    report: dict[str, Any],
    tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    now = time.time()
    started_at = session.worker_started_at
    worker_running = session.worker_running()
    log_rows = _read_log(session.output_dir / "pipeline_events.jsonl", limit=600)
    current_log_rows = [
        row
        for row in log_rows
        if _row_is_from_current_run(row, started_at)
    ]
    heartbeat = next(
        (row for row in current_log_rows if row.get("event") == "runtime_heartbeat"),
        {},
    )

    try:
        report_started_at = float(report.get("started_at_unix") or 0)
    except (TypeError, ValueError):
        report_started_at = 0.0
    report_is_current = bool(report) and (
        started_at is None or report_started_at >= started_at
    )
    current_report = report if report_is_current else {}
    event_source = current_report.get("event_source") or {}
    shotmap_source = current_report.get("shotmap_source") or {}

    segment_count = 0
    latest_segment_unix: float | None = None
    try:
        for path in (session.output_dir / "buffer").glob("*.ts"):
            modified = path.stat().st_mtime
            if started_at is not None and modified < started_at:
                continue
            segment_count += 1
            latest_segment_unix = max(latest_segment_unix or modified, modified)
    except OSError:
        pass

    heartbeat_unix = float(heartbeat.get("timestamp_unix") or 0) or None
    heartbeat_age = max(0.0, now - heartbeat_unix) if heartbeat_unix else None
    heartbeat_fresh = heartbeat_age is not None and heartbeat_age <= 9
    segment_age = (
        max(0.0, now - latest_segment_unix) if latest_segment_unix else None
    )
    segment_writing = segment_age is not None and segment_age <= 9
    raw_ingest_running = heartbeat.get("ingest_running")
    ingest_running = (
        raw_ingest_running if isinstance(raw_ingest_running, bool) else None
    )
    try:
        ingest_restart_count = int(
            heartbeat.get("ingest_restart_count")
            if heartbeat.get("ingest_restart_count") is not None
            else (current_report.get("runtime") or {}).get(
                "ingest_restart_count",
                0,
            )
        )
    except (TypeError, ValueError):
        ingest_restart_count = 0
    try:
        ingest_reconnect_due_unix = (
            float(heartbeat["ingest_reconnect_due_unix"])
            if heartbeat.get("ingest_reconnect_due_unix") is not None
            else None
        )
    except (TypeError, ValueError):
        ingest_reconnect_due_unix = None
    last_ingest_error = _latest_ingest_message(
        session.output_dir,
        started_at_unix=started_at,
    )
    elapsed = max(0.0, now - started_at) if started_at else None
    if not worker_running and current_report.get("processing_wall_seconds") is not None:
        elapsed = float(current_report["processing_wall_seconds"])
    event_poll_count = int(
        heartbeat.get("event_poll_count")
        or event_source.get("poll_count")
        or 0
    )
    event_error_count = int(
        heartbeat.get("event_error_count")
        or event_source.get("error_count")
        or 0
    )
    last_event_error = heartbeat.get("last_event_error") or event_source.get("last_error")
    shotmap_poll_count = int(
        heartbeat.get("shotmap_poll_count")
        or shotmap_source.get("request_count")
        or 0
    )
    shotmap_error_count = int(
        heartbeat.get("shotmap_error_count")
        or shotmap_source.get("error_count")
        or 0
    )
    last_shotmap_error = (
        heartbeat.get("last_shotmap_error") or shotmap_source.get("last_error")
    )
    task_counts = {
        status: sum(item.get("status") == status for item in tasks)
        for status in ("pending", "encoding", "encoded", "failed")
    }

    if session.worker_cleanup_failure:
        state, label = "failed", "旧进程清理失败 · 已阻止重启"
    elif session.worker_cleanup_process_group is not None:
        state, label = "recovering", "正在清理旧进程"
    elif (
        session.desired_running
        and not worker_running
        and session.worker_restart_due_at is not None
    ):
        state, label = "recovering", "等待自动恢复"
    elif session.lifecycle_state == "finishing":
        state, label = "finishing", "比赛已结束 · 正在收尾"
    elif session.lifecycle_state in {"completed", "completed_with_warnings"}:
        state = session.lifecycle_state
        label = "处理完成" if state == "completed" else "完成但有警告"
    elif session.lifecycle_state == "stopped":
        state, label = "stopped", "已手动停止"
    elif session.lifecycle_state == "stopping":
        state, label = "recovering", "正在停止处理进程"
    elif session.lifecycle_state == "failed":
        state, label = "failed", "处理异常退出"
    else:
        state = label = None

    ffmpeg_return_code = current_report.get("ffmpeg_return_code")
    stopped_by_user = bool(current_report.get("stopped_by_user"))
    live_source = not str(current_report.get("source") or "").lower().endswith(
        (".mp4", ".mov", ".mkv", ".webm")
    )
    if state is not None:
        pass
    elif worker_running:
        if heartbeat_age is None and elapsed is not None and elapsed < 12:
            state, label = "starting", "正在建立直播缓存"
        elif not heartbeat_fresh:
            state, label = "degraded", "Worker 存活 · 运行心跳超时"
        elif ingest_running is False and ingest_reconnect_due_unix is not None:
            state, label = "recovering", "Worker 存活 · FFmpeg 等待重连"
        elif ingest_running is False:
            state, label = "degraded", "Worker 存活 · FFmpeg 未运行"
        elif ingest_running is True and not segment_writing:
            if elapsed is not None and elapsed < 12 and segment_count == 0:
                state, label = "starting", "FFmpeg 已启动 · 等待首个视频分片"
            else:
                state, label = "degraded", "FFmpeg 运行中 · 视频分片未持续写入"
        elif ingest_running is True and last_event_error:
            state, label = "degraded", "视频采集正常 · 事件接口异常"
        elif ingest_running is True:
            state, label = "healthy", "实时链路正常"
        elif segment_writing and not last_event_error:
            # Compatibility with heartbeats written by older Workers that did
            # not yet expose ingest_running.
            state, label = "healthy", "实时链路正常"
        else:
            state, label = "degraded", "运行中 · 信号待确认"
    elif not started_at and not current_report:
        state, label = "idle", "未启动"
    elif stopped_by_user:
        state, label = "stopped", "已手动停止"
    elif ffmpeg_return_code is not None and ffmpeg_return_code != 0:
        state, label = "failed", "处理异常退出"
    elif current_report and not live_source and task_counts["encoded"]:
        state, label = "completed", "演示验收通过"
    elif current_report:
        state, label = "disconnected", "直播流已结束"
    else:
        state, label = "stopped", "处理进程已退出"

    exit_message = last_ingest_error
    if not worker_running and (started_at or current_report):
        if stopped_by_user:
            exit_message = "用户停止"
        elif ffmpeg_return_code is not None:
            exit_message = f"FFmpeg 返回码 {ffmpeg_return_code}"
        exit_message = last_ingest_error or exit_message
    if session.worker_cleanup_failure:
        exit_message = session.worker_cleanup_failure
    elif (
        session.desired_running
        and not worker_running
        and session.worker_restart_due_at is not None
    ):
        exit_message = "旧进程清理完成后，Dashboard 将按退避计划自动恢复 Worker"

    return {
        "state": state,
        "label": label,
        "worker_running": worker_running,
        "started_at_unix": started_at,
        "elapsed_seconds": round(elapsed, 1) if elapsed is not None else None,
        "heartbeat_unix": heartbeat_unix,
        "heartbeat_age_seconds": round(heartbeat_age, 1) if heartbeat_age is not None else None,
        "heartbeat_fresh": heartbeat_fresh,
        "stream_time_seconds": heartbeat.get("stream_time_sec"),
        "buffer_segment_count": int(heartbeat.get("buffer_segment_count") or segment_count),
        "buffer_coverage_seconds": heartbeat.get("buffer_coverage_seconds"),
        "latest_segment_unix": latest_segment_unix,
        "latest_segment_age_seconds": round(segment_age, 1) if segment_age is not None else None,
        "segment_writing": segment_writing,
        "event_poll_count": event_poll_count,
        "event_error_count": event_error_count,
        "last_event_error": last_event_error,
        "shotmap_poll_count": shotmap_poll_count,
        "shotmap_error_count": shotmap_error_count,
        "last_shotmap_error": last_shotmap_error,
        "shotmap_initialized": bool(
            heartbeat.get("shotmap_initialized")
            or shotmap_source.get("initialized")
        ),
        "task_counts": task_counts,
        "ffmpeg_return_code": ffmpeg_return_code,
        "ingest_running": ingest_running,
        "ingest_restart_count": ingest_restart_count,
        "ingest_reconnect_due_unix": ingest_reconnect_due_unix,
        "last_ingest_error": last_ingest_error,
        "exit_message": exit_message,
        "lifecycle_state": session.lifecycle_state,
        "finish_reason": session.finish_reason,
        "exit_reason": session.exit_reason,
        "finishing_deadline_unix": session.finishing_deadline,
    }


def _session_json(session: MatchSession) -> dict[str, Any]:
    report = session.output_report()
    reported_tasks = report.get("events", []) if isinstance(report, dict) else []
    task_map = {
        str(item.get("event_key")): dict(item)
        for item in reported_tasks
        if isinstance(item, dict) and item.get("event_key")
    }
    for item in reversed(_tasks_from_log(session.output_dir / "pipeline_events.jsonl")):
        key = str(item.get("event_key"))
        if key:
            task_map[key] = {**task_map.get(key, {}), **item}
    database_tasks, aliases, suppressed_keys = _tasks_from_database(
        session.output_dir / "pipeline_state.sqlite3"
    )
    for key in suppressed_keys:
        task_map.pop(key, None)
    for item in database_tasks:
        key = str(item.get("event_key"))
        if key:
            task_map[key] = {**task_map.get(key, {}), **item}
    tasks = _canonicalize_event_rows(
        list(task_map.values()), flatten_events(session.event_payload), aliases
    )
    publish_records = article_publisher.records_for_match(session.match_id)
    for task in tasks:
        event_key = str(task.get("event_key") or "")
        task["publish"] = publish_records.get(event_key)
    event_counts = {
        "unique": len(tasks),
        "encoded": sum(item.get("status") == "encoded" for item in tasks),
        "processing": sum(
            item.get("status") in {"discovered", "pending", "encoding"}
            for item in tasks
        ),
        "history": sum(item.get("status") == "history" for item in tasks),
        "failed": sum(item.get("status") == "failed" for item in tasks),
    }
    telemetry = _runtime_evidence(session, report, tasks)
    return {
        "match_id": session.match_id,
        "status": session.status(),
        "status_label": MATCH_STATUS_LABELS.get(session.status(), "未知"),
        "lifecycle": {
            "state": session.lifecycle_state,
            "played_confirmation_count": session.played_confirmation_count,
            "played_confirmations_required": PLAYED_CONFIRMATIONS_REQUIRED,
            "played_confirmed_at_unix": session.played_confirmed_at,
            "finishing_started_at_unix": session.finishing_started_at,
            "finishing_deadline_unix": session.finishing_deadline,
            "finish_requested": session.finish_requested,
            "finish_timeout_signaled": session.finish_timeout_signaled,
            "finish_reason": session.finish_reason,
            "exit_reason": session.exit_reason,
        },
        "detail": session.detail,
        "source": session.source,
        "source_health": {
            "configured": bool(session.source.get("resource")),
            "resource": session.source.get("resource"),
            "updated_at": session.source.get("updated_at"),
            "last_poll_unix": session.last_source_poll,
            "error": session.source_error,
            "changed": session.source_changed,
            "change_message": session.source_change_message,
        },
        "event_api": {
            "last_poll_unix": session.last_event_poll,
            "error": session.event_error,
            "count": len(flatten_events(session.event_payload)),
        },
        "worker": {
            "running": session.worker_running(),
            "desired_running": session.desired_running,
            "pid": session.worker.pid if session.worker else None,
            "return_code": session.worker.poll() if session.worker else None,
            "started_at_unix": session.worker_started_at,
            "command": session.worker_command,
            "mode": session.worker_mode,
            "restart_count": session.worker_restart_count,
            "restart_due_at_unix": session.worker_restart_due_at,
            "cleanup_process_group": session.worker_cleanup_process_group,
            "cleanup_stage": session.worker_cleanup_stage,
            "cleanup_due_at_unix": session.worker_cleanup_due_at,
            "cleanup_failure": session.worker_cleanup_failure,
            "finish_requested": session.finish_requested,
        },
        "polling": {
            "events_seconds": session.event_poll_seconds,
            "shotmap_seconds": session.shotmap_poll_seconds,
            "source_seconds": session.source_poll_seconds,
            "detail_seconds": session.detail_poll_seconds,
        },
        "gif": {
            "before_seconds": session.before_seconds,
            "after_seconds": session.after_seconds,
            "event_to_video_offset_seconds": session.event_to_video_offset_seconds,
            "shotmap_offset_seconds": session.shotmap_offset_seconds,
            "width": session.gif_width,
            "fps": session.gif_fps,
            "colors": session.gif_colors,
            "size_reference_mb": 10,
            "adaptive_quality_reduction": False,
        },
        "vision": {
            "enabled": session.vision_enabled,
            "worker_enabled": "--vision-enabled" in session.worker_command,
            "tdeed_enabled": session.tdeed_enabled,
            "worker_tdeed_enabled": "--tdeed-enabled" in session.worker_command,
            "clock_only": session.vision_clock_only,
            "worker_clock_only": "--ocr-clock-only" in session.worker_command,
            "before_seconds": session.vision_before_seconds,
            "after_seconds": session.vision_after_seconds,
            "search_before_seconds": VISION_SEARCH_BEFORE_SECONDS,
            "search_after_seconds": VISION_SEARCH_AFTER_SECONDS,
            "scoreboard_profile_path": session.scoreboard_profile_path or None,
            "model": "PaddleOCR",
            "workers": VISION_WORKERS,
            "fallback_gif": {
                "duration_seconds": 60.0,
                "exact_second_before_seconds": 30.0,
                "exact_second_after_seconds": 30.0,
                "minute_boundary_before_seconds": 60.0,
                "minute_boundary_after_seconds": 0.0,
                "width": FALLBACK_GIF_WIDTH,
                "fps": FALLBACK_GIF_FPS,
                "colors": FALLBACK_GIF_COLORS,
            },
        },
        "events": tasks,
        "event_counts": event_counts,
        "logs": _read_log(session.output_dir / "pipeline_events.jsonl"),
        "report": report,
        "telemetry": telemetry,
    }


class Dashboard:
    def __init__(
        self,
        *,
        background_monitors: bool = True,
        max_concurrent_matches: int | None = None,
        session_retention_seconds: float | None = None,
        disk_cleanup_interval_seconds: float | None = None,
        orphan_cleanup_grace_seconds: float | None = None,
    ) -> None:
        self.sessions: dict[str, MatchSession] = {}
        self.lock = threading.RLock()
        self.background_monitors = background_monitors
        self.monitor_threads: dict[str, threading.Thread] = {}
        self.max_concurrent_matches = (
            MAX_CONCURRENT_MATCHES
            if max_concurrent_matches is None
            else int(max_concurrent_matches)
        )
        if self.max_concurrent_matches < 1:
            raise ValueError("max_concurrent_matches 必须是正整数")
        self.session_retention_seconds = (
            SESSION_RETENTION_SECONDS
            if session_retention_seconds is None
            else float(session_retention_seconds)
        )
        self.disk_cleanup_interval_seconds = (
            DISK_CLEANUP_INTERVAL_SECONDS
            if disk_cleanup_interval_seconds is None
            else float(disk_cleanup_interval_seconds)
        )
        self.orphan_cleanup_grace_seconds = (
            ORPHAN_CLEANUP_GRACE_SECONDS
            if orphan_cleanup_grace_seconds is None
            else float(orphan_cleanup_grace_seconds)
        )
        if self.session_retention_seconds <= 0:
            raise ValueError("session_retention_seconds 必须是正数")
        if self.disk_cleanup_interval_seconds <= 0:
            raise ValueError("disk_cleanup_interval_seconds 必须是正数")
        if (
            not math.isfinite(self.session_retention_seconds)
            or not math.isfinite(self.disk_cleanup_interval_seconds)
            or not math.isfinite(self.orphan_cleanup_grace_seconds)
            or self.orphan_cleanup_grace_seconds <= 0
        ):
            raise ValueError("清理时间参数必须是有限正数")
        self._maintenance_stop = threading.Event()
        self.maintenance_thread: threading.Thread | None = None
        if self.background_monitors:
            self.maintenance_thread = threading.Thread(
                target=self._maintenance_loop,
                name="dashboard-disk-maintenance",
                daemon=True,
            )
            self.maintenance_thread.start()

    @staticmethod
    def _session_holds_worker_slot(session: MatchSession) -> bool:
        return bool(
            session.worker_running()
            or session.desired_running
            or session.lifecycle_state in {"starting", "finishing", "stopping"}
            or session.worker_cleanup_process_group is not None
        )

    def _active_match_summaries_locked(self) -> list[dict[str, Any]]:
        summaries = []
        for match_id, session in self.sessions.items():
            worker_running = session.worker_running()
            if not (
                worker_running
                or session.desired_running
                or session.lifecycle_state in {"starting", "finishing", "stopping"}
                or session.worker_cleanup_process_group is not None
            ):
                continue
            if session.worker_cleanup_process_group is not None:
                reservation_state = "cleaning"
            elif session.lifecycle_state == "finishing":
                reservation_state = "finishing"
            elif session.lifecycle_state == "stopping":
                reservation_state = "stopping"
            elif worker_running:
                reservation_state = "running"
            elif session.desired_running:
                reservation_state = "recovering"
            else:
                reservation_state = "starting"
            summaries.append(
                {
                    "match_id": match_id,
                    "state": reservation_state,
                    "match_status": session.status(),
                    "lifecycle_state": session.lifecycle_state,
                    "worker_running": worker_running,
                    "desired_running": session.desired_running,
                    "worker_mode": session.worker_mode,
                    "worker_pid": session.worker.pid if worker_running else None,
                    "restart_due_at_unix": session.worker_restart_due_at,
                    "cleanup_process_group": session.worker_cleanup_process_group,
                    "finish_reason": session.finish_reason,
                }
            )
        return sorted(summaries, key=lambda item: item["match_id"])

    def worker_slot_status(self) -> dict[str, Any]:
        with self.lock:
            active_matches = self._active_match_summaries_locked()
            active_match_ids = [item["match_id"] for item in active_matches]
            active_match_count = len(active_match_ids)
            available_slots = max(
                0, self.max_concurrent_matches - active_match_count
            )
            at_capacity = available_slots == 0
            return {
                # The singular field remains for old clients. It is intentionally
                # unset when several matches are active to avoid selecting one
                # arbitrary match as the global owner.
                "active_match_id": (
                    active_match_ids[0] if len(active_match_ids) == 1 else None
                ),
                "active_match_ids": active_match_ids,
                "active_matches": active_matches,
                "active_match_count": active_match_count,
                "max_concurrent_matches": self.max_concurrent_matches,
                "available_worker_slots": available_slots,
                "at_capacity": at_capacity,
                "locked": at_capacity,
            }

    def _assert_worker_slot_available(self, session: MatchSession) -> None:
        with self.lock:
            active_match_ids = {
                item["match_id"] for item in self._active_match_summaries_locked()
            }
            if session.match_id in active_match_ids:
                return
            if len(active_match_ids) >= self.max_concurrent_matches:
                raise RuntimeError(
                    f"并发比赛已达上限 {self.max_concurrent_matches} 场；"
                    "请先停止一场比赛并等待其 Worker 完全退出"
                )

    def _claim_worker_slot(self, session: MatchSession) -> None:
        with self.lock:
            if self.sessions.get(session.match_id) is not session:
                raise RuntimeError("比赛会话已过期，请重新打开该比赛")
            active_match_ids = {
                item["match_id"] for item in self._active_match_summaries_locked()
            }
            if (
                session.match_id not in active_match_ids
                and len(active_match_ids) >= self.max_concurrent_matches
            ) or len(active_match_ids) > self.max_concurrent_matches:
                raise RuntimeError(
                    f"并发比赛已达上限 {self.max_concurrent_matches} 场；"
                    "请先停止一场比赛并等待其 Worker 完全退出"
                )

    def _release_worker_slot(
        self,
        session: MatchSession,
        *,
        force: bool = False,
    ) -> None:
        # Capacity is derived from the per-match lifecycle instead of a mutable
        # global owner. Keep this method so existing transition call sites remain
        # explicit about when a match is expected to release its reservation.
        del force
        if self._session_holds_worker_slot(session):
            return

    def get(self, match_id: str) -> MatchSession:
        match_id = validate_match_id(match_id)
        now = time.time()
        with self.lock:
            self._prune_terminal_sessions_locked(now)
            self._prune_idle_sessions_locked(now)
            session = self.sessions.setdefault(
                match_id,
                MatchSession(match_id=match_id, output_dir=DEFAULT_OUTPUT / match_id),
            )
            session.last_access_at = now
        self._ensure_monitor(session)
        if match_id.startswith("demo-") and not session.detail:
            session.detail = _demo_detail(match_id)
            session.source = {
                "resource": str(
                    ROOT
                    / "downloads"
                    / "SV Wehen Wiesbaden vs FC Bayern Munchen [zqJI-83XFhM].mp4"
                ),
                "updated_at": "本地演示素材",
            }
            session.last_detail_poll = time.time()
            session.last_source_poll = time.time()
        return session

    def _ensure_monitor(self, session: MatchSession) -> None:
        if not self.background_monitors:
            return
        with self.lock:
            existing = self.monitor_threads.get(session.match_id)
            if existing is not None and existing.is_alive():
                return
            if self._terminal_and_inactive(session):
                return
            self.monitor_threads.pop(session.match_id, None)
            thread = threading.Thread(
                target=self._monitor,
                args=(session,),
                name=f"match-monitor-{session.match_id}",
                daemon=True,
            )
            self.monitor_threads[session.match_id] = thread
            thread.start()

    def _monitor(self, session: MatchSession) -> None:
        current_thread = threading.current_thread()
        try:
            while not self._maintenance_stop.is_set():
                try:
                    self.refresh(session)
                except Exception as exc:
                    self._log_control(session, "monitor_error", error=str(exc))
                with session.lock:
                    if self._terminal_and_inactive(session):
                        self._mark_terminal(session)
                        break
                    if self._idle_session_expired(session, time.time()):
                        break
                self._maintenance_stop.wait(0.5)
        finally:
            with self.lock:
                if self.monitor_threads.get(session.match_id) is current_thread:
                    self.monitor_threads.pop(session.match_id, None)

    @staticmethod
    def _terminal_and_inactive(session: MatchSession) -> bool:
        return bool(
            session.lifecycle_state in TERMINAL_LIFECYCLE_STATES
            and not session.worker_running()
            and not session.desired_running
            and session.worker_process_group is None
            and session.worker_cleanup_process_group is None
        )

    def _idle_session_expired(self, session: MatchSession, now: float) -> bool:
        """Return whether an unstarted/unowned session has gone cold."""
        if session.lifecycle_state in {
            "finishing",
            *TERMINAL_LIFECYCLE_STATES,
        }:
            return False
        if (
            session.worker_running()
            or session.desired_running
            or session.worker_process_group is not None
            or session.worker_cleanup_process_group is not None
        ):
            return False
        return now - session.last_access_at >= self.session_retention_seconds

    @staticmethod
    def _mark_terminal(session: MatchSession, now: float | None = None) -> None:
        if session.terminal_at is None:
            session.terminal_at = time.time() if now is None else now

    def _prune_terminal_sessions_locked(self, now: float) -> list[str]:
        removed: list[str] = []
        for match_id, session in list(self.sessions.items()):
            if not self._terminal_and_inactive(session):
                continue
            terminal_at = session.terminal_at
            if terminal_at is None:
                terminal_at = session.created_at
                session.terminal_at = terminal_at
            if now - terminal_at < self.session_retention_seconds:
                continue
            thread = self.monitor_threads.get(match_id)
            if thread is not None and thread.is_alive():
                continue
            self.monitor_threads.pop(match_id, None)
            self.sessions.pop(match_id, None)
            removed.append(match_id)
        return removed

    def _prune_idle_sessions_locked(self, now: float) -> list[str]:
        removed: list[str] = []
        for match_id, session in list(self.sessions.items()):
            if not self._idle_session_expired(session, now):
                continue
            thread = self.monitor_threads.get(match_id)
            if thread is not None and thread.is_alive():
                continue
            self.monitor_threads.pop(match_id, None)
            self.sessions.pop(match_id, None)
            removed.append(match_id)
        return removed

    @staticmethod
    def _active_segment_leases(
        state_db_path: Path,
        *,
        now: float,
    ) -> tuple[bool, set[str]]:
        if not state_db_path.exists():
            return True, set()
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(
                f"{state_db_path.resolve().as_uri()}?mode=ro",
                uri=True,
                timeout=2.0,
            )
            table = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'segment_leases'"
            ).fetchone()
            if table is None:
                return True, set()
            rows = connection.execute(
                "SELECT DISTINCT segment_path FROM segment_leases "
                "WHERE expires_at_unix > ?",
                (now,),
            ).fetchall()
            return True, {str(row[0]) for row in rows if row and row[0]}
        except (OSError, sqlite3.Error):
            return False, set()
        finally:
            if connection is not None:
                connection.close()

    def _cleanup_terminal_output(self, session: MatchSession, now: float) -> None:
        with session.lock:
            if not self._terminal_and_inactive(session):
                return
            self._mark_terminal(session, now)
            lifecycle = DiskLifecycleManager(
                session.output_dir,
                DiskLifecyclePolicy(
                    final_gif_retention_seconds=FINAL_GIF_RETENTION_SECONDS,
                ),
            )
            lifecycle.prune_final_gifs()
            if session.terminal_cleanup_done:
                return
            session.terminal_cleanup_last_attempt_at = now
            lease_check_ok, protected_paths = self._active_segment_leases(
                session.output_dir / "pipeline_state.sqlite3",
                now=now,
            )
            if not lease_check_ok or protected_paths:
                return
            summary = lifecycle.cleanup_finished_match(
                buffer_dir=session.output_dir / "buffer",
                manifest_path=session.output_dir / "buffer" / "segment_manifest.json",
                event_log_path=session.output_dir / "pipeline_events.jsonl",
                state_db_path=session.output_dir / "pipeline_state.sqlite3",
                protected_paths=protected_paths,
            )
            session.terminal_cleanup_done = summary.status == "completed"

    def _prune_expired_gifs(self) -> None:
        try:
            entries = list(os.scandir(DEFAULT_OUTPUT))
        except FileNotFoundError:
            return
        except OSError:
            return
        policy = DiskLifecyclePolicy(
            final_gif_retention_seconds=FINAL_GIF_RETENTION_SECONDS,
        )
        for entry in entries:
            try:
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                    continue
                validate_match_id(entry.name)
            except (OSError, ValueError):
                continue
            DiskLifecycleManager(Path(entry.path), policy).prune_final_gifs()

    @staticmethod
    def _latest_runtime_timestamp(log_path: Path) -> float | None:
        """Read the newest valid runtime record timestamp from a JSONL log."""
        try:
            records = _read_log(log_path, limit=80)
        except (OSError, UnicodeError):
            return None
        for record in records:
            try:
                timestamp = float(record.get("timestamp_unix"))
            except (TypeError, ValueError):
                continue
            if math.isfinite(timestamp) and timestamp > 0:
                return timestamp
        return None

    @staticmethod
    def _worker_process_is_alive(log_path: Path) -> bool | None:
        """Use the latest worker lifecycle record as a conservative guard."""
        try:
            records = _read_log(log_path, limit=600)
        except (OSError, UnicodeError):
            return None
        saw_runtime_signal = False
        for record in records:
            event = record.get("event")
            if event in {"worker_exited", "pipeline_stopped"}:
                return False
            if event == "runtime_heartbeat":
                saw_runtime_signal = True
                continue
            if event != "worker_started":
                continue
            try:
                pid = int(record.get("pid"))
            except (TypeError, ValueError):
                return None
            if pid <= 0:
                return None
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            except OSError:
                return False
            return True
        # Standalone pipeline runs do not emit Dashboard's worker_started
        # record. A stale heartbeat is still enough evidence for the grace
        # window above; a fresh heartbeat never reaches this method.
        return False if saw_runtime_signal else None

    def _prune_orphan_outputs(self, now: float) -> list[str]:
        """Clean stale match directories left after a dashboard/process crash.

        A directory is eligible only when it is not represented by an active
        Session, has no live segment lease, and its last structured runtime
        record is older than the grace period. Unknown or freshly running
        directories are left untouched for a later pass.
        """
        try:
            entries = list(os.scandir(DEFAULT_OUTPUT))
        except (FileNotFoundError, OSError):
            return []
        with self.lock:
            session_match_ids = set(self.sessions)
        policy = DiskLifecyclePolicy(
            final_gif_retention_seconds=FINAL_GIF_RETENTION_SECONDS,
        )
        cleaned: list[str] = []
        for entry in entries:
            try:
                if entry.is_symlink() or not entry.is_dir(follow_symlinks=False):
                    continue
                match_id = validate_match_id(entry.name)
                if match_id in session_match_ids:
                    continue
                output_dir = Path(entry.path)
                last_runtime_at = self._latest_runtime_timestamp(
                    output_dir / "pipeline_events.jsonl"
                )
                if (
                    last_runtime_at is None
                    or now - last_runtime_at < self.orphan_cleanup_grace_seconds
                ):
                    continue
                worker_alive = self._worker_process_is_alive(
                    output_dir / "pipeline_events.jsonl"
                )
                if worker_alive is not False:
                    continue
                lease_check_ok, protected_paths = self._active_segment_leases(
                    output_dir / "pipeline_state.sqlite3",
                    now=now,
                )
                if not lease_check_ok or protected_paths:
                    continue
                # Re-check under the Dashboard lock immediately before the
                # destructive pass. Holding the registry lock through cleanup
                # prevents a Session from being created between the check and
                # the final unlink operations.
                with self.lock:
                    if match_id in self.sessions:
                        continue
                    summary = DiskLifecycleManager(
                        output_dir,
                        policy,
                    ).cleanup_finished_match(
                        buffer_dir=output_dir / "buffer",
                        manifest_path=output_dir / "buffer" / "segment_manifest.json",
                        event_log_path=output_dir / "pipeline_events.jsonl",
                        state_db_path=output_dir / "pipeline_state.sqlite3",
                        protected_paths=protected_paths,
                    )
                if summary.status in {"completed", "completed_with_warnings"}:
                    cleaned.append(match_id)
            except (OSError, ValueError):
                continue
        return cleaned

    def run_maintenance(self, *, now: float | None = None) -> list[str]:
        timestamp = time.time() if now is None else now
        with self.lock:
            sessions = list(self.sessions.values())
        for session in sessions:
            try:
                self._cleanup_terminal_output(session, timestamp)
            except Exception as exc:
                try:
                    self._log_control(
                        session,
                        "terminal_cleanup_error",
                        error=str(exc),
                    )
                except OSError:
                    pass
        self._prune_expired_gifs()
        self._prune_orphan_outputs(timestamp)
        with self.lock:
            removed = self._prune_terminal_sessions_locked(timestamp)
            removed.extend(self._prune_idle_sessions_locked(timestamp))
            return removed

    def _maintenance_loop(self) -> None:
        while not self._maintenance_stop.wait(self.disk_cleanup_interval_seconds):
            self.run_maintenance()

    def close(self) -> None:
        self._maintenance_stop.set()

    @staticmethod
    def _process_group_exists(process_group: int) -> bool:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            if exc.errno == errno.EPERM:
                return True
            raise
        return True

    def _complete_worker_cleanup(
        self,
        session: MatchSession,
        process_group: int,
        *,
        forced: bool,
    ) -> None:
        if session.worker_process_group == process_group:
            session.worker_process_group = None
        session.worker_cleanup_process_group = None
        session.worker_cleanup_stage = None
        session.worker_cleanup_due_at = None
        session.worker_cleanup_failure = None
        self._log_control(
            session,
            "process_group_cleanup_complete",
            process_group=process_group,
            forced=forced,
        )

    @staticmethod
    def _reap_worker_leader(session: MatchSession, process_group: int) -> int | None:
        """Reap the Worker leader so a dead group is not mistaken for a zombie."""
        worker = session.worker
        if worker is None or worker.pid != process_group:
            return None
        return worker.poll()

    def _fail_worker_cleanup(
        self,
        session: MatchSession,
        process_group: int,
        error: str,
    ) -> None:
        session.worker_cleanup_stage = "failed"
        session.worker_cleanup_due_at = None
        session.worker_cleanup_failure = error
        self._log_control(
            session,
            "process_group_cleanup_failed",
            process_group=process_group,
            error=error,
        )

    def _advance_worker_cleanup(self, session: MatchSession, now: float) -> bool:
        process_group = session.worker_cleanup_process_group
        if process_group is None:
            return True
        self._reap_worker_leader(session, process_group)
        try:
            exists = self._process_group_exists(process_group)
        except OSError as exc:
            self._fail_worker_cleanup(
                session,
                process_group,
                f"无法确认进程组是否存在: {exc}",
            )
            return False
        if not exists:
            self._complete_worker_cleanup(
                session,
                process_group,
                forced=session.worker_cleanup_stage == "kill",
            )
            return True
        if session.worker_cleanup_stage == "failed":
            return False
        if (
            session.worker_cleanup_stage == "term"
            and session.worker_cleanup_due_at is not None
            and now >= session.worker_cleanup_due_at
        ):
            self._log_control(
                session,
                "process_group_term_timeout",
                process_group=process_group,
                grace_seconds=WORKER_TERM_GRACE_SECONDS,
            )
            try:
                os.killpg(process_group, signal.SIGKILL)
            except ProcessLookupError:
                self._complete_worker_cleanup(session, process_group, forced=False)
                return True
            except OSError as exc:
                if exc.errno == errno.ESRCH:
                    self._complete_worker_cleanup(session, process_group, forced=False)
                    return True
                self._fail_worker_cleanup(
                    session,
                    process_group,
                    f"SIGKILL 发送失败: {exc}",
                )
                return False
            session.worker_cleanup_stage = "kill"
            session.worker_cleanup_due_at = now + WORKER_KILL_GRACE_SECONDS
            self._log_control(
                session,
                "process_group_killed",
                process_group=process_group,
            )
            return False
        if (
            session.worker_cleanup_stage == "kill"
            and session.worker_cleanup_due_at is not None
            and now >= session.worker_cleanup_due_at
        ):
            self._fail_worker_cleanup(
                session,
                process_group,
                "SIGKILL 后进程组仍存在，已暂停 Worker 重启",
            )
            return False
        return False

    def _begin_worker_cleanup(
        self,
        session: MatchSession,
        process_group: int,
        now: float,
    ) -> bool:
        if session.worker_cleanup_process_group is not None:
            if session.worker_cleanup_process_group != process_group:
                self._fail_worker_cleanup(
                    session,
                    session.worker_cleanup_process_group,
                    "前一个 Worker 进程组尚未清理，拒绝覆盖清理目标",
                )
                return False
            return self._advance_worker_cleanup(session, now)
        if (
            process_group <= 1
            or process_group != session.worker_process_group
            or session.worker is None
            or session.worker.pid != process_group
            or process_group == os.getpgrp()
        ):
            session.worker_cleanup_process_group = process_group
            self._fail_worker_cleanup(
                session,
                process_group,
                "拒绝清理未记录、无效或属于 Dashboard 的进程组",
            )
            return False
        session.worker_cleanup_process_group = process_group
        session.worker_cleanup_stage = "term"
        session.worker_cleanup_due_at = now + WORKER_TERM_GRACE_SECONDS
        session.worker_cleanup_failure = None
        self._log_control(
            session,
            "process_group_stop_requested",
            process_group=process_group,
            signal="SIGTERM",
        )
        try:
            os.killpg(process_group, signal.SIGTERM)
        except ProcessLookupError:
            self._complete_worker_cleanup(session, process_group, forced=False)
            return True
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                self._complete_worker_cleanup(session, process_group, forced=False)
                return True
            self._fail_worker_cleanup(
                session,
                process_group,
                f"SIGTERM 发送失败: {exc}",
            )
            return False
        return self._advance_worker_cleanup(session, now)

    def _cleanup_worker_group_blocking(
        self,
        session: MatchSession,
        process_group: int,
    ) -> bool:
        if self._begin_worker_cleanup(session, process_group, time.time()):
            return True
        deadline = (
            time.monotonic()
            + WORKER_TERM_GRACE_SECONDS
            + WORKER_KILL_GRACE_SECONDS
            + 1
        )
        while (
            session.worker_cleanup_process_group is not None
            and session.worker_cleanup_stage != "failed"
            and time.monotonic() < deadline
        ):
            time.sleep(WORKER_CLEANUP_POLL_SECONDS)
            self._advance_worker_cleanup(session, time.time())
        if (
            session.worker_cleanup_process_group is not None
            and session.worker_cleanup_stage != "failed"
        ):
            self._fail_worker_cleanup(
                session,
                process_group,
                "等待进程组清理确认超时，已暂停 Worker 重启",
            )
        return session.worker_cleanup_process_group is None

    @staticmethod
    def _signal_worker_group(
        session: MatchSession,
        process_group: int,
        sig: signal.Signals,
    ) -> bool:
        """Signal only the recorded Worker group; a concurrent exit is success."""
        if (
            process_group <= 1
            or process_group != session.worker_process_group
            or session.worker is None
            or session.worker.pid != process_group
            or process_group == os.getpgrp()
        ):
            raise RuntimeError("拒绝向未记录、无效或属于 Dashboard 的进程组发送信号")
        try:
            os.killpg(process_group, sig)
        except ProcessLookupError:
            return False
        except OSError as exc:
            if exc.errno == errno.ESRCH:
                return False
            raise
        return True

    def _update_match_lifecycle(self, session: MatchSession, now: float) -> None:
        status = session.status()
        if session.lifecycle_state in {"finishing", *TERMINAL_LIFECYCLE_STATES}:
            return
        if status == "Played":
            session.played_confirmation_count += 1
            if session.played_confirmation_count >= PLAYED_CONFIRMATIONS_REQUIRED:
                self._begin_finishing(session, now)
            return

        session.played_confirmation_count = 0
        if status == "Playing":
            session.lifecycle_state = "playing"

    def _begin_finishing(self, session: MatchSession, now: float) -> None:
        if session.lifecycle_state == "finishing":
            return
        session.lifecycle_state = "finishing"
        session.played_confirmed_at = now
        session.finishing_started_at = now
        session.finishing_deadline = now + FINISHING_TIMEOUT_SECONDS
        session.finish_reason = "match_played"
        session.exit_reason = None
        session.finish_requested = False
        session.finish_timeout_signaled = False
        session.desired_running = False
        session.worker_restart_due_at = None
        session.source_changed = False
        session.source_change_message = None
        self._log_control(
            session,
            "match_finishing_started",
            status=session.status(),
            played_confirmations=session.played_confirmation_count,
            deadline_unix=session.finishing_deadline,
        )
        if session.worker_running():
            self._request_worker_finish(session)
        elif session.worker_cleanup_process_group is None:
            self._finalize_finishing(session)

    def _confirm_already_played_session(self, session: MatchSession, now: float) -> None:
        if session.lifecycle_state in {"finishing", *TERMINAL_LIFECYCLE_STATES}:
            return
        session.played_confirmation_count = PLAYED_CONFIRMATIONS_REQUIRED
        self._begin_finishing(session, now)

    def _request_worker_finish(self, session: MatchSession) -> None:
        if session.finish_requested or not session.worker_running():
            return
        assert session.worker is not None
        try:
            # SIGUSR1 reaches only the Python worker. FFmpeg keeps ingesting while
            # the worker performs its final event poll and drains pending GIFs.
            os.kill(session.worker.pid, signal.SIGUSR1)
        except OSError as exc:
            self._log_control(
                session,
                "worker_finish_request_failed",
                pid=session.worker.pid,
                error=str(exc),
            )
            return
        session.finish_requested = True
        self._log_control(
            session,
            "worker_finish_requested",
            pid=session.worker.pid,
            signal="SIGUSR1",
            reason=session.finish_reason,
        )

    def _signal_finish_timeout(self, session: MatchSession, now: float) -> None:
        if session.finish_timeout_signaled or not session.worker_running():
            return
        session.finish_timeout_signaled = True
        session.exit_reason = "match_played_finish_timeout"
        if session.worker_process_group is not None:
            self._begin_worker_cleanup(
                session,
                session.worker_process_group,
                now,
            )
        else:
            try:
                assert session.worker is not None
                session.worker.terminate()
            except OSError as exc:
                self._log_control(
                    session,
                    "worker_finish_timeout_signal_failed",
                    error=str(exc),
                )
        self._log_control(
            session,
            "worker_finish_timeout",
            deadline_unix=session.finishing_deadline,
        )

    def _finalize_finishing(self, session: MatchSession) -> None:
        report = session.output_report()
        try:
            report_started_at = float(report.get("started_at_unix") or 0)
        except (TypeError, ValueError):
            report_started_at = 0.0
        if (
            report
            and session.worker_started_at is not None
            and report_started_at < session.worker_started_at - 1
        ):
            report = {}
        reported_state = str(report.get("completion_state") or "")
        reported_reason = str(
            report.get("exit_reason") or report.get("stop_reason") or ""
        )
        return_code = session.worker.poll() if session.worker is not None else None

        if reported_state in {"completed", "completed_with_warnings"}:
            lifecycle_state = reported_state
        elif session.exit_reason == "match_played_finish_timeout":
            lifecycle_state = "completed_with_warnings"
        elif reported_reason == "match_played_stream_incomplete":
            lifecycle_state = "completed_with_warnings"
        elif session.worker is None:
            lifecycle_state = "completed"
        elif return_code == 0 and reported_reason == "match_played":
            lifecycle_state = "completed"
        else:
            lifecycle_state = "completed_with_warnings"

        session.lifecycle_state = lifecycle_state
        self._mark_terminal(session)
        session.finishing_deadline = None
        session.exit_reason = (
            session.exit_reason or reported_reason or session.finish_reason or "match_played"
        )
        self._log_control(
            session,
            "match_finishing_completed",
            lifecycle_state=lifecycle_state,
            exit_reason=session.exit_reason,
            worker_return_code=return_code,
        )
        self._release_worker_slot(session, force=True)

    def refresh(self, session: MatchSession) -> None:
        now = time.time()
        with session.lock:
            if session.match_id.startswith("demo-"):
                if session.worker is not None and session.worker.poll() is not None:
                    if session.worker_exit_logged_pid != session.worker.pid:
                        session.worker_exit_logged_pid = session.worker.pid
                        self._log_control(
                            session,
                            "worker_exited",
                            pid=session.worker.pid,
                            return_code=session.worker.returncode,
                            runtime_seconds=(
                                round(max(0.0, now - session.worker_started_at), 3)
                                if session.worker_started_at is not None
                                else 0.0
                            ),
                        )
                        session.desired_running = False
                        session.worker_restart_due_at = None
                        if session.worker_process_group is not None:
                            self._begin_worker_cleanup(
                                session,
                                session.worker_process_group,
                                now,
                            )
                    self._advance_worker_cleanup(session, now)
                    if session.worker_cleanup_process_group is None:
                        session.lifecycle_state = (
                            "completed" if session.worker.returncode == 0 else "failed"
                        )
                        self._mark_terminal(session, now)
                        self._release_worker_slot(session, force=True)
                    elif session.worker_cleanup_failure:
                        session.lifecycle_state = "failed"
                    else:
                        session.lifecycle_state = "stopping"
                return
            if (
                session.worker is not None
                and session.worker.poll() is not None
                and session.worker_exit_logged_pid != session.worker.pid
            ):
                runtime_seconds = (
                    max(0.0, now - session.worker_started_at)
                    if session.worker_started_at
                    else 0.0
                )
                session.worker_exit_logged_pid = session.worker.pid
                if session.worker_process_group is not None:
                    self._begin_worker_cleanup(
                        session,
                        session.worker_process_group,
                        now,
                    )
                self._log_control(
                    session,
                    "worker_exited",
                    pid=session.worker.pid,
                    return_code=session.worker.returncode,
                    runtime_seconds=round(runtime_seconds, 3),
                )
                if (
                    session.lifecycle_state == "finishing"
                    and session.worker_cleanup_process_group is None
                ):
                    self._finalize_finishing(session)
                elif session.desired_running and session.worker_mode == "live":
                    if runtime_seconds >= 30:
                        session.worker_consecutive_failures = 0
                    session.worker_consecutive_failures += 1
                    delay = min(
                        2 ** min(session.worker_consecutive_failures - 1, 5), 30
                    )
                    session.worker_restart_due_at = now + delay
                    self._log_control(
                        session,
                        "worker_restart_scheduled",
                        previous_pid=session.worker.pid,
                        delay_seconds=delay,
                        consecutive_failures=session.worker_consecutive_failures,
                    )
            lifecycle_active = session.lifecycle_state not in {
                "finishing",
                *TERMINAL_LIFECYCLE_STATES,
            }
            if lifecycle_active and (
                session.last_detail_poll is None
                or now - session.last_detail_poll >= session.detail_poll_seconds
            ):
                try:
                    response = query_detail(session.match_id)
                    session.detail = dict(response.get("matchSample") or response)
                    session.detail_error = None
                    self._update_match_lifecycle(session, now)
                except Exception as exc:
                    session.detail_error = str(exc)
                session.last_detail_poll = now
            if session.lifecycle_state not in {
                "finishing",
                *TERMINAL_LIFECYCLE_STATES,
            } and (
                session.last_source_poll is None
                or now - session.last_source_poll >= session.source_poll_seconds
            ):
                try:
                    response = query_source(session.match_id)
                    data = response.get("data") if isinstance(response, dict) else {}
                    data = data if isinstance(data, dict) else {}
                    previous_resource = session.source.get("resource")
                    previous_updated = session.source.get("updated_at")
                    if previous_resource and not data.get("resource"):
                        session.source_error = "直播源接口当前返回无数据，继续使用上一次有效 resource"
                    else:
                        session.source = data
                        session.source_error = None
                    if previous_resource and data.get("resource") and (
                        previous_resource != data["resource"]
                        or previous_updated != data.get("updated_at")
                    ):
                        session.source_changed = True
                        session.source_change_message = "直播源 resource 或 updated_at 已变化，需切换并重新确认缓冲"
                except Exception as exc:
                    session.source_error = str(exc)
                session.last_source_poll = now
            if (
                session.lifecycle_state not in {"finishing", *TERMINAL_LIFECYCLE_STATES}
                and not session.worker_running()
                and (
                    session.last_event_poll is None
                    or now - session.last_event_poll >= session.event_poll_seconds
                )
            ):
                try:
                    session.event_payload = query_events(session.match_id)
                    session.event_error = None
                except Exception as exc:
                    session.event_error = str(exc)
                session.last_event_poll = now

            if (
                session.source_changed
                and session.lifecycle_state not in {"finishing", *TERMINAL_LIFECYCLE_STATES}
                and session.worker_running()
                and session.worker_mode == "live"
                and session.source.get("resource")
                and session.worker_cleanup_process_group is None
            ):
                self._log_control(
                    session,
                    "live_source_changed",
                    message=session.source_change_message,
                    resource=session.source.get("resource"),
                    updated_at=session.source.get("updated_at"),
                )
                try:
                    self.stop(session, keep_desired=True)
                    if session.worker_cleanup_process_group is None:
                        self.start(session, recovery=True)
                except Exception as exc:
                    session.worker_consecutive_failures += 1
                    delay = min(
                        2 ** min(session.worker_consecutive_failures - 1, 5), 30
                    )
                    session.worker_restart_due_at = now + delay
                    self._log_control(
                        session,
                        "live_source_restart_failed",
                        error=str(exc),
                        delay_seconds=delay,
                        consecutive_failures=session.worker_consecutive_failures,
                    )

            self._advance_worker_cleanup(session, now)

            if (
                session.desired_running
                and session.lifecycle_state not in {"finishing", *TERMINAL_LIFECYCLE_STATES}
                and session.worker_mode == "live"
                and not session.worker_running()
                and session.worker_cleanup_process_group is None
                and session.worker_restart_due_at is not None
                and now >= session.worker_restart_due_at
            ):
                session.worker_restart_due_at = None
                try:
                    self.start(session, recovery=True)
                except Exception as exc:
                    session.worker_consecutive_failures += 1
                    delay = min(
                        2 ** min(session.worker_consecutive_failures - 1, 5), 30
                    )
                    session.worker_restart_due_at = time.time() + delay
                    self._log_control(
                        session,
                        "worker_restart_failed",
                        error=str(exc),
                        delay_seconds=delay,
                        consecutive_failures=session.worker_consecutive_failures,
                    )

            if session.lifecycle_state == "finishing":
                if session.worker_running():
                    self._request_worker_finish(session)
                    if (
                        session.finishing_deadline is not None
                        and now >= session.finishing_deadline
                    ):
                        self._signal_finish_timeout(session, now)
                elif session.worker_cleanup_process_group is None:
                    self._finalize_finishing(session)
            self._release_worker_slot(session)

    @staticmethod
    def _log_control(session: MatchSession, event: str, **fields: Any) -> None:
        session.output_dir.mkdir(parents=True, exist_ok=True)
        record = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "timestamp_unix": time.time(),
            "event": event,
            "match_id": session.match_id,
            **fields,
        }
        with (session.output_dir / "pipeline_events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")

    def start(
        self,
        session: MatchSession,
        *,
        demo: bool = False,
        recovery: bool = False,
    ) -> None:
        with session.lock:
            if session.worker_running():
                raise RuntimeError(f"比赛 {session.match_id} 的 Worker 已在运行")
            if session.worker_cleanup_process_group is not None:
                raise RuntimeError(
                    "前一个 Worker 进程组尚未确认清理，拒绝启动新的 Worker"
                )
            if not recovery and (
                session.desired_running
                or session.lifecycle_state in {"starting", "finishing", "stopping"}
            ):
                raise RuntimeError(
                    f"比赛 {session.match_id} 已处于 {session.lifecycle_state} 状态，"
                    "不能重复启动"
                )
            self._assert_worker_slot_available(session)
            if session.worker_process_group is not None:
                if not self._cleanup_worker_group_blocking(
                    session,
                    session.worker_process_group,
                ):
                    raise RuntimeError(
                        session.worker_cleanup_failure
                        or "前一个 Worker 进程组尚未确认清理，拒绝启动新的 Worker"
                    )
            if not demo:
                if session.lifecycle_state in {
                    "finishing",
                    "completed",
                    "completed_with_warnings",
                }:
                    raise RuntimeError("比赛已结束，不能重新启动实时处理")
                if not recovery and session.status() == "Played":
                    self._confirm_already_played_session(session, time.time())
                    raise RuntimeError("比赛已结束，不能重新启动实时处理")
            session.output_dir.mkdir(parents=True, exist_ok=True)
            source = str(session.source.get("resource") or "")
            if demo:
                source = str(ROOT / "downloads" / "SV Wehen Wiesbaden vs FC Bayern Munchen [zqJI-83XFhM].mp4")
                command = [
                    sys.executable, str(ROOT / "event_driven_pipeline.py"), source,
                    "--simulate-live", "--replay-speed", "4", "--start", "1037", "--duration", "95",
                    "--match-id", session.match_id, "--replay-events",
                    str(ROOT / "mock_events" / "api_snapshot_scenario.json"),
                    "--before", str(session.before_seconds), "--after", str(session.after_seconds),
                    "--event-to-video-offset", str(session.event_to_video_offset_seconds),
                    "--buffer-seconds", str(DASHBOARD_BUFFER_SECONDS),
                    "--event-poll-seconds", str(session.event_poll_seconds),
                    "--shotmap-poll-seconds", str(session.shotmap_poll_seconds),
                    "--shotmap-offset", str(session.shotmap_offset_seconds),
                    "--gif-width", str(session.gif_width), "--gif-fps", str(session.gif_fps),
                    "--gif-colors", str(session.gif_colors), "--output-dir", str(session.output_dir),
                    "--graceful-stop-grace-seconds", str(WORKER_FINISH_GRACE_SECONDS),
                    "--graceful-stop-timeout-seconds", str(WORKER_FINISH_TIMEOUT_SECONDS),
                ]
            else:
                if not source:
                    raise RuntimeError("尚未获取到可用的 RTMP resource")
                command = [
                    sys.executable, str(ROOT / "event_driven_pipeline.py"), source,
                    "--match-id", session.match_id,
                    "--event-url", DEFAULT_EVENT_URL,
                    "--event-user", _user(),
                    "--event-poll-seconds", str(session.event_poll_seconds),
                    "--shotmap-poll-seconds", str(session.shotmap_poll_seconds),
                    "--shotmap-offset", str(session.shotmap_offset_seconds),
                    "--before", str(session.before_seconds), "--after", str(session.after_seconds),
                    "--event-to-video-offset", str(session.event_to_video_offset_seconds),
                    "--buffer-seconds", str(DASHBOARD_BUFFER_SECONDS),
                    "--gif-width", str(session.gif_width), "--gif-fps", str(session.gif_fps),
                    "--gif-colors", str(session.gif_colors), "--output-dir", str(session.output_dir),
                    "--graceful-stop-grace-seconds", str(WORKER_FINISH_GRACE_SECONDS),
                    "--graceful-stop-timeout-seconds", str(WORKER_FINISH_TIMEOUT_SECONDS),
                ]
            match_start_play = _usable_match_start_play(session.detail)
            if match_start_play is not None:
                command.extend([
                    "--match-start-play", match_start_play,
                    "--match-start-naive-timezone", "utc",
                ])
            if session.vision_enabled:
                command.extend([
                    "--vision-enabled",
                    "--vision-search-before", str(VISION_SEARCH_BEFORE_SECONDS),
                    "--vision-search-after", str(VISION_SEARCH_AFTER_SECONDS),
                    "--vision-before", str(session.vision_before_seconds),
                    "--vision-after", str(session.vision_after_seconds),
                    "--vision-workers", str(VISION_WORKERS),
                    "--ocr-timeout-seconds", str(OCR_TIMEOUT_SECONDS),
                    "--fallback-gif-width", str(FALLBACK_GIF_WIDTH),
                    "--fallback-gif-fps", str(FALLBACK_GIF_FPS),
                    "--fallback-gif-colors", str(FALLBACK_GIF_COLORS),
                ])
                if session.vision_clock_only:
                    command.append("--ocr-clock-only")
                if session.tdeed_enabled:
                    command.append("--tdeed-enabled")
                configured_profile = session.scoreboard_profile_path
                if demo and not configured_profile:
                    configured_profile = str(DEFAULT_DEMO_SCOREBOARD_PROFILE)
                if configured_profile:
                    profile_path = Path(
                        configured_profile
                    ).expanduser().resolve()
                    if not profile_path.is_file():
                        raise RuntimeError(
                            f"记分牌 profile 文件不存在: {profile_path}"
                        )
                    command.extend(["--scoreboard-profile", str(profile_path)])
            session.worker_command = command
            session.worker_mode = "demo" if demo else "live"
            previous_lifecycle_state = session.lifecycle_state
            session.lifecycle_state = "starting"
            try:
                self._claim_worker_slot(session)
                session.worker = subprocess.Popen(
                    command,
                    cwd=ROOT,
                    text=True,
                    start_new_session=True,
                )
            except Exception:
                session.lifecycle_state = previous_lifecycle_state
                self._release_worker_slot(session, force=True)
                raise
            session.worker_process_group = session.worker.pid
            session.desired_running = True
            session.worker_started_at = time.time()
            session.worker_exit_logged_pid = None
            session.worker_restart_due_at = None
            session.worker_cleanup_failure = None
            session.terminal_at = None
            session.terminal_cleanup_done = False
            session.terminal_cleanup_last_attempt_at = None
            session.worker_restart_count += int(recovery)
            session.finish_requested = False
            session.finish_timeout_signaled = False
            session.finishing_started_at = None
            session.finishing_deadline = None
            session.finish_reason = None
            session.exit_reason = None
            if not recovery:
                session.worker_restart_count = 0
                session.worker_consecutive_failures = 0
                session.played_confirmation_count = 0
                session.played_confirmed_at = None
            session.lifecycle_state = "playing"
            session.source_changed = False
            session.source_change_message = None
            self._log_control(
                session,
                "worker_started",
                mode=session.worker_mode,
                pid=session.worker.pid,
                recovery=recovery,
                restart_count=session.worker_restart_count,
            )
        self._ensure_monitor(session)

    def stop(self, session: MatchSession, *, keep_desired: bool = False) -> None:
        with session.lock:
            if not keep_desired:
                session.desired_running = False
                session.worker_restart_due_at = None
                session.lifecycle_state = "stopping"
                session.finishing_deadline = None
                session.finish_reason = "manual_stop"
                session.exit_reason = "manual_stop"
            try:
                if not session.worker_running():
                    process_group = (
                        session.worker_cleanup_process_group
                        or session.worker_process_group
                    )
                    if process_group is not None and not self._cleanup_worker_group_blocking(
                        session,
                        process_group,
                    ):
                        raise RuntimeError(
                            session.worker_cleanup_failure
                            or "Worker 进程组未能完成清理"
                        )
                    if not keep_desired:
                        session.lifecycle_state = "stopped"
                        self._mark_terminal(session)
                        self._release_worker_slot(session, force=True)
                    return
                assert session.worker is not None
                worker = session.worker
                process_group = session.worker_process_group
                if process_group is not None:
                    self._signal_worker_group(session, process_group, signal.SIGINT)
                else:
                    try:
                        worker.send_signal(signal.SIGINT)
                    except ProcessLookupError:
                        pass
                try:
                    worker.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    if process_group is not None:
                        if not self._cleanup_worker_group_blocking(
                            session,
                            process_group,
                        ):
                            raise RuntimeError(
                                session.worker_cleanup_failure
                                or "Worker 进程组未能完成清理"
                            )
                    else:
                        worker.terminate()
                        try:
                            worker.wait(timeout=WORKER_TERM_GRACE_SECONDS)
                        except subprocess.TimeoutExpired:
                            worker.kill()
                            worker.wait(timeout=WORKER_KILL_GRACE_SECONDS)
                else:
                    if process_group is not None and not self._cleanup_worker_group_blocking(
                        session,
                        process_group,
                    ):
                        raise RuntimeError(
                            session.worker_cleanup_failure
                            or "Worker 进程组未能完成清理"
                        )
                if worker.returncode is None:
                    worker.wait(timeout=WORKER_KILL_GRACE_SECONDS)
                self._log_control(session, "worker_stopped", return_code=worker.returncode)
            except Exception:
                if not keep_desired:
                    session.lifecycle_state = "failed"
                raise
            if not keep_desired:
                session.lifecycle_state = "stopped"
                self._mark_terminal(session)
                self._release_worker_slot(session, force=True)


dashboard = Dashboard()
match_catalog = MatchCatalog()
heavy_task_monitor = HeavyTaskCoordinator.from_environment()
open_platform_client = OpenPlatformClient(
    OpenPlatformConfig.from_environment(ROOT)
)
article_publisher = ArticlePublisher(
    platform_client=open_platform_client,
    gif_store=PublishedGifStore(
        Path(
            os.environ.get(
                "ARTICLE_PUBLISH_GIF_DIR", str(DEFAULT_PUBLISHED_GIF_DIR)
            )
        ),
        os.environ.get(
            "GIF_PUBLIC_ORIGIN", "https://matchgif.aisportsapp.com"
        ),
        max_bytes=_positive_environment_integer(
            "ARTICLE_PUBLISH_GIF_MAX_BYTES", 50 * 1024 * 1024
        ),
    ),
    database_path=Path(
        os.environ.get(
            "ARTICLE_PUBLISH_DB_PATH", str(DEFAULT_ARTICLE_PUBLISH_DATABASE)
        )
    ),
    verify_public_url=environment_boolean(
        "ARTICLE_PUBLISH_VERIFY_PUBLIC_URL", True
    ),
)
app = Flask(__name__, static_folder="dashboard_static", static_url_path="/static")


def _heavy_task_status() -> dict[str, Any]:
    """Expose scheduler evidence without leaking local coordinator details."""
    try:
        snapshot = heavy_task_monitor.snapshot()
    except HeavyTaskCoordinatorError as exc:
        return {
            "state": "error",
            "error": str(exc),
            "total_slots": heavy_task_monitor.max_heavy_tasks,
            "vision_slots": heavy_task_monitor.max_vision_tasks,
            "occupied": 0,
            "vision_active": 0,
            "queued": 0,
            "oldest_wait_seconds": 0.0,
            "active_items": [],
            "waiting_items": [],
        }

    now = time.time()
    active_items = [
        {
            key: item.get(key)
            for key in (
                "task_kind",
                "match_id",
                "event_key",
                "acquired_at_unix",
                "heartbeat_at_unix",
            )
        }
        for item in snapshot["active"]["items"]
    ]
    waiting_items = []
    for item in snapshot["waiting"]["items"]:
        requested_at = float(item.get("requested_at_unix") or now)
        waiting_items.append(
            {
                **{
                    key: item.get(key)
                    for key in (
                        "task_kind",
                        "match_id",
                        "event_key",
                        "requested_at_unix",
                        "heartbeat_at_unix",
                    )
                },
                "wait_seconds": round(max(0.0, now - requested_at), 3),
            }
        )
    oldest_wait = max(
        (float(item["wait_seconds"]) for item in waiting_items),
        default=0.0,
    )
    return {
        "state": "healthy",
        "error": None,
        "updated_at_unix": snapshot["updated_at_unix"],
        "total_slots": snapshot["limits"]["heavy"],
        "vision_slots": snapshot["limits"]["vision"],
        "occupied": snapshot["active"]["heavy"],
        "vision_active": snapshot["active"]["vision"],
        "queued": snapshot["waiting"]["tasks"],
        "oldest_wait_seconds": round(oldest_wait, 3),
        "active_items": active_items,
        "waiting_items": waiting_items,
    }


@app.get("/")
def index():
    return send_from_directory(ROOT / "dashboard_static", "index.html")


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "port": PORT, "time_unix": time.time()})


@app.get("/api/article-publish/status")
def article_publish_status():
    return jsonify({"ok": True, **article_publisher.status()})


@app.get("/api/open-platform/oauth/start")
def open_platform_oauth_start():
    try:
        return redirect(open_platform_client.authorization_url(), code=302)
    except OpenPlatformError as exc:
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
                "stage": "authorization",
                "code": exc.code or "open_platform_not_configured",
            }
        ), exc.status_code


@app.get("/api/open-platform/oauth/callback")
def open_platform_oauth_callback():
    try:
        open_platform_client.exchange_oauth_code(
            str(request.args.get("code") or ""),
            str(request.args.get("state") or ""),
            error=str(request.args.get("error") or ""),
            error_description=str(request.args.get("error_description") or ""),
        )
    except OpenPlatformError as exc:
        return (
            "<!doctype html><meta charset='utf-8'>"
            "<title>开放平台授权失败</title>"
            f"<h1>授权失败</h1><p>{html.escape(str(exc))}</p>"
            "<p><a href='/'>返回 GIF 控制台</a></p>",
            exc.status_code,
            {"Content-Type": "text/html; charset=utf-8"},
        )
    return redirect("/?open_platform=authorized", code=302)


@app.get("/api/matches")
def matches_catalog():
    payload = match_catalog.snapshot()
    worker_slot = dashboard.worker_slot_status()
    payload.update(worker_slot)
    payload["heavy_tasks"] = _heavy_task_status()
    # Compatibility name consumed by the current dashboard UI.
    payload["selection_locked"] = worker_slot["locked"]
    return jsonify(payload)


@app.get("/api/session")
def session_view():
    try:
        session = dashboard.get(request.args.get("match_id", "demo-match-54154533"))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(_session_json(session))


@app.post("/api/session")
def session_configure():
    body = request.get_json(silent=True) or {}
    try:
        session = dashboard.get(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    with session.lock:
        for key, field_name, cast in (
            ("event_poll_seconds", "event_poll_seconds", float),
            ("shotmap_poll_seconds", "shotmap_poll_seconds", float),
            ("source_poll_seconds", "source_poll_seconds", float),
            ("detail_poll_seconds", "detail_poll_seconds", float),
            ("before_seconds", "before_seconds", float),
            ("after_seconds", "after_seconds", float),
            ("gif_width", "gif_width", int),
            ("gif_fps", "gif_fps", float),
            ("gif_colors", "gif_colors", int),
            ("vision_before_seconds", "vision_before_seconds", float),
            ("vision_after_seconds", "vision_after_seconds", float),
        ):
            if key in body:
                value = cast(body[key])
                if value <= 0:
                    return jsonify({"error": f"{field_name} 必须大于 0"}), 400
                setattr(session, field_name, value)
        if "event_to_video_offset_seconds" in body:
            value = float(body["event_to_video_offset_seconds"])
            if not math.isfinite(value):
                return jsonify({"error": "event_to_video_offset_seconds 必须是有限数字"}), 400
            session.event_to_video_offset_seconds = value
        if "shotmap_offset_seconds" in body:
            value = float(body["shotmap_offset_seconds"])
            if not math.isfinite(value):
                return jsonify({"error": "shotmap_offset_seconds 必须是有限数字"}), 400
            session.shotmap_offset_seconds = value
        if "vision_enabled" in body:
            session.vision_enabled = bool(body["vision_enabled"])
        if "tdeed_enabled" in body:
            value = body["tdeed_enabled"]
            if not isinstance(value, bool):
                return jsonify({"error": "tdeed_enabled 必须是 JSON 布尔值"}), 400
            session.tdeed_enabled = value
        if "vision_clock_only" in body:
            value = body["vision_clock_only"]
            if not isinstance(value, bool):
                return jsonify({"error": "vision_clock_only 必须是 JSON 布尔值"}), 400
            session.vision_clock_only = value
        if "scoreboard_profile_path" in body:
            profile_path = str(body["scoreboard_profile_path"] or "").strip()
            if profile_path and not Path(profile_path).expanduser().is_file():
                return jsonify({"error": "scoreboard_profile_path 文件不存在"}), 400
            session.scoreboard_profile_path = profile_path
    return jsonify(_session_json(session))


@app.post("/api/session/start")
def session_start():
    body = request.get_json(silent=True) or {}
    try:
        session = dashboard.get(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        if not bool(body.get("demo")) and not session.source.get("resource"):
            dashboard.refresh(session)
        dashboard.start(session, demo=bool(body.get("demo")))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 409
    return jsonify(_session_json(session))


@app.post("/api/session/stop")
def session_stop():
    body = request.get_json(silent=True) or {}
    try:
        session = dashboard.get(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        dashboard.stop(session)
    except Exception as exc:
        return jsonify({"error": str(exc), "session": _session_json(session)}), 409
    return jsonify(_session_json(session))


def _default_gif_source_path(match_id: str, event: dict[str, Any]) -> Path:
    output = str(event.get("output") or "").strip()
    if not output:
        raise ArticlePublishError(
            "事件记录没有默认 GIF 文件路径",
            code="default_gif_path_missing",
            stage="request_validation",
            status_code=409,
        )
    match_directory = (DEFAULT_OUTPUT / match_id).resolve()
    raw_path = Path(output).expanduser()
    candidates = [raw_path] if raw_path.is_absolute() else [ROOT / raw_path, match_directory / raw_path]
    for candidate in candidates:
        resolved = candidate.resolve()
        if (
            resolved.suffix.lower() == ".gif"
            and resolved.is_relative_to(match_directory)
            and resolved.is_file()
        ):
            return resolved
    raise ArticlePublishError(
        "默认 GIF 文件不存在，或文件不属于当前比赛目录",
        code="default_gif_not_found",
        stage="gif_validation",
        status_code=404,
    )


@app.post("/api/article-publish")
def article_publish():
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
        event_key = str(body.get("event_key") or "").strip()
        if not event_key:
            raise ArticlePublishError(
                "请求缺少 event_key",
                code="publish_event_key_missing",
                stage="request_validation",
                status_code=400,
            )
        session = dashboard.get(match_id)
        session_payload = _session_json(session)
        event = next(
            (
                item
                for item in session_payload.get("events", [])
                if str(item.get("event_key") or "") == event_key
            ),
            None,
        )
        if not event:
            raise ArticlePublishError(
                "没有找到对应事件，请刷新页面后重试",
                code="publish_event_not_found",
                stage="request_validation",
                status_code=404,
            )
        result = article_publisher.publish(
            match_id=match_id,
            event=event,
            match_detail=session_payload.get("detail") or {},
            source_path=_default_gif_source_path(match_id, event),
        )
        return jsonify({"ok": True, "publish": result})
    except ValueError as exc:
        return jsonify(
            {
                "ok": False,
                "error": str(exc),
                "code": "publish_request_invalid",
                "stage": "request_validation",
            }
        ), 400
    except ArticlePublishError as exc:
        payload = {
            "ok": False,
            "error": str(exc),
            "code": exc.code,
            "stage": exc.stage,
            "auth_required": exc.auth_required,
            "retriable": exc.retriable,
        }
        if exc.auth_required:
            payload["oauth_url"] = "/api/open-platform/oauth/start"
        return jsonify(payload), exc.status_code
    except Exception as exc:
        return jsonify(
            {
                "ok": False,
                "error": f"发布服务内部异常：{exc}",
                "code": "publish_internal_error",
                "stage": "internal",
            }
        ), 500


@app.get("/publish-gifs/<gif_id>.gif")
def published_gif_file(gif_id: str):
    if not re.fullmatch(r"[a-f0-9]{64}", gif_id):
        abort(404)
    path = article_publisher.gif_store.path_for(gif_id)
    if not path.is_file():
        abort(404)
    return send_from_directory(path.parent, path.name, mimetype="image/gif")


@app.get("/api/gif/<match_id>/<path:filename>")
def gif_file(match_id: str, filename: str):
    try:
        validate_match_id(match_id)
    except ValueError:
        abort(404)
    if Path(filename).name != filename or Path(filename).suffix.lower() != ".gif":
        abort(404)
    output_dir = DEFAULT_OUTPUT / match_id
    return send_from_directory(output_dir, filename)


if __name__ == "__main__":
    print(f"Football GIF dashboard: http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
