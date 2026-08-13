#!/usr/bin/env python3
"""Small Flask control plane for the football GIF pipeline.

The dashboard deliberately keeps network credentials on the server side. It
provides a read-only view of the three match APIs and starts the existing
event-driven worker only after a live source has been found.
"""

from __future__ import annotations

import errno
import json
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
from pathlib import Path
from typing import Any

from flask import Flask, abort, jsonify, request, send_from_directory

from match_event_identity import events_represent_same_incident


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
DEFAULT_OUTPUT = ROOT / "output_gifs" / "dashboard"
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
WORKER_FINISH_TIMEOUT_SECONDS = 120.0
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
    source_poll_seconds: float = 10.0
    detail_poll_seconds: float = 10.0
    before_seconds: float = 30.0
    after_seconds: float = 20.0
    gif_width: int = 384
    gif_fps: float = 6.0
    gif_colors: int = 160
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
    status = payload.get("status")
    if status not in (0, "0"):
        raise ValueError(f"事件接口返回异常状态 status={status!r}")
    events = payload.get("events")
    if events == []:
        payload["events"] = {}
    elif not isinstance(events, dict):
        raise ValueError("事件接口返回缺少有效的 events 对象")
    return payload


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
                if not isinstance(event, dict) or event.get("code") not in {"G", "OG", "YC", "RC"}:
                    continue
                code = str(event.get("code"))
                result.append(
                    {
                        "code": code,
                        "label": {"G": "进球", "OG": "乌龙球", "YC": "黄牌", "RC": "红牌"}[code],
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
            rows = connection.execute(
                """
                SELECT event_key, code, event_type, event_json, status,
                       discovered_at_unix, updated_at_unix, output_path,
                       output_bytes, result_json, error, """
                + suppression_field
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
                "seconds_after_event_observed": result.get(
                    "seconds_after_event_observed"
                ),
                "error": row["error"],
            }
        )
    aliases = {
        str(row["version_key"]): str(row["canonical_key"])
        for row in alias_rows
    }
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


def _latest_ingest_message(output_dir: Path) -> str | None:
    try:
        paths = list(output_dir.glob("ingest_ffmpeg_*.log"))
        latest = max(paths, key=lambda path: path.stat().st_mtime)
        lines = [
            line.strip()
            for line in latest.read_text(encoding="utf-8", errors="replace").splitlines()
            if line.strip()
        ]
    except (ValueError, OSError):
        return None
    return lines[-1] if lines else None


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
        if not started_at or float(row.get("timestamp_unix") or 0) >= started_at - 1
    ]
    heartbeat = next(
        (row for row in current_log_rows if row.get("event") == "runtime_heartbeat"),
        {},
    )

    report_started_at = float(report.get("started_at_unix") or 0)
    report_is_current = bool(report) and (
        started_at is None or report_started_at >= started_at - 1
    )
    current_report = report if report_is_current else {}
    event_source = current_report.get("event_source") or {}

    segment_count = 0
    latest_segment_unix: float | None = None
    try:
        for path in (session.output_dir / "buffer").glob("*.ts"):
            modified = path.stat().st_mtime
            if started_at is not None and modified < started_at - 2:
                continue
            segment_count += 1
            latest_segment_unix = max(latest_segment_unix or modified, modified)
    except OSError:
        pass

    heartbeat_unix = float(heartbeat.get("timestamp_unix") or 0) or None
    heartbeat_age = max(0.0, now - heartbeat_unix) if heartbeat_unix else None
    segment_age = (
        max(0.0, now - latest_segment_unix) if latest_segment_unix else None
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
        evidence_fresh = (
            heartbeat_age is not None
            and heartbeat_age <= 9
            and segment_age is not None
            and segment_age <= 9
        )
        if evidence_fresh and not last_event_error:
            state, label = "healthy", "实时链路正常"
        elif elapsed is not None and elapsed < 12 and heartbeat_age is None:
            state, label = "starting", "正在建立直播缓存"
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

    exit_message = None
    if not worker_running and (started_at or current_report):
        if stopped_by_user:
            exit_message = "用户停止"
        elif ffmpeg_return_code is not None:
            exit_message = f"FFmpeg 返回码 {ffmpeg_return_code}"
        exit_message = _latest_ingest_message(session.output_dir) or exit_message
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
        "stream_time_seconds": heartbeat.get("stream_time_sec"),
        "buffer_segment_count": int(heartbeat.get("buffer_segment_count") or segment_count),
        "buffer_coverage_seconds": heartbeat.get("buffer_coverage_seconds"),
        "latest_segment_unix": latest_segment_unix,
        "latest_segment_age_seconds": round(segment_age, 1) if segment_age is not None else None,
        "event_poll_count": event_poll_count,
        "event_error_count": event_error_count,
        "last_event_error": last_event_error,
        "task_counts": task_counts,
        "ffmpeg_return_code": ffmpeg_return_code,
        "ingest_restart_count": (current_report.get("runtime") or {}).get("ingest_restart_count", 0),
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
            "source_seconds": session.source_poll_seconds,
            "detail_seconds": session.detail_poll_seconds,
        },
        "gif": {
            "before_seconds": session.before_seconds,
            "after_seconds": session.after_seconds,
            "width": session.gif_width,
            "fps": session.gif_fps,
            "colors": session.gif_colors,
            "size_reference_mb": 10,
            "adaptive_quality_reduction": False,
        },
        "events": tasks,
        "event_counts": event_counts,
        "logs": _read_log(session.output_dir / "pipeline_events.jsonl"),
        "report": report,
        "telemetry": telemetry,
    }


class Dashboard:
    def __init__(self, *, background_monitors: bool = True) -> None:
        self.sessions: dict[str, MatchSession] = {}
        self.lock = threading.RLock()
        self.background_monitors = background_monitors
        self.monitor_threads: dict[str, threading.Thread] = {}

    def get(self, match_id: str) -> MatchSession:
        match_id = validate_match_id(match_id)
        with self.lock:
            session = self.sessions.setdefault(
                match_id,
                MatchSession(match_id=match_id, output_dir=DEFAULT_OUTPUT / match_id),
            )
            if self.background_monitors and match_id not in self.monitor_threads:
                thread = threading.Thread(
                    target=self._monitor,
                    args=(session,),
                    name=f"match-monitor-{match_id}",
                    daemon=True,
                )
                self.monitor_threads[match_id] = thread
                thread.start()
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

    def _monitor(self, session: MatchSession) -> None:
        while True:
            try:
                self.refresh(session)
            except Exception as exc:
                self._log_control(session, "monitor_error", error=str(exc))
            time.sleep(0.5)

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
        else:
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

    def refresh(self, session: MatchSession) -> None:
        now = time.time()
        with session.lock:
            if session.match_id.startswith("demo-"):
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
                return
            if session.worker_cleanup_process_group is not None:
                raise RuntimeError(
                    "前一个 Worker 进程组尚未确认清理，拒绝启动新的 Worker"
                )
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
                    "python3", str(ROOT / "event_driven_pipeline.py"), source,
                    "--simulate-live", "--replay-speed", "4", "--start", "1037", "--duration", "50",
                    "--match-id", session.match_id, "--replay-events",
                    str(ROOT / "mock_events" / "api_snapshot_scenario.json"),
                    "--before", str(session.before_seconds), "--after", str(session.after_seconds),
                    "--event-poll-seconds", str(session.event_poll_seconds),
                    "--gif-width", str(session.gif_width), "--gif-fps", str(session.gif_fps),
                    "--gif-colors", str(session.gif_colors), "--output-dir", str(session.output_dir),
                    "--graceful-stop-timeout-seconds", str(WORKER_FINISH_TIMEOUT_SECONDS),
                ]
            else:
                if not source:
                    raise RuntimeError("尚未获取到可用的 RTMP resource")
                command = [
                    "python3", str(ROOT / "event_driven_pipeline.py"), source,
                    "--match-id", session.match_id,
                    "--event-url", DEFAULT_EVENT_URL,
                    "--event-user", _user(),
                    "--event-poll-seconds", str(session.event_poll_seconds),
                    "--before", str(session.before_seconds), "--after", str(session.after_seconds),
                    "--gif-width", str(session.gif_width), "--gif-fps", str(session.gif_fps),
                    "--gif-colors", str(session.gif_colors), "--output-dir", str(session.output_dir),
                    "--graceful-stop-timeout-seconds", str(WORKER_FINISH_TIMEOUT_SECONDS),
                ]
            session.worker_command = command
            session.worker_mode = "demo" if demo else "live"
            session.worker = subprocess.Popen(
                command,
                cwd=ROOT,
                text=True,
                start_new_session=True,
            )
            session.worker_process_group = session.worker.pid
            session.desired_running = True
            session.worker_started_at = time.time()
            session.worker_exit_logged_pid = None
            session.worker_restart_due_at = None
            session.worker_cleanup_failure = None
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


dashboard = Dashboard()
app = Flask(__name__, static_folder="dashboard_static", static_url_path="/static")


@app.get("/")
def index():
    return send_from_directory(ROOT / "dashboard_static", "index.html")


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "port": PORT, "time_unix": time.time()})


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
            ("source_poll_seconds", "source_poll_seconds", float),
            ("detail_poll_seconds", "detail_poll_seconds", float),
            ("before_seconds", "before_seconds", float),
            ("after_seconds", "after_seconds", float),
            ("gif_width", "gif_width", int),
            ("gif_fps", "gif_fps", float),
            ("gif_colors", "gif_colors", int),
        ):
            if key in body:
                value = cast(body[key])
                if value <= 0:
                    return jsonify({"error": f"{field_name} 必须大于 0"}), 400
                setattr(session, field_name, value)
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
