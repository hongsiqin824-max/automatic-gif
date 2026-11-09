"""Disk retention for the 8901 monitor: expire old GIFs, warn when the disk fills.

8901 never deleted what it wrote. Every capture leaves a GIF + JSON sidecar in
``output_gifs/monitor/<match_id>/`` and every publish copies the GIF into the
content-addressed ``data/published_gifs/`` (plus a JPEG in ``covers/``). On
2026-11-09 the two held 66G of the 128G server disk and grew 2-10G a day.

Neither copy is needed for long. The platform hosts every published GIF
(``img1.qunliao.info``), a failed publish re-copies from the capture, and the
workshop only lists captures from the last 24 hours. Of 3886 deliveries the
slowest came 12.5 hours after its capture. So files older than a few days go,
except those a delivery task that is still moving (queued, creating, waiting
for a person, retry_wait) points at. Failed tasks are not protected: past 24
hours the workshop no longer offers their 重试 button.

The disk check pushes a Feishu warning when free space falls below a
percentage, whatever is filling the disk, so it is noticed before services
start failing.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import socket
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


def non_negative_env_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} 必须是不小于 0 的数") from exc
    if not math.isfinite(value) or value < 0:
        raise RuntimeError(f"{name} 必须是不小于 0 的数")
    return value


# Days a capture (GIF + sidecar) and a published copy (GIF + cover) are kept.
# 0 = keep forever. Anything shorter than a day is raised to a day: the
# workshop offers an unpublished capture for 24 hours and must find it there.
MIN_RETENTION_DAYS = 1.0


def retention_days_env(name: str, default: float) -> float:
    value = non_negative_env_float(name, default)
    return value if value == 0 else max(value, MIN_RETENTION_DAYS)


CLIP_RETENTION_DAYS = retention_days_env("GIF_MONITOR_CLIP_RETENTION_DAYS", 3.0)
PUBLISHED_COPY_RETENTION_DAYS = retention_days_env("GIF_MONITOR_PUBLISHED_COPY_RETENTION_DAYS", 3.0)
# Push a warning when free space on the 8901 disk is below this percent. 0 = off.
DISK_ALERT_FREE_PERCENT = non_negative_env_float("GIF_MONITOR_DISK_ALERT_FREE_PERCENT", 15.0)
if DISK_ALERT_FREE_PERCENT >= 100:
    raise RuntimeError("GIF_MONITOR_DISK_ALERT_FREE_PERCENT 必须小于 100")

RETENTION_INTERVAL_SECONDS = 3600.0
RETENTION_INITIAL_DELAY_SECONDS = 60.0
DISK_ALERT_REPEAT_SECONDS = 3600.0

# Statuses after which a delivery task never reads its files again. Anything
# else (including statuses added later) keeps its files.
SETTLED_TASK_STATUSES = ("published", "drafted", "failed")
_TASK_PATH_COLUMNS = ("source_path", "staged_path", "previous_staged_path")
_TASK_PATH_LIST_COLUMNS = ("extra_source_paths_json", "extra_staged_paths_json")
_PUBLISHED_NAME_RE = re.compile(r"[0-9a-f]{64}")


def _log_event(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def _resolved(value: Any) -> Path | None:
    text = str(value or "").strip()
    return Path(text).expanduser().resolve() if text else None


def protected_paths(database_path: Path | None) -> set[Path]:
    """Files that a delivery task still in flight may read.

    Raises ``sqlite3.Error`` / ``OSError`` when the queue database cannot be
    read: the caller must then delete nothing, since it cannot tell which
    files are still needed.
    """
    if database_path is None or not database_path.exists():
        return set()
    connection = sqlite3.connect(str(database_path), timeout=10)
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='article_delivery_tasks'"
        ).fetchone()
        if table is None:
            return set()
        columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(article_delivery_tasks)")}
        single = [name for name in _TASK_PATH_COLUMNS if name in columns]
        lists = [name for name in _TASK_PATH_LIST_COLUMNS if name in columns]
        placeholders = ", ".join("?" for _ in SETTLED_TASK_STATUSES)
        rows = connection.execute(
            f"SELECT {', '.join(single + lists)} FROM article_delivery_tasks "
            f"WHERE status NOT IN ({placeholders})",
            SETTLED_TASK_STATUSES,
        ).fetchall()
    finally:
        connection.close()
    protected: set[Path] = set()
    for row in rows:
        for value in row[: len(single)]:
            path = _resolved(value)
            if path is not None:
                protected.add(path)
        for value in row[len(single) :]:
            try:
                items = json.loads(value) if value else []
            except ValueError:
                items = []
            for item in items if isinstance(items, list) else []:
                path = _resolved(item)
                if path is not None:
                    protected.add(path)
    return protected


class _Tally:
    def __init__(self) -> None:
        self.files = 0
        self.bytes = 0
        self.errors = 0

    def unlink(self, path: Path) -> None:
        try:
            size = path.stat().st_size
            path.unlink()
        except FileNotFoundError:
            return
        except OSError:
            self.errors += 1
            return
        self.files += 1
        self.bytes += size


def _older_than(path: Path, cutoff: float) -> bool:
    try:
        return path.stat().st_mtime < cutoff
    except OSError:
        return False


def expire_clips(root: Path, cutoff: float, protected: set[Path]) -> _Tally:
    """Delete ``<match_id>/clip_*.gif`` older than ``cutoff`` with their sidecars.

    Pin directories (``.pin_*``) and anything not named like a capture are
    left alone. A match directory that ends up empty is removed.
    """
    tally = _Tally()
    try:
        match_dirs = [path for path in root.iterdir() if path.is_dir() and path.name.isdigit()]
    except OSError:
        return tally
    for match_dir in match_dirs:
        try:
            gifs = sorted(match_dir.glob("clip_*.gif"))
            sidecars = sorted(match_dir.glob("clip_*.json"))
        except OSError:
            tally.errors += 1
            continue
        for gif in gifs:
            if gif.resolve() in protected or not _older_than(gif, cutoff):
                continue
            tally.unlink(gif)
            tally.unlink(gif.with_suffix(".json"))
        for sidecar in sidecars:
            if sidecar.exists() and not sidecar.with_suffix(".gif").exists() and _older_than(sidecar, cutoff):
                tally.unlink(sidecar)
        try:
            if not any(match_dir.iterdir()):
                match_dir.rmdir()
        except OSError:
            pass
    return tally


def expire_published_copies(directory: Path, cutoff: float, protected: set[Path]) -> _Tally:
    """Delete ``<sha256>.gif`` and ``covers/<sha256>.jpg`` older than ``cutoff``."""
    tally = _Tally()
    try:
        gifs = [path for path in directory.glob("*.gif") if _PUBLISHED_NAME_RE.fullmatch(path.stem)]
    except OSError:
        return tally
    for gif in gifs:
        if gif.resolve() not in protected and _older_than(gif, cutoff):
            tally.unlink(gif)
    try:
        covers = [path for path in (directory / "covers").glob("*.jpg") if _PUBLISHED_NAME_RE.fullmatch(path.stem)]
    except OSError:
        return tally
    for cover in covers:
        if (directory / f"{cover.stem}.gif").resolve() not in protected and _older_than(cover, cutoff):
            tally.unlink(cover)
    return tally


class DiskAlarm:
    """Warn once when free space drops below the line, then at most hourly.

    Climbing back above the line re-arms it, so the next drop warns at once.
    """

    def __init__(
        self,
        free_percent: float,
        notify: Callable[[str], bool] | None,
        *,
        repeat_seconds: float = DISK_ALERT_REPEAT_SECONDS,
    ) -> None:
        self.free_percent = free_percent
        self.notify = notify
        self.repeat_seconds = repeat_seconds
        self._last_alert_at: float | None = None

    def check(self, total: int, free: int, now: float, detail: str = "") -> bool:
        """Return whether a warning went out for this reading."""
        if self.free_percent <= 0 or total <= 0:
            return False
        free_percent = free * 100.0 / total
        if free_percent >= self.free_percent:
            self._last_alert_at = None
            return False
        if self._last_alert_at is not None and now - self._last_alert_at < self.repeat_seconds:
            return False
        self._last_alert_at = now
        text = self.message(total, free, now, detail)
        pushed = bool(self.notify(text)) if self.notify is not None else False
        _log_event(
            "monitor_disk_low",
            free_gb=round(free / 1e9, 1),
            total_gb=round(total / 1e9, 1),
            free_percent=round(free_percent, 1),
            threshold_percent=self.free_percent,
            pushed=pushed,
        )
        return True

    def message(self, total: int, free: int, now: float, detail: str) -> str:
        lines = [
            "⚠ 服务器硬盘快满了",
            f"机器：{socket.gethostname()}",
            f"剩余：{free / 1e9:.1f}G / {total / 1e9:.1f}G"
            f"（{free * 100.0 / total:.1f}%，告警线 {self.free_percent:g}%）",
        ]
        if detail:
            lines.append(detail)
        lines.append("写满后 8901 截图、发布和这台机器上的其他服务都会出错，请尽快查是什么在占空间。")
        lines.append(f"时间：{datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}")
        return "\n".join(lines)


class RetentionWorker:
    """Hourly: expire old captures / published copies, then check the disk."""

    def __init__(
        self,
        *,
        clip_root: Path,
        published_dir: Path | None,
        database_path: Path | None,
        notify: Callable[[str], bool] | None,
        clip_retention_days: float = CLIP_RETENTION_DAYS,
        published_retention_days: float = PUBLISHED_COPY_RETENTION_DAYS,
        disk_alert_free_percent: float = DISK_ALERT_FREE_PERCENT,
        interval_seconds: float = RETENTION_INTERVAL_SECONDS,
        initial_delay_seconds: float = RETENTION_INITIAL_DELAY_SECONDS,
        disk_usage: Callable[[Path], Any] = shutil.disk_usage,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.clip_root = clip_root
        self.published_dir = published_dir
        self.database_path = database_path
        self.clip_retention_seconds = clip_retention_days * 86400.0
        self.published_retention_seconds = published_retention_days * 86400.0
        self.alarm = DiskAlarm(disk_alert_free_percent, notify)
        self.interval_seconds = interval_seconds
        self.initial_delay_seconds = initial_delay_seconds
        self.disk_usage = disk_usage
        self.clock = clock
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def run_once(self) -> dict[str, Any]:
        now = self.clock()
        summary: dict[str, Any] = {
            "clip_retention_days": self.clip_retention_seconds / 86400.0,
            "published_retention_days": self.published_retention_seconds / 86400.0,
        }
        sweep_clips = self.clip_retention_seconds > 0
        sweep_published = self.published_retention_seconds > 0 and self.published_dir is not None
        if sweep_clips or sweep_published:
            try:
                protected = protected_paths(self.database_path)
            except (sqlite3.Error, OSError) as exc:
                # Without the task list we cannot tell which files a pending
                # publish still needs, so this round deletes nothing.
                summary["skipped"] = f"读不了发布任务库：{exc}"
            else:
                summary["protected_files"] = len(protected)
                if sweep_clips:
                    tally = expire_clips(self.clip_root, now - self.clip_retention_seconds, protected)
                    summary.update(
                        deleted_clip_files=tally.files,
                        deleted_clip_mb=round(tally.bytes / 1e6, 1),
                        clip_errors=tally.errors,
                    )
                if sweep_published:
                    tally = expire_published_copies(
                        self.published_dir, now - self.published_retention_seconds, protected
                    )
                    summary.update(
                        deleted_published_files=tally.files,
                        deleted_published_mb=round(tally.bytes / 1e6, 1),
                        published_errors=tally.errors,
                    )
        try:
            usage = self.disk_usage(self.clip_root if self.clip_root.exists() else self.clip_root.parent)
        except OSError as exc:
            summary["disk_error"] = str(exc)
        else:
            summary.update(
                disk_free_gb=round(usage.free / 1e9, 1),
                disk_free_percent=round(usage.free * 100.0 / usage.total, 1) if usage.total else None,
            )
            days = [d for d in (self.clip_retention_seconds, self.published_retention_seconds) if d > 0]
            detail = (
                f"8901 已自动删除 {min(days) / 86400.0:g} 天前的截图和发布副本，空间还是不够。"
                if days
                else "8901 截图自动清理是关着的（保留天数 = 0）。"
            )
            self.alarm.check(usage.total, usage.free, now, detail)
        _log_event("monitor_retention_sweep", **summary)
        return summary

    def start(self) -> None:
        if self._thread is not None:
            return

        def _run() -> None:
            if self._stop.wait(self.initial_delay_seconds):
                return
            while True:
                try:
                    self.run_once()
                except Exception as exc:  # noqa: BLE001 - the worker must outlive one bad round
                    _log_event("monitor_retention_failed", error=str(exc))
                if self._stop.wait(self.interval_seconds):
                    return

        self._thread = threading.Thread(target=_run, name="monitor-retention", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
