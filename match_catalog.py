"""Shared cache for the global soccer match directory.

Used by the automated dashboard and by the manual monitor service to discover
which matches are currently ``Playing``. The class polls the public directory
API on its own clock; two independent processes each keep their own instance.
"""

from __future__ import annotations

import threading
import time
import urllib.parse
from datetime import datetime
from typing import Any

from dqd_api import (
    BEIJING_TIMEZONE,
    match_user,
    positive_env_float,
    request_json,
    validate_match_id,
)


MATCH_CATALOG_URL = "https://api.dongqiudi.com/data/tab/new/lotzc"
# Lazy refresh: re-pulled on the next /matches request once this old. The
# list page polls every 60s; 55s (not 60s) keeps the two from beating against
# each other — at exactly 60s a poll arriving a few ms early would find the
# cache "not yet due" and the pull would slip to the next minute. Bounds a
# new kick-off's appearance at ~2 minutes worst case (was ~3 at 120s). One
# tiny request per minute against the directory; 8899 keeps its own copy.
MATCH_CATALOG_REFRESH_SECONDS = 55.0
MATCH_CATALOG_BACKOFF_SECONDS = (5.0, 10.0, 20.0, 30.0)
MATCH_CATALOG_LIMIT = 20
MATCH_CATALOG_UPCOMING_SECONDS = 15 * 60
# No competition is hidden anymore: the browse list and alerts cover every live
# soccer match the directory returns. Kept as an empty set so importers (the
# monitor alert drop-reason) keep working without a code change.
MATCH_CATALOG_EXCLUDED_COMPETITION_NAMES = frozenset()
MATCH_CATALOG_CARD_FIELDS = (
    "match_id", "team_A_id", "team_A_name", "team_A_logo",
    "team_B_id", "team_B_name", "team_B_logo",
    "competition_id", "competition_name", "round_name", "status", "start_play",
    "sort_timestamp", "fs_A", "fs_B", "minute", "minute_extra", "minute_period",
    "cmp_type",
)
# Read from the same environment as the dashboard so both processes agree.
_AUTO_ADMISSION_POLL_SECONDS = positive_env_float(
    "GIF_AUTO_ADMISSION_POLL_SECONDS", 30.0
)
_AUTO_ADMISSION_MAX_BACKOFF_SECONDS = positive_env_float(
    "GIF_AUTO_ADMISSION_MAX_BACKOFF_SECONDS", 60.0
)


def catalog_match_timestamp(item: dict[str, Any]) -> float:
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


def catalog_required_text(item: dict[str, Any], field_name: str, index: int) -> str:
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
        # Full Playing set for background consumers. With filtering removed this
        # now mirrors ``playing``.
        self.all_playing: list[dict[str, Any]] = []
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
        self._pending_all_playing: list[dict[str, Any]] = []
        self.automation_next_attempt_at = 0.0
        self.automation_last_success_at: float | None = None
        self.automation_last_error: str | None = None
        self.automation_consecutive_failures = 0

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
            {"start": query_start, "init": "1", "user": match_user()}
        )
        payload = request_json(url)
        rows = payload.get("list")
        if not isinstance(rows, list):
            raise ValueError("赛事目录响应缺少有效的 list 数组")

        candidates: dict[str, tuple[dict[str, Any], float]] = {}
        all_candidates: dict[str, tuple[dict[str, Any], float]] = {}
        soccer_count = 0
        for index, raw_item in enumerate(rows):
            if not isinstance(raw_item, dict):
                raise ValueError(f"赛事目录第 {index + 1} 项不是 JSON 对象")
            if str(raw_item.get("cmp_type") or "").strip() != "soccer":
                continue
            soccer_count += 1
            competition_name = raw_item.get("competition_name")
            excluded = (
                isinstance(competition_name, str)
                and competition_name.strip()
                in MATCH_CATALOG_EXCLUDED_COMPETITION_NAMES
            )
            try:
                match_id = validate_match_id(
                    catalog_required_text(raw_item, "match_id", index)
                )
                catalog_required_text(raw_item, "team_A_name", index)
                catalog_required_text(raw_item, "team_B_name", index)
                status = catalog_required_text(raw_item, "status", index)
            except ValueError:
                if excluded:
                    continue
                raise
            if status not in {"Playing", "Fixture"}:
                continue
            item = {
                field_name: raw_item.get(field_name)
                for field_name in MATCH_CATALOG_CARD_FIELDS
            }
            item["match_id"] = match_id
            try:
                match_timestamp = catalog_match_timestamp(item)
            except ValueError:
                # One malformed directory row must not hide every other live
                # match from automatic admission. It is safe to omit this row
                # because it has no usable ordering/start-time information.
                continue
            for target in (all_candidates,):
                existing = target.get(match_id)
                if existing is None:
                    target[match_id] = (item, match_timestamp)
                    continue
                existing_status = str(existing[0].get("status") or "")
                if (
                    status == "Playing" and existing_status != "Playing"
                ) or (
                    status == existing_status and match_timestamp < existing[1]
                ):
                    target[match_id] = (item, match_timestamp)
            if not excluded:
                existing = candidates.get(match_id)
                if existing is None:
                    candidates[match_id] = (item, match_timestamp)
                else:
                    existing_status = str(existing[0].get("status") or "")
                    if (
                        status == "Playing" and existing_status != "Playing"
                    ) or (
                        status == existing_status and match_timestamp < existing[1]
                    ):
                        candidates[match_id] = (item, match_timestamp)

        playing_rows: list[tuple[dict[str, Any], float]] = []
        all_playing_rows: list[tuple[dict[str, Any], float]] = []
        upcoming_rows: list[tuple[dict[str, Any], float]] = []
        upcoming_deadline = now_unix + MATCH_CATALOG_UPCOMING_SECONDS
        for item, match_timestamp in candidates.values():
            if item.get("status") == "Playing":
                playing_rows.append((item, match_timestamp))
            elif now_unix <= match_timestamp <= upcoming_deadline:
                upcoming_rows.append((item, match_timestamp))
        for item, match_timestamp in all_candidates.values():
            if item.get("status") == "Playing":
                all_playing_rows.append((item, match_timestamp))

        playing_rows.sort(key=lambda entry: (entry[1], entry[0]["match_id"]))
        all_playing_rows.sort(key=lambda entry: (entry[1], entry[0]["match_id"]))
        upcoming_rows.sort(key=lambda entry: (entry[1], entry[0]["match_id"]))
        self._pending_all_playing = [item for item, _ in all_playing_rows]
        return (
            [item for item, _ in playing_rows],
            [item for item, _ in upcoming_rows],
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
        self.all_playing = list(self._pending_all_playing or playing)
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
                    "cache_age_seconds": round(cache_age, 1) if cache_age is not None else None,
                    "consecutive_failures": self.consecutive_failures,
                    "source_count": self.source_count,
                    "soccer_count": self.soccer_count,
                    "total_count": total_count,
                    "error": self.last_error,
                    "next_retry_at_unix": self.next_attempt_at if self.next_attempt_at > 0 else None,
                    "next_retry_at": self._iso_beijing(self.next_attempt_at) if self.next_attempt_at > 0 else None,
                    "refresh_interval_seconds": MATCH_CATALOG_REFRESH_SECONDS,
                    "query_start": self.last_query_start,
                },
            }

    def refresh_for_automation(self, now_unix: float | None = None) -> dict[str, Any]:
        """Refresh the full Playing set on an independent background clock.

        This deliberately does not use ``snapshot``'s UI cache
        (``MATCH_CATALOG_REFRESH_SECONDS``). A stale catalog is reported to
        the caller and is never treated as a new authoritative discovery
        result.
        """
        now = time.time() if now_unix is None else float(now_unix)
        with self.lock:
            if now < self.automation_next_attempt_at:
                return {
                    "playing": [dict(item) for item in self.all_playing],
                    "fresh": False,
                    "error": self.automation_last_error,
                    "last_success_at_unix": self.automation_last_success_at,
                }
            started_monotonic = time.monotonic()
            try:
                playing, upcoming, source_count, soccer_count, query_start = self._fetch(now)
            except Exception as exc:
                self.automation_consecutive_failures += 1
                backoff = min(
                    _AUTO_ADMISSION_MAX_BACKOFF_SECONDS,
                    max(
                        _AUTO_ADMISSION_POLL_SECONDS,
                        2 ** min(self.automation_consecutive_failures - 1, 6),
                    ),
                )
                self.automation_next_attempt_at = time.time() + backoff
                self.automation_last_error = str(exc)
                return {
                    "playing": [],
                    "fresh": False,
                    "error": str(exc),
                    "last_success_at_unix": self.automation_last_success_at,
                }

            completed_at = time.time()
            self.playing = playing
            self.all_playing = list(self._pending_all_playing or playing)
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
            self.automation_last_success_at = completed_at
            self.automation_last_error = None
            self.automation_consecutive_failures = 0
            self.automation_next_attempt_at = completed_at + _AUTO_ADMISSION_POLL_SECONDS
            self.next_attempt_at = max(
                self.next_attempt_at,
                completed_at + MATCH_CATALOG_REFRESH_SECONDS,
            )
            return {
                "playing": [dict(item) for item in self.all_playing],
                "fresh": True,
                "error": None,
                "last_success_at_unix": completed_at,
            }
