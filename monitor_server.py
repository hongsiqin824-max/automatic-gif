#!/usr/bin/env python3
"""Manual match-monitoring console (port 8901).

This standalone service is independent from the automated dashboard on 8899.
It auto-discovers the currently ``Playing`` matches, plays each live source in a
grid for a human to watch, and lets the operator capture the last N seconds of a
match as a GIF ("回捞"). Goal/red-card alerts from the standalone Dongqiudi push
monitor are received on ``/api/monitor/events/ingest`` and fanned out to the page
(toast + list) and a Feishu webhook.

It reuses the shared modules (``dqd_api``, ``source_client``, ``match_catalog``,
``live_preview``) so the live-source query, catalog, and HLS preview behave
exactly like the dashboard without importing that monolith.
"""

from __future__ import annotations

import atexit
import concurrent.futures
import hmac
import json
import math
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.request
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from flask import Flask, abort, jsonify, request, send_from_directory, session


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


# Must run before the project modules below are imported: live_preview and
# match_catalog read GIF_PREVIEW_* / catalog settings at import time, so a
# later dotenv load would leave them on their defaults.
_load_dotenv(ROOT / ".env")

from dqd_api import MATCH_STATUS_LABELS, positive_env_float, positive_env_int, validate_match_id  # noqa: E402
from source_client import classify_source_response, query_source
from match_catalog import MATCH_CATALOG_EXCLUDED_COMPETITION_NAMES, MatchCatalog
from live_preview import LivePreviewManager, resolve_ffmpeg
import live_preview as live_preview_module
from webrtc_gateway import WebRTCGateway
from article_draft_queue import (
    ArticleDraftQueue,
    DRAFT_DELIVERY_MODE,
    MANUAL_ARTIFACT_KIND,
)
from feishu_auth import FeishuAuthConfig, SESSION_KEY as FEISHU_SESSION_KEY, install_feishu_auth


HOST = os.environ.get("GIF_MONITOR_HOST", "127.0.0.1")
PORT = int(os.environ.get("GIF_MONITOR_PORT", "8901"))
# Keep enough rolling HLS buffer that a backward capture always has material.
CLIP_BEFORE_SECONDS = positive_env_float("GIF_MONITOR_CLIP_SECONDS", 10.0)
# Operator-chosen [start, end] range capture: longest allowed range, and how long
# a chosen start stays pinned (segments hard-linked) while the end is picked.
CLIP_RANGE_MAX_SECONDS = positive_env_float("GIF_MONITOR_CLIP_RANGE_MAX_SECONDS", 30.0)
CLIP_PIN_TTL_SECONDS = positive_env_float("GIF_MONITOR_CLIP_PIN_TTL_SECONDS", 180.0)
# How often live pins re-scan the playlist to hard-link segments written after
# the start was set (see _ClipPinStore.absorb_all).
CLIP_PIN_ABSORB_INTERVAL_SECONDS = positive_env_float("GIF_MONITOR_CLIP_PIN_ABSORB_INTERVAL_SECONDS", 5.0)
CLIP_GIF_WIDTH = positive_env_int("GIF_MONITOR_CLIP_WIDTH", 440)
# 50/3 ≈ 16.67, matching the automatic pipeline's GIF frame rate. Float rather
# than int so a non-integer rate like this one is expressible here and in .env.
CLIP_GIF_FPS = positive_env_float("GIF_MONITOR_CLIP_FPS", 50 / 3)
# 1s segments (live_preview.PREVIEW_SEGMENT_SECONDS), so 180 segments keep the
# same ~180s on-disk history as before. The operator often watches tens of
# seconds behind the live edge (hls.js back-buffer), so retain a generous
# history — enough to cover that lag plus the capture window — or the requested
# frames are already gone.
PREVIEW_LIST_SIZE = positive_env_int("GIF_MONITOR_PREVIEW_SEGMENTS", 180)
# Retry backoff for operator-published clips. The queue default (30/120/300/
# 600s) suits the unattended 8899 worker; here someone is watching the row, so
# a transient upload/CMS error should come back within seconds, not minutes.
# Also caps the auth_required wait (the queue uses the last entry for it).
# Observed platform 502 bursts last ~45-100s, so the schedule starts fast and
# keeps retrying for ~135s in total (6 attempts) before giving up.
MANUAL_DRAFT_RETRY_DELAYS_SECONDS = (5.0, 10.0, 20.0, 40.0, 60.0)


def env_flag(name: str, default: bool) -> bool:
    """Boolean .env switch: 0/false/no/off → False, anything else → True."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def _env_seconds_or_off(name: str, default: float = 0.0) -> float:
    """Seconds threshold where 0 / off means disabled (unlike
    positive_env_float, which rejects zero). Empty/unset → ``default``."""
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    if raw in {"0", "off", "false", "no"}:
        return 0.0
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} 必须是秒数或 0") from exc
    if not math.isfinite(value) or value < 0:
        raise RuntimeError(f"{name} 必须是秒数或 0")
    return value


# Browser-side auto catch-up (tile 1.5x when adrift, auto seek on foreground /
# hard limit; BACKLOG 11.8). Operator kill switch: false = tiles never change
# playbackRate and never auto-seek, exactly as before 75493ae.
CATCHUP_ENABLED = env_flag("GIF_MONITOR_CATCHUP_ENABLED", True)


def _build_latency_config() -> dict[str, Any]:
    """Collect every operator-tunable knob that shapes preview latency.

    Values come from .env, are bounded here so a typo cannot produce a
    player that stalls forever or seeks into deleted segments, and are
    both logged at start-up and handed to the browser via
    ``/api/monitor/matches`` so the effective set is visible in one place.
    Steady-state HLS delay ≈ segment_seconds × hls_live_sync_count (+ ~0.5s);
    those two are the levers, the rest only shape catch-up behaviour.
    """
    adjusted: list[str] = []

    def bound(name: str, value: float, lo: float, hi: float | None) -> float:
        fixed = max(lo, value if hi is None else min(hi, value))
        if fixed != value:
            adjusted.append(f"{name}={value}->{fixed}")
        return fixed

    segment_seconds = live_preview_module.PREVIEW_SEGMENT_SECONDS
    list_size = PREVIEW_LIST_SIZE
    # hls.js parks the playhead this many segments behind the edge. Below 2 it
    # is forever waiting for the segment ffmpeg is still writing.
    live_sync = int(bound("hls_live_sync_count",
                          positive_env_int("GIF_MONITOR_HLS_LIVE_SYNC_COUNT", 3), 2, 6))
    target = segment_seconds * live_sync
    catchup_rate = bound("catchup_rate",
                         positive_env_float("GIF_MONITOR_CATCHUP_RATE", 1.5), 1.0, 2.0)
    # Gentle layer: faster playback instead of a visible jump. Independent of
    # the master CATCHUP_ENABLED switch so the two layers can be tried alone.
    catchup_rate_enabled = env_flag("GIF_MONITOR_CATCHUP_RATE_ENABLED", True)
    # Start chasing only once clearly past the target, or the tile would be
    # permanently toggling between 1x and the catch-up rate.
    catchup_min_lag = bound("catchup_min_lag",
                            positive_env_float("GIF_MONITOR_CATCHUP_MIN_LAG", 12.0),
                            target + 2, None)
    stage_seek_lag = bound("stage_seek_lag",
                           positive_env_float("GIF_MONITOR_STAGE_SEEK_LAG", 30.0),
                           catchup_min_lag, None)
    # The ring buffer only holds list_size × segment_seconds of history; a seek
    # target beyond it lands on segments that have already been deleted.
    hard_seek_lag = bound("hard_seek_lag",
                          positive_env_float("GIF_MONITOR_HARD_SEEK_LAG", 150.0),
                          stage_seek_lag, max(stage_seek_lag, segment_seconds * list_size - 10))
    # Tile-only instant jump (0 / off = disabled). Never applied to the stage,
    # where the operator may have scrubbed back on purpose to capture.
    tile_seek_lag_raw = _env_seconds_or_off("GIF_MONITOR_TILE_SEEK_LAG")
    tile_seek_lag = 0.0 if tile_seek_lag_raw <= 0 else bound(
        "tile_seek_lag", tile_seek_lag_raw, target + 2, hard_seek_lag)
    # Stage auto seek (0 / off = disabled): same idea for the enlarged player,
    # but the browser suspends it while the operator is visibly working there
    # (range pin set, video paused, or a manual scrub/pause within the grace).
    stage_auto_seek_lag_raw = _env_seconds_or_off("GIF_MONITOR_STAGE_AUTO_SEEK_LAG")
    stage_auto_seek_lag = 0.0 if stage_auto_seek_lag_raw <= 0 else bound(
        "stage_auto_seek_lag", stage_auto_seek_lag_raw, target + 2, hard_seek_lag)
    stage_user_grace = bound("stage_user_grace_seconds",
                             _env_seconds_or_off("GIF_MONITOR_STAGE_USER_GRACE_SECONDS", 60.0),
                             0.0, 600.0)
    return {
        "segment_seconds": segment_seconds,
        "preview_segments": list_size,
        "history_seconds": segment_seconds * list_size,
        "x264_preset": live_preview_module.PREVIEW_X264_PRESET,
        "hls_live_sync_count": live_sync,
        "hls_target_seconds": target,
        "catchup_enabled": CATCHUP_ENABLED,
        "catchup_rate_enabled": catchup_rate_enabled,
        "catchup_rate": catchup_rate,
        "catchup_min_lag": catchup_min_lag,
        "tile_seek_lag": tile_seek_lag,
        "stage_auto_seek_lag": stage_auto_seek_lag,
        "stage_user_grace_seconds": stage_user_grace,
        "stage_seek_lag": stage_seek_lag,
        "hard_seek_lag": hard_seek_lag,
        "adjusted": adjusted,
    }


LATENCY_CONFIG = _build_latency_config()
MAX_PREVIEWS = positive_env_int("GIF_MONITOR_MAX_PREVIEWS", 16)
CLIP_OUTPUT_ROOT = ROOT / "output_gifs" / "monitor"

# ---- Event alerts (进球/红牌) fed by the external monitor WS service ----
# The standalone Dongqiudi push monitor (match_timeline.mjs) detects goals/red
# cards in real time and POSTs them to ``/api/monitor/events/ingest``. 8901 is the
# fan-out hub: it filters to capturable matches, resolves the team name from its
# catalog cache (by A/B side, not team_id), de-duplicates, keeps a small ring
# buffer for the page, and mirrors each alert to a Feishu webhook.
ALERT_ENABLED = os.environ.get("GIF_MONITOR_ALERT_ENABLED", "true").strip().lower() not in {
    "0", "false", "no", "off",
}
ALERT_EVENT_TYPES = frozenset(
    part.strip()
    for part in os.environ.get("GIF_MONITOR_ALERT_EVENTS", "goal,red_card").split(",")
    if part.strip()
) or frozenset({"goal", "red_card"})
FEISHU_WEBHOOK = os.environ.get("GIF_MONITOR_FEISHU_WEBHOOK", "").strip()
# Keep this strictly below the monitor-side POST timeout (3s) so 8901 always
# responds before the monitor aborts; otherwise a slow-but-successful push shows
# up as a false failure in the monitor log. See match_timeline.mjs AbortSignal.
FEISHU_TIMEOUT = positive_env_float("GIF_MONITOR_FEISHU_TIMEOUT_SECONDS", 2.5)
# Separate bot for "a GIF article went live" notices so they never land in the
# goal-alert or access-request groups. Empty = no publish notifications.
FEISHU_PUBLISH_WEBHOOK = os.environ.get("GIF_MONITOR_FEISHU_PUBLISH_WEBHOOK", "").strip()
# Publish-failure alert: after this many consecutive failed attempts on one
# manual clip, push a warning to FEISHU_PUBLISH_WEBHOOK (same bot as the
# success notice, so the team sees both in one group). Authorization failures
# (10007/10008/300002 → auth_required) retry silently forever and have gone
# unnoticed for hours; this is the only thing that makes them visible. 0 = off.
PUBLISH_FAIL_ALERT_AFTER_ATTEMPTS = int(_env_seconds_or_off("GIF_MONITOR_PUBLISH_FAIL_ALERT_ATTEMPTS", 3.0))
# Minimum gap between two alerts for the same clip, so a clip stuck in
# retry_wait for an hour does not spam the group once per retry.
PUBLISH_FAIL_ALERT_COOLDOWN_SECONDS = _env_seconds_or_off("GIF_MONITOR_PUBLISH_FAIL_ALERT_COOLDOWN_SECONDS", 600.0)
# Optional shared secret for the monitor→8901 loopback call. Empty = no check.
ALERT_INGEST_TOKEN = os.environ.get("GIF_MONITOR_INGEST_TOKEN", "").strip()
# Shared secret for the read-only publish-records API used by the daily GIF
# latency report. Empty = the API answers 404, i.e. it does not exist.
REPORT_TOKEN = os.environ.get("GIF_MONITOR_REPORT_TOKEN", "").strip()
# One report call may not span more than this, so a typo cannot dump the DB.
REPORT_MAX_RANGE_SECONDS = 3 * 24 * 3600
# Ring size for the page cursor and for clip→alert matching. A busy weekend has
# a dozen concurrent matches, so a few hundred keeps a whole evening in reach.
ALERT_BUFFER_SIZE = positive_env_int("GIF_MONITOR_ALERT_BUFFER", 500)
# Alerts are persisted so a deploy restart does not lose the goals needed to
# fill match_time/match_score on clips taken a minute later. Empty = memory only.
# A relative path is anchored to the repo, not the process cwd (systemd differs).
_alert_db_env = os.environ.get("GIF_MONITOR_ALERT_DB_PATH", "data/monitor_alerts.sqlite3").strip()
ALERT_DB_PATH = str(ROOT / _alert_db_env) if _alert_db_env and not os.path.isabs(_alert_db_env) else _alert_db_env
# Clip→alert matching window. The clip anchor is the frame's PROGRAM-DATE-TIME,
# i.e. the server wall clock when ffmpeg sliced it, so ``alert.ts_unix - anchor``
# is (push delay − live delay) and its sign is not known up front: the push may
# land before or after the frame. Look generously on both sides and pick the
# alert closest to ``anchor + EXPECTED_OFFSET`` rather than the newest one.
ALERT_MATCH_LOOKBACK_SECONDS = positive_env_float("GIF_MONITOR_ALERT_MATCH_LOOKBACK_SECONDS", 600.0)
ALERT_MATCH_LOOKAHEAD_SECONDS = positive_env_float("GIF_MONITOR_ALERT_MATCH_LOOKAHEAD_SECONDS", 180.0)
# Expected ``ts_unix - anchor`` (seconds, may be negative). 0 until measured from
# the ``delta_seconds`` field in ``monitor_clip`` logs.
try:
    ALERT_MATCH_EXPECTED_OFFSET_SECONDS = float(
        os.environ.get("GIF_MONITOR_ALERT_MATCH_EXPECTED_OFFSET_SECONDS", "0").strip() or 0
    )
except ValueError as exc:
    raise RuntimeError("GIF_MONITOR_ALERT_MATCH_EXPECTED_OFFSET_SECONDS 必须是数字（可为负）") from exc
# Runner-up closer than this to the best pick (in |delta − expected| space) is
# flagged ambiguous in the clip log / page.
ALERT_MATCH_AMBIGUOUS_SECONDS = positive_env_float("GIF_MONITOR_ALERT_MATCH_AMBIGUOUS_SECONDS", 180.0)
# Best pick farther than this from the expected offset is flagged weak: most
# likely the push for the clipped goal has not arrived yet and we picked the
# previous one, so the operator should verify match_time/match_score.
ALERT_MATCH_WEAK_SECONDS = positive_env_float("GIF_MONITOR_ALERT_MATCH_WEAK_SECONDS", 240.0)
EVENT_CN = {"goal": "进球", "red_card": "红牌", "corner": "角球"}


# Scheme H (BACKLOG 11.13): when enabled, each match is registered as a
# mediamtx path; the browser tile plays it over WebRTC (sub-second) while the
# preview ffmpeg reads the same path over loopback RTSP to keep writing the HLS
# ring buffer for the stage and for GIF capture. Off by default; any failure
# falls back to pulling RTMP directly, exactly as before.
webrtc_gateway = WebRTCGateway(
    enabled=os.environ.get("GIF_MONITOR_WEBRTC_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"},
    api_base=os.environ.get("GIF_MONITOR_MEDIAMTX_API", "http://127.0.0.1:9997"),
    rtsp_base=os.environ.get("GIF_MONITOR_MEDIAMTX_RTSP", "rtsp://127.0.0.1:8554"),
    whep_base=os.environ.get("GIF_MONITOR_WHEP_BASE", "whep/"),
    timeout=positive_env_float("GIF_MONITOR_MEDIAMTX_TIMEOUT_SECONDS", 2.0),
    ready_timeout=positive_env_float("GIF_MONITOR_MEDIAMTX_READY_SECONDS", 6.0),
)


def _before_preview_restart(match_id: str, resource: str) -> str | None:
    """Keep a WebRTC-backed preview alive across a mediamtx restart.

    mediamtx forgets API-added paths when it restarts, so an ffmpeg that was
    reading ``rtsp://127.0.0.1:8554/<id>`` would otherwise die again on every
    retry and the tile (HLS included) would stay dark until the operator
    re-opened the match. Re-register the path first; if mediamtx is really
    gone, point ffmpeg straight at the upstream RTMP - the tile then runs on
    plain HLS, exactly like the switch-off state, until the match is reopened.
    """
    if not webrtc_gateway.enabled or not resource.startswith(webrtc_gateway.rtsp_base + "/"):
        return None
    if webrtc_gateway.reensure_path(match_id):
        return None
    upstream = webrtc_gateway.upstream_for(match_id)
    print(
        json.dumps(
            {
                "event": "webrtc_fallback_hls",
                "match_id": match_id,
                "stage": "restart",
                "error": webrtc_gateway.last_error,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    webrtc_gateway.remove_path(match_id)
    return upstream


live_preview = LivePreviewManager(
    list_size=PREVIEW_LIST_SIZE,
    x264_preset=live_preview_module.PREVIEW_X264_PRESET,
    idle_timeout_seconds=positive_env_float("GIF_PREVIEW_IDLE_TIMEOUT_SECONDS", 120.0),
    # Auto-recover a preview whose RTMP source briefly drops while an operator is
    # still watching, so the tile reconnects itself instead of showing stale frames.
    auto_restart=os.environ.get("GIF_MONITOR_AUTO_RESTART", "true").strip().lower()
    not in {"0", "false", "no", "off"},
    restart_base_delay=positive_env_float("GIF_MONITOR_RESTART_BASE_DELAY", 5.0),
    restart_max_delay=positive_env_float("GIF_MONITOR_RESTART_MAX_DELAY", 60.0),
    restart_max_retries=positive_env_int("GIF_MONITOR_RESTART_MAX_RETRIES", 6),
    on_stop=webrtc_gateway.remove_path,
    on_restart=_before_preview_restart,
)
match_catalog = MatchCatalog()
atexit.register(live_preview.shutdown)

# How long a match that just left the ``Playing`` catalog stays known to the
# monitor. The directory drops a match the moment the feed flips to Played -
# exactly when an operator is scrubbing back for a stoppage-time goal and when
# that goal's push tends to arrive late. Within this grace the match keeps its
# tile (and so its preview and frozen history) and its alerts are accepted.
FINISHED_GRACE_SECONDS = positive_env_float("GIF_MONITOR_FINISHED_GRACE_SECONDS", 600.0)

# How long an unpublished GIF keeps its finished match on the list page. Past
# this the match drops out of the collapsed "已完赛待发布" section on its own;
# the GIF itself is never deleted here.
PENDING_CLIP_MAX_AGE_SECONDS = positive_env_float("GIF_MONITOR_PENDING_CLIP_MAX_AGE_HOURS", 24.0) * 3600.0

# Draft-record statuses that mean the GIF reached the CMS. Everything else
# (queued, publishing, retry_wait, failed) still needs the operator.
DELIVERED_CLIP_STATUSES = frozenset({"published", "drafted"})


class _FinishedMatchMemory:
    """Remember matches that recently disappeared from the Playing catalog.

    ``retain(match_id)`` lets a record outlive the grace period for as long as
    the callback says so (the monitor passes "still has unpublished GIFs"), so
    an operator who is mid-edit at full time + 10 min does not lose the match.
    """

    def __init__(self, grace_seconds: float, retain: Callable[[str], bool] | None = None) -> None:
        self.grace_seconds = float(grace_seconds)
        self.retain = retain
        self._lock = threading.Lock()
        self._last_playing: dict[str, dict[str, Any]] | None = None
        self._finished: dict[str, dict[str, Any]] = {}

    def _alive(self, match_id: str, record: dict[str, Any], now: float) -> bool:
        if now - record["finished_at"] <= self.grace_seconds:
            return True
        if self.retain is None:
            return False
        try:
            return bool(self.retain(match_id))
        except Exception:  # noqa: BLE001 - a disk hiccup must not break the list
            return False

    def observe(self, playing: list[dict[str, Any]], now: float) -> None:
        """Diff this catalog snapshot against the previous one."""
        current = {
            str(item.get("match_id") or ""): item
            for item in playing
            if isinstance(item, dict) and item.get("match_id")
        }
        with self._lock:
            if self._last_playing is not None:
                for match_id, item in self._last_playing.items():
                    if match_id not in current and match_id not in self._finished:
                        self._finished[match_id] = {"item": dict(item), "finished_at": now}
            # Back in Playing (catalog glitch, or the feed flipped early): forget it.
            for match_id in current:
                self._finished.pop(match_id, None)
            for match_id, record in list(self._finished.items()):
                if not self._alive(match_id, record, now):
                    del self._finished[match_id]
            self._last_playing = current

    def remember(self, match_id: str, item: dict[str, Any], finished_at: float) -> None:
        """Seed a finished match (process restart: rebuilt from clip sidecars)."""
        with self._lock:
            self._finished.setdefault(str(match_id), {"item": dict(item), "finished_at": finished_at})

    def get(self, match_id: str, now: float) -> dict[str, Any] | None:
        with self._lock:
            record = self._finished.get(str(match_id))
            if record is None or not self._alive(str(match_id), record, now):
                return None
            return {"item": dict(record["item"]), "finished_at": record["finished_at"]}

    def rows(self, now: float) -> list[dict[str, Any]]:
        with self._lock:
            live = [
                {"item": dict(record["item"]), "finished_at": record["finished_at"]}
                for match_id, record in self._finished.items()
                if self._alive(match_id, record, now)
            ]
        live.sort(key=lambda record: (record["finished_at"], str(record["item"].get("match_id") or "")))
        return live


def _pending_clip_count(match_id: str, now: float | None = None) -> int:
    """GIFs of a match that still need the operator: not delivered, not 放弃'd.

    "Delivered" means the draft record reached a terminal status (``published``
    / ``drafted`` - the latter is what the default draft delivery mode ends
    in). A record in ``queued`` / ``publishing`` / ``retry_wait`` / ``failed``
    still counts: the 重试 button lives in this match's stage window, so a
    publish that failed at full time must not make the match vanish and strand
    the GIF with no way to retry it.

    Clips older than ``PENDING_CLIP_MAX_AGE_SECONDS`` no longer count: a goal
    GIF nobody sent within a day will not be sent, so the finished match stops
    being listed instead of sitting in the collapsed section forever. The
    files stay on disk; only the list entry goes. Age comes from the GIF's
    mtime (the encode finishes the file and nothing rewrites it afterwards).

    Defined before the helpers it uses are bound; it only runs on requests, by
    which time ``_clip_dir`` / ``manual_draft_queue`` exist.
    """
    directory = _clip_dir(match_id)
    cutoff = (time.time() if now is None else now) - PENDING_CLIP_MAX_AGE_SECONDS
    try:
        gifs = [
            p
            for p in directory.iterdir()
            if p.is_file() and p.suffix == ".gif" and p.name.startswith("clip_") and p.stat().st_mtime >= cutoff
        ]
    except OSError:
        return 0
    if not gifs:
        return 0
    drafts: dict[str, Any] = {}
    if manual_draft_queue is not None:
        drafts = manual_draft_queue.records_by_source_name(match_id, artifact_kind=MANUAL_ARTIFACT_KIND)

    def delivered(name: str) -> bool:
        record = drafts.get(name)
        return record is not None and str(record.get("status") or "") in DELIVERED_CLIP_STATUSES

    # One title = one article = one pending item, the same rows ``_list_clips``
    # shows. An attached angle carries no publish record of its own (only its
    # main does), so counting it by file would keep a published match listed
    # and turn one 3-angle title into "3 条待发布". It is skipped while its main
    # is on disk and still lists it; an orphan whose main is gone is its own row.
    metas: dict[str, dict[str, Any]] = {}

    def meta_of(name: str) -> dict[str, Any]:
        if name not in metas:
            metas[name] = _read_clip_meta(directory / name)
        return metas[name]

    def attached(gif: Path) -> bool:
        parent = _clip_parent(meta_of(gif.name))
        return bool(parent) and (directory / parent).is_file() and gif.name in _clip_attachments(meta_of(parent))

    return sum(
        1
        for gif in gifs
        if not attached(gif) and not delivered(gif.name) and not meta_of(gif.name).get("discarded")
    )


finished_matches = _FinishedMatchMemory(
    FINISHED_GRACE_SECONDS, retain=lambda match_id: _pending_clip_count(match_id) > 0
)


def _catalog_playing() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Current Playing rows from the catalog, feeding the finished-match memory."""
    snapshot = match_catalog.snapshot()
    playing = [item for item in snapshot.get("playing", []) if isinstance(item, dict)]
    finished_matches.observe(playing, time.time())
    return playing, snapshot


class _AlertStore:
    """Thread-safe ring buffer of recent event alerts with monotonic cursors.

    The browser polls ``since`` with the last ``seq`` it has seen, so each new
    goal/red-card is delivered exactly once per client. ``dedup_key`` guards
    against the monitor re-POSTing the same event (its own retry or overlap).

    With ``db_path`` every accepted alert is also appended to a tiny SQLite
    table and the newest ``capacity`` rows are reloaded on start, so the ring
    (and the clip→alert match that relies on it) survives an 8901 restart.
    Persistence failures are logged and never block ingestion.
    """

    def __init__(self, capacity: int, db_path: str | Path | None = None) -> None:
        self._lock = threading.Lock()
        self._events: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._seen: set[str] = set()
        self._seen_order: deque[str] = deque(maxlen=max(512, capacity))
        self._seq = 0
        self._db: sqlite3.Connection | None = None
        if db_path:
            self._open_db(Path(db_path), capacity)

    def _open_db(self, path: Path, capacity: int) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            db = sqlite3.connect(str(path), check_same_thread=False)
            db.execute(
                "CREATE TABLE IF NOT EXISTS monitor_alerts ("
                "seq INTEGER PRIMARY KEY, ts_unix REAL NOT NULL, match_id TEXT NOT NULL,"
                " event_type TEXT NOT NULL, dedup_key TEXT, event_json TEXT NOT NULL)"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS idx_monitor_alerts_match ON monitor_alerts(match_id, ts_unix)"
            )
            db.commit()
            rows = db.execute(
                "SELECT seq, dedup_key, event_json FROM monitor_alerts ORDER BY seq DESC LIMIT ?",
                (capacity,),
            ).fetchall()
        except (sqlite3.Error, OSError) as exc:
            print(
                json.dumps({"event": "alert_db_unavailable", "path": str(path), "error": str(exc)}, ensure_ascii=False),
                flush=True,
            )
            return
        for seq, dedup_key, event_json in reversed(rows):
            try:
                event = json.loads(event_json)
            except (TypeError, ValueError):
                continue
            if not isinstance(event, dict):
                continue
            self._events.append(event)
            self._seq = max(self._seq, int(seq))
            if dedup_key:
                self._seen_order.append(dedup_key)
                self._seen.add(dedup_key)
        self._db = db

    def add(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Store an event; return the stored copy, or ``None`` if duplicate."""
        key = str(event.get("dedup_key") or "")
        with self._lock:
            if key and key in self._seen:
                return None
            if key:
                if len(self._seen_order) == self._seen_order.maxlen:
                    self._seen.discard(self._seen_order[0])
                self._seen_order.append(key)
                self._seen.add(key)
            self._seq += 1
            stored = dict(event)
            stored["seq"] = self._seq
            stored["ts_unix"] = time.time()
            self._events.append(stored)
            if self._db is not None:
                try:
                    self._db.execute(
                        "INSERT INTO monitor_alerts (seq, ts_unix, match_id, event_type, dedup_key, event_json)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            stored["seq"],
                            stored["ts_unix"],
                            str(stored.get("match_id") or ""),
                            str(stored.get("event_type") or ""),
                            key or None,
                            json.dumps(stored, ensure_ascii=False),
                        ),
                    )
                    self._db.commit()
                except sqlite3.Error as exc:
                    print(
                        json.dumps({"event": "alert_db_write_failed", "error": str(exc)}, ensure_ascii=False),
                        flush=True,
                    )
            return stored

    def since(self, cursor: int) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            events = [dict(item) for item in self._events if item["seq"] > cursor]
            return events, self._seq

    def for_match(self, match_id: str, since_ts: float, until_ts: float) -> list[dict[str, Any]]:
        """Alerts of one match with ``since_ts <= ts_unix <= until_ts``, oldest first."""
        wanted = str(match_id)
        with self._lock:
            return [
                dict(item)
                for item in self._events
                if str(item.get("match_id") or "") == wanted
                and since_ts <= float(item.get("ts_unix") or 0.0) <= until_ts
            ]

    def between(self, since_ts: float, until_ts: float) -> list[dict[str, Any]]:
        """All alerts with ``since_ts <= ts_unix < until_ts``, oldest first.

        Read from SQLite when persisted (a busy day can outgrow the ring),
        otherwise from the in-memory ring.
        """
        with self._lock:
            if self._db is None:
                return [
                    dict(item)
                    for item in self._events
                    if since_ts <= float(item.get("ts_unix") or 0.0) < until_ts
                ]
            try:
                rows = self._db.execute(
                    "SELECT event_json FROM monitor_alerts WHERE ts_unix >= ? AND ts_unix < ? ORDER BY seq",
                    (float(since_ts), float(until_ts)),
                ).fetchall()
            except sqlite3.Error:
                return []
        alerts = []
        for (event_json,) in rows:
            try:
                event = json.loads(event_json)
            except (TypeError, ValueError):
                continue
            if isinstance(event, dict):
                alerts.append(event)
        return alerts

    def close(self) -> None:
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None


alert_store = _AlertStore(ALERT_BUFFER_SIZE, ALERT_DB_PATH or None)
atexit.register(alert_store.close)
# Regenerated every process start. The browser watches this to detect an 8901
# restart and re-baseline its cursor (``seq`` is restored from SQLite when
# persistence is on, but falls back to 0 without it), so an already-open page
# keeps receiving alerts across deploys.
BOOT_ID = uuid.uuid4().hex


def _manual_delivery_mode() -> str:
    """Resolve how manual clips are delivered.

    ``draft`` (default) keeps every operator截取 as an unpublished draft for
    review during testing; ``publish`` makes them go live immediately. Switch via
    ``GIF_MONITOR_DELIVERY_MODE`` in ``.env`` and restart 8901 — no code change.
    Any unrecognized value falls back to the safe ``draft`` mode.
    """
    mode = os.environ.get("GIF_MONITOR_DELIVERY_MODE", "draft").strip().lower()
    return mode if mode in {"draft", "publish"} else "draft"


CLIP_SOURCE_NAMES = {"extension": "插件", "workshop": "工作台"}


def _clip_source_of(meta: dict) -> str:
    """Capture source of a clip from its sidecar; only the extension tags it."""
    return "extension" if meta.get("source") == "extension" else "workshop"


def _clip_source_label(event: dict) -> str:
    name = CLIP_SOURCE_NAMES.get(str(event.get("clip_source") or ""))
    return f"·{name}" if name else ""


def _format_publish_notice(delivered: dict) -> str:
    """Build the Feishu text for one delivered manual clip.

    ``delivered`` is the payload from ``ArticleDraftQueue.on_manual_delivered``.
    Lines whose data is missing (no match detail, event type "无") are dropped
    rather than printed empty so the group message stays readable.
    """
    event = delivered.get("event") if isinstance(delivered.get("event"), dict) else {}
    detail = (
        delivered.get("match_detail")
        if isinstance(delivered.get("match_detail"), dict)
        else {}
    )
    mode = str(delivered.get("delivery_mode") or "publish")
    label = _clip_source_label(event)
    head = f"【GIF 已发布{label}】" if mode == "publish" else f"【GIF 草稿已建{label}】"
    home = str(detail.get("team_A_name") or "").strip()
    away = str(detail.get("team_B_name") or "").strip()
    if home or away:
        head += f"{home or '?'} vs {away or '?'}"
    competition = str(detail.get("competition_name") or "").strip()
    if competition:
        head += f"（{competition}）"
    lines = [head]

    try:
        event_type = int(event.get("match_event") or 0)
    except (TypeError, ValueError):
        event_type = 0
    event_parts = []
    if event_type:
        event_parts.append(MATCH_EVENT_LABELS.get(event_type, str(event_type)))
    minute = str(event.get("minute") or "").strip()
    if minute:
        extra = str(event.get("minute_extra") or "").strip()
        event_parts.append(f"{minute}+{extra}'" if extra else f"{minute}'")
    score = str(event.get("score") or "").strip()
    if score:
        event_parts.append(score)
    if event_parts:
        lines.append("事件：" + " ".join(event_parts))

    title = str(delivered.get("title") or event.get("title_override") or "").strip()
    if title:
        suffix = "（懂球帝判定重复文章）" if delivered.get("duplicate") else ""
        lines.append(f"标题：{title}{suffix}")

    operator = event.get("operator") if isinstance(event.get("operator"), dict) else {}
    lines.append("操作人：" + (str(operator.get("name") or "").strip() or "未知"))

    stamp = delivered.get("delivered_at_unix")
    try:
        when = datetime.fromtimestamp(float(stamp)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError, OverflowError):
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"时间：{when}")

    if delivered.get("draft_url"):
        lines.append(f"文章：{delivered['draft_url']}")
    if delivered.get("gif_url"):
        try:
            gif_count = int(delivered.get("gif_count") or 1)
        except (TypeError, ValueError):
            gif_count = 1
        suffix = f"（共 {gif_count} 张）" if gif_count > 1 else ""
        lines.append(f"GIF：{delivered['gif_url']}{suffix}")
    return "\n".join(lines)


def _notify_manual_delivered(delivered: dict) -> None:
    """``on_manual_delivered`` hook: push the publish notice to its own bot.

    Runs on the queue worker thread after the row is already terminal, so a
    slow or failing webhook can only cost this one notice, never the article.
    """
    if not FEISHU_PUBLISH_WEBHOOK:
        return
    pushed = _send_feishu_text(_format_publish_notice(delivered), FEISHU_PUBLISH_WEBHOOK)
    print(
        json.dumps(
            {
                "event": "publish_notice_pushed",
                "match_id": delivered.get("match_id"),
                "event_key": delivered.get("event_key"),
                "article_id": delivered.get("article_id"),
                "ok": pushed,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


# (match_id, event_key) → unix time of the last failure alert, so one stuck
# clip alerts once per cooldown instead of once per retry. Process-local on
# purpose: a restart re-alerting once is fine, cross-restart silence is not.
_publish_fail_alert_sent: dict[tuple[str, str], float] = {}
_publish_fail_alert_lock = threading.Lock()


def _format_publish_failure_alert(failed: dict) -> str:
    event = failed.get("event") if isinstance(failed.get("event"), dict) else {}
    detail = (
        failed.get("match_detail")
        if isinstance(failed.get("match_detail"), dict)
        else {}
    )
    label = _clip_source_label(event)
    head = f"【GIF 发布卡住{label}】" if failed.get("will_retry") else f"【GIF 发布失败{label}】"
    home = str(detail.get("team_A_name") or "").strip()
    away = str(detail.get("team_B_name") or "").strip()
    if home or away:
        head += f"{home or '?'} vs {away or '?'}"
    competition = str(detail.get("competition_name") or "").strip()
    if competition:
        head += f"（{competition}）"
    lines = [head]
    title = str(failed.get("title") or event.get("title_override") or "").strip()
    if title:
        lines.append(f"标题：{title}")
    attempts = int(failed.get("attempt_count") or 0)
    reason = str(failed.get("error") or "").strip() or "未知错误"
    code = failed.get("platform_code")
    if code not in (None, ""):
        reason += f"（平台码 {code}）"
    lines.append(f"已连续失败 {attempts} 次：{reason}")
    if failed.get("auth_required"):
        lines.append("原因：开放平台授权失效。请到 8901「开放平台授权」重新授权；授权恢复后会自动重试。")
    elif failed.get("will_retry"):
        lines.append("系统仍在自动重试；持续失败请在 8901 发布卡上点「立即重试」或查看日志。")
    else:
        lines.append("已停止重试，请在 8901 发布卡上处理。")
    operator = event.get("operator") if isinstance(event.get("operator"), dict) else {}
    lines.append("操作人：" + (str(operator.get("name") or "").strip() or "未知"))
    stamp = failed.get("failed_at_unix")
    try:
        when = datetime.fromtimestamp(float(stamp)).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError, OverflowError):
        when = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"时间：{when}")
    return "\n".join(lines)


def _notify_manual_failed(failed: dict) -> None:
    """``on_manual_failed`` hook: alert the publish group once a clip has
    failed ``PUBLISH_FAIL_ALERT_AFTER_ATTEMPTS`` times in a row.

    Terminal failures (``will_retry`` False) alert immediately regardless of
    the attempt threshold, because nothing else will ever retry them. Per-clip
    cooldown keeps an hour-long auth outage to a handful of messages.
    """
    if not FEISHU_PUBLISH_WEBHOOK or PUBLISH_FAIL_ALERT_AFTER_ATTEMPTS <= 0:
        return
    attempts = int(failed.get("attempt_count") or 0)
    will_retry = bool(failed.get("will_retry"))
    if will_retry and attempts < PUBLISH_FAIL_ALERT_AFTER_ATTEMPTS:
        return
    key = (str(failed.get("match_id") or ""), str(failed.get("event_key") or ""))
    now = float(failed.get("failed_at_unix") or time.time())
    with _publish_fail_alert_lock:
        last = _publish_fail_alert_sent.get(key)
        if (
            will_retry
            and last is not None
            and now - last < PUBLISH_FAIL_ALERT_COOLDOWN_SECONDS
        ):
            return
        _publish_fail_alert_sent[key] = now
    pushed = _send_feishu_text(_format_publish_failure_alert(failed), FEISHU_PUBLISH_WEBHOOK)
    print(
        json.dumps(
            {
                "event": "publish_failure_alert_pushed",
                "match_id": key[0],
                "event_key": key[1],
                "attempt": attempts,
                "auth_required": bool(failed.get("auth_required")),
                "will_retry": will_retry,
                "error_code": failed.get("error_code"),
                "platform_code": failed.get("platform_code"),
                "ok": pushed,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def _build_manual_draft_queue() -> "ArticleDraftQueue | None":
    """Build a MANUAL-only draft queue that shares the dashboard's publish DB.

    Returns ``None`` (local-save-only) when publishing is disabled or the publish
    stack is not configured, so the monitor never breaks over a missing credential.
    The queue claims only ``MANUAL_ARTIFACT_KIND`` so the 8899 worker (OCR-only by
    default) and this worker share one table without stealing each other's tasks.
    """
    if os.environ.get("GIF_MONITOR_PUBLISH_ENABLED", "true").strip().lower() in {
        "0", "false", "no", "off",
    }:
        return None
    try:
        from article_publisher import (
            ArticlePublisher,
            PublishedGifStore,
            RemoteGifUploadClient,
            competition_publish_accounts,
            environment_boolean,
        )
        from publish_account_pool import PublishAccountPool
        from open_platform_client import OpenPlatformClient, OpenPlatformConfig

        # Fixed author per competition (e.g. 英超/中超 -> 咪咕体育). A bad value
        # must not take publishing down: log it and fall back to the pool.
        try:
            competition_accounts = competition_publish_accounts()
        except RuntimeError as exc:
            print(
                json.dumps(
                    {"event": "competition_publish_accounts_invalid", "error": str(exc)},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            competition_accounts = {}

        data_dir = ROOT / "data"
        gif_dir = Path(os.environ.get("ARTICLE_PUBLISH_GIF_DIR", str(data_dir / "published_gifs")))
        db_path = Path(os.environ.get("ARTICLE_PUBLISH_DB_PATH", str(data_dir / "article_publish.sqlite3")))
        accounts_path = Path(
            os.environ.get("ARTICLE_PUBLISH_ACCOUNTS_PATH", str(data_dir / "publish_accounts.json"))
        )
        backend = os.environ.get("OCR_IMAGE_UPLOAD_BACKEND", "self_hosted").strip() or "self_hosted"
        if backend not in {"official", "self_hosted"}:
            raise RuntimeError(f"不支持的 OCR 图片上传后端：{backend}")
        upload_endpoint = os.environ.get("GIF_UPLOAD_ENDPOINT", "").strip()
        upload_token = os.environ.get("GIF_UPLOAD_TOKEN", "").strip()

        platform_client = OpenPlatformClient(OpenPlatformConfig.from_environment(ROOT))
        publisher = ArticlePublisher(
            platform_client=platform_client,
            ocr_image_upload_client=(platform_client if backend == "official" else None),
            gif_store=PublishedGifStore(
                gif_dir,
                os.environ.get("GIF_PUBLIC_ORIGIN", "https://matchgif.aisportsapp.com"),
                max_bytes=positive_env_int("ARTICLE_PUBLISH_GIF_MAX_BYTES", 50 * 1024 * 1024),
            ),
            database_path=db_path,
            verify_public_url=environment_boolean("ARTICLE_PUBLISH_VERIFY_PUBLIC_URL", True),
            remote_upload_client=(
                RemoteGifUploadClient(
                    upload_endpoint,
                    upload_token,
                    timeout=positive_env_float("GIF_UPLOAD_TIMEOUT_SECONDS", 120.0),
                )
                if upload_endpoint
                else None
            ),
            account_pool=PublishAccountPool(accounts_path),
            competition_accounts=competition_accounts,
        )
        queue = ArticleDraftQueue(
            database_path=publisher.database_path,
            publisher=publisher,
            allowed_output_root=CLIP_OUTPUT_ROOT,
            poll_seconds=positive_env_float("GIF_MONITOR_DRAFT_POLL_SECONDS", 2.0),
            lease_seconds=positive_env_float("GIF_MONITOR_DRAFT_LEASE_SECONDS", 180.0),
            person_wait_seconds=positive_env_float("GIF_MONITOR_PERSON_WAIT_SECONDS", 5.0),
            retry_delays_seconds=MANUAL_DRAFT_RETRY_DELAYS_SECONDS,
            claim_artifact_kinds=(MANUAL_ARTIFACT_KIND,),
            manual_delivery_mode=_manual_delivery_mode(),
            on_manual_delivered=_notify_manual_delivered,
            on_manual_failed=_notify_manual_failed,
            start_worker=True,
        )
        return queue
    except Exception as exc:  # noqa: BLE001 - degrade to local-only on any wiring gap
        print(
            json.dumps(
                {"event": "manual_publish_disabled", "error": str(exc)},
                ensure_ascii=False,
            ),
            flush=True,
        )
        return None


manual_draft_queue = _build_manual_draft_queue()
if manual_draft_queue is not None:
    atexit.register(manual_draft_queue.close)

app = Flask(__name__, static_folder="monitor_static", static_url_path="/static")

# Feishu OAuth login wall for the whole console. Enabled only when the app
# credentials are configured in .env; /api/health, /static/ and the ingest
# endpoint (which keeps its own X-Monitor-Token check) stay exempt. See
# feishu_auth.py for the full contract.
FEISHU_AUTH = install_feishu_auth(app, FeishuAuthConfig.from_env(ROOT / "data"))


def _current_operator() -> dict[str, str] | None:
    """Identity of the logged-in Feishu user for the current request, or None.

    Only the display name and open_id are kept; this rides along inside the
    publish ``event`` so the delivery worker (which has no request context)
    can say who pressed 发布. Without the login wall there is no operator.
    """
    if not FEISHU_AUTH.enabled:
        return None
    user = session.get(FEISHU_SESSION_KEY)
    if not isinstance(user, dict):
        return None
    name = str(user.get("name") or "").strip()
    open_id = str(user.get("open_id") or "").strip()
    if not name and not open_id:
        return None
    return {"name": name, "open_id": open_id}


def _batch_query_sources(match_ids: list[str]) -> dict[str, str]:
    """Concurrently query live-source state for multiple matches.

    Returns ``{match_id: "available"|"no_source"|"error"}``. Individual query
    failures never block the batch — they are recorded as ``"error"``.
    """
    results: dict[str, str] = {}

    def _one(match_id: str) -> tuple[str, str]:
        try:
            result = classify_source_response(query_source(match_id))
            return match_id, result.state
        except Exception:
            return match_id, "error"

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_one, mid): mid for mid in match_ids
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                mid, state = future.result()
                results[mid] = state
            except Exception:
                # The _one wrapper already catches, but guard anyway.
                pass

    return results


def _resolve_source(match_id: str) -> str:
    """Return a usable live-source resource or raise a descriptive error."""
    # Local-only escape hatch: point every match at a fixed test stream so the
    # player path can be exercised without a real fixture. Never set in prod.
    override = os.environ.get("GIF_MONITOR_SOURCE_OVERRIDE", "").strip()
    if override:
        return override
    result = classify_source_response(query_source(match_id))
    if result.state == "available":
        return str(result.data.get("resource") or "").strip()
    if result.state == "no_source":
        raise LookupError("该比赛暂未配置直播源")
    raise RuntimeError(result.error or "直播源查询失败")


def _wait_playlist_ready(match_id: str, timeout: float = 9.0) -> bool:
    """Wait until ffmpeg has written the first HLS playlist and a segment.

    Returning only after the manifest exists removes the race where the browser
    attaches hls.js before the first segment is written and hits a fatal 404.
    """
    playlist = live_preview.media_dir(match_id) / "index.m3u8"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not live_preview.status(match_id).get("running"):
            return False
        if playlist.is_file() and _read_playlist_segments(playlist.parent):
            return True
        time.sleep(0.3)
    return playlist.is_file()


def _preview_has_history(match_id: str) -> bool:
    """True while this match's segments can be clipped.

    A running ffmpeg obviously qualifies. So does a preview whose ffmpeg has
    died (source dropped, match ended) but whose entry still exists: the
    manager keeps that run's playlist frozen on disk, and the final minutes
    are exactly what the operator wants to capture after a late goal.
    """
    status = live_preview.status(match_id)
    return bool(status.get("running")) or status.get("resource") is not None


def _parse_pdt(raw: str) -> float | None:
    """Parse an ``EXT-X-PROGRAM-DATE-TIME`` value into epoch seconds.

    ffmpeg emits a colon-less offset (``+0800``) which ``datetime.fromisoformat``
    rejects before Python 3.11, so normalise it to ``+08:00`` first.
    """
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    else:
        match = re.search(r"([+-]\d{2})(\d{2})$", raw)
        if match:
            raw = raw[: match.start()] + f"{match.group(1)}:{match.group(2)}"
    try:
        return datetime.fromisoformat(raw).timestamp()
    except ValueError:
        return None


def _read_playlist_segments(directory: Path) -> list[tuple[Path, float, float | None]]:
    """Parse the rolling HLS playlist into ordered (path, duration, pdt_epoch).

    ``pdt_epoch`` is the ``EXT-X-PROGRAM-DATE-TIME`` of each segment (epoch
    seconds) and lets a capture anchor on the exact displayed frame instead of
    the server's wall-clock receive time.
    """
    playlist = directory / "index.m3u8"
    try:
        lines = playlist.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    segments: list[tuple[Path, float, float | None]] = []
    pending_duration = 0.0
    pending_pdt: float | None = None
    for line in lines:
        line = line.strip()
        if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            pending_pdt = _parse_pdt(line[len("#EXT-X-PROGRAM-DATE-TIME:"):])
        elif line.startswith("#EXTINF:"):
            try:
                pending_duration = float(line[len("#EXTINF:"):].split(",", 1)[0])
            except ValueError:
                pending_duration = 0.0
        elif line and not line.startswith("#"):
            segment_path = directory / line
            if segment_path.is_file():
                segments.append((segment_path, pending_duration, pending_pdt))
            pending_duration = 0.0
            pending_pdt = None
    return segments


def _select_clip_window(
    segments: list[tuple[Path, float, float | None]],
    before_seconds: float,
    anchor_epoch: float | None,
    start_epoch: float | None = None,
) -> tuple[list[tuple[Path, float, float | None]], float, float]:
    """Return (kept_segments, start_offset, window) for the GIF cut.

    When ``anchor_epoch`` is given and every segment carries a PROGRAM-DATE-TIME,
    the cut is anchored on the exact displayed frame (``[anchor-before, anchor]``)
    so pipeline delay never shifts the result. ``start_epoch`` replaces the
    computed ``anchor-before`` with an operator-chosen absolute start. Otherwise
    it falls back to the trailing ``before_seconds`` of the rolling buffer.
    """
    pdts_ok = bool(segments) and all(seg[2] is not None for seg in segments)
    if anchor_epoch is not None and pdts_ok:
        last_end = segments[-1][2] + segments[-1][1]
        # Clamp a slightly future anchor (clock skew) to the latest frame we have.
        anchor = min(anchor_epoch, last_end)
        want_start = anchor - before_seconds if start_epoch is None else min(start_epoch, anchor)
        kept = [
            seg for seg in segments
            if (seg[2] + seg[1]) > want_start and seg[2] < anchor
        ]
        if not kept:
            kept = [segments[-1]]
        base = kept[0][2]
        start_offset = max(0.0, want_start - base)
        effective_start = base + start_offset
        available = (kept[-1][2] + kept[-1][1]) - effective_start
        window = min(anchor - effective_start, available)
        if window <= 0:
            window = min(before_seconds, sum(seg[1] for seg in kept))
        return kept, start_offset, window
    # Fallback: trailing window of the rolling buffer.
    kept = []
    accumulated = 0.0
    for segment in reversed(segments):
        kept.append(segment)
        accumulated += segment[1]
        if accumulated >= before_seconds + 2.0:
            break
    kept.reverse()
    kept_duration = sum(duration for _, duration, _ in kept)
    start_offset = max(0.0, kept_duration - before_seconds)
    window = min(before_seconds, kept_duration) or kept_duration
    return kept, start_offset, window


def _parse_clip_crop(raw: Any) -> dict[str, float] | None:
    """Validate an operator crop box; ``None`` = full frame.

    The browser sends fractions of the *video frame* (not the element), so
    the server never needs the stream resolution. A box that covers the whole
    frame is normalised to ``None`` so the GIF is encoded exactly as before.
    """
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("crop 必须是对象 {x, y, w, h}")
    values: dict[str, float] = {}
    for key in ("x", "y", "w", "h"):
        try:
            value = float(raw.get(key))
        except (TypeError, ValueError):
            raise ValueError(f"crop.{key} 必须是 0~1 的数字")
        if value != value or value < 0 or value > 1:  # NaN or out of range
            raise ValueError(f"crop.{key} 必须是 0~1 的数字")
        values[key] = value
    if values["w"] <= 0 or values["h"] <= 0:
        raise ValueError("crop 的宽高必须大于 0")
    if values["x"] + values["w"] > 1.0001 or values["y"] + values["h"] > 1.0001:
        raise ValueError("crop 超出画面范围")
    values["w"] = min(values["w"], 1.0 - values["x"])
    values["h"] = min(values["h"], 1.0 - values["y"])
    if values["x"] <= 0 and values["y"] <= 0 and values["w"] >= 0.999 and values["h"] >= 0.999:
        return None
    return {key: round(value, 4) for key, value in values.items()}


def _clip_video_filter(crop: dict[str, float] | None) -> str:
    """fps + (crop) + scale part of the GIF filter graph.

    Full frame keeps the historical ``scale=440:-2``. A crop is expressed with
    ``iw``/``ih`` so it works for any preview resolution, snapped to even
    pixels, and then scaled *down* only: ``min(440, iw)`` never upsamples a
    small region of the 480p preview into a blurry 440-wide GIF.
    """
    fps = f"fps={CLIP_GIF_FPS:g}"
    if crop is None:
        return f"{fps},scale={CLIP_GIF_WIDTH}:-2:flags=lanczos"
    crop_expr = (
        f"crop=floor(iw*{crop['w']:.4f}/2)*2:floor(ih*{crop['h']:.4f}/2)*2"
        f":floor(iw*{crop['x']:.4f}):floor(ih*{crop['y']:.4f})"
    )
    return f"{fps},{crop_expr},scale=w='min({CLIP_GIF_WIDTH}\\,iw)':h=-2:flags=lanczos"


def _encode_clip_gif(
    segments: list[tuple[Path, float, float | None]],
    before_seconds: float,
    output_path: Path,
    anchor_epoch: float | None = None,
    start_epoch: float | None = None,
    crop: dict[str, float] | None = None,
) -> None:
    """Concatenate recent segments and encode the target window to a GIF."""
    kept, start_offset, window = _select_clip_window(segments, before_seconds, anchor_epoch, start_epoch)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Hard-link the chosen segments into a private directory before encoding.
    # The preview process keeps deleting its oldest segments (delete_segments);
    # a hard link pins the inode so ffmpeg can still read the data even if the
    # original file is unlinked mid-encode. Same filesystem is required, so the
    # link dir lives beside the output (both under the project tree).
    link_dir = output_path.parent / f".src_{uuid.uuid4().hex}"
    link_dir.mkdir(parents=True, exist_ok=True)
    concat_list = output_path.parent / f".concat_{uuid.uuid4().hex}.txt"
    try:
        sources: list[Path] = []
        for index, (path, _duration, _pdt) in enumerate(kept):
            link_path = link_dir / f"seg_{index:05d}.ts"
            try:
                os.link(path, link_path)
                sources.append(link_path)
            except FileNotFoundError:
                raise RuntimeError("这一刻的画面刚被清理，请重试")
            except OSError:
                # Cross-filesystem or unsupported link: fall back to the original
                # path (no worse than before; the race simply remains possible).
                sources.append(path)
        concat_list.write_text(
            "".join(f"file '{src.resolve()}'\n" for src in sources),
            encoding="utf-8",
        )
        palette_filter = (
            # ``:g`` keeps a fractional rate short (16.6667, not 16.666666666666668).
            f"{_clip_video_filter(crop)},"
            "split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse"
        )
        command = [
            resolve_ffmpeg(), "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
            "-f", "concat", "-safe", "0", "-i", str(concat_list),
            "-ss", f"{start_offset:.3f}", "-t", f"{window:.3f}",
            "-filter_complex", palette_filter,
            str(output_path),
        ]
        completed = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=120,
        )
    finally:
        concat_list.unlink(missing_ok=True)
        shutil.rmtree(link_dir, ignore_errors=True)
    if completed.returncode != 0 or not output_path.is_file():
        detail = completed.stderr.decode("utf-8", "replace")[-400:]
        raise RuntimeError(f"GIF 编码失败：{detail or '未知错误'}")


def _clip_dir(match_id: str) -> Path:
    return CLIP_OUTPUT_ROOT / match_id


CLIP_LIST_LIMIT = positive_env_int("GIF_MONITOR_CLIP_LIST_LIMIT", 50)


# ---- clip sidecar metadata ----
#
# Capture and publish are two separate operator actions. The GIF alone cannot
# carry what the publish step needs (the alert the clip was matched to, the
# anchor time that forms the event key, the player/title typed at capture), so
# ``/api/monitor/clip`` writes it to ``<clip>.json`` beside the GIF and
# ``/api/monitor/clip/publish`` reads it back. A missing sidecar (clips from
# before this change) degrades to a catalog-clock match on the file mtime.

_CLIP_NAME_RE = re.compile(r"clip_\d{8}-\d{6}_[0-9a-f]{6}\.gif")


def _clip_meta_path(gif_path: Path) -> Path:
    return gif_path.with_suffix(".json")


def _write_clip_meta(gif_path: Path, meta: dict[str, Any]) -> None:
    try:
        _clip_meta_path(gif_path).write_text(
            json.dumps(meta, ensure_ascii=False, sort_keys=True), encoding="utf-8"
        )
    except OSError as exc:
        print(
            json.dumps(
                {"event": "clip_meta_write_error", "clip": gif_path.name, "error": str(exc)},
                ensure_ascii=False,
            ),
            flush=True,
        )


def _read_clip_meta(gif_path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(_clip_meta_path(gif_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


# ---- clip groups: one title, several angles ----
#
# A goal is often worth two or three GIFs (the live angle, the replay, the
# close-up) but the CMS refuses a second gif-article for the same match_time,
# so they have to travel in one article. The group is kept in the sidecars:
# the main GIF (the first one, or whichever the operator promotes) lists its
# ``attachments`` in body order, and every attached GIF records its ``parent``.
# Only the main GIF carries the form / event key / publish record; attached
# GIFs are folded under it on the page and in the article body. Groups are
# only edited before publishing.
#
# The operator declares the group *before* capturing: 「再截一个视角」 on a
# row arms append mode for this operator and match, and the *next* capture
# (stage S/E or the browser extension's Alt+3) lands under that row. The mode
# is single use — it is consumed the moment the capture request starts
# encoding, not when the GIF finishes, so a second capture already in flight
# (a double Alt+3, or the page and the extension overlapping) never lands in
# the same group by accident. Further angles need the button again. The mode
# also drops on 取消, when the row is published or discarded, or when it times
# out with no capture at all. 并入 / 拆出 / 设为主图 remain as corrections.

CLIP_GROUP_MAX = 4
# Long enough to arm the mode and then go find the replay; the deadline is
# fixed at arming time and is never extended, because the mode covers exactly
# one capture.
CLIP_APPEND_TTL_SECONDS = positive_env_float("GIF_MONITOR_CLIP_APPEND_TTL_SECONDS", 300.0)
# Sidecar keys that describe the article rather than the file; they move with
# the main role when the operator promotes another angle.
_CLIP_GROUP_META_KEYS = ("event_key", "form", "alert_match", "alert_match_public", "match_detail")


def _clip_attachments(meta: dict[str, Any]) -> list[str]:
    raw = meta.get("attachments")
    if not isinstance(raw, list):
        return []
    names: list[str] = []
    for value in raw:
        name = str(value or "").strip()
        if _CLIP_NAME_RE.fullmatch(name) and name not in names:
            names.append(name)
    return names


def _clip_parent(meta: dict[str, Any]) -> str | None:
    name = str(meta.get("parent") or "").strip()
    return name if _CLIP_NAME_RE.fullmatch(name) else None


def _clip_group_paths(match_id: str, gif_path: Path, meta: dict[str, Any]) -> list[Path]:
    """Main GIF first, then the attached GIFs that still exist on disk."""
    directory = _clip_dir(match_id)
    paths = [gif_path]
    for name in _clip_attachments(meta):
        candidate = directory / name
        if candidate.is_file() and candidate != gif_path:
            paths.append(candidate)
    return paths


def _clip_group_main(match_id: str, name: str) -> tuple[str, dict[str, Any]] | None:
    """Resolve ``name`` to its group's main GIF ``(name, meta)``; ``None`` if missing."""
    directory = _clip_dir(match_id)
    path = directory / name
    if not path.is_file():
        return None
    meta = _read_clip_meta(path)
    parent = _clip_parent(meta)
    if parent and (directory / parent).is_file():
        return parent, _read_clip_meta(directory / parent)
    return name, meta


def _clip_group_open_reason(match_id: str, main: str, main_meta: dict[str, Any], *, adding: int = 1) -> str | None:
    """Why ``main`` cannot take ``adding`` more GIFs right now, or ``None`` if it can."""
    if main_meta.get("discarded"):
        return "clip_discarded"
    if manual_draft_queue is not None and main in manual_draft_queue.records_by_source_name(
        match_id, artifact_kind=MANUAL_ARTIFACT_KIND
    ):
        return "already_published"
    directory = _clip_dir(match_id)
    existing = [n for n in _clip_attachments(main_meta) if (directory / n).is_file()]
    if 1 + len(existing) + adding > CLIP_GROUP_MAX:
        return "group_full"
    return None


_CLIP_GROUP_REASON_TEXT = {
    "clip_discarded": "已放弃的 GIF 不能合并，请先恢复",
    "already_published": "已提交发布的 GIF 不能再合并",
    "group_full": f"一个标题最多 {CLIP_GROUP_MAX} 张 GIF",
    "clip_missing": "目标 GIF 不存在或已被清理",
}


def _attach_clip(match_id: str, name: str, target: str) -> tuple[str | None, str | None]:
    """Fold ``name`` (plus anything attached to it) under ``target``'s group.

    Returns ``(main_name, None)`` on success or ``(None, reason_code)``.
    Pure sidecar bookkeeping; callers decide how to report the reason.
    """
    directory = _clip_dir(match_id)
    gif_path = directory / name
    if not gif_path.is_file():
        return None, "clip_missing"
    resolved = _clip_group_main(match_id, target)
    if resolved is None:
        return None, "clip_missing"
    main, main_meta = resolved
    if main == name:
        return None, "group_self"
    meta = _read_clip_meta(gif_path)
    if meta.get("discarded"):
        return None, "clip_discarded"
    if manual_draft_queue is not None and name in manual_draft_queue.records_by_source_name(
        match_id, artifact_kind=MANUAL_ARTIFACT_KIND
    ):
        return None, "already_published"
    parent = _clip_parent(meta)
    if parent and not (directory / parent).is_file():
        parent = None
    # Moving out of a group that already went to the CMS would put the same GIF
    # in two articles: the published body still references it and cannot be
    # edited any more. Only the main carries the publish record, so an attached
    # GIF is checked through its parent — its own name never matches one.
    if (
        parent
        and manual_draft_queue is not None
        and parent in manual_draft_queue.records_by_source_name(
            match_id, artifact_kind=MANUAL_ARTIFACT_KIND
        )
    ):
        return None, "already_published"
    if parent == main:
        return main, None
    moving = [name] + [n for n in _clip_attachments(meta) if (directory / n).is_file() and n != main]
    reason = _clip_group_open_reason(match_id, main, main_meta, adding=len(moving))
    if reason:
        return None, reason
    if parent:
        # Leaving a previous group: drop it from that main's list.
        old_parent_path = directory / parent
        old_meta = _read_clip_meta(old_parent_path)
        old_meta["attachments"] = [n for n in _clip_attachments(old_meta) if n != name]
        _write_clip_meta(old_parent_path, old_meta)
    for moved in moving:
        moved_path = directory / moved
        moved_meta = meta if moved == name else _read_clip_meta(moved_path)
        moved_meta.pop("attachments", None)
        moved_meta["parent"] = main
        _write_clip_meta(moved_path, moved_meta)
    existing = [n for n in _clip_attachments(main_meta) if (directory / n).is_file()]
    main_meta["attachments"] = existing + [n for n in moving if n not in existing]
    main_meta.pop("parent", None)
    _write_clip_meta(directory / main, main_meta)
    return main, None


class _ClipAppendStore:
    """Append mode: which main GIF this operator's *next* capture goes under.

    Keyed by (match, operator) so two people on the same match never share a
    target. An entry covers exactly one capture: ``take`` claims and removes
    it, so a second capture in flight finds nothing. Entries expire after
    ``CLIP_APPEND_TTL_SECONDS`` measured from ``set`` — the deadline is never
    extended. Memory only: a restart simply drops the mode.
    """

    def __init__(self, ttl_seconds: float) -> None:
        self._lock = threading.Lock()
        self._targets: dict[tuple[str, str], tuple[str, float]] = {}
        self.ttl_seconds = ttl_seconds

    def set(self, match_id: str, operator: str, name: str) -> None:
        with self._lock:
            self._targets[(match_id, operator)] = (name, time.time())

    def get(self, match_id: str, operator: str) -> str | None:
        with self._lock:
            entry = self._targets.get((match_id, operator))
            if entry is None:
                return None
            name, stamp = entry
            if time.time() - stamp > self.ttl_seconds:
                del self._targets[(match_id, operator)]
                return None
            return name

    def remaining(self, match_id: str, operator: str) -> float | None:
        """Seconds left on this operator's entry, or ``None`` when there is none."""
        with self._lock:
            entry = self._targets.get((match_id, operator))
            if entry is None:
                return None
            left = self.ttl_seconds - (time.time() - entry[1])
            return left if left > 0 else None

    def take(self, match_id: str, operator: str) -> dict[str, Any] | None:
        """Claim this operator's entry (removing it) so the mode is single use.

        Returns ``{"name", "stamp"}`` or ``None``. A capture that then fails
        gives the claim back with ``restore`` so the operator does not have to
        re-arm; the original stamp is kept, so a retry never extends the window.
        """
        with self._lock:
            entry = self._targets.pop((match_id, operator), None)
            if entry is None:
                return None
            name, stamp = entry
            if time.time() - stamp > self.ttl_seconds:
                return None
            return {"name": name, "stamp": stamp}

    def restore(self, match_id: str, operator: str, claim: dict[str, Any]) -> None:
        """Put a claimed entry back after a failed capture, unless replaced since."""
        with self._lock:
            self._targets.setdefault((match_id, operator), (claim["name"], claim["stamp"]))

    def clear(self, match_id: str, operator: str | None = None, *, name: str | None = None) -> None:
        """Drop this operator's target, or (``operator=None``) every target on ``name``."""
        with self._lock:
            for key in list(self._targets):
                if key[0] != match_id:
                    continue
                if operator is not None and key[1] != operator:
                    continue
                if name is not None and self._targets[key][0] != name:
                    continue
                del self._targets[key]

    def retarget(self, match_id: str, old: str, new: str) -> None:
        """The group's main changed (设为主图): keep everyone's mode on the group."""
        with self._lock:
            for key, (name, stamp) in list(self._targets.items()):
                if key[0] == match_id and name == old:
                    self._targets[key] = (new, stamp)


clip_append = _ClipAppendStore(CLIP_APPEND_TTL_SECONDS)


def _append_operator_key() -> str:
    operator = _current_operator() or {}
    return str(operator.get("open_id") or operator.get("name") or "anon")


def _append_target(match_id: str) -> dict[str, Any] | None:
    """The current operator's live append target as ``{name, gif_count, expires_in}``.

    A target that was published, discarded, deleted or filled up meanwhile is
    dropped and ``None`` returned, so the page and the capture paths agree.
    ``expires_in`` is the seconds left on the fixed deadline, so the page can
    drop its banner on time instead of waiting for the next refresh.
    """
    operator = _append_operator_key()
    name = clip_append.get(match_id, operator)
    if not name:
        return None
    resolved = _clip_group_main(match_id, name)
    if resolved is None or resolved[0] != name or _clip_group_open_reason(match_id, name, resolved[1]):
        clip_append.clear(match_id, operator)
        return None
    directory = _clip_dir(match_id)
    count = 1 + sum(1 for n in _clip_attachments(resolved[1]) if (directory / n).is_file())
    return {
        "name": name,
        "gif_count": count,
        "max": CLIP_GROUP_MAX,
        "expires_in": round(clip_append.remaining(match_id, operator) or 0),
    }


def _take_append_target(match_id: str) -> dict[str, Any] | None:
    """Claim (and thereby consume) this operator's append target, if any."""
    return clip_append.take(match_id, _append_operator_key())


def _restore_append_target(match_id: str, claim: dict[str, Any] | None) -> None:
    """Give a claimed target back after a capture that produced no GIF."""
    if claim is not None:
        clip_append.restore(match_id, _append_operator_key(), claim)


def _append_push_state(match_id: str, main: str, clip_meta: dict[str, Any]) -> dict[str, Any] | None:
    """How the appended GIF's matched push relates to the main GIF's *current* one.

    Compared against the push the operator has selected on the main row right
    now (``form.alert_seq``, which they may have changed since capture), not
    the push the main GIF was matched to when it was captured. Three outcomes:

    * ``conflict`` — both sides name a push and they differ: the appended angle
      most likely belongs to another event. Worth a loud warning.
    * ``unverified`` — the main names a push but this angle's own match is
      missing or flagged unsure. Quiet note only; replay angles drift often.
    * ``None`` — nothing to say: the pushes agree, or the main has none
      selected (the operator chose 跳过, so there is no expectation to check).
    """
    main_meta = _read_clip_meta(_clip_dir(match_id) / main)
    form = main_meta.get("form") if isinstance(main_meta.get("form"), dict) else {}
    main_seq = form.get("alert_seq")
    if main_seq in (None, ""):
        return None
    alert_match = clip_meta.get("alert_match") if isinstance(clip_meta.get("alert_match"), dict) else {}
    confident = (
        alert_match.get("mode") == "alert"
        and not alert_match.get("ambiguous")
        and not alert_match.get("weak")
        and alert_match.get("alert_seq") not in (None, "")
    )
    if not confident:
        return {"state": "unverified"}
    if str(alert_match.get("alert_seq")) == str(main_seq):
        return None
    return {
        "state": "conflict",
        "match_time": _join_match_time(alert_match.get("minute"), alert_match.get("minute_extra")),
        "score": str(alert_match.get("score") or ""),
    }


def _attach_pending_clip(match_id: str, gif_path: Path, claim: dict[str, Any] | None) -> dict[str, Any] | None:
    """After a capture: fold it under the append target claimed before encoding.

    ``claim`` comes from ``_take_append_target`` and is already consumed, so a
    capture that started later finds no target. Returns ``{"main", "gif_count",
    "push_state"}`` when attached, ``{"error"}`` when the target went stale
    (the clip stays its own row), ``None`` when append mode was off. Shared by
    the stage capture and the extension upload so Alt+3 honours 「再截一个视角」 too.
    """
    if not claim:
        return None
    target = claim["name"]
    main, reason = _attach_clip(match_id, gif_path.name, target)
    if main is None:
        text = _CLIP_GROUP_REASON_TEXT.get(reason or "", "无法追加")
        return {"error": f"未能追加到《{target}》：{text}，这条已单独保存", "code": reason}
    directory = _clip_dir(match_id)
    clip_meta = _read_clip_meta(gif_path)
    push_state = _append_push_state(match_id, main, clip_meta)
    if push_state is not None:
        # Persisted so the warning survives a reload and is still there at
        # publish time, the one moment that cannot be undone.
        clip_meta["append_push"] = push_state
        _write_clip_meta(gif_path, clip_meta)
    count = 1 + sum(1 for n in _clip_attachments(_read_clip_meta(directory / main)) if (directory / n).is_file())
    return {"main": main, "gif_count": count, "max": CLIP_GROUP_MAX, "push_state": push_state}


def _alert_match_public(alert_match: dict[str, Any]) -> dict[str, Any]:
    """The subset of a clip→alert match that the browser displays."""
    return {
        "mode": alert_match.get("mode"),
        "candidates": alert_match.get("candidates"),
        "ambiguous": alert_match.get("ambiguous"),
        "weak": alert_match.get("weak"),
        "match_time": _join_match_time(alert_match.get("minute"), alert_match.get("minute_extra")),
        "score": alert_match.get("score"),
        "alert_seq": alert_match.get("alert_seq"),
    }


# ---- operator form for the CMS gif-article fields ----
#
# The publish row on the page is a small form mirroring the CMS editor:
# ``match_event`` (0 无 / 1 进球 / 2 点球 / 3 点球不进 / 4 红牌 / 5 乌龙球 /
# 6 进球无效), ``match_time`` (``19`` / ``90+3``), ``match_score`` (``3-1``) and
# ``title``. The server pre-fills it from the alert the clip was matched to and
# the operator can change any field before publishing. Pushes only carry
# goal/red card, so a penalty/own goal/disallowed goal is the operator changing
# the type after picking the goal push (or 跳过 when there was no push at all).

MATCH_EVENT_LABELS = {0: "无", 1: "进球", 2: "点球", 3: "点球不进", 4: "红牌", 5: "乌龙球", 6: "进球无效"}


def _parse_score_event_types(raw: str) -> frozenset[int]:
    """``GIF_MONITOR_SCORE_EVENT_TYPES``: which CMS event types carry ``match_score``.

    Comma-separated codes from ``MATCH_EVENT_LABELS``; the page locks the 比分
    field and the server drops the value for every other type. Default ``1,2``
    (进球, 点球). Invalid input fails startup rather than silently reverting.
    """
    parsed: set[int] = set()
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if not part.isdigit() or int(part) not in MATCH_EVENT_LABELS:
            raise RuntimeError(
                "GIF_MONITOR_SCORE_EVENT_TYPES 必须是逗号分隔的事件类型编号（0~6），例如 1,2"
            )
        parsed.add(int(part))
    return frozenset(parsed)


SCORE_EVENT_TYPES = _parse_score_event_types(os.environ.get("GIF_MONITOR_SCORE_EVENT_TYPES", "1,2"))
_MATCH_TIME_RE = re.compile(r"(\d{1,3})(?:\+(\d{1,2}))?")
_MATCH_SCORE_RE = re.compile(r"(\d{1,2})\s*[-:：]\s*(\d{1,2})")
CLIP_TITLE_MAX = 100


def _join_match_time(minute: Any, extra: Any) -> str:
    minute = str(minute or "").strip()
    extra = str(extra or "").strip()
    if not minute:
        return ""
    return f"{minute}+{extra}" if extra not in ("", "0") else minute


def _split_match_time(value: str) -> tuple[str, str]:
    match = _MATCH_TIME_RE.fullmatch(value)
    return (match.group(1), match.group(2) or "") if match else ("", "")


def _alert_match_event(alert: dict[str, Any]) -> int:
    return 4 if str(alert.get("event_type") or alert.get("code") or "").upper() in {"RED_CARD", "RC"} else 1


def _default_clip_form(alert_match: dict[str, Any]) -> dict[str, Any]:
    """Initial form values: the matched push, or 跳过 (type 0, clock empty) without one."""
    if alert_match.get("mode") != "alert":
        return {"alert_seq": None, "match_event": 0, "match_time": "", "match_score": "", "title": ""}
    match_event = _alert_match_event(alert_match)
    return {
        "alert_seq": alert_match.get("alert_seq"),
        "match_event": match_event,
        "match_time": _join_match_time(alert_match.get("minute"), alert_match.get("minute_extra")),
        "match_score": str(alert_match.get("score") or "").strip() if match_event in SCORE_EVENT_TYPES else "",
        "title": "",
    }


def _normalize_clip_form(body: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    """Merge the request's form fields over ``current`` and validate; ``ValueError`` on bad input."""
    form = dict(current)
    if "alert_seq" in body:
        seq = body.get("alert_seq")
        if seq in (None, ""):
            form["alert_seq"] = None
        else:
            try:
                form["alert_seq"] = int(seq)
            except (TypeError, ValueError) as exc:
                raise ValueError("alert_seq 必须是数字") from exc
    if "match_event" in body:
        try:
            match_event = int(body.get("match_event"))
        except (TypeError, ValueError) as exc:
            raise ValueError("事件类型无效") from exc
        if match_event not in MATCH_EVENT_LABELS:
            raise ValueError("事件类型无效")
        form["match_event"] = match_event
    if "match_time" in body:
        text = str(body.get("match_time") or "").strip().replace(" ", "").rstrip("'")
        if text and not _MATCH_TIME_RE.fullmatch(text):
            raise ValueError("比赛时间格式应为 19 或 90+3")
        form["match_time"] = text
    if "match_score" in body:
        text = str(body.get("match_score") or "").strip()
        match = _MATCH_SCORE_RE.fullmatch(text) if text else None
        if text and not match:
            raise ValueError("比分格式应为 3-1（主队在前）")
        form["match_score"] = f"{match.group(1)}-{match.group(2)}" if match else ""
    if "title" in body:
        form["title"] = str(body.get("title") or "").strip()[:CLIP_TITLE_MAX]
    form.setdefault("alert_seq", None)
    form.setdefault("match_event", 0)
    form.setdefault("match_time", "")
    form.setdefault("match_score", "")
    form.setdefault("title", "")
    if form["match_event"] not in SCORE_EVENT_TYPES:
        form["match_score"] = ""
    return form


def _clip_candidates(match_id: str) -> list[dict[str, Any]]:
    """Every goal/red-card push of this match, newest first, as the page's radio options."""
    items = alert_store.for_match(match_id, 0.0, float("inf"))
    options: list[dict[str, Any]] = []
    for item in reversed(items):
        minute, extra = _alert_minute_parts(item.get("minute"))
        options.append(
            {
                "seq": item.get("seq"),
                "ts_unix": item.get("ts_unix"),
                "event_type": item.get("event_type"),
                "event_cn": item.get("event_cn") or EVENT_CN.get(str(item.get("event_type") or ""), ""),
                "team": item.get("team"),
                "team_name": item.get("team_name") or "",
                "match_event": _alert_match_event(item),
                "match_time": _join_match_time(minute, extra),
                "score": str(item.get("score") or "").strip(),
            }
        )
    return options


def _published_clip_with_match_time(
    match_id: str, match_time: str, *, exclude_event_key: str
) -> dict[str, Any] | None:
    """A published/drafted manual article of this match that already carries ``match_time``."""
    if manual_draft_queue is None:
        return None
    try:
        records = manual_draft_queue.records_for_match(match_id, artifact_kind=MANUAL_ARTIFACT_KIND)
    except Exception:  # noqa: BLE001 - the check is advisory, never block publishing on it
        return None
    if not isinstance(records, dict):
        return None
    for key, record in records.items():
        if key == exclude_event_key or not isinstance(record, dict):
            continue
        if record.get("status") not in {"published", "drafted"}:
            continue
        final_event = record.get("final_event") if isinstance(record.get("final_event"), dict) else {}
        sent = _join_match_time(final_event.get("minute"), final_event.get("minute_extra"))
        if not sent:
            diagnostics = record.get("diagnostics") if isinstance(record.get("diagnostics"), dict) else {}
            summary = diagnostics.get("request_summary") if isinstance(diagnostics.get("request_summary"), dict) else {}
            sent = str(summary.get("match_time") or "").strip()
        if sent == match_time:
            return record
    return None


def _list_clips(match_id: str) -> list[dict[str, Any]]:
    """Newest-first GIFs for a match, joined to their draft records when any.

    Attached GIFs are folded into their main GIF's ``attachments`` rather than
    listed as rows; an attachment whose parent is gone is listed on its own.
    """
    directory = _clip_dir(match_id)
    try:
        gifs = [p for p in directory.iterdir() if p.is_file() and p.suffix == ".gif" and p.name.startswith("clip_")]
        stats = {p: p.stat() for p in gifs}
    except OSError:
        return []
    gifs.sort(key=lambda p: stats[p].st_mtime, reverse=True)
    metas = {p.name: _read_clip_meta(p) for p in gifs}
    by_name = {p.name: p for p in gifs}
    attached: set[str] = set()
    for gif in gifs:
        for name in _clip_attachments(metas[gif.name]):
            if name in by_name and name != gif.name and _clip_parent(metas[name]) == gif.name:
                attached.add(name)
    mains = [p for p in gifs if p.name not in attached][:CLIP_LIST_LIMIT]
    drafts: dict[str, dict[str, Any]] = {}
    if manual_draft_queue is not None and mains:
        drafts = manual_draft_queue.records_by_source_name(match_id, artifact_kind=MANUAL_ARTIFACT_KIND)
    items: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] | None = None
    for gif in mains:
        stat = stats[gif]
        record = drafts.get(gif.name)
        meta = metas[gif.name]
        alert_match = meta.get("alert_match") if isinstance(meta.get("alert_match"), dict) else {}
        form = meta.get("form") if isinstance(meta.get("form"), dict) else _default_clip_form(alert_match)
        attachments: list[dict[str, Any]] = []
        for name in _clip_attachments(meta):
            if name not in attached or _clip_parent(metas[name]) != gif.name:
                continue
            sub_meta = metas[name]
            push_state = sub_meta.get("append_push") if isinstance(sub_meta.get("append_push"), dict) else None
            attachments.append(
                {
                    "name": name,
                    "clip_url": f"clips/{match_id}/{name}",
                    "bytes": stats[by_name[name]].st_size,
                    "created_unix": stats[by_name[name]].st_mtime,
                    "anchored": sub_meta.get("anchored"),
                    "crop": sub_meta.get("crop") if isinstance(sub_meta.get("crop"), dict) else None,
                    "push_state": push_state,
                }
            )
        item = {
            "name": gif.name,
            "clip_url": f"clips/{match_id}/{gif.name}",
            "bytes": stat.st_size,
            "created_unix": stat.st_mtime,
            "title": str(form.get("title") or ""),
            "form": form,
            "anchored": meta.get("anchored"),
            "anchor_epoch_ms": meta.get("anchor_epoch_ms"),
            "crop": meta.get("crop") if isinstance(meta.get("crop"), dict) else None,
            "discarded": bool(meta.get("discarded")),
            "alert_match": meta.get("alert_match_public"),
            "attachments": attachments,
            "gif_count": 1 + len(attachments),
            "event_key": record.get("event_key") if record else None,
            "draft_status": record.get("status") if record else None,
            "draft_stage": record.get("stage") if record else None,
            "draft_url": record.get("draft_url") if record else None,
            "draft_error": record.get("error") if record else None,
            "draft_retriable": bool(record.get("retriable")) if record else False,
            "draft_attempt": int(record.get("attempt_count") or 0) if record else 0,
            "draft_next_attempt_unix": record.get("next_attempt_at_unix") if record else None,
        }
        if record is None and manual_draft_queue is not None:
            # Unpublished rows get the radio options; computed once per listing.
            if candidates is None:
                candidates = _clip_candidates(match_id)
            item["candidates"] = candidates
        items.append(item)
    return items


class _ClipPinStore:
    """Pin the segments after an operator-chosen start point.

    The preview ffmpeg keeps deleting its oldest segments (~180s history). When
    the operator sets a start and then scrubs forward to pick an end, the start
    could fall off the buffer before they click capture. A pin hard-links every
    segment covering ``[start-1, start+max_range+2]`` into a private directory
    so the inodes outlive deletion. Segments that do not exist yet at pin time
    are absorbed on later ``merged_segments`` calls (the clip request).
    """

    def __init__(self, ttl_seconds: float, max_range_seconds: float) -> None:
        self._lock = threading.Lock()
        self._pins: dict[str, dict[str, Any]] = {}
        self.ttl_seconds = ttl_seconds
        self.max_range_seconds = max_range_seconds
        self._stop = threading.Event()
        self._reaper: threading.Thread | None = None

    @staticmethod
    def sweep_orphans(root: Path) -> int:
        """Delete ``.pin_*`` dirs left by a previous process (SIGKILL/OOM skip atexit)."""
        removed = 0
        if not root.is_dir():
            return removed
        for link_dir in root.glob("*/.pin_*"):
            if link_dir.is_dir():
                shutil.rmtree(link_dir, ignore_errors=True)
                removed += 1
        return removed

    def expire(self) -> None:
        with self._lock:
            self._expire_locked(time.time())

    def start_reaper(
        self,
        interval_seconds: float | None = None,
        live_segments_for: "Callable[[str], list[tuple[Path, float, float | None]]] | None" = None,
    ) -> None:
        """Expire pins on a timer and keep absorbing the live playlist.

        ``live_segments_for`` (match_id -> current playlist segments) makes
        every tick hard-link the segments written *after* the pin was set, so
        a start chosen just before the source drops keeps its footage even if
        the relaunched ffmpeg later rolls those segments out of the window
        before the operator clicks 「结束」.
        """
        if self._reaper is not None:
            return
        interval = max(1.0, interval_seconds if interval_seconds is not None else self.ttl_seconds)

        def _run() -> None:
            while not self._stop.wait(interval):
                self.expire()
                if live_segments_for is not None:
                    self.absorb_all(live_segments_for)

        self._reaper = threading.Thread(target=_run, name="clip-pin-reaper", daemon=True)
        self._reaper.start()

    def absorb_all(self, live_segments_for: "Callable[[str], list[tuple[Path, float, float | None]]]") -> int:
        """Absorb the current playlist into every live pin; return pins touched."""
        with self._lock:
            by_match: dict[str, list[dict[str, Any]]] = {}
            for pin in self._pins.values():
                by_match.setdefault(pin["match_id"], []).append(pin)
        touched = 0
        for match_id, pins in by_match.items():
            try:
                segments = live_segments_for(match_id)
            except Exception:  # noqa: BLE001 - a bad playlist must not stop the reaper
                continue
            if not segments:
                continue
            with self._lock:
                for pin in pins:
                    if pin["pin_id"] not in self._pins:
                        continue
                    before = len(pin["segments"])
                    self._absorb_locked(pin, segments)
                    if len(pin["segments"]) != before:
                        touched += 1
        return touched

    def _expire_locked(self, now: float) -> None:
        for pin_id in [k for k, p in self._pins.items() if now - p["created_at"] > self.ttl_seconds]:
            self._drop_locked(pin_id)

    def _drop_locked(self, pin_id: str) -> None:
        pin = self._pins.pop(pin_id, None)
        if pin is not None:
            shutil.rmtree(pin["link_dir"], ignore_errors=True)

    def _absorb_locked(self, pin: dict[str, Any], segments: list[tuple[Path, float, float | None]]) -> None:
        lo = pin["start_epoch"] - 1.0
        hi = pin["start_epoch"] + self.max_range_seconds + 2.0
        pinned: dict[str, tuple[Path, float, float | None]] = pin["segments"]
        for path, duration, pdt in segments:
            if pdt is None or path.name in pinned:
                continue
            if (pdt + duration) <= lo or pdt >= hi:
                continue
            link_path = pin["link_dir"] / path.name
            try:
                os.link(path, link_path)
                pinned[path.name] = (link_path, duration, pdt)
            except FileNotFoundError:
                continue
            except OSError:
                # Unsupported link: keep the original path (no protection, no worse).
                pinned[path.name] = (path, duration, pdt)

    def create(
        self,
        match_id: str,
        start_epoch: float,
        segments: list[tuple[Path, float, float | None]],
    ) -> dict[str, Any]:
        now = time.time()
        pin_id = uuid.uuid4().hex[:12]
        link_dir = _clip_dir(match_id) / f".pin_{pin_id}"
        link_dir.mkdir(parents=True, exist_ok=True)
        pin = {
            "pin_id": pin_id,
            "match_id": match_id,
            "start_epoch": start_epoch,
            "created_at": now,
            "link_dir": link_dir,
            "segments": {},
        }
        with self._lock:
            self._expire_locked(now)
            self._absorb_locked(pin, segments)
            self._pins[pin_id] = pin
        return pin

    def get(self, pin_id: str, match_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._expire_locked(time.time())
            pin = self._pins.get(pin_id)
            if pin is None or pin["match_id"] != match_id:
                return None
            return pin

    def merged_segments(
        self,
        pin: dict[str, Any],
        live_segments: list[tuple[Path, float, float | None]],
    ) -> list[tuple[Path, float, float | None]]:
        """Pinned links win over live paths of the same name; sorted by PDT."""
        with self._lock:
            self._absorb_locked(pin, live_segments)
            merged = dict(pin["segments"])
        for path, duration, pdt in live_segments:
            if pdt is not None and path.name not in merged:
                merged[path.name] = (path, duration, pdt)
        return sorted(merged.values(), key=lambda seg: seg[2])

    def release(self, pin_id: str) -> None:
        with self._lock:
            self._drop_locked(pin_id)

    def shutdown(self) -> None:
        self._stop.set()
        with self._lock:
            for pin_id in list(self._pins):
                self._drop_locked(pin_id)


clip_pins = _ClipPinStore(CLIP_PIN_TTL_SECONDS, CLIP_RANGE_MAX_SECONDS)
atexit.register(clip_pins.shutdown)
# Pins from a previous process are useless (their ids are gone); sweep the
# hard-link dirs a crash left behind, then keep expiring on a timer so a start
# the operator abandoned is freed within one TTL instead of at process exit.
# The same tick keeps absorbing the live playlist (every 5s, well under the 1s
# segment × window) so a start set before a source drop stays clippable.
_ClipPinStore.sweep_orphans(CLIP_OUTPUT_ROOT)
clip_pins.start_reaper(
    CLIP_PIN_ABSORB_INTERVAL_SECONDS,
    live_segments_for=lambda match_id: _read_playlist_segments(live_preview.media_dir(match_id)),
)


def _seed_finished_matches_from_clips(memory: _FinishedMatchMemory, root: Path, now: float) -> list[str]:
    """After a restart, re-list finished matches that still have unpublished GIFs.

    ``_FinishedMatchMemory`` is in-process, so ``deploy-gif`` would otherwise
    forget a match the operator was still publishing from. The sidecar written
    at capture carries the team names, which is all the list row needs. The
    record is dated as already past the grace: nothing survives a restart that
    the grace protects (the preview buffer is gone), so it goes straight to the
    collapsed finished section instead of crowding the live list for 10 min.
    The pending-clip retain rule then keeps it exactly as long as needed.
    """
    seeded: list[str] = []
    try:
        match_dirs = [p for p in root.iterdir() if p.is_dir() and p.name.isdigit()]
    except OSError:
        return seeded
    for match_dir in match_dirs:
        match_id = match_dir.name
        if _pending_clip_count(match_id) == 0:
            continue
        detail: dict[str, Any] = {}
        for sidecar in sorted(match_dir.glob("clip_*.json"), reverse=True):
            meta = _read_clip_meta(sidecar.with_suffix(".gif"))
            candidate = meta.get("match_detail")
            if isinstance(candidate, dict) and candidate.get("team_A_name"):
                detail = candidate
                break
        memory.remember(match_id, {"match_id": match_id, **detail}, now - memory.grace_seconds - 1.0)
        seeded.append(match_id)
    if seeded:
        print(
            json.dumps({"event": "finished_matches_seeded_from_clips", "match_ids": seeded}, ensure_ascii=False),
            flush=True,
        )
    return seeded


_seed_finished_matches_from_clips(finished_matches, CLIP_OUTPUT_ROOT, time.time())


@app.get("/")
def index():
    return send_from_directory(ROOT / "monitor_static", "index.html")


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "port": PORT, "service": "monitor", "time_unix": time.time()})


def _match_row(item: dict[str, Any], status: str) -> dict[str, Any]:
    return {
        "match_id": str(item.get("match_id") or ""),
        "team_A_name": item.get("team_A_name"),
        "team_B_name": item.get("team_B_name"),
        "team_A_logo": item.get("team_A_logo"),
        "team_B_logo": item.get("team_B_logo"),
        "fs_A": item.get("fs_A"),
        "fs_B": item.get("fs_B"),
        "minute": item.get("minute"),
        "minute_period": item.get("minute_period"),
        "competition_name": item.get("competition_name"),
        "status": status,
        "status_label": MATCH_STATUS_LABELS.get(status, status or "未知"),
    }


@app.get("/api/monitor/matches")
def monitor_matches():
    catalog_playing, snapshot = _catalog_playing()
    playing = []
    match_ids: list[str] = []
    for item in catalog_playing:
        playing.append(_match_row(item, str(item.get("status") or "")))
        match_ids.append(str(item.get("match_id") or ""))
    # Matches that left the catalog within FINISHED_GRACE_SECONDS stay listed
    # while someone still holds their preview: that preview's (frozen) buffer
    # is where the final minutes live, so the tile must not be torn down the
    # moment the feed says Played. A match with GIFs still waiting to be
    # published also stays (even past the grace, see _FinishedMatchMemory):
    # the publish form lives in this match's stage window, so dropping the row
    # would strand those GIFs with no UI to send them.
    now = time.time()
    for record in finished_matches.rows(now):
        item = record["item"]
        match_id = str(item.get("match_id") or "")
        if match_id in match_ids:
            continue
        pending = _pending_clip_count(match_id)
        if pending == 0 and live_preview.status(match_id).get("resource") is None:
            continue
        row = _match_row(item, "Played")
        row["finished"] = True
        row["finished_at_unix"] = record["finished_at"]
        row["finished_until_unix"] = record["finished_at"] + finished_matches.grace_seconds
        # Past the grace the row only exists for its unpublished GIFs; the
        # list page files those under a collapsed section instead of mixing
        # them with live matches. Decided here so the browser clock is moot.
        row["grace_active"] = now <= row["finished_until_unix"]
        row["pending_clips"] = pending
        playing.append(row)
        match_ids.append(match_id)
    if os.environ.get("GIF_MONITOR_SOURCE_OVERRIDE", "").strip() and not playing:
        # Local-only: pair with the source override so the tile/stage path can
        # be exercised in a browser when the platform has no live fixture.
        playing.append(
            {
                "match_id": "54477889",
                "team_A_name": "本地测试A",
                "team_B_name": "本地测试B",
                "team_A_logo": None,
                "team_B_logo": None,
                "fs_A": 0,
                "fs_B": 0,
                "minute": 1,
                "minute_period": None,
                "competition_name": "本地 WebRTC 验证",
                "status": "Playing",
                "status_label": MATCH_STATUS_LABELS.get("Playing", "Playing"),
            }
        )
        match_ids.append("54477889")
    # Optionally batch-query live-source states so the list page can show a
    # per-match source indicator without an N+1 waterfall. The browser passes
    # ``?with_sources=1`` on initial load and when the operator manually refreshes.
    if request.args.get("with_sources") == "1" and match_ids:
        source_states = _batch_query_sources(match_ids)
        for entry in playing:
            entry["source_state"] = source_states.get(entry["match_id"], "unknown")
    return jsonify(
        {
            "playing": playing,
            "health": snapshot.get("health", {}),
            "max_previews": MAX_PREVIEWS,
            "clip_before_seconds": CLIP_BEFORE_SECONDS,
            "clip_range_max_seconds": CLIP_RANGE_MAX_SECONDS,
            "catchup_enabled": CATCHUP_ENABLED,
            "latency": LATENCY_CONFIG,
        }
    )


def _frozen_preview_payload(match_id: str) -> dict[str, Any] | None:
    """``preview/start`` answer when the source is gone but history remains.

    After full time the source query fails, yet the manager may still hold the
    run's frozen playlist. Hand the browser that playlist instead of a hard
    ``no_source`` so a late-joining tab can still scrub and clip the finish.
    """
    status = live_preview.status(match_id)
    if status.get("resource") is None:
        return None
    if not (live_preview.media_dir(match_id) / "index.m3u8").is_file():
        return None
    return {
        "ok": True,
        "match_id": match_id,
        "playlist_url": live_preview.playlist_url(match_id),
        **status,
        "playlist_ready": True,
        "history_only": True,
        **_webrtc_fields(match_id, False),
    }


@app.post("/api/monitor/preview/start")
def monitor_preview_start():
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        resource = _resolve_source(match_id)
    except LookupError as exc:
        frozen = _frozen_preview_payload(match_id)
        if frozen:
            return jsonify(frozen)
        return jsonify({"error": str(exc), "code": "no_source"}), 409
    except Exception as exc:
        frozen = _frozen_preview_payload(match_id)
        if frozen:
            return jsonify(frozen)
        return jsonify({"error": f"查询直播源失败：{exc}", "code": "source_error"}), 502
    if not resource:
        frozen = _frozen_preview_payload(match_id)
        if frozen:
            return jsonify(frozen)
        return jsonify({"error": "该比赛暂未配置直播源", "code": "no_source"}), 409
    ffmpeg_source, webrtc = _preview_sources(match_id, resource)
    try:
        status = live_preview.start(match_id, ffmpeg_source)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    # Only report ready once the first playlist + segment exist, so the browser
    # never attaches hls.js to a not-yet-written manifest.
    playlist_ready = _wait_playlist_ready(match_id)
    status = live_preview.status(match_id)
    if not status.get("running"):
        return jsonify(
            {
                "error": status.get("error") or "直播源拉流启动失败，请稍后重试",
                "code": "preview_exited",
            }
        ), 502
    return jsonify(
        {
            "ok": True,
            "match_id": match_id,
            "playlist_url": live_preview.playlist_url(match_id),
            "playlist_ready": playlist_ready,
            **status,
            **_webrtc_fields(match_id, webrtc),
        }
    )


def _preview_sources(match_id: str, upstream: str) -> tuple[str, bool]:
    """Pick what the preview ffmpeg should read for ``match_id``.

    With scheme H enabled and mediamtx reachable, the upstream RTMP is pulled
    once by mediamtx and ffmpeg reads it back over loopback RTSP; the browser
    tile gets a WHEP URL for the same path. Otherwise (disabled, mediamtx down,
    source not ready in time) ffmpeg pulls the upstream directly as before and
    the tile stays on HLS - the operator sees no difference except latency.
    """
    if not webrtc_gateway.enabled:
        return upstream, False
    if webrtc_gateway.ensure_path(match_id, upstream):
        return webrtc_gateway.rtsp_url(match_id), True
    print(
        json.dumps(
            {
                "event": "webrtc_fallback_hls",
                "match_id": match_id,
                "error": webrtc_gateway.last_error,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    # Do not leave a half-registered path around pulling the upstream twice.
    webrtc_gateway.remove_path(match_id)
    return upstream, False


def _webrtc_fields(match_id: str, webrtc: bool) -> dict[str, Any]:
    return {
        "webrtc": bool(webrtc),
        "whep_url": webrtc_gateway.whep_url(match_id) if webrtc else None,
    }


def _preview_is_webrtc(status: dict[str, Any]) -> bool:
    """A running preview is WebRTC-backed iff ffmpeg reads from mediamtx."""
    resource = str(status.get("resource") or "")
    return webrtc_gateway.enabled and resource.startswith(webrtc_gateway.rtsp_base + "/")


@app.post("/api/monitor/preview/stop")
def monitor_preview_stop():
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    live_preview.stop(match_id)
    return jsonify({"ok": True, "match_id": match_id, "running": False})


@app.get("/api/monitor/preview/status")
def monitor_preview_status():
    try:
        match_id = validate_match_id(request.args.get("match_id", ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    # A status check means an operator still has the tile open; refresh the
    # access time so the idle reaper does not kill an actively-watched preview
    # during a transient playback stall.
    live_preview.touch(match_id)
    status = live_preview.status(match_id)
    status["match_id"] = match_id
    status["playlist_url"] = (
        live_preview.playlist_url(match_id) if status.get("running") else None
    )
    status.update(_webrtc_fields(match_id, status.get("running") and _preview_is_webrtc(status)))
    return jsonify(status)


@app.get("/preview/<match_id>/<path:filename>")
def preview_media(match_id, filename):
    try:
        match_id = validate_match_id(match_id)
    except ValueError:
        abort(404)
    if not (filename.endswith(".m3u8") or filename.endswith(".ts")):
        abort(404)
    directory = live_preview.media_dir(match_id)
    if not (directory / filename).is_file():
        abort(404)
    live_preview.touch(match_id)
    mimetype = (
        "application/vnd.apple.mpegurl"
        if filename.endswith(".m3u8")
        else "video/mp2t"
    )
    response = send_from_directory(directory, filename, mimetype=mimetype)
    response.headers["Cache-Control"] = "no-store"
    return response


def _catalog_item_for(match_id: str) -> dict[str, Any] | None:
    """The catalog row for a match: Playing now, or finished within grace."""
    playing, _snapshot = _catalog_playing()
    for item in playing:
        if str(item.get("match_id") or "") == str(match_id):
            return item
    record = finished_matches.get(match_id, time.time())
    return record["item"] if record else None


def _match_detail_for(match_id: str) -> dict[str, Any]:
    """Pull team names/ids for the title from the live catalog snapshot.

    A match that finished within FINISHED_GRACE_SECONDS still resolves, so a
    late goal push is not dropped as ``match_not_capturable`` and a clip of the
    final minutes is still titled with the right teams.
    """
    item = _catalog_item_for(match_id)
    if item is None:
        return {}
    return {
        "team_A_name": item.get("team_A_name"),
        "team_B_name": item.get("team_B_name"),
        "team_A_id": item.get("team_A_id"),
        "team_B_id": item.get("team_B_id"),
        "competition_name": item.get("competition_name"),
    }


_ALERT_MINUTE_RE = re.compile(r"(\d{1,3})(?:\s*\+\s*(\d{1,2}))?\s*'?\s*$")


def _alert_minute_parts(value: Any) -> tuple[str, str]:
    """Split a monitor minute such as ``"2H 68"`` / ``"45+2"`` into (minute, extra).

    The push monitor formats minute as ``"{period} {playing_time}"``; only the
    trailing number (with optional stoppage) is the match clock we can publish.
    """
    match = _ALERT_MINUTE_RE.search(str(value or "").strip())
    if not match:
        return "", ""
    return match.group(1), match.group(2) or ""


def _catalog_clock_for(match_id: str) -> dict[str, Any]:
    """Current minute/score of a match from the catalog snapshot (fallback source)."""
    item = _catalog_item_for(match_id)
    if item is None:
        return {}
    fs_a, fs_b = item.get("fs_A"), item.get("fs_B")
    score = f"{fs_a}-{fs_b}" if fs_a not in (None, "") and fs_b not in (None, "") else ""
    minute = str(item.get("minute") or "").strip()
    extra = str(item.get("minute_extra") or "").strip()
    return {"minute": minute if minute.isdigit() else "", "minute_extra": extra, "score": score}


def _match_clip_alert(match_id: str, anchor_epoch: float) -> dict[str, Any]:
    """Pick the alert a clip most likely captures, or fall back to the catalog clock.

    Rule: among this match's alerts inside ``[anchor - lookback, anchor + lookahead]``
    take the one whose ``ts_unix - anchor`` is closest to
    ``ALERT_MATCH_EXPECTED_OFFSET_SECONDS``. The anchor is the frame's PDT (server
    wall clock at slicing), so the delta is push delay minus live delay and may
    be of either sign; "newest" would silently pick the previous goal whenever
    the push for the clipped goal is still on its way. ``ambiguous`` marks a
    runner-up within ``ALERT_MATCH_AMBIGUOUS_SECONDS`` of the pick; ``weak``
    marks a pick farther than ``ALERT_MATCH_WEAK_SECONDS`` from the expected
    offset so the page can ask the operator to verify.

    Returns ``{"mode": "alert"|"catalog"|"none", "minute", "minute_extra",
    "score", "code", "team", "candidates", "ambiguous", "weak", "alert_seq",
    "alert_minute", "delta_seconds", "candidate_deltas"}``.
    """
    result: dict[str, Any] = {
        "mode": "none",
        "minute": "",
        "minute_extra": "",
        "score": "",
        "code": "",
        "team": "",
        "candidates": 0,
        "ambiguous": False,
        "weak": False,
        "alert_seq": None,
        "alert_minute": None,
        "delta_seconds": None,
        "candidate_deltas": [],
    }
    candidates = alert_store.for_match(
        match_id,
        anchor_epoch - ALERT_MATCH_LOOKBACK_SECONDS,
        anchor_epoch + ALERT_MATCH_LOOKAHEAD_SECONDS,
    )
    result["candidates"] = len(candidates)
    if candidates:
        deltas = [float(item.get("ts_unix") or 0.0) - anchor_epoch for item in candidates]
        result["candidate_deltas"] = [round(delta, 1) for delta in deltas]
        distances = [abs(delta - ALERT_MATCH_EXPECTED_OFFSET_SECONDS) for delta in deltas]
        best = min(range(len(candidates)), key=distances.__getitem__)
        chosen = candidates[best]
        if len(candidates) >= 2:
            runner_up = min(d for i, d in enumerate(distances) if i != best)
            result["ambiguous"] = runner_up - distances[best] < ALERT_MATCH_AMBIGUOUS_SECONDS
        minute, extra = _alert_minute_parts(chosen.get("minute"))
        side = str(chosen.get("team") or "").upper()
        result.update(
            {
                "mode": "alert",
                "minute": minute,
                "minute_extra": extra,
                "score": str(chosen.get("score") or "").strip(),
                "code": "RC" if chosen.get("event_type") == "red_card" else "G",
                # Only a resolved side feeds the title; "?" would be printed verbatim.
                "team": side if side in {"A", "B"} else "",
                "alert_seq": chosen.get("seq"),
                "alert_minute": chosen.get("minute"),
                "delta_seconds": round(deltas[best], 1),
                "weak": distances[best] > ALERT_MATCH_WEAK_SECONDS,
            }
        )
        return result
    clock = _catalog_clock_for(match_id)
    if clock.get("minute") or clock.get("score"):
        result.update({"mode": "catalog", "code": "G", **clock})
    return result


def _alert_drop_reason(match_id: str) -> dict[str, Any]:
    """Classify why a forwarded alert's match is not in the capturable catalog.

    The shown ``playing`` list is both league-filtered (six big leagues) and
    capped at ``MATCH_CATALOG_LIMIT``; ``all_playing`` is neither. Cross-checking
    against it tells us whether a dropped alert was a deliberately excluded big
    league, a match that fell past the display cap, or one not playing at all,
    so the drop is traceable instead of silent (see the ingest log below).
    """
    for item in match_catalog.all_playing:
        if isinstance(item, dict) and str(item.get("match_id") or "") == str(match_id):
            competition = str(item.get("competition_name") or "").strip()
            excluded = competition in MATCH_CATALOG_EXCLUDED_COMPETITION_NAMES
            return {
                "reason": "big_league_excluded" if excluded else "beyond_catalog_limit",
                "competition_name": competition or None,
                "match_title": _alert_match_title(
                    {
                        "team_A_name": item.get("team_A_name"),
                        "team_B_name": item.get("team_B_name"),
                        "competition_name": competition,
                    }
                )
                or None,
            }
    return {"reason": "not_in_catalog", "competition_name": None, "match_title": None}


def _alert_team_name(detail: dict[str, Any], side: str) -> str:
    """Resolve the scoring side's name from the catalog (by A/B, not team_id)."""
    if side == "A":
        return str(detail.get("team_A_name") or "").strip()
    if side == "B":
        return str(detail.get("team_B_name") or "").strip()
    return ""


def _alert_match_title(detail: dict[str, Any]) -> str:
    home = str(detail.get("team_A_name") or "").strip()
    away = str(detail.get("team_B_name") or "").strip()
    competition = str(detail.get("competition_name") or "").strip()
    teams = f"{home} vs {away}" if home and away else ""
    if competition and teams:
        return f"{competition} {teams}"
    return teams or competition


def _format_alert_text(event: dict[str, Any]) -> str:
    """One-line human-readable alert, e.g.
    ``🔔[进球] 哥伦杯 伊尔韦斯 vs 里耶卡 | 伊尔韦斯(50002103) 1→2 | 比分2-1 | 1H 41``.
    """
    title = event.get("match_title") or f"match {event.get('match_id')}"
    name = str(event.get("team_name") or "").strip()
    side = event.get("team")
    who = name or (f"{side}队" if side in {"A", "B"} else "?")
    team_id = event.get("team_id")
    if team_id:
        who = f"{who}({team_id})"
    delta = ""
    if event.get("from") is not None and event.get("to") is not None:
        delta = f" {event['from']}→{event['to']}"
    parts = [f"🔔[{event.get('event_cn')}] {title}", f"{who}{delta}"]
    if event.get("score"):
        parts.append(f"比分{event['score']}")
    if event.get("minute"):
        parts.append(str(event["minute"]))
    return " | ".join(parts)


def _send_feishu_text(text: str, webhook: str | None = None) -> bool:
    """POST a plain-text message to a Feishu bot webhook.

    Defaults to the goal-alert bot (``GIF_MONITOR_FEISHU_WEBHOOK``); pass
    ``webhook`` to target another group. Returns whether the push succeeded;
    never raises so a webhook outage cannot break event ingestion or publishing.
    No webhook configured means the message is silently skipped.
    """
    target = FEISHU_WEBHOOK if webhook is None else webhook
    if not target:
        return False
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode("utf-8")
    request_obj = urllib.request.Request(
        target,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=FEISHU_TIMEOUT) as response:
            response.read()
        return True
    except Exception as exc:  # noqa: BLE001 - alerting must not crash ingestion
        print(
            json.dumps({"event": "feishu_push_failed", "error": str(exc)}, ensure_ascii=False),
            flush=True,
        )
        return False


@app.post("/api/monitor/events/ingest")
def monitor_events_ingest():
    """Receive a goal/red-card event from the external push monitor (loopback)."""
    if ALERT_INGEST_TOKEN and not hmac.compare_digest(
        request.headers.get("X-Monitor-Token", ""), ALERT_INGEST_TOKEN
    ):
        return jsonify({"error": "unauthorized"}), 401
    if not ALERT_ENABLED:
        return jsonify({"ok": True, "accepted": False, "reason": "alerts_disabled"})
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    event_type = str(body.get("event_type") or "").strip()
    if event_type not in ALERT_EVENT_TYPES:
        return jsonify({"ok": True, "accepted": False, "reason": "event_type_filtered"})
    # Only alert for matches the operator can actually capture (i.e. shown in the
    # browse catalog; big leagues are intentionally excluded there).
    detail = _match_detail_for(match_id)
    if not detail:
        drop = _alert_drop_reason(match_id)
        print(
            json.dumps(
                {
                    "event": "alert_dropped",
                    "reason": "match_not_capturable",
                    "detail_reason": drop["reason"],
                    "match_id": match_id,
                    "event_type": event_type,
                    "competition_name": drop["competition_name"],
                    "match_title": str(body.get("match_title") or "").strip() or drop["match_title"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        return jsonify(
            {
                "ok": True,
                "accepted": False,
                "reason": "match_not_capturable",
                "detail_reason": drop["reason"],
            }
        )
    side = str(body.get("team") or "").strip().upper()
    side = side if side in {"A", "B"} else "?"
    team_id = body.get("team_id")
    score = body.get("score")
    minute = body.get("minute")
    match_title = str(body.get("match_title") or "").strip() or _alert_match_title(detail)
    event = {
        "match_id": match_id,
        "event_type": event_type,
        "event_cn": EVENT_CN.get(event_type, event_type),
        "team": side,
        "team_id": None if team_id in (None, "") else str(team_id),
        "team_name": _alert_team_name(detail, side) or None,
        "from": body.get("from"),
        "to": body.get("to"),
        "score": str(score).strip() if score not in (None, "") else None,
        "minute": str(minute).strip() if minute not in (None, "") else None,
        "match_title": match_title or None,
        "source": str(body.get("source") or "").strip() or None,
        "dedup_key": str(body.get("dedup_key") or "").strip()
        or f"{match_id}|{event_type}|{side}|{body.get('to')}",
    }
    stored = alert_store.add(event)
    if stored is None:
        return jsonify({"ok": True, "accepted": False, "reason": "duplicate"})
    pushed = _send_feishu_text(_format_alert_text(stored))
    print(
        json.dumps(
            {
                "event": "monitor_alert",
                "match_id": match_id,
                "event_type": event_type,
                "team": side,
                "seq": stored["seq"],
                "feishu": pushed,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return jsonify({"ok": True, "accepted": True, "seq": stored["seq"], "feishu_pushed": pushed})


@app.get("/api/monitor/events")
def monitor_events():
    """Return alerts newer than ``since`` plus the latest cursor for polling."""
    try:
        since = int(request.args.get("since", "0"))
    except ValueError:
        since = 0
    events, latest = alert_store.since(since)
    return jsonify({"events": events, "seq": latest, "boot_id": BOOT_ID, "alert_enabled": ALERT_ENABLED})


@app.post("/api/monitor/clip/pin")
def monitor_clip_pin():
    """Pin an operator-chosen start point so it survives buffer rollover."""
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    try:
        start_epoch = float(body["start_epoch_ms"]) / 1000.0
    except (KeyError, TypeError, ValueError):
        return jsonify({"error": "start_epoch_ms 必须是数字"}), 400
    if not _preview_has_history(match_id):
        return jsonify({"error": "请先开始预览该场，才能回捞画面", "code": "preview_not_running"}), 409
    segments = _read_playlist_segments(live_preview.media_dir(match_id))
    if not segments:
        return jsonify({"error": "缓冲还没有可用画面，请稍后再试", "code": "buffer_empty"}), 409
    if any(seg[2] is None for seg in segments):
        return jsonify({"error": "缓冲缺少时间戳，无法按区间截取", "code": "no_pdt"}), 409
    oldest = segments[0][2]
    last_end = segments[-1][2] + segments[-1][1]
    if start_epoch < oldest:
        retained = int(last_end - oldest)
        return (
            jsonify(
                {
                    "error": f"起点已超出缓冲（仅保留最近约 {retained} 秒），请离直播更近时再设起点",
                    "code": "anchor_out_of_buffer",
                }
            ),
            409,
        )
    start_epoch = min(start_epoch, last_end)
    pin = clip_pins.create(match_id, start_epoch, segments)
    return jsonify(
        {
            "ok": True,
            "pin_id": pin["pin_id"],
            "start_epoch_ms": round(start_epoch * 1000),
            "ttl_seconds": CLIP_PIN_TTL_SECONDS,
            "max_range_seconds": CLIP_RANGE_MAX_SECONDS,
        }
    )


@app.delete("/api/monitor/clip/pin/<pin_id>")
def monitor_clip_unpin(pin_id: str):
    clip_pins.release(pin_id)
    return jsonify({"ok": True})


@app.post("/api/monitor/clip")
def monitor_clip():
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    before_seconds = CLIP_BEFORE_SECONDS
    if "before_seconds" in body:
        try:
            before_seconds = float(body["before_seconds"])
        except (TypeError, ValueError):
            return jsonify({"error": "before_seconds 必须是数字"}), 400
        if not (0 < before_seconds <= 120):
            return jsonify({"error": "before_seconds 必须在 0~120 之间"}), 400
    anchor_epoch: float | None = None
    if body.get("anchor_epoch_ms") is not None:
        try:
            anchor_epoch = float(body["anchor_epoch_ms"]) / 1000.0
        except (TypeError, ValueError):
            return jsonify({"error": "anchor_epoch_ms 必须是数字"}), 400
    try:
        crop = _parse_clip_crop(body.get("crop"))
    except ValueError as exc:
        return jsonify({"error": str(exc), "code": "crop_invalid"}), 400
    # Range mode: a pinned start plus the end frame the operator is looking at.
    pin: dict[str, Any] | None = None
    start_epoch: float | None = None
    pin_id = str(body.get("pin_id") or "").strip()
    if pin_id:
        try:
            anchor_epoch = float(body["end_epoch_ms"]) / 1000.0
        except (KeyError, TypeError, ValueError):
            return jsonify({"error": "区间截取需要 end_epoch_ms"}), 400
        pin = clip_pins.get(pin_id, match_id)
        if pin is None:
            return (
                jsonify(
                    {
                        "error": f"起点已失效（超过 {CLIP_PIN_TTL_SECONDS:g} 秒未截取），请重新设起点",
                        "code": "pin_expired",
                    }
                ),
                409,
            )
        start_epoch = pin["start_epoch"]
        range_seconds = anchor_epoch - start_epoch
        if range_seconds < 0.5:
            return jsonify({"error": "终点必须在起点之后", "code": "range_invalid"}), 400
        if range_seconds > CLIP_RANGE_MAX_SECONDS:
            return (
                jsonify(
                    {
                        "error": f"区间最长 {CLIP_RANGE_MAX_SECONDS:g} 秒，当前 {range_seconds:.1f} 秒",
                        "code": "range_too_long",
                    }
                ),
                400,
            )
        before_seconds = range_seconds
    if not _preview_has_history(match_id):
        return jsonify({"error": "请先开始预览该场，才能回捞画面", "code": "preview_not_running"}), 409
    segments = _read_playlist_segments(live_preview.media_dir(match_id))
    if pin is not None:
        segments = clip_pins.merged_segments(pin, segments)
    if not segments:
        return jsonify({"error": "缓冲还没有可用画面，请稍后再试", "code": "buffer_empty"}), 409
    # When the operator has drifted further behind live than the on-disk history,
    # the frame they saw is already deleted; refuse rather than silently return
    # the live edge (which would be the wrong moment).
    if anchor_epoch is not None and all(seg[2] is not None for seg in segments):
        oldest = segments[0][2]
        if anchor_epoch < oldest:
            retained = int((segments[-1][2] + segments[-1][1]) - oldest)
            return (
                jsonify(
                    {
                        "error": f"这一刻已超出缓冲（仅保留最近约 {retained} 秒），请离直播更近时再截取",
                        "code": "anchor_out_of_buffer",
                    }
                ),
                409,
            )
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_path = _clip_dir(match_id) / f"clip_{timestamp}_{uuid.uuid4().hex[:6]}.gif"
    anchored = anchor_epoch is not None and all(seg[2] is not None for seg in segments)
    # Claimed before the slow encode, not after: a second capture that starts
    # while this one is still encoding must find the mode already used up.
    # A failed encode gives the claim back, so one bad attempt costs nothing.
    append_claim = _take_append_target(match_id)
    try:
        _encode_clip_gif(segments, before_seconds, output_path, anchor_epoch, start_epoch, crop=crop)
    except Exception as exc:
        _restore_append_target(match_id, append_claim)
        return jsonify({"error": str(exc)}), 500
    if pin is not None:
        clip_pins.release(pin["pin_id"])
    # Resolve which goal/red card this clip belongs to so the draft carries
    # match_time/match_score; the operator still picks match_event in the CMS.
    alert_match = _match_clip_alert(
        match_id, anchor_epoch if anchor_epoch is not None else time.time()
    )
    print(
        json.dumps(
            {
                "event": "monitor_clip",
                "match_id": match_id,
                "before_seconds": before_seconds,
                "anchored": anchored,
                "anchor_epoch_ms": None if anchor_epoch is None else round(anchor_epoch * 1000),
                "start_epoch_ms": None if start_epoch is None else round(start_epoch * 1000),
                "pin_id": pin_id or None,
                "crop": crop,
                "segments": len(segments),
                "with_pdt": sum(1 for seg in segments if seg[2] is not None),
                "clip": output_path.name,
                "alert_match": {
                    "mode": alert_match["mode"],
                    "candidates": alert_match["candidates"],
                    "ambiguous": alert_match["ambiguous"],
                    "weak": alert_match["weak"],
                    "delta_seconds": alert_match["delta_seconds"],
                    "candidate_deltas": alert_match["candidate_deltas"],
                    "alert_seq": alert_match["alert_seq"],
                    "alert_minute": alert_match["alert_minute"],
                    "minute": alert_match["minute"],
                    "minute_extra": alert_match["minute_extra"],
                    "score": alert_match["score"],
                },
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    # Capture only. Publishing is a separate operator action
    # (``/api/monitor/clip/publish``) taken after they have seen the GIF and
    # checked the pre-filled CMS fields, so persist everything that step needs.
    event_key = f"manual_{int(anchor_epoch if anchor_epoch is not None else time.time())}"
    form = _default_clip_form(alert_match)
    _write_clip_meta(
        output_path,
        {
            "event_key": event_key,
            "anchored": anchored,
            "anchor_epoch_ms": None if anchor_epoch is None else round(anchor_epoch * 1000),
            "start_epoch_ms": None if start_epoch is None else round(start_epoch * 1000),
            "before_seconds": before_seconds,
            "crop": crop,
            "form": form,
            "alert_match": alert_match,
            "alert_match_public": _alert_match_public(alert_match),
            "match_detail": _match_detail_for(match_id),
        },
    )
    appended = _attach_pending_clip(match_id, output_path, append_claim)
    return jsonify(
        {
            "ok": True,
            "match_id": match_id,
            "name": output_path.name,
            "before_seconds": before_seconds,
            "anchored": anchored,
            "crop": crop,
            "clip_url": f"clips/{match_id}/{output_path.name}",
            "bytes": output_path.stat().st_size,
            "publish_enabled": manual_draft_queue is not None,
            "delivery_mode": _manual_delivery_mode(),
            "event_key": event_key,
            "alert_match": _alert_match_public(alert_match),
            "form": form,
            "appended": appended,
        }
    )


@app.post("/api/monitor/clip/publish")
def monitor_clip_publish():
    """Send an already-captured GIF to the CMS (draft or publish per .env).

    Idempotent per clip: the event key is fixed at capture time, and the draft
    queue returns the existing task for a repeated key, so a double click or a
    retry after a network blip never creates a second article.
    """
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    name = str(body.get("name") or "").strip()
    if not _CLIP_NAME_RE.fullmatch(name):
        return jsonify({"error": "无效的 GIF 文件名"}), 400
    gif_path = _clip_dir(match_id) / name
    if not gif_path.is_file():
        return jsonify({"error": "GIF 不存在或已被清理", "code": "clip_missing"}), 404
    if manual_draft_queue is None:
        return jsonify({"error": "发文未启用（GIF 仅本地保存）", "code": "publish_disabled"}), 409
    meta = _read_clip_meta(gif_path)
    if meta.get("discarded"):
        # Abandoned in another tab/window while this page still showed the form:
        # 放弃 means "do not publish", so refuse until the operator 恢复s it.
        return jsonify({"error": "该 GIF 已放弃，请先恢复再发布", "code": "clip_discarded"}), 409
    parent = _clip_parent(meta)
    if parent and (_clip_dir(match_id) / parent).is_file():
        return jsonify({"error": "该 GIF 已并入其他标题，请发布主图或先拆出", "code": "clip_attached"}), 409
    group_paths = _clip_group_paths(match_id, gif_path, meta)
    alert_match = meta.get("alert_match")
    if not isinstance(alert_match, dict):
        # Pre-sidecar clip: best effort from the file's own timestamp.
        alert_match = _match_clip_alert(match_id, gif_path.stat().st_mtime)
    current = meta.get("form") if isinstance(meta.get("form"), dict) else _default_clip_form(alert_match)
    try:
        form = _normalize_clip_form(body, current)
    except ValueError as exc:
        return jsonify({"error": str(exc), "code": "form_invalid"}), 400
    if not form["title"]:
        # The CMS rejects an empty title (code=1); fail here so nothing is queued.
        return jsonify({"error": "请填写标题后再发布", "code": "title_required"}), 400
    event_key = str(meta.get("event_key") or f"manual_{int(gif_path.stat().st_mtime)}")
    match_detail = meta.get("match_detail")
    if not isinstance(match_detail, dict) or not match_detail:
        match_detail = _match_detail_for(match_id)
    elif not (match_detail.get("team_A_id") and match_detail.get("team_B_id")):
        # Clips captured before the catalog card carried team ids: fill them
        # from the live directory so the publisher can pick the exact tag.
        # A finished match is no longer in the directory; name fallback then.
        live_detail = _match_detail_for(match_id)
        match_detail = dict(match_detail)
        for key in ("team_A_id", "team_B_id"):
            if not match_detail.get(key) and live_detail.get(key):
                match_detail[key] = live_detail[key]
    minute, minute_extra = _split_match_time(form["match_time"])
    # The CMS rejects a second gif article with the same match_time (platform
    # code=3 创建失败, seen 2026-10-22 02:08 on 54493251). Same-minute events do
    # happen, so report the clash and let the page ask; ``force`` bypasses.
    if form["match_time"] and not body.get("force"):
        clash = _published_clip_with_match_time(match_id, form["match_time"], exclude_event_key=event_key)
        if clash is not None:
            return (
                jsonify(
                    {
                        "error": f"本场 {form['match_time']}' 已有文章（id {clash.get('article_id')}），懂球帝后台通常会拒绝重复",
                        "code": "duplicate_match_time",
                        "match_time": form["match_time"],
                        "article_id": clash.get("article_id"),
                        "draft_url": clash.get("draft_url"),
                        "event_key": clash.get("event_key"),
                    }
                ),
                409,
            )
    event = {
        "event_key": event_key,
        "code": "RC" if form["match_event"] == 4 else "G",
        # The operator picked the CMS event type on the page; send it as-is.
        "match_event": form["match_event"],
        "title_override": form["title"],
        "team": alert_match.get("team") or "",
        "minute": minute,
        "minute_extra": minute_extra,
        "score": form["match_score"],
        "alert_match_mode": alert_match.get("mode"),
        "alert_seq": form.get("alert_seq"),
        # Who pressed 发布; travels with the task to the worker for the Feishu
        # notice and lands in final_event_json for later audit. Not a CMS field.
        "operator": _current_operator(),
        # Where the main GIF was captured, for the Feishu notice label. Extra
        # angles in the group do not change it (one title = its main image).
        "clip_source": _clip_source_of(meta),
    }
    status: str | None = None
    publish_error: str | None = None
    try:
        task = manual_draft_queue.enqueue(
            match_id=match_id,
            event=event,
            match_detail=match_detail,
            source_path=gif_path,
            extra_source_paths=group_paths[1:],
            artifact_kind=MANUAL_ARTIFACT_KIND,
            delivery_mode=DRAFT_DELIVERY_MODE,
            artifact_result=None,
        )
        status = task.get("status")
    except Exception as exc:  # noqa: BLE001 - GIF stays on disk, operator can retry
        publish_error = str(exc)
    # Remember what was actually sent so the published row shows it after reload.
    if meta:
        meta["form"] = form
        _write_clip_meta(gif_path, meta)
    # Once sent, nobody should keep appending angles to this row.
    clip_append.clear(match_id, name=name)
    print(
        json.dumps(
            {
                "event": "manual_draft_enqueue",
                "match_id": match_id,
                "clip": name,
                "attachments": [p.name for p in group_paths[1:]],
                "event_key": event_key,
                "form": form,
                "delivery_mode": _manual_delivery_mode(),
                "status": status,
                "error": publish_error,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    if publish_error:
        return jsonify({"error": publish_error, "event_key": event_key}), 500
    return jsonify(
        {
            "ok": True,
            "match_id": match_id,
            "name": name,
            "event_key": event_key,
            "status": status,
            "delivery_mode": _manual_delivery_mode(),
        }
    )


@app.post("/api/monitor/clip/discard")
def monitor_clip_discard():
    """Mark a captured GIF as abandoned, or restore it.

    The file stays on disk either way: this only flips a ``discarded`` flag in the
    sidecar so the page can keep the row (greyed out, read-only, with a 恢复
    button) instead of leaving a bad clip stuck in the "待发布" list forever.
    Reversible, and it never touches the CMS.
    """
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    name = str(body.get("name") or "").strip()
    if not _CLIP_NAME_RE.fullmatch(name):
        return jsonify({"error": "无效的 GIF 文件名"}), 400
    gif_path = _clip_dir(match_id) / name
    if not gif_path.is_file():
        return jsonify({"error": "GIF 不存在或已被清理", "code": "clip_missing"}), 404
    restore = bool(body.get("restore"))
    # A clip already sent to the CMS has a draft record; abandoning it would only
    # hide a row that is already published, so refuse. 恢复 is always allowed.
    if not restore and manual_draft_queue is not None:
        record = manual_draft_queue.records_by_source_name(
            match_id, artifact_kind=MANUAL_ARTIFACT_KIND
        ).get(name)
        if record is not None:
            return jsonify({"error": "该 GIF 已提交发布，不能放弃", "code": "already_published"}), 409
    meta = _read_clip_meta(gif_path)
    parent = _clip_parent(meta)
    if parent and (_clip_dir(match_id) / parent).is_file():
        # The group follows its main GIF (放弃 the main hides the whole group);
        # a single angle leaves the group with 拆出 first.
        return jsonify({"error": "该 GIF 已并入其他标题，请先拆出", "code": "clip_attached"}), 409
    if restore:
        meta.pop("discarded", None)
    else:
        meta["discarded"] = True
        clip_append.clear(match_id, name=name)
    _write_clip_meta(gif_path, meta)
    return jsonify({"ok": True, "match_id": match_id, "name": name, "discarded": not restore})


@app.post("/api/monitor/clip/append")
def monitor_clip_append():
    """Arm or disarm append mode: 「再截一个视角」 / 「完成」.

    ``{"match_id", "name"}`` makes ``name``'s group the target of this
    operator's next captures (stage and extension alike); ``{"match_id",
    "clear": true}`` turns it off. The target must be an unpublished,
    non-discarded main GIF with room left.
    """
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    operator = _append_operator_key()
    if body.get("clear"):
        clip_append.clear(match_id, operator)
        return jsonify({"ok": True, "match_id": match_id, "append": None})
    name = str(body.get("name") or "").strip()
    if not _CLIP_NAME_RE.fullmatch(name):
        return jsonify({"error": "无效的 GIF 文件名"}), 400
    resolved = _clip_group_main(match_id, name)
    if resolved is None:
        return jsonify({"error": "GIF 不存在或已被清理", "code": "clip_missing"}), 404
    main, main_meta = resolved
    reason = _clip_group_open_reason(match_id, main, main_meta)
    if reason:
        return jsonify({"error": _CLIP_GROUP_REASON_TEXT.get(reason, reason), "code": reason}), 409
    clip_append.set(match_id, operator, main)
    print(
        json.dumps({"event": "monitor_clip_append", "match_id": match_id, "clip": main, "operator": operator}, ensure_ascii=False),
        flush=True,
    )
    return jsonify({"ok": True, "match_id": match_id, "append": _append_target(match_id)})


def _clip_capture_order(name: str) -> str:
    # ``clip_YYYYMMDD-HHMMSS_xxxxxx.gif``: the name sorts by capture time.
    return name


@app.post("/api/monitor/clip/group")
def monitor_clip_group():
    """Edit which GIFs share one title: ``attach`` / ``detach`` / ``set_main``.

    ``attach`` folds ``name`` (and anything already attached to it) under
    ``target``; ``detach`` makes an attached ``name`` its own row again;
    ``set_main`` promotes an attached ``name`` to be the group's main GIF
    (the one whose form, cover and publish record the article uses). Only
    unpublished, non-discarded GIFs of the same match take part, and a group
    never exceeds ``CLIP_GROUP_MAX`` GIFs.
    """
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    action = str(body.get("action") or "").strip()
    if action not in {"attach", "detach", "set_main"}:
        return jsonify({"error": "action 必须是 attach / detach / set_main"}), 400
    name = str(body.get("name") or "").strip()
    if not _CLIP_NAME_RE.fullmatch(name):
        return jsonify({"error": "无效的 GIF 文件名"}), 400
    directory = _clip_dir(match_id)
    gif_path = directory / name
    if not gif_path.is_file():
        return jsonify({"error": "GIF 不存在或已被清理", "code": "clip_missing"}), 404
    records: dict[str, dict[str, Any]] = {}
    if manual_draft_queue is not None:
        records = manual_draft_queue.records_by_source_name(match_id, artifact_kind=MANUAL_ARTIFACT_KIND)
    meta = _read_clip_meta(gif_path)
    parent = _clip_parent(meta)
    if parent and not (directory / parent).is_file():
        parent = None

    if action == "attach":
        target = str(body.get("target") or "").strip()
        if not _CLIP_NAME_RE.fullmatch(target):
            return jsonify({"error": "无效的目标 GIF 文件名"}), 400
        main, reason = _attach_clip(match_id, name, target)
        if main is None:
            if reason == "group_self":
                return jsonify({"error": "不能并入自己", "code": reason}), 400
            status = 404 if reason == "clip_missing" else 409
            return jsonify({"error": _CLIP_GROUP_REASON_TEXT.get(reason or "", reason), "code": reason}), status
    else:
        if not parent:
            return jsonify({"error": "该 GIF 不是附属，无需拆出", "code": "not_attached"}), 409
        parent_path = directory / parent
        parent_meta = _read_clip_meta(parent_path)
        if parent in records:
            return jsonify({"error": "已提交发布的标题不能再调整", "code": "already_published"}), 409
        siblings = [n for n in _clip_attachments(parent_meta) if n != name and (directory / n).is_file()]
        if action == "detach":
            parent_meta["attachments"] = siblings
            _write_clip_meta(parent_path, parent_meta)
            meta.pop("parent", None)
            _write_clip_meta(gif_path, meta)
            main = name
        else:  # set_main
            # The article identity (form, event key, matched push) stays with the
            # group, so it moves onto the promoted GIF; the old main keeps the
            # promoted GIF's own capture-time values in case it is detached later.
            for key in _CLIP_GROUP_META_KEYS:
                parent_value, own_value = parent_meta.get(key), meta.get(key)
                if parent_value is not None:
                    meta[key] = parent_value
                else:
                    meta.pop(key, None)
                if own_value is not None:
                    parent_meta[key] = own_value
                else:
                    parent_meta.pop(key, None)
            meta["attachments"] = sorted([parent, *siblings], key=_clip_capture_order)
            meta.pop("parent", None)
            parent_meta.pop("attachments", None)
            parent_meta["parent"] = name
            _write_clip_meta(gif_path, meta)
            _write_clip_meta(parent_path, parent_meta)
            for sibling in siblings:
                sibling_path = directory / sibling
                sibling_meta = _read_clip_meta(sibling_path)
                sibling_meta["parent"] = name
                _write_clip_meta(sibling_path, sibling_meta)
            clip_append.retarget(match_id, parent, name)
            main = name
    print(
        json.dumps(
            {"event": "monitor_clip_group", "match_id": match_id, "action": action, "clip": name, "main": main},
            ensure_ascii=False,
        ),
        flush=True,
    )
    return jsonify(
        {"ok": True, "match_id": match_id, "name": name, "main": main, "action": action, "append": _append_target(match_id)}
    )


@app.post("/api/monitor/clip/retry")
def monitor_clip_retry():
    """Re-queue a failed or backing-off clip publish right now.

    Only ``failed`` / ``retry_wait`` rows move; anything else (queued,
    publishing, published) is returned unchanged so a double click is harmless.
    """
    body = request.get_json(silent=True) or {}
    try:
        match_id = validate_match_id(str(body.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    event_key = str(body.get("event_key") or "").strip()
    if not event_key:
        return jsonify({"error": "缺少 event_key"}), 400
    if manual_draft_queue is None:
        return jsonify({"error": "发文未启用（GIF 仅本地保存）", "code": "publish_disabled"}), 409
    record = manual_draft_queue.retry(
        match_id=match_id, event_key=event_key, artifact_kind=MANUAL_ARTIFACT_KIND
    )
    if record is None:
        return jsonify({"error": "没有找到对应的发布任务", "code": "task_missing"}), 404
    print(
        json.dumps(
            {
                "event": "manual_draft_retry",
                "match_id": match_id,
                "event_key": event_key,
                "status": record.get("status"),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return jsonify({"ok": True, "match_id": match_id, "event_key": event_key, "status": record.get("status")})


@app.get("/api/monitor/publish/status")
def monitor_publish_status():
    if manual_draft_queue is None:
        return jsonify({"enabled": False, "records": {}})
    match_id = str(request.args.get("match_id") or "").strip()
    records: dict[str, Any] = {}
    if match_id:
        try:
            match_id = validate_match_id(match_id)
            records = manual_draft_queue.records_for_match(
                match_id, artifact_kind=MANUAL_ARTIFACT_KIND
            )
        except ValueError:
            records = {}
    return jsonify(
        {
            "enabled": True,
            "queue": manual_draft_queue.status(),
            "records": records,
        }
    )


@app.get("/api/monitor/publish/records")
def monitor_publish_records():
    """Delivered manual clips in ``[since, until)`` (unix seconds), for the
    daily GIF latency report. Token-only (no Feishu login), read-only."""
    if not REPORT_TOKEN:
        abort(404)
    if not hmac.compare_digest(request.headers.get("X-Report-Token", ""), REPORT_TOKEN):
        return jsonify({"error": "unauthorized"}), 401
    try:
        since = float(request.args.get("since", ""))
        until = float(request.args.get("until", ""))
    except ValueError:
        return jsonify({"error": "since/until 必须是 unix 秒"}), 400
    if not since < until or until - since > REPORT_MAX_RANGE_SECONDS:
        return jsonify({"error": "时间范围无效或超过 3 天"}), 400
    # Goal / red-card pushes received in the window: their ``ts_unix`` is the
    # data-source moment for events the CMS change log does not timestamp.
    alerts = alert_store.between(since, until)
    if manual_draft_queue is None:
        return jsonify({"enabled": False, "records": [], "alerts": alerts})
    records = manual_draft_queue.delivered_between(since, until, artifact_kind=MANUAL_ARTIFACT_KIND)
    return jsonify({"enabled": True, "since": since, "until": until, "records": records, "alerts": alerts})


@app.get("/api/monitor/clips")
def monitor_clips():
    try:
        match_id = validate_match_id(str(request.args.get("match_id") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify(
        {
            "match_id": match_id,
            "publish_enabled": manual_draft_queue is not None,
            "score_event_types": sorted(SCORE_EVENT_TYPES),
            "group_max": CLIP_GROUP_MAX,
            "append_target": _append_target(match_id),
            "clips": _list_clips(match_id),
        }
    )


@app.get("/clips/<match_id>/<path:filename>")
def clip_file(match_id, filename):
    try:
        match_id = validate_match_id(match_id)
    except ValueError:
        abort(404)
    if Path(filename).name != filename or not filename.endswith(".gif"):
        abort(404)
    directory = _clip_dir(match_id)
    if not (directory / filename).is_file():
        abort(404)
    return send_from_directory(directory, filename, mimetype="image/gif")


def _exit_on_sigterm(signum, frame):
    """Turn SIGTERM into a normal interpreter exit.

    Python's default SIGTERM action kills the process without running ``atexit``
    handlers, so ``live_preview.shutdown`` never ran on ``systemctl stop`` /
    ``deploy-gif`` / ``kill -TERM``: the preview ffmpeg children were orphaned,
    kept writing segments to disk, and the restarted server (which has no
    entry for them) answered 409 "请先开始预览该场" to every clip request.
    """
    raise SystemExit(0)


# ---- Browser-extension clip ingest (feature-flagged, see monitor_extension.py) ----
# Off by default: the routes are not registered at all, so the console behaves
# exactly as before. Turn on with GIF_MONITOR_EXTENSION_ENABLED=1 in .env.
from monitor_extension import EXTENSION_ENABLED, ExtensionHooks, install_extension_routes  # noqa: E402

ext_source_store = None
if EXTENSION_ENABLED:
    ext_source_store = install_extension_routes(
        app,
        ExtensionHooks(
            clip_dir=_clip_dir,
            clip_video_filter=_clip_video_filter,
            parse_clip_crop=_parse_clip_crop,
            write_clip_meta=_write_clip_meta,
            read_clip_meta=_read_clip_meta,
            match_clip_alert=_match_clip_alert,
            default_clip_form=_default_clip_form,
            alert_match_public=_alert_match_public,
            match_detail_for=_match_detail_for,
            validate_match_id=validate_match_id,
            clip_name_valid=lambda name: _CLIP_NAME_RE.fullmatch(name) is not None,
            resolve_ffmpeg=resolve_ffmpeg,
            clip_range_max_seconds=CLIP_RANGE_MAX_SECONDS,
            publish_enabled=lambda: manual_draft_queue is not None,
            delivery_mode=_manual_delivery_mode,
            take_append_target=_take_append_target,
            restore_append_target=_restore_append_target,
            attach_pending_clip=_attach_pending_clip,
        ),
        clip_output_root=CLIP_OUTPUT_ROOT,
    )


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _exit_on_sigterm)
    print(f"Match monitor console: http://{HOST}:{PORT}")
    # One greppable line with the effective latency knobs (and anything that
    # was clamped), so a deploy can be checked against .env at a glance.
    print(
        json.dumps(
            {"event": "monitor_latency_config", "webrtc_enabled": webrtc_gateway.enabled, **LATENCY_CONFIG},
            ensure_ascii=False,
        ),
        flush=True,
    )
    if webrtc_gateway.enabled:
        # A previous 8901 that crashed (no atexit) leaves its match paths in
        # mediamtx pulling the upstream for nobody; drop them before serving.
        stale = webrtc_gateway.cleanup_stale_paths()
        print(
            json.dumps(
                {
                    "event": "webrtc_gateway_startup",
                    "healthy": webrtc_gateway.healthy(),
                    "stale_paths_removed": stale,
                    "error": webrtc_gateway.last_error,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
