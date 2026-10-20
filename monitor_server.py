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
import json
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.request
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, abort, jsonify, request, send_from_directory

from dqd_api import MATCH_STATUS_LABELS, positive_env_float, positive_env_int, validate_match_id
from source_client import classify_source_response, query_source
from match_catalog import MATCH_CATALOG_EXCLUDED_COMPETITION_NAMES, MatchCatalog
from live_preview import LivePreviewManager, resolve_ffmpeg
from article_draft_queue import (
    ArticleDraftQueue,
    DRAFT_DELIVERY_MODE,
    MANUAL_ARTIFACT_KIND,
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

HOST = os.environ.get("GIF_MONITOR_HOST", "127.0.0.1")
PORT = int(os.environ.get("GIF_MONITOR_PORT", "8901"))
# Keep enough rolling HLS buffer that a backward capture always has material.
CLIP_BEFORE_SECONDS = positive_env_float("GIF_MONITOR_CLIP_SECONDS", 10.0)
# Operator-chosen [start, end] range capture: longest allowed range, and how long
# a chosen start stays pinned (segments hard-linked) while the end is picked.
CLIP_RANGE_MAX_SECONDS = positive_env_float("GIF_MONITOR_CLIP_RANGE_MAX_SECONDS", 30.0)
CLIP_PIN_TTL_SECONDS = positive_env_float("GIF_MONITOR_CLIP_PIN_TTL_SECONDS", 180.0)
CLIP_GIF_WIDTH = positive_env_int("GIF_MONITOR_CLIP_WIDTH", 440)
# 50/3 ≈ 16.67, matching the automatic pipeline's GIF frame rate. Float rather
# than int so a non-integer rate like this one is expressible here and in .env.
CLIP_GIF_FPS = positive_env_float("GIF_MONITOR_CLIP_FPS", 50 / 3)
# ~2s segments. The operator often watches tens of seconds behind the live edge
# (hls.js back-buffer), so retain a generous on-disk history — enough to cover
# that lag plus the capture window — or the requested frames are already gone.
PREVIEW_LIST_SIZE = positive_env_int("GIF_MONITOR_PREVIEW_SEGMENTS", 90)
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
# Optional shared secret for the monitor→8901 loopback call. Empty = no check.
ALERT_INGEST_TOKEN = os.environ.get("GIF_MONITOR_INGEST_TOKEN", "").strip()
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


live_preview = LivePreviewManager(
    list_size=PREVIEW_LIST_SIZE,
    idle_timeout_seconds=positive_env_float("GIF_PREVIEW_IDLE_TIMEOUT_SECONDS", 120.0),
    # Auto-recover a preview whose RTMP source briefly drops while an operator is
    # still watching, so the tile reconnects itself instead of showing stale frames.
    auto_restart=os.environ.get("GIF_MONITOR_AUTO_RESTART", "true").strip().lower()
    not in {"0", "false", "no", "off"},
    restart_base_delay=positive_env_float("GIF_MONITOR_RESTART_BASE_DELAY", 5.0),
    restart_max_delay=positive_env_float("GIF_MONITOR_RESTART_MAX_DELAY", 60.0),
    restart_max_retries=positive_env_int("GIF_MONITOR_RESTART_MAX_RETRIES", 6),
)
match_catalog = MatchCatalog()
atexit.register(live_preview.shutdown)


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
            environment_boolean,
        )
        from publish_account_pool import PublishAccountPool
        from open_platform_client import OpenPlatformClient, OpenPlatformConfig

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
        )
        queue = ArticleDraftQueue(
            database_path=publisher.database_path,
            publisher=publisher,
            allowed_output_root=CLIP_OUTPUT_ROOT,
            poll_seconds=positive_env_float("GIF_MONITOR_DRAFT_POLL_SECONDS", 2.0),
            lease_seconds=positive_env_float("GIF_MONITOR_DRAFT_LEASE_SECONDS", 180.0),
            person_wait_seconds=positive_env_float("GIF_MONITOR_PERSON_WAIT_SECONDS", 5.0),
            claim_artifact_kinds=(MANUAL_ARTIFACT_KIND,),
            manual_delivery_mode=_manual_delivery_mode(),
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


def _encode_clip_gif(
    segments: list[tuple[Path, float, float | None]],
    before_seconds: float,
    output_path: Path,
    anchor_epoch: float | None = None,
    start_epoch: float | None = None,
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
            f"fps={CLIP_GIF_FPS:g},scale={CLIP_GIF_WIDTH}:-2:flags=lanczos,"
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


def _list_clips(match_id: str) -> list[dict[str, Any]]:
    """Newest-first GIFs for a match, joined to their draft records when any."""
    directory = _clip_dir(match_id)
    try:
        gifs = [p for p in directory.iterdir() if p.is_file() and p.suffix == ".gif" and p.name.startswith("clip_")]
        stats = {p: p.stat() for p in gifs}
    except OSError:
        return []
    gifs.sort(key=lambda p: stats[p].st_mtime, reverse=True)
    gifs = gifs[:CLIP_LIST_LIMIT]
    drafts: dict[str, dict[str, Any]] = {}
    if manual_draft_queue is not None and gifs:
        drafts = manual_draft_queue.records_by_source_name(match_id, artifact_kind=MANUAL_ARTIFACT_KIND)
    items: list[dict[str, Any]] = []
    for gif in gifs:
        stat = stats[gif]
        record = drafts.get(gif.name)
        items.append(
            {
                "name": gif.name,
                "clip_url": f"clips/{match_id}/{gif.name}",
                "bytes": stat.st_size,
                "created_unix": stat.st_mtime,
                "event_key": record.get("event_key") if record else None,
                "draft_status": record.get("status") if record else None,
                "draft_url": record.get("draft_url") if record else None,
                "draft_error": record.get("error") if record else None,
                "draft_retriable": bool(record.get("retriable")) if record else False,
            }
        )
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
        with self._lock:
            for pin_id in list(self._pins):
                self._drop_locked(pin_id)


clip_pins = _ClipPinStore(CLIP_PIN_TTL_SECONDS, CLIP_RANGE_MAX_SECONDS)
atexit.register(clip_pins.shutdown)


@app.get("/")
def index():
    return send_from_directory(ROOT / "monitor_static", "index.html")


@app.get("/api/health")
def health():
    return jsonify({"ok": True, "port": PORT, "service": "monitor", "time_unix": time.time()})


@app.get("/api/monitor/matches")
def monitor_matches():
    snapshot = match_catalog.snapshot()
    playing = []
    match_ids: list[str] = []
    for item in snapshot.get("playing", []):
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "")
        playing.append(
            {
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
        )
        match_ids.append(str(item.get("match_id") or ""))
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
        }
    )


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
        return jsonify({"error": str(exc), "code": "no_source"}), 409
    except Exception as exc:
        return jsonify({"error": f"查询直播源失败：{exc}", "code": "source_error"}), 502
    if not resource:
        return jsonify({"error": "该比赛暂未配置直播源", "code": "no_source"}), 409
    try:
        status = live_preview.start(match_id, resource)
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
        }
    )


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


def _match_detail_for(match_id: str) -> dict[str, Any]:
    """Pull team names/ids for the title from the live catalog snapshot."""
    for item in match_catalog.snapshot().get("playing", []):
        if isinstance(item, dict) and str(item.get("match_id") or "") == str(match_id):
            return {
                "team_A_name": item.get("team_A_name"),
                "team_B_name": item.get("team_B_name"),
                "team_A_id": item.get("team_A_id"),
                "team_B_id": item.get("team_B_id"),
                "competition_name": item.get("competition_name"),
            }
    return {}


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
    for item in match_catalog.snapshot().get("playing", []):
        if isinstance(item, dict) and str(item.get("match_id") or "") == str(match_id):
            fs_a, fs_b = item.get("fs_A"), item.get("fs_B")
            score = f"{fs_a}-{fs_b}" if fs_a not in (None, "") and fs_b not in (None, "") else ""
            minute = str(item.get("minute") or "").strip()
            extra = str(item.get("minute_extra") or "").strip()
            return {"minute": minute if minute.isdigit() else "", "minute_extra": extra, "score": score}
    return {}


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


def _send_feishu_text(text: str) -> bool:
    """POST a plain-text message to the configured Feishu bot webhook.

    Returns whether the push succeeded; never raises so a webhook outage cannot
    break event ingestion. No webhook configured means page-only alerts.
    """
    if not FEISHU_WEBHOOK:
        return False
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode("utf-8")
    request_obj = urllib.request.Request(
        FEISHU_WEBHOOK,
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
    if ALERT_INGEST_TOKEN and request.headers.get("X-Monitor-Token", "") != ALERT_INGEST_TOKEN:
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
    if not live_preview.status(match_id).get("running"):
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
    person = str(body.get("person") or "").strip()[:40]
    title = str(body.get("title") or "").strip()[:100]
    if not live_preview.status(match_id).get("running"):
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
    try:
        _encode_clip_gif(segments, before_seconds, output_path, anchor_epoch, start_epoch)
    except Exception as exc:
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
    draft: dict[str, Any] | None = None
    publish_error: str | None = None
    if manual_draft_queue is not None:
        event_key = f"manual_{int(anchor_epoch if anchor_epoch is not None else time.time())}"
        event = {
            "event_key": event_key,
            # ``code`` only drives the generated title's wording; the CMS
            # ``match_event`` is left for the operator (see manual_match_event).
            "code": alert_match["code"] or "G",
            "manual_match_event": True,
            "person": person,
            "title_override": title,
            "team": alert_match["team"],
            "minute": alert_match["minute"],
            "minute_extra": alert_match["minute_extra"],
            "score": alert_match["score"],
            "alert_match_mode": alert_match["mode"],
        }
        try:
            task = manual_draft_queue.enqueue(
                match_id=match_id,
                event=event,
                match_detail=_match_detail_for(match_id),
                source_path=output_path,
                artifact_kind=MANUAL_ARTIFACT_KIND,
                delivery_mode=DRAFT_DELIVERY_MODE,
                artifact_result=None,
            )
            draft = {"event_key": event_key, "status": task.get("status")}
        except Exception as exc:  # noqa: BLE001 - GIF is already saved locally
            publish_error = str(exc)
        print(
            json.dumps(
                {
                    "event": "manual_draft_enqueue",
                    "match_id": match_id,
                    "event_key": event_key,
                    "person": bool(person),
                    "title_override": bool(title),
                    "delivery_mode": _manual_delivery_mode(),
                    "status": (draft or {}).get("status"),
                    "error": publish_error,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
    return jsonify(
        {
            "ok": True,
            "match_id": match_id,
            "before_seconds": before_seconds,
            "anchored": anchored,
            "clip_url": f"clips/{match_id}/{output_path.name}",
            "bytes": output_path.stat().st_size,
            "publish_enabled": manual_draft_queue is not None,
            "delivery_mode": _manual_delivery_mode(),
            "draft": draft,
            "publish_error": publish_error,
            "alert_match": {
                "mode": alert_match["mode"],
                "candidates": alert_match["candidates"],
                "ambiguous": alert_match["ambiguous"],
                "weak": alert_match["weak"],
                "match_time": (
                    f"{alert_match['minute']}+{alert_match['minute_extra']}"
                    if alert_match["minute"] and alert_match["minute_extra"] not in ("", "0")
                    else alert_match["minute"]
                ),
                "score": alert_match["score"],
            },
        }
    )


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


if __name__ == "__main__":
    print(f"Match monitor console: http://{HOST}:{PORT}")
    app.run(host=HOST, port=PORT, threaded=True, debug=False)
