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
import json
import os
import re
import shutil
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
ALERT_BUFFER_SIZE = positive_env_int("GIF_MONITOR_ALERT_BUFFER", 50)
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
    """

    def __init__(self, capacity: int) -> None:
        self._lock = threading.Lock()
        self._events: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._seen: set[str] = set()
        self._seen_order: deque[str] = deque(maxlen=512)
        self._seq = 0

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
            return stored

    def since(self, cursor: int) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            events = [dict(item) for item in self._events if item["seq"] > cursor]
            return events, self._seq


alert_store = _AlertStore(ALERT_BUFFER_SIZE)
# Regenerated every process start. The browser watches this to detect an 8901
# restart (which resets the in-memory ``seq`` to 0) and re-baseline its cursor,
# so an already-open page keeps receiving alerts across deploys.
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
) -> tuple[list[tuple[Path, float, float | None]], float, float]:
    """Return (kept_segments, start_offset, window) for the GIF cut.

    When ``anchor_epoch`` is given and every segment carries a PROGRAM-DATE-TIME,
    the cut is anchored on the exact displayed frame (``[anchor-before, anchor]``)
    so pipeline delay never shifts the result. Otherwise it falls back to the
    trailing ``before_seconds`` of the rolling buffer.
    """
    pdts_ok = bool(segments) and all(seg[2] is not None for seg in segments)
    if anchor_epoch is not None and pdts_ok:
        last_end = segments[-1][2] + segments[-1][1]
        # Clamp a slightly future anchor (clock skew) to the latest frame we have.
        anchor = min(anchor_epoch, last_end)
        want_start = anchor - before_seconds
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
) -> None:
    """Concatenate recent segments and encode the target window to a GIF."""
    kept, start_offset, window = _select_clip_window(segments, before_seconds, anchor_epoch)
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
    return jsonify(
        {
            "playing": playing,
            "health": snapshot.get("health", {}),
            "max_previews": MAX_PREVIEWS,
            "clip_before_seconds": CLIP_BEFORE_SECONDS,
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
    person = str(body.get("person") or "").strip()[:40]
    title = str(body.get("title") or "").strip()[:100]
    if not live_preview.status(match_id).get("running"):
        return jsonify({"error": "请先开始预览该场，才能回捞画面", "code": "preview_not_running"}), 409
    segments = _read_playlist_segments(live_preview.media_dir(match_id))
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
        _encode_clip_gif(segments, before_seconds, output_path, anchor_epoch)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
    print(
        json.dumps(
            {
                "event": "monitor_clip",
                "match_id": match_id,
                "before_seconds": before_seconds,
                "anchored": anchored,
                "anchor_epoch_ms": None if anchor_epoch is None else round(anchor_epoch * 1000),
                "segments": len(segments),
                "with_pdt": sum(1 for seg in segments if seg[2] is not None),
                "clip": output_path.name,
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
            "code": "G",
            "person": person,
            "title_override": title,
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
            "clip_url": f"/clips/{match_id}/{output_path.name}",
            "bytes": output_path.stat().st_size,
            "publish_enabled": manual_draft_queue is not None,
            "delivery_mode": _manual_delivery_mode(),
            "draft": draft,
            "publish_error": publish_error,
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
