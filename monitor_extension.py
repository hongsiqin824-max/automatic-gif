"""Browser-extension clip ingest for the 8901 monitor (feature-flagged).

The clipper extension records the ``<video>`` of whatever live page the
operator is watching (``captureStream`` + segmented ``MediaRecorder``) and
uploads the webm segments covering an operator-chosen ``[start, end]``. This
module turns them into a GIF that is indistinguishable from one captured by
``/api/monitor/clip``: same file name pattern, same sidecar fields, same
response shape, so listing / publishing / discarding need no changes.

Everything here is installed only when ``GIF_MONITOR_EXTENSION_ENABLED`` is
on. With the flag off the routes do not exist (404) and nothing else in the
monitor changes. ``monitor_server.py`` wires it in with one call at the end.
"""

from __future__ import annotations

import atexit
import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from flask import Flask, jsonify, request

from dqd_api import positive_env_float, positive_env_int

# ---- configuration -------------------------------------------------------


def _env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off"}


EXTENSION_ENABLED = _env_flag("GIF_MONITOR_EXTENSION_ENABLED", False)
# Raw webm kept after the GIF is made so the operator can re-crop; reaped after.
EXT_SOURCE_TTL_SECONDS = positive_env_float("GIF_MONITOR_EXT_SOURCE_TTL_SECONDS", 600.0)
EXT_UPLOAD_MAX_BYTES = positive_env_int("GIF_MONITOR_EXT_UPLOAD_MAX_MB", 64) * 1024 * 1024
# Handed to the extension via /config so a .env change reaches every operator
# without reinstalling. 300 s was the user's choice on 2026-10-27.
EXT_BUFFER_SECONDS = positive_env_int("GIF_MONITOR_EXT_BUFFER_SECONDS", 300)
EXT_RECORD_BITRATE = positive_env_int("GIF_MONITOR_EXT_RECORD_BITRATE", 2_000_000)
EXT_MAX_FILES = 4
EXT_REAPER_INTERVAL_SECONDS = 30.0
# Bumped when the request/response contract changes; the extension compares
# it with its own manifest version and shows "有新版本".
EXT_PROTOCOL_VERSION = "0.1.0"
EXT_SOURCE_DIR_PREFIX = ".ext_src_"
EBML_MAGIC = b"\x1a\x45\xdf\xa3"
_FFMPEG_SLOTS = threading.Semaphore(2)


@dataclass(frozen=True)
class ExtensionHooks:
    """Everything the routes borrow from ``monitor_server`` (no import cycle)."""

    clip_dir: Callable[[str], Path]
    clip_video_filter: Callable[[dict[str, float] | None], str]
    parse_clip_crop: Callable[[Any], dict[str, float] | None]
    write_clip_meta: Callable[[Path, dict[str, Any]], None]
    read_clip_meta: Callable[[Path], dict[str, Any]]
    match_clip_alert: Callable[[str, float], dict[str, Any]]
    # (alert_match, match_id): the match id lets the form know which pushes
    # were already published, so a replay clip is not pre-filled as 进球.
    default_clip_form: Callable[[dict[str, Any], str], dict[str, Any]]
    alert_match_public: Callable[[dict[str, Any]], dict[str, Any]]
    match_detail_for: Callable[[str], dict[str, Any]]
    validate_match_id: Callable[[str], str]
    clip_name_valid: Callable[[str], bool]
    resolve_ffmpeg: Callable[[], str]
    clip_range_max_seconds: float
    publish_enabled: Callable[[], bool]
    delivery_mode: Callable[[], str]
    # Append mode (「再截一个视角」 armed on the 8901 page). The target is
    # claimed *before* the slow encode and given back if encoding fails, so a
    # second upload already in flight never lands in the same group. Optional
    # so older test fixtures still build.
    take_append_target: Callable[[str], dict[str, Any] | None] | None = None
    restore_append_target: Callable[[str, dict[str, Any] | None], None] | None = None
    attach_pending_clip: Callable[[str, Path, dict[str, Any] | None], dict[str, Any] | None] | None = None


# ---- pure helpers (unit-tested without Flask) ----------------------------


def is_webm_head(head: bytes) -> bool:
    return head[:4] == EBML_MAGIC


def parse_segments_json(raw: str, file_count: int) -> list[dict[str, float]]:
    """``[{start_epoch_ms, duration_ms}, ...]`` in upload order; ``ValueError`` on bad input."""
    try:
        parsed = json.loads(raw or "")
    except ValueError as exc:
        raise ValueError("segments_json 不是合法 JSON") from exc
    if not isinstance(parsed, list) or not parsed:
        raise ValueError("segments_json 必须是非空数组")
    if len(parsed) != file_count:
        raise ValueError(f"segments_json 有 {len(parsed)} 项，但上传了 {file_count} 个文件")
    segments: list[dict[str, float]] = []
    for index, item in enumerate(parsed):
        if not isinstance(item, dict):
            raise ValueError(f"segments_json[{index}] 必须是对象")
        try:
            start = float(item.get("start_epoch_ms"))
            duration = float(item.get("duration_ms"))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"segments_json[{index}] 缺 start_epoch_ms / duration_ms") from exc
        if duration <= 0 or start != start or duration != duration:
            raise ValueError(f"segments_json[{index}] 时长必须大于 0")
        if segments and start < segments[-1]["start_epoch_ms"]:
            raise ValueError("segments_json 必须按开始时间升序")
        segments.append({"start_epoch_ms": start, "duration_ms": duration})
    return segments


def compute_trim(
    segments: list[dict[str, float]], start_ms: float, end_ms: float
) -> tuple[float, float]:
    """``(-ss, -t)`` in seconds for the concatenated upload.

    The extension already uploads only the segments it needs; the server just
    needs the offset of ``start`` inside the first one. A start more than 1 s
    before the first segment means the extension's buffer had already rolled
    it out: refuse rather than return the wrong moment.
    """
    first_start = segments[0]["start_epoch_ms"]
    last = segments[-1]
    last_end = last["start_epoch_ms"] + last["duration_ms"]
    if start_ms < first_start - 1000:
        raise ValueError("起点早于上传的画面，缓冲里已经没有这一刻")
    if end_ms > last_end + 1000:
        raise ValueError("终点晚于上传的画面")
    ss = max(0.0, (start_ms - first_start) / 1000.0)
    window = (end_ms - start_ms) / 1000.0
    return round(ss, 3), round(window, 3)


def encode_from_webm(
    ffmpeg: str,
    sources: list[Path],
    ss: float,
    window: float,
    video_filter: str,
    output_path: Path,
    timeout: float = 120.0,
) -> None:
    """Concat the uploaded webm segments and encode ``[ss, ss+window]`` to a GIF."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    concat_list = output_path.parent / f".concat_{uuid.uuid4().hex}.txt"
    concat_list.write_text(
        "".join(f"file '{src.resolve()}'\n" for src in sources), encoding="utf-8"
    )
    # Cut at the head of the graph, not with output-side -ss/-t: those apply
    # after the graph, and palettegen only emits at end of input, so every
    # uploaded frame (whole 30 s segments) was scaled and fed to the palette.
    # Same timeline and frame count, a fraction of the work, and the palette
    # comes from the clip alone.
    trim = f"trim=start={ss:.3f}:duration={window:.3f},setpts=PTS-STARTPTS"
    palette_filter = f"{trim},{video_filter},split[s0][s1];[s0]palettegen[p];[s1][p]paletteuse"
    command = [
        ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
        "-f", "concat", "-safe", "0", "-i", str(concat_list),
        "-filter_complex", palette_filter,
        str(output_path),
    ]
    try:
        with _FFMPEG_SLOTS:
            completed = subprocess.run(
                command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=timeout
            )
    finally:
        concat_list.unlink(missing_ok=True)
    if completed.returncode != 0 or not output_path.is_file():
        detail = completed.stderr.decode("utf-8", "replace")[-400:]
        raise RuntimeError(f"GIF 编码失败：{detail or '未知错误'}")


# ---- raw source retention ------------------------------------------------


class ExtSourceStore:
    """Keeps uploaded webm directories for ``ttl_seconds`` so a clip can be re-cropped.

    Same shape as ``_ClipPinStore``: in-memory registry + periodic reaper +
    startup sweep of directories a crashed process left behind. Directories
    live under ``<clip_dir>/<prefix><uuid>/``; ``_list_clips`` only looks at
    ``clip_*.gif`` files so they never show up in the page.
    """

    def __init__(self, ttl_seconds: float) -> None:
        self.ttl_seconds = float(ttl_seconds)
        self._lock = threading.Lock()
        self._entries: dict[str, float] = {}
        self._stop = threading.Event()
        self._reaper: threading.Thread | None = None

    @staticmethod
    def sweep_orphans(root: Path) -> int:
        removed = 0
        if not root.is_dir():
            return removed
        for directory in root.glob(f"*/{EXT_SOURCE_DIR_PREFIX}*"):
            if directory.is_dir():
                shutil.rmtree(directory, ignore_errors=True)
                removed += 1
        return removed

    def register(self, directory: Path, now: float | None = None) -> None:
        with self._lock:
            self._entries[str(directory)] = (time.time() if now is None else now) + self.ttl_seconds

    def alive(self, directory: Path) -> bool:
        with self._lock:
            return str(directory) in self._entries and directory.is_dir()

    def expire(self, now: float | None = None) -> int:
        current = time.time() if now is None else now
        with self._lock:
            expired = [key for key, deadline in self._entries.items() if deadline <= current]
            for key in expired:
                del self._entries[key]
        for key in expired:
            shutil.rmtree(key, ignore_errors=True)
        return len(expired)

    def start_reaper(self, interval_seconds: float) -> None:
        if self._reaper is not None:
            return
        interval = max(1.0, interval_seconds)

        def _run() -> None:
            while not self._stop.wait(interval):
                self.expire()

        self._reaper = threading.Thread(target=_run, name="ext-source-reaper", daemon=True)
        self._reaper.start()

    def shutdown(self) -> None:
        self._stop.set()
        with self._lock:
            keys = list(self._entries)
            self._entries.clear()
        for key in keys:
            shutil.rmtree(key, ignore_errors=True)


# ---- routes --------------------------------------------------------------


def _log(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str), flush=True)


def _float_field(name: str) -> float:
    raw = request.form.get(name)
    if raw is None or not str(raw).strip():
        raise ValueError(f"缺少 {name}")
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 必须是数字") from exc
    if value != value:
        raise ValueError(f"{name} 必须是数字")
    return value


def install_extension_routes(
    app: Flask,
    hooks: ExtensionHooks,
    *,
    clip_output_root: Path,
    source_store: ExtSourceStore | None = None,
    start_reaper: bool = True,
) -> ExtSourceStore:
    """Register ``/api/monitor/ext/*`` on ``app``. Returns the source store."""
    store = source_store or ExtSourceStore(EXT_SOURCE_TTL_SECONDS)
    if start_reaper:
        removed = ExtSourceStore.sweep_orphans(clip_output_root)
        store.start_reaper(EXT_REAPER_INTERVAL_SECONDS)
        atexit.register(store.shutdown)
        _log("extension_routes_installed", orphans_removed=removed, ttl_seconds=store.ttl_seconds)

    @app.get("/api/monitor/ext/config")
    def monitor_ext_config():
        return jsonify(
            {
                "enabled": True,
                "version": EXT_PROTOCOL_VERSION,
                "clip_range_max_seconds": hooks.clip_range_max_seconds,
                "buffer_seconds": EXT_BUFFER_SECONDS,
                "bitrate": EXT_RECORD_BITRATE,
                "upload_max_bytes": EXT_UPLOAD_MAX_BYTES,
                "max_files": EXT_MAX_FILES,
                "source_ttl_seconds": store.ttl_seconds,
                "publish_enabled": hooks.publish_enabled(),
                "delivery_mode": hooks.delivery_mode(),
            }
        )

    @app.post("/api/monitor/ext/clip")
    def monitor_ext_clip():
        if request.content_length and request.content_length > EXT_UPLOAD_MAX_BYTES:
            return (
                jsonify(
                    {
                        "error": f"上传超过 {EXT_UPLOAD_MAX_BYTES // (1024 * 1024)} MB 上限",
                        "code": "upload_too_large",
                    }
                ),
                413,
            )
        try:
            match_id = hooks.validate_match_id(str(request.form.get("match_id") or ""))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        try:
            start_ms = _float_field("start_epoch_ms")
            end_ms = _float_field("end_epoch_ms")
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        range_seconds = (end_ms - start_ms) / 1000.0
        if range_seconds < 0.5:
            return jsonify({"error": "终点必须在起点之后", "code": "range_invalid"}), 400
        if range_seconds > hooks.clip_range_max_seconds:
            return (
                jsonify(
                    {
                        "error": f"区间最长 {hooks.clip_range_max_seconds:g} 秒，当前 {range_seconds:.1f} 秒",
                        "code": "range_too_long",
                    }
                ),
                400,
            )
        files = request.files.getlist("file")
        if not files:
            return jsonify({"error": "没有上传任何画面文件", "code": "no_files"}), 400
        if len(files) > EXT_MAX_FILES:
            return jsonify({"error": f"最多上传 {EXT_MAX_FILES} 个片段", "code": "too_many_files"}), 400
        try:
            segments = parse_segments_json(request.form.get("segments_json") or "", len(files))
        except ValueError as exc:
            return jsonify({"error": str(exc), "code": "segments_invalid"}), 400
        try:
            ss, window = compute_trim(segments, start_ms, end_ms)
        except ValueError as exc:
            return jsonify({"error": str(exc), "code": "range_out_of_buffer"}), 409
        crop_raw = request.form.get("crop")
        try:
            crop = hooks.parse_clip_crop(json.loads(crop_raw) if crop_raw else None)
        except ValueError as exc:
            return jsonify({"error": f"crop 无效：{exc}", "code": "crop_invalid"}), 400
        offset_raw = request.form.get("source_offset_seconds")
        try:
            source_offset = float(offset_raw) if offset_raw not in (None, "") else 0.0
        except (TypeError, ValueError):
            return jsonify({"error": "source_offset_seconds 必须是数字"}), 400
        source_host = str(request.form.get("source_host") or "").strip()[:120]
        capture_mode = str(request.form.get("capture_mode") or "").strip()[:40]

        clip_dir = hooks.clip_dir(match_id)
        source_dir = clip_dir / f"{EXT_SOURCE_DIR_PREFIX}{uuid.uuid4().hex}"
        source_dir.mkdir(parents=True, exist_ok=True)
        sources: list[Path] = []
        total_bytes = 0
        try:
            for index, storage in enumerate(files):
                head = storage.stream.read(4)
                storage.stream.seek(0)
                if not is_webm_head(head):
                    raise ValueError(f"第 {index + 1} 个文件不是 webm")
                target = source_dir / f"seg_{index:03d}.webm"
                storage.save(str(target))
                size = target.stat().st_size
                if size == 0:
                    raise ValueError(f"第 {index + 1} 个文件是空的")
                total_bytes += size
                if total_bytes > EXT_UPLOAD_MAX_BYTES:
                    raise OverflowError
                sources.append(target)
        except ValueError as exc:
            shutil.rmtree(source_dir, ignore_errors=True)
            return jsonify({"error": str(exc), "code": "file_invalid"}), 400
        except OverflowError:
            shutil.rmtree(source_dir, ignore_errors=True)
            return (
                jsonify(
                    {
                        "error": f"上传超过 {EXT_UPLOAD_MAX_BYTES // (1024 * 1024)} MB 上限",
                        "code": "upload_too_large",
                    }
                ),
                413,
            )

        timestamp = time.strftime("%Y%m%d-%H%M%S")
        output_path = clip_dir / f"clip_{timestamp}_{uuid.uuid4().hex[:6]}.gif"
        # Claimed before the slow encode, same as the page capture: a second
        # upload already in flight must find the mode used up.
        append_claim = hooks.take_append_target(match_id) if hooks.take_append_target else None
        try:
            encode_from_webm(
                hooks.resolve_ffmpeg(), sources, ss, window,
                hooks.clip_video_filter(crop), output_path,
            )
        except Exception as exc:  # noqa: BLE001 - surface ffmpeg detail to the extension
            shutil.rmtree(source_dir, ignore_errors=True)
            if hooks.restore_append_target:
                hooks.restore_append_target(match_id, append_claim)
            _log("monitor_ext_clip_failed", match_id=match_id, error=str(exc))
            return jsonify({"error": str(exc), "code": "encode_failed"}), 500
        store.register(source_dir)

        anchor_epoch = end_ms / 1000.0 + source_offset
        alert_match = hooks.match_clip_alert(match_id, anchor_epoch)
        event_key = f"manual_{int(anchor_epoch)}"
        form = hooks.default_clip_form(alert_match, match_id)
        ext_source = {
            "dir": source_dir.name,
            "segments": segments,
            "start_epoch_ms": start_ms,
            "end_epoch_ms": end_ms,
            "source_host": source_host,
            "capture_mode": capture_mode,
            "source_offset_seconds": source_offset,
        }
        hooks.write_clip_meta(
            output_path,
            {
                "source": "extension",
                "ext_source": ext_source,
                "event_key": event_key,
                "anchored": True,
                "anchor_epoch_ms": round(anchor_epoch * 1000),
                "start_epoch_ms": round(start_ms),
                "before_seconds": round(window, 3),
                "crop": crop,
                "form": form,
                "alert_match": alert_match,
                "alert_match_public": hooks.alert_match_public(alert_match),
                "match_detail": hooks.match_detail_for(match_id),
            },
        )
        appended = (
            hooks.attach_pending_clip(match_id, output_path, append_claim)
            if hooks.attach_pending_clip
            else None
        )
        _log(
            "monitor_ext_clip",
            match_id=match_id,
            clip=output_path.name,
            source_host=source_host,
            capture_mode=capture_mode,
            files=len(sources),
            upload_bytes=total_bytes,
            ss=ss,
            window=window,
            crop=crop,
            source_offset_seconds=source_offset,
            appended=appended,
            alert_match={
                "mode": alert_match.get("mode"),
                "candidates": alert_match.get("candidates"),
                "weak": alert_match.get("weak"),
                "alert_seq": alert_match.get("alert_seq"),
            },
        )
        return jsonify(
            {
                "ok": True,
                "source": "extension",
                "match_id": match_id,
                "name": output_path.name,
                "before_seconds": round(window, 3),
                "anchored": True,
                "crop": crop,
                "clip_url": f"clips/{match_id}/{output_path.name}",
                "bytes": output_path.stat().st_size,
                "publish_enabled": hooks.publish_enabled(),
                "delivery_mode": hooks.delivery_mode(),
                "event_key": event_key,
                "alert_match": hooks.alert_match_public(alert_match),
                "form": form,
                "appended": appended,
            }
        )

    @app.post("/api/monitor/ext/clip/recrop")
    def monitor_ext_clip_recrop():
        body = request.get_json(silent=True) or {}
        try:
            match_id = hooks.validate_match_id(str(body.get("match_id") or ""))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        name = str(body.get("name") or "").strip()
        if not hooks.clip_name_valid(name):
            return jsonify({"error": "无效的 GIF 文件名"}), 400
        gif_path = hooks.clip_dir(match_id) / name
        if not gif_path.is_file():
            return jsonify({"error": "GIF 不存在或已被清理", "code": "clip_missing"}), 404
        meta = hooks.read_clip_meta(gif_path)
        ext_source = meta.get("ext_source") if isinstance(meta.get("ext_source"), dict) else None
        if meta.get("source") != "extension" or not ext_source:
            return jsonify({"error": "这条 GIF 不是插件截的，不能重新裁剪", "code": "not_extension"}), 409
        source_dir = hooks.clip_dir(match_id) / str(ext_source.get("dir") or "")
        if not store.alive(source_dir):
            return jsonify({"error": "原始画面已过期清理，无法重新裁剪", "code": "source_expired"}), 409
        try:
            crop = hooks.parse_clip_crop(body.get("crop"))
        except ValueError as exc:
            return jsonify({"error": str(exc), "code": "crop_invalid"}), 400
        try:
            segments = list(ext_source.get("segments") or [])
            ss, window = compute_trim(
                segments, float(ext_source["start_epoch_ms"]), float(ext_source["end_epoch_ms"])
            )
        except (KeyError, TypeError, ValueError, IndexError) as exc:
            return jsonify({"error": f"侧车数据损坏：{exc}", "code": "meta_invalid"}), 409
        sources = sorted(source_dir.glob("seg_*.webm"))
        if not sources:
            return jsonify({"error": "原始画面已过期清理，无法重新裁剪", "code": "source_expired"}), 409
        temp_output = gif_path.with_name(f".recrop_{uuid.uuid4().hex}.gif")
        try:
            encode_from_webm(
                hooks.resolve_ffmpeg(), sources, ss, window,
                hooks.clip_video_filter(crop), temp_output,
            )
            os.replace(temp_output, gif_path)
        except Exception as exc:  # noqa: BLE001
            temp_output.unlink(missing_ok=True)
            return jsonify({"error": str(exc), "code": "encode_failed"}), 500
        meta["crop"] = crop
        hooks.write_clip_meta(gif_path, meta)
        _log("monitor_ext_recrop", match_id=match_id, clip=name, crop=crop)
        return jsonify(
            {
                "ok": True,
                "source": "extension",
                "match_id": match_id,
                "name": name,
                "crop": crop,
                "clip_url": f"clips/{match_id}/{name}",
                "bytes": gif_path.stat().st_size,
            }
        )

    return store
