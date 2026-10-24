"""Browser-playable HLS preview of a match live source.

Shared by the automated dashboard and the manual monitor service. Each match
gets at most one ffmpeg process that re-encodes its RTMP source into a short
rolling HLS playlist. A plain ``-c copy`` keeps the H.264 parameter sets only
at the stream head, so later segments are undecodable in hls.js/MSE and the
player stalls; a light 480p veryfast encode writes in-band parameter sets on
every forced keyframe, making each segment independently playable and giving a
rolling buffer that doubles as the clip source.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from dqd_api import positive_env_float, positive_env_int


PREVIEW_ROOT = Path(__file__).resolve().parent / "tmp" / "preview"
PREVIEW_IDLE_TIMEOUT_SECONDS = positive_env_float(
    "GIF_PREVIEW_IDLE_TIMEOUT_SECONDS", 90.0
)
# 1s segments: hls.js sits liveSyncDurationCount (3) segments behind the edge
# and the browser only learns about a segment once it is fully written, so
# segment length is the single biggest lever on steady-state preview delay
# (2s -> 1s cuts it from ~8.5s to ~4.5s). Costs: one keyframe per second
# (+15-30% bitrate) and twice the request rate per stream. Operator kill
# switch: GIF_PREVIEW_SEGMENT_SECONDS=2 restores the previous behaviour
# (pair it with GIF_MONITOR_PREVIEW_SEGMENTS=90 to keep ~180s of history).
PREVIEW_SEGMENT_SECONDS = positive_env_int("GIF_PREVIEW_SEGMENT_SECONDS", 1)
# x264 speed/quality trade-off. Only a latency lever when the server CPU is
# saturated (encode slower than real time); otherwise leave it alone.
X264_PRESETS = (
    "ultrafast", "superfast", "veryfast", "faster", "fast", "medium",
)


def x264_preset_from_env(default: str = "veryfast") -> str:
    raw = os.environ.get("GIF_PREVIEW_X264_PRESET", "").strip().lower()
    return raw if raw in X264_PRESETS else default


PREVIEW_X264_PRESET = x264_preset_from_env()
PREVIEW_LIST_SIZE = 12
PREVIEW_HEIGHT = 480
# Previous ffmpeg stderr is copied here into the replacement's generation
# directory on restart so the exit reason can be inspected after the fact.
PREVIEW_PREV_LOG_NAME = "ffmpeg.prev.log"


def resolve_ffmpeg() -> str:
    return shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"


def _log_event(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


@dataclass
class _PreviewEntry:
    resource: str
    process: "subprocess.Popen[bytes]"
    directory: Path
    log_path: Path
    started_at: float
    last_access: float
    # Auto-restart bookkeeping (only consulted when auto_restart is enabled).
    restart_count: int = 0
    next_restart_at: float = 0.0
    # True once fast retries are exhausted and we have dropped to low-frequency
    # (max_delay-spaced) probing; we keep probing so a late source recovery
    # self-heals instead of requiring a manual refresh.
    cooling: bool = False
    # Diagnostics that survive the retry-budget reset: how many times this
    # entry has ever been restarted, and why the last process died.
    restart_count_total: int = 0
    last_exit_code: int | None = None
    last_exit_reason: str | None = None
    last_exit_at: float | None = None
    # Relaunch bookkeeping. Each ffmpeg run writes into its own generation
    # directory (``directory``). After a crash the dead run's files are *not*
    # wiped: ``serve_dir`` keeps pointing at that frozen playlist so the
    # operator can still scrub and clip the history (e.g. the final minutes
    # after the source stops at full time) while the replacement ffmpeg fills
    # a new generation. Once the replacement has written a playlist it is
    # promoted (``serve_dir = directory``) and the frozen files are removed.
    serve_dir: Path | None = None
    generation: int = 1


class LivePreviewManager:
    """Transcode a match live source (RTMP) into browser-playable HLS."""

    def __init__(
        self,
        *,
        root: Path = PREVIEW_ROOT,
        idle_timeout_seconds: float = PREVIEW_IDLE_TIMEOUT_SECONDS,
        segment_seconds: int = PREVIEW_SEGMENT_SECONDS,
        list_size: int = PREVIEW_LIST_SIZE,
        height: int = PREVIEW_HEIGHT,
        x264_preset: str = PREVIEW_X264_PRESET,
        background_reaper: bool = True,
        auto_restart: bool = False,
        restart_base_delay: float = 5.0,
        restart_max_delay: float = 60.0,
        restart_max_retries: int = 6,
        restart_reset_seconds: float = 30.0,
        on_stop: "Callable[[str], None] | None" = None,
        on_restart: "Callable[[str, str], str | None] | None" = None,
    ) -> None:
        self.root = Path(root)
        self.idle_timeout_seconds = float(idle_timeout_seconds)
        self.segment_seconds = int(segment_seconds)
        self.list_size = int(list_size)
        self.height = int(height)
        self.x264_preset = x264_preset if x264_preset in X264_PRESETS else "veryfast"
        # Called whenever an entry is torn down - explicit stop, idle reap or
        # shutdown - so a sibling resource registered for the same match (the
        # mediamtx WebRTC path, scheme H) is released with it.
        self.on_stop = on_stop
        # Called before the reaper relaunches a dead ffmpeg, with the current
        # resource; may return a different one (e.g. fall back from the
        # mediamtx RTSP loopback to the upstream RTMP when mediamtx is gone).
        self.on_restart = on_restart
        # When enabled, the reaper restarts an ffmpeg that died while the tile is
        # still being watched (dead/flaky RTMP source), with exponential backoff
        # and a retry ceiling so a permanently-dead source is not hammered.
        self.auto_restart = bool(auto_restart)
        self.restart_base_delay = float(restart_base_delay)
        self.restart_max_delay = float(restart_max_delay)
        self.restart_max_retries = int(restart_max_retries)
        self.restart_reset_seconds = float(restart_reset_seconds)
        self.lock = threading.RLock()
        self.entries: dict[str, _PreviewEntry] = {}
        self._ffmpeg = resolve_ffmpeg()
        self._stop = threading.Event()
        self._reaper: threading.Thread | None = None
        if background_reaper:
            self._reaper = threading.Thread(
                target=self._reap_loop, name="live-preview-reaper", daemon=True
            )
            self._reaper.start()

    def match_root(self, match_id: str) -> Path:
        """Per-match directory holding one ``gN`` sub-directory per ffmpeg run."""
        return self.root / match_id

    def media_dir(self, match_id: str) -> Path:
        """Directory whose playlist/segments are served for this match.

        Normally the running ffmpeg's generation directory. After a crash it
        stays on the dead run's frozen files until the replacement has
        produced a playlist, so history survives the relaunch.
        """
        with self.lock:
            entry = self.entries.get(match_id)
            if entry is not None and entry.serve_dir is not None:
                return entry.serve_dir
        return self.match_root(match_id) / "g1"

    def _generation_dir(self, match_id: str, generation: int) -> Path:
        return self.match_root(match_id) / f"g{generation}"

    def playlist_url(self, match_id: str) -> str:
        return f"preview/{urllib.parse.quote(match_id)}/index.m3u8"

    @staticmethod
    def _is_local_file(resource: str) -> bool:
        lowered = resource.lower()
        if lowered.startswith(("rtmp://", "rtmps://", "http://", "https://")):
            return False
        try:
            return Path(resource).is_file()
        except OSError:
            return False

    def _build_command(self, resource: str, directory: Path) -> list[str]:
        playlist = directory / "index.m3u8"
        segments = directory / "seg_%05d.ts"
        command = [self._ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "warning"]
        if self._is_local_file(resource):
            # Pace a local file at wall-clock speed so the rolling playlist
            # behaves like a live stream instead of flushing instantly.
            command += ["-re"]
        elif resource.lower().startswith(("rtsp://", "rtsps://")):
            # Loopback RTSP from mediamtx (scheme H): TCP interleaving avoids UDP
            # port negotiation and packet loss on the local hop. The RTSP
            # demuxer rejects -rw_timeout; its socket read bound is -timeout.
            command += ["-rtsp_transport", "tcp", "-timeout", "40000000"]
        else:
            # Bound a stalled network read so a dead RTMP source fails fast.
            # 40s (not 15s) tolerates brief HD-feed stalls without killing
            # ffmpeg; a genuinely dead source is still recovered by the backend
            # auto-restart plus the frontend 8s self-heal, so the longer bound
            # only trades slightly slower dead-source detection for far fewer
            # spurious "直播源中断" blips on heavier HD streams.
            command += ["-rw_timeout", "40000000"]
        command += [
            "-i", resource,
            "-vf", f"scale=-2:{self.height}",
            "-c:v", "libx264", "-preset", self.x264_preset, "-tune", "zerolatency",
            "-profile:v", "main", "-pix_fmt", "yuv420p",
            "-force_key_frames", f"expr:gte(t,n_forced*{self.segment_seconds})",
            "-sc_threshold", "0",
            "-c:a", "aac", "-ac", "2", "-b:a", "128k",
            "-f", "hls",
            "-hls_time", str(self.segment_seconds),
            "-hls_list_size", str(self.list_size),
            "-hls_flags", "delete_segments+append_list+omit_endlist+independent_segments+program_date_time",
            "-hls_segment_type", "mpegts",
            "-hls_segment_filename", str(segments),
            str(playlist),
        ]
        return command

    def start(self, match_id: str, resource: str) -> dict[str, Any]:
        resource = str(resource or "").strip()
        if not resource:
            raise ValueError("缺少可用的直播源地址")
        with self.lock:
            entry = self.entries.get(match_id)
            if entry is not None and entry.process.poll() is None:
                if entry.resource == resource:
                    entry.last_access = time.time()
                    return self._status_locked(match_id)
                # The live source changed; restart against the new resource.
                # Replacing is not a stop: the sibling resource (mediamtx
                # path) was just (re)registered by the caller and the new
                # ffmpeg is about to read from it, so do not release it.
                self._stop_entry_locked(match_id, release_sibling=False)
            else:
                self._stop_entry_locked(match_id, release_sibling=False)
            # An operator-driven (re)start is a clean slate: wipe every
            # generation of this match and begin at g1.
            shutil.rmtree(self.match_root(match_id), ignore_errors=True)
            directory = self._generation_dir(match_id, 1)
            directory.mkdir(parents=True, exist_ok=True)
            log_path = directory / "ffmpeg.log"
            command = self._build_command(resource, directory)
            log_file = log_path.open("wb")
            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=log_file,
                    stdin=subprocess.DEVNULL,
                )
            finally:
                log_file.close()
            now = time.time()
            self.entries[match_id] = _PreviewEntry(
                resource=resource,
                process=process,
                directory=directory,
                log_path=log_path,
                started_at=now,
                last_access=now,
                serve_dir=directory,
                generation=1,
            )
            return self._status_locked(match_id)

    def stop(self, match_id: str) -> None:
        with self.lock:
            self._stop_entry_locked(match_id)

    def touch(self, match_id: str) -> None:
        with self.lock:
            entry = self.entries.get(match_id)
            if entry is not None:
                entry.last_access = time.time()

    def status(self, match_id: str) -> dict[str, Any]:
        with self.lock:
            return self._status_locked(match_id)

    def _status_locked(self, match_id: str) -> dict[str, Any]:
        entry = self.entries.get(match_id)
        if entry is None:
            return {"running": False, "resource": None, "playlist_ready": False}
        self._promote_locked(match_id, entry)
        return_code = entry.process.poll()
        running = return_code is None
        history_frozen = entry.serve_dir != entry.directory
        playlist_ready = (
            not history_frozen and (entry.directory / "index.m3u8").is_file()
        )
        error = None
        if not running:
            error = self._read_log_tail(entry.log_path)
        return {
            "running": running,
            "resource": entry.resource,
            "playlist_ready": playlist_ready,
            # True while a relaunched ffmpeg has not yet produced a playlist:
            # the browser is still being served the previous run's frozen
            # history (scrubbable and clippable, but no new frames arrive).
            "history_frozen": history_frozen,
            "started_at_unix": entry.started_at,
            "return_code": return_code,
            "error": error,
            # True once fast retries are spent and we have dropped to low-frequency
            # probing; the source is still being auto-recovered, not abandoned.
            "auto_restart_cooling": entry.cooling,
            # Diagnostics: lifetime restart count and why the last process died.
            # These are not cleared by the stable-run budget reset, so an operator
            # can still see that a now-healthy tile had dropped earlier.
            "restart_count_total": entry.restart_count_total,
            "last_exit_code": entry.last_exit_code,
            "last_exit_reason": entry.last_exit_reason,
            "last_exit_at_unix": entry.last_exit_at,
        }

    @staticmethod
    def _read_log_tail(log_path: Path, limit: int = 600) -> str | None:
        try:
            text = log_path.read_text(encoding="utf-8", errors="replace").strip()
        except OSError:
            return None
        if not text:
            return None
        return text[-limit:]

    @staticmethod
    def _summarize_exit_reason(log_tail: str | None) -> str | None:
        """Collapse ffmpeg stderr into the single most informative line.

        ffmpeg's last line is almost always the real cause (``Connection timed
        out``, ``Server returned 404``, ``Error reading header`` ...); earlier
        lines are usually decode warnings that are not the exit reason.
        """
        if not log_tail:
            return None
        lines = [line.strip() for line in log_tail.splitlines() if line.strip()]
        if not lines:
            return None
        return lines[-1][:200]

    def _record_exit_locked(self, match_id: str, entry: _PreviewEntry, return_code: int | None, now: float) -> bytes:
        """Capture why the current process died; return its raw stderr.

        The raw log is handed back so the caller can re-write it as
        ``ffmpeg.prev.log`` inside the replacement's generation directory.
        """
        log_tail = self._read_log_tail(entry.log_path)
        reason = self._summarize_exit_reason(log_tail)
        entry.last_exit_code = return_code
        entry.last_exit_reason = reason
        entry.last_exit_at = now
        try:
            raw_log = entry.log_path.read_bytes()
        except OSError:
            raw_log = b""
        _log_event(
            "preview_ffmpeg_exited",
            match_id=match_id,
            resource=entry.resource,
            return_code=return_code,
            uptime_seconds=round(now - entry.started_at, 1),
            restart_count=entry.restart_count,
            restart_count_total=entry.restart_count_total,
            reason=reason,
        )
        return raw_log

    def _stop_entry_locked(self, match_id: str, *, release_sibling: bool = True) -> None:
        entry = self.entries.pop(match_id, None)
        if entry is None:
            return
        if release_sibling and self.on_stop is not None:
            try:
                self.on_stop(match_id)
            except Exception as exc:  # noqa: BLE001 - never block teardown
                _log_event("preview_on_stop_error", match_id=match_id, error=str(exc))
        process = entry.process
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
        # Every generation (frozen history included) goes with the entry.
        shutil.rmtree(self.match_root(match_id), ignore_errors=True)

    def _promote_locked(self, match_id: str, entry: _PreviewEntry) -> None:
        """Switch serving to the relaunched ffmpeg once it has a playlist.

        Until then the previous run's frozen directory keeps being served so a
        crash (or the source ending at full time) never erases the history the
        operator may still want to scrub back into and clip.
        """
        if entry.serve_dir == entry.directory:
            return
        if not (entry.directory / "index.m3u8").is_file():
            return
        frozen = entry.serve_dir
        entry.serve_dir = entry.directory
        if frozen is not None:
            shutil.rmtree(frozen, ignore_errors=True)
        _log_event(
            "preview_generation_promoted",
            match_id=match_id,
            generation=entry.generation,
            dropped=str(frozen) if frozen is not None else None,
        )

    def _reap_loop(self) -> None:
        while not self._stop.wait(5.0):
            self._reap_tick(time.time())

    def _reap_tick(self, now: float) -> None:
        with self.lock:
            for match_id in list(self.entries):
                entry = self.entries[match_id]
                # poll() reaps a finished child; the entry is kept so status
                # can still surface its error until the tab goes idle.
                return_code = entry.process.poll()
                if now - entry.last_access > self.idle_timeout_seconds:
                    self._stop_entry_locked(match_id)
                    continue
                self._promote_locked(match_id, entry)
                if not self.auto_restart:
                    continue
                if return_code is None:
                    # Alive. If it has run cleanly past the reset window, forget
                    # earlier failures so a later outage gets a fresh retry budget.
                    if entry.restart_count and now - entry.started_at > self.restart_reset_seconds:
                        entry.restart_count = 0
                        entry.cooling = False
                    continue
                # Died while still watched: restart under backoff. After the fast
                # retries are spent we keep probing at max_delay so a recovering
                # source self-heals rather than staying dead until a manual reload.
                if now < entry.next_restart_at:
                    continue
                self._restart_entry_locked(match_id, now)

    def _restart_entry_locked(self, match_id: str, now: float) -> None:
        entry = self.entries.get(match_id)
        if entry is None:
            return
        delay = min(
            self.restart_base_delay * (2 ** entry.restart_count),
            self.restart_max_delay,
        )
        prev_log = self._record_exit_locked(match_id, entry, entry.process.poll(), now)
        if entry.directory != entry.serve_dir:
            # The previous relaunch never produced a playlist (its stderr was
            # just captured above); drop that empty generation. The frozen
            # history in ``serve_dir`` is deliberately left untouched.
            shutil.rmtree(entry.directory, ignore_errors=True)
        entry.generation += 1
        directory = self._generation_dir(match_id, entry.generation)
        directory.mkdir(parents=True, exist_ok=True)
        if prev_log:
            try:
                (directory / PREVIEW_PREV_LOG_NAME).write_bytes(prev_log)
            except OSError:
                pass
        if self.on_restart is not None:
            try:
                replacement = self.on_restart(match_id, entry.resource)
            except Exception as exc:  # never let a hook stop the self-heal
                _log_event("preview_on_restart_error", match_id=match_id, error=str(exc))
                replacement = None
            if replacement and replacement != entry.resource:
                _log_event(
                    "preview_resource_switched",
                    match_id=match_id,
                    old=entry.resource,
                    new=replacement,
                )
                entry.resource = replacement
        command = self._build_command(entry.resource, directory)
        log_path = directory / "ffmpeg.log"
        log_file = log_path.open("wb")
        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=log_file,
                stdin=subprocess.DEVNULL,
            )
        finally:
            log_file.close()
        entry.process = process
        entry.directory = directory
        entry.log_path = log_path
        entry.started_at = now
        entry.restart_count += 1
        entry.restart_count_total += 1
        entry.next_restart_at = now + delay
        if entry.restart_count >= self.restart_max_retries:
            entry.cooling = True
        _log_event(
            "preview_ffmpeg_restarted",
            match_id=match_id,
            pid=process.pid,
            restart_count=entry.restart_count,
            restart_count_total=entry.restart_count_total,
            next_retry_in_seconds=delay,
            cooling=entry.cooling,
        )

    def shutdown(self) -> None:
        self._stop.set()
        with self.lock:
            for match_id in list(self.entries):
                self._stop_entry_locked(match_id)
