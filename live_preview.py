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

import shutil
import subprocess
import threading
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dqd_api import positive_env_float


PREVIEW_ROOT = Path(__file__).resolve().parent / "tmp" / "preview"
PREVIEW_IDLE_TIMEOUT_SECONDS = positive_env_float(
    "GIF_PREVIEW_IDLE_TIMEOUT_SECONDS", 90.0
)
PREVIEW_SEGMENT_SECONDS = 2
PREVIEW_LIST_SIZE = 6
PREVIEW_HEIGHT = 480


def resolve_ffmpeg() -> str:
    return shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"


@dataclass
class _PreviewEntry:
    resource: str
    process: "subprocess.Popen[bytes]"
    directory: Path
    log_path: Path
    started_at: float
    last_access: float


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
        background_reaper: bool = True,
    ) -> None:
        self.root = Path(root)
        self.idle_timeout_seconds = float(idle_timeout_seconds)
        self.segment_seconds = int(segment_seconds)
        self.list_size = int(list_size)
        self.height = int(height)
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

    def media_dir(self, match_id: str) -> Path:
        return self.root / match_id

    def playlist_url(self, match_id: str) -> str:
        return f"/preview/{urllib.parse.quote(match_id)}/index.m3u8"

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
        else:
            # Bound a stalled network read so a dead RTMP source fails fast.
            command += ["-rw_timeout", "15000000"]
        command += [
            "-i", resource,
            "-vf", f"scale=-2:{self.height}",
            "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
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
                self._stop_entry_locked(match_id)
            else:
                self._stop_entry_locked(match_id)
            directory = self.media_dir(match_id)
            shutil.rmtree(directory, ignore_errors=True)
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
        return_code = entry.process.poll()
        running = return_code is None
        playlist_ready = (entry.directory / "index.m3u8").is_file()
        error = None
        if not running:
            error = self._read_log_tail(entry.log_path)
        return {
            "running": running,
            "resource": entry.resource,
            "playlist_ready": playlist_ready,
            "started_at_unix": entry.started_at,
            "return_code": return_code,
            "error": error,
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

    def _stop_entry_locked(self, match_id: str) -> None:
        entry = self.entries.pop(match_id, None)
        if entry is None:
            return
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
        shutil.rmtree(entry.directory, ignore_errors=True)

    def _reap_loop(self) -> None:
        while not self._stop.wait(5.0):
            now = time.time()
            with self.lock:
                for match_id in list(self.entries):
                    entry = self.entries[match_id]
                    # poll() reaps a finished child; the entry is kept so status
                    # can still surface its error until the tab goes idle.
                    entry.process.poll()
                    if now - entry.last_access > self.idle_timeout_seconds:
                        self._stop_entry_locked(match_id)

    def shutdown(self) -> None:
        self._stop.set()
        with self.lock:
            for match_id in list(self.entries):
                self._stop_entry_locked(match_id)
