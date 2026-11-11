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
import math
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from dqd_api import positive_env_float, positive_env_int


PREVIEW_ROOT = Path(__file__).resolve().parent / "tmp" / "preview"
PREVIEW_IDLE_TIMEOUT_SECONDS = positive_env_float(
    "GIF_PREVIEW_IDLE_TIMEOUT_SECONDS", 90.0
)


def non_negative_env_float(name: str, default: float) -> float:
    """Like ``positive_env_float`` but 0 is allowed (used as "switched off")."""
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


# Stall watchdog. The source CDN (hs-wyc.dongqiudi.net) can keep an RTMP play
# session open while delivering no media: the socket keeps receiving
# something, so -rw_timeout never fires, and ffmpeg sits alive writing nothing
# until the CDN drops the session about 11 minutes later (11-05 54577162,
# 11-07 54494777 and 54493413 - the last one froze the stage at 63' with the
# operator reopening it 12 times). The reaper only relaunched a *dead* ffmpeg
# and start() reuses a live one, so nothing noticed. Once a run has been
# producing (see PREVIEW_STALL_MIN_RUN_SECONDS) and its playlist then stops
# growing for this many seconds, it is terminated and relaunched through the
# normal auto-restart path: same directory and source, history and a pinned
# start kept. A healthy run adds a segment every second (one forced keyframe
# per segment), so 20 s without one is never normal. 0 switches it off.
PREVIEW_STALL_SECONDS = non_negative_env_float("GIF_PREVIEW_STALL_SECONDS", 20.0)
# The watchdog only acts on a run whose newest segment came at least this long
# after it started. Right after a (re)connect to a stream nobody is pushing,
# the CDN replays a few seconds of cached picture and then holds the session
# until it refuses it at ~60 s (11-08 54340581 / 54475673: 4-26 segments in
# 60 s); that retry cycle is left to the CDN exactly as before.
PREVIEW_STALL_MIN_RUN_SECONDS = 60.0
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
# Previous ffmpeg stderr is copied here (same directory) on restart so the
# exit reason can be inspected after the fact.
PREVIEW_PREV_LOG_NAME = "ffmpeg.prev.log"
# Sub-directory of the per-match root that holds playlist + segments.
PREVIEW_MEDIA_SUBDIR = "g1"
# Source-black detection (54494599 on 11-07: the pusher's feed went black for
# minutes while the preview kept playing it). A black - or otherwise blank,
# static - picture re-encodes to almost nothing: that match's black stretches
# were 4-10 KB per 1 s segment, while 99% of the 1.17M segments served
# 11-01..11-09 were >= 100 KB. Black with normal sound still stays near
# 20-25 KB (the 128k AAC track alone is ~16 KB/s). A segment with fewer bytes
# than this per second of media counts as "no picture".
SOURCE_BLACK_BYTES_PER_SECOND = 30 * 1024
# The newest segments must all be that small for this long before it is
# reported: a 1-3 s fade to black between programmes is normal broadcast.
SOURCE_BLACK_MIN_SECONDS = 8.0
# Window for ``drops_recent`` in status: how many times the picture broke
# lately. The workshop suggests another source at 3 within it (user, 11-09).
PREVIEW_DROP_WINDOW_SECONDS = 600.0
_SEGMENT_SEQUENCE_RE = re.compile(r"(\d+)\.ts$")
# Recorded as the exit reason when an operator switches the source, so the
# workshop can say "switching" instead of "source dropped".
SWITCH_EXIT_REASON = "操作员切换直播源"


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
    # Stall watchdog bookkeeping for the current run (reset on every
    # relaunch): the newest playlist segment the reaper has seen and when it
    # first saw it. ``stall_reason`` is set just before the watchdog
    # terminates a stalled run so the exit is recorded with it instead of
    # ffmpeg's own "received signal 15" line.
    progress_tail: str | None = None
    progress_at: float = 0.0
    stall_reason: str | None = None
    # Relaunch bookkeeping. Every ffmpeg run of this entry writes into the
    # *same* directory: the HLS muxer's ``append_list`` reads the playlist the
    # dead run left behind, keeps its segments (with their original
    # PROGRAM-DATE-TIME), continues the sequence numbers and inserts an
    # EXT-X-DISCONTINUITY before the first new segment. The browser therefore
    # keeps one continuous timeline across a source drop: the operator can
    # still scrub back past the gap and a start pinned before the drop stays
    # valid. ``frozen_tail`` is the last segment name at relaunch time; while
    # the playlist still ends with it the replacement has not delivered a
    # single new segment yet and the history is "frozen" (served, scrubbable,
    # clippable, but not advancing).
    frozen_tail: str | None = None
    # Raw PROGRAM-DATE-TIME of ``frozen_tail`` at relaunch time. The whole
    # continuity scheme rests on ffmpeg's append_list re-emitting the old
    # segments with their original PDT (verified on 8.1; older hlsenc builds
    # are not guaranteed). When the history resumes, the tail's PDT is
    # re-read and any change is logged as ``preview_pdt_drift`` so a bad
    # ffmpeg build is visible in the server log instead of as mis-cut GIFs.
    frozen_tail_pdt: str | None = None
    # Whether the last source_black_seconds() call found the source black;
    # only used to log the start and the end of a black stretch once each.
    source_black: bool = False
    # When the picture broke: an exit of a run that had been delivering
    # (not one of the quick re-fails while the history is already frozen,
    # not a source that never delivered at all, and not an operator's source
    # switch). Pruned to the drop window.
    drop_times: list = field(default_factory=list)
    # Operator source switches (switch_resource): the sequence number of the
    # last segment the previous source wrote; later numbers came from the new
    # one. Two sources run tens of seconds apart (11-09: zb- 28 s behind
    # pt-), so a clip must not span one.
    switch_boundaries: list = field(default_factory=list)
    switched_at: float | None = None


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
        stall_seconds: float = PREVIEW_STALL_SECONDS,
        stall_min_run_seconds: float = PREVIEW_STALL_MIN_RUN_SECONDS,
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
        # Stall watchdog (see PREVIEW_STALL_SECONDS); acts only together with
        # auto_restart, whose relaunch path it reuses.
        self.stall_seconds = float(stall_seconds)
        self.stall_min_run_seconds = float(stall_min_run_seconds)
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
        """Per-match directory; the playlist lives in its ``g1`` sub-directory."""
        return self.root / match_id

    def media_dir(self, match_id: str) -> Path:
        """Directory whose playlist/segments are served for this match.

        One directory per match for the whole life of the entry: relaunches
        append to the same playlist (see ``_PreviewEntry.frozen_tail``), so
        history survives a source drop without any directory switch. The
        ``g1`` name is historical (one directory per ffmpeg run) and is kept
        so server-side tooling and docs stay valid.
        """
        with self.lock:
            entry = self.entries.get(match_id)
            if entry is not None:
                return entry.directory
        return self.match_root(match_id) / PREVIEW_MEDIA_SUBDIR

    @staticmethod
    def _playlist_entries(directory: Path) -> list[tuple[str, str | None]] | None:
        """``(segment_name, raw PROGRAM-DATE-TIME)`` per listed segment.

        ``None`` when there is no playlist at all; an empty list when the
        playlist exists but lists no segment yet.
        """
        try:
            lines = (directory / "index.m3u8").read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return None
        entries: list[tuple[str, str | None]] = []
        pdt: str | None = None
        for line in lines:
            line = line.strip()
            if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
                pdt = line[len("#EXT-X-PROGRAM-DATE-TIME:"):]
            elif line and not line.startswith("#"):
                entries.append((line, pdt))
                pdt = None
        return entries

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
            if entry is not None and entry.resource == resource:
                if entry.process.poll() is None:
                    entry.last_access = time.time()
                    return self._status_locked(match_id)
                # Same source, ffmpeg dead (source dropped, or the reaper's
                # backoff has not fired yet): relaunch in place so the buffered
                # history and any pinned start survive, instead of wiping the
                # directory as a cold start would.
                entry.last_access = time.time()
                self._restart_entry_locked(match_id, time.time(), operator=True)
                return self._status_locked(match_id)
            if entry is not None:
                # The live source changed; restart against the new resource.
                # Replacing is not a stop: the sibling resource (mediamtx
                # path) was just (re)registered by the caller and the new
                # ffmpeg is about to read from it, so do not release it.
                self._stop_entry_locked(match_id, release_sibling=False)
            # A cold start is a clean slate: wipe whatever this match left
            # behind and begin a fresh playlist.
            shutil.rmtree(self.match_root(match_id), ignore_errors=True)
            directory = self.match_root(match_id) / PREVIEW_MEDIA_SUBDIR
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

    def switch_resource(self, match_id: str, resource: str) -> dict[str, Any] | None:
        """Point a running preview at another source without losing history.

        The operator's source switch. It reuses the drop/relaunch path: the
        current ffmpeg is stopped and a new one is launched on ``resource``
        into the *same* directory, so ``append_list`` keeps every buffered
        segment (scrubbing and clipping the old source still work) and the
        browser keeps its player. Two sources run tens of seconds apart, so
        the sequence number of the last old segment is remembered
        (``switch_boundaries``) and a clip may not span it. ``None`` when
        the match has no preview entry. Not for a WebRTC-backed preview,
        whose ffmpeg reads the mediamtx loopback (the caller refuses those).
        """
        resource = str(resource or "").strip()
        if not resource:
            raise ValueError("缺少可用的直播源地址")
        with self.lock:
            entry = self.entries.get(match_id)
            if entry is None:
                return None
            if entry.resource == resource:
                entry.last_access = time.time()
                return self._status_locked(match_id)
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
            if process.poll() is None:
                # Never start a second ffmpeg on the same directory.
                raise RuntimeError("当前转码进程停不下来，请稍后再切换")
            # Read the tail only now: a terminated ffmpeg flushes its last
            # segment into the playlist on the way out.
            listed = self._playlist_entries(entry.directory) or []
            if listed:
                seq = _SEGMENT_SEQUENCE_RE.search(listed[-1][0])
                if seq and int(seq.group(1)) not in entry.switch_boundaries:
                    entry.switch_boundaries.append(int(seq.group(1)))
            now = time.time()
            old = entry.resource
            entry.last_access = now
            # A new source starts with a clean slate: a fresh retry budget and
            # its own drops only (the switch itself is not one).
            entry.drop_times = []
            entry.restart_count = 0
            entry.cooling = False
            entry.next_restart_at = 0.0
            entry.switched_at = now
            self._restart_entry_locked(
                match_id,
                now,
                operator=True,
                resource=resource,
                exit_reason=SWITCH_EXIT_REASON,
                count_drop=False,
            )
            _log_event(
                "preview_resource_switched",
                match_id=match_id,
                old=old,
                new=entry.resource,
                operator=True,
                boundary_segment=entry.switch_boundaries[-1] if entry.switch_boundaries else None,
            )
            return self._status_locked(match_id)

    def switch_boundaries(self, match_id: str) -> list[int]:
        """Sequence numbers of the last segment before each source switch."""
        with self.lock:
            entry = self.entries.get(match_id)
            return list(entry.switch_boundaries) if entry is not None else []

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
        self._refresh_frozen_locked(match_id, entry)
        return_code = entry.process.poll()
        running = return_code is None
        history_frozen = entry.frozen_tail is not None
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
            # True while a relaunched ffmpeg has not yet appended a new segment:
            # the browser is still being served the previous run's history
            # (scrubbable and clippable, but no new frames arrive).
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
            # How many times the picture broke in the last drops_window_seconds
            # (see _PreviewEntry.drop_times); reset by a source switch.
            "drops_recent": sum(
                1 for at in entry.drop_times
                if time.time() - at <= PREVIEW_DROP_WINDOW_SECONDS
            ),
            "drops_window_seconds": int(PREVIEW_DROP_WINDOW_SECONDS),
            "switched_at_unix": entry.switched_at,
            # Frozen because an operator switched the source and the new one
            # has not delivered yet (not a drop).
            "switching": history_frozen and entry.last_exit_reason == SWITCH_EXIT_REASON,
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

    _REPEATED_RE = re.compile(r"^(?:\[[^\]]*\]\s*)?Last message repeated (\d+) times?$")

    @classmethod
    def _summarize_exit_reason(cls, log_tail: str | None) -> str | None:
        """Collapse ffmpeg stderr into the single most informative line.

        ffmpeg's last line is almost always the real cause (``Connection timed
        out``, ``Server returned 404``, ``Error reading header`` ...); earlier
        lines are usually decode warnings that are not the exit reason. The
        exception is ffmpeg's own ``Last message repeated N times``, which says
        nothing by itself: the line it repeats is the one before, so that line
        is reported with the count (e.g. a flood of an unsupported-codec warning
        while the picture stays black).
        """
        if not log_tail:
            return None
        lines = [line.strip() for line in log_tail.splitlines() if line.strip()]
        if not lines:
            return None
        repeated = cls._REPEATED_RE.match(lines[-1])
        if repeated:
            earlier = [line for line in lines[:-1] if not cls._REPEATED_RE.match(line)]
            if earlier:
                return f"{earlier[-1][:180]} (x{int(repeated.group(1)) + 1})"
        return lines[-1][:200]

    def _record_exit_locked(
        self,
        match_id: str,
        entry: _PreviewEntry,
        return_code: int | None,
        now: float,
        *,
        reason_override: str | None = None,
        count_drop: bool = True,
    ) -> bytes:
        """Capture why the current process died; return its raw stderr.

        The raw log is handed back so the caller can re-write it as
        ``ffmpeg.prev.log`` next to the replacement's log. ``reason_override``
        names a deliberate stop (an operator's source switch), which is then
        not counted as a drop either (``count_drop=False``).
        """
        log_tail = self._read_log_tail(entry.log_path, limit=3000)
        reason = self._summarize_exit_reason(log_tail)
        if entry.stall_reason:
            # Terminated by the stall watchdog: ffmpeg's last line would only
            # say it received a signal. stderr_tail below still has its lines.
            reason, entry.stall_reason = entry.stall_reason, None
        if reason_override:
            reason = reason_override
        # A run that delivered since the last reaper tick still counts as
        # having delivered.
        self._refresh_frozen_locked(match_id, entry)
        if (
            count_drop
            and entry.frozen_tail is None
            and self._playlist_entries(entry.directory)
        ):
            # This run had been delivering: the picture broke. Re-fails while
            # already frozen are the same break. A source that never delivered
            # a single segment leaves no playlist, so ``frozen_tail`` stays
            # None across its re-fails; without the playlist check every retry
            # of it counted as a new break (12 retries -> drops_recent 12 before
            # kick-off, and a false "断了 N 次，建议换一路").
            entry.drop_times = [
                at for at in entry.drop_times if now - at <= PREVIEW_DROP_WINDOW_SECONDS
            ]
            entry.drop_times.append(now)
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
            stderr_tail=self._distinct_tail(log_tail),
        )
        return raw_log

    @classmethod
    def _distinct_tail(cls, log_tail: str | None, count: int = 5) -> list[str]:
        """The last ``count`` different stderr lines, oldest first, for the log.

        ``Last message repeated`` lines and exact repeats are dropped so a
        flood of one warning cannot push the other clues out.
        """
        if not log_tail:
            return []
        picked: list[str] = []
        for line in reversed(log_tail.splitlines()):
            line = line.strip()
            if not line or cls._REPEATED_RE.match(line) or line[:160] in picked:
                continue
            picked.append(line[:160])
            if len(picked) == count:
                break
        return picked[::-1]

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
        # The whole match directory (history included) goes with the entry.
        shutil.rmtree(self.match_root(match_id), ignore_errors=True)

    def _refresh_frozen_locked(self, match_id: str, entry: _PreviewEntry) -> None:
        """Clear the frozen flag once the relaunched ffmpeg has appended a segment.

        Until then the previous run's playlist keeps being served unchanged so
        a crash (or the source ending at full time) never erases the history
        the operator may still want to scrub back into and clip.
        """
        if entry.frozen_tail is None:
            return
        entries = self._playlist_entries(entry.directory)
        if not entries or entries[-1][0] == entry.frozen_tail:
            return
        tail = entries[-1][0]
        frozen_name = entry.frozen_tail
        old_pdt = entry.frozen_tail_pdt
        new_pdt = next((pdt for name, pdt in entries if name == frozen_name), None)
        entry.frozen_tail = None
        entry.frozen_tail_pdt = None
        _log_event(
            "preview_history_resumed",
            match_id=match_id,
            restart_count_total=entry.restart_count_total,
            first_new_segment=tail,
        )
        if old_pdt and new_pdt and old_pdt != new_pdt:
            # The relaunched ffmpeg rewrote the old segments' timestamps:
            # scrubbing still works but PDT-anchored clips of the history
            # would be cut at the wrong moment. Loud, so it is noticed.
            _log_event(
                "preview_pdt_drift",
                match_id=match_id,
                segment=frozen_name,
                before=old_pdt,
                after=new_pdt,
            )

    @staticmethod
    def _playlist_durations(directory: Path) -> list[tuple[str, float]]:
        """``(segment_name, EXTINF seconds)`` per listed segment, oldest first.

        An unreadable or missing playlist gives an empty list; an EXTINF that
        does not parse gives 0.0 for that segment.
        """
        try:
            lines = (directory / "index.m3u8").read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            return []
        segments: list[tuple[str, float]] = []
        duration = 0.0
        for line in lines:
            line = line.strip()
            if line.startswith("#EXTINF:"):
                try:
                    duration = float(line[len("#EXTINF:"):].split(",", 1)[0])
                except ValueError:
                    duration = 0.0
            elif line and not line.startswith("#"):
                segments.append((line, duration))
                duration = 0.0
        return segments

    def source_black_seconds(self, match_id: str) -> float:
        """Seconds the source has been sending no picture, 0.0 when it has one.

        54494599 on 11-07: the pusher's feed went black for about 4 minutes.
        The player kept playing the black picture, so nothing on the page
        looked broken, and the operator reloaded the page four times - which
        cannot help when the source itself is black. Counts back from the
        newest listed segment while each one is below
        SOURCE_BLACK_BYTES_PER_SECOND, and reports the total once it reaches
        SOURCE_BLACK_MIN_SECONDS (shorter is 0.0). Only while ffmpeg is
        producing: a stopped, dead or not-yet-resumed preview is reported by
        ``status`` instead. Logs ``preview_source_black`` when a black stretch
        is first seen and when the picture is back. Files are read outside
        the lock; ffmpeg deleting old segments meanwhile just ends the count.
        """
        with self.lock:
            entry = self.entries.get(match_id)
            if entry is None or entry.frozen_tail is not None or entry.process.poll() is not None:
                return 0.0
            directory = entry.directory
        seconds = 0.0
        for name, duration in reversed(self._playlist_durations(directory)):
            if duration <= 0:
                break
            try:
                size = (directory / name).stat().st_size
            except OSError:
                break
            if size >= SOURCE_BLACK_BYTES_PER_SECOND * duration:
                break
            seconds += duration
        black = seconds >= SOURCE_BLACK_MIN_SECONDS
        with self.lock:
            if self.entries.get(match_id) is entry and entry.source_black != black:
                entry.source_black = black
                _log_event(
                    "preview_source_black",
                    match_id=match_id,
                    black=black,
                    seconds=round(seconds, 1),
                )
        return seconds if black else 0.0

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
                self._refresh_frozen_locked(match_id, entry)
                if not self.auto_restart:
                    continue
                if return_code is None:
                    # Alive. If it has run cleanly past the reset window, forget
                    # earlier failures so a later outage gets a fresh retry budget.
                    if entry.restart_count and now - entry.started_at > self.restart_reset_seconds:
                        entry.restart_count = 0
                        entry.cooling = False
                    self._watch_stall_locked(match_id, entry, now)
                    continue
                # Died while still watched: restart under backoff. After the fast
                # retries are spent we keep probing at max_delay so a recovering
                # source self-heals rather than staying dead until a manual reload.
                if now < entry.next_restart_at:
                    continue
                self._restart_entry_locked(match_id, now)

    def _watch_stall_locked(self, match_id: str, entry: _PreviewEntry, now: float) -> None:
        """Relaunch a live ffmpeg whose playlist stopped growing (PREVIEW_STALL_SECONDS).

        Progress is the newest playlist segment changing between reaper ticks.
        Nothing happens until the run has produced for
        ``stall_min_run_seconds`` (see PREVIEW_STALL_MIN_RUN_SECONDS), nor when
        the playlist cannot be read. A stalled run is terminated and relaunched
        right away through ``_restart_entry_locked``: same resource, same
        directory, so the history is kept and reported frozen until the
        replacement appends a segment, exactly like a source drop.
        """
        if self.stall_seconds <= 0:
            return
        listed = self._playlist_entries(entry.directory)
        if not listed:
            return
        tail = listed[-1][0]
        if tail != entry.progress_tail:
            entry.progress_tail = tail
            entry.progress_at = now
            return
        if entry.progress_at - entry.started_at < self.stall_min_run_seconds:
            return
        stalled = now - entry.progress_at
        if stalled < self.stall_seconds:
            return
        process = entry.process
        _log_event(
            "preview_ffmpeg_stalled",
            match_id=match_id,
            resource=entry.resource,
            pid=process.pid,
            last_segment=tail,
            stalled_seconds=round(stalled, 1),
            uptime_seconds=round(now - entry.started_at, 1),
        )
        entry.stall_reason = f"源站连接卡住，{int(stalled)} 秒没有新画面"
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pass
        if process.poll() is None:
            # Never start a second ffmpeg on the same directory; once this one
            # is gone the normal auto-restart takes over (with this reason).
            return
        self._restart_entry_locked(match_id, now)

    def _restart_entry_locked(
        self,
        match_id: str,
        now: float,
        *,
        operator: bool = False,
        resource: str | None = None,
        exit_reason: str | None = None,
        count_drop: bool = True,
    ) -> None:
        """Relaunch a dead ffmpeg into its existing directory.

        ``operator=True`` is the ``start()`` path (the browser or the operator
        re-asked for the same source): the caller has just (re)resolved the
        resource itself, so the ``on_restart`` hook is skipped, and the relaunch
        does not consume the reaper's retry budget (``restart_count`` /
        ``next_restart_at`` / ``cooling`` stay as they are). ``resource``
        relaunches against a different source (``switch_resource``);
        ``exit_reason`` / ``count_drop`` go to ``_record_exit_locked``.
        """
        entry = self.entries.get(match_id)
        if entry is None:
            return
        delay = min(
            self.restart_base_delay * (2 ** entry.restart_count),
            self.restart_max_delay,
        )
        prev_log = self._record_exit_locked(
            match_id,
            entry,
            entry.process.poll(),
            now,
            reason_override=exit_reason,
            count_drop=count_drop,
        )
        directory = entry.directory
        directory.mkdir(parents=True, exist_ok=True)
        # Relaunch into the same directory: ffmpeg's ``append_list`` picks the
        # old playlist up, keeps its segments and sequence numbers and marks
        # the join with EXT-X-DISCONTINUITY (verified: old PDTs are preserved
        # byte for byte). Until a new segment lands the history is frozen.
        if entry.frozen_tail is None:
            entries = self._playlist_entries(directory)
            if entries is None:
                entry.frozen_tail = None
            elif entries:
                entry.frozen_tail, entry.frozen_tail_pdt = entries[-1]
            else:
                entry.frozen_tail, entry.frozen_tail_pdt = "", None
        if prev_log:
            try:
                (directory / PREVIEW_PREV_LOG_NAME).write_bytes(prev_log)
            except OSError:
                pass
        if self.on_restart is not None and not operator:
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
        if resource:
            entry.resource = resource
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
        # A fresh run: its progress is measured from now, against the tail it
        # inherits (the frozen history), never against the previous run.
        entry.progress_tail = entry.frozen_tail
        entry.progress_at = now
        entry.restart_count_total += 1
        if not operator:
            entry.restart_count += 1
            entry.next_restart_at = now + delay
            if entry.restart_count >= self.restart_max_retries:
                entry.cooling = True
        _log_event(
            "preview_ffmpeg_restarted",
            match_id=match_id,
            pid=process.pid,
            operator=operator,
            restart_count=entry.restart_count,
            restart_count_total=entry.restart_count_total,
            next_retry_in_seconds=None if operator else delay,
            cooling=entry.cooling,
        )

    def shutdown(self) -> None:
        self._stop.set()
        with self.lock:
            for match_id in list(self.entries):
                self._stop_entry_locked(match_id)
