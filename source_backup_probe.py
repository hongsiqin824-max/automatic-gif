"""Measure the backup live sources of matches whose 8901 preview is unstable.

Runs beside 8901 as its own service (``deploy/gif-source-probe.service``) and
never touches the 8901 process: it only reads what 8901 already leaves on
disk. Every few seconds it looks at the previews 8901 is running
(``tmp/preview/<match_id>/g1/index.m3u8`` is rewritten on every new segment;
the preview ffmpeg's command line names the source). Inside the match window
(kick-off .. +115 min) it counts the breaks and the seconds without new
picture of that source. Once the current source is on its way to being
unstable it pulls the next source in the default order (``hd`` -> ``hd-en``
-> ``sd`` -> ``pt``, as ranked by ``live_sources.SourceCatalog``) with
``ffmpeg -c copy -f null`` - no transcoding, so it costs download bandwidth
but almost no CPU - and measures that one the same way. If the backup turns
out unstable too, it moves on to the next one. Only matches someone opened in
8901 are measured, and at most ``GIF_SOURCE_PROBE_MAX`` backups are pulled at
once (0 = measure only the source 8901 plays).

At the end of each match one JSON line is appended to
``data/source_probe/results.jsonl``: every measured source with its breaks and
seconds without picture, and the match verdict - 可行 when at least one source
is stable, 不可行 when every source was measured unstable (or there is no
other source), 待定 when a source could not be measured long enough to tell.

A source is stable (user's rule, 2026-11-12) when, over the match window, it
was without new picture for less than 5 minutes and broke fewer than 6 times.
A source measured for only part of the window gets the same limits scaled to
the part it was measured for.
"""

from __future__ import annotations

import json
import os
import re
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent
BEIJING = timezone(timedelta(hours=8))

MATCH_WINDOW_SECONDS = 115 * 60
STABLE_MAX_NOPIC_SECONDS = 5 * 60
STABLE_MAX_DROPS = 6
# No verdict for a source measured less than this inside the window.
MIN_VERDICT_SECONDS = 15 * 60
# No new picture for this long = the picture broke.
STALL_SECONDS = 10.0
# Start pulling a backup once the current source broke this often / was
# without picture this long inside the window: about half of the limit, so the
# backup still gets most of the match to prove itself.
TRIGGER_DROPS = 3
TRIGGER_NOPIC_SECONDS = 120.0
TICK_SECONDS = 2.0
CATALOG_POLL_SECONDS = 60.0
BANDWIDTH_LOG_SECONDS = 60.0
BACKUP_RETRY_SECONDS = 5.0
# The preview directory vanished this long ago = the operator closed it.
PREVIEW_GONE_SECONDS = 120.0
DEFAULT_MAX_BACKUPS = 4

_OUT_TIME_RE = re.compile(r"^out_time_(?:us|ms)=(\d+)$")


def _log(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def _load_dotenv(path: Path) -> None:
    """Same ``KEY=value`` rules as monitor_server: the process env wins."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key:
            os.environ.setdefault(key, value)


def catalog_kickoff(item: dict[str, Any]) -> float:
    """Kick-off of a directory row. ``start_play`` is UTC wall time (checked
    2026-11-12: 中国U19-越南U19 ``11:35`` scored at 19:49 Beijing), while
    ``sort_timestamp`` - which ``match_catalog.catalog_match_timestamp``
    prefers - is that wall time read as Beijing, i.e. 8 hours early."""
    start_play = str(item.get("start_play") or "").strip()
    try:
        return datetime.strptime(start_play, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        pass
    try:
        return float(item.get("sort_timestamp")) + 8 * 3600
    except (TypeError, ValueError) as exc:
        raise ValueError(f"比赛 {item.get('match_id')!r} 没有开球时间") from exc


def stream_name(url: str) -> str:
    """Last path part (``pt-f7ww``); results never carry the full address."""
    return str(url or "").strip().split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]


# ── bookkeeping (pure, unit-tested) ─────────────────────────────────────────


class FlowTracker:
    """Breaks and seconds without picture of one source inside the window.

    ``sample(now, last_picture_at, in_window)`` is called every tick with the
    moment the newest picture arrived (``None`` = none yet). A break is
    counted when picture that had been flowing stops for ``STALL_SECONDS``.
    Seconds without picture count whether or not it had ever flowed: a source
    that never pushes is the worst case, not a free pass.
    """

    def __init__(self, stall_seconds: float = STALL_SECONDS) -> None:
        self.stall_seconds = stall_seconds
        self.observed = 0.0
        self.nopic = 0.0
        self.drops = 0
        self.longest_nopic = 0.0
        self._current_nopic = 0.0
        self._last_t: float | None = None
        self._flowing = False

    def sample(self, now: float, last_picture_at: float | None, in_window: bool) -> None:
        flowing = last_picture_at is not None and now - last_picture_at < self.stall_seconds
        if in_window:
            if self._last_t is not None:
                dt = max(0.0, now - self._last_t)
                self.observed += dt
                if not flowing:
                    self.nopic += dt
                    self._current_nopic += dt
                    self.longest_nopic = max(self.longest_nopic, self._current_nopic)
            if self._flowing and not flowing:
                self.drops += 1
        if flowing:
            self._current_nopic = 0.0
        self._flowing = flowing
        self._last_t = now if in_window else None

    def summary(self) -> dict[str, Any]:
        return {
            "observed_seconds": round(self.observed),
            "drops": self.drops,
            "nopic_seconds": round(self.nopic),
            "longest_nopic_seconds": round(self.longest_nopic),
            "stable": is_stable(self),
        }


def is_stable(tracker: FlowTracker) -> bool | None:
    """``None`` when measured too briefly to tell.

    Past the whole-match limits it is unstable however short the measurement.
    """
    if clearly_unstable(tracker):
        return False
    if tracker.observed < MIN_VERDICT_SECONDS:
        return None
    scale = min(1.0, tracker.observed / MATCH_WINDOW_SECONDS)
    return (
        tracker.nopic < STABLE_MAX_NOPIC_SECONDS * scale
        and tracker.drops < STABLE_MAX_DROPS * scale
    )


def clearly_unstable(tracker: FlowTracker) -> bool:
    """Already past the whole-match limits: no point measuring it further."""
    return tracker.nopic >= STABLE_MAX_NOPIC_SECONDS or tracker.drops >= STABLE_MAX_DROPS


def needs_backup(tracker: FlowTracker) -> bool:
    return tracker.drops >= TRIGGER_DROPS or tracker.nopic >= TRIGGER_NOPIC_SECONDS


def match_verdict(primaries: list[bool | None], backups: list[bool | None], untested: int) -> str:
    """可行 / 不可行 / 待定 from the stable flags of every measured source."""
    flags = list(primaries) + list(backups)
    if any(flag is True for flag in flags):
        return "可行"
    if not flags or any(flag is None for flag in flags) or untested > 0:
        return "待定"
    return "不可行"


def parse_out_time(line: str) -> int | None:
    """Media time from an ``ffmpeg -progress`` line, else ``None``."""
    match = _OUT_TIME_RE.match(line.strip())
    return int(match.group(1)) if match else None


def net_bytes(net_dev: str) -> tuple[int, int]:
    """``(received, sent)`` bytes over every interface but loopback."""
    rx = tx = 0
    for line in net_dev.splitlines()[2:]:
        if ":" not in line:
            continue
        name, data = line.split(":", 1)
        fields = data.split()
        if name.strip() == "lo" or len(fields) < 9:
            continue
        rx += int(fields[0])
        tx += int(fields[8])
    return rx, tx


def preview_source_of(argv: list[str], preview_root: Path) -> tuple[str, str] | None:
    """``(match_id, source_url)`` of an 8901 preview ffmpeg, else ``None``."""
    if not argv or "ffmpeg" not in Path(argv[0]).name or "-i" not in argv:
        return None
    root = str(preview_root).rstrip("/") + "/"
    index = argv.index("-i")
    if index + 1 >= len(argv):
        return None
    for arg in argv:
        if arg.startswith(root) and arg.endswith("index.m3u8"):
            match_id = arg[len(root):].split("/", 1)[0]
            if match_id.isdigit():
                return match_id, argv[index + 1]
    return None


# ── live parts ──────────────────────────────────────────────────────────────


class BackupPull:
    """One ``ffmpeg -c copy -f null`` pull of a backup source, restarted on exit."""

    def __init__(self, url: str, ffmpeg: str, clock: Callable[[], float] = time.time) -> None:
        self.url = url
        self.ffmpeg = ffmpeg
        self.clock = clock
        self.last_picture_at: float | None = None
        self._stop = threading.Event()
        self._process: subprocess.Popen[str] | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()

    def command(self) -> list[str]:
        return [
            self.ffmpeg, "-nostdin", "-hide_banner", "-loglevel", "error",
            # Global options before the output: ffmpeg 5.1 ignores trailing ones.
            "-progress", "pipe:1", "-stats_period", "1",
            "-rw_timeout", "15000000",
            "-i", self.url,
            "-map", "0", "-c", "copy", "-f", "null", "-",
        ]

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._process = subprocess.Popen(
                    self.command(),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                )
            except OSError as exc:
                _log("source_probe_backup_error", name=stream_name(self.url), error=str(exc))
                self._stop.wait(BACKUP_RETRY_SECONDS * 6)
                continue
            last_out = -1
            assert self._process.stdout is not None
            for line in self._process.stdout:
                value = parse_out_time(line)
                if value is None or value <= last_out:
                    continue
                if last_out >= 0:
                    self.last_picture_at = self.clock()
                last_out = value
            self._process.wait()
            self._stop.wait(BACKUP_RETRY_SECONDS)


@dataclass
class _Source:
    url: str
    kind: str
    tracker: FlowTracker = field(default_factory=FlowTracker)
    pull: Any = None
    reason: str = ""

    def summary(self) -> dict[str, Any]:
        return {"kind": self.kind, "name": stream_name(self.url), **self.tracker.summary()}


@dataclass
class _Match:
    match_id: str
    kickoff: float
    info: dict[str, Any]
    primary_url: str = ""
    primaries: dict[str, _Source] = field(default_factory=dict)
    backups: list[_Source] = field(default_factory=list)
    candidates: list[dict[str, Any]] | None = None
    preview_seen_at: float = 0.0
    no_backup_logged: bool = False


class SourceProbe:
    def __init__(
        self,
        *,
        preview_root: Path,
        results_path: Path,
        catalog_playing: Callable[[], list[dict[str, Any]]],
        collect_sources: Callable[[str], list[dict[str, Any]]],
        kickoff_of: Callable[[dict[str, Any]], float],
        kind_of: Callable[[str], str],
        make_pull: Callable[[str], Any],
        max_backups: int,
        clock: Callable[[], float] = time.time,
        scan_processes: Callable[[], dict[str, str]] | None = None,
        read_net_dev: Callable[[], str] | None = None,
    ) -> None:
        self.preview_root = preview_root
        self.results_path = results_path
        self.catalog_playing = catalog_playing
        self.collect_sources = collect_sources
        self.kickoff_of = kickoff_of
        self.kind_of = kind_of
        self.make_pull = make_pull
        self.max_backups = max_backups
        self.clock = clock
        self.scan_processes = scan_processes or self._scan_processes
        self.read_net_dev = read_net_dev or (lambda: Path("/proc/net/dev").read_text())
        self.matches: dict[str, _Match] = {}
        self.known: dict[str, dict[str, Any]] = {}
        self._catalog_at: float | None = None
        self._bw_at: float | None = None
        self._bw_bytes: tuple[int, int] | None = None

    # ── inputs ──
    def _scan_processes(self) -> dict[str, str]:
        found: dict[str, str] = {}
        for proc in Path("/proc").glob("[0-9]*"):
            try:
                raw = (proc / "cmdline").read_bytes()
            except OSError:
                continue
            args = [part.decode("utf-8", "replace") for part in raw.split(b"\0") if part]
            hit = preview_source_of(args, self.preview_root)
            if hit:
                found[hit[0]] = hit[1]
        return found

    def _refresh_catalog(self, now: float) -> None:
        if self._catalog_at is not None and now - self._catalog_at < CATALOG_POLL_SECONDS:
            return
        self._catalog_at = now
        try:
            playing = self.catalog_playing()
        except Exception as exc:  # noqa: BLE001 - keep the known kick-offs
            _log("source_probe_catalog_error", error=str(exc)[:200])
            return
        for item in playing:
            match_id = str(item.get("match_id") or "")
            try:
                kickoff = float(self.kickoff_of(item))
            except (TypeError, ValueError):
                continue
            self.known[match_id] = {
                "kickoff": kickoff,
                "competition": item.get("competition_name"),
                "home": item.get("team_A_name"),
                "away": item.get("team_B_name"),
            }
        stale = now - MATCH_WINDOW_SECONDS - 6 * 3600
        for match_id in [m for m, info in self.known.items() if info["kickoff"] < stale]:
            del self.known[match_id]

    def _previews(self) -> dict[str, float | None]:
        """``{match_id: mtime of its playlist}`` for every preview directory."""
        found: dict[str, float | None] = {}
        try:
            entries = list(self.preview_root.iterdir())
        except OSError:
            return found
        for entry in entries:
            if not entry.name.isdigit():
                continue
            try:
                found[entry.name] = (entry / "g1" / "index.m3u8").stat().st_mtime
            except OSError:
                found[entry.name] = None
        return found

    # ── loop ──
    def tick(self) -> None:
        now = self.clock()
        self._refresh_catalog(now)
        previews = self._previews()
        processes = self.scan_processes() if previews else {}
        for match_id, playlist_at in previews.items():
            match = self.matches.get(match_id)
            if match is None:
                info = self.known.get(match_id)
                if info is None or now > info["kickoff"] + MATCH_WINDOW_SECONDS:
                    continue
                match = _Match(match_id, info["kickoff"], info)
                self.matches[match_id] = match
            match.preview_seen_at = now
            url = processes.get(match_id) or match.primary_url
            if url and url != match.primary_url:
                self._switch_primary(match, url)
            source = match.primaries.get(match.primary_url)
            if source is not None:
                source.tracker.sample(now, playlist_at, self._in_window(match, now))
        for match in list(self.matches.values()):
            self._advance(match, now, match.match_id in previews)
        self._log_bandwidth(now)

    def _switch_primary(self, match: _Match, url: str) -> None:
        match.primary_url = url
        if url not in match.primaries:
            match.primaries[url] = _Source(url, self.kind_of(url))
        for backup in match.backups:
            if backup.url == url:
                # The operator switched 8901 to the source being measured:
                # 8901 now measures it, a second pull would only cost bandwidth.
                self._stop_backup(match, backup, "运营已切到这一路")

    def _in_window(self, match: _Match, now: float) -> bool:
        return match.kickoff <= now <= match.kickoff + MATCH_WINDOW_SECONDS

    def active_backups(self) -> int:
        return sum(1 for m in self.matches.values() for b in m.backups if b.pull is not None)

    def _advance(self, match: _Match, now: float, preview_present: bool) -> None:
        if now > match.kickoff + MATCH_WINDOW_SECONDS:
            self._finish(match)
            return
        in_window = self._in_window(match, now)
        preview_open = preview_present or now - match.preview_seen_at < PREVIEW_GONE_SECONDS
        for backup in match.backups:
            if backup.pull is None:
                continue
            backup.tracker.sample(now, backup.pull.last_picture_at, in_window)
            if not preview_open:
                self._stop_backup(match, backup, "预览已关闭")
            elif clearly_unstable(backup.tracker):
                self._stop_backup(match, backup, "备用源也不稳定")
        if not in_window or not preview_open:
            return
        primary = match.primaries.get(match.primary_url)
        if primary is None or not needs_backup(primary.tracker):
            return
        if any(b.pull is not None for b in match.backups):
            return
        self._start_next_backup(match, primary)

    def _untried(self, match: _Match) -> list[dict[str, Any]]:
        tried = {b.url for b in match.backups} | set(match.primaries)
        return [c for c in match.candidates or [] if c.get("url") and c["url"] not in tried]

    def _start_next_backup(self, match: _Match, primary: _Source) -> None:
        if self.active_backups() >= self.max_backups:
            return
        if match.candidates is None:
            try:
                match.candidates = list(self.collect_sources(match.match_id))
            except Exception as exc:  # noqa: BLE001 - try again next tick
                _log("source_probe_sources_error", match_id=match.match_id, error=str(exc)[:200])
                return
        untried = self._untried(match)
        if not untried:
            if not match.no_backup_logged:
                match.no_backup_logged = True
                _log("source_probe_no_backup", match_id=match.match_id, primary=primary.kind,
                     sources=len(match.candidates))
            return
        candidate = untried[0]
        reason = f"当前源断 {primary.tracker.drops} 次、没画面 {round(primary.tracker.nopic)} 秒"
        backup = _Source(candidate["url"], candidate.get("kind") or self.kind_of(candidate["url"]), reason=reason)
        backup.pull = self.make_pull(candidate["url"])
        backup.pull.start()
        match.backups.append(backup)
        _log("source_probe_backup_started", match_id=match.match_id, primary=primary.kind,
             backup=backup.kind, name=stream_name(backup.url), reason=reason,
             active=self.active_backups())

    def _stop_backup(self, match: _Match, backup: _Source, why: str) -> None:
        if backup.pull is None:
            return
        backup.pull.stop()
        backup.pull = None
        _log("source_probe_backup_stopped", match_id=match.match_id, backup=backup.kind,
             name=stream_name(backup.url), why=why, **backup.tracker.summary())

    def _finish(self, match: _Match) -> None:
        for backup in match.backups:
            self._stop_backup(match, backup, "比赛时段结束")
        self.matches.pop(match.match_id, None)
        if not match.primaries:
            return
        primary_flags = [is_stable(p.tracker) for p in match.primaries.values()]
        backup_flags = [is_stable(b.tracker) for b in match.backups]
        if True not in primary_flags + backup_flags and match.candidates is None:
            # Unstable without ever reaching the trigger (e.g. opened late):
            # look the other sources up now so 不可行 / 待定 can be told apart.
            try:
                match.candidates = list(self.collect_sources(match.match_id))
            except Exception as exc:  # noqa: BLE001 - verdict falls back to 待定
                _log("source_probe_sources_error", match_id=match.match_id, error=str(exc)[:200])
        untested = len(self._untried(match)) if match.candidates is not None else None
        # Lookup failed: other sources unknown, so never 不可行.
        verdict = match_verdict(primary_flags, backup_flags, 1 if untested is None else untested)
        record = {
            "match_id": match.match_id,
            "competition": match.info.get("competition"),
            "home": match.info.get("home"),
            "away": match.info.get("away"),
            "kickoff_unix": match.kickoff,
            "kickoff_bj": datetime.fromtimestamp(match.kickoff, BEIJING).strftime("%Y-%m-%d %H:%M"),
            "verdict": verdict,
            "source_count": len(match.candidates) if match.candidates is not None else None,
            "untested_sources": untested,
            "primary": [p.summary() for p in match.primaries.values()],
            "backups": [{**b.summary(), "reason": b.reason} for b in match.backups],
            "finished_at_unix": round(self.clock()),
        }
        self.results_path.parent.mkdir(parents=True, exist_ok=True)
        with self.results_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        _log("source_probe_result", match_id=match.match_id, competition=record["competition"],
             verdict=verdict, source_count=record["source_count"],
             primary=record["primary"], backups=record["backups"])

    def _log_bandwidth(self, now: float) -> None:
        if self._bw_at is not None and now - self._bw_at < BANDWIDTH_LOG_SECONDS:
            return
        try:
            current = net_bytes(self.read_net_dev())
        except (OSError, ValueError):
            current = None
        if self._bw_at is not None and current and self._bw_bytes and (self.matches or self.active_backups()):
            seconds = max(1.0, now - self._bw_at)
            _log(
                "source_probe_bandwidth",
                rx_mbps=round((current[0] - self._bw_bytes[0]) * 8 / seconds / 1e6, 2),
                tx_mbps=round((current[1] - self._bw_bytes[1]) * 8 / seconds / 1e6, 2),
                active_backups=self.active_backups(),
                matches=len(self.matches),
            )
        self._bw_at, self._bw_bytes = now, current

    def stop_all(self) -> None:
        for match in list(self.matches.values()):
            for backup in match.backups:
                self._stop_backup(match, backup, "测量程序停止")


def main() -> None:
    _load_dotenv(ROOT / ".env")
    import live_preview
    import live_sources
    from match_catalog import MatchCatalog

    try:
        max_backups = max(0, int(os.environ.get("GIF_SOURCE_PROBE_MAX", DEFAULT_MAX_BACKUPS)))
    except ValueError:
        max_backups = DEFAULT_MAX_BACKUPS
    ffmpeg = live_preview.resolve_ffmpeg()
    catalog = MatchCatalog()
    sources = live_sources.SourceCatalog.from_env()
    probe = SourceProbe(
        preview_root=live_preview.PREVIEW_ROOT,
        results_path=ROOT / "data" / "source_probe" / "results.jsonl",
        catalog_playing=lambda: catalog.snapshot().get("playing", []),
        collect_sources=lambda match_id: sources.collect(match_id)[0],
        kickoff_of=catalog_kickoff,
        kind_of=live_sources.kind_of,
        make_pull=lambda url: BackupPull(url, ffmpeg),
        max_backups=max_backups,
    )
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopping.set())
    signal.signal(signal.SIGINT, lambda *_: stopping.set())
    _log("source_probe_started", max_backups=max_backups, preview_root=str(live_preview.PREVIEW_ROOT))
    try:
        while not stopping.is_set():
            try:
                probe.tick()
            except Exception as exc:  # noqa: BLE001 - one bad tick must not end the service
                _log("source_probe_tick_error", error=str(exc)[:300])
            stopping.wait(TICK_SECONDS)
    finally:
        probe.stop_all()
        _log("source_probe_stopped")


if __name__ == "__main__":
    main()
