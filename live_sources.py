"""Every pullable live stream of a match, ranked, for the 8901 preview.

Three places can hold a stream for one match:

* ``live_source/query`` ``live_type=dqd`` - Dongqiudi's own record: ``hd-``
  (feed from the rights holder) or ``pt-`` (HD feed scraped from a website);
* ``live_source/query`` ``live_type=ls`` - the data provider's record:
  ``sd-`` / ``hd-en-``, or ``ai-`` (scraped by the provider itself when it has
  no rights; the only option for e.g. the A-League);
* ``stream/pull-addresses`` - streams pushed for the expert backend (not shown
  in the Dongqiudi app): every match that is live right now, each with all of
  its pull URLs (``zb-`` and others). Our match id is ``dqd_id + 50000000``.

The same URL can come back from two places (11-09: 54477923 ``pt-a9nz`` was
both the dqd record and a pull address). It is one stream and listed once.

The default order is measured stability, not resolution: the preview is
re-encoded to 480p and GIFs are 440 px wide, so anything above 480p looks the
same here. Stall waves per watched hour, 11-04..11-09 (nginx watch minutes as
the denominator): hd-en 0.21, hd 0.28, sd 0.70, ai 0.99 (4 matches), pt 3.88;
zb was never watched and goes last, as the source team ranks it.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable

from source_client import SourceCheckResult, classify_source_response, query_source


PULL_ADDRESSES_URL = "https://sportdata-api.aisportsapp.com/stream/pull-addresses"
# ``dqd_id`` of the pull-address API + this = the match id 8901 uses.
DQD_ID_OFFSET = 50_000_000
DEFAULT_SOURCE_ORDER = ("hd", "hd-en", "sd", "pt", "ai", "zb")
KIND_LABELS = {
    "hd": "懂球帝·版权方 hd",
    "pt": "懂球帝·网站抓取 pt",
    "hd-en": "数据源 hd-en",
    "sd": "数据源 sd",
    "ai": "数据源·自抓 ai",
    "zb": "备用流 zb",
    "other": "其他源",
}
# Shown next to each source in the menu; the 11-04..11-09 measurements above.
KIND_NOTES = {
    "hd-en": "实测约 5 小时卡一次",
    "hd": "实测约 3.5 小时卡一次",
    "sd": "实测约 1.5 小时卡一次",
    "ai": "实测约 1 小时卡一次（样本少）",
    "pt": "实测约 15 分钟卡一次（晚上更差）",
    "zb": "还没有实测数据",
    "other": "",
}
ORIGIN_LABELS = {"dqd": "懂球帝直播源", "ls": "数据源", "pull": "拉流接口"}
# Same rank: the dqd record first, then ls, then the pull API.
ORIGIN_PRIORITY = {"dqd": 0, "ls": 1, "pull": 2}
_KIND_RE = re.compile(r"(?:^|/)(hd-en|hd|pt|sd|ai|zb)-[^/]*$")
_STREAM_SCHEMES = ("rtmp://", "rtmps://", "http://", "https://", "rtsp://", "rtsps://")


def kind_of(url: str) -> str:
    """Stream type from the last path part's prefix (``hd-en-`` before ``hd-``)."""
    path = str(url or "").strip().split("?", 1)[0].rstrip("/")
    match = _KIND_RE.search(path)
    return match.group(1) if match else "other"


def source_id_of(url: str) -> str:
    """Opaque id the browser uses to name a source (never the URL itself)."""
    return hashlib.sha1(str(url).strip().encode("utf-8")).hexdigest()[:12]


def stream_name(url: str) -> str:
    """Last path part (``pt-f7ww``): enough to tell streams apart in a log."""
    return str(url or "").strip().split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]


def parse_source_order(raw: str | None) -> tuple[str, ...]:
    """``GIF_MONITOR_SOURCE_ORDER`` -> kinds, best first.

    Unknown names are ignored; known kinds the setting leaves out keep their
    default relative order after the listed ones, so a typo can never make a
    stream unselectable.
    """
    listed: list[str] = []
    for token in str(raw or "").split(","):
        kind = token.strip().lower().rstrip("-")
        if kind in DEFAULT_SOURCE_ORDER and kind not in listed:
            listed.append(kind)
    if not listed:
        return DEFAULT_SOURCE_ORDER
    return tuple(listed) + tuple(k for k in DEFAULT_SOURCE_ORDER if k not in listed)


def describe(url: str) -> dict[str, str]:
    kind = kind_of(url)
    return {
        "source_id": source_id_of(url),
        "kind": kind,
        "label": KIND_LABELS.get(kind, KIND_LABELS["other"]),
        "note": KIND_NOTES.get(kind, ""),
        "name": stream_name(url),
    }


def _log(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def resolve_ffprobe() -> str:
    found = shutil.which("ffprobe")
    if found:
        return found
    ffmpeg = shutil.which("ffmpeg") or "/opt/homebrew/bin/ffmpeg"
    return str(Path(ffmpeg).with_name("ffprobe"))


def probe_source(url: str, *, timeout_seconds: float = 5.0, ffprobe: str | None = None) -> dict[str, Any]:
    """Connect to ``url`` briefly and report whether it delivers decodable video.

    A pushing stream answers in well under a second (the CDN replays its
    cached GOP: 0.27 s for ``pt-a9nz`` on the server, 11-09); one that is not
    pushing holds the connection until ``-rw_timeout`` (or answers
    ``Operation not permitted``). A video stream ffprobe cannot name - the
    private H.265 flavour the server's ffmpeg 5.1 cannot decode - counts as
    unusable, so the preview never switches to a black picture.
    """
    command = [
        ffprobe or resolve_ffprobe(),
        "-v", "error",
        "-rw_timeout", str(int(timeout_seconds * 1_000_000)),
        "-analyzeduration", "1500000",
        "-probesize", "1000000",
        "-show_entries", "stream=codec_type,codec_name,width,height",
        "-of", "json",
        url,
    ]
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            timeout=timeout_seconds + 4.0,
        )
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "连接超时", "seconds": round(time.monotonic() - started, 2)}
    except OSError as exc:
        return {"ok": False, "error": f"无法运行 ffprobe：{exc}", "seconds": 0.0}
    seconds = round(time.monotonic() - started, 2)
    try:
        streams = json.loads(completed.stdout.decode("utf-8", "replace") or "{}").get("streams") or []
    except ValueError:
        streams = []
    video = next(
        (s for s in streams if s.get("codec_type") == "video" and s.get("codec_name")),
        None,
    )
    if video is None:
        lines = [
            line.strip()
            for line in completed.stderr.decode("utf-8", "replace").splitlines()
            if line.strip()
        ]
        reason = lines[-1] if lines else "没有可解码的视频画面"
        # ffprobe prefixes the URL; it is long and sensitive, drop it.
        reason = reason.replace(url, stream_name(url))
        return {"ok": False, "error": reason[:160], "seconds": seconds}
    return {
        "ok": True,
        "codec": video.get("codec_name"),
        "width": video.get("width"),
        "height": video.get("height"),
        "seconds": seconds,
    }


def fetch_pull_addresses(url: str, api_key: str, timeout: float) -> dict[str, list[str]]:
    """``{match_id: [pull urls]}`` from the pull-address API (raises on failure)."""
    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "football-gif-monitor/1.0",
            "X-Stream-Api-Key": api_key,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("items"), list):
        raise ValueError("拉流接口返回格式不对（缺少 items）")
    result: dict[str, list[str]] = {}
    for item in value["items"]:
        if not isinstance(item, dict):
            continue
        dqd_id = str(item.get("dqd_id") or "").strip()
        if not dqd_id.isdigit():
            continue
        match_id = str(int(dqd_id) + DQD_ID_OFFSET)
        urls = result.setdefault(match_id, [])
        for raw in item.get("pull_urls") or []:
            candidate = str(raw or "").strip()
            if candidate.lower().startswith(_STREAM_SCHEMES) and candidate not in urls:
                urls.append(candidate)
    return {match_id: urls for match_id, urls in result.items() if urls}


class PullAddressClient:
    """Cached view of the pull-address API.

    The API has no parameters and returns every live match at once, so one
    request per ``ttl_seconds`` serves all matches; the request is made on
    demand by whoever needs it first (no background thread). A failure keeps
    the previous answer for up to three TTLs - the API only lists streams that
    are pushing, so older data could offer dead streams - and is not retried
    for ``retry_seconds``, so an outage never adds its timeout to every
    preview open. Without an API key the client is off and lists nothing.
    """

    def __init__(
        self,
        *,
        url: str = PULL_ADDRESSES_URL,
        api_key: str = "",
        ttl_seconds: float = 45.0,
        timeout: float = 3.0,
        retry_seconds: float = 30.0,
        fetch: Callable[[str, str, float], dict[str, list[str]]] = fetch_pull_addresses,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.url = url
        self.api_key = api_key.strip()
        self.ttl_seconds = float(ttl_seconds)
        self.timeout = float(timeout)
        self.retry_seconds = float(retry_seconds)
        self._fetch = fetch
        self._clock = clock
        self._lock = threading.Lock()
        self._by_match: dict[str, list[str]] = {}
        self._fetched_at: float | None = None
        self._failed_at: float | None = None
        self._failing = False

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def urls_for(self, match_id: str) -> list[str]:
        if not self.enabled:
            return []
        with self._lock:
            now = self._clock()
            fresh = self._fetched_at is not None and now - self._fetched_at < self.ttl_seconds
            backing_off = self._failed_at is not None and now - self._failed_at < self.retry_seconds
            if not fresh and not backing_off:
                self._refresh_locked(now)
            if self._fetched_at is None or self._clock() - self._fetched_at >= 3 * self.ttl_seconds:
                return []
            return list(self._by_match.get(match_id, []))

    def _refresh_locked(self, now: float) -> None:
        try:
            data = self._fetch(self.url, self.api_key, self.timeout)
        except Exception as exc:  # noqa: BLE001 - an optional extra source
            self._failed_at = now
            if not self._failing:
                # Once per outage; the key is never part of the message.
                _log("stream_pull_failed", error=f"{type(exc).__name__}: {str(exc)[:160]}")
            self._failing = True
            return
        if self._failing:
            _log("stream_pull_recovered", matches=len(data))
        self._failing = False
        self._failed_at = None
        self._by_match = data
        self._fetched_at = self._clock()


class RecordCache:
    """Short-lived cache of ``live_source/query`` records (both live types).

    Opening a busy grid fires one preview/start per tile, each now reads both
    records, and every tile's source menu refreshes them too; the query
    gateway has answered 429 under such bursts (11-07 23:21 and 00:46). Only
    found records are kept, for ``ttl_seconds``: "no record" and failures are
    asked again every time, so a stream configured after kick-off is still
    picked up by the tile's next no-source retry, as before.
    """

    def __init__(
        self,
        query: Callable[..., Any] = query_source,
        *,
        ttl_seconds: float = 15.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._query = query
        self.ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, str], tuple[float, SourceCheckResult]] = {}

    def get(self, match_id: str, live_type: str) -> SourceCheckResult:
        key = (match_id, live_type)
        with self._lock:
            cached = self._entries.get(key)
            if cached is not None and self._clock() - cached[0] < self.ttl_seconds:
                return cached[1]
        result = classify_source_response(self._query(match_id, live_type=live_type))
        if result.state == "available":
            with self._lock:
                self._entries[key] = (self._clock(), result)
                if len(self._entries) > 2000:
                    cutoff = self._clock() - self.ttl_seconds
                    for stale in [k for k, (at, _) in self._entries.items() if at < cutoff]:
                        del self._entries[stale]
        return result


class SourceChoices:
    """Which source an operator picked for a match, remembered for the match.

    In memory only (a service restart forgets it, as it forgets the buffered
    history). Expires after ``ttl_seconds`` so a far later match reusing an
    id cannot inherit it.
    """

    def __init__(self, *, ttl_seconds: float = 6 * 3600.0, clock: Callable[[], float] = time.time) -> None:
        self.ttl_seconds = float(ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._choices: dict[str, dict[str, Any]] = {}

    def remember(self, match_id: str, url: str, by: str | None) -> dict[str, Any]:
        choice = {"url": url, "by": by or "", "at": self._clock()}
        with self._lock:
            self._choices[match_id] = choice
        return dict(choice)

    def get(self, match_id: str) -> dict[str, Any] | None:
        with self._lock:
            choice = self._choices.get(match_id)
            if choice is None:
                return None
            if self._clock() - choice["at"] > self.ttl_seconds:
                del self._choices[match_id]
                return None
            return dict(choice)


class SourceCatalog:
    """All streams of a match from the three places, merged and ranked."""

    def __init__(
        self,
        *,
        records: RecordCache,
        pull: PullAddressClient,
        choices: SourceChoices,
        order: tuple[str, ...] = DEFAULT_SOURCE_ORDER,
        probe: Callable[..., dict[str, Any]] = probe_source,
    ) -> None:
        self.records = records
        self.pull = pull
        self.choices = choices
        self.order = order
        self.probe = probe

    @classmethod
    def from_env(cls, query: Callable[..., Any] = query_source) -> "SourceCatalog":
        return cls(
            records=RecordCache(query, ttl_seconds=30.0),
            pull=PullAddressClient(api_key=os.environ.get("GIF_STREAM_PULL_API_KEY", "")),
            choices=SourceChoices(),
            order=parse_source_order(os.environ.get("GIF_MONITOR_SOURCE_ORDER")),
        )

    def rank_of(self, kind: str) -> int:
        try:
            return self.order.index(kind)
        except ValueError:
            return len(self.order)

    def collect(self, match_id: str) -> tuple[list[dict[str, Any]], list[str]]:
        """``(sources best first, query errors)``.

        Each source: ``source_id``, ``url``, ``kind``, ``label``, ``note``,
        ``name``, ``origins`` (where it was found), ``pushing`` (listed by the
        pull API, which only lists streams being pushed now) and ``rank``.
        """
        found: dict[str, dict[str, Any]] = {}
        errors: list[str] = []

        def add(url: str, origin: str) -> None:
            url = str(url or "").strip()
            if not url:
                return
            entry = found.get(url)
            if entry is None:
                entry = {**describe(url), "url": url, "origins": [], "pushing": False, "_seen": len(found)}
                found[url] = entry
            if origin not in entry["origins"]:
                entry["origins"].append(origin)
            if origin == "pull":
                entry["pushing"] = True

        for live_type in ("dqd", "ls"):
            try:
                result = self.records.get(match_id, live_type)
            except Exception as exc:  # noqa: BLE001 - recorded, others still count
                errors.append(f"{live_type}: {exc}")
                continue
            if result.state == "available":
                add(str(result.data.get("resource") or ""), live_type)
            elif result.state == "error":
                errors.append(f"{live_type}: {result.error or '查询失败'}")
        for url in self.pull.urls_for(match_id):
            add(url, "pull")
        sources = list(found.values())
        for source in sources:
            source["rank"] = self.rank_of(source["kind"])
            source["origins"].sort(key=lambda o: ORIGIN_PRIORITY.get(o, 9))
        sources.sort(
            key=lambda s: (s["rank"], ORIGIN_PRIORITY.get(s["origins"][0], 9), s["_seen"])
        )
        for source in sources:
            del source["_seen"]
        return sources, errors

    def pick_startable(
        self,
        sources: list[dict[str, Any]],
        *,
        preferred_url: str | None = None,
        timeout_seconds: float = 3.0,
        max_probes: int = 4,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """Best source that is pushing right now, for a cold preview start.

        With a single candidate nothing is probed (nothing to choose from).
        Otherwise the first ``max_probes`` candidates - the operator's pick
        first, then the ranking - are probed in parallel and the first one
        in that order with a picture wins: a live top choice answers in well
        under a second, a dead one costs at most ``timeout_seconds``. When
        none answers, the first candidate is used anyway and the preview's
        auto-restart keeps waiting for it, exactly as before.
        """
        if not sources:
            raise ValueError("没有可选的直播源")
        ordered = list(sources)
        if preferred_url:
            ordered.sort(key=lambda s: 0 if s["url"] == preferred_url else 1)
        if len(ordered) == 1:
            return ordered[0], None
        candidates = ordered[:max_probes]
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(candidates), thread_name_prefix="source-probe"
        )
        try:
            futures = [
                executor.submit(self.probe, s["url"], timeout_seconds=timeout_seconds)
                for s in candidates
            ]
            for source, future in zip(candidates, futures):
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001
                    result = {"ok": False, "error": str(exc)}
                if result.get("ok"):
                    return source, result
        finally:
            # Do not wait for slower probes of candidates that lost: their
            # ffprobe runs out on its own -rw_timeout in the background.
            executor.shutdown(wait=False, cancel_futures=True)
        return ordered[0], None
