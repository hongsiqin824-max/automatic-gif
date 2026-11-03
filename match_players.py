"""Player names for the 8901 goal/red-card pushes, from the match overview API.

The push monitor is fast but carries only the side, score and minute. The
overview API (the one the 8899 pipeline polls for its events) lists every
event with the player's name, and also tells a penalty / own goal / second
yellow apart from a plain goal / red card. It usually lags the push, so the
page asks again every few seconds until each push has its player or gives up.

Pairing a push with an overview event:

* goal: same side bucket and the same score. The score is exact while the
  minute is not (push 75', overview 74' for the same goal). An own goal sits
  in the bucket of the side it counted for, like the push, so the rule holds.
* red card: same side, a red/second-yellow code, minute within
  ``RED_CARD_MINUTE_TOLERANCE``; the nearest wins, a tie between two
  different players resolves to nothing rather than a guess.
"""

from __future__ import annotations

import re
import threading
import time
import urllib.parse
from typing import Any, Callable

from dqd_api import match_user, request_json


OVERVIEW_URL = "https://openapi.dongqiudi.com/internal/api/data/overview/match/{match_id}"
OVERVIEW_TIMEOUT_SECONDS = 5.0
# Below the page's 5 s poll, so a poll never gets an answer older than one
# poll interval even when several operators watch the same match.
OVERVIEW_CACHE_SECONDS = 4.0
RED_CARD_MINUTE_TOLERANCE = 3
GOAL_MINUTE_TOLERANCE = 2

# Overview code -> (word that replaces 进球/红牌 in the line, CMS match_event).
PLAYER_EVENT_CODES: dict[str, tuple[str, int]] = {
    "G": ("进球", 1),
    "PG": ("点球", 2),
    "OG": ("乌龙", 5),
    "RC": ("红牌", 4),
    "Y2C": ("两黄变红", 4),
}
GOAL_CODES = frozenset({"G", "PG", "OG"})
RED_CARD_CODES = frozenset({"RC", "Y2C"})

_SCORE_RE = re.compile(r"^\s*(\d+)\s*-\s*(\d+)\s*$")
_MINUTE_RE = re.compile(r"(\d{1,3})(?:\s*\+\s*\d{1,2})?\s*'?\s*$")
_STOPPAGE_RE = re.compile(r"^\s*(\d{1,3})\s*\+\s*(\d{1,2})\s*'?\s*$")


def _score_key(value: Any) -> str:
    match = _SCORE_RE.match(str(value or ""))
    return f"{int(match.group(1))}-{int(match.group(2))}" if match else ""


def _base_minute(value: Any) -> int | None:
    """``"2H 75"`` / ``"90+2"`` / ``"74"`` -> 75 / 90 / 74 (stoppage dropped)."""
    match = _MINUTE_RE.search(str(value or "").strip())
    return int(match.group(1)) if match else None


def _stoppage_time(*values: Any) -> str:
    """The first ``"90+6"``-style value, normalised; ``""`` when none has a ``+``."""
    for value in values:
        match = _STOPPAGE_RE.match(str(value or ""))
        if match:
            return f"{int(match.group(1))}+{int(match.group(2))}"
    return ""


def parse_overview_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Goals and red cards of an overview payload, one flat dict each.

    A bucket's ``minute`` is capped (``"90"``) while its key keeps the
    stoppage (``"90+6"``); the push is capped the same way, so ``match_time``
    carries the full clock the CMS needs to tell two stoppage-time events apart.
    """
    buckets = payload.get("events")
    if not isinstance(buckets, dict):
        return []
    events: list[dict[str, Any]] = []
    for bucket_key, bucket in buckets.items():
        if not isinstance(bucket, dict):
            continue
        minute = _base_minute(bucket.get("minute") or bucket_key)
        match_time = _stoppage_time(bucket_key, bucket.get("minute"))
        for side_key, side in (("teamAEvents", "A"), ("teamBEvents", "B")):
            raw_events = bucket.get(side_key)
            if not isinstance(raw_events, list):
                continue
            for raw in raw_events:
                if not isinstance(raw, dict):
                    continue
                code = str(raw.get("code") or "").upper()
                person = str(raw.get("person") or "").strip()
                if code not in PLAYER_EVENT_CODES or not person:
                    continue
                events.append(
                    {
                        "side": side,
                        "code": code,
                        "minute": minute,
                        "match_time": match_time,
                        "score": _score_key(raw.get("score")),
                        "person": person,
                        "person_id": str(raw.get("person_id") or ""),
                    }
                )
    return events


def _player_info(event: dict[str, Any]) -> dict[str, Any]:
    event_cn, match_event = PLAYER_EVENT_CODES[event["code"]]
    return {
        "person": event["person"],
        "person_id": event["person_id"],
        "code": event["code"],
        "event_cn": event_cn,
        "match_event": match_event,
        # "90+6" when the overview has the stoppage, "" otherwise.
        "match_time": event.get("match_time") or "",
    }


def _nearest(candidates: list[dict[str, Any]], minute: int | None) -> dict[str, Any] | None:
    if not candidates:
        return None
    if minute is None:
        persons = {c["person_id"] or c["person"] for c in candidates}
        return candidates[0] if len(persons) == 1 else None
    ranked = sorted(candidates, key=lambda c: abs((c["minute"] if c["minute"] is not None else 999) - minute))
    best = ranked[0]
    gap = abs((best["minute"] if best["minute"] is not None else 999) - minute)
    tied = [
        c for c in ranked
        if abs((c["minute"] if c["minute"] is not None else 999) - minute) == gap
        and (c["person_id"] or c["person"]) != (best["person_id"] or best["person"])
    ]
    return None if tied else best


def resolve_player(push: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The overview event behind one push, as page-ready player info, or None."""
    side = str(push.get("team") or "").upper()
    if side not in {"A", "B"}:
        return None
    minute = _base_minute(push.get("minute"))
    same_side = [e for e in events if e["side"] == side]
    if str(push.get("event_type") or "") == "red_card":
        reds = [
            e for e in same_side
            if e["code"] in RED_CARD_CODES
            and (minute is None or e["minute"] is None or abs(e["minute"] - minute) <= RED_CARD_MINUTE_TOLERANCE)
        ]
        found = _nearest(reds, minute)
        return _player_info(found) if found else None
    goals = [e for e in same_side if e["code"] in GOAL_CODES]
    score = _score_key(push.get("score"))
    if score:
        found = _nearest([e for e in goals if e["score"] == score], minute)
    else:
        found = _nearest(
            [
                e for e in goals
                if minute is not None and e["minute"] is not None
                and abs(e["minute"] - minute) <= GOAL_MINUTE_TOLERANCE
            ],
            minute,
        )
    return _player_info(found) if found else None


def fetch_overview(match_id: str) -> dict[str, Any]:
    query = urllib.parse.urlencode({"user": match_user()})
    url = OVERVIEW_URL.format(match_id=urllib.parse.quote(match_id)) + "?" + query
    return request_json(url, timeout=OVERVIEW_TIMEOUT_SECONDS)


class MatchPlayerLookup:
    """Per-match overview events, fetched at most once per ``cache_seconds``.

    A failed fetch keeps serving the last good events (or none), so a slow or
    broken overview API only delays player names; it never breaks the page.
    """

    def __init__(
        self,
        fetch: Callable[[str], dict[str, Any]] = fetch_overview,
        *,
        cache_seconds: float = OVERVIEW_CACHE_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._fetch = fetch
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[float, list[dict[str, Any]]]] = {}
        self.last_error: str | None = None

    def events_for(self, match_id: str) -> list[dict[str, Any]]:
        now = self._clock()
        with self._lock:
            cached = self._cache.get(match_id)
            if cached and now - cached[0] < self._cache_seconds:
                return cached[1]
        try:
            events = parse_overview_events(self._fetch(match_id))
            self.last_error = None
        except Exception as exc:  # noqa: BLE001 - player names are advisory
            self.last_error = str(exc)
            events = cached[1] if cached else []
        with self._lock:
            self._cache[match_id] = (now, events)
            if len(self._cache) > 64:
                oldest = min(self._cache, key=lambda key: self._cache[key][0])
                self._cache.pop(oldest, None)
        return events
