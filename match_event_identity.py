"""Stable identity and metadata helpers for cumulative match-event feeds."""

from __future__ import annotations

import math
from typing import Any, Mapping


GOAL_CODES = {"G", "OG", "PG"}
MISSING_IDS = {"", "0", "none", "null"}


def normalized_event_code(value: Any) -> str:
    return str(value or "").upper()


def event_family(value: Mapping[str, Any]) -> str:
    code = normalized_event_code(value.get("code"))
    return "goal" if code in GOAL_CODES else code.lower()


def meaningful_id(value: Any) -> str:
    normalized = str(value or "").strip()
    return "" if normalized.lower() in MISSING_IDS else normalized


def explicit_event_id(value: Mapping[str, Any]) -> str:
    metadata = value.get("metadata")
    sources = [value]
    if isinstance(metadata, Mapping):
        sources.append(metadata)
    for source in sources:
        for key in ("event_id", "eventId", "id"):
            event_id = meaningful_id(source.get(key))
            if event_id:
                return event_id
    return ""


def minute_value(value: Mapping[str, Any]) -> int | None:
    try:
        return int(str(value.get("minute") or "").strip())
    except ValueError:
        return None


def _nonnegative_integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or parsed < 0 or not parsed.is_integer():
        return None
    return int(parsed)


def stable_event_second(value: Mapping[str, Any]) -> int | None:
    """Return a trustworthy cumulative match second when one is available."""
    direct = _nonnegative_integer(value.get("second"))
    if direct is not None:
        return direct
    metadata = value.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    if (
        str(metadata.get("second_source") or "").lower() != "shotmap"
        and str(metadata.get("shotmap_match_status") or "").lower() != "matched"
    ):
        return None
    for key in ("second", "shotmap_second"):
        parsed = _nonnegative_integer(metadata.get(key))
        if parsed is not None:
            return parsed
    candidates = metadata.get("shotmap_candidate_details")
    if isinstance(candidates, list) and len(candidates) == 1:
        candidate = candidates[0]
        if isinstance(candidate, Mapping):
            return _nonnegative_integer(candidate.get("second"))
    return None


def events_represent_same_incident(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
    *,
    allow_exact_match: bool = True,
) -> bool:
    """Conservatively reconcile an API revision with an existing incident."""
    left_id = explicit_event_id(left)
    right_id = explicit_event_id(right)
    if left_id and right_id:
        return left_id == right_id
    left_second = stable_event_second(left)
    right_second = stable_event_second(right)
    if (
        left_second is not None
        and right_second is not None
        and left_second != right_second
    ):
        return False
    # A pair with no durable ID needs evidence that one record is a richer or
    # corrected version of the other. Identical task payloads can be separate
    # jobs created by an older worker and must not be silently suppressed.
    detail_keys = (
        "code",
        "event_type",
        "minute",
        "minute_extra",
        "team",
        "person",
        "person_id",
        "score",
        "reason",
    )
    if all(str(left.get(key) or "") == str(right.get(key) or "") for key in detail_keys):
        return allow_exact_match
    if event_family(left) != event_family(right):
        return False
    if str(left.get("team") or "") != str(right.get("team") or ""):
        return False

    left_minute = minute_value(left)
    right_minute = minute_value(right)
    minutes_are_close = (
        left_minute is not None
        and right_minute is not None
        and abs(left_minute - right_minute) <= (2 if event_family(left) == "goal" else 1)
    )
    if not minutes_are_close:
        return False

    family = event_family(left)
    if left_second is not None and right_second is not None:
        return left_second == right_second

    left_score = str(left.get("score") or "").replace(" ", "")
    right_score = str(right.get("score") or "").replace(" ", "")
    left_person = meaningful_id(left.get("person_id"))
    right_person = meaningful_id(right.get("person_id"))
    if left_person and right_person and left_person != right_person:
        return False
    if family in {"yc", "rc"} and left_person and right_person:
        return left_person == right_person and minutes_are_close
    if family in {"yc", "rc"}:
        # Keep incomplete card records separate until the provider supplies a
        # durable ID or player; combining them would hide real cards.
        return False
    if family != "goal":
        return False

    left_person_name = str(left.get("person") or "").strip()
    right_person_name = str(right.get("person") or "").strip()
    stable_person = bool(
        left_person and right_person and left_person == right_person
    )
    stable_person_name = bool(
        left_person_name
        and right_person_name
        and left_person_name.casefold() == right_person_name.casefold()
    )
    if stable_person:
        return True
    if left_person_name and right_person_name and not stable_person_name:
        return False
    if stable_person_name:
        return True
    if left_score and right_score:
        if left_score == right_score:
            # A populated score is a useful incident-level anchor even while
            # the provider is still filling in the scorer or minute.
            return True
        return False
    if bool(left_score) != bool(right_score):
        # A missing score is common in partially populated rows, but by itself
        # it cannot distinguish an API revision from the next real goal.
        return False
    # With neither score nor a stable scorer there is no revision signal.
    return False


def merge_event_metadata(
    current: Mapping[str, Any], update: Mapping[str, Any]
) -> dict[str, Any]:
    """Apply a newer feed revision without erasing useful populated fields."""
    merged = dict(current)
    for key, value in update.items():
        if key == "metadata":
            old_metadata = merged.get("metadata")
            merged[key] = {
                **(dict(old_metadata) if isinstance(old_metadata, Mapping) else {}),
                **(dict(value) if isinstance(value, Mapping) else {}),
            }
            continue
        if value is None or value == "":
            continue
        if key == "person_id" and not meaningful_id(value):
            continue
        merged[key] = value
    return merged
