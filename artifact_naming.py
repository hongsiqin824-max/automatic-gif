"""Deterministic, filesystem-safe names for generated event artifacts."""

from __future__ import annotations

import math
import unicodedata
from typing import Any, Mapping


EVENT_TYPE_BY_CODE = {
    "G": "goal",
    "OG": "own-goal",
    "YC": "yellow-card",
    "RC": "red-card",
}

MAX_FILENAME_BYTES = 240
_MAX_MATCH_BYTES = 64
_MAX_PERSON_BYTES = 96
_MAX_SCORE_BYTES = 24
_MISSING_PLAYER_IDS = {"", "0", "-", "none", "null", "unknown"}


def _truncate_utf8(value: str, max_bytes: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _safe_token(value: Any, *, fallback: str, max_bytes: int) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or "")).strip()
    characters: list[str] = []
    separator_pending = False
    for character in normalized:
        category = unicodedata.category(character)
        if character.isalnum() or category.startswith("M"):
            if separator_pending and characters:
                characters.append("-")
            characters.append(character)
            separator_pending = False
        elif character == "-":
            separator_pending = bool(characters)
        else:
            separator_pending = bool(characters)
    token = "".join(characters).strip("-") or fallback
    token = _truncate_utf8(token, max_bytes).strip("-") or fallback
    return token


def _nonnegative_integer(value: Any) -> int | None:
    normalized = unicodedata.normalize("NFKC", str(value or "")).strip()
    normalized = normalized.rstrip("'\N{PRIME}\N{RIGHT SINGLE QUOTATION MARK}").strip()
    if not normalized:
        return None
    try:
        number = float(normalized)
    except ValueError:
        return None
    if not math.isfinite(number) or number < 0 or not number.is_integer():
        return None
    return int(number)


def _minute_token(event_data: Mapping[str, Any]) -> str:
    raw_minute = unicodedata.normalize(
        "NFKC", str(event_data.get("minute") or "")
    ).strip()
    embedded_extra: str | None = None
    if "+" in raw_minute:
        raw_minute, embedded_extra = raw_minute.split("+", 1)
    minute = _nonnegative_integer(raw_minute)
    explicit_extra = _nonnegative_integer(event_data.get("minute_extra"))
    extra = explicit_extra if explicit_extra not in (None, 0) else None
    if extra is None:
        embedded = _nonnegative_integer(embedded_extra)
        extra = embedded if embedded not in (None, 0) else None
    token = f"m{minute:03d}" if minute is not None else "mUNK"
    if extra is not None:
        token += f"+{extra:02d}"
    return token


def _player_token(event_data: Mapping[str, Any]) -> str:
    person = _safe_token(
        event_data.get("person"),
        fallback="",
        max_bytes=_MAX_PERSON_BYTES,
    )
    if person:
        return person
    person_id = _safe_token(
        event_data.get("person_id"),
        fallback="",
        max_bytes=_MAX_PERSON_BYTES - len("player-"),
    )
    if person_id.casefold() in _MISSING_PLAYER_IDS:
        return "unknown"
    return f"player-{person_id}"


def build_gif_filename(
    *,
    match_id: str,
    event_data: Mapping[str, Any],
    variant: str = "default",
    max_bytes: int = MAX_FILENAME_BYTES,
) -> str:
    """Build a stable event GIF filename without changing existing artifacts."""
    if max_bytes <= 0 or max_bytes > 255:
        raise ValueError("GIF filename byte limit must be between 1 and 255")
    code = unicodedata.normalize(
        "NFKC", str(event_data.get("code") or "")
    ).strip().upper()
    try:
        event_type = EVENT_TYPE_BY_CODE[code]
    except KeyError as exc:
        raise ValueError(f"unsupported event code for GIF naming: {code or '-'}") from exc

    safe_match_id = _safe_token(
        match_id,
        fallback="match",
        max_bytes=_MAX_MATCH_BYTES,
    )
    player = _player_token(event_data)
    score = _safe_token(
        event_data.get("score"),
        fallback="",
        max_bytes=_MAX_SCORE_BYTES,
    )
    safe_variant = _safe_token(variant, fallback="default", max_bytes=24)
    event_key_tail = str(event_data.get("event_key") or "").rsplit(":", 1)[-1]
    digest = _safe_token(
        event_key_tail[:6],
        fallback="event",
        max_bytes=18,
    )

    parts = [safe_match_id, _minute_token(event_data), event_type, player]
    if score:
        parts.append(score)
    parts.extend((safe_variant, digest))
    filename = "_".join(parts) + ".gif"
    if len(filename.encode("utf-8")) > max_bytes:
        overflow = len(filename.encode("utf-8")) - max_bytes
        player_limit = max(8, len(player.encode("utf-8")) - overflow)
        player = _truncate_utf8(player, player_limit).strip("-") or "unknown"
        parts[3] = player
        filename = "_".join(parts) + ".gif"
    if len(filename.encode("utf-8")) > max_bytes:
        raise ValueError("GIF filename byte limit is too small for required fields")
    return filename
