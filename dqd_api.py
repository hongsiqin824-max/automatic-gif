"""Shared low-level primitives for the Dongqiudi GIF tooling.

Both the automated dashboard (port 8899) and the manual monitor service
(port 8901) depend on the same HTTP helper, match-id validation, Beijing
timezone, and credential/env readers. Keeping them here lets both services
import one definition instead of duplicating it.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import timedelta, timezone
from typing import Any


BEIJING_TIMEZONE = timezone(timedelta(hours=8))
MATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,80}$")
MATCH_STATUS_LABELS = {
    "Cancelled": "已取消",
    "Fixture": "未开始",
    "Played": "已结束",
    "Playing": "进行中",
    "Postponed": "已推迟",
    "Suspended": "暂停",
    "Uncertain": "未知",
}


def positive_env_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} 必须是正整数") from exc
    if value < 1:
        raise RuntimeError(f"{name} 必须是正整数")
    return value


def positive_env_float(name: str, default: float) -> float:
    raw_value = os.environ.get(name, str(default))
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise RuntimeError(f"{name} 必须是正数") from exc
    if not math.isfinite(value) or value <= 0:
        raise RuntimeError(f"{name} 必须是正数")
    return value


def validate_match_id(value: str) -> str:
    match_id = value.strip()
    if not MATCH_ID_PATTERN.fullmatch(match_id):
        raise ValueError("match_id 只能包含字母、数字、下划线和连字符，长度不超过 80")
    return match_id


def read_response_body(response: Any) -> bytes:
    """Response bytes, gunzipped when the server answered ``Content-Encoding: gzip``.

    urllib never decompresses on its own, so a caller that sends
    ``Accept-Encoding: gzip`` must come through here; a plain response is
    returned unchanged in case the gateway skips compression.
    """
    raw = response.read()
    encoding = str(response.headers.get("Content-Encoding") or "").strip().lower()
    if encoding == "gzip":
        return gzip.decompress(raw)
    return raw


def request_json(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: float = 8.0,
    accept_gzip: bool = False,
) -> dict[str, Any]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "football-gif-dashboard/1.0",
    }
    if accept_gzip:
        headers["Accept-Encoding"] = "gzip"
    req = urllib.request.Request(url, data=payload, method=method, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        value = json.loads(read_response_body(response).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("API response must be a JSON object")
    return value


def match_user() -> str:
    """Return the user identity for match detail/event/catalog APIs."""
    return os.environ.get("GIF_MATCH_USER", "hongsiqin@dongqiudi.com")


def source_user() -> str:
    """Return the user identity for the live-source query API."""
    return os.environ.get("GIF_SOURCE_USER", "xuxinan@dongqiudi.com")


def source_secret() -> str:
    """Return the live-source query secret (backend only, never exposed)."""
    return os.environ.get("GIF_SOURCE_SECRET", "")
