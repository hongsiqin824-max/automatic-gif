"""Live-source query client shared by the dashboard and the monitor service.

Wraps the Dongqiudi ``inner/tool/match/live_source/query`` endpoint and the
response classification used to tell a confirmed empty source apart from a
transient failure. See ``inner-tool-match-live-source-query-api`` docs.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Any

from dqd_api import request_json, source_secret, source_user


DEFAULT_SOURCE_URL = (
    "https://openapi.dongqiudi.com/internal/sport-data/inner/tool/"
    "match/live_source/query"
)


@dataclass(frozen=True)
class SourceCheckResult:
    """Classified result for a live-source probe."""

    state: str
    data: dict[str, Any]
    error: str | None = None
    error_kind: str | None = None
    retryable: bool = False


def query_source(match_id: str) -> dict[str, Any]:
    """Query the configured live source for one match.

    ``user`` must travel on the query string (gateway requirement) while the
    ``secret`` stays in the JSON body and is never exposed to the browser.
    """
    secret = source_secret()
    if not secret:
        raise RuntimeError("未设置 GIF_SOURCE_SECRET，暂不调用直播源鉴权接口")
    query = urllib.parse.urlencode({"user": source_user(), "match_id": match_id})
    return request_json(
        DEFAULT_SOURCE_URL + "?" + query,
        method="POST",
        body={"secret": secret},
    )


def classify_source_response(response: Any) -> SourceCheckResult:
    """Separate a confirmed empty source from a failed source request.

    The source API documents ``errno=0`` plus an empty ``data`` object as a
    valid "no source" result. Authentication, transport, and malformed
    responses must remain retryable so an outage cannot permanently discard a
    match. A missing ``errno`` is treated as a protocol error so an incomplete
    proxy response cannot be mistaken for a confirmed empty source.
    """
    if not isinstance(response, dict):
        return SourceCheckResult(
            "error", {}, "直播源接口返回不是 JSON 对象", "source_protocol_error", True
        )
    errno_value = response.get("errno")
    # ``bool`` is a subclass of ``int`` in Python; reject it explicitly so a
    # malformed ``errno=false`` response cannot be treated as success.
    errno_success = (
        not isinstance(errno_value, bool)
        and errno_value in (0, "0")
    )
    if not errno_success:
        if errno_value is None:
            return SourceCheckResult(
                "error",
                {},
                "直播源接口响应缺少 errno",
                "source_protocol_error",
                True,
            )
        message = str(response.get("message") or f"errno={errno_value}").strip()
        kind = (
            "source_auth_error"
            if any(token in message for token in ("secret", "user", "校验", "鉴权"))
            else "source_api_error"
        )
        # Even configuration errors are not classified as no-source. Retrying
        # lets an operator correct a secret/user without losing the match.
        return SourceCheckResult("error", {}, message, kind, True)
    if "data" not in response:
        return SourceCheckResult(
            "error", {}, "直播源接口响应缺少 data", "source_protocol_error", True
        )
    data = response.get("data")
    if not isinstance(data, dict):
        return SourceCheckResult(
            "error", {}, "直播源接口 data 不是对象", "source_protocol_error", True
        )
    resource = data.get("resource")
    if isinstance(resource, str) and resource.strip():
        normalized = dict(data)
        normalized["resource"] = resource.strip()
        return SourceCheckResult("available", normalized)
    if not data:
        return SourceCheckResult("no_source", {})
    return SourceCheckResult(
        "error",
        data,
        "直播源接口返回了记录，但缺少有效 resource",
        "source_protocol_error",
        True,
    )
