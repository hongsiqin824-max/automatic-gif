"""Validation and normalization for cumulative match-event API responses."""

from __future__ import annotations

from typing import Any


class EventApiResponseError(ValueError):
    """A response-shape error with a stable reason for caller-specific wording."""

    def __init__(self, reason: str, *, status: Any = None) -> None:
        self.reason = reason
        self.status = status
        if reason == "status":
            message = f"event API returned status={status!r}"
        elif reason == "events":
            message = "event API response does not contain an events object"
        else:
            message = "event API response must be a JSON object"
        super().__init__(message)


def normalize_event_api_response(payload: object) -> dict[str, Any]:
    """Return one valid event response shape shared by all event consumers.

    The provider may omit ``status`` or return it as ``null`` for a successful
    response.  An empty event feed is represented as an empty object internally
    so callers only need to handle one collection shape.
    """
    if not isinstance(payload, dict):
        raise EventApiResponseError("object")

    status = payload.get("status")
    is_success = status is None or (
        not isinstance(status, bool)
        and ((isinstance(status, int) and status == 0) or status == "0")
    )
    if not is_success:
        raise EventApiResponseError("status", status=status)

    events = payload.get("events")
    if events == []:
        normalized = dict(payload)
        normalized["events"] = {}
        return normalized
    if not isinstance(events, dict):
        raise EventApiResponseError("events")
    return payload
