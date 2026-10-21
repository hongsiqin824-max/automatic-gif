"""Thin client for the mediamtx control API (BACKLOG 11.13, scheme H).

mediamtx sits between the upstream RTMP feed and two consumers: the browser
tile (WebRTC/WHEP, sub-second) and the preview ffmpeg (RTSP on loopback, which
keeps writing the HLS ring buffer used by the stage and by GIF capture). This
module only registers/removes per-match paths and reports health; it never
raises into the request path. Every failure degrades to "no WebRTC", and the
caller falls back to pulling RTMP directly exactly as before scheme H.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable


class WebRTCGateway:
    def __init__(
        self,
        *,
        enabled: bool,
        api_base: str = "http://127.0.0.1:9997",
        rtsp_base: str = "rtsp://127.0.0.1:8554",
        whep_base: str = "whep/",
        timeout: float = 2.0,
        ready_timeout: float = 6.0,
        opener: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.enabled = bool(enabled)
        self.api_base = api_base.rstrip("/")
        self.rtsp_base = rtsp_base.rstrip("/")
        self.whep_base = whep_base if whep_base.endswith("/") else whep_base + "/"
        self.timeout = float(timeout)
        self.ready_timeout = float(ready_timeout)
        self._opener = opener or urllib.request.urlopen
        self._clock = clock
        self._sleep = sleep
        self.last_error: str | None = None
        # Upstream URL per registered match, so a path can be re-created after
        # mediamtx restarts (its API-added paths live only in memory).
        self.sources: dict[str, str] = {}
        # Circuit breaker: after a connection failure/timeout, skip the API for
        # a short window so a hung mediamtx costs one timeout per window, not
        # one per match (the reaper calls us under its lock).
        self.breaker_seconds = 5.0
        self._down_until = 0.0

    # ---- URL helpers -------------------------------------------------------

    def rtsp_url(self, match_id: str) -> str:
        return f"{self.rtsp_base}/{match_id}"

    def whep_url(self, match_id: str) -> str:
        # Relative so it works behind the /gif-monitor/ prefix like every other
        # URL we hand the browser (see DEPLOYMENT.md).
        return f"{self.whep_base}{match_id}/whep"

    # ---- API ---------------------------------------------------------------

    def healthy(self) -> bool:
        if not self.enabled:
            return False
        return self._request("GET", "/v3/paths/list") is not None

    def ensure_path(self, match_id: str, source: str, *, wait_ready: bool = True) -> bool:
        """Register ``match_id`` pulling ``source``; return True when ready.

        Uses ``replace`` so a changed upstream URL simply re-points the path.
        Readiness means mediamtx has connected to the source and sees at least
        one track; without that the browser's WHEP offer would be answered with
        no media and hls.js-style retries would be needed on the client.
        ``wait_ready=False`` returns right after registration (one API call,
        bounded by ``timeout``) for callers that hold a lock and can let the
        consumer retry on its own.
        """
        if not self.enabled:
            return False
        body = {
            "source": source,
            # Keep pulling while a path exists; we delete the path on stop.
            "sourceOnDemand": False,
        }
        if self._request("POST", f"/v3/config/paths/replace/{match_id}", body) is None:
            return False
        self.sources[match_id] = source
        if not wait_ready:
            return True
        deadline = self._clock() + self.ready_timeout
        while True:
            if self.path_ready(match_id):
                return True
            if self._clock() >= deadline:
                self.last_error = f"mediamtx path {match_id} not ready within {self.ready_timeout:g}s"
                return False
            self._sleep(0.3)

    def reensure_path(self, match_id: str) -> bool:
        """Re-register a path we created earlier (after a mediamtx restart).

        Does not wait for readiness: this runs from the preview reaper under
        its lock, and with many matches restarting at once a 6s wait each
        would freeze every preview API call for minutes. ffmpeg simply
        retries (with backoff) until mediamtx has the source up.
        """
        source = self.sources.get(match_id)
        if not source:
            self.last_error = f"no upstream remembered for {match_id}"
            return False
        return self.ensure_path(match_id, source, wait_ready=False)

    def upstream_for(self, match_id: str) -> str | None:
        return self.sources.get(match_id)

    def remove_path(self, match_id: str) -> None:
        self.sources.pop(match_id, None)
        if not self.enabled:
            return
        self._request("DELETE", f"/v3/config/paths/delete/{match_id}", ignore_404=True)

    def cleanup_stale_paths(self) -> list[str]:
        """Delete match paths left behind by a previous 8901 that did not exit
        cleanly. Only all-digit names are touched (match ids); anything an
        operator configured by hand in mediamtx.yml is left alone."""
        if not self.enabled:
            return []
        listing = self._request("GET", "/v3/config/paths/list")
        if not isinstance(listing, dict):
            return []
        removed: list[str] = []
        for item in listing.get("items") or []:
            name = str((item or {}).get("name") or "")
            if name.isdigit() and name not in self.sources:
                self._request("DELETE", f"/v3/config/paths/delete/{name}", ignore_404=True)
                removed.append(name)
        return removed

    def path_ready(self, match_id: str) -> bool:
        if not self.enabled:
            return False
        info = self._request("GET", f"/v3/paths/get/{match_id}", ignore_404=True)
        if not isinstance(info, dict):
            return False
        return bool(info.get("ready")) and bool(info.get("tracks"))

    # ---- internals ---------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        ignore_404: bool = False,
    ) -> Any:
        url = self.api_base + path
        if self._clock() < self._down_until:
            self.last_error = f"mediamtx {method} {path} -> skipped (API unreachable {self.breaker_seconds:g}s ago)"
            return None
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with self._opener(req, timeout=self.timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:
            if ignore_404 and exc.code == 404:
                return None
            self.last_error = f"mediamtx {method} {path} -> HTTP {exc.code}"
            return None
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            self.last_error = f"mediamtx {method} {path} -> {exc}"
            self._down_until = self._clock() + self.breaker_seconds
            return None
        self.last_error = None
        self._down_until = 0.0
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
