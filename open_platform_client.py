"""Small client for the Dongqiudi Open Platform article API."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PLATFORM_ORIGIN = "https://platform.dongqiudi.com"
DEFAULT_API_NAME = "admin-archive-createarticle"
TOKEN_REFRESH_LEEWAY_SECONDS = 5 * 60
OAUTH_STATE_TTL_SECONDS = 10 * 60


class OpenPlatformError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        status_code: int = 502,
        auth_required: bool = False,
        retriable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.auth_required = auth_required
        self.retriable = retriable


@dataclass(frozen=True)
class OpenPlatformConfig:
    appid: str
    app_secret: str
    api_name: str
    redirect_uri: str
    token_path: Path

    @classmethod
    def from_environment(cls, root: Path) -> "OpenPlatformConfig":
        return cls(
            appid=os.environ.get("OPEN_PLATFORM_APPID", "").strip(),
            app_secret=os.environ.get("OPEN_PLATFORM_APP_SECRET", "").strip(),
            api_name=(
                os.environ.get("OPEN_PLATFORM_API_NAME", DEFAULT_API_NAME).strip()
                or DEFAULT_API_NAME
            ),
            redirect_uri=os.environ.get("OPEN_PLATFORM_REDIRECT_URI", "").strip(),
            token_path=Path(
                os.environ.get(
                    "OPEN_PLATFORM_TOKEN_PATH",
                    str(root / "data" / "open-platform-token.json"),
                )
            ).expanduser(),
        )


def sign_query(parameters: dict[str, Any], app_secret: str) -> str:
    raw = "&".join(
        f"{key}={parameters[key]}"
        for key in sorted(parameters)
        if key != "sign"
    )
    return hmac.new(
        app_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def _business_result(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    response = payload.get("response")
    if isinstance(response, dict) and isinstance(response.get("data"), dict):
        return response["data"]
    data = payload.get("data")
    if payload.get("code") == 0 and isinstance(data, dict) and "code" in data:
        return data
    return payload


class OpenPlatformClient:
    def __init__(
        self,
        config: OpenPlatformConfig,
        *,
        request_timeout_seconds: float = 15.0,
    ) -> None:
        self.config = config
        self.request_timeout_seconds = request_timeout_seconds
        self._lock = threading.RLock()
        self._oauth_states: dict[str, float] = {}
        self._token_cache: dict[str, Any] | None | object = _UNSET

    def status(self) -> dict[str, Any]:
        with self._lock:
            token = self._read_token()
        now = time.time()
        expires_at = _number_or_none(token.get("expires_at")) if token else None
        refresh_expires_at = (
            _number_or_none(token.get("refresh_expires_at")) if token else None
        )
        app_matches = not token or not token.get("app_id") or token.get("app_id") == self.config.appid
        authorized = bool(
            token
            and token.get("access_token")
            and app_matches
            and (expires_at is None or expires_at > now + 30)
        )
        renewable = bool(
            token
            and token.get("refresh_token")
            and app_matches
            and (refresh_expires_at is None or refresh_expires_at > now)
        )
        return {
            "configured": bool(self.config.appid and self.config.app_secret),
            "oauth_configured": bool(
                self.config.appid
                and self.config.app_secret
                and self.config.redirect_uri
            ),
            "authorized": authorized,
            "renewable": renewable,
            "reauthorization_required": bool(token and not app_matches),
            "expires_at_unix": expires_at,
            "refresh_expires_at_unix": refresh_expires_at,
            "api_name": self.config.api_name,
            "redirect_uri": self.config.redirect_uri or None,
            "token_path": str(self.config.token_path),
        }

    def authorization_url(self) -> str:
        self._require_configuration(require_redirect_uri=True)
        now = time.time()
        with self._lock:
            self._oauth_states = {
                state: deadline
                for state, deadline in self._oauth_states.items()
                if deadline > now
            }
            state = secrets.token_hex(24)
            self._oauth_states[state] = now + OAUTH_STATE_TTL_SECONDS
        query = urllib.parse.urlencode(
            {
                "appid": self.config.appid,
                "api_name": self.config.api_name,
                "redirect_uri": self.config.redirect_uri,
                "state": state,
            }
        )
        return f"{PLATFORM_ORIGIN}/open/oauth/authorize?{query}"

    def exchange_oauth_code(self, code: str, state: str) -> dict[str, Any]:
        with self._lock:
            deadline = self._oauth_states.pop(state, None)
        if not state or deadline is None or deadline <= time.time():
            raise OpenPlatformError(
                "OAuth state 无效或已过期，请重新发起授权",
                status_code=400,
                auth_required=True,
            )
        if not code:
            raise OpenPlatformError("OAuth 回调缺少 code", status_code=400)
        self._require_configuration()
        payload = self._request_json(
            "POST",
            f"{PLATFORM_ORIGIN}/open/oauth/token",
            body=json.dumps(
                {
                    "appid": self.config.appid,
                    "app_secret": self.config.app_secret,
                    "code": code,
                    "grant_type": "authorization_code",
                }
            ).encode("utf-8"),
            content_type="application/json",
        )
        if payload.get("code") != 0 or not isinstance(payload.get("data"), dict):
            raise OpenPlatformError(
                str(payload.get("message") or "开放平台授权失败"),
                code=payload.get("code"),
                status_code=502,
            )
        token = payload["data"]
        if not token.get("access_token"):
            raise OpenPlatformError("开放平台授权结果缺少 access_token")
        return self._save_token(token)

    def create_article(self, fields: dict[str, Any]) -> dict[str, Any]:
        self._require_configuration()

        def send(access_token: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
            query = {
                "api_name": self.config.api_name,
                "appid": self.config.appid,
                "nonce": secrets.token_hex(16),
                "timestamp": int(time.time()),
            }
            query["sign"] = sign_query(query, self.config.app_secret)
            url = f"{PLATFORM_ORIGIN}/open/v1/do?{urllib.parse.urlencode(query)}"
            body = urllib.parse.urlencode(fields, doseq=True).encode("utf-8")
            payload = self._request_json(
                "POST",
                url,
                body=body,
                content_type="application/x-www-form-urlencoded",
                authorization=f"Bearer {access_token}",
            )
            return payload, _business_result(payload)

        access_token = self._ensure_access_token()
        payload, result = send(access_token)
        if result and result.get("code") == 10008:
            access_token = self._ensure_access_token(force_refresh=True)
            payload, result = send(access_token)
        if not result:
            raise OpenPlatformError("开放平台返回了无法解析的结果")
        code = result.get("code")
        if code not in (0, 2):
            auth_required = code in (10007, 10008)
            raise OpenPlatformError(
                str(result.get("message") or payload.get("message") or "文章发布失败"),
                code=code,
                status_code=401 if auth_required else 502,
                auth_required=auth_required,
                retriable=code in (4, 5, 10006, 50001, 50002),
            )
        article_id = _article_id(result)
        if not article_id:
            raise OpenPlatformError("开放平台返回成功，但没有文章 ID")
        return {
            "article_id": article_id,
            "duplicate": code == 2,
            "code": code,
            "message": str(result.get("message") or "ok"),
        }

    def _require_configuration(self, *, require_redirect_uri: bool = False) -> None:
        if not self.config.appid or not self.config.app_secret:
            raise OpenPlatformError(
                "服务器尚未配置 OPEN_PLATFORM_APPID 或 OPEN_PLATFORM_APP_SECRET",
                status_code=503,
            )
        if require_redirect_uri and not self.config.redirect_uri:
            raise OpenPlatformError(
                "服务器尚未配置 OPEN_PLATFORM_REDIRECT_URI",
                status_code=503,
            )

    def _ensure_access_token(self, *, force_refresh: bool = False) -> str:
        with self._lock:
            token = self._read_token()
            if not token:
                raise OpenPlatformError(
                    "开放平台尚未授权，请先完成一次 OAuth 授权",
                    status_code=401,
                    auth_required=True,
                )
            if token.get("app_id") and token.get("app_id") != self.config.appid:
                raise OpenPlatformError(
                    "开放平台 AppID 已变化，请重新授权",
                    code=10008,
                    status_code=401,
                    auth_required=True,
                )
            expires_at = _number_or_none(token.get("expires_at"))
            needs_refresh = bool(
                force_refresh
                or not token.get("access_token")
                or expires_at is None
                or expires_at <= time.time() + TOKEN_REFRESH_LEEWAY_SECONDS
            )
            if not needs_refresh:
                return str(token["access_token"])
            return str(self._refresh_token(token)["access_token"])

    def _refresh_token(self, previous: dict[str, Any]) -> dict[str, Any]:
        refresh_token = str(previous.get("refresh_token") or "")
        refresh_expires_at = _number_or_none(previous.get("refresh_expires_at"))
        if not refresh_token or (
            refresh_expires_at is not None and refresh_expires_at <= time.time()
        ):
            raise OpenPlatformError(
                "开放平台授权已失效，请重新授权",
                code=10008,
                status_code=401,
                auth_required=True,
            )
        payload = self._request_json(
            "POST",
            f"{PLATFORM_ORIGIN}/open/oauth/token/refresh",
            body=json.dumps({"refresh_token": refresh_token}).encode("utf-8"),
            content_type="application/json",
        )
        data = payload.get("data")
        if payload.get("code") != 0 or not isinstance(data, dict) or not data.get("access_token"):
            code = payload.get("code")
            retriable = code in (10006, 50001, 50002)
            raise OpenPlatformError(
                str(payload.get("message") or "刷新开放平台授权失败"),
                code=code,
                status_code=502 if retriable else 401,
                auth_required=not retriable,
                retriable=retriable,
            )
        if not data.get("refresh_token"):
            data["refresh_token"] = refresh_token
        return self._save_token(data, previous=previous)

    def _read_token(self) -> dict[str, Any] | None:
        if self._token_cache is not _UNSET:
            return self._token_cache  # type: ignore[return-value]
        try:
            value = json.loads(self.config.token_path.read_text(encoding="utf-8"))
            self._token_cache = value if isinstance(value, dict) else None
        except (OSError, json.JSONDecodeError):
            self._token_cache = None
        return self._token_cache  # type: ignore[return-value]

    def _save_token(
        self,
        token_data: dict[str, Any],
        *,
        previous: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = time.time()
        expires_in = _positive_number(token_data.get("expires_in"), 7200)
        refresh_token = str(
            token_data.get("refresh_token")
            or (previous or {}).get("refresh_token")
            or ""
        )
        refresh_expires_in = _number_or_none(
            token_data.get("refresh_expires_in")
            or token_data.get("refresh_token_expires_in")
        )
        refresh_expires_at = (
            now + refresh_expires_in
            if refresh_expires_in and refresh_expires_in > 0
            else (previous or {}).get("refresh_expires_at")
        )
        saved = {
            "app_id": self.config.appid,
            "access_token": str(token_data.get("access_token") or ""),
            "refresh_token": refresh_token,
            "token_type": str(token_data.get("token_type") or "Bearer"),
            "expires_in": expires_in,
            "expires_at": now + expires_in,
            "refresh_expires_at": refresh_expires_at,
            "user_info": token_data.get("user_info") or (previous or {}).get("user_info"),
            "saved_at": now,
        }
        self.config.token_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.config.token_path.with_name(
            f".{self.config.token_path.name}.{os.getpid()}.tmp"
        )
        temporary.write_text(
            json.dumps(saved, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.config.token_path)
        self._token_cache = saved
        return saved

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        body: bytes,
        content_type: str,
        authorization: str | None = None,
    ) -> dict[str, Any]:
        headers = {
            "Accept": "application/json",
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
            "User-Agent": "football-gif-dashboard/1.0",
        }
        if authorization:
            headers["Authorization"] = authorization
        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(
                request, timeout=self.request_timeout_seconds
            ) as response:
                raw = response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            payload = _parse_json(raw)
            result = _business_result(payload)
            code = result.get("code") if result else None
            auth_required = exc.code == 401 or code in (10007, 10008)
            raise OpenPlatformError(
                str(
                    (result or {}).get("message")
                    or payload.get("message")
                    or f"开放平台 HTTP {exc.code}"
                ),
                code=code,
                status_code=401 if auth_required else 502,
                auth_required=auth_required,
                retriable=exc.code == 429 or exc.code >= 500,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise OpenPlatformError(
                f"无法连接开放平台：{exc}", retriable=True
            ) from exc
        payload = _parse_json(raw)
        if not payload:
            raise OpenPlatformError("开放平台返回的不是有效 JSON")
        return payload


def _parse_json(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _article_id(result: dict[str, Any]) -> str:
    data = result.get("data")
    if isinstance(data, dict):
        value = data.get("archive_id") or data.get("id")
        if value not in (None, ""):
            return str(value)
    value = result.get("archive_id") or result.get("id")
    if value not in (None, ""):
        return str(value)
    match = re.search(r"id\s*[:：]\s*(\d+)", str(result.get("message") or ""), re.I)
    return match.group(1) if match else ""


def _number_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _positive_number(value: Any, default: float) -> float:
    number = _number_or_none(value)
    return number if number is not None and number > 0 else default


_UNSET = object()
