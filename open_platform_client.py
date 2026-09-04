"""Small client for the Dongqiudi Open Platform article API."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
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
IMAGE_UPLOAD_API_NAME = "image-uploadimage-ai"
TOKEN_REFRESH_LEEWAY_SECONDS = 5 * 60
OAUTH_STATE_TTL_SECONDS = 10 * 60
DEFAULT_IMAGE_UPLOAD_TIMEOUT_SECONDS = 120.0
MIN_IMAGE_UPLOAD_TIMEOUT_SECONDS = 60.0
MAX_IMAGE_UPLOAD_TIMEOUT_SECONDS = 120.0


class OpenPlatformError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: int | str | None = None,
        status_code: int = 502,
        auth_required: bool = False,
        retriable: bool = False,
        http_status: int | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.auth_required = auth_required
        self.retriable = retriable
        self.http_status = http_status
        self.diagnostics = dict(diagnostics or {})


class ImageUploadResults(list[dict[str, Any]]):
    """List-compatible image results carrying a safe upload lifecycle trace."""

    def __init__(
        self,
        items: list[dict[str, Any]] | None = None,
        *,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(items or [])
        self.diagnostics = dict(diagnostics or {})


@dataclass(frozen=True)
class OpenPlatformConfig:
    appid: str
    app_secret: str
    api_name: str
    redirect_uri: str
    token_path: Path
    token_service_url: str = ""
    image_upload_token: str = ""
    image_upload_timeout_seconds: float = DEFAULT_IMAGE_UPLOAD_TIMEOUT_SECONDS

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
            token_service_url=os.environ.get("TOKEN_SERVICE_URL", "").strip().rstrip("/"),
            image_upload_token=os.environ.get(
                "OPEN_PLATFORM_IMAGE_UPLOAD_TOKEN", ""
            ).strip(),
            image_upload_timeout_seconds=_image_upload_timeout_from_environment(),
        )


def _image_upload_timeout_from_environment() -> float:
    """Read the official image-upload timeout with a bounded safe range."""
    raw_value = os.environ.get(
        "OPEN_PLATFORM_IMAGE_UPLOAD_TIMEOUT_SECONDS",
        str(DEFAULT_IMAGE_UPLOAD_TIMEOUT_SECONDS),
    ).strip()
    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "OPEN_PLATFORM_IMAGE_UPLOAD_TIMEOUT_SECONDS 必须是 60 到 120 秒之间的数字"
        ) from exc
    if not _valid_image_upload_timeout(value):
        raise RuntimeError(
            "OPEN_PLATFORM_IMAGE_UPLOAD_TIMEOUT_SECONDS 必须是 60 到 120 秒之间的数字"
        )
    return value


def _valid_image_upload_timeout(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and MIN_IMAGE_UPLOAD_TIMEOUT_SECONDS
        <= float(value)
        <= MAX_IMAGE_UPLOAD_TIMEOUT_SECONDS
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
        if not _valid_image_upload_timeout(config.image_upload_timeout_seconds):
            raise ValueError(
                "官方图片上传超时必须是 60 到 120 秒之间的数字"
            )
        self.image_upload_timeout_seconds = float(config.image_upload_timeout_seconds)
        self._lock = threading.RLock()
        self._oauth_states: dict[str, float] = {}
        self._token_cache: dict[str, Any] | None | object = _UNSET
        # The most recent image-upload trace is intentionally kept in memory
        # only long enough for ArticlePublisher to persist a safe summary.
        # Tokens, signatures, and multipart bodies are never included.
        self.last_upload_diagnostics: dict[str, Any] = {}

    def status(self) -> dict[str, Any]:
        if self.config.token_service_url:
            return self._token_service_status()
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
            "token_service_url": None,
            "token_storage": "local_file",
        }

    def image_upload_status(self) -> dict[str, Any]:
        """Return image-upload readiness without exposing either credential."""
        gateway_configured = bool(self.config.appid and self.config.app_secret)
        token_configured = bool(self.config.image_upload_token)
        return {
            "enabled": bool(gateway_configured and token_configured),
            "configured": bool(gateway_configured and token_configured),
            "gateway_configured": gateway_configured,
            "authorization_configured": token_configured,
            "api_name": IMAGE_UPLOAD_API_NAME,
            "endpoint": f"{PLATFORM_ORIGIN}/open/v1/do",
            "type": "archive",
            "multipart_fields": ["file1", "file2"],
            "max_file_bytes": 20 * 1024 * 1024,
            "animated_gif_supported": True,
            "timeout_seconds": self.image_upload_timeout_seconds,
        }

    def _token_service_status(self) -> dict[str, Any]:
        """Read central auth state without making the dashboard wait on a dead service."""
        base = {
            "configured": bool(self.config.appid and self.config.app_secret),
            "oauth_configured": True,
            "authorized": False,
            "renewable": False,
            "reauthorization_required": False,
            "expires_at_unix": None,
            "refresh_expires_at_unix": None,
            "api_name": self.config.api_name,
            "redirect_uri": self.config.redirect_uri or None,
            "token_path": None,
            "token_service_url": self.config.token_service_url,
            "token_storage": "token_service",
            "token_service_available": False,
        }
        try:
            payload = self._request_token_service_json(
                "GET", "/auth/status", timeout_seconds=min(self.request_timeout_seconds, 2.0)
            )
        except OpenPlatformError as exc:
            base["token_service_error"] = str(exc)
            return base
        base["token_service_available"] = bool(payload.get("ok"))
        base["auth_status"] = payload.get("auth_status")
        base["authorized"] = bool(
            payload.get("ok")
            and payload.get("has_access_token")
            and str(payload.get("auth_status") or "") == "AUTHORIZED"
        )
        base["renewable"] = bool(payload.get("has_refresh_token"))
        base["reauthorization_required"] = str(
            payload.get("auth_status") or ""
        ) in {"EXPIRED", "ERROR"}
        base["expires_at"] = payload.get("token_expires_at")
        base["refresh_expires_at"] = payload.get("refresh_token_expires_at")
        if payload.get("last_error"):
            base["token_service_error"] = payload["last_error"]
        return base

    def authorization_url(self) -> str:
        if self.config.token_service_url:
            payload = self._request_token_service_json("POST", "/auth/start")
            authorize_url = str(payload.get("authorize_url") or "").strip()
            if not payload.get("ok") or not authorize_url:
                raise OpenPlatformError(
                    str(payload.get("error") or "Token Service 未返回授权地址"),
                    status_code=503,
                    retriable=True,
                )
            return authorize_url
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

    def exchange_oauth_code(
        self,
        code: str,
        state: str,
        *,
        error: str = "",
        error_description: str = "",
    ) -> dict[str, Any]:
        if self.config.token_service_url:
            if error:
                query = {
                    "error": error,
                    "state": state,
                }
                if error_description:
                    query["error_description"] = error_description
                self._request_token_service_raw(
                    "GET",
                    "/auth/callback?" + urllib.parse.urlencode(query),
                    timeout_seconds=self.request_timeout_seconds,
                )
                raise OpenPlatformError(
                    error_description or error,
                    status_code=400,
                    auth_required=True,
                )
            if not code:
                raise OpenPlatformError("OAuth 回调缺少 code", status_code=400)
            if not state:
                raise OpenPlatformError("OAuth 回调缺少 state", status_code=400)
            raw = self._request_token_service_raw(
                "GET",
                "/auth/callback?"
                + urllib.parse.urlencode({"code": code, "state": state}),
                timeout_seconds=self.request_timeout_seconds,
            )
            if "授权成功" not in raw:
                raise OpenPlatformError(
                    "Token Service 未确认授权成功",
                    status_code=502,
                    retriable=True,
                )
            return {"authorized": True, "token_service": True}
        if error:
            raise OpenPlatformError(
                error_description or error,
                status_code=400,
                auth_required=True,
            )
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
                http_status=payload.get("__http_status"),
            )
        article_id = _article_id(result)
        if not article_id:
            raise OpenPlatformError("开放平台返回成功，但没有文章 ID")
        return {
            "article_id": article_id,
            "duplicate": code == 2,
            "code": code,
            "message": str(result.get("message") or "ok"),
            "http_status": payload.get("__http_status"),
        }

    def upload_images(
        self,
        gif_path: Path,
        cover_path: Path,
        *,
        max_bytes: int = 20 * 1024 * 1024,
    ) -> list[dict[str, Any]]:
        """Upload a GIF and JPG through the Open Platform image endpoint.

        The image endpoint expects its business parameters in the query string
        and the files as multipart fields named ``file1`` and ``file2``.  The
        returned records are kept intact while normalizing common metadata so
        callers can rely on ``mime``, ``file_name``, ``size``, ``url`` and
        ``img1_url`` being present.
        """
        started_at = time.monotonic()
        lifecycle: list[dict[str, Any]] = []

        def mark(name: str, **details: Any) -> None:
            lifecycle.append(
                {
                    "event": name,
                    "at_ms": round((time.monotonic() - started_at) * 1000, 1),
                    **details,
                }
            )

        def traced_error(exc: OpenPlatformError) -> OpenPlatformError:
            diagnostics = {
                **getattr(exc, "diagnostics", {}),
                "http_status": exc.http_status or exc.status_code,
                "platform_code": exc.code,
                "lifecycle": list(lifecycle),
                "elapsed_ms": round((time.monotonic() - started_at) * 1000, 1),
            }
            exc.diagnostics = diagnostics
            self.last_upload_diagnostics = dict(diagnostics)
            return exc

        def trace_success() -> dict[str, Any]:
            response_events = [
                item
                for item in lifecycle
                if item.get("event") == "platform_response_received"
            ]
            response = response_events[-1] if response_events else {}
            return {
                "http_status": response.get("http_status"),
                "platform_code": response.get("platform_code", 0),
                "lifecycle": list(lifecycle),
                "elapsed_ms": round((time.monotonic() - started_at) * 1000, 1),
            }

        self.last_upload_diagnostics = {}
        mark("upload_started")
        try:
            self._require_configuration()
        except OpenPlatformError as exc:
            mark("upload_failed", stage="configuration", error=str(exc))
            raise traced_error(exc)
        configured_image_token = self.config.image_upload_token.strip()
        if not configured_image_token:
            exc = OpenPlatformError(
                "图片上传需要配置 OPEN_PLATFORM_IMAGE_UPLOAD_TOKEN（原始登录 token）",
                code="image_upload_token_missing",
                status_code=503,
                auth_required=True,
            )
            mark("upload_failed", stage="configuration", error=str(exc))
            raise traced_error(exc)
        if not isinstance(gif_path, Path) or not isinstance(cover_path, Path):
            raise traced_error(OpenPlatformError(
                "图片上传仅支持本地 Path 文件",
                status_code=400,
            ))
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes <= 0
        ):
            raise traced_error(OpenPlatformError("图片大小限制必须是正整数", status_code=400))

        files = (
            ("file1", gif_path, "image/gif"),
            ("file2", cover_path, "image/jpeg"),
        )
        file_metadata: list[tuple[str, Path, str, bytes]] = []
        for field_name, path, mime in files:
            try:
                if not path.is_file():
                    raise OSError("不是文件")
                size = path.stat().st_size
                if size <= 0 or size > max_bytes:
                    raise OpenPlatformError(
                        f"图片文件 {path.name} 大小非法（上限 {max_bytes} 字节）",
                        code=300007,
                        status_code=400,
                    )
                content = path.read_bytes()
            except OpenPlatformError as exc:
                raise traced_error(exc)
            except (OSError, ValueError) as exc:
                raise traced_error(OpenPlatformError(
                    f"无法读取图片文件 {path}",
                    status_code=400,
                )) from exc
            if len(content) == 0 or len(content) > max_bytes:
                raise traced_error(OpenPlatformError(
                    f"图片文件 {path.name} 大小非法（上限 {max_bytes} 字节）",
                    code=300007,
                    status_code=400,
                ))
            file_metadata.append((field_name, path, mime, content))

        boundary = f"----football-gif-{secrets.token_hex(16)}"
        body = _multipart_body(boundary, file_metadata)

        def send(access_token: str) -> dict[str, Any]:
            query = {
                "api_name": IMAGE_UPLOAD_API_NAME,
                "appid": self.config.appid,
                "nonce": secrets.token_hex(16),
                "timestamp": int(time.time()),
                "type": "archive",
            }
            query["sign"] = sign_query(query, self.config.app_secret)
            url = f"{PLATFORM_ORIGIN}/open/v1/do?{urllib.parse.urlencode(query)}"
            return self._request_json(
                "POST",
                url,
                body=body,
                content_type=f"multipart/form-data; boundary={boundary}",
                timeout_seconds=self.image_upload_timeout_seconds,
                # This image endpoint expects the original token value, not
                # the OAuth `Bearer <token>` scheme used by article creation.
                authorization=access_token,
            )

        # The app image endpoint uses a raw login token. Keep this credential
        # separate from the OAuth token used by article APIs; an OAuth access
        # token cannot identify the logged-in upload user for this endpoint.
        access_token = configured_image_token
        mark(
            "token_acquired",
            gif_bytes=len(file_metadata[0][3]),
            cover_bytes=len(file_metadata[1][3]),
        )
        try:
            mark(
                "platform_request_sent",
                attempt=1,
                body_bytes=len(body),
                field_names=[item[0] for item in file_metadata],
            )
            payload = send(access_token)
            mark(
                "platform_response_received",
                attempt=1,
                http_status=payload.get("__http_status"),
                platform_code=(
                    _image_upload_result(payload) or {}
                ).get("code"),
            )
        except OpenPlatformError as exc:
            mark(
                "platform_response_received",
                attempt=1,
                http_status=exc.http_status or exc.status_code,
                platform_code=exc.code,
                error=str(exc),
            )
            raise traced_error(exc)
        result = _image_upload_result(payload)
        if not result:
            raise traced_error(OpenPlatformError(
                "开放平台图片上传返回了无法解析的结果",
                http_status=payload.get("__http_status"),
            ))
        code = result.get("code")
        if not _code_matches(code, 0):
            auth_required = any(
                _code_matches(code, auth_code)
                for auth_code in (10007, 10008, 300002)
            )
            message = (
                result.get("messgae")
                or result.get("message")
                or payload.get("messgae")
                or payload.get("message")
                or "图片上传失败"
            )
            raise traced_error(OpenPlatformError(
                str(message),
                code=code,
                status_code=401 if auth_required else 502,
                auth_required=auth_required,
                retriable=any(
                    _code_matches(code, retry_code)
                    for retry_code in (4, 5, 10006, 300009, 50001, 50002)
                ),
                http_status=payload.get("__http_status"),
                diagnostics={
                    "lifecycle": lifecycle,
                    "elapsed_ms": round((time.monotonic() - started_at) * 1000, 1),
                },
            ))
        data = result.get("data")
        if not isinstance(data, list):
            raise traced_error(OpenPlatformError(
                "开放平台图片上传成功，但返回的 data 不是数组",
                code=code,
                http_status=payload.get("__http_status"),
                diagnostics={
                    "lifecycle": lifecycle,
                    "elapsed_ms": round((time.monotonic() - started_at) * 1000, 1),
                },
            ))
        normalized: list[dict[str, Any]] = []
        for index, item in enumerate(data):
            if not isinstance(item, dict):
                raise traced_error(OpenPlatformError(
                    "开放平台图片上传返回了无效的图片记录",
                    code=code,
                    http_status=payload.get("__http_status"),
                    diagnostics={
                        "lifecycle": lifecycle,
                        "elapsed_ms": round((time.monotonic() - started_at) * 1000, 1),
                    },
                ))
            _field_name, path, expected_mime, content = file_metadata[
                min(index, len(file_metadata) - 1)
            ]
            record = dict(item)
            record["image_id"] = item.get("image_id") or item.get("id")
            record["mime"] = str(item.get("mime") or expected_mime)
            record["file_name"] = str(item.get("file_name") or path.name)
            record["size"] = (
                item.get("size")
                if item.get("size") not in (None, "")
                else len(content)
            )
            record["url"] = str(item.get("url") or "")
            record["img1_url"] = str(item.get("img1_url") or "")
            normalized.append(record)
        mark(
            "upload_succeeded",
            image_count=len(normalized),
            http_status=payload.get("__http_status"),
            platform_code=code,
        )
        diagnostics = trace_success()
        self.last_upload_diagnostics = dict(diagnostics)
        return ImageUploadResults(normalized, diagnostics=diagnostics)

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
        if self.config.token_service_url:
            path = "/token?force=1" if force_refresh else "/token"
            payload = self._request_token_service_json("GET", path)
            access_token = str(payload.get("access_token") or "").strip()
            if payload.get("ok") and access_token:
                return access_token
            error = str(payload.get("error") or "Token Service 未返回 access_token")
            auth_required = "AUTH_REQUIRED" in error or "AUTH_EXPIRED" in error
            raise OpenPlatformError(
                error,
                status_code=401 if auth_required else 503,
                auth_required=auth_required,
                retriable=not auth_required,
            )
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
        timeout_seconds: float | None = None,
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
                request,
                timeout=(
                    self.request_timeout_seconds
                    if timeout_seconds is None
                    else timeout_seconds
                ),
            ) as response:
                raw = response.read().decode("utf-8", errors="replace")
                payload = _parse_json(raw)
                if not payload:
                    raise OpenPlatformError("开放平台返回的不是有效 JSON")
                payload["__http_status"] = int(getattr(response, "status", 200))
                return payload
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            payload = _parse_json(raw)
            result = _business_result(payload)
            code = result.get("code") if result else None
            auth_required = exc.code == 401 or any(
                _code_matches(code, auth_code)
                for auth_code in (10007, 10008, 300002)
            )
            raise OpenPlatformError(
                str(
                    (result or {}).get("message")
                    or (result or {}).get("messgae")
                    or payload.get("message")
                    or payload.get("messgae")
                    or f"开放平台 HTTP {exc.code}"
                ),
                code=code,
                status_code=401 if auth_required else 502,
                auth_required=auth_required,
                retriable=exc.code == 429 or exc.code >= 500,
                http_status=exc.code,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise OpenPlatformError(
                f"无法连接开放平台：{exc}", retriable=True
            ) from exc
        payload = _parse_json(raw)
        if not payload:
            raise OpenPlatformError("开放平台返回的不是有效 JSON")
        return payload

    def _request_token_service_json(
        self,
        method: str,
        path: str,
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        raw = self._request_token_service_raw(
            method,
            path,
            body=b"" if method.upper() == "GET" else b"{}",
            content_type="application/json",
            timeout_seconds=timeout_seconds,
        )
        payload = _parse_json(raw)
        if not payload:
            raise OpenPlatformError("Token Service 返回的不是有效 JSON", retriable=True)
        return payload

    def _request_token_service_raw(
        self,
        method: str,
        path: str,
        *,
        body: bytes = b"",
        content_type: str = "application/json",
        timeout_seconds: float | None = None,
    ) -> str:
        url = f"{self.config.token_service_url}{path}"
        headers = {
            "Accept": "application/json, text/html",
            "Content-Type": content_type,
            "Content-Length": str(len(body)),
            "User-Agent": "football-gif-dashboard/1.0",
        }
        request = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout_seconds or self.request_timeout_seconds,
            ) as response:
                return response.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            payload = _parse_json(raw)
            message = str(
                payload.get("error")
                or payload.get("message")
                or _html_text(raw)
                or f"Token Service HTTP {exc.code}"
            )
            auth_required = exc.code == 401 or "AUTH_REQUIRED" in message or "AUTH_EXPIRED" in message
            status_code = (
                401
                if auth_required
                else (exc.code if 400 <= exc.code < 500 else 503)
            )
            raise OpenPlatformError(
                message,
                status_code=status_code,
                auth_required=auth_required,
                retriable=exc.code == 429 or exc.code >= 500,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise OpenPlatformError(
                f"无法连接 Token Service：{exc}",
                status_code=503,
                retriable=True,
            ) from exc


def _parse_json(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _html_text(raw: str) -> str:
    """Extract a short readable message from the token service HTML callback."""
    text = re.sub(r"<[^>]+>", " ", raw or "")
    return re.sub(r"\s+", " ", text).strip()[:500]


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


def _code_matches(value: Any, expected: int) -> bool:
    """Compare API codes consistently when a service serializes them as strings."""
    if isinstance(value, bool):
        return False
    try:
        return int(value) == expected
    except (TypeError, ValueError):
        return False


def _image_upload_result(payload: Any) -> dict[str, Any] | None:
    """Extract the image endpoint result from direct or wrapped responses."""
    if not isinstance(payload, dict):
        return None
    response = payload.get("response")
    if isinstance(response, dict):
        nested = response.get("data")
        if isinstance(nested, dict) and "code" in nested:
            return nested
        if "code" in response and (
            "data" in response or "messgae" in response or "message" in response
        ):
            return response
    nested = payload.get("data")
    if isinstance(nested, dict) and "code" in nested:
        return nested
    if "code" in payload:
        return payload
    return None


def _multipart_body(
    boundary: str,
    files: list[tuple[str, Path, str, bytes]],
) -> bytes:
    """Build a deterministic multipart body for the two image fields."""
    chunks: list[bytes] = []
    boundary_bytes = boundary.encode("ascii")
    for field_name, path, mime, content in files:
        filename = (
            path.name.replace("\"", "_")
            .replace("\r", "")
            .replace("\n", "")
        )
        chunks.extend(
            (
                b"--" + boundary_bytes + b"\r\n",
                (
                    f'Content-Disposition: form-data; name="{field_name}"; '
                    f'filename="{filename}"\r\n'
                ).encode("utf-8"),
                f"Content-Type: {mime}\r\n\r\n".encode("ascii"),
                content,
                b"\r\n",
            )
        )
    chunks.append(b"--" + boundary_bytes + b"--\r\n")
    return b"".join(chunks)


def _number_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _positive_number(value: Any, default: float) -> float:
    number = _number_or_none(value)
    return number if number is not None and number > 0 else default


_UNSET = object()
