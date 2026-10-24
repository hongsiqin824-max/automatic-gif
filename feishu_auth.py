#!/usr/bin/env python3
"""Feishu (Lark) OAuth login wall for the 8901 monitor console.

Why a separate module: ``monitor_server.py`` is a single-file Flask service;
keeping authentication self-contained keeps that file reviewable and lets the
login flow be unit-tested without starting preview/catalog threads.

Behaviour contract:

* Disabled unless both ``GIF_MONITOR_FEISHU_APP_ID`` and
  ``GIF_MONITOR_FEISHU_APP_SECRET`` are configured — mirroring the
  ``GIF_MONITOR_INGEST_TOKEN`` opt-in pattern so local development and the
  test suite run unchanged.
* Browser page loads (GET with an HTML ``Accept``) are 302-redirected through
  the Feishu web OAuth flow; ``fetch`` API calls receive
  ``401 {"error": ..., "login_url": ...}`` and the frontend navigates there.
* ``/api/health`` (release health check), ``/api/monitor/events/ingest``
  (machine call guarded by its own ``X-Monitor-Token``), ``/static/`` assets
  and the auth routes themselves stay reachable without a session.
* The signed-cookie secret is read from ``GIF_MONITOR_AUTH_SECRET`` or, when
  empty, generated once and persisted to ``data/monitor_auth_secret`` so a
  restart does not log every operator out.

Access control has two modes, chosen by ``GIF_MONITOR_FEISHU_ADMIN_USERS``:

* **Static** (admin list empty): ``GIF_MONITOR_FEISHU_ALLOWED_USERS`` is the
  whole allowlist; empty allowlist means anyone inside the app's Feishu
  availability scope may log in.
* **Approval** (admin list set): admins and the static allowlist always get
  in. Anyone else lands on a page explaining they have no access yet, with
  an "apply" button. Clicking it records a *pending* access request in
  ``data/monitor_access.sqlite3`` and pings the admins through the group bot
  webhook; the page then polls for a decision. An admin approves/rejects on
  ``/admin/access``; the decision takes effect immediately without a restart
  because every request re-checks the store for approval-backed sessions.
* **External guests** (approval mode only): the admin generates a shared
  ``/guest?k=…`` link. Anyone opening it fills in a name and a note, which
  files a request in the same queue (``kind=guest``). A signed year-long
  cookie remembers the guest so an approved visitor re-enters after session
  expiry or logout without re-applying; rotating the link never affects
  approved guests, revoking a guest locks them out on the next request. A
  guest who lost the cookie gets a 7-day personal ``/guest?r=…`` recovery
  link from the admin page instead of a second approval.
* **Promoted admins**: an approved record can be given ``role=admin`` from
  ``/admin/access``. They get the same admin pages/APIs (approve, revoke,
  promote, demote, guest links) and can be demoted or revoked there; the
  ``.env`` admins are the only ones the page cannot touch.
"""

from __future__ import annotations

import base64
import hmac
import hashlib
import json
import os
import secrets
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, redirect, request, session

# Feishu web OAuth endpoints (企业自建应用「网页应用」登录).
AUTHORIZE_URL = "https://accounts.feishu.cn/open-apis/authen/v1/authorize"
TOKEN_URL = "https://open.feishu.cn/open-apis/authen/v2/oauth/token"
USER_INFO_URL = "https://open.feishu.cn/open-apis/authen/v1/user_info"

LOGIN_PATH = "/api/auth/feishu/login"
CALLBACK_PATH = "/api/auth/feishu/callback"
PENDING_PAGE_PATH = "/auth/pending"
PENDING_API_PATH = "/api/auth/pending"
REQUEST_API_PATH = "/api/auth/request"
GUEST_PAGE_PATH = "/guest"
GUEST_REQUEST_API_PATH = "/api/auth/guest/request"
ADMIN_PAGE_PATH = "/admin/access"
ADMIN_API_PATH = "/api/admin/access"
SESSION_KEY = "feishu_user"
# Identity of a user whose access request is still undecided; lets the
# waiting page poll for a decision without a second OAuth round-trip.
PENDING_SESSION_KEY = "feishu_pending"
# Long-lived signed cookie remembering an external guest's record id, so an
# approved guest who re-opens the link after the session expired gets in
# without filing (and the admin approving) a second request.
GUEST_COOKIE_NAME = "gif_monitor_guest"
GUEST_COOKIE_MAX_AGE = 365 * 24 * 3600
# A login attempt must round-trip the Feishu consent page within this window.
STATE_TTL_SECONDS = 600
HTTP_TIMEOUT_SECONDS = 8.0
WEBHOOK_TIMEOUT_SECONDS = 3.0
SECRET_FILENAME = "monitor_auth_secret"
ACCESS_DB_FILENAME = "monitor_access.sqlite3"

STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_REJECTED = "rejected"
# Pseudo-status for the waiting page: identity known, no request filed yet.
STATUS_ELIGIBLE = "eligible"

KIND_FEISHU = "feishu"
KIND_GUEST = "guest"
GUEST_NAME_MAX = 40
GUEST_NOTE_MAX = 200

# Per-record role. ``admin`` rows get the admin pages/APIs exactly like the
# .env admins, but can be demoted or revoked from the admin page; .env
# admins cannot be touched from there.
ROLE_USER = "user"
ROLE_ADMIN = "admin"
# A guest "recovery" link (admin page → 找回链接) is valid this long; it
# re-issues the guest cookie on a new browser without a second approval.
GUEST_RECOVER_TTL_SECONDS = 7 * 24 * 3600

# Reachable without a login session. Prefixes end with "/" on purpose so
# "/static/x" matches but a hypothetical "/staticfoo" does not.
EXEMPT_PATHS = frozenset(
    {"/api/health", "/api/monitor/events/ingest", "/favicon.ico", PENDING_PAGE_PATH,
     GUEST_PAGE_PATH}
)
EXEMPT_PREFIXES = ("/static/", "/api/auth/")

IDENTITY_KEYS = ("open_id", "union_id", "user_id", "email", "enterprise_email")


class FeishuAuthError(RuntimeError):
    """Raised when the Feishu OAuth backend rejects or misbehaves."""


def _log(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def _resolve_secret(env_value: str, data_dir: Path, enabled: bool) -> str:
    """Cookie/state signing secret: env wins; else persist a generated one.

    Nothing is written to disk while auth is disabled so tests and local runs
    leave no side effects.
    """
    if env_value:
        return env_value
    if not enabled:
        return ""
    path = data_dir / SECRET_FILENAME
    try:
        existing = path.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    except OSError:
        pass
    generated = secrets.token_hex(32)
    data_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(generated + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    _log("feishu_auth_secret_created", path=str(path))
    return generated


def _parse_user_list(raw: str) -> frozenset[str]:
    return frozenset(part.strip().lower() for part in raw.split(",") if part.strip())


@dataclass(frozen=True)
class FeishuAuthConfig:
    app_id: str = ""
    app_secret: str = ""
    redirect_uri: str = ""
    allowed_users: frozenset[str] = frozenset()
    session_hours: float = 72.0
    secret: str = ""
    # None = auto: secure cookies when the configured redirect URI is https.
    cookie_secure: bool | None = None
    # Public URL prefix for reverse-proxy sub-path deployments, e.g. "/gif-monitor".
    # Browser-facing URLs (redirects, login_url, callback URI auto-derivation)
    # are always prefixed; internal request.path checks are never affected because
    # the reverse proxy strips the prefix before forwarding to Flask.
    public_prefix: str = ""
    # Approval mode switch: non-empty = unknown users must be approved by one
    # of these identities (open_id recommended) before they get a session.
    admin_users: frozenset[str] = frozenset()
    # SQLite file holding access requests; ":memory:" is handy for tests.
    access_db_path: str = ""
    # Feishu group custom-bot webhook that receives "new access request" pings.
    notify_webhook: str = ""

    @property
    def login_path(self) -> str:
        return f"{self.public_prefix}{LOGIN_PATH}"

    @property
    def callback_path(self) -> str:
        return f"{self.public_prefix}{CALLBACK_PATH}"

    @property
    def enabled(self) -> bool:
        return bool(self.app_id and self.app_secret)

    @property
    def approval_enabled(self) -> bool:
        return self.enabled and bool(self.admin_users)

    def resolve_cookie_secure(self) -> bool:
        if self.cookie_secure is not None:
            return self.cookie_secure
        return self.redirect_uri.lower().startswith("https://")

    @classmethod
    def from_env(cls, data_dir: Path) -> "FeishuAuthConfig":
        app_id = os.environ.get("GIF_MONITOR_FEISHU_APP_ID", "").strip()
        app_secret = os.environ.get("GIF_MONITOR_FEISHU_APP_SECRET", "").strip()
        enabled = bool(app_id and app_secret)
        redirect_uri = os.environ.get("GIF_MONITOR_FEISHU_REDIRECT_URI", "").strip()
        allowed_users = _parse_user_list(os.environ.get("GIF_MONITOR_FEISHU_ALLOWED_USERS", ""))
        admin_users = _parse_user_list(os.environ.get("GIF_MONITOR_FEISHU_ADMIN_USERS", ""))
        hours_raw = os.environ.get("GIF_MONITOR_AUTH_SESSION_HOURS", "").strip()
        try:
            session_hours = float(hours_raw) if hours_raw else 72.0
        except ValueError as exc:
            raise RuntimeError("GIF_MONITOR_AUTH_SESSION_HOURS 必须是数字（小时）") from exc
        if session_hours <= 0:
            session_hours = 72.0
        secure_raw = os.environ.get("GIF_MONITOR_AUTH_COOKIE_SECURE", "").strip().lower()
        cookie_secure: bool | None
        if not secure_raw:
            cookie_secure = None
        else:
            cookie_secure = secure_raw not in {"0", "false", "no", "off"}
        secret = _resolve_secret(
            os.environ.get("GIF_MONITOR_AUTH_SECRET", "").strip(), data_dir, enabled
        )
        public_prefix = os.environ.get("GIF_MONITOR_PUBLIC_PREFIX", "").strip()
        # Normalise: keep exactly one leading "/" and strip any trailing "/"
        # so the caller never has to worry about double- or missing-slashes.
        if public_prefix:
            public_prefix = "/" + public_prefix.strip("/")
        # The access-request ping reuses the alert bot webhook unless a
        # dedicated one is configured, so no extra setup is needed.
        notify_webhook = (
            os.environ.get("GIF_MONITOR_FEISHU_ACCESS_WEBHOOK", "").strip()
            or os.environ.get("GIF_MONITOR_FEISHU_WEBHOOK", "").strip()
        )
        return cls(
            app_id=app_id,
            app_secret=app_secret,
            redirect_uri=redirect_uri,
            allowed_users=allowed_users,
            session_hours=session_hours,
            secret=secret,
            cookie_secure=cookie_secure,
            public_prefix=public_prefix,
            admin_users=admin_users,
            access_db_path=str(data_dir / ACCESS_DB_FILENAME),
            notify_webhook=notify_webhook,
        )


# ---- Access request store (approval mode) ----


class AccessStore:
    """SQLite-backed table of access requests keyed by subject id.

    The id is a Feishu ``open_id`` for employees and a generated ``guest_…``
    for external visitors (``kind`` tells them apart). A single connection
    guarded by a lock is plenty: writes happen only on apply and on admin
    decisions; the per-request revocation check is a primary-key lookup.
    """

    GUEST_KEY_SETTING = "guest_key"

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS access_requests (
                    open_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    requested_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    decided_by TEXT NOT NULL DEFAULT '',
                    last_login_at REAL,
                    kind TEXT NOT NULL DEFAULT 'feishu',
                    note TEXT NOT NULL DEFAULT '',
                    role TEXT NOT NULL DEFAULT 'user'
                )
                """
            )
            # Tables created by earlier releases lack kind/note/role; add them in place.
            existing = {
                row["name"] for row in self._conn.execute("PRAGMA table_info(access_requests)")
            }
            if "kind" not in existing:
                self._conn.execute(
                    "ALTER TABLE access_requests ADD COLUMN kind TEXT NOT NULL DEFAULT 'feishu'"
                )
            if "note" not in existing:
                self._conn.execute(
                    "ALTER TABLE access_requests ADD COLUMN note TEXT NOT NULL DEFAULT ''"
                )
            if "role" not in existing:
                self._conn.execute(
                    "ALTER TABLE access_requests ADD COLUMN role TEXT NOT NULL DEFAULT 'user'"
                )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            self._conn.commit()

    @staticmethod
    def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "open_id": row["open_id"],
            "name": row["name"],
            "kind": row["kind"],
            "note": row["note"],
            "status": row["status"],
            "role": row["role"],
            "requested_at": row["requested_at"],
            "updated_at": row["updated_at"],
            "decided_by": row["decided_by"],
            "last_login_at": row["last_login_at"],
        }

    def get(self, open_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def request(
        self, open_id: str, name: str, *, kind: str = KIND_FEISHU, note: str = ""
    ) -> tuple[dict[str, Any], bool]:
        """Create a pending request, or refresh the name of an existing row.

        Returns ``(record, created)`` so the caller only notifies on first
        contact rather than on every retry of the waiting page.
        """
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
            if row is None:
                self._conn.execute(
                    "INSERT INTO access_requests"
                    " (open_id, name, status, requested_at, updated_at, kind, note)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (open_id, name, STATUS_PENDING, now, now, kind, note),
                )
                created = True
            else:
                if name and name != row["name"]:
                    self._conn.execute(
                        "UPDATE access_requests SET name = ? WHERE open_id = ?", (name, open_id)
                    )
                created = False
            self._conn.commit()
            row = self._conn.execute(
                "SELECT * FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
        record = self._row_to_dict(row)
        assert record is not None
        return record, created

    def decide(self, open_id: str, status: str, decided_by: str) -> dict[str, Any] | None:
        if status not in {STATUS_APPROVED, STATUS_REJECTED}:
            raise ValueError(f"invalid status: {status}")
        with self._lock:
            if status == STATUS_REJECTED:
                # Revoking access also strips the admin role, so a later
                # re-approve does not silently hand admin rights back.
                cursor = self._conn.execute(
                    "UPDATE access_requests SET status = ?, role = ?, updated_at = ?,"
                    " decided_by = ? WHERE open_id = ?",
                    (status, ROLE_USER, time.time(), decided_by, open_id),
                )
            else:
                cursor = self._conn.execute(
                    "UPDATE access_requests SET status = ?, updated_at = ?, decided_by = ?"
                    " WHERE open_id = ?",
                    (status, time.time(), decided_by, open_id),
                )
            self._conn.commit()
            if cursor.rowcount == 0:
                return None
            row = self._conn.execute(
                "SELECT * FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def set_role(self, open_id: str, role: str, decided_by: str) -> dict[str, Any] | None:
        """Promote/demote an *approved* record. Returns None when no such row
        is approved (pending/rejected rows cannot hold the admin role)."""
        if role not in {ROLE_USER, ROLE_ADMIN}:
            raise ValueError(f"invalid role: {role}")
        with self._lock:
            cursor = self._conn.execute(
                "UPDATE access_requests SET role = ?, updated_at = ?, decided_by = ?"
                " WHERE open_id = ? AND status = ?",
                (role, time.time(), decided_by, open_id, STATUS_APPROVED),
            )
            self._conn.commit()
            if cursor.rowcount == 0:
                return None
            row = self._conn.execute(
                "SELECT * FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def is_approved(self, open_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
        return bool(row) and row["status"] == STATUS_APPROVED

    def is_admin(self, open_id: str) -> bool:
        """Approved *and* role=admin; a revoked admin loses both at once."""
        with self._lock:
            row = self._conn.execute(
                "SELECT status, role FROM access_requests WHERE open_id = ?", (open_id,)
            ).fetchone()
        return bool(row) and row["status"] == STATUS_APPROVED and row["role"] == ROLE_ADMIN

    def touch_login(self, open_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE access_requests SET last_login_at = ? WHERE open_id = ?",
                (time.time(), open_id),
            )
            self._conn.commit()

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM access_requests ORDER BY"
                " CASE status WHEN 'pending' THEN 0 WHEN 'approved' THEN 1 ELSE 2 END,"
                " requested_at DESC"
            ).fetchall()
        return [record for record in (self._row_to_dict(row) for row in rows) if record]

    def pending_count(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM access_requests WHERE status = ?", (STATUS_PENDING,)
            ).fetchone()
        return int(row["n"]) if row else 0

    def guest_key(self) -> str:
        """Current shared guest-link key, or "" when none has been generated."""
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM settings WHERE key = ?", (self.GUEST_KEY_SETTING,)
            ).fetchone()
        return str(row["value"]) if row else ""

    def reset_guest_key(self) -> str:
        """Rotate the guest-link key; old links stop working, approved guests stay."""
        key = secrets.token_urlsafe(24)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (self.GUEST_KEY_SETTING, key),
            )
            self._conn.commit()
        return key


# ---- Feishu HTTP calls (kept module-level so tests can monkeypatch) ----


def _http_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """One JSON round-trip to the Feishu open platform.

    A 4xx from Feishu still carries a JSON body describing the error, so
    HTTPError bodies are parsed instead of raised blindly.
    """
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request_headers = {"Content-Type": "application/json; charset=utf-8"}
    if headers:
        request_headers.update(headers)
    request_obj = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request_obj, timeout=HTTP_TIMEOUT_SECONDS) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read()
    except (urllib.error.URLError, OSError) as exc:
        raise FeishuAuthError(f"飞书接口不可达: {exc}") from exc
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise FeishuAuthError("飞书接口返回了非 JSON 响应") from exc
    if not isinstance(parsed, dict):
        raise FeishuAuthError("飞书接口返回了意外的 JSON 结构")
    return parsed


def _exchange_code(config: FeishuAuthConfig, code: str, redirect_uri: str) -> str:
    """Authorization code → user access token (authen v2 OAuth token)."""
    parsed = _http_json(
        TOKEN_URL,
        method="POST",
        payload={
            "grant_type": "authorization_code",
            "client_id": config.app_id,
            "client_secret": config.app_secret,
            "code": code,
            "redirect_uri": redirect_uri,
        },
    )
    token = str(parsed.get("access_token") or "").strip()
    if not token:
        detail = (
            parsed.get("error_description")
            or parsed.get("error")
            or parsed.get("msg")
            or parsed.get("code")
        )
        raise FeishuAuthError(f"换取飞书用户凭证失败: {detail}")
    return token


def _fetch_user_info(token: str) -> dict[str, Any]:
    parsed = _http_json(USER_INFO_URL, headers={"Authorization": f"Bearer {token}"})
    data = parsed.get("data")
    if parsed.get("code") != 0 or not isinstance(data, dict):
        raise FeishuAuthError(f"获取飞书用户信息失败: {parsed.get('msg') or parsed.get('code')}")
    return data


def _notify_webhook(url: str, text: str) -> bool:
    """Plain-text ping to a Feishu group custom bot. Never raises: a webhook
    outage must not turn a successful login into a 5xx."""
    if not url:
        return False
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode("utf-8")
    request_obj = urllib.request.Request(
        url, data=payload, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=WEBHOOK_TIMEOUT_SECONDS) as response:
            response.read()
        return True
    except Exception as exc:  # noqa: BLE001 - notification is best-effort
        _log("feishu_access_notify_failed", error=str(exc))
        return False


# ---- Signed state (CSRF + post-login redirect target) ----


def _sign(secret: str, body: str) -> str:
    return hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()


def _sanitize_next(value: str | None) -> str:
    """Keep the post-login redirect same-site: a single-slash absolute path."""
    value = (value or "").strip()
    if not value.startswith("/") or value.startswith("//") or "\\" in value:
        return "/"
    return value


def _encode_state(secret: str, next_path: str) -> str:
    payload = {
        "next": _sanitize_next(next_path),
        "ts": int(time.time()),
        "nonce": secrets.token_hex(8),
    }
    body = (
        base64.urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        .decode("ascii")
        .rstrip("=")
    )
    return f"{body}.{_sign(secret, body)}"


def _decode_state(secret: str, state: str) -> str | None:
    """Return the sanitized ``next`` path, or None when invalid/expired."""
    body, _, digest = state.rpartition(".")
    if not body or not digest:
        return None
    if not hmac.compare_digest(_sign(secret, body), digest):
        return None
    try:
        padded = body + "=" * (-len(body) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        issued_at = float(payload.get("ts", 0))
    except (TypeError, ValueError):
        return None
    now = time.time()
    # Expired, or issued implausibly far in the future (clock tampering).
    if now - issued_at >= STATE_TTL_SECONDS or issued_at - now > 60:
        return None
    return _sanitize_next(str(payload.get("next") or "/"))


def _identity_candidates(user: dict[str, Any]) -> set[str]:
    candidates = {str(user.get(key) or "").strip().lower() for key in IDENTITY_KEYS}
    candidates.discard("")
    return candidates


def _user_in(users: frozenset[str], user: dict[str, Any]) -> bool:
    return bool(users) and bool(_identity_candidates(user) & users)


def _user_allowed(config: FeishuAuthConfig, user: dict[str, Any]) -> bool:
    """Static-mode decision: empty allowlist means everyone in scope."""
    if not config.allowed_users:
        return True
    return _user_in(config.allowed_users, user)


# ---- Inline pages (kept tiny and dependency-free) ----

_PAGE_STYLE = """
  :root { color-scheme: light; }
  body { margin: 0; font: 15px/1.6 -apple-system, "PingFang SC", "Helvetica Neue", Arial, sans-serif;
         background: #f4f6f9; color: #1f2933; }
  .wrap { max-width: 720px; margin: 48px auto; padding: 0 20px; }
  .card { background: #fff; border-radius: 12px; padding: 28px 32px; box-shadow: 0 2px 12px rgba(0,0,0,.06); }
  h1 { font-size: 20px; margin: 0 0 12px; }
  .muted { color: #6b7280; font-size: 13px; }
  .pill { display: inline-block; padding: 2px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
  .pill.pending { background: #fff4e5; color: #b45309; }
  .pill.approved { background: #e7f7ed; color: #15803d; }
  .pill.rejected { background: #fdecec; color: #b91c1c; }
  button { font: inherit; padding: 6px 14px; border-radius: 8px; border: 1px solid #d1d5db;
           background: #fff; cursor: pointer; }
  button.primary { background: #2563eb; border-color: #2563eb; color: #fff; }
  button.danger { color: #b91c1c; border-color: #fca5a5; }
  button:disabled { opacity: .5; cursor: default; }
  table { width: 100%; border-collapse: collapse; margin-top: 16px; }
  th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #e5e7eb; vertical-align: top; }
  th { font-size: 12px; color: #6b7280; font-weight: 600; }
  code { font-size: 12px; background: #f3f4f6; padding: 1px 5px; border-radius: 4px; }
"""

_PENDING_PAGE_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>等待审批 · GIF 监控台</title>
<style>__STYLE__</style></head>
<body><div class="wrap"><div class="card">
<h1 id="title">正在检查访问权限…</h1>
<p id="detail" class="muted"></p>
<p class="muted" id="hint"></p>
<p id="actions">
<button id="apply" class="primary" style="display:none" onclick="apply()">申请访问</button>
<button id="retry" style="display:none" onclick="location.href=PREFIX + '/api/auth/feishu/login'">重新登录飞书</button>
</p>
</div></div>
<script>
var PREFIX = __PREFIX__;
var titleEl = document.getElementById('title');
var detailEl = document.getElementById('detail');
var hintEl = document.getElementById('hint');
var applyEl = document.getElementById('apply');
var retryEl = document.getElementById('retry');
var pollTimer = null;
function schedule(ms) { clearTimeout(pollTimer); pollTimer = setTimeout(poll, ms); }
function render(data) {
  var name = data.name ? data.name + '，' : '';
  applyEl.style.display = 'none';
  retryEl.style.display = 'none';
  if (data.status === 'approved') {
    titleEl.textContent = '审批已通过，正在进入…';
    detailEl.textContent = '';
    hintEl.textContent = '';
    location.replace(data.next || (PREFIX + '/'));
    return;
  }
  hintEl.textContent = (data.kind === 'guest' ? '访客 ID：' : '飞书身份：') + (data.open_id || '');
  if (data.status === 'eligible') {
    titleEl.textContent = name + '您还没有监控台的访问权限';
    detailEl.textContent = '点击下方按钮向管理员提交访问申请；管理员批准后本页会自动进入监控台。';
    applyEl.style.display = '';
    applyEl.disabled = false;
    applyEl.textContent = '申请访问';
    return;
  }
  if (data.status === 'pending') {
    titleEl.textContent = name + '您的访问申请已提交';
    detailEl.textContent = '管理员已收到通知。审批通过后本页会自动进入监控台，请保持页面打开或稍后再回来。';
    return;
  }
  if (data.status === 'rejected') {
    titleEl.textContent = name + '访问申请未通过';
    detailEl.textContent = '管理员拒绝了本次申请。如需访问请联系管理员，批准后刷新本页即可进入。';
    return;
  }
  titleEl.textContent = '登录状态已失效';
  detailEl.textContent = '公司员工请重新通过飞书登录；外部访客请重新打开管理员发给你的访客链接。';
  hintEl.textContent = '';
  if (data.kind !== 'guest') retryEl.style.display = '';
}
function poll() {
  fetch(PREFIX + '/api/auth/pending', {headers: {'Accept': 'application/json'}, credentials: 'same-origin'})
    .then(function (r) { return r.json(); })
    .then(function (data) {
      render(data);
      if (data.status === 'pending' || data.status === 'rejected') schedule(5000);
    })
    .catch(function () { schedule(8000); });
}
function apply() {
  applyEl.disabled = true;
  applyEl.textContent = '提交中…';
  fetch(PREFIX + '/api/auth/request', {method: 'POST', credentials: 'same-origin',
    headers: {'Accept': 'application/json'}})
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.error) { detailEl.textContent = data.error; applyEl.disabled = false; applyEl.textContent = '申请访问'; return; }
      render(data);
      if (data.status === 'pending' || data.status === 'rejected') schedule(5000);
    })
    .catch(function () { detailEl.textContent = '提交失败，请重试。'; applyEl.disabled = false; applyEl.textContent = '申请访问'; });
}
poll();
</script></body></html>
"""

_ADMIN_PAGE_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>访问审批 · GIF 监控台</title>
<style>__STYLE__ .wrap { max-width: 1080px; } .actions button { margin-right: 6px; }
  .box { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px 16px; margin: 12px 0 4px; }
  .box code { word-break: break-all; }
  .kind { font-size: 11px; padding: 1px 6px; border-radius: 4px; background: #eef2ff; color: #3730a3; }
  .kind.guest { background: #fdf2f8; color: #9d174d; }
  .role { font-size: 11px; padding: 1px 6px; border-radius: 4px; background: #fef3c7; color: #92400e; margin-left: 4px; }
  .note { color: #4b5563; font-size: 13px; }
</style></head>
<body><div class="wrap"><div class="card">
<h1>GIF 监控台 · 访问审批</h1>
<p class="muted">当前管理员：<span id="admin"></span> · <a href="__PREFIX_LINK__/">返回监控台</a> · <a href="#" onclick="load();return false;">刷新</a></p>
<div class="box">
  <strong>外部访客链接</strong>
  <span class="muted">— 发给公司外的人；点开后填姓名/备注提交申请，你在下方批准。链接可转发，谁点都要经你审批。</span>
  <p id="guestLink" style="margin: 8px 0 6px"></p>
  <button id="copyBtn" onclick="copyLink()" style="display:none">复制链接</button>
  <button id="resetBtn" class="danger" onclick="resetLink()">生成链接</button>
  <span class="muted" style="margin-left:8px">重置后旧链接立即失效，已批准的访客不受影响。</span>
</div>
<p class="muted">批准一次永久有效，直到你点「撤销」。「设为管理员」让对方拥有和你一样的审批权限；.env 里配置的管理员不能在这里撤销或降级。访客换浏览器/清了缓存进不来时，点「找回链接」生成一条 7 天有效的专属链接发给他。</p>
<p id="msg" class="muted"></p>
<table><thead><tr><th>申请人</th><th>类型</th><th>备注</th><th>身份</th><th>状态</th><th>申请时间</th><th>最近登录</th><th>操作</th></tr></thead>
<tbody id="rows"><tr><td colspan="8" class="muted">加载中…</td></tr></tbody></table>
</div></div>
<script>
var PREFIX = __PREFIX__;
var API = PREFIX + '/api/admin/access';
var LABEL = {pending: '待审批', approved: '已批准', rejected: '已拒绝'};
var KIND = {feishu: '飞书', guest: '访客'};
var currentLink = '';
function fmt(ts) { if (!ts) return '—'; var d = new Date(ts * 1000);
  return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0')
    + ' ' + String(d.getHours()).padStart(2,'0') + ':' + String(d.getMinutes()).padStart(2,'0'); }
function esc(s) { return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
  return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; }); }
function renderLink(link) {
  currentLink = link || '';
  var el = document.getElementById('guestLink');
  el.innerHTML = link ? '<code>' + esc(link) + '</code>' : '<span class="muted">尚未生成</span>';
  document.getElementById('copyBtn').style.display = link ? '' : 'none';
  document.getElementById('resetBtn').textContent = link ? '重置链接' : '生成链接';
}
function btn(openId, action, cls, text) {
  return '<button class="' + cls + '" onclick="decide(\\'' + esc(openId) + '\\',\\'' + action + '\\')">' + text + '</button>';
}
function load() {
  fetch(API, {headers: {'Accept': 'application/json'}, credentials: 'same-origin'})
    .then(function (r) { if (r.status === 401) { location.href = PREFIX + '/api/auth/feishu/login?next=' + encodeURIComponent(location.pathname); return null; } return r.json(); })
    .then(function (data) {
      if (!data) return;
      if (data.error) { document.getElementById('msg').textContent = data.error; return; }
      document.getElementById('admin').textContent = data.admin.name + ' (' + data.admin.open_id + ')' + (data.admin.env_admin ? ' · .env 管理员' : ' · 页面管理员');
      renderLink(data.guest_link);
      var rows = data.requests;
      var tbody = document.getElementById('rows');
      if (!rows.length) { tbody.innerHTML = '<tr><td colspan="8" class="muted">暂无申请</td></tr>'; return; }
      tbody.innerHTML = rows.map(function (r) {
        var kind = r.kind || 'feishu';
        var isAdmin = r.role === 'admin';
        var actions = '';
        if (r.status === 'approved') {
          actions += isAdmin ? btn(r.open_id, 'demote', '', '取消管理员') : btn(r.open_id, 'promote', '', '设为管理员');
          if (kind === 'guest') actions += '<button onclick="recoverLink(\\'' + esc(r.open_id) + '\\')">找回链接</button>';
          actions += btn(r.open_id, 'reject', 'danger', '撤销');
        } else if (r.status === 'rejected') {
          actions = btn(r.open_id, 'approve', 'primary', '改为批准');
        } else {
          actions = btn(r.open_id, 'approve', 'primary', '批准') + btn(r.open_id, 'reject', 'danger', '拒绝');
        }
        return '<tr><td>' + esc(r.name || '（未知）') + (isAdmin ? '<span class="role">管理员</span>' : '') + '</td>'
          + '<td><span class="kind ' + kind + '">' + (KIND[kind] || kind) + '</span></td>'
          + '<td class="note">' + esc(r.note || '—') + '</td>'
          + '<td><code>' + esc(r.open_id) + '</code></td>'
          + '<td><span class="pill ' + r.status + '">' + LABEL[r.status] + '</span></td>'
          + '<td>' + fmt(r.requested_at) + '</td><td>' + fmt(r.last_login_at) + '</td>'
          + '<td class="actions">' + actions + '</td></tr>';
      }).join('');
    })
    .catch(function (e) { document.getElementById('msg').textContent = '加载失败：' + e; });
}
function decide(openId, action) {
  if (action === 'reject' && !confirm('撤销/拒绝后对方立即无法访问，确定？')) return;
  if (action === 'promote' && !confirm('设为管理员后对方可以审批、撤销和设置其他管理员，确定？')) return;
  fetch(API + '/decide', {method: 'POST', credentials: 'same-origin',
    headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
    body: JSON.stringify({open_id: openId, action: action})})
    .then(function (r) { return r.json(); })
    .then(function (data) { document.getElementById('msg').textContent = data.error || ''; load(); })
    .catch(function (e) { document.getElementById('msg').textContent = '操作失败：' + e; });
}
function recoverLink(openId) {
  fetch(API + '/guest-link/recover', {method: 'POST', credentials: 'same-origin',
    headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
    body: JSON.stringify({open_id: openId})})
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.error) { document.getElementById('msg').textContent = data.error; return; }
      var link = data.recover_link;
      var done = function () { document.getElementById('msg').textContent = '找回链接已复制（7 天内有效，只发给本人）：' + link; };
      if (navigator.clipboard && navigator.clipboard.writeText) { navigator.clipboard.writeText(link).then(done, function () { prompt('找回链接（7 天内有效）：', link); }); }
      else { prompt('找回链接（7 天内有效）：', link); }
    })
    .catch(function (e) { document.getElementById('msg').textContent = '操作失败：' + e; });
}
function resetLink() {
  if (currentLink && !confirm('重置后旧链接立即失效，确定？')) return;
  fetch(API + '/guest-link/reset', {method: 'POST', credentials: 'same-origin',
    headers: {'Accept': 'application/json'}})
    .then(function (r) { return r.json(); })
    .then(function (data) { document.getElementById('msg').textContent = data.error || ''; if (data.guest_link) renderLink(data.guest_link); })
    .catch(function (e) { document.getElementById('msg').textContent = '操作失败：' + e; });
}
function copyLink() {
  if (!currentLink) return;
  var done = function () { document.getElementById('msg').textContent = '已复制链接'; };
  if (navigator.clipboard && navigator.clipboard.writeText) { navigator.clipboard.writeText(currentLink).then(done, function () { prompt('复制链接：', currentLink); }); }
  else { prompt('复制链接：', currentLink); }
}
load();
</script></body></html>
"""

_GUEST_PAGE_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>访客申请 · GIF 监控台</title>
<style>__STYLE__
  label { display: block; margin: 14px 0 4px; font-size: 13px; color: #374151; }
  input, textarea { width: 100%; box-sizing: border-box; font: inherit; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 8px; }
  textarea { min-height: 72px; resize: vertical; }
</style></head>
<body><div class="wrap"><div class="card">
<h1>申请访问 GIF 监控台</h1>
<p class="muted">你正在通过管理员发放的访客链接申请访问。填写姓名和备注后提交，管理员批准后本页会自动进入监控台。</p>
<form id="form" onsubmit="submitForm(event)">
  <label for="name">姓名 <span class="muted">（必填，让管理员认出你）</span></label>
  <input id="name" maxlength="__NAME_MAX__" autocomplete="name" required>
  <label for="note">备注 <span class="muted">（必填，例如：XX 公司 · 赛事运营）</span></label>
  <textarea id="note" maxlength="__NOTE_MAX__" required></textarea>
  <p id="msg" class="muted"></p>
  <p><button type="submit" class="primary" id="submitBtn">申请访问</button></p>
</form>
</div></div>
<script>
var PREFIX = __PREFIX__;
var KEY = new URLSearchParams(location.search).get('k') || '';
function submitForm(ev) {
  ev.preventDefault();
  var btn = document.getElementById('submitBtn');
  var msg = document.getElementById('msg');
  btn.disabled = true; btn.textContent = '提交中…'; msg.textContent = '';
  fetch(PREFIX + '/api/auth/guest/request', {method: 'POST', credentials: 'same-origin',
    headers: {'Content-Type': 'application/json', 'Accept': 'application/json'},
    body: JSON.stringify({key: KEY, name: document.getElementById('name').value, note: document.getElementById('note').value})})
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.error) { msg.textContent = data.error; btn.disabled = false; btn.textContent = '申请访问'; return; }
      location.replace(data.next || (PREFIX + '/auth/pending'));
    })
    .catch(function () { msg.textContent = '提交失败，请重试。'; btn.disabled = false; btn.textContent = '申请访问'; });
}
</script></body></html>
"""

_GUEST_INVALID_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>链接已失效 · GIF 监控台</title>
<style>__STYLE__</style></head>
<body><div class="wrap"><div class="card">
<h1>访客链接已失效</h1>
<p class="muted">这条链接已被管理员重置或不存在。请联系管理员获取新的访客链接。</p>
</div></div></body></html>
"""


def _render_page(template: str, public_prefix: str, status: int = 200) -> Response:
    html = (
        template.replace("__STYLE__", _PAGE_STYLE)
        .replace("__PREFIX__", json.dumps(public_prefix))
        .replace("__PREFIX_LINK__", public_prefix)
        .replace("__NAME_MAX__", str(GUEST_NAME_MAX))
        .replace("__NOTE_MAX__", str(GUEST_NOTE_MAX))
    )
    response = Response(html, status=status, mimetype="text/html; charset=utf-8")
    response.headers["Cache-Control"] = "no-store"
    return response


# ---- Flask wiring ----


def install_feishu_auth(app: Flask, config: FeishuAuthConfig) -> FeishuAuthConfig:
    """Register the login wall + auth routes on ``app``; returns ``config``."""
    store: AccessStore | None = None
    if config.enabled:
        if not config.secret:
            raise RuntimeError("飞书鉴权已启用但缺少会话签名密钥（GIF_MONITOR_AUTH_SECRET）")
        app.secret_key = config.secret
        # Dedicated cookie name so a same-domain deployment can never clash
        # with another Flask service's default "session" cookie.
        app.config["SESSION_COOKIE_NAME"] = "gif_monitor_session"
        app.config["SESSION_COOKIE_HTTPONLY"] = True
        app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
        app.config["SESSION_COOKIE_SECURE"] = config.resolve_cookie_secure()
        app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=config.session_hours)
        if config.approval_enabled:
            if not config.access_db_path:
                raise RuntimeError("飞书审批模式已启用但缺少申请存储路径（access_db_path）")
            store = AccessStore(config.access_db_path)
            app.extensions["feishu_auth_store"] = store
        # Logged only when enabled: several tests import monitor_server in a
        # subprocess and assert on its exact stdout.
        _log(
            "feishu_auth_config",
            enabled=True,
            allowed_users=len(config.allowed_users),
            admin_users=len(config.admin_users),
            approval_mode=config.approval_enabled,
            notify_webhook_configured=bool(config.notify_webhook),
            redirect_uri_configured=bool(config.redirect_uri),
            session_hours=config.session_hours,
            public_prefix=config.public_prefix or None,
        )

    def _redirect_uri() -> str:
        if config.redirect_uri:
            return config.redirect_uri
        # Behind nginx the externally visible scheme arrives via the proxy.
        forwarded = (request.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip()
        scheme = forwarded or request.scheme
        return f"{scheme}://{request.host}{config.callback_path}"

    def _public_base_url() -> str:
        """Absolute URL of the console root, e.g. https://host/gif-monitor."""
        uri = _redirect_uri()
        if uri.endswith(CALLBACK_PATH):
            return uri[: -len(CALLBACK_PATH)]
        parsed = urllib.parse.urlsplit(uri)
        return f"{parsed.scheme}://{parsed.netloc}{config.public_prefix}"

    def _session_user() -> dict[str, Any] | None:
        if not config.enabled:
            return None
        user = session.get(SESSION_KEY)
        if not isinstance(user, dict):
            return None
        try:
            expires_at = float(user.get("exp", 0))
        except (TypeError, ValueError):
            return None
        if expires_at <= time.time():
            return None
        # Approval-mode sessions stay valid only while the store still says
        # "approved", so an admin's revoke takes effect on the next request
        # instead of at cookie expiry. Admins and the static allowlist never
        # go through the store. Sessions issued before ``via`` existed are
        # treated as approval-backed unless the identity is an admin.
        if store is not None:
            via = user.get("via")
            if via is None:
                via = "admin" if _is_env_admin(user) else "approved"
            if via in ("approved", "guest") and not store.is_approved(
                str(user.get("open_id") or "")
            ):
                session.pop(SESSION_KEY, None)
                return None
        return user

    def _is_env_admin(user: dict[str, Any] | None) -> bool:
        """Super admin from .env: never goes through the store, cannot be
        demoted or revoked from the admin page."""
        return user is not None and _user_in(config.admin_users, user)

    def _is_admin(user: dict[str, Any] | None) -> bool:
        """.env admin, or an approved record promoted to role=admin. The
        store is consulted per call so a demotion lands on the next request."""
        if user is None:
            return False
        if _is_env_admin(user):
            return True
        return store is not None and store.is_admin(str(user.get("open_id") or ""))

    def _grant_session(user_info: dict[str, Any], via: str) -> None:
        session.permanent = True
        session.pop(PENDING_SESSION_KEY, None)
        session[SESSION_KEY] = {
            "open_id": str(user_info.get("open_id") or ""),
            "union_id": str(user_info.get("union_id") or ""),
            "user_id": str(user_info.get("user_id") or ""),
            "name": str(user_info.get("name") or ""),
            "via": via,
            "exp": time.time() + config.session_hours * 3600.0,
        }
        _log(
            "guest_login_ok" if via == "guest" else "feishu_login_ok",
            open_id=str(user_info.get("open_id") or ""),
            name=str(user_info.get("name") or ""),
            via=via,
        )

    def _login_via(user_info: dict[str, Any]) -> str:
        # Only .env admins get the store-exempt "admin" session; a promoted
        # record logs in as "approved" so revoke/demote keep working per request.
        if _is_env_admin(user_info):
            return "admin"
        if _user_in(config.allowed_users, user_info):
            return "allowlist"
        return "approved"

    def _pending_identity() -> dict[str, Any] | None:
        pending = session.get(PENDING_SESSION_KEY)
        if not isinstance(pending, dict) or not pending.get("open_id"):
            return None
        try:
            if float(pending.get("exp", 0)) <= time.time():
                return None
        except (TypeError, ValueError):
            return None
        return pending

    def _set_pending(open_id: str, name: str, next_target: str, kind: str) -> None:
        session.permanent = True
        session[PENDING_SESSION_KEY] = {
            "open_id": open_id,
            "name": name,
            "next": next_target,
            "kind": kind,
            "exp": time.time() + config.session_hours * 3600.0,
        }

    # -- guest cookie: signed "<guest_id>.<hmac>" remembered for a year --

    def _guest_cookie_value(guest_id: str) -> str:
        return f"{guest_id}.{_sign(config.secret, 'guest:' + guest_id)}"

    def _read_guest_cookie() -> str:
        raw = request.cookies.get(GUEST_COOKIE_NAME, "")
        guest_id, _, digest = raw.rpartition(".")
        if not guest_id or not digest:
            return ""
        if not hmac.compare_digest(_sign(config.secret, "guest:" + guest_id), digest):
            return ""
        return guest_id

    def _attach_guest_cookie(response: Response, guest_id: str) -> Response:
        response.set_cookie(
            GUEST_COOKIE_NAME,
            _guest_cookie_value(guest_id),
            max_age=GUEST_COOKIE_MAX_AGE,
            httponly=True,
            samesite="Lax",
            secure=config.resolve_cookie_secure(),
            path=(config.public_prefix or "") + "/",
        )
        return response

    # -- guest recovery link: "<guest_id>.<exp>.<hmac>", issued from the admin
    # page so an approved guest who lost the cookie (logged out elsewhere, new
    # browser, cleared storage) gets it back without a second approval --

    def _guest_recover_token(guest_id: str) -> str:
        exp = str(int(time.time()) + GUEST_RECOVER_TTL_SECONDS)
        return f"{guest_id}.{exp}.{_sign(config.secret, f'guest-recover:{guest_id}:{exp}')}"

    def _parse_guest_recover_token(raw: str) -> str:
        parts = raw.split(".")
        if len(parts) != 3:
            return ""
        guest_id, exp, digest = parts
        if not guest_id.startswith("guest_") or not exp.isdigit():
            return ""
        if int(exp) <= time.time():
            return ""
        if not hmac.compare_digest(_sign(config.secret, f"guest-recover:{guest_id}:{exp}"), digest):
            return ""
        return guest_id

    def _resume_guest_session() -> bool:
        """Re-issue a session for an approved guest whose 72h session lapsed.

        Lets a guest who bookmarked the console (rather than the /guest link)
        get back in silently; without this they would be bounced to the
        Feishu login they cannot pass. The cookie is HMAC-signed and the
        store is consulted, so a revoked or forged guest never resumes.
        """
        if store is None:
            return False
        guest_id = _read_guest_cookie()
        if not guest_id:
            return False
        record = store.get(guest_id)
        if record is None or record["kind"] != KIND_GUEST or record["status"] != STATUS_APPROVED:
            return False
        _grant_session({"open_id": guest_id, "name": record["name"]}, "guest")
        store.touch_login(guest_id)
        return True

    def _notify_new_request(record: dict[str, Any]) -> None:
        is_guest = record.get("kind") == KIND_GUEST
        lines = [
            "【GIF 监控台】新的访客访问申请" if is_guest else "【GIF 监控台】新的访问申请",
            f"申请人：{record.get('name') or '（未知）'}",
        ]
        if is_guest:
            lines.append(f"备注：{record.get('note') or '—'}")
            lines.append(f"访客 ID：{record['open_id']}")
        else:
            lines.append(f"open_id：{record['open_id']}")
        lines.append(f"时间：{time.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"审批页面：{_public_base_url()}{ADMIN_PAGE_PATH}")
        _log("feishu_access_requested", open_id=record["open_id"], name=record.get("name", ""),
             kind=record.get("kind", KIND_FEISHU))
        notified = _notify_webhook(config.notify_webhook, "\n".join(lines))
        _log("feishu_access_notified", open_id=record["open_id"], delivered=notified)

    @app.before_request
    def _feishu_auth_guard():  # pragma: no cover - exercised via test client
        if not config.enabled:
            return None
        path = request.path or "/"
        if path in EXEMPT_PATHS or any(path.startswith(prefix) for prefix in EXEMPT_PREFIXES):
            return None
        if _session_user() is not None:
            return None
        if _resume_guest_session():
            return None
        accepts_html = "text/html" in (request.headers.get("Accept") or "")
        if request.method == "GET" and accepts_html:
            next_target = path
            if request.query_string:
                next_target = f"{path}?{request.query_string.decode('utf-8', 'replace')}"
            return redirect(f"{config.login_path}?next={urllib.parse.quote(next_target)}")
        return jsonify({"error": "未登录飞书账号", "login_url": config.login_path}), 401

    @app.get(LOGIN_PATH)
    def feishu_login():
        if not config.enabled:
            return jsonify({"error": "飞书登录未启用"}), 404
        next_target = _sanitize_next(request.args.get("next"))
        # The browser may send next with the public prefix (e.g. when the
        # frontend ajax handler encodes location.pathname).  Strip it here so
        # the state only carries the internal path; feishu_callback adds the
        # prefix back before issuing the final redirect.
        if config.public_prefix and next_target.startswith(config.public_prefix):
            next_target = next_target[len(config.public_prefix):] or "/"
        params = urllib.parse.urlencode(
            {
                "client_id": config.app_id,
                "redirect_uri": _redirect_uri(),
                "response_type": "code",
                "state": _encode_state(config.secret, next_target),
            }
        )
        return redirect(f"{AUTHORIZE_URL}?{params}")

    def _approval_flow(user_info: dict[str, Any], next_target: str):
        """Approval-mode decision for a user who is neither admin nor allowlisted.

        Nothing is filed here: the user is parked on the waiting page, which
        shows an "apply" button unless a request already exists. Only an
        explicit click (``POST /api/auth/request``) creates the record and
        notifies the admins.
        """
        assert store is not None
        open_id = str(user_info.get("open_id") or "")
        name = str(user_info.get("name") or "")
        if not open_id:
            _log("feishu_login_denied", reason="missing_open_id", name=name)
            return jsonify({"error": "飞书未返回用户 open_id，无法提交访问申请"}), 502
        record = store.get(open_id)
        if record is not None and record["status"] == STATUS_APPROVED:
            _grant_session(user_info, "approved")
            store.touch_login(open_id)
            return redirect(config.public_prefix + next_target)
        _set_pending(open_id, name, next_target, KIND_FEISHU)
        _log(
            "feishu_login_pending",
            open_id=open_id,
            name=name,
            status=record["status"] if record else STATUS_ELIGIBLE,
        )
        return redirect(config.public_prefix + PENDING_PAGE_PATH)

    @app.get(CALLBACK_PATH)
    def feishu_callback():
        if not config.enabled:
            return jsonify({"error": "飞书登录未启用"}), 404
        auth_error = (request.args.get("error") or "").strip()
        if auth_error:
            _log("feishu_login_denied", reason="authorize_error", detail=auth_error)
            return jsonify({"error": f"飞书授权被拒绝: {auth_error}"}), 403
        code = (request.args.get("code") or "").strip()
        next_target = _decode_state(config.secret, (request.args.get("state") or "").strip())
        if not code or next_target is None:
            _log("feishu_login_denied", reason="bad_state")
            return jsonify({"error": "登录状态无效或已过期，请重新打开页面登录"}), 400
        try:
            token = _exchange_code(config, code, _redirect_uri())
            user_info = _fetch_user_info(token)
        except FeishuAuthError as exc:
            _log("feishu_auth_error", error=str(exc))
            return jsonify({"error": str(exc)}), 502
        if config.approval_enabled:
            via = _login_via(user_info)
            if via != "approved":
                _grant_session(user_info, via)
                return redirect(config.public_prefix + next_target)
            return _approval_flow(user_info, next_target)
        if not _user_allowed(config, user_info):
            _log(
                "feishu_login_denied",
                reason="not_in_allowlist",
                open_id=str(user_info.get("open_id") or ""),
                name=str(user_info.get("name") or ""),
            )
            return jsonify({"error": "该飞书账号不在监控台允许名单内"}), 403
        _grant_session(user_info, _login_via(user_info))
        return redirect(config.public_prefix + next_target)

    @app.get(PENDING_PAGE_PATH)
    def feishu_pending_page():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        return _render_page(_PENDING_PAGE_HTML, config.public_prefix)

    @app.get(PENDING_API_PATH)
    def feishu_pending_status():
        """Waiting-page poll. Promotes the pending identity to a full session
        the moment an admin approves, so the user never re-authorises."""
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        user = _session_user()
        if user is None and _resume_guest_session():
            user = _session_user()
        if user is not None:
            return jsonify({"status": STATUS_APPROVED, "name": user.get("name", ""),
                            "open_id": user.get("open_id", ""),
                            "kind": KIND_GUEST if user.get("via") == "guest" else KIND_FEISHU,
                            "next": config.public_prefix + "/"})
        pending = _pending_identity()
        if pending is None:
            return jsonify({"status": "none"})
        kind = str(pending.get("kind") or KIND_FEISHU)
        record = store.get(pending["open_id"])
        if record is None:
            if kind == KIND_GUEST:
                # Guest records are created at form submit; a missing row means
                # the identity is stale (e.g. database reset).
                session.pop(PENDING_SESSION_KEY, None)
                return jsonify({"status": "none", "kind": KIND_GUEST})
            return jsonify({"status": STATUS_ELIGIBLE, "name": pending.get("name", ""),
                            "open_id": pending["open_id"], "kind": kind})
        payload = {"status": record["status"], "name": record["name"] or pending.get("name", ""),
                   "open_id": record["open_id"], "kind": record["kind"]}
        if record["status"] == STATUS_APPROVED:
            via = "guest" if record["kind"] == KIND_GUEST else "approved"
            _grant_session({"open_id": record["open_id"], "name": payload["name"]}, via)
            store.touch_login(record["open_id"])
            payload["next"] = config.public_prefix + _sanitize_next(pending.get("next"))
        return jsonify(payload)

    @app.post(REQUEST_API_PATH)
    def feishu_request_access():
        """The "apply" button: file the access request and notify admins.

        Idempotent — a second click (or a re-login while pending/rejected)
        returns the current status without re-notifying.
        """
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        if _session_user() is not None:
            return jsonify({"status": STATUS_APPROVED, "next": config.public_prefix + "/"})
        pending = _pending_identity()
        if pending is None or pending.get("kind") == KIND_GUEST:
            return jsonify({"error": "登录状态已失效，请重新通过飞书登录",
                            "login_url": config.login_path}), 401
        open_id = pending["open_id"]
        name = str(pending.get("name") or "")
        record, created = store.request(open_id, name)
        if created:
            _notify_new_request(record)
        return jsonify({"status": record["status"], "name": record["name"] or name,
                        "open_id": open_id, "kind": KIND_FEISHU})

    # -- external guests: shared link → name/note form → same approval queue --

    def _guest_key_valid(key: str) -> bool:
        assert store is not None
        current = store.guest_key()
        return bool(current) and bool(key) and hmac.compare_digest(current, key)

    @app.get(GUEST_PAGE_PATH)
    def guest_page():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        # A recovery link from the admin page re-attaches the cookie for an
        # approved guest on this browser, then goes straight in.
        recover_id = _parse_guest_recover_token((request.args.get("r") or "").strip())
        if recover_id:
            record = store.get(recover_id)
            if record is not None and record["kind"] == KIND_GUEST and record["status"] == STATUS_APPROVED:
                _grant_session({"open_id": recover_id, "name": record["name"]}, "guest")
                store.touch_login(recover_id)
                _log("guest_recovered", open_id=recover_id, name=record["name"])
                return _attach_guest_cookie(redirect(config.public_prefix + "/"), recover_id)
            _log("guest_recover_rejected", open_id=recover_id)
            return _render_page(_GUEST_INVALID_HTML, config.public_prefix, status=403)
        # A returning guest is recognised by the long-lived cookie even when
        # the shared link has since been rotated.
        if _resume_guest_session():
            return redirect(config.public_prefix + "/")
        guest_id = _read_guest_cookie()
        if guest_id:
            record = store.get(guest_id)
            if record is not None and record["kind"] == KIND_GUEST:
                _set_pending(guest_id, record["name"], "/", KIND_GUEST)
                return redirect(config.public_prefix + PENDING_PAGE_PATH)
        if not _guest_key_valid((request.args.get("k") or "").strip()):
            _log("guest_link_rejected")
            return _render_page(_GUEST_INVALID_HTML, config.public_prefix, status=403)
        return _render_page(_GUEST_PAGE_HTML, config.public_prefix)

    @app.post(GUEST_REQUEST_API_PATH)
    def guest_request_access():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        body = request.get_json(silent=True) or {}
        if not _guest_key_valid(str(body.get("key") or "").strip()):
            return jsonify({"error": "访客链接已失效，请联系管理员获取新链接"}), 403
        name = " ".join(str(body.get("name") or "").split())[:GUEST_NAME_MAX]
        note = str(body.get("note") or "").strip()[:GUEST_NOTE_MAX]
        if not name:
            return jsonify({"error": "请填写姓名"}), 400
        if not note:
            return jsonify({"error": "请填写备注"}), 400
        # Reuse the existing record for a returning guest (cookie present)
        # so a re-submit after rejection doesn't spawn a duplicate row.
        guest_id = _read_guest_cookie()
        existing = store.get(guest_id) if guest_id else None
        if existing is None or existing["kind"] != KIND_GUEST:
            guest_id = "guest_" + secrets.token_hex(6)
        record, created = store.request(guest_id, name, kind=KIND_GUEST, note=note)
        if created:
            _notify_new_request(record)
        _set_pending(guest_id, name, "/", KIND_GUEST)
        response = jsonify({"status": record["status"], "name": record["name"],
                            "open_id": guest_id, "kind": KIND_GUEST,
                            "next": config.public_prefix + PENDING_PAGE_PATH})
        return _attach_guest_cookie(response, guest_id)

    def _require_admin():
        user = _session_user()
        if user is None:
            return None, (jsonify({"error": "未登录飞书账号", "login_url": config.login_path}), 401)
        if not _is_admin(user):
            _log("feishu_admin_denied", open_id=user.get("open_id", ""), path=request.path)
            return None, (jsonify({"error": "仅管理员可以审批访问申请"}), 403)
        return user, None

    @app.get(ADMIN_PAGE_PATH)
    def feishu_admin_page():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        # The guard already redirected anonymous browsers to login; here a
        # logged-in non-admin gets a plain 403 page instead of an empty table.
        _, error = _require_admin()
        if error is not None:
            return error
        return _render_page(_ADMIN_PAGE_HTML, config.public_prefix)

    def _guest_link() -> str | None:
        assert store is not None
        key = store.guest_key()
        if not key:
            return None
        return f"{_public_base_url()}{GUEST_PAGE_PATH}?k={urllib.parse.quote(key, safe='')}"

    @app.get(ADMIN_API_PATH)
    def feishu_admin_list():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        user, error = _require_admin()
        if error is not None:
            return error
        return jsonify({
            "admin": {
                "open_id": user.get("open_id", ""),
                "name": user.get("name", ""),
                "env_admin": _is_env_admin(user),
            },
            "requests": store.list(),
            "guest_link": _guest_link(),
        })

    def _record_is_env_admin(record: dict[str, Any]) -> bool:
        # A .env admin may also have a row (applied before being listed);
        # the page must not be able to reject/demote them through that row.
        return _user_in(config.admin_users, {"open_id": record.get("open_id", "")})

    @app.post(ADMIN_API_PATH + "/guest-link/reset")
    def feishu_admin_reset_guest_link():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        user, error = _require_admin()
        if error is not None:
            return error
        store.reset_guest_key()
        _log("guest_link_reset", by=user.get("open_id", ""))
        return jsonify({"ok": True, "guest_link": _guest_link()})

    @app.post(ADMIN_API_PATH + "/guest-link/recover")
    def feishu_admin_guest_recover_link():
        """Short-lived personal link for one approved guest (see 找回链接)."""
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        user, error = _require_admin()
        if error is not None:
            return error
        body = request.get_json(silent=True) or {}
        open_id = str(body.get("open_id") or "").strip()
        record = store.get(open_id) if open_id else None
        if record is None or record["kind"] != KIND_GUEST:
            return jsonify({"error": "没有找到该访客"}), 404
        if record["status"] != STATUS_APPROVED:
            return jsonify({"error": "只有已批准的访客才能生成找回链接"}), 409
        link = f"{_public_base_url()}{GUEST_PAGE_PATH}?r={urllib.parse.quote(_guest_recover_token(open_id), safe='')}"
        _log("guest_recover_link_issued", open_id=open_id, by=user.get("open_id", ""))
        return jsonify({"ok": True, "recover_link": link,
                        "expires_in_seconds": GUEST_RECOVER_TTL_SECONDS})

    @app.post(ADMIN_API_PATH + "/decide")
    def feishu_admin_decide():
        if not config.approval_enabled:
            return jsonify({"error": "飞书审批模式未启用"}), 404
        assert store is not None
        user, error = _require_admin()
        if error is not None:
            return error
        body = request.get_json(silent=True) or {}
        open_id = str(body.get("open_id") or "").strip()
        action = str(body.get("action") or "").strip().lower()
        status = {"approve": STATUS_APPROVED, "reject": STATUS_REJECTED}.get(action)
        role = {"promote": ROLE_ADMIN, "demote": ROLE_USER}.get(action)
        if not open_id or (status is None and role is None):
            return jsonify({"error": "参数无效：需要 open_id 和 action=approve|reject|promote|demote"}), 400
        existing = store.get(open_id)
        if existing is None:
            return jsonify({"error": "没有找到该申请"}), 404
        if action in ("reject", "demote") and _record_is_env_admin(existing):
            _log("feishu_admin_denied", open_id=user.get("open_id", ""), path=request.path,
                 reason="env_admin_target", target=open_id)
            return jsonify({"error": ".env 里配置的管理员不能在这里撤销或降级"}), 403
        if role is not None:
            record = store.set_role(open_id, role, str(user.get("open_id") or ""))
            if record is None:
                return jsonify({"error": "只有已批准的用户才能设为管理员"}), 409
            _log(
                "feishu_access_role_changed",
                open_id=open_id,
                name=record["name"],
                kind=record["kind"],
                role=role,
                decided_by=user.get("open_id", ""),
            )
            return jsonify({"ok": True, "request": record})
        assert status is not None
        record = store.decide(open_id, status, str(user.get("open_id") or ""))
        if record is None:
            return jsonify({"error": "没有找到该申请"}), 404
        _log(
            "feishu_access_decided",
            open_id=open_id,
            name=record["name"],
            kind=record["kind"],
            status=status,
            decided_by=user.get("open_id", ""),
        )
        return jsonify({"ok": True, "request": record})

    @app.get("/api/auth/me")
    def feishu_me():
        user = _session_user()
        public = None
        if user is not None:
            public = {
                "open_id": user.get("open_id", ""),
                "name": user.get("name", ""),
                "is_admin": _is_admin(user),
                "env_admin": _is_env_admin(user),
                "kind": KIND_GUEST if user.get("via") == "guest" else KIND_FEISHU,
            }
        payload: dict[str, Any] = {"enabled": config.enabled, "user": public}
        if store is not None and public is not None and public["is_admin"]:
            payload["pending_count"] = store.pending_count()
        return jsonify(payload)

    @app.post("/api/auth/logout")
    def feishu_logout():
        response = jsonify({"ok": True})
        if config.enabled:
            session.pop(SESSION_KEY, None)
            session.pop(PENDING_SESSION_KEY, None)
            # The guest cookie is deliberately kept: it is the guest's only
            # identity, and an approved guest must get back in after logging
            # out until an admin revokes them (see _resume_guest_session).
        return response

    @app.route("/api/auth/check")
    def feishu_auth_check():
        """nginx auth_request subrequest endpoint: 204 when session is valid, 401 otherwise.
        Returns no body so nginx's auth_request_set can't leak session details."""
        if not config.enabled:
            return "", 204
        user = _session_user()
        if user is not None or _resume_guest_session():
            return "", 204
        return "", 401

    return config
