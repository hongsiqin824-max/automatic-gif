"""Deterministic tests for the 8901 Feishu OAuth login wall (feishu_auth.py)."""

from __future__ import annotations

import time
import unittest
import urllib.parse
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from flask import Flask, jsonify

import feishu_auth
from feishu_auth import (
    CALLBACK_PATH,
    LOGIN_PATH,
    SESSION_KEY,
    FeishuAuthConfig,
    FeishuAuthError,
    _decode_state,
    _encode_state,
    _sanitize_next,
    install_feishu_auth,
)

SECRET = "unit-test-secret"


def make_config(**overrides) -> FeishuAuthConfig:
    values = {
        "app_id": "cli_test_app",
        "app_secret": "app-secret",
        "redirect_uri": "https://monitor.example.com" + CALLBACK_PATH,
        "secret": SECRET,
        "session_hours": 72.0,
    }
    values.update(overrides)
    return FeishuAuthConfig(**values)


def build_app(config: FeishuAuthConfig) -> Flask:
    """A miniature monitor app: index page, health, an API, and ingest."""
    app = Flask(__name__)
    install_feishu_auth(app, config)

    @app.get("/")
    def index():
        return "<html>console</html>"

    @app.get("/api/health")
    def health():
        return jsonify({"ok": True})

    @app.get("/api/monitor/matches")
    def matches():
        return jsonify({"matches": []})

    @app.post("/api/monitor/events/ingest")
    def ingest():
        return jsonify({"ok": True, "accepted": True})

    @app.get("/clips/m1/a.gif")
    def clip_file():
        return "gif-bytes"

    return app


def fake_feishu_http(user: dict | None = None, token: str = "u-token"):
    """mock side_effect answering the token exchange and user-info calls."""
    user_data = {"open_id": "ou_1", "union_id": "on_1", "user_id": "u1",
                 "name": "运营甲", "email": "op@example.com"}
    if user is not None:
        user_data = user

    def _side_effect(url, **kwargs):
        if url == feishu_auth.TOKEN_URL:
            payload = kwargs.get("payload") or {}
            assert payload.get("grant_type") == "authorization_code"
            assert payload.get("client_id") == "cli_test_app"
            return {"code": 0, "access_token": token, "expires_in": 7200}
        if url == feishu_auth.USER_INFO_URL:
            headers = kwargs.get("headers") or {}
            assert headers.get("Authorization") == f"Bearer {token}"
            return {"code": 0, "data": user_data}
        raise AssertionError(f"unexpected URL {url}")

    return _side_effect


class StateHelpersTests(unittest.TestCase):
    def test_round_trip_keeps_next_path(self):
        state = _encode_state(SECRET, "/x?a=1")
        self.assertEqual(_decode_state(SECRET, state), "/x?a=1")

    def test_rejects_tampered_signature(self):
        state = _encode_state(SECRET, "/")
        self.assertIsNone(_decode_state(SECRET, state[:-1] + ("0" if state[-1] != "0" else "1")))
        self.assertIsNone(_decode_state("other-secret", state))
        self.assertIsNone(_decode_state(SECRET, "garbage"))
        self.assertIsNone(_decode_state(SECRET, ""))

    def test_rejects_expired_state(self):
        state = _encode_state(SECRET, "/")
        expired = time.time() + feishu_auth.STATE_TTL_SECONDS + 1
        with mock.patch("feishu_auth.time.time", return_value=expired):
            self.assertIsNone(_decode_state(SECRET, state))

    def test_sanitize_next_blocks_offsite_targets(self):
        self.assertEqual(_sanitize_next("//evil.com"), "/")
        self.assertEqual(_sanitize_next("https://evil.com"), "/")
        self.assertEqual(_sanitize_next("\\\\evil"), "/")
        self.assertEqual(_sanitize_next(None), "/")
        self.assertEqual(_sanitize_next("/ok?x=1"), "/ok?x=1")


class DisabledAuthTests(unittest.TestCase):
    def test_everything_open_without_credentials(self):
        app = build_app(FeishuAuthConfig())
        client = app.test_client()
        self.assertEqual(client.get("/").status_code, 200)
        self.assertEqual(client.get("/api/monitor/matches").status_code, 200)
        me = client.get("/api/auth/me").get_json()
        self.assertEqual(me, {"enabled": False, "user": None})
        self.assertEqual(client.get(LOGIN_PATH).status_code, 404)
        self.assertEqual(client.get(CALLBACK_PATH).status_code, 404)
        self.assertEqual(client.post("/api/auth/logout").get_json(), {"ok": True})
        self.assertEqual(client.get("/api/auth/check").status_code, 204)

    def test_enabled_requires_secret(self):
        with self.assertRaises(RuntimeError):
            build_app(make_config(secret=""))


class LoginWallTests(unittest.TestCase):
    def setUp(self):
        self.app = build_app(make_config())
        self.client = self.app.test_client()

    def login(self, client=None, user: dict | None = None, next_path: str = "/"):
        client = client or self.client
        state = _encode_state(SECRET, next_path)
        with mock.patch("feishu_auth._http_json", side_effect=fake_feishu_http(user)):
            return client.get(f"{CALLBACK_PATH}?code=auth-code&state={urllib.parse.quote(state)}")

    def test_page_load_redirects_to_login_route(self):
        response = self.client.get("/", headers={"Accept": "text/html"})
        self.assertEqual(response.status_code, 302)
        # Older Werkzeug emits an absolute Location; compare path + query.
        location = urllib.parse.urlparse(response.headers["Location"])
        self.assertEqual(location.path, LOGIN_PATH)
        self.assertEqual(urllib.parse.parse_qs(location.query)["next"], ["/"])

    def test_api_fetch_gets_401_with_login_url(self):
        response = self.client.get("/api/monitor/matches")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["login_url"], LOGIN_PATH)
        post = self.client.post("/api/monitor/clip", json={})
        self.assertEqual(post.status_code, 401)

    def test_media_routes_are_protected(self):
        self.assertEqual(self.client.get("/clips/m1/a.gif").status_code, 401)

    def test_exempt_paths_stay_open(self):
        self.assertEqual(self.client.get("/api/health").get_json(), {"ok": True})
        self.assertEqual(
            self.client.post("/api/monitor/events/ingest", json={}).status_code, 200
        )
        self.assertEqual(self.client.get("/api/auth/me").status_code, 200)
        # auth/check is under /api/auth/ → exempt prefix; returns 401 (no session)
        self.assertEqual(self.client.get("/api/auth/check").status_code, 401)

    def test_login_route_redirects_to_feishu_authorize(self):
        response = self.client.get(f"{LOGIN_PATH}?next=/deep?x=1")
        self.assertEqual(response.status_code, 302)
        location = response.headers["Location"]
        self.assertTrue(location.startswith(feishu_auth.AUTHORIZE_URL + "?"))
        query = urllib.parse.parse_qs(urllib.parse.urlparse(location).query)
        self.assertEqual(query["client_id"], ["cli_test_app"])
        self.assertEqual(query["redirect_uri"], ["https://monitor.example.com" + CALLBACK_PATH])
        self.assertEqual(query["response_type"], ["code"])
        self.assertEqual(_decode_state(SECRET, query["state"][0]), "/deep?x=1")

    def test_callback_success_sets_session_and_redirects(self):
        response = self.login(next_path="/deep?x=1")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/deep?x=1")
        matches = self.client.get("/api/monitor/matches")
        self.assertEqual(matches.status_code, 200)
        me = self.client.get("/api/auth/me").get_json()
        self.assertEqual(me["user"], {"open_id": "ou_1", "name": "运营甲", "is_admin": False})

    def test_callback_rejects_bad_state(self):
        with mock.patch("feishu_auth._http_json", side_effect=AssertionError("must not be called")):
            response = self.client.get(f"{CALLBACK_PATH}?code=auth-code&state=forged")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.client.get("/api/monitor/matches").status_code, 401)

    def test_callback_without_code_rejected(self):
        state = _encode_state(SECRET, "/")
        response = self.client.get(f"{CALLBACK_PATH}?state={urllib.parse.quote(state)}")
        self.assertEqual(response.status_code, 400)

    def test_callback_reports_authorize_error(self):
        response = self.client.get(f"{CALLBACK_PATH}?error=access_denied")
        self.assertEqual(response.status_code, 403)

    def test_token_exchange_failure_returns_502(self):
        state = _encode_state(SECRET, "/")
        with mock.patch(
            "feishu_auth._http_json",
            return_value={"code": 20063, "error_description": "code expired"},
        ):
            response = self.client.get(
                f"{CALLBACK_PATH}?code=bad&state={urllib.parse.quote(state)}"
            )
        self.assertEqual(response.status_code, 502)
        self.assertIn("code expired", response.get_json()["error"])

    def test_unreachable_backend_returns_502(self):
        state = _encode_state(SECRET, "/")
        with mock.patch("feishu_auth._http_json", side_effect=FeishuAuthError("飞书接口不可达")):
            response = self.client.get(
                f"{CALLBACK_PATH}?code=c&state={urllib.parse.quote(state)}"
            )
        self.assertEqual(response.status_code, 502)

    def test_logout_clears_session(self):
        self.login()
        self.assertEqual(self.client.get("/api/monitor/matches").status_code, 200)
        self.assertEqual(self.client.post("/api/auth/logout").get_json(), {"ok": True})
        self.assertEqual(self.client.get("/api/monitor/matches").status_code, 401)

    def test_expired_session_requires_login_again(self):
        self.login()
        with self.client.session_transaction() as flask_session:
            expired_user = dict(flask_session[SESSION_KEY])
            expired_user["exp"] = time.time() - 1
            flask_session[SESSION_KEY] = expired_user
        self.assertEqual(self.client.get("/api/monitor/matches").status_code, 401)

    def test_auth_check_401_without_session(self):
        self.assertEqual(self.client.get("/api/auth/check").status_code, 401)

    def test_auth_check_204_when_logged_in(self):
        self.login()
        self.assertEqual(self.client.get("/api/auth/check").status_code, 204)
        # Body must be empty (nginx auth_request body is typically discarded).
        self.assertEqual(self.client.get("/api/auth/check").data, b"")


class AllowlistTests(unittest.TestCase):
    def test_user_outside_allowlist_rejected(self):
        app = build_app(make_config(allowed_users=frozenset({"ou_boss"})))
        client = app.test_client()
        state = _encode_state(SECRET, "/")
        with mock.patch("feishu_auth._http_json", side_effect=fake_feishu_http()):
            response = client.get(f"{CALLBACK_PATH}?code=c&state={urllib.parse.quote(state)}")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(client.get("/api/monitor/matches").status_code, 401)

    def test_allowlist_matches_email_case_insensitively(self):
        app = build_app(make_config(allowed_users=frozenset({"op@example.com"})))
        client = app.test_client()
        state = _encode_state(SECRET, "/")
        with mock.patch("feishu_auth._http_json", side_effect=fake_feishu_http()):
            response = client.get(f"{CALLBACK_PATH}?code=c&state={urllib.parse.quote(state)}")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(client.get("/api/monitor/matches").status_code, 200)


class ConfigFromEnvTests(unittest.TestCase):
    def test_disabled_without_both_credentials(self):
        with TemporaryDirectory() as tmp:
            with mock.patch.dict(
                "os.environ",
                {"GIF_MONITOR_FEISHU_APP_ID": "cli_x", "GIF_MONITOR_FEISHU_APP_SECRET": ""},
                clear=False,
            ):
                config = FeishuAuthConfig.from_env(Path(tmp))
            self.assertFalse(config.enabled)
            # Disabled mode must not create the secret file.
            self.assertFalse((Path(tmp) / feishu_auth.SECRET_FILENAME).exists())

    def test_secret_generated_and_reused_when_enabled(self):
        env = {
            "GIF_MONITOR_FEISHU_APP_ID": "cli_x",
            "GIF_MONITOR_FEISHU_APP_SECRET": "s",
            "GIF_MONITOR_AUTH_SECRET": "",
            "GIF_MONITOR_FEISHU_ALLOWED_USERS": "OU_a, op@Example.com",
            "GIF_MONITOR_AUTH_SESSION_HOURS": "24",
        }
        with TemporaryDirectory() as tmp:
            with mock.patch.dict("os.environ", env, clear=False):
                first = FeishuAuthConfig.from_env(Path(tmp))
                second = FeishuAuthConfig.from_env(Path(tmp))
            self.assertTrue(first.enabled)
            self.assertTrue(first.secret)
            self.assertEqual(first.secret, second.secret)
            self.assertTrue((Path(tmp) / feishu_auth.SECRET_FILENAME).exists())
            self.assertEqual(first.allowed_users, frozenset({"ou_a", "op@example.com"}))
            self.assertEqual(first.session_hours, 24.0)

    def test_env_secret_wins_over_file(self):
        env = {
            "GIF_MONITOR_FEISHU_APP_ID": "cli_x",
            "GIF_MONITOR_FEISHU_APP_SECRET": "s",
            "GIF_MONITOR_AUTH_SECRET": "env-secret",
        }
        with TemporaryDirectory() as tmp:
            with mock.patch.dict("os.environ", env, clear=False):
                config = FeishuAuthConfig.from_env(Path(tmp))
            self.assertEqual(config.secret, "env-secret")
            self.assertFalse((Path(tmp) / feishu_auth.SECRET_FILENAME).exists())

    def test_bad_session_hours_raises(self):
        env = {"GIF_MONITOR_AUTH_SESSION_HOURS": "abc"}
        with TemporaryDirectory() as tmp:
            with mock.patch.dict("os.environ", env, clear=False):
                with self.assertRaises(RuntimeError):
                    FeishuAuthConfig.from_env(Path(tmp))

    def test_cookie_secure_auto_follows_redirect_uri(self):
        self.assertTrue(make_config().resolve_cookie_secure())
        self.assertFalse(make_config(redirect_uri="http://x/cb").resolve_cookie_secure())
        self.assertTrue(
            make_config(redirect_uri="http://x/cb", cookie_secure=True).resolve_cookie_secure()
        )


class RedirectUriFallbackTests(unittest.TestCase):
    def test_derives_from_forwarded_proto_and_host(self):
        app = build_app(make_config(redirect_uri=""))
        client = app.test_client()
        response = client.get(
            LOGIN_PATH,
            headers={"X-Forwarded-Proto": "https", "Host": "monitor.example.com"},
        )
        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(response.headers["Location"]).query
        )
        self.assertEqual(
            query["redirect_uri"], ["https://monitor.example.com" + CALLBACK_PATH]
        )

    def test_derived_redirect_uri_includes_prefix(self):
        app = build_app(make_config(redirect_uri="", public_prefix="/gif-monitor"))
        client = app.test_client()
        response = client.get(
            LOGIN_PATH,
            headers={"X-Forwarded-Proto": "https", "Host": "monitor.example.com"},
        )
        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(response.headers["Location"]).query
        )
        self.assertEqual(
            query["redirect_uri"],
            ["https://monitor.example.com/gif-monitor" + CALLBACK_PATH],
        )


class PublicPrefixTests(unittest.TestCase):
    def setUp(self):
        self.config = make_config(public_prefix="/gif-monitor")
        self.app = build_app(self.config)
        self.client = self.app.test_client()

    def login(self, client=None, user: dict | None = None, next_path: str = "/"):
        client = client or self.client
        state = _encode_state(SECRET, next_path)
        with mock.patch("feishu_auth._http_json", side_effect=fake_feishu_http(user)):
            return client.get(
                f"{CALLBACK_PATH}?code=auth-code&state={urllib.parse.quote(state)}"
            )

    def test_page_load_redirects_to_prefixed_login(self):
        response = self.client.get("/", headers={"Accept": "text/html"})
        self.assertEqual(response.status_code, 302)
        location = urllib.parse.urlparse(response.headers["Location"])
        self.assertEqual(location.path, self.config.login_path)
        self.assertEqual(urllib.parse.parse_qs(location.query)["next"], ["/"])

    def test_json_401_includes_prefixed_login_url(self):
        response = self.client.get("/api/monitor/matches")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["login_url"], self.config.login_path)

    def test_login_strips_prefix_from_next(self):
        """feishu_login: when browser sends next with prefix, strip it so state carries the
        internal path; feishu_callback adds the prefix back before the final redirect."""
        with mock.patch(
            "feishu_auth._http_json", side_effect=AssertionError("must not be called")
        ):
            response = self.client.get(
                f"{LOGIN_PATH}?next=%2Fgif-monitor%2Fdeep%3Fx%3D1"
            )
        self.assertEqual(response.status_code, 302)
        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(response.headers["Location"]).query
        )
        self.assertEqual(_decode_state(SECRET, query["state"][0]), "/deep?x=1")

    def test_login_next_without_prefix_passes_through(self):
        """feishu_login: guard-emitted next is already prefix-free — no stripping needed."""
        response = self.client.get(f"{LOGIN_PATH}?next=%2Fdeep")
        self.assertEqual(response.status_code, 302)
        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(response.headers["Location"]).query
        )
        self.assertEqual(_decode_state(SECRET, query["state"][0]), "/deep")

    def test_callback_redirect_adds_prefix(self):
        response = self.login(next_path="/deep?x=1")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/gif-monitor/deep?x=1")

    def test_callback_redirect_root_has_prefix(self):
        response = self.login(next_path="/")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/gif-monitor/")

    def test_internal_routes_unaffected_by_prefix(self):
        """Route decorators always use the internal (un-prefixed) path because
        nginx strips the prefix before forwarding to Flask."""
        self.assertEqual(self.client.get(LOGIN_PATH + "?next=/"           ).status_code, 302)
        self.assertEqual(self.client.get(CALLBACK_PATH + "?error=test"    ).status_code, 403)
        self.assertEqual(self.login(next_path="/")                        .status_code, 302)
        self.assertEqual(self.client.get("/api/auth/me"                   ).status_code, 200)


class ApprovalModeTests(unittest.TestCase):
    """GIF_MONITOR_FEISHU_ADMIN_USERS set → unknown users queue for approval."""

    ADMIN = {"open_id": "ou_admin", "name": "管理员"}
    STRANGER = {"open_id": "ou_new", "name": "新人"}

    def setUp(self):
        self.config = make_config(
            admin_users=frozenset({"ou_admin"}),
            allowed_users=frozenset({"ou_static"}),
            access_db_path=":memory:",
            notify_webhook="https://open.feishu.cn/open-apis/bot/v2/hook/test",
        )
        self.app = build_app(self.config)
        self.notify = mock.patch("feishu_auth._notify_webhook", return_value=True)
        self.notify_mock = self.notify.start()
        self.addCleanup(self.notify.stop)

    def login(self, client, user: dict, next_path: str = "/"):
        state = _encode_state(SECRET, next_path)
        with mock.patch("feishu_auth._http_json", side_effect=fake_feishu_http(user)):
            return client.get(f"{CALLBACK_PATH}?code=c&state={urllib.parse.quote(state)}")

    def apply(self, client):
        return client.post(feishu_auth.REQUEST_API_PATH)

    def login_and_apply(self, client, user: dict, next_path: str = "/"):
        self.login(client, user, next_path)
        return self.apply(client)

    def test_admin_logs_in_directly(self):
        client = self.app.test_client()
        response = self.login(client, self.ADMIN, "/deep")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], "/deep")
        me = client.get("/api/auth/me").get_json()
        self.assertTrue(me["user"]["is_admin"])
        self.assertEqual(me["pending_count"], 0)
        self.notify_mock.assert_not_called()

    def test_static_allowlist_still_bypasses_approval(self):
        client = self.app.test_client()
        response = self.login(client, {"open_id": "ou_static", "name": "老人"})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(client.get("/api/monitor/matches").status_code, 200)
        self.assertFalse(client.get("/api/auth/me").get_json()["user"]["is_admin"])

    def test_stranger_is_parked_as_eligible_without_filing_anything(self):
        client = self.app.test_client()
        response = self.login(client, self.STRANGER, "/deep?x=1")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["Location"], feishu_auth.PENDING_PAGE_PATH)
        # No real session yet: APIs stay locked.
        self.assertEqual(client.get("/api/monitor/matches").status_code, 401)
        # Waiting page is reachable without a session and shows the apply button.
        page = client.get(feishu_auth.PENDING_PAGE_PATH, headers={"Accept": "text/html"})
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"/api/auth/pending", page.data)
        self.assertIn(b"/api/auth/request", page.data)
        status = client.get(feishu_auth.PENDING_API_PATH).get_json()
        self.assertEqual(status["status"], "eligible")
        self.assertEqual(status["open_id"], "ou_new")
        self.assertEqual(status["name"], "新人")
        # Nothing filed, nobody notified until the button is clicked.
        self.notify_mock.assert_not_called()
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        self.assertEqual(admin.get(feishu_auth.ADMIN_API_PATH).get_json()["requests"], [])

    def test_clicking_apply_files_request_and_notifies_once(self):
        client = self.app.test_client()
        self.login(client, self.STRANGER, "/deep?x=1")
        result = self.apply(client).get_json()
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["open_id"], "ou_new")
        self.assertEqual(client.get(feishu_auth.PENDING_API_PATH).get_json()["status"], "pending")
        self.notify_mock.assert_called_once()
        url, text = self.notify_mock.call_args[0]
        self.assertEqual(url, self.config.notify_webhook)
        self.assertIn("新人", text)
        self.assertIn("ou_new", text)
        self.assertIn("https://monitor.example.com" + feishu_auth.ADMIN_PAGE_PATH, text)
        # Second click and re-login do not re-notify.
        self.assertEqual(self.apply(client).get_json()["status"], "pending")
        self.login(client, self.STRANGER)
        self.assertEqual(client.get(feishu_auth.PENDING_API_PATH).get_json()["status"], "pending")
        self.assertEqual(self.notify_mock.call_count, 1)

    def test_apply_without_identity_is_401(self):
        client = self.app.test_client()
        response = self.apply(client)
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.get_json()["login_url"], LOGIN_PATH)
        self.notify_mock.assert_not_called()

    def test_approval_promotes_waiting_page_to_session(self):
        stranger = self.app.test_client()
        self.login_and_apply(stranger, self.STRANGER, "/deep?x=1")
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        listing = admin.get(feishu_auth.ADMIN_API_PATH).get_json()
        self.assertEqual(listing["admin"]["open_id"], "ou_admin")
        self.assertEqual([r["open_id"] for r in listing["requests"]], ["ou_new"])
        self.assertEqual(listing["requests"][0]["status"], "pending")
        self.assertEqual(admin.get("/api/auth/me").get_json()["pending_count"], 1)

        decision = admin.post(
            feishu_auth.ADMIN_API_PATH + "/decide",
            json={"open_id": "ou_new", "action": "approve"},
        ).get_json()
        self.assertTrue(decision["ok"])
        self.assertEqual(decision["request"]["status"], "approved")
        self.assertEqual(decision["request"]["decided_by"], "ou_admin")

        # Stranger's next poll gets a session without another OAuth round-trip.
        status = stranger.get(feishu_auth.PENDING_API_PATH).get_json()
        self.assertEqual(status["status"], "approved")
        self.assertEqual(status["next"], "/deep?x=1")
        self.assertEqual(stranger.get("/api/monitor/matches").status_code, 200)
        me = stranger.get("/api/auth/me").get_json()
        self.assertEqual(me["user"]["open_id"], "ou_new")
        self.assertFalse(me["user"]["is_admin"])
        self.assertEqual(admin.get("/api/auth/me").get_json()["pending_count"], 0)

    def test_approved_user_logs_in_directly_next_time(self):
        stranger = self.app.test_client()
        self.login_and_apply(stranger, self.STRANGER)
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                   json={"open_id": "ou_new", "action": "approve"})
        fresh = self.app.test_client()
        response = self.login(fresh, self.STRANGER, "/deep")
        self.assertEqual(response.headers["Location"], "/deep")
        self.assertEqual(fresh.get("/api/monitor/matches").status_code, 200)
        listing = admin.get(feishu_auth.ADMIN_API_PATH).get_json()
        self.assertIsNotNone(listing["requests"][0]["last_login_at"])

    def test_rejected_user_stays_out_and_can_be_reapproved(self):
        stranger = self.app.test_client()
        self.login_and_apply(stranger, self.STRANGER)
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                   json={"open_id": "ou_new", "action": "reject"})
        status = stranger.get(feishu_auth.PENDING_API_PATH).get_json()
        self.assertEqual(status["status"], "rejected")
        self.assertEqual(stranger.get("/api/monitor/matches").status_code, 401)
        # Logging in again lands back on the waiting page; re-applying does
        # not reopen the request or re-notify.
        again = self.login(stranger, self.STRANGER)
        self.assertEqual(again.headers["Location"], feishu_auth.PENDING_PAGE_PATH)
        self.assertEqual(self.apply(stranger).get_json()["status"], "rejected")
        self.assertEqual(self.notify_mock.call_count, 1)
        admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                   json={"open_id": "ou_new", "action": "approve"})
        self.assertEqual(stranger.get(feishu_auth.PENDING_API_PATH).get_json()["status"],
                         "approved")
        self.assertEqual(stranger.get("/api/monitor/matches").status_code, 200)

    def test_revoking_approved_user_blocks_next_login(self):
        stranger = self.app.test_client()
        self.login_and_apply(stranger, self.STRANGER)
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                   json={"open_id": "ou_new", "action": "approve"})
        admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                   json={"open_id": "ou_new", "action": "reject"})
        fresh = self.app.test_client()
        response = self.login(fresh, self.STRANGER)
        self.assertEqual(response.headers["Location"], feishu_auth.PENDING_PAGE_PATH)
        self.assertEqual(fresh.get("/api/monitor/matches").status_code, 401)

    def test_non_admin_cannot_use_admin_api(self):
        client = self.app.test_client()
        self.login(client, {"open_id": "ou_static", "name": "老人"})
        self.assertEqual(client.get(feishu_auth.ADMIN_API_PATH).status_code, 403)
        self.assertEqual(client.get(feishu_auth.ADMIN_PAGE_PATH).status_code, 403)
        response = client.post(feishu_auth.ADMIN_API_PATH + "/decide",
                               json={"open_id": "ou_new", "action": "approve"})
        self.assertEqual(response.status_code, 403)

    def test_anonymous_admin_routes_require_login(self):
        client = self.app.test_client()
        self.assertEqual(client.get(feishu_auth.ADMIN_API_PATH).status_code, 401)
        page = client.get(feishu_auth.ADMIN_PAGE_PATH, headers={"Accept": "text/html"})
        self.assertEqual(page.status_code, 302)

    def test_admin_page_renders_for_admin(self):
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        page = admin.get(feishu_auth.ADMIN_PAGE_PATH, headers={"Accept": "text/html"})
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"/api/admin/access", page.data)

    def test_decide_validates_input(self):
        admin = self.app.test_client()
        self.login(admin, self.ADMIN)
        bad = admin.post(feishu_auth.ADMIN_API_PATH + "/decide", json={"open_id": "ou_new"})
        self.assertEqual(bad.status_code, 400)
        missing = admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                             json={"open_id": "ou_ghost", "action": "approve"})
        self.assertEqual(missing.status_code, 404)

    def test_pending_poll_without_identity_reports_none(self):
        client = self.app.test_client()
        self.assertEqual(client.get(feishu_auth.PENDING_API_PATH).get_json(), {"status": "none"})

    def test_webhook_failure_does_not_break_apply(self):
        self.notify_mock.return_value = False
        client = self.app.test_client()
        self.login(client, self.STRANGER)
        response = self.apply(client)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["status"], "pending")
        self.assertEqual(client.get(feishu_auth.PENDING_API_PATH).get_json()["status"], "pending")

    def test_logout_clears_pending_identity(self):
        client = self.app.test_client()
        self.login(client, self.STRANGER)
        client.post("/api/auth/logout")
        self.assertEqual(client.get(feishu_auth.PENDING_API_PATH).get_json(), {"status": "none"})
        self.assertEqual(self.apply(client).status_code, 401)

    def test_pending_page_with_prefix_embeds_prefix(self):
        config = make_config(admin_users=frozenset({"ou_admin"}), access_db_path=":memory:",
                             public_prefix="/gif-monitor")
        app = build_app(config)
        client = app.test_client()
        response = self.login(client, self.STRANGER, "/deep")
        self.assertEqual(response.headers["Location"], "/gif-monitor" + feishu_auth.PENDING_PAGE_PATH)
        page = client.get(feishu_auth.PENDING_PAGE_PATH)
        self.assertIn(b'var PREFIX = "/gif-monitor";', page.data)
        self.apply(client)
        admin = app.test_client()
        self.login(admin, self.ADMIN)
        admin.post(feishu_auth.ADMIN_API_PATH + "/decide",
                   json={"open_id": "ou_new", "action": "approve"})
        status = client.get(feishu_auth.PENDING_API_PATH).get_json()
        self.assertEqual(status["next"], "/gif-monitor/deep")


class ApprovalDisabledTests(unittest.TestCase):
    def test_static_mode_hides_approval_routes(self):
        app = build_app(make_config())
        client = app.test_client()
        self.assertEqual(client.get(feishu_auth.PENDING_PAGE_PATH).status_code, 404)
        self.assertEqual(client.get(feishu_auth.PENDING_API_PATH).status_code, 404)
        self.assertEqual(client.post(feishu_auth.REQUEST_API_PATH).status_code, 404)
        self.assertFalse(make_config().approval_enabled)

    def test_admin_env_parsed(self):
        env = {
            "GIF_MONITOR_FEISHU_APP_ID": "cli_x",
            "GIF_MONITOR_FEISHU_APP_SECRET": "s",
            "GIF_MONITOR_AUTH_SECRET": "env-secret",
            "GIF_MONITOR_FEISHU_ADMIN_USERS": "OU_boss",
            "GIF_MONITOR_FEISHU_WEBHOOK": "https://hook/alerts",
            "GIF_MONITOR_FEISHU_ACCESS_WEBHOOK": "",
        }
        with TemporaryDirectory() as tmp:
            with mock.patch.dict("os.environ", env, clear=False):
                config = FeishuAuthConfig.from_env(Path(tmp))
            self.assertTrue(config.approval_enabled)
            self.assertEqual(config.admin_users, frozenset({"ou_boss"}))
            self.assertEqual(config.notify_webhook, "https://hook/alerts")
            self.assertEqual(config.access_db_path, str(Path(tmp) / feishu_auth.ACCESS_DB_FILENAME))
            env["GIF_MONITOR_FEISHU_ACCESS_WEBHOOK"] = "https://hook/access"
            with mock.patch.dict("os.environ", env, clear=False):
                config = FeishuAuthConfig.from_env(Path(tmp))
            self.assertEqual(config.notify_webhook, "https://hook/access")

    def test_store_persists_across_restart(self):
        with TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "access.sqlite3")
            first = feishu_auth.AccessStore(path)
            first.request("ou_new", "新人")
            first.decide("ou_new", "approved", "ou_admin")
            second = feishu_auth.AccessStore(path)
            self.assertEqual(second.get("ou_new")["status"], "approved")
            self.assertEqual(second.pending_count(), 0)


if __name__ == "__main__":
    unittest.main()
