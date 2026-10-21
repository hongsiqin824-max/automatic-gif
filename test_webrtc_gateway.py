import io
import json
import unittest
import urllib.error

from webrtc_gateway import WebRTCGateway


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def _opener(script):
    """Build a fake urlopen driven by ``script``: list of (method, path) -> result.

    A result is bytes (200 body), an int (HTTP error code) or an Exception.
    Calls are recorded on ``opener.calls`` for assertions.
    """
    calls = []

    def opener(req, timeout):
        path = req.full_url.split("9997", 1)[1]
        calls.append((req.get_method(), path, req.data, timeout))
        for (method, prefix), result in script:
            if req.get_method() == method and path.startswith(prefix):
                if isinstance(result, Exception):
                    raise result
                if isinstance(result, int):
                    raise urllib.error.HTTPError(req.full_url, result, "err", {}, None)
                return _FakeResponse(result)
        raise AssertionError(f"unexpected call {req.get_method()} {path}")

    opener.calls = calls
    return opener


def _gateway(script, **kw):
    clock = {"t": 100.0}

    def now():
        return clock["t"]

    def sleep(seconds):
        clock["t"] += seconds

    return WebRTCGateway(
        enabled=True, opener=_opener(script), clock=now, sleep=sleep, **kw
    )


class WebRTCGatewayTests(unittest.TestCase):
    def test_disabled_gateway_never_touches_network(self):
        gw = WebRTCGateway(enabled=False, opener=lambda *a, **k: self.fail("network used"))
        self.assertFalse(gw.healthy())
        self.assertFalse(gw.ensure_path("1", "rtmp://x"))
        self.assertFalse(gw.path_ready("1"))
        gw.remove_path("1")  # no raise

    def test_urls_follow_configured_bases(self):
        gw = WebRTCGateway(enabled=True, rtsp_base="rtsp://127.0.0.1:8554/", whep_base="whep")
        self.assertEqual(gw.rtsp_url("54477889"), "rtsp://127.0.0.1:8554/54477889")
        # Relative (no leading slash) so it survives the /gif-monitor/ prefix.
        self.assertEqual(gw.whep_url("54477889"), "whep/54477889/whep")

    def test_ensure_path_replaces_then_waits_for_ready(self):
        ready_after = {"n": 0}

        def get_result():
            ready_after["n"] += 1
            if ready_after["n"] < 3:
                return json.dumps({"ready": False, "tracks": []}).encode()
            return json.dumps({"ready": True, "tracks": ["H264"]}).encode()

        class _Dyn(bytes):
            pass

        script = [
            (("POST", "/v3/config/paths/replace/54477889"), b"{}"),
        ]
        gw = _gateway(script)
        orig = gw._opener

        def opener(req, timeout):
            if req.get_method() == "GET" and "/v3/paths/get/" in req.full_url:
                orig.calls.append(("GET", "/v3/paths/get/54477889", None, timeout))
                return _FakeResponse(get_result())
            return orig(req, timeout)

        gw._opener = opener
        self.assertTrue(gw.ensure_path("54477889", "rtmp://up/stream"))
        post = orig.calls[0]
        self.assertEqual(post[0], "POST")
        body = json.loads(post[2])
        self.assertEqual(body["source"], "rtmp://up/stream")
        self.assertFalse(body["sourceOnDemand"])
        self.assertEqual(sum(1 for c in orig.calls if c[0] == "GET"), 3)
        self.assertIsNone(gw.last_error)

    def test_ensure_path_times_out_when_source_never_ready(self):
        script = [
            (("POST", "/v3/config/paths/replace/"), b"{}"),
            (("GET", "/v3/paths/get/"), json.dumps({"ready": False, "tracks": []}).encode()),
        ]
        gw = _gateway(script, ready_timeout=1.0)
        self.assertFalse(gw.ensure_path("1", "rtmp://dead"))
        self.assertIn("not ready", gw.last_error)

    def test_ensure_path_fails_fast_when_api_down(self):
        script = [(("POST", "/v3/config/paths/replace/"), urllib.error.URLError("refused"))]
        gw = _gateway(script)
        self.assertFalse(gw.ensure_path("1", "rtmp://x"))
        self.assertIn("refused", gw.last_error)
        # No readiness polling after a failed registration.
        self.assertEqual(len(gw._opener.calls), 1)

    def test_ensure_path_reports_http_error(self):
        script = [(("POST", "/v3/config/paths/replace/"), 500)]
        gw = _gateway(script)
        self.assertFalse(gw.ensure_path("1", "rtmp://x"))
        self.assertIn("HTTP 500", gw.last_error)

    def test_path_ready_requires_ready_and_tracks(self):
        gw = _gateway([(("GET", "/v3/paths/get/"), json.dumps({"ready": True, "tracks": []}).encode())])
        self.assertFalse(gw.path_ready("1"))
        gw = _gateway([(("GET", "/v3/paths/get/"), 404)])
        self.assertFalse(gw.path_ready("1"))
        self.assertIsNone(gw.last_error)  # 404 is "not registered", not an error

    def test_remove_path_ignores_404(self):
        gw = _gateway([(("DELETE", "/v3/config/paths/delete/"), 404)])
        gw.remove_path("1")
        self.assertIsNone(gw.last_error)

    def test_healthy_reflects_api_reachability(self):
        self.assertTrue(_gateway([(("GET", "/v3/paths/list"), b'{"items":[]}')]).healthy())
        self.assertFalse(_gateway([(("GET", "/v3/paths/list"), TimeoutError("slow"))]).healthy())


if __name__ == "__main__":
    unittest.main()
