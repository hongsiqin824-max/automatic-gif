from __future__ import annotations

import gzip
import json
import unittest
from unittest.mock import patch

import dashboard_server
import dqd_api
import match_catalog


class _FakeResponse:
    def __init__(self, body: bytes, encoding: str | None = None):
        self._body = body
        self.headers = {"Content-Encoding": encoding} if encoding else {}

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Recorder:
    """Stands in for urllib.request.urlopen and keeps the request it was given."""

    def __init__(self, response: _FakeResponse):
        self.response = response
        self.request = None
        self.timeout = None

    def __call__(self, request, timeout=None):
        self.request = request
        self.timeout = timeout
        return self.response


PAYLOAD = {"list": [{"match_id": "1", "status": "Playing"}]}
PLAIN = json.dumps(PAYLOAD).encode("utf-8")


class ReadResponseBodyTests(unittest.TestCase):
    def test_gunzips_a_gzip_response(self):
        body = dqd_api.read_response_body(_FakeResponse(gzip.compress(PLAIN), "gzip"))
        self.assertEqual(body, PLAIN)

    def test_header_value_is_case_insensitive(self):
        body = dqd_api.read_response_body(_FakeResponse(gzip.compress(PLAIN), " GZIP "))
        self.assertEqual(body, PLAIN)

    def test_plain_response_is_returned_unchanged(self):
        self.assertEqual(dqd_api.read_response_body(_FakeResponse(PLAIN)), PLAIN)

    def test_corrupt_gzip_body_raises(self):
        with self.assertRaises(OSError):
            dqd_api.read_response_body(_FakeResponse(b"not gzip", "gzip"))


class RequestJsonGzipTests(unittest.TestCase):
    def test_accept_gzip_sends_header_and_decodes_compressed_body(self):
        opener = _Recorder(_FakeResponse(gzip.compress(PLAIN), "gzip"))
        with patch("dqd_api.urllib.request.urlopen", opener):
            value = dqd_api.request_json("https://example.test/x", accept_gzip=True)
        self.assertEqual(value, PAYLOAD)
        self.assertEqual(opener.request.get_header("Accept-encoding"), "gzip")

    def test_accept_gzip_still_reads_an_uncompressed_answer(self):
        opener = _Recorder(_FakeResponse(PLAIN))
        with patch("dqd_api.urllib.request.urlopen", opener):
            value = dqd_api.request_json("https://example.test/x", accept_gzip=True)
        self.assertEqual(value, PAYLOAD)

    def test_default_request_does_not_ask_for_gzip(self):
        # The live-source query shares request_json and must stay unchanged.
        opener = _Recorder(_FakeResponse(PLAIN))
        with patch("dqd_api.urllib.request.urlopen", opener):
            dqd_api.request_json("https://example.test/x", method="POST", body={"secret": "s"})
        self.assertIsNone(opener.request.get_header("Accept-encoding"))
        self.assertEqual(opener.timeout, 8.0)


class MatchCatalogEndpointTests(unittest.TestCase):
    def test_monitor_catalog_uses_internal_gateway_gzip_and_long_timeout(self):
        opener = _Recorder(_FakeResponse(gzip.compress(json.dumps({"list": []}).encode()), "gzip"))
        catalog = match_catalog.MatchCatalog()
        with patch("dqd_api.urllib.request.urlopen", opener):
            catalog._fetch(1_793_000_000.0)
        url = opener.request.full_url
        self.assertTrue(
            url.startswith("https://openapi.dongqiudi.com/internal/api/data/tab/new/whole?"), url
        )
        self.assertIn("init=1", url)
        self.assertEqual(opener.request.get_header("Accept-encoding"), "gzip")
        self.assertEqual(opener.timeout, 15.0)

    def test_dashboard_catalog_uses_internal_gateway_gzip_and_long_timeout(self):
        opener = _Recorder(_FakeResponse(gzip.compress(json.dumps({"list": []}).encode()), "gzip"))
        catalog = dashboard_server.MatchCatalog()
        with patch("dashboard_server.urllib.request.urlopen", opener):
            catalog._fetch(1_793_000_000.0)
        url = opener.request.full_url
        self.assertTrue(
            url.startswith("https://openapi.dongqiudi.com/internal/api/data/tab/new/whole?"), url
        )
        self.assertEqual(opener.request.get_header("Accept-encoding"), "gzip")
        self.assertEqual(opener.timeout, 15.0)

    def test_dashboard_default_request_does_not_ask_for_gzip(self):
        opener = _Recorder(_FakeResponse(PLAIN))
        with patch("dashboard_server.urllib.request.urlopen", opener):
            self.assertEqual(dashboard_server._json_request("https://example.test/x"), PAYLOAD)
        self.assertIsNone(opener.request.get_header("Accept-encoding"))


if __name__ == "__main__":
    unittest.main()
