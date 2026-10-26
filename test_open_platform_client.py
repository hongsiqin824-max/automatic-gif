import os
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import Mock, patch

from open_platform_client import (
    LOCAL_FILE_SIZE_ERROR_CODE,
    OpenPlatformClient,
    OpenPlatformConfig,
    OpenPlatformError,
    sign_query,
)


class OpenPlatformImageUploadTests(unittest.TestCase):
    def make_client(self, root: Path) -> OpenPlatformClient:
        return OpenPlatformClient(
            OpenPlatformConfig(
                appid="app-1",
                app_secret="secret-1",
                api_name="admin-archive-createarticle",
                redirect_uri="",
                token_path=root / "token.json",
                image_upload_token="raw-image-token",
            )
        )

    def write_images(self, root: Path) -> tuple[Path, Path]:
        gif = root / "goal.gif"
        cover = root / "goal.jpg"
        # Two image descriptors make this a real animated GIF, not merely a
        # filename/header placeholder.
        gif.write_bytes(
            b"GIF89a"
            b"\x01\x00\x01\x00"
            b"\x80\x00\x00"
            b"\x00\x00\x00\xff\xff\xff"
            b"\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00"
            b"\x2c\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02\x44\x01\x00"
            b"\x3b"
        )
        cover.write_bytes(b"\xff\xd8\xff\xd9-jpg")
        return gif, cover

    def test_image_upload_status_is_independent_from_oauth_status(self):
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(Path(directory))
            status = client.image_upload_status()
        self.assertTrue(status["enabled"])
        self.assertTrue(status["configured"])
        self.assertEqual(status["api_name"], "image-uploadimage-ai")
        self.assertEqual(status["multipart_fields"], ["file1", "file2"])
        self.assertTrue(status["animated_gif_supported"])
        self.assertEqual(status["timeout_seconds"], 120.0)

    def test_image_upload_timeout_is_read_from_environment(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ,
            {"OPEN_PLATFORM_IMAGE_UPLOAD_TIMEOUT_SECONDS": "75"},
            clear=False,
        ):
            config = OpenPlatformConfig.from_environment(Path(directory))
        self.assertEqual(config.image_upload_timeout_seconds, 75.0)

    def test_image_upload_timeout_rejects_values_outside_safe_range(self):
        for value in (59, 121, "not-a-number"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"OPEN_PLATFORM_IMAGE_UPLOAD_TIMEOUT_SECONDS": str(value)},
                clear=False,
            ):
                with self.assertRaises(RuntimeError):
                    OpenPlatformConfig.from_environment(Path("/tmp"))

    def test_uploads_fixed_multipart_fields_and_normalizes_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            request_json = Mock(
                return_value={
                    "code": 0,
                    "messgae": "success",
                    "data": [
                        {
                            "image_id": 11,
                            "mime": "image/gif",
                            "size": len(gif.read_bytes()),
                            "url": "/fastdfs/gif",
                            "img1_url": "https://img1.example/gif",
                        },
                        {
                            "id": 12,
                            "url": "/fastdfs/jpg",
                            "img1_url": "https://img1.example/jpg",
                        },
                    ],
                }
            )
            with patch.object(client, "_ensure_access_token", return_value="token"), patch.object(
                client, "_request_json", request_json
            ):
                result = client.upload_images(gif, cover)

            request_args = request_json.call_args
            url = request_args.args[1]
            parsed_url = urllib.parse.urlsplit(url)
            self.assertEqual(parsed_url.scheme, "https")
            self.assertEqual(parsed_url.netloc, "platform.dongqiudi.com")
            self.assertEqual(parsed_url.path, "/open/v1/do")
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
            self.assertEqual(query["api_name"], ["image-uploadimage-ai"])
            self.assertEqual(query["type"], ["archive"])
            self.assertNotIn("relate_id", query)
            unsigned = {key: values[0] for key, values in query.items() if key != "sign"}
            self.assertEqual(query["sign"], [sign_query(unsigned, "secret-1")])

            body = request_args.kwargs["body"]
            self.assertIn(b'name="file1"; filename="goal.gif"', body)
            self.assertIn(b'name="file2"; filename="goal.jpg"', body)
            self.assertEqual(
                request_args.kwargs["authorization"], "raw-image-token"
            )
            self.assertEqual(request_args.kwargs["timeout_seconds"], 120.0)
            self.assertEqual(result[0]["image_id"], 11)
            self.assertEqual(result[1]["image_id"], 12)
            self.assertEqual(result[1]["mime"], "image/jpeg")
            self.assertEqual(result[1]["file_name"], "goal.jpg")
            self.assertEqual(result[1]["size"], len(cover.read_bytes()))

    def test_upload_does_not_refresh_oauth_after_expired_image_token_code(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            request_json = Mock(
                side_effect=[
                    {"code": 10008, "messgae": "expired", "data": []},
                ]
            )
            with patch.object(client, "_ensure_access_token") as ensure_token, patch.object(
                client, "_request_json", request_json
            ):
                with self.assertRaises(OpenPlatformError) as context:
                    client.upload_images(gif, cover)
            self.assertEqual(request_json.call_count, 1)
            ensure_token.assert_not_called()
            self.assertEqual(context.exception.code, 10008)
            self.assertTrue(context.exception.auth_required)

    def test_upload_requires_configured_raw_image_token(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = OpenPlatformClient(
                OpenPlatformConfig(
                    appid="app-1",
                    app_secret="secret-1",
                    api_name="admin-archive-createarticle",
                    redirect_uri="",
                    token_path=root / "token.json",
                )
            )
            with patch.object(client, "_ensure_access_token") as ensure_token, patch.object(
                client, "_request_json"
            ) as request_json:
                with self.assertRaises(OpenPlatformError) as context:
                    client.upload_images(gif, cover)
            ensure_token.assert_not_called()
            request_json.assert_not_called()
            self.assertEqual(context.exception.code, "image_upload_token_missing")
            self.assertTrue(context.exception.auth_required)

    def test_upload_uses_configured_raw_image_token_without_refreshing_oauth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = OpenPlatformClient(
                OpenPlatformConfig(
                    appid="app-1",
                    app_secret="secret-1",
                    api_name="admin-archive-createarticle",
                    redirect_uri="",
                    token_path=root / "token.json",
                    image_upload_token=" raw-image-token ",
                )
            )
            request_json = Mock(
                return_value={"code": 0, "messgae": "success", "data": []}
            )
            with patch.object(client, "_ensure_access_token") as ensure_token, patch.object(
                client, "_request_json", request_json
            ):
                self.assertEqual(client.upload_images(gif, cover), [])

            ensure_token.assert_not_called()
            self.assertEqual(
                request_json.call_args.kwargs["authorization"], "raw-image-token"
            )

    def test_upload_unwraps_nested_response_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            with patch.object(client, "_ensure_access_token", return_value="token"), patch.object(
                client,
                "_request_json",
                return_value={
                    "response": {
                        "code": 0,
                        "data": {
                            "code": 0,
                            "messgae": "success",
                            "data": [
                                {"url": "/fastdfs/gif", "mime": "image/gif"},
                                {"url": "/fastdfs/cover", "mime": "image/jpeg"},
                            ],
                        },
                    }
                },
            ):
                result = client.upload_images(gif, cover)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["url"], "/fastdfs/gif")
            self.assertEqual(result[1]["url"], "/fastdfs/cover")

    def test_upload_rejects_business_error_using_messgae(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            with patch.object(client, "_ensure_access_token", return_value="token"), patch.object(
                client,
                "_request_json",
                return_value={"code": 300002, "messgae": "Not login", "data": []},
            ):
                with self.assertRaises(OpenPlatformError) as context:
                    client.upload_images(gif, cover)
            self.assertEqual(context.exception.code, 300002)
            self.assertTrue(context.exception.auth_required)
            self.assertEqual(str(context.exception), "Not login")

    def test_upload_marks_gateway_transient_errors_retriable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            for code in (10006, 50001, 50002, 300007, 300009):
                with self.subTest(code=code), patch.object(
                    client,
                    "_request_json",
                    return_value={"code": code, "messgae": "temporary", "data": []},
                ):
                    with self.assertRaises(OpenPlatformError) as context:
                        client.upload_images(gif, cover)
                self.assertTrue(context.exception.retriable)

    def test_platform_300007_is_retriable_because_size_was_checked_locally(self):
        # Seen in production: the same ≤20MB file got 50001/502, then 300007
        # "file size is illegal", then succeeded. The platform only reports
        # 300007 for a locally-bounded file when its gateway truncated the body,
        # so it must not strand the row in a terminal "failed" state.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            with patch.object(
                client,
                "_request_json",
                return_value={"code": 300007, "messgae": "file size is illegal", "data": []},
            ):
                with self.assertRaises(OpenPlatformError) as context:
                    client.upload_images(gif, cover)
        self.assertEqual(context.exception.code, 300007)
        self.assertTrue(context.exception.retriable)
        self.assertFalse(context.exception.auth_required)

    def test_upload_rejects_oversized_file_before_request(self):
        # A genuinely oversized file is terminal: it must not borrow the
        # platform's 300007 (which the queue treats as a retriable gateway
        # fault) or the operator would be told "临时故障，系统会自动重试".
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            with patch.object(client, "_ensure_access_token", return_value="token"), patch.object(
                client, "_request_json"
            ) as request_json:
                with self.assertRaises(OpenPlatformError) as context:
                    client.upload_images(gif, cover, max_bytes=1)
            self.assertEqual(context.exception.code, LOCAL_FILE_SIZE_ERROR_CODE)
            self.assertNotEqual(str(context.exception.code), "300007")
            self.assertFalse(context.exception.retriable)
            self.assertIn("大小非法", str(context.exception))
            request_json.assert_not_called()


class OpenPlatformChannelSearchTests(unittest.TestCase):
    def make_client(self, root: Path) -> OpenPlatformClient:
        return OpenPlatformClient(
            OpenPlatformConfig(
                appid="app-1",
                app_secret="secret-1",
                api_name="admin-archive-createarticle",
                redirect_uri="",
                token_path=root / "token.json",
            )
        )

    def test_search_signs_keyword_and_unwraps_gateway_envelope(self):
        rows = [
            {"value": 1108, "name": "皇家马德里", "type": "球队", "en_type": "Team"},
            {"value": 2299, "name": "皇家马德里卡斯蒂亚", "type": "球队"},
        ]
        request_json = Mock(
            return_value={"code": 0, "data": {"code": 0, "message": "ok", "data": rows}}
        )
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(Path(directory))
            with patch.object(client, "_request_json", request_json):
                result = client.search_channels(" 皇马 ")

        self.assertEqual(result, rows)
        args = request_json.call_args
        self.assertEqual(args.args[0], "GET")
        parsed = urllib.parse.urlsplit(args.args[1])
        self.assertEqual(parsed.path, "/open/v1/do")
        query = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(query["api_name"], ["admin-archive-channelsearch"])
        self.assertEqual(query["keyword"], ["皇马"])
        # need_login=0: no OAuth token is attached, but the gateway signature
        # must cover keyword too (the platform answers 10002 otherwise).
        self.assertIsNone(args.kwargs.get("authorization"))
        # Best-effort lookup inside the publisher lock: short timeout, not 15s.
        self.assertEqual(args.kwargs["timeout_seconds"], 5.0)
        unsigned = {key: values[0] for key, values in query.items() if key != "sign"}
        self.assertEqual(query["sign"], [sign_query(unsigned, "secret-1")])

    def test_search_rejects_empty_keyword_without_request(self):
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(Path(directory))
            with patch.object(client, "_request_json") as request_json:
                with self.assertRaises(OpenPlatformError):
                    client.search_channels("   ")
        request_json.assert_not_called()

    def test_search_raises_on_business_error_and_tolerates_missing_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(Path(directory))
            with patch.object(
                client,
                "_request_json",
                return_value={"code": 0, "data": {"code": 5, "message": "服务异常"}},
            ):
                with self.assertRaises(OpenPlatformError) as context:
                    client.search_channels("皇马")
            self.assertEqual(context.exception.code, 5)
            self.assertTrue(context.exception.retriable)
            with patch.object(
                client,
                "_request_json",
                return_value={"code": 0, "data": {"code": 0, "message": "ok", "data": None}},
            ):
                self.assertEqual(client.search_channels("皇马"), [])

    def test_player_team_signs_channel_id_and_normalises_payload(self):
        payload = {
            "value": 11695165,
            "name": "迈阿密国际",
            "type": "球队",
            "table_id": 57,
            "table_name": "五洲",
            "channel_id": "14572",
            "new_table_id": [57, 253],
        }
        request_json = Mock(
            return_value={"code": 0, "data": {"code": 0, "message": "ok", "data": payload}}
        )
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(Path(directory))
            with patch.object(client, "_request_json", request_json):
                result = client.player_team(14572)

        self.assertEqual(result, payload)
        args = request_json.call_args
        self.assertEqual(args.args[0], "GET")
        parsed = urllib.parse.urlsplit(args.args[1])
        query = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(query["api_name"], ["admin-archive-playerteam"])
        self.assertEqual(query["channel_id"], ["14572"])
        self.assertIsNone(args.kwargs.get("authorization"))
        self.assertEqual(args.kwargs["timeout_seconds"], 5.0)
        unsigned = {key: values[0] for key, values in query.items() if key != "sign"}
        self.assertEqual(query["sign"], [sign_query(unsigned, "secret-1")])

    def test_player_team_rejects_bad_id_and_tolerates_empty_or_error(self):
        with tempfile.TemporaryDirectory() as directory:
            client = self.make_client(Path(directory))
            with patch.object(client, "_request_json") as request_json:
                for bad in ("", "0", "abc", None):
                    with self.assertRaises(OpenPlatformError):
                        client.player_team(bad)
            request_json.assert_not_called()
            # "no match" is data: [] on the platform; callers get {}.
            with patch.object(
                client,
                "_request_json",
                return_value={"code": 0, "data": {"code": 0, "message": "ok", "data": []}},
            ):
                self.assertEqual(client.player_team("14572"), {})
            with patch.object(
                client,
                "_request_json",
                return_value={"code": 0, "data": {"code": 5, "message": "服务异常"}},
            ):
                with self.assertRaises(OpenPlatformError) as context:
                    client.player_team(14572)
            self.assertEqual(context.exception.code, 5)
            self.assertTrue(context.exception.retriable)


if __name__ == "__main__":
    unittest.main()
