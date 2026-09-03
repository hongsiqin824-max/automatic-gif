import tempfile
import unittest
import urllib.parse
from pathlib import Path
from unittest.mock import Mock, patch

from open_platform_client import (
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
            for code in (10006, 50001, 50002):
                with self.subTest(code=code), patch.object(
                    client,
                    "_request_json",
                    return_value={"code": code, "messgae": "temporary", "data": []},
                ):
                    with self.assertRaises(OpenPlatformError) as context:
                        client.upload_images(gif, cover)
                self.assertTrue(context.exception.retriable)

    def test_upload_rejects_oversized_file_before_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            gif, cover = self.write_images(root)
            client = self.make_client(root)
            with patch.object(client, "_ensure_access_token", return_value="token"), patch.object(
                client, "_request_json"
            ) as request_json:
                with self.assertRaises(OpenPlatformError) as context:
                    client.upload_images(gif, cover, max_bytes=1)
            self.assertEqual(context.exception.code, 300007)
            request_json.assert_not_called()


if __name__ == "__main__":
    unittest.main()
