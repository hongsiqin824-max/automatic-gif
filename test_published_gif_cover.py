import base64
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import dashboard_server
from article_publisher import PublishedGifStore


def animated_gif_bytes():
    header = (
        b"GIF89a"
        b"\x01\x00\x01\x00"
        b"\x80\x00\x00"
        b"\x00\x00\x00\xff\xff\xff"
    )
    frame = (
        b"\x2c"
        b"\x00\x00\x00\x00\x01\x00\x01\x00\x00"
        b"\x02\x02\x44\x01\x00"
    )
    return header + frame + frame + b"\x3b"


def different_color_frames_gif_bytes():
    return base64.b64decode(
        "R0lGODlhAgACAIEAAP8AAAAAAAAAAAAAACH/C05FVFNDQVBFMi4wAwEAAAAh+QQA"
        "CgAAACwAAAAAAgACAAAIBgABCAQQEAAh+QQBCgABACwAAAAAAgACAIEAAP8AAAA"
        "AAAAAAAAIBgABCAQQEAA7"
    )


def jpeg_dimensions(body: bytes) -> tuple[int, int]:
    if not body.startswith(b"\xff\xd8"):
        raise AssertionError("not a JPEG")
    offset = 2
    start_of_frame_markers = {
        0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
        0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
    }
    while offset + 4 <= len(body):
        if body[offset] != 0xFF:
            offset += 1
            continue
        marker = body[offset + 1]
        offset += 2
        if marker in {0xD8, 0xD9}:
            continue
        length = int.from_bytes(body[offset : offset + 2], "big")
        if length < 2 or offset + length > len(body):
            break
        if marker in start_of_frame_markers:
            height = int.from_bytes(body[offset + 3 : offset + 5], "big")
            width = int.from_bytes(body[offset + 5 : offset + 7], "big")
            return width, height
        offset += length
    raise AssertionError("JPEG dimensions not found")


class PublishedGifCoverTests(unittest.TestCase):
    def test_cover_uses_the_first_frame(self):
        with tempfile.TemporaryDirectory() as directory:
            store = PublishedGifStore(
                Path(directory), "https://matchgif.aisportsapp.com"
            )
            saved = store.create_bytes(different_color_frames_gif_bytes())
            completed = subprocess.run(
                [
                    shutil.which("ffmpeg") or "ffmpeg",
                    "-loglevel",
                    "error",
                    "-i",
                    saved["cover_path"],
                    "-vf",
                    "crop=1:1:480:270",
                    "-frames:v",
                    "1",
                    "-f",
                    "rawvideo",
                    "-pix_fmt",
                    "rgb24",
                    "pipe:1",
                ],
                capture_output=True,
                check=True,
            )
            red, green, blue = completed.stdout[:3]
            self.assertGreater(red, 200)
            self.assertLess(green, 40)
            self.assertLess(blue, 40)

    def test_store_generates_sha_named_960x540_cover_and_reuses_it(self):
        with tempfile.TemporaryDirectory() as directory:
            store = PublishedGifStore(
                Path(directory), "https://matchgif.aisportsapp.com"
            )
            result = store.create_bytes(animated_gif_bytes())
            cover_path = Path(result["cover_path"])

            self.assertTrue(cover_path.is_file())
            self.assertEqual(cover_path.parent, store.directory / "covers")
            self.assertEqual(cover_path.name, f'{result["gif_id"]}.jpg')
            self.assertEqual(result["cover_width"], 960)
            self.assertEqual(result["cover_height"], 540)
            self.assertEqual(jpeg_dimensions(cover_path.read_bytes()), (960, 540))
            self.assertEqual(
                result["cover_url"],
                f'https://matchgif.aisportsapp.com/publish-gif-covers/{result["gif_id"]}.jpg',
            )
            self.assertEqual(
                [path for path in store.cover_directory.iterdir() if path.suffix == ".tmp"],
                [],
            )

            with patch("article_publisher.subprocess.run") as ffmpeg:
                reused = store.ensure_cover(result["gif_id"])
            ffmpeg.assert_not_called()
            self.assertTrue(reused["cover_reused"])

    def test_cover_route_lazily_regenerates_missing_cover(self):
        with tempfile.TemporaryDirectory() as directory:
            store = PublishedGifStore(
                Path(directory), "https://matchgif.aisportsapp.com"
            )
            saved = store.create_bytes(animated_gif_bytes())
            cover_path = Path(saved["cover_path"])
            cover_path.unlink()
            fake_publisher = Mock(gif_store=store)
            with patch.object(dashboard_server, "article_publisher", fake_publisher):
                client = dashboard_server.app.test_client()
                response = client.get(
                    f'/publish-gif-covers/{saved["gif_id"]}.jpg'
                )
                invalid = client.get("/publish-gif-covers/not-a-sha.jpg")

            self.assertEqual(response.status_code, 200)
            self.assertTrue(response.content_type.startswith("image/jpeg"))
            self.assertEqual(
                response.headers["Cache-Control"],
                "public, max-age=31536000, immutable",
            )
            self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
            self.assertTrue(response.data.startswith(b"\xff\xd8"))
            self.assertEqual(jpeg_dimensions(response.data), (960, 540))
            self.assertTrue(cover_path.is_file())
            self.assertEqual(invalid.status_code, 404)
            response.close()
            invalid.close()

    def test_store_rebuilds_corrupt_or_wrong_size_existing_cover(self):
        with tempfile.TemporaryDirectory() as directory:
            store = PublishedGifStore(
                Path(directory), "https://matchgif.aisportsapp.com"
            )
            saved = store.create_bytes(animated_gif_bytes())
            cover_path = Path(saved["cover_path"])

            invalid_covers = (
                b"\xff\xd8<html>not a jpeg</html>\xff\xd9",
                b"\xff\xd8\xff\xc0\x00\x07\x08\x00\x01\x00\x01\xff\xd9",
            )
            for invalid_cover in invalid_covers:
                with self.subTest(invalid_cover=invalid_cover):
                    cover_path.write_bytes(invalid_cover)
                    rebuilt = store.ensure_cover(saved["gif_id"])

                    self.assertFalse(rebuilt["cover_reused"])
                    body = cover_path.read_bytes()
                    self.assertEqual(jpeg_dimensions(body), (960, 540))
                    self.assertTrue(body.endswith(b"\xff\xd9"))


if __name__ == "__main__":
    unittest.main()
