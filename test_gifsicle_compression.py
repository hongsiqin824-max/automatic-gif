from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import vision_runtime


def _eligible_result(path: Path) -> dict[str, object]:
    return {
        "output": str(path),
        "output_kind": "ocr_window",
        "artifact_kind": "ocr_window",
        "localization_source": "exact_second",
        "clip_before_seconds": 30.0,
        "clip_after_seconds": 30.0,
        "width": 384,
        "height": 216,
        "duration_sec": 60.0,
    }


class GifsicleCompressionTests(unittest.TestCase):
    def test_strict_selection_excludes_fallbacks_and_tdeed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clip.gif"
            path.write_bytes(b"original")
            eligible = _eligible_result(path)
            self.assertTrue(
                vision_runtime._strict_ocr_gif_result(
                    eligible,
                    artifact_kind="ocr_window",
                )
            )
            for changes, artifact_kind in (
                ({"output_kind": "minute_range_fallback"}, "ocr_window"),
                ({"localization_source": "minute_boundary"}, "ocr_window"),
                ({}, "tdeed_refined"),
                ({"clip_before_seconds": 60.0}, "ocr_window"),
            ):
                candidate = {**eligible, **changes}
                self.assertFalse(
                    vision_runtime._strict_ocr_gif_result(
                        candidate,
                        artifact_kind=artifact_kind,
                    )
                )

    def test_disabled_is_a_successful_noop_with_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clip.gif"
            path.write_bytes(b"original")
            with patch.dict(os.environ, {"OCR_GIF_GIFSICLE_ENABLED": "0"}, clear=False):
                diagnostic = vision_runtime._compress_ocr_gif(
                    _eligible_result(path),
                    artifact_kind="ocr_window",
                    ffprobe=None,
                )
            self.assertEqual(diagnostic["status"], "disabled")
            self.assertEqual(path.read_bytes(), b"original")

    def test_missing_tool_falls_back_without_changing_gif(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clip.gif"
            path.write_bytes(b"original")
            with patch.dict(os.environ, {"OCR_GIF_GIFSICLE_ENABLED": "1"}, clear=False), patch(
                "vision_runtime._ocr_gifsicle_binary", return_value=None
            ):
                diagnostic = vision_runtime._compress_ocr_gif(
                    _eligible_result(path),
                    artifact_kind="ocr_window",
                    ffprobe=None,
                )
            self.assertEqual(diagnostic["status"], "tool_missing")
            self.assertEqual(path.read_bytes(), b"original")

    def test_smaller_valid_output_is_adopted_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clip.gif"
            path.write_bytes(b"original-content")

            def run_gifsicle(command, **kwargs):
                Path(command[-1]).write_bytes(b"small")
                return subprocess.CompletedProcess(command, 0, "", "")

            with patch.dict(os.environ, {"OCR_GIF_GIFSICLE_ENABLED": "1"}, clear=False), patch(
                "vision_runtime._ocr_gifsicle_binary", return_value="gifsicle"
            ), patch("vision_runtime._probe_gif_metadata", return_value={
                "width": 384,
                "height": 216,
                "frames": 360,
                "duration_sec": 60.0,
            }), patch("vision_runtime.subprocess.run", side_effect=run_gifsicle) as run:
                diagnostic = vision_runtime._compress_ocr_gif(
                    _eligible_result(path),
                    artifact_kind="ocr_window",
                    ffprobe=None,
                )
            self.assertEqual(diagnostic["status"], "compressed")
            self.assertTrue(diagnostic["adopted"])
            self.assertEqual(diagnostic["original_bytes"], len(b"original-content"))
            self.assertEqual(diagnostic["compressed_bytes"], len(b"small"))
            self.assertEqual(path.read_bytes(), b"small")
            run.assert_called_once()
            self.assertEqual(run.call_args.args[0][:3], ["gifsicle", "-O3", "--colors"])

    def test_larger_or_failed_output_keeps_original(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clip.gif"
            original = b"original-content"
            path.write_bytes(original)

            def run_larger(command, **kwargs):
                Path(command[-1]).write_bytes(original + b"-larger")
                return subprocess.CompletedProcess(command, 0, "", "")

            with patch.dict(os.environ, {"OCR_GIF_GIFSICLE_ENABLED": "1"}, clear=False), patch(
                "vision_runtime._ocr_gifsicle_binary", return_value="gifsicle"
            ), patch("vision_runtime._probe_gif_metadata", return_value={
                "width": 384,
                "height": 216,
                "frames": 360,
                "duration_sec": 60.0,
            }), patch("vision_runtime.subprocess.run", side_effect=run_larger):
                diagnostic = vision_runtime._compress_ocr_gif(
                    _eligible_result(path),
                    artifact_kind="ocr_window",
                    ffprobe=None,
                )
            self.assertEqual(diagnostic["status"], "larger_or_equal")
            self.assertEqual(path.read_bytes(), original)

            path.write_bytes(original)
            with patch.dict(os.environ, {"OCR_GIF_GIFSICLE_ENABLED": "1"}, clear=False), patch(
                "vision_runtime._ocr_gifsicle_binary", return_value="gifsicle"
            ), patch(
                "vision_runtime.subprocess.run",
                side_effect=subprocess.CalledProcessError(2, ["gifsicle"]),
            ):
                failed = vision_runtime._compress_ocr_gif(
                    _eligible_result(path),
                    artifact_kind="ocr_window",
                    ffprobe=None,
                )
            self.assertEqual(failed["status"], "failed")
            self.assertEqual(path.read_bytes(), original)

    def test_timeout_keeps_original(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "clip.gif"
            original = b"original-content"
            path.write_bytes(original)
            with patch.dict(
                os.environ,
                {
                    "OCR_GIF_GIFSICLE_ENABLED": "1",
                    "OCR_GIF_GIFSICLE_TIMEOUT_SECONDS": "2",
                },
                clear=False,
            ), patch(
                "vision_runtime._ocr_gifsicle_binary", return_value="gifsicle"
            ), patch(
                "vision_runtime.subprocess.run",
                side_effect=subprocess.TimeoutExpired(["gifsicle"], 2),
            ):
                diagnostic = vision_runtime._compress_ocr_gif(
                    _eligible_result(path),
                    artifact_kind="ocr_window",
                    ffprobe=None,
                )
            self.assertEqual(diagnostic["status"], "timeout")
            self.assertEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
