import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from live_goal_pipeline import (
    BufferUnavailable,
    CoverageStatus,
    PendingEvent,
    Segment,
    analyze_video_coverage,
    compact_segment_list,
    rolling_segment_list_size,
    encode_gif,
    run,
)


class GifBufferTests(unittest.TestCase):
    def _encode_gap_offset_case(
        self,
        *,
        gap_start: float,
        gap_end: float,
        anchor: float,
    ) -> dict:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.write_bytes(b"first")
            second.write_bytes(b"second")
            segments = [
                Segment(first, 0.0, gap_start),
                Segment(second, gap_end, 20.0),
            ]
            coverage = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=20.0,
                anchor=anchor,
                allow_degraded=True,
                stitch_across_gaps=True,
                allow_anchor_adjustment=True,
                max_anchor_gap_seconds=10.0,
                max_anchor_shift_seconds=5.0,
            )
            event = PendingEvent(
                event_type="goal",
                stream_time=anchor,
                source_time=None,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=20.0,
            )

            def fake_run(command, **_kwargs):
                if command[0] == "ffmpeg":
                    self.assertEqual(command[command.index("-t") + 1], "18.000")
                    Path(command[-1]).write_bytes(b"GIF89a")
                    return SimpleNamespace(stderr="", stdout="")
                return SimpleNamespace(
                    stderr="",
                    stdout=(
                        '{"streams":[{"width":384,"height":216,'
                        '"r_frame_rate":"6/1"}],'
                        '"format":{"duration":"18.0","size":"6"}}'
                    ),
                )

            with patch("live_goal_pipeline.run", side_effect=fake_run):
                return encode_gif(
                    "ffmpeg",
                    "ffprobe",
                    segments,
                    event,
                    root,
                    before=anchor,
                    after=20.0 - anchor,
                    width=384,
                    fps=6,
                    colors=160,
                    size_reference_bytes=10_000_000,
                    coverage=coverage,
                )

    def test_encoded_anchor_offset_compresses_gap_before_anchor(self):
        result = self._encode_gap_offset_case(
            gap_start=4.0,
            gap_end=6.0,
            anchor=10.0,
        )

        self.assertEqual(result["requested_anchor_offset_seconds"], 10.0)
        self.assertEqual(result["estimated_encoded_anchor_offset_seconds"], 8.0)
        self.assertEqual(result["timeline_compression_before_anchor_seconds"], 2.0)
        self.assertEqual(result["available_media_duration_seconds"], 18.0)
        self.assertEqual(result["anchor_offset_mapping_basis"], "segment_timeline_estimate")

    def test_encoded_anchor_offset_ignores_gap_after_anchor(self):
        result = self._encode_gap_offset_case(
            gap_start=4.0,
            gap_end=6.0,
            anchor=3.0,
        )

        self.assertEqual(result["requested_anchor_offset_seconds"], 3.0)
        self.assertEqual(result["estimated_encoded_anchor_offset_seconds"], 3.0)
        self.assertEqual(result["timeline_compression_before_anchor_seconds"], 0.0)

    def test_encoded_anchor_offset_uses_adjusted_gap_boundary(self):
        result = self._encode_gap_offset_case(
            gap_start=4.0,
            gap_end=6.0,
            anchor=5.0,
        )

        self.assertTrue(result["anchor_adjusted"])
        self.assertEqual(result["anchor_adjusted_to_stream_time"], 4.0)
        self.assertEqual(result["requested_anchor_offset_seconds"], 5.0)
        self.assertEqual(result["estimated_encoded_anchor_offset_seconds"], 4.0)
        self.assertEqual(result["timeline_compression_before_anchor_seconds"], 1.0)

    def test_rolling_segment_list_capacity_is_finite(self):
        self.assertEqual(rolling_segment_list_size(60.0, 2.0), 34)
        self.assertGreater(
            rolling_segment_list_size(60.0, 2.0, extra_retention_seconds=30.0),
            34,
        )

    def test_compact_segment_list_keeps_only_existing_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid.ts"
            valid.write_bytes(b"media")
            listing = root / "segments.csv"
            listing.write_text(
                "valid.ts,0,1\nmissing.ts,1,2\n",
                encoding="utf-8",
            )

            self.assertEqual(compact_segment_list(listing, root), (2, 1))
            self.assertEqual(
                listing.read_text(encoding="utf-8"),
                "valid.ts,0,1\n",
            )

    def test_encode_gif_honors_an_explicit_safe_output_filename(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            event = PendingEvent(
                event_type="goal",
                stream_time=5.0,
                source_time=5.0,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=8.0,
            )
            filename = "match-42_m090_goal_张三_1-0_default_abcdef.gif"

            def fake_run(command, **kwargs):
                del kwargs
                if command[0] == "ffmpeg":
                    Path(command[-1]).write_bytes(b"GIF89a")
                    return SimpleNamespace(stderr="", stdout="")
                return SimpleNamespace(
                    stderr="",
                    stdout=(
                        '{"streams":[{"width":384,"height":216,'
                        '"r_frame_rate":"6/1"}],'
                        '"format":{"duration":"6.0","size":"6"}}'
                    ),
                )

            with patch("live_goal_pipeline.run", side_effect=fake_run):
                result = encode_gif(
                    "ffmpeg", "ffprobe", [Segment(segment_path, 0.0, 10.0)],
                    event, root, before=3.0, after=3.0, width=384, fps=6,
                    colors=160, size_reference_bytes=10_000_000,
                    output_filename=filename,
                )

            self.assertEqual(Path(result["output"]).name, filename)
            self.assertTrue((root / filename).is_file())
            self.assertFalse((root / filename.replace(".gif", "_segments.txt")).exists())

    def test_encode_gif_rejects_output_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            event = PendingEvent(
                event_type="goal",
                stream_time=5.0,
                source_time=5.0,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=8.0,
            )

            with self.assertRaisesRegex(ValueError, "plain .gif filename"):
                encode_gif(
                    "ffmpeg", "ffprobe", [Segment(segment_path, 0.0, 10.0)],
                    event, root, before=3.0, after=3.0, width=384, fps=6,
                    colors=160, size_reference_bytes=10_000_000,
                    output_filename="../outside.gif",
                )

    def test_encode_gif_cleans_concat_manifest_when_ffmpeg_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            event = PendingEvent(
                event_type="goal",
                stream_time=5.0,
                source_time=5.0,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=8.0,
            )

            with patch(
                "live_goal_pipeline.run",
                side_effect=RuntimeError("ffmpeg failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "ffmpeg failed"):
                    encode_gif(
                        "ffmpeg", "ffprobe", [Segment(segment_path, 0.0, 10.0)],
                        event, root, before=3.0, after=3.0, width=384, fps=6,
                        colors=160, size_reference_bytes=10_000_000,
                    )

            self.assertEqual(list(root.glob("*_segments.txt")), [])

    def test_encode_gif_validates_nonempty_ffmpeg_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            segment_path = root / "segment.ts"
            segment_path.write_bytes(b"video")
            event = PendingEvent(
                event_type="goal",
                stream_time=5.0,
                source_time=5.0,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=8.0,
            )

            with patch(
                "live_goal_pipeline.run",
                return_value=SimpleNamespace(stderr="", stdout=""),
            ):
                with self.assertRaisesRegex(RuntimeError, "did not create GIF"):
                    encode_gif(
                        "ffmpeg", "ffprobe", [Segment(segment_path, 0.0, 10.0)],
                        event, root, before=3.0, after=3.0, width=384, fps=6,
                        colors=160, size_reference_bytes=10_000_000,
                    )

            self.assertEqual(list(root.glob("*_segments.txt")), [])

    def test_run_can_cancel_a_stuck_encoder_process(self):
        cancel_event = threading.Event()
        cancel_event.set()
        with self.assertRaisesRegex(RuntimeError, "cancelled"):
            run(
                ["python3", "-c", "import time; time.sleep(10)"],
                cancel_event=cancel_event,
            )

    def test_rejects_clip_when_anchor_falls_inside_video_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.ts"
            second = root / "second.ts"
            first.touch()
            second.touch()
            event = PendingEvent(
                event_type="goal",
                stream_time=5.0,
                source_time=None,
                detected_wall_time=0.0,
                change_fraction=0.0,
                stability_fraction=0.0,
                output_due_stream_time=8.0,
            )
            with self.assertRaisesRegex(BufferUnavailable, "anchor"):
                encode_gif(
                    "ffmpeg",
                    "ffprobe",
                    [Segment(first, 0.0, 4.0), Segment(second, 6.0, 10.0)],
                    event,
                    root,
                    before=3.0,
                    after=3.0,
                    width=384,
                    fps=6,
                    colors=160,
                    size_reference_bytes=10_000_000,
                )

    def test_coverage_distinguishes_tail_wait_from_permanent_gaps(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / f"{index}.ts" for index in range(3)]
            for path in paths:
                path.write_bytes(b"video")

            waiting = analyze_video_coverage(
                [Segment(paths[0], 0.0, 8.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=5.0,
            )
            self.assertEqual(waiting.status, CoverageStatus.WAITING)
            self.assertEqual(waiting.error_kind, "waiting_for_tail")

            segments = [
                Segment(paths[0], 0.0, 2.0),
                Segment(paths[1], 3.0, 4.0),
                Segment(paths[2], 5.0, 10.0),
            ]
            unavailable = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=10.0,
                anchor=8.0,
            )
            self.assertEqual(unavailable.status, CoverageStatus.UNAVAILABLE)
            self.assertEqual(unavailable.error_kind, "internal_video_gap")
            self.assertEqual(len(unavailable.gaps), 2)

            gap_before_live_tail = analyze_video_coverage(
                segments[:2],
                window_start=0.0,
                window_end=10.0,
                anchor=1.0,
            )
            self.assertEqual(
                gap_before_live_tail.status,
                CoverageStatus.UNAVAILABLE,
            )
            self.assertEqual(
                gap_before_live_tail.error_kind,
                "internal_video_gap",
            )

            confirmed_gap = analyze_video_coverage(
                segments[:2],
                window_start=0.0,
                window_end=10.0,
                anchor=1.0,
                allow_degraded=True,
            )
            self.assertEqual(
                confirmed_gap.status,
                CoverageStatus.READY_DEGRADED,
            )
            self.assertEqual(
                (confirmed_gap.effective_start, confirmed_gap.effective_end),
                (0.0, 2.0),
            )
            self.assertEqual(confirmed_gap.segments, (segments[0],))

            degraded = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=10.0,
                anchor=8.0,
                allow_degraded=True,
            )
            self.assertEqual(degraded.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(degraded.effective_start, 5.0)
            self.assertEqual(degraded.effective_end, 10.0)
            self.assertEqual(degraded.segments, (segments[2],))

    def test_degraded_coverage_keeps_only_the_anchor_component(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / f"component-{index}.ts" for index in range(3)]
            for path in paths:
                path.write_bytes(b"video")
            segments = [
                Segment(paths[0], 0.0, 2.0),
                Segment(paths[1], 3.0, 7.0),
                Segment(paths[2], 8.0, 10.0),
            ]

            middle = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=10.0,
                anchor=5.0,
                allow_degraded=True,
            )
            self.assertEqual(middle.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual((middle.effective_start, middle.effective_end), (3.0, 7.0))
            self.assertEqual(middle.segments, (segments[1],))

            before_gap = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=10.0,
                anchor=1.0,
                allow_degraded=True,
            )
            self.assertEqual(before_gap.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(
                (before_gap.effective_start, before_gap.effective_end),
                (0.0, 2.0),
            )
            self.assertEqual(before_gap.segments, (segments[0],))

            after_gap = analyze_video_coverage(
                segments,
                window_start=0.0,
                window_end=10.0,
                anchor=9.0,
                allow_degraded=True,
            )
            self.assertEqual(after_gap.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(
                (after_gap.effective_start, after_gap.effective_end),
                (8.0, 10.0),
            )
            self.assertEqual(after_gap.segments, (segments[2],))

    def test_tail_wait_can_degrade_at_deadline_but_rejects_tiny_clips(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            usable_path = root / "usable.ts"
            tiny_path = root / "tiny.ts"
            usable_path.write_bytes(b"video")
            tiny_path.write_bytes(b"video")

            waiting = analyze_video_coverage(
                [Segment(usable_path, 0.0, 8.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=5.0,
                allow_degraded=True,
            )
            self.assertEqual(waiting.status, CoverageStatus.WAITING)

            deadline = analyze_video_coverage(
                [Segment(usable_path, 0.0, 8.0)],
                window_start=0.0,
                window_end=10.0,
                anchor=5.0,
                allow_degraded=True,
                force_degraded=True,
            )
            self.assertEqual(deadline.status, CoverageStatus.READY_DEGRADED)
            self.assertEqual(deadline.error_kind, "degraded_deadline")
            self.assertEqual((deadline.effective_start, deadline.effective_end), (0.0, 8.0))

            too_short = analyze_video_coverage(
                [Segment(tiny_path, 5.0, 5.5)],
                window_start=0.0,
                window_end=10.0,
                anchor=5.25,
                allow_degraded=True,
                force_degraded=True,
            )
            self.assertEqual(too_short.status, CoverageStatus.UNAVAILABLE)
            self.assertEqual(too_short.error_kind, "degraded_clip_too_short")


if __name__ == "__main__":
    unittest.main()
