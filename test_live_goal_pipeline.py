import tempfile
import threading
import unittest
from pathlib import Path

from live_goal_pipeline import (
    BufferUnavailable,
    PendingEvent,
    Segment,
    encode_gif,
    run,
)


class GifBufferTests(unittest.TestCase):
    def test_run_can_cancel_a_stuck_encoder_process(self):
        cancel_event = threading.Event()
        cancel_event.set()
        with self.assertRaisesRegex(RuntimeError, "cancelled"):
            run(
                ["python3", "-c", "import time; time.sleep(10)"],
                cancel_event=cancel_event,
            )

    def test_rejects_clip_spanning_a_video_gap(self):
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
            with self.assertRaisesRegex(BufferUnavailable, "video gap"):
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


if __name__ == "__main__":
    unittest.main()
