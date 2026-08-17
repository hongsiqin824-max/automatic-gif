from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from scoreboard_ocr import ClockContinuityStateMachine, ScoreboardProfile
from scoreboard_ocr_worker import (
    BatchOcrWorker,
    WorkerError,
    _profile_readings,
    _recognize_paths_shared,
    _validate_profile_content_quality,
    extract_profile_frames,
    extract_scoreboard_frames,
    frame_reading,
    locate_from_readings,
    probe_video_dimensions,
    recognize_batch,
    serve_socket,
    split_frame_reading,
)


class FakeEngine:
    def __init__(self) -> None:
        self.calls: list[list[object]] = []

    def predict(self, crops):
        self.calls.append(list(crops))
        return [
            {"rec_texts": [str(crop)], "rec_scores": [0.9]}
            for crop in crops
        ]


class BatchRecognitionTests(unittest.TestCase):
    def test_recognize_batch_calls_backend_once_and_keeps_alignment(self):
        engine = FakeEngine()

        results = recognize_batch(
            engine,
            ["44:59", "0-0", "45:00"],
            minimum_confidences=[0.0, 0.95, 0.0],
        )

        self.assertEqual(engine.calls, [["44:59", "0-0", "45:00"]])
        self.assertEqual(results[0], (["44:59"], [0.9]))
        self.assertEqual(results[1], ([], []))
        self.assertEqual(results[2], (["45:00"], [0.9]))

    def test_rejects_misaligned_backend_results(self):
        class BrokenEngine:
            def predict(self, _crops):
                return [{"rec_texts": ["45:00"], "rec_scores": [0.9]}]

        with self.assertRaises(WorkerError) as raised:
            recognize_batch(
                BrokenEngine(),
                ["clock-a", "clock-b"],
                minimum_confidences=[0.0, 0.0],
            )

        self.assertEqual(raised.exception.kind, "ocr_inference_failed")


class ProfileCropTests(unittest.TestCase):
    @staticmethod
    def _profile(**overrides):
        values = {
            "profile_id": "source-a",
            "reference_width": 1920,
            "reference_height": 1080,
            "clock_roi": (100, 40, 220, 90),
            "score_roi": (230, 40, 340, 90),
        }
        values.update(overrides)
        return ScoreboardProfile(**values)

    def test_extracts_aligned_clock_and_score_rois(self):
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            candidate = output_dir / "candidate.mp4"
            candidate.write_bytes(b"video")
            commands = []

            def runner(command, **_kwargs):
                commands.append(command)
                if command[0] == "ffprobe":
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=json.dumps(
                            {"streams": [{"width": 1280, "height": 720}]}
                        ),
                        stderr="",
                    )
                (output_dir / "clock_000001.png").write_bytes(b"clock")
                (output_dir / "score_000001.png").write_bytes(b"score")
                return subprocess.CompletedProcess(
                    command, 0, stdout="", stderr=""
                )

            with patch("scoreboard_ocr_worker.subprocess.run", side_effect=runner):
                pairs, diagnostics = extract_profile_frames(
                    candidate,
                    output_dir,
                    ffmpeg="ffmpeg",
                    sample_interval_seconds=1,
                    profile=self._profile(),
                )

        self.assertEqual(len(pairs), 1)
        self.assertEqual(diagnostics["clock_roi"], [67, 27, 147, 60])
        self.assertEqual(diagnostics["score_roi"], [153, 27, 227, 60])
        filter_graph = commands[1][commands[1].index("-filter_complex") + 1]
        self.assertIn("crop=80:33:67:27", filter_graph)
        self.assertIn("crop=74:33:153:27", filter_graph)
        self.assertIn("split=2", filter_graph)

    def test_profile_aspect_mismatch_fails_closed_before_extraction(self):
        probe = subprocess.CompletedProcess(
            ["ffprobe"],
            0,
            stdout=json.dumps({"streams": [{"width": 1024, "height": 768}]}),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "scoreboard_ocr_worker.subprocess.run", return_value=probe
            ) as runner:
                with self.assertRaises(WorkerError) as raised:
                    extract_profile_frames(
                        Path(directory) / "candidate.mp4",
                        Path(directory),
                        ffmpeg="ffmpeg",
                        sample_interval_seconds=1,
                        profile=self._profile(),
                    )

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(runner.call_count, 1)

    def test_separate_parsing_repairs_clock_without_using_score_text(self):
        recognized = [
            (["68:55"], [0.9]),
            (["1-0"], [0.9]),
            (["B:56"], [0.8]),
            (["1-0"], [0.9]),
            ([], []),
            ([], []),
            (["68:58"], [0.9]),
            (["1-0"], [0.9]),
        ]

        readings, continuity = _profile_readings(
            recognized,
            profile=self._profile(),
            sample_interval=1,
            period=2,
        )

        self.assertEqual(readings[0].clock_seconds, 68 * 60 + 55)
        self.assertEqual(readings[1].clock_seconds, 68 * 60 + 56)
        self.assertEqual(readings[1].score, (1, 0))
        self.assertEqual(continuity[1]["reason"], "ocr_character_repaired")
        self.assertIsNone(readings[2].clock_seconds)
        self.assertEqual(
            continuity[2]["reason"], "scoreboard_temporarily_missing"
        )
        self.assertEqual(readings[3].clock_seconds, 68 * 60 + 58)

    def test_replay_scoreboard_gap_stays_in_one_minute_interval(self):
        readings = [
            frame_reading(0, 0, ["59:05"], [0.9]),
            *[
                frame_reading(index, index, [], [])
                for index in range(1, 11)
            ],
            frame_reading(11, 11, ["59:16"], [0.9]),
        ]

        result = locate_from_readings(
            readings,
            {
                "event_code": "YC",
                "event_minute": 59,
                "sample_interval_seconds": 1,
            },
        )

        self.assertEqual(result["candidate_interval_start_seconds"], 0)
        self.assertEqual(result["candidate_interval_end_seconds"], 12)

    def test_profile_content_rejects_one_random_clock_reading(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = [
            split_frame_reading(
                index,
                index,
                ["59:10"] if index == 4 else [],
                [],
                tracker=tracker,
                period=2,
            )
            for index in range(10)
        ]

        with self.assertRaises(WorkerError) as raised:
            _validate_profile_content_quality(readings)

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(raised.exception.diagnostics["trusted_clock_frame_count"], 1)
        self.assertEqual(raised.exception.diagnostics["minimum_trusted_clock_frames"], 3)
        self.assertEqual(raised.exception.diagnostics["minimum_trusted_clock_rate"], 0.2)

    def test_profile_content_rejects_repeated_static_clock_like_text(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = [
            split_frame_reading(
                index,
                index,
                ["59:10"],
                ["1-0"],
                tracker=tracker,
                period=2,
            )
            for index in range(10)
        ]

        with self.assertRaises(WorkerError) as raised:
            _validate_profile_content_quality(readings)

        self.assertEqual(raised.exception.kind, "clock_profile_mismatch")
        self.assertEqual(raised.exception.diagnostics["clock_progression_seconds"], 0)
        self.assertEqual(
            raised.exception.diagnostics["minimum_clock_progression_seconds"], 1
        )

    def test_profile_content_allows_replay_gap_with_trusted_clock_around_it(self):
        tracker = ClockContinuityStateMachine(self._profile())
        readings = []
        for index in range(15):
            if 5 <= index < 10:
                clock_texts = []
                score_texts = []
            else:
                clock_texts = [f"59:{index:02d}"]
                score_texts = ["1-0"]
            readings.append(
                split_frame_reading(
                    index,
                    index,
                    clock_texts,
                    score_texts,
                    tracker=tracker,
                    period=2,
                )
            )

        diagnostics = _validate_profile_content_quality(readings)

        self.assertEqual(diagnostics["trusted_clock_frame_count"], 10)
        self.assertEqual(diagnostics["scoreboard_missing_frame_count"], 5)
        self.assertGreater(diagnostics["trusted_clock_rate"], 0.6)


class BatchOcrWorkerTests(unittest.TestCase):
    def test_shared_path_recognition_streams_more_crops_than_queue_capacity(self):
        def slow_recognizer(_engine, crops, **_kwargs):
            time.sleep(0.35)
            return [([str(crop)], [0.9]) for crop in crops]

        worker = BatchOcrWorker(
            engine_factory=lambda _language: object(),
            batch_recognizer=slow_recognizer,
            max_batch_size=4,
            batch_wait_seconds=0.02,
            queue_capacity=4,
        )
        paths = [Path(f"clock-{index}.png") for index in range(12)]
        try:
            recognized, failed = _recognize_paths_shared(
                worker,
                paths,
                kinds=["clock"] * len(paths),
                match_id="match-a",
                profile_id="source-a",
                candidate_start_seconds=0,
                sample_interval=1,
                minimum_confidence=0,
                deadline_monotonic=time.monotonic() + 4.0,
            )
        finally:
            worker.close(timeout=1)

        self.assertEqual(failed, [])
        self.assertEqual(
            [texts[0] for texts, _confidences in recognized],
            [str(path) for path in paths],
        )

    def test_batches_clock_and_score_crops_across_matches(self):
        factory_calls: list[str] = []
        engine = FakeEngine()

        def factory(language):
            factory_calls.append(language)
            return engine

        worker = BatchOcrWorker(
            engine_factory=factory,
            max_batch_size=4,
            batch_wait_seconds=0.1,
            queue_capacity=8,
        )
        try:
            futures = [
                worker.submit(
                    match_id="match-a",
                    video_pts=10.0,
                    kind="clock",
                    profile="source-a",
                    crop="44:59",
                ),
                worker.submit(
                    match_id="match-a",
                    video_pts=10.0,
                    kind="score",
                    profile="source-a",
                    crop="0-0",
                ),
                worker.submit(
                    match_id="match-b",
                    video_pts=28.5,
                    kind="clock",
                    profile="source-b",
                    crop="63:12",
                ),
                worker.submit(
                    match_id="match-b",
                    video_pts=28.5,
                    kind="score",
                    profile="source-b",
                    crop="2-1",
                ),
            ]
            results = [future.result(timeout=1) for future in futures]
        finally:
            worker.close(timeout=1)

        self.assertEqual(factory_calls, ["en"])
        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(results[0].match_id, "match-a")
        self.assertEqual(results[0].video_pts, 10.0)
        self.assertEqual(results[0].kind, "clock")
        self.assertEqual(results[0].profile, "source-a")
        self.assertEqual(results[0].texts, ("44:59",))
        self.assertTrue(all(result.batch_size == 4 for result in results))

    def test_reuses_one_engine_for_later_batches(self):
        factory_calls = 0
        engine = FakeEngine()

        def factory(_language):
            nonlocal factory_calls
            factory_calls += 1
            return engine

        worker = BatchOcrWorker(
            engine_factory=factory,
            max_batch_size=2,
            batch_wait_seconds=0,
            queue_capacity=4,
        )
        try:
            first = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile="source-a",
                crop="10:00",
            ).result(timeout=1)
            second = worker.submit(
                match_id="match-a",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            ).result(timeout=1)
        finally:
            worker.close(timeout=1)

        self.assertEqual(factory_calls, 1)
        self.assertEqual(len(engine.calls), 2)
        self.assertEqual(first.texts, ("10:00",))
        self.assertEqual(second.texts, ("10:01",))

    def test_queue_backpressure_is_structured(self):
        release_factory = threading.Event()

        def blocked_factory(_language):
            release_factory.wait(1)
            return FakeEngine()

        worker = BatchOcrWorker(
            engine_factory=blocked_factory,
            max_batch_size=1,
            queue_capacity=1,
        )
        try:
            first = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile="source-a",
                crop="10:00",
            )
            with self.assertRaises(WorkerError) as raised:
                worker.submit(
                    match_id="match-b",
                    video_pts=2,
                    kind="score",
                    profile="source-b",
                    crop="1-0",
                )
            self.assertEqual(raised.exception.kind, "ocr_queue_full")
            release_factory.set()
            self.assertEqual(first.result(timeout=1).texts, ("10:00",))
        finally:
            release_factory.set()
            worker.close(timeout=1)

    def test_batch_failure_isolated_with_request_diagnostics(self):
        class FailingEngine:
            def predict(self, _crops):
                raise RuntimeError("backend failed")

        worker = BatchOcrWorker(
            engine_factory=lambda _language: FailingEngine(),
            max_batch_size=2,
            batch_wait_seconds=0.1,
            queue_capacity=4,
        )
        try:
            clock = worker.submit(
                match_id="match-a",
                video_pts=42,
                kind="clock",
                profile="source-a",
                crop="bad-clock",
            )
            score = worker.submit(
                match_id="match-b",
                video_pts=99,
                kind="score",
                profile="source-b",
                crop="bad-score",
            )
            for future, match_id, kind in (
                (clock, "match-a", "clock"),
                (score, "match-b", "score"),
            ):
                with self.assertRaises(WorkerError) as raised:
                    future.result(timeout=1)
                self.assertEqual(raised.exception.kind, "ocr_inference_failed")
                self.assertEqual(raised.exception.diagnostics["match_id"], match_id)
                self.assertEqual(raised.exception.diagnostics["kind"], kind)
        finally:
            worker.close(timeout=1)

    def test_malformed_batch_result_does_not_kill_worker(self):
        calls = 0

        def recognizer(_engine, crops, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return [None for _crop in crops]
            return [([str(crop)], [0.9]) for crop in crops]

        worker = BatchOcrWorker(
            engine_factory=lambda _language: object(),
            batch_recognizer=recognizer,
            max_batch_size=1,
            batch_wait_seconds=0,
            queue_capacity=2,
        )
        try:
            malformed = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile="source-a",
                crop="bad",
            )
            with self.assertRaises(WorkerError) as raised:
                malformed.result(timeout=1)
            self.assertEqual(raised.exception.kind, "ocr_inference_failed")

            recovered = worker.submit(
                match_id="match-a",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            ).result(timeout=1)
            self.assertEqual(recovered.texts, ("10:01",))
            self.assertTrue(worker.is_alive)
        finally:
            worker.close(timeout=1)

    def test_accepts_profile_object_and_returns_profile_id(self):
        class Profile:
            profile_id = "source-profile"

        worker = BatchOcrWorker(
            engine_factory=lambda _language: FakeEngine(),
            max_batch_size=1,
            batch_wait_seconds=0,
            queue_capacity=1,
        )
        try:
            result = worker.submit(
                match_id="match-a",
                video_pts=1,
                kind="clock",
                profile=Profile(),
                crop="10:00",
            ).result(timeout=1)
        finally:
            worker.close(timeout=1)

        self.assertEqual(result.profile, "source-profile")
        self.assertEqual(result.profile_id, "source-profile")
        self.assertEqual(result.as_dict()["profile_id"], "source-profile")

    def test_close_cancels_queued_work_and_rejects_new_submissions(self):
        release_factory = threading.Event()
        worker = BatchOcrWorker(
            engine_factory=lambda _language: (
                release_factory.wait(1) or FakeEngine()
            ),
            max_batch_size=1,
            queue_capacity=2,
        )
        pending = worker.submit(
            match_id="match-a",
            video_pts=1,
            kind="clock",
            profile="source-a",
            crop="10:00",
        )

        self.assertFalse(
            worker.close(wait=False, cancel_pending=True)
        )
        with self.assertRaises(WorkerError) as cancelled:
            pending.result(timeout=1)
        self.assertEqual(cancelled.exception.kind, "ocr_worker_closed")
        with self.assertRaises(WorkerError) as closed:
            worker.submit(
                match_id="match-a",
                video_pts=2,
                kind="clock",
                profile="source-a",
                crop="10:01",
            )
        self.assertEqual(closed.exception.kind, "ocr_worker_closed")
        release_factory.set()
        self.assertTrue(worker.close(timeout=1))

    def test_engine_initialization_failure_is_terminal(self):
        def unavailable(_language):
            raise WorkerError("ocr_model_unavailable", "missing model")

        worker = BatchOcrWorker(engine_factory=unavailable)
        try:
            with self.assertRaises(WorkerError) as ready:
                worker.wait_until_ready(timeout=1)
            self.assertEqual(ready.exception.kind, "ocr_model_unavailable")
            with self.assertRaises(WorkerError) as submit:
                worker.submit(
                    match_id="match-a",
                    video_pts=1,
                    kind="clock",
                    profile="source-a",
                    crop="10:00",
                )
            self.assertEqual(submit.exception.kind, "ocr_model_unavailable")
        finally:
            worker.close(timeout=1)


class PersistentSocketTests(unittest.TestCase):
    @staticmethod
    def _wait_for_socket(socket_path: Path) -> None:
        deadline = time.monotonic() + 2.0
        while not socket_path.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        if not socket_path.exists():
            raise AssertionError("socket server did not start")

    @staticmethod
    def _exchange(socket_path: Path, request: dict) -> dict:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(3.0)
        connection.connect(str(socket_path))
        with connection, connection.makefile("rwb") as stream:
            stream.write(json.dumps(request).encode("utf-8") + b"\n")
            stream.flush()
            return json.loads(stream.readline().decode("utf-8"))

    def test_real_socket_batches_crops_from_concurrent_matches(self):
        engine = FakeEngine()
        factory_calls: list[str] = []
        rendezvous = threading.Barrier(2)

        def engine_factory(language):
            factory_calls.append(language)
            return engine

        def execute(request, *, batch_worker, request_timeout_seconds):
            rendezvous.wait(timeout=1)
            future = batch_worker.submit(
                match_id=request["match_id"],
                video_pts=request["video_pts"],
                kind=request["kind"],
                profile=request["profile"],
                crop=request["crop"],
            )
            result = future.result(timeout=request_timeout_seconds)
            return {"ok": True, "result": result.as_dict()}, 0

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            server = threading.Thread(
                target=serve_socket,
                args=(socket_path,),
                kwargs={
                    "engine_factory": engine_factory,
                    "request_executor": execute,
                },
            )
            server.start()
            self._wait_for_socket(socket_path)
            responses: list[dict] = []

            def send(request):
                responses.append(self._exchange(socket_path, request))

            clients = [
                threading.Thread(
                    target=send,
                    args=(
                        {
                            "match_id": match_id,
                            "video_pts": 10.0,
                            "kind": kind,
                            "profile": "source-a",
                            "crop": crop,
                            "_request_timeout_seconds": 2.0,
                        },
                    ),
                )
                for match_id, kind, crop in (
                    ("match-a", "clock", "44:59"),
                    ("match-b", "score", "1-0"),
                )
            ]
            for client in clients:
                client.start()
            for client in clients:
                client.join(timeout=2)
            self._exchange(socket_path, {"command": "shutdown"})
            server.join(timeout=2)

        self.assertFalse(server.is_alive())
        self.assertEqual(factory_calls, ["en"])
        self.assertEqual(len(responses), 2)
        self.assertTrue(all(response["ok"] for response in responses))
        self.assertEqual({response["result"]["batch_size"] for response in responses}, {2})
        self.assertEqual(len(engine.calls), 1)
        self.assertEqual(set(engine.calls[0]), {"44:59", "1-0"})

    def test_disconnected_client_does_not_stop_real_socket_server(self):
        abandoned_started = threading.Event()
        release_abandoned = threading.Event()

        def execute(request, **_kwargs):
            if request.get("id") == "abandoned":
                abandoned_started.set()
                release_abandoned.wait(1)
            return {"ok": True, "result": {"id": request.get("id")}}, 0

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            server = threading.Thread(
                target=serve_socket,
                args=(socket_path,),
                kwargs={
                    "engine_factory": lambda _language: FakeEngine(),
                    "request_executor": execute,
                },
            )
            server.start()
            self._wait_for_socket(socket_path)

            abandoned = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            abandoned.connect(str(socket_path))
            abandoned.sendall(b'{"id":"abandoned"}\n')
            abandoned.close()
            self.assertTrue(abandoned_started.wait(1))
            release_abandoned.set()
            time.sleep(0.05)

            response = self._exchange(socket_path, {"id": "healthy"})
            self.assertEqual(response, {"ok": True, "result": {"id": "healthy"}})
            self.assertTrue(server.is_alive())
            self._exchange(socket_path, {"command": "shutdown"})
            server.join(timeout=2)

        self.assertFalse(server.is_alive())

    def test_backend_timeout_stops_daemon_for_clean_restart(self):
        release_backend = threading.Event()

        def blocked_recognizer(_engine, crops, **_kwargs):
            release_backend.wait(2)
            return [([str(crop)], [0.9]) for crop in crops]

        def execute(request, *, batch_worker, request_timeout_seconds):
            try:
                _recognize_paths_shared(
                    batch_worker,
                    [Path("clock.png")],
                    kinds=["clock"],
                    match_id="match-a",
                    profile_id="source-a",
                    candidate_start_seconds=0,
                    sample_interval=1,
                    minimum_confidence=0,
                    deadline_monotonic=time.monotonic() + request_timeout_seconds,
                )
            except WorkerError as exc:
                return {"ok": False, "error": exc.as_dict()}, 2
            raise AssertionError("blocked backend unexpectedly completed")

        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            server = threading.Thread(
                target=serve_socket,
                args=(socket_path,),
                kwargs={
                    "engine_factory": lambda _language: object(),
                    "batch_recognizer": blocked_recognizer,
                    "request_executor": execute,
                },
            )
            server.start()
            self._wait_for_socket(socket_path)
            response = self._exchange(
                socket_path,
                {"id": "slow", "_request_timeout_seconds": 0.2},
            )
            release_backend.set()
            server.join(timeout=2)

        self.assertFalse(response["ok"])
        self.assertEqual(response["error"]["kind"], "inference_timeout")
        self.assertTrue(response["error"]["diagnostics"]["backend_unhealthy"])
        self.assertFalse(server.is_alive())

    def test_ffprobe_timeout_is_structured(self):
        with patch(
            "scoreboard_ocr_worker.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["ffprobe"], 0.1),
        ):
            with self.assertRaises(WorkerError) as raised:
                probe_video_dimensions(
                    Path("candidate.mp4"),
                    ffmpeg="ffmpeg",
                    timeout_seconds=0.1,
                )

        self.assertEqual(raised.exception.kind, "inference_timeout")
        self.assertEqual(raised.exception.diagnostics["stage"], "ffprobe")

    def test_ffmpeg_timeout_is_structured(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "scoreboard_ocr_worker.subprocess.run",
                side_effect=subprocess.TimeoutExpired(["ffmpeg"], 0.1),
            ):
                with self.assertRaises(WorkerError) as raised:
                    extract_scoreboard_frames(
                        Path("candidate.mp4"),
                        Path(directory),
                        ffmpeg="ffmpeg",
                        sample_interval_seconds=1,
                        roi_width_ratio=0.5,
                        roi_height_ratio=0.25,
                        timeout_seconds=0.1,
                    )

        self.assertEqual(raised.exception.kind, "inference_timeout")
        self.assertEqual(
            raised.exception.diagnostics["stage"],
            "frame_extraction",
        )

    def test_empty_readiness_probe_does_not_stop_server(self):
        writes: list[bytes] = []

        class FakeStream:
            def __init__(self, line: bytes) -> None:
                self.line = line

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def readline(self, _limit: int) -> bytes:
                return self.line

            def write(self, value: bytes) -> None:
                writes.append(value)

            def flush(self) -> None:
                pass

        class FakeConnection:
            def __init__(self, line: bytes) -> None:
                self.stream = FakeStream(line)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def settimeout(self, _timeout: float) -> None:
                pass

            def makefile(self, _mode: str) -> FakeStream:
                return self.stream

        class FakeServer:
            def __init__(self) -> None:
                self.connections = iter(
                    [
                        FakeConnection(b""),
                        FakeConnection(b'{"command":"shutdown"}\n'),
                    ]
                )

            def bind(self, _path: str) -> None:
                pass

            def listen(self, _backlog: int) -> None:
                pass

            def settimeout(self, _timeout: float) -> None:
                pass

            def accept(self):
                return next(self.connections), None

            def close(self) -> None:
                pass

        fake_server = FakeServer()
        with tempfile.TemporaryDirectory() as directory:
            socket_path = Path(directory) / "ocr.sock"
            with patch(
                "scoreboard_ocr_worker.socket.socket", return_value=fake_server
            ), patch("scoreboard_ocr_worker.os.chmod"):
                result = serve_socket(socket_path)

        self.assertEqual(result, 0)
        self.assertEqual(
            [json.loads(value.decode("utf-8")) for value in writes],
            [{"ok": True, "result": {"shutdown": True}}],
        )


if __name__ == "__main__":
    unittest.main()
