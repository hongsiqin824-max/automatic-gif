import json
import tempfile
import unittest
from pathlib import Path

import source_backup_probe as probe_mod
from source_backup_probe import (
    FlowTracker,
    SourceProbe,
    is_stable,
    match_verdict,
    needs_backup,
    net_bytes,
    parse_out_time,
    preview_source_of,
)

WINDOW = probe_mod.MATCH_WINDOW_SECONDS


def run_tracker(pattern, step=2.0, start=0.0, in_window=True):
    """Feed ``pattern`` (True = new picture this tick) and return the tracker."""
    tracker = FlowTracker()
    last_picture = None
    t = start
    for flowing in pattern:
        if flowing:
            last_picture = t
        tracker.sample(t, last_picture, in_window)
        t += step
    return tracker


class FlowTrackerTest(unittest.TestCase):
    def test_steady_picture_has_no_break(self):
        tracker = run_tracker([True] * 100)
        self.assertEqual(tracker.drops, 0)
        self.assertEqual(tracker.nopic, 0)
        self.assertAlmostEqual(tracker.observed, 198)

    def test_a_stall_longer_than_the_limit_is_one_break(self):
        tracker = run_tracker([True] * 10 + [False] * 15 + [True] * 10)
        self.assertEqual(tracker.drops, 1)
        # Counted from the moment the stall limit (10s) is reached.
        self.assertGreater(tracker.nopic, 15)
        self.assertLess(tracker.nopic, 31)

    def test_a_short_gap_is_not_a_break(self):
        tracker = run_tracker([True] * 10 + [False] * 3 + [True] * 10)
        self.assertEqual(tracker.drops, 0)
        self.assertEqual(tracker.nopic, 0)

    def test_a_source_that_never_pushes_counts_as_no_picture_but_no_break(self):
        tracker = run_tracker([False] * 50)
        self.assertEqual(tracker.drops, 0)
        self.assertAlmostEqual(tracker.nopic, 98)

    def test_outside_the_window_nothing_counts(self):
        tracker = run_tracker([True] * 10 + [False] * 20, in_window=False)
        self.assertEqual((tracker.drops, tracker.nopic, tracker.observed), (0, 0, 0))


def make_tracker(observed, nopic=0.0, drops=0):
    tracker = FlowTracker()
    tracker.observed, tracker.nopic, tracker.drops = observed, nopic, drops
    return tracker


class VerdictTest(unittest.TestCase):
    def test_too_short_to_tell(self):
        self.assertIsNone(is_stable(make_tracker(10 * 60)))

    def test_past_the_match_limits_is_unstable_even_when_short(self):
        self.assertFalse(is_stable(make_tracker(6 * 60, nopic=300)))
        self.assertFalse(is_stable(make_tracker(6 * 60, drops=6)))

    def test_whole_window_limits(self):
        self.assertTrue(is_stable(make_tracker(WINDOW, nopic=299, drops=5)))
        self.assertFalse(is_stable(make_tracker(WINDOW, nopic=300, drops=0)))
        self.assertFalse(is_stable(make_tracker(WINDOW, nopic=0, drops=6)))

    def test_limits_scale_with_the_measured_part(self):
        half = WINDOW / 2
        self.assertTrue(is_stable(make_tracker(half, nopic=140, drops=2)))
        self.assertFalse(is_stable(make_tracker(half, nopic=160, drops=0)))
        self.assertFalse(is_stable(make_tracker(half, nopic=0, drops=3)))

    def test_trigger(self):
        self.assertFalse(needs_backup(make_tracker(600, nopic=60, drops=2)))
        self.assertTrue(needs_backup(make_tracker(600, drops=3)))
        self.assertTrue(needs_backup(make_tracker(600, nopic=120)))

    def test_match_verdict(self):
        self.assertEqual(match_verdict([True], [], 2), "可行")
        self.assertEqual(match_verdict([False], [True], 0), "可行")
        self.assertEqual(match_verdict([False], [False], 0), "不可行")
        self.assertEqual(match_verdict([False], [], 0), "不可行")
        self.assertEqual(match_verdict([False], [False], 1), "待定")
        self.assertEqual(match_verdict([False], [None], 0), "待定")
        self.assertEqual(match_verdict([None], [], 0), "待定")


class ParsingTest(unittest.TestCase):
    def test_progress_lines(self):
        self.assertEqual(parse_out_time("out_time_us=1500000\n"), 1500000)
        self.assertEqual(parse_out_time("out_time_ms=1500000"), 1500000)
        self.assertIsNone(parse_out_time("out_time_us=N/A"))
        self.assertIsNone(parse_out_time("total_size=100"))

    def test_net_bytes_skips_loopback(self):
        text = (
            "Inter-|   Receive                                                |  Transmit\n"
            " face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets\n"
            "    lo: 999 1 0 0 0 0 0 0 999 1 0 0 0 0 0 0\n"
            "  eth0: 1000 10 0 0 0 0 0 0 2000 20 0 0 0 0 0 0\n"
        )
        self.assertEqual(net_bytes(text), (1000, 2000))

    def test_preview_source_of(self):
        root = Path("/opt/gif-monitor/tmp/preview")
        argv = ["/usr/bin/ffmpeg", "-nostdin", "-i", "rtmp://h/live/sd-abc", "-f", "hls",
                "-hls_segment_filename", "/opt/gif-monitor/tmp/preview/54632802/g1/seg_%05d.ts",
                "/opt/gif-monitor/tmp/preview/54632802/g1/index.m3u8"]
        self.assertEqual(preview_source_of(argv, root), ("54632802", "rtmp://h/live/sd-abc"))
        self.assertIsNone(preview_source_of(["/usr/bin/ffmpeg", "-i", "x", "out.mp4"], root))
        self.assertIsNone(preview_source_of(["python3", "monitor_server.py"], root))


class FakePull:
    def __init__(self, url, clock):
        self.url = url
        self.clock = clock
        self.flowing = True
        self.started = False
        self.stopped = False
        self._last = None

    @property
    def last_picture_at(self):
        if self.flowing:
            self._last = self.clock()
        return self._last

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class SourceProbeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / "preview"
        self.results_path = Path(self.tmp.name) / "results.jsonl"
        self.now = 1_000_000.0
        self.kickoff = self.now + 60
        self.pulls = []
        self.candidates = [
            {"url": "rtmp://h/live/pt-main", "kind": "pt"},
            {"url": "rtmp://h/live/sd-b1", "kind": "sd"},
            {"url": "rtmp://h/live/sd-b2", "kind": "sd"},
        ]
        self.collect_calls = 0
        self.processes = {"54000001": "rtmp://h/live/pt-main"}
        self.primary_flowing = True
        self.preview_open = True
        self.probe = SourceProbe(
            preview_root=self.root,
            results_path=self.results_path,
            catalog_playing=lambda: [{"match_id": "54000001", "competition_name": "英冠",
                                      "team_A_name": "A", "team_B_name": "B"}],
            collect_sources=self._collect,
            kickoff_of=lambda item: self.kickoff,
            kind_of=lambda url: url.rsplit("/", 1)[-1].split("-", 1)[0],
            make_pull=self._make_pull,
            max_backups=4,
            clock=lambda: self.now,
            scan_processes=lambda: dict(self.processes),
            read_net_dev=lambda: "h\nh\n  eth0: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _collect(self, match_id):
        self.collect_calls += 1
        return list(self.candidates)

    def _make_pull(self, url):
        pull = FakePull(url, lambda: self.now)
        self.pulls.append(pull)
        return pull

    def _playlist(self):
        return self.root / "54000001" / "g1" / "index.m3u8"

    def run_for(self, seconds, step=2.0):
        end = self.now + seconds
        while self.now < end:
            playlist = self._playlist()
            if self.preview_open:
                playlist.parent.mkdir(parents=True, exist_ok=True)
                if self.primary_flowing or not playlist.exists():
                    playlist.write_text("#EXTM3U\n")
                    import os
                    os.utime(playlist, (self.now, self.now))
            elif playlist.parent.parent.exists():
                import shutil
                shutil.rmtree(playlist.parent.parent)
            self.probe.tick()
            self.now += step

    def break_primary(self, times, gap=30):
        for _ in range(times):
            self.primary_flowing = False
            self.run_for(gap)
            self.primary_flowing = True
            self.run_for(60)

    def results(self):
        return [json.loads(line) for line in self.results_path.read_text().splitlines()]

    def test_stable_primary_never_pulls_a_backup(self):
        self.run_for(WINDOW + 200, step=10)
        self.assertEqual(self.pulls, [])
        self.assertEqual(self.collect_calls, 0)
        [record] = self.results()
        self.assertEqual(record["verdict"], "可行")
        self.assertEqual(record["primary"][0]["kind"], "pt")
        self.assertEqual(record["primary"][0]["drops"], 0)

    def test_unstable_primary_pulls_the_next_source_and_the_match_is_feasible(self):
        self.run_for(120)
        self.break_primary(2)
        self.assertEqual(self.pulls, [])
        self.break_primary(1)
        self.assertEqual([p.url for p in self.pulls], ["rtmp://h/live/sd-b1"])
        self.assertTrue(self.pulls[0].started)
        self.break_primary(4)
        self.run_for(WINDOW, step=10)
        self.assertTrue(self.pulls[0].stopped)
        [record] = self.results()
        self.assertEqual(record["verdict"], "可行")
        self.assertFalse(record["primary"][0]["stable"])
        self.assertTrue(record["backups"][0]["stable"])
        self.assertEqual(record["source_count"], 3)
        self.assertNotIn("rtmp://", json.dumps(record))

    def test_every_source_unstable_is_not_feasible(self):
        self.candidates = self.candidates[:2]
        self.run_for(120)
        self.break_primary(3)
        self.pulls[0].flowing = False
        self.run_for(400)
        self.assertTrue(self.pulls[0].stopped)  # past 5 minutes without picture
        self.assertEqual(len(self.pulls), 1)  # nothing left to try
        self.primary_flowing = False
        self.run_for(WINDOW, step=10)
        [record] = self.results()
        self.assertEqual(record["verdict"], "不可行")
        self.assertEqual(record["untested_sources"], 0)

    def test_cap_leaves_the_match_undecided(self):
        self.probe.max_backups = 0
        self.run_for(120)
        self.break_primary(7)
        self.assertEqual(self.pulls, [])
        self.run_for(WINDOW, step=10)
        [record] = self.results()
        self.assertEqual(record["verdict"], "待定")
        self.assertEqual(record["untested_sources"], 2)

    def test_closing_the_preview_stops_the_backup(self):
        self.run_for(120)
        self.break_primary(3)
        self.assertEqual(len(self.pulls), 1)
        self.preview_open = False
        self.run_for(probe_mod.PREVIEW_GONE_SECONDS + 10)
        self.assertTrue(self.pulls[0].stopped)

    def test_operator_switching_to_the_backup_stops_the_extra_pull(self):
        self.run_for(120)
        self.break_primary(3)
        self.processes["54000001"] = "rtmp://h/live/sd-b1"
        self.run_for(10)
        self.assertTrue(self.pulls[0].stopped)
        self.run_for(WINDOW, step=10)
        [record] = self.results()
        self.assertEqual([p["kind"] for p in record["primary"]], ["pt", "sd"])
        self.assertEqual(record["verdict"], "可行")

    def test_a_preview_of_an_unknown_match_is_ignored(self):
        self.probe.catalog_playing = lambda: []
        self.run_for(WINDOW + 200, step=10)
        self.assertFalse(self.results_path.exists())


if __name__ == "__main__":
    unittest.main()
