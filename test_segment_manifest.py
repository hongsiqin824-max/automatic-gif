from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from segment_manifest import (
    SEGMENT_MANIFEST_VERSION,
    SegmentManifestCorruptError,
    SegmentManifestError,
    SegmentManifestMismatchError,
    SegmentManifestVersionError,
    load_segment_manifest,
    new_segment_manifest,
    save_segment_manifest,
    update_manifest_source,
    update_timeline_origin,
    upsert_segment_generation,
)


class SegmentManifestTests(unittest.TestCase):
    def test_round_trip_preserves_timeline_and_generations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            list_path = root / "segments_run_g000.csv"
            list_path.write_text("segment.ts,0,2\n", encoding="utf-8")
            (root / "segment.ts").touch()
            manifest_path = root / "segment_manifest.json"
            manifest = new_segment_manifest("match-1", "rtmp://source/live", 1000.0)
            manifest = upsert_segment_generation(
                manifest,
                list_path=list_path,
                stream_offset=12.5,
                started_at_wall=1012.5,
            )

            save_segment_manifest(manifest_path, manifest)
            loaded = load_segment_manifest(
                manifest_path,
                expected_match_id="match-1",
                expected_source="rtmp://source/live",
            )

            self.assertEqual(loaded, manifest)
            self.assertEqual(loaded.version, SEGMENT_MANIFEST_VERSION)
            self.assertEqual(loaded.generations[0].stream_offset, 12.5)
            self.assertEqual(loaded.stale_list_paths, ())

    def test_missing_manifest_returns_none(self):
        with tempfile.TemporaryDirectory() as directory:
            loaded = load_segment_manifest(
                Path(directory) / "missing.json",
                expected_match_id="match-1",
                expected_source="source-1",
            )
            self.assertIsNone(loaded)

    def test_load_drops_and_reports_stale_generation_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "active.csv"
            active.write_text("active.ts,0,2\n", encoding="utf-8")
            (root / "active.ts").touch()
            manifest_path = root / "segment_manifest.json"
            manifest = new_segment_manifest("match-1", "source-1", 1000.0)
            manifest = upsert_segment_generation(
                manifest,
                list_path=Path("active.csv"),
                stream_offset=0.0,
                started_at_wall=1000.0,
            )
            manifest = upsert_segment_generation(
                manifest,
                list_path=Path("already-pruned.csv"),
                stream_offset=10.0,
                started_at_wall=1010.0,
            )
            save_segment_manifest(manifest_path, manifest)

            loaded = load_segment_manifest(
                manifest_path,
                expected_match_id="match-1",
                expected_source="source-1",
            )

            self.assertEqual(
                [generation.list_path for generation in loaded.generations],
                [Path("active.csv")],
            )
            self.assertEqual(loaded.stale_list_paths, (Path("already-pruned.csv"),))

            unfiltered = load_segment_manifest(
                manifest_path,
                expected_match_id="match-1",
                expected_source="source-1",
                drop_stale=False,
            )
            self.assertEqual(len(unfiltered.generations), 2)
            self.assertEqual(
                [item.status for item in unfiltered.generation_health],
                ["healthy", "missing_list"],
            )

    def test_empty_and_fully_pruned_lists_are_compatible_stale_entries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "empty.csv").touch()
            (root / "pruned.csv").write_text(
                "old_000.ts,0,2\nold_001.ts,2,4\n",
                encoding="utf-8",
            )
            path = root / "manifest.json"
            manifest = new_segment_manifest("match-1", "source-1", 1000.0)
            manifest = upsert_segment_generation(
                manifest,
                list_path=Path("empty.csv"),
                stream_offset=0.0,
                started_at_wall=1000.0,
            )
            manifest = upsert_segment_generation(
                manifest,
                list_path=Path("pruned.csv"),
                stream_offset=10.0,
                started_at_wall=1010.0,
            )
            save_segment_manifest(path, manifest)

            loaded = load_segment_manifest(
                path,
                expected_match_id="match-1",
                expected_source="source-1",
            )

            self.assertEqual(loaded.generations, ())
            self.assertEqual(
                loaded.stale_list_paths,
                (Path("empty.csv"), Path("pruned.csv")),
            )
            self.assertEqual(
                [item.status for item in loaded.generation_health],
                ["empty_list", "fully_pruned"],
            )

    def test_partial_pruning_keeps_generation_and_reports_remaining_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "manifest.json"
            (root / "segments.csv").write_text(
                "old.ts,0,2\nlive.ts,2,4\n",
                encoding="utf-8",
            )
            (root / "live.ts").touch()
            manifest = upsert_segment_generation(
                new_segment_manifest("match-1", "source-1", 1000.0),
                list_path=Path("segments.csv"),
                stream_offset=12.5,
                started_at_wall=1012.5,
            )
            save_segment_manifest(path, manifest)

            loaded = load_segment_manifest(
                path,
                expected_match_id="match-1",
                expected_source="source-1",
            )

            self.assertEqual(len(loaded.generations), 1)
            self.assertEqual(loaded.stale_list_paths, ())
            health = loaded.generation_health[0]
            self.assertEqual(health.status, "partially_pruned")
            self.assertEqual(health.listed_segment_count, 2)
            self.assertEqual(health.available_segment_count, 1)
            self.assertEqual(health.missing_media_paths, (root / "old.ts",))
            self.assertEqual(health.available_start_stream_time, 14.5)
            self.assertEqual(health.available_end_stream_time, 16.5)

    def test_malformed_or_backward_segment_list_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "manifest.json"
            list_path = root / "segments.csv"
            manifest = upsert_segment_generation(
                new_segment_manifest("match-1", "source-1", 1000.0),
                list_path=Path("segments.csv"),
                stream_offset=0.0,
                started_at_wall=1000.0,
            )
            save_segment_manifest(path, manifest)

            for contents, message in (
                ("broken-row\n", "must contain"),
                (",0,2\n", "empty media path"),
                ("segment.ts,nope,2\n", "invalid times"),
                ("segment.ts,2,1\n", "invalid time range"),
                ("one.ts,2,4\ntwo.ts,1,3\n", "moves backward"),
                ("one.ts,0,4\ntwo.ts,2,3\n", "moves backward"),
            ):
                with self.subTest(contents=contents):
                    list_path.write_text(contents, encoding="utf-8")
                    with self.assertRaisesRegex(SegmentManifestCorruptError, message):
                        load_segment_manifest(
                            path,
                            expected_match_id="match-1",
                            expected_source="source-1",
                        )

    def test_upsert_is_idempotent_and_updates_in_place(self):
        manifest = new_segment_manifest("match-1", "source-1", 1000.0)
        first = upsert_segment_generation(
            manifest,
            list_path=Path("segments.csv"),
            stream_offset=5.0,
            started_at_wall=1005.0,
        )
        repeated = upsert_segment_generation(
            first,
            list_path=Path("segments.csv"),
            stream_offset=5.0,
            started_at_wall=1005.0,
        )
        updated = upsert_segment_generation(
            repeated,
            list_path=Path("segments.csv"),
            stream_offset=8.0,
            started_at_wall=1008.0,
        )

        self.assertIs(repeated, first)
        self.assertEqual(len(updated.generations), 1)
        self.assertEqual(updated.generations[0].stream_offset, 8.0)
        self.assertIs(update_timeline_origin(updated, 1000.0), updated)
        self.assertEqual(update_timeline_origin(updated, 900.0).timeline_origin_wall, 900.0)

    def test_corrupt_json_and_schema_raise_explicit_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text("{broken", encoding="utf-8")
            with self.assertRaisesRegex(SegmentManifestCorruptError, "invalid JSON"):
                load_segment_manifest(
                    path,
                    expected_match_id="match-1",
                    expected_source="source-1",
                )

            path.write_text(
                json.dumps(
                    {
                        "version": SEGMENT_MANIFEST_VERSION,
                        "match_id": "match-1",
                        "source": "source-1",
                        "timeline_origin_wall": 1000.0,
                        "generations": [{"list_path": "segments.csv"}],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(SegmentManifestCorruptError, "schema"):
                load_segment_manifest(
                    path,
                    expected_match_id="match-1",
                    expected_source="source-1",
                )

    def test_identity_mismatch_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            save_segment_manifest(
                path,
                new_segment_manifest("match-1", "source-1", 1000.0),
            )
            with self.assertRaisesRegex(SegmentManifestMismatchError, "match"):
                load_segment_manifest(
                    path,
                    expected_match_id="match-2",
                    expected_source="source-1",
                )
            with self.assertRaisesRegex(SegmentManifestMismatchError, "video source"):
                load_segment_manifest(
                    path,
                    expected_match_id="match-1",
                    expected_source="source-2",
                )
            loaded_for_source_switch = load_segment_manifest(
                path,
                expected_match_id="match-1",
                expected_source=None,
            )
            switched = update_manifest_source(
                loaded_for_source_switch,
                "source-2",
            )
            self.assertEqual(switched.source, "source-2")
            self.assertEqual(switched.match_id, "match-1")

    def test_unknown_schema_version_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "manifest.json"
            path.write_text(
                json.dumps(
                    {
                        "version": 999,
                        "match_id": "match-1",
                        "source": "source-1",
                        "timeline_origin_wall": 1000.0,
                        "generations": [],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(SegmentManifestVersionError, "version 999"):
                load_segment_manifest(
                    path,
                    expected_match_id="match-1",
                    expected_source="source-1",
                )

    def test_failed_atomic_replace_preserves_old_file_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "manifest.json"
            path.write_text("old-content\n", encoding="utf-8")
            manifest = new_segment_manifest("match-1", "source-1", 1000.0)

            with patch("segment_manifest.os.replace", side_effect=OSError("disk error")):
                with self.assertRaisesRegex(SegmentManifestError, "cannot write"):
                    save_segment_manifest(path, manifest)

            self.assertEqual(path.read_text(encoding="utf-8"), "old-content\n")
            self.assertEqual(list(root.glob(".manifest.json.*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
