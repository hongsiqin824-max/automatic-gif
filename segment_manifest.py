"""Durable metadata for video segment generations.

The manifest keeps every FFmpeg generation on one continuous stream timeline.
It deliberately contains no pipeline policy: callers decide when a match is
finished and when old segment lists may be removed.
"""

from __future__ import annotations

import csv
import json
import math
import os
import stat
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any


SEGMENT_MANIFEST_VERSION = 1


class SegmentManifestError(RuntimeError):
    """Base class for errors that make a manifest unsafe to use."""


class SegmentManifestCorruptError(SegmentManifestError):
    """The manifest cannot be parsed or does not satisfy its schema."""


class SegmentManifestMismatchError(SegmentManifestError):
    """The manifest belongs to a different match or video source."""


class SegmentManifestVersionError(SegmentManifestError):
    """The manifest uses a schema version this runtime does not understand."""


@dataclass(frozen=True)
class SegmentManifestGeneration:
    list_path: Path
    stream_offset: float
    started_at_wall: float


@dataclass(frozen=True)
class SegmentGenerationHealth:
    """Read-only recovery diagnostics for one generation's segment list."""

    list_path: Path
    status: str
    listed_segment_count: int
    available_segment_count: int
    missing_media_paths: tuple[Path, ...] = ()
    available_start_stream_time: float | None = None
    available_end_stream_time: float | None = None


@dataclass(frozen=True)
class SegmentManifest:
    version: int
    match_id: str
    source: str
    timeline_origin_wall: float
    generations: tuple[SegmentManifestGeneration, ...] = ()
    # Populated only while loading. Recovery diagnostics are not written back.
    stale_list_paths: tuple[Path, ...] = field(default=(), compare=False, repr=False)
    generation_health: tuple[SegmentGenerationHealth, ...] = field(
        default=(), compare=False, repr=False
    )


def new_segment_manifest(
    match_id: str,
    source: str,
    timeline_origin_wall: float,
) -> SegmentManifest:
    """Create and validate a new, empty manifest."""
    manifest = SegmentManifest(
        version=SEGMENT_MANIFEST_VERSION,
        match_id=match_id,
        source=source,
        timeline_origin_wall=timeline_origin_wall,
    )
    _validate_manifest(manifest, Path("<new segment manifest>"))
    return manifest


def load_segment_manifest(
    path: Path,
    *,
    expected_match_id: str,
    expected_source: str | None,
    drop_stale: bool = True,
) -> SegmentManifest | None:
    """Load a manifest, returning ``None`` when it has not been created yet.

    Missing, empty, and fully-pruned segment lists are normal after buffer
    maintenance. With ``drop_stale=True`` they are omitted from ``generations``
    and reported in ``stale_list_paths``. ``generation_health`` describes each
    original entry, including partial pruning and its remaining stream-time
    coverage. Existing CSV files are parsed conservatively; malformed rows or
    invalid time ranges raise typed errors because silently accepting them can
    make a restored historical window appear usable when it has no safe video.
    """
    path = Path(path)
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise SegmentManifestError(f"cannot read segment manifest {path}: {exc}") from exc

    try:
        payload = json.loads(raw_text)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise SegmentManifestCorruptError(
            f"invalid JSON in segment manifest {path}: {exc}"
        ) from exc

    manifest = _manifest_from_payload(payload, path)
    if manifest.match_id != expected_match_id:
        raise SegmentManifestMismatchError(
            f"segment manifest {path} belongs to match {manifest.match_id!r}, "
            f"not {expected_match_id!r}"
        )
    if expected_source is not None and manifest.source != expected_source:
        raise SegmentManifestMismatchError(
            f"segment manifest {path} belongs to a different video source"
        )
    active: list[SegmentManifestGeneration] = []
    stale: list[Path] = []
    health: list[SegmentGenerationHealth] = []
    for generation in manifest.generations:
        generation_health = _inspect_generation(path, generation)
        health.append(generation_health)
        if generation_health.available_segment_count > 0:
            active.append(generation)
        else:
            stale.append(generation.list_path)
    return replace(
        manifest,
        generations=tuple(active) if drop_stale else manifest.generations,
        stale_list_paths=tuple(stale),
        generation_health=tuple(health),
    )


def save_segment_manifest(path: Path, manifest: SegmentManifest) -> None:
    """Atomically persist a validated manifest in UTF-8 JSON format."""
    path = Path(path)
    _validate_manifest(manifest, path)
    payload = {
        "version": manifest.version,
        "match_id": manifest.match_id,
        "source": manifest.source,
        "timeline_origin_wall": manifest.timeline_origin_wall,
        "generations": [
            {
                "list_path": str(generation.list_path),
                "stream_offset": generation.stream_offset,
                "started_at_wall": generation.started_at_wall,
            }
            for generation in manifest.generations
        ],
    }
    encoded = (json.dumps(payload, ensure_ascii=True, indent=2) + "\n").encode("utf-8")

    temporary_path: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            temporary.write(encoded)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
        _fsync_directory(path.parent)
    except OSError as exc:
        raise SegmentManifestError(f"cannot write segment manifest {path}: {exc}") from exc
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass


def upsert_segment_generation(
    manifest: SegmentManifest,
    *,
    list_path: Path,
    stream_offset: float,
    started_at_wall: float,
) -> SegmentManifest:
    """Append a generation or update the matching list path in place.

    Repeating an identical call returns the original object and never creates
    duplicate entries.
    """
    _validate_manifest(manifest, Path("<segment manifest>"))
    candidate = SegmentManifestGeneration(
        list_path=Path(list_path),
        stream_offset=stream_offset,
        started_at_wall=started_at_wall,
    )
    _validate_generation(candidate, Path("<segment generation>"), 0)

    generations = list(manifest.generations)
    for index, generation in enumerate(generations):
        if generation.list_path != candidate.list_path:
            continue
        if generation == candidate and not manifest.stale_list_paths:
            return manifest
        generations[index] = candidate
        return replace(
            manifest,
            generations=tuple(generations),
            stale_list_paths=(),
            generation_health=(),
        )
    generations.append(candidate)
    return replace(
        manifest,
        generations=tuple(generations),
        stale_list_paths=(),
        generation_health=(),
    )


def update_timeline_origin(
    manifest: SegmentManifest,
    timeline_origin_wall: float,
) -> SegmentManifest:
    """Return an idempotent update of the persistent timeline origin."""
    _validate_manifest(manifest, Path("<segment manifest>"))
    _require_finite_number(
        timeline_origin_wall,
        Path("<segment manifest>"),
        "timeline_origin_wall",
        minimum=0.0,
    )
    if manifest.timeline_origin_wall == timeline_origin_wall:
        return manifest
    return replace(
        manifest,
        timeline_origin_wall=float(timeline_origin_wall),
        stale_list_paths=(),
        generation_health=(),
    )


def update_manifest_source(
    manifest: SegmentManifest,
    source: str,
) -> SegmentManifest:
    """Move a same-match manifest to a replacement live resource."""
    _require_nonempty_string(source, Path("<segment manifest>"), "source")
    if manifest.source == source:
        return manifest
    return replace(
        manifest,
        source=source,
        stale_list_paths=(),
        generation_health=(),
    )


def resolve_generation_path(manifest_path: Path, list_path: Path) -> Path:
    """Resolve a generation path relative to its manifest when necessary."""
    list_path = Path(list_path)
    if list_path.is_absolute():
        return list_path
    return Path(manifest_path).parent / list_path


def _inspect_generation(
    manifest_path: Path,
    generation: SegmentManifestGeneration,
) -> SegmentGenerationHealth:
    list_path = resolve_generation_path(manifest_path, generation.list_path)
    try:
        list_mode = list_path.stat().st_mode
    except FileNotFoundError:
        return SegmentGenerationHealth(
            list_path=generation.list_path,
            status="missing_list",
            listed_segment_count=0,
            available_segment_count=0,
        )
    except OSError as exc:
        raise SegmentManifestError(f"cannot inspect segment list {list_path}: {exc}") from exc
    if not stat.S_ISREG(list_mode):
        raise SegmentManifestCorruptError(
            f"segment list {list_path} is not a regular file"
        )

    rows = _read_segment_rows(list_path, generation.stream_offset)
    if not rows:
        return SegmentGenerationHealth(
            list_path=generation.list_path,
            status="empty_list",
            listed_segment_count=0,
            available_segment_count=0,
        )

    available_rows: list[tuple[Path, float, float]] = []
    missing_media_paths: list[Path] = []
    media_root = Path(manifest_path).parent
    for media_path, start, end in rows:
        resolved_media_path = media_path if media_path.is_absolute() else media_root / media_path
        try:
            media_mode = resolved_media_path.stat().st_mode
        except FileNotFoundError:
            media_mode = None
        except OSError as exc:
            raise SegmentManifestError(
                f"cannot inspect segment media {resolved_media_path}: {exc}"
            ) from exc
        if media_mode is not None and stat.S_ISREG(media_mode):
            available_rows.append((resolved_media_path, start, end))
        else:
            missing_media_paths.append(resolved_media_path)

    if not available_rows:
        status = "fully_pruned"
        start_stream_time = None
        end_stream_time = None
    else:
        status = "partially_pruned" if missing_media_paths else "healthy"
        start_stream_time = min(row[1] for row in available_rows)
        end_stream_time = max(row[2] for row in available_rows)

    return SegmentGenerationHealth(
        list_path=generation.list_path,
        status=status,
        listed_segment_count=len(rows),
        available_segment_count=len(available_rows),
        missing_media_paths=tuple(missing_media_paths),
        available_start_stream_time=start_stream_time,
        available_end_stream_time=end_stream_time,
    )


def _read_segment_rows(
    list_path: Path,
    stream_offset: float,
) -> list[tuple[Path, float, float]]:
    rows: list[tuple[Path, float, float]] = []
    seen_media_paths: set[Path] = set()
    previous_start: float | None = None
    previous_end: float | None = None
    try:
        with list_path.open(newline="", encoding="utf-8") as handle:
            for row_number, row in enumerate(csv.reader(handle, strict=True), start=1):
                if not row or all(not value.strip() for value in row):
                    continue
                if len(row) < 3:
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} must contain "
                        "path, start, and end"
                    )
                if not row[0].strip():
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} has an empty media path"
                    )
                media_path = Path(row[0])
                if media_path in seen_media_paths:
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} repeats media path "
                        f"{str(media_path)!r}"
                    )
                try:
                    raw_start = float(row[1])
                    raw_end = float(row[2])
                except ValueError as exc:
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} has invalid times"
                    ) from exc
                if (
                    not math.isfinite(raw_start)
                    or not math.isfinite(raw_end)
                    or raw_start < 0.0
                    or raw_end <= raw_start
                ):
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} has an invalid "
                        f"time range {raw_start!r}..{raw_end!r}"
                    )
                if (
                    previous_start is not None
                    and previous_end is not None
                    and (raw_start < previous_start or raw_end < previous_end)
                ):
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} moves backward "
                        "on its timeline"
                    )
                start = stream_offset + raw_start
                end = stream_offset + raw_end
                if not math.isfinite(start) or not math.isfinite(end):
                    raise SegmentManifestCorruptError(
                        f"segment list {list_path} row {row_number} overflows its "
                        "manifest stream offset"
                    )
                rows.append((media_path, start, end))
                seen_media_paths.add(media_path)
                previous_start = raw_start
                previous_end = raw_end
    except SegmentManifestCorruptError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise SegmentManifestError(f"cannot read segment list {list_path}: {exc}") from exc
    return rows


def _manifest_from_payload(payload: Any, path: Path) -> SegmentManifest:
    if not isinstance(payload, dict):
        raise SegmentManifestCorruptError(
            f"segment manifest {path} must contain a JSON object"
        )
    version = payload.get("version")
    if version != SEGMENT_MANIFEST_VERSION:
        raise SegmentManifestVersionError(
            f"unsupported segment manifest version {version!r} in {path}; "
            f"expected {SEGMENT_MANIFEST_VERSION}"
        )
    try:
        raw_generations = payload["generations"]
        if not isinstance(raw_generations, list):
            raise TypeError("generations must be a list")
        generations = tuple(
            SegmentManifestGeneration(
                list_path=Path(item["list_path"]),
                stream_offset=item["stream_offset"],
                started_at_wall=item["started_at_wall"],
            )
            for item in raw_generations
        )
        manifest = SegmentManifest(
            version=version,
            match_id=payload["match_id"],
            source=payload["source"],
            timeline_origin_wall=payload["timeline_origin_wall"],
            generations=generations,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SegmentManifestCorruptError(
            f"invalid segment manifest schema in {path}: {exc}"
        ) from exc
    _validate_manifest(manifest, path)
    return manifest


def _validate_manifest(manifest: SegmentManifest, path: Path) -> None:
    if manifest.version != SEGMENT_MANIFEST_VERSION:
        raise SegmentManifestVersionError(
            f"unsupported segment manifest version {manifest.version!r} in {path}; "
            f"expected {SEGMENT_MANIFEST_VERSION}"
        )
    _require_nonempty_string(manifest.match_id, path, "match_id")
    _require_nonempty_string(manifest.source, path, "source")
    _require_finite_number(
        manifest.timeline_origin_wall,
        path,
        "timeline_origin_wall",
        minimum=0.0,
    )
    seen_paths: set[Path] = set()
    for index, generation in enumerate(manifest.generations):
        _validate_generation(generation, path, index)
        if generation.list_path in seen_paths:
            raise SegmentManifestCorruptError(
                f"duplicate generation list_path {str(generation.list_path)!r} in {path}"
            )
        seen_paths.add(generation.list_path)


def _validate_generation(
    generation: SegmentManifestGeneration,
    path: Path,
    index: int,
) -> None:
    if not isinstance(generation.list_path, Path) or not str(generation.list_path):
        raise SegmentManifestCorruptError(
            f"generations[{index}].list_path in {path} must be a non-empty path"
        )
    _require_finite_number(
        generation.stream_offset,
        path,
        f"generations[{index}].stream_offset",
        minimum=0.0,
    )
    _require_finite_number(
        generation.started_at_wall,
        path,
        f"generations[{index}].started_at_wall",
        minimum=0.0,
    )


def _require_nonempty_string(value: Any, path: Path, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise SegmentManifestCorruptError(
            f"{field_name} in {path} must be a non-empty string"
        )


def _require_finite_number(
    value: Any,
    path: Path,
    field_name: str,
    *,
    minimum: float,
) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SegmentManifestCorruptError(
            f"{field_name} in {path} must be a number"
        )
    if not math.isfinite(value) or value < minimum:
        raise SegmentManifestCorruptError(
            f"{field_name} in {path} must be finite and >= {minimum:g}"
        )


def _fsync_directory(directory: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(directory, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)
