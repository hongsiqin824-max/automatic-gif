"""Bounded, conservative cleanup for one match output directory.

The live pipeline can recover an interrupted match from its buffer, so cleanup
is split into two phases: cheap history rotation may happen while a worker is
running, while media deletion is only performed after ingest and all encoders
have stopped.  Every destructive operation is constrained to ``output_dir``
and returns a structured summary instead of making cleanup failure fatal.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable


VISION_CANDIDATE_SUFFIXES = frozenset(
    {".mp4", ".json", ".png", ".jpg", ".jpeg", ".webp", ".bmp"}
)


@dataclass(frozen=True)
class DiskLifecyclePolicy:
    """Retention limits for one match directory.

    ``post_match_buffer_seconds`` is measured using segment file mtime.  Zero
    means that no unprotected TS is retained after a successful match.
    """

    keep_ingest_logs: int = 8
    keep_run_reports: int = 20
    event_log_max_bytes: int = 20_000_000
    event_log_archives: int = 3
    post_match_buffer_seconds: float = 0.0
    vision_candidate_retention_seconds: float = 24 * 60 * 60
    final_gif_retention_seconds: float = 24 * 60 * 60

    def __post_init__(self) -> None:
        if self.keep_ingest_logs < 1:
            raise ValueError("keep_ingest_logs must be at least 1")
        if self.keep_run_reports < 1:
            raise ValueError("keep_run_reports must be at least 1")
        if self.event_log_max_bytes < 1:
            raise ValueError("event_log_max_bytes must be positive")
        if self.event_log_archives < 0:
            raise ValueError("event_log_archives cannot be negative")
        if self.post_match_buffer_seconds < 0:
            raise ValueError("post_match_buffer_seconds cannot be negative")
        if self.vision_candidate_retention_seconds <= 0:
            raise ValueError("vision_candidate_retention_seconds must be positive")
        if self.final_gif_retention_seconds <= 0:
            raise ValueError("final_gif_retention_seconds must be positive")


@dataclass
class CleanupSummary:
    """Serializable outcome of a cleanup pass."""

    phase: str
    status: str = "completed"
    deleted_files: int = 0
    deleted_bytes: int = 0
    rotated_files: int = 0
    retained_files: int = 0
    skipped_files: int = 0
    errors: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)

    def add_error(self, path: Path, error: BaseException) -> None:
        self.errors.append(f"{path}: {error}")
        self.status = "completed_with_warnings"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class DiskLifecycleManager:
    """Perform bounded cleanup for a single match output directory."""

    def __init__(self, output_dir: Path, policy: DiskLifecyclePolicy | None = None) -> None:
        self.output_dir = Path(output_dir).resolve()
        self.policy = policy or DiskLifecyclePolicy()

    def policy_dict(self) -> dict[str, object]:
        return asdict(self.policy)

    def prune_ingest_logs(self) -> CleanupSummary:
        """Keep only the newest FFmpeg logs while a worker is running."""
        summary = CleanupSummary(phase="active_history")
        self._prune_numbered_files(
            self.output_dir,
            patterns=("ingest_ffmpeg_*.log", "ingest_ffmpeg.log"),
            keep=self.policy.keep_ingest_logs,
            summary=summary,
            label="ingest_logs",
        )
        self._cleanup_vision_candidates(summary)
        return summary

    def cleanup_finished_match(
        self,
        *,
        buffer_dir: Path,
        manifest_path: Path,
        event_log_path: Path,
        state_db_path: Path,
        protected_paths: Iterable[str | Path] = (),
    ) -> CleanupSummary:
        """Reclaim terminal-match media and rotate bounded operational history.

        The caller must invoke this only after the ingest child and all media
        consumers have stopped.  Active lease paths are still honored as a
        final defense against accidental deletion.
        """
        summary = CleanupSummary(phase="finished_match")
        buffer_root = self._safe_root(buffer_dir, self.output_dir, summary)
        manifest = self._safe_path(manifest_path, self.output_dir, summary)
        protected = self._normalize_protected_paths(protected_paths)

        if buffer_root is not None:
            with self._segment_lease_guard(state_db_path, summary) as active_leases:
                if active_leases is None:
                    media_cleanup_allowed = False
                elif active_leases:
                    protected.update(active_leases)
                    media_cleanup_allowed = False
                    summary.actions.append("media_cleanup_skipped_after_active_lease")
                else:
                    media_cleanup_allowed = True
                if media_cleanup_allowed:
                    clear_manifest_first = (
                        self.policy.post_match_buffer_seconds == 0 and not protected
                    )
                    if clear_manifest_first and not self._clear_manifest(manifest, summary):
                        media_cleanup_allowed = False
                        summary.actions.append(
                            "media_cleanup_skipped_after_manifest_write_failure"
                        )
                    if media_cleanup_allowed:
                        retained_media = self._cleanup_ts_files(
                            buffer_root,
                            protected=protected,
                            summary=summary,
                        )
                        retained_lists = self._cleanup_segment_lists(
                            buffer_root,
                            retained_media=retained_media,
                            protected=protected,
                            summary=summary,
                        )
                        if (
                            not clear_manifest_first
                            and not retained_lists
                            and not retained_media
                        ):
                            self._clear_manifest(manifest, summary)
        else:
            summary.status = "completed_with_warnings"

        self._prune_numbered_files(
            self.output_dir,
            patterns=("ingest_ffmpeg_*.log", "ingest_ffmpeg.log"),
            keep=self.policy.keep_ingest_logs,
            summary=summary,
            label="ingest_logs",
        )
        self._prune_numbered_files(
            self.output_dir,
            patterns=("event_pipeline_report_*.json",),
            keep=self.policy.keep_run_reports,
            summary=summary,
            label="run_reports",
            exclude={self.output_dir / "event_pipeline_report.json"},
        )
        self._rotate_event_log(event_log_path, summary)
        self._checkpoint_sqlite(state_db_path, summary)
        self._cleanup_vision_candidates(summary)
        self._cleanup_final_gifs(summary)
        return summary

    def prune_final_gifs(self) -> CleanupSummary:
        """Delete expired product GIFs directly under this match directory.

        Only regular files below ``output_dir`` are considered.  Nested
        diagnostics and symlinks are deliberately left untouched because they
        may be owned by another cleanup policy or point outside the match.
        """
        summary = CleanupSummary(phase="final_gif_retention")
        self._cleanup_final_gifs(summary)
        return summary

    def _cleanup_final_gifs(self, summary: CleanupSummary) -> None:
        cutoff = time.time() - self.policy.final_gif_retention_seconds
        try:
            candidates = sorted(self.output_dir.glob("*.gif"))
        except OSError as exc:
            summary.add_error(self.output_dir, exc)
            return
        for path in candidates:
            if not self._is_safe_regular_file(path, self.output_dir):
                summary.skipped_files += 1
                continue
            try:
                if path.stat().st_mtime >= cutoff:
                    summary.retained_files += 1
                    continue
            except OSError as exc:
                summary.add_error(path, exc)
                continue
            self._delete_file(path, summary)
        if candidates:
            summary.actions.append("expired_final_gifs_pruned")

    @contextmanager
    def _segment_lease_guard(
        self,
        state_db_path: Path,
        summary: CleanupSummary,
    ) -> Iterable[set[Path] | None]:
        """Hold SQLite's writer lock while terminal media is being deleted.

        Lease acquisition uses the same database. Holding ``BEGIN IMMEDIATE``
        across the media pass prevents a new lease from appearing between the
        final check and an unlink operation. ``None`` means the guard could not
        be established, so callers must defer destructive media cleanup.
        """
        path = Path(state_db_path)
        if not path.exists():
            yield set()
            return
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(path, timeout=2.0)
            connection.execute("BEGIN IMMEDIATE")
            table = connection.execute(
                "SELECT 1 FROM sqlite_master "
                "WHERE type = 'table' AND name = 'segment_leases'"
            ).fetchone()
            active: set[Path] = set()
            if table is not None:
                connection.execute(
                    "DELETE FROM segment_leases WHERE expires_at_unix <= ?",
                    (time.time(),),
                )
                rows = connection.execute(
                    "SELECT DISTINCT segment_path FROM segment_leases "
                    "WHERE expires_at_unix > ?",
                    (time.time(),),
                ).fetchall()
                active = {
                    Path(str(row[0])).resolve()
                    for row in rows
                    if row and row[0]
                }
        except (OSError, sqlite3.Error) as exc:
            summary.add_error(path, exc)
            if connection is not None:
                connection.close()
            yield None
            return

        try:
            yield active
        except BaseException:
            connection.rollback()
            raise
        else:
            try:
                connection.commit()
            except (OSError, sqlite3.Error) as exc:
                summary.add_error(path, exc)
                connection.rollback()
        finally:
            connection.close()

    def _cleanup_vision_candidates(self, summary: CleanupSummary) -> None:
        """Delete expired visual-debug artifacts without traversing symlinks."""
        candidate_root = self.output_dir / "vision_candidates"
        try:
            if candidate_root.is_symlink():
                summary.skipped_files += 1
                summary.actions.append("vision_candidates_symlink_skipped")
                return
            if not candidate_root.exists():
                return
            if not candidate_root.is_dir():
                summary.skipped_files += 1
                summary.actions.append("vision_candidates_non_directory_skipped")
                return
            candidate_root.resolve().relative_to(self.output_dir)
        except (OSError, ValueError) as exc:
            summary.add_error(candidate_root, exc)
            return

        cutoff = time.time() - self.policy.vision_candidate_retention_seconds

        def scan(directory: Path) -> None:
            try:
                entries = list(os.scandir(directory))
            except OSError as exc:
                summary.add_error(directory, exc)
                return
            for entry in entries:
                path = Path(entry.path)
                try:
                    if entry.is_symlink():
                        summary.skipped_files += 1
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        scan(path)
                        continue
                    if not entry.is_file(follow_symlinks=False):
                        summary.skipped_files += 1
                        continue
                    if path.suffix.lower() not in VISION_CANDIDATE_SUFFIXES:
                        summary.retained_files += 1
                        continue
                    if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                        summary.retained_files += 1
                        continue
                except OSError as exc:
                    summary.add_error(path, exc)
                    continue
                if not self._is_safe_regular_file(path, self.output_dir):
                    summary.skipped_files += 1
                    continue
                self._delete_file(path, summary)

        scan(candidate_root)
        summary.actions.append("expired_vision_candidates_pruned")

    def _cleanup_ts_files(
        self,
        buffer_root: Path,
        *,
        protected: set[Path],
        summary: CleanupSummary,
    ) -> set[Path]:
        cutoff = time.time() - self.policy.post_match_buffer_seconds
        retained: set[Path] = set()
        for path in sorted(buffer_root.glob("*.ts")):
            if not self._is_safe_regular_file(path, self.output_dir):
                summary.skipped_files += 1
                continue
            resolved = path.resolve()
            try:
                keep = resolved in protected or path.stat().st_mtime >= cutoff
            except OSError as exc:
                summary.add_error(path, exc)
                continue
            if keep:
                retained.add(resolved)
                summary.retained_files += 1
            else:
                self._delete_file(path, summary)
        return retained

    def _cleanup_segment_lists(
        self,
        buffer_root: Path,
        *,
        retained_media: set[Path],
        protected: set[Path],
        summary: CleanupSummary,
    ) -> set[Path]:
        retained_lists: set[Path] = set()
        for path in sorted(buffer_root.glob("*.csv")):
            if not self._is_safe_regular_file(path, self.output_dir):
                summary.skipped_files += 1
                continue
            try:
                references = self._csv_media_paths(path, buffer_root)
            except (OSError, UnicodeError, csv.Error, ValueError) as exc:
                summary.add_error(path, exc)
                summary.skipped_files += 1
                retained_lists.add(path.resolve())
                continue
            if any(reference in retained_media or reference in protected for reference in references):
                retained_lists.add(path.resolve())
                summary.retained_files += 1
            else:
                self._delete_file(path, summary)
        return retained_lists

    @staticmethod
    def _csv_media_paths(path: Path, buffer_root: Path) -> list[Path]:
        references: list[Path] = []
        with path.open(newline="", encoding="utf-8") as handle:
            for row_number, row in enumerate(csv.reader(handle, strict=True), start=1):
                if not row or all(not value.strip() for value in row):
                    continue
                if not row[0].strip():
                    raise ValueError(f"row {row_number} has an empty media path")
                raw_path = Path(row[0])
                resolved = raw_path.resolve() if raw_path.is_absolute() else (buffer_root / raw_path).resolve()
                try:
                    resolved.relative_to(buffer_root.resolve())
                except ValueError as exc:
                    raise ValueError(f"row {row_number} references media outside buffer") from exc
                references.append(resolved)
        return references

    def _clear_manifest(self, path: Path | None, summary: CleanupSummary) -> bool:
        if path is None or not path.exists():
            return True
        if not self._is_safe_regular_file(path, self.output_dir):
            summary.skipped_files += 1
            return False
        temporary_path: Path | None = None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or not isinstance(payload.get("generations"), list):
                raise ValueError("manifest generations must be a list")
            if not payload["generations"]:
                return True
            payload["generations"] = []
            encoded = (json.dumps(payload, ensure_ascii=True, indent=2) + "\n").encode("utf-8")
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(encoded)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
            summary.actions.append("manifest_generations_cleared")
            return True
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
            summary.add_error(path, exc)
            return False
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError as exc:
                    summary.add_error(temporary_path, exc)

    def _rotate_event_log(self, event_log_path: Path, summary: CleanupSummary) -> None:
        path = Path(event_log_path).resolve()
        try:
            path.relative_to(self.output_dir)
        except ValueError:
            summary.skipped_files += 1
            summary.actions.append("event_log_outside_output_dir_skipped")
            return
        if not self._is_safe_regular_file(path, self.output_dir):
            return
        try:
            if path.stat().st_size <= self.policy.event_log_max_bytes:
                return
        except OSError as exc:
            summary.add_error(path, exc)
            return
        if self.policy.event_log_archives == 0:
            self._delete_file(path, summary)
            try:
                path.touch()
                summary.actions.append("event_log_truncated")
            except OSError as exc:
                summary.add_error(path, exc)
            return
        for index in range(self.policy.event_log_archives - 1, 0, -1):
            source = path.with_name(f"{path.name}.{index}")
            target = path.with_name(f"{path.name}.{index + 1}")
            if target.exists():
                self._delete_file(target, summary)
            if source.exists():
                try:
                    os.replace(source, target)
                    summary.rotated_files += 1
                except OSError as exc:
                    summary.add_error(source, exc)
        try:
            first_archive = path.with_name(f"{path.name}.1")
            if first_archive.exists():
                self._delete_file(first_archive, summary)
            os.replace(path, path.with_name(f"{path.name}.1"))
            path.touch()
            summary.rotated_files += 1
            summary.actions.append("event_log_rotated")
        except OSError as exc:
            summary.add_error(path, exc)

    @staticmethod
    def _checkpoint_sqlite(path: Path, summary: CleanupSummary) -> None:
        path = Path(path)
        if not path.exists():
            return
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(path, timeout=2.0)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            connection.commit()
            summary.actions.append("sqlite_wal_checkpoint")
        except (OSError, sqlite3.Error) as exc:
            summary.add_error(path, exc)
        finally:
            if connection is not None:
                connection.close()

    @staticmethod
    def _normalize_protected_paths(paths: Iterable[str | Path]) -> set[Path]:
        protected: set[Path] = set()
        for path in paths:
            try:
                protected.add(Path(path).resolve())
            except OSError:
                continue
        return protected

    @staticmethod
    def _safe_root(path: Path, root: Path, summary: CleanupSummary) -> Path | None:
        resolved = Path(path).resolve()
        try:
            resolved.relative_to(root.resolve())
        except ValueError as exc:
            summary.add_error(resolved, ValueError("path is outside output directory"))
            return None
        return resolved

    @staticmethod
    def _safe_path(path: Path, root: Path, summary: CleanupSummary) -> Path | None:
        resolved = Path(path).resolve()
        try:
            resolved.relative_to(root.resolve())
        except ValueError:
            summary.add_error(resolved, ValueError("path is outside output directory"))
            return None
        return resolved

    @staticmethod
    def _is_safe_regular_file(path: Path, root: Path) -> bool:
        try:
            path.resolve().relative_to(root.resolve())
        except (OSError, ValueError):
            return False
        return path.is_file() and not path.is_symlink()

    def _prune_numbered_files(
        self,
        directory: Path,
        *,
        patterns: tuple[str, ...],
        keep: int,
        summary: CleanupSummary,
        label: str,
        exclude: set[Path] | None = None,
    ) -> None:
        excluded = {path.resolve() for path in (exclude or set())}
        paths: set[Path] = set()
        for pattern in patterns:
            paths.update(directory.glob(pattern))
        candidates = [
            path for path in paths
            if self._is_safe_regular_file(path, self.output_dir)
            and path.resolve() not in excluded
        ]
        dated_candidates: list[tuple[float, Path]] = []
        for path in candidates:
            try:
                dated_candidates.append((path.stat().st_mtime, path))
            except OSError as exc:
                summary.add_error(path, exc)
        dated_candidates.sort(key=lambda item: (item[0], str(item[1])), reverse=True)
        for _, path in dated_candidates[keep:]:
            self._delete_file(path, summary)
        if dated_candidates:
            summary.actions.append(f"{label}_bounded_to_{keep}")

    def _delete_file(self, path: Path, summary: CleanupSummary) -> None:
        try:
            size = path.stat().st_size
            path.unlink()
            summary.deleted_files += 1
            summary.deleted_bytes += size
        except FileNotFoundError:
            return
        except OSError as exc:
            summary.add_error(path, exc)
