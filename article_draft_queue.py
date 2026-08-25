"""Persistent delivery queue for automatically creating OCR GIF drafts."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from article_publisher import (
    ArticlePublisher,
    ArticlePublishError,
    inspect_animated_gif,
)


OCR_ARTIFACT_KIND = "ocr_window"
DRAFT_DELIVERY_MODE = "draft"
DRAFT_ADMIN_ORIGIN = "https://dadmin.dongqiudi.com"
DEFAULT_RETRY_DELAYS_SECONDS = (30.0, 120.0, 300.0, 600.0)


def draft_admin_url(article_id: Any) -> str | None:
    value = str(article_id or "").strip()
    if not re.fullmatch(r"\d{1,20}", value):
        return None
    return (
        f"{DRAFT_ADMIN_ORIGIN}/admin/archives/articlePublish?"
        f"articleId={value}"
    )


def ocr_quality_label(result: dict[str, Any] | None) -> str:
    value = result if isinstance(result, dict) else {}
    candidates = (
        value.get("ocr_pipeline_status"),
        value.get("visual_resolution"),
        value.get("stage"),
        value.get("localization_source"),
        value.get("precision"),
    )
    labels = {
        "ocr_second_exact": "精确到秒",
        "exact_second": "精确到秒",
        "observed_second": "精确到秒",
        "ocr_second_interpolated": "根据前后画面推算到秒",
        "interpolated": "根据前后画面推算到秒",
        "interpolated_second": "根据前后画面推算到秒",
        "ocr_second_estimated": "目标正负 5 秒内定位",
        "estimated": "目标正负 5 秒内定位",
        "estimated_second": "目标正负 5 秒内定位",
        "ocr_second_projected": "根据连续比赛时钟推算",
        "projected": "根据连续比赛时钟推算",
        "projected_second": "根据连续比赛时钟推算",
        "ocr_minute_fallback": "分钟附近定位",
        "minute_boundary": "分钟附近定位",
        "ocr_range_fallback": "120 秒范围兜底",
        "ocr_api_range_fallback_encoded": "120 秒范围兜底",
    }
    if value.get("output_kind") == "api_time_range_fallback":
        if value.get("fallback_complete") is True:
            return "120 秒范围兜底"
        return "残缺范围片段"
    for candidate in candidates:
        label = labels.get(str(candidate or "").strip())
        if label:
            return label
    return "OCR GIF 已生成"


class ArticleDraftQueue:
    """SQLite-backed OCR draft queue with one background delivery worker."""

    def __init__(
        self,
        *,
        database_path: Path,
        publisher: ArticlePublisher | None = None,
        allowed_output_root: Path | None = None,
        staging_directory: Path | None = None,
        max_staged_bytes: int = 50 * 1024 * 1024,
        retry_delays_seconds: Iterable[float] = DEFAULT_RETRY_DELAYS_SECONDS,
        poll_seconds: float = 2.0,
        lease_seconds: float = 180.0,
        start_worker: bool = False,
    ) -> None:
        self.database_path = database_path.expanduser().resolve()
        self.publisher = publisher
        self.allowed_output_root = (
            allowed_output_root.expanduser().resolve()
            if allowed_output_root is not None
            else None
        )
        self.staging_directory = (
            staging_directory.expanduser().resolve()
            if staging_directory is not None
            else (
                publisher.gif_store.directory
                if publisher is not None
                else None
            )
        )
        self.max_staged_bytes = int(max_staged_bytes)
        if self.max_staged_bytes < 1:
            raise ValueError("草稿 GIF 大小上限必须是正整数")
        self.retry_delays_seconds = tuple(
            float(value) for value in retry_delays_seconds
        )
        if not self.retry_delays_seconds or any(
            not math.isfinite(value) or value <= 0
            for value in self.retry_delays_seconds
        ):
            raise ValueError("草稿重试时间必须全部是正数")
        self.poll_seconds = float(poll_seconds)
        self.lease_seconds = float(lease_seconds)
        if (
            not math.isfinite(self.poll_seconds)
            or self.poll_seconds <= 0
            or not math.isfinite(self.lease_seconds)
            or self.lease_seconds <= 0
        ):
            raise ValueError("草稿队列轮询和租约时间必须是正数")
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        if start_worker:
            self.start()

    def start(self) -> None:
        if self.publisher is None:
            raise RuntimeError("草稿队列缺少文章发布器，无法启动")
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._worker_loop,
            name="ocr-article-draft-worker",
            daemon=True,
        )
        self._thread.start()

    def close(self, timeout: float = 3.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=max(0.0, timeout))

    def status(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        if self.database_path.exists():
            try:
                with self._connect() as connection:
                    rows = connection.execute(
                        "SELECT status, COUNT(*) AS count "
                        "FROM article_delivery_tasks GROUP BY status"
                    ).fetchall()
                counts = {str(row["status"]): int(row["count"]) for row in rows}
            except sqlite3.Error:
                counts = {}
        return {
            "worker_running": bool(self._thread and self._thread.is_alive()),
            "database_path": str(self.database_path),
            "counts": counts,
            "retry_delays_seconds": list(self.retry_delays_seconds),
        }

    def enqueue(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any],
        source_path: Path,
        artifact_kind: str = OCR_ARTIFACT_KIND,
        delivery_mode: str = DRAFT_DELIVERY_MODE,
        artifact_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        event_key = str(event.get("event_key") or "").strip()
        if not re.fullmatch(r"\d{1,20}", str(match_id)):
            raise ValueError("自动草稿需要有效的数字比赛 ID")
        if not event_key:
            raise ValueError("自动草稿缺少事件标识")
        if artifact_kind != OCR_ARTIFACT_KIND or delivery_mode != DRAFT_DELIVERY_MODE:
            raise ValueError("自动草稿仅支持 OCR GIF")

        resolved_source = source_path.expanduser().resolve()
        if self.allowed_output_root is not None:
            match_root = (self.allowed_output_root / str(match_id)).resolve()
            if not resolved_source.is_relative_to(match_root):
                raise ValueError("OCR GIF 路径不属于这场比赛的输出目录")
        task_key = hashlib.sha256(
            (
                f"{match_id}\n{event_key}\n{artifact_kind}\n{delivery_mode}"
            ).encode("utf-8")
        ).hexdigest()
        now = time.time()
        event_payload = dict(event)
        event_payload["event_key"] = event_key
        event_json = json.dumps(
            event_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        detail_json = json.dumps(
            match_detail if isinstance(match_detail, dict) else {},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        quality_label = ocr_quality_label(artifact_result)
        source_signature, source_error = _source_signature(resolved_source)
        if source_error is None:
            unchanged = self._refresh_unchanged_task(
                task_key=task_key,
                source_path=resolved_source,
                source_signature=source_signature,
                event_json=event_json,
                detail_json=detail_json,
                quality_label=quality_label,
                now=now,
            )
            if unchanged is not None:
                self._wake.set()
                return _public_task(unchanged)

        staged: dict[str, Any] = {}
        if source_error is None:
            try:
                staged = self._stage_file(resolved_source)
            except (ArticlePublishError, OSError) as exc:
                source_error = str(exc)

        previous_chain_path: str | None = None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key = ?",
                (task_key,),
            ).fetchone()
            if existing is None:
                status = "failed" if source_error else "queued"
                stage = "source_check" if source_error else "queued"
                connection.execute(
                    """
                    INSERT INTO article_delivery_tasks (
                        task_key, match_id, event_key, artifact_kind, delivery_mode,
                        source_path, source_signature, event_json, match_detail_json,
                        quality_label, status, stage, artifact_sha256, staged_path,
                        generation, retriable, auth_required,
                        error_code, error, attempt_count, created_at_unix,
                        updated_at_unix
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0, ?, ?, 0, ?, ?)
                    """,
                    (
                        task_key,
                        str(match_id),
                        event_key,
                        artifact_kind,
                        delivery_mode,
                        str(resolved_source),
                        source_signature,
                        event_json,
                        detail_json,
                        quality_label,
                        status,
                        stage,
                        staged.get("gif_id"),
                        staged.get("path"),
                        "draft_source_missing" if source_error else None,
                        source_error,
                        now,
                        now,
                    ),
                )
            else:
                # The staged content hash is the artifact identity. A source file can
                # be touched or moved without producing a new OCR result.
                artifact_changed = bool(
                    staged.get("gif_id")
                    and str(existing["artifact_sha256"] or "")
                    != str(staged["gif_id"])
                )
                assignments = [
                    "source_path=?",
                    "event_json=?",
                    "match_detail_json=?",
                    "quality_label=?",
                    "updated_at_unix=?",
                ]
                values: list[Any] = [
                    str(resolved_source),
                    event_json,
                    detail_json,
                    quality_label,
                    now,
                ]
                if source_error is None:
                    assignments.append("source_signature=?")
                    values.append(source_signature)
                if artifact_changed:
                    previous_chain_path = str(
                        existing["previous_staged_path"] or ""
                    ).strip() or None
                    assignments.extend(
                        [
                            "status=?",
                            "stage=?",
                            "previous_staged_path=staged_path",
                            "artifact_sha256=?",
                            "staged_path=?",
                            "gif_url=NULL",
                            "platform_code=NULL",
                            "duplicate=0",
                            "attempt_count=0",
                            "next_attempt_at_unix=NULL",
                            "lease_until_unix=NULL",
                            "lease_token=NULL",
                            "generation=generation+1",
                            "retriable=0",
                            "auth_required=0",
                            "error_code=?",
                            "error=?",
                            "completed_at_unix=NULL",
                        ]
                    )
                    values.extend(
                        [
                            "failed" if source_error else "queued",
                            "source_check" if source_error else "queued",
                            staged.get("gif_id"),
                            staged.get("path"),
                            "draft_source_missing" if source_error else None,
                            source_error,
                        ]
                    )
                connection.execute(
                    f"UPDATE article_delivery_tasks SET {', '.join(assignments)} "
                    "WHERE task_key=?",
                    (*values, task_key),
                )
            row = connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key = ?",
                (task_key,),
            ).fetchone()
        self._delete_previous_staged_if_unused(previous_chain_path)
        self._wake.set()
        if row is None:
            raise RuntimeError("自动草稿任务登记后无法读取")
        return _public_task(row)

    def _refresh_unchanged_task(
        self,
        *,
        task_key: str,
        source_path: Path,
        source_signature: str,
        event_json: str,
        detail_json: str,
        quality_label: str,
        now: float,
    ) -> sqlite3.Row | None:
        """Avoid reading a historical GIF again when its staged copy is intact."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key=?",
                (task_key,),
            ).fetchone()
            if (
                existing is None
                or str(existing["source_signature"]) != source_signature
                or not existing["artifact_sha256"]
                or not existing["staged_path"]
                or not Path(str(existing["staged_path"])).is_file()
            ):
                return None
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET source_path=?, event_json=?, match_detail_json=?,
                    quality_label=?, updated_at_unix=?
                WHERE task_key=? AND generation=? AND source_signature=?
                """,
                (
                    str(source_path),
                    event_json,
                    detail_json,
                    quality_label,
                    now,
                    task_key,
                    existing["generation"],
                    source_signature,
                ),
            )
            if updated.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key=?",
                (task_key,),
            ).fetchone()

    def records_for_match(self, match_id: str) -> dict[str, dict[str, Any]]:
        if not self.database_path.exists():
            return {}
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT * FROM article_delivery_tasks
                    WHERE match_id = ? AND artifact_kind = ? AND delivery_mode = ?
                    ORDER BY updated_at_unix DESC
                    """,
                    (str(match_id), OCR_ARTIFACT_KIND, DRAFT_DELIVERY_MODE),
                ).fetchall()
        except sqlite3.Error:
            return {}
        records: dict[str, dict[str, Any]] = {}
        for row in rows:
            records.setdefault(str(row["event_key"]), _public_task(row))
        return records

    def run_once(self, *, now: float | None = None) -> bool:
        if self.publisher is None:
            raise RuntimeError("草稿队列缺少文章发布器")
        claimed_at = time.time() if now is None else float(now)
        row = self._claim_due(claimed_at)
        if row is None:
            return False
        task = dict(row)
        try:
            event = json.loads(str(task["event_json"]))
            detail = json.loads(str(task["match_detail_json"]))
            if not isinstance(event, dict) or not isinstance(detail, dict):
                raise ValueError("草稿任务中的比赛信息损坏")
            source_path = self._stage_source(task)
            if not self._claim_owned(task):
                return True
            result = self.publisher.create_or_update_draft(
                match_id=str(task["match_id"]),
                event=event,
                match_detail=detail,
                source_path=source_path,
                archive_id=task.get("article_id"),
            )
        except ArticlePublishError as exc:
            self._record_failure(task, exc, now=claimed_at)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            wrapped = ArticlePublishError(
                _friendly_draft_error("source_check", str(exc)),
                code="draft_source_unavailable",
                stage="source_check",
            )
            self._record_failure(task, wrapped, now=claimed_at)
        except Exception as exc:
            wrapped = ArticlePublishError(
                _friendly_draft_error("internal", str(exc)),
                code="draft_internal_error",
                stage="internal",
                status_code=500,
                retriable=True,
            )
            self._record_failure(task, wrapped, now=claimed_at)
        else:
            self._record_success(task, result, now=claimed_at)
        return True

    def _connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS article_delivery_tasks (
                task_key TEXT PRIMARY KEY,
                match_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                delivery_mode TEXT NOT NULL,
                source_path TEXT NOT NULL,
                source_signature TEXT NOT NULL,
                event_json TEXT NOT NULL,
                match_detail_json TEXT NOT NULL,
                quality_label TEXT,
                status TEXT NOT NULL,
                stage TEXT NOT NULL,
                article_id TEXT,
                artifact_sha256 TEXT,
                staged_path TEXT,
                gif_url TEXT,
                platform_code TEXT,
                duplicate INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at_unix REAL,
                lease_until_unix REAL,
                retriable INTEGER NOT NULL DEFAULT 0,
                auth_required INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                error TEXT,
                created_at_unix REAL NOT NULL,
                updated_at_unix REAL NOT NULL,
                completed_at_unix REAL,
                generation INTEGER NOT NULL DEFAULT 1,
                lease_token TEXT,
                previous_staged_path TEXT,
                UNIQUE(match_id, event_key, artifact_kind, delivery_mode)
            )
            """
        )
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(article_delivery_tasks)")
        }
        for name, definition in (
            ("generation", "INTEGER NOT NULL DEFAULT 1"),
            ("lease_token", "TEXT"),
            ("previous_staged_path", "TEXT"),
        ):
            if name not in columns:
                connection.execute(
                    f"ALTER TABLE article_delivery_tasks ADD COLUMN {name} {definition}"
                )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS article_delivery_due
            ON article_delivery_tasks(status, next_attempt_at_unix, lease_until_unix)
            """
        )
        return connection

    def _claim_due(self, now: float) -> sqlite3.Row | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM article_delivery_tasks
                WHERE (
                    status = 'queued'
                    OR (status = 'retry_wait' AND COALESCE(next_attempt_at_unix, 0) <= ?)
                    OR (status = 'creating' AND COALESCE(lease_until_unix, 0) <= ?)
                )
                ORDER BY
                    CASE status WHEN 'creating' THEN 0 WHEN 'queued' THEN 1 ELSE 2 END,
                    COALESCE(next_attempt_at_unix, created_at_unix), created_at_unix
                LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            lease_token = uuid.uuid4().hex
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status='creating', stage='creating', attempt_count=attempt_count+1,
                    next_attempt_at_unix=NULL, lease_until_unix=?,
                    lease_token=?, updated_at_unix=?
                WHERE task_key=? AND generation=? AND (
                    status='queued'
                    OR (status='retry_wait' AND COALESCE(next_attempt_at_unix, 0) <= ?)
                    OR (status='creating' AND COALESCE(lease_until_unix, 0) <= ?)
                )
                """,
                (
                    now + self.lease_seconds,
                    lease_token,
                    now,
                    row["task_key"],
                    row["generation"],
                    now,
                    now,
                ),
            )
            if updated.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key=? AND lease_token=?",
                (row["task_key"], lease_token),
            ).fetchone()

    def _stage_source(self, task: dict[str, Any]) -> Path:
        staged_path = str(task.get("staged_path") or "").strip()
        if staged_path:
            resolved_staged = Path(staged_path).expanduser().resolve()
            if resolved_staged.is_file():
                return resolved_staged

        source_path = Path(str(task["source_path"])).expanduser().resolve()
        match_id = str(task["match_id"])
        if self.allowed_output_root is not None:
            match_root = (self.allowed_output_root / match_id).resolve()
            if not source_path.is_relative_to(match_root):
                raise ValueError("OCR GIF 路径不属于这场比赛的输出目录")
        if source_path.suffix.lower() != ".gif" or not source_path.is_file():
            raise ValueError("OCR GIF 文件不存在，无法创建草稿")
        current_signature, source_error = _source_signature(source_path)
        if source_error:
            raise ValueError(source_error)
        if current_signature != str(task["source_signature"]):
            self._refresh_source_signature(task, current_signature)

        gif = self._stage_file(source_path)
        staged = Path(str(gif["path"])).resolve()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE article_delivery_tasks
                SET artifact_sha256=?, staged_path=?,
                    stage='gif_prepared', updated_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=? AND status='creating'
                """,
                (
                    str(gif["gif_id"]),
                    str(staged),
                    time.time(),
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )
        return staged

    def _stage_file(self, source_path: Path) -> dict[str, Any]:
        if self.staging_directory is None:
            raise ArticlePublishError(
                "草稿队列没有配置永久 GIF 目录",
                code="draft_staging_unavailable",
                stage="gif_storage",
                status_code=503,
            )
        body = source_path.read_bytes()
        if not body:
            raise ArticlePublishError(
                "OCR GIF 是空文件",
                code="draft_gif_empty",
                stage="gif_validation",
            )
        if len(body) > self.max_staged_bytes:
            raise ArticlePublishError(
                f"OCR GIF 超过草稿上限（{len(body)} > {self.max_staged_bytes} 字节）",
                code="draft_gif_too_large",
                stage="gif_validation",
            )
        inspect_animated_gif(body)
        gif_id = hashlib.sha256(body).hexdigest()
        self.staging_directory.mkdir(parents=True, exist_ok=True, mode=0o755)
        target = self.staging_directory / f"{gif_id}.gif"
        if not target.exists():
            temporary = target.with_name(f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
            temporary.write_bytes(body)
            os.chmod(temporary, 0o644)
            os.replace(temporary, target)
        return {"gif_id": gif_id, "path": str(target.resolve())}

    def _refresh_source_signature(
        self, task: dict[str, Any], signature: str
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE article_delivery_tasks SET source_signature=?, updated_at_unix=? "
                "WHERE task_key=? AND generation=? AND lease_token=? AND status='creating'",
                (
                    signature,
                    time.time(),
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )

    def _claim_owned(self, task: dict[str, Any]) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM article_delivery_tasks
                WHERE task_key=? AND generation=? AND lease_token=? AND status='creating'
                """,
                (task["task_key"], task["generation"], task["lease_token"]),
            ).fetchone()
        return row is not None

    def _record_success(
        self, task: dict[str, Any], result: dict[str, Any], *, now: float
    ) -> None:
        gif = result.get("gif") if isinstance(result.get("gif"), dict) else {}
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status='success', stage=?, article_id=?, artifact_sha256=?,
                    staged_path=?, gif_url=?, platform_code=?, duplicate=?,
                    next_attempt_at_unix=NULL, lease_until_unix=NULL, lease_token=NULL,
                    retriable=0, auth_required=0, error_code=NULL, error=NULL,
                    previous_staged_path=NULL, updated_at_unix=?, completed_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=? AND status='creating'
                """,
                (
                    "draft_updated" if result.get("updated") else "draft_created",
                    str(result["article_id"]),
                    gif.get("gif_id") or task.get("artifact_sha256"),
                    gif.get("path") or task.get("staged_path"),
                    gif.get("url") or task.get("gif_url"),
                    None
                    if result.get("platform_code") is None
                    else str(result.get("platform_code")),
                    int(bool(result.get("duplicate"))),
                    now,
                    now,
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )
        if updated.rowcount == 1:
            self._delete_previous_staged_if_unused(task.get("previous_staged_path"))
        else:
            self._record_stale_article_id(task, result)

    def _record_failure(
        self, task: dict[str, Any], exc: ArticlePublishError, *, now: float
    ) -> None:
        attempts = int(task.get("attempt_count") or 0)
        if exc.auth_required:
            status = "retry_wait"
            next_attempt = now + self.retry_delays_seconds[-1]
            retriable = True
        elif exc.retriable and attempts <= len(self.retry_delays_seconds):
            status = "retry_wait"
            delay_index = min(max(attempts - 1, 0), len(self.retry_delays_seconds) - 1)
            next_attempt = now + self.retry_delays_seconds[delay_index]
            retriable = True
        else:
            status = "failed"
            next_attempt = None
            retriable = False
        friendly_error = _friendly_draft_error(exc.stage, str(exc))
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status=?, stage=?, next_attempt_at_unix=?, lease_until_unix=NULL,
                    lease_token=NULL, retriable=?, auth_required=?, error_code=?, error=?,
                    updated_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=? AND status='creating'
                """,
                (
                    status,
                    exc.stage,
                    next_attempt,
                    int(retriable),
                    int(exc.auth_required),
                    exc.code,
                    friendly_error,
                    now,
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )

    def _record_stale_article_id(
        self, task: dict[str, Any], result: dict[str, Any]
    ) -> None:
        article_id = str(result.get("article_id") or "").strip()
        if not re.fullmatch(r"\d{1,20}", article_id):
            return
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE article_delivery_tasks SET article_id=COALESCE(article_id, ?)
                WHERE task_key=? AND (
                    generation>?
                    OR (
                        generation=?
                        AND COALESCE(lease_token, '')<>COALESCE(?, '')
                    )
                )
                """,
                (
                    article_id,
                    task["task_key"],
                    task["generation"],
                    task["generation"],
                    task.get("lease_token"),
                ),
            )

    def _delete_previous_staged_if_unused(self, value: Any) -> None:
        previous = str(value or "").strip()
        if not previous or self.staging_directory is None:
            return
        path = Path(previous).expanduser().resolve()
        if not path.is_relative_to(self.staging_directory) or path.suffix != ".gif":
            return
        try:
            with self._connect() as connection:
                delivery_reference = connection.execute(
                    """
                    SELECT 1 FROM article_delivery_tasks
                    WHERE staged_path=? OR previous_staged_path=? LIMIT 1
                    """,
                    (str(path), str(path)),
                ).fetchone()
                publish_table = connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type='table' AND name='article_publish_records'
                    """
                ).fetchone()
                publish_reference = (
                    connection.execute(
                        "SELECT 1 FROM article_publish_records WHERE gif_path=? LIMIT 1",
                        (str(path),),
                    ).fetchone()
                    if publish_table is not None
                    else None
                )
            if delivery_reference is None and publish_reference is None:
                path.unlink(missing_ok=True)
        except (OSError, sqlite3.Error):
            return

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                processed = self.run_once()
            except Exception:
                processed = False
            if processed:
                continue
            self._wake.wait(self.poll_seconds)
            self._wake.clear()


def _source_signature(path: Path) -> tuple[str, str | None]:
    try:
        stat = path.stat()
    except OSError as exc:
        return f"missing:{path}", f"OCR GIF 文件不存在或无法读取：{exc}"
    if not path.is_file():
        return f"invalid:{path}", "OCR GIF 路径不是文件"
    return f"{stat.st_size}:{stat.st_mtime_ns}", None


def _friendly_draft_error(stage: str, detail: str) -> str:
    cleaned = str(detail or "").strip()
    if stage == "authorization":
        return "懂球帝开放平台尚未授权或授权已过期；授权恢复后会继续创建草稿。"
    if stage == "public_url_check":
        return "OCR GIF 已生成，但公网地址暂时无法访问；系统会稍后重试。"
    if stage == "platform_publish":
        return f"OCR GIF 已生成，但懂球帝草稿接口没有接受请求：{cleaned or '未说明原因'}"
    if stage in {"gif_validation", "gif_storage", "source_check"}:
        return f"未创建草稿：{cleaned or 'OCR GIF 文件无法使用'}"
    return f"草稿创建暂时失败：{cleaned or '未说明原因'}"


def _public_task(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    article_id = value.get("article_id")
    return {
        "task_key": value.get("task_key"),
        "match_id": value.get("match_id"),
        "event_key": value.get("event_key"),
        "artifact_kind": value.get("artifact_kind"),
        "delivery_mode": value.get("delivery_mode"),
        "quality_label": value.get("quality_label"),
        "status": value.get("status"),
        "stage": value.get("stage"),
        "article_id": article_id,
        "draft_url": draft_admin_url(article_id),
        "attempt_count": int(value.get("attempt_count") or 0),
        "next_attempt_at_unix": value.get("next_attempt_at_unix"),
        "retriable": bool(value.get("retriable")),
        "auth_required": bool(value.get("auth_required")),
        "error_code": value.get("error_code"),
        "error": value.get("error"),
        "gif_url": value.get("gif_url"),
        "updated_at_unix": value.get("updated_at_unix"),
        "completed_at_unix": value.get("completed_at_unix"),
    }
