"""Persistent delivery queue for automatically publishing OCR GIF articles."""

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
from typing import Any, Callable, Iterable

from article_publisher import (
    ArticlePublisher,
    ArticlePublishError,
    has_reliable_person,
    inspect_animated_gif,
)
from match_event_identity import merge_event_metadata


OCR_ARTIFACT_KIND = "ocr_window"
DRAFT_DELIVERY_MODE = "draft"
DRAFT_ADMIN_ORIGIN = "https://dadmin.dongqiudi.com"
DEFAULT_RETRY_DELAYS_SECONDS = (30.0, 120.0, 300.0, 600.0)
DEFAULT_PERSON_WAIT_SECONDS = 60.0
TERMINAL_DELIVERY_STATUSES = {"published"}
PUBLICATION_HOLD_STATUS = "held"
AUTO_PUBLISH_MIN_PLAYABLE_SECONDS = 55.0
AUTO_PUBLISH_ANCHOR_TOLERANCE_SECONDS = 1.0
AUTO_PUBLISH_MIN_CENTERED_SIDE_SECONDS = 25.0


def _event_with_team_fallback(
    event: dict[str, Any], match_detail: dict[str, Any]
) -> dict[str, Any]:
    """Return a publishable event using the scoring team's name as actor."""
    fallback = dict(event)
    team = str(event.get("team") or "").strip()
    normalized = team.casefold().replace("-", "_")

    # Overview events normally use ``teamA``/``teamB`` while shotmap events
    # can carry a numeric team ID.  Resolve both forms against the match
    # detail before falling back to a human-readable value.
    metadata = event.get("metadata")
    metadata_team_id = (
        metadata.get("team_id") if isinstance(metadata, dict) else None
    )
    team_ids = {
        str(value).strip()
        for value in (team, event.get("team_id"), metadata_team_id)
        if str(value or "").strip()
    }
    home_ids = {
        str(match_detail.get(key) or "").strip()
        for key in ("team_A_id", "team_a_id")
        if str(match_detail.get(key) or "").strip()
    }
    away_ids = {
        str(match_detail.get(key) or "").strip()
        for key in ("team_B_id", "team_b_id")
        if str(match_detail.get(key) or "").strip()
    }
    if normalized in {"a", "teama", "team_a", "home"} or team_ids & home_ids:
        person = str(
            match_detail.get("team_A_name")
            or match_detail.get("team_a_name")
            or "主队"
        ).strip()
    elif normalized in {"b", "teamb", "team_b", "away"} or team_ids & away_ids:
        person = str(
            match_detail.get("team_B_name")
            or match_detail.get("team_b_name")
            or "客队"
        ).strip()
    else:
        person = str(event.get("team_name") or team).strip()
        if re.fullmatch(r"\d{1,20}", person):
            person = ""
    fallback["person"] = person or "球队"
    fallback["person_fallback"] = True
    return fallback


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


def ocr_publication_eligibility(result: dict[str, Any] | None) -> dict[str, Any]:
    """Classify whether an OCR GIF is safe for automatic article delivery.

    The media worker is allowed to retain degraded clips for inspection. Article
    delivery is stricter: a clip must have a trustworthy OCR clock anchor, keep
    the event frame, and contain enough continuous video around that anchor.
    """
    value = result if isinstance(result, dict) else {}

    def hold(reason_code: str, reason: str, **details: Any) -> dict[str, Any]:
        return {
            "eligible": False,
            "reason_code": reason_code,
            "reason": reason,
            **details,
        }

    def allow(reason_code: str, reason: str, **details: Any) -> dict[str, Any]:
        return {
            "eligible": True,
            "reason_code": reason_code,
            "reason": reason,
            **details,
        }

    if not value:
        return hold(
            "metadata_missing",
            "未提供 OCR GIF 的定位和覆盖范围元数据，无法确认事件画面是否在片段中。",
        )

    output_kind = str(value.get("output_kind") or "").strip()
    fallback_label = str(value.get("fallback_label") or "").strip()
    if fallback_label == "history_missing_nearest_clip" or value.get("fallback_anchor_history_missing") is True:
        return hold(
            "history_misaligned",
            "事件对应的历史视频已被缓存淘汰，当前片段只是最近邻画面，无法确认包含事件。",
        )
    if output_kind == "api_time_range_fallback" or value.get("localization_source") == "api_time_range":
        return hold(
            "unverified_api_fallback",
            "这是接口时间范围兜底片段，没有通过画面 OCR 确认事件锚点，暂不自动发布。",
        )

    localization_source = str(value.get("localization_source") or "").strip()
    trusted_sources = {
        "exact",
        "exact_second",
        "interpolated",
        "interpolated_second",
        "estimated",
        "estimated_second",
        "projected",
        "projected_second",
        "minute_boundary",
    }
    if localization_source not in trusted_sources:
        return hold(
            "ocr_unverified",
            "片段没有可信的 OCR 比赛时钟锚点，无法确认事件发生位置。",
            localization_source=localization_source or None,
        )
    if value.get("ocr_verified") is False:
        return hold(
            "ocr_unverified",
            "OCR 结果明确标记为未验证，不能据此自动发布。",
            localization_source=localization_source,
        )
    if value.get("event_frame_may_be_missing") is True:
        return hold(
            "event_frame_risk",
            "视频断口或锚点调整可能导致事件画面缺失，不能自动发布。",
        )
    if value.get("anchor_adjusted") is True:
        return hold(
            "anchor_adjusted",
            "OCR 锚点被移动到相邻视频片段，事件时间与画面位置不再严格对应。",
        )
    gap_count = _optional_float(value.get("video_gap_count"))
    if value.get("stitched_across_gap") is True or (gap_count is not None and gap_count > 0):
        return hold(
            "internal_gap",
            "片段包含内部直播断口，虽然可以拼接播放，但事件附近的连续性无法保证。",
            video_gap_count=gap_count,
        )

    requested = value.get("requested_media_window")
    if not isinstance(requested, dict):
        requested = {
            "start_stream_time": value.get("requested_clip_stream_start_sec"),
            "end_stream_time": value.get("requested_clip_stream_end_sec"),
        }
    actual = value.get("actual_media_window")
    if not isinstance(actual, dict):
        actual = {
            "start_stream_time": value.get("clip_stream_start_sec"),
            "end_stream_time": value.get("clip_stream_end_sec"),
        }
    requested_start = _optional_float(requested.get("start_stream_time"))
    requested_end = _optional_float(requested.get("end_stream_time"))
    actual_start = _optional_float(actual.get("start_stream_time"))
    actual_end = _optional_float(actual.get("end_stream_time"))
    anchor = _optional_float(value.get("anchor_stream_time"))
    if anchor is None:
        anchor = _optional_float(value.get("anchor_stream_time_sec"))
    requested_duration = (
        requested_end - requested_start
        if requested_start is not None
        and requested_end is not None
        and requested_end > requested_start
        else None
    )
    actual_duration = _optional_float(value.get("available_media_duration_seconds"))
    if actual_duration is None:
        actual_duration = _optional_float(value.get("duration_sec"))
    if actual_duration is None and actual_start is not None and actual_end is not None:
        actual_duration = max(0.0, actual_end - actual_start)
    if requested_duration is None or actual_duration is None:
        return hold(
            "metadata_missing",
            "缺少请求区间或实际可播放时长，无法判断残缺是否仅发生在片段边缘。",
            requested_duration_seconds=requested_duration,
            actual_duration_seconds=actual_duration,
        )
    minimum_duration = min(
        AUTO_PUBLISH_MIN_PLAYABLE_SECONDS,
        max(0.0, requested_duration),
    )
    if actual_duration + AUTO_PUBLISH_ANCHOR_TOLERANCE_SECONDS < minimum_duration:
        return hold(
            "insufficient_coverage",
            f"实际可播放时长约 {actual_duration:.1f} 秒，低于自动发布要求的 {minimum_duration:.1f} 秒。",
            requested_duration_seconds=round(requested_duration, 3),
            actual_duration_seconds=round(actual_duration, 3),
        )

    clip_before = _optional_float(value.get("clip_before_seconds"))
    clip_after = _optional_float(value.get("clip_after_seconds"))
    is_minute_boundary = localization_source in {"minute_boundary", "minute_boundary_second"}
    if actual_start is None or actual_end is None or anchor is None:
        return hold(
            "metadata_missing",
            "缺少实际起止时间或 OCR 锚点位置，无法确认事件位于片段内。",
            requested_duration_seconds=round(requested_duration, 3),
            actual_duration_seconds=round(actual_duration, 3),
        )
    if is_minute_boundary:
        if actual_end + AUTO_PUBLISH_ANCHOR_TOLERANCE_SECONDS < anchor:
            return hold(
                "anchor_tail_missing",
                "片段末尾没有覆盖 OCR 确认的分钟锚点，事件画面可能在缺失的尾部。",
            )
    else:
        expected_before = clip_before if clip_before is not None else min(30.0, requested_duration / 2.0)
        expected_after = clip_after if clip_after is not None else min(30.0, requested_duration / 2.0)
        before_available = anchor - actual_start
        after_available = actual_end - anchor
        if before_available + AUTO_PUBLISH_ANCHOR_TOLERANCE_SECONDS < min(
            expected_before, AUTO_PUBLISH_MIN_CENTERED_SIDE_SECONDS
        ):
            return hold(
                "anchor_leading_coverage_insufficient",
                f"事件前连续画面约 {max(0.0, before_available):.1f} 秒，低于自动发布要求。",
            )
        if after_available + AUTO_PUBLISH_ANCHOR_TOLERANCE_SECONDS < min(
            expected_after, AUTO_PUBLISH_MIN_CENTERED_SIDE_SECONDS
        ):
            return hold(
                "anchor_trailing_coverage_insufficient",
                f"事件后连续画面约 {max(0.0, after_available):.1f} 秒，低于自动发布要求。",
            )

    if actual_duration + AUTO_PUBLISH_ANCHOR_TOLERANCE_SECONDS < requested_duration:
        return allow(
            "trusted_ocr_edge_truncated",
            f"OCR 锚点可信，片段仅在边缘缺少约 {requested_duration - actual_duration:.1f} 秒，允许自动发布。",
            requested_duration_seconds=round(requested_duration, 3),
            actual_duration_seconds=round(actual_duration, 3),
        )
    return allow(
        "trusted_ocr_complete",
        "OCR 锚点可信且片段覆盖完整，允许自动发布。",
        requested_duration_seconds=round(requested_duration, 3),
        actual_duration_seconds=round(actual_duration, 3),
    )


class ArticleDraftQueue:
    """SQLite-backed OCR article queue with one background delivery worker."""

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
        person_wait_seconds: float = DEFAULT_PERSON_WAIT_SECONDS,
        auto_publish_after_unix: float | None = None,
        latest_event_loader: Callable[
            [str, str, dict[str, Any]], dict[str, Any] | None
        ]
        | None = None,
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
        self.person_wait_seconds = float(person_wait_seconds)
        self.auto_publish_after_unix = (
            None
            if auto_publish_after_unix is None
            else float(auto_publish_after_unix)
        )
        self.latest_event_loader = latest_event_loader
        if (
            not math.isfinite(self.poll_seconds)
            or self.poll_seconds <= 0
            or not math.isfinite(self.lease_seconds)
            or self.lease_seconds <= 0
            or not math.isfinite(self.person_wait_seconds)
            or self.person_wait_seconds <= 0
        ):
            raise ValueError("文章队列轮询、租约和姓名等待时间必须是正数")
        if self.auto_publish_after_unix is not None and not math.isfinite(
            self.auto_publish_after_unix
        ):
            raise ValueError("文章自动发布时间边界必须是有效时间戳")
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
        with self._connect():
            pass
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
            "person_wait_seconds": self.person_wait_seconds,
            "auto_publish_after_unix": self.auto_publish_after_unix,
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
        terminal = self._terminal_task(task_key)
        if terminal is not None:
            return _public_task(terminal)
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
        eligibility = ocr_publication_eligibility(artifact_result)
        eligibility_json = json.dumps(
            eligibility, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        source_signature, source_error = _source_signature(resolved_source)
        if source_error is None:
            unchanged = self._refresh_unchanged_task(
                task_key=task_key,
                source_path=resolved_source,
                source_signature=source_signature,
                event_json=event_json,
                detail_json=detail_json,
                quality_label=quality_label,
                eligibility_json=eligibility_json,
                person_available=has_reliable_person(event_payload),
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
                status = (
                    "failed"
                    if source_error
                    else "queued"
                    if eligibility.get("eligible")
                    else PUBLICATION_HOLD_STATUS
                )
                stage = (
                    "source_check"
                    if source_error
                    else "queued"
                    if eligibility.get("eligible")
                    else "publication_gate"
                )
                connection.execute(
                    """
                    INSERT INTO article_delivery_tasks (
                        task_key, match_id, event_key, artifact_kind, delivery_mode,
                        source_path, source_signature, event_json, match_detail_json,
                        quality_label, eligibility_json, status, stage, artifact_sha256, staged_path,
                        generation, retriable, auth_required,
                        error_code, error, attempt_count, created_at_unix,
                        updated_at_unix
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, 0, ?, ?, 0, ?, ?)
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
                        eligibility_json,
                        status,
                        stage,
                        staged.get("gif_id"),
                        staged.get("path"),
                        "draft_source_missing"
                        if source_error
                        else None
                        if eligibility.get("eligible")
                        else str(eligibility.get("reason_code") or "auto_publish_not_eligible"),
                        source_error
                        if source_error
                        else None
                        if eligibility.get("eligible")
                        else str(eligibility.get("reason") or "OCR GIF 不符合自动发布条件"),
                        now,
                        now,
                    ),
                )
            else:
                if str(existing["status"] or "") in TERMINAL_DELIVERY_STATUSES:
                    return _public_task(existing)
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
                    "eligibility_json=?",
                    "updated_at_unix=?",
                ]
                values: list[Any] = [
                    str(resolved_source),
                    event_json,
                    detail_json,
                    quality_label,
                    eligibility_json,
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
                            "platform_status_code=NULL",
                            "diagnostics_json=NULL",
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
                            "draft_created_at_unix=NULL",
                            "person_deadline_at_unix=NULL",
                            "published_at_unix=NULL",
                            "publish_reason=NULL",
                            "final_event_json=NULL",
                        ]
                    )
                    values.extend(
                        [
                            "failed"
                            if source_error
                            else "queued"
                            if eligibility.get("eligible")
                            else PUBLICATION_HOLD_STATUS,
                            "source_check"
                            if source_error
                            else "queued"
                            if eligibility.get("eligible")
                            else "publication_gate",
                            staged.get("gif_id"),
                            staged.get("path"),
                            "draft_source_missing"
                            if source_error
                            else None
                            if eligibility.get("eligible")
                            else str(eligibility.get("reason_code") or "auto_publish_not_eligible"),
                            source_error
                            if source_error
                            else None
                            if eligibility.get("eligible")
                            else str(eligibility.get("reason") or "OCR GIF 不符合自动发布条件"),
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
        eligibility_json: str,
        person_available: bool,
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
            if str(existing["status"] or "") in TERMINAL_DELIVERY_STATUSES:
                return existing
            eligibility = _json_object(eligibility_json) or {
                "eligible": False,
                "reason_code": "metadata_missing",
                "reason": "未提供 OCR GIF 的定位和覆盖范围元数据，无法确认事件画面是否在片段中。",
            }
            eligible = bool(eligibility.get("eligible"))
            next_status = str(existing["status"] or "")
            next_stage = str(existing["stage"] or "")
            if next_status == PUBLICATION_HOLD_STATUS and eligible:
                next_status, next_stage = "queued", "queued"
            elif next_status in {"queued", "waiting_person"} and not eligible:
                next_status, next_stage = PUBLICATION_HOLD_STATUS, "publication_gate"
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET source_path=?, event_json=?, match_detail_json=?,
                    quality_label=?, eligibility_json=?,
                    status=?, stage=?, error_code=?, error=?,
                    next_attempt_at_unix=CASE
                        WHEN ? THEN CASE
                            WHEN status='waiting_person' AND ? THEN ?
                            ELSE next_attempt_at_unix
                        END
                        ELSE NULL
                    END,
                    updated_at_unix=?
                WHERE task_key=? AND generation=? AND source_signature=?
                """,
                (
                    str(source_path),
                    event_json,
                    detail_json,
                    quality_label,
                    eligibility_json,
                    next_status,
                    next_stage,
                    None if eligible else str(eligibility.get("reason_code") or "auto_publish_not_eligible"),
                    None if eligible else str(eligibility.get("reason") or "OCR GIF 不符合自动发布条件"),
                    int(eligible),
                    int(person_available),
                    now,
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

    def _terminal_task(self, task_key: str) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM article_delivery_tasks "
                "WHERE task_key=? AND status='published'",
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

    def refresh_event(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any] | None = None,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        """Refresh a nonterminal task and wake it when a usable name arrives."""
        event_key = str(event.get("event_key") or "").strip()
        if not event_key:
            raise ValueError("刷新文章任务缺少事件标识")
        timestamp = time.time() if now is None else float(now)
        detail_json = (
            json.dumps(
                match_detail,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            if isinstance(match_detail, dict)
            else None
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM article_delivery_tasks
                WHERE match_id=? AND event_key=? AND artifact_kind=? AND delivery_mode=?
                ORDER BY updated_at_unix DESC LIMIT 1
                """,
                (
                    str(match_id),
                    event_key,
                    OCR_ARTIFACT_KIND,
                    DRAFT_DELIVERY_MODE,
                ),
            ).fetchone()
            if row is None or str(row["status"] or "") in TERMINAL_DELIVERY_STATUSES:
                return _public_task(row) if row is not None else None
            stored_event = _json_object(row["event_json"]) or {}
            merged_event = merge_event_metadata(stored_event, event)
            merged_event["event_key"] = event_key
            event_json = json.dumps(
                merged_event,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                """
                UPDATE article_delivery_tasks
                SET event_json=?,
                    match_detail_json=COALESCE(?, match_detail_json),
                    next_attempt_at_unix=CASE
                        WHEN status='waiting_person' AND ? THEN ?
                        ELSE next_attempt_at_unix
                    END,
                    updated_at_unix=?
                WHERE task_key=? AND generation=? AND status<>'published'
                """,
                (
                    event_json,
                    detail_json,
                    int(has_reliable_person(merged_event)),
                    timestamp,
                    timestamp,
                    row["task_key"],
                    row["generation"],
                ),
            )
            row = connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key=?",
                (row["task_key"],),
            ).fetchone()
        self._wake.set()
        return _public_task(row) if row is not None else None

    def retry(self, *, match_id: str, event_key: str) -> dict[str, Any] | None:
        """Put one existing OCR draft task at the front of the queue."""
        normalized_match = str(match_id).strip()
        normalized_event = str(event_key).strip()
        if not re.fullmatch(r"\d{1,20}", normalized_match):
            raise ValueError("重新创建草稿需要有效的数字比赛 ID")
        if not normalized_event:
            raise ValueError("重新创建草稿缺少事件标识")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM article_delivery_tasks
                WHERE match_id=? AND event_key=? AND artifact_kind=? AND delivery_mode=?
                ORDER BY updated_at_unix DESC LIMIT 1
                """,
                (
                    normalized_match,
                    normalized_event,
                    OCR_ARTIFACT_KIND,
                    DRAFT_DELIVERY_MODE,
                ),
            ).fetchone()
            if row is None:
                return None
            if str(row["status"] or "") in {"failed", "retry_wait"}:
                connection.execute(
                    """
                    UPDATE article_delivery_tasks
                    SET status='queued', stage='manual_retry', next_attempt_at_unix=NULL,
                        lease_until_unix=NULL, lease_token=NULL, retriable=0,
                        auth_required=0, error_code=NULL, error=NULL,
                        updated_at_unix=?
                    WHERE task_key=? AND status IN ('failed', 'retry_wait')
                    """,
                    (time.time(), row["task_key"]),
                )
                row = connection.execute(
                    "SELECT * FROM article_delivery_tasks WHERE task_key=?",
                    (row["task_key"],),
                ).fetchone()
        self._wake.set()
        return _public_task(row) if row is not None else None

    def run_once(self, *, now: float | None = None) -> bool:
        if self.publisher is None:
            raise RuntimeError("草稿队列缺少文章发布器")
        claimed_at = time.time() if now is None else float(now)
        row = self._claim_due(claimed_at)
        if row is None:
            return False
        task = dict(row)
        operation = "draft"
        final_event: dict[str, Any] | None = None
        publish_reason: str | None = None
        try:
            event, detail = self._reload_claim_payload(task)
            if not isinstance(event, dict) or not isinstance(detail, dict):
                raise ValueError("草稿任务中的比赛信息损坏")
            eligibility = _json_object(task.get("eligibility_json"))
            if not isinstance(eligibility, dict):
                self._hold_claim(
                    task,
                    reason_code="metadata_missing",
                    reason="旧文章任务没有保存 OCR 定位和覆盖范围元数据，无法确认事件画面是否在片段中。",
                    now=claimed_at,
                )
                return True
            if not bool(eligibility.get("eligible")):
                self._hold_claim(
                    task,
                    reason_code=str(eligibility.get("reason_code") or "auto_publish_not_eligible"),
                    reason=str(eligibility.get("reason") or "OCR GIF 不符合自动发布条件"),
                    now=claimed_at,
                )
                return True
            source_path = self._stage_source(task)
            if not self._claim_owned(task):
                return True
            event, detail = self._reload_claim_payload(task)
            article_id = str(task.get("article_id") or "").strip()
            draft_created_at = _optional_float(task.get("draft_created_at_unix"))
            deadline = _optional_float(task.get("person_deadline_at_unix"))
            if article_id:
                event = self._load_latest_event(task, event)
            person_available = has_reliable_person(event)
            # New events without a player stay local until the event API fills
            # the name.  Do not create a platform draft (status=0) just to
            # reserve an article ID; only legacy rows with article_id keep the
            # old draft-update path below.
            if (
                not article_id
                and not person_available
                and deadline is not None
                and self.latest_event_loader is not None
            ):
                # Once the local wait has started, use the optional loader on
                # each due check so a newly enriched API event can publish
                # immediately, including at the deadline boundary.
                event = self._load_latest_event(task, event)
                person_available = has_reliable_person(event)
            if not article_id and not person_available:
                if deadline is None:
                    deadline = claimed_at + self.person_wait_seconds
                if claimed_at < deadline:
                    self._return_to_person_wait(task, deadline=deadline, now=claimed_at)
                    return True
                final_event = _event_with_team_fallback(event, detail)
                operation = "publish"
                publish_reason = "team_fallback"
                self._set_claim_phase(task, "publishing", now=claimed_at)
                result = self.publisher.create_or_update_article(
                    match_id=str(task["match_id"]),
                    event=final_event,
                    match_detail=detail,
                    source_path=source_path,
                    delivery_mode="publish",
                )
                self._record_published(
                    task,
                    result,
                    event=final_event,
                    publish_reason=str(publish_reason),
                    now=claimed_at,
                )
                return True
            if article_id and draft_created_at is None and not person_available:
                self._set_claim_phase(task, "creating_draft", now=claimed_at)
                result = self.publisher.create_or_update_draft(
                    match_id=str(task["match_id"]),
                    event=event,
                    match_detail=detail,
                    source_path=source_path,
                    archive_id=article_id,
                )
            elif article_id:
                if not person_available and deadline is not None and claimed_at < deadline:
                    self._return_to_person_wait(task, deadline=deadline, now=claimed_at)
                    return True
                operation = "publish"
                publish_reason = (
                    "person_available" if person_available else "team_fallback"
                )
                final_event = (
                    dict(event)
                    if person_available
                    else _event_with_team_fallback(event, detail)
                )
                self._set_claim_phase(task, "publishing", now=claimed_at)
                result = self.publisher.publish_draft(
                    match_id=str(task["match_id"]),
                    event=final_event,
                    match_detail=detail,
                    source_path=source_path,
                    archive_id=article_id,
                )
            elif person_available:
                operation = "publish"
                publish_reason = "person_available"
                final_event = dict(event)
                self._set_claim_phase(task, "publishing", now=claimed_at)
                result = self.publisher.create_or_update_article(
                    match_id=str(task["match_id"]),
                    event=final_event,
                    match_detail=detail,
                    source_path=source_path,
                    delivery_mode="publish",
                )
            else:
                # Defensive fallback for a task whose state changed while it
                # was being claimed. New no-player tasks must never create a
                # status=0 platform draft.
                deadline = deadline or claimed_at + self.person_wait_seconds
                self._return_to_person_wait(task, deadline=deadline, now=claimed_at)
                return True
        except ArticlePublishError as exc:
            self._record_failure(
                task, exc, now=time.time() if now is None else claimed_at
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            wrapped = ArticlePublishError(
                _friendly_draft_error("source_check", str(exc)),
                code="draft_source_unavailable",
                stage="source_check",
            )
            self._record_failure(
                task, wrapped, now=time.time() if now is None else claimed_at
            )
        except Exception as exc:
            wrapped = ArticlePublishError(
                _friendly_draft_error("internal", str(exc)),
                code="draft_internal_error",
                stage="internal",
                status_code=500,
                retriable=True,
            )
            self._record_failure(
                task, wrapped, now=time.time() if now is None else claimed_at
            )
        else:
            completed_at = time.time() if now is None else claimed_at
            if operation == "publish" and final_event is not None:
                self._record_published(
                    task,
                    result,
                    event=final_event,
                    publish_reason=str(publish_reason),
                    now=completed_at,
                )
            else:
                # The name window starts only after the platform confirms that
                # the draft exists. Synthetic `now` values keep tests deterministic.
                self._record_draft_created(task, result, now=completed_at)
        return True

    def _hold_claim(
        self,
        task: dict[str, Any],
        *,
        reason_code: str,
        reason: str,
        now: float,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status='held', stage='publication_gate',
                    next_attempt_at_unix=NULL, lease_until_unix=NULL, lease_token=NULL,
                    retriable=0, auth_required=0, error_code=?, error=?,
                    updated_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=?
                  AND status IN ('creating', 'creating_draft', 'publishing')
                """,
                (
                    reason_code,
                    reason,
                    now,
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )

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
                eligibility_json TEXT,
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
                platform_status_code INTEGER,
                diagnostics_json TEXT,
                draft_created_at_unix REAL,
                person_deadline_at_unix REAL,
                published_at_unix REAL,
                publish_reason TEXT,
                final_event_json TEXT,
                UNIQUE(match_id, event_key, artifact_kind, delivery_mode)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS article_delivery_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        if self.auto_publish_after_unix is None:
            connection.execute(
                "INSERT OR IGNORE INTO article_delivery_meta(key, value) VALUES (?, ?)",
                ("auto_publish_after_unix", str(time.time())),
            )
            marker = connection.execute(
                "SELECT value FROM article_delivery_meta WHERE key=?",
                ("auto_publish_after_unix",),
            ).fetchone()
            try:
                self.auto_publish_after_unix = float(marker["value"])
            except (TypeError, ValueError):
                self.auto_publish_after_unix = time.time()
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(article_delivery_tasks)")
        }
        for name, definition in (
            ("generation", "INTEGER NOT NULL DEFAULT 1"),
            ("eligibility_json", "TEXT"),
            ("lease_token", "TEXT"),
            ("previous_staged_path", "TEXT"),
            ("platform_status_code", "INTEGER"),
            ("diagnostics_json", "TEXT"),
            ("draft_created_at_unix", "REAL"),
            ("person_deadline_at_unix", "REAL"),
            ("published_at_unix", "REAL"),
            ("publish_reason", "TEXT"),
            ("final_event_json", "TEXT"),
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
        connection.commit()
        return connection

    def _claim_due(self, now: float) -> sqlite3.Row | None:
        publish_after = self.auto_publish_after_unix or 0.0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM article_delivery_tasks
                WHERE (
                    status = 'queued'
                    OR (status = 'retry_wait' AND COALESCE(next_attempt_at_unix, 0) <= ?)
                    OR (status = 'waiting_person' AND COALESCE(next_attempt_at_unix, 0) <= ?)
                    OR (
                        status IN ('creating', 'creating_draft', 'publishing')
                        AND COALESCE(lease_until_unix, 0) <= ?
                    )
                )
                AND created_at_unix >= ?
                ORDER BY
                    CASE
                        WHEN status IN ('creating', 'creating_draft', 'publishing') THEN 0
                        WHEN status='waiting_person' THEN 1
                        WHEN status='queued' THEN 2
                        ELSE 3
                    END,
                    COALESCE(next_attempt_at_unix, created_at_unix), created_at_unix
                LIMIT 1
                """,
                (now, now, now, publish_after),
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
                    OR (status='waiting_person' AND COALESCE(next_attempt_at_unix, 0) <= ?)
                    OR (
                        status IN ('creating', 'creating_draft', 'publishing')
                        AND COALESCE(lease_until_unix, 0) <= ?
                    )
                )
                AND created_at_unix >= ?
                """,
                (
                    now + self.lease_seconds,
                    lease_token,
                    now,
                    row["task_key"],
                    row["generation"],
                    now,
                    now,
                    now,
                    publish_after,
                ),
            )
            if updated.rowcount != 1:
                return None
            return connection.execute(
                "SELECT * FROM article_delivery_tasks WHERE task_key=? AND lease_token=?",
                (row["task_key"], lease_token),
            ).fetchone()

    def _reload_claim_payload(
        self, task: dict[str, Any]
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM article_delivery_tasks "
                "WHERE task_key=? AND generation=? AND lease_token=? "
                "AND status IN ('creating', 'creating_draft', 'publishing')",
                (task["task_key"], task["generation"], task["lease_token"]),
            ).fetchone()
        if row is None:
            raise ValueError("文章任务租约已经失效")
        task.update(dict(row))
        event = json.loads(str(task["event_json"]))
        detail = json.loads(str(task["match_detail_json"]))
        if not isinstance(event, dict) or not isinstance(detail, dict):
            raise ValueError("文章任务中的比赛信息损坏")
        return event, detail

    def _load_latest_event(
        self, task: dict[str, Any], current_event: dict[str, Any]
    ) -> dict[str, Any]:
        if self.latest_event_loader is None:
            return current_event
        try:
            latest = self.latest_event_loader(
                str(task["match_id"]),
                str(task["event_key"]),
                dict(current_event),
            )
        except Exception:
            return current_event
        if not isinstance(latest, dict):
            return current_event
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT event_json FROM article_delivery_tasks "
                "WHERE task_key=? AND generation=? AND lease_token=? "
                "AND status IN ('creating', 'creating_draft', 'publishing')",
                (task["task_key"], task["generation"], task["lease_token"]),
            ).fetchone()
            stored_event = (
                _json_object(current["event_json"])
                if current is not None
                else current_event
            )
            latest_event = merge_event_metadata(stored_event or {}, latest)
            latest_event["event_key"] = str(task["event_key"])
            event_json = json.dumps(
                latest_event,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            connection.execute(
                "UPDATE article_delivery_tasks SET event_json=?, updated_at_unix=? "
                "WHERE task_key=? AND generation=? AND lease_token=? "
                "AND status IN ('creating', 'creating_draft', 'publishing')",
                (
                    event_json,
                    time.time(),
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )
        task["event_json"] = event_json
        return latest_event

    def _set_claim_phase(self, task: dict[str, Any], status: str, *, now: float) -> None:
        if status not in {"creating_draft", "publishing"}:
            raise ValueError("不支持的文章处理阶段")
        with self._connect() as connection:
            updated = connection.execute(
                "UPDATE article_delivery_tasks SET status=?, stage=?, updated_at_unix=? "
                "WHERE task_key=? AND generation=? AND lease_token=? "
                "AND status IN ('creating', 'creating_draft', 'publishing')",
                (
                    status,
                    status,
                    now,
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )
        if updated.rowcount != 1:
            raise ValueError("文章任务租约已经失效")
        task["status"] = status
        task["stage"] = status

    def _return_to_person_wait(
        self, task: dict[str, Any], *, deadline: float, now: float
    ) -> None:
        previous_staged_path = task.get("previous_staged_path")
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status='waiting_person', stage='waiting_person',
                    next_attempt_at_unix=?, lease_until_unix=NULL, lease_token=NULL,
                    previous_staged_path=NULL,
                    person_deadline_at_unix=COALESCE(person_deadline_at_unix, ?),
                    updated_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=?
                  AND status IN ('creating', 'creating_draft', 'publishing')
                """,
                (
                    (
                        min(deadline, now + self.person_wait_seconds / 2.0)
                        if self.latest_event_loader is not None and now < deadline
                        else deadline
                    ),
                    deadline,
                    now,
                    task["task_key"],
                    task["generation"],
                    task["lease_token"],
                ),
            )
        if updated.rowcount == 1:
            self._delete_previous_staged_if_unused(previous_staged_path)

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
                WHERE task_key=? AND generation=? AND lease_token=?
                  AND status IN ('creating', 'creating_draft', 'publishing')
                """,
                (task["task_key"], task["generation"], task["lease_token"]),
            ).fetchone()
        return row is not None

    def _record_draft_created(
        self, task: dict[str, Any], result: dict[str, Any], *, now: float
    ) -> None:
        gif = result.get("gif") if isinstance(result.get("gif"), dict) else {}
        deadline = now + self.person_wait_seconds
        with self._connect() as connection:
            current = connection.execute(
                "SELECT event_json FROM article_delivery_tasks "
                "WHERE task_key=? AND generation=? AND lease_token=?",
                (task["task_key"], task["generation"], task["lease_token"]),
            ).fetchone()
            latest_event = (
                _json_object(current["event_json"])
                if current is not None
                else None
            )
            if has_reliable_person(latest_event):
                next_attempt = now
            elif self.latest_event_loader is not None:
                next_attempt = min(deadline, now + self.person_wait_seconds / 2.0)
            else:
                next_attempt = deadline
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status='waiting_person', stage='waiting_person',
                    article_id=?, artifact_sha256=?,
                    staged_path=?, gif_url=?, platform_code=?, duplicate=?,
                    platform_status_code=?, diagnostics_json=?,
                    next_attempt_at_unix=?, lease_until_unix=NULL, lease_token=NULL,
                    retriable=0, auth_required=0, error_code=NULL, error=NULL,
                    previous_staged_path=NULL, draft_created_at_unix=?,
                    person_deadline_at_unix=?, updated_at_unix=?, completed_at_unix=NULL
                WHERE task_key=? AND generation=? AND lease_token=?
                  AND status IN ('creating', 'creating_draft')
                """,
                (
                    str(result["article_id"]),
                    gif.get("gif_id") or task.get("artifact_sha256"),
                    gif.get("path") or task.get("staged_path"),
                    gif.get("url") or task.get("gif_url"),
                    None
                    if result.get("platform_code") is None
                    else str(result.get("platform_code")),
                    int(bool(result.get("duplicate"))),
                    _diagnostic_value(result.get("diagnostics"), "http_status"),
                    _json_value(result.get("diagnostics")),
                    next_attempt,
                    now,
                    deadline,
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

    def _record_published(
        self,
        task: dict[str, Any],
        result: dict[str, Any],
        *,
        event: dict[str, Any],
        publish_reason: str,
        now: float,
    ) -> None:
        gif = result.get("gif") if isinstance(result.get("gif"), dict) else {}
        final_event_json = json.dumps(
            event, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        with self._connect() as connection:
            updated = connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status='published', stage='published', article_id=?,
                    artifact_sha256=?, staged_path=?, gif_url=?, platform_code=?,
                    duplicate=?, platform_status_code=?, diagnostics_json=?,
                    next_attempt_at_unix=NULL, lease_until_unix=NULL, lease_token=NULL,
                    retriable=0, auth_required=0, error_code=NULL, error=NULL,
                    previous_staged_path=NULL, published_at_unix=?, publish_reason=?,
                    final_event_json=?, updated_at_unix=?, completed_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=?
                  AND status IN ('creating', 'publishing')
                """,
                (
                    str(result["article_id"]),
                    gif.get("gif_id") or task.get("artifact_sha256"),
                    gif.get("path") or task.get("staged_path"),
                    gif.get("url") or task.get("gif_url"),
                    None
                    if result.get("platform_code") is None
                    else str(result.get("platform_code")),
                    int(bool(result.get("duplicate"))),
                    _diagnostic_value(result.get("diagnostics"), "http_status"),
                    _json_value(result.get("diagnostics")),
                    now,
                    publish_reason,
                    final_event_json,
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

    # Kept for callers and migrations that still use the old helper name.
    def _record_success(
        self, task: dict[str, Any], result: dict[str, Any], *, now: float
    ) -> None:
        self._record_draft_created(task, result, now=now)

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
        friendly_error = _friendly_draft_error(
            exc.stage,
            str(exc),
            platform_code=exc.platform_code,
            publishing=str(task.get("status") or "") == "publishing",
        )
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE article_delivery_tasks
                SET status=?, stage=?, next_attempt_at_unix=?, lease_until_unix=NULL,
                    lease_token=NULL, retriable=?, auth_required=?, error_code=?, error=?,
                    platform_code=?, platform_status_code=?, diagnostics_json=?,
                    updated_at_unix=?
                WHERE task_key=? AND generation=? AND lease_token=?
                  AND status IN ('creating', 'creating_draft', 'publishing')
                """,
                (
                    status,
                    exc.stage,
                    next_attempt,
                    int(retriable),
                    int(exc.auth_required),
                    exc.code,
                    friendly_error,
                    None if exc.platform_code is None else str(exc.platform_code),
                    _diagnostic_value(exc.diagnostics, "http_status"),
                    _json_value(exc.diagnostics),
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


def _json_value(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _optional_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _diagnostic_value(value: Any, key: str) -> Any:
    return value.get(key) if isinstance(value, dict) else None


def _friendly_draft_error(
    stage: str,
    detail: str,
    *,
    platform_code: Any = None,
    publishing: bool = False,
) -> str:
    cleaned = str(detail or "").strip()
    if stage == "authorization":
        return "懂球帝开放平台尚未授权或授权已过期；授权恢复后会继续创建草稿。"
    if stage == "public_url_check":
        return "OCR GIF 已生成，但公网地址暂时无法访问；系统会稍后重试。"
    if stage == "platform_publish":
        code_text = (
            f"（平台返回 code={platform_code}）"
            if platform_code is not None
            else ""
        )
        action = "正式发布" if publishing else "草稿创建"
        return (
            f"OCR GIF 已生成，但懂球帝{action}接口没有接受请求"
            f"{code_text}：{cleaned or '未说明原因'}"
        )
    if stage in {"gif_validation", "gif_storage", "source_check"}:
        return f"未创建草稿：{cleaned or 'OCR GIF 文件无法使用'}"
    action = "正式发布" if publishing else "草稿创建"
    return f"{action}暂时失败：{cleaned or '未说明原因'}"


def _public_task(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    article_id = value.get("article_id")
    publication_eligibility = _json_object(value.get("eligibility_json"))
    diagnostics = _json_object(value.get("diagnostics_json"))
    request_summary = (
        diagnostics.get("request_summary")
        if isinstance(diagnostics, dict)
        else None
    )
    publish_account = None
    if isinstance(request_summary, dict):
        user_id = request_summary.get("user_id")
        user_name = str(request_summary.get("user_name") or "").strip()
        if user_id is not None or user_name:
            publish_account = {"user_id": user_id, "user_name": user_name}
    return {
        "task_key": value.get("task_key"),
        "match_id": value.get("match_id"),
        "event_key": value.get("event_key"),
        "artifact_kind": value.get("artifact_kind"),
        "delivery_mode": value.get("delivery_mode"),
        "quality_label": value.get("quality_label"),
        "publication_eligibility": publication_eligibility,
        # Keep the original key for Dashboard/API clients that were deployed
        # before the publication terminology was standardized.
        "ocr_article_eligibility": publication_eligibility,
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
        "platform_code": value.get("platform_code"),
        "platform_status_code": value.get("platform_status_code"),
        "diagnostics": diagnostics,
        "publish_account": publish_account,
        "gif_url": value.get("gif_url"),
        "draft_created_at_unix": value.get("draft_created_at_unix"),
        "person_deadline_at_unix": value.get("person_deadline_at_unix"),
        "published_at_unix": value.get("published_at_unix"),
        "publish_reason": value.get("publish_reason"),
        "final_event": _json_object(value.get("final_event_json")),
        "updated_at_unix": value.get("updated_at_unix"),
        "completed_at_unix": value.get("completed_at_unix"),
    }
