"""Publish an already generated default GIF as a Dongqiudi GIF article."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable

from open_platform_client import OpenPlatformClient, OpenPlatformError


GIF_ID_PATTERN = re.compile(r"^[a-f0-9]{64}$")
GIF_HEADERS = {b"GIF87a", b"GIF89a"}
EVENT_TYPES = {"G": 1, "PG": 2, "RC": 4, "OG": 5}
EVENT_LABELS = {
    "G": "进球",
    "PG": "点球破门",
    "OG": "乌龙球",
    "RC": "红牌",
    "YC": "黄牌",
}


class ArticlePublishError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        stage: str,
        status_code: int = 422,
        auth_required: bool = False,
        retriable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.stage = stage
        self.status_code = status_code
        self.auth_required = auth_required
        self.retriable = retriable


def inspect_animated_gif(body: bytes) -> dict[str, Any]:
    """Validate GIF structure and require at least two image frames."""
    if len(body) < 14 or body[:6] not in GIF_HEADERS:
        raise ArticlePublishError(
            "文件不是标准 GIF",
            code="publish_gif_invalid",
            stage="gif_validation",
        )
    offset = 13
    global_packed = body[10]
    if global_packed & 0x80:
        offset += 3 * (2 ** ((global_packed & 0x07) + 1))
    if offset > len(body):
        raise ArticlePublishError(
            "GIF 全局颜色表不完整",
            code="publish_gif_invalid",
            stage="gif_validation",
        )

    frames = 0
    trailer = False
    while offset < len(body):
        introducer = body[offset]
        offset += 1
        if introducer == 0x3B:
            trailer = True
            break
        if introducer == 0x21:
            if offset >= len(body):
                break
            offset += 1  # Extension label.
            offset = _skip_gif_sub_blocks(body, offset)
            continue
        if introducer == 0x2C:
            if offset + 9 > len(body):
                break
            image_packed = body[offset + 8]
            offset += 9
            if image_packed & 0x80:
                offset += 3 * (2 ** ((image_packed & 0x07) + 1))
            if offset >= len(body):
                break
            offset += 1  # LZW minimum code size.
            offset = _skip_gif_sub_blocks(body, offset)
            frames += 1
            continue
        raise ArticlePublishError(
            "GIF 包含无法识别的数据块",
            code="publish_gif_invalid",
            stage="gif_validation",
        )
    if not trailer:
        raise ArticlePublishError(
            "GIF 缺少结束标记",
            code="publish_gif_invalid",
            stage="gif_validation",
        )
    if frames < 2:
        raise ArticlePublishError(
            "文件不是动画 GIF",
            code="publish_gif_not_animated",
            stage="gif_validation",
        )
    return {"animated": True, "frame_count": frames, "header": body[:6].decode("ascii")}


def _skip_gif_sub_blocks(body: bytes, offset: int) -> int:
    while offset < len(body):
        length = body[offset]
        offset += 1
        if length == 0:
            return offset
        if offset + length > len(body):
            raise ArticlePublishError(
                "GIF 数据块不完整",
                code="publish_gif_invalid",
                stage="gif_validation",
            )
        offset += length
    raise ArticlePublishError(
        "GIF 数据块缺少结束标记",
        code="publish_gif_invalid",
        stage="gif_validation",
    )


class PublishedGifStore:
    def __init__(
        self,
        directory: Path,
        public_origin: str,
        *,
        max_bytes: int = 50 * 1024 * 1024,
    ) -> None:
        self.directory = directory.expanduser().resolve()
        self.public_origin = _normalize_public_origin(public_origin)
        self.max_bytes = max_bytes

    def path_for(self, gif_id: str) -> Path:
        if not GIF_ID_PATTERN.fullmatch(gif_id):
            raise ArticlePublishError(
                "永久 GIF 文件 ID 无效",
                code="publish_gif_id_invalid",
                stage="gif_storage",
                status_code=404,
            )
        return self.directory / f"{gif_id}.gif"

    def url_for(self, gif_id: str) -> str:
        self.path_for(gif_id)
        return f"{self.public_origin}/publish-gifs/{gif_id}.gif"

    def create(self, source_path: Path) -> dict[str, Any]:
        try:
            body = source_path.read_bytes()
        except OSError as exc:
            raise ArticlePublishError(
                f"无法读取默认 GIF：{exc}",
                code="default_gif_unreadable",
                stage="gif_validation",
                status_code=404 if not source_path.exists() else 500,
            ) from exc
        if not body:
            raise ArticlePublishError(
                "默认 GIF 是空文件",
                code="default_gif_empty",
                stage="gif_validation",
            )
        if len(body) > self.max_bytes:
            raise ArticlePublishError(
                f"默认 GIF 超过发布上限（{len(body)} > {self.max_bytes} 字节）",
                code="publish_gif_too_large",
                stage="gif_validation",
            )
        gif_info = inspect_animated_gif(body)
        gif_id = hashlib.sha256(body).hexdigest()
        target = self.path_for(gif_id)
        try:
            self.directory.mkdir(parents=True, exist_ok=True, mode=0o755)
            if not target.exists():
                temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
                temporary.write_bytes(body)
                os.chmod(temporary, 0o644)
                os.replace(temporary, target)
        except OSError as exc:
            raise ArticlePublishError(
                f"永久 GIF 目录不可写：{exc}",
                code="publish_gif_storage_unavailable",
                stage="gif_storage",
                status_code=503,
            ) from exc
        return {
            "gif_id": gif_id,
            "path": str(target),
            "url": self.url_for(gif_id),
            "bytes": len(body),
            **gif_info,
        }


class ArticlePublisher:
    def __init__(
        self,
        *,
        platform_client: OpenPlatformClient,
        gif_store: PublishedGifStore,
        database_path: Path,
        verify_public_url: bool = True,
        public_url_checker: Callable[[str], None] | None = None,
    ) -> None:
        self.platform_client = platform_client
        self.gif_store = gif_store
        self.database_path = database_path.expanduser().resolve()
        self.verify_public_url = verify_public_url
        self.public_url_checker = public_url_checker or _check_public_gif_url
        self._lock = threading.RLock()

    def status(self) -> dict[str, Any]:
        return {
            "archive_level": "B",
            "add_to_tab": 1,
            "type": "article",
            "style": "gif",
            "default_account": True,
            "public_origin": self.gif_store.public_origin,
            "public_origin_https": self.gif_store.public_origin.startswith("https://"),
            "verify_public_url": self.verify_public_url,
            "gif_directory": str(self.gif_store.directory),
            "database_path": str(self.database_path),
            "oauth": self.platform_client.status(),
        }

    def records_for_match(self, match_id: str) -> dict[str, dict[str, Any]]:
        if not self.database_path.exists():
            return {}
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT * FROM article_publish_records
                    WHERE match_id = ?
                    ORDER BY updated_at_unix DESC
                    """,
                    (match_id,),
                ).fetchall()
        except sqlite3.Error:
            return {}
        records: dict[str, dict[str, Any]] = {}
        for row in rows:
            event_key = str(row["event_key"])
            records.setdefault(event_key, _public_record(row))
        return records

    def publish(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any],
        source_path: Path,
    ) -> dict[str, Any]:
        event_key = str(event.get("event_key") or "").strip()
        if not event_key:
            raise ArticlePublishError(
                "事件缺少稳定标识，无法防止重复发布",
                code="publish_event_key_missing",
                stage="request_validation",
            )
        if str(event.get("status")) != "encoded":
            raise ArticlePublishError(
                "默认 GIF 尚未生成完成",
                code="default_gif_not_ready",
                stage="request_validation",
                status_code=409,
            )

        with self._lock:
            gif = self.gif_store.create(source_path)
            stable_id = hashlib.sha256(
                f"{match_id}\n{event_key}\n{gif['gif_id']}".encode("utf-8")
            ).hexdigest()
            previous = self._get_record(stable_id)
            if previous and previous.get("status") == "success":
                previous["idempotent_replay"] = True
                return previous

            title = build_article_title(event, match_detail)
            fields = build_article_fields(
                match_id=match_id,
                event=event,
                gif_url=str(gif["url"]),
                title=title,
            )
            self._save_record(
                stable_id=stable_id,
                match_id=match_id,
                event_key=event_key,
                gif=gif,
                title=title,
                status="prepared",
                stage="gif_prepared",
            )
            try:
                if self.verify_public_url:
                    self.public_url_checker(str(gif["url"]))
                self._save_record(
                    stable_id=stable_id,
                    match_id=match_id,
                    event_key=event_key,
                    gif=gif,
                    title=title,
                    status="publishing",
                    stage="platform_publish",
                )
                result = self.platform_client.create_article(fields)
            except ArticlePublishError as exc:
                self._save_failure(stable_id, exc.stage, str(exc))
                raise
            except OpenPlatformError as exc:
                stage = "authorization" if exc.auth_required else "platform_publish"
                self._save_failure(stable_id, stage, str(exc), platform_code=exc.code)
                raise ArticlePublishError(
                    str(exc),
                    code="open_platform_auth_required" if exc.auth_required else "open_platform_rejected",
                    stage=stage,
                    status_code=exc.status_code,
                    auth_required=exc.auth_required,
                    retriable=exc.retriable,
                ) from exc

            published_at = time.time()
            self._save_record(
                stable_id=stable_id,
                match_id=match_id,
                event_key=event_key,
                gif=gif,
                title=title,
                status="success",
                stage="completed",
                article_id=str(result["article_id"]),
                platform_code=result.get("code"),
                duplicate=bool(result.get("duplicate")),
                published_at_unix=published_at,
            )
            record = self._get_record(stable_id)
            if record is None:
                raise ArticlePublishError(
                    "文章已经发布，但本地发布记录读取失败",
                    code="publish_record_unreadable",
                    stage="record_persistence",
                    status_code=500,
                )
            return record

    def _connect(self) -> sqlite3.Connection:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.database_path, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS article_publish_records (
                stable_id TEXT PRIMARY KEY,
                match_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                gif_sha256 TEXT NOT NULL,
                gif_url TEXT NOT NULL,
                gif_path TEXT NOT NULL,
                title TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT NOT NULL,
                article_id TEXT,
                platform_code TEXT,
                duplicate INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                created_at_unix REAL NOT NULL,
                updated_at_unix REAL NOT NULL,
                published_at_unix REAL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS article_publish_match_event
            ON article_publish_records(match_id, event_key, updated_at_unix)
            """
        )
        return connection

    def _get_record(self, stable_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM article_publish_records WHERE stable_id = ?",
                (stable_id,),
            ).fetchone()
        return _public_record(row) if row else None

    def _save_record(
        self,
        *,
        stable_id: str,
        match_id: str,
        event_key: str,
        gif: dict[str, Any],
        title: str,
        status: str,
        stage: str,
        article_id: str | None = None,
        platform_code: Any = None,
        duplicate: bool = False,
        published_at_unix: float | None = None,
    ) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO article_publish_records (
                    stable_id, match_id, event_key, gif_sha256, gif_url, gif_path,
                    title, status, stage, article_id, platform_code, duplicate,
                    error, created_at_unix, updated_at_unix, published_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
                ON CONFLICT(stable_id) DO UPDATE SET
                    status=excluded.status,
                    stage=excluded.stage,
                    article_id=COALESCE(excluded.article_id, article_publish_records.article_id),
                    platform_code=COALESCE(excluded.platform_code, article_publish_records.platform_code),
                    duplicate=excluded.duplicate,
                    error=NULL,
                    updated_at_unix=excluded.updated_at_unix,
                    published_at_unix=COALESCE(excluded.published_at_unix, article_publish_records.published_at_unix)
                """,
                (
                    stable_id,
                    match_id,
                    event_key,
                    gif["gif_id"],
                    gif["url"],
                    gif["path"],
                    title,
                    status,
                    stage,
                    article_id,
                    None if platform_code is None else str(platform_code),
                    int(duplicate),
                    now,
                    now,
                    published_at_unix,
                ),
            )

    def _save_failure(
        self,
        stable_id: str,
        stage: str,
        error: str,
        *,
        platform_code: Any = None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE article_publish_records
                SET status='failed', stage=?, error=?, platform_code=?, updated_at_unix=?
                WHERE stable_id=?
                """,
                (
                    stage,
                    error,
                    None if platform_code is None else str(platform_code),
                    time.time(),
                    stable_id,
                ),
            )


def build_article_title(event: dict[str, Any], detail: dict[str, Any]) -> str:
    minute = _match_time(event)
    minute_text = f"{minute}分钟" if minute else "比赛中"
    person = str(event.get("person") or "").strip()
    code = str(event.get("code") or "").upper()
    action = EVENT_LABELS.get(code, "比赛事件")
    home = str(detail.get("team_A_name") or "主队").strip()
    away = str(detail.get("team_B_name") or "客队").strip()
    score = _match_score(event.get("score"))
    actor = person or _event_team_name(event.get("team"), detail)
    action_text = f"{actor}{action}" if actor else action
    score_text = f"，{home} {score} {away}" if score else f"，{home}对阵{away}"
    return f"{minute_text}，{action_text}{score_text}"[:100]


def build_article_fields(
    *,
    match_id: str,
    event: dict[str, Any],
    gif_url: str,
    title: str,
) -> dict[str, Any]:
    if not re.fullmatch(r"\d{1,20}", match_id):
        raise ArticlePublishError(
            "正式发布需要有效的数字比赛 ID",
            code="publish_match_id_invalid",
            stage="request_validation",
            status_code=400,
        )
    if not gif_url.startswith("https://"):
        raise ArticlePublishError(
            "正式发布的 GIF 公网地址必须使用 HTTPS",
            code="publish_gif_https_required",
            stage="public_url_check",
            status_code=503,
        )
    code = str(event.get("code") or "").upper()
    fields: dict[str, Any] = {
        "title": title,
        "body": (
            f"<p>{html.escape(title)}</p>"
            f'<p><img src="{html.escape(gif_url, quote=True)}" alt="比赛 GIF"></p>'
        ),
        "archive_level": "B",
        "status": 1,
        "type": "article",
        "style": "gif",
        "match_id": match_id,
        "add_to_tab": 1,
    }
    match_time = _match_time(event)
    if match_time:
        fields["match_time"] = match_time
    if code in EVENT_TYPES:
        fields["match_event"] = EVENT_TYPES[code]
    score = _match_score(event.get("score"))
    if score:
        fields["match_score"] = score
    return fields


def _match_time(event: dict[str, Any]) -> str:
    minute = str(event.get("minute") or "").strip().rstrip("'")
    extra = str(event.get("minute_extra") or "").strip()
    if not re.fullmatch(r"\d{1,3}", minute):
        return ""
    if extra and extra != "0" and re.fullmatch(r"\d{1,2}", extra):
        return f"{minute}+{extra}"
    return minute


def _match_score(value: Any) -> str:
    score = str(value or "").strip().replace(":", "-").replace("–", "-")
    match = re.fullmatch(r"\s*(\d{1,2})\s*-\s*(\d{1,2})\s*", score)
    return f"{match.group(1)}-{match.group(2)}" if match else ""


def _event_team_name(value: Any, detail: dict[str, Any]) -> str:
    team = str(value or "").strip()
    normalized = team.lower().replace("-", "_")
    if normalized in {"teama", "team_a", "a", "home"}:
        return str(detail.get("team_A_name") or "").strip()
    if normalized in {"teamb", "team_b", "b", "away"}:
        return str(detail.get("team_B_name") or "").strip()
    return team


def _normalize_public_origin(value: str) -> str:
    origin = value.strip().rstrip("/")
    parsed = urllib.parse.urlsplit(origin)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise RuntimeError("GIF_PUBLIC_ORIGIN 必须是完整的 HTTP 或 HTTPS 地址")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def _check_public_gif_url(url: str) -> None:
    request = urllib.request.Request(
        url,
        method="HEAD",
        headers={"Accept": "image/gif", "User-Agent": "football-gif-publisher/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=8.0) as response:
            content_type = str(response.headers.get("Content-Type") or "")
            status = int(getattr(response, "status", 200))
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ArticlePublishError(
            f"公网 GIF 地址不可访问：{exc}",
            code="publish_gif_public_unreachable",
            stage="public_url_check",
            status_code=503,
            retriable=True,
        ) from exc
    if status != 200 or not content_type.lower().startswith("image/gif"):
        raise ArticlePublishError(
            f"公网 GIF 检查失败：HTTP {status}，Content-Type={content_type or '缺失'}",
            code="publish_gif_public_invalid",
            stage="public_url_check",
            status_code=503,
        )


def _public_record(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    return {
        "stable_id": value.get("stable_id"),
        "match_id": value.get("match_id"),
        "event_key": value.get("event_key"),
        "gif_url": value.get("gif_url"),
        "title": value.get("title"),
        "status": value.get("status"),
        "stage": value.get("stage"),
        "article_id": value.get("article_id"),
        "platform_code": value.get("platform_code"),
        "duplicate": bool(value.get("duplicate")),
        "error": value.get("error"),
        "updated_at_unix": value.get("updated_at_unix"),
        "published_at_unix": value.get("published_at_unix"),
    }


def environment_boolean(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} 必须是 true 或 false")
