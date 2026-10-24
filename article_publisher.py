"""Store and publish generated GIFs as Dongqiudi GIF articles."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Sequence

from open_platform_client import OpenPlatformClient, OpenPlatformError
from publish_account_pool import (
    MAX_USER_ID,
    PublishAccountPool,
    PublishAccountPoolError,
)


GIF_ID_PATTERN = re.compile(r"^[a-f0-9]{64}$")
GIF_HEADERS = {b"GIF87a", b"GIF89a"}
OFFICIAL_IMAGE_MAX_BYTES = 20 * 1024 * 1024
EVENT_TYPES = {"G": 1, "PG": 2, "RC": 4, "OG": 5}
EVENT_LABELS = {
    "G": "进球",
    "PG": "点球破门",
    "OG": "乌龙球",
    "RC": "红牌",
    "YC": "黄牌",
}
GOAL_EVENT_CODES = frozenset({"G", "PG", "OG"})


# Read at call time, not import time: both servers import this module before
# they load ``.env``, so a module-level constant would never see the file.
def article_add_to_tab() -> int:
    """Whether created articles appear in the news list (``1``) or not (``0``)."""
    value = os.environ.get("ARTICLE_ADD_TO_TAB", "0").strip()
    if value not in {"0", "1"}:
        raise RuntimeError("ARTICLE_ADD_TO_TAB 必须是 0 或 1")
    return int(value)


def article_channelsnew() -> str:
    """Secondary tag ids (``channelsnew``), comma separated; empty = not sent."""
    raw = os.environ.get("ARTICLE_CHANNELSNEW", "12039035")
    ids = [part.strip() for part in raw.split(",") if part.strip()]
    for item in ids:
        if not re.fullmatch(r"\d{1,20}", item):
            raise RuntimeError("ARTICLE_CHANNELSNEW 必须是逗号分隔的数字标签 id")
    return ",".join(ids)


def article_tabs() -> list[str]:
    """Column ids (``tabs[]``) from ``ARTICLE_TABS``; empty = not sent."""
    raw = os.environ.get("ARTICLE_TABS", "")
    ids = [part.strip() for part in raw.split(",") if part.strip()]
    for item in ids:
        if not re.fullmatch(r"\d{1,20}", item):
            raise RuntimeError("ARTICLE_TABS 必须是逗号分隔的数字栏目 id")
    return ids


LITPIC_SOURCES = frozenset({"gif", "cover"})


def article_litpic_source() -> str:
    """Which upload becomes the article cover (``litpic``).

    ``gif`` (default) sends the GIF's own FastDFS path so the platform shows the
    animation as the thumbnail, matching its native gif-collection articles.
    ``cover`` keeps the legacy first-frame JPEG. The JPEG is generated and
    uploaded either way because the official image endpoint pairs both files.
    """
    value = os.environ.get("ARTICLE_LITPIC_SOURCE", "gif").strip().lower()
    if value not in LITPIC_SOURCES:
        raise RuntimeError("ARTICLE_LITPIC_SOURCE 必须是 gif 或 cover")
    return value


CHANNELS_SOURCES = frozenset({"teams", "off"})
TABS_SOURCES = frozenset({"teams", "off"})
TEAM_CHANNEL_CACHE_TTL_SECONDS = 6 * 60 * 60
# Misses (platform error or no usable tag) are remembered briefly so an outage
# costs one search per team per window instead of two timeouts per article.
TEAM_CHANNEL_NEGATIVE_TTL_SECONDS = 120.0
TEAM_CHANNEL_TAG_TYPE = "球队"
# Match-directory team ids are the CMS tag row ``id`` plus this offset
# (verified on 18/18 teams: 50016400 → row id 16400 for 辽宁铁人, including
# teams whose name appears twice in the tag library).
TEAM_ID_TAG_OFFSET = 50_000_000


def article_channels_source() -> str:
    """Where the primary tags (``channels``) come from.

    ``teams`` (default) resolves the home and away teams to CMS tag ids and
    sends both. The platform merges these with the tags it derives from
    ``match_id`` and de-duplicates by tag id (verified with drafts), so a
    match that already had both teams is unchanged and one that had 0/1 gets
    filled in. ``off`` sends nothing, restoring the old behaviour.
    """
    value = os.environ.get("ARTICLE_CHANNELS_SOURCE", "teams").strip().lower()
    if value not in CHANNELS_SOURCES:
        raise RuntimeError("ARTICLE_CHANNELS_SOURCE 必须是 teams 或 off")
    return value


def article_tabs_source() -> str:
    """Where per-match columns (``tabs[]``) come from, on top of ``ARTICLE_TABS``.

    ``teams`` (default) asks ``admin-archive-playerteam`` for each resolved
    team tag and appends its ``new_table_id`` list, mirroring the CMS editor
    that ticks the competition/sport columns when a team tag is added.
    ``off`` sends only the fixed ``ARTICLE_TABS``.
    """
    value = os.environ.get("ARTICLE_TABS_SOURCE", "teams").strip().lower()
    if value not in TABS_SOURCES:
        raise RuntimeError("ARTICLE_TABS_SOURCE 必须是 teams 或 off")
    return value


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and re.fullmatch(r"[1-9]\d{0,19}", value.strip()):
        return int(value.strip())
    return None


def team_tag_row_id(team_id: Any) -> int | None:
    """Expected tag-library row ``id`` for a directory ``team_A_id``/``team_B_id``."""
    numeric = _positive_int(team_id)
    if numeric is None or numeric <= TEAM_ID_TAG_OFFSET:
        return None
    return numeric - TEAM_ID_TAG_OFFSET


def pick_team_channel(
    rows: list[dict[str, Any]], team_name: str, team_id: Any = None
) -> tuple[int, str] | None:
    """Pick the tag ``value`` for one team; returns ``(value, matched_by)``.

    1. ``id``: a 球队 row whose ``id`` equals ``team_id − 50000000``. This is
       the row the platform itself attaches for the match, so it is always
       the right one even when the library holds two tags with the same name
       (10 of 26 live teams on 2026-10-23 did), and it tolerates small name
       differences between the directory and the CMS.
    2. ``name``: only when no usable ``team_id`` or the id row is absent from
       the results. A 球队 row whose ``name`` equals the team exactly, and
       only if there is exactly one such row. Two same-name rows mean we
       cannot tell which the platform uses; sending the wrong one leaves the
       article with the team tagged twice, which is worse than not tagging.
    """
    team_rows = [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("type") or "").strip() == TEAM_CHANNEL_TAG_TYPE
        and _positive_int(row.get("value")) is not None
    ]
    expected_row_id = team_tag_row_id(team_id)
    if expected_row_id is not None:
        for row in team_rows:
            if _positive_int(row.get("id")) == expected_row_id:
                return _positive_int(row.get("value")), "id"
    wanted = str(team_name or "").strip()
    if not wanted:
        return None
    by_name = [
        row for row in team_rows if str(row.get("name") or "").strip() == wanted
    ]
    if len(by_name) != 1:
        return None
    return _positive_int(by_name[0].get("value")), "name"


def pick_team_tabs(payload: Any) -> list[int]:
    """Column ids from an ``admin-archive-playerteam`` payload, in order.

    ``new_table_id`` is the platform's recommended list (it already includes
    ``table_id`` plus related columns); ``table_id`` alone is the fallback for
    older rows. Anything that is not a positive int is dropped.
    """
    data = payload if isinstance(payload, dict) else {}
    raw = data.get("new_table_id")
    candidates = list(raw) if isinstance(raw, (list, tuple)) else []
    if not candidates:
        candidates = [data.get("table_id")]
    tabs: list[int] = []
    for item in candidates:
        numeric = _positive_int(item)
        if numeric is not None and numeric not in tabs:
            tabs.append(numeric)
    return tabs


class TeamChannelResolver:
    """Resolve teams to CMS tag ids with an in-process TTL cache.

    One match produces many articles, so each team is searched once per TTL.
    The cache key is the directory team id when present (it is the team's
    identity; the display name may differ between sources) and the name
    otherwise. Misses (platform error or no usable tag) are remembered for the
    short negative TTL and never raised: a missing tag is simply left out, and
    the article goes out with whatever the platform derives on its own.
    """

    def __init__(
        self,
        search: Callable[[str], list[dict[str, Any]]],
        player_team: Callable[[int], dict[str, Any]] | None = None,
        *,
        ttl_seconds: float = TEAM_CHANNEL_CACHE_TTL_SECONDS,
        negative_ttl_seconds: float = TEAM_CHANNEL_NEGATIVE_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._search = search
        self._player_team = player_team
        self._ttl = float(ttl_seconds)
        self._negative_ttl = float(negative_ttl_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        # value None = cached miss (search failed or no usable tag); it expires
        # on the shorter negative TTL so a platform outage or a late-added tag
        # is retried without pinning every article behind two timeouts.
        self._cache: dict[str, tuple[int | None, float, str]] = {}
        # Columns per resolved team tag id; None = cached miss, same TTLs.
        self._tabs_cache: dict[int, tuple[list[int] | None, float]] = {}

    def _remember(self, cache_key: str, value: int | None, now: float, reason: str) -> None:
        ttl = self._ttl if value is not None else self._negative_ttl
        with self._lock:
            self._cache[cache_key] = (value, now + ttl, reason)

    def resolve(self, team_name: str, team_id: Any = None) -> int | None:
        name = str(team_name or "").strip()
        if not name:
            return None
        numeric_team_id = _positive_int(team_id)
        cache_key = f"id:{numeric_team_id}" if numeric_team_id else f"name:{name}"
        now = self._clock()
        with self._lock:
            cached = self._cache.get(cache_key)
        if cached and cached[1] > now:
            value, _, reason = cached
            if value is None:
                print(
                    json.dumps(
                        {
                            "event": "team_channel_skipped_cached_miss",
                            "team": name,
                            "team_id": numeric_team_id,
                            "reason": reason,
                            "retry_in_seconds": round(cached[1] - now, 1),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
            return value
        try:
            rows = self._search(name)
        except Exception as exc:  # noqa: BLE001 - lookup must not block publishing
            print(
                json.dumps(
                    {
                        "event": "team_channel_search_failed",
                        "team": name,
                        "team_id": numeric_team_id,
                        "error": str(exc),
                        "platform_code": getattr(exc, "code", None),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            self._remember(cache_key, None, now, "search_failed")
            return None
        if not isinstance(rows, list):
            rows = []
        picked = pick_team_channel(rows, name, numeric_team_id)
        if picked is None:
            same_name = sum(
                1
                for row in rows
                if isinstance(row, dict)
                and str(row.get("type") or "").strip() == TEAM_CHANNEL_TAG_TYPE
                and str(row.get("name") or "").strip() == name
            )
            print(
                json.dumps(
                    {
                        "event": "team_channel_not_found",
                        "team": name,
                        "team_id": numeric_team_id,
                        "rows": len(rows),
                        "same_name_team_rows": same_name,
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            self._remember(cache_key, None, now, "not_found")
            return None
        value, matched_by = picked
        print(
            json.dumps(
                {
                    "event": "team_channel_resolved",
                    "team": name,
                    "team_id": numeric_team_id,
                    "value": value,
                    "matched_by": matched_by,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        self._remember(cache_key, value, now, matched_by)
        return value

    def channels_for(self, match_detail: dict[str, Any]) -> str:
        """Comma-joined tag ids for home then away; empty when none resolved."""
        # Tagging is best-effort end to end, so a misspelt switch must not stop
        # publishing. It is still loud: logged per article here, and status()
        # keeps raising so the dashboard shows the bad value.
        try:
            source = article_channels_source()
        except RuntimeError as exc:
            print(
                json.dumps(
                    {"event": "team_channel_config_invalid", "error": str(exc)},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return ""
        if source == "off":
            return ""
        detail = match_detail if isinstance(match_detail, dict) else {}
        ids: list[int] = []
        for side in ("A", "B"):
            value = self.resolve(
                str(detail.get(f"team_{side}_name") or ""),
                detail.get(f"team_{side}_id"),
            )
            if value is not None and value not in ids:
                ids.append(value)
        return ",".join(str(item) for item in ids)

    def tabs_for_channel(self, channel_value: int) -> list[int]:
        """Columns for one team tag id via playerteam; ``[]`` on miss/error."""
        if self._player_team is None:
            return []
        now = self._clock()
        with self._lock:
            cached = self._tabs_cache.get(channel_value)
        if cached and cached[1] > now:
            return list(cached[0] or [])
        try:
            payload = self._player_team(channel_value)
        except Exception as exc:  # noqa: BLE001 - lookup must not block publishing
            print(
                json.dumps(
                    {
                        "event": "team_tabs_lookup_failed",
                        "channel_id": channel_value,
                        "error": str(exc),
                        "platform_code": getattr(exc, "code", None),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            with self._lock:
                self._tabs_cache[channel_value] = (None, now + self._negative_ttl)
            return []
        tabs = pick_team_tabs(payload)
        print(
            json.dumps(
                {
                    "event": "team_tabs_resolved" if tabs else "team_tabs_not_found",
                    "channel_id": channel_value,
                    "tabs": tabs,
                    "table_name": (
                        payload.get("table_name") if isinstance(payload, dict) else None
                    ),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        ttl = self._ttl if tabs else self._negative_ttl
        with self._lock:
            self._tabs_cache[channel_value] = (tabs or None, now + ttl)
        return list(tabs)

    def tabs_for(self, match_detail: dict[str, Any]) -> list[str]:
        """Column ids for both teams (home first), de-duplicated; ``[]`` when off."""
        try:
            source = article_tabs_source()
        except RuntimeError as exc:
            print(
                json.dumps(
                    {"event": "team_tabs_config_invalid", "error": str(exc)},
                    ensure_ascii=False,
                ),
                flush=True,
            )
            return []
        if source == "off":
            return []
        detail = match_detail if isinstance(match_detail, dict) else {}
        tabs: list[int] = []
        for side in ("A", "B"):
            # resolve() is cached, so this shares the channelsearch call with
            # channels_for() for the same article.
            value = self.resolve(
                str(detail.get(f"team_{side}_name") or ""),
                detail.get(f"team_{side}_id"),
            )
            if value is None:
                continue
            for tab in self.tabs_for_channel(value):
                if tab not in tabs:
                    tabs.append(tab)
        return [str(item) for item in tabs]


RELIABLE_PERSON_PLACEHOLDERS = frozenset(
    {
        "0",
        "unknown",
        "none",
        "null",
        "未提供球员",
        "未知球员",
    }
)


def has_reliable_person(event: dict[str, Any] | None) -> bool:
    """Return whether an event has a usable display name, not only an ID."""
    value = event if isinstance(event, dict) else {}
    person = str(value.get("person") or "").strip()
    return bool(person) and person.casefold() not in RELIABLE_PERSON_PLACEHOLDERS


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
        platform_code: int | str | None = None,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.stage = stage
        self.status_code = status_code
        self.auth_required = auth_required
        self.retriable = retriable
        self.platform_code = platform_code
        self.diagnostics = dict(diagnostics or {})


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
        self.cover_directory = self.directory / "covers"

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

    def cover_path_for(self, gif_id: str) -> Path:
        self.path_for(gif_id)
        return self.cover_directory / f"{gif_id}.jpg"

    def cover_url_for(self, gif_id: str) -> str:
        self.cover_path_for(gif_id)
        return f"{self.public_origin}/publish-gif-covers/{gif_id}.jpg"

    def ensure_cover(self, gif_id: str) -> dict[str, Any]:
        """Generate or reuse the immutable 960x540 first-frame JPEG."""
        source = self.path_for(gif_id)
        target = self.cover_path_for(gif_id)
        if self._valid_jpeg_file(target):
            return self._cover_result(gif_id, target, reused=True)
        if not source.is_file():
            raise ArticlePublishError(
                "永久 GIF 文件不存在，无法生成封面",
                code="publish_gif_not_found",
                stage="gif_cover",
                status_code=404,
            )

        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise ArticlePublishError(
                "服务器未找到 FFmpeg，无法生成 GIF 首帧封面",
                code="publish_gif_cover_ffmpeg_missing",
                stage="gif_cover",
                status_code=503,
            )
        temporary = target.with_name(
            f".{target.name}.{os.getpid()}.{threading.get_ident()}."
            f"{secrets.token_hex(6)}.tmp"
        )
        command = [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-i",
            str(source),
            "-vf",
            (
                "scale=960:540:force_original_aspect_ratio=decrease,"
                "pad=960:540:(ow-iw)/2:(oh-ih)/2:color=black"
            ),
            "-frames:v",
            "1",
            "-q:v",
            "3",
            "-f",
            "image2",
            "-vcodec",
            "mjpeg",
            "-update",
            "1",
            str(temporary),
        ]
        try:
            self.cover_directory.mkdir(parents=True, exist_ok=True, mode=0o755)
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            if completed.returncode != 0:
                detail = str(completed.stderr or "").strip()[-800:]
                raise ArticlePublishError(
                    f"FFmpeg 生成 GIF 首帧封面失败：{detail or '未知错误'}",
                    code="publish_gif_cover_generation_failed",
                    stage="gif_cover",
                    status_code=503,
                    retriable=True,
                )
            if not self._valid_jpeg_file(temporary):
                raise ArticlePublishError(
                    "FFmpeg 没有生成有效的 GIF 首帧封面",
                    code="publish_gif_cover_generation_failed",
                    stage="gif_cover",
                    status_code=503,
                    retriable=True,
                )
            os.chmod(temporary, 0o644)
            os.replace(temporary, target)
        except subprocess.TimeoutExpired as exc:
            raise ArticlePublishError(
                "生成 GIF 首帧封面超过 30 秒",
                code="publish_gif_cover_timeout",
                stage="gif_cover",
                status_code=503,
                retriable=True,
            ) from exc
        except ArticlePublishError:
            raise
        except OSError as exc:
            raise ArticlePublishError(
                f"GIF 首帧封面目录不可写：{exc}",
                code="publish_gif_cover_storage_unavailable",
                stage="gif_cover",
                status_code=503,
            ) from exc
        finally:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        return self._cover_result(gif_id, target, reused=False)

    @staticmethod
    def _valid_jpeg_file(path: Path) -> bool:
        try:
            if not path.is_file() or path.stat().st_size <= 4:
                return False
            body = path.read_bytes()
        except OSError:
            return False
        if not body.startswith(b"\xff\xd8") or not body.endswith(b"\xff\xd9"):
            return False

        offset = 2
        start_of_frame_markers = {
            0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7,
            0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF,
        }
        while offset + 4 <= len(body):
            if body[offset] != 0xFF:
                offset += 1
                continue
            while offset < len(body) and body[offset] == 0xFF:
                offset += 1
            if offset >= len(body):
                return False
            marker = body[offset]
            offset += 1
            if marker in {0x01, 0xD8} or 0xD0 <= marker <= 0xD7:
                continue
            if marker == 0xD9:
                return False
            if offset + 2 > len(body):
                return False
            length = int.from_bytes(body[offset : offset + 2], "big")
            if length < 2 or offset + length > len(body):
                return False
            if marker in start_of_frame_markers:
                if length < 7:
                    return False
                height = int.from_bytes(body[offset + 3 : offset + 5], "big")
                width = int.from_bytes(body[offset + 5 : offset + 7], "big")
                return (width, height) == (960, 540)
            if marker == 0xDA:
                return False
            offset += length
        return False

    def _cover_result(
        self, gif_id: str, target: Path, *, reused: bool
    ) -> dict[str, Any]:
        return {
            "cover_path": str(target),
            "cover_url": self.cover_url_for(gif_id),
            "cover_width": 960,
            "cover_height": 540,
            "cover_reused": reused,
        }

    def create(self, source_path: Path) -> dict[str, Any]:
        try:
            body = source_path.read_bytes()
        except OSError as exc:
            raise ArticlePublishError(
                f"无法读取 GIF：{exc}",
                code="default_gif_unreadable",
                stage="gif_validation",
                status_code=404 if not source_path.exists() else 500,
            ) from exc
        return self.create_bytes(body, empty_code="default_gif_empty")

    def create_bytes(
        self,
        body: bytes,
        *,
        empty_code: str = "publish_gif_empty",
    ) -> dict[str, Any]:
        """Validate and persist GIF bytes using the content-addressed path."""
        if not body:
            raise ArticlePublishError(
                "GIF 是空文件",
                code=empty_code,
                stage="gif_validation",
            )
        if len(body) > self.max_bytes:
            raise ArticlePublishError(
                f"GIF 超过发布上限（{len(body)} > {self.max_bytes} 字节）",
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
        cover = self.ensure_cover(gif_id)
        return {
            "gif_id": gif_id,
            "path": str(target),
            "url": self.url_for(gif_id),
            "bytes": len(body),
            **cover,
            **gif_info,
        }


class RemoteGifUploadClient:
    """Upload a locally generated GIF to the storage-only server."""

    def __init__(self, endpoint: str, token: str, *, timeout: float = 120.0) -> None:
        self.endpoint = str(endpoint or "").strip()
        self.token = str(token or "").strip()
        self.timeout = float(timeout)
        if self.endpoint:
            parsed = urllib.parse.urlsplit(self.endpoint)
            if parsed.scheme != "https" or not parsed.netloc:
                raise RuntimeError("GIF_UPLOAD_ENDPOINT 必须是完整的 HTTPS 地址")

    @property
    def enabled(self) -> bool:
        return bool(self.endpoint)

    def status(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "configured": bool(self.endpoint and self.token),
            "endpoint": self.endpoint,
            "timeout_seconds": self.timeout,
        }

    def upload(
        self,
        *,
        source_path: Path,
        match_id: str,
        event_key: str,
        artifact_kind: str,
        max_bytes: int,
    ) -> dict[str, Any]:
        if not self.endpoint:
            raise ArticlePublishError(
                "未配置 GIF 上传地址；本地发布需要先把 GIF 上传到服务器",
                code="remote_gif_upload_not_configured",
                stage="remote_gif_upload",
                status_code=503,
            )
        if not self.token:
            raise ArticlePublishError(
                "未配置 GIF 上传密钥；请在本机 .env 设置 GIF_UPLOAD_TOKEN",
                code="remote_gif_upload_token_missing",
                stage="remote_gif_upload",
                status_code=503,
            )
        try:
            body = source_path.read_bytes()
        except OSError as exc:
            raise ArticlePublishError(
                f"读取本地 GIF 失败：{exc}",
                code="remote_gif_source_unreadable",
                stage="remote_gif_upload",
                status_code=404,
            ) from exc
        if not body:
            raise ArticlePublishError(
                "本地 GIF 是空文件，无法上传",
                code="publish_gif_empty",
                stage="gif_validation",
            )
        if len(body) > max_bytes:
            raise ArticlePublishError(
                f"GIF 超过发布上限（当前上限 {max_bytes} 字节）",
                code="publish_gif_too_large",
                stage="gif_validation",
            )
        gif_info = inspect_animated_gif(body)
        boundary = f"----automatic-gif-{os.urandom(12).hex()}"
        fields = {
            "match_id": str(match_id),
            "event_key": str(event_key),
            "artifact_kind": str(artifact_kind),
        }
        payload = bytearray()
        for name, value in fields.items():
            payload.extend(f"--{boundary}\r\n".encode())
            payload.extend(
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n'.encode()
            )
        payload.extend(f"--{boundary}\r\n".encode())
        payload.extend(
            b'Content-Disposition: form-data; name="gif"; filename="event.gif"\r\n'
            b"Content-Type: image/gif\r\n\r\n"
        )
        payload.extend(body)
        payload.extend(f"\r\n--{boundary}--\r\n".encode())
        request = urllib.request.Request(
            self.endpoint,
            data=bytes(payload),
            method="POST",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                response_body = response.read()
                status_code = int(getattr(response, "status", 200) or 200)
        except urllib.error.HTTPError as exc:
            detail = exc.read(4096).decode("utf-8", errors="replace")
            raise ArticlePublishError(
                f"GIF 上传服务器拒绝请求（HTTP {exc.code}）：{_remote_error_message(detail)}",
                code="remote_gif_upload_rejected",
                stage="remote_gif_upload",
                status_code=502 if exc.code >= 500 else exc.code,
                retriable=exc.code >= 500 or exc.code == 429,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ArticlePublishError(
                f"无法连接 GIF 上传服务器：{exc}",
                code="remote_gif_upload_unreachable",
                stage="remote_gif_upload",
                status_code=503,
                retriable=True,
            ) from exc
        try:
            parsed = json.loads(response_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArticlePublishError(
                f"GIF 上传服务器返回了无法识别的结果（HTTP {status_code}）",
                code="remote_gif_upload_invalid_response",
                stage="remote_gif_upload",
                status_code=502,
                retriable=True,
            ) from exc
        if not isinstance(parsed, dict) or not parsed.get("ok"):
            message = parsed.get("error") if isinstance(parsed, dict) else "未知错误"
            raise ArticlePublishError(
                f"GIF 上传失败：{message}",
                code="remote_gif_upload_rejected",
                stage="remote_gif_upload",
                status_code=502,
                retriable=True,
            )
        remote_gif = parsed.get("gif")
        if not isinstance(remote_gif, dict):
            raise ArticlePublishError(
                "GIF 上传成功但没有返回公网地址",
                code="remote_gif_upload_invalid_response",
                stage="remote_gif_upload",
                status_code=502,
            )
        gif_id = str(remote_gif.get("gif_id") or "").strip()
        gif_url = str(remote_gif.get("url") or "").strip()
        cover_url = str(remote_gif.get("cover_url") or "").strip()
        if not GIF_ID_PATTERN.fullmatch(gif_id) or not gif_url.startswith("https://"):
            raise ArticlePublishError(
                "GIF 上传成功但返回的文件标识或公网地址无效",
                code="remote_gif_upload_invalid_response",
                stage="remote_gif_upload",
                status_code=502,
            )
        try:
            cover_path_name = Path(
                urllib.parse.urlsplit(cover_url).path
            ).name.lower()
        except ValueError:
            cover_path_name = ""
        if (
            not _is_https_jpeg_url(cover_url)
            or cover_path_name not in {f"{gif_id}.jpg", f"{gif_id}.jpeg"}
        ):
            raise ArticlePublishError(
                "GIF 上传成功但返回的首帧封面公网地址无效",
                code="remote_gif_upload_invalid_response",
                stage="remote_gif_upload",
                status_code=502,
            )
        return {
            "gif_id": gif_id,
            "path": str(source_path.expanduser().resolve()),
            "url": gif_url,
            "cover_url": cover_url,
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
        public_cover_url_checker: Callable[[str], None] | None = None,
        remote_upload_client: RemoteGifUploadClient | None = None,
        ocr_image_upload_client: OpenPlatformClient | None = None,
        account_pool: PublishAccountPool | None = None,
    ) -> None:
        self.platform_client = platform_client
        self.gif_store = gif_store
        self.database_path = database_path.expanduser().resolve()
        self.verify_public_url = verify_public_url
        self.public_url_checker = public_url_checker or _check_public_gif_url
        self.public_cover_url_checker = (
            public_cover_url_checker
            or public_url_checker
            or _check_public_cover_url
        )
        self.remote_upload_client = remote_upload_client
        # The official image API is deliberately opt-in and only used by the
        # OCR article path. Keeping it separate preserves the default GIF
        # uploader and its existing configuration.
        self.ocr_image_upload_client = ocr_image_upload_client
        self.account_pool = account_pool
        self._lock = threading.RLock()
        search = getattr(platform_client, "search_channels", None)
        if not callable(search):
            def search(keyword: str) -> list[dict[str, Any]]:
                raise OpenPlatformError("当前开放平台客户端不支持标签搜索")
        player_team = getattr(platform_client, "player_team", None)
        self.team_channels = TeamChannelResolver(
            search, player_team if callable(player_team) else None
        )

    def _verify_public_media(self, gif: dict[str, Any]) -> None:
        """HEAD-probe the GIF/cover URLs when they are ours to serve.

        The probe exists for the self-hosted uploader (our own nginx must be
        reachable before the CMS fetches the file). A GIF that went through
        the official image API already lives on the platform's FastDFS: it
        was confirmed by the upload response, and probing its CDN URL right
        after upload only adds 8s+8s of latency and a spurious retriable
        failure while the edge warms up.
        """
        if not self.verify_public_url:
            return
        if gif.get("official_upload") or gif.get("upload_source") == "official":
            return
        if _is_absolute_http_url(gif.get("url")):
            self.public_url_checker(str(gif["url"]))
        if _is_absolute_http_url(gif.get("cover_url")):
            self.public_cover_url_checker(str(gif["cover_url"]))

    def status(self) -> dict[str, Any]:
        try:
            account_pool_status = (
                self.account_pool.status()
                if self.account_pool is not None
                else {"enabled": False, "count": 0, "active_count": 0}
            )
        except PublishAccountPoolError as exc:
            account_pool_status = {
                "enabled": True,
                "available": False,
                "count": 0,
                "active_count": 0,
                "error": str(exc),
            }
        oauth_status = self.platform_client.status()
        if self.ocr_image_upload_client is not None:
            # Image upload has its own token and readiness requirements. Do
            # not mirror OAuth status, otherwise a valid image token can look
            # unauthorized when the article OAuth token is absent or expired.
            image_status = getattr(
                self.ocr_image_upload_client, "image_upload_status", None
            )
            ocr_upload_status = (
                image_status()
                if callable(image_status)
                else self.ocr_image_upload_client.status()
            )
        else:
            ocr_upload_status = {"enabled": False, "configured": False}
        return {
            "archive_level": "B",
            "add_to_tab": article_add_to_tab(),
            "channelsnew": article_channelsnew() or None,
            "tabs": ",".join(article_tabs()) or None,
            "tabs_source": article_tabs_source(),
            "channels_source": article_channels_source(),
            "type": "article",
            "style": "gif",
            "default_account": self.account_pool is None,
            "account_pool": account_pool_status,
            "public_origin": self.gif_store.public_origin,
            "public_origin_https": self.gif_store.public_origin.startswith("https://"),
            "verify_public_url": self.verify_public_url,
            "gif_directory": str(self.gif_store.directory),
            "gif_cover_directory": str(self.gif_store.cover_directory),
            "database_path": str(self.database_path),
            "oauth": oauth_status,
            "remote_upload": (
                self.remote_upload_client.status()
                if self.remote_upload_client
                else {"enabled": False, "configured": False}
            ),
            "ocr_image_upload": ocr_upload_status,
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

    def record_for_source_path(
        self, match_id: str, event_key: str, source_path: Path
    ) -> dict[str, Any] | None:
        """Return the publish record for one concrete GIF artifact."""
        resolved_source = str(source_path.expanduser().resolve())
        if not self.database_path.exists():
            return None
        try:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT * FROM article_publish_records
                    WHERE match_id=? AND event_key=? AND source_path=?
                    ORDER BY updated_at_unix DESC LIMIT 1
                    """,
                    (str(match_id), str(event_key), resolved_source),
                ).fetchone()
        except sqlite3.Error:
            return None
        return _public_record(row) if row else None

    def _account_for_assignment(self, assignment_key: str) -> dict[str, Any]:
        """Bind one logical article to one account before its first request."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT user_id, user_name
                FROM article_publish_account_assignments
                WHERE assignment_key=?
                """,
                (assignment_key,),
            ).fetchone()
            if existing is not None:
                return {
                    "user_id": int(existing["user_id"]),
                    "user_name": str(existing["user_name"]),
                }

            try:
                active_accounts = self.account_pool.active_accounts()
            except PublishAccountPoolError as exc:
                raise ArticlePublishError(
                    f"发布账号池暂时不可用：{exc}",
                    code="publish_account_pool_unavailable",
                    stage="account_selection",
                    status_code=503,
                ) from exc
            if not active_accounts:
                raise ArticlePublishError(
                    "发布账号池中没有启用的账号，请先添加或启用至少一个账号",
                    code="publish_account_pool_empty",
                    stage="account_selection",
                    status_code=409,
                )
            usage_rows = connection.execute(
                """
                SELECT user_id, COUNT(*) AS assignment_count
                FROM article_publish_account_assignments
                GROUP BY user_id
                """
            ).fetchall()
            usage = {
                int(row["user_id"]): int(row["assignment_count"])
                for row in usage_rows
            }
            minimum = min(
                usage.get(int(account["user_id"]), 0)
                for account in active_accounts
            )
            candidates = [
                account
                for account in active_accounts
                if usage.get(int(account["user_id"]), 0) == minimum
            ]
            selected = secrets.choice(candidates)
            connection.execute(
                """
                INSERT INTO article_publish_account_assignments (
                    assignment_key, user_id, user_name, assigned_at_unix
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    assignment_key,
                    int(selected["user_id"]),
                    str(selected["user_name"]),
                    time.time(),
                ),
            )
            return {
                "user_id": int(selected["user_id"]),
                "user_name": str(selected["user_name"]),
            }

    def _existing_account_assignment(
        self, assignment_key: str
    ) -> dict[str, Any] | None:
        if self.account_pool is None or not self.database_path.exists():
            return None
        with self._connect() as connection:
            row = connection.execute(
                "SELECT user_id, user_name FROM article_publish_account_assignments "
                "WHERE assignment_key=?",
                (assignment_key,),
            ).fetchone()
        if row is None:
            return None
        return {"user_id": int(row["user_id"]), "user_name": str(row["user_name"])}

    def upload_gif(
        self,
        *,
        body: bytes,
        match_id: str,
        event_key: str,
        artifact_kind: str = "default",
    ) -> dict[str, Any]:
        """Store an externally generated GIF and associate it with an event."""
        if not str(match_id).strip():
            raise ArticlePublishError(
                "上传 GIF 需要比赛 ID",
                code="publish_match_id_missing",
                stage="request_validation",
                status_code=400,
            )
        if not str(event_key).strip():
            raise ArticlePublishError(
                "上传 GIF 需要事件标识",
                code="publish_event_key_missing",
                stage="request_validation",
                status_code=400,
            )
        if artifact_kind not in {"default", "ocr_window"}:
            raise ArticlePublishError(
                "不支持的 GIF 类型",
                code="publish_artifact_kind_invalid",
                stage="request_validation",
                status_code=400,
            )
        with self._lock:
            gif = self.gif_store.create_bytes(body)
            self._save_uploaded_mapping(
                match_id=match_id,
                event_key=event_key,
                artifact_kind=artifact_kind,
                gif=gif,
            )
        return {
            "gif": gif,
            "match_id": str(match_id),
            "event_key": str(event_key),
            "artifact_kind": artifact_kind,
        }

    def _save_uploaded_mapping(
        self,
        *,
        match_id: str,
        event_key: str,
        artifact_kind: str,
        gif: dict[str, Any],
    ) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO article_uploaded_gifs (
                    match_id, event_key, artifact_kind, gif_sha256,
                    gif_path, gif_url, cover_url, upload_source,
                    upload_diagnostics_json, uploaded_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(match_id, event_key, artifact_kind) DO UPDATE SET
                    gif_sha256=excluded.gif_sha256,
                    gif_path=excluded.gif_path,
                    gif_url=excluded.gif_url,
                    cover_url=excluded.cover_url,
                    upload_source=excluded.upload_source,
                    upload_diagnostics_json=COALESCE(
                        excluded.upload_diagnostics_json,
                        article_uploaded_gifs.upload_diagnostics_json
                    ),
                    uploaded_at_unix=excluded.uploaded_at_unix
                """,
                (
                    str(match_id).strip(),
                    str(event_key).strip(),
                    artifact_kind,
                    str(gif["gif_id"]),
                    str(gif["path"]),
                    str(gif["url"]),
                    str(gif["cover_url"]),
                    str(
                        gif.get("upload_source")
                        or ("official" if gif.get("official_upload") else "remote")
                    ),
                    _diagnostics_json(gif.get("upload_diagnostics")),
                    now,
                ),
            )

    def uploaded_gif_for(
        self,
        match_id: str,
        event_key: str,
        artifact_kind: str = "default",
    ) -> dict[str, Any] | None:
        """Return the most recent externally uploaded GIF for one event."""
        if not self.database_path.exists():
            return None
        try:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT * FROM article_uploaded_gifs
                    WHERE match_id=? AND event_key=? AND artifact_kind=?
                    LIMIT 1
                    """,
                    (str(match_id), str(event_key), str(artifact_kind)),
                ).fetchone()
        except sqlite3.Error:
            return None
        if not row:
            return None
        path = Path(str(row["gif_path"])).expanduser()
        if not path.is_file():
            return None
        result = {
            "gif_id": str(row["gif_sha256"]),
            "path": str(path),
            "url": str(row["gif_url"]),
            "cover_url": str(row["cover_url"] or ""),
            "upload_source": str(row["upload_source"] or "remote"),
            "uploaded_at_unix": row["uploaded_at_unix"],
        }
        upload_diagnostics = _json_object(row["upload_diagnostics_json"])
        if upload_diagnostics:
            result["upload_diagnostics"] = upload_diagnostics
        return result

    def _prepare_public_gif(
        self,
        *,
        source_path: Path,
        match_id: str,
        event_key: str,
        artifact_kind: str,
    ) -> dict[str, Any]:
        """Persist a GIF locally and make it available at its public URL."""
        local_gif = self.gif_store.create(source_path)
        uploaded = self.uploaded_gif_for(match_id, event_key, artifact_kind)
        if artifact_kind == "ocr_window" and self.ocr_image_upload_client is not None:
            # Once the official OCR client is enabled, a legacy remote mapping
            # must not block migration to FastDFS. The persisted upload_source
            # marker identifies an upload made through the official endpoint.
            can_reuse_uploaded = bool(
                uploaded
                and uploaded.get("gif_id") == local_gif["gif_id"]
                and uploaded.get("upload_source") == "official"
                and _is_media_reference(uploaded.get("url"))
                and _is_media_reference(uploaded.get("cover_url"), allow_relative=True)
            )
        else:
            # Preserve the legacy/default uploader's validation exactly.
            can_reuse_uploaded = bool(
                uploaded
                and uploaded.get("gif_id") == local_gif["gif_id"]
                and uploaded.get("upload_source") == "remote"
                and _is_https_jpeg_url(uploaded.get("cover_url"))
            )
        if can_reuse_uploaded:
            return uploaded

        # OCR GIFs use the official image endpoint when it is configured. The
        # endpoint receives both files in one request and returns a FastDFS
        # `url` plus a complete `img1_url` for each file.
        if artifact_kind == "ocr_window" and self.ocr_image_upload_client is not None:
            gif = self._upload_ocr_images(local_gif, match_id=match_id, event_key=event_key)
            self._save_uploaded_mapping(
                match_id=match_id,
                event_key=event_key,
                artifact_kind=artifact_kind,
                gif=gif,
            )
            return gif

        # Keep the previous uploader as a compatibility fallback when the
        # official OCR client has not been injected yet. The normal/default
        # GIF path remains exactly the same.
        if not self.remote_upload_client or not self.remote_upload_client.enabled:
            return local_gif

        gif = self.remote_upload_client.upload(
            source_path=Path(local_gif["path"]),
            match_id=match_id,
            event_key=event_key,
            artifact_kind=artifact_kind,
            max_bytes=self.gif_store.max_bytes,
        )
        if gif["gif_id"] != local_gif["gif_id"]:
            raise ArticlePublishError(
                "服务器保存后的 GIF 与本地文件不一致，已停止发布",
                code="remote_gif_upload_hash_mismatch",
                stage="remote_gif_upload",
                status_code=502,
            )
        self._save_uploaded_mapping(
            match_id=match_id,
            event_key=event_key,
            artifact_kind=artifact_kind,
            gif=gif,
        )
        return gif

    def _upload_ocr_images(
        self,
        local_gif: dict[str, Any],
        *,
        match_id: str,
        event_key: str,
    ) -> dict[str, Any]:
        """Upload an OCR GIF and its generated JPEG cover to the platform."""
        gif_path = Path(str(local_gif["path"])).expanduser()
        cover_path = Path(str(local_gif.get("cover_path") or "")).expanduser()
        if not cover_path.is_file():
            raise ArticlePublishError(
                "官方图片上传缺少 GIF 封面文件",
                code="official_image_cover_missing",
                stage="official_image_upload",
                status_code=500,
            )
        try:
            gif_size = gif_path.stat().st_size
            cover_size = cover_path.stat().st_size
        except OSError as exc:
            raise ArticlePublishError(
                f"读取官方图片上传文件大小失败：{exc}",
                code="official_image_source_unreadable",
                stage="official_image_upload",
                status_code=404,
            ) from exc
        if gif_size > OFFICIAL_IMAGE_MAX_BYTES or cover_size > OFFICIAL_IMAGE_MAX_BYTES:
            raise ArticlePublishError(
                "GIF 或封面超过懂球帝官方图片接口的 20MB 单文件上限",
                code="official_image_too_large",
                stage="official_image_upload",
                status_code=413,
            )
        try:
            results = self.ocr_image_upload_client.upload_images(
                gif_path,
                cover_path,
                max_bytes=OFFICIAL_IMAGE_MAX_BYTES,
            )
        except ArticlePublishError:
            raise
        except OpenPlatformError as exc:
            raise ArticlePublishError(
                f"懂球帝官方图片上传失败：{exc}",
                code="official_image_upload_failed",
                stage="official_image_upload",
                status_code=exc.status_code,
                auth_required=exc.auth_required,
                retriable=exc.retriable,
                platform_code=exc.code,
                diagnostics=getattr(exc, "diagnostics", None),
            ) from exc
        except (OSError, TimeoutError) as exc:
            raise ArticlePublishError(
                f"懂球帝官方图片上传连接失败：{exc}",
                code="official_image_upload_unreachable",
                stage="official_image_upload",
                status_code=503,
                retriable=True,
            ) from exc

        if isinstance(results, dict):
            results = results.get("images") or results.get("data") or []
        if not isinstance(results, (list, tuple)) or len(results) < 2:
            raise ArticlePublishError(
                "官方图片接口返回结果不完整，GIF 和封面地址均未确认",
                code="official_image_upload_invalid_response",
                stage="official_image_upload",
                status_code=502,
            )
        gif_item = _find_official_image_result(results, kind="gif")
        cover_item = _find_official_image_result(results, kind="cover")
        if gif_item is None or cover_item is None or gif_item is cover_item:
            # The documented file1/file2 order is the final fallback when the
            # service omits MIME/file-name metadata.
            gif_item, cover_item = results[0], results[1]
        if not isinstance(gif_item, dict) or not isinstance(cover_item, dict):
            raise ArticlePublishError(
                "官方图片接口返回了无法识别的图片项目",
                code="official_image_upload_invalid_response",
                stage="official_image_upload",
                status_code=502,
            )
        gif_path_url = str(gif_item.get("url") or "").strip()
        gif_url = str(gif_item.get("img1_url") or "").strip()
        cover_url = str(cover_item.get("url") or "").strip()
        cover_public_url = str(cover_item.get("img1_url") or "").strip()
        if not _is_media_reference(gif_path_url, allow_relative=True):
            raise ArticlePublishError(
                "官方图片接口未返回有效的 GIF url 路径",
                code="official_image_upload_invalid_response",
                stage="official_image_upload",
                status_code=502,
            )
        if not _is_media_reference(gif_url):
            raise ArticlePublishError(
                "官方图片接口未返回有效的 GIF 完整访问地址 img1_url",
                code="official_image_upload_invalid_response",
                stage="official_image_upload",
                status_code=502,
            )
        if not _is_media_reference(cover_url, allow_relative=True):
            raise ArticlePublishError(
                "官方图片接口未返回有效的封面 url 路径",
                code="official_image_upload_invalid_response",
                stage="official_image_upload",
                status_code=502,
            )
        if not _is_media_reference(cover_public_url):
            raise ArticlePublishError(
                "官方图片接口未返回有效的封面完整访问地址 img1_url",
                code="official_image_upload_invalid_response",
                stage="official_image_upload",
                status_code=502,
            )
        return {
            **local_gif,
            "url": gif_url,
            "cover_url": cover_url,
            "official_upload": True,
            "official_gif_path": gif_path_url,
            "official_gif_url": gif_url,
            "official_cover_path": cover_url,
            "official_cover_url": cover_public_url,
            "upload_diagnostics": dict(
                getattr(results, "diagnostics", {}) or {}
            ),
            "official_gif_image_id": gif_item.get("image_id"),
            "official_cover_image_id": cover_item.get("image_id"),
            "official_upload_match_id": str(match_id),
            "official_upload_event_key": str(event_key),
        }

    def publish(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any],
        source_path: Path,
        artifact_kind: str = "default",
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
                "要发布的 GIF 尚未生成完成",
                code="gif_not_ready",
                stage="request_validation",
                status_code=409,
            )

        if artifact_kind not in {"default", "ocr_window"}:
            raise ArticlePublishError(
                "不支持的 GIF 类型",
                code="publish_artifact_kind_invalid",
                stage="request_validation",
                status_code=400,
            )

        with self._lock:
            gif = self._prepare_public_gif(
                source_path=source_path,
                match_id=match_id,
                event_key=event_key,
                artifact_kind=artifact_kind,
            )
            stable_id = hashlib.sha256(
                f"{match_id}\n{event_key}\n{gif['gif_id']}".encode("utf-8")
            ).hexdigest()
            previous = self._get_record(stable_id)
            if previous and previous.get("status") == "success":
                previous["idempotent_replay"] = True
                return previous

            publish_account = (
                self._account_for_assignment(f"match:{match_id}")
                if self.account_pool is not None
                else None
            )
            title = build_article_title(event, match_detail)
            fields = build_article_fields(
                match_id=match_id,
                event=event,
                gif_url=str(gif["url"]),
                cover_url=str(gif["cover_url"]),
                litpic_url=_official_gif_litpic(gif),
                title=title,
                publish_account=publish_account,
                channels=self.team_channels.channels_for(match_detail),
                tabs=self.team_channels.tabs_for(match_detail),
                allow_relative_media=bool(
                    artifact_kind == "ocr_window"
                    and (
                        gif.get("official_upload")
                        or gif.get("upload_source") == "official"
                    )
                ),
            )
            self._save_record(
                stable_id=stable_id,
                match_id=match_id,
                event_key=event_key,
                source_path=source_path,
                gif=gif,
                title=title,
                status="prepared",
                stage="gif_prepared",
                publish_account=publish_account,
            )
            try:
                self._verify_public_media(gif)
                self._save_record(
                    stable_id=stable_id,
                    match_id=match_id,
                    event_key=event_key,
                    source_path=source_path,
                    gif=gif,
                    title=title,
                    status="publishing",
                    stage="platform_publish",
                    publish_account=publish_account,
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
                    platform_code=exc.code,
                ) from exc

            published_at = time.time()
            self._save_record(
                stable_id=stable_id,
                match_id=match_id,
                event_key=event_key,
                source_path=source_path,
                gif=gif,
                title=title,
                status="success",
                stage="completed",
                article_id=str(result["article_id"]),
                platform_code=result.get("code"),
                duplicate=bool(result.get("duplicate")),
                published_at_unix=published_at,
                publish_account=publish_account,
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

    def create_or_update_draft(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any],
        source_path: Path,
        archive_id: str | int | None = None,
    ) -> dict[str, Any]:
        """Create or update an OCR GIF draft without touching publish records."""
        if not str(event.get("event_key") or "").strip():
            raise ArticlePublishError(
                "事件缺少稳定标识，无法创建草稿",
                code="draft_event_key_missing",
                stage="request_validation",
            )
        return self.create_or_update_article(
            match_id=match_id,
            event=event,
            match_detail=match_detail,
            source_path=source_path,
            delivery_mode="draft",
            archive_id=archive_id,
        )

    def publish_draft(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any],
        source_path: Path,
        archive_id: str | int,
    ) -> dict[str, Any]:
        """Update one existing OCR GIF draft and make it public."""
        if archive_id is None or not str(archive_id).strip():
            raise ArticlePublishError(
                "发布草稿需要有效的数字文章 ID",
                code="publish_archive_id_missing",
                stage="request_validation",
                status_code=400,
            )
        return self.create_or_update_article(
            match_id=match_id,
            event=event,
            match_detail=match_detail,
            source_path=source_path,
            delivery_mode="publish",
            archive_id=archive_id,
        )

    def create_or_update_article(
        self,
        *,
        match_id: str,
        event: dict[str, Any],
        match_detail: dict[str, Any],
        source_path: Path,
        delivery_mode: str = "draft",
        archive_id: str | int | None = None,
    ) -> dict[str, Any]:
        """Create or update an OCR GIF article as a draft or published article."""
        event_key = str(event.get("event_key") or "").strip()
        if not event_key:
            raise ArticlePublishError(
                "事件缺少稳定标识，无法创建或更新文章",
                code="article_event_key_missing",
                stage="request_validation",
            )

        with self._lock:
            started_at = time.monotonic()
            gif = self._prepare_public_gif(
                source_path=source_path,
                match_id=match_id,
                event_key=event_key,
                artifact_kind="ocr_window",
            )
            assignment_key = f"match:{match_id}"
            publish_account = self._existing_account_assignment(assignment_key)
            # An archive_id without a prior assignment identifies a legacy
            # draft. Never change its author while updating it.
            if (
                publish_account is None
                and archive_id is None
                and self.account_pool is not None
            ):
                publish_account = self._account_for_assignment(assignment_key)
            title = build_article_title(event, match_detail)
            fields = build_article_fields(
                match_id=match_id,
                event=event,
                gif_url=str(gif["url"]),
                cover_url=str(gif["cover_url"]),
                litpic_url=_official_gif_litpic(gif),
                title=title,
                delivery_mode=delivery_mode,
                archive_id=archive_id,
                publish_account=publish_account,
                channels=self.team_channels.channels_for(match_detail),
                tabs=self.team_channels.tabs_for(match_detail),
                # A reused DB mapping predates the in-memory marker, so infer
                # official mode from its persisted upload source marker.
                allow_relative_media=bool(
                    gif.get("official_upload")
                    or gif.get("upload_source") == "official"
                ),
            )
            try:
                self._verify_public_media(gif)
                result = self.platform_client.create_article(fields)
            except ArticlePublishError as exc:
                base_diagnostics = _draft_diagnostics(
                    fields=fields,
                    gif=gif,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    platform_message=str(exc),
                )
                if exc.diagnostics:
                    # Keep the upload trace under its own key while retaining
                    # the safe request summary used by the Dashboard.
                    exc.diagnostics = {
                        **base_diagnostics,
                        "image_upload": dict(exc.diagnostics),
                    }
                else:
                    exc.diagnostics = base_diagnostics
                raise
            except OpenPlatformError as exc:
                stage = "authorization" if exc.auth_required else "platform_publish"
                diagnostics = _draft_diagnostics(
                    fields=fields,
                    gif=gif,
                    elapsed_ms=(time.monotonic() - started_at) * 1000,
                    platform_message=str(exc),
                    http_status=exc.http_status or exc.status_code,
                )
                raise ArticlePublishError(
                    str(exc),
                    code=(
                        "open_platform_auth_required"
                        if exc.auth_required
                        else "open_platform_rejected"
                    ),
                    stage=stage,
                    status_code=exc.status_code,
                    auth_required=exc.auth_required,
                    retriable=exc.retriable,
                    platform_code=exc.code,
                    diagnostics=diagnostics,
                ) from exc

            diagnostics = _draft_diagnostics(
                fields=fields,
                gif=gif,
                elapsed_ms=(time.monotonic() - started_at) * 1000,
                platform_message=str(result.get("message") or "ok"),
                http_status=result.get("http_status"),
            )
            return {
                "article_id": str(result["article_id"]),
                "gif": gif,
                "title": title,
                "delivery_mode": delivery_mode,
                "updated": archive_id is not None,
                "duplicate": bool(result.get("duplicate")),
                "platform_code": result.get("code"),
                "diagnostics": diagnostics,
                "publish_account": (
                    dict(publish_account) if publish_account is not None else None
                ),
            }

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
                source_path TEXT,
                gif_sha256 TEXT NOT NULL,
                gif_url TEXT NOT NULL,
                cover_url TEXT,
                gif_path TEXT NOT NULL,
                title TEXT NOT NULL,
                status TEXT NOT NULL,
                stage TEXT NOT NULL,
                article_id TEXT,
                publish_user_id INTEGER,
                publish_user_name TEXT,
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
            CREATE TABLE IF NOT EXISTS article_uploaded_gifs (
                match_id TEXT NOT NULL,
                event_key TEXT NOT NULL,
                artifact_kind TEXT NOT NULL,
                gif_sha256 TEXT NOT NULL,
                gif_path TEXT NOT NULL,
                gif_url TEXT NOT NULL,
                cover_url TEXT,
                upload_source TEXT NOT NULL DEFAULT 'remote',
                upload_diagnostics_json TEXT,
                uploaded_at_unix REAL NOT NULL,
                PRIMARY KEY(match_id, event_key, artifact_kind)
            )
            """
        )
        columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(article_publish_records)"
            )
        }
        if "source_path" not in columns:
            connection.execute(
                "ALTER TABLE article_publish_records ADD COLUMN source_path TEXT"
            )
        if "cover_url" not in columns:
            connection.execute(
                "ALTER TABLE article_publish_records ADD COLUMN cover_url TEXT"
            )
        if "publish_user_id" not in columns:
            connection.execute(
                "ALTER TABLE article_publish_records ADD COLUMN publish_user_id INTEGER"
            )
        if "publish_user_name" not in columns:
            connection.execute(
                "ALTER TABLE article_publish_records ADD COLUMN publish_user_name TEXT"
            )
        uploaded_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(article_uploaded_gifs)"
            )
        }
        if "cover_url" not in uploaded_columns:
            connection.execute(
                "ALTER TABLE article_uploaded_gifs ADD COLUMN cover_url TEXT"
            )
        if "upload_source" not in uploaded_columns:
            connection.execute(
                "ALTER TABLE article_uploaded_gifs ADD COLUMN upload_source TEXT NOT NULL DEFAULT 'remote'"
            )
        if "upload_diagnostics_json" not in uploaded_columns:
            connection.execute(
                "ALTER TABLE article_uploaded_gifs ADD COLUMN upload_diagnostics_json TEXT"
            )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS article_publish_account_assignments (
                assignment_key TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                user_name TEXT NOT NULL,
                assigned_at_unix REAL NOT NULL
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
        source_path: Path,
        gif: dict[str, Any],
        title: str,
        status: str,
        stage: str,
        article_id: str | None = None,
        platform_code: Any = None,
        duplicate: bool = False,
        published_at_unix: float | None = None,
        publish_account: dict[str, Any] | None = None,
    ) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO article_publish_records (
                    stable_id, match_id, event_key, source_path, gif_sha256,
                    gif_url, cover_url, gif_path,
                    title, status, stage, article_id, platform_code, duplicate,
                    publish_user_id, publish_user_name, error,
                    created_at_unix, updated_at_unix, published_at_unix
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
                ON CONFLICT(stable_id) DO UPDATE SET
                    status=excluded.status,
                    stage=excluded.stage,
                    cover_url=excluded.cover_url,
                    article_id=COALESCE(excluded.article_id, article_publish_records.article_id),
                    platform_code=COALESCE(excluded.platform_code, article_publish_records.platform_code),
                    duplicate=excluded.duplicate,
                    publish_user_id=COALESCE(excluded.publish_user_id, article_publish_records.publish_user_id),
                    publish_user_name=COALESCE(excluded.publish_user_name, article_publish_records.publish_user_name),
                    error=NULL,
                    updated_at_unix=excluded.updated_at_unix,
                    published_at_unix=COALESCE(excluded.published_at_unix, article_publish_records.published_at_unix)
                """,
                (
                    stable_id,
                    match_id,
                    event_key,
                    str(source_path.expanduser().resolve()),
                    gif["gif_id"],
                    gif["url"],
                    gif["cover_url"],
                    gif["path"],
                    title,
                    status,
                    stage,
                    article_id,
                    None if platform_code is None else str(platform_code),
                    int(duplicate),
                    (
                        int(publish_account["user_id"])
                        if publish_account is not None
                        else None
                    ),
                    (
                        str(publish_account["user_name"])
                        if publish_account is not None
                        else None
                    ),
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
    # An explicit operator-provided title wins over the generated one. OCR events
    # never set this field, so automatic titles are unaffected.
    override = str(event.get("title_override") or "").strip()
    if override:
        return override[:100]
    minute = _match_time(event)
    minute_text = f"{minute}分钟" if minute else "比赛中"
    person = str(event.get("person") or "").strip()
    code = str(event.get("code") or "").upper()
    action = EVENT_LABELS.get(code, "比赛事件")
    home = str(detail.get("team_A_name") or "主队").strip()
    away = str(detail.get("team_B_name") or "客队").strip()
    score = _match_score(event.get("score"))
    actor = (
        person
        if has_reliable_person(event)
        else _event_team_name(event.get("team"), detail)
    )
    action_text = f"{actor}{action}" if actor else action
    if code in GOAL_EVENT_CODES:
        # Goal events change the score, so include the score when the API
        # provides one. Keep the matchup fallback for incomplete event data.
        score_text = f"，{home} {score} {away}" if score else f"，{home}对阵{away}"
    elif code in {"YC", "RC"}:
        # Cards do not change the score; their concise title is easier to scan.
        score_text = ""
    else:
        # Preserve the existing format for event types outside the supported
        # football event set until they receive an explicit title rule.
        score_text = f"，{home} {score} {away}" if score else f"，{home}对阵{away}"
    return f"{minute_text}，{action_text}{score_text}"[:100]


def build_article_fields(
    *,
    match_id: str,
    event: dict[str, Any],
    gif_url: str,
    cover_url: str,
    title: str,
    delivery_mode: str = "publish",
    archive_id: str | int | None = None,
    publish_account: dict[str, Any] | None = None,
    allow_relative_media: bool = False,
    litpic_url: str = "",
    channels: str = "",
    tabs: Sequence[str | int] = (),
) -> dict[str, Any]:
    if delivery_mode not in {"publish", "draft"}:
        raise ArticlePublishError(
            "文章处理方式必须是 publish 或 draft",
            code="publish_delivery_mode_invalid",
            stage="request_validation",
            status_code=400,
        )
    if not re.fullmatch(r"\d{1,20}", match_id):
        raise ArticlePublishError(
            "创建文章需要有效的数字比赛 ID",
            code="publish_match_id_invalid",
            stage="request_validation",
            status_code=400,
        )
    if (
        not (
            allow_relative_media
            and _is_relative_media_reference(gif_url)
        )
        and not gif_url.startswith("https://")
    ):
        raise ArticlePublishError(
            "文章使用的 GIF 地址必须是 HTTPS 公网地址或官方 FastDFS url 路径",
            code="publish_gif_https_required",
            stage="public_url_check",
            status_code=503,
        )
    normalized_cover_url = str(cover_url or "").strip()
    if not (
        (allow_relative_media and _is_relative_media_reference(normalized_cover_url))
        or _is_https_jpeg_url(normalized_cover_url)
    ):
        raise ArticlePublishError(
            "文章封面必须是完整 HTTPS JPG/JPEG 地址或官方 FastDFS url 路径",
            code="publish_cover_url_invalid",
            stage="cover_public_url_check",
            status_code=503,
        )
    # The GIF itself becomes the cover only on the official FastDFS path: the
    # legacy remote uploader still requires an HTTPS JPEG cover.
    normalized_litpic = normalized_cover_url
    candidate_litpic = str(litpic_url or "").strip()
    if (
        article_litpic_source() == "gif"
        and allow_relative_media
        and _is_relative_media_reference(candidate_litpic)
    ):
        normalized_litpic = candidate_litpic
    normalized_archive_id: int | None = None
    if archive_id is not None:
        archive_id_text = str(archive_id).strip()
        if not re.fullmatch(r"\d{1,20}", archive_id_text):
            raise ArticlePublishError(
                "更新草稿需要有效的数字文章 ID",
                code="publish_archive_id_invalid",
                stage="request_validation",
                status_code=400,
            )
        normalized_archive_id = int(archive_id_text)
    code = str(event.get("code") or "").upper()
    is_draft = delivery_mode == "draft"
    fields: dict[str, Any] = {
        "title": title,
        "body": (
            f'<p><img src="{html.escape(gif_url, quote=True)}" alt="比赛 GIF"></p>'
        ),
        "archive_level": "B",
        "status": 0 if is_draft else 1,
        "type": "article",
        "style": "gif",
        "litpic": normalized_litpic,
        "match_id": match_id,
        "add_to_tab": article_add_to_tab(),
    }
    channelsnew = article_channelsnew()
    if channelsnew:
        fields["channelsnew"] = channelsnew
    tabs_out: list[str] = []
    for item in [*article_tabs(), *(tabs or ())]:
        text = str(item).strip()
        if text and text not in tabs_out:
            tabs_out.append(text)
    if tabs_out:
        # Platform expects PHP-style array params; urlencode(doseq=True) emits
        # one ``tabs[]=id`` pair per element. Fixed ARTICLE_TABS come first,
        # then the per-team columns from playerteam.
        fields["tabs[]"] = tabs_out
    normalized_channels = str(channels or "").strip()
    if normalized_channels:
        if not re.fullmatch(r"\d{1,20}(,\d{1,20})*", normalized_channels):
            raise ArticlePublishError(
                "主标签 channels 必须是逗号分隔的数字标签 id",
                code="publish_channels_invalid",
                stage="request_validation",
                status_code=400,
            )
        fields["channels"] = normalized_channels
    if publish_account is not None:
        user_id_text = str(publish_account.get("user_id") or "").strip()
        user_name = str(publish_account.get("user_name") or "").strip()
        if (
            not re.fullmatch(r"[1-9]\d{0,18}", user_id_text)
            or int(user_id_text) > MAX_USER_ID
            or not user_name
        ):
            raise ArticlePublishError(
                "文章绑定的发布账号信息不完整",
                code="publish_account_invalid",
                stage="account_selection",
                status_code=409,
            )
        fields["user_id"] = int(user_id_text)
        fields["user_name"] = user_name
    if normalized_archive_id is not None:
        fields["archive_id"] = normalized_archive_id
    match_time = _match_time(event)
    if match_time:
        fields["match_time"] = match_time
    # Operator clips (8901) pick the CMS event type on the page and send it
    # explicitly (0 = 无 is sent too, so 跳过 lands as "no event"); OCR events
    # derive it from the event code. ``manual_match_event`` is the legacy
    # "leave it to the CMS editor" flag from before the page had the picker.
    explicit_match_event = event.get("match_event")
    if explicit_match_event is not None and str(explicit_match_event).strip() != "":
        fields["match_event"] = int(explicit_match_event)
    elif code in EVENT_TYPES and not event.get("manual_match_event"):
        fields["match_event"] = EVENT_TYPES[code]
    score = _match_score(event.get("score"))
    if score:
        fields["match_score"] = score
    return fields


def _draft_diagnostics(
    *,
    fields: dict[str, Any],
    gif: dict[str, Any],
    elapsed_ms: float,
    platform_message: str,
    http_status: int | None = None,
) -> dict[str, Any]:
    """Return safe debugging details without credentials or request bodies."""
    summary = {
        key: fields.get(key)
        for key in (
            "status",
            "type",
            "style",
            "match_id",
            "match_time",
            "match_event",
            "match_score",
            "add_to_tab",
            "channels",
            "channelsnew",
            "tabs[]",
            "archive_id",
            "litpic",
            "user_id",
            "user_name",
        )
        if fields.get(key) is not None
    }
    diagnostics = {
        "http_status": http_status,
        "gif_bytes": int(gif.get("bytes") or 0),
        "elapsed_ms": round(max(float(elapsed_ms), 0.0), 1),
        "request_summary": summary,
        "platform_message": str(platform_message or ""),
    }
    upload_diagnostics = gif.get("upload_diagnostics")
    if isinstance(upload_diagnostics, dict) and upload_diagnostics:
        diagnostics["image_upload"] = dict(upload_diagnostics)
    return diagnostics


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return dict(decoded) if isinstance(decoded, dict) else {}


def _diagnostics_json(value: Any) -> str | None:
    if not isinstance(value, dict) or not value:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError):
        return None


def _remote_error_message(raw: str) -> str:
    """Extract a short server-side error without echoing a full response."""
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        payload = None
    if isinstance(payload, dict) and payload.get("error"):
        return str(payload["error"])[:240]
    text = " ".join(str(raw or "").split())
    return text[:240] or "未知错误"


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


def _is_https_jpeg_url(value: Any) -> bool:
    try:
        parsed = urllib.parse.urlsplit(str(value or "").strip())
    except ValueError:
        return False
    return (
        parsed.scheme.lower() == "https"
        and bool(parsed.netloc)
        and parsed.path.lower().endswith((".jpg", ".jpeg"))
    )


def _is_absolute_http_url(value: Any) -> bool:
    try:
        parsed = urllib.parse.urlsplit(str(value or "").strip())
    except ValueError:
        return False
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def _is_media_reference(value: Any, *, allow_relative: bool = False) -> bool:
    """Validate a returned image reference without assuming a file suffix."""
    text = str(value or "").strip()
    if not text:
        return False
    if _is_absolute_http_url(text):
        return text.lower().startswith("https://")
    if not allow_relative:
        return False
    parsed = urllib.parse.urlsplit(text)
    return (
        not parsed.scheme
        and not parsed.netloc
        and parsed.path.startswith("/")
        and ".." not in Path(parsed.path).parts
    )


def _is_relative_media_reference(value: Any) -> bool:
    text = str(value or "").strip()
    return bool(text) and _is_media_reference(text, allow_relative=True) and not _is_absolute_http_url(text)


def _official_gif_litpic(gif: dict[str, Any]) -> str:
    """FastDFS path of the uploaded GIF, for use as the article ``litpic``.

    A fresh official upload carries ``official_gif_path``. A mapping reused
    from ``article_uploaded_gifs`` only persists the full ``img1_url``, which
    the platform documents as ``image domain + url``; its path component is
    therefore the same FastDFS path. Returns ``""`` when neither applies so
    the caller falls back to the JPEG cover.
    """
    direct = str(gif.get("official_gif_path") or "").strip()
    if _is_relative_media_reference(direct):
        return direct
    url = str(gif.get("url") or "").strip()
    if not _is_absolute_http_url(url):
        return ""
    try:
        path = urllib.parse.urlsplit(url).path
    except ValueError:
        return ""
    # A bare "/" or directory-only path is not a file reference.
    if not Path(path).name or not _is_relative_media_reference(path):
        return ""
    return path


def _find_official_image_result(results: list[Any] | tuple[Any, ...], *, kind: str) -> dict[str, Any] | None:
    expected_mimes = {
        "gif": {"image/gif", "image/gif; charset=binary"},
        "cover": {"image/jpeg", "image/jpg", "image/jpg; charset=binary"},
    }
    expected_suffixes = {"gif": (".gif",), "cover": (".jpg", ".jpeg")}
    for item in results:
        if not isinstance(item, dict):
            continue
        mime = str(item.get("mime") or item.get("content_type") or "").split(";", 1)[0].strip().lower()
        if mime in expected_mimes[kind]:
            return item
        name = str(item.get("file_name") or item.get("filename") or "").lower()
        if name.endswith(expected_suffixes[kind]):
            return item
    return None


def _check_public_gif_url(url: str) -> None:
    _check_public_media_url(
        url,
        expected_content_type="image/gif",
        label="GIF",
        unreachable_code="publish_gif_public_unreachable",
        invalid_code="publish_gif_public_invalid",
        stage="public_url_check",
    )


def _check_public_cover_url(url: str) -> None:
    _check_public_media_url(
        url,
        expected_content_type="image/jpeg",
        label="封面",
        unreachable_code="publish_cover_public_unreachable",
        invalid_code="publish_cover_public_invalid",
        stage="cover_public_url_check",
    )


def _check_public_media_url(
    url: str,
    *,
    expected_content_type: str,
    label: str,
    unreachable_code: str,
    invalid_code: str,
    stage: str,
) -> None:
    request = urllib.request.Request(
        url,
        method="HEAD",
        headers={
            "Accept": expected_content_type,
            "User-Agent": "football-gif-publisher/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=8.0) as response:
            content_type = str(response.headers.get("Content-Type") or "")
            status = int(getattr(response, "status", 200))
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise ArticlePublishError(
            f"公网{label}地址不可访问：{exc}",
            code=unreachable_code,
            stage=stage,
            status_code=503,
            retriable=True,
        ) from exc
    actual_content_type = content_type.split(";", 1)[0].strip().lower()
    if status != 200 or actual_content_type != expected_content_type:
        raise ArticlePublishError(
            f"公网{label}检查失败：HTTP {status}，Content-Type={content_type or '缺失'}",
            code=invalid_code,
            stage=stage,
            status_code=503,
        )


def _public_record(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    value = dict(row)
    account = None
    if value.get("publish_user_id") is not None or value.get("publish_user_name"):
        account = {
            "user_id": value.get("publish_user_id"),
            "user_name": value.get("publish_user_name"),
        }
    return {
        "stable_id": value.get("stable_id"),
        "match_id": value.get("match_id"),
        "event_key": value.get("event_key"),
        "source_path": value.get("source_path"),
        "gif_url": value.get("gif_url"),
        "cover_url": value.get("cover_url"),
        "title": value.get("title"),
        "status": value.get("status"),
        "stage": value.get("stage"),
        "article_id": value.get("article_id"),
        "platform_code": value.get("platform_code"),
        "duplicate": bool(value.get("duplicate")),
        "error": value.get("error"),
        "updated_at_unix": value.get("updated_at_unix"),
        "published_at_unix": value.get("published_at_unix"),
        "publish_account": account,
        "account_user_id": value.get("publish_user_id"),
        "account_user_name": value.get("publish_user_name"),
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
