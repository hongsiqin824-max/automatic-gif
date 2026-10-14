#!/usr/bin/env python3
"""Build and deliver the daily OCR GIF publication report.

The report is intentionally a read-mostly sidecar.  It reads durable event,
OCR, and article-delivery state without importing the Dashboard process, so a
failed report request cannot stop match workers or article publishing.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "output_gifs" / "dashboard"
DEFAULT_ARTICLE_DATABASE = ROOT / "data" / "article_publish.sqlite3"
DEFAULT_TIMEZONE = "Asia/Shanghai"
DEFAULT_SEND_TIMEOUT_SECONDS = 15.0
MAX_REASON_LENGTH = 180
MAX_FEISHU_TEXT_LENGTH = 28_000


def _load_dotenv(path: Path) -> None:
    """Load simple KEY=VALUE settings without overriding shell variables."""
    if not path.is_file():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, value = line.partition("=")
        key = key.strip()
        if not separator or not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class ReportWindow:
    start_unix: float
    end_unix: float
    timezone_name: str
    start_local: datetime
    end_local: datetime


@dataclass
class EventOutcome:
    match_id: str
    event_key: str
    code: str
    event_time: str
    status: str
    reason: str = ""
    person: str = ""


@dataclass
class MatchSummary:
    match_id: str
    team_a: str = ""
    team_b: str = ""
    outcomes: list[EventOutcome] = field(default_factory=list)

    @property
    def expected(self) -> int:
        return len(self.outcomes)

    def counts(self) -> dict[str, int]:
        values = {
            "published": 0,
            "ocr_failed": 0,
            "publish_failed": 0,
            "held": 0,
            "pending": 0,
            "not_registered": 0,
        }
        for outcome in self.outcomes:
            values[outcome.status] = values.get(outcome.status, 0) + 1
        return values

    @property
    def publication_result(self) -> str:
        counts = self.counts()
        if counts["published"] == self.expected:
            return "发布成功"
        if counts["published"]:
            return "部分发布"
        return "未发布"


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"无效统计时区：{name}") from exc


def _parse_datetime(value: str, tz: ZoneInfo) -> datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"无效时间：{value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def completed_report_window(
    now: datetime | None = None,
    *,
    timezone_name: str = DEFAULT_TIMEZONE,
) -> ReportWindow:
    """Return the most recently completed local 19:00-to-19:00 window."""
    tz = _zone(timezone_name)
    current = (now or datetime.now(tz)).astimezone(tz)
    boundary = current.replace(hour=19, minute=0, second=0, microsecond=0)
    if current < boundary:
        end_local = boundary - timedelta(days=1)
    else:
        end_local = boundary
    start_local = end_local - timedelta(days=1)
    return ReportWindow(
        start_unix=start_local.timestamp(),
        end_unix=end_local.timestamp(),
        timezone_name=timezone_name,
        start_local=start_local,
        end_local=end_local,
    )


def _read_only_connect(path: Path) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    connection = sqlite3.connect(
        f"file:{resolved}?mode=ro", uri=True, timeout=5.0
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def _json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return dict(decoded) if isinstance(decoded, Mapping) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _short_reason(*values: Any) -> str:
    for value in values:
        text = _text(value)
        if text:
            return text[:MAX_REASON_LENGTH]
    return "未记录具体原因"


def _event_time(row: sqlite3.Row, event: Mapping[str, Any]) -> str:
    minute = event.get("minute")
    extra = event.get("minute_extra")
    if minute is None:
        return "比赛中"
    value = _text(minute)
    if extra not in (None, "", 0, "0"):
        value += f"+{_text(extra)}"
    return value + "'"


def _match_name(detail: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _text(detail.get("team_A_name") or detail.get("team_a_name")),
        _text(detail.get("team_B_name") or detail.get("team_b_name")),
    )


def _load_delivery_rows(article_database: Path) -> dict[tuple[str, str], dict[str, Any]]:
    if not article_database.is_file():
        return {}
    try:
        with _read_only_connect(article_database) as connection:
            table = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                ("article_delivery_tasks",),
            ).fetchone()
            if table is None:
                return {}
            rows = connection.execute(
                "SELECT * FROM article_delivery_tasks "
                "WHERE artifact_kind='ocr_window' AND delivery_mode='draft' "
                "ORDER BY updated_at_unix DESC, created_at_unix DESC"
            ).fetchall()
    except sqlite3.Error:
        return {}
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (_text(row["match_id"]), _text(row["event_key"]))
        result.setdefault(key, dict(row))
    return result


def _classify_event(
    event_row: sqlite3.Row,
    vision_row: sqlite3.Row | None,
    delivery_row: Mapping[str, Any] | None,
) -> EventOutcome:
    event = _json_object(event_row["event_json"])
    event_status = _text(event_row["status"])
    vision_status = _text(vision_row["status"]) if vision_row is not None else ""
    delivery_status = _text(delivery_row.get("status")) if delivery_row else ""
    reason = ""
    if delivery_status == "published":
        status = "published"
    elif delivery_status == "held":
        status = "held"
        reason = _short_reason(
            delivery_row.get("error"),
            delivery_row.get("publish_reason"),
            delivery_row.get("error_code"),
        )
    elif delivery_status == "failed":
        status = "publish_failed"
        reason = _short_reason(
            delivery_row.get("error"),
            delivery_row.get("error_code"),
        )
    elif delivery_status:
        status = "pending"
        reason = _short_reason(
            delivery_row.get("error"),
            f"文章队列状态：{delivery_status}",
        )
    elif vision_status == "failed":
        status = "ocr_failed"
        reason = _short_reason(
            vision_row["failure_reason"],
            vision_row["error"],
            vision_row["last_error_kind"],
        )
    elif vision_status in {"pending", "locating", "located", "encoding"}:
        status = "pending"
        reason = _short_reason(f"OCR 状态：{vision_status}")
    elif vision_status == "encoded":
        status = "not_registered"
        reason = "OCR GIF 已生成，但没有登记到文章发布队列。"
    elif event_status == "failed":
        status = "ocr_failed"
        reason = _short_reason(event_row["error"], event_row["last_error_kind"])
    elif event_status in {"discovered", "pending", "encoding"}:
        status = "pending"
        reason = _short_reason(f"事件处理状态：{event_status}")
    else:
        status = "not_registered"
        reason = "检测到事件，但没有找到 OCR 处理记录。"
    return EventOutcome(
        match_id=_text(event_row["match_id"]),
        event_key=_text(event_row["event_key"]),
        code=_text(event_row["code"]),
        event_time=_event_time(event_row, event),
        status=status,
        reason=reason,
        person=_text(event.get("person")),
    )


def collect_report(
    output_root: Path,
    article_database: Path,
    window: ReportWindow,
) -> list[MatchSummary]:
    """Collect final outcomes for distinct events detected in *window*."""
    deliveries = _load_delivery_rows(article_database)
    summaries: dict[str, MatchSummary] = {}
    if not output_root.is_dir():
        return []
    try:
        entries = sorted(output_root.iterdir(), key=lambda item: item.name)
    except OSError:
        return []
    for entry in entries:
        state_path = entry / "pipeline_state.sqlite3"
        if not entry.is_dir() or not state_path.is_file():
            continue
        try:
            with _read_only_connect(state_path) as connection:
                tables = {
                    _text(row["name"])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                if "event_tasks" not in tables:
                    continue
                event_rows = connection.execute(
                    "SELECT * FROM event_tasks "
                    "WHERE discovered_at_unix >= ? AND discovered_at_unix < ? "
                    "AND COALESCE(suppressed_by_event_key, '') = '' "
                    "ORDER BY discovered_at_unix, event_key",
                    (window.start_unix, window.end_unix),
                ).fetchall()
                vision_by_key: dict[str, sqlite3.Row] = {}
                if "vision_tasks" in tables and event_rows:
                    keys = [_text(row["event_key"]) for row in event_rows]
                    placeholders = ",".join("?" for _ in keys)
                    vision_rows = connection.execute(
                        "SELECT * FROM vision_tasks WHERE artifact_kind='ocr_window' "
                        f"AND event_key IN ({placeholders})",
                        keys,
                    ).fetchall()
                    vision_by_key = {
                        _text(row["event_key"]): row for row in vision_rows
                    }
        except (OSError, sqlite3.Error):
            continue
        for event_row in event_rows:
            match_id = _text(event_row["match_id"]) or entry.name
            event_key = _text(event_row["event_key"])
            outcome = _classify_event(
                event_row,
                vision_by_key.get(event_key),
                deliveries.get((match_id, event_key)),
            )
            summary = summaries.setdefault(match_id, MatchSummary(match_id=match_id))
            summary.outcomes.append(outcome)
            delivery = deliveries.get((match_id, event_key))
            if delivery:
                detail = _json_object(delivery.get("match_detail_json"))
                if not summary.team_a and not summary.team_b:
                    summary.team_a, summary.team_b = _match_name(detail)
    return [summaries[key] for key in sorted(summaries)]


STATUS_LABELS = {
    "published": "已发布",
    "ocr_failed": "OCR失败",
    "publish_failed": "发布失败",
    "held": "规则拦截",
    "pending": "处理中/待重试",
    "not_registered": "未登记",
}


def format_report(
    summaries: Iterable[MatchSummary],
    window: ReportWindow,
) -> str:
    items = list(summaries)
    totals = {
        key: sum(summary.counts().get(key, 0) for summary in items)
        for key in STATUS_LABELS
    }
    total_expected = sum(item.expected for item in items)
    lines = [
        "足球 GIF 自动发布日报",
        f"统计窗口：{window.start_local:%Y-%m-%d %H:%M} 至 {window.end_local:%Y-%m-%d %H:%M}（{window.timezone_name}）",
        f"比赛数：{len(items)} | 应发事件：{total_expected} | 发布成功：{totals['published']} | 未成功：{total_expected - totals['published']}",
        "",
    ]
    if not items:
        lines.append("本统计窗口没有检测到事件。")
        return "\n".join(lines)
    for summary in items:
        counts = summary.counts()
        name = " vs ".join(part for part in (summary.team_a, summary.team_b) if part)
        label = f"{summary.match_id}（{name}）" if name else summary.match_id
        lines.append(
            f"比赛 {label}：应发 {summary.expected} | 成功 {counts['published']} | "
            f"失败 {summary.expected - counts['published']} "
            f"(OCR {counts['ocr_failed']}/发布 {counts['publish_failed']}/拦截 {counts['held']}/"
            f"待处理 {counts['pending']}/未登记 {counts['not_registered']}) | "
            f"最终结果：{summary.publication_result}"
        )
    reasons: dict[tuple[str, str], int] = {}
    for summary in items:
        for outcome in summary.outcomes:
            if outcome.status == "published":
                continue
            key = (STATUS_LABELS.get(outcome.status, outcome.status), outcome.reason)
            reasons[key] = reasons.get(key, 0) + 1
    if reasons:
        lines.extend(("", "失败原因汇总（按相同原因合并）："))
        for (status, reason), count in sorted(
            reasons.items(), key=lambda item: (-item[1], item[0][0], item[0][1])
        )[:20]:
            lines.append(f"- {status} × {count}：{reason}")
        if len(reasons) > 20:
            lines.append(f"- 其余 {len(reasons) - 20} 种原因未展开。")
    lines.append("")
    lines.append(
        "总计：" + " | ".join(
            f"{STATUS_LABELS[key]} {totals[key]}" for key in STATUS_LABELS
        )
    )
    text = "\n".join(lines)
    # Feishu limits the UTF-8 payload, not Python's Unicode code-point count.
    # Keep every match summary and the totals; trim only the optional reason
    # list if an unusually large schedule still exceeds the safe limit.
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_FEISHU_TEXT_LENGTH:
        total_line = "总计：" + " | ".join(
            f"{STATUS_LABELS[key]} {totals[key]}" for key in STATUS_LABELS
        )
        mandatory = lines[:4]
        match_lines = [line for line in lines[4:] if line.startswith("比赛 ")]
        for line in match_lines:
            candidate = "\n".join(mandatory + [line])
            if len((candidate + "\n\n" + total_line).encode("utf-8")) > MAX_FEISHU_TEXT_LENGTH:
                break
            mandatory.append(line)
        if len(mandatory) - 4 < len(match_lines):
            mandatory.extend(["", "（比赛汇总过多，部分比赛行未展开）"])
        mandatory.extend(["", total_line])
        text = "\n".join(mandatory)
    return text


def _ensure_delivery_log(database: Path) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database, timeout=10.0) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_report_deliveries (
                report_key TEXT PRIMARY KEY,
                window_start_unix REAL NOT NULL,
                window_end_unix REAL NOT NULL,
                status TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                sent_at_unix REAL,
                updated_at_unix REAL NOT NULL,
                error TEXT
            )
            """
        )


def _claim_report(database: Path, report_key: str, payload_sha256: str, *, force: bool) -> bool:
    _ensure_delivery_log(database)
    now = time.time()
    with sqlite3.connect(database, timeout=10.0) as connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT status,updated_at_unix FROM daily_report_deliveries WHERE report_key=?",
            (report_key,),
        ).fetchone()
        if row and row[0] == "sent" and not force:
            return False
        if (
            row
            and row[0] == "sending"
            and not force
            and now - float(row[1] or 0) < 15 * 60
        ):
            return False
        window_start, window_end = (float(value) for value in report_key.split(":", 1))
        connection.execute(
            "INSERT INTO daily_report_deliveries "
            "(report_key,window_start_unix,window_end_unix,status,payload_sha256,updated_at_unix,error) "
            "VALUES (?,?,?,?,?,?,NULL) "
            "ON CONFLICT(report_key) DO UPDATE SET status='sending',payload_sha256=?,updated_at_unix=?,error=NULL",
            (
                report_key,
                window_start,
                window_end,
                "sending",
                payload_sha256,
                now,
                payload_sha256,
                now,
            ),
        )
    return True


def _finish_report(database: Path, report_key: str, *, sent: bool, error: str | None = None) -> None:
    with sqlite3.connect(database, timeout=10.0) as connection:
        connection.execute(
            "UPDATE daily_report_deliveries SET status=?,sent_at_unix=?,updated_at_unix=?,error=? WHERE report_key=?",
            ("sent" if sent else "failed", time.time() if sent else None, time.time(), error, report_key),
        )


def send_feishu(webhook_url: str, text: str, *, timeout_seconds: float = DEFAULT_SEND_TIMEOUT_SECONDS) -> dict[str, Any]:
    payload = json.dumps(
        {"msg_type": "text", "content": {"text": text}},
        ensure_ascii=False,
    ).encode("utf-8")
    request = urllib.request.Request(
        webhook_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            status_code = int(response.status)
    except (OSError, urllib.error.URLError) as exc:
        raise RuntimeError(f"飞书请求失败：{exc}") from exc
    if status_code < 200 or status_code >= 300:
        raise RuntimeError(f"飞书 HTTP 状态异常：{status_code}")
    try:
        result = json.loads(body) if body else {}
    except ValueError as exc:
        raise RuntimeError("飞书返回不是合法 JSON") from exc
    if isinstance(result, Mapping):
        response_code = result.get("code", result.get("StatusCode", 0))
        try:
            response_code_value = int(response_code or 0)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"飞书返回异常 code：{response_code}") from exc
        if response_code_value != 0:
            raise RuntimeError(
                f"飞书接口返回错误：{response_code} "
                f"{result.get('msg') or result.get('StatusMessage') or ''}".strip()
            )
    return dict(result) if isinstance(result, Mapping) else {}


def run_report(
    *,
    output_root: Path,
    article_database: Path,
    webhook_url: str,
    window: ReportWindow,
    dry_run: bool = False,
    force: bool = False,
    timeout_seconds: float = DEFAULT_SEND_TIMEOUT_SECONDS,
) -> tuple[str, bool]:
    summaries = collect_report(output_root, article_database, window)
    text = format_report(summaries, window)
    if dry_run:
        return text, False
    if not webhook_url.strip():
        raise ValueError("未配置 GIF_DAILY_REPORT_FEISHU_WEBHOOK")
    report_key = f"{window.start_unix:.6f}:{window.end_unix:.6f}"
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if not _claim_report(article_database, report_key, digest, force=force):
        return text, True
    try:
        send_feishu(webhook_url.strip(), text, timeout_seconds=timeout_seconds)
    except Exception as exc:
        _finish_report(article_database, report_key, sent=False, error=str(exc))
        raise
    _finish_report(article_database, report_key, sent=True)
    return text, False


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="发送足球 GIF 19:00 日报")
    parser.add_argument("--output-root", type=Path, default=Path(os.environ.get("GIF_DASHBOARD_OUTPUT_ROOT", DEFAULT_OUTPUT_ROOT)))
    parser.add_argument(
        "--article-database",
        type=Path,
        default=Path(
            os.environ.get(
                "ARTICLE_PUBLISH_DB_PATH",
                os.environ.get("GIF_ARTICLE_PUBLISH_DATABASE", DEFAULT_ARTICLE_DATABASE),
            )
        ),
    )
    parser.add_argument("--timezone", default=os.environ.get("GIF_DAILY_REPORT_TIMEZONE", DEFAULT_TIMEZONE))
    parser.add_argument("--webhook-url", default=os.environ.get("GIF_DAILY_REPORT_FEISHU_WEBHOOK", ""))
    parser.add_argument("--now", help="测试用当前时间，ISO-8601；不传则使用系统时间")
    parser.add_argument("--dry-run", action="store_true", help="只打印日报，不发送飞书")
    parser.add_argument("--force", action="store_true", help="允许重复发送同一个统计窗口")
    parser.add_argument("--timeout", type=float, default=DEFAULT_SEND_TIMEOUT_SECONDS)
    return parser


def main(argv: list[str] | None = None) -> int:
    _load_dotenv(ROOT / ".env")
    args = _parser().parse_args(argv)
    try:
        tz = _zone(args.timezone)
        now = _parse_datetime(args.now, tz) if args.now else datetime.now(tz)
        window = completed_report_window(now, timezone_name=args.timezone)
        text, skipped = run_report(
            output_root=args.output_root,
            article_database=args.article_database,
            webhook_url=args.webhook_url,
            window=window,
            dry_run=args.dry_run,
            force=args.force,
            timeout_seconds=args.timeout,
        )
        if args.dry_run or skipped:
            print(text)
        if skipped:
            print("[daily-report] 该统计窗口已经发送过，已跳过重复发送。", file=sys.stderr)
        return 0
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        print(f"[daily-report:error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
