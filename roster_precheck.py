"""Pre-check the GIF roster: will every scheduled match have a live source?

The GIF 排班表 (Feishu sheet, one sub-sheet per month) lists the matches the
team must cover; it is fixed and is what GIFs are made from. For every match
on it this service checks, ahead of kick-off, that

1. 8901 can see it: the match is in the match directory (8901's list and the
   clipper's dropdown are built from it). If not, the 懂球帝 back office
   (dadmin) is searched by team: either the directory missed the match (its id
   is written to ``data/roster_precheck/extra_matches.json`` so 8901 can add
   it later), or it was rescheduled, or it cannot be found at all;
2. it has a live source: 懂球帝 record, data-provider record or pull API;
3. a registered source is really pushing, once the match has kicked off.

Problems go to one Feishu group (``GIF_ROSTER_PRECHECK_WEBHOOK``), batched per
run, each match and stage at most once:

* 20:00 daily - summary of the next 24 hours: matches 8901 will not list;
* kick-off -2 h - no source yet (data-provider sources usually appear ~5 h
  before kick-off, 懂球帝 ones ~1.6 days before; checked 2026-11-12);
* kick-off -30 min - still no source (urgent), or 已补上;
* kick-off +5 min - no source, or none of the registered sources delivers.

Runs as its own service beside 8901 (``deploy/gif-roster-precheck.service``)
and never touches the 8901 process. ``--dry-run`` prints the messages instead
of sending them and keeps no state; ``--once`` runs a single cycle.
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent
BEIJING = timezone(timedelta(hours=8))
FEISHU_HOST = "https://open.feishu.cn"
DADMIN_HOST = "https://dadmin.dongqiudi.com"
DADMIN_ID_OFFSET = 50_000_000

SAME_FIXTURE_SECONDS = 3 * 3600        # roster time vs real kick-off
MOVED_SEARCH_DAYS = 3                  # look this far for a rescheduled match
HORIZON_BEFORE = 3 * 3600              # rows still handled after kick-off
HORIZON_AFTER = 36 * 3600              # rows handled before kick-off
SUMMARY_HOUR = 20
SUMMARY_SPAN = 24 * 3600
ROSTER_REFRESH_SECONDS = 10 * 60
CATALOG_REFRESH_SECONDS = 30 * 60
CATALOG_RETRY_SECONDS = 5 * 60
DADMIN_RETRY_SECONDS = 2 * 3600
LOOP_SECONDS = 60
STATE_KEEP_SECONDS = (MOVED_SEARCH_DAYS + 2) * 86400

# (name, opens at kick-off + start, closes at kick-off + end)
STAGES = (
    ("t2h", -2 * 3600, -30 * 60),
    ("t30", -30 * 60, 5 * 60),
    ("live", 5 * 60, 30 * 60),
)

_DATE_RE = re.compile(r"^(\d{1,2})月(\d{1,2})日$")
_TIME_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
_SHEET_RE = re.compile(r"^(\d{4})年(\d{1,2})月$")
_DADMIN_ROW_RE = re.compile(r'<tr class="match-row" match_id="(\d+)"[^>]*>([\s\S]*?)</tr>')
_TD_RE = re.compile(r"<td[^>]*>([\s\S]*?)</td>")


def _log(event: str, **fields: Any) -> None:
    print(json.dumps({"event": event, **fields}, ensure_ascii=False), flush=True)


def _load_dotenv(path: Path) -> None:
    """Same ``KEY=value`` rules as monitor_server: the process env wins."""
    for key, value in read_env_file(path).items():
        os.environ.setdefault(key, value)


def read_env_file(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    out: dict[str, str] = {}
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if key:
            out[key] = value
    return out


def fmt_bj(ts: float, pattern: str = "%m-%d %H:%M") -> str:
    return datetime.fromtimestamp(ts, BEIJING).strftime(pattern)


# ── roster (pure) ───────────────────────────────────────────────────────────


@dataclass
class RosterRow:
    kickoff: float            # roster time, Beijing
    competition: str
    home: str
    away: str
    staff: str = ""
    grade: str = ""
    form: str = ""

    @property
    def key(self) -> str:
        return f"{fmt_bj(self.kickoff, '%Y-%m-%d %H:%M')}|{self.home}vs{self.away}"


def _cell(row: list[Any], index: int) -> str:
    value = row[index] if index < len(row) else None
    return "" if value is None else str(value).strip()


def _parse_time(value: Any) -> tuple[int, int] | None:
    if isinstance(value, (int, float)) and 0 <= value < 1:
        minutes = round(value * 24 * 60)
        return minutes // 60, minutes % 60
    match = _TIME_RE.match(str(value or "").strip())
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    return (hour, minute) if hour < 24 and minute < 60 else None


def parse_roster(values: list[list[Any]], year: int, sheet_month: int) -> list[RosterRow]:
    """One month sub-sheet -> rows (same layout rules as project1's report).

    A 「11月1日」 cell starts a day; each following row with a time in A and
    「主vs客」 in C is a match. F = GIF staff (E when F is empty or 「/」),
    G = grade, H = output form.
    """
    rows: list[RosterRow] = []
    day: tuple[int, int, int] | None = None
    for row in values or []:
        if not row:
            continue
        first = _cell(row, 0)
        date_match = _DATE_RE.match(first)
        if date_match:
            month = int(date_match.group(1))
            rollover = 1 if sheet_month - month > 6 else 0
            day = (year + rollover, month, int(date_match.group(2)))
            continue
        hm = _parse_time(row[0])
        versus = _cell(row, 2)
        if not hm or not day or not versus:
            continue
        index = versus.lower().find("vs")
        home, away = (versus[:index].strip(), versus[index + 2:].strip()) if index >= 0 else (versus, "")
        staff = _cell(row, 5)
        if not staff or staff == "/":
            staff = _cell(row, 4)
        kickoff = datetime(day[0], day[1], day[2], hm[0], hm[1], tzinfo=BEIJING).timestamp()
        rows.append(RosterRow(
            kickoff=kickoff, competition=_cell(row, 1), home=home, away=away,
            staff="" if staff == "/" else staff, grade=_cell(row, 6), form=_cell(row, 7),
        ))
    return rows


def _norm(name: str) -> str:
    return re.sub(r"\s+", "", str(name or "")).lower()


def same_team(a: str, b: str) -> bool:
    """Loose team-name match, as project1's GIF report: containment, or more
    than 60% shared characters both ways."""
    x, y = _norm(a), _norm(b)
    if not x or not y:
        return False
    if x == y or x in y or y in x:
        return True
    if len(x) < 2 or len(y) < 2:
        return False
    overlap = len([ch for ch in set(x) if ch in y])
    return overlap / len(x) > 0.6 and overlap / len(y) > 0.6


def same_fixture(row: RosterRow, home: str, away: str) -> bool:
    return (same_team(row.home, home) and same_team(row.away, away)) or (
        same_team(row.home, away) and same_team(row.away, home)
    )


def find_fixture(row: RosterRow, candidates: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, bool]:
    """``(candidate, moved)``: the nearest same fixture; ``moved`` when it is
    more than 3 hours from the roster time (but within the search span)."""
    best: tuple[float, dict[str, Any]] | None = None
    for cand in candidates:
        if not same_fixture(row, cand.get("home", ""), cand.get("away", "")):
            continue
        gap = abs(float(cand["kickoff"]) - row.kickoff)
        if gap > MOVED_SEARCH_DAYS * 86400 + SAME_FIXTURE_SECONDS:
            continue
        if best is None or gap < best[0]:
            best = (gap, cand)
    if best is None:
        return None, False
    return best[1], best[0] > SAME_FIXTURE_SECONDS


def parse_dadmin_list(page: str) -> list[dict[str, Any]]:
    """dadmin 比赛列表 rows -> ``{match_id, competition, kickoff, home, away}``."""
    out: list[dict[str, Any]] = []
    for match in _DADMIN_ROW_RE.finditer(page or ""):
        cells = [
            re.sub(r"\s+", " ", html_lib.unescape(re.sub(r"<[^>]+>", " ", td))).strip()
            for td in _TD_RE.findall(match.group(2))
        ]
        if len(cells) < 4:
            continue
        teams = re.split(r"\s+VS\s+", cells[3], maxsplit=1, flags=re.IGNORECASE)
        try:
            kickoff = datetime.strptime(cells[2], "%Y-%m-%d %H:%M:%S").replace(tzinfo=BEIJING).timestamp()
        except ValueError:
            continue
        match_id = cells[0] if cells[0].isdigit() else str(int(match.group(1)) + DADMIN_ID_OFFSET)
        out.append({
            "match_id": match_id,
            "competition": cells[1],
            "kickoff": kickoff,
            "home": teams[0].strip(),
            "away": teams[1].strip() if len(teams) > 1 else "",
        })
    return out


def due_stage(kickoff: float, now: float, done: dict[str, Any]) -> str | None:
    for name, start, end in STAGES:
        if kickoff + start <= now < kickoff + end and name not in done:
            return name
    return None


# ── messages (pure) ─────────────────────────────────────────────────────────


def row_line(row: RosterRow, info: dict[str, Any]) -> str:
    kickoff = float(info.get("kickoff") or row.kickoff)
    parts = [f"{fmt_bj(kickoff)} {row.competition} {row.home} vs {row.away}"]
    tags = "｜".join(x for x in (row.grade, row.form, row.staff) if x)
    if tags:
        parts.append(f"（{tags}）")
    if info.get("match_id"):
        parts.append(f" 比赛ID {info['match_id']}")
    return "· " + "".join(parts)


SECTION_TITLES = {
    "not_listed": "8901 列表里看不到（赛事目录漏收，后台有这场）",
    "moved": "排班表时间和实际开球时间不一样（8901 按实际时间列出）",
    "missing": "赛事目录和懂球帝后台都找不到这场（可能写错或取消）",
    "unknown": "赛事目录里找不到，懂球帝后台暂时查不了（登录态失效？）",
    "t2h": "开球前 2 小时还没有直播源，请补",
    "t30": "开球前 30 分钟仍没有直播源，请尽快补",
    "live_none": "已开球，还没有直播源",
    "live_dead": "已开球，登记的直播源都没有画面",
    "fixed": "已补上直播源",
}
SECTION_ORDER = ("t30", "live_none", "live_dead", "t2h", "not_listed", "moved", "missing", "unknown", "fixed")


def compose(sections: dict[str, list[str]], header: str = "【排班表预检】") -> str | None:
    """One message for the run; ``None`` when there is nothing to say (a
    ``_summary`` key forces the header out even with no problem listed)."""
    blocks = []
    for name in SECTION_ORDER:
        lines = sections.get(name) or []
        if lines:
            blocks.append(f"{SECTION_TITLES[name]}（{len(lines)} 场）\n" + "\n".join(lines))
    if not blocks:
        return header if "_summary" in sections else None
    return header + "\n\n" + "\n\n".join(blocks)


# ── live parts ──────────────────────────────────────────────────────────────


class FeishuSheet:
    def __init__(self, app_id: str, app_secret: str, token: str) -> None:
        self.app_id, self.app_secret, self.token = app_id, app_secret, token
        self._tenant: tuple[str, float] | None = None

    def _call(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Content-Type": "application/json; charset=utf-8"}
        if not path.endswith("/tenant_access_token/internal"):
            headers["Authorization"] = "Bearer " + self._tenant_token()
        request = urllib.request.Request(
            FEISHU_HOST + path, method=method, headers=headers,
            data=json.dumps(body).encode("utf-8") if body is not None else None,
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
        if data.get("code") != 0:
            raise RuntimeError(f"飞书 {path} 返回 {data.get('code')}: {data.get('msg')}")
        return data

    def _tenant_token(self) -> str:
        if self._tenant and self._tenant[1] > time.time() + 300:
            return self._tenant[0]
        data = self._call("POST", "/open-apis/auth/v3/tenant_access_token/internal",
                          {"app_id": self.app_id, "app_secret": self.app_secret})
        self._tenant = (data["tenant_access_token"], time.time() + float(data.get("expire") or 3600))
        return self._tenant[0]

    def rows(self, now: float) -> list[RosterRow]:
        sheets = self._call("GET", f"/open-apis/sheets/v3/spreadsheets/{self.token}/sheets/query")["data"]["sheets"]
        wanted = {fmt_bj(now - 86400, "%Y-%-m"), fmt_bj(now, "%Y-%-m"), fmt_bj(now + 2 * 86400, "%Y-%-m")}
        out: list[RosterRow] = []
        for sheet in sheets:
            match = _SHEET_RE.match(str(sheet.get("title") or "").strip())
            if not match or f"{match.group(1)}-{int(match.group(2))}" not in wanted:
                continue
            grid = sheet.get("grid_properties") or {}
            rng = f"{sheet['sheet_id']}!A1:H{int(grid.get('row_count') or 1000)}"
            query = urllib.parse.urlencode({"valueRenderOption": "FormattedValue",
                                            "dateTimeRenderOption": "FormattedString"})
            data = self._call("GET", f"/open-apis/sheets/v2/spreadsheets/{self.token}/values/{rng}?{query}")
            values = data["data"]["valueRange"].get("values") or []
            out.extend(parse_roster(values, int(match.group(1)), int(match.group(2))))
        return out


def fetch_catalog(now: float) -> list[dict[str, Any]]:
    """Union of several directory windows (``start`` is not where a window
    starts, see 进度快照「踩过的坑」), as ``{match_id, kickoff, home, away}``."""
    from match_catalog import MATCH_CATALOG_URL, match_user, request_json
    from source_backup_probe import catalog_kickoff

    found: dict[str, dict[str, Any]] = {}
    for days in (-2, -1, 0, 1):
        start = fmt_bj(now + days * 86400, "%Y-%m-%d%H:%M:%S")
        url = MATCH_CATALOG_URL + "?" + urllib.parse.urlencode({"start": start, "init": "1", "user": match_user()})
        payload = request_json(url, timeout=30, accept_gzip=True)
        for item in payload.get("list") or []:
            if not isinstance(item, dict) or str(item.get("cmp_type") or "") != "soccer":
                continue
            try:
                kickoff = catalog_kickoff(item)
            except ValueError:
                continue
            found[str(item.get("match_id"))] = {
                "match_id": str(item.get("match_id")), "kickoff": kickoff,
                "home": item.get("team_A_name") or "", "away": item.get("team_B_name") or "",
                "competition": item.get("competition_name") or "",
            }
    return list(found.values())


class Dadmin:
    """Searches dadmin's match list with project1's ``DQD_SESSION`` (re-read
    on every call so a cookie refreshed on the /zhibo8/ page is picked up)."""

    def __init__(self, env_file: str) -> None:
        self.env_file = Path(env_file) if env_file else None

    def available(self) -> bool:
        return bool(self.env_file and read_env_file(self.env_file).get("DQD_SESSION"))

    def search(self, day: float, team: str) -> list[dict[str, Any]]:
        session = read_env_file(self.env_file).get("DQD_SESSION", "") if self.env_file else ""
        if not session:
            raise RuntimeError("没有 DQD_SESSION")
        ymd = fmt_bj(day, "%Y-%m-%d")
        query = urllib.parse.urlencode({"start": f"{ymd} 00:00:00", "end": f"{ymd} 23:59:59",
                                        "tab": "all", "competition": "", "team": team})
        request = urllib.request.Request(
            f"{DADMIN_HOST}/admin/live/new/match_live_list?{query}",
            headers={"Cookie": f"laravel_session={session}", "User-Agent": "Mozilla/5.0"},
        )

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args: Any, **kwargs: Any) -> None:
                return None

        opener = urllib.request.build_opener(_NoRedirect)
        try:
            with opener.open(request, timeout=20) as response:
                page = response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as exc:
            if exc.code in (301, 302, 401):
                raise RuntimeError("DQD_SESSION 已失效（dadmin 跳转登录）") from exc
            raise
        return parse_dadmin_list(page)


def send_webhook(url: str, text: str) -> bool:
    payload = json.dumps({"msg_type": "text", "content": {"text": text}}).encode("utf-8")
    request = urllib.request.Request(url, data=payload, method="POST",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=15) as response:
        body = json.loads(response.read().decode("utf-8") or "{}")
    code = body.get("code", body.get("StatusCode", 0))
    if code not in (0, None):
        raise RuntimeError(f"飞书群机器人返回 {code}: {body.get('msg') or body.get('StatusMessage')}")
    return True


# ── the check ───────────────────────────────────────────────────────────────


@dataclass
class Precheck:
    read_roster: Callable[[float], list[RosterRow]]
    fetch_catalog: Callable[[float], list[dict[str, Any]]]
    dadmin_search: Callable[[float, str], list[dict[str, Any]]] | None
    collect_sources: Callable[[str], list[dict[str, Any]]]
    probe: Callable[[str], dict[str, Any]]
    send: Callable[[str], Any]
    state_path: Path | None
    extra_path: Path | None
    clock: Callable[[], float] = time.time
    state: dict[str, Any] = field(default_factory=dict)
    _roster: tuple[float, list[RosterRow]] | None = None
    _catalog: tuple[float, list[dict[str, Any]]] | None = None
    _catalog_failed_at: float = float("-inf")

    def __post_init__(self) -> None:
        if self.state_path is not None and self.state_path.exists():
            try:
                self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                self.state = {}
        self.state.setdefault("rows", {})
        self.state.setdefault("summaries", [])

    # inputs, cached
    def _rows(self, now: float) -> list[RosterRow]:
        if self._roster is None or now - self._roster[0] >= ROSTER_REFRESH_SECONDS:
            self._roster = (now, self.read_roster(now))
        return self._roster[1]

    def _catalog_items(self, now: float) -> list[dict[str, Any]]:
        if self._catalog is None or now - self._catalog[0] >= CATALOG_REFRESH_SECONDS:
            if now - self._catalog_failed_at < CATALOG_RETRY_SECONDS:
                # Failed a moment ago: one attempt per run, not one per row.
                raise RuntimeError("赛事目录刚才没拉到，稍后再试")
            try:
                self._catalog = (now, self.fetch_catalog(now))
            except Exception:
                self._catalog_failed_at = now
                raise
        return self._catalog[1]

    def _resolve(self, row: RosterRow, info: dict[str, Any], now: float) -> None:
        """Fill ``info`` with match_id / kickoff / how it was found."""
        if info.get("how") == "catalog":
            return
        try:
            catalog = self._catalog_items(now)
        except Exception as exc:  # noqa: BLE001 - try again next run
            _log("roster_precheck_catalog_error", error=str(exc)[:200])
            return
        hit, moved = find_fixture(row, catalog)
        if hit is not None:
            info.update(match_id=hit["match_id"], kickoff=hit["kickoff"], how="moved" if moved else "catalog")
            return
        if info.get("how") in ("not_listed", "moved") and info.get("match_id"):
            return
        if self.dadmin_search is None or now - float(info.get("dadmin_at") or 0) < DADMIN_RETRY_SECONDS:
            info.setdefault("how", "unknown")
            return
        info["dadmin_at"] = now
        found: list[dict[str, Any]] = []
        try:
            for team in [t for t in (row.home, row.away) if t]:
                for days in sorted(range(-MOVED_SEARCH_DAYS, MOVED_SEARCH_DAYS + 1), key=abs):
                    found.extend(self.dadmin_search(row.kickoff + days * 86400, team))
                    hit, moved = find_fixture(row, found)
                    if hit is not None and not moved:
                        break
                if find_fixture(row, found)[0] is not None:
                    break
        except Exception as exc:  # noqa: BLE001 - recorded as unknown, retried later
            _log("roster_precheck_dadmin_error", row=row.key, error=str(exc)[:200])
            info["how"] = "unknown"
            return
        hit, moved = find_fixture(row, found)
        if hit is None:
            info["how"] = "missing"
        else:
            info.update(match_id=hit["match_id"], kickoff=hit["kickoff"], how="moved" if moved else "not_listed",
                        dadmin_competition=hit.get("competition"), dadmin_home=hit.get("home"),
                        dadmin_away=hit.get("away"))

    def run_once(self) -> str | None:
        now = self.clock()
        rows = [r for r in self._rows(now) if self._in_horizon(r, now)]
        sections: dict[str, list[str]] = {}
        add = lambda name, line: sections.setdefault(name, []).append(line)  # noqa: E731
        summary_due = self._summary_due(now)
        for row in rows:
            info = self.state["rows"].setdefault(row.key, {})
            self._resolve(row, info, now)
            kickoff = float(info.get("kickoff") or row.kickoff)
            line = row_line(row, info)
            how = info.get("how")
            if how in ("not_listed", "moved", "missing", "unknown") and not info.get("reported_how"):
                # Announced once: in the 20:00 summary, or 2 h before the
                # roster time for a match added after the summary. Roster
                # time, not the real one: the team plans by the roster.
                if (summary_due and now < row.kickoff <= now + SUMMARY_SPAN) or now >= row.kickoff - 2 * 3600:
                    if how == "moved":
                        line += f"（排班表写 {fmt_bj(row.kickoff)}）"
                    add(how, line)
                    info["reported_how"] = how
            if not info.get("match_id"):
                continue
            done = info.setdefault("stages", {})
            stage = due_stage(kickoff, now, done)
            if stage is None:
                continue
            if self._run_stage(stage, row, info, kickoff, line, add):
                done[stage] = now
        header = "【排班表预检】"
        if summary_due:
            self.state["summaries"].append(fmt_bj(now, "%Y-%m-%d"))
            upcoming = [r for r in rows if now < r.kickoff <= now + SUMMARY_SPAN]
            listed = sum(1 for r in upcoming if self.state["rows"][r.key].get("how") in ("catalog", "moved"))
            header += (f"{fmt_bj(now)} 汇总：接下来 24 小时排班表 {len(upcoming)} 场，"
                       f"8901 列表能看到 {listed} 场。直播源一般开球前约 5 小时才登记，"
                       f"开球前 2 小时、30 分钟、开球后 5 分钟会逐场再查。")
            sections.setdefault("_summary", [])
        text = compose(sections, header)
        if text:
            self.send(text)
        self._prune(now)
        self._save(now)
        return text

    def _in_horizon(self, row: RosterRow, now: float) -> bool:
        """Roster time or, for a rescheduled match, its real kick-off."""
        times = [row.kickoff]
        real = self.state["rows"].get(row.key, {}).get("kickoff")
        if real:
            times.append(float(real))
        return any(now - HORIZON_BEFORE <= t <= now + HORIZON_AFTER for t in times)

    def _summary_due(self, now: float) -> bool:
        today = fmt_bj(now, "%Y-%m-%d")
        return int(fmt_bj(now, "%H")) >= SUMMARY_HOUR and today not in self.state["summaries"]

    def _run_stage(self, stage: str, row: RosterRow, info: dict[str, Any], kickoff: float,
                   line: str, add: Callable[[str, str], None]) -> bool:
        """``False`` when the sources could not be looked up (retried next run)."""
        try:
            sources = list(self.collect_sources(info["match_id"]))
        except Exception as exc:  # noqa: BLE001 - retried next run, never alerted
            _log("roster_precheck_sources_error", match_id=info["match_id"], error=str(exc)[:200])
            return False
        kinds = "、".join(f"{s.get('kind')}" for s in sources)
        alerted = info.get("alerted_no_source")
        if stage in ("t2h", "t30"):
            if not sources:
                add(stage, line)
                info["alerted_no_source"] = True
            elif alerted:
                add("fixed", f"{line}：{kinds}")
                info["alerted_no_source"] = False
            return True
        # live: someone must be delivering picture
        if not sources:
            add("live_none", line)
            info["alerted_no_source"] = True
            return True
        results = []
        for source in sources:
            result = self.probe(source["url"])
            results.append((source, result))
            if result.get("ok"):
                break
        if any(r.get("ok") for _, r in results):
            if alerted:
                add("fixed", f"{line}：{kinds}")
                info["alerted_no_source"] = False
            return True
        detail = "；".join(f"{s.get('kind')} {str(r.get('error') or '没画面')[:40]}" for s, r in results)
        add("live_dead", f"{line}：{detail}")
        return True

    def _prune(self, now: float) -> None:
        keep = {}
        for key, info in self.state["rows"].items():
            try:
                kickoff = datetime.strptime(key.split("|", 1)[0], "%Y-%m-%d %H:%M").replace(tzinfo=BEIJING).timestamp()
            except ValueError:
                continue
            if now - kickoff < STATE_KEEP_SECONDS:
                keep[key] = info
        self.state["rows"] = keep
        self.state["summaries"] = self.state["summaries"][-7:]

    def _save(self, now: float) -> None:
        if self.extra_path is not None:
            extra = [
                {"match_id": info["match_id"], "kickoff_unix": info.get("kickoff"), "roster_key": key,
                 "competition": info.get("dadmin_competition"),
                 "team_A_name": info.get("dadmin_home"), "team_B_name": info.get("dadmin_away")}
                for key, info in self.state["rows"].items()
                if info.get("how") == "not_listed" and info.get("match_id")
                and now - HORIZON_BEFORE <= float(info.get("kickoff") or 0) <= now + HORIZON_AFTER
            ]
            self.extra_path.parent.mkdir(parents=True, exist_ok=True)
            self.extra_path.write_text(json.dumps(extra, ensure_ascii=False, indent=1), encoding="utf-8")
        if self.state_path is not None:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=1), encoding="utf-8")
            tmp.replace(self.state_path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--dry-run", action="store_true", help="print messages, send nothing, keep no state")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--now", help="dry-run only: pretend it is this Beijing time (YYYY-MM-DD HH:MM)")
    args = parser.parse_args(argv)
    _load_dotenv(ROOT / ".env")
    sys.path.insert(0, str(ROOT))
    import live_sources

    token = os.environ.get("GIF_ROSTER_SPREADSHEET_TOKEN") or ""
    app_id = os.environ.get("GIF_MONITOR_FEISHU_APP_ID") or ""
    app_secret = os.environ.get("GIF_MONITOR_FEISHU_APP_SECRET") or ""
    if not (token and app_id and app_secret):
        _log("roster_precheck_disabled", reason="缺 GIF_ROSTER_SPREADSHEET_TOKEN 或飞书应用凭证")
        return 2
    if args.now and not args.dry_run:
        parser.error("--now 只能和 --dry-run 一起用")
    if args.dry_run:
        send: Callable[[str], Any] = lambda text: print("----- 会发出的消息 -----\n" + text + "\n-----", flush=True)
    else:
        webhook = os.environ.get("GIF_ROSTER_PRECHECK_WEBHOOK") or ""
        if not webhook.startswith("https://"):
            _log("roster_precheck_disabled", reason="GIF_ROSTER_PRECHECK_WEBHOOK 没配或不是 https 地址")
            return 2
        send = lambda text: send_webhook(webhook, text)  # noqa: E731
    clock: Callable[[], float] = time.time
    if args.now:
        fixed = datetime.strptime(args.now, "%Y-%m-%d %H:%M").replace(tzinfo=BEIJING).timestamp()
        clock = lambda: fixed  # noqa: E731

    dadmin = Dadmin(os.environ.get("GIF_ROSTER_DQD_ENV_FILE") or "/opt/project1/.env")
    sources = live_sources.SourceCatalog.from_env()
    data_dir = ROOT / "data" / "roster_precheck"
    check = Precheck(
        read_roster=FeishuSheet(app_id, app_secret, token).rows,
        fetch_catalog=fetch_catalog,
        dadmin_search=dadmin.search if dadmin.available() else None,
        collect_sources=lambda match_id: sources.collect(match_id)[0],
        probe=lambda url: live_sources.probe_source(url, timeout_seconds=6.0),
        send=send,
        state_path=None if args.dry_run else data_dir / "state.json",
        extra_path=None if args.dry_run else data_dir / "extra_matches.json",
        clock=clock,
    )
    _log("roster_precheck_started", dry_run=args.dry_run, dadmin=dadmin.available())
    while True:
        try:
            text = check.run_once()
            if text and not args.dry_run:
                _log("roster_precheck_sent", chars=len(text), lines=text.count("\n· ") + text.count("\n·"))
        except Exception as exc:  # noqa: BLE001 - one bad run must not end the service
            _log("roster_precheck_error", error=str(exc)[:300])
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(LOOP_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
