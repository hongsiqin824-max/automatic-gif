# Football GIF Pipeline Validation

This workspace contains a first-stage offline validator for the live football
highlight pipeline. It validates the part that can be proven from a recording:

```text
video/RTMP-equivalent input -> event timestamp -> retroactive clip -> GIF
```

The primary implementation is now event-feed driven. It supports `G` (goal),
`OG` (own goal), `YC` (yellow card), and `RC` (red card). The default GIF does
not require a broadcast scoreboard; optional precise localization uses its
clock and score. `live_goal_pipeline.py` remains as the earlier baseline.

## Run

```bash
python3 validate_pipeline.py \
  "downloads/SV Wehen Wiesbaden vs FC Bayern Munchen [zqJI-83XFhM].mp4" \
  --events goal@1046,goal@5039,goal@6566 \
  --before 12 --after 24 --fps 8 --width 480 \
  --output-dir output_gifs/run_480_8
```

`--events` is intentionally an explicit timestamp list in this stage. It lets
us measure clip completeness and encoding latency independently of detector
quality. The report is written to `validation_report.json`.

The default window is 12 seconds before and 24 seconds after the event. GIF
output keeps the source 16:9 ratio. In the supplied recording, a 36-second
clip measured approximately:

| Profile | Approx. size | Encode time |
| --- | ---: | ---: |
| 640x360, 10 fps | 38-40 MB | 11-13 s |
| 480x270, 8 fps | 19 MB | 4 s |
| 384x216, 6 fps | 9.5 MB | 2.3 s |

The final profile must be selected after the publishing API provides its GIF
size limit. The source recording is 1280x720/30 fps with AAC stereo audio and
is about 134 minutes long.

## Visual dashboard

The first internal control console runs on the fixed local port `8899`. Create
the local configuration once, fill in the live-source secret, and start it:

```bash
cp .env.example .env
# Edit .env and set GIF_SOURCE_SECRET.
python3 dashboard_server.py
```

Open `http://127.0.0.1:8899`. The default host is intentionally localhost so
the live-source credential and internal controls are not exposed to the local
network. `dashboard_server.py` automatically loads `.env` from the project
directory. Existing shell environment variables take precedence, so a
temporary override is also supported:

```bash
export GIF_SOURCE_SECRET="provided-live-source-secret"
python3 dashboard_server.py
```

`.env.example` documents all supported environment variables, while `.env` is
git-ignored and contains the local values. The live-source secret is read by
the Flask backend only and is never returned to the browser.
The default API identities and polling intervals are:

| Purpose | Default |
| --- | --- |
| Match event polling | 3 seconds |
| Live source polling | 10 seconds |
| Match detail polling | 10 seconds |
| Match detail/event user | `hongsiqin@dongqiudi.com` |
| Live-source query user | `xuxinan@dongqiudi.com` |

The console shows match status, teams, RTMP availability, worker/FFmpeg health,
supported events, GIF task status, output links, fixed GIF settings, and the
structured runtime log. Enter a real `match_id` for production API data. The
"运行演示链路" button uses the supplied MP4 and cumulative API snapshots, so
it remains usable without network access or a live match.

When a default GIF reaches `encoded`, the Dashboard also shows a manual
**发布** button. It validates and copies that GIF to permanent SHA-256 storage,
checks its public HTTPS URL, and only then submits a formal GIF article. The
default article fields are `archive_level=B`, `add_to_tab=1`, `type=article`,
and `style=gif`; no `user_id` is sent, so the authorized default operation
account is used. Publishing is an independent action and never delays or
retries GIF generation. See [PUBLISHING_DEPLOYMENT.md](PUBLISHING_DEPLOYMENT.md)
for HTTPS, persistent storage, and first OAuth authorization.

One console can manage several matches. Active matches appear as tabs, and
switching tabs only changes the visible match; it does not stop the other
Workers. The Apple Silicon defaults allow eight active matches while limiting
the expensive OCR path to two shared slots:

```bash
GIF_MAX_CONCURRENT_MATCHES=8
GIF_MAX_CONCURRENT_HEAVY_TASKS=5
GIF_MAX_CONCURRENT_VISION_TASKS=2
GIF_VISION_WORKERS=2
GIF_OCR_TIMEOUT_SECONDS=300
GIF_WORKER_FINISH_TIMEOUT_SECONDS=600
```

On a stronger server, set both `GIF_MAX_CONCURRENT_VISION_TASKS=4` and
`GIF_VISION_WORKERS=4` (and use `GIF_MAX_CONCURRENT_HEAVY_TASKS=5`) to run four
OCR tasks. The OCR timeout applies to one recognition subprocess; slow but
progressing FFmpeg preparation and earlier OCR passes no longer consume a
single fatal event budget.

The limits apply across all match Worker processes. A ninth match receives an
HTTP 409 response until one active match has completely stopped. GIF encoding
has queue priority over optional vision refinement, and the activity panel
shows current heavy-task occupancy and queue depth.

When the API reports that a match has ended, the worker stops ingesting only
after pending OCR work drains. It waits up to `GIF_WORKER_FINISH_TIMEOUT_SECONDS`;
anything still running at that point is recorded as “OCR incomplete” while the
default GIF task remains independent.

The default event anchor is the API first-observed stream time minus 30
seconds. With the normal 30-second pre-roll and 20-second post-roll, the
default GIF covers `[T-60, T-10]` and can be encoded immediately from history.
Optional refinement uses the unshifted API time `T`, scans `[T-120, T]`, and is
not submitted until the corresponding default GIF has succeeded.

For a real match, "启动实时处理" is enabled by behavior only after the source
query has returned a non-empty `resource`. A source `resource` or `updated_at`
change is logged and causes the worker to restart against the new source while
retaining SQLite event deduplication state.

## What is proven

- FFmpeg can decode the supplied recording and produce valid GIFs.
- A 36-second clip can be encoded well within the one-minute publication
  target on this machine.
- The manually verified score changes have strong accompanying audio peaks.
- On this broadcast, stable score changes can automatically confirm all three
  reviewed goals without passing their timestamps to the detector.
- The live replay path writes rolling MPEG-TS segments and extracts its GIF from
  those segments rather than seeking back into the original MP4.

## What is not proven yet

- The timing relationship between a real event API and its matching RTMP feed.
- Real-network RTMP reconnect behavior with the selected production provider.
- The real feed's red-card payload and behavior for corrections such as VAR.

## Event API-driven pipeline

`event_driven_pipeline.py` keeps the latest 900 seconds of video and polls a
match event source. A newly observed `G`, `OG`, `YC`, or `RC` event creates one
GIF job. The first API response seeds the already-known event set by default, so
starting the process during a match does not regenerate every earlier event.

Run a recording as a live stream with the local simulated event feed:

```bash
python3 event_driven_pipeline.py \
  "downloads/SV Wehen Wiesbaden vs FC Bayern Munchen [zqJI-83XFhM].mp4" \
  --simulate-live --start 1037 --duration 50 \
  --match-id demo-match-54154533 \
  --mock-events mock_events/first_goal.json \
  --before 12 --after 18 \
  --output-dir output_gifs/event_driven_goal_1x
```

The mock file schedules when an event feed would first expose an event. Its
known source timestamp belongs only to the test harness; the pipeline itself
reacts to the first observed event and does not receive a manually selected
clip timestamp.

For API-behavior testing, `--replay-events` replays complete cumulative API
snapshots at stream-relative times. This is closer to the production endpoint
than `--mock-events`: it can represent an unchanged repeated response, a
temporary request failure, and an event that only appears in a later response.

```bash
python3 event_driven_pipeline.py \
  "downloads/SV Wehen Wiesbaden vs FC Bayern Munchen [zqJI-83XFhM].mp4" \
  --simulate-live --start 1037 --duration 50 \
  --match-id demo-match-54154533 \
  --replay-events mock_events/api_snapshot_scenario.json \
  --before 12 --after 18 \
  --output-dir output_gifs/snapshot_replay
```

The replay JSON contains an ordered `steps` array. Each step has
`at_stream_sec` and exactly one of `payload` (a complete real API response) or
`error` (a simulated transient failure message). As with the HTTP source, the
first successful payload seeds existing history unless `--emit-existing-events`
is passed. Failures preserve the seen-event state, and repeated snapshots do
not create duplicate GIF jobs. The example scenario is
`mock_events/api_snapshot_scenario.json`.

Use the real API with an RTMP feed:

```bash
python3 event_driven_pipeline.py \
  "rtmp://example/live/stream" \
  --match-id 54154533 \
  --event-url "https://openapi.dongqiudi.com/internal/api/data/overview/match/{match_id}" \
  --event-user "user@example.com" \
  --match-start-play "2026-05-20 11:00:00" \
  --event-poll-seconds 5 \
  --output-dir output_gifs/match_54154533
```

`--event-to-video-offset` configures the measured relationship between the
event's first appearance and the corresponding action in the received video.
The Dashboard and direct Worker default is `-10` seconds; override it after a
same-match API/RTMP live test establishes a better measured offset.
`--match-start-play` accepts the match-detail API's Beijing time. It supplies a
coarse match-clock reference for the visual search window; the API observation
time remains the default GIF anchor. OCR and T-DEED run as separate optional
artifacts and never replace or delay that default GIF.
GIF parameters are fixed for the entire run. The 10 MB value is a reporting
reference only and never causes automatic resolution, FPS, or color reduction.

### Reliability files and behavior

Every run now creates these operational artifacts under `--output-dir`:

- `pipeline_state.sqlite3`: durable event and GIF-task state. Restarting with
  the same match ID and output directory does not regenerate encoded events.
  It also stores the per-match wall-clock/stream-time mapping.
- `pipeline_events.jsonl`: immediately flushed records for discovery, duplicate
  events, API failures, task transitions, ingest restarts, and completed GIFs.
- `ingest_ffmpeg_RUN_ID_gNNN.log`: one FFmpeg log per initial connection or
  reconnect, preserved across program restarts.
- `event_pipeline_report_RUN_ID.json`: immutable summary for one run;
  `event_pipeline_report.json` points to the latest run's contents.
- `buffer/segments_RUN_ID_gNNN.csv` and `buffer/segment_RUN_ID_gNNN_*.ts`:
  rolling video segments retained across reconnects and program restarts.
- `buffer/segment_manifest.json`: atomically written generation list and stream
  offsets used to restore those segments on Worker restart.

磁盘生命周期由终态清理器统一管理：

- 运行中每次创建新的 FFmpeg generation 前，会把本场历史 ingest 日志限制在
  `--lifecycle-keep-ingest-logs` 个以内。
- 确认比赛正常结束、FFmpeg 已停止且 GIF/视觉任务已收敛后，默认原子清空
  manifest 的 generation，再回收未被 lease 保护的 TS 和无引用 CSV；手动停止、
  进程异常退出或 lease 查询失败时会延后清理，保留现场供恢复。
- `--post-match-buffer-retention-seconds` 可保留赛后最近一段 TS；运行报告按
  `--lifecycle-keep-run-reports` 限制数量，过大的 `pipeline_events.jsonl` 按
  `--lifecycle-event-log-max-mb` 和 `--lifecycle-event-log-archives` 轮转。
- 清理结果写入 `event_pipeline_report.json` 的 `disk_lifecycle` 字段，包含删除
  文件数、释放字节数、保留/跳过数量和错误信息。SQLite 任务库不会被删除，只会
  在终态执行 WAL checkpoint。
- Dashboard 还会每 5 分钟扫描没有活跃 Session、没有有效 lease、Worker PID 已退出且
  超过 15 分钟没有运行心跳的比赛目录，清理异常退出遗留的 TS、CSV、manifest 和视觉候选文件；
  可用 `GIF_ORPHAN_CLEANUP_GRACE_SECONDS` 调整这段保护窗口。终态和长期未访问的
  未启动 Session 默认保留 24 小时，最终 GIF 默认保留 24 小时，分别可用
  `GIF_SESSION_RETENTION_SECONDS`、`GIF_FINAL_GIF_RETENTION_SECONDS` 和
  `GIF_DISK_CLEANUP_INTERVAL_SECONDS` 配置。

RTMP input is supervised and restarted after either a clean or non-zero FFmpeg
exit, with progressive backoff of 2, 3, then 5 seconds (capped at 5 seconds).
Producing a new non-empty
segment resets that backoff. By default retries do not
expire; the event API continues polling during the reconnect delay. Set a
finite retry budget only when needed with `--rtmp-max-reconnects`,
`--rtmp-reconnect-initial-seconds`, and `--rtmp-reconnect-max-seconds`.

When the dashboard starts a live Worker, it records an in-memory desired
running state. If that Worker exits unexpectedly while the dashboard remains
up, the dashboard restarts it with capped exponential backoff. A manual stop
clears the desired state first, so it is never undone by automatic recovery.
The event-feed cursor and GIF tasks are stored in SQLite. A recovered Worker
compares the current API snapshot with that cursor, preserving events that
arrived during the outage without regenerating already-known GIFs or treating
the pre-start match history as new.

GIF encoding uses a bounded worker pool (`--gif-workers`, default 2), so two
near-simultaneous events can encode independently without blocking event/API
polling. New event-driven outputs use readable names such as
`54154533_m090+03_goal_Player-Name_2-1_default_abcdef.gif`. The `default` or
`ai` variant and stable six-character event suffix prevent the two artifacts,
or otherwise similar events, from overwriting each other. Existing outputs are
not renamed.

Run all deterministic tests with:

```bash
python3 -m unittest -v
```

### Event-driven validation results

The supplied recording was replayed at 1x. A mock `G` notification appeared
eight seconds after the reviewed goal action. The pipeline generated a
30-second, 384x216, 6 FPS GIF containing the action and celebration:

| Measurement | Result |
| --- | ---: |
| API-event observation to GIF ready | 26.81 s |
| GIF encoding time | 1.73 s |
| GIF size | 7.62 MB |
| Event jobs emitted / encoded | 1 / 1 |

A separate accelerated run emitted `G`, `YC`, and `RC`. All three event jobs
were deduplicated and encoded independently with `goal_`, `yellow_card_`, and
`red_card_` output names. This proves event routing, not visual card accuracy;
the card clips used simulated event records on the goal recording.

## Live MP4 replay baseline

`live_goal_pipeline.py` runs the post-detection path as a stream. FFmpeg reads
the MP4 at wall-clock speed, writes two-second MPEG-TS rolling-buffer segments,
and sends a two-FPS scoreboard crop to the online detector. No event timestamps
are passed to the detector.

```bash
python3 live_goal_pipeline.py \
  "downloads/SV Wehen Wiesbaden vs FC Bayern Munchen [zqJI-83XFhM].mp4" \
  --simulate-live --start 1037 --duration 46 \
  --output-dir output_gifs/live_realtime_goal_1
```

The default event window is 12 seconds before score confirmation and 18 seconds
after it. Default GIF encoding uses a fixed 640-pixel, 12 FPS, 128-color profile
that is independent from the OCR GIF profile. The 10 MB setting is a reporting
reference only: the encoder does not reduce colors, FPS, or resolution, and a
larger GIF still succeeds. The fixed profile can be selected explicitly with
`--gif-width`, `--gif-fps`, and `--gif-colors`.

For comparison, a 30-second clip from the supplied match encoded at the source
size (`1280x720`, `30 FPS`, `256 GIF colors`) was 334.57 MB and took 92.63
seconds to encode on the validation machine. This is why 16:9, source
resolution, source FPS, and an approximately 10 MB GIF size cannot all be
treated as hard requirements at the same time.

The default scoreboard coordinates are specific to the supplied 1280x720
broadcast. Another broadcaster needs its own `--anchor-roi` and `--score-roi`
configuration. The detector looks for a persistent score-glyph change; it does
not need to know the current or next numeric score.

### Results on the supplied recording

The full-recording detector scan produced only the following three score-change
candidates. The per-event live path then generated one GIF for each candidate.
The machine-readable scan result is stored at
`tmp/verification/live_detector_full_scan.json`.

| Goal confirmation in source | GIF size | GIF format |
| ---: | ---: | --- |
| 1051.5 s | 7.63 MB | 30 s, 384x216, 6 FPS |
| 5037.0 s | 8.51 MB | 30 s, 384x216, 6 FPS |
| 6564.5 s | 7.58 MB | 30 s, 384x216, 6 FPS |

The 46-second 1x replay completed in 45.96 wall-clock seconds. Its GIF was ready
26.00 seconds after score confirmation: 18 seconds of requested post-roll, up
to seven seconds for the current keyframe-based segment to close, and about two
seconds of GIF encoding. Compared with the manually reviewed visible goal action,
the end-to-end result was ready in approximately 28-29 seconds.

## Optional scoreboard OCR

PaddleOCR runs in an isolated environment so its dependencies cannot change the
Dashboard or T-DEED runtime:

```bash
python3 -m venv tmp/ocr_venv
tmp/ocr_venv/bin/python -m pip install -r ocr_requirements.txt
```

Enable `OCR 第二链路` before starting a match Worker. Each new event then has
two active artifacts by default: the unchanged default GIF and an OCR-located
60-second GIF. The optional T-DEED-refined 20-second artifact remains available
behind `--tdeed-enabled`, but the Dashboard keeps that third chain paused until
it is explicitly enabled. The Worker polls shotmap independently every
five seconds and only treats newly added `outcome=goal` rows as goal events. Its
first valid JSON response is a durable SQLite baseline, so goals that existed
before the Worker started are not replayed after startup or restart. A new goal
uses the cumulative `second` directly: for example, `455` targets `07:35` while
the API display minute remains `8`. The default GIF uses the independent shotmap
offset (zero by default); OCR locates the exact `MM:SS` and uses `-30/+30`, then
T-DEED refines only that OCR window. If shotmap has no shot rows, overview goals
remain available as the compatibility fallback. Red and yellow cards continue
to use overview and the minute-boundary OCR rule. OCR reads only the clock area;
the score is not required. The rolling buffer retains 900 seconds. OCR uses a
two-stage scan: a coarse 10-second sample to find the neighborhood, followed by
a 1-second local scan around that candidate. A persistent local worker is shared
by active matches; queue wait is tracked separately from the OCR execution
deadline. With a known scoreboard profile and continuous TS coverage, the
clock-only path reads the leased TS files directly and extracts only the clock
ROI. It falls back to the legacy MP4 materialization path when the direct path is
unavailable, while preserving the same remaining timeout. Dashboard deployments
can override the search window with `GIF_VISION_SEARCH_BEFORE_SECONDS`; direct
Workers use `--vision-search-before`.

The shotmap cursor stores the last valid response and a fingerprint made from
player, team, minute, cumulative second, situation, and normalized coordinates.
Invalid JSON or HTTP failures update diagnostics but never initialize or replace
the durable baseline. Runtime heartbeat and Dashboard output expose shotmap poll
count, initialization, errors, event source, target clock, and each artifact's
stage and failure reason.

The OCR worker automatically searches the upper-left scoreboard area when no
layout is configured. A known broadcast layout can optionally provide exact
pixel ROIs with `GIF_SCOREBOARD_PROFILE` or the `scoreboard_profile_path`
session field; this skips discovery and remains the fastest path. For example:

```json
{
  "profile_id": "source-a-1080p",
  "reference_resolution": [1920, 1080],
  "clock_roi": [40, 30, 180, 82],
  "score_roi": [180, 30, 360, 82],
  "second_half_clock_mode": "continuous"
}
```

Unknown or mismatched layouts never publish a false precise result. The OCR GIF
keeps its separate 384px / 6 FPS / 160-color profile and is encoded directly
from the original TS; lowering the default GIF profile does not change it.
T-DEED analyzes only that OCR 60-second window and, when successful, publishes
the short `-8/+12` second `_ai_` GIF. OCR and T-DEED persist separate status,
window metadata, output path, failure stage, and failure reason. A failure in
either optional path does not change the default GIF. Extra time and penalty
shootouts remain outside V1.

## Current boundary

The default event-driven path does not depend on OCR or a visual classifier.
The supplied recording proves event routing, streaming buffer, retroactive
extraction, GIF generation, durable deduplication, and concurrent jobs. The
automatic clock finder has been exercised against four cached broadcast
layouts without profiles, including a `90:00 +00:xx` clock and a temporary goal
graphic. A live shadow run is still required to measure full 120-second OCR
accuracy, 60-second GIF file size, and multi-match production
latency before assigning a production accuracy percentage.

## Visual localization research

The official `T-DEED SoccerNet_small` checkpoint now runs locally on short
windows from the supplied recording. At a fixed `Goal >= 0.20` threshold it
found all three reviewed goals within 2.4 seconds and produced no Goal result
in three sampled negative windows. This is a six-window feasibility test, not
a production accuracy claim. See `VISION_MODEL_RESEARCH.md` and
`tmp/verification/tdeed_evaluation_report.json` for inputs, raw results,
latency, limitations, card-class status, and licensing notes.
