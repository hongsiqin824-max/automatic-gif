# Football GIF Pipeline Validation

This workspace contains a first-stage offline validator for the live football
highlight pipeline. It validates the part that can be proven from a recording:

```text
video/RTMP-equivalent input -> event timestamp -> retroactive clip -> GIF
```

The primary implementation is now event-feed driven. It supports `G` (goal),
`YC` (yellow card), and `RC` (red card) without requiring a broadcast
scoreboard. `live_goal_pipeline.py` remains as the earlier scoreboard baseline.

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

`event_driven_pipeline.py` keeps the latest 120 seconds of video and polls a
match event source. A newly observed `G`, `YC`, or `RC` event creates one GIF
job. The first API response seeds the already-known event set by default, so
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
  --event-poll-seconds 5 \
  --output-dir output_gifs/match_54154533
```

`--event-to-video-offset` configures the measured relationship between the
event's first appearance and the corresponding action in the received video.
It remains `0` until a same-match API/RTMP live test establishes that offset.
GIF parameters are fixed for the entire run. The 10 MB value is a reporting
reference only and never causes automatic resolution, FPS, or color reduction.

### Reliability files and behavior

Every run now creates these operational artifacts under `--output-dir`:

- `pipeline_state.sqlite3`: durable event and GIF-task state. Restarting with
  the same match ID and output directory does not regenerate encoded events.
- `pipeline_events.jsonl`: immediately flushed records for discovery, duplicate
  events, API failures, task transitions, ingest restarts, and completed GIFs.
- `ingest_ffmpeg_RUN_ID_gNNN.log`: one FFmpeg log per initial connection or
  reconnect, preserved across program restarts.
- `event_pipeline_report_RUN_ID.json`: immutable summary for one run;
  `event_pipeline_report.json` points to the latest run's contents.
- `buffer/segments_RUN_ID_gNNN.csv` and `buffer/segment_RUN_ID_gNNN_*.ts`:
  rolling video segments retained across reconnects and program restarts.

RTMP input is supervised and restarted after either a clean or non-zero FFmpeg
exit, with exponential backoff capped at 30 seconds. By default retries do not
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
polling. Output filenames include a stable event-key suffix to prevent two
same-type events at the same observation time from overwriting each other.

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
after it. GIF encoding uses one fixed profile, currently 384 pixels wide, 6 FPS,
and 160 palette colors. The 10 MB setting is a reporting reference only: the
encoder does not reduce colors, FPS, or resolution, and a larger GIF still
succeeds. The fixed profile can be selected explicitly with `--gif-width`,
`--gif-fps`, and `--gif-colors`.

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

## Current boundary

The event-driven implementation no longer depends on a scoreboard or a visual
goal classifier. The supplied recording proves event routing, streaming buffer,
retroactive extraction, post-roll wait, fixed-profile GIF generation, durable
deduplication, concurrent jobs, and the one-minute latency budget for the tested
profile. The reconnect policy is deterministically tested, but a same-match live
RTMP feed and event API are still required to measure their clock offset, real
network behavior, event corrections, and production event-code payloads.

## Visual localization research

The official `T-DEED SoccerNet_small` checkpoint now runs locally on short
windows from the supplied recording. At a fixed `Goal >= 0.20` threshold it
found all three reviewed goals within 2.4 seconds and produced no Goal result
in three sampled negative windows. This is a six-window feasibility test, not
a production accuracy claim. See `VISION_MODEL_RESEARCH.md` and
`tmp/verification/tdeed_evaluation_report.json` for inputs, raw results,
latency, limitations, card-class status, and licensing notes.
