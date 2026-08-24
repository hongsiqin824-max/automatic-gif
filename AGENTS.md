# Repository Guidelines

## Project Structure

This is a Python football live-event to GIF pipeline. `dashboard_server.py`
serves the Dashboard on port 8899 and starts match workers from
`event_driven_pipeline.py`. Runtime coordination is in `pipeline_runtime.py`,
`live_runtime.py`, `live_goal_pipeline.py`, and `heavy_task_coordinator.py`.
Clock-only OCR lives in `scoreboard_ocr.py` and `scoreboard_ocr_worker.py`;
optional vision refinement lives in `vision_runtime.py` and
`vision_locator.py`. Tests use the `test_*.py` naming pattern.

## Development Commands

```bash
python3 -m unittest -v
python3 -m py_compile dashboard_server.py event_driven_pipeline.py \
  scoreboard_ocr.py scoreboard_ocr_worker.py
python3 dashboard_server.py
```

The first command runs all tests, the second catches syntax errors, and the
third starts the local Dashboard at `http://127.0.0.1:8899`. Keep OCR packages
in `tmp/ocr_venv`; do not commit `.env`, caches, downloaded media, or GIFs.

## Style and Testing

Use four-space indentation, `snake_case` Python names, focused functions, and
the existing structured JSON/JSONL log fields. Frontend JavaScript uses
camelCase while API field names remain unchanged. Add deterministic regression
tests beside the affected module; cover both success and failure paths for OCR,
event identity, and task state. Run the full unittest command before release.

## Commits and Reviews

Use short imperative commit subjects, for example `Refine scoreboard OCR
worker`. Keep unrelated changes separate. A review/PR should describe behavior
changes, tests, deployment checks, configuration changes, and relevant
Dashboard or GIF artifacts.

## Standard Server Update

Use the active server directory (currently
`/opt/automatic-gif-release-c24aa3c`) and follow this order:

```bash
cd /opt/automatic-gif-release-c24aa3c
GIT_TERMINAL_PROMPT=0 git -c http.version=HTTP/1.1 pull --ff-only origin main
test -x tmp/ocr_venv/bin/python \
  && echo "OCR environment OK" \
  || echo "OCR environment missing"
ps -eo pid,ppid,args | grep -E '[d]ashboard_server.py'
kill -TERM <current-dashboard-pid>
sleep 3
cd /opt/automatic-gif-release-c24aa3c
nohup env PYTHONUNBUFFERED=1 \
  /opt/automatic-gif/.venv/bin/python dashboard_server.py \
  >> dashboard.log 2>&1 &
echo $!
sleep 3
curl -fsS http://127.0.0.1:8899/api/health
tail -f dashboard.log
```

The health response must be `{"ok":true,"port":8899}`. Confirm the process
working directory and inspect the log for OCR worker startup, event handling,
and GIF generation. Do not run `git clean -fd`; it may delete OCR links,
SQLite state, caches, or server files. Do not recreate or delete
`tmp/ocr_venv` for a normal code update. Rebuild it only after creating a new
release directory, losing the symlink, changing Python/Paddle versions, or
intentionally upgrading Paddle/PaddleOCR. Keep the previous release and OCR
environment until health and worker checks pass so rollback remains possible.
