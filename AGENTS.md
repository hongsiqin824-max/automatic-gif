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

## Fixed Server Update Workflow

This is the authoritative procedure for every update of the production
Dashboard. Run these commands in the server SSH terminal, in order, from the
fixed release directory (currently `/opt/automatic-gif-release-c24aa3c`):

1. Pull only a fast-forward update. A successful pull ends with a summary such
   as `Updating <old>..<new>`; never use `reset --hard` or force a merge.

   ```bash
   cd /opt/automatic-gif-release-c24aa3c
   GIT_TERMINAL_PROMPT=0 git -c http.version=HTTP/1.1 pull --ff-only origin main
   ```

2. Verify that the existing OCR virtual-environment link is still executable.
   The expected output is `OCR environment OK`.

   ```bash
   test -x tmp/ocr_venv/bin/python \
     && echo "OCR environment OK" \
     || echo "OCR environment missing"
   ```

3. Restart only the 8899 Dashboard. First list the process, then replace
   `<current-dashboard-pid>` with the PID printed by `ps`.

   ```bash
   ps -eo pid,ppid,args | grep -E '[d]ashboard_server.py'
   kill -TERM <current-dashboard-pid>
   sleep 3
   cd /opt/automatic-gif-release-c24aa3c
   nohup env PYTHONUNBUFFERED=1 \
     /opt/automatic-gif/.venv/bin/python dashboard_server.py \
     >> dashboard.log 2>&1 &
   echo $!
   ```

4. Verify health and then watch the log for OCR-worker startup, event handling,
   and GIF generation. Health must return `{"ok":true,"port":8899}`.

   ```bash
   sleep 3
   curl -fsS http://127.0.0.1:8899/api/health
   tail -f dashboard.log
   ```

Do not run `git clean -fd`: it can remove the OCR link, SQLite state, caches,
or server-only files. Do not recreate/delete `tmp/ocr_venv` for a normal code
update. Rebuild it only when creating a new release directory, the link was
manually deleted, Python/Paddle/PaddleOCR is intentionally changed, or a
cleanup command removed ignored files. If the server keeps a deliberate
oneDNN-only edit, preserve that edit with the repository's backup/stash
procedure before pulling and verify it again after the pull. Keep the previous
release and environment until health checks pass so rollback remains possible.
