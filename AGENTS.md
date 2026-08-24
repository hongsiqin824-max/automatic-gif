# Repository Guidelines

## Project Structure & Module Organization

This is a Python football live-event to GIF pipeline. The Dashboard entry point
is `dashboard_server.py`; it serves the control UI in `dashboard_static/` and
starts per-match Workers from `event_driven_pipeline.py`. Runtime coordination
and artifact generation live in `pipeline_runtime.py`, `live_runtime.py`,
`live_goal_pipeline.py`, and `heavy_task_coordinator.py`. Clock-only OCR is
split between `scoreboard_ocr.py` and `scoreboard_ocr_worker.py`; optional vision
refinement is in `vision_runtime.py` and `vision_locator.py`. Deterministic
fixtures are in `mock_events/` and `scoreboard_profiles/`; tests and generated
outputs are kept in `test_*.py` and `output_gifs/` respectively.

## Build, Test, and Development Commands

There is no separate build step. From the repository root, run the complete
suite with:

```bash
python3 -m unittest -v
```

Compile the production modules before deployment:

```bash
python3 -m py_compile dashboard_server.py event_driven_pipeline.py \
  scoreboard_ocr.py scoreboard_ocr_worker.py
```

Start the local Dashboard on port 8899 with `python3 dashboard_server.py` and
open `http://127.0.0.1:8899`. Keep OCR dependencies isolated in
`tmp/ocr_venv`; install them with `tmp/ocr_venv/bin/python -m pip install -r
ocr_requirements.txt`. Do not commit `.env`, caches, downloaded media, or GIFs.

## Coding Style & Naming Conventions

Use four-space indentation, clear snake_case names, and type-friendly standard
Python. Keep public behavior documented in nearby docstrings or comments. Use
small, focused functions and preserve the existing structured JSON/JSONL log
fields. Frontend JavaScript in `dashboard_static/` follows camelCase for local
variables and existing API field names must remain stable.

## Testing Guidelines

Add regression coverage in the matching `test_*.py` module. Name tests
`test_<behavior>` and use deterministic fixtures rather than live network or
RTMP calls. Run the full unittest command before every deployment; a change to
OCR, event identity, or task state should include both success and failure-path
tests.

## Commit & Pull Request Guidelines

Use short imperative commit subjects such as `Refine scoreboard OCR worker` or
`Disable unstable oneDNN OCR inference`. Keep unrelated changes separate. Pull
requests should explain the behavior change, list tests and deployment checks,
call out configuration changes, and include Dashboard screenshots or sample
artifact names when UI/output behavior changes.

## Configuration & Deployment Notes

Copy `.env.example` to `.env` and keep secrets server-side. Preserve SQLite
state, `output_gifs`, video segments, and virtual environments across releases.
Deploy by starting the new release, checking `/api/health` and its process
working directory, then switching traffic; retain the previous release for
rollback if health checks or Worker startup fail.
