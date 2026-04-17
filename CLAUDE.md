# econatlas-backend

FastAPI Python backend. Production: https://api.velqon.xyz (OpenAPI docs at `/docs`).

## Layout
- `app/api/` — FastAPI routes
- `app/services/` — business logic (scrapers, notifications, LLM suggestions)
- `app/queue/` — background job dispatch
- `app/scheduler/` — cron-like job runners
- `app/core/` — shared: `database.py` (DB pool), `settings.py`, `utils.py`
- `app/schemas/` — Pydantic models
- `sql/` — migrations / queries
- `scripts/` — one-off scripts

## Central primitives (god nodes — read these first)
- `app/core/database.py::get_pool()` — **every DB code path** goes through this. 169 callers.
- `app/services/scrapers/base.py::BaseScraper` — all scrapers inherit this.
- `app/core/utils.py::record_to_dict`, `_run_with_retry`, `parse_ts`
- `app/core/settings.py::get_settings()`

## Working efficiently (reduce tokens)
Before opening files, query the pre-built knowledge graph:

```bash
graphify query "<question>"            # BFS over graph.json, ~2k tokens
graphify explain "<node>"              # explain a function/class and its neighbors
graphify path "<A>" "<B>"              # shortest connection between two symbols
```

Graph lives at `graphify-out/graph.json` (not committed — regenerate with `graphify update .`).

Prefer `graphify query` over wide `grep` / reading many files. Fall back to Read only for the specific files the graph points to.

## Stack
Python 3.10+, FastAPI, asyncpg (Postgres), Redis, Firebase, Docker, Prometheus + Grafana.

## Conventions
- Async everywhere; DB access via `get_pool()`.
- Logging: structured; see `logs/` (gitignored).
- Secrets: `.env` + `firebase-key.json` (both gitignored).
- Never edit `graphify-out/` — it's a generated artifact.

## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)
