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

## Debug & Ops Runbook
Production API: `https://api.velqon.xyz` (open, no auth currently).

**DB (read `docs/db_schema.md` first — 37 tables, 504 columns snapshot):**
```bash
curl -sS -X POST https://api.velqon.xyz/ops/sql \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT ... LIMIT 50"}'
```
Always add `LIMIT`. Filter by `created_at > now() - interval '1 hour'` for recent state.

**Logs — default filter to errors + a window + limit:**
```bash
curl -sS 'https://api.velqon.xyz/ops/logs?level=error&since=1h&limit=200'
```
Only widen (INFO, 24h) if error logs show nothing relevant. Never pull unbounded.

**Jobs:** `GET /ops/jobs`, `GET /ops/jobs/running`, `POST /ops/jobs/trigger/{name}`, `POST /ops/jobs/abort/{name}`.

**Grafana dashboards** (`https://api.velqon.xyz/grafana/d/<uid>`):
- `econatlas-v2` — overall monitor
- `econatlas-api-perf` — API latency + error rate
- `econatlas-external` — external API health
- `econatlas-jobs` — job + pipeline status
- `econatlas-system` — host, DB, Redis

**Debug order (follow this to avoid wandering):**
1. `graphify query "<what is broken>"` — locate relevant subgraph, don't Read/Grep yet.
2. `/ops/logs?level=error&since=1h` — is it already logged?
3. `/ops/sql` against the relevant table (check `docs/db_schema.md` first).
4. Open only the specific files graphify pointed to. Confirm the hypothesis before editing.
5. Check Grafana only when metrics (latency, throughput) are the question.

## DB schema changes — IMPORTANT
Whenever you edit anything that changes the DB schema (files in `sql/`, any `CREATE TABLE` / `ALTER TABLE` / `DROP TABLE` / `ADD COLUMN` / migration), you **must** also:

1. After the migration is applied to the DB, run `scripts/regen_db_schema.sh` to refresh `docs/db_schema.md`.
2. Commit the updated `docs/db_schema.md` in the **same commit** as the schema change.
3. If the migration hasn't been applied yet, note in the commit message that `db_schema.md` will be regenerated post-deploy.

This keeps the schema snapshot that every session loads in sync with reality. Stale `db_schema.md` silently poisons queries.

## Planning gate (cuts rework)
Before any edit, state the approach as 3–5 bullets and wait for "go" before editing. Applies to every change, no matter how small. Saves the "wrong logic, redo" loop.

## Comment discipline
Keep comments minimal. Default to none. Only add one when the *why* is non-obvious — a hidden constraint, subtle invariant, or workaround for a known bug. Never comment what the code does (good names suffice). Never reference the current task, fix, or PR.

## graphify

This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `graphify update .` to keep the graph current (AST-only, no API cost)
