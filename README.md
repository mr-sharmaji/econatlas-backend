# EconAtlas Backend

REST API + scheduled-job backend for **EconAtlas** — a Personal Economic Intelligence System for Indian retail investors.

Production: **https://api.velqon.xyz** · OpenAPI: `/docs` · Schema spec: `/openapi.json` (84 endpoints across stocks, mutual funds, indices, FX, commodities, news, IPOs, macro, chat).

## Architecture

```
Flutter app (econatlas-app)
        ↓ HTTPS
FastAPI (econatlas-backend)   ← this repo
        ├── Postgres        (asyncpg)
        ├── Redis           (arq queue + cache)
        ├── Firebase Admin  (push notifications)
        ├── Prometheus      (metrics + Grafana)
        └── Cloudflare Workers (Yahoo / Screener egress proxies)
```

- **API layer** — FastAPI routers under `app/api/routes/`, ~84 endpoints grouped by domain.
- **Service layer** — Business logic, scrapers, scoring, narratives (`app/services/`).
- **Job queue** — arq + Redis dispatches background work (`app/queue/`).
- **Scheduler** — APScheduler cron + interval triggers fanning into the arq queue (`app/scheduler/`). Live cadence (market / commodity / crypto / notifications) ticks every 30 s; daily pipelines (discover stock / MF / price history) run on IST cron.
- **Postgres** — 37 tables snapshot in [docs/db_schema.md](docs/db_schema.md).

## Tech Stack

| Layer        | Choice                                  |
| ------------ | --------------------------------------- |
| Language     | Python 3.10+                            |
| Framework    | FastAPI + Pydantic v2                   |
| Server       | Uvicorn                                 |
| Database     | PostgreSQL (asyncpg pool via `get_pool()`) |
| Queue        | arq + Redis                             |
| Scheduler    | APScheduler (cron + interval triggers)  |
| Notifications| Firebase Admin SDK                      |
| Metrics      | Prometheus + Grafana                    |
| Proxy egress | Cloudflare Workers (Yahoo, Screener)    |
| Container    | Docker Compose                          |
| Deploy       | GitHub Actions → self-hosted Windows runner |

## Project Structure

```
app/
├── api/
│   ├── router.py
│   └── routes/        # health, market, commodities, crypto, screener, brief,
│                      #   chat, news, ipos, macro, events, broker_charges,
│                      #   feedback, ops, …
├── core/
│   ├── config.py      # Settings (env vars, pydantic-settings)
│   ├── database.py    # asyncpg pool — every DB path goes here (god node)
│   ├── utils.py       # parse_ts, _run_with_retry, record_to_dict
│   ├── asset_catalog.py
│   └── metrics.py     # Prometheus instrumentation
├── queue/             # arq settings + task wrappers
├── scheduler/         # market_job, commodity_job, crypto_job,
│                      #   discover_stock_job, discover_stock_intraday_job,
│                      #   discover_stock_price_job,
│                      #   discover_mutual_fund_job, discover_mf_nav_job,
│                      #   notification_job, ipo_job, news_job, brief_job,
│                      #   gap_backfill_job, macro_job, market_score_job,
│                      #   stock_future_prospects_job, runner.py, …
├── services/          # market, commodity, news, ipo, mf, ai, brief,
│                      #   chat, notification, scrapers/, …
├── schemas/           # Pydantic request/response models
└── main.py
docs/
├── db_schema.md       # auto-generated 37-table snapshot (single source of truth)
├── DEPLOY-PUBLIC.md   # Cloudflare Tunnel setup
└── ERRORS.md          # Postmortems
sql/                   # migrations + init.sql
scripts/               # one-off backfills, schema regen, ops helpers
.github/workflows/deploy.yml   # CI/CD to Windows host
docker-compose.yml
```

## Quick Start

### Docker (recommended)

```bash
cp .env.example .env          # fill in DATABASE_URL, FIREBASE_CREDENTIALS_JSON, etc.
docker compose up -d
```

- API → http://localhost:8000
- Docs → http://localhost:8000/docs
- Postgres → localhost:5432 (`econatlas` / `econatlas` / `econatlas`)
- Prometheus → http://localhost:9090
- Grafana → http://localhost:3000

### Local (no Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Run Postgres + Redis locally, set DATABASE_URL and REDIS_URL in .env
uvicorn app.main:app --reload --port 8000
```

The app applies `sql/init.sql` on first run.

## API surface

Full spec at `/docs` (Swagger UI) and `/openapi.json`. Major route groups:

| Prefix             | What it serves                                                                              |
| ------------------ | ------------------------------------------------------------------------------------------- |
| `/health`          | Liveness                                                                                    |
| `/market`          | Latest + history + intraday + status + scores + story for indices / FX / bonds              |
| `/commodities`     | Latest + history + intraday for gold, silver, crude, NG, etc.                               |
| `/crypto`          | Latest + history for top crypto                                                             |
| `/assets`          | Canonical asset catalog with region/session metadata                                        |
| `/screener`        | Search + overview + stocks/mutual-funds list & detail (history, intraday, sparklines, peers, story, score-history) |
| `/brief`           | Post-market brief, top movers, most active, sectors                                         |
| `/chat`            | Streaming chat sessions, suggestions, autocomplete, feedback                                |
| `/ipos`            | Active / upcoming IPOs, alerts, device registration                                         |
| `/broker-charges`  | Brokerage / STT / GST calculator                                                            |
| `/macro`           | Indicators, flows, forecasts, calendar, regime, linkages, summary                           |
| `/news`            | Articles, filters, embeddings                                                               |
| `/events`          | Economic events timeline                                                                    |
| `/feedback`        | User feedback ingress                                                                       |
| `/ops`             | Admin: `/jobs`, `/jobs/trigger/{name}`, `/jobs/abort/{name}`, `/sql`, `/logs`, `/data-health`, `/dlq/`, `/tables`, `/health`, `/notification_ai_metrics` |

Use `/openapi.json` (e.g. `jq '.paths["/screener/stocks"]'`) to inspect a specific route.

## Scheduled jobs

All jobs are registered in [`app/scheduler/runner.py`](app/scheduler/runner.py) and dispatched through arq. Key cadence (Asia/Kolkata):

| Job                              | Schedule                       | Purpose                                                                  |
| -------------------------------- | ------------------------------ | ------------------------------------------------------------------------ |
| `market`                         | every 30 s                     | Live indices / FX / bonds, intraday writes                               |
| `commodity`                      | every 30 s                     | Live commodity quotes                                                    |
| `crypto`                         | every 30 s                     | Live crypto quotes (CoinGecko + Yahoo)                                   |
| `notification_check`             | every 30 s                     | Push notifications via Firebase                                          |
| `intraday_autofill`              | every 10 min                   | Full-universe stock intraday refresh (NSE bulk + Upstox + Yahoo)         |
| `discover_stock_intraday`        | 30 min (09:15-15:30 IST)       | Stale-snapshot fill during market hours                                  |
| `gap_backfill`, `intraday_gap_backfill` | hourly                  | Daily / minute-level history gap repair                                  |
| `news`, `news_embed`             | hourly                         | News scrape + embeddings                                                 |
| `macro`                          | hourly                         | Macro indicators (FRED, IMF, World Bank)                                 |
| `market_score`                   | every 20 min                   | Recompute composite market scores                                        |
| `brief`                          | every ~4 min                   | Post-market brief snapshot                                               |
| `discover_stock`                 | **17:00 IST**, Mon-Fri         | Daily fundamentals + price snapshot (uses today's NSE bhavcopy)          |
| `discover_stock_price`           | 17:30 IST, Mon-Fri             | Daily 7-day OHLCV history backfill                                       |
| `reconcile_stock_snapshots`      | 18:05 IST, Mon-Fri             | Heal any snapshot whose price drifted from price_history                 |
| `discover_mutual_funds`          | 22:00 IST                      | MF rescore / reclassify / rerank                                         |
| `discover_mf_nav`                | 22:00 IST                      | AMFI + mfapi NAV refresh + snapshot sync                                 |
| `ipo`, `ipo_notification`        | hourly                         | IPO calendar refresh + alerts                                            |
| `stock_future_prospects(_embed)` | 4-hourly                       | LLM forward-look extraction + embeddings                                 |
| `econ_calendar`, `imf_weo_scraper` | daily                        | Macro calendar refresh                                                   |

## Database

Schema snapshot (auto-generated, single source of truth): **[docs/db_schema.md](docs/db_schema.md)** — 37 tables, 504 columns.

Headline tables:
- `discover_stock_snapshots`, `discover_stock_intraday`, `discover_stock_price_history`, `discover_stock_score_history`
- `discover_mutual_fund_snapshots`, `discover_mf_nav_history`
- `market_prices`, `market_prices_intraday`, `market_scores`
- `news_articles`, `news_embeddings`, `economic_events`
- `chat_messages`, `chat_sessions`
- `ops_logs`, `ipo_alerts`, `devices`

When changing schema, regen the snapshot and commit it in the same change:

```bash
scripts/regen_db_schema.sh
```

## Configuration

Edit `.env` (or set env vars in compose). Common keys:

| Key                              | Purpose                                                                       |
| -------------------------------- | ----------------------------------------------------------------------------- |
| `DATABASE_URL`                   | Postgres DSN                                                                  |
| `REDIS_URL`                      | Redis URL for arq queue                                                       |
| `FIREBASE_CREDENTIALS_JSON`      | Path inside container to the service-account key                              |
| `INTRADAY_YAHOO_PROXY_URL`       | Cloudflare Worker forwarding to `query1.finance.yahoo.com` (bypasses datacenter-IP 429s) |
| `DISCOVER_STOCK_PRIMARY_URL`     | Override for screener.in base (defaults to direct, can point at a CF Worker)  |
| `LOG_LEVEL`                      | DEBUG / INFO / WARNING / ERROR                                                |
| `OPS_LOGS_ENABLED`, `OPS_LOG_BUFFER_SIZE`, `OPS_LOGS_TOKEN` | `/ops/logs` controls                                                          |
| `MARKET_INTERVAL_SECONDS`, `COMMODITY_INTERVAL_SECONDS`     | Override default 30 s live cadence (set 0 to use minutes)                     |
| `HF_TOKEN`                       | HuggingFace authenticated downloads (faster + higher limits)                  |
| `GRAFANA_SERVICE_TOKEN`          | Grafana provisioning                                                          |

## Ops & runbook

Production API is open (no auth) at https://api.velqon.xyz.

```bash
# Logs (default to errors + last hour)
curl -sS 'https://api.velqon.xyz/ops/logs?level=error&since=1h&limit=200'

# DB introspection
curl -sS -X POST https://api.velqon.xyz/ops/sql \
  -H 'Content-Type: application/json' \
  -d '{"query":"SELECT ... LIMIT 50"}'

# Job control
curl -sS https://api.velqon.xyz/ops/jobs                        # list
curl -sS https://api.velqon.xyz/ops/jobs/running                # currently running
curl -sS -X POST https://api.velqon.xyz/ops/jobs/trigger/<name> # manual trigger
curl -sS -X POST https://api.velqon.xyz/ops/jobs/abort/<name>   # abort

# Dead-letter queue
curl -sS https://api.velqon.xyz/ops/dlq/
curl -sS -X POST https://api.velqon.xyz/ops/dlq/<id>/retry
curl -sS -X POST https://api.velqon.xyz/ops/dlq/<id>/dismiss

# Data freshness summary
curl -sS https://api.velqon.xyz/ops/data-health
```

Grafana dashboards (proxied at `https://api.velqon.xyz/grafana/d/<uid>`):

- `econatlas-v2` — overall monitor
- `econatlas-api-perf` — API latency + error rate
- `econatlas-external` — external API health
- `econatlas-jobs` — job + pipeline status
- `econatlas-system` — host, DB, Redis

## Deployment

### CI/CD — GitHub Actions to a self-hosted Windows runner

`.github/workflows/deploy.yml` handles every push to `main`:

1. Checkout on the self-hosted runner.
2. `robocopy /MIR` source into the deploy directory (excludes `.git`, `.github`, `_work`, `.env`, `.DS_Store`, `firebase-key.json`).
3. `docker compose build app` → `docker compose up -d --force-recreate app`.
4. Smoke-test `http://127.0.0.1:8000/ops/health`.

`firebase-key.json` is excluded from mirroring so a real key placed once on the host is preserved across deploys. Restore from your secret manager if it goes missing.

### Cloudflare Tunnel (public hostname)

See [docs/DEPLOY-PUBLIC.md](docs/DEPLOY-PUBLIC.md) for the cloudflared + DNS setup currently exposing `api.velqon.xyz`.

### Cloudflare Worker proxies

Two Workers shield the backend from datacenter-IP rate limits:

- `yahoo-proxy` — forwards to `query1.finance.yahoo.com/v8/finance/chart/*` (set `INTRADAY_YAHOO_PROXY_URL`).
- `screener-proxy` — forwards to `www.screener.in/company/*` (set `DISCOVER_STOCK_PRIMARY_URL`).

Free tier (100 K req/day) covers steady-state load; paid tier ($5/mo) recommended once daily volume exceeds 50 K.

## Local Postgres (no Docker)

```sql
CREATE USER econatlas WITH PASSWORD 'econatlas';
CREATE DATABASE econatlas OWNER econatlas;
```

```env
DATABASE_URL=postgresql://econatlas:econatlas@localhost:5432/econatlas
```

## Roadmap

- Tighter regime/scenario notifications (macro shock → portfolio impact)
- Multi-asset portfolio overlay
- WebSocket fan-out for live ticks (replacing 30 s poll on the app side)
- Knowledge graph queries via the chat interface
