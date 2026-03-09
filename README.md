# EconAtlas Backend

REST API backend for **EconAtlas** — a Personal Economic Intelligence System. The backend is **production-ready**: it serves the Flutter app, runs scheduled jobs for market/commodity/macro/news data, and can be deployed locally or exposed via Cloudflare Tunnel.

## Architecture

```
Flutter App (econatlas-app)
        ↓
FastAPI Backend (econatlas-backend)   ← this repo
        ↓
PostgreSQL (Docker or local)
```

- **API**: REST endpoints for health, market prices, commodities, macro indicators, news, and events.
- **Scheduler**: Background jobs (market, commodity, macro, news) run on an interval; data is written to PostgreSQL with one row per day for prices and proper handling of market holidays. Market and commodity jobs default to **every 30 seconds** for live accuracy when markets are open; set `MARKET_INTERVAL_SECONDS=0` and `COMMODITY_INTERVAL_SECONDS=0` to use minute-based intervals instead.

## Tech Stack

| Layer     | Choice        |
| --------- | ------------- |
| Language  | Python 3.13   |
| Framework | FastAPI       |
| Server    | Uvicorn       |
| Database  | PostgreSQL    |
| Driver    | asyncpg       |
| Scheduler | APScheduler   |
| Config    | python-dotenv |
| Container | Docker        |

## Project Structure

```
app/
├── core/
│   ├── config.py         # Settings (env vars)
│   └── database.py       # asyncpg pool and schema init
├── api/
│   ├── router.py
│   └── routes/           # health, events, macro, market, commodities, news
├── schemas/
├── services/             # market, macro, news, event (PostgreSQL)
├── scheduler/            # market_job, commodity_job, macro_job, news_job, trading_calendar
└── main.py
docs/
└── DEPLOY-PUBLIC.md      # Making the backend public (Cloudflare Tunnel, etc.)
sql/
└── init.sql              # Table definitions
scripts/
├── backfill_last_session.py   # Backfill last trading session (daily + optional intraday)
└── backfill_last_5_years.py   # Historical backfill (years of data)
```

## Quick Start

**With Docker (recommended):**

```bash
docker compose up -d
```

- API: **http://localhost:8000**
- Docs: **http://localhost:8000/docs**
- Postgres: `localhost:5432` (user `econatlas`, password `econatlas`, db `econatlas`)

**Local run (no Docker):**

1. Run PostgreSQL and create DB/user (see below).
2. Copy `.env.example` to `.env` and set `DATABASE_URL`.
3. Create venv, install deps, run:

```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

On first run, the app applies `sql/init.sql` if present.

**Docs:** See `docs/DEPLOY-PUBLIC.md` for making the backend public (e.g. Cloudflare Tunnel).

## Scripts

**Backfill last trading session** — Refresh the most recent trading day’s daily data (market + commodities). Use after a scheduler gap or restart. Idempotent (upsert).

```bash
# From backend repo root, with venv activated and .env set:
python scripts/backfill_last_session.py
# Or: PYTHONPATH=. python scripts/backfill_last_session.py
```

When the market is open, the script also writes intraday points for the 1D chart.

**Verify with curl** (start the backend first, then run backfill if needed):

```bash
curl -s http://localhost:8000/health
curl -s "http://localhost:8000/market/latest" | head -c 400
```

If `market/latest` is empty, run `python scripts/backfill_last_session.py`, then curl again.

## API Endpoints

| Method | Path                | Description                                      |
| ------ | ------------------- | ------------------------------------------------ |
| GET    | /health             | Health check                                     |
| GET    | /market/status      | Whether markets are live (NSE/NYSE in session); cached 30s, `Cache-Control` set |
| GET    | /market/intraday    | Intraday points for 1D chart (last 24h; query `asset`, `instrument_type`) |
| GET    | /market/latest      | Latest price per asset (indices, FX, bonds)      |
| GET    | /market             | Market prices, optional filters, history for charts |
| POST   | /market             | Ingest market record (scheduler)                  |
| GET    | /commodities/intraday | Intraday points for 1D chart (last 24h; query `asset`) |
| GET    | /commodities/latest | Latest price per commodity (gold, silver, oil…)   |
| GET    | /commodities        | Commodity prices, optional filters, history      |
| POST   | /commodities        | Ingest commodity record (scheduler)               |
| GET    | /macro              | Macro indicators (inflation, rates, GDP, etc.)   |
| POST   | /macro              | Ingest macro indicator (scheduler)               |
| GET    | /news               | News articles (filter by entity, impact, source) |
| POST   | /news               | Ingest news record (scheduler)                    |
| GET    | /events             | Economic events timeline                         |
| POST   | /events             | Create economic event                            |
| GET    | /ops/logs           | Tail in-memory backend logs (limit/filter/after_id; optional token) |

Interactive docs: **http://localhost:8000/docs**.

## Database Tables

Defined in `sql/init.sql`:

- **market_prices** — Asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close (indices, FX, bonds, commodities). One row per (asset, instrument_type, date).
- **market_prices_intraday** — Intraday points for 1D live chart (asset, instrument_type, price, timestamp). Written when market is open; last 24h returned by API.
- **macro_indicators** — Indicator name, value, country, timestamp, unit, source.
- **news_articles** — Title, summary, body, timestamp, source, url, primary_entity, impact, confidence.
- **economic_events** — Event type, entity, impact, confidence, created_at.
- **devices** — For future push notifications.

## Data Model: One Row per Day (All Time Series)

**One row per calendar day** applies to all time-series data so the app gets one point per day in charts and lists.

### Exchange trading date (market & commodity)

To avoid **timezone bugs** (e.g. Monday’s US close stored as “Tuesday” when the server is already in Tuesday UTC), we assign each price the **exchange’s trading date**, not the server’s UTC date:

- Each asset is mapped to an exchange (NSE for India indices/bonds, NYSE for US indices, FX, US bonds, commodities). The scheduler uses `exchange-calendars` to get the **trading date** in that exchange’s timezone: if that local date is a session day, we use it; otherwise we use the previous session date. The stored timestamp is that date at 00:00 UTC.
- So Monday’s close in New York is always stored with date Monday (UTC midnight), even if the job runs on Tuesday UTC.

- **Market** — Indices, FX, bond yields: one row per `(asset, instrument_type, date)` in `market_prices` with date = **exchange trading date** (NSE or NYSE by asset). When the calendar says closed we still fetch; we **only write when the price changed** from the last stored value (handles calendar errors).
- **Commodities** — Gold, silver, oil, etc.: same, with date = **NYSE trading date** (US futures).
- **Macro** — One row per `(indicator_name, country, date)` in `macro_indicators`; macro job uses **exchange trading date by country** (US → NYSE date, India → NSE date) so US and India indicators are stored under the correct local trading day.

## Deploying on a Windows Server (Docker)

### Prerequisites

- Docker Desktop (or Docker Engine + Docker Compose)
- Git

### One-time setup

In **Administrator PowerShell**:

```powershell
git clone https://github.com/<your-user>/econatlas-backend.git C:\econatlas-backend
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\C:\econatlas-backend\scripts\setup-server.ps1 -RepoUrl https://github.com/<your-user>/econatlas-backend.git
```

The script ensures Docker/Compose and Git are available, creates `.env` from `.env.example` if missing, then runs `docker compose up -d`.

### Making the API public (internet)

To expose the backend so the mobile app can reach it from anywhere, use **Cloudflare Tunnel** with a custom domain. See **[docs/DEPLOY-PUBLIC.md](docs/DEPLOY-PUBLIC.md)** for step-by-step setup (cloudflared, DNS, Windows service). Port forwarding is also documented as an alternative.

### Managing containers

```powershell
cd C:\econatlas-backend
docker compose ps
docker compose logs -f app
docker compose down
docker compose up -d
```

### Optional: open firewall for LAN

```powershell
New-NetFirewallRule -DisplayName "EconAtlas API" `
  -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

## Local PostgreSQL (no Docker)

Create DB and user:

```sql
CREATE USER econatlas WITH PASSWORD 'econatlas';
CREATE DATABASE econatlas OWNER econatlas;
```

Set in `.env`:

```
DATABASE_URL=postgresql://econatlas:econatlas@localhost:5432/econatlas
```

Optional scheduler intervals and cache (in `.env`):

- `MARKET_INTERVAL_SECONDS=30` — market job every 30s (default; for live accuracy). Use `0` to fall back to `MARKET_INTERVAL_MINUTES`.
- `COMMODITY_INTERVAL_SECONDS=30` — commodity job every 30s (default). Use `0` to use minutes.
- `MACRO_INTERVAL_MINUTES=1`, `NEWS_INTERVAL_MINUTES=30` — macro and news intervals in minutes.
- `MARKET_STATUS_CACHE_SECONDS=30` — cache for `GET /market/status` (reduces calendar lookups; set to `0` to disable).
- `OPS_LOGS_ENABLED=true` — enable `GET /ops/logs` endpoint.
- `OPS_LOG_BUFFER_SIZE=5000` — max log entries stored in memory for `/ops/logs`.
- `OPS_LOGS_TOKEN=` — optional token; when set, clients must send header `x-ops-token`.

## Roadmap

- AI event extraction pipelines
- Economic knowledge graph
- Portfolio exposure analysis
- Push notifications (devices table ready)
