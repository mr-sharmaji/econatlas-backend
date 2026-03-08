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
- **Scheduler**: Background jobs (market, commodity, macro, news) run on an interval; data is written to PostgreSQL with one row per day for prices and proper handling of market holidays.

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
sql/
└── init.sql              # Table definitions
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

## API Endpoints

| Method | Path                | Description                                      |
| ------ | ------------------- | ------------------------------------------------ |
| GET    | /health             | Health check                                     |
| GET    | /market/latest      | Latest price per asset (indices, FX, bonds)      |
| GET    | /market             | Market prices, optional filters, history for charts |
| POST   | /market             | Ingest market record (scheduler)                  |
| GET    | /commodities/latest | Latest price per commodity (gold, silver, oil…)   |
| GET    | /commodities        | Commodity prices, optional filters, history      |
| POST   | /commodities        | Ingest commodity record (scheduler)               |
| GET    | /macro              | Macro indicators (inflation, rates, GDP, etc.)   |
| POST   | /macro              | Ingest macro indicator (scheduler)               |
| GET    | /news               | News articles (filter by entity, impact, source) |
| POST   | /news               | Ingest news record (scheduler)                    |
| GET    | /events             | Economic events timeline                         |
| POST   | /events             | Create economic event                            |

Interactive docs: **http://localhost:8000/docs**.

## Database Tables

Defined in `sql/init.sql`:

- **market_prices** — Asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close (indices, FX, bonds, commodities).
- **macro_indicators** — Indicator name, value, country, timestamp, unit, source.
- **news_articles** — Title, summary, body, timestamp, source, url, primary_entity, impact, confidence.
- **economic_events** — Event type, entity, impact, confidence, created_at.
- **devices** — For future push notifications.

## Data Model: One Row per Day (All Time Series)

**One row per calendar day** applies to all time-series data so the app gets one point per day in charts and lists:

- **Market** — Indices, currencies (FX), and bond yields: at most one row per `(asset, instrument_type, date)` in `market_prices`. Scheduler uses **today 00:00 UTC** and **upserts** so each run updates that day’s row. When the calendar says markets are closed (NSE/NYSE via `exchange-calendars`), we still fetch; we **only write when the price changed** from the last stored value (handles calendar errors).
- **Commodities** — Gold, silver, oil, etc.: same as market (one row per `(asset, instrument_type, date)` in `market_prices`), with the same calendar + price-change logic.
- **Macro** — Inflation, rates, GDP, etc.: at most one row per `(indicator_name, country, date)` in `macro_indicators`. The macro job normalizes timestamps to **today 00:00 UTC** and **upserts** so each run updates that day’s row.

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

To expose the backend so the mobile app can reach it from anywhere, use **Cloudflare Tunnel** with a custom domain. See **[DEPLOY-PUBLIC.md](DEPLOY-PUBLIC.md)** for step-by-step setup (cloudflared, DNS, Windows service). Port forwarding is also documented as an alternative.

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

## Roadmap

- AI event extraction pipelines
- Economic knowledge graph
- Portfolio exposure analysis
- Push notifications (devices table ready)
