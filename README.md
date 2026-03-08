# EconAtlas Backend

REST API backend for **EconAtlas** — a Personal Economic Intelligence System.

## Architecture

```
Flutter App (econatlas-app)
        ↓
FastAPI Backend (econatlas-backend)   ← this repo
        ↓
PostgreSQL (Docker or local)
```

The backend exposes REST endpoints for the mobile app and runs scheduled jobs (market, commodity, macro, news) that write into PostgreSQL.

## Tech Stack

| Layer     | Choice        |
| --------- | ------------- |
| Language  | Python 3.13   |
| Framework | FastAPI       |
| Server    | Uvicorn       |
| Database  | PostgreSQL    |
| Driver    | asyncpg       |
| Config    | python-dotenv |
| Container | Docker        |

## Project Structure

```
app/
├── core/
│   ├── config.py      # settings (env vars)
│   └── database.py    # asyncpg pool and schema init
├── api/
│   ├── router.py
│   └── routes/        # health, events, macro, market, commodities, news
├── schemas/
├── services/          # market, macro, news, event (PostgreSQL)
├── scheduler/         # background jobs (market, commodity, macro, news)
└── main.py
sql/
└── init.sql           # table definitions
```

## Running with Docker Compose (recommended)

Starts PostgreSQL and the API in one go:

```bash
docker compose up -d
```

- API: `http://localhost:8000`
- Docs: `http://localhost:8000/docs`
- Postgres: `localhost:5432` (user `econatlas`, password `econatlas`, db `econatlas`)

Build and recreate:

```bash
docker compose build --no-cache
docker compose up -d
```

## Running locally (no Docker)

1. **PostgreSQL** must be running (e.g. local install or a Postgres container).

2. **Create DB and user** (if needed):

   ```sql
   CREATE USER econatlas WITH PASSWORD 'econatlas';
   CREATE DATABASE econatlas OWNER econatlas;
   ```

3. **Env and run:**

   ```bash
   cp .env.example .env
   # Edit .env: DATABASE_URL=postgresql://econatlas:econatlas@localhost:5432/econatlas
   python -m venv .venv
   source .venv/bin/activate   # or .venv\Scripts\activate on Windows
   pip install -r requirements.txt
   uvicorn app.main:app --reload --port 8000
   ```

   On first run, the app applies `sql/init.sql` if the file exists.

## API Endpoints

| Method | Path                | Description                                           |
| ------ | ------------------- | ----------------------------------------------------- |
| GET    | /health             | Health check                                          |
| POST   | /events             | Create a new economic event                           |
| GET    | /events             | List recent economic events                           |
| POST   | /market             | Ingest normalized market record                       |
| GET    | /market             | List market prices (filter by instrument_type, asset) |
| GET    | /market/latest      | Latest price per asset (de-duplicated)                |
| POST   | /commodities        | Ingest normalized commodity record                    |
| GET    | /commodities        | List commodity prices (filter by asset)               |
| GET    | /commodities/latest | Latest price per commodity asset                      |
| POST   | /news               | Ingest news record and emit event                     |
| GET    | /news               | List news articles (filter by entity, impact, source) |
| POST   | /macro              | Ingest macro-economic indicator                       |
| GET    | /macro              | List macro-economic indicators (filter by country)    |

## Database Tables

Defined in `sql/init.sql`:

- **economic_events** — `id`, `event_type`, `entity`, `impact`, `confidence`, `created_at`
- **macro_indicators** — `id`, `indicator_name`, `value`, `country`, `timestamp`, `unit`, `source`
- **market_prices** — `id`, `asset`, `price`, `timestamp`, `source`, `instrument_type`, `unit`, `change_percent`, `previous_close`
- **news_articles** — `id`, `title`, `summary`, `body`, `timestamp`, `source`, `url` (unique), `primary_entity`, `impact`, `confidence`
- **devices** — `id`, `user_id`, `device_token`, `platform` (for future use)

## Data model: one row per day (market & commodity)

For **market** (indices, FX, bond yields) and **commodity** prices we keep **at most one row per (asset, instrument_type, calendar day)** so charts get one point per day.

- The scheduler uses **today 00:00 UTC** as the timestamp and **upserts** (insert or update) so each run overwrites today’s row with the latest price. No duplicate rows for the same day.
- **When markets are closed**: we avoid writing stale data. The job uses **exchange calendars** (NSE/NYSE) as the main gate. If the calendar says **closed**, we still fetch; we **only write when the price changed** from the last stored value (so we don’t miss days when the market was actually open but the calendar was wrong). If the calendar says **open** we always write. So wrong calendar (holiday vs live or vice versa) is partly corrected by the data.
- **Macro** indicators use table `macro_indicators` with one row per `(indicator_name, country, timestamp)`; timestamps are **observation/release dates** from FRED/World Bank (e.g. monthly), so there is no “many rows per day” issue.

## Deploying on a Windows Server (Docker)

### Prerequisites

- **Docker Desktop** (or Docker Engine + Docker Compose)
- **Git**

### One-Time Setup

In **Administrator PowerShell**:

```powershell
git clone https://github.com/<your-user>/econatlas-backend.git C:\econatlas-backend
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\C:\econatlas-backend\scripts\setup-server.ps1 -RepoUrl https://github.com/<your-user>/econatlas-backend.git
```

The script checks Docker/Docker Compose and Git, creates `.env` from `.env.example` if missing, then runs `docker compose up -d`.

### CI/CD — GitHub Actions (self-hosted runner)

1. Add a **self-hosted runner** (Windows) to the repo.
2. On push to `main`, the workflow in `.github/workflows/deploy.yml`:
   - Syncs the repo into `C:\PersonalProjects\econatlas-backend`
   - Runs `docker compose build --no-cache` and `docker compose up -d`

### Managing containers

```powershell
cd C:\econatlas-backend
docker compose ps
docker compose logs -f app
docker compose down
docker compose up -d
```

### Firewall (optional)

To expose the API on the LAN:

```powershell
New-NetFirewallRule -DisplayName "EconAtlas API" `
  -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

## Future Roadmap

- AI event extraction pipelines
- Economic knowledge graph
- Portfolio exposure analysis
- Push notifications
