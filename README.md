# EconAtlas Backend

REST API backend for **EconAtlas** — a Personal Economic Intelligence System.

## Architecture

```
Flutter App (econatlas-app)
        ↓
FastAPI Backend (econatlas-backend)   ← this repo
        ↓
Supabase (Postgres · Auth · Storage · Edge Functions)

Scraper Workers (econatlas-scraper)
        ↓
FastAPI Backend API
        ↓
Supabase Database
```

The backend sits between the Flutter frontend and Supabase.
It exposes REST endpoints for the mobile app and accepts data pushed by the
scraper service. It does **not** scrape data itself.

## Tech Stack

| Layer          | Choice               |
|----------------|----------------------|
| Language       | Python 3.13          |
| Framework      | FastAPI              |
| Server         | Uvicorn              |
| Database       | Supabase Postgres    |
| Supabase SDK   | supabase-py          |
| HTTP Client    | httpx                |
| Validation     | Pydantic             |
| Config         | python-dotenv        |
| Container      | Docker               |

## Project Structure

```
app/
├── core/
│   ├── config.py            # centralised settings (env vars)
│   └── supabase_client.py   # singleton Supabase client
├── api/
│   ├── router.py            # top-level API router
│   └── routes/
│       ├── health.py        # GET /health
│       ├── events.py        # POST & GET /events
│       ├── macro.py         # POST & GET /macro
│       ├── market.py        # POST & GET /market, GET /market/latest
│       ├── commodities.py   # POST & GET /commodities, GET /commodities/latest
│       └── news.py          # POST & GET /news
├── schemas/
│   ├── event_schema.py      # Pydantic models for events
│   ├── macro_schema.py      # Pydantic models for macro indicators
│   ├── market_schema.py     # Pydantic models for market price responses
│   ├── news_schema.py       # Pydantic models for news article responses
│   └── ingest_schema.py     # Shared ingestion payload/ack models
├── services/
│   ├── event_service.py     # business logic — events
│   ├── macro_service.py     # business logic — macro indicators
│   ├── market_service.py    # persistence for market/commodity prices
│   └── news_service.py      # persistence for news article records
└── main.py                  # FastAPI app factory
```

## Running Locally

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment

```bash
cp .env.example .env
# edit .env with your Supabase credentials
```

### 4. Start the server

```bash
uvicorn app.main:app --reload --port 8000
```

The API docs are available at `http://localhost:8000/docs`.

## Docker

### Build

```bash
docker build -t econatlas-backend .
```

### Run

```bash
docker run -p 8000:8000 --env-file .env econatlas-backend
```

## API Endpoints

| Method | Path                | Description                                         |
|--------|---------------------|-----------------------------------------------------|
| GET    | /health             | Health check                                        |
| POST   | /events             | Create a new economic event                         |
| GET    | /events             | List recent economic events                         |
| POST   | /market             | Ingest normalized market record                     |
| GET    | /market             | List market prices (filter by instrument_type, asset) |
| GET    | /market/latest      | Latest price per asset (de-duplicated)              |
| POST   | /commodities        | Ingest normalized commodity record                  |
| GET    | /commodities        | List commodity prices (filter by asset)             |
| GET    | /commodities/latest | Latest price per commodity asset                    |
| POST   | /news               | Ingest news record and emit event                   |
| GET    | /news               | List news articles (filter by entity, impact, source) |
| POST   | /macro              | Ingest macro-economic indicator                     |
| GET    | /macro              | List macro-economic indicators (filter by country)  |

## Supabase Tables

The backend expects the following tables to exist in Supabase:

- **economic_events** — `id`, `event_type`, `entity`, `impact`, `confidence`, `created_at`
- **macro_indicators** — `id`, `indicator_name`, `value`, `country`, `timestamp`, `unit`, `source`
- **market_prices** — `id`, `asset`, `price`, `timestamp`, `source`, `instrument_type`, `unit`
- **news_articles** — `id`, `title`, `summary`, `body`, `timestamp`, `source`, `url`, `primary_entity`, `impact`, `confidence`
- **devices** — `id`, `user_id`, `device_token`, `platform`

## Ingestion Notes

- The scraper can post directly to `/market`, `/commodities`, `/news`, and `/macro`.
- Market and commodity routes persist both a price row (`market_prices`) and an event row (`economic_events`).
- News ingestion tries to store the article in `news_articles` and always emits an event in `economic_events`.
- The legacy `/events` endpoint remains available for generic event ingestion and compatibility.

## Deploying on a Windows Server

This section covers running the backend as a persistent Windows service with
automatic deployments on every push to `main`.

### Prerequisites

| Requirement | Install |
|-------------|---------|
| Python 3.13+ | [python.org](https://www.python.org/downloads/) — check "Add to PATH" |
| Git | [git-scm.com](https://git-scm.com/download/win) |
| NSSM | `winget install nssm.nssm` or [nssm.cc](https://nssm.cc/download) |

### One-Time Setup

Open an **Administrator PowerShell** and run:

```powershell
# Clone the repo
git clone https://github.com/<your-user>/econatlas-backend.git C:\econatlas-backend

# Run the setup script
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\C:\econatlas-backend\scripts\setup-server.ps1 -RepoUrl https://github.com/<your-user>/econatlas-backend.git
```

The script will:
1. Create a Python venv and install dependencies
2. Prompt you for Supabase credentials (`.env`)
3. Register `econatlas-backend` as a Windows service via NSSM
4. Start the service on port **8000**

After setup the API is live at `http://localhost:8000/docs`.

### CI/CD — GitHub Actions Self-Hosted Runner

So that every push to `main` automatically deploys to your Windows machine:

**1. Add a self-hosted runner to your repo**

Go to your GitHub repo → **Settings → Actions → Runners → New self-hosted runner**.
Select **Windows / x64** and follow the download + configure instructions:

```powershell
mkdir C:\actions-runner && cd C:\actions-runner

# Download (URL from GitHub UI)
Invoke-WebRequest -Uri <RUNNER_DOWNLOAD_URL> -OutFile actions-runner.zip
Expand-Archive -Path actions-runner.zip -DestinationPath .

# Configure
.\config.cmd --url https://github.com/<your-user>/econatlas-backend --token <TOKEN>
```

**2. Install the runner as a Windows service**

```powershell
cd C:\actions-runner
.\svc.ps1 install
.\svc.ps1 start
```

**3. Done!** Every push to `main` now triggers the workflow in
`.github/workflows/deploy.yml`, which syncs code, updates dependencies,
and restarts the service — all automatically.

### Managing the Service

```powershell
nssm status econatlas-backend    # Check status
nssm restart econatlas-backend   # Restart
nssm stop econatlas-backend      # Stop
nssm start econatlas-backend     # Start
nssm edit econatlas-backend      # Edit config (GUI)
```

Logs are written to `C:\econatlas-backend\logs\`.

### Firewall (optional — expose to LAN/internet)

```powershell
New-NetFirewallRule -DisplayName "EconAtlas API" `
  -Direction Inbound -Protocol TCP -LocalPort 8000 -Action Allow
```

The API is then accessible at `http://<WINDOWS_IP>:8000` from other devices.

## Future Roadmap

- AI event extraction pipelines
- Economic knowledge graph
- Portfolio exposure analysis
- Push notification triggers via Supabase Edge Functions
