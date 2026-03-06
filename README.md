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
│       └── macro.py         # GET /macro
├── schemas/
│   ├── event_schema.py      # Pydantic models for events
│   └── macro_schema.py      # Pydantic models for macro indicators
├── services/
│   ├── event_service.py     # business logic — events
│   └── macro_service.py     # business logic — macro indicators
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

| Method | Path      | Description                      |
|--------|-----------|----------------------------------|
| GET    | /health   | Health check                     |
| POST   | /events   | Create a new economic event      |
| GET    | /events   | List recent economic events      |
| GET    | /macro    | List macro-economic indicators   |

## Supabase Tables

The backend expects the following tables to exist in Supabase:

- **economic_events** — `id`, `event_type`, `entity`, `impact`, `confidence`, `created_at`
- **macro_indicators** — `id`, `indicator_name`, `value`, `country`, `timestamp`
- **market_prices** — `id`, `asset`, `price`, `timestamp`
- **devices** — `id`, `user_id`, `device_token`, `platform`

## Future Roadmap

- AI event extraction pipelines
- Economic knowledge graph
- Portfolio exposure analysis
- Push notification triggers via Supabase Edge Functions
