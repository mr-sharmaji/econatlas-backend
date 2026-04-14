"""Daily job to update mutual fund NAV history for the last 7 days.

Fetches recent NAV data from mfapi.in and upserts into
discover_mf_nav_history. Lightweight — only appends new data via
INSERT ON CONFLICT DO NOTHING.

Runs after the main discover_mutual_fund_job (~10:30 PM IST weekdays).

Uses bounded parallelism: _MAX_CONCURRENCY scheme fetches run
concurrently in a dedicated thread pool (because mfapi.in is hit via
the blocking `requests` library), coordinated by an asyncio.Semaphore
on the coroutine side. Each worker spaces its own requests by
_PER_WORKER_SPACING_SECONDS so the aggregate request rate stays
bounded. Prior to this, the job was strictly serial with a 1 s
rate-limit sleep per scheme and took ~37 minutes for ~2,200 schemes;
with concurrency=8 and spacing=0.5 s it finishes in ~5 minutes.
_fetch_mf_nav_7d already retries with exponential backoff on 429/5xx
so the API will auto-throttle us if we're being too aggressive.
"""
from __future__ import annotations

import asyncio
import logging
import time

from app.scheduler.job_executors import get_job_executor
from datetime import datetime, timedelta, timezone

import requests

from app.core.database import get_pool

logger = logging.getLogger(__name__)

MFAPI_URL = "https://api.mfapi.in/mf/{scheme_code}"
MAX_RETRIES = 3

# Bounded-parallelism tuning. Aggregate req/sec ≈
# _MAX_CONCURRENCY / _PER_WORKER_SPACING_SECONDS, i.e. ~16 req/sec
# with these defaults. mfapi.in has no published rate limit but
# hobby-tier free public APIs typically tolerate 10–30 req/sec
# before 429. _fetch_mf_nav_7d has retry-on-429 so if we do hit a
# cap the job self-throttles.
_MAX_CONCURRENCY = 8
_PER_WORKER_SPACING_SECONDS = 0.5

INSERT_SQL = """
INSERT INTO discover_mf_nav_history (scheme_code, nav_date, nav, source)
VALUES ($1, $2, $3, 'mfapi')
ON CONFLICT (scheme_code, nav_date) DO NOTHING
"""


def _fetch_mf_nav(
    scheme_code: int, cutoff_days: int = 7,
) -> list[tuple[int, datetime, float]]:
    """Fetch NAV history from mfapi.in, keep only rows newer than
    `cutoff_days` days old. `cutoff_days=7` is the default daily
    append; `cutoff_days=1830` (≈5y) is used by the backfill job to
    repair historical gaps in discover_mf_nav_history."""
    url = MFAPI_URL.format(scheme_code=scheme_code)
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=15)
        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = (2 ** attempt) * 3
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        return []

    payload = resp.json()
    cutoff = datetime.now(tz=timezone.utc).date() - timedelta(days=cutoff_days)
    nav_entries = payload.get("data", [])

    rows: list[tuple[int, datetime, float]] = []
    for entry in nav_entries:
        try:
            nav_date = datetime.strptime(entry["date"], "%d-%m-%Y").date()
        except (ValueError, KeyError):
            continue
        if nav_date < cutoff:
            continue
        try:
            nav_value = float(entry["nav"])
        except (ValueError, KeyError):
            continue
        rows.append((scheme_code, nav_date, nav_value))

    return rows


def _fetch_mf_nav_7d(scheme_code: int) -> list[tuple[int, datetime, float]]:
    return _fetch_mf_nav(scheme_code, cutoff_days=7)


# Backfill window — matches the point-to-point returns helper in
# discover_service which reads ≤5y of NAV history. No point filling
# gaps older than that since nothing in the UI reads from there.
_BACKFILL_WINDOW_DAYS = 1830


def _fetch_mf_nav_backfill(scheme_code: int) -> list[tuple[int, datetime, float]]:
    return _fetch_mf_nav(scheme_code, cutoff_days=_BACKFILL_WINDOW_DAYS)


async def run_discover_mf_nav_job() -> None:
    """Fetch last 7 days of NAV for all discover mutual funds and upsert.

    Parallelized: _MAX_CONCURRENCY coroutines run concurrently,
    each dispatching blocking mfapi.in fetches to a dedicated
    thread pool (same size) and then persisting the resulting
    rows via asyncpg. Per-worker spacing provides the rate limit.
    See module docstring for rationale + tuning.
    """
    pool = await get_pool()
    codes = await pool.fetch(
        "SELECT DISTINCT scheme_code FROM discover_mutual_fund_snapshots ORDER BY scheme_code"
    )
    code_list = [row["scheme_code"] for row in codes]
    total = len(code_list)
    logger.info(
        "MF NAV daily update: %d scheme codes to process (concurrency=%d, spacing=%.2fs).",
        total, _MAX_CONCURRENCY, _PER_WORKER_SPACING_SECONDS,
    )

    # Size the thread pool to match asyncio concurrency so run_in_executor
    # calls don't queue behind a single worker. First call wins — safe
    # because this is the only caller of the "discover-mf-nav" executor.
    executor = get_job_executor("discover-mf-nav", max_workers=_MAX_CONCURRENCY)
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(_MAX_CONCURRENCY)

    # Counters mutated from concurrent coroutines. asyncio is cooperative
    # single-threaded so simple += is safe; the dict just groups them.
    counters = {"inserted": 0, "errors": 0, "processed": 0}
    t_start = time.monotonic()

    async def process_one(scheme_code: int) -> None:
        async with semaphore:
            try:
                rows = await loop.run_in_executor(
                    executor, _fetch_mf_nav_7d, scheme_code,
                )
                if rows:
                    await pool.executemany(INSERT_SQL, rows)
                    counters["inserted"] += len(rows)
            except Exception:
                counters["errors"] += 1
                logger.exception("Error updating NAV for %s — skipping.", scheme_code)
            counters["processed"] += 1
            if counters["processed"] % 250 == 0:
                elapsed = time.monotonic() - t_start
                rate = counters["processed"] / elapsed if elapsed > 0 else 0.0
                eta = (total - counters["processed"]) / rate if rate > 0 else 0.0
                logger.info(
                    "MF NAV update progress: %d / %d scheme codes (%.1f/s, eta %.0fs).",
                    counters["processed"], total, rate, eta,
                )
            # Per-worker spacing: holds the semaphore slot long enough
            # that the aggregate request rate is bounded. With
            # concurrency=8 and spacing=0.5s the ceiling is ~16 req/s.
            await asyncio.sleep(_PER_WORKER_SPACING_SECONDS)

    tasks = [asyncio.create_task(process_one(code)) for code in code_list]
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.monotonic() - t_start
    logger.info(
        "MF NAV daily update complete: %d scheme codes, %d rows upserted, "
        "%d errors, %.1fs elapsed.",
        total, counters["inserted"], counters["errors"], elapsed,
    )


async def run_discover_mf_nav_backfill_job() -> None:
    """One-shot backfill: fetch ≤5y NAV history for every scheme and
    upsert. Uses ON CONFLICT DO NOTHING so existing rows are untouched
    — only gaps get filled. Same parallelism/spacing as the daily job
    so it's safe to run on top of normal traffic.

    Runs ~7-10 minutes for ~6k schemes at 16 req/s aggregate. Log
    progress every 250 schemes like the daily job.
    """
    pool = await get_pool()
    codes = await pool.fetch(
        "SELECT DISTINCT scheme_code FROM discover_mutual_fund_snapshots ORDER BY scheme_code"
    )
    code_list = [row["scheme_code"] for row in codes]
    total = len(code_list)
    logger.info(
        "MF NAV BACKFILL: %d scheme codes, window=%d days, "
        "concurrency=%d, spacing=%.2fs.",
        total, _BACKFILL_WINDOW_DAYS, _MAX_CONCURRENCY,
        _PER_WORKER_SPACING_SECONDS,
    )

    executor = get_job_executor("discover-mf-nav", max_workers=_MAX_CONCURRENCY)
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(_MAX_CONCURRENCY)

    counters = {"inserted": 0, "errors": 0, "processed": 0}
    t_start = time.monotonic()

    async def process_one(scheme_code: int) -> None:
        async with semaphore:
            try:
                rows = await loop.run_in_executor(
                    executor, _fetch_mf_nav_backfill, scheme_code,
                )
                if rows:
                    # executemany preserves ON CONFLICT DO NOTHING, so
                    # existing good rows are not modified.
                    await pool.executemany(INSERT_SQL, rows)
                    counters["inserted"] += len(rows)
            except Exception:
                counters["errors"] += 1
                logger.exception("MF NAV BACKFILL: error scheme=%s — skipping.", scheme_code)
            counters["processed"] += 1
            if counters["processed"] % 250 == 0:
                elapsed = time.monotonic() - t_start
                rate = counters["processed"] / elapsed if elapsed > 0 else 0.0
                eta = (total - counters["processed"]) / rate if rate > 0 else 0.0
                logger.info(
                    "MF NAV BACKFILL progress: %d / %d (%.1f/s, eta %.0fs), "
                    "rows_inserted_so_far=%d.",
                    counters["processed"], total, rate, eta,
                    counters["inserted"],
                )
            await asyncio.sleep(_PER_WORKER_SPACING_SECONDS)

    tasks = [asyncio.create_task(process_one(code)) for code in code_list]
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.monotonic() - t_start
    logger.info(
        "MF NAV BACKFILL complete: %d schemes, %d rows inserted, "
        "%d errors, %.1fs elapsed.",
        total, counters["inserted"], counters["errors"], elapsed,
    )
