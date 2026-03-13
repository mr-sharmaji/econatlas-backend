"""Daily job to update mutual fund NAV history for the last 7 days.

Fetches recent NAV data from mfapi.in and upserts into
discover_mf_nav_history. Lightweight — only appends new data via
INSERT ON CONFLICT DO NOTHING.

Runs after the main discover_mutual_fund_job (~10:30 PM IST weekdays).
"""
from __future__ import annotations

import logging
import time

from app.scheduler.job_executors import get_job_executor
from datetime import datetime, timedelta, timezone

import requests

from app.core.database import get_pool

logger = logging.getLogger(__name__)

MFAPI_URL = "https://api.mfapi.in/mf/{scheme_code}"
MAX_RETRIES = 3
RATE_LIMIT_SECONDS = 1.0

INSERT_SQL = """
INSERT INTO discover_mf_nav_history (scheme_code, nav_date, nav, source)
VALUES ($1, $2, $3, 'mfapi')
ON CONFLICT (scheme_code, nav_date) DO NOTHING
"""


def _fetch_mf_nav_7d(scheme_code: int) -> list[tuple[int, datetime, float]]:
    """Fetch NAV history from mfapi.in and keep only last 7 days."""
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
    cutoff = datetime.now(tz=timezone.utc).date() - timedelta(days=7)
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


async def run_discover_mf_nav_job() -> None:
    """Fetch last 7 days of NAV for all discover mutual funds and upsert."""
    import asyncio

    pool = await get_pool()
    codes = await pool.fetch(
        "SELECT DISTINCT scheme_code FROM discover_mutual_fund_snapshots ORDER BY scheme_code"
    )
    code_list = [row["scheme_code"] for row in codes]
    logger.info("MF NAV daily update: %d scheme codes to process.", len(code_list))

    loop = asyncio.get_event_loop()
    total_inserted = 0
    errors = 0

    for i, scheme_code in enumerate(code_list, start=1):
        try:
            rows = await loop.run_in_executor(get_job_executor("discover-mf-nav"), _fetch_mf_nav_7d, scheme_code)
            if rows:
                await pool.executemany(INSERT_SQL, rows)
                total_inserted += len(rows)
        except Exception:
            errors += 1
            logger.exception("Error updating NAV for %s — skipping.", scheme_code)

        await asyncio.sleep(RATE_LIMIT_SECONDS)

        if i % 100 == 0:
            logger.info("MF NAV update progress: %d / %d scheme codes.", i, len(code_list))

    logger.info(
        "MF NAV daily update complete: %d scheme codes, %d rows upserted, %d errors.",
        len(code_list),
        total_inserted,
        errors,
    )
