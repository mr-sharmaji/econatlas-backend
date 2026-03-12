"""Backfill 1 year of daily NAV history for all discover mutual funds.

Fetches NAV history from the public mfapi.in API and upserts into
discover_mf_nav_history table. Idempotent: uses ON CONFLICT DO NOTHING.

Usage:
  python scripts/backfill_discover_mf_nav_history.py
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure backend root is on path when run as: python scripts/backfill_discover_mf_nav_history.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

import requests

from app.core.database import close_pool, get_pool, init_pool

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

MFAPI_URL = "https://api.mfapi.in/mf/{scheme_code}"
RATE_LIMIT_SECONDS = 0.5
LOG_EVERY = 25

INSERT_SQL = """
INSERT INTO discover_mf_nav_history (scheme_code, nav_date, nav, source)
VALUES ($1, $2, $3, 'mfapi')
ON CONFLICT (scheme_code, nav_date) DO NOTHING
"""


def _fetch_mf_nav(scheme_code: int) -> list[tuple[int, datetime, float]]:
    """Fetch NAV history from mfapi.in for a given scheme code.

    Returns a list of (scheme_code, nav_date, nav) tuples filtered to the last 365 days.
    """
    url = MFAPI_URL.format(scheme_code=scheme_code)
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    payload = resp.json()

    cutoff = datetime.now(tz=timezone.utc).date() - timedelta(days=365)
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


async def main() -> None:
    await init_pool()
    pool = await get_pool()

    try:
        # 1. Fetch all distinct scheme_codes from discover_mutual_fund_snapshots
        codes = await pool.fetch("SELECT DISTINCT scheme_code FROM discover_mutual_fund_snapshots")
        code_list = [row["scheme_code"] for row in codes]
        logger.info("Found %d distinct scheme codes to backfill.", len(code_list))

        loop = asyncio.get_event_loop()
        total_inserted = 0

        for i, scheme_code in enumerate(code_list, start=1):
            try:
                rows = await loop.run_in_executor(None, _fetch_mf_nav, scheme_code)
                if rows:
                    await pool.executemany(INSERT_SQL, rows)
                    total_inserted += len(rows)
                    if i % LOG_EVERY == 0:
                        logger.info("Progress: %d / %d scheme codes processed (%d rows so far).", i, len(code_list), total_inserted)
            except Exception:
                logger.exception("Error fetching/upserting scheme_code %s – skipping.", scheme_code)

            await asyncio.sleep(RATE_LIMIT_SECONDS)

        logger.info("Backfill complete: %d scheme codes, %d total rows upserted.", len(code_list), total_inserted)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
