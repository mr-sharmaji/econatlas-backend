"""Backfill recent crypto daily prices and intraday data.

Fetches current crypto prices via CoinGecko (primary) and Yahoo (fallback),
upserts daily rows, and builds minute-level intraday rows from Yahoo 1m bars
for the last 5 calendar days. Crypto is 24/7 so no trading calendar filtering
is needed.

Usage (from repo root econatlas-backend):

  # With venv activated and .env containing DATABASE_URL:
  python scripts/backfill_crypto.py

  # Or with PYTHONPATH set:
  PYTHONPATH=. python scripts/backfill_crypto.py

Idempotent: uses upsert (ON CONFLICT DO UPDATE) so re-running is safe.
"""
from __future__ import annotations

import asyncio
import argparse
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Ensure backend root is on path when run as: python scripts/backfill_crypto.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from app.core.database import close_pool, init_pool
from app.services import market_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def main(repair: bool = False) -> None:
    await init_pool()

    try:
        from app.scheduler.crypto_job import (
            _fetch_crypto_rows_sync,
            build_crypto_intraday_rows_last_session_yahoo,
        )
    except ImportError as e:
        logger.error("Import failed: %s. Run from backend root with deps installed.", e)
        return

    loop = asyncio.get_event_loop()
    now = datetime.now(timezone.utc)

    # Fetch crypto rows — daily snapshot
    crypto_rows, _ = await loop.run_in_executor(None, _fetch_crypto_rows_sync)
    if not crypto_rows:
        logger.warning("Crypto: no rows fetched (check network). Nothing to insert.")
    else:
        n_crypto = await market_service.insert_prices_batch_upsert_daily(crypto_rows)
        logger.info("Crypto: %d rows upserted (daily).", n_crypto)

    # Populate intraday for 1D chart: last 5 calendar days including today
    def _recent_calendar_dates(days: int) -> list:
        return [now.date() - timedelta(days=i) for i in range(days - 1, -1, -1)]

    recent_calendar_dates = _recent_calendar_dates(5)

    if crypto_rows:
        intraday_crypto = await loop.run_in_executor(
            None,
            lambda: build_crypto_intraday_rows_last_session_yahoo(crypto_rows, recent_calendar_dates),
        )
        if intraday_crypto:
            n = await market_service.insert_intraday_batch(intraday_crypto)
            logger.info("Intraday (crypto): %d points inserted (last 5 calendar days, minute-level).", n)

    logger.info("Crypto backfill complete.")
    if repair:
        repaired = await market_service.repair_intraday_ticks()
        logger.info("Repair mode: removed %d duplicate canonical intraday ticks.", repaired)
    await close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill crypto daily and rolling 24H intraday data.")
    parser.add_argument("--repair", action="store_true", help="Repair canonical intraday duplicates after backfill.")
    args = parser.parse_args()
    asyncio.run(main(repair=args.repair))
