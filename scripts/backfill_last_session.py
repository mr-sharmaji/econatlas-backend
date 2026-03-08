"""Backfill the last trading session's daily data (market + commodities) into market_prices.

Use this to refresh or backfill the most recent trading day when the scheduler missed a run
or after a restart. Fetches current prices, assigns the exchange trading date (NSE/NYSE),
and upserts into market_prices. Does not filter by price change (unlike the scheduler when
markets are closed), so the last session is fully written.

Usage (from repo root econatlas-backend):

  # With venv activated and .env containing DATABASE_URL:
  python scripts/backfill_last_session.py

  # Or with PYTHONPATH set:
  PYTHONPATH=. python scripts/backfill_last_session.py

Idempotent: uses upsert (ON CONFLICT DO UPDATE) so re-running is safe.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path

# Ensure backend root is on path when run as: python scripts/backfill_last_session.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from app.core.database import close_pool, init_pool
from app.services import market_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    await init_pool()

    try:
        from app.scheduler.market_job import _fetch_market_rows_sync
        from app.scheduler.commodity_job import _fetch_commodity_rows_sync
        from app.scheduler.trading_calendar import get_market_status
        from datetime import datetime, timezone
    except ImportError as e:
        logger.error("Import failed: %s. Run from backend root with deps installed.", e)
        return

    loop = asyncio.get_event_loop()

    # Fetch market rows (indices, FX, bonds) – timestamp = exchange trading date
    market_rows, _ = await loop.run_in_executor(None, _fetch_market_rows_sync)
    if not market_rows:
        logger.warning("Market: no rows fetched (check network / Yahoo/FRED). Nothing to insert.")
    else:
        n_market = await market_service.insert_prices_batch_upsert_daily(market_rows)
        logger.info("Market: %d rows upserted (daily) for last trading session.", n_market)

    # Fetch commodity rows – timestamp = NYSE trading date
    commodity_rows, _ = await loop.run_in_executor(None, _fetch_commodity_rows_sync)
    if not commodity_rows:
        logger.warning("Commodities: no rows fetched (check network). Nothing to insert.")
    else:
        n_commodity = await market_service.insert_prices_batch_upsert_daily(commodity_rows)
        logger.info("Commodities: %d rows upserted (daily) for last trading session.", n_commodity)

    # Optionally write intraday points if market is open (for 1D chart)
    status = get_market_status()
    now = datetime.now(timezone.utc)
    ts_rounded = market_service._round_to_minute(now).isoformat()
    if status.get("nse_open") or status.get("nyse_open"):
        from app.scheduler.market_job import ASSET_EXCHANGE, NSE, NYSE
        intraday_rows = []
        for r in market_rows:
            exchange = ASSET_EXCHANGE.get(r["asset"], NYSE)
            if (exchange == NSE and status.get("nse_open")) or (exchange == NYSE and status.get("nyse_open")):
                intraday_rows.append({
                    "asset": r["asset"],
                    "instrument_type": r.get("instrument_type") or "index",
                    "price": r["price"],
                    "timestamp": ts_rounded,
                })
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Intraday (market): %d points inserted.", n)
    if status.get("nyse_open") and commodity_rows:
        intraday_commodity = [
            {"asset": r["asset"], "instrument_type": "commodity", "price": r["price"], "timestamp": ts_rounded}
            for r in commodity_rows
        ]
        n = await market_service.insert_intraday_batch(intraday_commodity)
        logger.info("Intraday (commodities): %d points inserted.", n)

    logger.info("Backfill last session complete.")
    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
