"""Backfill the last trading session's daily data (market + commodities) and intraday data.

Use this to refresh or backfill the most recent trading day when the scheduler missed a run
or after a restart. Fetches current prices, assigns the exchange trading date (NSE/NYSE),
and upserts into market_prices. When markets are closed, fetches full minute-level intraday
data from Yahoo (same granularity as live) and inserts into market_prices_intraday. When
markets are open, inserts a single current snapshot for intraday (same as scheduler).
Does not filter by price change (unlike the scheduler when closed), so the last session
is fully written.

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
        from app.scheduler.trading_calendar import get_market_status, get_trading_date
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

    # Populate intraday for 1D chart: when market open use same logic as scheduler (live timestamp); when closed fetch full minute-level data from Yahoo.
    from app.scheduler.trading_calendar import NSE, NYSE

    status = get_market_status()
    now = datetime.now(timezone.utc)
    ts_rounded = market_service._round_to_minute(now).isoformat()
    if status.get("nse_open") or status.get("nyse_open"):
        from app.scheduler.market_job import build_market_intraday_rows_for_open
        from app.scheduler.commodity_job import build_commodity_intraday_rows_for_open
        intraday_rows = build_market_intraday_rows_for_open(
            market_rows,
            status,
            ts_rounded,
        )
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Intraday (market): %d points inserted (market open).", n)
        if commodity_rows:
            intraday_commodity = build_commodity_intraday_rows_for_open(commodity_rows, ts_rounded)
            n = await market_service.insert_intraday_batch(intraday_commodity)
            logger.info("Intraday (commodities): %d points inserted (market open).", n)
    else:
        # Market closed: fetch full minute-level intraday from Yahoo (same as live session granularity).
        from app.scheduler.market_job import build_market_intraday_rows_last_session_yahoo
        from app.scheduler.commodity_job import build_commodity_intraday_rows_last_session_yahoo

        trading_date_nse = get_trading_date(now, NSE)
        trading_date_nyse = get_trading_date(now, NYSE)
        trading_date_by_exchange = {NSE: trading_date_nse, NYSE: trading_date_nyse}
        intraday_market = await loop.run_in_executor(
            None,
            lambda: build_market_intraday_rows_last_session_yahoo(market_rows, trading_date_by_exchange),
        )
        if intraday_market:
            n = await market_service.insert_intraday_batch(intraday_market)
            logger.info("Intraday (market): %d points inserted (last session, minute-level).", n)
        if commodity_rows:
            intraday_commodity = await loop.run_in_executor(
                None,
                lambda: build_commodity_intraday_rows_last_session_yahoo(commodity_rows, trading_date_nyse),
            )
            if intraday_commodity:
                n = await market_service.insert_intraday_batch(intraday_commodity)
                logger.info("Intraday (commodities): %d points inserted (last session, minute-level).", n)

    logger.info("Backfill last session complete.")
    await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
