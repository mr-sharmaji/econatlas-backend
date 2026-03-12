"""Backfill 1 year of daily stock price history for all discover stocks.

Fetches daily close prices from Yahoo Finance chart API and upserts into
discover_stock_price_history table. Idempotent: uses ON CONFLICT DO NOTHING.

Usage:
  python scripts/backfill_discover_stock_history.py
"""
from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure backend root is on path when run as: python scripts/backfill_discover_stock_history.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

import requests

from app.core.database import close_pool, get_pool, init_pool

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS?range=1y&interval=1d"
RATE_LIMIT_SECONDS = 2.0
MAX_RETRIES = 4
LOG_EVERY = 25

INSERT_SQL = """
INSERT INTO discover_stock_price_history (symbol, trade_date, close, volume, source)
VALUES ($1, $2, $3, $4, 'yahoo')
ON CONFLICT (symbol, trade_date) DO NOTHING
"""


def _fetch_yahoo_chart(symbol: str) -> list[tuple[str, datetime, float, int | None]]:
    """Fetch 1Y daily chart data from Yahoo Finance for a given symbol.

    Returns a list of (symbol, trade_date, close, volume) tuples.
    Retries with exponential backoff on 429 / 5xx responses.
    """
    import time

    url = YAHOO_CHART_URL.format(symbol=symbol)
    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, timeout=15)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = (2 ** attempt) * 5  # 5s, 10s, 20s, 40s
            logger.warning("Got %d for %s – retrying in %ds (attempt %d/%d)", resp.status_code, symbol, wait, attempt + 1, MAX_RETRIES)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        resp.raise_for_status()  # final attempt failed — raise

    data = resp.json()

    result = data["chart"]["result"][0]
    timestamps = result["timestamps"]
    quotes = result["indicators"]["quote"][0]
    closes = quotes["close"]
    volumes = quotes.get("volume", [None] * len(timestamps))

    rows: list[tuple[str, datetime, float, int | None]] = []
    for ts, close, volume in zip(timestamps, closes, volumes):
        if close is None:
            continue
        trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        rows.append((symbol, trade_date, float(close), int(volume) if volume is not None else None))

    return rows


async def main() -> None:
    await init_pool()
    pool = await get_pool()

    try:
        # 1. Fetch all distinct symbols from discover_stock_snapshots
        symbols = await pool.fetch("SELECT DISTINCT symbol FROM discover_stock_snapshots")
        symbol_list = [row["symbol"] for row in symbols]
        logger.info("Found %d distinct symbols to backfill.", len(symbol_list))

        loop = asyncio.get_event_loop()
        total_inserted = 0

        for i, symbol in enumerate(symbol_list, start=1):
            try:
                rows = await loop.run_in_executor(None, _fetch_yahoo_chart, symbol)
                if rows:
                    await pool.executemany(INSERT_SQL, rows)
                    total_inserted += len(rows)
                    if i % LOG_EVERY == 0:
                        logger.info("Progress: %d / %d symbols processed (%d rows so far).", i, len(symbol_list), total_inserted)
            except Exception:
                logger.exception("Error fetching/upserting symbol %s – skipping.", symbol)

            await asyncio.sleep(RATE_LIMIT_SECONDS)

        logger.info("Backfill complete: %d symbols, %d total rows upserted.", len(symbol_list), total_inserted)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
