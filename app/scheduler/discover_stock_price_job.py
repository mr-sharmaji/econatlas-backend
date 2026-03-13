"""Daily job to update stock price history for the last 7 days.

Fetches recent daily close prices from Yahoo Finance and upserts into
discover_stock_price_history. Lightweight — only appends new data via
INSERT ON CONFLICT DO NOTHING.

Runs after the main discover_stock_job (~4:30 PM IST weekdays).
"""
from __future__ import annotations

import logging
import random
import time

from app.scheduler.job_executors import get_job_executor
from datetime import datetime, timezone

import requests

from app.core.database import get_pool

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS?range=7d&interval=1d"
MAX_RETRIES = 3
RATE_LIMIT_SECONDS = 2.0

INSERT_SQL = """
INSERT INTO discover_stock_price_history (symbol, trade_date, close, volume, source)
VALUES ($1, $2, $3, $4, 'yahoo')
ON CONFLICT (symbol, trade_date) DO NOTHING
"""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def _fetch_yahoo_7d(symbol: str) -> list[tuple[str, datetime, float, int | None]]:
    """Fetch last 7 days of daily prices from Yahoo Finance."""
    url = YAHOO_CHART_URL.format(symbol=symbol)
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
        except requests.exceptions.RequestException:
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = (2 ** attempt) * 5 + random.uniform(0, 3)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        return []

    data = resp.json()
    result = data.get("chart", {}).get("result", [{}])[0]
    timestamps = result.get("timestamp") or []
    if not timestamps:
        return []
    quotes = result.get("indicators", {}).get("quote", [{}])[0]
    closes = quotes.get("close", [])
    volumes = quotes.get("volume", [None] * len(timestamps))

    rows: list[tuple[str, datetime, float, int | None]] = []
    for ts, close, volume in zip(timestamps, closes, volumes):
        if close is None:
            continue
        trade_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()
        rows.append((symbol, trade_date, float(close), int(volume) if volume else None))

    return rows


async def run_discover_stock_price_job() -> None:
    """Fetch last 7 days of prices for all discover stocks and upsert."""
    import asyncio

    pool = await get_pool()
    symbols = await pool.fetch(
        "SELECT DISTINCT symbol FROM discover_stock_snapshots ORDER BY symbol"
    )
    symbol_list = [row["symbol"] for row in symbols]
    logger.info("Stock price daily update: %d symbols to process.", len(symbol_list))

    loop = asyncio.get_event_loop()
    total_inserted = 0
    errors = 0

    for i, symbol in enumerate(symbol_list, start=1):
        try:
            rows = await loop.run_in_executor(get_job_executor("discover-stock-price"), _fetch_yahoo_7d, symbol)
            if rows:
                await pool.executemany(INSERT_SQL, rows)
                total_inserted += len(rows)
        except Exception:
            errors += 1
            logger.exception("Error updating price for %s — skipping.", symbol)

        await asyncio.sleep(RATE_LIMIT_SECONDS + random.uniform(0, 1))

        if i % 50 == 0:
            logger.info("Stock price update progress: %d / %d symbols.", i, len(symbol_list))

    logger.info(
        "Stock price daily update complete: %d symbols, %d rows upserted, %d errors.",
        len(symbol_list),
        total_inserted,
        errors,
    )
