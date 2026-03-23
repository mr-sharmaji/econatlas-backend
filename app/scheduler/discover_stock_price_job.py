"""Daily job to update stock price history for the last 7 days.

Fetches recent daily close prices from Yahoo Finance and upserts into
discover_stock_price_history. Uses concurrent thread pool workers (10 parallel)
to complete in ~15 minutes instead of 2.5 hours.

Runs after the main discover_stock_job (~4:30 PM IST weekdays).
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

from app.core.database import get_pool

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS?range=7d&interval=1d"
MAX_RETRIES = 5
CONCURRENCY = 10          # parallel threads
BASE_DELAY = 0.5          # seconds between requests per thread
BATCH_LOG_EVERY = 100     # log progress every N symbols
THROTTLE_AFTER_429S = 3   # shared 429 count before global pause
GLOBAL_PAUSE_SECONDS = 60 # all workers pause on sustained 429s

INSERT_SQL = """
INSERT INTO discover_stock_price_history (symbol, trade_date, close, volume, source)
VALUES ($1, $2, $3, $4, 'yahoo')
ON CONFLICT (symbol, trade_date) DO NOTHING
"""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def _fetch_yahoo_7d(symbol: str, stats: dict) -> list[tuple[str, datetime, float, int | None]]:
    """Fetch last 7 days of daily prices from Yahoo Finance (sync, thread-safe)."""
    url = YAHOO_CHART_URL.format(symbol=symbol)
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
        except requests.exceptions.RequestException:
            stats["timeouts"] += 1
            time.sleep(2 ** attempt + random.uniform(0, 1))
            continue

        if resp.status_code == 429:
            stats["throttled"] += 1
            wait = (2 ** attempt) * 5 + random.uniform(1, 3)
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            time.sleep((2 ** attempt) * 2 + random.uniform(0, 2))
            continue

        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            return []

        # Reset throttle on success
        if stats["throttled"] > 0:
            stats["throttled"] = max(0, stats["throttled"] - 1)

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

    return []


async def run_discover_stock_price_job() -> None:
    """Fetch last 7 days of prices for all discover stocks using thread pool."""
    pool = await get_pool()
    symbols = await pool.fetch(
        "SELECT DISTINCT symbol FROM discover_stock_snapshots ORDER BY symbol"
    )
    symbol_list = [row["symbol"] for row in symbols]
    logger.info("Stock price daily update: %d symbols, %d workers.", len(symbol_list), CONCURRENCY)

    # Shuffle to spread load across Yahoo endpoints
    random.shuffle(symbol_list)

    stats = {"done": 0, "inserted": 0, "errors": 0, "throttled": 0, "timeouts": 0, "total": len(symbol_list)}
    executor = ThreadPoolExecutor(max_workers=CONCURRENCY, thread_name_prefix="yahoo-price")
    loop = asyncio.get_event_loop()

    # Process in chunks to allow async DB writes between fetches
    chunk_size = CONCURRENCY * 2
    for chunk_start in range(0, len(symbol_list), chunk_size):
        chunk = symbol_list[chunk_start:chunk_start + chunk_size]

        # If throttled globally, pause all
        if stats["throttled"] >= THROTTLE_AFTER_429S:
            logger.warning("Global throttle — pausing %ds (%d 429s)", GLOBAL_PAUSE_SECONDS, stats["throttled"])
            await asyncio.sleep(GLOBAL_PAUSE_SECONDS)
            stats["throttled"] = 0

        # Fetch chunk in parallel threads
        futures = [
            loop.run_in_executor(executor, _fetch_yahoo_7d, sym, stats)
            for sym in chunk
        ]
        results = await asyncio.gather(*futures, return_exceptions=True)

        # Upsert results to DB
        for sym, result in zip(chunk, results):
            if isinstance(result, Exception):
                stats["errors"] += 1
                logger.warning("Error for %s: %s", sym, str(result)[:80])
            elif result:
                try:
                    await pool.executemany(INSERT_SQL, result)
                    stats["inserted"] += len(result)
                except Exception as e:
                    stats["errors"] += 1
                    logger.warning("DB error for %s: %s", sym, str(e)[:80])
            stats["done"] += 1

        if stats["done"] % BATCH_LOG_EVERY < chunk_size:
            logger.info(
                "Stock price progress: %d / %d done (%d rows, %d errors, %d timeouts)",
                stats["done"], stats["total"], stats["inserted"], stats["errors"], stats["timeouts"],
            )

        # Small delay between chunks
        await asyncio.sleep(BASE_DELAY)

    executor.shutdown(wait=False)
    logger.info(
        "Stock price daily update complete: %d symbols, %d rows upserted, %d errors, %d timeouts.",
        stats["total"], stats["inserted"], stats["errors"], stats["timeouts"],
    )
