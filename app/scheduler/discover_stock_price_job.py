"""Daily job to update stock price history for the last 7 days.

Fetches recent daily close prices from Yahoo Finance and upserts into
discover_stock_price_history. Uses concurrent workers (10 parallel) to
complete in ~10 minutes instead of 2.5 hours.

Runs after the main discover_stock_job (~4:30 PM IST weekdays).
"""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone

import aiohttp

from app.core.database import get_pool

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS?range=7d&interval=1d"
MAX_RETRIES = 5
CONCURRENCY = 10          # parallel workers
BASE_DELAY = 0.5          # seconds between requests per worker
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


async def _fetch_yahoo_7d_async(
    session: aiohttp.ClientSession, symbol: str, stats: dict,
) -> list[tuple[str, datetime, float, int | None]]:
    """Fetch last 7 days of daily prices from Yahoo Finance (async).

    Updates stats["throttled"] on 429s so workers can coordinate backoff.
    """
    url = YAHOO_CHART_URL.format(symbol=symbol)
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 429:
                    stats["throttled"] += 1
                    wait = (2 ** attempt) * 5 + random.uniform(1, 3)
                    logger.debug("429 for %s — waiting %.0fs (attempt %d)", symbol, wait, attempt + 1)
                    await asyncio.sleep(wait)
                    continue
                if resp.status >= 500:
                    await asyncio.sleep((2 ** attempt) * 2 + random.uniform(0, 2))
                    continue
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            stats["timeouts"] += 1
            await asyncio.sleep(2 ** attempt + random.uniform(0, 1))
            continue
        except Exception:
            return []

        # Reset throttle counter on success
        stats["throttled"] = max(0, stats["throttled"] - 1)

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


async def _worker(
    name: str,
    queue: asyncio.Queue,
    session: aiohttp.ClientSession,
    pool,
    stats: dict,
) -> None:
    """Worker coroutine that pulls symbols from queue and fetches prices."""
    while True:
        try:
            symbol = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            # If too many 429s globally, all workers pause
            if stats["throttled"] >= THROTTLE_AFTER_429S:
                logger.warning("Worker %s: global throttle — pausing %ds", name, GLOBAL_PAUSE_SECONDS)
                await asyncio.sleep(GLOBAL_PAUSE_SECONDS)
                stats["throttled"] = 0

            rows = await _fetch_yahoo_7d_async(session, symbol, stats)
            if rows:
                await pool.executemany(INSERT_SQL, rows)
                stats["inserted"] += len(rows)
            stats["done"] += 1
        except Exception as e:
            stats["errors"] += 1
            logger.warning("Worker %s: error for %s: %s", name, symbol, str(e)[:80])
            stats["done"] += 1

        # Adaptive delay: slower when seeing throttles
        delay = BASE_DELAY + random.uniform(0, 0.5)
        if stats["throttled"] > 0:
            delay += 2.0  # back off when throttled
        await asyncio.sleep(delay)

        if stats["done"] % BATCH_LOG_EVERY == 0:
            logger.info(
                "Stock price progress: %d / %d done (%d rows, %d errors)",
                stats["done"], stats["total"], stats["inserted"], stats["errors"],
            )


async def run_discover_stock_price_job() -> None:
    """Fetch last 7 days of prices for all discover stocks using concurrent workers."""
    pool = await get_pool()
    symbols = await pool.fetch(
        "SELECT DISTINCT symbol FROM discover_stock_snapshots ORDER BY symbol"
    )
    symbol_list = [row["symbol"] for row in symbols]
    logger.info("Stock price daily update: %d symbols, %d workers.", len(symbol_list), CONCURRENCY)

    # Shuffle to spread load across different Yahoo endpoints
    random.shuffle(symbol_list)

    queue: asyncio.Queue = asyncio.Queue()
    for s in symbol_list:
        queue.put_nowait(s)

    stats = {"done": 0, "inserted": 0, "errors": 0, "throttled": 0, "timeouts": 0, "total": len(symbol_list)}

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        workers = [
            asyncio.create_task(_worker(f"W{i}", queue, session, pool, stats))
            for i in range(CONCURRENCY)
        ]
        await asyncio.gather(*workers)

    logger.info(
        "Stock price daily update complete: %d symbols, %d rows upserted, %d errors, %d timeouts.",
        stats["total"], stats["inserted"], stats["errors"], stats["timeouts"],
    )
