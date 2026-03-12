"""Backfill 5 years of daily stock price history for all discover stocks.

Fetches daily close prices from Yahoo Finance chart API and upserts into
discover_stock_price_history table. Idempotent: uses ON CONFLICT DO NOTHING.

Skips symbols that already have >= 1000 rows (already backfilled).
Processes in batches of 50 with a cooldown pause between batches.

Usage:
  python scripts/backfill_discover_stock_history.py
"""
from __future__ import annotations

import asyncio
import logging
import random
import sys
import time
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

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS?range=5y&interval=1d"
RATE_LIMIT_SECONDS = 4.0  # base delay between symbols
BATCH_SIZE = 50  # pause longer after every N symbols
BATCH_COOLDOWN = 30  # seconds to wait between batches
MAX_RETRIES = 5
LOG_EVERY = 10

INSERT_SQL = """
INSERT INTO discover_stock_price_history (symbol, trade_date, close, volume, source)
VALUES ($1, $2, $3, $4, 'yahoo')
ON CONFLICT (symbol, trade_date) DO NOTHING
"""

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


def _fetch_yahoo_chart(symbol: str) -> list[tuple[str, datetime, float, int | None]]:
    """Fetch 1Y daily chart data from Yahoo Finance for a given symbol.

    Returns a list of (symbol, trade_date, close, volume) tuples.
    Retries with exponential backoff on 429 / 5xx responses.
    """
    url = YAHOO_CHART_URL.format(symbol=symbol)
    for attempt in range(MAX_RETRIES):
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = (2 ** attempt) * 10 + random.uniform(0, 5)  # 10s, 20s, 40s, 80s, 160s + jitter
            logger.warning(
                "Got %d for %s – retrying in %.0fs (attempt %d/%d)",
                resp.status_code, symbol, wait, attempt + 1, MAX_RETRIES,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        resp.raise_for_status()

    data = resp.json()

    result = data["chart"]["result"][0]
    timestamps = result.get("timestamp") or result.get("timestamps") or []
    if not timestamps:
        return []
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
        # 1. Fetch symbols that haven't been backfilled yet (< 1000 rows = not done)
        symbols = await pool.fetch("""
            SELECT s.symbol
            FROM discover_stock_snapshots s
            LEFT JOIN (
                SELECT symbol, COUNT(*) AS cnt
                FROM discover_stock_price_history
                GROUP BY symbol
            ) h ON h.symbol = s.symbol
            WHERE COALESCE(h.cnt, 0) < 1000
            ORDER BY s.symbol
        """)
        symbol_list = [row["symbol"] for row in symbols]
        logger.info("Found %d symbols needing backfill (skipped already-done).", len(symbol_list))

        if not symbol_list:
            logger.info("Nothing to do — all symbols already backfilled.")
            return

        loop = asyncio.get_event_loop()
        total_inserted = 0
        consecutive_429s = 0

        for i, symbol in enumerate(symbol_list, start=1):
            try:
                rows = await loop.run_in_executor(None, _fetch_yahoo_chart, symbol)
                consecutive_429s = 0  # reset on success
                if rows:
                    await pool.executemany(INSERT_SQL, rows)
                    total_inserted += len(rows)
                if i % LOG_EVERY == 0:
                    logger.info("Progress: %d / %d symbols (%d rows so far).", i, len(symbol_list), total_inserted)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    consecutive_429s += 1
                    if consecutive_429s >= 3:
                        pause = 120
                        logger.warning("3 consecutive 429s — pausing %ds to cool down.", pause)
                        await asyncio.sleep(pause)
                        consecutive_429s = 0
                logger.warning("Skipping %s: %s", symbol, e)
            except Exception:
                logger.exception("Error fetching/upserting symbol %s – skipping.", symbol)

            # Base delay with jitter
            await asyncio.sleep(RATE_LIMIT_SECONDS + random.uniform(0, 2))

            # Batch cooldown
            if i % BATCH_SIZE == 0:
                logger.info("Batch cooldown: pausing %ds after %d symbols.", BATCH_COOLDOWN, i)
                await asyncio.sleep(BATCH_COOLDOWN)

        logger.info("Backfill complete: %d symbols processed, %d total rows upserted.", len(symbol_list), total_inserted)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
