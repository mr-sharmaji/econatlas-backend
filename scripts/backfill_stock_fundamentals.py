"""Backfill missing stock fundamentals from Screener.in + Yahoo Finance v10.

Fetches fundamentals for stocks that have NULL key metrics, updates snapshot
rows, and re-computes scores. Idempotent: only updates existing rows.

Usage:
  python scripts/backfill_stock_fundamentals.py [--limit N] [--dry-run] [--yahoo-only] [--screener-only]
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sys
import time
from pathlib import Path

# Ensure backend root is on path
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from app.core.database import close_pool, get_pool, init_pool
from app.scheduler.discover_stock_job import DiscoverStockScraper, YahooFinanceSession

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 100
BATCH_COOLDOWN = 60  # seconds between batches
LOG_EVERY = 25


async def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill stock fundamentals")
    parser.add_argument("--limit", type=int, default=0, help="Max symbols to process (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't update DB")
    parser.add_argument("--yahoo-only", action="store_true", help="Only fetch from Yahoo v10")
    parser.add_argument("--screener-only", action="store_true", help="Only fetch from Screener.in")
    args = parser.parse_args()

    await init_pool()
    pool = await get_pool()

    try:
        # 1. Find stocks needing backfill: NULL key metrics
        symbols = await pool.fetch("""
            SELECT symbol, last_price, market_cap, sector
            FROM discover_stock_snapshots
            WHERE (
                pe_ratio IS NULL
                OR roe IS NULL
                OR eps IS NULL
                OR beta IS NULL
                OR promoter_holding IS NULL
                OR analyst_target_mean IS NULL
                OR free_cash_flow IS NULL
            )
            ORDER BY
                CASE WHEN market_cap IS NOT NULL THEN -market_cap ELSE 0 END,
                symbol
        """)
        symbol_list = [(row["symbol"], row["last_price"], row["market_cap"], row["sector"]) for row in symbols]

        if args.limit > 0:
            symbol_list = symbol_list[:args.limit]

        logger.info("Found %d symbols needing fundamentals backfill.", len(symbol_list))
        if not symbol_list:
            logger.info("Nothing to do — all symbols have fundamentals.")
            return

        scraper = DiscoverStockScraper()
        yahoo_session = YahooFinanceSession(crumb_ttl=600) if not args.screener_only else None
        loop = asyncio.get_event_loop()

        total_updated = 0
        screener_ok = 0
        yahoo_ok = 0
        errors = 0

        for i, (symbol, last_price, market_cap, sector) in enumerate(symbol_list, start=1):
            fundamentals: dict = {}
            source = "unavailable"

            # Screener.in
            if not args.yahoo_only:
                try:
                    result = await loop.run_in_executor(
                        None, scraper._fetch_screener_fundamentals, symbol
                    )
                    screener_data, screener_source = result
                    if screener_source == "screener_in":
                        fundamentals.update({k: v for k, v in screener_data.items() if v is not None and not k.startswith("_")})
                        source = "screener_in"
                        screener_ok += 1
                    time.sleep(0.5 + random.uniform(0, 0.5))
                except Exception:
                    logger.debug("Screener failed for %s", symbol, exc_info=True)

            # Yahoo v10
            if yahoo_session and not args.screener_only:
                try:
                    yahoo_data = await loop.run_in_executor(
                        None, yahoo_session.get_stock_data, symbol
                    )
                    # Fill missing fields
                    for field in ("pe_ratio", "price_to_book", "eps", "debt_to_equity",
                                  "market_cap", "high_52w", "low_52w", "dividend_yield"):
                        if fundamentals.get(field) is None and yahoo_data.get(field) is not None:
                            fundamentals[field] = yahoo_data[field]

                    # Always add Yahoo-exclusive fields
                    for field in ("beta", "free_cash_flow", "operating_cash_flow", "total_cash",
                                  "total_debt", "total_revenue", "gross_margins", "operating_margins",
                                  "profit_margins", "revenue_growth", "earnings_growth",
                                  "forward_pe", "analyst_target_mean", "analyst_count",
                                  "analyst_recommendation", "analyst_recommendation_mean",
                                  "payout_ratio", "fifty_day_avg", "two_hundred_day_avg"):
                        if yahoo_data.get(field) is not None:
                            fundamentals[field] = yahoo_data[field]

                    yahoo_ok += 1
                    if source == "unavailable":
                        source = "yahoo_fundamentals"
                    elif source == "screener_in":
                        source = "screener_in+yahoo"
                    time.sleep(0.5 + random.uniform(0, 0.3))
                except Exception:
                    logger.debug("Yahoo v10 failed for %s", symbol, exc_info=True)
                    errors += 1

            if not fundamentals:
                continue

            if args.dry_run:
                logger.info("[DRY-RUN] %s: %d fields from %s", symbol, len(fundamentals), source)
            else:
                # Build SET clause dynamically
                set_parts = []
                values = []
                param_idx = 2  # $1 = symbol
                for key, val in fundamentals.items():
                    if key.startswith("_"):
                        continue
                    set_parts.append(f"{key} = ${param_idx}")
                    values.append(val)
                    param_idx += 1

                if set_parts:
                    set_parts.append(f"primary_source = ${param_idx}")
                    values.append(source)
                    param_idx += 1

                    sql = f"UPDATE discover_stock_snapshots SET {', '.join(set_parts)} WHERE symbol = $1"
                    await pool.execute(sql, symbol, *values)
                    total_updated += 1

            if i % LOG_EVERY == 0:
                logger.info(
                    "Progress: %d/%d (updated=%d, screener=%d, yahoo=%d, errors=%d)",
                    i, len(symbol_list), total_updated, screener_ok, yahoo_ok, errors,
                )

            # Batch cooldown
            if i % BATCH_SIZE == 0:
                logger.info("Batch cooldown: pausing %ds after %d symbols.", BATCH_COOLDOWN, i)
                await asyncio.sleep(BATCH_COOLDOWN)

        logger.info(
            "Backfill complete: %d symbols processed, %d updated, "
            "%d screener hits, %d yahoo hits, %d errors.",
            len(symbol_list), total_updated, screener_ok, yahoo_ok, errors,
        )
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
