#!/usr/bin/env python3
"""Backfill macro indicators from all sources.

Usage:
    python scripts/backfill_macro.py [--source fred|te|imf|calendar|all]

Runs all scrapers and pushes data directly to the DB.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_macro")


async def backfill_fred():
    """Backfill FRED indicators (GDP, inflation, unemployment, repo/fed rate, core CPI)."""
    from app.scheduler.macro_job import MacroScraper
    scraper = MacroScraper()
    items = scraper.fetch_all()
    logger.info("FRED + NSE: %d items fetched", len(items))

    from app.services import macro_service
    if items:
        await macro_service.insert_indicators_batch_upsert_source_timestamp(items)
        logger.info("FRED: %d items upserted", len(items))


async def backfill_te():
    """Backfill Trading Economics indicators."""
    from app.scheduler.trading_economics_scraper import TradingEconomicsScraper
    scraper = TradingEconomicsScraper()
    items = scraper.fetch_all()
    logger.info("TE: %d items fetched", len(items))

    if items:
        from app.services import macro_service
        await macro_service.insert_indicators_batch_upsert_source_timestamp(items)
        logger.info("TE: %d items upserted", len(items))


async def backfill_imf():
    """Backfill IMF WEO forecasts."""
    from app.scheduler.imf_weo_scraper import _fetch_imf_forecasts_sync
    from app.services import macro_service

    forecasts = _fetch_imf_forecasts_sync()
    logger.info("IMF: %d forecasts fetched", len(forecasts))

    if forecasts:
        count = await macro_service.upsert_forecasts(forecasts)
        logger.info("IMF: %d forecasts upserted", count)


async def backfill_calendar():
    """Backfill economic calendar events."""
    from app.scheduler.econ_calendar import _scrape_all_calendars_sync
    from app.services import macro_service

    events = _scrape_all_calendars_sync()
    logger.info("Calendar: %d events scraped", len(events))

    if events:
        count = await macro_service.upsert_calendar_events(events)
        logger.info("Calendar: %d events upserted", count)


async def main(source: str):
    # Initialize DB pool
    from app.core.database import init_pool, close_pool
    await init_pool()

    try:
        if source in ("fred", "all"):
            await backfill_fred()
        if source in ("te", "all"):
            await backfill_te()
        if source in ("imf", "all"):
            await backfill_imf()
        if source in ("calendar", "all"):
            await backfill_calendar()
    finally:
        await close_pool()

    logger.info("Backfill complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill macro data")
    parser.add_argument("--source", choices=["fred", "te", "imf", "calendar", "all"], default="all")
    args = parser.parse_args()
    asyncio.run(main(args.source))
