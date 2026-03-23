#!/usr/bin/env python3
"""Backfill macro history used by Economy-page change labels.

This script focuses on IN/US indicators shown on the compact Economy page and
reports which indicators still have fewer than 2 data points (where change
labels would appear as "No prior release").

Usage:
    python scripts/backfill_macro_changes.py
    python scripts/backfill_macro_changes.py --dry-run
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

# Ensure backend root is on import path when run via `python scripts/...`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import close_pool, get_pool, init_pool
from app.scheduler.trading_economics_scraper import TradingEconomicsScraper
from app.services import macro_service
from scripts.backfill_fred_history import BOND_SERIES, SERIES, backfill_series

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_macro_changes")

TARGET_COUNTRIES = ("IN", "US")
TARGET_INDICATORS = (
    "gdp_growth",
    "inflation",
    "repo_rate",
    "unemployment",
    "core_inflation",
    "food_inflation",
    "iip",
    "pmi_manufacturing",
    "pmi_services",
    "bank_credit_growth",
    "trade_balance",
    "current_account_deficit",
    "fiscal_deficit",
    "forex_reserves",
    "bond_yield_10y",
    "bond_yield_2y",
    "gst_collection",
)


async def _backfill_fred_history(pool, dry_run: bool) -> int:
    total = 0
    for indicator_name, country, series_id, transform, unit in SERIES + BOND_SERIES:
        if country not in TARGET_COUNTRIES:
            continue
        if indicator_name not in TARGET_INDICATORS:
            continue
        inserted = await backfill_series(
            pool,
            indicator_name=indicator_name,
            country=country,
            series_id=series_id,
            transform=transform,
            unit=unit,
            dry_run=dry_run,
        )
        total += inserted
    logger.info("FRED backfill rows: %d", total)
    return total


async def _backfill_te_snapshot(dry_run: bool) -> int:
    scraper = TradingEconomicsScraper()
    items = scraper.fetch_all()
    filtered = [
        item
        for item in items
        if item.get("country") in TARGET_COUNTRIES
        and item.get("indicator_name") in TARGET_INDICATORS
    ]
    if dry_run:
        logger.info("[DRY RUN] TE rows to upsert: %d", len(filtered))
        return len(filtered)
    if not filtered:
        logger.info("No TE rows fetched for target indicators.")
        return 0
    upserted = await macro_service.insert_indicators_batch_upsert_source_timestamp(filtered)
    logger.info("TE rows upserted: %d", upserted)
    return upserted


async def _report_coverage(pool) -> None:
    rows = await pool.fetch(
        """
        SELECT country, indicator_name, COUNT(*)::int AS row_count
        FROM macro_indicators
        WHERE country = ANY($1::text[])
          AND indicator_name = ANY($2::text[])
        GROUP BY country, indicator_name
        ORDER BY country, indicator_name
        """,
        list(TARGET_COUNTRIES),
        list(TARGET_INDICATORS),
    )
    count_map = {(r["country"], r["indicator_name"]): int(r["row_count"]) for r in rows}

    lacking = []
    for country in TARGET_COUNTRIES:
        for indicator in TARGET_INDICATORS:
            row_count = count_map.get((country, indicator), 0)
            if row_count < 2:
                lacking.append((country, indicator, row_count))

    if not lacking:
        logger.info("All target indicators have at least 2 rows for IN/US.")
        return

    logger.warning("Indicators still lacking change baseline (<2 rows): %d", len(lacking))
    for country, indicator, row_count in lacking:
        logger.warning("  %s/%s -> %d row(s)", country, indicator, row_count)


async def main(dry_run: bool) -> None:
    await init_pool()
    try:
        pool = await get_pool()
        await _backfill_fred_history(pool, dry_run=dry_run)
        await _backfill_te_snapshot(dry_run=dry_run)
        await _report_coverage(pool)
    finally:
        await close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill IN/US macro history used for Economy-page change labels.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and report only; do not write to DB.",
    )
    args = parser.parse_args()
    asyncio.run(main(dry_run=bool(args.dry_run)))
