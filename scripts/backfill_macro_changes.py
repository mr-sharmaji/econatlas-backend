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
from datetime import datetime, timezone

import requests

# Ensure backend root is on import path when run via `python scripts/...`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import close_pool, get_pool, init_pool
from app.scheduler.trading_economics_scraper import TradingEconomicsScraper
from app.services import macro_service
from scripts.backfill_fred_history import BOND_SERIES, SERIES, backfill_series

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_macro_changes")

WORLD_BANK_URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
WORLD_BANK_BACKFILL = (
    ("IN", "unemployment", "SL.UEM.TOTL.ZS", "percent"),
)

# Keep expectations country-aware. This avoids false "missing baseline" warnings
# for combinations we do not actually ingest (e.g. US/gst_collection).
TARGET_INDICATORS_BY_COUNTRY = {
    "IN": (
        "gdp_growth",
        "inflation",
        "repo_rate",
        "unemployment",
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
        "gst_collection",
    ),
    "US": (
        "gdp_growth",
        "inflation",
        "core_inflation",
        "repo_rate",
        "unemployment",
        "iip",
        "pmi_manufacturing",
        "pmi_services",
        "trade_balance",
        "bond_yield_10y",
        "bond_yield_2y",
    ),
}
TARGET_COUNTRIES = tuple(TARGET_INDICATORS_BY_COUNTRY.keys())
TARGET_INDICATORS = tuple(
    sorted(
        {
            indicator
            for indicators in TARGET_INDICATORS_BY_COUNTRY.values()
            for indicator in indicators
        }
    )
)


def _is_target(country: str, indicator: str) -> bool:
    return indicator in TARGET_INDICATORS_BY_COUNTRY.get(country, ())


async def _backfill_fred_history(pool, dry_run: bool) -> int:
    total = 0
    for indicator_name, country, series_id, transform, unit in SERIES + BOND_SERIES:
        if country not in TARGET_COUNTRIES or not _is_target(country, indicator_name):
            continue
        # Deprecated proxy series now returns 404 from FRED; unemployment backfill
        # is handled via World Bank below.
        if series_id == "SL.UEM.TOTL.NE.ZS":
            logger.info(
                "Skipping deprecated FRED proxy series %s for %s/%s",
                series_id,
                country,
                indicator_name,
            )
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


def _fetch_world_bank_series(
    *,
    country: str,
    indicator_code: str,
) -> list[tuple[datetime, float]]:
    response = requests.get(
        WORLD_BANK_URL.format(country=country.lower(), indicator=indicator_code),
        params={"format": "json", "per_page": 80},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        return []

    rows: list[tuple[datetime, float]] = []
    for rec in payload[1]:
        year = rec.get("date")
        value = rec.get("value")
        if year is None or value is None:
            continue
        try:
            ts = datetime(int(str(year)), 1, 1, tzinfo=timezone.utc)
            fval = float(value)
        except (TypeError, ValueError):
            continue
        rows.append((ts, fval))
    rows.sort(key=lambda item: item[0])
    return rows


async def _backfill_world_bank_history(dry_run: bool) -> int:
    total = 0
    for country, indicator_name, wb_indicator_code, unit in WORLD_BANK_BACKFILL:
        if not _is_target(country, indicator_name):
            continue
        try:
            rows = _fetch_world_bank_series(
                country=country,
                indicator_code=wb_indicator_code,
            )
        except Exception:
            logger.exception(
                "World Bank fetch failed for %s/%s (%s)",
                country,
                indicator_name,
                wb_indicator_code,
            )
            continue

        if dry_run:
            logger.info(
                "[DRY RUN] World Bank rows for %s/%s: %d",
                country,
                indicator_name,
                len(rows),
            )
            total += len(rows)
            continue

        payload = [
            {
                "indicator_name": indicator_name,
                "value": value,
                "country": country,
                "timestamp": ts.isoformat(),
                "unit": unit,
                "source": "world_bank",
            }
            for ts, value in rows
        ]
        upserted = await macro_service.insert_indicators_batch_upsert_source_timestamp(payload)
        total += upserted
        logger.info(
            "World Bank rows upserted for %s/%s: %d",
            country,
            indicator_name,
            upserted,
        )
    logger.info("World Bank backfill rows: %d", total)
    return total


async def _backfill_te_snapshot(dry_run: bool) -> int:
    scraper = TradingEconomicsScraper()
    items = scraper.fetch_all()
    filtered = [
        item
        for item in items
        if item.get("country") in TARGET_COUNTRIES
        and _is_target(str(item.get("country")), str(item.get("indicator_name")))
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
        for indicator in TARGET_INDICATORS_BY_COUNTRY.get(country, ()):
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
        await _backfill_world_bank_history(dry_run=dry_run)
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
