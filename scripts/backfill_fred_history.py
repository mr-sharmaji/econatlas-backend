#!/usr/bin/env python3
"""Backfill full FRED historical data for all macro indicators.

Downloads complete CSV history from FRED and upserts into macro_indicators.
This gives us decades of data for charts and trend analysis.

Usage:
    python scripts/backfill_fred_history.py [--dry-run]
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("backfill_fred")

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# All FRED series to backfill: (indicator_name, country, series_id, transform)
# transform: "direct" = use value as-is, "yoy" = compute year-over-year % change
SERIES: list[tuple[str, str, str, str, str]] = [
    # US
    ("gdp_growth", "US", "A191RL1Q225SBEA", "direct", "percent"),
    ("unemployment", "US", "UNRATE", "direct", "percent"),
    ("repo_rate", "US", "FEDFUNDS", "direct", "percent"),
    ("inflation", "US", "CPIAUCSL", "yoy", "percent_yoy"),
    ("core_inflation", "US", "CPILFESL", "yoy", "percent_yoy"),
    ("iip", "US", "INDPRO", "yoy", "percent_yoy"),
    # India
    ("gdp_growth", "IN", "INDGDPRQPSMEI", "direct", "percent"),
    ("repo_rate", "IN", "IRSTCI01INM156N", "direct", "percent"),
    ("inflation", "IN", "INDCPIALLMINMEI", "yoy", "percent_yoy"),
    ("unemployment", "IN", "SL.UEM.TOTL.NE.ZS", "direct", "percent"),  # World Bank via FRED proxy
]

# Bond yields from market_prices (separate backfill)
BOND_SERIES = [
    ("bond_yield_10y", "IN", "INDIRLTLT01STM", "direct", "percent"),
    ("bond_yield_10y", "US", "DGS10", "direct", "percent"),
    ("bond_yield_2y", "US", "DGS2", "direct", "percent"),
]


def fetch_fred_csv(series_id: str) -> list[tuple[datetime, float]]:
    """Fetch full history from FRED CSV endpoint."""
    url = f"{FRED_CSV_URL}?id={series_id}"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        logger.warning("FRED %s: HTTP %d", series_id, resp.status_code)
        return []

    reader = csv.DictReader(io.StringIO(resp.text))
    rows = []
    for row in reader:
        date_str = row.get("observation_date") or row.get("DATE")
        val_str = row.get(series_id, ".")
        if not date_str or val_str in (".", ""):
            continue
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            val = float(val_str)
            rows.append((dt, val))
        except (ValueError, TypeError):
            continue

    logger.info("FRED %s: %d rows fetched (%s to %s)",
                series_id, len(rows),
                rows[0][0].date() if rows else "?",
                rows[-1][0].date() if rows else "?")
    return rows


def compute_yoy(rows: list[tuple[datetime, float]]) -> list[tuple[datetime, float]]:
    """Compute year-over-year percentage change from level data."""
    if len(rows) < 13:
        return []
    result = []
    for i in range(12, len(rows)):
        current = rows[i]
        year_ago = rows[i - 12]
        if year_ago[1] != 0:
            pct = ((current[1] - year_ago[1]) / year_ago[1]) * 100
            result.append((current[0], round(pct, 4)))
    return result


async def backfill_series(pool, indicator_name: str, country: str, series_id: str,
                          transform: str, unit: str, dry_run: bool) -> int:
    """Fetch and upsert one series."""
    rows = fetch_fred_csv(series_id)
    if not rows:
        return 0

    if transform == "yoy":
        rows = compute_yoy(rows)
        if not rows:
            logger.warning("%s/%s: not enough data for YoY", country, indicator_name)
            return 0

    if dry_run:
        logger.info("[DRY RUN] Would upsert %d rows for %s/%s", len(rows), country, indicator_name)
        return len(rows)

    count = 0
    async with pool.acquire() as conn:
        for dt, val in rows:
            await conn.execute(
                """
                INSERT INTO macro_indicators (indicator_name, value, country, timestamp, unit, source)
                VALUES ($1, $2, $3, $4, $5, 'fred_api')
                ON CONFLICT (indicator_name, country, timestamp)
                DO UPDATE SET value = EXCLUDED.value, unit = EXCLUDED.unit, source = EXCLUDED.source
                """,
                indicator_name, val, country, dt, unit,
            )
            count += 1

    logger.info("Upserted %d rows for %s/%s", count, country, indicator_name)
    return count


async def backfill_bond_yields(pool, dry_run: bool) -> int:
    """Copy bond yield history from market_prices to macro_indicators."""
    # Map asset names to indicator names
    mapping = {
        "India 10Y Bond Yield": ("bond_yield_10y", "IN"),
        "US 10Y Treasury Yield": ("bond_yield_10y", "US"),
        "US 2Y Treasury Yield": ("bond_yield_2y", "US"),
    }

    total = 0
    for asset_name, (indicator_name, country) in mapping.items():
        rows = await pool.fetch(
            "SELECT price, timestamp FROM market_prices WHERE asset = $1 ORDER BY timestamp",
            asset_name,
        )
        if not rows:
            logger.warning("No market_prices data for %s", asset_name)
            continue

        if dry_run:
            logger.info("[DRY RUN] Would copy %d bond yield rows for %s/%s", len(rows), country, indicator_name)
            total += len(rows)
            continue

        async with pool.acquire() as conn:
            for row in rows:
                await conn.execute(
                    """
                    INSERT INTO macro_indicators (indicator_name, value, country, timestamp, unit, source)
                    VALUES ($1, $2, $3, $4, 'percent', 'market_prices')
                    ON CONFLICT (indicator_name, country, timestamp)
                    DO UPDATE SET value = EXCLUDED.value
                    """,
                    indicator_name, float(row["price"]), country, row["timestamp"],
                )
                total += 1

        logger.info("Copied %d bond yield rows for %s/%s", len(rows), country, indicator_name)

    return total


async def main(dry_run: bool):
    from app.core.database import init_pool, close_pool, get_pool
    await init_pool()
    pool = await get_pool()

    total = 0

    # FRED macro indicators
    for indicator_name, country, series_id, transform, unit in SERIES:
        count = await backfill_series(pool, indicator_name, country, series_id, transform, unit, dry_run)
        total += count
        time.sleep(0.5)  # rate limit FRED

    # FRED bond yields
    for indicator_name, country, series_id, transform, unit in BOND_SERIES:
        count = await backfill_series(pool, indicator_name, country, series_id, transform, unit, dry_run)
        total += count
        time.sleep(0.5)

    # Copy bond yields from market_prices
    total += await backfill_bond_yields(pool, dry_run)

    await close_pool()
    logger.info("Backfill complete: %d total rows %s", total, "(dry run)" if dry_run else "upserted")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill FRED historical macro data")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    args = parser.parse_args()
    asyncio.run(main(args.dry_run))
