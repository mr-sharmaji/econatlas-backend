"""Auto-recovery gap backfill: detect missing daily market_prices rows and
backfill from Yahoo Finance on server startup.

Only processes assets from the asset_catalog that have a Yahoo-compatible
symbol (no TE:* or IM:* prefixes, no bond/FRED symbols). Skips weekends
and holidays when checking for gaps. Uses ON CONFLICT DO NOTHING.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import UTC, datetime, timedelta

import requests

from app.core.asset_catalog import ASSET_CATALOG, AssetCatalogItem
from app.core.database import get_pool
from app.scheduler.base import BaseScraper

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

# Symbols Yahoo does not support for INR cross rates
_UNSUPPORTED_YAHOO_FX = {"SARINR=X", "MXNINR=X"}

# Yahoo reports these commodity symbols in USX (cents) — divide by 100
_USX_SYMBOLS = {"ZW=F", "ZC=F", "ZS=F", "ZO=F", "CT=F", "SB=F", "KC=F"}

# Instrument types to backfill (daily close only — no intraday)
_BACKFILLABLE_TYPES = {"index", "currency", "commodity", "crypto"}


def _is_yahoo_symbol(item: AssetCatalogItem) -> bool:
    """True if the asset uses a Yahoo Finance-compatible symbol."""
    sym = item.symbol
    # Exclude non-Yahoo sources (TE:*, IM:*, FRED series IDs for bonds)
    if sym.startswith("TE:") or sym.startswith("IM:"):
        return False
    # Bond yields use FRED series IDs — not Yahoo-compatible
    if item.instrument_type == "bond_yield":
        return False
    # Predictive indices (e.g. Gift Nifty) have no Yahoo history
    if item.session_policy == "predictive":
        return False
    if sym in _UNSUPPORTED_YAHOO_FX:
        return False
    return True


def _last_trading_day(today: datetime) -> datetime:
    """Return the most recent weekday at midnight UTC (could be today if weekday)."""
    d = today.replace(hour=0, minute=0, second=0, microsecond=0)
    # If it's Saturday (5) or Sunday (6), go back
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _is_gap(latest_date: datetime, expected_latest: datetime) -> bool:
    """True if latest_date is more than 1 trading day behind expected_latest.

    Accounts for weekends: if today is Monday and latest is Friday, that is NOT a gap.
    """
    if latest_date >= expected_latest:
        return False
    # Walk from latest_date forward, counting business days
    biz_days = 0
    d = latest_date + timedelta(days=1)
    while d <= expected_latest:
        if d.weekday() < 5:  # Mon-Fri
            biz_days += 1
        d += timedelta(days=1)
    # Allow 1 business day grace (today's close may not be in yet)
    return biz_days > 1


class GapBackfiller(BaseScraper):
    """Lightweight Yahoo fetcher for gap periods only."""

    def fetch_series(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
    ) -> list[tuple[datetime, float]]:
        """Fetch daily close prices from Yahoo for a date range."""
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": "1d",
            "events": "history",
            "includePrePost": "false",
        }
        try:
            payload = self._get_json(
                YAHOO_CHART_URL.format(symbol=symbol), params=params
            )
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code in {400, 404}:
                logger.warning("Yahoo returned %d for %s — skipping", e.response.status_code, symbol)
                return []
            raise
        except Exception:
            logger.exception("Yahoo fetch failed for %s", symbol)
            return []

        result = payload.get("chart", {}).get("result", [])
        if not result:
            return []

        block = result[0]
        timestamps = block.get("timestamp", []) or []
        closes = (
            ((block.get("indicators", {}) or {}).get("quote", []) or [{}])[0]
        ).get("close", []) or []

        out: list[tuple[datetime, float]] = []
        for idx, epoch in enumerate(timestamps):
            if idx >= len(closes):
                break
            close = closes[idx]
            if close is None:
                continue
            value = float(close)
            if not math.isfinite(value) or value <= 0:
                continue
            # Normalize to midnight UTC
            ts = datetime.fromtimestamp(int(epoch), tz=UTC).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            out.append((ts, value))
        return out


async def run_gap_backfill_job() -> None:
    """Scan all backfillable assets for data gaps and fill from Yahoo."""
    logger.info("Gap backfill: starting scan...")
    pool = await get_pool()

    # Filter to Yahoo-backfillable assets only
    assets = [
        item
        for item in ASSET_CATALOG
        if item.instrument_type in _BACKFILLABLE_TYPES and _is_yahoo_symbol(item)
    ]
    logger.info("Gap backfill: checking %d assets", len(assets))

    now = datetime.now(UTC)
    expected_latest = _last_trading_day(now)

    fetcher = GapBackfiller()
    total_inserted = 0
    total_gaps = 0

    for item in assets:
        # Query the latest date for this asset
        row = await pool.fetchrow(
            """
            SELECT MAX("timestamp") AS latest_ts
            FROM market_prices
            WHERE asset = $1 AND instrument_type = $2
            """,
            item.asset,
            item.instrument_type,
        )
        latest_ts = row["latest_ts"] if row else None

        if latest_ts is None:
            # No data at all — treat as a full-seed: start from 5 years ago.
            logger.info("Gap backfill: FULL SEED needed for %s (no existing data)", item.asset)
            latest_ts = now - timedelta(days=1825)  # 5 years

        # Ensure timezone-aware
        if latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=UTC)

        # Normalize to midnight
        latest_date = latest_ts.replace(hour=0, minute=0, second=0, microsecond=0)

        # Also check if the asset has very few rows (< 90 days of data):
        # treat it as a seed by pushing the gap start back to 5 years ago
        # so the chart has enough history for the 1Y/3Y/5Y views.
        row_count = await pool.fetchval(
            "SELECT COUNT(*) FROM market_prices WHERE asset = $1 AND instrument_type = $2",
            item.asset,
            item.instrument_type,
        )
        if row_count < 90:
            seed_start = now - timedelta(days=1825)
            if seed_start < latest_date:
                logger.info(
                    "Gap backfill: LOW DATA SEED for %s — only %d rows, seeding from %s",
                    item.asset,
                    row_count,
                    seed_start.date(),
                )
                latest_date = seed_start

        if not _is_gap(latest_date, expected_latest):
            continue

        total_gaps += 1
        gap_start = latest_date + timedelta(days=1)
        gap_end = now
        logger.info(
            "Gap backfill: %s (%s) — gap from %s to %s",
            item.asset,
            item.symbol,
            gap_start.date(),
            gap_end.date(),
        )

        # Fetch from Yahoo
        points = fetcher.fetch_series(item.symbol, gap_start, gap_end)
        if not points:
            logger.warning("Gap backfill: no data returned for %s", item.asset)
            time.sleep(0.3)
            continue

        # Apply USX divisor for grain/soft commodities
        usx_divisor = 100.0 if item.symbol in _USX_SYMBOLS else 1.0

        # Insert rows
        inserted = 0
        # Sort by timestamp to compute change_percent
        points.sort(key=lambda p: p[0])

        # Get the previous close from existing data
        prev_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices
            WHERE asset = $1 AND instrument_type = $2
            ORDER BY "timestamp" DESC LIMIT 1
            """,
            item.asset,
            item.instrument_type,
        )
        prev_close = float(prev_row["price"]) if prev_row else None

        for ts, raw_price in points:
            price = raw_price / usx_divisor
            change_pct = None
            if prev_close is not None and prev_close > 0:
                change_pct = round(((price - prev_close) / prev_close) * 100, 2)

            result = await pool.execute(
                """
                INSERT INTO market_prices
                    (asset, price, "timestamp", source, instrument_type, unit,
                     change_percent, previous_close)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (asset, instrument_type, "timestamp") DO NOTHING
                """,
                item.asset,
                price,
                ts,
                "yahoo_gap_backfill",
                item.instrument_type,
                item.unit,
                change_pct,
                prev_close,
            )
            if result and result.endswith("1"):
                inserted += 1
            prev_close = price

        total_inserted += inserted
        logger.info(
            "Gap backfill: %s — inserted %d rows (of %d fetched)",
            item.asset,
            inserted,
            len(points),
        )

        # Rate limit: 0.3s between Yahoo API calls
        time.sleep(0.3)

    logger.info(
        "Gap backfill complete: %d gaps detected, %d rows inserted across %d assets",
        total_gaps,
        total_inserted,
        len(assets),
    )
