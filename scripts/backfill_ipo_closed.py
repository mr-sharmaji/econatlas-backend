"""Backfill IPO snapshots (especially closed/listed entries) from InvestorGain feeds.

Usage:
  python scripts/backfill_ipo_closed.py --months 12 --dry-run
  python scripts/backfill_ipo_closed.py --months 12
  python scripts/backfill_ipo_closed.py --months 12 --include-active --keep-days 14
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

# Ensure backend root is on path when run as: python scripts/backfill_ipo_closed.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from app.core.database import close_pool, init_pool
from app.services import ipo_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("ipo_backfill")

_IST = timezone(timedelta(hours=5, minutes=30))


def _month_anchors(months: int) -> list[date]:
    if months < 1:
        return []
    today = datetime.now(_IST).date()
    first_of_this_month = today.replace(day=1)
    anchors: list[date] = []
    base = first_of_this_month.year * 12 + (first_of_this_month.month - 1)
    for offset in range(months):
        x = base - offset
        year = x // 12
        month = (x % 12) + 1
        anchors.append(date(year, month, 1))
    return anchors


def _status_priority(status: str) -> int:
    s = (status or "").lower()
    if s == "open":
        return 3
    if s == "upcoming":
        return 2
    if s == "closed":
        return 1
    return 0


def _merge_row(existing: dict | None, candidate: dict) -> dict:
    if existing is None:
        return candidate
    a_ts = existing.get("source_timestamp")
    b_ts = candidate.get("source_timestamp")
    if isinstance(a_ts, datetime) and isinstance(b_ts, datetime):
        if b_ts > a_ts:
            return candidate
        if b_ts < a_ts:
            return existing
    if _status_priority(str(candidate.get("status"))) > _status_priority(
        str(existing.get("status"))
    ):
        return candidate
    return existing


def _source_plan(include_active: bool) -> list[tuple[str, str]]:
    plan: list[tuple[str, str]] = [
        ("close", "closed"),
        ("listed", "closed"),
    ]
    if include_active:
        plan.extend(
            [
                ("open", "open"),
                ("current", "upcoming"),
            ]
        )
    return plan


def _normalize_row(row: dict) -> dict:
    out = dict(row)
    status = str(out.get("status") or "closed").lower()
    out["status"] = status
    listing_price = out.get("listing_price")
    if listing_price is None:
        out["listing_gain_pct"] = None
    else:
        out["listing_gain_pct"] = ipo_service._listing_gain_pct(
            listing_price=listing_price,
            price_band=out.get("price_band"),
        )

    if status == "closed":
        out["outcome_state"] = (
            "listed" if out.get("listing_price") is not None else "awaiting_listing_data"
        )
    else:
        out["outcome_state"] = None
    out["source"] = out.get("source") or "investorgain_webnode_backfill"
    return out


def collect_rows(*, months: int, include_active: bool) -> list[dict]:
    merged: dict[str, dict] = {}
    anchors = _month_anchors(months)
    plan = _source_plan(include_active=include_active)
    for anchor in anchors:
        for source_code, status in plan:
            try:
                rows = ipo_service._fetch_rows_for_status(
                    source_code=source_code,
                    status=status,
                    today_ist=anchor,
                )
            except Exception:
                logger.exception(
                    "Fetch failed for source=%s month=%04d-%02d",
                    source_code,
                    anchor.year,
                    anchor.month,
                )
                continue
            for row in rows:
                symbol = str(row.get("symbol") or "").strip()
                if not symbol:
                    continue
                normalized = _normalize_row(row)
                merged[symbol] = _merge_row(merged.get(symbol), normalized)
    return list(merged.values())


def summarize(rows: Iterable[dict]) -> str:
    rows_list = list(rows)
    by_status = Counter(str(r.get("status") or "unknown") for r in rows_list)
    listed = sum(1 for r in rows_list if r.get("listing_price") is not None)
    with_listing_date = sum(1 for r in rows_list if r.get("listing_date") is not None)
    return (
        f"rows={len(rows_list)} "
        f"status={dict(by_status)} "
        f"with_listing_price={listed} "
        f"with_listing_date={with_listing_date}"
    )


async def upsert_rows(rows: list[dict], *, keep_days: int) -> tuple[int, int]:
    await init_pool()
    from app.core.database import get_pool

    pool = await get_pool()
    today_ist = datetime.now(_IST).date()
    archived = 0
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO ipo_snapshots
                    (symbol, company_name, market, status, ipo_type, issue_size_cr, price_band,
                     gmp_percent, subscription_multiple, listing_price, listing_gain_pct, outcome_state,
                     open_date, close_date, listing_date, source_timestamp, ingested_at, archived_at, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, NOW(), NULL, $17)
                    ON CONFLICT (symbol)
                    DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        market = EXCLUDED.market,
                        status = EXCLUDED.status,
                        ipo_type = EXCLUDED.ipo_type,
                        issue_size_cr = EXCLUDED.issue_size_cr,
                        price_band = EXCLUDED.price_band,
                        gmp_percent = EXCLUDED.gmp_percent,
                        subscription_multiple = EXCLUDED.subscription_multiple,
                        listing_price = EXCLUDED.listing_price,
                        listing_gain_pct = EXCLUDED.listing_gain_pct,
                        outcome_state = EXCLUDED.outcome_state,
                        open_date = EXCLUDED.open_date,
                        close_date = EXCLUDED.close_date,
                        listing_date = EXCLUDED.listing_date,
                        source_timestamp = EXCLUDED.source_timestamp,
                        ingested_at = NOW(),
                        archived_at = NULL,
                        source = EXCLUDED.source
                    """,
                    r.get("symbol"),
                    r.get("company_name"),
                    r.get("market") or "IN",
                    r.get("status") or "closed",
                    r.get("ipo_type") or "mainboard",
                    r.get("issue_size_cr"),
                    r.get("price_band"),
                    r.get("gmp_percent"),
                    r.get("subscription_multiple"),
                    r.get("listing_price"),
                    r.get("listing_gain_pct"),
                    r.get("outcome_state"),
                    r.get("open_date"),
                    r.get("close_date"),
                    r.get("listing_date"),
                    r.get("source_timestamp"),
                    r.get("source") or "investorgain_webnode_backfill",
                )

            if keep_days >= 0:
                status = await conn.execute(
                    """
                    UPDATE ipo_snapshots
                    SET archived_at = NOW()
                    WHERE status = 'closed'
                      AND archived_at IS NULL
                      AND COALESCE(close_date, listing_date) IS NOT NULL
                      AND COALESCE(close_date, listing_date) <= ($1::date - $2::int)
                    """,
                    today_ist,
                    keep_days,
                )
                try:
                    archived = int(str(status).split()[-1])
                except Exception:
                    archived = 0

            await conn.execute(
                """
                DELETE FROM device_ipo_alerts
                WHERE symbol NOT IN (
                    SELECT symbol FROM ipo_snapshots WHERE archived_at IS NULL
                )
                """
            )
    await close_pool()
    return len(rows), archived


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill IPO snapshots with close/listed data from InvestorGain feeds.",
    )
    parser.add_argument(
        "--months",
        type=int,
        default=12,
        help="How many months (including current month) to sweep.",
    )
    parser.add_argument(
        "--include-active",
        action="store_true",
        help="Also ingest open/upcoming rows while backfilling closed/listed rows.",
    )
    parser.add_argument(
        "--keep-days",
        type=int,
        default=14,
        help="Soft-archive closed rows older than this many days (default: 14). Use -1 to disable.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and parse only; do not write to database.",
    )
    args = parser.parse_args()

    rows = collect_rows(months=max(1, args.months), include_active=args.include_active)
    logger.info("Collected IPO rows: %s", summarize(rows))
    if args.dry_run:
        logger.info("Dry-run only: no database writes.")
        return

    upserted, archived = await upsert_rows(rows, keep_days=args.keep_days)
    logger.info(
        "Backfill complete: upserted=%d archived=%d",
        upserted,
        archived,
    )


if __name__ == "__main__":
    asyncio.run(main())
