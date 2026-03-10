"""Backfill FII/DII macro indicators for chart history.

Data sources:
1) NSE live endpoint (/api/fiidiiTradeNse) for latest session.
2) Optional CSV files (recommended for historical backfill depth).

Usage:
  python scripts/backfill_fii_dii.py --dry-run
  python scripts/backfill_fii_dii.py --csv /path/to/historical_fii_dii.csv
  python scripts/backfill_fii_dii.py --csv file1.csv --csv file2.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import re
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import requests

# Ensure backend root is on path when run as: python scripts/backfill_fii_dii.py
_backend_root = Path(__file__).resolve().parent.parent
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from app.core.database import close_pool, init_pool
from app.services import macro_service

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("fii_dii_backfill")

_IST = timezone(timedelta(hours=5, minutes=30))
_NSE_BASE = "https://www.nseindia.com"
_NSE_API = "https://www.nseindia.com/api/fiidiiTradeNse"


def _to_float(value: object | None) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").replace("₹", "").strip()
    if not text:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _parse_date(value: object | None) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y", "%d-%B-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_category(category: object | None) -> str | None:
    label = str(category or "").strip().lower()
    if not label:
        return None
    if "dii" in label or "domestic" in label:
        return "dii_net_cash"
    if "fii" in label or "fpi" in label or "foreign" in label:
        return "fii_net_cash"
    return None


def _record_to_row(raw: dict, source: str) -> dict | None:
    indicator = _normalize_category(raw.get("category"))
    if indicator is None:
        return None
    d = _parse_date(raw.get("date"))
    if d is None:
        return None

    net = _to_float(raw.get("netValue"))
    if net is None:
        buy = _to_float(raw.get("buyValue"))
        sell = _to_float(raw.get("sellValue"))
        if buy is None or sell is None:
            return None
        net = round(buy - sell, 2)

    ts_local = datetime.combine(d, time(15, 30), tzinfo=_IST)
    ts_utc = ts_local.astimezone(timezone.utc)
    return {
        "indicator_name": indicator,
        "value": round(net, 2),
        "country": "IN",
        "timestamp": ts_utc.isoformat(),
        "unit": "inr_cr",
        "source": source,
    }


def _fetch_live_rows() -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{_NSE_BASE}/reports/fii-dii",
    }
    session = requests.Session()
    session.get(_NSE_BASE, headers=headers, timeout=20)
    response = session.get(_NSE_API, headers=headers, timeout=20)
    response.raise_for_status()
    payload = response.json()
    rows = payload if isinstance(payload, list) else payload.get("data") or payload.get("rows") or []
    if not isinstance(rows, list):
        return []
    out: list[dict] = []
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row = _record_to_row(raw, source="nse_fiidii_api_backfill")
        if row is not None:
            out.append(row)
    return out


def _find_key(row: dict, token: str) -> str | None:
    token_l = token.lower()
    for key in row.keys():
        if token_l in str(key).lower():
            return key
    return None


def _read_csv_rows(path: Path) -> list[dict]:
    out: list[dict] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            if not isinstance(raw, dict):
                continue
            category_key = _find_key(raw, "category")
            date_key = _find_key(raw, "date")
            net_key = _find_key(raw, "net")
            buy_key = _find_key(raw, "buy")
            sell_key = _find_key(raw, "sell")
            if category_key is None or date_key is None:
                continue
            normalized = {
                "category": raw.get(category_key),
                "date": raw.get(date_key),
                "netValue": raw.get(net_key) if net_key else None,
                "buyValue": raw.get(buy_key) if buy_key else None,
                "sellValue": raw.get(sell_key) if sell_key else None,
            }
            row = _record_to_row(normalized, source="nse_fiidii_csv_backfill")
            if row is not None:
                out.append(row)
    return out


def _dedupe(rows: Iterable[dict]) -> list[dict]:
    best: dict[tuple[str, str], dict] = {}
    for row in rows:
        indicator = str(row.get("indicator_name") or "")
        ts = str(row.get("timestamp") or "")
        if not indicator or not ts:
            continue
        key = (indicator, ts)
        existing = best.get(key)
        if existing is None:
            best[key] = row
            continue
        # Prefer CSV source for historical correction when same key collides.
        if str(row.get("source")).endswith("_csv_backfill"):
            best[key] = row
    out = list(best.values())
    out.sort(key=lambda r: (r["indicator_name"], r["timestamp"]))
    return out


def summarize(rows: list[dict]) -> str:
    if not rows:
        return "rows=0"
    by_indicator = Counter(r["indicator_name"] for r in rows)
    timestamps = sorted(str(r["timestamp"]) for r in rows)
    return (
        f"rows={len(rows)} by_indicator={dict(by_indicator)} "
        f"range={timestamps[0]}..{timestamps[-1]}"
    )


async def upsert_rows(rows: list[dict]) -> int:
    await init_pool()
    try:
        return await macro_service.insert_indicators_batch_upsert_source_timestamp(rows)
    finally:
        await close_pool()


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill FII/DII macro history for overview charts from NSE live endpoint "
            "and optional historical CSV files."
        ),
    )
    parser.add_argument(
        "--csv",
        action="append",
        default=[],
        help="Path to historical CSV exported from NSE report page. Can be used multiple times.",
    )
    parser.add_argument(
        "--skip-live",
        action="store_true",
        help="Skip live NSE API pull and use CSV inputs only.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch/parse only; do not write to database.",
    )
    args = parser.parse_args()

    collected: list[dict] = []
    if not args.skip_live:
        try:
            live_rows = _fetch_live_rows()
            logger.info("Fetched live rows: %d", len(live_rows))
            collected.extend(live_rows)
        except Exception:
            logger.exception("Failed to fetch live FII/DII rows from NSE")

    for csv_path in args.csv:
        path = Path(csv_path).expanduser().resolve()
        if not path.exists():
            logger.warning("CSV not found, skipping: %s", path)
            continue
        parsed = _read_csv_rows(path)
        logger.info("Parsed CSV rows: %d from %s", len(parsed), path)
        collected.extend(parsed)

    rows = _dedupe(collected)
    logger.info("Prepared rows: %s", summarize(rows))
    if args.dry_run:
        logger.info("Dry-run only: no database writes.")
        if len(rows) <= 2 and not args.csv:
            logger.warning(
                "Only live snapshot rows detected. Provide --csv files for deeper historical chart backfill."
            )
        return

    inserted = await upsert_rows(rows)
    logger.info("Backfill complete: upserted=%d rows", inserted)


if __name__ == "__main__":
    asyncio.run(main())
