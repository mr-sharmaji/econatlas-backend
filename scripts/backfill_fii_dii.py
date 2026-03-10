"""Backfill FII/DII macro indicators for chart history.

Data sources:
1) NSE live endpoint (/api/fiidiiTradeNse) for latest session.
2) Moneycontrol month-detail endpoint for historical daily cash flows.
3) Optional CSV files for manual correction/enrichment.

Usage:
  python scripts/backfill_fii_dii.py --dry-run
  python scripts/backfill_fii_dii.py --months 2 --dry-run
  python scripts/backfill_fii_dii.py --csv /path/to/historical_fii_dii.csv
  python scripts/backfill_fii_dii.py --csv file1.csv --csv file2.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import html
import logging
import re
import sys
from collections import Counter, defaultdict
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
_MC_BASE = "https://www.moneycontrol.com"
_MC_MONTHLY_DETAIL_API = f"{_MC_BASE}/techmvc/responsive/fiidii/monthly"


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


def _strip_html(value: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", "", value)).strip()


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


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _shift_month(d: date, delta: int) -> date:
    year = d.year + ((d.month - 1 + delta) // 12)
    month = ((d.month - 1 + delta) % 12) + 1
    return date(year, month, 1)


def _iter_recent_months(*, months: int) -> list[tuple[int, int]]:
    today_ist = datetime.now(_IST).date()
    cursor = _month_start(today_ist)
    out: list[tuple[int, int]] = []
    for _ in range(max(0, months)):
        out.append((cursor.year, cursor.month))
        cursor = _shift_month(cursor, -1)
    return out


def _moneycontrol_row_to_rows(date_value: date, fii_net: float, dii_net: float) -> list[dict]:
    ts_local = datetime.combine(date_value, time(15, 30), tzinfo=_IST)
    ts_utc = ts_local.astimezone(timezone.utc).isoformat()
    return [
        {
            "indicator_name": "fii_net_cash",
            "value": round(fii_net, 2),
            "country": "IN",
            "timestamp": ts_utc,
            "unit": "inr_cr",
            "source": "moneycontrol_fiidii_backfill",
        },
        {
            "indicator_name": "dii_net_cash",
            "value": round(dii_net, 2),
            "country": "IN",
            "timestamp": ts_utc,
            "unit": "inr_cr",
            "source": "moneycontrol_fiidii_backfill",
        },
    ]


def _parse_moneycontrol_month_html(content: str) -> list[dict]:
    tbody_match = re.search(r"<tbody[^>]*>(.*?)</tbody>", content, re.IGNORECASE | re.DOTALL)
    if not tbody_match:
        return []
    tbody = tbody_match.group(1)
    rows: list[dict] = []
    for row_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", tbody, re.IGNORECASE | re.DOTALL):
        row_html = row_match.group(1)
        cells = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row_html, re.IGNORECASE | re.DOTALL)
        if len(cells) < 7:
            continue
        date_text = _strip_html(cells[0]).replace("</a", "").strip()
        parsed_date = _parse_date(date_text)
        if parsed_date is None:
            continue
        fii_net = _to_float(_strip_html(cells[3]))
        dii_net = _to_float(_strip_html(cells[6]))
        if fii_net is None or dii_net is None:
            continue
        rows.extend(_moneycontrol_row_to_rows(parsed_date, fii_net, dii_net))
    return rows


def _fetch_moneycontrol_month(year: int, month: int) -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html, */*;q=0.8",
        "Referer": f"{_MC_BASE}/stocks/marketstats/fii_dii_activity/index.php",
    }
    params = {
        "month": f"{month:02d}",
        "year": str(year),
        "section": "cash",
        "sub_section": "",
    }
    response = requests.get(_MC_MONTHLY_DETAIL_API, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    return _parse_moneycontrol_month_html(response.text)


def _fetch_moneycontrol_rows(*, months: int) -> list[dict]:
    out: list[dict] = []
    for year, month in _iter_recent_months(months=months):
        try:
            month_rows = _fetch_moneycontrol_month(year, month)
            logger.info(
                "Fetched Moneycontrol daily rows for %04d-%02d: %d",
                year,
                month,
                len(month_rows),
            )
            out.extend(month_rows)
        except Exception:
            logger.exception("Failed Moneycontrol fetch for %04d-%02d", year, month)
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
        # Prefer more reliable historical sources on collisions.
        existing_source = str(existing.get("source") or "")
        new_source = str(row.get("source") or "")
        if existing_source.endswith("_csv_backfill"):
            continue
        if new_source.endswith("_csv_backfill"):
            best[key] = row
            continue
        if existing_source.startswith("moneycontrol_") and not new_source.startswith("moneycontrol_"):
            continue
        if new_source.startswith("moneycontrol_"):
            best[key] = row
    out = list(best.values())
    out.sort(key=lambda r: (r["indicator_name"], r["timestamp"]))
    return out


def _extract_session_dates(rows: Iterable[dict]) -> dict[str, set[date]]:
    out: dict[str, set[date]] = defaultdict(set)
    for row in rows:
        indicator = str(row.get("indicator_name") or "")
        if indicator not in {"fii_net_cash", "dii_net_cash"}:
            continue
        ts_raw = str(row.get("timestamp") or "")
        if not ts_raw:
            continue
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        out[indicator].add(ts.astimezone(_IST).date())
    return out


def _coverage_summary(rows: Iterable[dict]) -> str:
    sessions = _extract_session_dates(rows)
    union_dates = sorted(sessions.get("fii_net_cash", set()) | sessions.get("dii_net_cash", set()))
    if not union_dates:
        return "coverage_days=0"
    span_days = (union_dates[-1] - union_dates[0]).days + 1
    return (
        f"coverage_days={len(union_dates)} span_days={span_days} "
        f"range={union_dates[0].isoformat()}..{union_dates[-1].isoformat()}"
    )


def _has_min_coverage(rows: Iterable[dict], *, min_trading_days: int) -> bool:
    sessions = _extract_session_dates(rows)
    union_dates = sessions.get("fii_net_cash", set()) | sessions.get("dii_net_cash", set())
    return len(union_dates) >= min_trading_days


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
        "--months",
        type=int,
        default=2,
        help=(
            "How many recent months to pull from Moneycontrol daily month-detail source. "
            "Default 2 (current + previous month), which covers at least ~1 month of sessions."
        ),
    )
    parser.add_argument(
        "--min-trading-days",
        type=int,
        default=20,
        help="Minimum distinct session dates required across FII/DII rows (default: 20).",
    )
    parser.add_argument(
        "--skip-moneycontrol",
        action="store_true",
        help="Skip Moneycontrol historical month-detail fetch.",
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
    if not args.skip_moneycontrol:
        if args.months <= 0:
            logger.error("--months must be >= 1 when Moneycontrol source is enabled")
            raise SystemExit(2)
        collected.extend(_fetch_moneycontrol_rows(months=args.months))

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
    logger.info("Coverage: %s", _coverage_summary(rows))
    if args.min_trading_days > 0 and not _has_min_coverage(rows, min_trading_days=args.min_trading_days):
        logger.error(
            "Insufficient FII/DII coverage. Need at least %d trading days; got %s",
            args.min_trading_days,
            _coverage_summary(rows),
        )
        raise SystemExit(2)

    if args.dry_run:
        logger.info("Dry-run only: no database writes.")
        if len(rows) <= 2 and not args.csv and args.skip_moneycontrol:
            logger.warning(
                "Only live snapshot rows detected. Provide --csv files for deeper historical chart backfill."
            )
        return

    inserted = await upsert_rows(rows)
    logger.info("Backfill complete: upserted=%d rows", inserted)


if __name__ == "__main__":
    asyncio.run(main())
