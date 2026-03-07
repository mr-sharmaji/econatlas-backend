"""Backfill EconAtlas with daily (or native-frequency) history for indices, commodities, currencies, bonds, and macros.

Fetches from today back N years (default 10). Yahoo series (indices, FX, commodities) are requested
year-by-year so each day in range is returned; FRED/World Bank use their native frequency.

Idempotent / safe to re-run: existing (asset, instrument_type, timestamp) and (indicator_name, country, timestamp)
are skipped in code, and the DB has unique indexes so INSERT uses ON CONFLICT DO NOTHING (no duplicates).
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging
import math
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import requests

from app.core.database import close_pool, get_pool, init_pool, parse_ts
from app.scheduler.base import BaseScraper

logger = logging.getLogger("backfill")

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
WORLD_BANK_URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"

INDEX_SYMBOLS = {
    "^GSPC": "S&P500",
    "^IXIC": "NASDAQ",
    "^DJI": "Dow Jones",
    "^NSEI": "Nifty 50",
    "^BSESN": "Sensex",
    "^NSEBANK": "Nifty Bank",
    "^CRSLDX": "Nifty 500",
    "^CNXIT": "Nifty IT",
    "NIFTYMIDCAP150.NS": "Nifty Midcap 150",
    "NIFTYSMLCAP250.NS": "Nifty Smallcap 250",
}
# Yahoo returns 400 for older years on these NSE indices; only request from min_year to avoid 400s.
INDEX_MIN_YEAR: dict[str, int] = {
    "NIFTYMIDCAP150.NS": 2019,
    "NIFTYSMLCAP250.NS": 2023,
}
# Nifty Smallcap 250: Yahoo chart API returns no history; we use NSE India as fallback (see _fetch_nse_index_series).
SMALLCAP_250_SYMBOL = "NIFTYSMLCAP250.NS"
# NSE India historical index API (requires session cookie from visiting nseindia.com first).
NSE_BASE_URL = "https://www.nseindia.com"
NSE_INDEX_HISTORY_PATH = "api/historical/indicesHistory"
NSE_INDEX_NAME_SMALLCAP_250 = "NIFTY SMALLCAP 250"

FX_SYMBOLS = {
    "USDINR=X": "USD/INR",
    "EURINR=X": "EUR/INR",
    "GBPINR=X": "GBP/INR",
    "JPYINR=X": "JPY/INR",
}

COMMODITY_SYMBOLS = {
    "GC=F": ("gold", "usd_per_troy_ounce"),
    "SI=F": ("silver", "usd_per_troy_ounce"),
    "CL=F": ("crude oil", "usd_per_barrel"),
    "NG=F": ("natural gas", "usd_per_mmbtu"),
    "HG=F": ("copper", "usd_per_pound"),
}

BOND_SERIES = [
    ("US 10Y Treasury Yield", "DGS10"),
    ("US 2Y Treasury Yield", "DGS2"),
    ("India 10Y Bond Yield", "INDIRLTLT01STM"),
]

FRED_DIRECT = {
    "US": [
        ("gdp_growth", "A191RL1Q225SBEA"),
        ("unemployment", "UNRATE"),
    ],
    "IN": [
        ("gdp_growth", "INDGDPRQPSMEI"),
        ("repo_rate", "IRSTCI01INM156N"),
    ],
}

FRED_CPI = {
    "US": "CPIAUCSL",
    "IN": "INDCPIALLMINMEI",
}

WORLD_BANK_FALLBACK = {
    "IN": [
        ("unemployment", "SL.UEM.TOTL.ZS"),
    ]
}


@dataclass
class BackfillConfig:
    years: int
    api_batch_size: int
    api_sleep_seconds: float
    db_batch_size: int
    db_sleep_seconds: float
    validate_only: bool


def _pct_change(current: float, previous: float | None) -> float | None:
    if previous is None or previous == 0:
        return None
    return round(((current - previous) / previous) * 100, 2)


def _fill_change_percent(rows: list[dict[str, Any]]) -> None:
    """Fill change_percent and previous_close from previous row (rows must be sorted by timestamp ascending)."""
    rows.sort(key=lambda r: r["timestamp"])
    prev_price: float | None = None
    for r in rows:
        price = r.get("price")
        if isinstance(price, (int, float)):
            price = float(price)
        else:
            prev_price = None
            continue
        r["previous_close"] = prev_price
        r["change_percent"] = _pct_change(price, prev_price) if prev_price is not None else None
        prev_price = price


class HistoricalBackfiller(BaseScraper):
    def __init__(self, config: BackfillConfig, start_ts: datetime, end_ts: datetime) -> None:
        super().__init__()
        self.config = config
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.start_date = start_ts.date()
        self.end_date = end_ts.date()

    def _batched(self, items: list[Any], size: int) -> list[list[Any]]:
        return [items[i : i + size] for i in range(0, len(items), size)]

    def _to_dt(self, epoch: int) -> datetime:
        return datetime.fromtimestamp(epoch, tz=UTC)

    def _fetch_yahoo_series(
        self,
        symbol: str,
        range_start: datetime | None = None,
        range_end: datetime | None = None,
    ) -> list[tuple[datetime, float]]:
        """Fetch daily OHLC history for a symbol. Use range_start/range_end to chunk (e.g. per year) for full daily coverage."""
        start = range_start or self.start_ts
        end = range_end or self.end_ts
        params = {
            "period1": int(start.timestamp()),
            "period2": int(end.timestamp()),
            "interval": "1d",
            "events": "history",
            "includePrePost": "false",
        }
        payload = self._get_json(YAHOO_CHART_URL.format(symbol=symbol), params=params)
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return []
        block = result[0]
        timestamps = block.get("timestamp", []) or []
        closes = (((block.get("indicators", {}) or {}).get("quote", []) or [{}])[0]).get("close", []) or []
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
            ts = self._to_dt(int(epoch))
            if self.start_ts <= ts <= self.end_ts:
                out.append((ts, value))
        return out

    def _ensure_nse_cookies(self) -> None:
        """Ensure NSE session has cookies (visit homepage first). Required for indicesHistory API."""
        if getattr(self, "_nse_cookies_set", False):
            return
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        }
        try:
            self.session.get(
                f"{NSE_BASE_URL}/",
                headers=nse_headers,
                timeout=15,
            )
            self._nse_cookies_set = True
        except requests.RequestException as e:
            logger.warning("NSE cookie fetch failed: %s", e)

    def _parse_nse_date(self, raw: Any) -> datetime | None:
        """Parse NSE timestamp: can be epoch (ms or s) or string like 08-Mar-2026."""
        if raw is None:
            return None
        if isinstance(raw, (int, float)):
            epoch = int(raw)
            if epoch > 1e12:
                epoch = epoch // 1000
            return datetime.fromtimestamp(epoch, tz=UTC)
        if isinstance(raw, str):
            s = (raw.strip().split()[0] or "")[:12]
            if not s:
                return None
            for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(s, fmt).replace(tzinfo=UTC)
                except ValueError:
                    continue
        return None

    def _fetch_nse_index_series(
        self,
        index_name: str,
        range_start: datetime,
        range_end: datetime,
    ) -> list[tuple[datetime, float]]:
        """Fetch historical index close from NSE India API. Returns list of (datetime, price)."""
        self._ensure_nse_cookies()
        url = f"{NSE_BASE_URL}/{NSE_INDEX_HISTORY_PATH}"
        params = {
            "indexType": index_name,
            "from": range_start.strftime("%d-%m-%Y"),
            "to": range_end.strftime("%d-%m-%Y"),
        }
        nse_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": f"{NSE_BASE_URL}/",
        }
        out: list[tuple[datetime, float]] = []
        try:
            resp = self.session.get(url, params=params, headers=nse_headers, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as e:
            logger.warning("NSE index history request failed: %s", e)
            return out
        records = (data or {}).get("data", {}).get("indexCloseOnlineRecords") or []
        for rec in records:
            ts = self._parse_nse_date(rec.get("EOD_TIMESTAMP") or rec.get("TIMESTAMP"))
            close_raw = rec.get("EOD_CLOSE_INDEX_VAL") or rec.get("CH_CLOSING_PRICE")
            if ts is None or close_raw is None:
                continue
            try:
                price = float(close_raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(price) or price <= 0:
                continue
            if self.start_ts <= ts <= self.end_ts:
                out.append((ts, price))
        out.sort(key=lambda x: x[0])
        return out

    def _iter_yearly_chunks(self, min_year: int | None = None) -> list[tuple[datetime, datetime]]:
        """(start_ts, end_ts) for each calendar year in range. If min_year set, only years >= min_year (for indices where Yahoo returns 400 for old years)."""
        chunks: list[tuple[datetime, datetime]] = []
        current = self.start_ts.date()
        end_date = self.end_ts.date()
        if min_year is not None and current.year < min_year:
            current = date(min_year, 1, 1)
        while current <= end_date:
            year_start = datetime(current.year, 1, 1, tzinfo=UTC)
            year_end = datetime(current.year, 12, 31, 23, 59, 59, tzinfo=UTC)
            chunk_start = max(year_start, self.start_ts)
            chunk_end = min(year_end, self.end_ts)
            if chunk_start <= chunk_end:
                chunks.append((chunk_start, chunk_end))
            current = date(current.year + 1, 1, 1)
        return chunks

    def _fetch_fred_series(self, series_id: str) -> list[tuple[datetime, float]]:
        text = self._get_text(FRED_CSV_URL, params={"id": series_id})
        reader = csv.DictReader(io.StringIO(text))
        out: list[tuple[datetime, float]] = []
        for row in reader:
            ds = row.get("observation_date") or row.get("DATE")
            val = row.get(series_id)
            if not ds or not val or val.strip() == ".":
                continue
            try:
                ts = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=UTC)
                fval = float(val)
            except ValueError:
                continue
            if self.start_ts <= ts <= self.end_ts:
                out.append((ts, fval))
        return out

    def _compute_yoy(self, rows: list[tuple[datetime, float]]) -> list[tuple[datetime, float]]:
        indexed = {(ts.year, ts.month): value for ts, value in rows}
        out: list[tuple[datetime, float]] = []
        for ts, value in rows:
            prev = indexed.get((ts.year - 1, ts.month))
            if prev is None or prev == 0:
                continue
            yoy = round(((value - prev) / prev) * 100, 2)
            out.append((ts, yoy))
        return out

    def _fetch_world_bank(self, country: str, indicator: str) -> list[tuple[datetime, float]]:
        payload = self._get_json(
            WORLD_BANK_URL.format(country=country.lower(), indicator=indicator),
            params={"format": "json", "per_page": 15},
        )
        if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
            return []
        out: list[tuple[datetime, float]] = []
        for rec in payload[1]:
            year = rec.get("date")
            value = rec.get("value")
            if year is None or value is None:
                continue
            try:
                ts = datetime(int(str(year)), 1, 1, tzinfo=UTC)
                fval = float(value)
            except ValueError:
                continue
            if self.start_ts <= ts <= self.end_ts:
                out.append((ts, fval))
        out.sort(key=lambda item: item[0])
        return out

    def collect_market_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        items = [(k, v, "index", "points") for k, v in INDEX_SYMBOLS.items()]
        items.extend((k, v, "currency", "inr") for k, v in FX_SYMBOLS.items())
        for batch_num, chunk in enumerate(self._batched(items, self.config.api_batch_size), start=1):
            logger.info("Market API batch %d/%d", batch_num, math.ceil(len(items) / self.config.api_batch_size))
            for symbol, asset, instrument_type, unit in chunk:
                min_year = INDEX_MIN_YEAR.get(symbol)
                yearly = self._iter_yearly_chunks(min_year=min_year)
                if min_year is not None:
                    logger.info("%s: requesting from year %d only (%d chunks)", asset, min_year, len(yearly))
                symbol_rows: list[dict[str, Any]] = []
                # Nifty Smallcap 250: Yahoo often returns 400 per year but 200 with data for full range
                if symbol == SMALLCAP_250_SYMBOL and yearly:
                    range_start, range_end = yearly[0][0], yearly[-1][1]
                    try:
                        points = self._fetch_yahoo_series(symbol, range_start, range_end)
                        for ts, price in points:
                            symbol_rows.append(
                                {
                                    "asset": asset,
                                    "price": price,
                                    "timestamp": ts.isoformat(),
                                    "source": "yahoo_chart_api_backfill",
                                    "instrument_type": instrument_type,
                                    "unit": unit,
                                    "change_percent": None,
                                    "previous_close": None,
                                }
                            )
                        if points:
                            logger.info("%s: got %d points from single full-range request", asset, len(points))
                        time.sleep(0.5)
                    except requests.exceptions.HTTPError as e:
                        if e.response is not None and e.response.status_code == 400:
                            logger.warning("%s: full-range request 400, falling back to year-by-year", asset)
                        else:
                            raise
                if not symbol_rows:
                    for range_start, range_end in yearly:
                        try:
                            points = self._fetch_yahoo_series(symbol, range_start, range_end)
                            for ts, price in points:
                                symbol_rows.append(
                                    {
                                        "asset": asset,
                                        "price": price,
                                        "timestamp": ts.isoformat(),
                                        "source": "yahoo_chart_api_backfill",
                                        "instrument_type": instrument_type,
                                        "unit": unit,
                                        "change_percent": None,
                                        "previous_close": None,
                                    }
                                )
                        except requests.exceptions.HTTPError as e:
                            if e.response is not None and e.response.status_code == 400:
                                logger.warning(
                                    "Skipping %s–%s for %s (no data for this period, e.g. index launched later)",
                                    range_start.date(),
                                    range_end.date(),
                                    symbol,
                                )
                                continue
                            raise
                        except Exception:
                            logger.exception("Market history fetch failed for %s", symbol)
                            break
                        time.sleep(0.3)
                # Nifty Smallcap 250: if Yahoo returned no history, use NSE India API
                if not symbol_rows and symbol == SMALLCAP_250_SYMBOL:
                    logger.info("%s: trying NSE India as fallback (Yahoo returned no history)", asset)
                    for range_start, range_end in yearly:
                        points = self._fetch_nse_index_series(
                            NSE_INDEX_NAME_SMALLCAP_250, range_start, range_end
                        )
                        for ts, price in points:
                            symbol_rows.append(
                                {
                                    "asset": asset,
                                    "price": price,
                                    "timestamp": ts.isoformat(),
                                    "source": "nse_indices_history_backfill",
                                    "instrument_type": instrument_type,
                                    "unit": unit,
                                    "change_percent": None,
                                    "previous_close": None,
                                }
                            )
                        if points:
                            logger.info(
                                "%s: NSE returned %d points for %s–%s",
                                asset,
                                len(points),
                                range_start.date(),
                                range_end.date(),
                            )
                        time.sleep(0.5)
                _fill_change_percent(symbol_rows)
                rows.extend(symbol_rows)
            if batch_num < math.ceil(len(items) / self.config.api_batch_size):
                time.sleep(self.config.api_sleep_seconds)
        return rows

    def collect_commodity_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        items = list(COMMODITY_SYMBOLS.items())
        yearly = self._iter_yearly_chunks()
        for batch_num, chunk in enumerate(self._batched(items, self.config.api_batch_size), start=1):
            logger.info("Commodity API batch %d/%d", batch_num, math.ceil(len(items) / self.config.api_batch_size))
            for symbol, (asset, unit) in chunk:
                symbol_rows = []
                for range_start, range_end in yearly:
                    try:
                        points = self._fetch_yahoo_series(symbol, range_start, range_end)
                        for ts, price in points:
                            symbol_rows.append(
                                {
                                    "asset": asset,
                                    "price": price,
                                    "timestamp": ts.isoformat(),
                                    "source": "yahoo_chart_api_backfill",
                                    "instrument_type": "commodity",
                                    "unit": unit,
                                    "change_percent": None,
                                    "previous_close": None,
                                }
                            )
                    except requests.exceptions.HTTPError as e:
                        if e.response is not None and e.response.status_code == 400:
                            logger.warning(
                                "Skipping %s–%s for %s (no data for this period)",
                                range_start.date(),
                                range_end.date(),
                                symbol,
                            )
                            continue
                        raise
                    except Exception:
                        logger.exception("Commodity history fetch failed for %s", symbol)
                        break
                    time.sleep(0.3)
                _fill_change_percent(symbol_rows)
                rows.extend(symbol_rows)
            if batch_num < math.ceil(len(items) / self.config.api_batch_size):
                time.sleep(self.config.api_sleep_seconds)
        return rows

    def collect_bond_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        items = list(BOND_SERIES)
        for batch_num, chunk in enumerate(self._batched(items, self.config.api_batch_size), start=1):
            logger.info("Bond API batch %d/%d", batch_num, math.ceil(len(items) / self.config.api_batch_size))
            for asset, series_id in chunk:
                try:
                    for ts, value in self._fetch_fred_series(series_id):
                        rows.append(
                            {
                                "asset": asset,
                                "price": value,
                                "timestamp": ts.isoformat(),
                                "source": "fred_api_backfill",
                                "instrument_type": "bond_yield",
                                "unit": "percent",
                                "change_percent": None,
                                "previous_close": None,
                            }
                        )
                except Exception:
                    logger.exception("Bond history fetch failed for %s", series_id)
            if batch_num < math.ceil(len(items) / self.config.api_batch_size):
                time.sleep(self.config.api_sleep_seconds)
        return rows

    def collect_macro_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        work_items: list[tuple[str, str, str]] = []
        for country, pairs in FRED_DIRECT.items():
            for indicator_name, series_id in pairs:
                work_items.append(("fred_direct", country, f"{indicator_name}:{series_id}"))
        for country, series_id in FRED_CPI.items():
            work_items.append(("fred_cpi", country, series_id))
        for country, pairs in WORLD_BANK_FALLBACK.items():
            for indicator_name, indicator_id in pairs:
                work_items.append(("world_bank", country, f"{indicator_name}:{indicator_id}"))

        total_batches = math.ceil(len(work_items) / self.config.api_batch_size)
        for batch_num, chunk in enumerate(self._batched(work_items, self.config.api_batch_size), start=1):
            logger.info("Macro API batch %d/%d", batch_num, total_batches)
            for source_type, country, ref in chunk:
                try:
                    if source_type == "fred_direct":
                        indicator_name, series_id = ref.split(":", 1)
                        for ts, value in self._fetch_fred_series(series_id):
                            rows.append(
                                {
                                    "indicator_name": indicator_name,
                                    "value": value,
                                    "country": country,
                                    "timestamp": ts.isoformat(),
                                    "unit": None,
                                    "source": "fred_api_backfill",
                                }
                            )
                    elif source_type == "fred_cpi":
                        cpi_rows = self._fetch_fred_series(ref)
                        for ts, value in self._compute_yoy(cpi_rows):
                            if self.start_ts <= ts <= self.end_ts:
                                rows.append(
                                    {
                                        "indicator_name": "inflation",
                                        "value": value,
                                        "country": country,
                                        "timestamp": ts.isoformat(),
                                        "unit": "percent_yoy",
                                        "source": "fred_api_backfill",
                                    }
                                )
                    else:
                        indicator_name, indicator_id = ref.split(":", 1)
                        for ts, value in self._fetch_world_bank(country, indicator_id):
                            rows.append(
                                {
                                    "indicator_name": indicator_name,
                                    "value": value,
                                    "country": country,
                                    "timestamp": ts.isoformat(),
                                    "unit": "percent",
                                    "source": "world_bank_backfill",
                                }
                            )
                except Exception:
                    logger.exception("Macro history fetch failed for %s %s", source_type, ref)
            if batch_num < total_batches:
                time.sleep(self.config.api_sleep_seconds)
        return rows


async def _existing_market_timestamps(
    asset: str,
    instrument_type: str,
    start_ts: datetime,
    end_ts: datetime,
) -> set[datetime]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT "timestamp"
        FROM market_prices
        WHERE asset = $1
          AND instrument_type = $2
          AND "timestamp" >= $3
          AND "timestamp" <= $4
        """,
        asset,
        instrument_type,
        start_ts,
        end_ts,
    )
    return {row["timestamp"] for row in rows}


async def _existing_macro_timestamps(
    indicator_name: str,
    country: str,
    start_ts: datetime,
    end_ts: datetime,
) -> set[datetime]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT "timestamp"
        FROM macro_indicators
        WHERE indicator_name = $1
          AND country = $2
          AND "timestamp" >= $3
          AND "timestamp" <= $4
        """,
        indicator_name,
        country,
        start_ts,
        end_ts,
    )
    return {row["timestamp"] for row in rows}


def _chunk_rows(rows: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [rows[i : i + size] for i in range(0, len(rows), size)]


async def insert_market_rows_idempotent(
    rows: list[dict[str, Any]],
    start_ts: datetime,
    end_ts: datetime,
    db_batch_size: int,
    db_sleep_seconds: float,
) -> tuple[int, int]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["asset"], row["instrument_type"])].append(row)

    pool = await get_pool()
    inserted = 0
    skipped = 0
    total_groups = len(grouped)
    for group_num, ((asset, instrument_type), group_rows) in enumerate(grouped.items(), start=1):
        existing = await _existing_market_timestamps(asset, instrument_type, start_ts, end_ts)
        pending = []
        for row in group_rows:
            ts = parse_ts(row["timestamp"])
            if ts in existing:
                skipped += 1
                continue
            pending.append(row)
            existing.add(ts)

        batches = _chunk_rows(pending, db_batch_size)
        for batch_idx, batch in enumerate(batches, start=1):
            async with pool.acquire() as conn:
                for row in batch:
                    await conn.execute(
                        """
                        INSERT INTO market_prices
                        (asset, price, "timestamp", source, instrument_type, unit, change_percent, previous_close)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        row["asset"],
                        row["price"],
                        parse_ts(row["timestamp"]),
                        row.get("source"),
                        row["instrument_type"],
                        row.get("unit"),
                        row.get("change_percent"),
                        row.get("previous_close"),
                    )
                inserted += len(batch)
            if batch_idx < len(batches):
                await asyncio.sleep(db_sleep_seconds)

        logger.info(
            "DB group %d/%d complete for %s (%s): inserted=%d skipped=%d",
            group_num,
            total_groups,
            asset,
            instrument_type,
            len(pending),
            len(group_rows) - len(pending),
        )
        if group_num < total_groups:
            await asyncio.sleep(db_sleep_seconds)
    return inserted, skipped


async def insert_macro_rows_idempotent(
    rows: list[dict[str, Any]],
    start_ts: datetime,
    end_ts: datetime,
    db_batch_size: int,
    db_sleep_seconds: float,
) -> tuple[int, int]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["indicator_name"], row["country"])].append(row)

    pool = await get_pool()
    inserted = 0
    skipped = 0
    total_groups = len(grouped)
    for group_num, ((indicator_name, country), group_rows) in enumerate(grouped.items(), start=1):
        existing = await _existing_macro_timestamps(indicator_name, country, start_ts, end_ts)
        pending = []
        for row in group_rows:
            ts = parse_ts(row["timestamp"])
            if ts in existing:
                skipped += 1
                continue
            pending.append(row)
            existing.add(ts)

        batches = _chunk_rows(pending, db_batch_size)
        for batch_idx, batch in enumerate(batches, start=1):
            async with pool.acquire() as conn:
                for row in batch:
                    await conn.execute(
                        """
                        INSERT INTO macro_indicators
                        (indicator_name, value, country, "timestamp", unit, source)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        row["indicator_name"],
                        row["value"],
                        row["country"],
                        parse_ts(row["timestamp"]),
                        row.get("unit"),
                        row.get("source"),
                    )
                inserted += len(batch)
            if batch_idx < len(batches):
                await asyncio.sleep(db_sleep_seconds)

        logger.info(
            "DB group %d/%d complete for %s (%s): inserted=%d skipped=%d",
            group_num,
            total_groups,
            indicator_name,
            country,
            len(pending),
            len(group_rows) - len(pending),
        )
        if group_num < total_groups:
            await asyncio.sleep(db_sleep_seconds)
    return inserted, skipped


async def validate_backfill(start_ts: datetime, end_ts: datetime) -> dict[str, Any]:
    pool = await get_pool()

    market_types = ["index", "commodity", "currency", "bond_yield"]
    market_summary: dict[str, Any] = {}
    for instrument_type in market_types:
        row = await pool.fetchrow(
            """
            SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (WHERE price <= 0 OR price IS NULL) AS invalid_price_count,
                MIN("timestamp") AS min_ts,
                MAX("timestamp") AS max_ts
            FROM market_prices
            WHERE instrument_type = $1
              AND "timestamp" >= $2
              AND "timestamp" <= $3
            """,
            instrument_type,
            start_ts,
            end_ts,
        )
        dup = await pool.fetchval(
            """
            SELECT COUNT(*) FROM (
                SELECT asset, "timestamp", instrument_type, COUNT(*) AS c
                FROM market_prices
                WHERE instrument_type = $1
                  AND "timestamp" >= $2
                  AND "timestamp" <= $3
                GROUP BY asset, "timestamp", instrument_type
                HAVING COUNT(*) > 1
            ) d
            """,
            instrument_type,
            start_ts,
            end_ts,
        )
        market_summary[instrument_type] = {
            "total_count": int(row["total_count"]),
            "invalid_price_count": int(row["invalid_price_count"]),
            "min_ts": row["min_ts"].isoformat() if row["min_ts"] else None,
            "max_ts": row["max_ts"].isoformat() if row["max_ts"] else None,
            "duplicate_key_count": int(dup or 0),
        }

    macro_row = await pool.fetchrow(
        """
        SELECT
            COUNT(*) AS total_count,
            COUNT(*) FILTER (WHERE value IS NULL) AS invalid_value_count,
            MIN("timestamp") AS min_ts,
            MAX("timestamp") AS max_ts
        FROM macro_indicators
        WHERE "timestamp" >= $1
          AND "timestamp" <= $2
        """,
        start_ts,
        end_ts,
    )
    macro_dup = await pool.fetchval(
        """
        SELECT COUNT(*) FROM (
            SELECT indicator_name, country, "timestamp", COUNT(*) AS c
            FROM macro_indicators
            WHERE "timestamp" >= $1
              AND "timestamp" <= $2
            GROUP BY indicator_name, country, "timestamp"
            HAVING COUNT(*) > 1
        ) d
        """,
        start_ts,
        end_ts,
    )

    return {
        "market": market_summary,
        "macro": {
            "total_count": int(macro_row["total_count"]),
            "invalid_value_count": int(macro_row["invalid_value_count"]),
            "min_ts": macro_row["min_ts"].isoformat() if macro_row["min_ts"] else None,
            "max_ts": macro_row["max_ts"].isoformat() if macro_row["max_ts"] else None,
            "duplicate_key_count": int(macro_dup or 0),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill EconAtlas historical data for every day from today back N years (indices, commodities, currencies, bonds, macros).",
    )
    parser.add_argument("--years", type=int, default=10, help="Years of history to backfill (default: 10).")
    parser.add_argument("--passes", type=int, default=2, help="How many times to run backfill for idempotency checks.")
    parser.add_argument("--api-batch-size", type=int, default=3, help="Number of symbols/series per API batch.")
    parser.add_argument(
        "--api-sleep-seconds",
        type=float,
        default=1.5,
        help="Sleep between API batches to avoid rate limits.",
    )
    parser.add_argument("--db-batch-size", type=int, default=250, help="Rows per DB write batch.")
    parser.add_argument(
        "--db-sleep-seconds",
        type=float,
        default=0.2,
        help="Sleep between DB batches.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Skip fetch/insert and only run validation for selected window.",
    )
    return parser.parse_args()


async def run_once(backfiller: HistoricalBackfiller, cfg: BackfillConfig) -> dict[str, Any]:
    market_rows = backfiller.collect_market_rows()
    commodity_rows = backfiller.collect_commodity_rows()
    bond_rows = backfiller.collect_bond_rows()
    macro_rows = backfiller.collect_macro_rows()

    all_market_rows = [*market_rows, *commodity_rows, *bond_rows]
    logger.info(
        "Fetched rows: market=%d commodity=%d bond=%d macro=%d",
        len(market_rows),
        len(commodity_rows),
        len(bond_rows),
        len(macro_rows),
    )

    inserted_market = skipped_market = inserted_macro = skipped_macro = 0
    if not cfg.validate_only:
        inserted_market, skipped_market = await insert_market_rows_idempotent(
            all_market_rows,
            backfiller.start_ts,
            backfiller.end_ts,
            cfg.db_batch_size,
            cfg.db_sleep_seconds,
        )
        inserted_macro, skipped_macro = await insert_macro_rows_idempotent(
            macro_rows,
            backfiller.start_ts,
            backfiller.end_ts,
            cfg.db_batch_size,
            cfg.db_sleep_seconds,
        )

    validation = await validate_backfill(backfiller.start_ts, backfiller.end_ts)
    return {
        "fetched_market_rows": len(all_market_rows),
        "fetched_macro_rows": len(macro_rows),
        "inserted_market_rows": inserted_market,
        "skipped_market_rows": skipped_market,
        "inserted_macro_rows": inserted_macro,
        "skipped_macro_rows": skipped_macro,
        "validation": validation,
    }


async def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    end_ts = datetime.now(tz=UTC).replace(hour=23, minute=59, second=59, microsecond=0)
    start_ts = datetime.combine(
        date.today() - timedelta(days=365 * args.years),
        datetime.min.time(),
        tzinfo=UTC,
    )
    logger.info("Backfill range: %s to %s (%d years)", start_ts.date(), end_ts.date(), args.years)
    cfg = BackfillConfig(
        years=args.years,
        api_batch_size=max(args.api_batch_size, 1),
        api_sleep_seconds=max(args.api_sleep_seconds, 0),
        db_batch_size=max(args.db_batch_size, 1),
        db_sleep_seconds=max(args.db_sleep_seconds, 0),
        validate_only=bool(args.validate_only),
    )

    await init_pool()
    try:
        backfiller = HistoricalBackfiller(cfg, start_ts=start_ts, end_ts=end_ts)
        for pass_num in range(1, max(args.passes, 1) + 1):
            logger.info(
                "Backfill pass %d starting for range %s -> %s",
                pass_num,
                start_ts.isoformat(),
                end_ts.isoformat(),
            )
            result = await run_once(backfiller, cfg)
            logger.info(
                "Backfill pass %d result: %s",
                pass_num,
                result,
            )
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(main())
