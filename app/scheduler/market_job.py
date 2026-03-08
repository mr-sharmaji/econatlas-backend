from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from typing import Dict, List, Tuple

from app.scheduler.base import BaseScraper
from app.scheduler.trading_calendar import is_trading_day_markets
from app.services import event_service, market_service

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FX_USD_BASE_URL = "https://open.er-api.com/v6/latest/USD"
GIFT_NIFTY_URL = "https://giftcitynifty.com/gift-nifty-intraday-price-data/"

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

FX_SYMBOLS = {
    "USDINR=X": "USD/INR",
    "EURINR=X": "EUR/INR",
    "GBPINR=X": "GBP/INR",
    "JPYINR=X": "JPY/INR",
}

BOND_SERIES: List[Tuple[str, str]] = [
    ("US 10Y Treasury Yield", "DGS10"),
    ("US 2Y Treasury Yield", "DGS2"),
    ("India 10Y Bond Yield", "INDIRLTLT01STM"),
]


def _pct_change(current: float, previous: float | None) -> float | None:
    if previous is None or previous == 0:
        return None
    return round(((current - previous) / previous) * 100, 2)


class MarketScraper(BaseScraper):

    def _fetch_all_quotes(self) -> List[Dict]:
        symbols = [*INDEX_SYMBOLS.keys(), *FX_SYMBOLS.keys()]
        records = []
        for symbol in symbols:
            try:
                payload = self._get_json(YAHOO_CHART_URL.format(symbol=symbol))
                result = payload.get("chart", {}).get("result", [])
                if not result:
                    continue
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price is None:
                    continue
                prev = meta.get("chartPreviousClose") or meta.get("previousClose")
                records.append({
                    "symbol": symbol,
                    "price": float(price),
                    "prev": float(prev) if prev else None,
                    "currency": str(meta.get("currency", "USD")),
                })
            except Exception:
                logger.warning("Failed to fetch %s", symbol, exc_info=True)
        return records

    def _parse_indices(self, records: List[Dict]) -> List[Dict]:
        items = []
        for r in records:
            if r["symbol"] not in INDEX_SYMBOLS:
                continue
            items.append({
                "asset": INDEX_SYMBOLS[r["symbol"]],
                "price": r["price"],
                "instrument_type": "index",
                "unit": "points",
                "source": "yahoo_finance_api",
                "change_percent": _pct_change(r["price"], r["prev"]),
                "previous_close": r["prev"],
            })
        return items

    def _parse_fx(self, records: List[Dict]) -> List[Dict]:
        items = []
        for r in records:
            if r["symbol"] not in FX_SYMBOLS:
                continue
            items.append({
                "asset": FX_SYMBOLS[r["symbol"]],
                "price": r["price"],
                "instrument_type": "currency",
                "unit": "inr",
                "source": "yahoo_finance_api",
                "change_percent": _pct_change(r["price"], r["prev"]),
                "previous_close": r["prev"],
            })
        return items

    def _fetch_gift_nifty(self) -> Dict | None:
        try:
            html = self._get_text(GIFT_NIFTY_URL)
            tables = re.findall(r"<table[^>]*>([\s\S]*?)</table>", html)
            if not tables:
                return None
            rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", tables[0])
            for row in rows:
                cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", row)
                if len(cells) < 4:
                    continue
                raw_price = re.sub(r"<[^>]+>", "", cells[1]).strip().replace(",", "")
                raw_pct = re.sub(r"<[^>]+>", "", cells[3]).strip()
                try:
                    price = float(raw_price)
                    pct = float(raw_pct) if raw_pct else None
                    if price > 0:
                        return {
                            "asset": "Gift Nifty",
                            "price": price,
                            "instrument_type": "index",
                            "unit": "points",
                            "source": "giftcitynifty_scrape",
                            "change_percent": pct,
                            "previous_close": None,
                        }
                except ValueError:
                    continue
        except Exception:
            logger.exception("Gift Nifty scrape failed")
        return None

    def _fetch_bond_yields(self) -> List[Dict]:
        items = []
        for name, series_id in BOND_SERIES:
            try:
                text = self._get_text(FRED_CSV_URL, params={"id": series_id})
                reader = csv.DictReader(io.StringIO(text))
                latest = None
                for row in reader:
                    val = row.get(series_id)
                    if val and val != ".":
                        latest = float(val)
                if latest is None:
                    continue
                items.append({
                    "asset": name,
                    "price": latest,
                    "instrument_type": "bond_yield",
                    "unit": "percent",
                    "source": "fred_api",
                    "change_percent": None,
                    "previous_close": None,
                })
            except Exception:
                logger.exception("Bond yield failed for %s", series_id)
        return items

    def _fetch_fx_fallback(self) -> List[Dict]:
        payload = self._get_json(FX_USD_BASE_URL)
        rates = payload.get("rates", {})
        if not isinstance(rates, dict):
            return []
        inr = rates.get("INR")
        if not inr:
            return []
        inr = float(inr)
        items = [{"asset": "USD/INR", "price": inr, "instrument_type": "currency", "unit": "inr", "source": "er_api", "change_percent": None, "previous_close": None}]
        for code, pair in [("EUR", "EUR/INR"), ("GBP", "GBP/INR"), ("JPY", "JPY/INR")]:
            r = rates.get(code)
            if r and float(r) > 0:
                items.append({"asset": pair, "price": inr / float(r), "instrument_type": "currency", "unit": "inr", "source": "er_api", "change_percent": None, "previous_close": None})
        return items

    def fetch_all(self) -> List[Dict]:
        all_items = []
        try:
            records = self._fetch_all_quotes()
            all_items.extend(self._parse_indices(records))
            all_items.extend(self._parse_fx(records))
        except Exception:
            logger.warning("Yahoo quote fetch failed; using fallbacks", exc_info=True)
            for sym, name in INDEX_SYMBOLS.items():
                item = self._fetch_single_index(sym, name)
                if item:
                    all_items.append(item)
            try:
                all_items.extend(self._fetch_fx_fallback())
            except Exception:
                logger.exception("FX fallback failed")

        gift = self._fetch_gift_nifty()
        if gift:
            all_items.append(gift)

        try:
            all_items.extend(self._fetch_bond_yields())
        except Exception:
            logger.exception("Bond yield fetch failed")
        return all_items

    def _fetch_single_index(self, symbol: str, name: str) -> Dict | None:
        try:
            payload = self._get_json(YAHOO_CHART_URL.format(symbol=symbol))
            result = payload.get("chart", {}).get("result", [])
            if not result:
                return None
            meta = result[0].get("meta", {})
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if price is None:
                return None
            prev = meta.get("chartPreviousClose") or meta.get("previousClose")
            p = float(price)
            pv = float(prev) if prev else None
            return {"asset": name, "price": p, "instrument_type": "index", "unit": "points", "source": "yahoo_chart_api", "change_percent": _pct_change(p, pv), "previous_close": pv}
        except Exception:
            return None


_scraper = MarketScraper()


def _fetch_market_rows_sync() -> tuple[List[Dict], bool]:
    """Sync scrape; run in thread executor. Returns (rows, calendar_says_trading_day).
    When calendar says closed we still fetch; caller may write only rows where price changed (calendar can be wrong)."""
    now = _scraper.utc_now()
    calendar_open = is_trading_day_markets(now)
    items = _scraper.fetch_all()
    ts = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    rows = [
        {
            "asset": it["asset"],
            "price": it["price"],
            "timestamp": ts,
            "source": it.get("source"),
            "instrument_type": it["instrument_type"],
            "unit": it.get("unit"),
            "change_percent": it.get("change_percent"),
            "previous_close": it.get("previous_close"),
        }
        for it in items
    ]
    return (rows, calendar_open)


_PRICE_CHANGE_TOLERANCE = 1e-9


async def run_market_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        rows, calendar_says_open = await loop.run_in_executor(None, _fetch_market_rows_sync)
        if not rows:
            return
        if not calendar_says_open:
            pairs = [(r["asset"], r.get("instrument_type") or "") for r in rows]
            latest = await market_service.get_latest_price_per_asset_type(pairs)
            rows = [
                r for r in rows
                if (latest.get((r["asset"], r.get("instrument_type") or "")) is None)
                or abs(float(r["price"]) - latest[(r["asset"], r.get("instrument_type") or "")]) > _PRICE_CHANGE_TOLERANCE
            ]
        if not rows:
            logger.info("Market job: calendar said closed and no price change; skipped")
            return
        updated = await market_service.insert_prices_batch_upsert_daily(rows)
        logger.info("Market job complete: %d rows upserted (daily)", updated)
    except Exception:
        logger.exception("Market job failed")
