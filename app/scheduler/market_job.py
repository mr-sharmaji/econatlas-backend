from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from datetime import date, datetime, timezone
from typing import Dict, List, Tuple

from app.scheduler.base import BaseScraper
from app.scheduler.trading_calendar import (
    get_trading_date,
    get_market_status,
    get_gift_nifty_trading_date,
    is_gift_nifty_open,
    is_trading_day_markets,
    NSE,
    NYSE,
)
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

# Asset → exchange for correct trading-date assignment (avoid Monday close stored as Tuesday UTC)
ASSET_EXCHANGE: Dict[str, str] = {
    "S&P500": NYSE,
    "NASDAQ": NYSE,
    "Dow Jones": NYSE,
    "Nifty 50": NSE,
    "Sensex": NSE,
    "Nifty 500": NSE,
    "Nifty Bank": NSE,
    "Nifty IT": NSE,
    "Nifty Midcap 150": NSE,
    "Nifty Smallcap 250": NSE,
    "Gift Nifty": NSE,
    "USD/INR": NYSE,
    "EUR/INR": NYSE,
    "GBP/INR": NYSE,
    "JPY/INR": NYSE,
    "India 10Y Bond Yield": NSE,
    "US 10Y Treasury Yield": NYSE,
    "US 2Y Treasury Yield": NYSE,
}


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


def _fetch_yahoo_1m_bars(symbol: str) -> tuple[list[tuple[datetime, float]], str]:
    """Fetch 1-minute OHLC bars from Yahoo Chart API (range=2d, interval=1m).
    Returns (list of (utc_datetime, close_price), currency_code). Returns ([], 'USD') on failure."""
    try:
        url = YAHOO_CHART_URL.format(symbol=symbol)
        payload = _scraper._get_json(url, params={"interval": "1m", "range": "2d"})
        result = payload.get("chart", {}).get("result", [])
        if not result:
            return ([], "USD")
        res = result[0]
        meta = res.get("meta", {})
        currency = str(meta.get("currency", "USD") or "USD")
        timestamps = res.get("timestamp") or []
        quote = (res.get("indicators", {}).get("quote", []) or [{}])[0]
        closes = quote.get("close") or []
        out = []
        for i, ts in enumerate(timestamps):
            if i >= len(closes):
                break
            c = closes[i]
            if c is None:
                continue
            try:
                dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                out.append((dt, float(c)))
            except (TypeError, ValueError):
                continue
        return (out, currency)
    except Exception:
        logger.warning("Failed to fetch 1m bars for %s", symbol, exc_info=True)
        return ([], "USD")


def build_market_intraday_rows_last_session_yahoo(
    market_rows: list[dict], trading_date_by_exchange: dict[str, date]
) -> list[dict]:
    """Build full minute-level intraday rows for last session using Yahoo 1m chart data.
    For assets with a Yahoo symbol we fetch 1m bars and filter to the given trading date;
    for others (Gift Nifty, bonds) we add one point from market_rows."""
    from app.services import market_service as svc

    rows_out = []
    # Assets we can get 1m data for (symbol -> (asset, instrument_type))
    yahoo_assets = {}
    for sym, asset in INDEX_SYMBOLS.items():
        yahoo_assets[sym] = (asset, "index")
    for sym, asset in FX_SYMBOLS.items():
        yahoo_assets[sym] = (asset, "currency")

    # Reverse: asset -> (symbol, instrument_type)
    asset_to_symbol: dict[str, tuple[str, str]] = {}
    for sym, (asset, itype) in yahoo_assets.items():
        asset_to_symbol[asset] = (sym, itype)

    for sym, (asset_name, instrument_type) in yahoo_assets.items():
        exchange = ASSET_EXCHANGE.get(asset_name, NYSE)
        trading_date = trading_date_by_exchange.get(exchange)
        if not trading_date:
            continue
        day_str = trading_date.strftime("%Y-%m-%d")
        bars, _ = _fetch_yahoo_1m_bars(sym)
        for dt, close in bars:
            if dt.strftime("%Y-%m-%d") != day_str:
                continue
            ts_rounded = svc._round_to_minute(dt).isoformat()
            rows_out.append({
                "asset": asset_name,
                "instrument_type": instrument_type,
                "price": close,
                "timestamp": ts_rounded,
            })

    # Single point for assets without Yahoo intraday (Gift Nifty, bonds)
    yahoo_asset_set = set(asset_to_symbol.keys())
    for r in market_rows:
        if r.get("asset") in yahoo_asset_set:
            continue
        ts = r.get("timestamp")
        if isinstance(ts, datetime):
            ts = ts.isoformat()
        rows_out.append({
            "asset": r["asset"],
            "instrument_type": r.get("instrument_type") or "index",
            "price": r["price"],
            "timestamp": ts,
        })
    return rows_out


def _fetch_market_rows_sync() -> tuple[List[Dict], bool]:
    """Sync scrape; run in thread executor. Returns (rows, calendar_says_trading_day).
    Each row's timestamp is the exchange trading date (NSE/NYSE) so Monday's close is not stored as Tuesday."""
    now = _scraper.utc_now()
    calendar_open = is_trading_day_markets(now) or is_gift_nifty_open(now)
    items = _scraper.fetch_all()
    rows = []
    for it in items:
        if it["asset"] == "Gift Nifty":
            # Gift Nifty follows its own extended session model.
            trading_date = get_gift_nifty_trading_date(now)
        else:
            exchange = ASSET_EXCHANGE.get(it["asset"], NYSE)
            trading_date = get_trading_date(now, exchange)
        ts = datetime(trading_date.year, trading_date.month, trading_date.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
        rows.append({
            "asset": it["asset"],
            "price": it["price"],
            "timestamp": ts,
            "source": it.get("source"),
            "instrument_type": it["instrument_type"],
            "unit": it.get("unit"),
            "change_percent": it.get("change_percent"),
            "previous_close": it.get("previous_close"),
        })
    return (rows, calendar_open)


_PRICE_CHANGE_TOLERANCE = 1e-9


def build_market_intraday_rows_for_open(
    market_rows: list[dict],
    status: dict,
    ts_rounded: str,
) -> list[dict]:
    """Build intraday rows for 1D chart.
    Currencies are written every run (24/7 behavior).
    Gift Nifty follows dedicated Gift Nifty session status.
    Other indices/bonds follow exchange open status."""
    intraday_rows = []
    for r in market_rows:
        instrument_type = r.get("instrument_type") or "index"
        exchange = ASSET_EXCHANGE.get(r["asset"], NYSE)
        include = False
        if instrument_type == "currency":
            include = True
        elif r.get("asset") == "Gift Nifty":
            include = bool(status.get("gift_nifty_open"))
        elif (exchange == NSE and status.get("nse_open")) or (exchange == NYSE and status.get("nyse_open")):
            include = True
        if include:
            intraday_rows.append({
                "asset": r["asset"],
                "instrument_type": instrument_type,
                "price": r["price"],
                "timestamp": ts_rounded,
            })
    return intraday_rows


async def run_market_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        fetched_rows, calendar_says_open = await loop.run_in_executor(None, _fetch_market_rows_sync)
        if not fetched_rows:
            return
        rows = fetched_rows
        if not calendar_says_open:
            pairs = [(r["asset"], r.get("instrument_type") or "") for r in rows]
            latest = await market_service.get_latest_price_per_asset_type(pairs)
            rows = [
                r for r in rows
                if (latest.get((r["asset"], r.get("instrument_type") or "")) is None)
                or abs(float(r["price"]) - latest[(r["asset"], r.get("instrument_type") or "")]) > _PRICE_CHANGE_TOLERANCE
            ]
        updated = 0
        if rows:
            updated = await market_service.insert_prices_batch_upsert_daily(rows)
        status = get_market_status()
        now = datetime.now(timezone.utc)
        ts_rounded = market_service._round_to_minute(now).isoformat()
        intraday_rows = build_market_intraday_rows_for_open(
            fetched_rows,
            status,
            ts_rounded,
        )
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Market job: %d daily upserted, %d intraday", updated, n)
        elif updated == 0:
            logger.info("Market job: no daily or intraday rows written")
        else:
            logger.info("Market job complete: %d rows upserted (daily)", updated)
    except Exception:
        logger.exception("Market job failed")
