from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from datetime import date, datetime, timezone
from typing import Dict, List, Tuple

from app.core.database import parse_ts
from app.scheduler.base import BaseScraper
from app.scheduler.provider_router import QuoteProvider, QuoteTick, select_best_quotes
from app.scheduler.trading_calendar import (
    get_trading_date,
    get_market_status,
    get_gift_nifty_trading_date,
    is_gift_nifty_open,
    is_trading_day_markets,
    is_exchange_expected_open,
    NSE,
    NYSE,
    LSE,
    XETRA,
    EURONEXT,
    TSE,
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
    "^VIX": "CBOE VIX",
    "XLK": "S&P 500 Tech",
    "XLF": "S&P 500 Financials",
    "XLE": "S&P 500 Energy",
    "^NSEI": "Nifty 50",
    "^BSESN": "Sensex",
    "^NSEBANK": "Nifty Bank",
    "^CRSLDX": "Nifty 500",
    "^CNXIT": "Nifty IT",
    "NIFTYMIDCAP150.NS": "Nifty Midcap 150",
    "NIFTYSMLCAP250.NS": "Nifty Smallcap 250",
    "^CNXAUTO": "Nifty Auto",
    "^CNXPHARMA": "Nifty Pharma",
    "^CNXMETAL": "Nifty Metal",
    "^FTSE": "FTSE 100",
    "^GDAXI": "DAX",
    "^FCHI": "CAC 40",
    "^STOXX50E": "Euro Stoxx 50",
    "^N225": "Nikkei 225",
    "^TOPX": "TOPIX",
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
    "Nifty Auto": NSE,
    "Nifty Pharma": NSE,
    "Nifty Metal": NSE,
    "Gift Nifty": NSE,
    "FTSE 100": LSE,
    "DAX": XETRA,
    "CAC 40": EURONEXT,
    "Euro Stoxx 50": EURONEXT,
    "Nikkei 225": TSE,
    "TOPIX": TSE,
    "CBOE VIX": NYSE,
    "S&P 500 Tech": NYSE,
    "S&P 500 Financials": NYSE,
    "S&P 500 Energy": NYSE,
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


def _pick_previous_close(meta: dict) -> tuple[float | None, str | None]:
    """Pick previous close with market-convention priority.
    Prefer regular session references over chart-derived fallback."""
    candidates = (
        ("regularMarketPreviousClose", meta.get("regularMarketPreviousClose")),
        ("previousClose", meta.get("previousClose")),
        ("chartPreviousClose", meta.get("chartPreviousClose")),
    )
    for key, value in candidates:
        if value is None:
            continue
        try:
            return float(value), key
        except (TypeError, ValueError):
            continue
    return None, None


class MarketScraper(BaseScraper, QuoteProvider):

    def _fetch_all_quotes(self) -> List[Dict]:
        symbols = [*INDEX_SYMBOLS.keys(), *FX_SYMBOLS.keys()]
        records = []
        logger.debug("Fetching Yahoo quotes for %d symbols", len(symbols))
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
                prev, prev_key = _pick_previous_close(meta)
                if prev_key == "chartPreviousClose":
                    logger.debug("Using chartPreviousClose fallback for %s", symbol)
                source_ts = None
                regular_market_time = meta.get("regularMarketTime")
                if regular_market_time is not None:
                    try:
                        source_ts = datetime.fromtimestamp(int(regular_market_time), tz=timezone.utc)
                    except (TypeError, ValueError, OSError):
                        source_ts = None
                records.append({
                    "symbol": symbol,
                    "price": float(price),
                    "prev": prev,
                    "currency": str(meta.get("currency", "USD")),
                    "source_timestamp": source_ts or datetime.now(timezone.utc),
                })
            except Exception:
                logger.warning("Failed to fetch %s", symbol, exc_info=True)
        logger.debug("Yahoo quote fetch complete: %d/%d symbols", len(records), len(symbols))
        return records

    def _parse_indices(self, records: List[Dict]) -> List[QuoteTick]:
        items: list[QuoteTick] = []
        for r in records:
            if r["symbol"] not in INDEX_SYMBOLS:
                continue
            items.append(
                QuoteTick(
                    asset=INDEX_SYMBOLS[r["symbol"]],
                    price=r["price"],
                    instrument_type="index",
                    unit="points",
                    source="yahoo_finance_api",
                    previous_close=r["prev"],
                    change_percent=_pct_change(r["price"], r["prev"]),
                    provider="yahoo",
                    provider_priority=1,
                    confidence_level=0.95,
                    source_timestamp=r["source_timestamp"],
                    quality="primary",
                )
            )
        return items

    def _parse_fx(self, records: List[Dict]) -> List[QuoteTick]:
        items: list[QuoteTick] = []
        for r in records:
            if r["symbol"] not in FX_SYMBOLS:
                continue
            items.append(
                QuoteTick(
                    asset=FX_SYMBOLS[r["symbol"]],
                    price=r["price"],
                    instrument_type="currency",
                    unit="inr",
                    source="yahoo_finance_api",
                    previous_close=r["prev"],
                    change_percent=_pct_change(r["price"], r["prev"]),
                    provider="yahoo",
                    provider_priority=1,
                    confidence_level=0.92,
                    source_timestamp=r["source_timestamp"],
                    quality="primary",
                )
            )
        return items

    def _fetch_gift_nifty(self) -> QuoteTick | None:
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
                        return QuoteTick(
                            asset="Gift Nifty",
                            price=price,
                            instrument_type="index",
                            unit="points",
                            source="giftcitynifty_scrape",
                            change_percent=pct,
                            previous_close=None,
                            provider="giftcitynifty",
                            provider_priority=1,
                            confidence_level=0.9,
                            source_timestamp=datetime.now(timezone.utc),
                            quality="primary",
                            is_predictive=True,
                            session_source="gift_nifty_windows",
                        )
                except ValueError:
                    continue
        except Exception:
            logger.exception("Gift Nifty scrape failed")
        return None

    def _fetch_bond_yields(self) -> List[QuoteTick]:
        items: list[QuoteTick] = []
        for name, series_id in BOND_SERIES:
            try:
                text = self._get_text(FRED_CSV_URL, params={"id": series_id})
                reader = csv.DictReader(io.StringIO(text))
                latest = None
                latest_date = None
                for row in reader:
                    val = row.get(series_id)
                    if val and val != ".":
                        latest = float(val)
                        latest_date = row.get("DATE")
                if latest is None:
                    continue
                try:
                    if latest_date:
                        source_ts = datetime.fromisoformat(str(latest_date)).replace(tzinfo=timezone.utc)
                    else:
                        source_ts = datetime.now(timezone.utc)
                except ValueError:
                    source_ts = datetime.now(timezone.utc)
                items.append(
                    QuoteTick(
                        asset=name,
                        price=latest,
                        instrument_type="bond_yield",
                        unit="percent",
                        source="fred_api",
                        change_percent=None,
                        previous_close=None,
                        provider="fred",
                        provider_priority=1,
                        confidence_level=0.9,
                        source_timestamp=source_ts,
                        quality="primary",
                    )
                )
            except Exception:
                logger.exception("Bond yield failed for %s", series_id)
        logger.debug("FRED bond fetch complete: %d/%d series", len(items), len(BOND_SERIES))
        return items

    def _fetch_fx_fallback(self) -> List[QuoteTick]:
        payload = self._get_json(FX_USD_BASE_URL)
        rates = payload.get("rates", {})
        if not isinstance(rates, dict):
            return []
        inr = rates.get("INR")
        if not inr:
            return []
        inr = float(inr)
        raw_ts = payload.get("time_last_update_unix")
        try:
            source_ts = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc) if raw_ts is not None else datetime.now(timezone.utc)
        except (TypeError, ValueError, OSError):
            source_ts = datetime.now(timezone.utc)
        items: list[QuoteTick] = [
            QuoteTick(
                asset="USD/INR",
                price=inr,
                instrument_type="currency",
                unit="inr",
                source="er_api",
                change_percent=None,
                previous_close=None,
                provider="er_api",
                provider_priority=5,
                confidence_level=0.55,
                source_timestamp=source_ts,
                is_fallback=True,
                quality="fallback",
            )
        ]
        for code, pair in [("EUR", "EUR/INR"), ("GBP", "GBP/INR"), ("JPY", "JPY/INR")]:
            r = rates.get(code)
            if r and float(r) > 0:
                items.append(
                    QuoteTick(
                        asset=pair,
                        price=inr / float(r),
                        instrument_type="currency",
                        unit="inr",
                        source="er_api",
                        change_percent=None,
                        previous_close=None,
                        provider="er_api",
                        provider_priority=5,
                        confidence_level=0.55,
                        source_timestamp=source_ts,
                        is_fallback=True,
                        quality="fallback",
                    )
                )
        return items

    def fetch_quotes(self) -> list[QuoteTick]:
        all_ticks: list[QuoteTick] = []
        try:
            records = self._fetch_all_quotes()
            logger.debug("Yahoo raw records: %d", len(records))
            all_ticks.extend(self._parse_indices(records))
            all_ticks.extend(self._parse_fx(records))
            logger.debug(
                "Yahoo parsed ticks: indices=%d fx=%d",
                len([t for t in all_ticks if t.instrument_type == "index"]),
                len([t for t in all_ticks if t.instrument_type == "currency"]),
            )
        except Exception:
            logger.warning("Yahoo quote fetch failed; using fallbacks", exc_info=True)
            for sym, name in INDEX_SYMBOLS.items():
                item = self._fetch_single_index(sym, name)
                if item:
                    all_ticks.append(item)
            try:
                all_ticks.extend(self._fetch_fx_fallback())
            except Exception:
                logger.exception("FX fallback failed")
        else:
            # Always include fallback for any FX pair missing from Yahoo.
            try:
                all_ticks.extend(self._fetch_fx_fallback())
            except Exception:
                logger.exception("FX fallback failed")

        gift = self._fetch_gift_nifty()
        if gift:
            all_ticks.append(gift)

        try:
            all_ticks.extend(self._fetch_bond_yields())
        except Exception:
            logger.exception("Bond yield fetch failed")
        selected = select_best_quotes(all_ticks)
        logger.debug("Provider routing selected %d/%d ticks", len(selected), len(all_ticks))
        return selected

    def fetch_all(self) -> List[Dict]:
        """Backward-compatible shape for scheduler code."""
        out: list[dict] = []
        for tick in self.fetch_quotes():
            out.append({
                "asset": tick.asset,
                "price": tick.price,
                "instrument_type": tick.instrument_type,
                "unit": tick.unit,
                "source": tick.source,
                "change_percent": tick.change_percent,
                "previous_close": tick.previous_close,
                "source_timestamp": tick.source_timestamp.isoformat(),
                "provider": tick.provider,
                "provider_priority": tick.provider_priority,
                "confidence_level": tick.confidence_level,
                "is_fallback": tick.is_fallback,
                "quality": tick.quality,
                "is_predictive": tick.is_predictive,
                "session_source": tick.session_source,
            })
        return out

    def _fetch_single_index(self, symbol: str, name: str) -> QuoteTick | None:
        try:
            payload = self._get_json(YAHOO_CHART_URL.format(symbol=symbol))
            result = payload.get("chart", {}).get("result", [])
            if not result:
                return None
            meta = result[0].get("meta", {})
            price = meta.get("regularMarketPrice") or meta.get("previousClose")
            if price is None:
                return None
            prev, prev_key = _pick_previous_close(meta)
            if prev_key == "chartPreviousClose":
                logger.debug("Using chartPreviousClose fallback for %s", symbol)
            p = float(price)
            pv = prev
            raw_ts = meta.get("regularMarketTime")
            try:
                source_ts = datetime.fromtimestamp(int(raw_ts), tz=timezone.utc) if raw_ts is not None else datetime.now(timezone.utc)
            except (TypeError, ValueError, OSError):
                source_ts = datetime.now(timezone.utc)
            return QuoteTick(
                asset=name,
                price=p,
                instrument_type="index",
                unit="points",
                source="yahoo_chart_api",
                change_percent=_pct_change(p, pv),
                previous_close=pv,
                provider="yahoo",
                provider_priority=1,
                confidence_level=0.9,
                source_timestamp=source_ts,
                quality="primary",
            )
        except Exception:
            return None


_scraper = MarketScraper()


def _fetch_yahoo_1m_bars(symbol: str, range_period: str = "2d") -> tuple[list[tuple[datetime, float]], str]:
    """Fetch 1-minute OHLC bars from Yahoo Chart API.
    Returns (list of (utc_datetime, close_price), currency_code). Returns ([], 'USD') on failure."""
    try:
        url = YAHOO_CHART_URL.format(symbol=symbol)
        payload = _scraper._get_json(url, params={"interval": "1m", "range": range_period})
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
    market_rows: list[dict],
    trading_date_by_exchange: dict[str, date | list[date] | set[date]],
    trading_date_by_instrument: dict[str, date | list[date] | set[date]] | None = None,
) -> list[dict]:
    """Build full minute-level intraday rows for target sessions using Yahoo 1m chart data.
    For assets with a Yahoo symbol we fetch 1m bars and filter to the given target date(s);
    instrument-level date targets (e.g. currency 24/7 windows) override exchange targets.
    for others (Gift Nifty, bonds) we add one point from market_rows."""
    from app.services import market_service as svc

    def _normalize_dates(v: date | list[date] | set[date] | None) -> set[date]:
        if v is None:
            return set()
        if isinstance(v, date):
            return {v}
        return set(v)

    rows_out = []
    instrument_targets = trading_date_by_instrument or {}
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
        target_dates = _normalize_dates(instrument_targets.get(instrument_type))
        if not target_dates:
            exchange = ASSET_EXCHANGE.get(asset_name, NYSE)
            target_dates = _normalize_dates(trading_date_by_exchange.get(exchange))
        if not target_dates:
            continue
        bars, _ = _fetch_yahoo_1m_bars(sym, range_period="7d")
        for dt, close in bars:
            if dt.date() not in target_dates:
                continue
            ts_rounded = svc._round_to_minute(dt).isoformat()
            rows_out.append({
                "asset": asset_name,
                "instrument_type": instrument_type,
                "price": close,
                "timestamp": ts_rounded,
                "source_timestamp": ts_rounded,
                "provider": "yahoo_1m",
                "provider_priority": 1,
                "confidence_level": 0.95,
                "is_fallback": False,
                "quality": "primary",
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
            "source_timestamp": ts,
            "provider": r.get("provider") or "unknown",
            "provider_priority": r.get("provider_priority") or 99,
            "confidence_level": r.get("confidence_level"),
            "is_fallback": bool(r.get("is_fallback")) if r.get("is_fallback") is not None else False,
            "quality": r.get("quality"),
            "is_predictive": bool(r.get("is_predictive")) if r.get("is_predictive") is not None else False,
            "session_source": r.get("session_source"),
        })
    return rows_out


def _fetch_market_rows_sync() -> tuple[List[Dict], bool]:
    """Sync scrape; run in thread executor. Returns (rows, calendar_says_trading_day).
    Each row's timestamp is the exchange trading date (NSE/NYSE) so Monday's close is not stored as Tuesday."""
    now = _scraper.utc_now()
    calendar_open = is_trading_day_markets(now) or is_gift_nifty_open(now)
    items = _scraper.fetch_all()
    logger.debug(
        "Fetched market rows sync: now=%s calendar_open=%s raw_items=%d",
        now.isoformat(),
        calendar_open,
        len(items),
    )
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
            "source_timestamp": it.get("source_timestamp"),
            "provider": it.get("provider"),
            "provider_priority": it.get("provider_priority"),
            "confidence_level": it.get("confidence_level"),
            "is_fallback": it.get("is_fallback"),
            "quality": it.get("quality"),
            "is_predictive": it.get("is_predictive"),
            "session_source": it.get("session_source"),
        })
    logger.debug("Prepared market rows for persistence: %d", len(rows))
    return (rows, calendar_open)


_PRICE_CHANGE_TOLERANCE = 1e-9


def _num_changed(a, b, tolerance: float = _PRICE_CHANGE_TOLERANCE) -> bool:
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    try:
        return abs(float(a) - float(b)) > tolerance
    except (TypeError, ValueError):
        return True


def _daily_row_changed(new_row: dict, latest: dict | None) -> bool:
    if latest is None:
        return True
    return (
        _num_changed(new_row.get("price"), latest.get("price"))
        or _num_changed(new_row.get("previous_close"), latest.get("previous_close"))
        or _num_changed(new_row.get("change_percent"), latest.get("change_percent"))
    )


def build_market_intraday_rows_for_open(
    market_rows: list[dict],
    status: dict,
) -> list[dict]:
    """Build intraday rows for 1D chart.
    Currencies are written every run (24/7 behavior).
    Gift Nifty follows dedicated Gift Nifty session status.
    Other indices/bonds follow exchange open status."""
    intraday_rows = []
    now_utc = datetime.now(timezone.utc)
    for r in market_rows:
        instrument_type = r.get("instrument_type") or "index"
        exchange = ASSET_EXCHANGE.get(r["asset"], NYSE)
        include = False
        if instrument_type == "currency":
            include = True
        elif r.get("asset") == "Gift Nifty":
            include = bool(status.get("gift_nifty_open"))
        elif exchange in {NSE, NYSE, LSE, XETRA, EURONEXT, TSE}:
            include = is_exchange_expected_open(exchange, now_utc, status=status)
        if include:
            source_dt = parse_ts(r.get("source_timestamp")) if r.get("source_timestamp") else None
            if source_dt is None:
                source_dt = datetime.now(timezone.utc)
            ts_rounded = market_service._round_to_minute(source_dt).isoformat()
            intraday_rows.append({
                "asset": r["asset"],
                "instrument_type": instrument_type,
                "price": r["price"],
                "timestamp": ts_rounded,
                "source_timestamp": ts_rounded,
                "provider": r.get("provider") or "unknown",
                "provider_priority": r.get("provider_priority") or 99,
                "confidence_level": r.get("confidence_level"),
                "is_fallback": bool(r.get("is_fallback")) if r.get("is_fallback") is not None else False,
                "quality": r.get("quality"),
                "is_predictive": bool(r.get("is_predictive")) if r.get("is_predictive") is not None else False,
                "session_source": r.get("session_source"),
            })
    logger.debug(
        "Built market intraday rows: input=%d output=%d nse_open=%s nyse_open=%s gift_nifty_open=%s",
        len(market_rows),
        len(intraday_rows),
        bool(status.get("nse_open")),
        bool(status.get("nyse_open")),
        bool(status.get("gift_nifty_open")),
    )
    return intraday_rows


async def run_market_job() -> None:
    try:
        logger.debug("Market job cycle started")
        loop = asyncio.get_event_loop()
        fetched_rows, calendar_says_open = await loop.run_in_executor(None, _fetch_market_rows_sync)
        logger.debug("Market job fetched_rows=%d calendar_says_open=%s", len(fetched_rows), calendar_says_open)
        if not fetched_rows:
            logger.debug("Market job exiting early: no fetched rows")
            return
        rows = fetched_rows
        if not calendar_says_open:
            before = len(rows)
            pairs = [(r["asset"], r.get("instrument_type") or "") for r in rows]
            latest = await market_service.get_latest_daily_snapshot_per_asset_type(pairs)
            rows = [
                r for r in rows
                if _daily_row_changed(r, latest.get((r["asset"], r.get("instrument_type") or "")))
            ]
            logger.debug("Market job filtered unchanged rows while calendar closed: %d -> %d", before, len(rows))
        updated = 0
        if rows:
            updated = await market_service.insert_prices_batch_upsert_daily(rows)
        logger.debug("Market job daily rows written=%d", updated)
        status = get_market_status()
        intraday_rows = build_market_intraday_rows_for_open(
            fetched_rows,
            status,
        )
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Market job: %d daily upserted, %d intraday", updated, n)
            logger.debug("Market job intraday rows attempted=%d inserted_or_updated=%d", len(intraday_rows), n)
        elif updated == 0:
            logger.info("Market job: no daily or intraday rows written")
        else:
            logger.info("Market job complete: %d rows upserted (daily)", updated)
        logger.debug("Market job cycle completed")
    except Exception:
        logger.exception("Market job failed")
