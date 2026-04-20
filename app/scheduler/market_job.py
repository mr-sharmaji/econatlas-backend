from __future__ import annotations

import asyncio
import csv
import html
import io
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Tuple

import requests

from app.core.database import parse_ts
from app.core.config import get_settings
from app.scheduler.base import BaseScraper
from app.scheduler.job_executors import get_job_executor
from app.scheduler.provider_router import QuoteProvider, QuoteTick, select_best_quotes
from app.scheduler.trading_calendar import (
    get_trading_date,
    get_market_status,
    get_gift_nifty_trading_date,
    is_gift_nifty_open,
    is_trading_day_markets,
    is_exchange_expected_open,
    is_fx_session_expected_open,
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


def _yahoo_chart_url(symbol: str) -> str:
    # Route through Cloudflare Worker when INTRADAY_YAHOO_PROXY_URL is set —
    # Yahoo blanket-429s datacenter IPs on direct calls.
    proxy = os.environ.get("INTRADAY_YAHOO_PROXY_URL", "").strip()
    if proxy:
        return proxy.rstrip("/") + f"/v8/finance/chart/{symbol}"
    return YAHOO_CHART_URL.format(symbol=symbol)


# Yahoo's /v8/chart serves today's 1m bars only from query-time forward.
# Morning bars (index open → ~09:30 UTC on a fresh proxy IP) are returned
# as null closes, so the backfill writes zero. Upstox's historical-candle
# /intraday endpoint serves every 1m bar for the current NSE/BSE session,
# so we pull index data from there and layer it on top of the Yahoo rows.
_UPSTOX_INSTRUMENTS_CSV_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
)
_UPSTOX_INTRADAY_URL_TMPL = (
    "https://api.upstox.com/v2/historical-candle/intraday/{key}/1minute"
)
_upstox_index_cache: dict[str, str] = {}
_upstox_index_cache_ts: float = 0.0

# Our display name → upstox `name` column. Used when upstox spells a
# known index differently ("BSE SENSEX" vs "Sensex", "NIFTY SMLCAP 250"
# vs "Nifty Smallcap 250"). Every entry here must resolve in the CSV.
_UPSTOX_INDEX_NAME_ALIASES = {
    "Sensex": "BSE SENSEX",
    "Nifty Smallcap 250": "NIFTY SMLCAP 250",
}


def _load_upstox_index_map() -> dict[str, str]:
    global _upstox_index_cache, _upstox_index_cache_ts
    now = time.monotonic()
    if _upstox_index_cache and now - _upstox_index_cache_ts < 24 * 3600:
        return _upstox_index_cache
    try:
        import csv as _csv
        import gzip as _gzip
        import io as _io
        resp = requests.get(_UPSTOX_INSTRUMENTS_CSV_URL, timeout=30)
        resp.raise_for_status()
        raw = _gzip.decompress(resp.content).decode("utf-8", errors="replace")
        rdr = _csv.reader(_io.StringIO(raw))
        next(rdr)  # header
        mapping: dict[str, str] = {}
        for row in rdr:
            if not row or not row[0].startswith(("NSE_INDEX", "BSE_INDEX")):
                continue
            key = row[0]
            name = (row[3] if len(row) > 3 else "").strip()
            if name:
                mapping.setdefault(name.casefold(), key)
        if mapping:
            _upstox_index_cache = mapping
            _upstox_index_cache_ts = now
            logger.info("market: upstox index map loaded, %d indices", len(mapping))
    except Exception as exc:
        logger.warning("market: upstox index map load failed: %s", exc)
    return _upstox_index_cache


def _fetch_upstox_index_bars_today(instrument_key: str) -> list[tuple[datetime, float, int | None]]:
    import urllib.parse
    url = _UPSTOX_INTRADAY_URL_TMPL.format(key=urllib.parse.quote(instrument_key, safe=""))
    try:
        resp = requests.get(url, timeout=10, headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug("market: upstox index fetch failed for %s: %s", instrument_key, exc)
        return []
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    candles = (payload.get("data") or {}).get("candles") or []
    out: list[tuple[datetime, float, int | None]] = []
    for row in candles:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts_str = row[0]
            close = float(row[4])
            volume_raw = row[5]
            volume = int(volume_raw) if volume_raw is not None else None
        except (TypeError, ValueError, IndexError):
            continue
        try:
            ts_utc = datetime.fromisoformat(ts_str).astimezone(timezone.utc)
        except (TypeError, ValueError):
            continue
        out.append((ts_utc, close, volume))
    return out
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FX_USD_BASE_URL = "https://open.er-api.com/v6/latest/USD"
GIFT_NIFTY_URL = "https://giftcitynifty.com/gift-nifty-intraday-price-data/"
GOOGLE_FINANCE_QUOTE_URL = "https://www.google.com/finance/quote/{code}"

INDEX_SYMBOLS = {
    "^GSPC": "S&P500",
    "^IXIC": "NASDAQ",
    "^NDX": "Nasdaq 100",
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
    "^INDIAVIX": "India VIX",
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
    "AUDINR=X": "AUD/INR",
    "CADINR=X": "CAD/INR",
    "CHFINR=X": "CHF/INR",
    "CNYINR=X": "CNY/INR",
    "SGDINR=X": "SGD/INR",
    "HKDINR=X": "HKD/INR",
    "KRWINR=X": "KRW/INR",
    "AEDINR=X": "AED/INR",
    "NZDINR=X": "NZD/INR",
    "SARINR=X": "SAR/INR",
    "THBINR=X": "THB/INR",
    "MYRINR=X": "MYR/INR",
    "IDRINR=X": "IDR/INR",
    "PHPINR=X": "PHP/INR",
    "ZARINR=X": "ZAR/INR",
    "BRLINR=X": "BRL/INR",
    "MXNINR=X": "MXN/INR",
    "QARINR=X": "QAR/INR",
    "KWDINR=X": "KWD/INR",
    "BHDINR=X": "BHD/INR",
    "OMRINR=X": "OMR/INR",
    "ILSINR=X": "ILS/INR",
    "SEKINR=X": "SEK/INR",
    "NOKINR=X": "NOK/INR",
    "DKKINR=X": "DKK/INR",
    "PLNINR=X": "PLN/INR",
    "TRYINR=X": "TRY/INR",
    "TWDINR=X": "TWD/INR",
    "VNDINR=X": "VND/INR",
    "BDTINR=X": "BDT/INR",
    "LKRINR=X": "LKR/INR",
    "PKRINR=X": "PKR/INR",
    "NPRINR=X": "NPR/INR",
}

# Known Yahoo chart endpoints that return 404. We keep these pairs in the
# catalog and fill them via fallback provider (ER API) instead of failing noisy.
UNSUPPORTED_YAHOO_FX_SYMBOLS = {
    "SARINR=X",
    "MXNINR=X",
    "QARINR=X",
    "KWDINR=X",
    "BHDINR=X",
    "OMRINR=X",
    "ILSINR=X",
    "SEKINR=X",
    "NOKINR=X",
    "DKKINR=X",
    "PLNINR=X",
    "TRYINR=X",
    "TWDINR=X",
    "VNDINR=X",
    "BDTINR=X",
    "LKRINR=X",
    "PKRINR=X",
    "NPRINR=X",
}

BOND_SERIES: List[Tuple[str, str]] = [
    ("US 10Y Treasury Yield", "DGS10"),
    ("US 2Y Treasury Yield", "DGS2"),
    ("India 10Y Bond Yield", "INDIRLTLT01STM"),
    ("Germany 10Y Bond Yield", "IRLTLT01DEM156N"),
    ("Japan 10Y Bond Yield", "IRLTLT01JPM156N"),
]

GOOGLE_INDEX_FALLBACKS: dict[str, dict[str, str]] = {
    "S&P500": {"code": ".INX:INDEXSP", "token": '".INX","INDEXSP"'},
    "NASDAQ": {"code": ".IXIC:INDEXNASDAQ", "token": '".IXIC","INDEXNASDAQ"'},
    "Nasdaq 100": {"code": "NDX:INDEXNASDAQ", "token": '"NDX","INDEXNASDAQ"'},
    "Dow Jones": {"code": ".DJI:INDEXDJX", "token": '".DJI","INDEXDJX"'},
    "CBOE VIX": {"code": "VIX:INDEXCBOE", "token": '"VIX","INDEXCBOE"'},
    "S&P 500 Tech": {"code": "XLK:NYSEARCA", "token": '"XLK","NYSEARCA"'},
    "S&P 500 Financials": {"code": "XLF:NYSEARCA", "token": '"XLF","NYSEARCA"'},
    "S&P 500 Energy": {"code": "XLE:NYSEARCA", "token": '"XLE","NYSEARCA"'},
    "Nifty 50": {"code": "NIFTY_50:INDEXNSE", "token": '"NIFTY_50","INDEXNSE"'},
    "Sensex": {
        "code": "SENSEX:INDEXBOM",
        "token": '"SENSEX","INDEXBOM"',
    },
    "Nifty Bank": {"code": "NIFTY_BANK:INDEXNSE", "token": '"NIFTY_BANK","INDEXNSE"'},
    "Nifty 500": {"code": "NIFTY_500:INDEXNSE", "token": '"NIFTY_500","INDEXNSE"'},
    "Nifty IT": {"code": "NIFTY_IT:INDEXNSE", "token": '"NIFTY_IT","INDEXNSE"'},
    "Nifty Midcap 150": {"code": "NIFTY_MIDCAP_150:INDEXNSE", "token": '"NIFTY_MIDCAP_150","INDEXNSE"'},
    "Nifty Smallcap 250": {"code": "NIFTY_SMALLCAP_250:INDEXNSE", "token": '"NIFTY_SMALLCAP_250","INDEXNSE"'},
    "Nifty Auto": {"code": "NIFTY_AUTO:INDEXNSE", "token": '"NIFTY_AUTO","INDEXNSE"'},
    "Nifty Pharma": {"code": "NIFTY_PHARMA:INDEXNSE", "token": '"NIFTY_PHARMA","INDEXNSE"'},
    "Nifty Metal": {"code": "NIFTY_METAL:INDEXNSE", "token": '"NIFTY_METAL","INDEXNSE"'},
    "India VIX": {"code": "INDIA_VIX:INDEXNSE", "token": '"INDIA_VIX","INDEXNSE"'},
    "FTSE 100": {"code": "UKX:INDEXFTSE", "token": '"UKX","INDEXFTSE"'},
    "DAX": {"code": "DAX:INDEXDB", "token": '"DAX","INDEXDB"'},
    "CAC 40": {"code": "PX1:INDEXEURO", "token": '"PX1","INDEXEURO"'},
    "Euro Stoxx 50": {"code": "SX5E:INDEXSTOXX", "token": '"SX5E","INDEXSTOXX"'},
    "Nikkei 225": {"code": "NIKKEI_225:INDEXNIKKEI", "token": '"NIKKEI_225","INDEXNIKKEI"'},
    "TOPIX": {"code": "TOPIX:INDEXTOPIX", "token": '"TOPIX","INDEXTOPIX"'},
}
INDEX_FALLBACK_MAX_CLOCK_SKEW_SECONDS = 180
FX_FALLBACK_MAX_CLOCK_SKEW_SECONDS = 180
FX_FALLBACK_MIN_FRESHNESS_GAIN_SECONDS = 120
FX_SANITY_MAX_DEVIATION_PCT = 20.0

# Asset → exchange for correct trading-date assignment (avoid Monday close stored as Tuesday UTC)
ASSET_EXCHANGE: Dict[str, str] = {
    "S&P500": NYSE,
    "NASDAQ": NYSE,
    "Nasdaq 100": NYSE,
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
    "India VIX": NSE,
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
    "AUD/INR": NYSE,
    "CAD/INR": NYSE,
    "CHF/INR": NYSE,
    "CNY/INR": NYSE,
    "SGD/INR": NYSE,
    "HKD/INR": NYSE,
    "KRW/INR": NYSE,
    "AED/INR": NYSE,
    "NZD/INR": NYSE,
    "SAR/INR": NYSE,
    "THB/INR": NYSE,
    "MYR/INR": NYSE,
    "IDR/INR": NYSE,
    "PHP/INR": NYSE,
    "ZAR/INR": NYSE,
    "BRL/INR": NYSE,
    "MXN/INR": NYSE,
    "QAR/INR": NYSE,
    "KWD/INR": NYSE,
    "BHD/INR": NYSE,
    "OMR/INR": NYSE,
    "ILS/INR": NYSE,
    "SEK/INR": NYSE,
    "NOK/INR": NYSE,
    "DKK/INR": NYSE,
    "PLN/INR": NYSE,
    "TRY/INR": NYSE,
    "TWD/INR": NYSE,
    "VND/INR": NYSE,
    "BDT/INR": NYSE,
    "LKR/INR": NYSE,
    "PKR/INR": NYSE,
    "NPR/INR": NYSE,
    "India 10Y Bond Yield": NSE,
    "US 10Y Treasury Yield": NYSE,
    "US 2Y Treasury Yield": NYSE,
    "Germany 10Y Bond Yield": XETRA,
    "Japan 10Y Bond Yield": TSE,
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


def _within_post_close_grace(exchange: str, now_utc: datetime, tick_ts: datetime | None) -> bool:
    if tick_ts is None:
        return False
    grace_seconds = max(0, int(get_settings().session_post_close_grace_seconds))
    if grace_seconds == 0:
        return False
    ts = tick_ts if tick_ts.tzinfo is not None else tick_ts.replace(tzinfo=timezone.utc)
    now = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    age_seconds = (now - ts).total_seconds()
    if age_seconds < 0:
        age_seconds = 0
    if age_seconds > grace_seconds:
        return False
    try:
        return get_trading_date(now, exchange) == get_trading_date(ts, exchange)
    except Exception:
        return True


class MarketScraper(BaseScraper, QuoteProvider):
    def _effective_index_fallback_threshold_seconds(self, asset: str, base_threshold: int) -> int:
        _ = asset
        return max(1, int(base_threshold))

    def _promote_delayed_index_fallbacks(
        self,
        selected: list[QuoteTick],
        all_ticks: list[QuoteTick],
    ) -> list[QuoteTick]:
        """If an open-session index feed is delayed, prefer fallback provider ticks.

        Some free primary feeds can lag during active sessions. Once delay
        crosses promote threshold, use fallback to keep quotes live.
        """
        now = datetime.now(timezone.utc)
        status = get_market_status(now)
        cfg = get_settings()
        promote_threshold = max(1, int(cfg.effective_session_live_max_age_seconds()))

        fallback_by_asset: dict[str, QuoteTick] = {}
        for tick in all_ticks:
            if tick.instrument_type != "index" or tick.provider == "yahoo":
                continue
            prev = fallback_by_asset.get(tick.asset)
            if prev is None:
                fallback_by_asset[tick.asset] = tick
                continue
            prev_ts = prev.source_timestamp
            cur_ts = tick.source_timestamp
            if prev_ts.tzinfo is None:
                prev_ts = prev_ts.replace(tzinfo=timezone.utc)
            if cur_ts.tzinfo is None:
                cur_ts = cur_ts.replace(tzinfo=timezone.utc)
            if tick.provider_priority > prev.provider_priority:
                continue
            if tick.provider_priority < prev.provider_priority or cur_ts > prev_ts:
                fallback_by_asset[tick.asset] = tick

        promoted: list[QuoteTick] = []
        for tick in selected:
            if tick.instrument_type != "index":
                promoted.append(tick)
                continue
            if tick.provider != "yahoo":
                promoted.append(tick)
                continue

            exchange = ASSET_EXCHANGE.get(tick.asset, NYSE)
            exchange_open = is_exchange_expected_open(exchange, now, status=status)
            if not exchange_open and not _within_post_close_grace(exchange, now, tick.source_timestamp):
                promoted.append(tick)
                continue

            tick_ts = tick.source_timestamp
            if tick_ts.tzinfo is None:
                tick_ts = tick_ts.replace(tzinfo=timezone.utc)
            age_seconds = (now - tick_ts).total_seconds()
            threshold = self._effective_index_fallback_threshold_seconds(tick.asset, promote_threshold)
            if age_seconds <= threshold:
                promoted.append(tick)
                continue

            fb = fallback_by_asset.get(tick.asset)
            if fb is None:
                cfg = GOOGLE_INDEX_FALLBACKS.get(tick.asset)
                if cfg is not None:
                    try:
                        fetched = self._fetch_google_index_fallback_for_asset(tick.asset, cfg, now)
                    except Exception:
                        logger.debug("On-demand index fallback fetch failed for %s", tick.asset, exc_info=True)
                        fetched = None
                    if fetched is not None:
                        fallback_by_asset[tick.asset] = fetched
                        fb = fetched
            if fb is None or fb.provider == "yahoo":
                promoted.append(tick)
                continue

            fb_ts = fb.source_timestamp
            if fb_ts.tzinfo is None:
                fb_ts = fb_ts.replace(tzinfo=timezone.utc)
            if fb_ts <= tick_ts:
                promoted.append(tick)
                continue
            logger.info(
                "Promoted delayed index to fallback: asset=%s age_seconds=%.1f primary_ts=%s fallback_provider=%s fallback_ts=%s",
                tick.asset,
                age_seconds,
                tick_ts.isoformat(),
                fb.provider,
                fb_ts.isoformat(),
            )
            promoted.append(fb)

        return promoted

    def _apply_fx_sanity_guard(
        self,
        selected: list[QuoteTick],
        all_ticks: list[QuoteTick],
    ) -> list[QuoteTick]:
        """Replace clearly anomalous FX ticks with ER API reference ticks.

        Yahoo/Google can occasionally emit inverted/scaled INR crosses
        (observed with IDR/INR and MYR/INR). We keep primary feeds by default,
        but if deviation versus ER reference is abnormally large, switch to ER
        for that cycle.
        """
        er_ref: dict[str, QuoteTick] = {}
        for tick in all_ticks:
            if tick.instrument_type != "currency" or tick.provider != "er_api":
                continue
            prev = er_ref.get(tick.asset)
            if prev is None:
                er_ref[tick.asset] = tick
                continue
            prev_ts = prev.source_timestamp
            cur_ts = tick.source_timestamp
            if prev_ts.tzinfo is None:
                prev_ts = prev_ts.replace(tzinfo=timezone.utc)
            if cur_ts.tzinfo is None:
                cur_ts = cur_ts.replace(tzinfo=timezone.utc)
            if cur_ts > prev_ts:
                er_ref[tick.asset] = tick

        guarded: list[QuoteTick] = []
        for tick in selected:
            if tick.instrument_type != "currency" or tick.provider == "er_api":
                guarded.append(tick)
                continue
            ref = er_ref.get(tick.asset)
            if ref is None or ref.price <= 0 or tick.price <= 0:
                guarded.append(tick)
                continue
            deviation_pct = abs((tick.price - ref.price) / ref.price) * 100.0
            if deviation_pct <= FX_SANITY_MAX_DEVIATION_PCT:
                guarded.append(tick)
                continue
            logger.warning(
                "FX sanity guard replaced quote: asset=%s provider=%s price=%s ref=%s deviation_pct=%.2f",
                tick.asset,
                tick.provider,
                tick.price,
                ref.price,
                deviation_pct,
            )
            guarded.append(ref)
        return guarded

    def _fetch_all_quotes(self) -> List[Dict]:
        symbols = [*INDEX_SYMBOLS.keys(), *FX_SYMBOLS.keys()]
        records = []
        logger.debug("Fetching Yahoo quotes for %d symbols", len(symbols))
        for symbol in symbols:
            if symbol in UNSUPPORTED_YAHOO_FX_SYMBOLS:
                logger.debug("Skipping Yahoo quote fetch for unsupported symbol %s", symbol)
                continue
            try:
                payload = self._get_json(_yahoo_chart_url(symbol))
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
            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 404 and symbol in UNSUPPORTED_YAHOO_FX_SYMBOLS:
                    logger.debug("Yahoo symbol not found (%s), fallback provider will be used", symbol)
                    continue
                logger.warning("Failed to fetch %s", symbol, exc_info=True)
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
        """Scrape giftcitynifty.com and return the latest tick as a QuoteTick.

        Also populates ``self._gift_nifty_intraday_backfill`` with one
        intraday-row dict for EVERY row on the page (the site typically
        serves hundreds of minute-level rows for the current session),
        so a single successful scrape can backfill the full day's
        intraday chart. The caller consumes this list in
        ``_fetch_market_rows_sync`` and writes to market_prices_intraday
        via ``insert_intraday_batch``; the ``(asset, instrument_type,
        source_timestamp, provider)`` upsert key dedups any rows that
        were already in the table from a prior scrape, so replaying
        unchanged history is free.

        The backfill list is reset at the start of every call so a
        transient scrape failure doesn't leave stale data around.
        """
        self._gift_nifty_intraday_backfill = []
        latest_tick: QuoteTick | None = None
        try:
            def _clean_cell(raw: str) -> str:
                return html.unescape(re.sub(r"<[^>]+>", "", raw or "")).strip()

            def _parse_num(raw: str) -> float | None:
                text = _clean_cell(raw).replace(",", "").replace("%", "")
                text = text.replace("−", "-").replace("–", "-")
                m = re.search(r"[-+]?\d*\.?\d+", text)
                if not m:
                    return None
                try:
                    return float(m.group(0))
                except (TypeError, ValueError):
                    return None

            page_html = self._get_text(GIFT_NIFTY_URL)
            tables = re.findall(r"<table[^>]*>([\s\S]*?)</table>", page_html)
            if not tables:
                return None
            rows = re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", tables[0])
            now_utc = datetime.now(timezone.utc)
            now_ist = now_utc.astimezone(timezone(timedelta(hours=5, minutes=30)))
            for row in rows:
                cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", row)
                if len(cells) < 5:
                    continue
                price = _parse_num(cells[1])
                change = _parse_num(cells[2])
                pct = _parse_num(cells[3])
                direction = _clean_cell(cells[4]).lower()
                if price is None or price <= 0:
                    continue
                is_down = ("↓" in direction) or ("darr" in direction) or ("down" in direction)
                is_up = ("↑" in direction) or ("uarr" in direction) or ("up" in direction)

                # Site can show unsigned numbers with arrow direction.
                if change is not None:
                    if change > 0 and is_down:
                        change = -change
                    elif change < 0 and is_up:
                        change = abs(change)

                if pct is not None:
                    if pct > 0 and is_down:
                        pct = -pct
                    elif pct < 0 and is_up:
                        pct = abs(pct)

                previous_close = None
                if change is not None:
                    previous_close = round(price - change, 2)
                elif pct is not None and pct != -100:
                    previous_close = round(price / (1 + (pct / 100.0)), 2)

                if pct is None and previous_close not in (None, 0):
                    pct = round(((price - previous_close) / previous_close) * 100, 2)

                hhmm = _clean_cell(cells[0])
                source_ts = now_utc
                m_time = re.search(r"(\d{1,2}):(\d{2})", hhmm)
                if m_time:
                    hh = int(m_time.group(1))
                    mm = int(m_time.group(2))
                    local_ts = now_ist.replace(hour=hh, minute=mm, second=0, microsecond=0)
                    if local_ts > (now_ist + timedelta(minutes=5)):
                        local_ts -= timedelta(days=1)
                    source_ts = local_ts.astimezone(timezone.utc)
                else:
                    # No parseable HH:MM → skip intraday row, since we
                    # have no real source_timestamp. The first such row
                    # still feeds the daily QuoteTick below.
                    if latest_tick is None:
                        latest_tick = QuoteTick(
                            asset="Gift Nifty",
                            price=price,
                            instrument_type="index",
                            unit="points",
                            source="giftcitynifty_scrape",
                            change_percent=pct,
                            previous_close=previous_close,
                            provider="giftcitynifty",
                            provider_priority=1,
                            confidence_level=0.9,
                            source_timestamp=source_ts,
                            quality="primary",
                            is_predictive=True,
                            session_source="gift_nifty_windows",
                        )
                    continue

                # First valid row becomes the "latest" QuoteTick driving
                # the daily market_prices upsert.
                if latest_tick is None:
                    latest_tick = QuoteTick(
                        asset="Gift Nifty",
                        price=price,
                        instrument_type="index",
                        unit="points",
                        source="giftcitynifty_scrape",
                        change_percent=pct,
                        previous_close=previous_close,
                        provider="giftcitynifty",
                        provider_priority=1,
                        confidence_level=0.9,
                        source_timestamp=source_ts,
                        quality="primary",
                        is_predictive=True,
                        session_source="gift_nifty_windows",
                    )

                # Every row (including row 0) becomes an intraday tick
                # for the backfill list. The upsert key is (asset,
                # instrument_type, source_timestamp, provider), so
                # repeated scrapes collapse to a single row per minute
                # regardless of how many times they re-appear on the
                # source page.
                ts_iso = source_ts.isoformat()
                self._gift_nifty_intraday_backfill.append({
                    "asset": "Gift Nifty",
                    "instrument_type": "index",
                    "price": price,
                    "timestamp": ts_iso,
                    "source_timestamp": ts_iso,
                    "provider": "giftcitynifty",
                    "provider_priority": 1,
                    "confidence_level": 0.9,
                    "is_fallback": False,
                    "quality": "primary",
                    "is_predictive": True,
                    "session_source": "gift_nifty_windows",
                })
        except Exception:
            logger.exception("Gift Nifty scrape failed")
            return None
        return latest_tick

    def _parse_google_index_quote(self, page_html: str, token: str) -> tuple[float, float | None, float | None, datetime] | None:
        # Google Finance inline payload shape:
        # ["/m/...",[TOKEN],"...",[price,change,pct,...],..., [source_ts], ...]
        pattern = re.compile(
            rf'\["/[^"]+",\[{re.escape(token)}\][\s\S]{{0,520}}?'
            r"\[\s*(?P<price>-?\d+(?:\.\d+)?)\s*,\s*(?P<chg>-?\d+(?:\.\d+)?)\s*,\s*(?P<pct>-?\d+(?:\.\d+)?)\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\]"
            r"[\s\S]{0,260}?\[(?P<ts>\d{10})\]",
            re.IGNORECASE,
        )
        m = pattern.search(page_html)
        if not m:
            return None
        try:
            price = float(m.group("price"))
            chg = float(m.group("chg"))
            pct = float(m.group("pct"))
            ts = datetime.fromtimestamp(int(m.group("ts")), tz=timezone.utc)
            block = page_html[m.start():m.end() + 180]
            m_prev = re.search(r"\]\s*,\s*null\s*,\s*(-?\d+(?:\.\d+)?)\s*,", block)
            prev = float(m_prev.group(1)) if m_prev else round(price - chg, 4)
            return (price, prev, pct, ts)
        except (TypeError, ValueError, OSError):
            return None

    def _parse_google_fx_quote(
        self,
        page_html: str,
        base_ccy: str,
        quote_ccy: str,
    ) -> tuple[float, float | None, float | None, datetime] | None:
        # Google Finance FX shape appears under labels like "BRL / INR".
        # Some pages can present the reverse pair (e.g., "INR / JPY"), so we
        # support inversion when needed.
        candidates = (
            (f"{base_ccy} / {quote_ccy}", False),
            (f"{quote_ccy} / {base_ccy}", True),
        )
        for label, inverted in candidates:
            pattern = re.compile(
                rf'"{re.escape(label)}"[\s\S]{{0,260}}?'
                r"\[(?P<price>-?\d+(?:\.\d+)?)\s*,\s*(?P<chg>-?\d+(?:\.\d+)?)\s*,\s*(?P<pct>-?\d+(?:\.\d+)?)\s*,\s*\d+\s*,\s*\d+\s*,\s*\d+\s*\]"
                r"[\s\S]{0,240}?\[(?P<ts>\d{10})\]",
                re.IGNORECASE,
            )
            m = pattern.search(page_html)
            if not m:
                continue
            try:
                raw_price = float(m.group("price"))
                raw_chg = float(m.group("chg"))
                raw_prev = raw_price - raw_chg
                ts = datetime.fromtimestamp(int(m.group("ts")), tz=timezone.utc)
                if raw_price <= 0:
                    continue
                if not inverted:
                    prev = raw_prev if raw_prev > 0 else None
                    pct = _pct_change(raw_price, prev)
                    return raw_price, prev, pct, ts
                if raw_prev <= 0:
                    continue
                inv_price = 1.0 / raw_price
                inv_prev = 1.0 / raw_prev
                pct = _pct_change(inv_price, inv_prev)
                return inv_price, inv_prev, pct, ts
            except (TypeError, ValueError, OSError, ZeroDivisionError):
                continue
        return None

    def _fetch_google_index_fallback_for_asset(
        self,
        asset: str,
        cfg: dict[str, str],
        now: datetime,
    ) -> QuoteTick | None:
        url = GOOGLE_FINANCE_QUOTE_URL.format(code=cfg["code"])
        html_text = self._get_text(url)
        parsed = self._parse_google_index_quote(html_text, cfg["token"])
        if not parsed:
            return None
        price, prev, pct, ts = parsed
        # Timestamp sanity: avoid provider clock glitches and stale snapshots.
        if ts > (now + timedelta(seconds=INDEX_FALLBACK_MAX_CLOCK_SKEW_SECONDS)):
            logger.debug("Index fallback skipped (future timestamp): asset=%s ts=%s", asset, ts.isoformat())
            return None
        if (now - ts).total_seconds() > 24 * 3600:
            logger.debug("Index fallback skipped (too old): asset=%s ts=%s", asset, ts.isoformat())
            return None
        return QuoteTick(
            asset=asset,
            price=price,
            instrument_type="index",
            unit="points",
            source="google_finance_html",
            previous_close=prev,
            change_percent=round(pct, 2) if pct is not None else _pct_change(price, prev),
            provider="google_finance",
            provider_priority=4,
            confidence_level=0.8,
            source_timestamp=ts,
            is_fallback=True,
            quality="fallback",
        )

    def _fetch_index_fallbacks(self, yahoo_index_ticks: list[QuoteTick]) -> list[QuoteTick]:
        by_asset = {t.asset: t for t in yahoo_index_ticks}
        now = datetime.now(timezone.utc)
        cfg = get_settings()
        promote_seconds = max(60, int(cfg.effective_session_live_max_age_seconds()))
        status = get_market_status(now)
        out: list[QuoteTick] = []
        for asset, cfg in GOOGLE_INDEX_FALLBACKS.items():
            exchange = ASSET_EXCHANGE.get(asset, NYSE)
            primary = by_asset.get(asset)
            exchange_open = is_exchange_expected_open(exchange, now, status=status)
            if not exchange_open:
                if primary is None or not _within_post_close_grace(exchange, now, primary.source_timestamp):
                    continue
            needs_fallback = primary is None
            if primary is not None:
                p_ts = primary.source_timestamp
                if p_ts.tzinfo is None:
                    p_ts = p_ts.replace(tzinfo=timezone.utc)
                threshold = self._effective_index_fallback_threshold_seconds(asset, promote_seconds)
                needs_fallback = (now - p_ts).total_seconds() > threshold
            if not needs_fallback:
                continue
            try:
                fallback_tick = self._fetch_google_index_fallback_for_asset(asset, cfg, now)
                if fallback_tick is not None:
                    out.append(fallback_tick)
            except Exception:
                logger.debug("Index fallback fetch failed for %s", asset, exc_info=True)
        return out

    def _fetch_fx_google_fallbacks(self, yahoo_fx_ticks: list[QuoteTick]) -> list[QuoteTick]:
        now = datetime.now(timezone.utc)
        if not is_fx_session_expected_open(now):
            return []
        by_asset = {t.asset: t for t in yahoo_fx_ticks}
        stale_seconds = max(60, int(get_settings().effective_rolling_live_max_age_seconds()))
        out: list[QuoteTick] = []
        # Google Finance returns USD/INR rate ($94.55) for ALL exotic
        # INR crosses (PHP, PKR, IDR, VND, etc.) — completely wrong.
        # Skip Google for these; they'll get correct data from ER API
        # in _fetch_fx_fallback which computes cross-rates properly.
        _GOOGLE_FX_BROKEN = frozenset({
            "PHP/INR", "PKR/INR", "IDR/INR", "VND/INR", "BDT/INR",
            "LKR/INR", "NPR/INR", "QAR/INR", "KWD/INR", "BHD/INR",
            "OMR/INR", "ILS/INR", "NOK/INR", "DKK/INR", "PLN/INR",
            "TRY/INR", "SAR/INR", "MXN/INR", "SEK/INR", "TWD/INR",
            "KRW/INR", "THB/INR", "JPY/INR",
        })
        for _symbol, pair in FX_SYMBOLS.items():
            if pair in _GOOGLE_FX_BROKEN:
                continue
            primary = by_asset.get(pair)
            needs_fallback = primary is None
            if primary is not None:
                p_ts = primary.source_timestamp
                if p_ts.tzinfo is None:
                    p_ts = p_ts.replace(tzinfo=timezone.utc)
                needs_fallback = (now - p_ts).total_seconds() > stale_seconds
            if not needs_fallback:
                continue
            base_ccy, quote_ccy = pair.split("/")
            try:
                page_html = self._get_text(GOOGLE_FINANCE_QUOTE_URL.format(code=f"{base_ccy}-{quote_ccy}"))
                parsed = self._parse_google_fx_quote(page_html, base_ccy, quote_ccy)
                if not parsed:
                    continue
                price, prev, pct, ts = parsed
                if ts > (now + timedelta(seconds=FX_FALLBACK_MAX_CLOCK_SKEW_SECONDS)):
                    logger.debug("FX fallback skipped (future timestamp): asset=%s ts=%s", pair, ts.isoformat())
                    continue
                if (now - ts).total_seconds() > 24 * 3600:
                    logger.debug("FX fallback skipped (too old): asset=%s ts=%s", pair, ts.isoformat())
                    continue
                if primary is not None:
                    p_ts = primary.source_timestamp
                    if p_ts.tzinfo is None:
                        p_ts = p_ts.replace(tzinfo=timezone.utc)
                    min_gain = timedelta(seconds=FX_FALLBACK_MIN_FRESHNESS_GAIN_SECONDS)
                    if ts <= (p_ts + min_gain):
                        continue
                out.append(
                    QuoteTick(
                        asset=pair,
                        price=price,
                        instrument_type="currency",
                        unit="inr",
                        source="google_finance_html",
                        previous_close=prev,
                        change_percent=pct,
                        provider="google_finance",
                        provider_priority=4,
                        confidence_level=0.75,
                        source_timestamp=ts,
                        is_fallback=True,
                        quality="fallback",
                    )
                )
            except Exception:
                logger.debug("FX fallback fetch failed for %s", pair, exc_info=True)
        return out

    def _fetch_bond_yields(self) -> List[QuoteTick]:
        items: list[QuoteTick] = []
        for name, series_id in BOND_SERIES:
            try:
                # Bypass BaseScraper session for FRED — browser headers
                # break FRED's CDN. Use bare requests with no extra headers.
                import requests as _req
                _resp = _req.get(FRED_CSV_URL, params={"id": series_id}, timeout=30)
                _resp.raise_for_status()
                text = _resp.text
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
        items: list[QuoteTick] = []
        for symbol, pair in FX_SYMBOLS.items():
            base = symbol.split("INR=")[0].replace("=X", "")
            if base == "USD":
                price = inr
            else:
                r = rates.get(base)
                if r is None or float(r) <= 0:
                    continue
                price = inr / float(r)
            items.append(
                QuoteTick(
                    asset=pair,
                    price=price,
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
            index_ticks = self._parse_indices(records)
            fx_ticks = self._parse_fx(records)
            all_ticks.extend(index_ticks)
            all_ticks.extend(fx_ticks)
            try:
                fallbacks = self._fetch_index_fallbacks(index_ticks)
                if fallbacks:
                    logger.info("Index fallback quotes added: %d", len(fallbacks))
                    all_ticks.extend(fallbacks)
            except Exception:
                logger.debug("Index fallback scan failed", exc_info=True)
            try:
                fx_fallbacks = self._fetch_fx_google_fallbacks(fx_ticks)
                if fx_fallbacks:
                    logger.info("FX fallback quotes added: %d", len(fx_fallbacks))
                    all_ticks.extend(fx_fallbacks)
            except Exception:
                logger.debug("FX fallback scan failed", exc_info=True)
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
        selected = self._promote_delayed_index_fallbacks(selected, all_ticks)
        selected = self._apply_fx_sanity_guard(selected, all_ticks)
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
            payload = self._get_json(_yahoo_chart_url(symbol))
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
    if symbol in UNSUPPORTED_YAHOO_FX_SYMBOLS:
        logger.debug("Skipping Yahoo 1m bars for unsupported symbol %s", symbol)
        return ([], "USD")
    try:
        url = _yahoo_chart_url(symbol)
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
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else None
        if status == 404 and symbol in UNSUPPORTED_YAHOO_FX_SYMBOLS:
            logger.debug("Yahoo 1m endpoint unavailable for %s; skipping bars", symbol)
            return ([], "USD")
        logger.warning("Failed to fetch 1m bars for %s", symbol, exc_info=True)
        return ([], "USD")
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
        if sym in UNSUPPORTED_YAHOO_FX_SYMBOLS:
            continue
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
    for r in (market_rows or []):
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


async def run_market_intraday_backfill_job() -> None:
    """Backfill today's full 1-minute intraday bar history for every
    market index and FX pair with a Yahoo symbol.

    The live intraday loop intentionally refuses post-session ticks
    (see market_job's session gate) which means the closing auction
    print for NSE/BSE/LSE/XETRA/Euronext/TSE never lands in
    market_prices_intraday through the normal path. The 1D chart
    and the notification freshness gate both read the last intraday
    tick, so without this job the closing auction is effectively
    invisible after market hours.

    This job hits Yahoo's v8/chart endpoint (1m interval, 7d range)
    once per Yahoo-backed symbol, filters to today's trading date
    per exchange, and upserts every bar into market_prices_intraday
    via insert_intraday_batch. Provider tag 'yahoo_1m' so repeat
    runs are idempotent (ON CONFLICT DO UPDATE on
    (asset, instrument_type, source_timestamp, provider)).

    Triggered manually via /ops/jobs/trigger/market_intraday_backfill
    — not wired into the cron schedule because it duplicates what
    the live intraday loop already writes during market hours; it
    only earns its keep for end-of-session catch-up.
    """
    loop = asyncio.get_event_loop()
    now = datetime.now(timezone.utc)

    def _build_date_targets() -> dict[str, set[date]]:
        """Compute today's trading date for each exchange we care about."""
        return {
            NSE:      {get_trading_date(now, NSE)},
            NYSE:     {get_trading_date(now, NYSE)},
            LSE:      {get_trading_date(now, LSE)},
            XETRA:    {get_trading_date(now, XETRA)},
            EURONEXT: {get_trading_date(now, EURONEXT)},
            TSE:      {get_trading_date(now, TSE)},
        }

    def _sync_build_rows() -> list[dict]:
        trading_date_by_exchange = _build_date_targets()
        return build_market_intraday_rows_last_session_yahoo(
            market_rows=[],
            trading_date_by_exchange=trading_date_by_exchange,
        )

    rows = await loop.run_in_executor(
        get_job_executor("market"),
        _sync_build_rows,
    )

    def _build_upstox_index_rows() -> list[dict]:
        index_map = _load_upstox_index_map()
        if not index_map:
            return []
        from app.services import market_service as _svc
        nse_target = {get_trading_date(now, NSE)}
        out: list[dict] = []
        for sym, asset_name in INDEX_SYMBOLS.items():
            lookup = _UPSTOX_INDEX_NAME_ALIASES.get(asset_name, asset_name).casefold()
            key = index_map.get(lookup)
            if key is None:
                continue
            bars = _fetch_upstox_index_bars_today(key)
            for dt, close, volume in bars:
                if dt.date() not in nse_target:
                    continue
                ts_rounded = _svc._round_to_minute(dt).isoformat()
                out.append({
                    "asset": asset_name,
                    "instrument_type": "index",
                    "price": close,
                    "timestamp": ts_rounded,
                    "source_timestamp": ts_rounded,
                    "provider": "upstox_intraday",
                    "provider_priority": 1,
                    "confidence_level": 0.95,
                    "is_fallback": False,
                    "quality": "primary",
                })
        return out

    upstox_rows = await loop.run_in_executor(
        get_job_executor("market"),
        _build_upstox_index_rows,
    )
    all_rows = (rows or []) + upstox_rows

    if not all_rows:
        logger.info("market_intraday_backfill: no rows built")
        return
    inserted = await market_service.insert_intraday_batch(all_rows)
    logger.info(
        "market_intraday_backfill: %d bars built (%d yahoo + %d upstox), %d upserted",
        len(all_rows), len(rows or []), len(upstox_rows), inserted,
    )


def _fetch_market_rows_sync() -> tuple[List[Dict], bool, List[Dict]]:
    """Sync scrape; run in thread executor. Returns (rows, calendar_says_trading_day, gift_nifty_intraday_backfill).
    Each row's timestamp is the exchange trading date (NSE/NYSE) so Monday's close is not stored as Tuesday.
    The third element is a list of intraday-tick dicts scraped from the
    giftcitynifty.com historical table — the main scrape returns only
    the latest tick as a daily row, but the page carries hundreds of
    minute-level history rows that we batch-insert into
    market_prices_intraday so the 1D chart has real coverage even when
    the source pauses publishing (e.g. on Indian holidays)."""
    now = _scraper.utc_now()
    calendar_open = is_trading_day_markets(now) or is_gift_nifty_open(now)
    items = _scraper.fetch_all()
    gift_backfill = list(getattr(_scraper, "_gift_nifty_intraday_backfill", []) or [])
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
    return (rows, calendar_open, gift_backfill)


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


async def _build_closing_auction_intraday_rows(daily_rows: list[dict]) -> list[dict]:
    """For each daily row whose price differs from the last intraday
    tick, build an intraday row with the closing auction price.

    The source_timestamp is set to 1 second after the last intraday
    tick so it sorts correctly and the upsert key (asset,
    instrument_type, source_timestamp, provider) doesn't collide.
    """
    from app.core.database import get_pool
    pool = await get_pool()
    auction_rows: list[dict] = []
    for r in daily_rows:
        asset = r.get("asset")
        inst = r.get("instrument_type") or "index"
        daily_price = r.get("price")
        if daily_price is None or inst in ("currency", "commodity", "crypto"):
            continue
        # Only backfill for closed exchanges — during live hours the
        # intraday table is authoritative and may briefly lag the
        # daily upsert within the same scraper cycle.
        exchange = ASSET_EXCHANGE.get(asset, NYSE)
        if is_exchange_expected_open(exchange, datetime.now(timezone.utc)):
            continue
        last_tick = await pool.fetchrow(
            """
            SELECT price, COALESCE(source_timestamp, "timestamp") AS ts
            FROM market_prices_intraday
            WHERE asset = $1 AND instrument_type = $2
            ORDER BY COALESCE(source_timestamp, "timestamp") DESC
            LIMIT 1
            """,
            asset,
            inst,
        )
        if not last_tick:
            continue
        last_price = float(last_tick["price"])
        dp = float(daily_price)
        if abs(dp - last_price) < 0.01:
            continue
        tick_ts = last_tick["ts"]
        if tick_ts.tzinfo is None:
            tick_ts = tick_ts.replace(tzinfo=timezone.utc)
        auction_ts = (tick_ts + timedelta(seconds=1)).isoformat()
        auction_rows.append({
            "asset": asset,
            "instrument_type": inst,
            "price": dp,
            "timestamp": auction_ts,
            "source_timestamp": auction_ts,
            "provider": "closing_auction",
            "provider_priority": 1,
            "quality": "primary",
            "is_fallback": False,
            "is_predictive": False,
            "session_source": "daily_upsert",
        })
        logger.debug(
            "Closing auction tick: %s %s %.2f -> %.2f at %s",
            asset, inst, last_price, dp, auction_ts,
        )
    return auction_rows


def build_market_intraday_rows_for_open(
    market_rows: list[dict],
    status: dict,
) -> list[dict]:
    """Build intraday rows for 1D chart.
    Currencies are written every run (24/7 behavior).
    Gift Nifty follows dedicated Gift Nifty session status.
    Other indices/bonds follow exchange open status — if the
    exchange is closed at fetch time the row is dropped entirely.

    Why no post-close grace anymore: the scraper sets
    source_timestamp to wall-clock now() rather than the upstream's
    actual price-update time, so _within_post_close_grace saw
    age_seconds ≈ 0 and let stale closes through hours after the
    session ended. The visible bug: at 04:30 UTC (10:00 IST), 13
    hours after Frankfurt close, our scraper polled Google Finance,
    got DAX = 24044.22 (= yesterday's close, served as "current
    quote" by Google when the market is shut), and inserted it as
    a fresh intraday row. The Markets tab then showed "Updated 3
    hours ago" with a stale price, and the AI notification job
    generated "European shares rise as DAX up 1.3%" before
    Frankfurt was even open. Hard exchange-hours gate is the only
    correct boundary until the scraper learns to honour real
    upstream timestamps. The closing print at exactly the
    session-end minute is still captured because
    is_exchange_expected_open is inclusive of the close bell.
    """
    intraday_rows = []
    now_utc = datetime.now(timezone.utc)
    for r in market_rows:
        instrument_type = r.get("instrument_type") or "index"
        exchange = ASSET_EXCHANGE.get(r["asset"], NYSE)
        source_dt = parse_ts(r.get("source_timestamp")) if r.get("source_timestamp") else None
        if source_dt is not None and source_dt.tzinfo is None:
            source_dt = source_dt.replace(tzinfo=timezone.utc)

        # ── Stale-repeat dedupe ──────────────────────────────────
        # If the scraped price exactly equals the same response's
        # previous_close, the upstream is almost certainly serving
        # a cached "last known close" instead of a real live tick.
        # Real intraday prices essentially never return EXACTLY the
        # previous close to 4+ decimal places — when they do it's
        # a cache artifact (Google Finance and Yahoo both do this
        # for closed/pre-open exchanges).
        #
        # The session-time gate below catches the case where the
        # provider's cache-refresh timestamp falls outside session
        # hours, but it MISSES the case where the cache refreshed
        # at e.g. 08:00 UTC (Frankfurt is technically open) with
        # yesterday's close still in the cache. This dedupe catches
        # that residual case. The only legitimate sacrifice is the
        # vanishingly rare opening tick that prints exactly at
        # the previous close — the next minute's tick still gets
        # through.
        try:
            price_v = float(r.get("price")) if r.get("price") is not None else None
            prev_v = float(r.get("previous_close")) if r.get("previous_close") is not None else None
        except (TypeError, ValueError):
            price_v = prev_v = None
        if (price_v is not None and prev_v is not None
                and abs(price_v - prev_v) < 1e-6):
            try:
                from app.core.metrics import MARKET_GATE_REJECTIONS
                MARKET_GATE_REJECTIONS.labels(reason="price_equality").inc()
            except Exception:
                pass
            continue

        # Anchor the open-check to the row's CLAIMED source time
        # (source_timestamp), not wall-clock now_utc. Google Finance
        # for closed exchanges publishes a "last update time" of
        # e.g. 04:30 UTC even though Frankfurt opens at 07:00 UTC,
        # so checking now_utc admits a stale-price row whenever
        # we happen to scrape during the exchange's session. The
        # row's source_timestamp is the only honest signal of when
        # the price was actually current.
        gate_time = source_dt if source_dt is not None else now_utc
        # Post-close grace: providers often publish the closing
        # print with a source_timestamp slightly AFTER the actual
        # close (e.g. 15:31 UTC for a 15:30 UTC Frankfurt close,
        # because that's when their cache refreshed with the close
        # tick). Without a buffer the gate would reject the
        # closing print and the visible "close" on the chart would
        # be the last in-session tick a minute earlier. A 5-minute
        # buffer captures every realistic provider lag. Stale
        # repeats during this window are still rejected by the
        # price-equality dedupe above.
        gate_time_buffered = gate_time - timedelta(minutes=5)
        include = False
        if instrument_type == "currency":
            include = True
        elif r.get("asset") == "Gift Nifty":
            include = bool(status.get("gift_nifty_open"))
        elif exchange in {NSE, NYSE, LSE, XETRA, EURONEXT, TSE}:
            include = (
                is_exchange_expected_open(exchange, gate_time, status=status)
                or is_exchange_expected_open(
                    exchange, gate_time_buffered, status=status,
                )
            )
        if not include and exchange in {NSE, NYSE, LSE, XETRA, EURONEXT, TSE}:
            # Categorise the rejection for observability. pre_session
            # = source_dt is before the session window, post_session
            # = source_dt is after the post-close grace window.
            try:
                from app.core.metrics import MARKET_GATE_REJECTIONS
                session_open_now = is_exchange_expected_open(exchange, now_utc, status=status)
                reason = "pre_session" if session_open_now else "post_session"
                MARKET_GATE_REJECTIONS.labels(reason=reason).inc()
            except Exception:
                pass
        if include:
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
        fetched_rows, calendar_says_open, gift_nifty_backfill = await loop.run_in_executor(
            get_job_executor("market"),
            _fetch_market_rows_sync,
        )
        logger.debug(
            "Market job fetched_rows=%d calendar_says_open=%s gift_backfill=%d",
            len(fetched_rows), calendar_says_open, len(gift_nifty_backfill),
        )
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

        # ── Closing auction backfill ────────────────────────────────
        # The session gate rejects post-close ticks, so the closing
        # auction price only lands in the daily row. For each daily
        # row that was just upserted, if its price diverges from the
        # last intraday tick, write a closing tick to intraday so the
        # 1D chart and latest-price API reflect the actual close.
        if rows:
            auction_rows = await _build_closing_auction_intraday_rows(rows)
            if auction_rows:
                n_auction = await market_service.insert_intraday_batch(auction_rows)
                logger.info("Market job: %d closing auction ticks backfilled", n_auction)

        # Backfill Gift Nifty historical ticks from the HTML table. A
        # single successful scrape carries the full day of minute-level
        # history, so even when the upstream source pauses publishing
        # (e.g. the 2026-04-14 Ambedkar Jayanti stall) the 1D chart
        # still has real data going back hours. Upsert key is
        # (asset, instrument_type, source_timestamp, provider), so
        # repeated scrapes of the same history are free no-ops.
        if gift_nifty_backfill:
            try:
                gift_n = await market_service.insert_intraday_batch(gift_nifty_backfill)
                logger.debug(
                    "Gift Nifty historical backfill: %d rows attempted, %d upserted",
                    len(gift_nifty_backfill), gift_n,
                )
            except Exception:
                logger.exception("Gift Nifty historical backfill failed")

        logger.debug("Market job cycle completed")
    except Exception:
        logger.exception("Market job failed")
