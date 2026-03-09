from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

from app.scheduler.base import BaseScraper
from app.scheduler.trading_calendar import get_trading_date, is_trading_day_commodities, NYSE
from app.services import market_service

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FX_USD_BASE_URL = "https://open.er-api.com/v6/latest/USD"

SYMBOLS = {
    "GC=F": ("gold", "usd_per_troy_ounce"),
    "SI=F": ("silver", "usd_per_troy_ounce"),
    "CL=F": ("crude oil", "usd_per_barrel"),
    "NG=F": ("natural gas", "usd_per_mmbtu"),
    "HG=F": ("copper", "usd_per_pound"),
}


class CommodityScraper(BaseScraper):

    def __init__(self) -> None:
        super().__init__()
        self._cached_fx: Optional[Dict[str, float]] = None

    def _get_usd_rates(self) -> Dict[str, float]:
        if self._cached_fx:
            return self._cached_fx
        data = self._get_json(FX_USD_BASE_URL)
        rates = data.get("rates", {})
        self._cached_fx = {str(k).upper(): float(v) for k, v in rates.items()}
        return self._cached_fx

    def _to_usd(self, value: float, currency: str) -> tuple[float, float]:
        currency = currency.upper()
        if currency == "USD":
            return value, 1.0
        rates = self._get_usd_rates()
        if currency not in rates or rates[currency] <= 0:
            raise ValueError(f"Missing FX rate for {currency}")
        return value / rates[currency], 1.0 / rates[currency]

    def _fetch_yahoo(self) -> List[Dict]:
        items = []
        for symbol, (asset, unit) in SYMBOLS.items():
            try:
                payload = self._get_json(YAHOO_CHART_URL.format(symbol=symbol))
                result = payload.get("chart", {}).get("result", [])
                if not result:
                    continue
                meta = result[0].get("meta", {})
                price = meta.get("regularMarketPrice") or meta.get("previousClose")
                if price is None:
                    continue
                currency = str(meta.get("currency", "USD"))
                usd_val, fx = self._to_usd(float(price), currency)
                prev_raw = meta.get("chartPreviousClose") or meta.get("previousClose")
                prev_usd = None
                pct = None
                if prev_raw is not None:
                    try:
                        prev_usd, _ = self._to_usd(float(prev_raw), currency)
                        if prev_usd > 0:
                            pct = round(((usd_val - prev_usd) / prev_usd) * 100, 2)
                    except (ValueError, ZeroDivisionError):
                        pass
                items.append({"asset": asset, "price": usd_val, "unit": unit, "source": "yahoo_chart_api", "change_percent": pct, "previous_close": prev_usd})
            except Exception:
                logger.warning("Commodity fetch failed for %s", symbol, exc_info=True)
        return items

    def fetch_all(self) -> List[Dict]:
        try:
            return self._fetch_yahoo()
        except Exception:
            logger.exception("Commodity Yahoo fetch failed")
            return []


_scraper = CommodityScraper()


def _fetch_commodity_rows_sync() -> tuple[List[Dict], bool]:
    """Sync scrape; run in thread executor. Returns (rows, calendar_says_trading_day).
    Timestamp uses NYSE trading date so Monday's close is not stored as Tuesday."""
    now = _scraper.utc_now()
    calendar_open = is_trading_day_commodities(now)
    items = _scraper.fetch_all()
    trading_date = get_trading_date(now, NYSE)
    ts = datetime(trading_date.year, trading_date.month, trading_date.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
    rows = [
        {
            "asset": it["asset"],
            "price": it["price"],
            "timestamp": ts,
            "source": it.get("source"),
            "instrument_type": "commodity",
            "unit": it.get("unit"),
            "change_percent": it.get("change_percent"),
            "previous_close": it.get("previous_close"),
        }
        for it in items
    ]
    return (rows, calendar_open)


_PRICE_CHANGE_TOLERANCE = 1e-9


def build_commodity_intraday_rows_for_open(commodity_rows: list[dict], ts_rounded: str) -> list[dict]:
    """Build intraday rows for 1D chart when NYSE (commodities) is open. Same logic for scheduler and backfill."""
    return [
        {"asset": r["asset"], "instrument_type": "commodity", "price": r["price"], "timestamp": ts_rounded}
        for r in commodity_rows
    ]


def build_commodity_intraday_rows_last_session_yahoo(
    commodity_rows: list[dict], trading_date: date | list[date] | set[date]
) -> list[dict]:
    """Build full minute-level intraday rows for last session using Yahoo 1m chart data.
    Fetches 1m bars per symbol, converts close to USD, filters to target trading date(s)."""
    from app.scheduler.market_job import _fetch_yahoo_1m_bars

    if isinstance(trading_date, date):
        target_dates = {trading_date}
    else:
        target_dates = set(trading_date)
    if not target_dates:
        return []

    rows_out = []
    for symbol, (asset_name, _unit) in SYMBOLS.items():
        bars, currency = _fetch_yahoo_1m_bars(symbol, range_period="7d")
        for dt, close in bars:
            if dt.date() not in target_dates:
                continue
            try:
                usd_price, _ = _scraper._to_usd(float(close), currency)
            except (ValueError, TypeError):
                continue
            ts_rounded = market_service._round_to_minute(dt).isoformat()
            rows_out.append({
                "asset": asset_name,
                "instrument_type": "commodity",
                "price": usd_price,
                "timestamp": ts_rounded,
            })
    return rows_out


async def run_commodity_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        fetched_rows, calendar_says_open = await loop.run_in_executor(None, _fetch_commodity_rows_sync)
        if not fetched_rows:
            return
        rows = fetched_rows
        if not calendar_says_open:
            pairs = [(r["asset"], "commodity") for r in rows]
            latest = await market_service.get_latest_price_per_asset_type(pairs)
            rows = [
                r for r in rows
                if (latest.get((r["asset"], "commodity")) is None)
                or abs(float(r["price"]) - latest[(r["asset"], "commodity")]) > _PRICE_CHANGE_TOLERANCE
            ]
        updated = 0
        if rows:
            updated = await market_service.insert_prices_batch_upsert_daily(rows)
        now = datetime.now(timezone.utc)
        ts_rounded = market_service._round_to_minute(now).isoformat()
        intraday_rows = build_commodity_intraday_rows_for_open(fetched_rows, ts_rounded)
        if intraday_rows:
            n = await market_service.insert_intraday_batch(intraday_rows)
            logger.info("Commodity job: %d daily upserted, %d intraday", updated, n)
        elif updated == 0:
            logger.info("Commodity job: no daily or intraday rows written")
        else:
            logger.info("Commodity job complete: %d rows upserted (daily)", updated)
    except Exception:
        logger.exception("Commodity job failed")
