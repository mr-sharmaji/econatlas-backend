from __future__ import annotations

import csv
import io
import logging
from typing import Dict, List, Optional

from app.scheduler.base import BaseScraper
from app.services import market_service

logger = logging.getLogger(__name__)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
FX_USD_BASE_URL = "https://open.er-api.com/v6/latest/USD"
STOOQ_CSV_URL = "https://stooq.com/q/l/"

SYMBOLS = {
    "GC=F": ("gold", "usd_per_troy_ounce"),
    "SI=F": ("silver", "usd_per_troy_ounce"),
    "CL=F": ("crude oil", "usd_per_barrel"),
    "NG=F": ("natural gas", "usd_per_mmbtu"),
    "HG=F": ("copper", "usd_per_pound"),
}

STOOQ_FALLBACK = {
    "gold": "gc.f",
    "silver": "si.f",
    "crude oil": "cl.f",
    "natural gas": "ng.f",
    "copper": "hg.f",
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

    def _fetch_stooq(self) -> List[Dict]:
        items = []
        unit_map = {a: u for _, (a, u) in SYMBOLS.items()}
        for asset, symbol in STOOQ_FALLBACK.items():
            try:
                text = self._get_text(STOOQ_CSV_URL, params={"s": symbol, "f": "sd2t2ohlcv", "h": ""})
                row = next(csv.DictReader(io.StringIO(text)), None)
                if not row:
                    continue
                close = row.get("Close")
                if not close or close in {"N/D", "0"}:
                    continue
                items.append({"asset": asset, "price": float(close), "unit": unit_map.get(asset, "usd"), "source": "stooq_fallback", "change_percent": None, "previous_close": None})
            except Exception:
                logger.warning("Stooq fallback failed for %s", asset)
        return items

    def fetch_all(self) -> List[Dict]:
        try:
            items = self._fetch_yahoo()
            if items:
                return items
        except Exception:
            logger.warning("Commodity Yahoo failed; trying Stooq", exc_info=True)
        try:
            return self._fetch_stooq()
        except Exception:
            logger.exception("Commodity Stooq fallback failed")
            return []


_scraper = CommodityScraper()


async def run_commodity_job() -> None:
    try:
        items = _scraper.fetch_all()
        ts = _scraper.utc_now().isoformat()
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
        count = await market_service.insert_prices_batch(rows)
        logger.info("Commodity job complete: %d items inserted", count)
    except Exception:
        logger.exception("Commodity job failed")
