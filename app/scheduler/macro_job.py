from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.scheduler.base import BaseScraper
from app.services import macro_service

logger = logging.getLogger(__name__)

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
WORLD_BANK_URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"

FRED_DIRECT: Dict[str, List[Tuple[str, str]]] = {
    "US": [
        ("gdp_growth", "A191RL1Q225SBEA"),
        ("unemployment", "UNRATE"),
    ],
    "IN": [
        ("gdp_growth", "INDGDPRQPSMEI"),
        ("repo_rate", "IRSTCI01INM156N"),
    ],
}

FRED_CPI: Dict[str, str] = {
    "US": "CPIAUCSL",
    "IN": "INDCPIALLMINMEI",
}

WORLD_BANK_FALLBACK: Dict[str, List[Tuple[str, str]]] = {
    "IN": [
        ("unemployment", "SL.UEM.TOTL.ZS"),
    ],
}

COUNTRIES = ["US", "IN"]


class MacroScraper(BaseScraper):

    def _fetch_fred_csv(self, series_id: str) -> List[Tuple[str, str]]:
        text = self._get_text(FRED_CSV_URL, params={"id": series_id})
        reader = csv.DictReader(io.StringIO(text))
        rows = []
        for row in reader:
            date_str = row.get("observation_date") or row.get("DATE")
            val = row.get(series_id)
            if date_str and val and val.strip() != "." and val.strip():
                rows.append((date_str, val.strip()))
        return rows

    def _fred_latest(self, series_id: str) -> Optional[Tuple[float, datetime]]:
        rows = self._fetch_fred_csv(series_id)
        if not rows:
            return None
        d, v = rows[-1]
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = self.utc_now()
        return float(v), dt

    def _compute_yoy_inflation(self, series_id: str) -> Optional[Tuple[float, datetime]]:
        rows = self._fetch_fred_csv(series_id)
        if len(rows) < 13:
            return None
        parsed = []
        for d, v in rows:
            try:
                dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                parsed.append((dt, float(v)))
            except ValueError:
                continue
        if len(parsed) < 13:
            return None
        latest_dt, latest_val = parsed[-1]
        year_ago = None
        for dt, val in parsed:
            if dt.year == latest_dt.year - 1 and dt.month == latest_dt.month:
                year_ago = val
                break
        if not year_ago or year_ago == 0:
            return None
        yoy = round(((latest_val - year_ago) / year_ago) * 100, 2)
        return yoy, latest_dt

    def _world_bank(self, country: str, indicator: str) -> Optional[Tuple[float, datetime]]:
        url = WORLD_BANK_URL.format(country=country.lower(), indicator=indicator)
        payload = self._get_json(url, params={"format": "json", "per_page": 60})
        if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
            return None
        for rec in payload[1]:
            val = rec.get("value")
            date = rec.get("date")
            if val is None or date is None:
                continue
            return float(val), datetime(int(date), 1, 1, tzinfo=timezone.utc)
        return None

    def fetch_all(self) -> List[Dict]:
        items = []
        for country in COUNTRIES:
            cpi = FRED_CPI.get(country)
            if cpi:
                try:
                    result = self._compute_yoy_inflation(cpi)
                    if result:
                        val, ts = result
                        items.append({
                            "indicator_name": "inflation",
                            "value": val,
                            "country": country,
                            "timestamp": ts.isoformat(),
                            "unit": "percent_yoy",
                            "source": "fred_api",
                        })
                except Exception:
                    logger.exception("Inflation fetch failed for %s", country)

            for name, series in FRED_DIRECT.get(country, []):
                try:
                    result = self._fred_latest(series)
                    if result:
                        val, ts = result
                        items.append({
                            "indicator_name": name,
                            "value": val,
                            "country": country,
                            "timestamp": ts.isoformat(),
                            "unit": "percent",
                            "source": "fred_api",
                        })
                except Exception:
                    logger.exception("FRED failed %s %s", country, series)

            for name, wb_ind in WORLD_BANK_FALLBACK.get(country, []):
                try:
                    result = self._world_bank(country, wb_ind)
                    if result:
                        val, ts = result
                        items.append({
                            "indicator_name": name,
                            "value": val,
                            "country": country,
                            "timestamp": ts.isoformat(),
                            "unit": "percent",
                            "source": "world_bank",
                        })
                except Exception:
                    logger.exception("World Bank failed %s %s", country, wb_ind)
        return items


_scraper = MacroScraper()


def _fetch_macro_items_sync() -> list:
    """Sync scrape; run in thread executor so main loop is not blocked."""
    return _scraper.fetch_all()


async def run_macro_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(None, _fetch_macro_items_sync)
        for item in items:
            await macro_service.insert_indicator(item)
        logger.info("Macro job complete: %d items", len(items))
    except Exception:
        logger.exception("Macro job failed")
