from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.scheduler.base import BaseScraper
from app.scheduler.trading_calendar import get_trading_date, NSE, NYSE, XETRA, TSE
from app.services import macro_service

logger = logging.getLogger(__name__)

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
WORLD_BANK_URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
NSE_BASE_URL = "https://www.nseindia.com"
NSE_FII_DII_URL = "https://www.nseindia.com/api/fiidiiTradeReact"

FRED_DIRECT: Dict[str, List[Tuple[str, str]]] = {
    "US": [
        ("gdp_growth", "A191RL1Q225SBEA"),
        ("unemployment", "UNRATE"),
    ],
    "IN": [
        ("gdp_growth", "INDGDPRQPSMEI"),
        ("repo_rate", "IRSTCI01INM156N"),
    ],
    "EU": [
        ("inflation", "CP0000EZ19M086NEST"),
        ("gdp_growth", "EUNGDPRQPSMEI"),
        ("unemployment", "LRHUTTTTEZM156S"),
        ("repo_rate", "ECBDFR"),
    ],
    "JP": [
        ("inflation", "CPALTT01JPM657N"),
        ("gdp_growth", "JPNGDPRQPSMEI"),
        ("unemployment", "LRHUTTTTJPM156S"),
        ("repo_rate", "IRSTCB01JPM156N"),
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
    "EU": [
        ("inflation", "FP.CPI.TOTL.ZG"),
        ("gdp_growth", "NY.GDP.MKTP.KD.ZG"),
        ("unemployment", "SL.UEM.TOTL.ZS"),
    ],
    "JP": [
        ("inflation", "FP.CPI.TOTL.ZG"),
        ("gdp_growth", "NY.GDP.MKTP.KD.ZG"),
        ("unemployment", "SL.UEM.TOTL.ZS"),
    ],
}

COUNTRIES = ["US", "IN", "EU", "JP"]
WORLD_BANK_COUNTRY: Dict[str, str] = {
    "US": "US",
    "IN": "IN",
    "EU": "EMU",
    "JP": "JP",
}


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
        wb_country = WORLD_BANK_COUNTRY.get(country, country)
        url = WORLD_BANK_URL.format(country=wb_country.lower(), indicator=indicator)
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

    @staticmethod
    def _parse_number(value: object) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).replace(",", "").replace("₹", "").strip()
        if not text:
            return None
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None

    @staticmethod
    def _parse_date(value: object) -> datetime | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%d-%b-%Y", "%d %b %Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    def _extract_net_from_row(self, row: dict, prefix: str | None = None) -> float | None:
        keys = list(row.keys())
        for key in keys:
            key_l = str(key).lower()
            if "net" not in key_l:
                continue
            if prefix and prefix not in key_l:
                continue
            net = self._parse_number(row.get(key))
            if net is not None:
                return net
        buy = None
        sell = None
        for key in keys:
            key_l = str(key).lower()
            if prefix and prefix not in key_l:
                continue
            if "buy" in key_l:
                buy = self._parse_number(row.get(key))
            elif "sell" in key_l:
                sell = self._parse_number(row.get(key))
        if buy is not None and sell is not None:
            return round(buy - sell, 2)
        return None

    def _fetch_fii_dii_flows(self) -> List[Dict]:
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": f"{NSE_BASE_URL}/market-data/fii-dii-trading-activity",
        }
        try:
            self.session.get(NSE_BASE_URL, headers=headers, timeout=15)
            response = self.session.get(NSE_FII_DII_URL, headers=headers, timeout=15)
            response.raise_for_status()
            payload = response.json()
        except Exception:
            logger.exception("NSE FII/DII fetch failed")
            return []

        rows = payload if isinstance(payload, list) else payload.get("data") or payload.get("rows") or []
        if not isinstance(rows, list):
            return []

        fii_value: float | None = None
        dii_value: float | None = None
        fii_ts: datetime | None = None
        dii_ts: datetime | None = None

        for row in rows:
            if not isinstance(row, dict):
                continue
            row_date = None
            for k, v in row.items():
                if "date" in str(k).lower():
                    row_date = self._parse_date(v)
                    if row_date is not None:
                        break

            # Wide-format rows with explicit FII/DII net keys.
            if fii_value is None:
                net = self._extract_net_from_row(row, "fii")
                if net is not None:
                    fii_value = net
                    fii_ts = row_date
            if dii_value is None:
                net = self._extract_net_from_row(row, "dii")
                if net is not None:
                    dii_value = net
                    dii_ts = row_date

            label_parts = []
            for k, v in row.items():
                key_l = str(k).lower()
                if any(token in key_l for token in ("category", "client", "participant", "type", "name")):
                    label_parts.append(str(v).lower())
            label = " ".join(label_parts)
            if label:
                net = self._extract_net_from_row(row)
                if net is not None and ("fii" in label or "fpi" in label or "foreign" in label):
                    fii_value = net
                    fii_ts = row_date or fii_ts
                elif net is not None and ("dii" in label or "domestic" in label):
                    dii_value = net
                    dii_ts = row_date or dii_ts

        ts_default = self.utc_now()
        items: List[Dict] = []
        if fii_value is not None:
            items.append({
                "indicator_name": "fii_net_cash",
                "value": round(fii_value, 2),
                "country": "IN",
                "timestamp": (fii_ts or ts_default).isoformat(),
                "unit": "inr_cr",
                "source": "nse_fiidii_api",
            })
        if dii_value is not None:
            items.append({
                "indicator_name": "dii_net_cash",
                "value": round(dii_value, 2),
                "country": "IN",
                "timestamp": (dii_ts or ts_default).isoformat(),
                "unit": "inr_cr",
                "source": "nse_fiidii_api",
            })
        return items

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
        items.extend(self._fetch_fii_dii_flows())
        return items


_scraper = MacroScraper()


def _fetch_macro_items_sync() -> list:
    """Sync scrape; run in thread executor so main loop is not blocked."""
    return _scraper.fetch_all()


# Country → exchange for trading-date assignment (same as market: US=NYSE, India=NSE)
COUNTRY_EXCHANGE: Dict[str, str] = {
    "US": NYSE,
    "IN": NSE,
    "EU": XETRA,
    "JP": TSE,
}


def _assign_trading_date_per_country(items: List[Dict]) -> List[Dict]:
    """Set each item's timestamp to its exchange trading date (US→NYSE, IN→NSE) at 00:00 UTC."""
    now = datetime.now(timezone.utc)
    out = []
    for item in items:
        source = str(item.get("source") or "").lower()
        # Keep provider timestamp for flow snapshots (trade-date specific).
        if source == "nse_fiidii_api" and item.get("timestamp"):
            out.append(item)
            continue
        country = item.get("country", "US")
        exchange = COUNTRY_EXCHANGE.get(country, NYSE)
        trading_date = get_trading_date(now, exchange)
        ts = datetime(trading_date.year, trading_date.month, trading_date.day, 0, 0, 0, tzinfo=timezone.utc).isoformat()
        out.append({**item, "timestamp": ts})
    return out


async def run_macro_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(None, _fetch_macro_items_sync)
        items = _assign_trading_date_per_country(items)
        count = await macro_service.insert_indicators_batch_upsert_daily(items)
        logger.info("Macro job complete: %d rows upserted (daily)", count)
    except Exception:
        logger.exception("Macro job failed")
