from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.scheduler.base import BaseScraper
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

    @staticmethod
    def _parse_time(value: object) -> tuple[int, int, int] | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        for fmt in ("%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p"):
            try:
                parsed = datetime.strptime(text, fmt)
                return parsed.hour, parsed.minute, parsed.second
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

        def _pick_latest(
            current_value: float | None,
            current_ts: datetime | None,
            new_value: float | None,
            new_ts: datetime | None,
        ) -> tuple[float | None, datetime | None]:
            if new_value is None:
                return current_value, current_ts
            if current_value is None:
                return new_value, new_ts
            if current_ts is None and new_ts is not None:
                return new_value, new_ts
            if current_ts is not None and new_ts is not None and new_ts >= current_ts:
                return new_value, new_ts
            return current_value, current_ts

        for row in rows:
            if not isinstance(row, dict):
                continue
            row_date = None
            row_time = None
            for k, v in row.items():
                key_l = str(k).lower()
                if "date" in key_l:
                    row_date = self._parse_date(v)
                    if row_date is not None:
                        break
            for k, v in row.items():
                if "time" in str(k).lower():
                    row_time = self._parse_time(v)
                    if row_time is not None:
                        break
            if row_date is not None and row_time is not None:
                row_date = row_date.replace(
                    hour=row_time[0],
                    minute=row_time[1],
                    second=row_time[2],
                    microsecond=0,
                )

            # Wide-format rows with explicit FII/DII net keys.
            fii_value, fii_ts = _pick_latest(
                fii_value,
                fii_ts,
                self._extract_net_from_row(row, "fii"),
                row_date,
            )
            dii_value, dii_ts = _pick_latest(
                dii_value,
                dii_ts,
                self._extract_net_from_row(row, "dii"),
                row_date,
            )

            label_parts = []
            for k, v in row.items():
                key_l = str(k).lower()
                if any(token in key_l for token in ("category", "client", "participant", "type", "name")):
                    label_parts.append(str(v).lower())
            label = " ".join(label_parts)
            if label:
                net = self._extract_net_from_row(row)
                if net is not None and ("fii" in label or "fpi" in label or "foreign" in label):
                    fii_value, fii_ts = _pick_latest(
                        fii_value, fii_ts, net, row_date
                    )
                elif net is not None and ("dii" in label or "domestic" in label):
                    dii_value, dii_ts = _pick_latest(
                        dii_value, dii_ts, net, row_date
                    )

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
        logger.debug(
            "NSE FII/DII parsed rows=%d fii=%s@%s dii=%s@%s",
            len(rows),
            fii_value,
            (fii_ts or ts_default).date().isoformat() if fii_value is not None else "-",
            dii_value,
            (dii_ts or ts_default).date().isoformat() if dii_value is not None else "-",
        )
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


async def run_macro_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(None, _fetch_macro_items_sync)
        pruned = await macro_service.delete_rows_newer_than_source_timestamps(
            items,
            sources={"fred_api", "world_bank"},
        )
        count = await macro_service.insert_indicators_batch_upsert_source_timestamp(items)
        logger.info(
            "Macro job complete: %d rows upserted (source timestamp), %d legacy rows pruned",
            count,
            pruned,
        )
    except Exception:
        logger.exception("Macro job failed")
