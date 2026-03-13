from __future__ import annotations

import asyncio
import csv
import io
import logging
import math
import re
import statistics
import zipfile
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from html import unescape
from zoneinfo import ZoneInfo

import requests

from app.core.config import get_settings
from app.scheduler.base import BaseScraper
from app.scheduler.brief_job import INDIA_STOCKS
from app.scheduler.job_executors import get_job_executor
from app.services import discover_service

logger = logging.getLogger(__name__)

NSE_HOME_URL = "https://www.nseindia.com"
NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
NSE_EQUITY_MASTER_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_BHAVCOPY_URL_TMPL = "https://archives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{yyyymmdd}_F_0000.csv.zip"
NSE_STOCK_SERIES = {"EQ", "BE", "BZ"}
IST = ZoneInfo("Asia/Kolkata")
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"


# Mapping from Screener.in / NSE industry terms to broad sector categories
_SECTOR_MAPPING: dict[str, str] = {
    # Energy
    "oil": "Energy", "gas": "Energy", "petroleum": "Energy", "crude": "Energy",
    "energy": "Energy", "power": "Energy", "renewable": "Energy",
    "electric utilities": "Utilities", "utilities": "Utilities",
    # IT
    "information technology": "IT", "software": "IT", "it ": "IT",
    "computer": "IT", "digital": "IT",
    # Financials
    "bank": "Financials", "finance": "Financials", "insurance": "Financials",
    "nbfc": "Financials", "financial": "Financials", "credit": "Financials",
    # Healthcare
    "pharma": "Healthcare", "healthcare": "Healthcare", "hospital": "Healthcare",
    "drug": "Healthcare", "medical": "Healthcare", "biotech": "Healthcare",
    # Consumer
    "fmcg": "Consumer", "consumer": "Consumer", "retail": "Consumer",
    "food": "Consumer", "beverage": "Consumer", "textile": "Consumer",
    "apparel": "Consumer", "personal care": "Consumer",
    # Auto
    "auto": "Auto", "automobile": "Auto", "vehicle": "Auto",
    "tyre": "Auto", "tire": "Auto",
    # Industrials
    "capital goods": "Industrials", "industrial": "Industrials",
    "engineering": "Industrials", "construction": "Industrials",
    "infrastructure": "Industrials", "cement": "Industrials",
    "defence": "Industrials", "defense": "Industrials",
    # Materials
    "metals": "Materials", "steel": "Materials", "aluminium": "Materials",
    "mining": "Materials", "chemicals": "Materials", "paper": "Materials",
    "fertilizer": "Materials", "plastic": "Materials",
    # Telecom
    "telecom": "Telecom", "communication": "Telecom",
    # Real Estate
    "real estate": "Real Estate", "realty": "Real Estate", "housing": "Real Estate",
    # Media
    "media": "Media", "entertainment": "Media",
}

# Expanded curated sector mapping for common stocks not in INDIA_STOCKS
_EXTRA_SECTOR_MAP: dict[str, str] = {
    "ATGL": "Energy", "NTPCGREEN": "Energy", "JSWENERGY": "Energy",
    "ADANIENERGY": "Energy", "CESC": "Energy", "NHPC": "Energy",
    "TORNTPOWER": "Utilities", "SJVN": "Energy",
    "DOMS": "Consumer", "JINDALSAW": "Materials", "JINDALSTEL": "Materials",
    "TATASTEEL": "Materials", "HINDALCO": "Materials", "VEDL": "Materials",
    "NMDC": "Materials", "COALINDIA": "Energy",
    "DABUR": "Consumer", "GODREJCP": "Consumer", "MARICO": "Consumer",
    "PIDILITIND": "Consumer", "COLPAL": "Consumer", "BRITANNIA": "Consumer",
    "PAGEIND": "Consumer", "VBL": "Consumer", "TRENT": "Consumer",
    "IRCTC": "Consumer", "ZOMATO": "Consumer", "NYKAA": "Consumer",
    "DMART": "Consumer", "TITAN": "Consumer",
    "SBICARD": "Financials", "CHOLAFIN": "Financials", "MUTHOOTFIN": "Financials",
    "MANAPPURAM": "Financials", "PEL": "Financials", "CANFINHOME": "Financials",
    "ICICIGI": "Financials", "SBILIFE": "Financials", "HDFCLIFE": "Financials",
    "MAXHEALTH": "Healthcare", "FORTIS": "Healthcare", "LALPATHLAB": "Healthcare",
    "METROPOLIS": "Healthcare", "AUROPHARMA": "Healthcare", "ALKEM": "Healthcare",
    "LAURUSLABS": "Healthcare", "GLENMARK": "Healthcare", "IPCALAB": "Healthcare",
    "MPHASIS": "IT", "COFORGE": "IT", "LTTS": "IT", "PERSISTENT": "IT",
    "MFSL": "Financials", "NAUKRI": "IT", "PAYTM": "IT",
    "MOTHERSON": "Auto", "BALKRISIND": "Auto", "ASHOKLEY": "Auto",
    "ESCORTS": "Auto", "TVSMTR": "Auto", "TIINDIA": "Auto",
    "GODREJPROP": "Real Estate", "DLF": "Real Estate", "OBEROIRLTY": "Real Estate",
    "PRESTIGE": "Real Estate", "PHOENIXLTD": "Real Estate",
    "INDUSTOWER": "Telecom", "TATACOMM": "Telecom",
    "ABB": "Industrials", "SIEMENS": "Industrials", "HAVELLS": "Industrials",
    "POLYCAB": "Industrials", "VOLTAS": "Industrials", "CGPOWER": "Industrials",
    "BEL": "Industrials", "HAL": "Industrials", "CONCOR": "Industrials",
    "IRFC": "Financials", "PFC": "Financials", "RECLTD": "Financials",
    "ULTRACEMCO": "Industrials", "AMBUJACEM": "Industrials", "SHREECEM": "Industrials",
    "DELHIVERY": "Industrials", "PIIND": "Materials", "AARTI": "Materials",
    "DEEPAKNTR": "Materials", "UPL": "Materials", "SRF": "Materials",
    "PVRINOX": "Media", "SUNTV": "Media", "ZEEL": "Media",
    # Additional sector mappings to reduce "Other"
    "JUBLFOOD": "Consumer", "MCDOWELL": "Consumer", "UBL": "Consumer",
    "TATACONSUM": "Consumer", "EMAMILTD": "Consumer", "JYOTHYLAB": "Consumer",
    "RADICO": "Consumer", "BATAINDIA": "Consumer", "RELAXO": "Consumer",
    "WHIRLPOOL": "Consumer", "BLUESTARLT": "Consumer", "CROMPTON": "Consumer",
    "KAJARIACER": "Materials", "CENTURYTEX": "Materials", "GRASIM": "Materials",
    "FLUOROCHEM": "Materials", "CLEAN": "Materials", "NAVINFLUOR": "Materials",
    "SUMICHEM": "Materials", "BASF": "Materials", "TATACHEM": "Materials",
    "FINEORG": "Materials", "ALKYLAMINE": "Materials",
    "HINDPETRO": "Energy", "BPCL": "Energy", "IOC": "Energy",
    "GAIL": "Energy", "PETRONET": "Energy", "ONGC": "Energy",
    "ADANIGREEN": "Energy", "TATAPOWER": "Energy", "ADANIPOWER": "Energy",
    "KPITTECH": "IT", "ZENSAR": "IT", "BIRLASOFT": "IT",
    "TATAELXSI": "IT", "INTELLECT": "IT", "HAPPSTMNDS": "IT",
    "ROUTE": "IT", "MASTEK": "IT", "CYIENT": "IT",
    "ICICIPRULI": "Financials", "BAJFINANCE": "Financials", "BAJAJFINSV": "Financials",
    "LICHSGFIN": "Financials", "M&MFIN": "Financials", "SHRIRAMFIN": "Financials",
    "SUNDARMFIN": "Financials", "CANARAHSBK": "Financials", "FEDERALBNK": "Financials",
    "BANDHANBNK": "Financials", "RBLBANK": "Financials", "IDFC": "Financials",
    "IDFCFIRSTB": "Financials", "INDUSINDBK": "Financials",
    "APOLLOHOSP": "Healthcare", "SYNGENE": "Healthcare", "BIOCON": "Healthcare",
    "NATCOPHARMA": "Healthcare", "TORNTPHARM": "Healthcare",
    "APLLTD": "Healthcare", "GRANULES": "Healthcare",
    "EXIDEIND": "Auto", "AMARAJABAT": "Auto", "SONACOMS": "Auto",
    "SAMVARDHNA": "Auto", "ENDURANCE": "Auto", "BHARATFORG": "Industrials",
    "CUMMINSIND": "Industrials", "THERMAX": "Industrials", "LTIM": "IT",
    "KAYNES": "Industrials", "AFFLE": "IT", "MAPMY": "IT",
    "RVNL": "Industrials", "IRCON": "Industrials", "NCC": "Industrials",
    "NBCC": "Industrials", "KECINTL": "Industrials", "KALPATPOWR": "Industrials",
    "AIAENG": "Industrials", "GRINFRA": "Industrials",
    "SBICARD": "Financials", "BSE": "Financials", "MCX": "Financials",
    "CDSL": "Financials", "CAMS": "Financials", "KFIN": "Financials",
    "BRIGADE": "Real Estate", "SOBHA": "Real Estate", "MAHLIFE": "Real Estate",
    "LODHA": "Real Estate", "RAYMOND": "Consumer",
    "TTML": "Telecom", "VODAFONE": "Telecom",
    "GPPL": "Utilities", "POWERGRID": "Utilities",
    "IEX": "Utilities", "SJVN": "Energy",
}


def _map_screener_sector(raw: str) -> str:
    """Map a raw Screener.in sector/industry string to a broad sector category."""
    lowered = raw.strip().lower()
    for keyword, sector in _SECTOR_MAPPING.items():
        if keyword in lowered:
            return sector
    return raw.strip().title()  # Use the raw value title-cased as fallback


@dataclass(frozen=True)
class DiscoverStockDef:
    nse_symbol: str
    yahoo_symbol: str
    display_name: str
    sector: str
    fundamentals_enabled: bool = True


def _build_core_universe() -> tuple[DiscoverStockDef, ...]:
    rows: list[DiscoverStockDef] = []
    seen: set[str] = set()
    for item in INDIA_STOCKS:
        y_symbol = item.symbol
        n_symbol = y_symbol.replace(".NS", "").strip().upper()
        if not n_symbol or n_symbol in seen:
            continue
        seen.add(n_symbol)
        rows.append(
            DiscoverStockDef(
                nse_symbol=n_symbol,
                yahoo_symbol=y_symbol,
                display_name=item.display_name,
                sector=item.sector,
                fundamentals_enabled=True,
            )
        )
    return tuple(rows)


CORE_UNIVERSE = _build_core_universe()


class DiscoverStockScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self._nse_ready = False
        self._nse_disabled_until: datetime | None = None
        self._nse_timeout = max(1, int(getattr(self.settings, "discover_stock_nse_timeout_seconds", 4)))
        self._nse_cooldown = max(30, int(getattr(self.settings, "discover_stock_nse_cooldown_seconds", 300)))
        self._screener_timeout = max(2, int(getattr(self.settings, "discover_stock_screener_timeout_seconds", 8)))
        self._fundamentals_limit = max(
            len(CORE_UNIVERSE),
            int(getattr(self.settings, "discover_stock_fundamentals_limit", 600)),
        )
        self._bhavcopy_lookback_days = max(
            1,
            int(getattr(self.settings, "discover_stock_bhavcopy_lookback_days", 7)),
        )
        self._universe_cache_ttl_seconds = max(
            300,
            int(getattr(self.settings, "discover_stock_universe_cache_ttl_seconds", 21600)),
        )
        self._missing_quote_retry_limit = max(
            0,
            int(getattr(self.settings, "discover_stock_missing_quote_retry_limit", 400)),
        )
        self._core_symbol_map = {row.nse_symbol: row for row in CORE_UNIVERSE}
        self._universe_cache: tuple[DiscoverStockDef, ...] | None = None
        self._universe_cache_at: datetime | None = None

    def _nse_on_cooldown(self) -> bool:
        if self._nse_disabled_until is None:
            return False
        if datetime.now(timezone.utc) >= self._nse_disabled_until:
            self._nse_disabled_until = None
            return False
        return True

    def _activate_nse_cooldown(self, *, reason: str) -> None:
        if self._nse_on_cooldown():
            return
        self._nse_ready = False
        self._nse_disabled_until = datetime.now(timezone.utc) + timedelta(seconds=self._nse_cooldown)
        logger.warning("NSE quote path disabled for %ds (%s); using Yahoo fallback", self._nse_cooldown, reason)

    def _ensure_nse_session(self) -> None:
        if self._nse_ready:
            return
        if self._nse_on_cooldown():
            raise RuntimeError("NSE session cooldown active")
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": NSE_HOME_URL,
        }
        try:
            self.session.get(NSE_HOME_URL, headers=headers, timeout=self._nse_timeout)
            self._nse_ready = True
        except Exception:
            self._activate_nse_cooldown(reason="session bootstrap failed")
            raise

    def _fetch_nse_quote(self, symbol: str) -> dict | None:
        if self._nse_on_cooldown():
            return None
        try:
            self._ensure_nse_session()
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": f"{NSE_HOME_URL}/get-quotes/equity?symbol={symbol}",
                "X-Requested-With": "XMLHttpRequest",
            }
            resp = self.session.get(
                NSE_QUOTE_URL,
                params={"symbol": symbol},
                headers=headers,
                timeout=self._nse_timeout,
            )
            resp.raise_for_status()
            payload = resp.json()
            info = payload.get("priceInfo") or {}
            sec = payload.get("securityWiseDP") or {}
            meta = payload.get("metadata") or {}

            last = info.get("lastPrice")
            if last is None:
                return None
            last_price = float(str(last).replace(",", ""))

            point_change = info.get("change")
            pct_change = info.get("pChange")
            volume = sec.get("quantityTraded") or info.get("totalTradedVolume")
            traded_value = sec.get("valueTraded") or info.get("totalTradedValue")

            ts_text = meta.get("lastUpdateTime")
            source_ts = datetime.now(timezone.utc)
            if ts_text:
                # NSE format example: 12-Mar-2026 15:30:00
                try:
                    parsed = datetime.strptime(str(ts_text), "%d-%b-%Y %H:%M:%S")
                    source_ts = parsed.replace(tzinfo=timezone.utc)
                except ValueError:
                    pass

            return {
                "last_price": last_price,
                "point_change": float(str(point_change).replace(",", "")) if point_change is not None else None,
                "percent_change": float(str(pct_change).replace(",", "")) if pct_change is not None else None,
                "volume": int(float(str(volume).replace(",", ""))) if volume is not None else None,
                "traded_value": float(str(traded_value).replace(",", "")) if traded_value is not None else None,
                "source_timestamp": source_ts,
                "source": "nse_quote_api",
            }
        except Exception:
            self._activate_nse_cooldown(reason=f"quote fetch failed for {symbol}")
            logger.debug("NSE quote fetch failed for %s", symbol, exc_info=True)
            return None

    @staticmethod
    def _parse_float(value: object) -> float | None:
        try:
            if value is None:
                return None
            text = str(value).replace(",", "").strip()
            if not text:
                return None
            return float(text)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_int(value: object) -> int | None:
        try:
            if value is None:
                return None
            text = str(value).replace(",", "").strip()
            if not text:
                return None
            return int(float(text))
        except (TypeError, ValueError):
            return None

    def _fetch_nse_master_universe(self) -> tuple[DiscoverStockDef, ...]:
        url = str(getattr(self.settings, "discover_stock_universe_url", NSE_EQUITY_MASTER_URL)).strip() or NSE_EQUITY_MASTER_URL
        text = self._get_text(url, timeout=20)
        reader = csv.DictReader(io.StringIO(text))
        out: list[DiscoverStockDef] = []
        seen: set[str] = set()
        for row in reader:
            symbol = str(row.get("SYMBOL") or "").strip().upper()
            if not symbol or symbol in seen:
                continue
            series = str(row.get(" SERIES") or row.get("SERIES") or "").strip().upper()
            if series and series not in NSE_STOCK_SERIES:
                continue

            display_name = str(row.get("NAME OF COMPANY") or symbol).strip() or symbol
            core = self._core_symbol_map.get(symbol)
            # Determine sector: curated > extra map > "Other"
            if core:
                sector = core.sector
            else:
                sector = _EXTRA_SECTOR_MAP.get(symbol, "Other")
                if sector == "Other":
                    logger.debug("Stock %s mapped to 'Other' sector", symbol)
            out.append(
                DiscoverStockDef(
                    nse_symbol=symbol,
                    yahoo_symbol=f"{symbol}.NS",
                    display_name=core.display_name if core else display_name,
                    sector=sector,
                    fundamentals_enabled=core is not None,
                )
            )
            seen.add(symbol)
        return tuple(out)

    def _build_effective_universe(self) -> tuple[DiscoverStockDef, ...]:
        now = datetime.now(timezone.utc)
        if self._universe_cache and self._universe_cache_at is not None:
            age = (now - self._universe_cache_at).total_seconds()
            if age <= self._universe_cache_ttl_seconds:
                return self._universe_cache
        try:
            universe = self._fetch_nse_master_universe()
            if universe:
                self._universe_cache = universe
                self._universe_cache_at = now
                return universe
        except Exception:
            logger.debug("Failed to build full NSE universe from master file", exc_info=True)
        if self._universe_cache:
            return self._universe_cache
        return CORE_UNIVERSE

    def _select_fundamentals_symbols(
        self,
        universe: tuple[DiscoverStockDef, ...],
        quotes_by_symbol: dict[str, dict],
    ) -> set[str]:
        prioritized: list[tuple[int, float, int, str]] = []
        for stock in universe:
            quote = quotes_by_symbol.get(stock.nse_symbol) or {}
            prioritized.append(
                (
                    0 if stock.nse_symbol in self._core_symbol_map else 1,
                    -float(quote.get("traded_value") or 0.0),
                    -int(quote.get("volume") or 0),
                    stock.nse_symbol,
                )
            )

        selected: set[str] = set()
        for _, _, _, symbol in sorted(prioritized):
            selected.add(symbol)
            if len(selected) >= self._fundamentals_limit:
                break

        selected.update(self._core_symbol_map.keys())
        return selected

    def _fetch_latest_bhavcopy_quotes(self) -> tuple[dict[str, dict], datetime | None]:
        base_url = str(getattr(self.settings, "discover_stock_bhavcopy_url_template", NSE_BHAVCOPY_URL_TMPL)).strip()
        if not base_url:
            base_url = NSE_BHAVCOPY_URL_TMPL

        now_ist = datetime.now(IST)
        for day_offset in range(self._bhavcopy_lookback_days + 1):
            d = (now_ist - timedelta(days=day_offset)).date()
            yyyymmdd = d.strftime("%Y%m%d")
            url = base_url.format(yyyymmdd=yyyymmdd)
            try:
                resp = self.session.get(url, timeout=20)
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
                    csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
                    if not csv_names:
                        continue
                    payload = archive.read(csv_names[0]).decode("utf-8", errors="ignore")
                rows = csv.DictReader(io.StringIO(payload))
            except Exception:
                logger.debug("Failed to load NSE bhavcopy url=%s", url, exc_info=True)
                continue

            source_ts = datetime.combine(d, time(hour=16, minute=0), tzinfo=IST).astimezone(timezone.utc)
            out: dict[str, dict] = {}
            for row in rows:
                symbol = str(row.get("TckrSymb") or "").strip().upper()
                if not symbol:
                    continue
                series = str(row.get("SctySrs") or "").strip().upper()
                if series not in NSE_STOCK_SERIES:
                    continue
                last_price = self._parse_float(row.get("ClsPric"))
                prev_close = self._parse_float(row.get("PrvsClsgPric"))
                if last_price is None or last_price <= 0:
                    continue
                point_change = None
                pct_change = None
                if prev_close is not None and prev_close != 0:
                    point_change = round(last_price - prev_close, 2)
                    pct_change = round(((last_price - prev_close) / prev_close) * 100.0, 2)

                out[symbol] = {
                    "last_price": last_price,
                    "point_change": point_change,
                    "percent_change": pct_change,
                    "volume": self._parse_int(row.get("TtlTradgVol")),
                    "traded_value": self._parse_float(row.get("TtlTrfVal")),
                    "source_timestamp": source_ts,
                    "source": "nse_bhavcopy",
                }
            if out:
                return out, source_ts
        return {}, None

    def _fetch_yahoo_quote(self, yahoo_symbol: str) -> dict | None:
        try:
            payload = self._get_json(YAHOO_CHART_URL.format(symbol=yahoo_symbol))
            result = payload.get("chart", {}).get("result", [])
            if not result:
                return None
            meta = result[0].get("meta", {})
            price_raw = meta.get("regularMarketPrice")
            prev_raw = meta.get("regularMarketPreviousClose") or meta.get("previousClose")
            if price_raw is None:
                return None

            price = float(price_raw)
            prev = float(prev_raw) if prev_raw is not None else None
            point = round(price - prev, 2) if prev is not None else None
            pct = round(((price - prev) / prev) * 100, 2) if prev not in (None, 0) else None

            vol_raw = meta.get("regularMarketVolume")
            volume = int(vol_raw) if vol_raw is not None else None
            traded_value = round(price * volume, 2) if volume is not None else None

            ts_raw = meta.get("regularMarketTime")
            if ts_raw is not None:
                try:
                    source_ts = datetime.fromtimestamp(int(ts_raw), tz=timezone.utc)
                except Exception:
                    source_ts = datetime.now(timezone.utc)
            else:
                source_ts = datetime.now(timezone.utc)

            return {
                "last_price": price,
                "point_change": point,
                "percent_change": pct,
                "volume": volume,
                "traded_value": traded_value,
                "source_timestamp": source_ts,
                "source": "yahoo_finance_api",
            }
        except Exception:
            logger.debug("Yahoo quote fetch failed for %s", yahoo_symbol, exc_info=True)
            return None

    def _extract_labeled_number(self, text: str, labels: list[str]) -> float | None:
        for label in labels:
            # Allow optional currency symbols (₹, Rs, Rs.) and "in Rs" between label and number
            patt = rf"{re.escape(label)}\s*(?:in\s+)?[:\-]?\s*(?:[₹]|Rs\.?\s*)?\s*([\-]?[0-9][0-9,]*(?:\.[0-9]+)?)"
            match = re.search(patt, text, flags=re.IGNORECASE)
            if not match:
                continue
            raw = match.group(1).replace(",", "").strip()
            try:
                return float(raw)
            except ValueError:
                continue
        return None

    @staticmethod
    def _extract_balance_sheet_de(html: str) -> float | None:
        """Compute Debt-to-Equity from the balance sheet table on Screener.in.

        D/E = Total Borrowings / (Equity Capital + Reserves).
        Extracts the last column (most recent period) from each row.
        """
        bs_match = re.search(r'id="balance-sheet"', html)
        if not bs_match:
            return None
        bs_chunk = html[bs_match.start(): bs_match.start() + 20000]

        def _last_number(label: str) -> float | None:
            """Find a balance-sheet row by label and return its last numeric value."""
            idx = bs_chunk.find(label)
            if idx < 0:
                return None
            # Walk forward to find the closing </td> of the label cell
            close_td = bs_chunk.find("</td>", idx)
            if close_td < 0:
                return None
            # Extract numbers only within this row — stop at next </tr>
            after_start = close_td + 5
            end_tr = bs_chunk.find("</tr>", after_start)
            row_slice = bs_chunk[after_start: end_tr] if end_tr > 0 else bs_chunk[after_start: after_start + 2000]
            nums = re.findall(r'<td[^>]*>\s*([\-]?[\d,]+(?:\.\d+)?)\s*</td>', row_slice)
            if not nums:
                return None
            try:
                return float(nums[-1].replace(",", ""))
            except ValueError:
                return None

        borrowings = _last_number("Borrowings")
        equity_capital = _last_number("Equity Capital")
        reserves = _last_number("Reserves")

        if borrowings is None:
            return None
        equity = (equity_capital or 0) + (reserves or 0)
        if equity <= 0:
            return None
        return round(borrowings / equity, 2)

    def _fetch_screener_fundamentals(self, nse_symbol: str) -> tuple[dict, str]:
        base = self.settings.discover_stock_primary_url.rstrip("/")
        candidates = [
            f"{base}/company/{nse_symbol}/consolidated/",
            f"{base}/company/{nse_symbol}/",
        ]
        for url in candidates:
            try:
                html = self._get_text(url, timeout=self._screener_timeout)
                text = unescape(re.sub(r"<[^>]+>", " ", html))
                text = re.sub(r"\s+", " ", text)

                book_value = self._extract_labeled_number(text, ["Book Value"])
                current_price = self._extract_labeled_number(text, ["Current Price"])

                # Compute D/E from balance sheet (Borrowings / Equity)
                debt_to_equity = self._extract_balance_sheet_de(html)

                fundamentals = {
                    "pe_ratio": self._extract_labeled_number(text, ["Stock P/E", "P/E"]),
                    "roe": self._extract_labeled_number(text, ["ROE", "Return on equity"]),
                    "roce": self._extract_labeled_number(text, ["ROCE", "Return on capital employed"]),
                    "debt_to_equity": debt_to_equity,
                    "price_to_book": (
                        round(current_price / book_value, 2)
                        if current_price and book_value and book_value > 0
                        else None
                    ),
                    "eps": self._extract_labeled_number(text, ["EPS", "Earnings Per Share"]),
                    "market_cap": self._extract_labeled_number(text, ["Market Cap"]),
                    "dividend_yield": self._extract_labeled_number(text, ["Dividend Yield"]),
                }

                # Extract sector/industry from Screener.in page
                sector_match = re.search(
                    r"(?:Sector|Industry)\s*[:\s]+([A-Za-z &\-/]+?)(?:\s{2,}|\s*\d|\s*$)",
                    text,
                    flags=re.IGNORECASE,
                )
                if sector_match:
                    raw_sector = sector_match.group(1).strip()
                    fundamentals["_screener_sector"] = raw_sector

                # 52-week High / Low: pattern like "High / Low ₹ 1,234 / ₹ 567"
                hl_match = re.search(
                    r"High\s*/\s*Low\s*[₹Rs.\s]*([\d,]+(?:\.\d+)?)\s*/\s*[₹Rs.\s]*([\d,]+(?:\.\d+)?)",
                    text,
                    flags=re.IGNORECASE,
                )
                if hl_match:
                    try:
                        fundamentals["high_52w"] = float(hl_match.group(1).replace(",", ""))
                    except ValueError:
                        fundamentals["high_52w"] = None
                    try:
                        fundamentals["low_52w"] = float(hl_match.group(2).replace(",", ""))
                    except ValueError:
                        fundamentals["low_52w"] = None
                else:
                    fundamentals["high_52w"] = None
                    fundamentals["low_52w"] = None
                return fundamentals, "screener_in"
            except Exception:
                logger.debug("Screener fundamentals fetch failed for %s url=%s", nse_symbol, url, exc_info=True)
                continue
        return {
            "pe_ratio": None,
            "roe": None,
            "roce": None,
            "debt_to_equity": None,
            "price_to_book": None,
            "eps": None,
            "market_cap": None,
            "high_52w": None,
            "low_52w": None,
            "dividend_yield": None,
        }, "unavailable"

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _percentile_rank(values: list[float], target: float) -> float:
        """Return 0-100 percentile rank of *target* within *values*."""
        if not values:
            return 50.0
        eps = 1e-9
        below = sum(1 for v in values if v < (target - eps))
        equal = sum(1 for v in values if abs(v - target) <= eps)
        return ((below + (equal * 0.5)) / len(values)) * 100.0

    @staticmethod
    def _quantile(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        clipped_q = max(0.0, min(1.0, q))
        s = sorted(values)
        if len(s) == 1:
            return s[0]
        pos = (len(s) - 1) * clipped_q
        lo = int(pos)
        hi = min(lo + 1, len(s) - 1)
        if lo == hi:
            return s[lo]
        frac = pos - lo
        return s[lo] + ((s[hi] - s[lo]) * frac)

    @staticmethod
    def _shrink_to_neutral(
        score: float,
        coverage: float,
        *,
        neutral: float = 50.0,
        min_factor: float = 0.35,
    ) -> float:
        c = max(0.0, min(1.0, coverage))
        factor = min_factor + ((1.0 - min_factor) * c)
        return neutral + ((score - neutral) * factor)

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        n = len(s)
        mid = n // 2
        return (s[mid] + s[mid - 1]) / 2.0 if n % 2 == 0 else s[mid]

    def _score_52w_position(
        self,
        price: float | None,
        high_52w: float | None,
        low_52w: float | None,
    ) -> float | None:
        """Score based on position within 52-week range (0-100).

        Near 52W high = strong momentum confirmation → higher score.
        Near 52W low = weak momentum → lower score.
        """
        if price is None or high_52w is None or low_52w is None:
            return None
        if high_52w <= low_52w or price <= 0:
            return None
        position = (price - low_52w) / (high_52w - low_52w)
        position = max(0.0, min(1.0, position))
        # Map 0→5, 1→100
        return round(self._clamp(position * 95 + 5), 2)

    @staticmethod
    def _adjust_volatility_for_cap(
        vol_score: float,
        market_cap: float | None,
    ) -> float:
        """Scale volatility score expectations by market cap tier.

        Small caps naturally have higher volatility — penalizing them equally
        to large caps is unfair.
        """
        if market_cap is None or market_cap <= 0:
            return vol_score
        if market_cap >= 20_000:  # Large cap (₹20k+ Cr)
            return vol_score       # No adjustment
        if market_cap >= 5_000:   # Mid cap (₹5k-20k Cr)
            return min(100.0, vol_score * 1.08)   # ~8% boost
        return min(100.0, vol_score * 1.15)        # Small cap: ~15% boost

    def _score_fundamentals(
        self,
        row: dict,
        sector_medians: dict[str, dict[str, float]],
    ) -> tuple[float, int]:
        """Weighted fundamentals score with sector-relative PE/PB/D/E."""
        sector = str(row.get("sector") or "Other")
        medians = sector_medians.get(sector, {})
        parts: dict[str, float] = {}
        metrics_used = 0

        # --- PE (lower is better, sector-relative) ---
        pe = row.get("pe_ratio")
        if pe is not None and pe > 0:
            median_pe = medians.get("pe", 25.0)
            ratio = pe / max(median_pe, 1.0)
            parts["pe"] = self._clamp(100 - (ratio * 50))
            metrics_used += 1

        # --- ROE (higher is better) ---
        roe = row.get("roe")
        if roe is not None:
            parts["roe"] = self._clamp(roe * 4.5)
            metrics_used += 1

        # --- ROCE (higher is better) ---
        roce = row.get("roce")
        if roce is not None:
            parts["roce"] = self._clamp(roce * 4.0)
            metrics_used += 1

        # --- Debt-to-Equity (lower is better, sector-relative) ---
        dte = row.get("debt_to_equity")
        if dte is not None:
            median_de = medians.get("de", 1.0)
            ratio = dte / max(median_de, 0.1)
            # At sector median → score 60, at 2× → 20, at 0 → 100
            parts["dte"] = self._clamp(100 - (ratio * 40))
            metrics_used += 1

        # --- Price-to-Book (lower is better, sector-relative) ---
        pb = row.get("price_to_book")
        if pb is not None and pb > 0:
            median_pb = medians.get("pb", 4.0)
            ratio = pb / max(median_pb, 0.5)
            parts["pb"] = self._clamp(100 - (ratio * 40))
            metrics_used += 1

        if not parts:
            return 50.0, metrics_used

        # Weighted average: ROE 30%, ROCE 25%, PE 20%, D/E 15%, P/B 10%
        weights = {"roe": 0.30, "roce": 0.25, "pe": 0.20, "dte": 0.15, "pb": 0.10}
        total_w = 0.0
        weighted_sum = 0.0
        for key, score in parts.items():
            w = weights.get(key, 0.10)
            weighted_sum += score * w
            total_w += w
        result = weighted_sum / total_w if total_w > 0 else 50.0

        # EPS quality gate: negative EPS caps fundamentals at 40
        eps = row.get("eps")
        if eps is not None and eps < 0:
            result = min(result, 40.0)

        return round(result, 2), metrics_used

    @staticmethod
    def _generate_tags(
        row: dict,
        *,
        sector_pe_median: float,
        has_volatility: bool,
        volatility_score: float,
        has_growth: bool,
        growth_score: float,
        fundamentals_score: float,
        liquidity_score: float,
        pct_raw: float,
        source_status: str,
    ) -> list[str]:
        """Generate descriptive auto-tags (max 4)."""
        tags: list[str] = []
        market_cap = row.get("market_cap")
        roe = row.get("roe")
        roce = row.get("roce")
        dte = row.get("debt_to_equity")
        eps = row.get("eps")
        pe = row.get("pe_ratio")
        dividend_yield = row.get("dividend_yield")
        pct_3m = row.get("_pct_change_3m")

        # --- Market Cap tags ---
        if market_cap is not None and market_cap > 0:
            if market_cap >= 20_000:
                tags.append("Large Cap")
            elif market_cap >= 5_000:
                tags.append("Mid Cap")
            else:
                tags.append("Small Cap")

        # --- Quality tags ---
        if (roe is not None and roe >= 20
                and roce is not None and roce >= 20
                and (dte is None or dte <= 0.5)):
            tags.append("High Quality")
        elif dte is not None and dte == 0 and eps is not None and eps > 0:
            tags.append("Debt Free")

        # --- Dividend tag ---
        if dividend_yield is not None and dividend_yield >= 2.0:
            tags.append("High Dividend")

        # --- Value / Growth ---
        if (pe is not None and pe > 0 and pe < sector_pe_median * 0.7
                and roe is not None and roe >= 12):
            tags.append("Value Pick")
        elif (pct_3m is not None and pct_3m >= 15
              and has_growth and growth_score >= 70):
            tags.append("Growth Stock")

        # --- Low Volatility ---
        if has_volatility and volatility_score >= 75 and len(tags) < 4:
            tags.append("Low Volatility")

        # --- Negative EPS warning ---
        if eps is not None and eps < 0 and len(tags) < 4:
            tags.append("Negative EPS")

        # Limit to 4 tags
        return tags[:4]

    def _compute_scores(
        self,
        rows: list[dict],
        *,
        volatility_data: dict[str, dict] | None = None,
    ) -> list[dict]:
        if not rows:
            return []

        vol_data = volatility_data or {}

        # ── Pre-compute sector medians for PE, PB, and D/E ──
        sector_pe: dict[str, list[float]] = {}
        sector_pb: dict[str, list[float]] = {}
        sector_de: dict[str, list[float]] = {}
        for r in rows:
            sector = str(r.get("sector") or "Other")
            pe = r.get("pe_ratio")
            if pe is not None and pe > 0:
                sector_pe.setdefault(sector, []).append(float(pe))
            pb = r.get("price_to_book")
            if pb is not None and pb > 0:
                sector_pb.setdefault(sector, []).append(float(pb))
            de = r.get("debt_to_equity")
            if de is not None and de >= 0:
                sector_de.setdefault(sector, []).append(float(de))

        # Global medians computed from all stocks (not hardcoded)
        all_pe = [v for vals in sector_pe.values() for v in vals]
        all_pb = [v for vals in sector_pb.values() for v in vals]
        all_de = [v for vals in sector_de.values() for v in vals]
        overall_pe_med = statistics.median(all_pe) if len(all_pe) >= 3 else 25.0
        overall_pb_med = statistics.median(all_pb) if len(all_pb) >= 3 else 4.0
        overall_de_med = statistics.median(all_de) if len(all_de) >= 3 else 1.0

        sector_medians: dict[str, dict[str, float]] = {}
        all_sectors = set(str(r.get("sector") or "Other") for r in rows)
        for sector in all_sectors:
            pe_vals = sector_pe.get(sector, [])
            pb_vals = sector_pb.get(sector, [])
            de_vals = sector_de.get(sector, [])
            sector_medians[sector] = {
                "pe": statistics.median(pe_vals) if len(pe_vals) >= 3 else overall_pe_med,
                "pb": statistics.median(pb_vals) if len(pb_vals) >= 3 else overall_pb_med,
                "de": statistics.median(de_vals) if len(de_vals) >= 3 else overall_de_med,
            }

        # ── Pre-compute robust percentile data for daily momentum (winsorized) ──
        all_pcts_raw = [float(r.get("percent_change") or 0.0) for r in rows]
        if all_pcts_raw:
            if len(all_pcts_raw) >= 8:
                pct_lo = self._quantile(all_pcts_raw, 0.05)
                pct_hi = self._quantile(all_pcts_raw, 0.95)
            else:
                pct_lo = min(all_pcts_raw)
                pct_hi = max(all_pcts_raw)
        else:
            pct_lo = -5.0
            pct_hi = 5.0
        if pct_lo > pct_hi:
            pct_lo, pct_hi = pct_hi, pct_lo
        all_pcts = [max(pct_lo, min(pct_hi, v)) for v in all_pcts_raw]

        # ── Pre-compute multi-day momentum percentile data (5d, 20d returns) ──
        all_momentum_5d: list[float] = []
        all_momentum_20d: list[float] = []
        momentum_5d_by_sym: dict[str, float] = {}
        momentum_20d_by_sym: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd:
                m5 = vd.get("momentum_5d")
                if m5 is not None:
                    all_momentum_5d.append(m5)
                    momentum_5d_by_sym[sym] = m5
                m20 = vd.get("momentum_20d")
                if m20 is not None:
                    all_momentum_20d.append(m20)
                    momentum_20d_by_sym[sym] = m20

        # ── Pre-compute 52W position scores for blending into momentum ──
        pos_52w_by_sym: dict[str, float] = {}
        all_pos_52w: list[float] = []
        for r in rows:
            sym = str(r.get("symbol") or "")
            price = r.get("last_price")
            high_52w = r.get("high_52w")
            low_52w = r.get("low_52w")
            pos = self._score_52w_position(price, high_52w, low_52w)
            if pos is not None:
                pos_52w_by_sym[sym] = pos
                all_pos_52w.append(pos)

        # ── Pre-compute multi-day liquidity percentile data (5d, 20d avg volume) ──
        all_avg_vol_5d: list[float] = []
        all_avg_vol_20d: list[float] = []
        avg_vol_5d_by_sym: dict[str, float] = {}
        avg_vol_20d_by_sym: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd:
                v5 = vd.get("avg_vol_5d")
                if v5 is not None and v5 > 0:
                    log_v5 = math.log1p(v5)
                    all_avg_vol_5d.append(log_v5)
                    avg_vol_5d_by_sym[sym] = log_v5
                v20 = vd.get("avg_vol_20d")
                if v20 is not None and v20 > 0:
                    log_v20 = math.log1p(v20)
                    all_avg_vol_20d.append(log_v20)
                    avg_vol_20d_by_sym[sym] = log_v20

        # Fallback: daily liquidity percentile data using log scale.
        all_tv_logs = [
            math.log1p(max(0.0, float(r.get("traded_value") or 0.0)))
            for r in rows
        ]
        all_vol_logs = [
            math.log1p(max(0.0, float(r.get("volume") or 0.0)))
            for r in rows
        ]

        # ── Pre-compute volatility std_dev values for percentile ranking ──
        all_std_devs: list[float] = []
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd and vd.get("std_dev") is not None:
                all_std_devs.append(vd["std_dev"])

        # ── Pre-compute multi-period growth data for percentile ranking ──
        # Growth = blend of 3M (25%) + 1Y (35%) + 3Y (40%) price appreciation.
        all_pct_3m: list[float] = []
        all_pct_1y: list[float] = []
        all_pct_3y: list[float] = []
        pct_3m_by_sym: dict[str, float] = {}
        pct_1y_by_sym: dict[str, float] = {}
        pct_3y_by_sym: dict[str, float] = {}
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if not vd:
                continue
            p3m = vd.get("pct_change_3m")
            if p3m is not None:
                pct_3m_by_sym[sym] = p3m
                all_pct_3m.append(p3m)
            p1y = vd.get("pct_change_1y")
            if p1y is not None:
                pct_1y_by_sym[sym] = p1y
                all_pct_1y.append(p1y)
            p3y = vd.get("pct_change_3y")
            if p3y is not None:
                pct_3y_by_sym[sym] = p3y
                all_pct_3y.append(p3y)

        # Track sector scores for sector_leader tag
        sector_best: dict[str, tuple[float, str]] = {}

        out: list[dict] = []
        for row in rows:
            symbol = str(row.get("symbol") or "")
            sector = str(row.get("sector") or "Other")
            pct_raw = float(row.get("percent_change") or 0.0)
            pct = max(pct_lo, min(pct_hi, pct_raw))
            tv_log = math.log1p(max(0.0, float(row.get("traded_value") or 0.0)))
            vol_log = math.log1p(max(0.0, float(row.get("volume") or 0.0)))

            # ── Momentum: blend price momentum (5d/20d/daily) + 52W position ──
            daily_momentum = self._clamp(self._percentile_rank(all_pcts, pct))
            has_m5 = symbol in momentum_5d_by_sym and all_momentum_5d
            has_m20 = symbol in momentum_20d_by_sym and all_momentum_20d
            if has_m5 and has_m20:
                m5_pctile = self._percentile_rank(all_momentum_5d, momentum_5d_by_sym[symbol])
                m20_pctile = self._percentile_rank(all_momentum_20d, momentum_20d_by_sym[symbol])
                price_momentum = self._clamp(m5_pctile * 0.50 + m20_pctile * 0.30 + daily_momentum * 0.20)
            elif has_m5:
                m5_pctile = self._percentile_rank(all_momentum_5d, momentum_5d_by_sym[symbol])
                price_momentum = self._clamp(m5_pctile * 0.65 + daily_momentum * 0.35)
            else:
                price_momentum = daily_momentum

            # Blend 52W position into momentum (60% price + 40% 52W position)
            if symbol in pos_52w_by_sym:
                momentum = self._clamp(price_momentum * 0.60 + pos_52w_by_sym[symbol] * 0.40)
            else:
                momentum = price_momentum

            # ── Liquidity: multi-day volume ──
            has_v5 = symbol in avg_vol_5d_by_sym and all_avg_vol_5d
            has_v20 = symbol in avg_vol_20d_by_sym and all_avg_vol_20d
            if has_v5 and has_v20:
                v5_pctile = self._percentile_rank(all_avg_vol_5d, avg_vol_5d_by_sym[symbol])
                v20_pctile = self._percentile_rank(all_avg_vol_20d, avg_vol_20d_by_sym[symbol])
                liquidity = self._clamp(v5_pctile * 0.70 + v20_pctile * 0.30)
            elif has_v5:
                v5_pctile = self._percentile_rank(all_avg_vol_5d, avg_vol_5d_by_sym[symbol])
                liquidity = self._clamp(v5_pctile)
            else:
                tv_percentile = self._percentile_rank(all_tv_logs, tv_log)
                vol_percentile = self._percentile_rank(all_vol_logs, vol_log)
                liquidity = self._clamp((tv_percentile * 0.60) + (vol_percentile * 0.40))

            # ── Fundamentals ──
            fundamentals, metrics_used = self._score_fundamentals(row, sector_medians)
            if metrics_used > 0:
                coverage = metrics_used / 5.0
                fundamentals = self._clamp(
                    self._shrink_to_neutral(fundamentals, coverage),
                )

            # ── Volatility score (lower std_dev → higher score = stability premium) ──
            vd = vol_data.get(symbol)
            has_volatility = False
            volatility_score: float | None = None
            pct_change_3m = None
            pct_change_1y = None
            pct_change_3y = None
            if vd:
                pct_change_3m = vd.get("pct_change_3m")
                pct_change_1y = vd.get("pct_change_1y")
                pct_change_3y = vd.get("pct_change_3y")
                sd = vd.get("std_dev")
                if sd is not None and all_std_devs:
                    # Invert: lower volatility → higher percentile (more stable = better)
                    raw_pctile = self._percentile_rank(all_std_devs, sd)
                    vol_raw = self._clamp(100.0 - raw_pctile)
                    # Adjust for market cap: small caps get a boost
                    volatility_score = self._adjust_volatility_for_cap(
                        vol_raw, row.get("market_cap"),
                    )
                    has_volatility = True

            # ── Growth score (multi-period: 25% 3M + 35% 1Y + 40% 3Y) ──
            has_growth = False
            growth_score: float | None = None

            has_3m = symbol in pct_3m_by_sym and all_pct_3m
            has_1y = symbol in pct_1y_by_sym and all_pct_1y
            has_3y = symbol in pct_3y_by_sym and all_pct_3y

            if has_3m or has_1y or has_3y:
                growth_parts: list[tuple[float, float]] = []
                if has_3m:
                    rank_3m = self._percentile_rank(all_pct_3m, pct_3m_by_sym[symbol])
                    growth_parts.append((rank_3m, 0.25))
                if has_1y:
                    rank_1y = self._percentile_rank(all_pct_1y, pct_1y_by_sym[symbol])
                    growth_parts.append((rank_1y, 0.35))
                if has_3y:
                    rank_3y = self._percentile_rank(all_pct_3y, pct_3y_by_sym[symbol])
                    growth_parts.append((rank_3y, 0.40))
                total_gw = sum(w for _, w in growth_parts)
                growth_score = self._clamp(
                    sum(s * (w / total_gw) for s, w in growth_parts)
                )
                has_growth = True

            # ── 5-component weighted total ──
            # Target weights: Momentum 15%, Liquidity 10%, Fundamentals 35%,
            #                 Volatility 15%, Growth 25%
            target_weights = {
                "momentum": 0.15,
                "liquidity": 0.10,
                "fundamentals": 0.35,
                "volatility": 0.15,
                "growth": 0.25,
            }
            scores_map = {
                "momentum": momentum,
                "liquidity": liquidity,
                "fundamentals": fundamentals,
                "volatility": volatility_score if has_volatility else 0.0,
                "growth": growth_score if has_growth else 0.0,
            }

            # Dynamic reweighting: exclude unavailable components, redistribute.
            available_weights: dict[str, float] = {}
            for k, w in target_weights.items():
                if k == "volatility" and not has_volatility:
                    continue
                if k == "growth" and not has_growth:
                    continue
                if k == "fundamentals" and metrics_used == 0:
                    continue
                available_weights[k] = w

            if not available_weights:
                # Absolute fallback: signal-only
                combined_signal = (momentum + liquidity) / 2.0
                total = self._clamp(combined_signal, lo=20.0, hi=80.0)
            else:
                total_w = sum(available_weights.values())
                total = sum(
                    scores_map[k] * (w / total_w) for k, w in available_weights.items()
                )

            # Shrink fundamentals influence when data coverage is low
            if metrics_used > 0 and metrics_used < 4:
                signal_only = (momentum * 0.6 + liquidity * 0.4)
                blend_factor = metrics_used / 5.0
                total = (total * blend_factor) + (signal_only * (1.0 - blend_factor))

            source_status = str(row.get("source_status") or "limited").strip().lower()
            if metrics_used == 0 and source_status == "primary":
                source_status = "fallback"
            status_penalty = 0.0
            if source_status == "fallback":
                status_penalty = 5.0
            elif source_status == "limited":
                status_penalty = 12.0

            score = round(self._clamp(total - status_penalty), 2)

            # ── Auto-tags ──
            med_pe = sector_medians.get(sector, {}).get("pe", 25.0)
            # Pass pct_change_3m via a temporary key for tag generation
            row_for_tags = {**row, "_pct_change_3m": pct_change_3m}
            tags = self._generate_tags(
                row_for_tags,
                sector_pe_median=med_pe,
                has_volatility=has_volatility,
                volatility_score=volatility_score if volatility_score is not None else 50.0,
                has_growth=has_growth,
                growth_score=growth_score if growth_score is not None else 50.0,
                fundamentals_score=fundamentals,
                liquidity_score=liquidity,
                pct_raw=pct_raw,
                source_status=source_status,
            )

            # Track sector leader
            if sector not in sector_best or score > sector_best[sector][0]:
                sector_best[sector] = (score, symbol)

            # Extract 1W change from volatility data
            pct_change_1w = vd.get("pct_change_1w") if vd else None

            enriched = {
                **row,
                "score": score,
                "score_momentum": round(momentum, 2),
                "score_liquidity": round(liquidity, 2),
                "score_fundamentals": round(fundamentals, 2),
                "score_volatility": round(volatility_score, 2) if volatility_score is not None else None,
                "score_growth": round(growth_score, 2) if growth_score is not None else None,
                "percent_change_3m": pct_change_3m,
                "percent_change_1w": pct_change_1w,
                "percent_change_1y": pct_change_1y,
                "percent_change_3y": pct_change_3y,
                "score_breakdown": {
                    "momentum": round(momentum, 2),
                    "liquidity": round(liquidity, 2),
                    "fundamentals": round(fundamentals, 2),
                    "volatility": round(volatility_score, 2) if volatility_score is not None else None,
                    "growth": round(growth_score, 2) if growth_score is not None else None,
                    "52w_position": pos_52w_by_sym.get(symbol),
                    "combined_signal": round((momentum + liquidity) / 2.0, 2),
                },
                "tags": tags,
                "source_status": source_status,
            }
            out.append(enriched)

        # Add sector_leader tag to the top scorer in each sector
        for enriched in out:
            symbol = str(enriched.get("symbol") or "")
            sector = str(enriched.get("sector") or "Other")
            best = sector_best.get(sector)
            if best and best[1] == symbol:
                tags = enriched["tags"]
                if "Sector Leader" not in tags and len(tags) < 4:
                    tags.insert(0, "Sector Leader")

        out.sort(
            key=lambda item: (
                -float(item.get("score") or 0.0),
                -float(item.get("percent_change") or 0.0),
                str(item.get("symbol") or ""),
            )
        )
        return out

    def _build_snapshot_row(
        self,
        stock: DiscoverStockDef,
        quote: dict,
        quote_source: str,
        *,
        fundamentals_enabled: bool | None = None,
    ) -> dict:
        use_fundamentals = bool(getattr(stock, "fundamentals_enabled", True))
        if fundamentals_enabled is not None:
            use_fundamentals = fundamentals_enabled

        if use_fundamentals:
            fundamentals, fundamentals_source = self._fetch_screener_fundamentals(stock.nse_symbol)
        else:
            fundamentals = {
                "pe_ratio": None,
                "roe": None,
                "roce": None,
                "debt_to_equity": None,
                "price_to_book": None,
                "eps": None,
                "market_cap": None,
                "high_52w": None,
                "low_52w": None,
                "dividend_yield": None,
            }
            fundamentals_source = "unavailable"
        fundamentals_count = sum(1 for k, v in fundamentals.items() if v is not None and not k.startswith("_"))

        source_status = "primary" if (fundamentals_source == "screener_in" and fundamentals_count >= 2) else "fallback"
        if fundamentals_count == 0 and quote_source not in {"nse_quote_api", "nse_bhavcopy"}:
            source_status = "limited"

        # Use Screener.in sector if stock had default "Diversified" sector
        sector = stock.sector
        screener_sector = fundamentals.pop("_screener_sector", None)
        if sector in ("Diversified", "Other") and screener_sector:
            sector = _map_screener_sector(screener_sector)

        return {
            "market": "IN",
            "symbol": stock.nse_symbol,
            "display_name": stock.display_name,
            "sector": sector,
            "last_price": quote["last_price"],
            "point_change": quote.get("point_change"),
            "percent_change": quote.get("percent_change"),
            "volume": quote.get("volume"),
            "traded_value": quote.get("traded_value"),
            "pe_ratio": fundamentals.get("pe_ratio"),
            "roe": fundamentals.get("roe"),
            "roce": fundamentals.get("roce"),
            "debt_to_equity": fundamentals.get("debt_to_equity"),
            "price_to_book": fundamentals.get("price_to_book"),
            "eps": fundamentals.get("eps"),
            "market_cap": fundamentals.get("market_cap"),
            "high_52w": fundamentals.get("high_52w"),
            "low_52w": fundamentals.get("low_52w"),
            "dividend_yield": fundamentals.get("dividend_yield"),
            "source_status": source_status,
            "source_timestamp": quote.get("source_timestamp") or datetime.now(timezone.utc),
            "primary_source": fundamentals_source,
            "secondary_source": quote_source,
        }

    def _fetch_one(self, stock: DiscoverStockDef) -> dict | None:
        quote = self._fetch_nse_quote(stock.nse_symbol)
        quote_source = "nse_quote_api"
        if quote is None:
            quote = self._fetch_yahoo_quote(stock.yahoo_symbol)
            quote_source = "yahoo_finance_api"
        if quote is None:
            return None
        return self._build_snapshot_row(stock, quote, quote_source)

    def fetch_raw_rows(self) -> list[dict]:
        """Fetch quotes + fundamentals (sync I/O). Does NOT score."""
        universe = self._build_effective_universe()
        bulk_quotes, _ = self._fetch_latest_bhavcopy_quotes()
        raw_rows: list[dict] = []
        if bulk_quotes:
            fundamentals_symbols = self._select_fundamentals_symbols(universe, bulk_quotes)
            missing: list[DiscoverStockDef] = []
            for stock in universe:
                quote = bulk_quotes.get(stock.nse_symbol)
                if quote is None:
                    missing.append(stock)
                    continue
                raw_rows.append(
                    self._build_snapshot_row(
                        stock,
                        quote,
                        "nse_bhavcopy",
                        fundamentals_enabled=stock.nse_symbol in fundamentals_symbols,
                    )
                )
            if missing and self._missing_quote_retry_limit > 0:
                retry_batch = missing[: self._missing_quote_retry_limit]
                logger.warning(
                    "Bhavcopy missing %d/%d symbols; retrying fallback quotes for %d symbols",
                    len(missing),
                    len(universe),
                    len(retry_batch),
                )
                for stock in retry_batch:
                    item = self._fetch_one(stock)
                    if item is not None:
                        raw_rows.append(item)
        else:
            # If bhavcopy is unavailable, keep serving a smaller reliable subset.
            fallback_universe = CORE_UNIVERSE if len(universe) > len(CORE_UNIVERSE) else universe
            for stock in fallback_universe:
                item = self._fetch_one(stock)
                if item is not None:
                    raw_rows.append(item)
            if len(raw_rows) < len(fallback_universe):
                logger.warning(
                    "Fallback quote path updated %d/%d symbols",
                    len(raw_rows),
                    len(fallback_universe),
                )
        return raw_rows

    def fetch_all(self, *, volatility_data: dict[str, dict] | None = None) -> list[dict]:
        raw_rows = self.fetch_raw_rows()
        return self._compute_scores(raw_rows, volatility_data=volatility_data)


_scraper = DiscoverStockScraper()


def _fetch_discover_stock_raw_sync() -> list[dict]:
    return _scraper.fetch_raw_rows()


async def run_discover_stock_job() -> None:
    try:
        # 1. Pre-fetch volatility data from PostgreSQL (async).
        volatility_data = await discover_service.get_bulk_stock_volatility_data()
        logger.info(
            "Discover stock: volatility_data has %d symbols (sample keys: %s)",
            len(volatility_data),
            list(volatility_data.keys())[:5] if volatility_data else "EMPTY",
        )

        # 2. Fetch quotes + fundamentals (sync network I/O in executor).
        loop = asyncio.get_event_loop()
        raw_rows = await loop.run_in_executor(
            get_job_executor("discover-stock"),
            _fetch_discover_stock_raw_sync,
        )
        logger.info("Discover stock: fetched %d raw rows", len(raw_rows))

        # 3. Score with all 5 components (CPU-bound, fast).
        rows = _scraper._compute_scores(raw_rows, volatility_data=volatility_data)

        # Log sample scores to verify all components populated
        if rows:
            sample = rows[0]
            logger.info(
                "Discover stock: sample scored row %s → score=%.2f vol=%s growth=%s "
                "pct3m=%s pct1y=%s pct3y=%s tags=%s",
                sample.get("symbol"),
                sample.get("score", 0),
                sample.get("score_volatility"),
                sample.get("score_growth"),
                sample.get("percent_change_3m"),
                sample.get("percent_change_1y"),
                sample.get("percent_change_3y"),
                sample.get("tags", [])[:3],
            )

        count = await discover_service.upsert_discover_stock_snapshots(rows)
        logger.info("Discover stock job complete: %d snapshots upserted", count)
    except requests.RequestException:
        logger.exception("Discover stock job failed due to network exception")
    except Exception:
        logger.exception("Discover stock job failed")
