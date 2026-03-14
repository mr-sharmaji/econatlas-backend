from __future__ import annotations

import asyncio
import csv
import io
import logging
import math
import re
import statistics
import time as time_mod
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


# ---------------------------------------------------------------------------
# Screener.in Broad Sector → clean sector name mapping
# ---------------------------------------------------------------------------
_SCREENER_BROAD_SECTOR_MAP: dict[str, str] = {
    "energy": "Energy",
    "information technology": "IT",
    "financial services": "Financials",
    "fast moving consumer goods": "FMCG",
    "consumer discretionary": "Consumer Discretionary",
    "consumer staples": "FMCG",
    "healthcare": "Healthcare",
    "pharmaceuticals": "Healthcare",
    "industrials": "Industrials",
    "capital goods": "Industrials",
    "construction": "Industrials",
    "construction materials": "Industrials",
    "materials": "Materials",
    "chemicals": "Chemicals",
    "metals & mining": "Materials",
    "telecommunication": "Telecom",
    "real estate": "Real Estate",
    "media entertainment & publication": "Media & Entertainment",
    "media": "Media & Entertainment",
    "services": "Services",
    "utilities": "Utilities",
    "automobile and auto components": "Auto",
    "automobile": "Auto",
    "diversified": "Diversified",
    "textiles": "Textiles",
    "forest materials": "Materials",
    "consumer services": "Services",
    "oil gas & consumable fuels": "Energy",
    "power": "Energy",
    "realty": "Real Estate",
}

# Legacy keyword-based fallback for sector classification
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
    "fmcg": "FMCG", "consumer": "Consumer Discretionary", "retail": "Consumer Discretionary",
    "food": "FMCG", "beverage": "FMCG", "textile": "Textiles",
    "apparel": "Consumer Discretionary", "personal care": "FMCG",
    # Auto
    "auto": "Auto", "automobile": "Auto", "vehicle": "Auto",
    "tyre": "Auto", "tire": "Auto",
    # Industrials
    "capital goods": "Industrials", "industrial": "Industrials",
    "engineering": "Industrials", "construction": "Industrials",
    "infrastructure": "Industrials", "cement": "Industrials",
    "defence": "Industrials", "defense": "Industrials",
    # Materials / Chemicals
    "metals": "Materials", "steel": "Materials", "aluminium": "Materials",
    "mining": "Materials", "chemicals": "Chemicals", "paper": "Materials",
    "fertilizer": "Chemicals", "plastic": "Chemicals",
    # Telecom
    "telecom": "Telecom", "communication": "Telecom",
    # Real Estate
    "real estate": "Real Estate", "realty": "Real Estate", "housing": "Real Estate",
    # Media
    "media": "Media & Entertainment", "entertainment": "Media & Entertainment",
}

# Expanded curated sector mapping for common stocks not in INDIA_STOCKS
_EXTRA_SECTOR_MAP: dict[str, str] = {
    "ATGL": "Energy", "NTPCGREEN": "Energy", "JSWENERGY": "Energy",
    "ADANIENERGY": "Energy", "CESC": "Energy", "NHPC": "Energy",
    "TORNTPOWER": "Utilities", "SJVN": "Energy",
    "DOMS": "Consumer Discretionary", "JINDALSAW": "Materials", "JINDALSTEL": "Materials",
    "TATASTEEL": "Materials", "HINDALCO": "Materials", "VEDL": "Materials",
    "NMDC": "Materials", "COALINDIA": "Energy",
    "DABUR": "FMCG", "GODREJCP": "FMCG", "MARICO": "FMCG",
    "PIDILITIND": "FMCG", "COLPAL": "FMCG", "BRITANNIA": "FMCG",
    "PAGEIND": "Consumer Discretionary", "VBL": "FMCG", "TRENT": "Consumer Discretionary",
    "IRCTC": "Consumer Discretionary", "ZOMATO": "Consumer Discretionary", "NYKAA": "Consumer Discretionary",
    "DMART": "Consumer Discretionary", "TITAN": "Consumer Discretionary",
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
    "DELHIVERY": "Industrials", "PIIND": "Chemicals", "AARTI": "Chemicals",
    "DEEPAKNTR": "Chemicals", "UPL": "Chemicals", "SRF": "Chemicals",
    "PVRINOX": "Media & Entertainment", "SUNTV": "Media & Entertainment", "ZEEL": "Media & Entertainment",
    # Additional sector mappings to reduce "Other"
    "JUBLFOOD": "Consumer Discretionary", "MCDOWELL": "FMCG", "UBL": "FMCG",
    "TATACONSUM": "FMCG", "EMAMILTD": "FMCG", "JYOTHYLAB": "FMCG",
    "RADICO": "FMCG", "BATAINDIA": "Consumer Discretionary", "RELAXO": "Consumer Discretionary",
    "WHIRLPOOL": "Consumer Discretionary", "BLUESTARLT": "Consumer Discretionary", "CROMPTON": "Consumer Discretionary",
    "KAJARIACER": "Materials", "CENTURYTEX": "Textiles", "GRASIM": "Materials",
    "FLUOROCHEM": "Chemicals", "CLEAN": "Chemicals", "NAVINFLUOR": "Chemicals",
    "SUMICHEM": "Chemicals", "BASF": "Chemicals", "TATACHEM": "Chemicals",
    "FINEORG": "Chemicals", "ALKYLAMINE": "Chemicals",
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
    "BSE": "Financials", "MCX": "Financials",
    "CDSL": "Financials", "CAMS": "Financials", "KFIN": "Financials",
    "BRIGADE": "Real Estate", "SOBHA": "Real Estate", "MAHLIFE": "Real Estate",
    "LODHA": "Real Estate", "RAYMOND": "Consumer Discretionary",
    "TTML": "Telecom", "VODAFONE": "Telecom",
    "GPPL": "Utilities", "POWERGRID": "Utilities",
    "IEX": "Utilities",
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


# ---------------------------------------------------------------------------
# Yahoo Finance v10 quoteSummary via curl_cffi (browser impersonation)
# ---------------------------------------------------------------------------

class YahooFinanceSession:
    """Yahoo v10 quoteSummary via curl_cffi with crumb caching."""

    def __init__(self, crumb_ttl: int = 600):
        self._session = None
        self._crumb: str | None = None
        self._crumb_ts: float = 0.0
        self._crumb_ttl = crumb_ttl

    def _ensure_session(self) -> None:
        if self._session and self._crumb and time_mod.time() - self._crumb_ts < self._crumb_ttl:
            return
        # Try curl_cffi first (bypasses TLS fingerprinting), fall back to requests
        session = None
        try:
            from curl_cffi import requests as cffi_requests
            session = cffi_requests.Session(impersonate="chrome")
            logger.info("Yahoo v10: using curl_cffi session")
        except Exception as exc:
            logger.warning("curl_cffi unavailable (%s), falling back to requests", exc)
            import requests as std_requests
            session = std_requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            })
        self._session = session
        try:
            self._session.get("https://fc.yahoo.com", timeout=10)
        except Exception:
            pass
        time_mod.sleep(1)
        r = self._session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        crumb = r.text.strip()
        if "Too Many" in crumb or "error" in crumb.lower() or len(crumb) < 5:
            raise RuntimeError(f"Yahoo crumb failed: {crumb!r}")
        self._crumb = crumb
        self._crumb_ts = time_mod.time()
        logger.info("Yahoo v10: crumb obtained successfully (len=%d)", len(crumb))

    def get_stock_data(self, nse_symbol: str) -> dict:
        """Fetch comprehensive stock data from Yahoo v10 quoteSummary."""
        self._ensure_session()
        modules = "defaultKeyStatistics,financialData,summaryDetail,recommendationTrend"
        url = (
            f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
            f"{nse_symbol}.NS?modules={modules}&crumb={self._crumb}"
        )
        r = self._session.get(url, timeout=10)
        data = r.json()
        result = data.get("quoteSummary", {}).get("result", [{}])[0]
        ks = result.get("defaultKeyStatistics", {})
        fd = result.get("financialData", {})
        sd = result.get("summaryDetail", {})
        rt = result.get("recommendationTrend", {}).get("trend", [])

        def _r(d: dict, k: str):
            v = d.get(k, {})
            return v.get("raw") if isinstance(v, dict) else None

        # Analyst consensus
        current_reco = rt[0] if rt else {}

        return {
            # Fundamentals (fallback for Screener gaps)
            "pe_ratio": _r(sd, "trailingPE"),
            "forward_pe": _r(sd, "forwardPE") or _r(ks, "forwardPE"),
            "price_to_book": _r(ks, "priceToBook"),
            "eps": _r(ks, "trailingEps"),
            "forward_eps": _r(ks, "forwardEps"),
            "debt_to_equity": (
                (_r(fd, "debtToEquity") or 0) / 100.0
                if _r(fd, "debtToEquity") is not None else None
            ),
            "dividend_yield": (
                (_r(sd, "dividendYield") or 0) * 100
                if _r(sd, "dividendYield") is not None else None
            ),
            "market_cap": _r(sd, "marketCap"),
            "high_52w": _r(sd, "fiftyTwoWeekHigh"),
            "low_52w": _r(sd, "fiftyTwoWeekLow"),
            # Yahoo-exclusive: Financial Health
            "beta": _r(ks, "beta") or _r(sd, "beta"),
            "free_cash_flow": _r(fd, "freeCashflow"),
            "operating_cash_flow": _r(fd, "operatingCashflow"),
            "total_cash": _r(fd, "totalCash"),
            "total_debt": _r(fd, "totalDebt"),
            "total_revenue": _r(fd, "totalRevenue"),
            "gross_margins": _r(fd, "grossMargins"),
            "operating_margins": _r(fd, "operatingMargins"),
            "profit_margins": _r(fd, "profitMargins"),
            "ebitda_margins": _r(fd, "ebitdaMargins"),
            # Yahoo-exclusive: Growth
            "revenue_growth": _r(fd, "revenueGrowth"),
            "earnings_growth": _r(fd, "earningsGrowth"),
            "earnings_quarterly_growth": _r(ks, "earningsQuarterlyGrowth"),
            # Yahoo-exclusive: Analyst
            "analyst_target_mean": _r(fd, "targetMeanPrice"),
            "analyst_target_median": _r(fd, "targetMedianPrice"),
            "analyst_target_high": _r(fd, "targetHighPrice"),
            "analyst_target_low": _r(fd, "targetLowPrice"),
            "analyst_count": _r(fd, "numberOfAnalystOpinions"),
            "analyst_recommendation": fd.get("recommendationKey"),
            "analyst_recommendation_mean": _r(fd, "recommendationMean"),
            "analyst_strong_buy": current_reco.get("strongBuy", 0),
            "analyst_buy": current_reco.get("buy", 0),
            "analyst_hold": current_reco.get("hold", 0),
            "analyst_sell": (current_reco.get("sell", 0) or 0) + (current_reco.get("strongSell", 0) or 0),
            # Ownership (fallback if Screener missing)
            "held_percent_insiders": _r(ks, "heldPercentInsiders"),
            "held_percent_institutions": _r(ks, "heldPercentInstitutions"),
            # Valuation
            "enterprise_value": _r(ks, "enterpriseValue"),
            "ev_to_ebitda": _r(ks, "enterpriseToEbitda"),
            "ev_to_revenue": _r(ks, "enterpriseToRevenue"),
            "price_to_sales": _r(sd, "priceToSalesTrailing12Months"),
            "payout_ratio": _r(sd, "payoutRatio"),
            # Moving averages
            "fifty_day_avg": _r(sd, "fiftyDayAverage"),
            "two_hundred_day_avg": _r(sd, "twoHundredDayAverage"),
        }


class DiscoverStockScraper(BaseScraper):
    def __init__(self) -> None:
        super().__init__()
        self.settings = get_settings()
        self._nse_ready = False
        self._nse_disabled_until: datetime | None = None
        self._nse_timeout = max(1, int(getattr(self.settings, "discover_stock_nse_timeout_seconds", 4)))
        self._nse_cooldown = max(30, int(getattr(self.settings, "discover_stock_nse_cooldown_seconds", 300)))
        self._screener_timeout = max(2, int(getattr(self.settings, "discover_stock_screener_timeout_seconds", 10)))
        self._screener_max_retries = max(1, int(getattr(self.settings, "discover_stock_screener_max_retries", 3)))
        self._screener_retry_delay = max(0.5, float(getattr(self.settings, "discover_stock_screener_retry_delay", 5.0)))
        self._screener_batch_delay = max(0.0, float(getattr(self.settings, "discover_stock_screener_batch_delay", 0.5)))
        self._yahoo_batch_delay = max(0.0, float(getattr(self.settings, "discover_stock_yahoo_batch_delay", 0.5)))
        self._yahoo_crumb_ttl = max(60, int(getattr(self.settings, "discover_stock_yahoo_crumb_ttl", 600)))
        self._fundamentals_limit = max(
            len(CORE_UNIVERSE),
            int(getattr(self.settings, "discover_stock_fundamentals_limit", 5000)),
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
        # Yahoo v10 session (lazy init)
        self._yahoo_session: YahooFinanceSession | None = None

    def _get_yahoo_session(self) -> YahooFinanceSession:
        if self._yahoo_session is None:
            self._yahoo_session = YahooFinanceSession(crumb_ttl=self._yahoo_crumb_ttl)
        return self._yahoo_session

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
        """Compute Debt-to-Equity from the balance sheet table on Screener.in."""
        bs_match = re.search(r'id="balance-sheet"', html)
        if not bs_match:
            return None
        bs_chunk = html[bs_match.start(): bs_match.start() + 20000]

        def _last_number(label: str) -> float | None:
            idx = bs_chunk.find(label)
            if idx < 0:
                return None
            close_td = bs_chunk.find("</td>", idx)
            if close_td < 0:
                return None
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

    @staticmethod
    def _extract_shareholding(html: str) -> dict:
        """Extract shareholding data from Screener.in <section id="shareholding"> table.

        Returns dict with: promoter_holding, fii_holding, dii_holding,
        government_holding, public_holding, num_shareholders,
        and *_prev variants for QoQ change computation.
        """
        result: dict = {}
        sh_match = re.search(r'id="shareholding"', html)
        if not sh_match:
            return result
        sh_chunk = html[sh_match.start(): sh_match.start() + 15000]

        def _extract_row(label: str) -> tuple[float | None, float | None]:
            """Extract the latest and previous quarter values for a shareholding row."""
            idx = sh_chunk.find(label)
            if idx < 0:
                return None, None
            close_td = sh_chunk.find("</td>", idx)
            if close_td < 0:
                return None, None
            after_start = close_td + 5
            end_tr = sh_chunk.find("</tr>", after_start)
            row_slice = sh_chunk[after_start: end_tr] if end_tr > 0 else sh_chunk[after_start: after_start + 2000]
            nums = re.findall(r'<td[^>]*>\s*([\d,]+(?:\.\d+)?)\s*%?\s*</td>', row_slice)
            latest = None
            prev = None
            if nums:
                try:
                    latest = float(nums[-1].replace(",", ""))
                except ValueError:
                    pass
                if len(nums) >= 2:
                    try:
                        prev = float(nums[-2].replace(",", ""))
                    except ValueError:
                        pass
            return latest, prev

        promoter, promoter_prev = _extract_row("Promoters")
        if promoter is not None:
            result["promoter_holding"] = promoter
            if promoter_prev is not None:
                result["promoter_holding_change"] = round(promoter - promoter_prev, 2)

        fii, fii_prev = _extract_row("FIIs")
        if fii is None:
            fii, fii_prev = _extract_row("Foreign Institutions")
        if fii is not None:
            result["fii_holding"] = fii
            if fii_prev is not None:
                result["fii_holding_change"] = round(fii - fii_prev, 2)

        dii, dii_prev = _extract_row("DIIs")
        if dii is None:
            dii, dii_prev = _extract_row("Domestic Institutions")
        if dii is not None:
            result["dii_holding"] = dii
            if dii_prev is not None:
                result["dii_holding_change"] = round(dii - dii_prev, 2)

        gov, _ = _extract_row("Government")
        if gov is not None:
            result["government_holding"] = gov

        pub, _ = _extract_row("Public")
        if pub is not None:
            result["public_holding"] = pub

        # Number of shareholders
        ns, _ = _extract_row("No. of Shareholders")
        if ns is not None:
            result["num_shareholders"] = int(ns)

        # Pledged promoter shares (critical risk signal)
        pledged, _ = _extract_row("Pledged")
        if pledged is None:
            pledged, _ = _extract_row("Pledge")
        if pledged is not None:
            result["pledged_promoter_pct"] = pledged

        return result

    def _fetch_screener_fundamentals(self, nse_symbol: str) -> tuple[dict, str]:
        """Fetch fundamentals from Screener.in with retry logic and shareholding extraction."""
        base = self.settings.discover_stock_primary_url.rstrip("/")
        candidates = [
            f"{base}/company/{nse_symbol}/consolidated/",
            f"{base}/company/{nse_symbol}/",
        ]
        for url in candidates:
            for attempt in range(self._screener_max_retries):
                try:
                    html = self._get_text(url, timeout=self._screener_timeout)
                    text = unescape(re.sub(r"<[^>]+>", " ", html))
                    text = re.sub(r"\s+", " ", text)

                    book_value = self._extract_labeled_number(text, ["Book Value"])
                    current_price = self._extract_labeled_number(text, ["Current Price"])

                    debt_to_equity = self._extract_balance_sheet_de(html)

                    fundamentals: dict = {
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

                    # --- Extract sector/industry from HTML attributes (NOT flattened text) ---
                    broad_sector_match = re.search(r'title="Broad Sector">([^<]+)</a>', html)
                    industry_match = re.search(r'title="Industry">([^<]+)</a>', html)

                    if broad_sector_match:
                        fundamentals["_screener_broad_sector"] = unescape(broad_sector_match.group(1).strip())
                    if industry_match:
                        fundamentals["_screener_industry"] = unescape(industry_match.group(1).strip())

                    # 52-week High / Low
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

                    # --- Extract shareholding from HTML ---
                    shareholding = self._extract_shareholding(html)
                    sh_fields = sum(1 for v in shareholding.values() if v is not None)
                    if sh_fields > 0:
                        logger.debug(
                            "Screener shareholding for %s: %d fields (promoter=%.1f%%)",
                            nse_symbol, sh_fields,
                            shareholding.get("promoter_holding") or 0,
                        )
                    fundamentals.update(shareholding)

                    return fundamentals, "screener_in"
                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code if e.response is not None else 0
                    if status in (429, 503) and attempt < self._screener_max_retries - 1:
                        wait = self._screener_retry_delay * (attempt + 1)
                        logger.warning("Screener %d for %s; retry in %.0fs (%d/%d)", status, nse_symbol, wait, attempt + 1, self._screener_max_retries)
                        time_mod.sleep(wait)
                        continue
                    break
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
                    if attempt < self._screener_max_retries - 1:
                        time_mod.sleep(self._screener_retry_delay)
                        continue
                    break
                except Exception:
                    logger.debug("Screener fundamentals fetch failed for %s url=%s", nse_symbol, url, exc_info=True)
                    break
        return {
            "pe_ratio": None, "roe": None, "roce": None,
            "debt_to_equity": None, "price_to_book": None,
            "eps": None, "market_cap": None, "high_52w": None,
            "low_52w": None, "dividend_yield": None,
        }, "unavailable"

    @staticmethod
    def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
        return max(lo, min(hi, value))

    @staticmethod
    def _percentile_rank(values: list[float], target: float) -> float:
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
        neutral: float = 42.0,
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
        if price is None or high_52w is None or low_52w is None:
            return None
        if high_52w <= low_52w or price <= 0:
            return None
        position = (price - low_52w) / (high_52w - low_52w)
        position = max(0.0, min(1.0, position))
        return round(self._clamp(position * 95 + 5), 2)

    @staticmethod
    def _adjust_volatility_for_cap(
        vol_score: float,
        market_cap: float | None,
    ) -> float:
        if market_cap is None or market_cap <= 0:
            return vol_score
        if market_cap >= 20_000:
            return vol_score
        if market_cap >= 5_000:
            return min(100.0, vol_score * 1.08)
        return min(100.0, vol_score * 1.15)

    def _score_fundamentals(
        self,
        row: dict,
        sector_medians: dict[str, dict[str, float]],
    ) -> tuple[float, int]:
        """Weighted fundamentals score with sector-relative PE/PB/D/E.

        Uses sector-specific weight profiles:
        - Financials: ROE-heavy, skip D/E (banks are naturally leveraged),
          use operating_margins as NIM proxy.
        - Cyclicals (Commodities, Energy): P/B over PE (high PE = buy signal
          at cycle bottoms).
        - Default: balanced ROE/ROCE/PE/D/E/P/B weights.
        """
        sector = str(row.get("sector") or "Other")
        medians = sector_medians.get(sector, {})
        parts: dict[str, float] = {}
        metrics_used = 0

        is_financial = sector == "Financials"
        is_cyclical = sector in ("Commodities", "Energy")

        pe = row.get("pe_ratio")
        if pe is not None and pe > 0:
            median_pe = medians.get("pe", 25.0)
            ratio = pe / max(median_pe, 1.0)
            parts["pe"] = self._clamp(100 - (ratio * 50))
            metrics_used += 1

        roe = row.get("roe")
        roce = row.get("roce")
        if roe is not None:
            roe_score = self._clamp(roe * 4.5)
            # DuPont discount: if ROE is >1.5× ROCE, it's leverage-inflated
            if roce is not None and roce > 0 and roe > roce * 1.5:
                roe_score *= 0.85
            parts["roe"] = roe_score
            metrics_used += 1

        if roce is not None:
            parts["roce"] = self._clamp(roce * 4.0)
            metrics_used += 1

        dte = row.get("debt_to_equity")
        # Skip D/E for financials — banks are naturally leveraged
        if not is_financial and dte is not None:
            median_de = medians.get("de", 1.0)
            ratio = dte / max(median_de, 0.1)
            parts["dte"] = self._clamp(100 - (ratio * 40))
            metrics_used += 1

        pb = row.get("price_to_book")
        if pb is not None and pb > 0:
            median_pb = medians.get("pb", 4.0)
            ratio = pb / max(median_pb, 0.5)
            parts["pb"] = self._clamp(100 - (ratio * 40))
            metrics_used += 1

        # For financials: use operating_margins as NIM proxy
        if is_financial:
            op_margin = row.get("operating_margins")
            if op_margin is not None:
                parts["margins_proxy"] = self._clamp(op_margin * 200 + 30)
                metrics_used += 1

        if not parts:
            return 50.0, metrics_used

        # Sector-specific weight profiles
        if is_financial:
            weights = {
                "roe": 0.35, "pb": 0.30, "margins_proxy": 0.20,
                "pe": 0.15, "roce": 0.00, "dte": 0.00,
            }
        elif is_cyclical:
            # P/B over PE for cyclicals (high PE = buy signal at trough)
            weights = {
                "roe": 0.30, "roce": 0.25, "pb": 0.20,
                "dte": 0.15, "pe": 0.10,
            }
        else:
            weights = {
                "roe": 0.30, "roce": 0.25, "pe": 0.20,
                "dte": 0.15, "pb": 0.10,
            }

        total_w = 0.0
        weighted_sum = 0.0
        for key, score in parts.items():
            w = weights.get(key, 0.10)
            if w <= 0:
                continue
            weighted_sum += score * w
            total_w += w
        result = weighted_sum / total_w if total_w > 0 else 50.0

        eps = row.get("eps")
        if eps is not None and eps < 0:
            result = min(result, 40.0)

        return round(result, 2), metrics_used

    # ---------------------------------------------------------------------------
    # NEW: Financial Health Score (weight 15%)
    # ---------------------------------------------------------------------------
    def _score_financial_health(self, row: dict) -> tuple[float | None, dict]:
        """Score based on margins, FCF, cash position, debt coverage."""
        parts: dict[str, float] = {}
        mcap = row.get("market_cap")

        # FCF Yield
        fcf = row.get("free_cash_flow")
        if fcf is not None and mcap and mcap > 0:
            fcf_yield = (fcf / (mcap * 1e7)) * 100  # mcap in Cr, fcf in absolute
            parts["fcf_yield"] = self._clamp(fcf_yield * 8 + 30)

        # Operating Margin
        op_margin = row.get("operating_margins")
        if op_margin is not None:
            parts["op_margin"] = self._clamp(op_margin * 200 + 20)

        # Profit Margin
        profit_margin = row.get("profit_margins")
        if profit_margin is not None:
            parts["profit_margin"] = self._clamp(profit_margin * 200 + 20)

        # Net Cash Position
        cash = row.get("total_cash")
        debt = row.get("total_debt")
        if cash is not None and debt is not None and mcap and mcap > 0:
            net_cash_pct = ((cash - debt) / (mcap * 1e7)) * 100
            parts["net_cash"] = self._clamp(50 + net_cash_pct * 3)

        # Payout Ratio
        payout = row.get("payout_ratio")
        if payout is not None and payout > 0:
            if 0.1 <= payout <= 0.6:
                parts["payout"] = 80.0
            elif payout < 0.1:
                parts["payout"] = 50.0
            else:
                parts["payout"] = max(20.0, 100 - payout * 80)

        if not parts:
            return None, {}

        weights = {"fcf_yield": 0.30, "op_margin": 0.25, "profit_margin": 0.20,
                    "net_cash": 0.15, "payout": 0.10}
        total_w = sum(weights.get(k, 0.10) for k in parts)
        score = sum(parts[k] * weights.get(k, 0.10) for k in parts) / total_w

        return round(score, 2), parts

    # ---------------------------------------------------------------------------
    # NEW: Analyst Consensus Score (weight 10%)
    # ---------------------------------------------------------------------------
    def _score_analyst_consensus(self, row: dict) -> tuple[float | None, dict]:
        """Score based on analyst recommendations and price target upside."""
        parts: dict[str, float] = {}

        target = row.get("analyst_target_mean")
        price = row.get("last_price")
        count = row.get("analyst_count")

        if target and price and price > 0:
            upside_pct = ((target - price) / price) * 100
            parts["upside"] = self._clamp(50 + upside_pct * 1.0)

        rec_mean = row.get("analyst_recommendation_mean")
        if rec_mean is not None:
            parts["recommendation"] = self._clamp(100 - (rec_mean - 1.0) * 25)

        if count is not None and count > 0:
            parts["coverage"] = self._clamp(min(100, count * 4 + 20))

        sb = row.get("analyst_strong_buy", 0) or 0
        b = row.get("analyst_buy", 0) or 0
        h = row.get("analyst_hold", 0) or 0
        s = row.get("analyst_sell", 0) or 0
        total = sb + b + h + s
        if total >= 3:
            buy_ratio = (sb + b) / total
            parts["buy_ratio"] = self._clamp(buy_ratio * 100)

        if not parts:
            return None, {}

        weights = {"upside": 0.35, "recommendation": 0.25, "buy_ratio": 0.25, "coverage": 0.15}
        total_w = sum(weights.get(k, 0.10) for k in parts)
        score = sum(parts[k] * weights.get(k, 0.10) for k in parts) / total_w

        return round(score, 2), parts

    # ---------------------------------------------------------------------------
    # NEW: Ownership Score (weight 12%)
    # ---------------------------------------------------------------------------
    def _score_ownership(self, row: dict) -> tuple[float | None, dict]:
        """Score based on promoter/FII/DII levels and QoQ trends."""
        parts: dict[str, float] = {}

        promoter = row.get("promoter_holding")
        if promoter is not None:
            # 50-75% promoter is ideal; too low = weak control, too high = low float
            if 50 <= promoter <= 75:
                parts["promoter_level"] = 80.0
            elif promoter > 75:
                parts["promoter_level"] = 60.0  # Low float risk
            elif promoter >= 30:
                parts["promoter_level"] = 65.0
            else:
                parts["promoter_level"] = 40.0

        fii = row.get("fii_holding")
        if fii is not None:
            # FII > 20% = strong institutional confidence
            parts["fii_level"] = self._clamp(fii * 3 + 20)

        dii = row.get("dii_holding")
        if dii is not None:
            # DII > 25% = domestic institutional backing
            parts["dii_level"] = self._clamp(dii * 3 + 15)

        # QoQ trend signals (buying = bullish, selling = bearish)
        promoter_chg = row.get("promoter_holding_change")
        if promoter_chg is not None:
            parts["promoter_trend"] = self._clamp(50 + promoter_chg * 20)  # +0.5% → 60, -1% → 30

        fii_chg = row.get("fii_holding_change")
        if fii_chg is not None:
            parts["fii_trend"] = self._clamp(50 + fii_chg * 15)

        dii_chg = row.get("dii_holding_change")
        if dii_chg is not None:
            parts["dii_trend"] = self._clamp(50 + dii_chg * 15)

        if not parts:
            return None, {}

        weights = {
            "promoter_level": 0.25, "fii_level": 0.20, "dii_level": 0.15,
            "promoter_trend": 0.20, "fii_trend": 0.10, "dii_trend": 0.10,
        }
        total_w = sum(weights.get(k, 0.10) for k in parts)
        score = sum(parts[k] * weights.get(k, 0.10) for k in parts) / total_w

        # Pledged promoter shares penalty (#1 Indian small-cap risk factor)
        pledged = row.get("pledged_promoter_pct")
        if pledged is not None:
            if pledged > 40:
                score = max(0, score - 30)
            elif pledged > 20:
                score = max(0, score - 15)

        # Low free-float penalty (manipulation risk)
        public = row.get("public_holding")
        if public is not None and public < 15:
            score = max(0, score - 10)

        return round(score, 2), parts

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
        paper_profits: bool = False,
    ) -> list[str]:
        """Generate descriptive auto-tags (max 5)."""
        candidates: list[tuple[str, int]] = []  # (tag, priority) — lower priority = more important
        market_cap = row.get("market_cap")
        roe = row.get("roe")
        roce = row.get("roce")
        dte = row.get("debt_to_equity")
        eps = row.get("eps")
        pe = row.get("pe_ratio")
        dividend_yield = row.get("dividend_yield")
        pct_3m = row.get("_pct_change_3m")
        revenue_growth = row.get("revenue_growth")
        earnings_growth = row.get("earnings_growth")
        fcf = row.get("free_cash_flow")
        total_cash = row.get("total_cash")
        total_debt = row.get("total_debt")
        analyst_count = row.get("analyst_count")
        rec_mean = row.get("analyst_recommendation_mean")
        analyst_target = row.get("analyst_target_mean")
        last_price = row.get("last_price")
        promoter = row.get("promoter_holding")
        fii = row.get("fii_holding")
        dii = row.get("dii_holding")
        promoter_chg = row.get("promoter_holding_change")
        fii_chg = row.get("fii_holding_change")
        dii_chg = row.get("dii_holding_change")
        fifty_dma = row.get("fifty_day_avg")
        two_hundred_dma = row.get("two_hundred_day_avg")

        # --- Market Cap tags (always first) ---
        if market_cap is not None and market_cap > 0:
            if market_cap >= 20_000:
                candidates.append(("Large Cap", 1))
            elif market_cap >= 5_000:
                candidates.append(("Mid Cap", 1))
            else:
                candidates.append(("Small Cap", 1))

        # --- Quality tags ---
        if (roe is not None and roe >= 20
                and roce is not None and roce >= 20
                and (dte is None or dte <= 0.5)):
            candidates.append(("High Quality", 2))
        elif dte is not None and dte == 0 and eps is not None and eps > 0:
            candidates.append(("Debt Free", 3))

        # --- Paper profits warning (OCF < 0 but EPS > 0) ---
        if paper_profits:
            candidates.append(("Paper Profits", 2))

        # --- Pledged shares risk ---
        pledged = row.get("pledged_promoter_pct")
        if pledged is not None and pledged > 20:
            candidates.append(("High Pledge Risk", 2))

        # --- Financial Health tags ---
        if fcf is not None and market_cap and market_cap > 0:
            fcf_yield = (fcf / (market_cap * 1e7)) * 100
            if fcf_yield > 5:
                candidates.append(("FCF Machine", 3))
        if total_cash is not None and total_debt is not None and market_cap and market_cap > 0:
            net_cash = total_cash - total_debt
            if net_cash > 0 and net_cash > (market_cap * 1e7 * 0.10):
                candidates.append(("Cash Rich", 3))

        # --- Value / Growth ---
        if (pe is not None and pe > 0 and pe < sector_pe_median * 0.7
                and roe is not None and roe >= 12):
            candidates.append(("Value Pick", 3))

        # Growth: fundamental or price-based
        if revenue_growth is not None and revenue_growth >= 0.15:
            candidates.append(("Growth Stock", 3))
        elif earnings_growth is not None and earnings_growth >= 0.20:
            candidates.append(("Growth Stock", 3))
        elif (pct_3m is not None and pct_3m >= 15
              and has_growth and growth_score >= 70):
            candidates.append(("Growth Stock", 4))

        # --- Dividend tag ---
        if dividend_yield is not None and dividend_yield >= 2.0:
            candidates.append(("High Dividend", 4))

        # --- Analyst tags ---
        if rec_mean is not None and rec_mean <= 1.5 and analyst_count is not None and analyst_count >= 10:
            candidates.append(("Analyst Strong Buy", 3))
        elif (analyst_target and last_price and last_price > 0
              and ((analyst_target - last_price) / last_price) > 0.25
              and analyst_count is not None and analyst_count >= 5):
            candidates.append(("Analyst Undervalued", 4))

        # --- Low free-float warning ---
        public = row.get("public_holding")
        if public is not None and public < 25:
            candidates.append(("Low Free Float", 5))

        # --- Ownership tags ---
        if promoter is not None and promoter >= 55:
            candidates.append(("High Promoter", 5))
        if fii is not None and fii >= 20:
            candidates.append(("FII Favorite", 5))
        if dii is not None and dii >= 25:
            candidates.append(("DII Backed", 5))
        if promoter_chg is not None and promoter_chg >= 0.5:
            candidates.append(("Promoter Buying", 4))
        if fii_chg is not None and fii_chg >= 1.0:
            candidates.append(("FII Buying", 4))
        if dii_chg is not None and dii_chg >= 1.0:
            candidates.append(("DII Buying", 4))

        # --- Technical trend tags (50/200 DMA) ---
        if fifty_dma and two_hundred_dma and last_price:
            if fifty_dma > two_hundred_dma and last_price > fifty_dma:
                candidates.append(("Bullish Trend", 5))
            elif fifty_dma < two_hundred_dma and last_price < fifty_dma:
                candidates.append(("Bearish Trend", 5))

        # --- Low Volatility ---
        if has_volatility and volatility_score >= 75:
            candidates.append(("Low Volatility", 6))

        # --- Negative EPS warning ---
        if eps is not None and eps < 0:
            candidates.append(("Negative EPS", 6))

        # Sort by priority, keep all unique tags
        candidates.sort(key=lambda x: x[1])
        tags = []
        seen_tags: set[str] = set()
        for tag, _ in candidates:
            if tag not in seen_tags:
                tags.append(tag)
                seen_tags.add(tag)
        return tags

    @staticmethod
    def _generate_why_narrative(
        score: float,
        row: dict,
        *,
        momentum: float,
        fundamentals: float,
        growth_score: float | None,
        volatility_score: float | None,
        paper_profits: bool = False,
    ) -> str:
        """Generate a 1-2 sentence human-readable score explanation."""
        tier = (
            "Strong" if score >= 75 else
            "Good" if score >= 50 else
            "Average" if score >= 25 else
            "Weak"
        )
        strengths: list[str] = []
        concerns: list[str] = []

        roe = row.get("roe")
        roce = row.get("roce")
        dte = row.get("debt_to_equity")
        pe = row.get("pe_ratio")
        eps = row.get("eps")
        price = row.get("last_price")
        high_52w = row.get("high_52w")
        rg = row.get("revenue_growth")
        promoter = row.get("promoter_holding")

        # Strengths
        if roe is not None and roe >= 15:
            strengths.append(f"strong ROE of {roe:.0f}%")
        if roce is not None and roce >= 18:
            strengths.append(f"high ROCE of {roce:.0f}%")
        if dte is not None and dte <= 0.3:
            strengths.append("low debt" if dte > 0 else "debt-free")
        elif dte is not None and dte == 0:
            strengths.append("debt-free")
        if rg is not None and rg >= 0.15:
            strengths.append(f"revenue growing {rg*100:.0f}%")
        if momentum >= 70:
            strengths.append("strong price momentum")
        if promoter is not None and promoter >= 55:
            strengths.append("high promoter stake")
        if volatility_score is not None and volatility_score >= 75:
            strengths.append("low volatility")

        # Concerns
        if paper_profits:
            concerns.append("negative cash flow despite positive earnings")
        if pe is not None and pe > 50:
            concerns.append(f"expensive at PE {pe:.0f}x")
        if price and high_52w and high_52w > 0 and price > high_52w * 0.95:
            concerns.append("trading near 52-week high")
        if dte is not None and dte > 2.0:
            concerns.append(f"high debt (D/E {dte:.1f})")
        if rg is not None and rg < -0.05:
            concerns.append("declining revenue")
        if eps is not None and eps < 0:
            concerns.append("loss-making")

        pledged = row.get("pledged_promoter_pct")
        if pledged is not None and pledged > 20:
            concerns.append(f"{pledged:.0f}% shares pledged")

        # Build narrative
        strength_text = ", ".join(strengths[:2]) if strengths else "balanced metrics"
        parts = [f"Scored {score:.0f} ({tier}): {strength_text.capitalize()}"]
        if concerns:
            parts.append(f"but {concerns[0]}")
        return ", ".join(parts) + "."

    def _compute_scores(
        self,
        rows: list[dict],
        *,
        volatility_data: dict[str, dict] | None = None,
    ) -> list[dict]:
        if not rows:
            return []

        vol_data = volatility_data or {}

        # ── Sanitize numeric fields (Screener.in can return strings) ──
        _NUMERIC_FIELDS = {
            "pe_ratio", "price_to_book", "debt_to_equity", "roe", "roce",
            "eps", "dividend_yield", "last_price", "percent_change",
            "volume", "traded_value", "high_52w", "low_52w", "market_cap",
            "beta", "forward_pe", "gross_margins", "operating_margins",
            "profit_margins", "revenue_growth", "earnings_growth",
            "promoter_holding", "fii_holding", "dii_holding",
            "total_cash", "total_debt", "total_revenue",
            "free_cash_flow", "operating_cash_flow", "payout_ratio",
        }
        for r in rows:
            for field in _NUMERIC_FIELDS:
                v = r.get(field)
                if v is not None and not isinstance(v, (int, float)):
                    try:
                        r[field] = float(v)
                    except (ValueError, TypeError):
                        r[field] = None

        # ── Derive missing fields from available data ──
        _derived_pe = 0
        _derived_pb = 0
        _derived_roce = 0
        _defaulted_fii = 0
        _defaulted_dii = 0
        for r in rows:
            price = r.get("last_price")
            # PE = price / EPS when PE is missing but both inputs exist
            if r.get("pe_ratio") is None and price and price > 0:
                eps = r.get("eps")
                if eps is not None and eps > 0:
                    r["pe_ratio"] = round(price / eps, 2)
                    _derived_pe += 1
            # P/B = price / (EPS / ROE) when P/B is missing
            if r.get("price_to_book") is None and price and price > 0:
                eps = r.get("eps")
                roe = r.get("roe")
                if eps is not None and eps > 0 and roe is not None and roe > 0:
                    book_value = eps / (roe / 100.0)
                    if book_value > 0:
                        r["price_to_book"] = round(price / book_value, 2)
                        _derived_pb += 1
            # ROCE ≈ ROE × (1 + D/E) — standard approximation
            # When D/E is unavailable, ROCE ≈ ROE (assumes no debt)
            if r.get("roce") is None:
                roe = r.get("roe")
                if roe is not None:
                    dte = r.get("debt_to_equity")
                    if dte is not None and dte >= 0:
                        r["roce"] = round(roe * (1 + dte), 2)
                    else:
                        r["roce"] = round(roe, 2)  # no-debt fallback
                    _derived_roce += 1
            # Default FII/DII to 0 when promoter data exists (no FII ≠ unknown)
            if r.get("promoter_holding") is not None:
                if r.get("fii_holding") is None:
                    r["fii_holding"] = 0.0
                    _defaulted_fii += 1
                if r.get("dii_holding") is None:
                    r["dii_holding"] = 0.0
                    _defaulted_dii += 1

        if _derived_pe or _derived_pb or _derived_roce or _defaulted_fii or _defaulted_dii:
            logger.info(
                "Derived fields: PE=%d, P/B=%d, ROCE=%d, FII→0=%d, DII→0=%d",
                _derived_pe, _derived_pb, _derived_roce, _defaulted_fii, _defaulted_dii,
            )

        # ── Pre-compute sector medians for PE, PB, and D/E ──
        sector_pe: dict[str, list[float]] = {}
        sector_pb: dict[str, list[float]] = {}
        sector_de: dict[str, list[float]] = {}
        def _safe_float(v: object) -> float | None:
            if v is None:
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None

        for r in rows:
            sector = str(r.get("sector") or "Other")
            pe = _safe_float(r.get("pe_ratio"))
            if pe is not None and pe > 0:
                sector_pe.setdefault(sector, []).append(pe)
            pb = _safe_float(r.get("price_to_book"))
            if pb is not None and pb > 0:
                sector_pb.setdefault(sector, []).append(pb)
            de = _safe_float(r.get("debt_to_equity"))
            if de is not None and de >= 0:
                sector_de.setdefault(sector, []).append(de)

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

        # ── Pre-compute multi-day momentum percentile data ──
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

        # ── Pre-compute 52W position scores ──
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

        # ── Pre-compute multi-day liquidity data ──
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

        all_tv_logs = [
            math.log1p(max(0.0, float(r.get("traded_value") or 0.0)))
            for r in rows
        ]
        all_vol_logs = [
            math.log1p(max(0.0, float(r.get("volume") or 0.0)))
            for r in rows
        ]

        # ── Pre-compute volatility ──
        all_std_devs: list[float] = []
        for r in rows:
            sym = str(r.get("symbol") or "")
            vd = vol_data.get(sym)
            if vd and vd.get("std_dev") is not None:
                all_std_devs.append(vd["std_dev"])

        # ── Pre-compute multi-period growth data ──
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

        # ── Pre-compute revenue/earnings growth percentile data ──
        all_rev_growth: list[float] = []
        all_earn_growth: list[float] = []
        for r in rows:
            rg = r.get("revenue_growth")
            if rg is not None:
                all_rev_growth.append(rg)
            eg = r.get("earnings_growth")
            if eg is not None:
                all_earn_growth.append(eg)

        # Track sector scores for sector_leader tag
        sector_best: dict[str, list[tuple[float, str]]] = {}

        out: list[dict] = []
        for row in rows:
            symbol = str(row.get("symbol") or "")
            sector = str(row.get("sector") or "Other")
            pct_raw = float(row.get("percent_change") or 0.0)
            pct = max(pct_lo, min(pct_hi, pct_raw))
            tv_log = math.log1p(max(0.0, float(row.get("traded_value") or 0.0)))
            vol_log = math.log1p(max(0.0, float(row.get("volume") or 0.0)))

            # ── Momentum: blend short-term price + 52W + multi-period returns ──
            daily_momentum = self._clamp(self._percentile_rank(all_pcts, pct))
            has_m5 = symbol in momentum_5d_by_sym and all_momentum_5d
            has_m20 = symbol in momentum_20d_by_sym and all_momentum_20d
            if has_m5 and has_m20:
                m5_pctile = self._percentile_rank(all_momentum_5d, momentum_5d_by_sym[symbol])
                m20_pctile = self._percentile_rank(all_momentum_20d, momentum_20d_by_sym[symbol])
                short_term_momentum = self._clamp(m5_pctile * 0.50 + m20_pctile * 0.30 + daily_momentum * 0.20)
            elif has_m5:
                m5_pctile = self._percentile_rank(all_momentum_5d, momentum_5d_by_sym[symbol])
                short_term_momentum = self._clamp(m5_pctile * 0.65 + daily_momentum * 0.35)
            else:
                short_term_momentum = daily_momentum

            # Multi-period return percentiles (3M/1Y/3Y) — moved here from growth
            has_3m = symbol in pct_3m_by_sym and all_pct_3m
            has_1y = symbol in pct_1y_by_sym and all_pct_1y
            has_3y = symbol in pct_3y_by_sym and all_pct_3y
            multi_period_parts: list[tuple[float, float]] = []
            if has_3m:
                multi_period_parts.append((self._percentile_rank(all_pct_3m, pct_3m_by_sym[symbol]), 0.30))
            if has_1y:
                multi_period_parts.append((self._percentile_rank(all_pct_1y, pct_1y_by_sym[symbol]), 0.35))
            if has_3y:
                multi_period_parts.append((self._percentile_rank(all_pct_3y, pct_3y_by_sym[symbol]), 0.35))

            # Blend: short-term 0.30 + 52w 0.20 + multi-period 0.50
            if multi_period_parts:
                mp_w = sum(w for _, w in multi_period_parts)
                multi_period_score = self._clamp(sum(s * (w / mp_w) for s, w in multi_period_parts))
                if symbol in pos_52w_by_sym:
                    momentum = self._clamp(
                        short_term_momentum * 0.30
                        + pos_52w_by_sym[symbol] * 0.20
                        + multi_period_score * 0.50
                    )
                else:
                    momentum = self._clamp(short_term_momentum * 0.40 + multi_period_score * 0.60)
            elif symbol in pos_52w_by_sym:
                momentum = self._clamp(short_term_momentum * 0.60 + pos_52w_by_sym[symbol] * 0.40)
            else:
                momentum = short_term_momentum

            # ── Liquidity ──
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

            # ── OCF earnings quality gate ──
            # If operating cash flow is negative but EPS is positive,
            # the company generates "paper profits" — cap fundamentals.
            paper_profits = False
            _ocf = row.get("operating_cash_flow")
            _eps_val = row.get("eps")
            if _ocf is not None and _eps_val is not None and _eps_val > 0 and _ocf < 0:
                fundamentals = min(fundamentals, 45.0)
                paper_profits = True

            # ── Volatility (lower std_dev → higher score = stability premium) ──
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
                    raw_pctile = self._percentile_rank(all_std_devs, sd)
                    vol_raw = self._clamp(100.0 - raw_pctile)
                    vol_raw = self._adjust_volatility_for_cap(vol_raw, row.get("market_cap"))
                    # Incorporate Yahoo beta if available
                    # Lower beta = higher score (defensive premium)
                    beta = row.get("beta")
                    if beta is not None:
                        beta_score = self._clamp(100 - beta * 35)
                        volatility_score = self._clamp(vol_raw * 0.70 + beta_score * 0.30)
                    else:
                        volatility_score = vol_raw
                    has_volatility = True

                    # Liquidity gate: illiquid stocks shouldn't get high
                    # volatility scores just because they don't trade often
                    traded_val = row.get("traded_value")
                    if traded_val is not None and traded_val < 10_00_000:
                        volatility_score = min(volatility_score, 50.0)

            # ── Growth (fundamental-only; price returns live in momentum) ──
            # Revenue growth is the primary signal (97% coverage).
            # Earnings growth and forward PE dropped (poor coverage: 25%/47%).
            # If no fundamental growth data, fall back to price-based growth
            # at half weight to avoid zeroing out data-poor stocks.
            has_growth = False
            growth_score: float | None = None

            rg = row.get("revenue_growth")
            if rg is not None:
                growth_score = self._clamp(50 + rg * 200)  # 10% → 70, 25% → 100
                has_growth = True
            else:
                # Fallback: use price-based growth at 0.5× weight
                price_growth_parts: list[tuple[float, float]] = []
                if has_3m:
                    price_growth_parts.append((self._percentile_rank(all_pct_3m, pct_3m_by_sym[symbol]), 0.30))
                if has_1y:
                    price_growth_parts.append((self._percentile_rank(all_pct_1y, pct_1y_by_sym[symbol]), 0.35))
                if has_3y:
                    price_growth_parts.append((self._percentile_rank(all_pct_3y, pct_3y_by_sym[symbol]), 0.35))
                if price_growth_parts:
                    pgw = sum(w for _, w in price_growth_parts)
                    raw_price_growth = self._clamp(sum(s * (w / pgw) for s, w in price_growth_parts))
                    # Blend toward neutral (50) since this is a weak proxy
                    growth_score = self._clamp(50 + (raw_price_growth - 50) * 0.50)
                    has_growth = True

            # ── Financial Health (NEW) ──
            has_financial_health = False
            financial_health_score: float | None = None
            fh_score, _fh_parts = self._score_financial_health(row)
            if fh_score is not None:
                financial_health_score = fh_score
                has_financial_health = True

            # ── Ownership (NEW) ──
            has_ownership = False
            ownership_score: float | None = None
            own_score, _own_parts = self._score_ownership(row)
            if own_score is not None:
                ownership_score = own_score
                has_ownership = True

            # ── 7-component weighted total ──
            # Momentum 13%, Liquidity 5%, Fundamentals 25%, Growth 18%,
            # Financial Health 17%, Volatility 9%, Ownership 13%
            target_weights = {
                "momentum": 0.13,
                "liquidity": 0.05,
                "fundamentals": 0.25,
                "growth": 0.18,
                "financial_health": 0.17,
                "volatility": 0.09,
                "ownership": 0.13,
            }
            scores_map = {
                "momentum": momentum,
                "liquidity": liquidity,
                "fundamentals": fundamentals,
                "growth": growth_score if has_growth else 0.0,
                "financial_health": financial_health_score if has_financial_health else 0.0,
                "volatility": volatility_score if has_volatility else 0.0,
                "ownership": ownership_score if has_ownership else 0.0,
            }

            # Dynamic reweighting: exclude unavailable components
            available_weights: dict[str, float] = {}
            for k, w in target_weights.items():
                if k == "volatility" and not has_volatility:
                    continue
                if k == "growth" and not has_growth:
                    continue
                if k == "fundamentals" and metrics_used == 0:
                    continue
                if k == "financial_health" and not has_financial_health:
                    continue
                if k == "ownership" and not has_ownership:
                    continue
                available_weights[k] = w

            if not available_weights:
                combined_signal = (momentum + liquidity) / 2.0
                total = self._clamp(combined_signal, lo=20.0, hi=80.0)
            else:
                total_w = sum(available_weights.values())
                total = sum(
                    scores_map[k] * (w / total_w) for k, w in available_weights.items()
                )

            # Shrink fundamentals influence when coverage is low
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

            # ── Score range expansion ──
            # Stretch the compressed middle band (was ~15 pts) to ~21 pts
            score = round(self._clamp(50 + (score - 50) * 1.4), 2)

            # ── Confidence caps ──
            data_quality = "full"
            total_data_metrics = metrics_used
            if has_financial_health:
                total_data_metrics += 1
            if has_ownership:
                total_data_metrics += 1
            # analyst data not used in scoring but kept for display

            if total_data_metrics == 0:
                score = min(score, 65.0)
                data_quality = "limited"
            elif total_data_metrics <= 2:
                score = min(score, 75.0)
                data_quality = "partial"
            elif total_data_metrics <= 4:
                data_quality = "partial"

            # ── Auto-tags ──
            med_pe = sector_medians.get(sector, {}).get("pe", 25.0)
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
                paper_profits=paper_profits,
            )

            # Track sector leaders (top 3)
            sector_best.setdefault(sector, []).append((score, symbol))

            pct_change_1w = vd.get("pct_change_1w") if vd else None

            # Generate human-readable narrative
            why_narrative = self._generate_why_narrative(
                score, row,
                momentum=momentum,
                fundamentals=fundamentals,
                growth_score=growth_score,
                volatility_score=volatility_score,
                paper_profits=paper_profits,
            )

            enriched = {
                **row,
                "score": score,
                "score_momentum": round(momentum, 2),
                "score_liquidity": round(liquidity, 2),
                "score_fundamentals": round(fundamentals, 2),
                "score_volatility": round(volatility_score, 2) if volatility_score is not None else None,
                "score_growth": round(growth_score, 2) if growth_score is not None else None,
                "score_financial_health": round(financial_health_score, 2) if financial_health_score is not None else None,
                "score_ownership": round(ownership_score, 2) if ownership_score is not None else None,
                "score_analyst": None,
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
                    "financial_health": round(financial_health_score, 2) if financial_health_score is not None else None,
                    "ownership": round(ownership_score, 2) if ownership_score is not None else None,
                    "52w_position": pos_52w_by_sym.get(symbol),
                    "combined_signal": round((momentum + liquidity) / 2.0, 2),
                    "fundamentals_coverage": f"{metrics_used}/5",
                    "data_quality": data_quality,
                    "why_narrative": why_narrative,
                },
                "tags": tags,
                "source_status": source_status,
            }
            out.append(enriched)

        # Add sector_leader tag to top 3 scorers in each sector
        sector_leaders: set[str] = set()
        for sector, scores_list in sector_best.items():
            scores_list.sort(key=lambda x: -x[0])
            for rank_score, sym in scores_list[:3]:
                sector_leaders.add(sym)

        for enriched in out:
            symbol = str(enriched.get("symbol") or "")
            if symbol in sector_leaders:
                tags = enriched["tags"]
                if "Sector Leader" not in tags:
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
            time_mod.sleep(self._screener_batch_delay)

            # Yahoo v10 for EVERY stock: fills gaps + adds exclusive data
            try:
                yahoo_session = self._get_yahoo_session()
                yahoo = yahoo_session.get_stock_data(stock.nse_symbol)
                time_mod.sleep(self._yahoo_batch_delay)

                yahoo_fields_filled = 0

                # Fill missing Screener fields from Yahoo
                for field in ("pe_ratio", "price_to_book", "eps", "debt_to_equity",
                              "market_cap", "high_52w", "low_52w", "dividend_yield"):
                    if fundamentals.get(field) is None and yahoo.get(field) is not None:
                        fundamentals[field] = yahoo[field]
                        yahoo_fields_filled += 1

                # Add Yahoo-exclusive fields (always overwrite with Yahoo data)
                for field in ("beta", "free_cash_flow", "operating_cash_flow", "total_cash",
                              "total_debt", "total_revenue", "gross_margins", "operating_margins",
                              "profit_margins", "revenue_growth", "earnings_growth",
                              "forward_pe",
                              "analyst_target_mean", "analyst_count", "analyst_recommendation",
                              "analyst_recommendation_mean", "analyst_strong_buy", "analyst_buy",
                              "analyst_hold", "analyst_sell",
                              "payout_ratio", "fifty_day_avg", "two_hundred_day_avg"):
                    if yahoo.get(field) is not None:
                        fundamentals[field] = yahoo[field]
                        yahoo_fields_filled += 1

                if fundamentals_source == "unavailable":
                    fundamentals_source = "yahoo_fundamentals"
                elif fundamentals_source == "screener_in":
                    fundamentals_source = "screener_in+yahoo"

                logger.debug(
                    "Yahoo v10 OK for %s: %d fields enriched → source=%s",
                    stock.nse_symbol, yahoo_fields_filled, fundamentals_source,
                )
            except Exception as exc:
                logger.warning("Yahoo v10 failed for %s: %s", stock.nse_symbol, exc)
        else:
            fundamentals = {
                "pe_ratio": None, "roe": None, "roce": None,
                "debt_to_equity": None, "price_to_book": None,
                "eps": None, "market_cap": None, "high_52w": None,
                "low_52w": None, "dividend_yield": None,
            }
            fundamentals_source = "unavailable"

        fundamentals_count = sum(1 for k, v in fundamentals.items() if v is not None and not k.startswith("_"))

        source_status = "primary" if (fundamentals_source in ("screener_in", "screener_in+yahoo") and fundamentals_count >= 2) else "fallback"
        if fundamentals_count == 0 and quote_source not in {"nse_quote_api", "nse_bhavcopy"}:
            source_status = "limited"

        # Sector resolution: curated > Screener Broad Sector > existing > "Other"
        sector = stock.sector
        broad_sector_raw = fundamentals.pop("_screener_broad_sector", None)
        industry_raw = fundamentals.pop("_screener_industry", None)
        # Remove legacy key if present
        fundamentals.pop("_screener_sector", None)

        if stock.nse_symbol in _EXTRA_SECTOR_MAP:
            sector = _EXTRA_SECTOR_MAP[stock.nse_symbol]
        elif broad_sector_raw:
            mapped = _SCREENER_BROAD_SECTOR_MAP.get(broad_sector_raw.lower())
            if mapped:
                sector = mapped
            else:
                sector = _map_screener_sector(broad_sector_raw)
        elif sector in ("Diversified", "Other") and industry_raw:
            sector = _map_screener_sector(industry_raw)

        return {
            "market": "IN",
            "symbol": stock.nse_symbol,
            "display_name": stock.display_name,
            "sector": sector,
            "industry": industry_raw,
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
            # Shareholding
            "promoter_holding": fundamentals.get("promoter_holding"),
            "fii_holding": fundamentals.get("fii_holding"),
            "dii_holding": fundamentals.get("dii_holding"),
            "government_holding": fundamentals.get("government_holding"),
            "public_holding": fundamentals.get("public_holding"),
            "num_shareholders": fundamentals.get("num_shareholders"),
            "promoter_holding_change": fundamentals.get("promoter_holding_change"),
            "fii_holding_change": fundamentals.get("fii_holding_change"),
            "dii_holding_change": fundamentals.get("dii_holding_change"),
            # Yahoo-exclusive fundamentals
            "beta": fundamentals.get("beta"),
            "free_cash_flow": fundamentals.get("free_cash_flow"),
            "operating_cash_flow": fundamentals.get("operating_cash_flow"),
            "total_cash": fundamentals.get("total_cash"),
            "total_debt": fundamentals.get("total_debt"),
            "total_revenue": fundamentals.get("total_revenue"),
            "gross_margins": fundamentals.get("gross_margins"),
            "operating_margins": fundamentals.get("operating_margins"),
            "profit_margins": fundamentals.get("profit_margins"),
            "revenue_growth": fundamentals.get("revenue_growth"),
            "earnings_growth": fundamentals.get("earnings_growth"),
            "forward_pe": fundamentals.get("forward_pe"),
            # Analyst data
            "analyst_target_mean": fundamentals.get("analyst_target_mean"),
            "analyst_count": fundamentals.get("analyst_count"),
            "analyst_recommendation": fundamentals.get("analyst_recommendation"),
            "analyst_recommendation_mean": fundamentals.get("analyst_recommendation_mean"),
            "analyst_strong_buy": fundamentals.get("analyst_strong_buy"),
            "analyst_buy": fundamentals.get("analyst_buy"),
            "analyst_hold": fundamentals.get("analyst_hold"),
            "analyst_sell": fundamentals.get("analyst_sell"),
            # Technical
            "fifty_day_avg": fundamentals.get("fifty_day_avg"),
            "two_hundred_day_avg": fundamentals.get("two_hundred_day_avg"),
            "payout_ratio": fundamentals.get("payout_ratio"),
            "pledged_promoter_pct": fundamentals.get("pledged_promoter_pct"),
            # Metadata
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

    def fetch_raw_rows(self, on_batch: "Callable[[list[dict]], None] | None" = None, batch_size: int = 50) -> list[dict]:
        """Fetch quotes + fundamentals (sync I/O). Does NOT score.

        If *on_batch* is provided, it is called every *batch_size* rows with
        the latest batch for incremental DB visibility.
        """
        universe = self._build_effective_universe()
        bulk_quotes, _ = self._fetch_latest_bhavcopy_quotes()
        raw_rows: list[dict] = []
        # Counters for progress / diagnostic logging
        _total = len(universe)
        _processed = 0
        _yahoo_ok = 0
        _yahoo_fail = 0
        _yahoo_skip = 0
        _screener_ok = 0
        _screener_fail = 0
        _t_start = time_mod.time()

        _aborted = False
        _pending_batch: list[dict] = []
        _batch_count = 0

        def _flush_batch(force: bool = False) -> None:
            nonlocal _batch_count
            if on_batch is None or (not force and len(_pending_batch) < batch_size):
                return
            if _pending_batch:
                batch = list(_pending_batch)
                _pending_batch.clear()
                _batch_count += 1
                try:
                    on_batch(batch)
                except Exception as exc:
                    logger.warning("Incremental upsert batch %d failed: %s", _batch_count, exc)

        def _log_progress(force: bool = False) -> None:
            nonlocal _processed, _aborted
            if not force and _processed % 100 != 0:
                return
            # Check for abort every 100 stocks
            if not _aborted and _processed % 100 == 0 and _check_abort():
                logger.warning("ABORT requested — stopping stock fetch at %d/%d", _processed, _total)
                _aborted = True
            elapsed = time_mod.time() - _t_start
            rate = _processed / elapsed if elapsed > 0 else 0
            eta = (_total - _processed) / rate if rate > 0 else 0
            logger.info(
                "Stock progress: %d/%d (%.0f%%) | yahoo ok=%d fail=%d skip=%d | "
                "screener ok=%d fail=%d | %.1f stocks/min | ETA %.0fm",
                _processed, _total, (_processed / max(_total, 1)) * 100,
                _yahoo_ok, _yahoo_fail, _yahoo_skip,
                _screener_ok, _screener_fail,
                rate * 60, eta / 60,
            )

        if bulk_quotes:
            fundamentals_symbols = self._select_fundamentals_symbols(universe, bulk_quotes)
            logger.info(
                "Bhavcopy loaded: %d quotes for %d universe stocks, "
                "fundamentals enabled for %d symbols",
                len(bulk_quotes), _total, len(fundamentals_symbols),
            )
            missing: list[DiscoverStockDef] = []
            for stock in universe:
                if _aborted:
                    break
                quote = bulk_quotes.get(stock.nse_symbol)
                if quote is None:
                    missing.append(stock)
                    continue
                row = self._build_snapshot_row(
                    stock,
                    quote,
                    "nse_bhavcopy",
                    fundamentals_enabled=stock.nse_symbol in fundamentals_symbols,
                )
                raw_rows.append(row)
                _pending_batch.append(row)
                _processed += 1
                # Track Yahoo / Screener stats from primary_source
                src = row.get("primary_source", "")
                if "yahoo" in src:
                    _yahoo_ok += 1
                if "screener" in src:
                    _screener_ok += 1
                if src == "screener_in" and "yahoo" not in src:
                    _yahoo_fail += 1
                if src == "unavailable":
                    _yahoo_skip += 1
                _flush_batch()
                _log_progress()
            if not _aborted and missing and self._missing_quote_retry_limit > 0:
                retry_batch = missing[: self._missing_quote_retry_limit]
                logger.warning(
                    "Bhavcopy missing %d/%d symbols; retrying fallback quotes for %d symbols",
                    len(missing),
                    len(universe),
                    len(retry_batch),
                )
                for stock in retry_batch:
                    if _aborted:
                        break
                    item = self._fetch_one(stock)
                    if item is not None:
                        raw_rows.append(item)
                        _pending_batch.append(item)
                    _processed += 1
                    _flush_batch()
                    _log_progress()
        else:
            logger.warning("Bhavcopy unavailable — using fallback quote path for all symbols")
            fallback_universe = CORE_UNIVERSE if len(universe) > len(CORE_UNIVERSE) else universe
            _total = len(fallback_universe)
            for stock in fallback_universe:
                if _aborted:
                    break
                item = self._fetch_one(stock)
                if item is not None:
                    raw_rows.append(item)
                    _pending_batch.append(item)
                _processed += 1
                _flush_batch()
                _log_progress()
            if len(raw_rows) < len(fallback_universe):
                logger.warning(
                    "Fallback quote path updated %d/%d symbols",
                    len(raw_rows),
                    len(fallback_universe),
                )
        _flush_batch(force=True)  # flush remaining rows
        _log_progress(force=True)
        elapsed_total = time_mod.time() - _t_start
        status_word = "ABORTED" if _aborted else "complete"
        logger.info(
            "Stock fetch %s: %d rows in %.1fm | yahoo ok=%d fail=%d skip=%d | "
            "screener ok=%d fail=%d | incremental batches=%d",
            status_word, len(raw_rows), elapsed_total / 60,
            _yahoo_ok, _yahoo_fail, _yahoo_skip,
            _screener_ok, _screener_fail, _batch_count,
        )
        return raw_rows

    def fetch_all(self, *, volatility_data: dict[str, dict] | None = None) -> list[dict]:
        raw_rows = self.fetch_raw_rows()
        return self._compute_scores(raw_rows, volatility_data=volatility_data)


_scraper = DiscoverStockScraper()


def _check_abort(job_name: str = "discover_stock") -> bool:
    """Check Redis for an abort flag. Returns True if abort requested."""
    try:
        import redis as _redis

        settings = get_settings()
        r = _redis.from_url(settings.redis_url, decode_responses=True)
        val = r.get(f"job:abort:{job_name}")
        if val:
            r.delete(f"job:abort:{job_name}")
            return True
    except Exception:
        pass
    return False


def _fetch_discover_stock_raw_sync() -> list[dict]:
    return _scraper.fetch_raw_rows()


_UPSERT_BATCH_SIZE = 200
_INCREMENTAL_BATCH_SIZE = 50


async def run_discover_stock_job() -> None:
    try:
        job_t0 = time_mod.time()

        # 1. Pre-fetch volatility data from PostgreSQL (async).
        volatility_data = await discover_service.get_bulk_stock_volatility_data()
        logger.info(
            "Discover stock: volatility_data has %d symbols (sample keys: %s)",
            len(volatility_data),
            list(volatility_data.keys())[:5] if volatility_data else "EMPTY",
        )

        # 2. Fetch quotes + fundamentals (sync network I/O in executor).
        #    Incremental upsert every 50 rows for early DB visibility (unscored).
        loop = asyncio.get_event_loop()

        # Keys that only exist after scoring — strip from incremental upserts
        # so we don't overwrite existing scores with 0/None.
        _SCORE_KEYS = {
            "score", "score_momentum", "score_liquidity", "score_fundamentals",
            "score_volatility", "score_growth", "score_financial_health",
            "score_ownership", "score_analyst", "score_breakdown",
            "quality_tier", "tags", "why_ranked",
        }

        def _incremental_upsert(batch: list[dict]) -> None:
            """Called from sync thread every 50 rows — upserts raw (unscored) data."""
            # Strip score fields so we don't overwrite existing scores with 0/None
            clean_batch = [{k: v for k, v in row.items() if k not in _SCORE_KEYS} for row in batch]
            future = asyncio.run_coroutine_threadsafe(
                discover_service.upsert_discover_stock_snapshots(clean_batch),
                loop,
            )
            try:
                count = future.result(timeout=30)
                logger.info("Incremental upsert: %d rows written to DB", count)
            except Exception as exc:
                logger.warning("Incremental upsert failed: %s", exc)

        raw_rows = await loop.run_in_executor(
            get_job_executor("discover-stock"),
            lambda: _scraper.fetch_raw_rows(
                on_batch=_incremental_upsert,
                batch_size=_INCREMENTAL_BATCH_SIZE,
            ),
        )
        fetch_elapsed = time_mod.time() - job_t0
        logger.info(
            "Discover stock: fetched %d raw rows in %.1fm",
            len(raw_rows), fetch_elapsed / 60,
        )

        # Log data source distribution
        source_counts: dict[str, int] = {}
        for r in raw_rows:
            src = r.get("primary_source", "unknown")
            source_counts[src] = source_counts.get(src, 0) + 1
        logger.info("Discover stock: source distribution: %s", source_counts)

        # Log sector distribution
        sector_counts: dict[str, int] = {}
        for r in raw_rows:
            sec = r.get("sector", "Other")
            sector_counts[sec] = sector_counts.get(sec, 0) + 1
        other_count = sector_counts.get("Other", 0)
        logger.info(
            "Discover stock: %d sectors, 'Other'=%d/%d (%.1f%%)",
            len(sector_counts), other_count, len(raw_rows),
            (other_count / max(len(raw_rows), 1)) * 100,
        )

        # 3. Score with all 8 components (CPU-bound, fast).
        score_t0 = time_mod.time()
        rows = _scraper._compute_scores(raw_rows, volatility_data=volatility_data)
        logger.info(
            "Discover stock: scored %d rows in %.1fs",
            len(rows), time_mod.time() - score_t0,
        )

        # Log sample scores
        if rows:
            sample = rows[0]
            logger.info(
                "Discover stock: sample scored row %s → score=%.2f "
                "mom=%.1f liq=%.1f fun=%.1f vol=%s gro=%s fh=%s own=%s tags=%s",
                sample.get("symbol"),
                sample.get("score", 0),
                sample.get("score_momentum", 0),
                sample.get("score_liquidity", 0),
                sample.get("score_fundamentals", 0),
                sample.get("score_volatility"),
                sample.get("score_growth"),
                sample.get("score_financial_health"),
                sample.get("score_ownership"),
                sample.get("tags", [])[:5],
            )

        # 4. Upsert in batches for incremental visibility + fault tolerance.
        upsert_t0 = time_mod.time()
        total_upserted = 0
        for batch_start in range(0, len(rows), _UPSERT_BATCH_SIZE):
            batch = rows[batch_start : batch_start + _UPSERT_BATCH_SIZE]
            count = await discover_service.upsert_discover_stock_snapshots(batch)
            total_upserted += count
            logger.info(
                "Discover stock: upserted batch %d–%d (%d rows, %d total so far)",
                batch_start, batch_start + len(batch) - 1, count, total_upserted,
            )

        total_elapsed = time_mod.time() - job_t0
        logger.info(
            "Discover stock job complete: %d snapshots upserted in %.1fm "
            "(fetch=%.1fm, score=%.1fs, upsert=%.1fs)",
            total_upserted, total_elapsed / 60,
            fetch_elapsed / 60,
            time_mod.time() - score_t0,  # includes upsert time too but close enough
            time_mod.time() - upsert_t0,
        )
    except requests.RequestException:
        logger.exception("Discover stock job failed due to network exception")
    except Exception:
        logger.exception("Discover stock job failed")


async def rescore_discover_stocks() -> dict:
    """Read all stock rows from DB, re-compute scores, and write back.

    No network fetching — purely DB read → score → DB write.
    """
    t0 = time_mod.time()

    # 1. Read all raw rows from the DB
    from app.core.database import get_pool

    pool = await get_pool()
    async with pool.acquire() as conn:
        db_rows = await conn.fetch(
            f"SELECT * FROM {discover_service.STOCK_TABLE} WHERE market = 'IN'"
        )
    raw_rows = [dict(r) for r in db_rows]
    read_elapsed = time_mod.time() - t0
    logger.info("Rescore: read %d rows from DB in %.1fs", len(raw_rows), read_elapsed)

    if not raw_rows:
        return {"status": "empty", "rows": 0}

    # 2. Data quality audit — track missing fields
    _KEY_FIELDS = {
        "price": ["last_price", "percent_change", "volume"],
        "fundamentals": ["pe_ratio", "roe", "roce", "debt_to_equity", "eps", "price_to_book"],
        "yahoo": ["beta", "gross_margins", "operating_margins", "profit_margins",
                   "forward_pe", "revenue_growth", "earnings_growth",
                   "total_debt", "total_revenue", "total_cash"],
        "shareholding": ["promoter_holding", "fii_holding", "dii_holding", "public_holding"],
        "analyst": ["analyst_count", "analyst_target_mean", "analyst_recommendation"],
        "meta": ["sector", "industry", "market_cap", "high_52w", "low_52w"],
    }
    missing_stats: dict[str, dict[str, int]] = {}
    stocks_missing_all: dict[str, list[str]] = {}  # group → symbols with ALL fields missing

    for group, fields in _KEY_FIELDS.items():
        field_counts: dict[str, int] = {}
        all_missing_syms: list[str] = []
        for row in raw_rows:
            missing_in_group = 0
            for f in fields:
                if row.get(f) is None:
                    field_counts[f] = field_counts.get(f, 0) + 1
                    missing_in_group += 1
            if missing_in_group == len(fields):
                all_missing_syms.append(str(row.get("symbol", "?")))
        if field_counts:
            missing_stats[group] = field_counts
        if all_missing_syms:
            stocks_missing_all[group] = all_missing_syms

    total = len(raw_rows)
    for group, counts in missing_stats.items():
        parts = ", ".join(f"{f}={c}/{total}" for f, c in sorted(counts.items(), key=lambda x: -x[1]))
        logger.info("Rescore data gaps [%s]: %s", group, parts)

    for group, syms in stocks_missing_all.items():
        logger.warning(
            "Rescore: %d stocks missing ALL %s fields (first 10): %s",
            len(syms), group, syms[:10],
        )

    # Coverage summary
    has_yahoo = sum(1 for r in raw_rows if r.get("beta") is not None)
    has_shareholding = sum(1 for r in raw_rows if r.get("promoter_holding") is not None)
    has_analyst = sum(1 for r in raw_rows if r.get("analyst_count") is not None)
    has_fundamentals = sum(1 for r in raw_rows if r.get("pe_ratio") is not None)
    has_industry = sum(1 for r in raw_rows if r.get("industry") is not None)
    logger.info(
        "Rescore coverage: %d total | fundamentals=%d (%.0f%%) | yahoo=%d (%.0f%%) | "
        "shareholding=%d (%.0f%%) | analyst=%d (%.0f%%) | industry=%d (%.0f%%)",
        total,
        has_fundamentals, has_fundamentals / total * 100,
        has_yahoo, has_yahoo / total * 100,
        has_shareholding, has_shareholding / total * 100,
        has_analyst, has_analyst / total * 100,
        has_industry, has_industry / total * 100,
    )

    # 3. Pre-fetch volatility data
    volatility_data = await discover_service.get_bulk_stock_volatility_data()
    logger.info("Rescore: volatility data for %d symbols", len(volatility_data))

    # 4. Score
    score_t0 = time_mod.time()
    scored_rows = _scraper._compute_scores(raw_rows, volatility_data=volatility_data)
    score_elapsed = time_mod.time() - score_t0
    logger.info("Rescore: scored %d rows in %.1fs", len(scored_rows), score_elapsed)

    # Score distribution
    scores = [r.get("score", 0) for r in scored_rows if r.get("score") is not None]
    if scores:
        scores.sort()
        p25 = scores[len(scores) // 4]
        p50 = scores[len(scores) // 2]
        p75 = scores[3 * len(scores) // 4]
        tiers = {"Strong": 0, "Good": 0, "Average": 0, "Weak": 0}
        for s in scores:
            if s >= 75:
                tiers["Strong"] += 1
            elif s >= 50:
                tiers["Good"] += 1
            elif s >= 25:
                tiers["Average"] += 1
            else:
                tiers["Weak"] += 1
        logger.info(
            "Rescore scores: min=%.1f p25=%.1f p50=%.1f p75=%.1f max=%.1f | "
            "Strong=%d Good=%d Average=%d Weak=%d",
            scores[0], p25, p50, p75, scores[-1],
            tiers["Strong"], tiers["Good"], tiers["Average"], tiers["Weak"],
        )

    # 5. Upsert scored rows back
    upsert_t0 = time_mod.time()
    total_upserted = 0
    for batch_start in range(0, len(scored_rows), _UPSERT_BATCH_SIZE):
        batch = scored_rows[batch_start: batch_start + _UPSERT_BATCH_SIZE]
        count = await discover_service.upsert_discover_stock_snapshots(batch)
        total_upserted += count

    # Insert score history snapshot for trend tracking
    try:
        history_count = await discover_service.insert_score_history(scored_rows)
        await discover_service.prune_score_history(days=30)
        logger.info("Score history: inserted %d snapshots, pruned >30d", history_count)
    except Exception:
        logger.exception("Score history insert failed (non-fatal)")

    total_elapsed = time_mod.time() - t0
    logger.info(
        "Rescore complete: %d rows in %.1fs (read=%.1fs, score=%.1fs, upsert=%.1fs)",
        total_upserted, total_elapsed, read_elapsed, score_elapsed, time_mod.time() - upsert_t0,
    )

    return {
        "status": "completed",
        "rows_scored": len(scored_rows),
        "rows_upserted": total_upserted,
        "elapsed_seconds": round(total_elapsed, 1),
        "coverage": {
            "total": total,
            "fundamentals": has_fundamentals,
            "yahoo": has_yahoo,
            "shareholding": has_shareholding,
            "analyst": has_analyst,
            "industry": has_industry,
        },
        "missing_all": {group: len(syms) for group, syms in stocks_missing_all.items()},
        "score_distribution": tiers if scores else {},
    }
