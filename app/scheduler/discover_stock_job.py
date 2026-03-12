from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
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

                fundamentals = {
                    "pe_ratio": self._extract_labeled_number(text, ["Stock P/E", "P/E"]),
                    "roe": self._extract_labeled_number(text, ["ROE", "Return on equity"]),
                    "roce": self._extract_labeled_number(text, ["ROCE", "Return on capital employed"]),
                    "debt_to_equity": self._extract_labeled_number(text, ["Debt to equity", "Debt to Equity"]),
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
        below = sum(1 for v in values if v < target)
        return (below / len(values)) * 100.0

    @staticmethod
    def _median(values: list[float]) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        n = len(s)
        mid = n // 2
        return (s[mid] + s[mid - 1]) / 2.0 if n % 2 == 0 else s[mid]

    def _score_fundamentals(
        self,
        row: dict,
        sector_medians: dict[str, dict[str, float]],
    ) -> tuple[float, int]:
        """Weighted fundamentals score with sector-relative PE/PB."""
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

        # --- Debt-to-Equity (lower is better) ---
        dte = row.get("debt_to_equity")
        if dte is not None:
            parts["dte"] = self._clamp(100 - (dte * 45))
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

    def _compute_scores(self, rows: list[dict]) -> list[dict]:
        if not rows:
            return []

        # Pre-compute sector medians for PE and PB
        sector_pe: dict[str, list[float]] = {}
        sector_pb: dict[str, list[float]] = {}
        for r in rows:
            sector = str(r.get("sector") or "Other")
            pe = r.get("pe_ratio")
            if pe is not None and pe > 0:
                sector_pe.setdefault(sector, []).append(float(pe))
            pb = r.get("price_to_book")
            if pb is not None and pb > 0:
                sector_pb.setdefault(sector, []).append(float(pb))

        # Overall medians as fallback for sectors with < 3 stocks
        all_pe = [v for vals in sector_pe.values() for v in vals]
        all_pb = [v for vals in sector_pb.values() for v in vals]
        overall_pe_med = self._median(all_pe) if all_pe else 25.0
        overall_pb_med = self._median(all_pb) if all_pb else 4.0

        sector_medians: dict[str, dict[str, float]] = {}
        all_sectors = set(str(r.get("sector") or "Other") for r in rows)
        for sector in all_sectors:
            pe_vals = sector_pe.get(sector, [])
            pb_vals = sector_pb.get(sector, [])
            sector_medians[sector] = {
                "pe": self._median(pe_vals) if len(pe_vals) >= 3 else overall_pe_med,
                "pb": self._median(pb_vals) if len(pb_vals) >= 3 else overall_pb_med,
            }

        # Pre-compute percentile data for momentum
        all_pcts = [float(r.get("percent_change") or 0.0) for r in rows]

        # Pre-compute max values for liquidity
        max_tv = max((float(r.get("traded_value") or 0.0) for r in rows), default=0.0)
        max_vol = max((float(r.get("volume") or 0.0) for r in rows), default=0.0)

        # Track sector scores for sector_leader tag
        sector_best: dict[str, tuple[float, str]] = {}

        out: list[dict] = []
        for row in rows:
            pct = float(row.get("percent_change") or 0.0)
            tv = float(row.get("traded_value") or 0.0)
            vol = float(row.get("volume") or 0.0)

            # Percentile-based momentum
            momentum = self._clamp(self._percentile_rank(all_pcts, pct))

            # Weighted liquidity: 60% traded_value + 40% volume
            tv_norm = (tv / max_tv) if max_tv > 0 else 0.0
            vol_norm = (vol / max_vol) if max_vol > 0 else 0.0
            liquidity = self._clamp((tv_norm * 0.60 + vol_norm * 0.40) * 100)

            fundamentals, metrics_used = self._score_fundamentals(row, sector_medians)
            combined_signal = round((momentum + liquidity) / 2.0, 2)
            if metrics_used == 0:
                total = combined_signal
            else:
                total = (combined_signal * 0.30) + (fundamentals * 0.70)

            source_status = str(row.get("source_status") or "limited")
            if metrics_used == 0 and source_status == "primary":
                source_status = "fallback"

            tags: list[str] = []
            if pct >= 2:
                tags.append("momentum_up")
            elif pct <= -2:
                tags.append("momentum_down")
            if liquidity >= 70:
                tags.append("high_liquidity")
            if fundamentals >= 70:
                tags.append("strong_fundamentals")
            eps = row.get("eps")
            if eps is not None and eps < 0:
                tags.append("negative_eps")
            # Check for undervalued: PE below sector median + strong fundamentals
            pe = row.get("pe_ratio")
            sector = str(row.get("sector") or "Other")
            med_pe = sector_medians.get(sector, {}).get("pe", 25.0)
            if pe is not None and pe > 0 and pe < med_pe and fundamentals >= 60:
                tags.append("undervalued")
            if source_status != "primary":
                tags.append("limited_data")
            if not tags:
                tags.append("balanced")

            score = round(self._clamp(total), 2)
            symbol = str(row.get("symbol") or "")

            # Track sector leader
            if sector not in sector_best or score > sector_best[sector][0]:
                sector_best[sector] = (score, symbol)

            enriched = {
                **row,
                "score": score,
                "score_momentum": round(momentum, 2),
                "score_liquidity": round(liquidity, 2),
                "score_fundamentals": round(fundamentals, 2),
                "score_breakdown": {
                    "momentum": round(momentum, 2),
                    "liquidity": round(liquidity, 2),
                    "fundamentals": round(fundamentals, 2),
                    "combined_signal": round(combined_signal, 2),
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
            if best and best[1] == symbol and "sector_leader" not in enriched["tags"]:
                enriched["tags"].insert(0, "sector_leader")

        out.sort(key=lambda item: (float(item.get("score") or 0.0), float(item.get("percent_change") or 0.0)), reverse=True)
        return out

    def _build_snapshot_row(self, stock: DiscoverStockDef, quote: dict, quote_source: str) -> dict:
        if bool(getattr(stock, "fundamentals_enabled", True)):
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

    def fetch_all(self) -> list[dict]:
        universe = self._build_effective_universe()
        bulk_quotes, _ = self._fetch_latest_bhavcopy_quotes()
        raw_rows: list[dict] = []
        if bulk_quotes:
            missing: list[DiscoverStockDef] = []
            for stock in universe:
                quote = bulk_quotes.get(stock.nse_symbol)
                if quote is None:
                    missing.append(stock)
                    continue
                raw_rows.append(self._build_snapshot_row(stock, quote, "nse_bhavcopy"))
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
        return self._compute_scores(raw_rows)


_scraper = DiscoverStockScraper()


def _fetch_discover_stock_rows_sync() -> list[dict]:
    return _scraper.fetch_all()


async def run_discover_stock_job() -> None:
    try:
        loop = asyncio.get_event_loop()
        rows = await loop.run_in_executor(
            get_job_executor("discover-stock"),
            _fetch_discover_stock_rows_sync,
        )
        count = await discover_service.upsert_discover_stock_snapshots(rows)
        logger.info("Discover stock job complete: %d snapshots upserted", count)
    except requests.RequestException:
        logger.exception("Discover stock job failed due to network exception")
    except Exception:
        logger.exception("Discover stock job failed")
