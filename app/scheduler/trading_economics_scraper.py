"""Trading Economics scraper for macro indicators.

Fetches PMI, IIP, trade balance, forex reserves, and other indicators
from tradingeconomics.com using browser-like HTTP requests.
"""
from __future__ import annotations

import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Indicator definitions: (indicator_name, country, url_path, unit, negate)
# negate=True for deficit indicators where TE reports absolute value but we store negative
TE_INDICATORS: List[Tuple[str, str, str, str, bool]] = [
    # Growth indicators
    ("pmi_manufacturing", "IN", "/india/manufacturing-pmi", "index", False),
    ("pmi_manufacturing", "US", "/united-states/manufacturing-pmi", "index", False),
    ("pmi_services", "IN", "/india/services-pmi", "index", False),
    ("pmi_services", "US", "/united-states/non-manufacturing-pmi", "index", False),
    ("iip", "IN", "/india/industrial-production", "percent_yoy", False),
    ("iip", "US", "/united-states/industrial-production", "percent_yoy", False),
    # Prices
    ("core_inflation", "IN", "/india/core-inflation-rate", "percent_yoy", False),
    ("food_inflation", "IN", "/india/food-inflation", "percent_yoy", False),
    # Trade & Fiscal — negate deficits
    ("forex_reserves", "IN", "/india/foreign-exchange-reserves", "usd_mn", False),
    ("trade_balance", "IN", "/india/balance-of-trade", "usd_mn", True),
    ("trade_balance", "US", "/united-states/balance-of-trade", "usd_mn", True),
    ("current_account_deficit", "IN", "/india/current-account", "usd_bn", True),
    ("fiscal_deficit", "IN", "/india/government-budget", "percent_gdp", False),
    ("bank_credit_growth", "IN", "/india/bank-lending-rate", "percent", False),
]

_BASE_URL = "https://tradingeconomics.com"
_RATE_LIMIT_MIN = 2.0
_RATE_LIMIT_MAX = 4.0
_MAX_CONSECUTIVE_FAILURES = 3

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Month name patterns for date parsing
_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_te_date(text: str) -> Optional[datetime]:
    """Parse TE date strings like 'Feb 2026', 'Jan/26', '2026-02-01'."""
    text = text.strip()
    # Try ISO format first
    try:
        return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        pass
    # Try "Feb 2026" or "February 2026"
    m = re.match(r"([A-Za-z]+)\s*[\-/]?\s*(\d{4})", text)
    if m:
        month_str = m.group(1)[:3].lower()
        year = int(m.group(2))
        month = _MONTH_MAP.get(month_str)
        if month:
            return datetime(year, month, 1, tzinfo=timezone.utc)
    # Try "Jan/26" or "Feb/25"
    m = re.match(r"([A-Za-z]+)\s*[\-/]\s*(\d{2})", text)
    if m:
        month_str = m.group(1)[:3].lower()
        year = 2000 + int(m.group(2))
        month = _MONTH_MAP.get(month_str)
        if month:
            return datetime(year, month, 1, tzinfo=timezone.utc)
    return None


def _parse_value(text: str) -> Optional[float]:
    """Parse numeric value from TE page, handling commas and units."""
    text = text.strip().replace(",", "").replace("%", "")
    # Remove trailing unit suffixes like 'B', 'M', 'K'
    multiplier = 1.0
    if text.endswith("B"):
        text = text[:-1]
        multiplier = 1.0  # already in billions context
    elif text.endswith("M"):
        text = text[:-1]
        multiplier = 1.0
    elif text.endswith("K"):
        text = text[:-1]
        multiplier = 1.0
    try:
        return float(text) * multiplier
    except (ValueError, TypeError):
        return None


class TradingEconomicsScraper:
    """Scrapes indicator values from Trading Economics."""

    def __init__(self) -> None:
        self._session: Optional[requests.Session] = None
        self._consecutive_failures = 0

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(_HEADERS)
            # Establish cookies by visiting homepage
            try:
                self._session.get(_BASE_URL, timeout=15)
                time.sleep(1.0)
            except Exception:
                logger.warning("TE homepage visit failed; proceeding without cookies")
        return self._session

    def _fetch_page(self, path: str) -> Optional[str]:
        """Fetch a TE indicator page, returns HTML or None."""
        session = self._get_session()
        url = f"{_BASE_URL}{path}"
        try:
            resp = session.get(url, timeout=20)
            if resp.status_code == 403:
                self._consecutive_failures += 1
                logger.warning("TE 403 for %s (consecutive: %d)", path, self._consecutive_failures)
                return None
            if resp.status_code != 200:
                logger.warning("TE HTTP %d for %s", resp.status_code, path)
                return None
            self._consecutive_failures = 0
            return resp.text
        except Exception:
            logger.exception("TE fetch failed for %s", path)
            self._consecutive_failures += 1
            return None

    def _extract_value_and_date(self, html: str) -> Optional[Tuple[float, datetime]]:
        """Extract the current indicator value and date from TE HTML.

        Primary strategy: parse the #historical-desc paragraph which contains
        sentences like "rose to 56.9 in February 2026" or "decreased to 709760
        USD Million in March 13".
        """
        soup = BeautifulSoup(html, "html.parser")

        value = None
        date = None

        # Strategy 1: Extract from #historical-desc (most reliable)
        desc_el = soup.find(id="historical-desc")
        if desc_el:
            text = desc_el.get_text()

            # Extract value from various TE description patterns:
            # "rose to 56.9", "fell to 4.2", "rose by 4.8%", "widened to $27.10",
            # "revised...to 58.1", "deficit to 4.8%", "remained unchanged at 9.06"
            val_match = re.search(
                r"(?:rose to|fell to|decreased to|increased to|was|stood at|"
                r"came in at|remained (?:unchanged )?at|edged (?:up|down) to|"
                r"went up to|went down to|dropped to|climbed to|hit|reached|"
                r"rose by|fell by|increased by|decreased by|grew by|shrank by|"
                r"rose |fell |increased |decreased |"
                r"widened to|narrowed to|revised\b.*?\bto|"
                r"deficit (?:of|to)|surplus (?:of|to))\s*"
                r"(?:USD\s+)?(?:INR\s+)?(?:\$\s*)?([\d,\.]+)",
                text, re.IGNORECASE,
            )
            if val_match:
                value = _parse_value(val_match.group(1))

            # Extract date: "in February 2026" or "in March 13" (TE uses day-of-month for weekly)
            date_match = re.search(r"in\s+([A-Z][a-z]+)\s+(\d{4})", text)
            if date_match:
                date = _parse_te_date(f"{date_match.group(1)} {date_match.group(2)}")
            else:
                # Weekly data: "in March 13" means March 13 of current year
                date_match2 = re.search(r"in\s+([A-Z][a-z]+)\s+(\d{1,2})(?:\s+from)", text)
                if date_match2:
                    now = datetime.now(timezone.utc)
                    month_str = date_match2.group(1)[:3].lower()
                    day = int(date_match2.group(2))
                    month = _MONTH_MAP.get(month_str)
                    if month:
                        date = datetime(now.year, month, min(day, 28), tzinfo=timezone.utc)

        # Strategy 2: If desc failed, try CSS selectors
        if value is None:
            for selector in ["div#tinline td", "span.nch"]:
                el = soup.select_one(selector)
                if el:
                    value = _parse_value(el.get_text())
                    if value is not None:
                        break

        if value is None:
            return None

        # Fallback date: current month
        if date is None:
            now = datetime.now(timezone.utc)
            date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

        return value, date

    def fetch_all(self) -> List[Dict]:
        """Fetch all TE indicators. Returns list of dicts compatible with MacroScraper._select_best."""
        results: List[Dict] = []
        self._consecutive_failures = 0
        fetched = 0
        succeeded = 0

        for indicator_name, country, path, unit, negate in TE_INDICATORS:
            if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                logger.warning(
                    "TE circuit breaker: %d consecutive failures, aborting remaining %d indicators",
                    self._consecutive_failures,
                    len(TE_INDICATORS) - fetched,
                )
                break

            html = self._fetch_page(path)
            fetched += 1

            if html:
                parsed = self._extract_value_and_date(html)
                if parsed:
                    val, ts = parsed
                    if negate and val > 0:
                        val = -val
                    results.append({
                        "indicator_name": indicator_name,
                        "value": round(val, 4),
                        "country": country,
                        "timestamp": ts.isoformat(),
                        "unit": unit,
                        "source": "trading_economics",
                    })
                    succeeded += 1
                    logger.info("TE OK %s/%s = %.4f (%s)", country, indicator_name, val, ts.date())
                else:
                    logger.warning("TE parse failed for %s/%s", country, indicator_name)

            time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))

        logger.info(
            "TE scraper complete: %d/%d fetched, %d succeeded",
            fetched, len(TE_INDICATORS), succeeded,
        )
        return results
