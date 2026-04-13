"""Trading Economics scraper for macro indicators.

Fetches PMI, IIP, trade balance, forex reserves, and other indicators
from tradingeconomics.com using browser-like HTTP requests.
"""
from __future__ import annotations

import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
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
    # NOTE: India core inflation endpoint is currently unavailable on public TE pages.
    # Keep core_inflation in metadata for UI, but skip scraping until source stabilizes.
    ("food_inflation", "IN", "/india/food-inflation", "percent_yoy", False),
    # Trade & Fiscal — negate deficits
    ("forex_reserves", "IN", "/india/foreign-exchange-reserves", "usd_mn", False),
    ("trade_balance", "IN", "/india/balance-of-trade", "usd_bn", True),
    ("trade_balance", "US", "/united-states/balance-of-trade", "usd_bn", True),
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
    # Try "March 13" (assume current year)
    m = re.match(r"([A-Za-z]+)\s+(\d{1,2})$", text)
    if m:
        now = datetime.now(timezone.utc)
        month_str = m.group(1)[:3].lower()
        month = _MONTH_MAP.get(month_str)
        if month:
            day = min(max(int(m.group(2)), 1), 28)
            return datetime(now.year, month, day, tzinfo=timezone.utc)
    return None


def _parse_value(text: str) -> Optional[float]:
    """Parse numeric value from TE page, handling commas and units."""
    text = text.strip().replace(",", "").replace("%", "")
    text = text.rstrip(".,;:")
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

    def _month_ref_to_date(
        self,
        month_ref: str,
        latest_date: datetime,
    ) -> Optional[datetime]:
        ref = month_ref.strip().replace("’", "'")
        if not ref:
            return None
        parsed = _parse_te_date(ref)
        if parsed is not None:
            # If month/day inferred to current year but would be after latest date,
            # push one year back to preserve chronology.
            if parsed > latest_date:
                try:
                    return parsed.replace(year=parsed.year - 1)
                except ValueError:
                    return parsed
            return parsed

        m = re.match(r"([A-Za-z]+)$", ref)
        if not m:
            return None
        month = _MONTH_MAP.get(m.group(1)[:3].lower())
        if not month:
            return None
        year = latest_date.year
        if month > latest_date.month:
            year -= 1
        return datetime(year, month, 1, tzinfo=timezone.utc)

    def _relative_ref_to_date(
        self,
        relative_ref: str,
        latest_date: datetime,
    ) -> Optional[datetime]:
        ref = relative_ref.lower().strip()
        if "previous week" in ref:
            return latest_date - timedelta(days=7)
        if "previous month" in ref:
            month = latest_date.month - 1
            year = latest_date.year
            if month == 0:
                month = 12
                year -= 1
            return datetime(year, month, 1, tzinfo=timezone.utc)
        if "previous quarter" in ref:
            month = latest_date.month - 3
            year = latest_date.year
            while month <= 0:
                month += 12
                year -= 1
            return datetime(year, month, 1, tzinfo=timezone.utc)
        if "previous year" in ref:
            return datetime(latest_date.year - 1, latest_date.month, 1, tzinfo=timezone.utc)
        if "previous fiscal year" in ref:
            return datetime(latest_date.year - 1, 3, 1, tzinfo=timezone.utc)
        return None

    def _extract_previous_point(
        self,
        desc_text: str,
        latest_date: datetime,
    ) -> Optional[Tuple[float, datetime]]:
        text = desc_text.replace("’", "'")
        patterns = [
            # "... from the upwardly revised ... 8% increase in the previous month"
            (
                re.compile(
                    r"from.{0,90}?(?:USD\s+)?(?:INR\s+)?(?:\$\s*)?([\d,\.]+)"
                    r"(?:\s*(?:percent|%|USD\s+Million|billion|million|bn|index))?"
                    r".{0,60}?\s+in\s+the\s+previous\s+(week|month|quarter|year|fiscal\s+year)",
                    re.IGNORECASE,
                ),
                1,
                2,
            ),
            # "... from the 8.4% increase in December"
            (
                re.compile(
                    r"from\s+the\s+([\d,\.]+)\s*(?:percent|%)\s+\w+\s+in\s+([A-Za-z]+(?:\s+\d{4})?)",
                    re.IGNORECASE,
                ),
                1,
                2,
            ),
            # "... from 55.4 in January", "... from 716810 USD Million in the previous week"
            (
                re.compile(
                    r"from\s+(?:an\s+initial\s+estimate\s+of\s+)?"
                    r"(?:USD\s+)?(?:INR\s+)?(?:\$\s*)?([\d,\.]+)"
                    r"(?:\s*(?:percent|%|USD\s+Million|billion|million|bn|index))?"
                    r"\s+in\s+(the\s+previous\s+(?:week|month|quarter|year|fiscal\s+year)|[A-Za-z]+(?:\s+\d{4})?)",
                    re.IGNORECASE,
                ),
                1,
                2,
            ),
            # "... from $14.42 billion a year earlier"
            (
                re.compile(
                    r"from\s+(?:\$\s*)?([\d,\.]+)"
                    r"(?:\s*(?:percent|%|billion|million|bn|index))?"
                    r"\s+(?:a\s+year\s+earlier|the\s+previous\s+year)",
                    re.IGNORECASE,
                ),
                1,
                None,
            ),
            # "... after a 2.13 percent rise in January"
            (
                re.compile(
                    r"after\s+(?:a\s+)?(?:\$\s*)?([\d,\.]+)"
                    r"(?:\s*(?:percent|%))?(?:\s+\w+){0,5}\s+in\s+([A-Za-z]+(?:\s+\d{4})?)",
                    re.IGNORECASE,
                ),
                1,
                2,
            ),
            # "January's reading of 58.5"
            (
                re.compile(
                    r"([A-Za-z]+)'s\s+reading\s+of\s+(?:\$\s*)?([\d,\.]+)",
                    re.IGNORECASE,
                ),
                2,
                1,
            ),
            # "... following a revised $72.9 billion in December"
            (
                re.compile(
                    r"following\s+(?:a\s+revised\s+)?(?:\$\s*)?([\d,\.]+)"
                    r"(?:\s*(?:percent|%|billion|million|bn|index))?"
                    r"\s+in\s+([A-Za-z]+(?:\s+\d{4})?)",
                    re.IGNORECASE,
                ),
                1,
                2,
            ),
            # "... from the $11.3 billion gap on the corresponding period of the previous year"
            (
                re.compile(
                    r"from\s+(?:the\s+)?(?:\$\s*)?([\d,\.]+)"
                    r"(?:\s*(?:percent|%|billion|million|bn|index))?"
                    r"(?:\s+\w+){0,6}\s+on\s+the\s+corresponding\s+period\s+of\s+the\s+previous\s+year",
                    re.IGNORECASE,
                ),
                1,
                None,
            ),
        ]

        for pattern, value_idx, date_idx in patterns:
            for match in pattern.finditer(text):
                value = _parse_value(match.group(value_idx))
                if value is None:
                    continue
                if date_idx is None:
                    ref_date = datetime(
                        latest_date.year - 1,
                        latest_date.month,
                        1,
                        tzinfo=timezone.utc,
                    )
                    return value, ref_date

                date_ref = match.group(date_idx).strip()
                if re.fullmatch(r"(week|month|quarter|year|fiscal\s+year)", date_ref, re.IGNORECASE):
                    date_ref = f"previous {date_ref}"
                ref_date = self._relative_ref_to_date(date_ref, latest_date)
                if ref_date is None:
                    ref_date = self._month_ref_to_date(date_ref, latest_date)
                if ref_date is not None and ref_date < latest_date:
                    return value, ref_date
        return None

    def _extract_points(self, html: str) -> List[Tuple[float, datetime]]:
        """Extract latest (and when possible previous) points from TE HTML."""
        soup = BeautifulSoup(html, "html.parser")
        points: List[Tuple[float, datetime]] = []

        # Strategy 1: Extract from #historical-desc (most reliable)
        desc_el = soup.find(id="historical-desc")
        value = None
        date = None
        desc_text = ""
        if desc_el:
            desc_text = desc_el.get_text(" ", strip=True)

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
                r"held (?:steady )?at|accelerated to|decelerated to|"
                r"expanded to|contracted to|moderated to|improved to|"
                r"slowed to|surged to|plunged to|recovered to|"
                r"deficit (?:of|to)|surplus (?:of|to))\s*"
                r"(?:USD\s+)?(?:INR\s+)?(?:\$\s*)?([\d,\.]+)",
                desc_text, re.IGNORECASE,
            )
            if val_match:
                value = _parse_value(val_match.group(1))

            # Extract date: "in February 2026" or "in March 13" (TE uses day-of-month for weekly)
            date_match = re.search(r"in\s+([A-Z][a-z]+)\s+(\d{4})", desc_text)
            if date_match:
                date = _parse_te_date(f"{date_match.group(1)} {date_match.group(2)}")
            else:
                # Weekly data: "in March 13" means March 13 of current year
                date_match2 = re.search(r"in\s+([A-Z][a-z]+)\s+(\d{1,2})(?:\s+from)", desc_text)
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
            return points

        # Fallback date: current month
        if date is None:
            now = datetime.now(timezone.utc)
            date = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
        points.append((value, date))

        if desc_text:
            previous = self._extract_previous_point(desc_text, date)
            if previous is not None:
                prev_val, prev_date = previous
                if prev_date < date:
                    points.append(previous)
        return points

    def _extract_value_and_date(self, html: str) -> Optional[Tuple[float, datetime]]:
        """Backward-compatible wrapper returning latest point only."""
        points = self._extract_points(html)
        if not points:
            return None
        return points[0]

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
                points = self._extract_points(html)
                if points:
                    emitted = 0
                    seen_dates = set()
                    for val, ts in points:
                        # Keep at most one point per day per indicator in the payload.
                        key = ts.date().isoformat()
                        if key in seen_dates:
                            continue
                        seen_dates.add(key)
                        adj_val = -val if negate and val > 0 else val
                        results.append({
                            "indicator_name": indicator_name,
                            "value": round(adj_val, 4),
                            "country": country,
                            "timestamp": ts.isoformat(),
                            "unit": unit,
                            "source": "trading_economics",
                        })
                        emitted += 1
                    succeeded += 1
                    latest_val, latest_ts = points[0]
                    latest_adj = -latest_val if negate and latest_val > 0 else latest_val
                    logger.info(
                        "TE OK %s/%s = %.4f (%s) [points=%d]",
                        country,
                        indicator_name,
                        latest_adj,
                        latest_ts.date(),
                        emitted,
                    )
                else:
                    logger.warning("TE parse failed for %s/%s", country, indicator_name)

            time.sleep(random.uniform(_RATE_LIMIT_MIN, _RATE_LIMIT_MAX))

        logger.info(
            "TE scraper complete: %d/%d fetched, %d succeeded",
            fetched, len(TE_INDICATORS), succeeded,
        )
        return results
