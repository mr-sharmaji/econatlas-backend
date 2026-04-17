"""Broker trade charges scraper + seeder.

Scrapes official pricing pages of Indian brokers weekly and upserts
into broker_charges + statutory_charges tables. Falls back to
hardcoded reference values when scraping fails.

Sources:
  - zerodha.com/charges
  - upstox.com/brokerage-charges
  - groww.in/pricing
  - angelone.in/exchange-transaction-charges
"""
from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup
from app.scheduler.base import get_browser_headers

logger = logging.getLogger(__name__)

# ── Segments ──────────────────────────────────────────────────────────
SEGMENTS = [
    "equity_delivery", "equity_intraday",
    "equity_futures", "equity_options",
    "currency_futures", "currency_options",
    "commodity_futures", "commodity_options",
]

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_TIMEOUT = 15.0


# ── Fallback reference data (2025-26, from official pricing pages) ────

_FALLBACK_BROKER_DATA: dict[str, dict[str, dict]] = {
    # Zerodha: 0.03% or ₹20 whichever is lower for intraday/F&O
    "zerodha": {
        "equity_delivery":   {"brokerage_mode": "free", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 0, "min_charge": 0},
        "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0003, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "equity_futures":    {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0003, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "currency_futures":  {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0003, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "commodity_futures": {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0003, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
    },
    # Upstox: ₹20 delivery, 0.1% or ₹20 intraday, 0.05% or ₹20 futures, ₹20 options
    "upstox": {
        "equity_delivery":   {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "equity_futures":    {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "currency_futures":  {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "commodity_futures": {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 0},
        "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
    },
    "groww": {
        "equity_delivery":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "equity_futures":    {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 5},
        "currency_futures":  {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 5},
        "commodity_futures": {"brokerage_mode": "percent_cap", "brokerage_pct": 0.0005, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 5},
    },
    "angel_one": {
        "equity_delivery":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "equity_intraday":   {"brokerage_mode": "percent_cap", "brokerage_pct": 0.001, "brokerage_cap": 20, "brokerage_flat": 0, "min_charge": 5},
        "equity_futures":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "equity_options":    {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "currency_futures":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "currency_options":  {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "commodity_futures": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
        "commodity_options": {"brokerage_mode": "flat", "brokerage_pct": 0, "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0},
    },
}

# Taglines are BROKERAGE-ONLY one-liners. AMC details live in
# broker_charges_service._BROKER_AMC_NOTES (served as amc_note +
# amc_rules fields on the API response). `amc_yearly` stores the
# non-BSDA (standard) figure as a single numeric value for sorting
# and headline display only — it is NOT the whole story.
#
# dp_includes_gst note:
#   Zerodha publishes ₹15.34 as ₹3.5 CDSL + ₹9.5 Zerodha + ₹2.34 GST
#   — ALREADY GST-inclusive, so we must NOT re-apply GST on top or
#   the calculator over-estimates by ~₹2.76 per delivery trade.
#   Upstox/Groww/Angel One quote a clean ₹20 which is also treated
#   as consumer-facing (GST-inclusive) to stay consistent.
_FALLBACK_BROKER_META: dict[str, dict] = {
    "zerodha":   {"tagline": "Delivery free · 0.03%/₹20 intraday/F&O · options flat ₹20", "dp_charge": 15.34, "dp_includes_gst": True,  "amc_yearly": 354, "account_opening_fee": 0, "call_trade_fee": 50},
    "upstox":    {"tagline": "₹20 delivery · 0.1%/₹20 intraday · 0.05% futures · ₹20 options", "dp_charge": 20.0,  "dp_includes_gst": True,  "amc_yearly": 354, "account_opening_fee": 0, "call_trade_fee": 88.5},
    # Groww / Angel One both have a "min ₹5 per order" rule on equity
    # that only matters for tiny trades (under ~₹5,000 per side, where
    # 0.1% is less than ₹5). It's still applied in the calculation via
    # the per-segment min_charge field, but we keep it OUT of the
    # tagline so users don't read it as a base charge that always
    # applies. The chip / breakdown surface it when it actually fires.
    "groww":     {"tagline": "0.1%/₹20 equity · ₹20 flat F&O", "dp_charge": 20.0,  "dp_includes_gst": True,  "amc_yearly": 0,   "account_opening_fee": 0, "call_trade_fee": 0},
    "angel_one": {"tagline": "0.1%/₹20 equity · ₹20 flat F&O", "dp_charge": 20.0,  "dp_includes_gst": True,  "amc_yearly": 450, "account_opening_fee": 0, "call_trade_fee": 0},
}

# ── Statutory rates (2025-26, revised Oct 2024) ──────────────────────

_FALLBACK_STATUTORY: dict[str, dict] = {
    "equity_delivery":   {"stt_buy": 0.001, "stt_sell": 0.001, "nse_txn": 0.0000307, "bse_txn": 0.0000375, "stamp": 0.00015, "ipft_nse": 0.000001},
    "equity_intraday":   {"stt_buy": 0, "stt_sell": 0.00025, "nse_txn": 0.0000307, "bse_txn": 0.0000375, "stamp": 0.00003, "ipft_nse": 0.000001},
    "equity_futures":    {"stt_buy": 0, "stt_sell": 0.0002, "nse_txn": 0.0000183, "bse_txn": 0, "stamp": 0.00002, "ipft_nse": 0.000001},
    "equity_options":    {"stt_buy": 0, "stt_sell": 0.001, "nse_txn": 0.0003553, "bse_txn": 0.000325, "stamp": 0.00003, "ipft_nse": 0.000005},
    "currency_futures":  {"stt_buy": 0, "stt_sell": 0, "nse_txn": 0.0000035, "bse_txn": 0.0000045, "stamp": 0.000001, "ipft_nse": 0.0000005},
    "currency_options":  {"stt_buy": 0, "stt_sell": 0, "nse_txn": 0.000311, "bse_txn": 0.00001, "stamp": 0.000001, "ipft_nse": 0},
    "commodity_futures": {"stt_buy": 0, "stt_sell": 0.0001, "nse_txn": 0.000001, "bse_txn": 0, "stamp": 0.00002, "ipft_nse": 0, "mcx_txn": 0.000021},
    "commodity_options": {"stt_buy": 0, "stt_sell": 0.0005, "nse_txn": 0.000001, "bse_txn": 0, "stamp": 0.00003, "ipft_nse": 0, "mcx_txn": 0.000418},
}

_BROKER_URLS = {
    "zerodha":   "https://zerodha.com/charges",
    "upstox":    "https://upstox.com/brokerage-charges/",
    "groww":     "https://groww.in/pricing",
    "angel_one": "https://www.angelone.in/exchange-transaction-charges",
}


# ── HTML Scrapers ────────────────────────────────────────────────────

def _fetch_html(url: str) -> str | None:
    """Fetch a URL and return its HTML, or None on failure."""
    try:
        resp = requests.get(url, headers=get_browser_headers(), timeout=_TIMEOUT)
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


_RUPEE_RE = re.compile(
    r'(?:[\u20b9₹]|Rs\.?)\s*(\d+(?:\.\d+)?)',
    re.IGNORECASE,
)


def _parse_rupee(text: str) -> float | None:
    """Extract a rupee amount. Requires ₹ or Rs. prefix to avoid
    swallowing bare numbers from percentages ('0.03%' → 0.03)."""
    m = _RUPEE_RE.search(text.replace(',', ''))
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _parse_percent(text: str) -> float | None:
    """Extract a percentage from text like '0.05%', '0.1 %'. Returns as decimal (0.0005)."""
    m = re.search(r'(\d+(?:\.\d+)?)\s*%', text)
    if m:
        try:
            return float(m.group(1)) / 100
        except ValueError:
            pass
    return None


def _parse_brokerage_cell(text: str) -> dict | None:
    """Parse a brokerage cell like:
      'Zero Brokerage'                                   → free
      '0.03% or Rs. 20/executed order whichever is lower' → percent_cap
      '₹20 per executed order'                            → flat
      'Flat ₹20 per executed order'                       → flat
      '₹20 per executed order or 0.1% (whichever is lower)' → percent_cap
    Returns a segment dict or None.
    """
    low = text.lower().strip()
    if not low:
        return None

    # Zero / free
    if "zero" in low or low in ("free", "nil"):
        return {"brokerage_mode": "free", "brokerage_pct": 0,
                "brokerage_cap": 0, "brokerage_flat": 0, "min_charge": 0}

    # Find ALL percentages and rupee amounts — brokerage cells often
    # mix promo offers ("₹0 upto ₹500") with real rates ("₹20 or 0.1%")
    # and a minimum ("₹5"), so a naive "first match" would grab noise.
    pcts = [
        float(m.group(1)) / 100
        for m in re.finditer(r'(\d+(?:\.\d+)?)\s*%', text)
    ]
    rupees = [float(m.group(1)) for m in _RUPEE_RE.finditer(text)]
    pcts = [p for p in pcts if p > 0]
    rupees = [r for r in rupees if r > 0]

    # Pick the smallest non-zero percentage (real rate, not spurious)
    pct = min(pcts) if pcts else None

    # Cap is typically ₹10-₹100. Prefer values in that range; otherwise
    # fall back to the smallest non-zero rupee. This separates cap (~₹20)
    # from minimum charge (~₹5) in promo cells like Angel One's.
    cap_candidates = [r for r in rupees if 10 <= r <= 100]
    if cap_candidates:
        rupee = min(cap_candidates)
    elif rupees:
        rupee = min(rupees)
    else:
        rupee = None

    # Minimum charge: if we have a rupee < 10, treat it as min charge
    min_charge_candidates = [r for r in rupees if r < 10]
    min_charge = min_charge_candidates[0] if min_charge_candidates else 0

    # percent_cap: has both pct and cap
    if pct is not None and rupee is not None:
        return {"brokerage_mode": "percent_cap", "brokerage_pct": pct,
                "brokerage_cap": rupee, "brokerage_flat": 0,
                "min_charge": min_charge}

    # flat: only rupee, no pct
    if rupee is not None and pct is None:
        return {"brokerage_mode": "flat", "brokerage_pct": 0,
                "brokerage_cap": 0, "brokerage_flat": rupee,
                "min_charge": min_charge}

    return None


def _normalize_header(text: str) -> str | None:
    """Map a table header cell to a canonical segment key.

    Returns one of the SEGMENTS values or None if not recognized.
    """
    t = text.lower().strip()
    # Equity
    if "equity" in t and "delivery" in t:
        return "equity_delivery"
    if "equity" in t and "intraday" in t:
        return "equity_intraday"
    if ("equity" in t and ("futures" in t or "future" in t)):
        return "equity_futures"
    if ("equity" in t and ("options" in t or "option" in t)):
        return "equity_options"
    # Stock Investments = delivery (Angel One), Intraday Trading = intraday
    if "stock investment" in t or t == "delivery":
        return "equity_delivery"
    if t == "intraday" or "intraday trading" in t:
        return "equity_intraday"
    # F&O header variants (Zerodha: "F&O - Futures", "F&O - Options")
    if ("f&o" in t or "f & o" in t) and ("futures" in t or "future" in t):
        return "equity_futures"
    if ("f&o" in t or "f & o" in t) and ("options" in t or "option" in t):
        return "equity_options"
    # Currency
    if "currency" in t and ("futures" in t or "future" in t):
        return "currency_futures"
    if "currency" in t and ("options" in t or "option" in t):
        return "currency_options"
    # Commodity
    if "commodity" in t and ("futures" in t or "future" in t):
        return "commodity_futures"
    if "commodity" in t and ("options" in t or "option" in t):
        return "commodity_options"
    return None


def _parse_brokerage_table(table) -> dict[str, dict]:
    """Generic parser: find a 'Brokerage' row in a table and map each
    column to a canonical segment via the header row.
    """
    rows = table.find_all("tr")
    if not rows:
        return {}

    # Find header row: first row with any recognizable segment name
    header_idx = -1
    headers: list[str | None] = []
    for i, row in enumerate(rows):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["th", "td"])]
        mapped = [_normalize_header(c) for c in cells]
        if sum(1 for m in mapped if m) >= 2:
            header_idx = i
            headers = mapped
            break

    if header_idx < 0:
        return {}

    # Find brokerage row
    result: dict[str, dict] = {}
    for row in rows[header_idx + 1:]:
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        label = cells[0].get_text(" ", strip=True).lower()
        if "brokerage" not in label:
            continue

        # Each subsequent cell corresponds to a header column.
        # Header has a leading empty cell before the segment names;
        # body rows also have the label in cell 0 — so body cell i
        # aligns to header cell i.
        for i, cell in enumerate(cells):
            if i >= len(headers):
                break
            segment = headers[i]
            if not segment:
                continue
            parsed = _parse_brokerage_cell(cell.get_text(" ", strip=True))
            if parsed:
                result[segment] = parsed
        break  # only first brokerage row

    return result


def _scrape_generic_tables(html: str, broker_name: str) -> dict | None:
    """Generic multi-table scraper.

    Walks every <table> on the page and parses any table with a
    'Brokerage' row via _parse_brokerage_table. Results are merged
    across tables so multi-table layouts (e.g. Zerodha has separate
    equity / F&O / currency / commodity tables) are handled uniformly.

    NB: intentionally does NOT scrape metadata (DP / AMC / call-trade) —
    those fields vary too much in page layout to extract reliably, and
    tiered AMC tables (Zerodha: free <₹4L → ₹300/yr >₹10L) can't be
    compressed to a single value. Metadata comes from the hand-maintained
    _FALLBACK_BROKER_META dict.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        result: dict[str, Any] = {"segments": {}, "meta": {}}

        for table in soup.find_all("table"):
            parsed = _parse_brokerage_table(table)
            if parsed:
                result["segments"].update(parsed)

        if result["segments"]:
            logger.info(
                "%s scraper: extracted %d segments",
                broker_name, len(result["segments"]),
            )
            return result

        logger.warning("%s scraper: no segments extracted from HTML", broker_name)
        return None
    except Exception as exc:
        logger.warning("%s scraper parse error: %s", broker_name, exc)
        return None


def _scrape_zerodha(html: str) -> dict | None:
    return _scrape_generic_tables(html, "Zerodha")


def _scrape_upstox(html: str) -> dict | None:
    return _scrape_generic_tables(html, "Upstox")


def _scrape_groww(html: str) -> dict | None:
    """Groww's pricing page has no table with a 'Brokerage' row.
    The equity brokerage is in body copy like:
      'Equity Brokerage ₹ 20 0.1% per executed order whichever is lower, minimum ₹5'

    F&O rates are not stated on the page — those fall back to reference.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        result: dict[str, Any] = {"segments": {}, "meta": {}}

        # Isolate the "Equity Brokerage ... ₹X ... Y% ... minimum ₹Z" snippet.
        # NB: not using re.IGNORECASE because it makes [^A-Z] exclude
        # lowercase letters too — stops the match at the first letter.
        m = re.search(
            r'Equity\s+[Bb]rokerage[^A-Z]{0,200}',
            text,
        )
        if m:
            snippet = m.group(0)
            parsed = _parse_brokerage_cell(snippet)
            if parsed and parsed["brokerage_mode"] == "percent_cap":
                for seg in ["equity_delivery", "equity_intraday"]:
                    result["segments"][seg] = dict(parsed)

        if result["segments"]:
            logger.info(
                "Groww scraper: extracted %d segments",
                len(result["segments"]),
            )
            return result

        logger.warning("Groww scraper: no segments extracted from HTML")
        return None
    except Exception as exc:
        logger.warning("Groww scraper parse error: %s", exc)
        return None


def _scrape_angel_one(html: str) -> dict | None:
    return _scrape_generic_tables(html, "Angel One")


# ── Statutory rates scraper (from zerodha.com/charges) ───────────────

def _scrape_statutory_from_zerodha(html: str) -> dict | None:
    """Extract statutory charge rates from Zerodha's charges page.

    Zerodha lists comprehensive statutory charges including STT, exchange
    transaction charges, stamp duty, SEBI fees, and GST for all segments.
    These are government/exchange-mandated and same across all brokers.
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True).lower()
        result = {}

        # Look for STT rates in tables
        tables = soup.find_all("table")
        for table in tables:
            headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
            if any("stt" in h or "security transaction" in h for h in headers):
                for row in table.find_all("tr"):
                    cells = [td.get_text(strip=True).lower() for td in row.find_all(["td", "th"])]
                    if len(cells) >= 2:
                        seg_text = cells[0]
                        rate_text = cells[1] if len(cells) > 1 else ""
                        rate = _parse_percent(rate_text)
                        if rate is not None:
                            # Map table rows to our segments
                            if "delivery" in seg_text:
                                result.setdefault("equity_delivery", {})["stt_buy"] = rate
                                result.setdefault("equity_delivery", {})["stt_sell"] = rate
                            elif "intraday" in seg_text:
                                result.setdefault("equity_intraday", {})["stt_sell"] = rate

        if result:
            logger.info("Statutory scraper: extracted rates for %d segments", len(result))
            return result

        logger.debug("Statutory scraper: no rates extracted (using fallback)")
        return None
    except Exception as exc:
        logger.warning("Statutory scraper parse error: %s", exc)
        return None


_SCRAPERS = {
    "zerodha":   _scrape_zerodha,
    "upstox":    _scrape_upstox,
    "groww":     _scrape_groww,
    "angel_one": _scrape_angel_one,
}


# ── Main job ─────────────────────────────────────────────────────────

def _build_broker_data() -> tuple[list[dict], dict[str, dict]]:
    """Scrape broker sites and merge with fallback data.

    Returns (broker_rows, broker_meta) — scraper results override
    fallback values field-by-field; any fields the scraper doesn't
    extract keep their fallback value.
    """
    broker_meta = dict(_FALLBACK_BROKER_META)
    all_rows: list[dict] = []
    scraped_brokers: list[str] = []
    fallback_brokers: list[str] = []

    for broker, scraper_fn in _SCRAPERS.items():
        url = _BROKER_URLS[broker]
        scraped: dict | None = None

        html = _fetch_html(url)
        if html:
            scraped = scraper_fn(html)

        fallback_segments = _FALLBACK_BROKER_DATA[broker]
        meta = dict(_FALLBACK_BROKER_META.get(broker, {}))

        if scraped:
            scraped_brokers.append(broker)
            # Merge scraped meta over fallback
            if scraped.get("meta"):
                meta.update(scraped["meta"])
        else:
            fallback_brokers.append(broker)

        broker_meta[broker] = meta

        # Build rows per segment, merging scraped data over fallback
        for segment in SEGMENTS:
            base = dict(fallback_segments.get(segment, {
                "brokerage_mode": "flat", "brokerage_pct": 0,
                "brokerage_cap": 0, "brokerage_flat": 20, "min_charge": 0,
            }))
            if scraped and segment in scraped.get("segments", {}):
                base.update(scraped["segments"][segment])

            all_rows.append({
                "broker": broker,
                "segment": segment,
                "source_url": url,
                **base,
            })

        # Be polite between broker sites
        time.sleep(1.5)

    if scraped_brokers:
        logger.info("Broker scraper: scraped %s", ", ".join(scraped_brokers))
    if fallback_brokers:
        logger.info("Broker scraper: using fallback for %s", ", ".join(fallback_brokers))

    return all_rows, broker_meta


def _build_statutory_data(zerodha_html: str | None) -> list[dict]:
    """Build statutory charge rows, merging any scraped data over fallback."""
    scraped_statutory = None
    if zerodha_html:
        scraped_statutory = _scrape_statutory_from_zerodha(zerodha_html)

    rows: list[dict] = []
    for seg, rates in _FALLBACK_STATUTORY.items():
        # If we scraped data for this segment, merge it
        if scraped_statutory and seg in scraped_statutory:
            merged = dict(rates)
            merged.update(scraped_statutory[seg])
            rates = merged

        for exchange, txn_key in [("nse", "nse_txn"), ("bse", "bse_txn"), ("mcx", "mcx_txn")]:
            txn = rates.get(txn_key, 0)
            if txn == 0 and exchange not in ("nse", "bse"):
                continue
            rows.append({
                "segment": seg,
                "exchange": exchange,
                "stt_buy_rate": rates.get("stt_buy", 0),
                "stt_sell_rate": rates.get("stt_sell", 0),
                "exchange_txn_rate": txn,
                "stamp_duty_buy_rate": rates.get("stamp", 0),
                "ipft_rate": rates.get("ipft_nse", 0) if exchange == "nse" else 0,
                "sebi_fee_rate": 0.000001,
                "gst_rate": 0.18,
            })

    return rows


async def run_broker_charges_job() -> dict:
    """Scrape broker pricing pages and refresh DB.

    Scrapes each broker's official pricing page, extracts brokerage
    rates per segment, DP charges, and AMC. Falls back to hardcoded
    reference data for any broker or field that can't be scraped.
    """
    import asyncio

    from app.core.database import get_pool

    # Run scrapers in thread pool (they use blocking HTTP)
    loop = asyncio.get_running_loop()
    broker_rows, broker_meta = await loop.run_in_executor(None, _build_broker_data)

    # Fetch zerodha HTML for statutory rates (may already be cached by _build_broker_data,
    # but we re-fetch to keep the code decoupled)
    zerodha_html = await loop.run_in_executor(
        None, _fetch_html, _BROKER_URLS["zerodha"],
    )
    statutory_rows = await loop.run_in_executor(
        None, _build_statutory_data, zerodha_html,
    )

    pool = await get_pool()
    now = datetime.now(timezone.utc)

    # Upsert broker charges
    broker_count = 0
    for row in broker_rows:
        meta = broker_meta.get(row["broker"], {})
        await pool.execute(
            """
            INSERT INTO broker_charges
                (broker, segment, brokerage_mode, brokerage_pct, brokerage_cap,
                 brokerage_flat, min_charge, dp_charge, dp_includes_gst,
                 tagline, amc_yearly, account_opening_fee, call_trade_fee,
                 source_url, scraped_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (broker, segment) DO UPDATE SET
                brokerage_mode = EXCLUDED.brokerage_mode,
                brokerage_pct = EXCLUDED.brokerage_pct,
                brokerage_cap = EXCLUDED.brokerage_cap,
                brokerage_flat = EXCLUDED.brokerage_flat,
                min_charge = EXCLUDED.min_charge,
                dp_charge = EXCLUDED.dp_charge,
                dp_includes_gst = EXCLUDED.dp_includes_gst,
                tagline = EXCLUDED.tagline,
                amc_yearly = EXCLUDED.amc_yearly,
                account_opening_fee = EXCLUDED.account_opening_fee,
                call_trade_fee = EXCLUDED.call_trade_fee,
                source_url = EXCLUDED.source_url,
                scraped_at = EXCLUDED.scraped_at
            """,
            row["broker"], row["segment"], row["brokerage_mode"],
            row.get("brokerage_pct", 0), row.get("brokerage_cap", 0),
            row.get("brokerage_flat", 0), row.get("min_charge", 0),
            meta.get("dp_charge", 0), meta.get("dp_includes_gst", False),
            meta.get("tagline", ""), meta.get("amc_yearly", 0),
            meta.get("account_opening_fee", 0), meta.get("call_trade_fee", 0),
            row.get("source_url", ""), now,
        )
        broker_count += 1

    # Upsert statutory charges
    stat_count = 0
    for row in statutory_rows:
        await pool.execute(
            """
            INSERT INTO statutory_charges
                (segment, exchange, stt_buy_rate, stt_sell_rate,
                 exchange_txn_rate, stamp_duty_buy_rate, ipft_rate,
                 sebi_fee_rate, gst_rate, scraped_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (segment, exchange) DO UPDATE SET
                stt_buy_rate = EXCLUDED.stt_buy_rate,
                stt_sell_rate = EXCLUDED.stt_sell_rate,
                exchange_txn_rate = EXCLUDED.exchange_txn_rate,
                stamp_duty_buy_rate = EXCLUDED.stamp_duty_buy_rate,
                ipft_rate = EXCLUDED.ipft_rate,
                sebi_fee_rate = EXCLUDED.sebi_fee_rate,
                gst_rate = EXCLUDED.gst_rate,
                scraped_at = EXCLUDED.scraped_at
            """,
            row["segment"], row["exchange"],
            row["stt_buy_rate"], row["stt_sell_rate"],
            row["exchange_txn_rate"], row["stamp_duty_buy_rate"],
            row["ipft_rate"], row["sebi_fee_rate"], row["gst_rate"], now,
        )
        stat_count += 1

    logger.info(
        "Broker charges job: %d broker rows, %d statutory rows upserted",
        broker_count, stat_count,
    )
    return {"brokers": broker_count, "statutory": stat_count}
