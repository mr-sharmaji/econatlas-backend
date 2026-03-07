#!/usr/bin/env python3
"""Test data sources for Nifty Smallcap 250 before backfill. Run: python scripts/test_smallcap250_sources.py"""

from __future__ import annotations

import json
import sys
from datetime import UTC, date, datetime

import requests

# Same constants as backfill
NSE_BASE_URL = "https://www.nseindia.com"
NSE_INDEX_HISTORY_PATH = "api/historical/indicesHistory"
NSE_INDEX_NAME = "NIFTY SMALLCAP 250"
NIFTYINDICES_URL = "https://niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString"
YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/HDFCSML250.NS"


def test_nse() -> tuple[bool, str]:
    """NSE indicesHistory: expect 404 (known broken)."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
        "Referer": f"{NSE_BASE_URL}/",
    })
    try:
        session.get(f"{NSE_BASE_URL}/", timeout=10)
        r = session.get(
            f"{NSE_BASE_URL}/{NSE_INDEX_HISTORY_PATH}",
            params={"indexType": NSE_INDEX_NAME, "from": "01-01-2024", "to": "31-01-2024"},
            timeout=15,
        )
        if r.status_code == 404:
            return True, "NSE returns 404 (expected); backfill will skip."
        if r.status_code == 200:
            data = r.json()
            recs = (data or {}).get("data", {}).get("indexCloseOnlineRecords") or []
            return True, f"NSE returned 200 with {len(recs)} records (unexpected; would work)."
        return False, f"NSE returned {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"NSE request failed: {e}"


def test_niftyindices_post() -> tuple[bool, str]:
    """niftyindices.com POST (jugaad-data endpoint)."""
    try:
        r = requests.post(
            NIFTYINDICES_URL,
            json={"name": NSE_INDEX_NAME, "startDate": "01-Jan-2024", "endDate": "31-Jan-2024"},
            headers={
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/80.0",
                "Referer": "https://niftyindices.com/",
            },
            timeout=15,
        )
        if r.status_code != 200:
            return True, f"niftyindices POST returned {r.status_code} (optional; jugaad may still work locally)."
        body = r.json()
        if "d" in body:
            parsed = json.loads(body["d"]) if isinstance(body["d"], str) else body["d"]
            n = len(parsed) if isinstance(parsed, list) else 0
            return True, f"niftyindices returned 200 with {n} rows (jugaad_data fallback would work)."
        return True, f"niftyindices 200 but unexpected body: {list(body.keys())}"
    except Exception as e:
        return True, f"niftyindices request failed (optional): {e}"


def test_yahoo_etf() -> tuple[bool, str]:
    """Yahoo chart for HDFCSML250.NS (ETF): must return multiple points for backfill."""
    try:
        start = int(datetime(2024, 1, 1, tzinfo=UTC).timestamp())
        end = int(datetime(2024, 1, 31, tzinfo=UTC).timestamp())
        r = requests.get(
            YAHOO_CHART_URL,
            params={"period1": start, "period2": end, "interval": "1d", "events": "history"},
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0"},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        result = (data.get("chart") or {}).get("result") or []
        if not result:
            return False, "Yahoo ETF: chart.result empty"
        block = result[0]
        ts = block.get("timestamp") or []
        quote = ((block.get("indicators") or {}).get("quote") or [{}])[0]
        closes = (quote.get("close") or [])
        points = sum(1 for i, c in enumerate(closes) if i < len(ts) and c is not None and c > 0)
        if points >= 5:
            return True, f"Yahoo ETF: {points} points in Jan 2024 (ETF fallback will work)."
        return False, f"Yahoo ETF: only {points} points; need at least 5 for fallback."
    except requests.RequestException as e:
        return False, f"Yahoo ETF request failed: {e}"
    except Exception as e:
        return False, f"Yahoo ETF error: {e}"


def test_jugaad_data() -> tuple[bool, str]:
    """Optional: jugaad_data.nse.index_df for NIFTY SMALLCAP 250."""
    try:
        from jugaad_data.nse import index_df
    except ImportError:
        return True, "jugaad_data not installed (optional); backfill will use ETF if needed."
    try:
        df = index_df(NSE_INDEX_NAME, from_date=date(2024, 1, 1), to_date=date(2024, 1, 15))
        if df is None or df.empty:
            return True, "jugaad_data returned empty (optional)."
        date_col = "HistoricalDate" if "HistoricalDate" in df.columns else "DATE"
        if date_col not in df.columns or "CLOSE" not in df.columns:
            return True, f"jugaad_data columns: {list(df.columns)} (unexpected format)."
        n = len(df)
        return True, f"jugaad_data: {n} rows for Jan 1–15, 2024 (jugaad fallback would work)."
    except Exception as e:
        return True, f"jugaad_data index_df failed (optional): {e}"


def main() -> int:
    print("Testing Nifty Smallcap 250 data sources (used by backfill)...\n")
    results = []
    results.append(("NSE indicesHistory", test_nse()))
    results.append(("niftyindices.com POST", test_niftyindices_post()))
    results.append(("Yahoo ETF (HDFCSML250.NS)", test_yahoo_etf()))
    results.append(("jugaad_data index_df", test_jugaad_data()))

    ok = True
    for name, (passed, msg) in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: [{status}] {msg}")
        if not passed:
            ok = False
    print()
    if ok:
        print("All checks passed (or optional skipped). Safe to run backfill.")
        return 0
    print("At least one required check failed. Fix before relying on Smallcap 250 backfill.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
