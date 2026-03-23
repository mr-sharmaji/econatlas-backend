"""Economic calendar — scraped from official central bank websites.

Fetches upcoming rate decision dates from Fed, RBI, ECB, BoJ
official schedule pages.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from datetime import date, datetime, timezone
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from app.scheduler.job_executors import get_job_executor
from app.services import macro_service

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
}

_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _scrape_fed_fomc() -> List[Dict]:
    """Scrape FOMC meeting dates from federalreserve.gov."""
    events: List[Dict] = []
    try:
        resp = requests.get(
            "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            headers=_HEADERS, timeout=20,
        )
        if resp.status_code != 200:
            logger.warning("Fed calendar HTTP %d", resp.status_code)
            return events

        soup = BeautifulSoup(resp.text, "html.parser")
        panels = soup.find_all("div", class_="fomc-meeting")
        if not panels:
            panels = soup.find_all("div", class_="panel")

        for panel in panels:
            text = panel.get_text()
            # Match patterns like "January 28-29" or "March 18-19, 2025"
            m = re.search(
                r"(January|February|March|April|May|June|July|August|"
                r"September|October|November|December)\s+(\d{1,2})"
                r"(?:\s*[-–]\s*(\d{1,2}))?\s*,?\s*(\d{4})?",
                text,
            )
            if not m:
                continue

            month_name = m.group(1).lower()
            # Use the last day of the meeting (when decision is announced)
            day = int(m.group(3) or m.group(2))
            year_str = m.group(4)

            # Year might be in a parent heading
            if not year_str:
                parent = panel.find_parent("div", class_="panel")
                if parent:
                    heading = parent.find(class_="panel-heading")
                    if heading:
                        ym = re.search(r"(\d{4})", heading.get_text())
                        if ym:
                            year_str = ym.group(1)
            if not year_str:
                continue

            month = _MONTH_MAP.get(month_name)
            year = int(year_str)
            if month and 2024 <= year <= 2028:
                event_date = f"{year}-{month:02d}-{day:02d}"
                events.append({
                    "event_name": "US Fed FOMC Decision",
                    "institution": "Fed",
                    "event_date": event_date,
                    "country": "US",
                    "event_type": "rate_decision",
                    "description": "Federal Reserve interest rate decision",
                    "source": "federalreserve.gov",
                })

        logger.info("Fed FOMC calendar: %d meetings scraped", len(events))
    except Exception:
        logger.exception("Fed FOMC scrape failed")
    return events


def _scrape_rbi_mpc() -> List[Dict]:
    """Scrape RBI MPC dates from rbi.org.in."""
    events: List[Dict] = []
    try:
        resp = requests.get(
            "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid=57560",
            headers=_HEADERS, timeout=20,
        )
        if resp.status_code != 200:
            # Fallback: try TE for RBI info
            logger.warning("RBI calendar HTTP %d, using Trading Economics fallback", resp.status_code)
            return _scrape_te_calendar_for("india/interest-rate", "RBI MPC Decision", "RBI", "IN")

        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text()

        # RBI publishes MPC dates in press releases
        # Look for patterns like "April 7 to 9, 2025" or "February 5-7, 2026"
        for m in re.finditer(
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{1,2})\s*"
            r"(?:to|[-–])\s*(\d{1,2})\s*,?\s*(\d{4})",
            text,
        ):
            month_name = m.group(1).lower()
            last_day = int(m.group(3))
            year = int(m.group(4))
            month = _MONTH_MAP.get(month_name)
            if month and 2024 <= year <= 2028:
                event_date = f"{year}-{month:02d}-{last_day:02d}"
                events.append({
                    "event_name": "RBI MPC Decision",
                    "institution": "RBI",
                    "event_date": event_date,
                    "country": "IN",
                    "event_type": "rate_decision",
                    "description": "RBI Monetary Policy Committee rate decision",
                    "source": "rbi.org.in",
                })

        logger.info("RBI MPC calendar: %d meetings scraped", len(events))
    except Exception:
        logger.exception("RBI MPC scrape failed")
    return events


def _scrape_te_calendar_for(
    path: str, event_name: str, institution: str, country: str,
) -> List[Dict]:
    """Try to extract calendar info from TE's description text."""
    events: List[Dict] = []
    try:
        resp = requests.get(
            f"https://tradingeconomics.com/{path}",
            headers=_HEADERS, timeout=20,
        )
        if resp.status_code != 200:
            return events

        soup = BeautifulSoup(resp.text, "html.parser")
        desc = soup.find(id="historical-desc")
        if not desc:
            return events

        text = desc.get_text()
        # Extract "next meeting" or "next decision" date mentions
        m = re.search(
            r"(?:next|upcoming|scheduled)\s+(?:meeting|decision)\s+.*?"
            r"(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{1,2})\s*,?\s*(\d{4})?",
            text, re.IGNORECASE,
        )
        if m:
            month = _MONTH_MAP.get(m.group(1).lower())
            day = int(m.group(2))
            year = int(m.group(3)) if m.group(3) else datetime.now().year
            if month:
                events.append({
                    "event_name": event_name,
                    "institution": institution,
                    "event_date": f"{year}-{month:02d}-{day:02d}",
                    "country": country,
                    "event_type": "rate_decision",
                    "description": f"{institution} rate decision",
                    "source": "trading_economics",
                })
    except Exception:
        logger.exception("TE calendar fallback failed for %s", path)
    return events


def _scrape_all_calendars_sync() -> List[Dict]:
    """Fetch all calendar events from official sources."""
    events: List[Dict] = []

    events.extend(_scrape_fed_fomc())
    time.sleep(1)
    events.extend(_scrape_rbi_mpc())
    time.sleep(1)

    # ECB and BoJ — use TE description as source
    events.extend(
        _scrape_te_calendar_for("euro-area/interest-rate", "ECB Rate Decision", "ECB", "EU")
    )
    time.sleep(1)
    events.extend(
        _scrape_te_calendar_for("japan/interest-rate", "BoJ Rate Decision", "BoJ", "JP")
    )

    # Filter to relevant dates only (not too far in past)
    cutoff = date(date.today().year - 1, 1, 1)
    events = [e for e in events if date.fromisoformat(e["event_date"]) >= cutoff]

    logger.info("Calendar scraper complete: %d total events", len(events))
    return events


async def run_econ_calendar_job() -> None:
    """Scrape and persist economic calendar events."""
    try:
        loop = asyncio.get_event_loop()
        events = await loop.run_in_executor(
            get_job_executor("econ-calendar"),
            _scrape_all_calendars_sync,
        )
        if events:
            count = await macro_service.upsert_calendar_events(events)
            logger.info("Economic calendar job: %d events upserted", count)
        else:
            logger.info("Economic calendar job: no events scraped")
    except Exception:
        logger.exception("Economic calendar job failed")
