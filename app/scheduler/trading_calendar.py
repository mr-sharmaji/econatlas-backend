"""Trading-day checks using exchange calendars (NSE, NYSE). Used as the main gate;
when calendar says closed the jobs still fetch and may write if price changed (calendar can be wrong)."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Lazy-loaded calendars (avoid import cost at module load)
_nse_calendar = None
_nyse_calendar = None


def _get_nse():
    global _nse_calendar
    if _nse_calendar is None:
        try:
            import exchange_calendars as xcals
            _nse_calendar = xcals.get_calendar("XNSE")
        except Exception as e:
            logger.warning("NSE calendar unavailable: %s", e)
    return _nse_calendar


def _get_nyse():
    global _nyse_calendar
    if _nyse_calendar is None:
        try:
            import exchange_calendars as xcals
            _nyse_calendar = xcals.get_calendar("XNYS")
        except Exception as e:
            logger.warning("NYSE calendar unavailable: %s", e)
    return _nyse_calendar


def is_trading_day_markets(utc_now: datetime) -> bool:
    """True if 'today' is a trading day in NSE (India) or NYSE (US). Use for indices, FX, bonds."""
    nse = _get_nse()
    nyse = _get_nyse()
    if nse is None and nyse is None:
        return utc_now.weekday() < 5
    nse_date = utc_now.astimezone(ZoneInfo("Asia/Kolkata")).date()
    nyse_date = utc_now.astimezone(ZoneInfo("America/New_York")).date()
    if nse and nse.is_session(nse_date):
        return True
    if nyse and nyse.is_session(nyse_date):
        return True
    return False


def is_trading_day_commodities(utc_now: datetime) -> bool:
    """True if 'today' is a trading day for commodity futures (US session). Use for gold, oil, etc."""
    nyse = _get_nyse()
    if nyse is None:
        return utc_now.weekday() < 5
    nyse_date = utc_now.astimezone(ZoneInfo("America/New_York")).date()
    return nyse.is_session(nyse_date)
