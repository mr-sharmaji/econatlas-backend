"""Trading-day checks and trading-date assignment using exchange calendars (NSE, NYSE).
Used so we store prices under the exchange's trading date, not server UTC date (avoids
e.g. Monday's US close being stored as Tuesday when server is already in Tuesday UTC)."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

# Lazy-loaded calendars (avoid import cost at module load)
_nse_calendar = None
_nyse_calendar = None

# Exchange identifier for get_trading_date
NSE = "NSE"
NYSE = "NYSE"


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


def get_trading_date(utc_now: datetime, exchange: str) -> date:
    """Return the exchange trading date that the 'last price' belongs to (for timestamp assignment).
    When the exchange's local date is a session day we use it; when it is not (weekend/holiday) we use
    the previous session. Note: before today's session open we still return today if today is a session
    (last price is then from yesterday); that edge case is a small window and acceptable."""
    if exchange == NSE:
        cal = _get_nse()
        tz = ZoneInfo("Asia/Kolkata")
    elif exchange == NYSE:
        cal = _get_nyse()
        tz = ZoneInfo("America/New_York")
    else:
        # Fallback: use UTC date
        return utc_now.date()
    if cal is None:
        # No calendar: use local exchange date (best-effort)
        return utc_now.astimezone(tz).date()
    local_date = utc_now.astimezone(tz).date()
    if cal.is_session(local_date):
        return local_date
    try:
        # date_to_session(..., "previous") gives the session on or before this date
        session_ts = cal.date_to_session(local_date, "previous")
        return session_ts.date() if hasattr(session_ts, "date") else date(session_ts.year, session_ts.month, session_ts.day)
    except Exception:
        # Fallback: return exchange local date (no calendar; may be wrong on weekends)
        return local_date
