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


def get_market_status(utc_now: datetime | None = None) -> dict:
    """Return whether NSE and NYSE are currently in a trading session (market 'live').
    Returns e.g. {"nse_open": bool, "nyse_open": bool, "live": bool}."""
    now = utc_now if utc_now is not None else datetime.now(timezone.utc)
    # Ensure timezone-aware for comparison with exchange_calendars timestamps
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    def _is_open(cal, local_date) -> bool:
        if cal is None or not cal.is_session(local_date):
            return False
        try:
            open_ts = cal.session_open(local_date)
            close_ts = cal.session_close(local_date)
            # exchange_calendars returns UTC timestamps; compare with now
            open_dt = open_ts.to_pydatetime() if hasattr(open_ts, "to_pydatetime") else open_ts
            close_dt = close_ts.to_pydatetime() if hasattr(close_ts, "to_pydatetime") else close_ts
            if open_dt.tzinfo is None:
                open_dt = open_dt.replace(tzinfo=timezone.utc)
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=timezone.utc)
            return open_dt <= now <= close_dt
        except Exception:
            return False

    nse = _get_nse()
    nyse = _get_nyse()
    nse_date = now.astimezone(ZoneInfo("Asia/Kolkata")).date()
    nyse_date = now.astimezone(ZoneInfo("America/New_York")).date()

    nse_open = _is_open(nse, nse_date)
    nyse_open = _is_open(nyse, nyse_date)
    return {"nse_open": nse_open, "nyse_open": nyse_open, "live": nse_open or nyse_open}
