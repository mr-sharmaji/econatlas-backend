"""Trading-day checks and trading-date assignment using exchange calendars (NSE, NYSE).
Used so we store prices under the exchange's trading date, not server UTC date (avoids
e.g. Monday's US close being stored as Tuesday when server is already in Tuesday UTC)."""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# In-memory cache for get_market_status (avoids repeated calendar lookups; status only changes at session boundaries).
_status_cache: dict | None = None
_status_cache_until: float = 0.0

# Lazy-loaded calendars (avoid import cost at module load)
_nse_calendar = None
_nyse_calendar = None

# Exchange identifier for get_trading_date
NSE = "NSE"
NYSE = "NYSE"

# exchange_calendars canonical names: XBOM = BSE (India, same tz/session as NSE); XNYS = NYSE (US).
# Package has no XNSE; XBOM is the India exchange calendar in gerrymanoim/exchange_calendars.
_NSE_CALENDAR_NAME = "XBOM"
_NYSE_CALENDAR_NAME = "XNYS"

_NSE_TZ = ZoneInfo("Asia/Kolkata")
_NYSE_TZ = ZoneInfo("America/New_York")


def _get_nse():
    global _nse_calendar
    if _nse_calendar is None:
        try:
            import exchange_calendars as xcals
            _nse_calendar = xcals.get_calendar(_NSE_CALENDAR_NAME)
        except Exception as e:
            logger.warning("NSE calendar (%s) unavailable: %s. Using weekday/date fallback for India.", _NSE_CALENDAR_NAME, e)
    return _nse_calendar


def _get_nyse():
    global _nyse_calendar
    if _nyse_calendar is None:
        try:
            import exchange_calendars as xcals
            _nyse_calendar = xcals.get_calendar(_NYSE_CALENDAR_NAME)
        except Exception as e:
            logger.warning("NYSE calendar (%s) unavailable: %s. Using weekday/date fallback for US.", _NYSE_CALENDAR_NAME, e)
    return _nyse_calendar


def is_trading_day_markets(utc_now: datetime) -> bool:
    """True if 'today' is a trading day in NSE (India) or NYSE (US). Use for indices, FX, bonds."""
    nse = _get_nse()
    nyse = _get_nyse()
    if nse is None and nyse is None:
        return utc_now.weekday() < 5
    nse_date = utc_now.astimezone(_NSE_TZ).date()
    nyse_date = utc_now.astimezone(_NYSE_TZ).date()
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
    nyse_date = utc_now.astimezone(_NYSE_TZ).date()
    return nyse.is_session(nyse_date)


def _to_utc(dt_like) -> datetime:
    dt = dt_like.to_pydatetime() if hasattr(dt_like, "to_pydatetime") else dt_like
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _session_to_date(session_like) -> date:
    if hasattr(session_like, "date"):
        return session_like.date()
    return date(session_like.year, session_like.month, session_like.day)


def _previous_session_date(cal, session_date: date) -> date:
    try:
        return _session_to_date(cal.previous_session(session_date))
    except Exception:
        try:
            prev = cal.date_to_session(session_date, "previous")
            prev_date = _session_to_date(prev)
            if prev_date == session_date:
                return _session_to_date(cal.previous_session(prev))
            return prev_date
        except Exception:
            return session_date


def _fallback_is_open(exchange: str, utc_now: datetime) -> bool:
    # No fixed exchange hours fallback: if calendar is unavailable, do not claim "open".
    return False


def _fallback_trading_date(exchange: str, utc_now: datetime) -> date:
    if exchange == NSE:
        local = utc_now.astimezone(_NSE_TZ)
    else:
        local = utc_now.astimezone(_NYSE_TZ)
    d = local.date()
    if local.weekday() >= 5:
        d = d.fromordinal(d.toordinal() - 1)
        while d.weekday() >= 5:
            d = d.fromordinal(d.toordinal() - 1)
    return d


def get_trading_date(utc_now: datetime, exchange: str) -> date:
    """Return the exchange trading date that the 'last price' belongs to (for timestamp assignment).
    When the exchange's local date is a session day we use it; when it is not (weekend/holiday) we use
    the previous session. Before session open on a trading day, this also returns the previous session date."""
    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)

    if exchange == NSE:
        cal = _get_nse()
        tz = _NSE_TZ
    elif exchange == NYSE:
        cal = _get_nyse()
        tz = _NYSE_TZ
    else:
        # Fallback: use UTC date
        return now.date()
    if cal is None:
        return _fallback_trading_date(exchange, now)

    local_date = now.astimezone(tz).date()
    if cal.is_session(local_date):
        try:
            session_open = _to_utc(cal.session_open(local_date))
            # Before local open, keep writing to previous session day.
            if now < session_open:
                return _previous_session_date(cal, local_date)
            return local_date
        except Exception:
            return local_date
    try:
        # date_to_session(..., "previous") gives the session on or before this date
        session_ts = cal.date_to_session(local_date, "previous")
        return _session_to_date(session_ts)
    except Exception:
        return _fallback_trading_date(exchange, now)


def get_market_status(utc_now: datetime | None = None) -> dict:
    """Return whether NSE and NYSE are currently in a trading session (market 'live').
    Returns e.g. {"nse_open": bool, "nyse_open": bool, "live": bool}.
    Result is cached for market_status_cache_seconds to reduce calendar lookups."""
    global _status_cache, _status_cache_until
    ttl = get_settings().market_status_cache_seconds
    if ttl > 0 and _status_cache is not None and time.monotonic() < _status_cache_until:
        return _status_cache

    now = utc_now if utc_now is not None else datetime.now(timezone.utc)
    # Ensure timezone-aware for comparison with exchange_calendars timestamps
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    def _is_open(cal, local_date, exchange: str) -> bool:
        if cal is None:
            return _fallback_is_open(exchange, now)
        if not cal.is_session(local_date):
            return False
        try:
            open_ts = cal.session_open(local_date)
            close_ts = cal.session_close(local_date)
            # exchange_calendars returns UTC timestamps; compare with now
            open_dt = _to_utc(open_ts)
            close_dt = _to_utc(close_ts)
            return open_dt <= now <= close_dt
        except Exception:
            return _fallback_is_open(exchange, now)

    nse = _get_nse()
    nyse = _get_nyse()
    nse_date = now.astimezone(_NSE_TZ).date()
    nyse_date = now.astimezone(_NYSE_TZ).date()

    nse_open = _is_open(nse, nse_date, NSE)
    nyse_open = _is_open(nyse, nyse_date, NYSE)
    result = {"nse_open": nse_open, "nyse_open": nyse_open, "live": nse_open or nyse_open}
    if ttl > 0:
        _status_cache = result
        _status_cache_until = time.monotonic() + ttl
    return result
