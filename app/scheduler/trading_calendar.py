"""Trading-day checks and trading-date assignment using exchange calendars (NSE, NYSE).
Used so we store prices under the exchange's trading date, not server UTC date (avoids
e.g. Monday's US close being stored as Tuesday when server is already in Tuesday UTC)."""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
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
LSE = "LSE"
XETRA = "XETRA"
EURONEXT = "EURONEXT"
TSE = "TSE"

SESSION_OPEN = "open"
SESSION_BREAK = "break"
SESSION_CLOSED = "closed"

# exchange_calendars canonical names: XBOM = BSE (India, same tz/session as NSE); XNYS = NYSE (US).
# Package has no XNSE; XBOM is the India exchange calendar in gerrymanoim/exchange_calendars.
_NSE_CALENDAR_NAME = "XBOM"
_NYSE_CALENDAR_NAME = "XNYS"

_NSE_TZ = ZoneInfo("Asia/Kolkata")
_NYSE_TZ = ZoneInfo("America/New_York")
_LSE_TZ = ZoneInfo("Europe/London")
_XETRA_TZ = ZoneInfo("Europe/Berlin")
_EURONEXT_TZ = ZoneInfo("Europe/Paris")
_TSE_TZ = ZoneInfo("Asia/Tokyo")


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
    elif exchange == LSE:
        local = utc_now.astimezone(_LSE_TZ)
    elif exchange == XETRA:
        local = utc_now.astimezone(_XETRA_TZ)
    elif exchange == EURONEXT:
        local = utc_now.astimezone(_EURONEXT_TZ)
    elif exchange == TSE:
        local = utc_now.astimezone(_TSE_TZ)
    else:
        local = utc_now.astimezone(_NYSE_TZ)
    d = local.date()
    if local.weekday() >= 5:
        d = d.fromordinal(d.toordinal() - 1)
        while d.weekday() >= 5:
            d = d.fromordinal(d.toordinal() - 1)
    return d


def _parse_hhmm(value: str, default: dtime) -> dtime:
    try:
        h, m = value.strip().split(":")
        return dtime(hour=int(h), minute=int(m))
    except Exception:
        return default


def _gift_default_sessions() -> tuple[dtime, dtime, dtime, dtime]:
    s = get_settings()
    s1_open = _parse_hhmm(getattr(s, "gift_nifty_session1_open", "06:30"), dtime(6, 30))
    s1_close = _parse_hhmm(getattr(s, "gift_nifty_session1_close", "15:40"), dtime(15, 40))
    s2_open = _parse_hhmm(getattr(s, "gift_nifty_session2_open", "16:35"), dtime(16, 35))
    s2_close = _parse_hhmm(getattr(s, "gift_nifty_session2_close", "02:45"), dtime(2, 45))
    return s1_open, s1_close, s2_open, s2_close


def _gift_special_sessions() -> dict[date, list[tuple[dtime, dtime]]]:
    raw = getattr(get_settings(), "gift_nifty_special_sessions_json", None)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception:
        logger.warning("Invalid gift_nifty_special_sessions_json; ignoring")
        return {}
    if not isinstance(parsed, dict):
        return {}
    out: dict[date, list[tuple[dtime, dtime]]] = {}
    for k, windows in parsed.items():
        try:
            d = date.fromisoformat(str(k))
        except ValueError:
            continue
        if not isinstance(windows, list):
            continue
        valid_windows = []
        for w in windows:
            if not isinstance(w, list) or len(w) != 2:
                continue
            start = _parse_hhmm(str(w[0]), dtime(0, 0))
            end = _parse_hhmm(str(w[1]), dtime(0, 0))
            valid_windows.append((start, end))
        if valid_windows:
            out[d] = valid_windows
    return out


def _in_window(t: dtime, start: dtime, end: dtime) -> bool:
    if end >= start:
        return start <= t <= end
    # Cross-midnight window (e.g., 16:35 -> 02:45)
    return t >= start or t <= end


def is_gift_nifty_open(utc_now: datetime) -> bool:
    """True when Gift Nifty session is active in IST.
    Uses configurable default sessions plus optional per-date override windows."""
    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)
    local = now.astimezone(_NSE_TZ)
    t = local.timetz().replace(tzinfo=None)
    d = local.date()

    specials = _gift_special_sessions()
    # Exact-date special windows
    for start, end in specials.get(d, []):
        if _in_window(t, start, end):
            return True
    # Previous-date cross-midnight special windows
    prev_d = d - timedelta(days=1)
    for start, end in specials.get(prev_d, []):
        if end < start and t <= end:
            return True
    if d in specials or prev_d in specials:
        return False

    s1_open, s1_close, s2_open, s2_close = _gift_default_sessions()
    wd = local.weekday()  # Mon=0 ... Sun=6

    # Session 1: weekday daytime
    if wd <= 4 and s1_open <= t <= s1_close:
        return True
    # Session 2 evening: Mon-Fri and Sunday
    if wd in {0, 1, 2, 3, 4, 6} and t >= s2_open:
        return True
    # Session 2 after midnight continuation: Tue-Sat
    if wd in {1, 2, 3, 4, 5} and t <= s2_close:
        return True
    return False


def get_gift_nifty_trading_date(utc_now: datetime) -> date:
    """Gift Nifty trading date in IST for daily upsert.
    Before session-1 open, keep attributing to the previous IST date."""
    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)
    local = now.astimezone(_NSE_TZ)
    s1_open, _s1_close, _s2_open, _s2_close = _gift_default_sessions()
    if local.timetz().replace(tzinfo=None) < s1_open:
        return local.date() - timedelta(days=1)
    return local.date()


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
    elif exchange == LSE:
        return _fallback_trading_date(exchange, now)
    elif exchange == XETRA:
        return _fallback_trading_date(exchange, now)
    elif exchange == EURONEXT:
        return _fallback_trading_date(exchange, now)
    elif exchange == TSE:
        return _fallback_trading_date(exchange, now)
    else:
        return _fallback_trading_date(exchange, now)
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


def get_recent_trading_dates(utc_now: datetime, exchange: str, count: int) -> list[date]:
    """Return up to `count` most recent trading dates for `exchange` (oldest -> newest)."""
    if count <= 0:
        return []

    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)
    latest = get_trading_date(now, exchange)

    if exchange == NSE:
        cal = _get_nse()
    elif exchange == NYSE:
        cal = _get_nyse()
    else:
        cal = None

    if cal is None:
        out = []
        d = latest
        while len(out) < count:
            if d.weekday() < 5:
                out.append(d)
            d = d - timedelta(days=1)
        out.reverse()
        return out

    dates = [latest]
    cur = latest
    while len(dates) < count:
        prev = _previous_session_date(cal, cur)
        if prev == cur:
            break
        dates.append(prev)
        cur = prev
    dates.reverse()
    return dates


def get_market_status(utc_now: datetime | None = None) -> dict:
    """Return current session status across core regions and sessions.
    Returns keys such as nse_open, nyse_open, europe_open, japan_open, fx_open, commodities_open, live.
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
    gift_nifty_open = is_gift_nifty_open(now)
    lse_open = is_exchange_expected_open(LSE, now)
    xetra_open = is_exchange_expected_open(XETRA, now)
    euronext_open = is_exchange_expected_open(EURONEXT, now)
    europe_open = lse_open or xetra_open or euronext_open
    japan_open = is_exchange_expected_open(TSE, now)
    fx_open = is_fx_session_expected_open(now)
    commodities_open = is_commodity_session_expected_open(now)
    india_open = nse_open
    us_open = nyse_open
    result = {
        "nse_open": nse_open,
        "nyse_open": nyse_open,
        "gift_nifty_open": gift_nifty_open,
        "india_open": india_open,
        "us_open": us_open,
        "europe_open": europe_open,
        "japan_open": japan_open,
        "fx_open": fx_open,
        "commodities_open": commodities_open,
        "live": (
            nse_open
            or nyse_open
            or gift_nifty_open
            or europe_open
            or japan_open
            or fx_open
            or commodities_open
        ),
    }
    if ttl > 0:
        _status_cache = result
        _status_cache_until = time.monotonic() + ttl
    return result


def is_fx_session_expected_open(utc_now: datetime) -> bool:
    """Best-effort 24x5 FX session check in New York local time.
    Open: Sun 17:00 -> Fri 17:00 (ET)."""
    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)
    ny = now.astimezone(_NYSE_TZ)
    wd = ny.weekday()  # Mon=0 ... Sun=6
    t = ny.timetz().replace(tzinfo=None)
    if wd == 5:  # Saturday
        return False
    if wd == 6:  # Sunday
        return t >= dtime(17, 0)
    if wd == 4:  # Friday
        return t < dtime(17, 0)
    return True  # Mon-Thu


def is_commodity_session_expected_open(utc_now: datetime) -> bool:
    """Best-effort commodity futures session check in New York local time."""
    return get_commodity_session_state(utc_now) == SESSION_OPEN


def get_commodity_session_state(utc_now: datetime) -> str:
    """Commodity futures state in New York local time: open | break | closed.

    Window: Sun 18:00 -> Fri 17:00 ET with daily maintenance break 17:00-18:00 ET.
    """
    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)
    ny = now.astimezone(_NYSE_TZ)
    wd = ny.weekday()  # Mon=0 ... Sun=6
    t = ny.timetz().replace(tzinfo=None)
    if wd == 5:  # Saturday
        return SESSION_CLOSED
    if wd == 6:  # Sunday
        return SESSION_OPEN if t >= dtime(18, 0) else SESSION_CLOSED
    if wd == 4:  # Friday
        return SESSION_OPEN if t < dtime(17, 0) else SESSION_CLOSED
    # Mon-Thu: open except daily maintenance break.
    if dtime(17, 0) <= t < dtime(18, 0):
        return SESSION_BREAK
    return SESSION_OPEN


def _local_window_state(
    utc_now: datetime,
    *,
    tz: ZoneInfo,
    start: dtime,
    end: dtime,
    weekdays: set[int] | None = None,
    breaks: list[tuple[dtime, dtime]] | None = None,
) -> str:
    local = utc_now.astimezone(tz)
    wd = local.weekday()
    if weekdays is not None and wd not in weekdays:
        return SESSION_CLOSED
    t = local.timetz().replace(tzinfo=None)
    if not (start <= t <= end):
        return SESSION_CLOSED
    for b_start, b_end in breaks or []:
        if b_start <= t < b_end:
            return SESSION_BREAK
    return SESSION_OPEN


def is_exchange_expected_open(exchange: str, utc_now: datetime, status: dict | None = None) -> bool:
    """Best-effort expected-open check for region-aware phase computation."""
    return get_exchange_session_state(exchange, utc_now, status=status) == SESSION_OPEN


def get_exchange_session_state(exchange: str, utc_now: datetime, status: dict | None = None) -> str:
    """Best-effort exchange state: open | break | closed."""
    now = utc_now if utc_now.tzinfo is not None else utc_now.replace(tzinfo=timezone.utc)
    if exchange == NSE:
        st = status or get_market_status(now)
        return SESSION_OPEN if bool(st.get("nse_open")) else SESSION_CLOSED
    if exchange == NYSE:
        st = status or get_market_status(now)
        return SESSION_OPEN if bool(st.get("nyse_open")) else SESSION_CLOSED
    if exchange == LSE:
        return _local_window_state(
            now,
            tz=_LSE_TZ,
            start=dtime(8, 0),
            end=dtime(16, 30),
            weekdays={0, 1, 2, 3, 4},
        )
    if exchange == XETRA:
        return _local_window_state(
            now,
            tz=_XETRA_TZ,
            start=dtime(9, 0),
            end=dtime(17, 30),
            weekdays={0, 1, 2, 3, 4},
        )
    if exchange == EURONEXT:
        return _local_window_state(
            now,
            tz=_EURONEXT_TZ,
            start=dtime(9, 0),
            end=dtime(17, 30),
            weekdays={0, 1, 2, 3, 4},
        )
    if exchange == TSE:
        return _local_window_state(
            now,
            tz=_TSE_TZ,
            start=dtime(9, 0),
            end=dtime(15, 0),
            weekdays={0, 1, 2, 3, 4},
            breaks=[(dtime(11, 30), dtime(12, 30))],
        )
    return SESSION_CLOSED
