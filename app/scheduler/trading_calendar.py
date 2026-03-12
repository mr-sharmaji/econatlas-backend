"""Trading-day checks and trading-date assignment using exchange calendars (NSE, NYSE).
Used so we store prices under the exchange's trading date, not server UTC date (avoids
e.g. Monday's US close being stored as Tuesday when server is already in Tuesday UTC)."""
from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# In-memory cache for get_market_status (avoids repeated calendar lookups; status only changes at session boundaries).
_status_cache: dict | None = None
_status_cache_until: float = 0.0
_india_session_cache: dict | None = None
_india_session_cache_until: float = 0.0

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
_NSE_BASE_URL = "https://www.nseindia.com"
_GOOGLE_FINANCE_QUOTE_URL = "https://www.google.com/finance/quote/{code}"
_GOOGLE_INDIA_SYMBOLS: tuple[tuple[str, str], ...] = (
    ("NIFTY_50:INDEXNSE", '"NIFTY_50","INDEXNSE"'),
    ("SENSEX:INDEXBOM", '"SENSEX","INDEXBOM"'),
)
_INDIA_DEFAULT_WINDOW = (dtime(9, 15), dtime(15, 30))
_http = requests.Session()


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
    india = get_india_session_info(utc_now)
    if bool(india.get("is_trading_day")):
        return True
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


def _india_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{_NSE_BASE_URL}/market-data/live-market-indices",
    }


def _parse_nse_trade_date(value: object | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%d-%b-%Y %H:%M", "%d-%b-%Y"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=_NSE_TZ)
        except ValueError:
            continue
    return None


def _window_label(windows: list[tuple[dtime, dtime]]) -> str | None:
    if not windows:
        return None
    return ", ".join(f"{s.strftime('%H:%M')}-{e.strftime('%H:%M')}" for s, e in windows) + " IST"


def _extract_windows_from_message(message: str | None) -> list[tuple[dtime, dtime]]:
    if not message:
        return []
    # Handles common NSE phrasing variants:
    # "09:15 to 15:30", "18:00 - 19:00", "18:00–19:00".
    matches = re.findall(r"(\d{1,2}:\d{2})\s*(?:to|-|–|—)\s*(\d{1,2}:\d{2})", message)
    windows: list[tuple[dtime, dtime]] = []
    for start_raw, end_raw in matches:
        start = _parse_hhmm(start_raw, dtime(0, 0))
        end = _parse_hhmm(end_raw, dtime(0, 0))
        if start == end:
            continue
        windows.append((start, end))
    return windows


def _next_transition_utc(
    now_utc: datetime,
    *,
    ist_date: date,
    windows: list[tuple[dtime, dtime]],
) -> datetime | None:
    if not windows:
        return None
    now = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    local_now = now.astimezone(_NSE_TZ)
    candidates: list[datetime] = []
    for start, end in windows:
        start_local = datetime.combine(ist_date, start, tzinfo=_NSE_TZ)
        end_local = datetime.combine(ist_date, end, tzinfo=_NSE_TZ)
        if end <= start:
            end_local += timedelta(days=1)
        if local_now < start_local:
            candidates.append(start_local)
        if local_now < end_local:
            candidates.append(end_local)
    if not candidates:
        return None
    return min(candidates).astimezone(timezone.utc)


def _fetch_india_session_from_nse(now_utc: datetime) -> dict | None:
    settings = get_settings()
    timeout_seconds = max(2, int(settings.india_session_timeout_seconds))
    headers = _india_headers()
    try:
        # Warmup call for cookie/session consistency on NSE.
        _http.get(_NSE_BASE_URL, headers=headers, timeout=timeout_seconds)
        response = _http.get(settings.india_session_primary_url, headers=headers, timeout=timeout_seconds)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        logger.debug("India NSE session fetch failed: %s", exc)
        return None

    rows = payload.get("marketState") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return None
    cap = None
    for row in rows:
        market = str((row or {}).get("market") or "").strip().lower()
        if market in {"capital market", "capitalmarket", "capital"}:
            cap = row
            break
    if not isinstance(cap, dict):
        return None

    now = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    local_now = now.astimezone(_NSE_TZ)
    today_ist = local_now.date()
    status = str(cap.get("marketStatus") or "").strip().lower()
    is_open = status == "open"
    trade_dt = _parse_nse_trade_date(cap.get("tradeDate"))
    message = str(cap.get("marketStatusMessage") or "").strip() or None

    windows = _extract_windows_from_message(message)
    is_trading_day = bool((trade_dt is not None and trade_dt.date() == today_ist) or is_open or windows)

    return {
        "source": "nse_api",
        "is_open": is_open,
        "windows": windows,
        "window_source": "nse_api" if windows else None,
        "ist_date": today_ist,
        "is_trading_day": is_trading_day,
        "trade_date": trade_dt.date() if trade_dt is not None else None,
        "fallback_reason": None,
        "next_transition_utc": _next_transition_utc(now, ist_date=today_ist, windows=windows),
        "window_label": _window_label(windows),
        "message": message,
    }


def _extract_google_window(
    html_text: str,
    token: str,
    *,
    ist_date: date,
) -> tuple[dtime, dtime] | None:
    pattern = re.compile(
        rf"\[{re.escape(token)}\][\s\S]{{0,1800}}?\[\[1,\[(\d{{4}}),(\d{{1,2}}),(\d{{1,2}}),(\d{{1,2}}),(\d{{1,2}})[\s\S]{{0,120}}?\],\[(\d{{4}}),(\d{{1,2}}),(\d{{1,2}}),(\d{{1,2}}),(\d{{1,2}})",
        re.IGNORECASE,
    )
    m = pattern.search(html_text)
    if not m:
        return None
    try:
        y1, mo1, d1, h1, mi1, y2, mo2, d2, h2, mi2 = [int(v) for v in m.groups()]
        d_start = date(y1, mo1, d1)
        d_end = date(y2, mo2, d2)
        if d_start != ist_date and d_end != ist_date:
            return None
        return dtime(h1, mi1), dtime(h2, mi2)
    except (TypeError, ValueError):
        return None


def _fetch_india_session_from_google(now_utc: datetime) -> dict | None:
    timeout_seconds = max(2, int(get_settings().india_session_timeout_seconds))
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml"}
    now = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    local_now = now.astimezone(_NSE_TZ)
    today_ist = local_now.date()

    windows: list[tuple[dtime, dtime]] = []
    reasons: list[str] = []
    for code, token in _GOOGLE_INDIA_SYMBOLS:
        try:
            html_text = _http.get(
                _GOOGLE_FINANCE_QUOTE_URL.format(code=code),
                headers=headers,
                timeout=timeout_seconds,
            ).text
            win = _extract_google_window(html_text, token, ist_date=today_ist)
            if win is not None:
                windows.append(win)
                break
            reasons.append(f"window_not_found:{code}")
        except Exception as exc:
            reasons.append(f"fetch_failed:{code}:{type(exc).__name__}")

    if not windows:
        logger.debug("India Google session window fetch failed: %s", ";".join(reasons))
        return None

    is_open = any(_in_window(local_now.timetz().replace(tzinfo=None), start, end) for start, end in windows)
    return {
        "source": "google_quote",
        "is_open": is_open,
        "windows": windows,
        "window_source": "google_quote",
        "ist_date": today_ist,
        "is_trading_day": True,
        "trade_date": today_ist,
        "fallback_reason": None,
        "next_transition_utc": _next_transition_utc(now, ist_date=today_ist, windows=windows),
        "window_label": _window_label(windows),
        "message": None,
    }


def _india_session_from_xbom(now_utc: datetime, reason: str | None = None) -> dict:
    now = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=timezone.utc)
    local_now = now.astimezone(_NSE_TZ)
    today_ist = local_now.date()
    cal = _get_nse()
    windows: list[tuple[dtime, dtime]] = []
    is_open = False
    is_trading_day = False
    if cal is not None and cal.is_session(today_ist):
        is_trading_day = True
        try:
            open_dt = _to_utc(cal.session_open(today_ist)).astimezone(_NSE_TZ)
            close_dt = _to_utc(cal.session_close(today_ist)).astimezone(_NSE_TZ)
            start = open_dt.timetz().replace(tzinfo=None)
            end = close_dt.timetz().replace(tzinfo=None)
            windows = [(start, end)]
            is_open = open_dt <= local_now < close_dt
        except Exception:
            windows = [_INDIA_DEFAULT_WINDOW]
            is_open = _in_window(local_now.timetz().replace(tzinfo=None), *_INDIA_DEFAULT_WINDOW)
    return {
        "source": "xbom_fallback",
        "is_open": is_open,
        "windows": windows,
        "window_source": "xbom_fallback" if windows else None,
        "ist_date": today_ist,
        "is_trading_day": is_trading_day,
        "trade_date": today_ist if is_trading_day else None,
        "fallback_reason": reason or "primary_and_secondary_unavailable",
        "next_transition_utc": _next_transition_utc(now, ist_date=today_ist, windows=windows),
        "window_label": _window_label(windows),
        "message": None,
    }


def get_india_session_info(utc_now: datetime | None = None) -> dict:
    global _india_session_cache, _india_session_cache_until
    now = utc_now if utc_now is not None else datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    cache_ttl = max(0, int(get_settings().india_session_cache_seconds))
    if cache_ttl > 0 and _india_session_cache is not None and time.monotonic() < _india_session_cache_until:
        return _india_session_cache

    settings = get_settings()
    info: dict
    if not settings.india_session_auto_enabled:
        info = _india_session_from_xbom(now, reason="india_session_auto_disabled")
    else:
        nse_info = _fetch_india_session_from_nse(now)
        if nse_info is not None:
            info = dict(nse_info)
            if not info.get("windows"):
                google_info = _fetch_india_session_from_google(now)
                if google_info and google_info.get("windows"):
                    info["windows"] = list(google_info.get("windows") or [])
                    info["window_source"] = "google_quote"
                    info["window_label"] = _window_label(info["windows"])
                    info["next_transition_utc"] = _next_transition_utc(
                        now,
                        ist_date=info["ist_date"],
                        windows=info["windows"],
                    )
                    if not info.get("is_trading_day"):
                        info["is_trading_day"] = bool(google_info.get("is_trading_day"))
                    info["fallback_reason"] = "nse_window_unavailable"
        else:
            google_info = _fetch_india_session_from_google(now)
            if google_info is not None:
                info = dict(google_info)
                info["fallback_reason"] = "nse_primary_unavailable"
            else:
                info = _india_session_from_xbom(now)

    logger.info(
        "India session resolved: source=%s open=%s window=%s fallback_reason=%s",
        info.get("source"),
        info.get("is_open"),
        info.get("window_label"),
        info.get("fallback_reason"),
    )

    if cache_ttl > 0:
        ttl_seconds = cache_ttl
        next_transition = info.get("next_transition_utc")
        if isinstance(next_transition, datetime):
            secs_to_transition = (next_transition - now).total_seconds()
            if secs_to_transition > 0:
                ttl_seconds = min(ttl_seconds, max(1, int(secs_to_transition)))
        _india_session_cache = info
        _india_session_cache_until = time.monotonic() + ttl_seconds
    return info


def get_india_session_diagnostics(utc_now: datetime | None = None) -> dict:
    info = get_india_session_info(utc_now)
    return {
        "source": info.get("source"),
        "window": info.get("window_label"),
        "window_source": info.get("window_source"),
        "fallback_reason": info.get("fallback_reason"),
    }


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
        return start <= t < end
    # Cross-midnight window (e.g., 16:35 -> 02:45)
    return t >= start or t < end


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
        if end < start and t < end:
            return True
    if d in specials or prev_d in specials:
        return False

    s1_open, s1_close, s2_open, s2_close = _gift_default_sessions()
    wd = local.weekday()  # Mon=0 ... Sun=6

    # Session 1: weekday daytime
    if wd <= 4 and s1_open <= t < s1_close:
        return True
    # Session 2 evening: Mon-Fri and Sunday
    if wd in {0, 1, 2, 3, 4, 6} and t >= s2_open:
        return True
    # Session 2 after midnight continuation: Tue-Sat
    if wd in {1, 2, 3, 4, 5} and t < s2_close:
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
        india = get_india_session_info(now)
        local = now.astimezone(_NSE_TZ)
        local_date = local.date()
        local_time = local.timetz().replace(tzinfo=None)
        if bool(india.get("is_trading_day")) and india.get("ist_date") == local_date:
            windows = list(india.get("windows") or [])
            if bool(india.get("is_open")):
                return local_date
            if windows:
                earliest = min(start for start, _end in windows)
                if local_time >= earliest:
                    return local_date
            else:
                if local_time >= _INDIA_DEFAULT_WINDOW[0]:
                    return local_date
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
            return open_dt <= now < close_dt
        except Exception:
            return _fallback_is_open(exchange, now)

    india = get_india_session_info(now)
    nyse = _get_nyse()
    nyse_date = now.astimezone(_NYSE_TZ).date()

    nse_open = bool(india.get("is_open"))
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
        "india_session_source": india.get("source"),
        "india_session_window": india.get("window_label"),
        "india_session_fallback_reason": india.get("fallback_reason"),
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
        cache_seconds = ttl
        next_transition = india.get("next_transition_utc")
        if isinstance(next_transition, datetime):
            secs_to_transition = (next_transition - now).total_seconds()
            if secs_to_transition > 0:
                cache_seconds = min(cache_seconds, max(1, int(secs_to_transition)))
        _status_cache = result
        _status_cache_until = time.monotonic() + cache_seconds
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
    if not (start <= t < end):
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
