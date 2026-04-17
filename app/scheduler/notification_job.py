"""Background job: detect market state transitions and send push notifications."""
import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services import notification_service
from app.scheduler.trading_calendar import (
    get_india_session_info,
    get_market_status,
    is_exchange_holiday,
)

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")

# Track previous state to detect transitions
_prev_state: dict[str, bool] = {}

# Gift Nifty alert state
_gift_nifty_state: dict = {
    "last_band": None,
    "was_open": None,
}

# FII/DII alert state
_fii_dii_state: dict = {
    "last_date": None,
}

# Pre-market summary state
_pre_market_state: dict = {
    "last_date": None,
}

# Commodity spike state — track alerted assets per day
_commodity_spike_state: dict = {
    "last_date": None,
    "alerted": {},  # asset -> last alerted band (2% bands)
}

# Open notification pending state — the market transitioned to open and
# we're waiting for the scraper to land today's UTC-session row so the
# notification builder has real numbers to render. Value is the UTC
# transition time. See the flush loop in run_notification_job for the
# "fire when data lands, fall back after max wait" logic.
_open_pending: dict[str, datetime | None] = {}  # market -> transition timestamp

# Hard ceiling on how long we wait for today's row to land before firing
# a degraded simple-open notification. Under normal conditions today's
# row appears within 1–3 minutes of the scraper's first post-open tick;
# 15 minutes is a generous upper bound that still gives the user the
# "market opened" signal even if the upstream feed is seriously broken.
_OPEN_DATA_MAX_WAIT_SECONDS = 900

# Close notification pending state — on the open→closed transition we
# queue the notification and let the flush loop fire it after the daily
# market_prices row has had time to settle on the actual closing
# auction print. Without this delay the transition fires at the exact
# moment the market closes, and the daily row still holds the last
# pre-close intraday tick (insert_prices_batch_upsert_daily drifts on
# every scraper cycle), so the notification reports a value that's off
# by the close-auction move. India is excluded — its close routes
# through _check_post_market_summary which already has a 5-minute
# settle delay. Value is the UTC transition time. See the close flush
# loop in run_notification_job.
_close_pending: dict[str, datetime | None] = {}  # market -> transition timestamp

# How long to wait after the market-close transition before fetching
# data and firing the close notification. Three minutes is enough for
# yahoo_finance_api to surface the closing auction print across NYSE,
# LSE/XETRA/Euronext, and TSE in practice, without meaningfully delaying
# the user-visible notification.
_CLOSE_SETTLE_SECONDS = 180

# India-specific settle. NSE's closing auction publishes ~1–3 min after
# 15:30 IST and Yahoo's spot endpoint surfaces it a minute or two later.
# Five minutes was not enough in practice — the post-market notification
# was firing with the last pre-close intraday tick (e.g. Nifty +1.5%
# instead of the actual close +1.63%). Bumped to 10 min; the
# freshness gate in _fetch_india_close_data will also refuse to send
# until the daily row diverges from the last intraday tick, so this
# value is an upper bound on the wait, not a rigid fire time.
_POST_MARKET_SETTLE_SECONDS_INDIA = 600

# Post-market summary state
_post_market_state: dict = {
    "last_date": None,
    "pending": False,
    "close_time": None,
}


# ---------------------------------------------------------------------------
# Helper: trailing historical data for enriched AI notification context
# ---------------------------------------------------------------------------


async def _fetch_trailing_changes(pool, asset: str, days: int = 5) -> dict | None:
    """Return last N days of change_percent for an asset.

    Returns dict with:
      trailing: list of (date_str, change_pct) newest-first
      streak: int (positive = consecutive green days, negative = consecutive red)
      week_pct: float (sum of this week's daily changes)
    """
    try:
        rows = await pool.fetch(
            """
            SELECT change_percent, timestamp::date AS d
            FROM market_prices
            WHERE asset = $1 AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            asset, days,
        )
        if not rows:
            return None
        trailing = [
            (r["d"].isoformat(), round(float(r["change_percent"]), 2))
            for r in rows
        ]
        # Streak: count consecutive same-sign days from most recent
        streak = 0
        for _, pct in trailing:
            if streak == 0:
                streak = 1 if pct >= 0 else -1
            elif (pct >= 0 and streak > 0) or (pct < 0 and streak < 0):
                streak += 1 if streak > 0 else -1
            else:
                break
        # Calendar week total (Mon-Fri of current week only)
        from datetime import date as _date
        today = _date.today()
        monday = today - timedelta(days=today.weekday())
        week_pct = round(sum(
            pct for d_str, pct in trailing
            if _date.fromisoformat(d_str) >= monday
        ), 2)
        return {"trailing": trailing, "streak": streak, "week_pct": week_pct}
    except Exception:
        logger.debug("Trailing changes lookup failed for %s", asset, exc_info=True)
        return None


async def _fetch_fii_dii_trailing(pool, days: int = 5) -> dict | None:
    """Return last N days of FII/DII net cash data.

    Returns dict with:
      fii_trailing: list of (date_str, value) newest-first
      dii_trailing: list of (date_str, value) newest-first
      fii_streak: int (positive = consecutive buying days)
      dii_streak: int
      fii_week_total: float
      dii_week_total: float
    """
    try:
        rows = await pool.fetch(
            """
            SELECT indicator_name, value, timestamp::date AS d
            FROM macro_indicators
            WHERE indicator_name IN ('fii_net_cash', 'dii_net_cash')
              AND value IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT $1
            """,
            days * 2,  # both FII and DII per day
        )
        if not rows:
            return None
        fii = [(r["d"].isoformat(), round(float(r["value"]), 0))
               for r in rows if r["indicator_name"] == "fii_net_cash"]
        dii = [(r["d"].isoformat(), round(float(r["value"]), 0))
               for r in rows if r["indicator_name"] == "dii_net_cash"]

        def _streak(vals):
            s = 0
            for _, v in vals:
                if s == 0:
                    s = 1 if v >= 0 else -1
                elif (v >= 0 and s > 0) or (v < 0 and s < 0):
                    s += 1 if s > 0 else -1
                else:
                    break
            return s

        from datetime import date as _date
        today = _date.today()
        monday = today - timedelta(days=today.weekday())
        fii_week = round(sum(v for d, v in fii[:days] if _date.fromisoformat(d) >= monday), 0)
        dii_week = round(sum(v for d, v in dii[:days] if _date.fromisoformat(d) >= monday), 0)

        return {
            "fii_trailing": fii[:days],
            "dii_trailing": dii[:days],
            "fii_streak": _streak(fii),
            "dii_streak": _streak(dii),
            "fii_week_total": fii_week,
            "dii_week_total": dii_week,
        }
    except Exception:
        logger.debug("FII/DII trailing lookup failed", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Helper: relative context ("best day in 2 weeks", "largest drop in 3 weeks")
# ---------------------------------------------------------------------------

async def _get_relative_context(pool, asset: str, change_pct: float, days: int = 30) -> str | None:
    """Compare today's change to recent history and return a short phrase.

    Returns e.g. "best day in 2 weeks" or "largest drop in 3 weeks", or None.
    """
    if change_pct == 0:
        return None
    try:
        rows = await pool.fetch(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = $1 AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            asset, days,
        )
        if len(rows) < 2:
            return None

        direction_positive = change_pct > 0
        # Count how many trading days since a bigger move in the same direction
        streak = 0
        for row in rows[1:]:  # skip today (index 0)
            prev_pct = float(row["change_percent"])
            if direction_positive and prev_pct >= change_pct:
                break
            if not direction_positive and prev_pct <= change_pct:
                break
            streak += 1

        if streak < 2:
            return None

        # Convert trading days to human-readable period
        if streak >= 20:
            period = f"{streak // 5} weeks"
        elif streak >= 10:
            period = f"{streak // 5} weeks"
        elif streak >= 5:
            period = "1 week" if streak < 10 else f"{streak // 5} weeks"
        else:
            period = f"{streak} trading days"

        if direction_positive:
            return f"best day in {period}"
        else:
            return f"largest drop in {period}"
    except Exception:
        logger.debug("Relative context lookup failed for %s", asset, exc_info=True)
        return None


async def _get_52week_context(pool, asset: str, current_close: float | None) -> str | None:
    """Return 'near 52-week high' if within 2% of the max close in last 252 trading days."""
    if current_close is None:
        return None
    try:
        row = await pool.fetchrow(
            """
            SELECT MAX(price) AS max_price, MIN(price) AS min_price
            FROM (
                SELECT price FROM market_prices
                WHERE asset = $1
                ORDER BY timestamp DESC
                LIMIT 252
            ) sub
            """,
            asset,
        )
        if not row or row["max_price"] is None:
            return None
        max_price = float(row["max_price"])
        min_price = float(row["min_price"])
        if max_price == 0:
            return None
        pct_from_high = (max_price - current_close) / max_price * 100
        if pct_from_high <= 2.0:
            return "near 52-week high"
        pct_from_low = (current_close - min_price) / min_price * 100 if min_price > 0 else None
        if pct_from_low is not None and pct_from_low <= 2.0:
            return "near 52-week low"
        return None
    except Exception:
        logger.debug("52-week context lookup failed for %s", asset, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Data fetchers for each market close notification
# ---------------------------------------------------------------------------

async def _fetch_india_close_data() -> dict | None:
    """Fetch data needed for India market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # The three broad market-cap indices for India: Nifty 50
        # (large-cap), Nifty Midcap 150, Nifty Smallcap 250. Sensex is
        # intentionally excluded — it duplicates the Nifty 50 large-cap
        # signal and nothing in the close builder or the AI narrative
        # prompt consumes it anymore.
        #
        # Freshness gate: only accept rows from the current UTC session
        # date. market_prices rows are stored with UTC-midnight-of-session
        # as their timestamp, so comparing UTC dates is the native match.
        # For India specifically this also happens to equal the IST session
        # date at every moment India notifications fire (India is closed in
        # the 18:30–23:59 UTC window where UTC/IST dates would disagree),
        # so this behaves identically to the previous Asia/Kolkata gate.
        # Kept unified with every other fetcher in this file so there's a
        # single convention to reason about.
        #
        # On holidays the latest Nifty row is from the previous trading
        # session (e.g. Friday close still sitting in the DB on Ambedkar
        # Jayanti) and we must NOT present it as "today's close". This is
        # a third-layer defence — layers 1 and 2 in run_notification_job
        # already gate holidays, this gate catches any future bug path
        # that reaches us without going through the transition loop.
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price, timestamp FROM market_prices
            WHERE asset IN ('Nifty 50', 'Nifty Midcap 150', 'Nifty Smallcap 250')
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 6
            """
        )
        if not index_rows:
            logger.info(
                "_fetch_india_close_data: no Nifty rows for today — "
                "market was closed or data hasn't arrived yet"
            )
            return None

        data: dict = {}
        nifty_close = None
        seen_idx: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen_idx:
                continue
            seen_idx.add(a)
            if a == "Nifty 50" and row["change_percent"] is not None:
                data["nifty_change_pct"] = float(row["change_percent"])
                nifty_close = float(row["price"])
            elif a == "Nifty Midcap 150" and row["change_percent"] is not None:
                data["midcap_change_pct"] = float(row["change_percent"])
            elif a == "Nifty Smallcap 250" and row["change_percent"] is not None:
                data["smallcap_change_pct"] = float(row["change_percent"])

        if "nifty_change_pct" not in data:
            return None

        # ── Closing-auction freshness gate ──
        #
        # The daily market_prices row gets rewritten by every market_job
        # tick from whatever Yahoo's spot endpoint is reporting. Between
        # 15:30 and ~15:33 IST Yahoo typically still returns the last
        # pre-close intraday price, not the closing auction print, so a
        # notification fired in that window reports (e.g.) Nifty +1.54%
        # when the actual close is +1.63% — a measurable drift the user
        # will compare against Google's live widget.
        #
        # Detect "close auction has landed" by comparing the daily row's
        # Nifty 50 price against the most recent market_prices_intraday
        # tick for Nifty 50 today. The intraday gate in market_job drops
        # post-session ticks, so the last intraday row is ALWAYS the
        # last pre-close tick. Once Yahoo publishes the auction print,
        # market_job writes it to the daily row only — the intraday row
        # stays frozen — and the two diverge. That divergence is our
        # "ready" signal. No clock arithmetic required.
        #
        # If the two rows are still identical (within 0.05 points of
        # slop for float rounding), return None so the caller retries
        # on the next notification tick. The post-market pending loop
        # will keep polling until either the divergence appears or the
        # hard ceiling in _check_post_market_summary fires the fallback.
        if nifty_close is not None:
            last_intra = await pool.fetchrow(
                """
                SELECT price
                FROM market_prices_intraday
                WHERE asset = 'Nifty 50'
                  AND instrument_type = 'index'
                  AND (source_timestamp AT TIME ZONE 'UTC')::date
                      = (NOW() AT TIME ZONE 'UTC')::date
                  AND provider NOT IN ('closing_auction', 'yahoo_1m')
                ORDER BY source_timestamp DESC
                LIMIT 1
                """
            )
            if last_intra is not None and last_intra["price"] is not None:
                try:
                    last_intra_price = float(last_intra["price"])
                    if abs(last_intra_price - nifty_close) < 0.05:
                        logger.info(
                            "_fetch_india_close_data: daily Nifty 50 has not "
                            "diverged from last intraday tick (%.2f vs %.2f) — "
                            "waiting for closing auction print",
                            last_intra_price, nifty_close,
                        )
                        return None
                except (TypeError, ValueError):
                    pass

        # Relative context for Nifty 50
        data["relative_context"] = await _get_relative_context(
            pool, "Nifty 50", data["nifty_change_pct"]
        )
        data["week52_context"] = await _get_52week_context(pool, "Nifty 50", nifty_close)

        # Breadth from brief_service
        try:
            from app.services.brief_service import get_post_market_overview
            overview = await get_post_market_overview(market="IN")
            data["advancers"] = overview.get("advancers", 0)
            data["decliners"] = overview.get("decliners", 0)
        except Exception:
            logger.debug("Breadth data unavailable for India close")

        # Sector indices — gated to today's UTC session to match the
        # primary Nifty query above. Without this the "sector rotation"
        # narrative could echo yesterday's bank vs. IT split on a day the
        # sector scraper hasn't landed today's rows yet.
        sector_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('Nifty Bank', 'Nifty IT', 'Nifty Pharma', 'Nifty Auto', 'Nifty Metal')
              AND change_percent IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 10
            """
        )
        seen: set[str] = set()
        sectors: list[tuple[str, float]] = []
        for row in sector_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            sectors.append((a, float(row["change_percent"])))

        if sectors:
            sectors.sort(key=lambda x: x[1], reverse=True)
            data["top_sector"] = sectors[0][0]
            data["top_sector_pct"] = sectors[0][1]
            data["bottom_sector"] = sectors[-1][0]
            data["bottom_sector_pct"] = sectors[-1][1]

        # Trailing context for richer AI narrative
        trailing = await _fetch_trailing_changes(pool, "Nifty 50", days=10)
        if trailing:
            data["nifty_trailing"] = trailing

        return data
    except Exception:
        logger.exception("Failed to fetch India close data")
        return None


async def _fetch_us_close_data() -> dict | None:
    """Fetch data needed for US market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Gate to today's UTC session — see _fetch_us_open_data for the
        # rationale. Prevents the close notification from echoing yesterday's
        # numbers if the scraper hasn't landed today's row yet.
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'Dow Jones')
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 6
            """
        )
        data: dict = {}
        sp_close = None
        seen: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"]) if row["change_percent"] is not None else None
            if a == "S&P500":
                data["sp500_change_pct"] = pct
                sp_close = float(row["price"]) if row["price"] else None
            elif a == "NASDAQ":
                data["nasdaq_change_pct"] = pct
            elif a == "Dow Jones":
                data["dow_change_pct"] = pct

        if "sp500_change_pct" not in data or data["sp500_change_pct"] is None:
            return None

        data["relative_context"] = await _get_relative_context(
            pool, "S&P500", data["sp500_change_pct"]
        )
        data["week52_context"] = await _get_52week_context(pool, "S&P500", sp_close)

        # Gift Nifty latest price for India next-day signal
        gift_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices_intraday
            WHERE asset = 'Gift Nifty'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        nifty_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if gift_row and nifty_row:
            gift_price = float(gift_row["price"])
            nifty_close = float(nifty_row["price"])
            if nifty_close > 0:
                data["gift_nifty_price"] = gift_price
                data["gift_nifty_change_pct"] = (gift_price - nifty_close) / nifty_close * 100

        trailing = await _fetch_trailing_changes(pool, "S&P500", days=10)
        if trailing:
            data["sp500_trailing"] = trailing

        # CBOE VIX
        vix_row = await pool.fetchrow(
            """
            SELECT price, change_percent FROM market_prices
            WHERE asset = 'CBOE VIX' AND price IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
            """
        )
        if vix_row:
            data["cboe_vix"] = float(vix_row["price"])
            if vix_row["change_percent"] is not None:
                data["cboe_vix_pct"] = float(vix_row["change_percent"])

        return data
    except Exception:
        logger.exception("Failed to fetch US close data")
        return None


async def _fetch_europe_close_data() -> dict | None:
    """Fetch data needed for Europe market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Gate to today's UTC session — see _fetch_us_open_data.
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('FTSE 100', 'DAX', 'CAC 40')
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 6
            """
        )
        data: dict = {}
        ftse_close = None
        seen: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"]) if row["change_percent"] is not None else None
            if a == "FTSE 100":
                data["ftse_change_pct"] = pct
                ftse_close = float(row["price"]) if row["price"] else None
            elif a == "DAX":
                data["dax_change_pct"] = pct
            elif a == "CAC 40":
                data["cac_change_pct"] = pct

        if "ftse_change_pct" not in data and "dax_change_pct" not in data:
            return None

        # Relative context for FTSE 100 (primary European index)
        primary_asset = "FTSE 100" if "ftse_change_pct" in data else "DAX"
        primary_pct = data.get("ftse_change_pct") or data.get("dax_change_pct")
        primary_close = ftse_close
        if primary_pct is not None:
            data["relative_context"] = await _get_relative_context(pool, primary_asset, primary_pct)
        data["week52_context"] = await _get_52week_context(pool, primary_asset, primary_close)

        # Brent crude — gated.
        brent_row = await pool.fetchrow(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = 'brent crude'
              AND change_percent IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if brent_row:
            data["brent_change_pct"] = float(brent_row["change_percent"])

        trailing = await _fetch_trailing_changes(pool, "FTSE 100", days=10)
        if trailing:
            data["ftse_trailing"] = trailing

        return data
    except Exception:
        logger.exception("Failed to fetch Europe close data")
        return None


async def _fetch_japan_close_data() -> dict | None:
    """Fetch data needed for Japan market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Gate to today's UTC session — see _fetch_us_open_data.
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('Nikkei 225', 'TOPIX')
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 4
            """
        )
        data: dict = {}
        nikkei_close = None
        seen: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"]) if row["change_percent"] is not None else None
            if a == "Nikkei 225":
                data["nikkei_change_pct"] = pct
                nikkei_close = float(row["price"]) if row["price"] else None
            elif a == "TOPIX":
                data["topix_change_pct"] = pct

        if "nikkei_change_pct" not in data or data["nikkei_change_pct"] is None:
            return None

        data["relative_context"] = await _get_relative_context(
            pool, "Nikkei 225", data["nikkei_change_pct"]
        )
        data["week52_context"] = await _get_52week_context(pool, "Nikkei 225", nikkei_close)

        # JPY/INR for yen context
        jpy_row = await pool.fetchrow(
            """
            SELECT price, change_percent FROM market_prices
            WHERE asset = 'JPY/INR' AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if jpy_row:
            data["jpy_inr_price"] = float(jpy_row["price"])
            data["jpy_inr_change_pct"] = float(jpy_row["change_percent"])

        trailing = await _fetch_trailing_changes(pool, "Nikkei 225", days=10)
        if trailing:
            data["nikkei_trailing"] = trailing

        return data
    except Exception:
        logger.exception("Failed to fetch Japan close data")
        return None


# ---------------------------------------------------------------------------
# Data fetcher for market open notifications
# ---------------------------------------------------------------------------

async def _fetch_india_open_data() -> dict | None:
    """Fetch data for India market open: Gift Nifty + overnight US/Asia.

    Returns None before 9:15 IST so the open notification doesn't fire
    during NSE's pre-open session (9:00-9:15 IST). NSE's API reports
    isMarketOpen=True during pre-open which would otherwise trip the
    transition detection 15 min early. The flush loop polls every
    30s and will fire as soon as 9:15 passes.
    """
    now_ist = datetime.now(timezone.utc).astimezone(_IST)
    ist_minutes = now_ist.hour * 60 + now_ist.minute
    if ist_minutes < 555:  # 9:15 IST = 9*60+15
        return None
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Nifty 50 PREVIOUS SESSION CLOSE.
        # If today's daily row already exists, previous_close holds
        # yesterday's close (correct baseline).  But pre-market the
        # latest row is *yesterday's*, whose previous_close is the
        # day-before-yesterday's close — one day too stale.  In that
        # case use yesterday's price (= yesterday's close) instead.
        nifty_row = await pool.fetchrow(
            """
            SELECT previous_close, price, timestamp::date AS d
            FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not nifty_row:
            return None
        today_utc = datetime.now(timezone.utc).date()
        if nifty_row["d"] == today_utc:
            # Today's row exists — previous_close is yesterday's close
            nifty_close = float(nifty_row["previous_close"] or nifty_row["price"])
        else:
            # Latest row is from a prior session — its price IS the close
            nifty_close = float(nifty_row["price"])
        if nifty_close == 0:
            return None

        # Gift Nifty latest
        gift_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices_intraday
            WHERE asset = 'Gift Nifty'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not gift_row:
            return None

        gift_price = float(gift_row["price"])
        data: dict = {
            "gift_nifty_price": gift_price,
            "gift_nifty_change_pct": (gift_price - nifty_close) / nifty_close * 100,
        }

        # Overnight US/Asia + Gold
        global_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'Nikkei 225', 'gold')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 8
            """
        )
        seen: set[str] = set()
        for row in global_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"])
            if a == "S&P500":
                data["us_sp500_pct"] = pct
            elif a == "NASDAQ":
                data["us_nasdaq_pct"] = pct
            elif a == "Nikkei 225":
                data["nikkei_pct"] = pct
            elif a == "gold":
                data["gold_pct"] = pct

        trailing = await _fetch_trailing_changes(pool, "Nifty 50", days=10)
        if trailing:
            data["nifty_trailing"] = trailing

        return data
    except Exception:
        logger.exception("Failed to fetch India open data")
        return None


async def _fetch_japan_open_data() -> dict | None:
    """Fetch data for Japan market open: Nikkei/TOPIX + overnight US + FTSE + JPY/INR + gold."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Opening move = (first intraday tick today - previous_close) /
        # previous_close. See the long comment in _fetch_us_open_data for
        # the full rationale — same shape, same reason.
        # Previous close for Nikkei/TOPIX.  Prefer today's daily row
        # (previous_close = yesterday's close).  But TSE opens at 00:00
        # UTC and the daily row may not exist yet — fall back to the
        # latest row's price (= yesterday's close) just like the Nifty
        # fix for pre-market notifications.
        today_utc = datetime.now(timezone.utc).date()
        prev_close_by_asset: dict[str, float] = {}
        for idx_asset in ("Nikkei 225", "TOPIX"):
            row = await pool.fetchrow(
                """
                SELECT previous_close, price, timestamp::date AS d
                FROM market_prices
                WHERE asset = $1 AND instrument_type = 'index'
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                idx_asset,
            )
            if not row:
                continue
            if row["d"] == today_utc:
                val = row["previous_close"] or row["price"]
            else:
                val = row["price"]
            if val and float(val) != 0:
                prev_close_by_asset[idx_asset] = float(val)

        opening_rows = await pool.fetch(
            """
            SELECT DISTINCT ON (asset, instrument_type)
                asset, price
            FROM market_prices_intraday
            WHERE asset IN ('Nikkei 225', 'TOPIX')
              AND instrument_type = 'index'
              AND (COALESCE(source_timestamp, "timestamp") AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY asset, instrument_type, COALESCE(source_timestamp, "timestamp") ASC
            """,
        )
        data: dict = {}
        for row in opening_rows:
            asset = row["asset"]
            prev = prev_close_by_asset.get(asset)
            if prev is None or prev == 0:
                continue
            opening_price = float(row["price"])
            pct = (opening_price - prev) / prev * 100
            if asset == "Nikkei 225":
                data["nikkei_pct"] = pct
            elif asset == "TOPIX":
                data["topix_pct"] = pct

        if "nikkei_pct" not in data:
            return None

        # Overnight US (Wall Street) + gold — the two global cues that
        # _build_japan_open actually renders. FTSE used to be fetched
        # here too but the builder never consumed it (dead data path).
        global_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'gold')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 6
            """,
        )
        seen2: set[str] = set()
        for row in global_rows:
            a = row["asset"]
            if a in seen2:
                continue
            seen2.add(a)
            pct = float(row["change_percent"])
            if a == "S&P500":
                data["us_sp500_pct"] = pct
            elif a == "NASDAQ":
                data["us_nasdaq_pct"] = pct
            elif a == "gold":
                data["gold_pct"] = pct

        # JPY/INR
        jpy_row = await pool.fetchrow(
            """
            SELECT price, change_percent FROM market_prices
            WHERE asset = 'JPY/INR'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
        )
        if jpy_row and jpy_row["price"] is not None:
            data["jpy_inr_price"] = float(jpy_row["price"])
            if jpy_row["change_percent"] is not None:
                data["jpy_inr_pct"] = float(jpy_row["change_percent"])

        return data
    except Exception:
        logger.exception("Failed to fetch Japan open data")
        return None


async def _fetch_europe_open_data() -> dict | None:
    """Fetch data for Europe market open: FTSE/DAX/CAC + Asia + Brent crude."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Previous close for FTSE/DAX/CAC.  Same resilient pattern as
        # Japan: if today's daily row exists use previous_close, else
        # fall back to yesterday's row's price.
        today_utc = datetime.now(timezone.utc).date()
        prev_close_by_asset: dict[str, float] = {}
        for idx_asset in ("FTSE 100", "DAX", "CAC 40"):
            row = await pool.fetchrow(
                """
                SELECT previous_close, price, timestamp::date AS d
                FROM market_prices
                WHERE asset = $1 AND instrument_type = 'index'
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                idx_asset,
            )
            if not row:
                continue
            if row["d"] == today_utc:
                val = row["previous_close"] or row["price"]
            else:
                val = row["price"]
            if val and float(val) != 0:
                prev_close_by_asset[idx_asset] = float(val)

        opening_rows = await pool.fetch(
            """
            SELECT DISTINCT ON (asset, instrument_type)
                asset, price
            FROM market_prices_intraday
            WHERE asset IN ('FTSE 100', 'DAX', 'CAC 40')
              AND instrument_type = 'index'
              AND (COALESCE(source_timestamp, "timestamp") AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY asset, instrument_type, COALESCE(source_timestamp, "timestamp") ASC
            """,
        )
        data: dict = {}
        for row in opening_rows:
            asset = row["asset"]
            prev = prev_close_by_asset.get(asset)
            if prev is None or prev == 0:
                continue
            opening_price = float(row["price"])
            pct = (opening_price - prev) / prev * 100
            if asset == "FTSE 100":
                data["ftse_pct"] = pct
            elif asset == "DAX":
                data["dax_pct"] = pct
            elif asset == "CAC 40":
                data["cac_pct"] = pct

        if "ftse_pct" not in data and "dax_pct" not in data:
            return None
        # Wait for at least two of the three indices so the header
        # isn't incomplete (intraday ticks can land seconds apart).
        if sum(k in data for k in ("ftse_pct", "dax_pct", "cac_pct")) < 2:
            return None

        # Asia cues: Nikkei + Nifty 50 — gated. At Europe open (~08:00 UTC),
        # Nikkei already closed (06:00 UTC, same UTC day) and Nifty is
        # mid-session; any row used as a cue must be from today.
        asia_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('Nikkei 225', 'Nifty 50')
              AND change_percent IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 4
            """,
        )
        seen2: set[str] = set()
        for row in asia_rows:
            a = row["asset"]
            if a in seen2:
                continue
            seen2.add(a)
            pct = float(row["change_percent"])
            if a == "Nikkei 225":
                data["nikkei_pct"] = pct
            elif a == "Nifty 50":
                data["nifty_pct"] = pct

        # Brent crude — gated.
        brent_row = await pool.fetchrow(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = 'brent crude'
              AND change_percent IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 1
            """,
        )
        if brent_row:
            data["brent_pct"] = float(brent_row["change_percent"])

        return data
    except Exception:
        logger.exception("Failed to fetch Europe open data")
        return None


async def _fetch_us_open_data() -> dict | None:
    """Fetch data for US market open: S&P500/NASDAQ/Dow + Europe + crude + Gift Nifty."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # "S&P 500 opens +X%" must compare the FIRST tick of today's
        # session to yesterday's close — not the latest tick. Reading
        # `change_percent` from the daily market_prices row is wrong
        # because that row is upserted on every scraper cycle
        # (insert_prices_batch_upsert_daily does DO UPDATE SET price,
        # change_percent, ...), so the field drifts throughout the
        # session. By the time the open-notification fire reaches this
        # function, the row already reflects several minutes of
        # post-open drift.
        #
        # Correct formula:
        #   opening_tick  = earliest market_prices_intraday row for
        #                   today's UTC session date
        #   previous_close = market_prices.previous_close from today's
        #                    daily row (stable field — the scraper
        #                    always passes yesterday's close value, so
        #                    it doesn't drift with intraday upserts)
        #   pct            = (opening_tick - previous_close) / previous_close
        #
        # Both queries are gated to today's UTC session so the function
        # returns None (→ caller waits for data to land, then falls back
        # to simple open after _OPEN_DATA_MAX_WAIT_SECONDS) instead of
        # serving yesterday's numbers.
        prev_close_rows = await pool.fetch(
            """
            SELECT asset, previous_close FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'Dow Jones')
              AND instrument_type = 'index'
              AND previous_close IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            """
        )
        prev_close_by_asset: dict[str, float] = {
            r["asset"]: float(r["previous_close"]) for r in prev_close_rows
        }

        opening_rows = await pool.fetch(
            """
            SELECT DISTINCT ON (asset, instrument_type)
                asset, price
            FROM market_prices_intraday
            WHERE asset IN ('S&P500', 'NASDAQ', 'Dow Jones')
              AND instrument_type = 'index'
              AND (COALESCE(source_timestamp, "timestamp") AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY asset, instrument_type, COALESCE(source_timestamp, "timestamp") ASC
            """
        )
        data: dict = {}
        for row in opening_rows:
            asset = row["asset"]
            prev = prev_close_by_asset.get(asset)
            if prev is None or prev == 0:
                continue
            opening_price = float(row["price"])
            pct = (opening_price - prev) / prev * 100
            if asset == "S&P500":
                data["sp500_pct"] = pct
            elif asset == "NASDAQ":
                data["nasdaq_pct"] = pct
            elif asset == "Dow Jones":
                data["dow_pct"] = pct

        if "sp500_pct" not in data:
            return None

        # Europe cues: FTSE + DAX — also gated to today's UTC session so
        # "Europe closed mixed with FTSE +0.1% and DAX +1.2%" never echoes
        # yesterday's European close when today's row hasn't landed.
        europe_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('FTSE 100', 'DAX')
              AND change_percent IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 4
            """,
        )
        seen2: set[str] = set()
        for row in europe_rows:
            a = row["asset"]
            if a in seen2:
                continue
            seen2.add(a)
            pct = float(row["change_percent"])
            if a == "FTSE 100":
                data["ftse_pct"] = pct
            elif a == "DAX":
                data["dax_pct"] = pct

        # Crude oil — gated to today's UTC session (same rationale).
        crude_row = await pool.fetchrow(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = 'crude oil'
              AND change_percent IS NOT NULL
              AND (timestamp AT TIME ZONE 'UTC')::date
                  = (NOW() AT TIME ZONE 'UTC')::date
            ORDER BY timestamp DESC
            LIMIT 1
            """,
        )
        if crude_row:
            data["crude_pct"] = float(crude_row["change_percent"])

        # Gift Nifty latest price + change from Nifty close
        gift_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices_intraday
            WHERE asset = 'Gift Nifty'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
        )
        nifty_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
        )
        if gift_row and nifty_row:
            gift_price = float(gift_row["price"])
            nifty_close = float(nifty_row["price"])
            if nifty_close > 0:
                data["gift_nifty_price"] = gift_price
                data["gift_nifty_change_pct"] = (gift_price - nifty_close) / nifty_close * 100

        return data
    except Exception:
        logger.exception("Failed to fetch US open data")
        return None


async def _check_gift_nifty(status: dict, now: datetime) -> None:
    """Check Gift Nifty movement and send alert if threshold crossed."""
    gift_open = bool(status.get("gift_nifty_open"))
    was_open = _gift_nifty_state.get("was_open")

    # Reset state on transition from open to closed
    if was_open is True and not gift_open:
        _gift_nifty_state["last_band"] = None
        logger.debug("Gift Nifty closed — reset alert state")

    _gift_nifty_state["was_open"] = gift_open

    if not gift_open:
        return

    # Suppress during NSE market hours — Gift Nifty alerts are only
    # meaningful as pre/post-market signals, not while NSE is live.
    now_ist = now.astimezone(_IST)
    ist_minutes = now_ist.hour * 60 + now_ist.minute
    if 540 <= ist_minutes <= 1020:  # 9:00 AM – 5:00 PM IST
        return

    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Latest Gift Nifty price from intraday table
        gift_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices_intraday
            WHERE asset = 'Gift Nifty'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not gift_row:
            return

        # Previous Nifty 50 SESSION CLOSE.
        # If today's daily row already exists, previous_close holds
        # yesterday's close (correct).  But pre-market the latest
        # row is *yesterday's*, whose previous_close is the
        # day-before-yesterday's close — one day too stale.  In that
        # case use yesterday's price (= yesterday's close) instead.
        nifty_row = await pool.fetchrow(
            """
            SELECT previous_close, price, timestamp::date AS d
            FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not nifty_row:
            return

        today_utc = datetime.now(timezone.utc).date()
        if nifty_row["d"] == today_utc:
            # Today's row exists — previous_close is yesterday's close
            nifty_close_raw = nifty_row["previous_close"]
            if nifty_close_raw is None or float(nifty_close_raw) == 0:
                nifty_close_raw = nifty_row["price"]
        else:
            # Latest row is from a prior session — its price IS the close
            nifty_close_raw = nifty_row["price"]

        gift_price = float(gift_row["price"])
        nifty_close = float(nifty_close_raw)
        if nifty_close == 0:
            return

        change_pct = (gift_price - nifty_close) / nifty_close * 100
        # 1.0% bands (was 0.5%) — wider bands mean fewer
        # notifications. At 0.5% bands, small oscillations
        # around a band boundary fired 3+ alerts per morning.
        current_band = int(change_pct) if abs(change_pct) >= 1 else (1 if change_pct > 0 else -1)

        today = now.astimezone(_IST).strftime("%Y-%m-%d")
        band_key = f"{current_band}"
        dedup_key = f"{today}_gift_nifty_{band_key}"

        # Cooldown: at least 30 minutes between Gift Nifty alerts
        # to prevent notification spam from volatile pre-market.
        last_sent = _gift_nifty_state.get("last_sent_at")
        if last_sent is not None:
            elapsed = (now - last_sent).total_seconds()
            if elapsed < 1800:  # 30 minutes
                return

        # Max 3 Gift Nifty alerts per day
        daily_count = _gift_nifty_state.get("daily_count", 0)
        daily_date = _gift_nifty_state.get("daily_date")
        if daily_date != today:
            daily_count = 0

        if abs(change_pct) > 0.5 and current_band != _gift_nifty_state.get("last_band") and daily_count < 3:
            logger.info(
                "Gift Nifty alert: %.1f%% (price=%.0f, nifty_close=%.0f, band=%d)",
                change_pct, gift_price, nifty_close, current_band,
            )
            await notification_service.notify_gift_nifty_move(
                change_pct, gift_price, dedup_key=dedup_key,
            )
            _gift_nifty_state["last_band"] = current_band
            _gift_nifty_state["last_sent_at"] = now
            _gift_nifty_state["daily_count"] = daily_count + 1
            _gift_nifty_state["daily_date"] = today

    except Exception:
        logger.exception("Gift Nifty check failed")


async def _check_fii_dii(now: datetime) -> tuple[float | None, float | None]:
    """Check for new FII/DII data and send alert. Returns (fii_net, dii_net) or (None, None)."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        now_ist = now.astimezone(_IST)

        # Only check after 17:00 IST
        if now_ist.hour < 17:
            return None, None

        rows = await pool.fetch(
            """
            SELECT DISTINCT ON (indicator_name) indicator_name, value, timestamp
            FROM macro_indicators
            WHERE indicator_name IN ('fii_net_cash', 'dii_net_cash')
              AND unit = 'inr_cr'
            ORDER BY indicator_name, timestamp DESC
            """
        )
        if len(rows) < 2:
            return None, None

        fii_net = None
        dii_net = None
        latest_date = None
        for row in rows:
            ts = row["timestamp"]
            row_date = ts.astimezone(_IST).date() if ts.tzinfo else ts.date()
            if latest_date is None:
                latest_date = row_date
            if row["indicator_name"] == "fii_net_cash":
                fii_net = float(row["value"])
            elif row["indicator_name"] == "dii_net_cash":
                dii_net = float(row["value"])

        if fii_net is None or dii_net is None or latest_date is None:
            return None, None

        # Only notify for today's data — ignore stale/yesterday's data
        today = now_ist.date()
        if latest_date != today:
            return None, None

        today_str = today.strftime("%Y-%m-%d")
        dedup_key = f"{today_str}_fii_dii"

        if latest_date != _fii_dii_state.get("last_date"):
            logger.info("FII/DII alert: fii=%.0f, dii=%.0f, date=%s", fii_net, dii_net, latest_date)
            trailing = await _fetch_fii_dii_trailing(pool, days=10)
            await notification_service.notify_fii_dii_data(
                fii_net, dii_net, dedup_key=dedup_key,
                trailing=trailing,
            )
            _fii_dii_state["last_date"] = latest_date

        return fii_net, dii_net

    except Exception:
        logger.exception("FII/DII check failed")
        return None, None


async def _check_pre_market_summary(status: dict, now: datetime) -> None:
    """Send pre-market summary between 8:55-8:58 AM IST on trading days.

    Strictly before 8:59 IST so it never overlaps NSE's pre-open
    session (which starts at 9:00 IST) or the continuous trading
    session (9:15 IST).
    """
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

    # Only fire between 8:55 and 8:58 IST (strictly before 8:59)
    total_minutes = now_ist.hour * 60 + now_ist.minute
    if total_minutes < 535 or total_minutes > 538:  # 8:55 to 8:58
        return

    # Already sent today
    if _pre_market_state.get("last_date") == today:
        return

    # Hard gate: NSE must be a trading day.  The old check used
    # gift_nifty_open as a proxy, but Gift Nifty opens on SGX
    # before NSE trading days AND on some weekends (Sunday
    # evening for Monday's session), so the pre-market summary
    # was incorrectly firing on Sundays/holidays.
    from app.scheduler.trading_calendar import get_india_session_info
    india_info = get_india_session_info(now)
    if not bool(india_info.get("is_trading_day")):
        logger.debug(
            "Pre-market summary skipped: not an NSE trading day (%s)",
            india_info.get("fallback_reason"),
        )
        return

    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Previous Nifty 50 SESSION CLOSE.
        # If today's daily row already exists, previous_close holds
        # yesterday's close (correct).  But pre-market the latest
        # row is *yesterday's*, whose previous_close is the
        # day-before-yesterday's close — one day too stale.  In that
        # case use yesterday's price (= yesterday's close) instead.
        nifty_close_row = await pool.fetchrow(
            """
            SELECT previous_close, price, timestamp::date AS d
            FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not nifty_close_row:
            return
        today_utc = datetime.now(timezone.utc).date()
        if nifty_close_row["d"] == today_utc:
            nifty_close = float(nifty_close_row["previous_close"] or nifty_close_row["price"])
        else:
            nifty_close = float(nifty_close_row["price"])
        if nifty_close == 0:
            return

        # Gift Nifty latest price
        gift_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices_intraday
            WHERE asset = 'Gift Nifty'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not gift_row:
            return
        gift_price = float(gift_row["price"])
        gift_change_pct = (gift_price - nifty_close) / nifty_close * 100

        # Global cues — overnight US and Asia markets
        us_change = {}
        asia_change = {}
        global_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('S&P500', 'Dow Jones', 'NASDAQ', 'Nikkei 225', 'Hang Seng')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 10
            """
        )
        seen = set()
        for row in global_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"])
            # Use clean display names for notifications
            _display = {"S&P500": "S&P 500"}.get(a, a)
            if a in ("S&P500", "Dow Jones", "NASDAQ"):
                us_change[_display] = pct
            else:
                asia_change[_display] = pct

        # India VIX for fear gauge
        vix_row = await pool.fetchrow(
            """
            SELECT price, change_percent FROM market_prices
            WHERE asset = 'India VIX' AND price IS NOT NULL
            ORDER BY timestamp DESC LIMIT 1
            """
        )
        india_vix = None
        india_vix_pct = None
        if vix_row:
            india_vix = float(vix_row["price"])
            if vix_row["change_percent"] is not None:
                india_vix_pct = float(vix_row["change_percent"])

        logger.info(
            "Pre-market summary: gift_nifty=%.0f (%.1f%%), us=%s, asia=%s",
            gift_price, gift_change_pct, us_change, asia_change,
        )

        today_str = today.strftime("%Y-%m-%d")
        dedup_key = f"{today_str}_pre_market"

        nifty_trailing = await _fetch_trailing_changes(pool, "Nifty 50", days=10)

        await notification_service.notify_pre_market_summary(
            gift_nifty_price=gift_price,
            gift_nifty_change_pct=gift_change_pct,
            us_change=us_change or None,
            asia_change=asia_change or None,
            dedup_key=dedup_key,
            nifty_trailing=nifty_trailing,
            india_vix=india_vix,
            india_vix_pct=india_vix_pct,
        )

        _pre_market_state["last_date"] = today

    except Exception:
        logger.exception("Pre-market summary check failed")


async def _check_commodity_spikes(now: datetime) -> None:
    """Check for commodity price spikes (±3% from previous session close).

    Previous version had three issues:
    1. Used source's change_percent which could be stale after holidays.
    2. Used 2% bands → crude oil going 0→7% fired 3 notifications (bands 2,4,6).
    3. No cooldown → oscillation near a band boundary fired repeatedly.

    Now: compute change from DB's previous-day price, use 3% bands,
    add 2-hour cooldown per asset, cap at 2 alerts per asset per day.
    """
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

    # Gate: only alert on commodity trading days. Prevents stale
    # weekend data from triggering false spikes.
    from app.scheduler.trading_calendar import is_trading_day_commodities
    if not is_trading_day_commodities(now):
        return

    # Reset alerted state on new day
    if _commodity_spike_state.get("last_date") != today:
        _commodity_spike_state["last_date"] = today
        _commodity_spike_state["alerted"] = {}
        _commodity_spike_state["daily_counts"] = {}
        _commodity_spike_state["last_sent"] = {}

    try:
        from app.core.database import get_pool
        pool = await get_pool()

        _SPIKE_ASSETS = ('gold', 'silver', 'crude oil', 'natural gas')

        # Use the source's OWN change_percent and previous_close.
        # These are calculated by Google/Yahoo relative to the SAME
        # futures contract's prior close — always consistent.
        # NEVER compute change from DB rows across sources: Google
        # (CLW00:NYMEX) and Yahoo (CL=F continuous) track different
        # contracts that diverge by 5-15% during roll periods.
        rows = await pool.fetch(
            """
            SELECT DISTINCT ON (asset)
                asset, price, unit, change_percent, previous_close
            FROM market_prices
            WHERE instrument_type = 'commodity'
              AND asset = ANY($1)
              AND change_percent IS NOT NULL
            ORDER BY asset, timestamp DESC
            """,
            list(_SPIKE_ASSETS),
        )

        if not rows:
            return

        # Fetch USD/INR rate for Indian-friendly commodity prices
        usd_inr: float | None = None
        try:
            _fx_row = await pool.fetchrow(
                """
                SELECT price FROM market_prices
                WHERE asset = 'USD/INR' AND price > 0
                ORDER BY "timestamp" DESC LIMIT 1
                """
            )
            if _fx_row:
                usd_inr = float(_fx_row["price"])
        except Exception:
            logger.debug("USD/INR rate unavailable for commodity notification")

        alerted = _commodity_spike_state["alerted"]
        daily_counts = _commodity_spike_state.get("daily_counts", {})
        last_sent = _commodity_spike_state.get("last_sent", {})

        for row in rows:
            asset = row["asset"]
            price = float(row["price"])
            unit = row.get("unit")

            # Use the source's own change_percent — already computed
            # relative to the same contract's prior close.
            change_pct = float(row.get("change_percent") or 0)

            # Only alert on ≥3% moves (was 2% — too noisy for volatile commodities)
            if abs(change_pct) < 3.0:
                continue

            # Sanity gate: Yahoo's change_percent field occasionally
            # reports stale/inconsistent values (e.g. +3.3% while
            # price vs previous_close is actually flat/down). Validate
            # change_pct reconciles with (price - previous_close)/previous_close
            # within 0.5% before firing to prevent false-positive
            # "Crude Oil surges 3%" notifications.
            prev_close_raw = row.get("previous_close")
            if prev_close_raw is not None and float(prev_close_raw) > 0:
                prev_close = float(prev_close_raw)
                computed_pct = (price - prev_close) / prev_close * 100
                if abs(computed_pct - change_pct) > 0.5:
                    logger.info(
                        "Commodity spike rejected for %s: "
                        "source change_pct=%.2f%% but (price=%s vs prev=%s) "
                        "computes %.2f%% — mismatch > 0.5%% (stale data)",
                        asset, change_pct, price, prev_close, computed_pct,
                    )
                    continue

            # 3% bands to avoid spamming
            band = int(change_pct / 3) * 3
            if alerted.get(asset) == band:
                continue

            # Cooldown: 2 hours between alerts per asset
            asset_last = last_sent.get(asset)
            if asset_last is not None and (now - asset_last).total_seconds() < 7200:
                continue

            # Max 2 alerts per asset per day
            if daily_counts.get(asset, 0) >= 2:
                continue

            display_name = asset.replace("_", " ").title()

            # Use UTC date for dedup_key so a spike that straddles
            # IST midnight (crude oil trades 24x7 on NYMEX) doesn't
            # re-fire as a "new day" alert at 00:01 IST.
            utc_today_str = now.astimezone(timezone.utc).strftime("%Y-%m-%d")
            dedup_key = f"{utc_today_str}_commodity_spike_{asset}_{band}"

            # Compute INR price if USD/INR available
            inr_price: float | None = None
            inr_unit: str | None = None
            if usd_inr:
                if asset == "gold":
                    # Gold: USD/oz → INR/10g (1 troy oz ≈ 31.1035g)
                    inr_price = price * usd_inr / 31.1035 * 10
                    inr_unit = "10g"
                elif asset == "silver":
                    # Silver: USD/oz → INR/kg (1 troy oz ≈ 31.1035g)
                    inr_price = price * usd_inr / 31.1035 * 1000
                    inr_unit = "kg"
                elif asset in ("crude oil", "natural gas"):
                    # Crude/Gas: USD/bbl or USD/MMBtu → INR equivalent
                    inr_price = price * usd_inr
                    inr_unit = unit

            logger.info(
                "Commodity spike: %s %.1f%% at $%.2f (INR: %s)",
                asset, change_pct, price,
                f"₹{inr_price:,.0f}/{inr_unit}" if inr_price else "N/A",
            )
            await notification_service.notify_commodity_spike(
                asset=asset,
                display_name=display_name,
                change_pct=change_pct,
                price=price,
                unit=unit,
                inr_price=inr_price,
                inr_unit=inr_unit,
                dedup_key=dedup_key,
            )
            alerted[asset] = band
            last_sent[asset] = now
            daily_counts[asset] = daily_counts.get(asset, 0) + 1
            _commodity_spike_state["daily_counts"] = daily_counts
            _commodity_spike_state["last_sent"] = last_sent

    except Exception:
        logger.exception("Commodity spike check failed")


async def _check_post_market_summary(now: datetime, india_closed_transition: bool) -> None:
    """Send post-market summary ~5 min after NSE close (survives restarts).

    Uses transition detection when available. Falls back to DB-based detection:
    if India is closed, today is a trading day, and we haven't sent yet, fire.
    """
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

    # ── Holiday / non-trading-day gate (first line of defence) ──
    #
    # On NSE holidays (Ambedkar Jayanti, Gandhi Jayanti, Republic Day, etc.)
    # upstream data sources (Google Finance, Yahoo) sometimes report
    # transient "open" states as stale prices flap, which produces a
    # false open→closed transition in the main loop. That bug used to
    # reach this function with `india_closed_transition=True` and the
    # "pending" logic would then flush a fake post-market notification
    # showing stale data from the previous trading day's close.
    #
    # Gate at the top regardless of transition state: if XBOM/NSE says
    # today isn't a trading day, clear any stale pending state and exit.
    # This layer is a backstop — layer 2 (the transition loop itself)
    # skips tracking India state entirely on holidays.
    india_info = get_india_session_info(now)
    if not india_info.get("is_trading_day"):
        if _post_market_state.get("pending"):
            logger.info(
                "Post-market summary: dropping pending state — today (%s) is "
                "not an NSE trading day (source=%s)",
                today, india_info.get("source"),
            )
            _post_market_state["pending"] = False
            _post_market_state["close_time"] = None
        return

    # Already sent today
    if _post_market_state.get("last_date") == today:
        return

    if india_closed_transition:
        # We caught the live transition — wait 5 minutes for data to settle
        _post_market_state["pending"] = True
        _post_market_state["close_time"] = now
        logger.info("Post-market summary pending — will send after 5 minutes")
        return

    if _post_market_state.get("pending"):
        # Waiting for the post-market settle window after a live transition.
        # See _POST_MARKET_SETTLE_SECONDS_INDIA for rationale on the 10-min
        # floor — the data-driven freshness gate in _fetch_india_close_data
        # still has the final say within this window.
        close_time = _post_market_state.get("close_time")
        if close_time and (now - close_time).total_seconds() < _POST_MARKET_SETTLE_SECONDS_INDIA:
            return
    else:
        # No live transition detected (e.g., server restarted after close).
        # Fallback: check if India is closed and today's close data exists in DB.
        from app.scheduler.trading_calendar import get_market_status

        # Gate 1: today must actually be an NSE trading day. Saturdays,
        # Sundays and holidays must never trigger a post-market summary
        # even though the market ticker cron writes rows every 30s.
        india_info = get_india_session_info(now)
        if not bool(india_info.get("is_trading_day")):
            return

        status = get_market_status(now)
        if status.get("india_open"):
            return  # Market still open

        # Gate 2: only fire within the 1-hour window *after* NSE close
        # (15:30-16:30 IST = 930-990 minutes). The previous check had the
        # comparison inverted and allowed firing all morning on non-trading
        # days when stale Nifty rows existed in market_prices.
        total_minutes = now_ist.hour * 60 + now_ist.minute
        if total_minutes < 930 or total_minutes > 990:
            return

    try:
        # Use the same rich fetcher as the regular close path so all
        # indices (Nifty 50, Midcap 150, Smallcap 250), sector leaders,
        # relative context, and 52-week context flow through to the
        # `_build_india_close` title/body builder in notification_service.
        close_data = await _fetch_india_close_data()
        if not close_data:
            logger.warning("Post-market summary: _fetch_india_close_data returned None")
            return

        logger.info(
            "Post-market summary: nifty=%.2f%% midcap150=%s smallcap250=%s "
            "adv=%d dec=%d top=%s bottom=%s",
            close_data.get("nifty_change_pct", 0.0),
            f"{close_data.get('midcap_change_pct'):.2f}%"
            if close_data.get("midcap_change_pct") is not None else "n/a",
            f"{close_data.get('smallcap_change_pct'):.2f}%"
            if close_data.get("smallcap_change_pct") is not None else "n/a",
            close_data.get("advancers", 0),
            close_data.get("decliners", 0),
            close_data.get("top_sector"),
            close_data.get("bottom_sector"),
        )

        today_str = today.strftime("%Y-%m-%d")
        # Single shared dedup key across both paths (post-market and
        # missed-close fallback) to prevent duplicate notifications.
        dedup_key = f"{today_str}_market_close_india"

        await notification_service.notify_market_close(
            "india",
            market_data=close_data,
            dedup_key=dedup_key,
        )

        _post_market_state["pending"] = False
        _post_market_state["last_date"] = today

    except Exception:
        logger.exception("Post-market summary check failed")


# Expected open hours in IST for each market (approximate)
_OPEN_WINDOWS_IST: dict[str, tuple[int, int]] = {
    # (earliest_open_hour, latest_check_hour) in IST — narrow windows to avoid duplicates
    "india": (9, 10),     # NSE opens 9:15, check until 10:00
    "us": (19, 20),       # NYSE opens ~19:00 IST, check until 20:00 only
    "europe": (13, 14),   # LSE opens ~13:30 IST, check until 14:00 only
    "japan": (5, 6),      # TSE opens ~5:30 IST, check until 6:00 only
}


async def _check_missed_open_notifications(
    markets: dict[str, bool],
    now: datetime,
    open_data_fetchers: dict,
) -> None:
    """After a restart, check if we missed a market open today and send it."""
    now_ist = now.astimezone(_IST)
    today_str = now_ist.strftime("%Y-%m-%d")

    for market, is_open in markets.items():
        # Only check if market is currently open
        if not is_open:
            continue
        # Check if we're in the window after open
        window = _OPEN_WINDOWS_IST.get(market)
        if not window:
            continue
        earliest, latest = window
        if earliest <= latest:
            in_window = earliest <= now_ist.hour <= latest
        else:
            in_window = now_ist.hour >= earliest or now_ist.hour <= latest

        if not in_window:
            continue

        # Skip holidays
        if market == "us" and is_exchange_holiday("NYSE", now):
            continue
        if market == "europe" and is_exchange_holiday("LSE", now):
            continue
        if market == "japan" and is_exchange_holiday("TSE", now):
            continue

        dedup_key = f"{today_str}_market_open_{market}"

        open_data = None
        fetcher = open_data_fetchers.get(market)
        if fetcher:
            try:
                open_data = await fetcher()
            except Exception:
                logger.warning("Fallback open data fetch failed for %s", market, exc_info=True)

        if open_data is None:
            # Data hasn't landed yet. If we're in the last 5 minutes
            # of the window, send the simple fallback so the user at
            # least knows the market opened. Otherwise retry next tick.
            ist_mins = now_ist.hour * 60 + now_ist.minute
            window_end_mins = latest * 60
            if ist_mins >= window_end_mins - 5:
                logger.info("Missed open for %s — window ending, sending simple fallback", market)
            else:
                logger.debug(
                    "Missed open for %s — data not available yet, will retry",
                    market,
                )
                continue
        else:
            logger.info("Missed open for %s — sending rich notification", market)

        await notification_service.notify_market_open(
            market, market_data=open_data, dedup_key=dedup_key,
        )


# Expected close hours in IST for each market (approximate)
_CLOSE_WINDOWS_IST: dict[str, tuple[int, int]] = {
    # (earliest_close_hour, latest_check_hour) in IST.
    # Wider windows to catch missed notifications after server
    # restarts. The dedup_key (date + market) prevents duplicates.
    "india": (15, 17),    # NSE closes 15:30, check until 17:00
    "us": (1, 6),         # US closes ~1:30 AM IST, check until 6:00 AM
    "europe": (20, 23),   # Europe closes ~20:30 IST, check until 23:00
    "japan": (11, 14),    # TSE closes ~11:30 IST, check until 14:00
}


async def _check_missed_close_notifications(
    markets: dict[str, bool],
    now: datetime,
    close_data_fetchers: dict,
) -> None:
    """After a restart, check if we missed a market close today and send it."""
    now_ist = now.astimezone(_IST)
    today_str = now_ist.strftime("%Y-%m-%d")

    for market, is_open in markets.items():
        # Only check if market is currently closed
        if is_open:
            continue
        # Check if we're in the window after close
        window = _CLOSE_WINDOWS_IST.get(market)
        if not window:
            continue
        earliest, latest = window
        # Handle US which crosses midnight (1-6 AM IST)
        if earliest <= latest:
            in_window = earliest <= now_ist.hour <= latest
        else:
            in_window = now_ist.hour >= earliest or now_ist.hour <= latest

        if not in_window:
            continue

        # Skip holidays — matches the intent of the three-layer India
        # holiday gate in run_notification_job (line 1494). Without this,
        # the outer gate forcing markets["india"] = False on a holiday
        # makes this function think the close notification was missed
        # and fires a fallback, even though the market never opened.
        # Europe/Japan mirror the gates in _check_missed_open_notifications.
        if market == "india" and not get_india_session_info(now).get("is_trading_day"):
            continue

        # India is owned by _check_post_market_summary for the first
        # _POST_MARKET_SETTLE_SECONDS_INDIA after NSE close — that path
        # has the closing-auction freshness gate and the settle delay.
        # If we fire from here instead, we race ahead of the settle and
        # send the last pre-close intraday tick (the "Nifty +1.5% vs
        # Google's +1.63%" bug). Only fall through to this fallback
        # after the settle window when post_market_summary has had a
        # real chance to run; the dedup key inside notify_market_close
        # still prevents a double-send.
        if market == "india":
            total_minutes = now_ist.hour * 60 + now_ist.minute
            post_close_minutes = total_minutes - 930  # 930 = 15:30 IST
            if 0 <= post_close_minutes * 60 < _POST_MARKET_SETTLE_SECONDS_INDIA:
                continue
        if market == "us" and is_exchange_holiday("NYSE", now):
            continue
        if market == "europe" and is_exchange_holiday("LSE", now):
            continue
        if market == "japan" and is_exchange_holiday("TSE", now):
            continue

        # Fetch data and send
        logger.info("Missed close for %s — sending fallback notification", market)
        close_data = None
        fetcher = close_data_fetchers.get(market)
        if fetcher:
            try:
                close_data = await fetcher()
            except Exception:
                logger.warning("Fallback close data fetch failed for %s", market, exc_info=True)

        dedup_key = f"{today_str}_market_close_{market}"
        if market == "india":
            # Route India through the same rich close builder as every
            # other market. _build_india_close() produces the
            # Nifty | Midcap | Smallcap title + smallcap-divergence
            # narrative + sector leaders + 52-week context.
            if close_data:
                try:
                    await notification_service.notify_market_close(
                        "india",
                        market_data=close_data,
                        dedup_key=dedup_key,
                    )
                except Exception:
                    logger.warning("Fallback India close failed", exc_info=True)
        else:
            await notification_service.notify_market_close(
                market, market_data=close_data, dedup_key=dedup_key,
            )


async def run_notification_job() -> None:
    """Check market status and send notifications on state transitions.
    Called every 30 seconds alongside the market scraper."""
    global _prev_state
    try:
        now = datetime.now(timezone.utc)
        today_str = now.astimezone(_IST).strftime("%Y-%m-%d")
        status = get_market_status(now)

        # --- Cleanup old notification_log entries (older than 7 days) ---
        try:
            from app.core.database import get_pool
            pool = await get_pool()
            await pool.execute(
                "DELETE FROM notification_log WHERE sent_at < NOW() - INTERVAL '7 days'"
            )
        except Exception:
            logger.debug("notification_log cleanup failed", exc_info=True)

        markets = {
            "india": bool(status.get("nse_open")),
            "us": bool(status.get("nyse_open")),
            "europe": bool(status.get("europe_open")),
            "japan": bool(status.get("japan_open")),
        }

        # ── Holiday gate (primary defence) ──
        #
        # Force `is_open = False` for any market that's on a holiday today.
        # Upstream data sources occasionally flap a transient "open" state
        # on holidays (stale price feeds, late TTL flushes, etc.). Without
        # this gate, a spurious True→False transition fires the close
        # notification even though the market never actually opened.
        #
        # Europe and Japan already have per-transition holiday gates below,
        # but the issue is they can still build up a false "open" state in
        # _prev_state that triggers on the next tick. Forcing is_open=False
        # AND resetting _prev_state handles both markets cleanly.
        india_info = get_india_session_info(now)
        if not india_info.get("is_trading_day"):
            if markets["india"] or _prev_state.get("india"):
                logger.info(
                    "Holiday gate: India is not a trading day (%s) — "
                    "forcing nse_open=False and resetting prev_state",
                    india_info.get("source"),
                )
            markets["india"] = False
            _prev_state["india"] = False

        if is_exchange_holiday("NYSE", now):
            if markets["us"] or _prev_state.get("us"):
                logger.info(
                    "Holiday gate: NYSE holiday — forcing nyse_open=False "
                    "and resetting prev_state"
                )
            markets["us"] = False
            _prev_state["us"] = False

        if is_exchange_holiday("LSE", now):
            markets["europe"] = False
            _prev_state["europe"] = False

        if is_exchange_holiday("TSE", now):
            markets["japan"] = False
            _prev_state["japan"] = False

        india_closed_transition = False

        _close_data_fetchers = {
            "india": _fetch_india_close_data,
            "us": _fetch_us_close_data,
            "europe": _fetch_europe_close_data,
            "japan": _fetch_japan_close_data,
        }
        _open_data_fetchers = {
            "india": _fetch_india_open_data,
            "japan": _fetch_japan_open_data,
            "europe": _fetch_europe_open_data,
            "us": _fetch_us_open_data,
        }

        for market, is_open in markets.items():
            was_open = _prev_state.get(market)
            if was_open is None:
                # First run — just record state, don't notify
                _prev_state[market] = is_open
                continue
            if is_open and not was_open:
                # Transition: closed -> open — queue the notification and let
                # the flush loop below fire it as soon as the fetcher returns
                # today's row (falling back to a simple notification after
                # _OPEN_DATA_MAX_WAIT_SECONDS if the data never lands).
                logger.info("Market transition: %s OPENED — queuing open notification", market)
                # Skip Europe/Japan notifications on exchange holidays
                if market == "europe" and is_exchange_holiday("LSE", now):
                    logger.info("Skipping Europe open notification — LSE holiday")
                elif market == "japan" and is_exchange_holiday("TSE", now):
                    logger.info("Skipping Japan open notification — TSE holiday")
                else:
                    _open_pending[market] = now
            elif not is_open and was_open:
                # Transition: open -> closed. India routes through
                # _check_post_market_summary (5-minute settle delay, rich
                # builder). US/Europe/Japan get queued onto _close_pending
                # so the flush loop below fires them after
                # _CLOSE_SETTLE_SECONDS — otherwise the daily market_prices
                # row still holds the last pre-close intraday tick instead
                # of the actual closing auction print.
                logger.info("Market transition: %s CLOSED — queuing close notification", market)
                if market == "europe" and is_exchange_holiday("LSE", now):
                    logger.info("Skipping Europe close notification — LSE holiday")
                elif market == "japan" and is_exchange_holiday("TSE", now):
                    logger.info("Skipping Japan close notification — TSE holiday")
                elif market == "india":
                    # India close fires from _check_post_market_summary
                    # on a 5-minute delay so the data (breadth, sectors)
                    # has time to settle. Both paths eventually go
                    # through notify_market_close("india", close_data)
                    # → _build_india_close.
                    india_closed_transition = True
                else:
                    _close_pending[market] = now
            _prev_state[market] = is_open

        # --- Flush pending open notifications when today's data lands ---
        #
        # The per-market fetchers are gated to today's UTC session date
        # (see the _fetch_*_open_data functions), so they return None until
        # the scraper writes today's row. We poll on every notification
        # tick and fire the rich notification the moment the fetcher
        # returns real data, instead of using a fixed delay that could
        # either miss the data (if we fired too early) or silently serve
        # yesterday's numbers (which is how the 2026-04-14 US open
        # notification ended up with 2026-04-13 values before this fix).
        #
        # Hard ceiling: if today's row still hasn't appeared after
        # _OPEN_DATA_MAX_WAIT_SECONDS, fall back to the simple
        # "US Markets Open" banner so the user still learns the
        # transition happened. This is a degraded but honest outcome
        # — it never shows stale numbers labelled as today's.
        for market in list(_open_pending):
            pending_time = _open_pending[market]
            if pending_time is None:
                continue
            elapsed = (now - pending_time).total_seconds()

            open_data = None
            fetcher = _open_data_fetchers.get(market)
            if fetcher:
                try:
                    open_data = await fetcher()
                except Exception:
                    logger.warning("Open data fetch failed for %s", market, exc_info=True)

            if open_data is not None:
                logger.info(
                    "Open data for %s landed after %.0fs — sending rich notification",
                    market, elapsed,
                )
            elif elapsed >= _OPEN_DATA_MAX_WAIT_SECONDS:
                logger.warning(
                    "Open data for %s did not land within %.0fs — "
                    "falling back to simple notification",
                    market, elapsed,
                )
            else:
                # Still waiting for today's row to land — check again next tick.
                continue

            dedup_key = f"{today_str}_market_open_{market}"
            await notification_service.notify_market_open(
                market, market_data=open_data, dedup_key=dedup_key,
            )
            del _open_pending[market]

        # --- Flush pending close notifications after settle delay ---
        #
        # On the open→closed transition we queue _close_pending[market]
        # rather than firing immediately. The flush here waits
        # _CLOSE_SETTLE_SECONDS so the daily market_prices row has time
        # to pick up the closing auction print (insert_prices_batch_upsert_daily
        # drifts on every scraper cycle, so at the exact transition
        # moment it still holds the last pre-close intraday tick). Once
        # the delay elapses we fetch and fire — with whatever data is
        # available at that point. If the fetcher still returns None
        # (upstream broken, holiday edge case, etc.) we fall back to
        # the simple close banner so the user still learns the market
        # closed.
        for market in list(_close_pending):
            pending_time = _close_pending[market]
            if pending_time is None:
                continue
            elapsed = (now - pending_time).total_seconds()
            if elapsed < _CLOSE_SETTLE_SECONDS:
                continue  # still settling

            close_data = None
            fetcher = _close_data_fetchers.get(market)
            if fetcher:
                try:
                    close_data = await fetcher()
                except Exception:
                    logger.warning("Close data fetch failed for %s", market, exc_info=True)

            if close_data is not None:
                logger.info(
                    "Close data for %s settled after %.0fs — sending rich notification",
                    market, elapsed,
                )
            else:
                logger.warning(
                    "Close data for %s unavailable %.0fs after close — "
                    "falling back to simple notification",
                    market, elapsed,
                )

            dedup_key = f"{today_str}_market_close_{market}"
            await notification_service.notify_market_close(
                market, market_data=close_data, dedup_key=dedup_key,
            )
            del _close_pending[market]

        # --- DB fallback: send missed open/close notifications after restart ---
        await _check_missed_open_notifications(markets, now, _open_data_fetchers)
        await _check_missed_close_notifications(markets, now, _close_data_fetchers)

        # --- Pre-market summary (8:58-9:05 AM IST) ---
        await _check_pre_market_summary(status, now)

        # --- Gift Nifty alert ---
        await _check_gift_nifty(status, now)

        # --- FII/DII alert ---
        await _check_fii_dii(now)

        # --- Commodity spikes ---
        await _check_commodity_spikes(now)

        # --- Post-market summary ---
        await _check_post_market_summary(now, india_closed_transition)

    except Exception:
        logger.exception("Notification job failed")
