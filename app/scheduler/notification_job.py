"""Background job: detect market state transitions and send push notifications."""
import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services import notification_service
from app.scheduler.trading_calendar import get_market_status, is_exchange_holiday

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")

# Track previous state to detect transitions
_prev_state: dict[str, bool] = {}

# Track which close/open notifications we've sent today (survives missed transitions)
_close_sent_today: dict[str, str | None] = {}  # market -> date string
_open_sent_today: dict[str, str | None] = {}   # market -> date string

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

# Open notification delay state — wait ~2 min after open for data to arrive
_open_pending: dict[str, datetime | None] = {}  # market -> transition timestamp

# Post-market summary state
_post_market_state: dict = {
    "last_date": None,
    "pending": False,
    "close_time": None,
}


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

        # Nifty 50, Midcap 150, Smallcap 250 latest
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('Nifty 50', 'Nifty Midcap 150', 'Nifty Smallcap 250')
            ORDER BY timestamp DESC
            LIMIT 6
            """
        )
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

        # Sector indices
        sector_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('Nifty Bank', 'Nifty IT', 'Nifty Pharma', 'Nifty Auto', 'Nifty Metal')
              AND change_percent IS NOT NULL
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

        return data
    except Exception:
        logger.exception("Failed to fetch India close data")
        return None


async def _fetch_us_close_data() -> dict | None:
    """Fetch data needed for US market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'Dow Jones')
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

        return data
    except Exception:
        logger.exception("Failed to fetch US close data")
        return None


async def _fetch_europe_close_data() -> dict | None:
    """Fetch data needed for Europe market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('FTSE 100', 'DAX', 'CAC 40')
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

        # Brent crude
        brent_row = await pool.fetchrow(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = 'brent crude' AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if brent_row:
            data["brent_change_pct"] = float(brent_row["change_percent"])

        return data
    except Exception:
        logger.exception("Failed to fetch Europe close data")
        return None


async def _fetch_japan_close_data() -> dict | None:
    """Fetch data needed for Japan market close notification."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('Nikkei 225', 'TOPIX')
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

        # JPY/INR
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

        return data
    except Exception:
        logger.exception("Failed to fetch Japan close data")
        return None


# ---------------------------------------------------------------------------
# Data fetcher for market open notifications
# ---------------------------------------------------------------------------

async def _fetch_india_open_data() -> dict | None:
    """Fetch data for India market open: Gift Nifty + overnight US/Asia."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Nifty 50 last close
        nifty_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not nifty_row:
            return None
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

        return data
    except Exception:
        logger.exception("Failed to fetch India open data")
        return None


async def _fetch_japan_open_data() -> dict | None:
    """Fetch data for Japan market open: Nikkei/TOPIX + overnight US + FTSE + USD/JPY + gold."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Nikkei 225 and TOPIX latest
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('Nikkei 225', 'TOPIX')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 4
            """,
        )
        data: dict = {}
        seen: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"])
            if a == "Nikkei 225":
                data["nikkei_pct"] = pct
            elif a == "TOPIX":
                data["topix_pct"] = pct

        if "nikkei_pct" not in data:
            return None

        # Overnight US + FTSE (previous close) + gold
        global_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'FTSE 100', 'gold')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 8
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
            elif a == "FTSE 100":
                data["ftse_pct"] = pct
            elif a == "gold":
                data["gold_pct"] = pct

        # USD/JPY
        jpy_row = await pool.fetchrow(
            """
            SELECT price, change_percent FROM market_prices
            WHERE asset = 'USD/JPY'
            ORDER BY timestamp DESC
            LIMIT 1
            """,
        )
        if jpy_row and jpy_row["price"] is not None:
            data["usd_jpy_price"] = float(jpy_row["price"])

        return data
    except Exception:
        logger.exception("Failed to fetch Japan open data")
        return None


async def _fetch_europe_open_data() -> dict | None:
    """Fetch data for Europe market open: FTSE/DAX/CAC + Asia + Brent crude."""
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # FTSE 100, DAX, CAC 40 latest
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('FTSE 100', 'DAX', 'CAC 40')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 6
            """,
        )
        data: dict = {}
        seen: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"])
            if a == "FTSE 100":
                data["ftse_pct"] = pct
            elif a == "DAX":
                data["dax_pct"] = pct
            elif a == "CAC 40":
                data["cac_pct"] = pct

        if "ftse_pct" not in data and "dax_pct" not in data:
            return None

        # Asia cues: Nikkei + Nifty 50
        asia_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('Nikkei 225', 'Nifty 50')
              AND change_percent IS NOT NULL
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

        # Brent crude
        brent_row = await pool.fetchrow(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = 'brent crude' AND change_percent IS NOT NULL
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

        # S&P500, NASDAQ, Dow Jones latest
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent, price FROM market_prices
            WHERE asset IN ('S&P500', 'NASDAQ', 'Dow Jones')
              AND change_percent IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 6
            """,
        )
        data: dict = {}
        seen: set[str] = set()
        for row in index_rows:
            a = row["asset"]
            if a in seen:
                continue
            seen.add(a)
            pct = float(row["change_percent"])
            if a == "S&P500":
                data["sp500_pct"] = pct
            elif a == "NASDAQ":
                data["nasdaq_pct"] = pct
            elif a == "Dow Jones":
                data["dow_pct"] = pct

        if "sp500_pct" not in data:
            return None

        # Europe cues: FTSE + DAX
        europe_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('FTSE 100', 'DAX')
              AND change_percent IS NOT NULL
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

        # Crude oil
        crude_row = await pool.fetchrow(
            """
            SELECT change_percent FROM market_prices
            WHERE asset = 'crude oil' AND change_percent IS NOT NULL
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

        # Previous Nifty 50 daily close
        nifty_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not nifty_row:
            return

        gift_price = float(gift_row["price"])
        nifty_close = float(nifty_row["price"])
        if nifty_close == 0:
            return

        change_pct = (gift_price - nifty_close) / nifty_close * 100
        current_band = int(change_pct * 2) / 2  # 0.5% bands

        if abs(change_pct) > 0.5 and current_band != _gift_nifty_state.get("last_band"):
            logger.info(
                "Gift Nifty alert: %.1f%% (price=%.0f, nifty_close=%.0f, band=%.1f)",
                change_pct, gift_price, nifty_close, current_band,
            )
            await notification_service.notify_gift_nifty_move(change_pct, gift_price)
            _gift_nifty_state["last_band"] = current_band

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

        if latest_date != _fii_dii_state.get("last_date"):
            logger.info("FII/DII alert: fii=%.0f, dii=%.0f, date=%s", fii_net, dii_net, latest_date)
            await notification_service.notify_fii_dii_data(fii_net, dii_net)
            _fii_dii_state["last_date"] = latest_date

        return fii_net, dii_net

    except Exception:
        logger.exception("FII/DII check failed")
        return None, None


async def _check_pre_market_summary(status: dict, now: datetime) -> None:
    """Send pre-market summary between 8:58-9:05 AM IST on trading days."""
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

    # Only fire between 8:58 and 9:05 IST
    total_minutes = now_ist.hour * 60 + now_ist.minute
    if total_minutes < 538 or total_minutes > 545:  # 8:58 to 9:05
        return

    # Already sent today
    if _pre_market_state.get("last_date") == today:
        return

    # Only send if Gift Nifty is open (confirms it's a trading day)
    if not status.get("gift_nifty_open"):
        return

    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Previous Nifty 50 daily close
        nifty_close_row = await pool.fetchrow(
            """
            SELECT price FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
        if not nifty_close_row:
            return
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
            if a in ("S&P500", "Dow Jones", "NASDAQ"):
                us_change[a] = pct
            else:
                asia_change[a] = pct

        logger.info(
            "Pre-market summary: gift_nifty=%.0f (%.1f%%), us=%s, asia=%s",
            gift_price, gift_change_pct, us_change, asia_change,
        )

        await notification_service.notify_pre_market_summary(
            gift_nifty_price=gift_price,
            gift_nifty_change_pct=gift_change_pct,
            us_change=us_change or None,
            asia_change=asia_change or None,
        )

        _pre_market_state["last_date"] = today

    except Exception:
        logger.exception("Pre-market summary check failed")


async def _check_commodity_spikes(now: datetime) -> None:
    """Check for commodity price spikes (±2% from previous close)."""
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

    # Reset alerted state on new day
    if _commodity_spike_state.get("last_date") != today:
        _commodity_spike_state["last_date"] = today
        _commodity_spike_state["alerted"] = {}

    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Get latest commodity prices with significant moves (today only)
        rows = await pool.fetch(
            """
            SELECT DISTINCT ON (asset) asset, price, change_percent, unit
            FROM market_prices
            WHERE instrument_type = 'commodity'
              AND change_percent IS NOT NULL
              AND ABS(change_percent) >= 2.0
              AND date >= $1
            ORDER BY asset, date DESC
            """,
            today,
        )

        if not rows:
            return

        alerted = _commodity_spike_state["alerted"]

        for row in rows:
            asset = row["asset"]
            change_pct = float(row["change_percent"])
            price = float(row["price"])
            unit = row.get("unit")

            # Track in 2% bands to avoid spamming
            band = int(change_pct / 2) * 2
            if alerted.get(asset) == band:
                continue

            display_name = asset.replace("_", " ").title()

            logger.info(
                "Commodity spike: %s %.1f%% at %.2f",
                asset, change_pct, price,
            )
            await notification_service.notify_commodity_spike(
                asset=asset,
                display_name=display_name,
                change_pct=change_pct,
                price=price,
                unit=unit,
            )
            alerted[asset] = band

    except Exception:
        logger.exception("Commodity spike check failed")


async def _check_post_market_summary(now: datetime, india_closed_transition: bool) -> None:
    """Send post-market summary ~5 min after NSE close (survives restarts).

    Uses transition detection when available. Falls back to DB-based detection:
    if India is closed, today is a trading day, and we haven't sent yet, fire.
    """
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

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
        # Waiting for 5-minute delay after live transition
        close_time = _post_market_state.get("close_time")
        if close_time and (now - close_time).total_seconds() < 300:
            return
    else:
        # No live transition detected (e.g., server restarted after close).
        # Fallback: check if India is closed and today's close data exists in DB.
        from app.scheduler.trading_calendar import get_market_status
        status = get_market_status(now)
        if status.get("india_open"):
            return  # Market still open

        # Only fire within 1 hour after market close (~15:30-16:30 IST)
        total_minutes = now_ist.hour * 60 + now_ist.minute
        if total_minutes > 990:  # 16:30 IST = 990 minutes
            return

        # Check if today had a trading session by looking for today's data
        try:
            from app.core.database import get_pool
            pool = await get_pool()
            has_today = await pool.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM market_prices
                    WHERE asset = 'Nifty 50' AND date = $1
                )
                """,
                today,
            )
            if not has_today:
                return  # Not a trading day or data not yet available
        except Exception:
            logger.exception("Post-market fallback check failed")
            return

    try:
        from app.core.database import get_pool
        from app.services.brief_service import get_post_market_overview

        pool = await get_pool()

        # Get Nifty 50 and Sensex change percent
        index_rows = await pool.fetch(
            """
            SELECT asset, change_percent FROM market_prices
            WHERE asset IN ('Nifty 50', 'Sensex')
            ORDER BY timestamp DESC
            LIMIT 2
            """
        )

        nifty_change = 0.0
        sensex_change = 0.0
        for row in index_rows:
            if row["asset"] == "Nifty 50" and row["change_percent"] is not None:
                nifty_change = float(row["change_percent"])
            elif row["asset"] == "Sensex" and row["change_percent"] is not None:
                sensex_change = float(row["change_percent"])

        # Get breadth data
        overview = await get_post_market_overview(market="IN")
        advancers = overview.get("advancers", 0)
        decliners = overview.get("decliners", 0)
        top_sector = overview.get("top_sector")
        bottom_sector = overview.get("bottom_sector")

        logger.info(
            "Post-market summary: nifty=%.1f%%, sensex=%.1f%%, adv=%d, dec=%d, top=%s, bottom=%s",
            nifty_change, sensex_change, advancers, decliners, top_sector, bottom_sector,
        )

        await notification_service.notify_post_market_summary(
            nifty_change_pct=nifty_change,
            sensex_change_pct=sensex_change,
            advancers=advancers,
            decliners=decliners,
            top_sector=top_sector,
            bottom_sector=bottom_sector,
        )

        _post_market_state["pending"] = False
        _post_market_state["last_date"] = today
        _close_sent_today["india"] = str(today)

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
        # Already sent today
        if _open_sent_today.get(market) == today_str:
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
        if market == "europe" and is_exchange_holiday("LSE", now):
            continue
        if market == "japan" and is_exchange_holiday("TSE", now):
            continue

        logger.info("Missed open for %s — sending fallback notification", market)
        open_data = None
        fetcher = open_data_fetchers.get(market)
        if fetcher:
            try:
                open_data = await fetcher()
            except Exception:
                logger.warning("Fallback open data fetch failed for %s", market, exc_info=True)

        await notification_service.notify_market_open(market, market_data=open_data)
        _open_sent_today[market] = today_str


# Expected close hours in IST for each market (approximate)
_CLOSE_WINDOWS_IST: dict[str, tuple[int, int]] = {
    # (earliest_close_hour, latest_check_hour) in IST — narrow windows to avoid duplicates
    "india": (15, 16),    # NSE closes 15:30, check until 16:00
    "us": (1, 2),         # US closes ~1:30 AM IST, check until 2:00 only
    "europe": (20, 21),   # Europe closes ~20:30 IST, check until 21:00 only
    "japan": (11, 12),    # TSE closes ~11:30 IST, check until 12:00 only
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
        # Already sent today
        if _close_sent_today.get(market) == today_str:
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

        # Fetch data and send
        logger.info("Missed close for %s — sending fallback notification", market)
        close_data = None
        fetcher = close_data_fetchers.get(market)
        if fetcher:
            try:
                close_data = await fetcher()
            except Exception:
                logger.warning("Fallback close data fetch failed for %s", market, exc_info=True)

        if market == "india":
            # Use post-market summary path
            if close_data:
                try:
                    await notification_service.notify_post_market_summary(
                        nifty_change_pct=close_data.get("nifty_change_pct", 0),
                        sensex_change_pct=close_data.get("sensex_change_pct", 0),
                        advancers=close_data.get("advancers", 0),
                        decliners=close_data.get("decliners", 0),
                        top_sector=close_data.get("top_sector"),
                        bottom_sector=close_data.get("bottom_sector"),
                    )
                except Exception:
                    logger.warning("Fallback India post-market failed", exc_info=True)
        else:
            await notification_service.notify_market_close(market, market_data=close_data)

        _close_sent_today[market] = today_str


async def run_notification_job() -> None:
    """Check market status and send notifications on state transitions.
    Called every 30 seconds alongside the market scraper."""
    global _prev_state
    try:
        now = datetime.now(timezone.utc)
        status = get_market_status(now)

        markets = {
            "india": bool(status.get("nse_open")),
            "us": bool(status.get("nyse_open")),
            "europe": bool(status.get("europe_open")),
            "japan": bool(status.get("japan_open")),
        }

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
                # Transition: closed -> open — delay 2 min so early data is available
                logger.info("Market transition: %s OPENED — queuing open notification", market)
                # Skip Europe/Japan notifications on exchange holidays
                if market == "europe" and is_exchange_holiday("LSE", now):
                    logger.info("Skipping Europe open notification — LSE holiday")
                elif market == "japan" and is_exchange_holiday("TSE", now):
                    logger.info("Skipping Japan open notification — TSE holiday")
                else:
                    _open_pending[market] = now
                    _open_sent_today[market] = now.astimezone(_IST).strftime("%Y-%m-%d")
            elif not is_open and was_open:
                # Transition: open -> closed
                logger.info("Market transition: %s CLOSED", market)
                # Skip Europe/Japan notifications on exchange holidays
                if market == "europe" and is_exchange_holiday("LSE", now):
                    logger.info("Skipping Europe close notification — LSE holiday")
                elif market == "japan" and is_exchange_holiday("TSE", now):
                    logger.info("Skipping Japan close notification — TSE holiday")
                else:
                    close_data = None
                    fetcher = _close_data_fetchers.get(market)
                    if fetcher:
                        try:
                            close_data = await fetcher()
                        except Exception:
                            logger.warning("Close data fetch failed for %s", market, exc_info=True)
                    if market == "india":
                        # Skip generic close — post-market summary handles India
                        india_closed_transition = True
                    else:
                        await notification_service.notify_market_close(market, market_data=close_data)
                    _close_sent_today[market] = now.astimezone(_IST).strftime("%Y-%m-%d")
            _prev_state[market] = is_open

        # --- Flush pending open notifications after 2-minute delay ---
        for market in list(_open_pending):
            pending_time = _open_pending[market]
            if pending_time and (now - pending_time).total_seconds() >= 120:
                logger.info("Open notification delay elapsed for %s — sending now", market)
                open_data = None
                fetcher = _open_data_fetchers.get(market)
                if fetcher:
                    try:
                        open_data = await fetcher()
                    except Exception:
                        logger.warning("Open data fetch failed for %s", market, exc_info=True)
                await notification_service.notify_market_open(market, market_data=open_data)
                del _open_pending[market]

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
