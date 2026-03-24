"""Background job: detect market state transitions and send push notifications."""
import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from app.services import notification_service
from app.scheduler.trading_calendar import get_market_status

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
    "alerted": {},  # asset -> last alerted band (3% bands)
}

# Post-market summary state
_post_market_state: dict = {
    "last_date": None,
    "pending": False,
    "close_time": None,
}


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
            SELECT close FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY date DESC
            LIMIT 1
            """
        )
        if not nifty_row:
            return

        gift_price = float(gift_row["price"])
        nifty_close = float(nifty_row["close"])
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
            SELECT indicator_name, value, timestamp
            FROM macro_indicators
            WHERE indicator_name IN ('fii_net_cash', 'dii_net_cash')
              AND unit = 'inr_cr'
            ORDER BY timestamp DESC
            LIMIT 2
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

        if latest_date != _fii_dii_state.get("last_date"):
            logger.info("FII/DII alert: fii=%.0f, dii=%.0f, date=%s", fii_net, dii_net, latest_date)
            await notification_service.notify_fii_dii_data(fii_net, dii_net)
            _fii_dii_state["last_date"] = latest_date

        return fii_net, dii_net

    except Exception:
        logger.exception("FII/DII check failed")
        return None, None


async def _check_pre_market_summary(status: dict, now: datetime) -> None:
    """Send pre-market summary at ~9:10 AM IST on trading days."""
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
            SELECT close FROM market_prices
            WHERE asset = 'Nifty 50'
            ORDER BY date DESC
            LIMIT 1
            """
        )
        if not nifty_close_row:
            return
        nifty_close = float(nifty_close_row["close"])
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
            WHERE asset IN ('S&P 500', 'Dow Jones', 'NASDAQ', 'Nikkei 225', 'Hang Seng')
              AND change_percent IS NOT NULL
            ORDER BY date DESC
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
            if a in ("S&P 500", "Dow Jones", "NASDAQ"):
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
    """Check for commodity price spikes (±3% from previous close)."""
    now_ist = now.astimezone(_IST)
    today = now_ist.date()

    # Reset alerted state on new day
    if _commodity_spike_state.get("last_date") != today:
        _commodity_spike_state["last_date"] = today
        _commodity_spike_state["alerted"] = {}

    try:
        from app.core.database import get_pool
        pool = await get_pool()

        # Get latest commodity prices with significant moves
        rows = await pool.fetch(
            """
            SELECT DISTINCT ON (asset) asset, price, change_percent, unit
            FROM market_prices
            WHERE instrument_type = 'commodity'
              AND change_percent IS NOT NULL
              AND ABS(change_percent) >= 3.0
            ORDER BY asset, date DESC
            """
        )

        if not rows:
            return

        alerted = _commodity_spike_state["alerted"]

        for row in rows:
            asset = row["asset"]
            change_pct = float(row["change_percent"])
            price = float(row["price"])
            unit = row.get("unit")

            # Track in 3% bands to avoid spamming
            band = int(change_pct / 3) * 3
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
    """Send post-market summary 5 minutes after NSE close."""
    if india_closed_transition:
        _post_market_state["pending"] = True
        _post_market_state["close_time"] = now
        logger.info("Post-market summary pending — will send after 5 minutes")
        return

    if not _post_market_state.get("pending"):
        return

    close_time = _post_market_state.get("close_time")
    if close_time is None:
        return

    elapsed = (now - close_time).total_seconds()
    if elapsed < 300:  # 5 minutes
        return

    today = now.astimezone(_IST).date()
    if _post_market_state.get("last_date") == today:
        _post_market_state["pending"] = False
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
            ORDER BY date DESC
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

    except Exception:
        logger.exception("Post-market summary check failed")


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

        for market, is_open in markets.items():
            was_open = _prev_state.get(market)
            if was_open is None:
                # First run — just record state, don't notify
                _prev_state[market] = is_open
                continue
            if is_open and not was_open:
                # Transition: closed -> open
                logger.info("Market transition: %s OPENED", market)
                await notification_service.notify_market_open(market)
            elif not is_open and was_open:
                # Transition: open -> closed
                logger.info("Market transition: %s CLOSED", market)
                await notification_service.notify_market_close(market)
                if market == "india":
                    india_closed_transition = True
            _prev_state[market] = is_open

        # --- Pre-market summary (9:10 AM IST) ---
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
