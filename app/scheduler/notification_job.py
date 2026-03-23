"""Background job: detect market state transitions and send push notifications."""
import asyncio
import logging

from app.services import notification_service
from app.scheduler.trading_calendar import get_market_status

logger = logging.getLogger(__name__)

# Track previous state to detect transitions
_prev_state: dict[str, bool] = {}


async def run_notification_job() -> None:
    """Check market status and send notifications on state transitions.
    Called every 30 seconds alongside the market scraper."""
    global _prev_state
    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        status = get_market_status(now)

        markets = {
            "india": bool(status.get("nse_open")),
            "us": bool(status.get("nyse_open")),
            "europe": bool(status.get("europe_open")),
            "japan": bool(status.get("japan_open")),
        }

        for market, is_open in markets.items():
            was_open = _prev_state.get(market)
            if was_open is None:
                # First run — just record state, don't notify
                _prev_state[market] = is_open
                continue
            if is_open and not was_open:
                # Transition: closed → open
                logger.info("Market transition: %s OPENED", market)
                await notification_service.notify_market_open(market)
            elif not is_open and was_open:
                # Transition: open → closed
                logger.info("Market transition: %s CLOSED", market)
                await notification_service.notify_market_close(market)
            _prev_state[market] = is_open

    except Exception:
        logger.exception("Notification job failed")
