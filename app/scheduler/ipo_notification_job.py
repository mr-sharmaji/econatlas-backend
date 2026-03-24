"""Background job: send per-device IPO alert notifications.

Checks IPO date milestones and sends targeted push notifications to
devices that have alerts enabled for each IPO symbol.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from app.services import notification_service

logger = logging.getLogger(__name__)

_IST = ZoneInfo("Asia/Kolkata")


async def run_ipo_notification_job() -> None:
    """Main entry point — called by the ARQ task wrapper."""
    from app.core.database import get_pool

    pool = await get_pool()

    now_ist = datetime.now(_IST)
    today = now_ist.date()
    tomorrow = today + timedelta(days=1)

    events = await _collect_ipo_events(pool, today, tomorrow)
    if not events:
        logger.debug("IPO notification check: no events to notify")
        return

    total_sent = 0
    for event in events:
        sent = await _send_event_notifications(pool, event)
        total_sent += sent

    logger.info(
        "IPO notification job: %d events, %d notifications sent",
        len(events),
        total_sent,
    )


async def _collect_ipo_events(pool, today: date, tomorrow: date) -> list[dict]:
    """Query ipo_snapshots for date-based milestones and return event dicts."""
    events: list[dict] = []

    # 1. Opens tomorrow
    rows = await pool.fetch(
        """
        SELECT symbol, company_name, price_band, gmp_percent
        FROM ipo_snapshots
        WHERE open_date = $1 AND archived_at IS NULL
        """,
        tomorrow,
    )
    for r in rows:
        price_band = r["price_band"] or "N/A"
        events.append({
            "symbol": r["symbol"],
            "event": "opens_tomorrow",
            "title": f"\U0001f4cb {r['company_name']} IPO opens tomorrow",
            "body": f"Price band {price_band}",
            "data": {"type": "ipo_alert", "symbol": r["symbol"], "event": "opens_tomorrow"},
        })

    # 2. Opens today
    rows = await pool.fetch(
        """
        SELECT symbol, company_name, price_band
        FROM ipo_snapshots
        WHERE open_date = $1 AND archived_at IS NULL
        """,
        today,
    )
    for r in rows:
        price_band = r["price_band"] or "N/A"
        events.append({
            "symbol": r["symbol"],
            "event": "opens_today",
            "title": f"\U0001f7e2 {r['company_name']} IPO is now open for subscription",
            "body": f"Price band {price_band}",
            "data": {"type": "ipo_alert", "symbol": r["symbol"], "event": "opens_today"},
        })

    # 3. Closes today (last day to apply)
    rows = await pool.fetch(
        """
        SELECT symbol, company_name, subscription_multiple
        FROM ipo_snapshots
        WHERE close_date = $1 AND archived_at IS NULL
        """,
        today,
    )
    for r in rows:
        sub = r["subscription_multiple"]
        sub_text = f" \u2014 {sub:.1f}x subscribed" if sub else ""
        events.append({
            "symbol": r["symbol"],
            "event": "closes_today",
            "title": f"\u23f0 Last day to apply for {r['company_name']} IPO",
            "body": f"Subscription closes today{sub_text}",
            "data": {"type": "ipo_alert", "symbol": r["symbol"], "event": "closes_today"},
        })

    # 4. Listing today
    rows = await pool.fetch(
        """
        SELECT symbol, company_name, gmp_percent
        FROM ipo_snapshots
        WHERE listing_date = $1
          AND (outcome_state IS NULL OR outcome_state != 'listed')
          AND archived_at IS NULL
        """,
        today,
    )
    for r in rows:
        gmp = r["gmp_percent"]
        gmp_text = f" \u2014 GMP {gmp:+.1f}%" if gmp is not None else ""
        events.append({
            "symbol": r["symbol"],
            "event": "listing_today",
            "title": f"\U0001f4c8 {r['company_name']} lists today",
            "body": f"Listing expected today{gmp_text}",
            "data": {"type": "ipo_alert", "symbol": r["symbol"], "event": "listing_today"},
        })

    # 5. Listed (outcome_state = 'listed' and listing_date = today)
    rows = await pool.fetch(
        """
        SELECT symbol, company_name, listing_price, listing_gain_pct, price_band
        FROM ipo_snapshots
        WHERE listing_date = $1
          AND outcome_state = 'listed'
          AND listing_price IS NOT NULL
          AND archived_at IS NULL
        """,
        today,
    )
    for r in rows:
        lp = r["listing_price"]
        gain = r["listing_gain_pct"]
        gain_text = f" \u2014 {gain:+.1f}% from issue price" if gain is not None else ""
        events.append({
            "symbol": r["symbol"],
            "event": "listed",
            "title": f"\U0001f389 {r['company_name']} listed at \u20b9{lp:,.0f}",
            "body": f"Listing complete{gain_text}",
            "data": {"type": "ipo_alert", "symbol": r["symbol"], "event": "listed"},
        })

    return events


async def _send_event_notifications(pool, event: dict) -> int:
    """For one IPO event, find subscribed devices, dedup, and send."""
    symbol = event["symbol"]
    event_name = event["event"]
    today_str = datetime.now(_IST).strftime("%Y-%m-%d")
    dedup_key = f"ipo_{event_name}_{symbol}_{today_str}"

    # Check dedup
    if await notification_service._was_already_sent(dedup_key):
        logger.debug("IPO notification already sent: %s", dedup_key)
        return 0

    # Get FCM tokens for devices that have alerts for this symbol
    rows = await pool.fetch(
        """
        SELECT dt.fcm_token
        FROM device_ipo_alerts dia
        JOIN device_tokens dt ON dt.device_id = dia.device_id
        WHERE dia.symbol = $1
        """,
        symbol,
    )
    if not rows:
        logger.debug("No devices subscribed to IPO alerts for %s", symbol)
        return 0

    fcm_tokens = [r["fcm_token"] for r in rows]
    logger.info(
        "Sending IPO %s notification for %s to %d devices",
        event_name,
        symbol,
        len(fcm_tokens),
    )

    sent = await notification_service.send_to_devices(
        fcm_tokens=fcm_tokens,
        title=event["title"],
        body=event["body"],
        data=event.get("data"),
    )

    # Log for dedup
    if sent > 0:
        await notification_service._log_sent(
            notification_type=f"ipo_{event_name}",
            dedup_key=dedup_key,
            title=event["title"],
        )

    return sent
