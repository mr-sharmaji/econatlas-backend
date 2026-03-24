"""FCM push notification service.

Uses topic-based messaging (no device token storage needed for v1).
All devices subscribe to 'market_alerts' topic via the Flutter app.
"""
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_firebase_app = None


def _get_firebase():
    """Lazy-init Firebase Admin SDK."""
    global _firebase_app
    if _firebase_app is not None:
        return _firebase_app
    try:
        import firebase_admin
        from firebase_admin import credentials
        try:
            _firebase_app = firebase_admin.get_app()
        except ValueError:
            cred_value = os.environ.get("FIREBASE_CREDENTIALS_JSON") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_value:
                # Support both inline JSON string and file path
                stripped = cred_value.strip()
                if stripped.startswith("{"):
                    cred = credentials.Certificate(json.loads(stripped))
                else:
                    cred = credentials.Certificate(stripped)
                _firebase_app = firebase_admin.initialize_app(cred)
            else:
                _firebase_app = firebase_admin.initialize_app()
        logger.info("Firebase Admin SDK initialized")
        return _firebase_app
    except Exception as e:
        logger.error("Firebase Admin SDK init failed: %s", e, exc_info=True)
        raise


async def send_topic_notification(
    topic: str,
    title: str,
    body: str,
    data: dict | None = None,
) -> bool:
    """Send a push notification to all devices subscribed to a topic."""
    app = _get_firebase()
    if app is None:
        logger.debug("Skipping push notification — Firebase not configured")
        return False
    try:
        from firebase_admin import messaging
        message = messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=data or {},
            topic=topic,
        )
        response = messaging.send(message)
        logger.info("FCM sent to topic=%s: %s", topic, response)
        return True
    except Exception:
        logger.warning("FCM send failed for topic=%s", topic, exc_info=True)
        return False


async def notify_market_open(market: str) -> bool:
    """Send notification when a market opens."""
    titles = {
        "india": "\U0001f1ee\U0001f1f3 Indian Markets Open",
        "us": "\U0001f1fa\U0001f1f8 US Markets Open",
        "europe": "\U0001f1ea\U0001f1fa European Markets Open",
        "japan": "\U0001f1ef\U0001f1f5 Japanese Markets Open",
    }
    bodies = {
        "india": "NSE & BSE are now live. Nifty 50 and Sensex trading has begun.",
        "us": "NYSE & NASDAQ are now live. S&P 500 and Dow Jones trading has begun.",
        "europe": "FTSE, DAX, and CAC 40 are now live.",
        "japan": "Nikkei 225 and TOPIX are now live.",
    }
    title = titles.get(market, f"{market.title()} Markets Open")
    body = bodies.get(market, f"{market.title()} market session has started.")
    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={"type": "market_open", "market": market},
    )


def _format_inr(value: float) -> str:
    """Format a number with Indian comma style, e.g. 2100 -> '2,100'."""
    abs_val = abs(value)
    int_part = int(abs_val)
    s = f"{int_part:,}"
    return s


async def notify_gift_nifty_move(change_pct: float, price: float) -> bool:
    """Send notification when Gift Nifty moves >0.5% from previous NSE close."""
    emoji = "\U0001f7e2" if change_pct >= 0 else "\U0001f534"
    sign = "+" if change_pct >= 0 else ""
    title = f"{emoji} Gift Nifty {sign}{change_pct:.1f}%"

    formatted_price = f"{price:,.0f}"
    magnitude = "significant " if abs(change_pct) >= 1.5 else ""
    direction = "bullish" if change_pct >= 0 else "bearish"
    body = f"Trading at {formatted_price} — {magnitude}{direction} signal for market open"

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={
            "type": "gift_nifty_alert",
            "change_pct": f"{change_pct:.1f}",
            "price": f"{price:.0f}",
        },
    )


async def notify_fii_dii_data(fii_net: float, dii_net: float) -> bool:
    """Send notification with FII/DII activity update."""
    title = "\U0001f4ca FII/DII Activity Update"

    fii_action = "bought" if fii_net >= 0 else "sold"
    dii_action = "bought" if dii_net >= 0 else "sold"
    net = fii_net + dii_net
    net_sign = "+" if net >= 0 else "-"

    body = (
        f"FIIs {fii_action} \u20b9{_format_inr(fii_net)} Cr"
        f" | DIIs {dii_action} \u20b9{_format_inr(dii_net)} Cr"
        f" | Net: {net_sign}\u20b9{_format_inr(net)} Cr"
    )

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={
            "type": "fii_dii_data",
            "fii_net": f"{fii_net:.0f}",
            "dii_net": f"{dii_net:.0f}",
        },
    )


async def notify_post_market_summary(
    nifty_change_pct: float,
    sensex_change_pct: float,
    advancers: int,
    decliners: int,
    top_sector: str | None,
    bottom_sector: str | None,
) -> bool:
    """Send post-market summary notification."""
    n_sign = "+" if nifty_change_pct >= 0 else ""
    s_sign = "+" if sensex_change_pct >= 0 else ""
    emoji = "\U0001f4c8" if nifty_change_pct >= 0 else "\U0001f4c9"
    title = f"{emoji} Nifty {n_sign}{nifty_change_pct:.1f}% | Sensex {s_sign}{sensex_change_pct:.1f}%"

    # One-line market commentary
    total = advancers + decliners
    breadth_ratio = advancers / total if total > 0 else 0.5
    abs_nifty = abs(nifty_change_pct)

    if abs_nifty < 0.2:
        tone = "Flat session"
    elif nifty_change_pct > 0:
        tone = "Strong rally" if abs_nifty >= 1.5 else "Positive session"
    else:
        tone = "Sharp selloff" if abs_nifty >= 1.5 else "Weak session"

    # Breadth context
    if breadth_ratio >= 0.7:
        breadth = "broad-based buying"
    elif breadth_ratio <= 0.3:
        breadth = "broad-based selling"
    else:
        breadth = "mixed breadth"

    # Sector leaders/laggards
    sector_parts = []
    if top_sector and nifty_change_pct >= 0:
        sector_parts.append(f"{top_sector} led gains")
    elif bottom_sector and nifty_change_pct < 0:
        sector_parts.append(f"{bottom_sector} dragged")
    if top_sector and bottom_sector and top_sector != bottom_sector:
        if nifty_change_pct >= 0 and bottom_sector:
            sector_parts.append(f"{bottom_sector} lagged")
        elif nifty_change_pct < 0 and top_sector:
            sector_parts.append(f"{top_sector} held up")

    sector_text = ", ".join(sector_parts[:2])
    commentary = f"{tone} with {breadth}. {advancers} advancers, {decliners} decliners."
    if sector_text:
        commentary += f" {sector_text}."

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=commentary,
        data={"type": "post_market_summary"},
    )


async def notify_pre_market_summary(
    gift_nifty_price: float,
    gift_nifty_change_pct: float,
    us_change: dict | None = None,
    europe_change: dict | None = None,
    asia_change: dict | None = None,
) -> bool:
    """Send pre-market summary at ~9:00 AM IST with Gift Nifty + global cues."""
    sign = "+" if gift_nifty_change_pct >= 0 else ""
    emoji = "\U0001f7e2" if gift_nifty_change_pct >= 0 else "\U0001f534"
    title = f"{emoji} Gift Nifty {sign}{gift_nifty_change_pct:.1f}% at {gift_nifty_price:,.0f}"

    # Build global cues
    cues = []
    if us_change:
        for idx, pct in us_change.items():
            s = "+" if pct >= 0 else ""
            cues.append(f"{idx} {s}{pct:.1f}%")
    if europe_change:
        for idx, pct in europe_change.items():
            s = "+" if pct >= 0 else ""
            cues.append(f"{idx} {s}{pct:.1f}%")
    if asia_change:
        for idx, pct in asia_change.items():
            s = "+" if pct >= 0 else ""
            cues.append(f"{idx} {s}{pct:.1f}%")

    # Opening signal based on Gift Nifty
    if gift_nifty_change_pct >= 1.0:
        outlook = "Gap-up opening expected"
    elif gift_nifty_change_pct >= 0.3:
        outlook = "Positive opening expected"
    elif gift_nifty_change_pct > -0.3:
        outlook = "Flat opening expected"
    elif gift_nifty_change_pct > -1.0:
        outlook = "Negative opening expected"
    else:
        outlook = "Gap-down opening expected"

    parts = [f"{outlook}."]
    if cues:
        parts.append(f"Overnight: {', '.join(cues[:3])}.")

    body = " ".join(parts)

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={"type": "pre_market_summary", "gift_nifty_price": f"{gift_nifty_price:.0f}"},
    )


_COMMODITY_EMOJIS: dict[str, str] = {
    "gold": "\U0001f947",         # 🥇
    "silver": "\U0001f948",       # 🥈
    "crude_oil": "\U0001f6e2\ufe0f",  # 🛢️
    "brent_crude": "\U0001f6e2\ufe0f",
    "natural_gas": "\U0001f525",  # 🔥
    "copper": "\U0001f7e0",       # 🟠
    "platinum": "\u2b50",         # ⭐
    "palladium": "\u2b50",
    "iron_ore": "\u26cf\ufe0f",   # ⛏️
    "coal": "\u26ab",             # ⚫
    "wheat": "\U0001f33e",        # 🌾
    "corn": "\U0001f33d",         # 🌽
    "cotton": "\u2601\ufe0f",     # ☁️
    "sugar": "\U0001f36c",        # 🍬
    "coffee": "\u2615",           # ☕
    "palm_oil": "\U0001f334",     # 🌴
    "rubber": "\U0001f6de",       # 🛞
}


async def notify_commodity_spike(
    asset: str,
    display_name: str,
    change_pct: float,
    price: float,
    unit: str | None = None,
) -> bool:
    """Send notification when a commodity moves ±3% from previous close."""
    emoji = _COMMODITY_EMOJIS.get(asset, "\U0001f4e6")  # 📦 fallback
    direction = "surges" if change_pct > 0 else "drops"
    sign = "+" if change_pct >= 0 else ""

    title = f"{emoji} {display_name} {direction} {sign}{change_pct:.1f}%"

    unit_label = f"/{unit}" if unit else ""
    price_str = f"${price:,.2f}{unit_label}" if price < 10000 else f"${price:,.0f}{unit_label}"

    # Context based on magnitude
    if abs(change_pct) >= 5:
        magnitude = "Major move"
    elif abs(change_pct) >= 3:
        magnitude = "Significant move"
    else:
        magnitude = "Notable move"

    body = f"{magnitude} — trading at {price_str}"

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={
            "type": "commodity_spike",
            "asset": asset,
            "change_pct": f"{change_pct:.1f}",
            "price": f"{price:.2f}",
        },
    )


async def notify_market_close(market: str) -> bool:
    """Send notification when a market closes."""
    titles = {
        "india": "\U0001f1ee\U0001f1f3 Indian Markets Closed",
        "us": "\U0001f1fa\U0001f1f8 US Markets Closed",
        "europe": "\U0001f1ea\U0001f1fa European Markets Closed",
        "japan": "\U0001f1ef\U0001f1f5 Japanese Markets Closed",
    }
    bodies = {
        "india": "NSE & BSE session ended. Check your portfolio performance.",
        "us": "NYSE & NASDAQ session ended. After-hours trading may continue.",
        "europe": "European market session has ended.",
        "japan": "Japanese market session has ended.",
    }
    title = titles.get(market, f"{market.title()} Markets Closed")
    body = bodies.get(market, f"{market.title()} market session has ended.")
    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={"type": "market_close", "market": market},
    )
