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


async def notify_market_open(market: str, market_data: dict | None = None) -> bool:
    """Send notification when a market opens.

    If *market_data* is provided, include richer context (e.g. Gift Nifty
    level for India open, overnight performance for US open).
    """
    if market_data:
        try:
            title, body = _build_rich_open(market, market_data)
        except Exception:
            logger.warning("Rich open build failed for %s, falling back", market, exc_info=True)
            title, body = _simple_open(market)
    else:
        title, body = _simple_open(market)

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={"type": "market_open", "market": market},
    )


def _simple_open(market: str) -> tuple[str, str]:
    """Fallback simple open title & body."""
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
    return (
        titles.get(market, f"{market.title()} Markets Open"),
        bodies.get(market, f"{market.title()} market session has started."),
    )


def _build_rich_open(market: str, d: dict) -> tuple[str, str]:
    """Build data-driven open notification."""
    if market == "india":
        return _build_india_open(d)
    if market == "japan":
        return _build_japan_open(d)
    if market == "europe":
        return _build_europe_open(d)
    if market == "us":
        return _build_us_open(d)
    return _simple_open(market)


def _open_tone(avg_pct: float, is_india: bool = False) -> str:
    """Return an opening-tone phrase based on the average change of own indices."""
    if avg_pct >= 1.0:
        return "Gap-up opening" if is_india else "Strong opening"
    if avg_pct >= 0.3:
        return "Positive opening"
    if avg_pct > -0.3:
        return "Flat opening"
    if avg_pct > -1.0:
        return "Weak opening"
    return "Gap-down opening" if is_india else "Sharp selloff at open"


def _build_india_open(d: dict) -> tuple[str, str]:
    gift_price = d.get("gift_nifty_price")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_price is None or gift_pct is None:
        return _simple_open("india")

    emoji = "\U0001f7e2" if gift_pct >= 0 else "\U0001f534"
    title = f"{emoji} Nifty opens {_sign(gift_pct)}{gift_pct:.1f}% at {gift_price:,.0f}"

    parts: list[str] = [_open_tone(gift_pct, is_india=True)]

    # Gift Nifty context
    parts.append(f"Gift Nifty was at {_sign(gift_pct)}{gift_pct:.1f}%")

    # Overnight US
    us_sp = d.get("us_sp500_pct")
    us_nas = d.get("us_nasdaq_pct")
    overnight_parts: list[str] = []
    if us_sp is not None:
        overnight_parts.append(f"S&P 500 {_sign(us_sp)}{us_sp:.1f}%")
    if us_nas is not None:
        overnight_parts.append(f"NASDAQ {_sign(us_nas)}{us_nas:.1f}%")
    # Asia cues
    nikkei_pct = d.get("nikkei_pct")
    if nikkei_pct is not None:
        overnight_parts.append(f"Nikkei {_sign(nikkei_pct)}{nikkei_pct:.1f}%")
    if overnight_parts:
        parts.append(f"Overnight: {', '.join(overnight_parts)}")

    # Gold
    gold_pct = d.get("gold_pct")
    if gold_pct is not None:
        parts.append(f"Gold {_sign(gold_pct)}{gold_pct:.1f}%")

    body = ". ".join(parts[:4]) + "."
    return title, body


def _build_japan_open(d: dict) -> tuple[str, str]:
    nikkei_pct = d.get("nikkei_pct")
    if nikkei_pct is None:
        return _simple_open("japan")

    emoji = _emoji_arrow(nikkei_pct)
    idx_parts = [f"Nikkei {_sign(nikkei_pct)}{nikkei_pct:.1f}%"]
    topix_pct = d.get("topix_pct")
    if topix_pct is not None:
        idx_parts.append(f"TOPIX {_sign(topix_pct)}{topix_pct:.1f}%")
    title = f"{emoji} {' | '.join(idx_parts)}"

    # Tone based on own indices
    own_pcts = [p for p in [nikkei_pct, topix_pct] if p is not None]
    avg_pct = sum(own_pcts) / len(own_pcts)
    parts: list[str] = [_open_tone(avg_pct)]

    # Yen
    usd_jpy = d.get("usd_jpy_price")
    if usd_jpy is not None:
        parts.append(f"Yen at {usd_jpy:.1f}/USD")

    # Overnight US
    us_sp = d.get("us_sp500_pct")
    us_nas = d.get("us_nasdaq_pct")
    overnight_parts: list[str] = []
    if us_sp is not None:
        overnight_parts.append(f"S&P 500 {_sign(us_sp)}{us_sp:.1f}%")
    if us_nas is not None:
        overnight_parts.append(f"NASDAQ {_sign(us_nas)}{us_nas:.1f}%")
    if overnight_parts:
        parts.append(f"Overnight: {', '.join(overnight_parts)}")

    # Gold
    gold_pct = d.get("gold_pct")
    if gold_pct is not None:
        parts.append(f"Gold {_sign(gold_pct)}{gold_pct:.1f}%")

    body = ". ".join(parts[:4]) + "."
    return title, body


def _build_europe_open(d: dict) -> tuple[str, str]:
    ftse_pct = d.get("ftse_pct")
    dax_pct = d.get("dax_pct")
    cac_pct = d.get("cac_pct")
    if ftse_pct is None and dax_pct is None:
        return _simple_open("europe")

    primary = ftse_pct if ftse_pct is not None else dax_pct
    emoji = _emoji_arrow(primary)
    idx_parts: list[str] = []
    if ftse_pct is not None:
        idx_parts.append(f"FTSE {_sign(ftse_pct)}{ftse_pct:.1f}%")
    if dax_pct is not None:
        idx_parts.append(f"DAX {_sign(dax_pct)}{dax_pct:.1f}%")
    if cac_pct is not None:
        idx_parts.append(f"CAC {_sign(cac_pct)}{cac_pct:.1f}%")
    title = f"{emoji} {' | '.join(idx_parts)}"

    # Tone based on own indices
    own_pcts = [p for p in [ftse_pct, dax_pct, cac_pct] if p is not None]
    avg_pct = sum(own_pcts) / len(own_pcts)
    parts: list[str] = [_open_tone(avg_pct)]

    # Brent crude
    brent_pct = d.get("brent_pct")
    if brent_pct is not None:
        parts.append(f"Brent crude {_sign(brent_pct)}{brent_pct:.1f}%")

    # Asia cues
    asia_parts: list[str] = []
    nikkei_pct = d.get("nikkei_pct")
    nifty_pct = d.get("nifty_pct")
    if nikkei_pct is not None:
        asia_parts.append(f"Nikkei {_sign(nikkei_pct)}{nikkei_pct:.1f}%")
    if nifty_pct is not None:
        asia_parts.append(f"Nifty {_sign(nifty_pct)}{nifty_pct:.1f}%")
    if asia_parts:
        parts.append(f"Asia: {', '.join(asia_parts)}")

    body = ". ".join(parts[:3]) + "."
    return title, body


def _build_us_open(d: dict) -> tuple[str, str]:
    sp_pct = d.get("sp500_pct")
    if sp_pct is None:
        return _simple_open("us")

    nas_pct = d.get("nasdaq_pct")
    dow_pct = d.get("dow_pct")

    emoji = _emoji_arrow(sp_pct)
    idx_parts = [f"S&P 500 {_sign(sp_pct)}{sp_pct:.1f}%"]
    if nas_pct is not None:
        idx_parts.append(f"NASDAQ {_sign(nas_pct)}{nas_pct:.1f}%")
    if dow_pct is not None:
        idx_parts.append(f"Dow {_sign(dow_pct)}{dow_pct:.1f}%")
    title = f"{emoji} {' | '.join(idx_parts)}"

    # Tone based on own indices
    own_pcts = [p for p in [sp_pct, nas_pct, dow_pct] if p is not None]
    avg_pct = sum(own_pcts) / len(own_pcts)
    parts: list[str] = [_open_tone(avg_pct)]

    # Europe cues
    europe_parts: list[str] = []
    ftse_pct = d.get("ftse_pct")
    dax_pct = d.get("dax_pct")
    if ftse_pct is not None:
        europe_parts.append(f"FTSE {_sign(ftse_pct)}{ftse_pct:.1f}%")
    if dax_pct is not None:
        europe_parts.append(f"DAX {_sign(dax_pct)}{dax_pct:.1f}%")
    if europe_parts:
        parts.append(f"Europe: {', '.join(europe_parts)}")

    # Crude oil
    crude_pct = d.get("crude_pct")
    if crude_pct is not None:
        parts.append(f"Crude {_sign(crude_pct)}{crude_pct:.1f}%")

    # Gift Nifty
    gift_price = d.get("gift_nifty_price")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_price is not None and gift_pct is not None:
        parts.append(f"Gift Nifty at {gift_price:,.0f} ({_sign(gift_pct)}{gift_pct:.1f}%)")

    body = ". ".join(parts[:4]) + "."
    return title, body


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
    """Send notification when a commodity moves ±2% from previous close."""
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


async def notify_market_close(market: str, market_data: dict | None = None) -> bool:
    """Send notification when a market closes.

    If *market_data* is provided it should contain pre-fetched numbers so
    the notification can include a data-driven title and commentary.
    Falls back to a simple message when data is unavailable.
    """
    if market_data:
        try:
            title, body = _build_rich_close(market, market_data)
        except Exception:
            logger.warning("Rich close build failed for %s, falling back", market, exc_info=True)
            title, body = _simple_close(market)
    else:
        title, body = _simple_close(market)

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={"type": "market_close", "market": market},
    )


def _simple_close(market: str) -> tuple[str, str]:
    """Fallback simple close title & body."""
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
    return (
        titles.get(market, f"{market.title()} Markets Closed"),
        bodies.get(market, f"{market.title()} market session has ended."),
    )


def _sign(v: float) -> str:
    return "+" if v >= 0 else ""


def _emoji_arrow(v: float) -> str:
    return "\U0001f4c8" if v >= 0 else "\U0001f4c9"


def _build_rich_close(market: str, d: dict) -> tuple[str, str]:
    """Build data-driven title & body for a market close notification."""
    if market == "india":
        return _build_india_close(d)
    if market == "us":
        return _build_us_close(d)
    if market == "europe":
        return _build_europe_close(d)
    if market == "japan":
        return _build_japan_close(d)
    return _simple_close(market)


def _build_india_close(d: dict) -> tuple[str, str]:
    nifty_pct = d.get("nifty_change_pct")
    sensex_pct = d.get("sensex_change_pct")
    if nifty_pct is None or sensex_pct is None:
        return _simple_close("india")

    emoji = _emoji_arrow(nifty_pct)
    title = f"{emoji} Nifty {_sign(nifty_pct)}{nifty_pct:.1f}% | Sensex {_sign(sensex_pct)}{sensex_pct:.1f}%"

    parts: list[str] = []
    # Relative context
    ctx = d.get("relative_context")
    if ctx:
        parts.append(ctx.capitalize())
    # 52-week context
    w52 = d.get("week52_context")
    if w52:
        parts.append(w52)
    # Breadth
    adv = d.get("advancers")
    dec = d.get("decliners")
    if adv is not None and dec is not None:
        parts.append(f"{adv} advancers vs {dec} decliners")
    # Top / bottom sector
    top_sec = d.get("top_sector")
    top_sec_pct = d.get("top_sector_pct")
    bottom_sec = d.get("bottom_sector")
    bottom_sec_pct = d.get("bottom_sector_pct")
    if top_sec and top_sec_pct is not None:
        parts.append(f"{top_sec} {_sign(top_sec_pct)}{top_sec_pct:.1f}% led")
    if bottom_sec and bottom_sec_pct is not None:
        parts.append(f"{bottom_sec} {_sign(bottom_sec_pct)}{bottom_sec_pct:.1f}% lagged")

    body = ". ".join(parts[:4]) + "." if parts else "NSE & BSE session ended."
    return title, body


def _build_us_close(d: dict) -> tuple[str, str]:
    sp_pct = d.get("sp500_change_pct")
    nas_pct = d.get("nasdaq_change_pct")
    dow_pct = d.get("dow_change_pct")
    if sp_pct is None:
        return _simple_close("us")

    emoji = _emoji_arrow(sp_pct)
    idx_parts = [f"S&P 500 {_sign(sp_pct)}{sp_pct:.1f}%"]
    if nas_pct is not None:
        idx_parts.append(f"NASDAQ {_sign(nas_pct)}{nas_pct:.1f}%")
    if dow_pct is not None:
        idx_parts.append(f"Dow {_sign(dow_pct)}{dow_pct:.1f}%")
    title = f"{emoji} {' | '.join(idx_parts)}"

    parts: list[str] = []
    ctx = d.get("relative_context")
    if ctx:
        parts.append(ctx.capitalize())
    w52 = d.get("week52_context")
    if w52:
        parts.append(w52)
    # Gift Nifty signal
    gift_price = d.get("gift_nifty_price")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_price is not None and gift_pct is not None:
        parts.append(f"Gift Nifty at {gift_price:,.0f} ({_sign(gift_pct)}{gift_pct:.1f}% from Nifty close)")

    body = ". ".join(parts[:3]) + "." if parts else "NYSE & NASDAQ session ended."
    return title, body


def _build_europe_close(d: dict) -> tuple[str, str]:
    ftse_pct = d.get("ftse_change_pct")
    dax_pct = d.get("dax_change_pct")
    cac_pct = d.get("cac_change_pct")
    if ftse_pct is None and dax_pct is None:
        return _simple_close("europe")

    primary = ftse_pct if ftse_pct is not None else dax_pct
    emoji = _emoji_arrow(primary)
    idx_parts = []
    if ftse_pct is not None:
        idx_parts.append(f"FTSE {_sign(ftse_pct)}{ftse_pct:.1f}%")
    if dax_pct is not None:
        idx_parts.append(f"DAX {_sign(dax_pct)}{dax_pct:.1f}%")
    if cac_pct is not None:
        idx_parts.append(f"CAC {_sign(cac_pct)}{cac_pct:.1f}%")
    title = f"{emoji} {' | '.join(idx_parts)}"

    parts: list[str] = []
    ctx = d.get("relative_context")
    if ctx:
        parts.append(ctx.capitalize())
    w52 = d.get("week52_context")
    if w52:
        parts.append(w52)
    # Brent crude if significant
    brent_pct = d.get("brent_change_pct")
    if brent_pct is not None and abs(brent_pct) >= 1.0:
        direction = "up" if brent_pct > 0 else "down"
        parts.append(f"Brent crude {direction} {_sign(brent_pct)}{brent_pct:.1f}%")

    body = ". ".join(parts[:3]) + "." if parts else "European market session has ended."
    return title, body


def _build_japan_close(d: dict) -> tuple[str, str]:
    nikkei_pct = d.get("nikkei_change_pct")
    topix_pct = d.get("topix_change_pct")
    if nikkei_pct is None:
        return _simple_close("japan")

    emoji = _emoji_arrow(nikkei_pct)
    idx_parts = [f"Nikkei {_sign(nikkei_pct)}{nikkei_pct:.1f}%"]
    if topix_pct is not None:
        idx_parts.append(f"TOPIX {_sign(topix_pct)}{topix_pct:.1f}%")
    title = f"{emoji} {' | '.join(idx_parts)}"

    parts: list[str] = []
    ctx = d.get("relative_context")
    if ctx:
        parts.append(ctx.capitalize())
    w52 = d.get("week52_context")
    if w52:
        parts.append(w52)
    # JPY/INR
    jpy_price = d.get("jpy_inr_price")
    jpy_pct = d.get("jpy_inr_change_pct")
    if jpy_price is not None and jpy_pct is not None:
        parts.append(f"JPY/INR at {jpy_price:.2f} ({_sign(jpy_pct)}{jpy_pct:.1f}%)")

    body = ". ".join(parts[:3]) + "." if parts else "Japanese market session has ended."
    return title, body
