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


# ---------------------------------------------------------------------------
# Notification dedup helpers (DB-backed, survives restarts/deploys)
# ---------------------------------------------------------------------------

async def _was_already_sent(dedup_key: str) -> bool:
    """Check if a notification with this dedup key was already sent."""
    from app.core.database import get_pool
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT 1 FROM notification_log WHERE dedup_key = $1",
        dedup_key,
    )
    return row is not None


async def _log_sent(notification_type: str, dedup_key: str, title: str) -> None:
    """Log that a notification was sent."""
    from app.core.database import get_pool
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO notification_log (notification_type, dedup_key, title)
        VALUES ($1, $2, $3)
        ON CONFLICT (dedup_key) DO NOTHING
        """,
        notification_type, dedup_key, title,
    )


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
    *,
    dedup_key: str | None = None,
    notification_type: str | None = None,
) -> bool:
    """Send a push notification to all devices subscribed to a topic.

    If *dedup_key* is provided, the notification is checked against the
    ``notification_log`` table first.  If an entry with the same key already
    exists the send is skipped (returns ``False``).  After a successful send
    the key is logged so subsequent calls (even after a restart) are no-ops.
    """
    # --- DB dedup gate ---
    if dedup_key:
        try:
            if await _was_already_sent(dedup_key):
                logger.debug(
                    "Notification already sent (dedup_key=%s) — skipping",
                    dedup_key,
                )
                return False
        except Exception:
            # If DB check fails, proceed with send to avoid silencing alerts
            logger.warning("Dedup check failed for %s — proceeding", dedup_key, exc_info=True)

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

        # --- Log successful send for dedup ---
        if dedup_key:
            try:
                await _log_sent(
                    notification_type or "unknown",
                    dedup_key,
                    title,
                )
            except Exception:
                logger.warning("Failed to log sent notification (dedup_key=%s)", dedup_key, exc_info=True)

        return True
    except Exception:
        logger.warning("FCM send failed for topic=%s", topic, exc_info=True)
        return False


async def notify_market_open(
    market: str,
    market_data: dict | None = None,
    *,
    dedup_key: str | None = None,
) -> bool:
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
        dedup_key=dedup_key,
        notification_type=f"market_open_{market}",
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


def _close_tone(avg_pct: float) -> str:
    """Return a closing-tone phrase based on the average change of own indices."""
    if avg_pct >= 1.5:
        return "Broad rally"
    if avg_pct >= 0.3:
        return "Positive session"
    if avg_pct > -0.3:
        return "Flat session"
    if avg_pct > -1.5:
        return "Markets closed lower"
    return "Sharp selloff"


def _nse_outlook(gift_pct: float) -> str:
    """Return NSE outlook phrase based on Gift Nifty change from Nifty close."""
    if gift_pct >= 0.5:
        return "positive opening expected for NSE"
    if gift_pct > -0.5:
        return "flat opening expected for NSE"
    return "negative opening expected for NSE"


def _build_india_open(d: dict) -> tuple[str, str]:
    gift_price = d.get("gift_nifty_price")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_price is None or gift_pct is None:
        return _simple_open("india")

    emoji = "\U0001f7e2" if gift_pct >= 0 else "\U0001f534"
    title = f"{emoji} Nifty opens at {gift_price:,.0f} ({_sign(gift_pct)}{gift_pct:.1f}%)"

    sentences: list[str] = [f"{_open_tone(gift_pct, is_india=True)}."]

    # Gift Nifty indicated
    sentences.append(f"Gift Nifty indicated {_sign(gift_pct)}{gift_pct:.1f}% before open.")

    # Overnight US
    us_sp = d.get("us_sp500_pct")
    us_nas = d.get("us_nasdaq_pct")
    overnight_parts: list[str] = []
    if us_sp is not None:
        overnight_parts.append(f"S&P 500 {_sign(us_sp)}{us_sp:.1f}%")
    if us_nas is not None:
        overnight_parts.append(f"NASDAQ {_sign(us_nas)}{us_nas:.1f}%")
    if overnight_parts:
        sentences.append(f"Overnight: {', '.join(overnight_parts)}.")

    # Asia cues (Nikkei trading live)
    nikkei_pct = d.get("nikkei_pct")
    if nikkei_pct is not None:
        sentences.append(f"Nikkei trading {_sign(nikkei_pct)}{nikkei_pct:.1f}%.")

    # Gold
    gold_pct = d.get("gold_pct")
    if gold_pct is not None:
        label = "steady at" if abs(gold_pct) < 0.5 else ""
        if label:
            sentences.append(f"Gold {label} {_sign(gold_pct)}{gold_pct:.1f}%.")
        else:
            sentences.append(f"Gold {_sign(gold_pct)}{gold_pct:.1f}%.")

    body = " ".join(sentences[:5])
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
    sentences: list[str] = [f"{_open_tone(avg_pct)} for Japanese markets."]

    # JPY/INR context (JPY/INR up = yen strengthened vs INR)
    jpy_inr = d.get("jpy_inr_price")
    jpy_inr_pct = d.get("jpy_inr_pct")
    if jpy_inr is not None:
        yen_dir = "yen strengthened" if (jpy_inr_pct is not None and jpy_inr_pct > 0) else "yen weakened overnight"
        sentences.append(f"JPY/INR at {jpy_inr:.4f} \u2014 {yen_dir}.")

    # Overnight US (Wall Street)
    us_sp = d.get("us_sp500_pct")
    us_nas = d.get("us_nasdaq_pct")
    ws_parts: list[str] = []
    if us_sp is not None:
        ws_parts.append(f"S&P 500 {_sign(us_sp)}{us_sp:.1f}%")
    if us_nas is not None:
        ws_parts.append(f"NASDAQ {_sign(us_nas)}{us_nas:.1f}%")
    if ws_parts:
        # Determine if Wall Street closed positive or negative
        ws_avg = sum(p for p in [us_sp, us_nas] if p is not None) / len(ws_parts)
        if ws_avg < -0.2:
            sentences.append(f"Wall Street closed lower: {', '.join(ws_parts)}.")
        else:
            sentences.append(f"Wall Street: {', '.join(ws_parts)}.")

    # Gold
    gold_pct = d.get("gold_pct")
    if gold_pct is not None:
        label = "steady at" if abs(gold_pct) < 0.5 else ""
        if label:
            sentences.append(f"Gold {label} {_sign(gold_pct)}{gold_pct:.1f}%.")
        else:
            sentences.append(f"Gold {_sign(gold_pct)}{gold_pct:.1f}%.")

    body = " ".join(sentences[:4])
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
    sentences: list[str] = [f"{_open_tone(avg_pct)} for European markets."]

    # Asia cues
    asia_parts: list[str] = []
    nikkei_pct = d.get("nikkei_pct")
    nifty_pct = d.get("nifty_pct")
    if nikkei_pct is not None:
        asia_parts.append(f"Nikkei {_sign(nikkei_pct)}{nikkei_pct:.1f}%")
    if nifty_pct is not None:
        asia_parts.append(f"Nifty {_sign(nifty_pct)}{nifty_pct:.1f}%")
    if asia_parts:
        # Determine Asia tone
        asia_avg = sum(p for p in [nikkei_pct, nifty_pct] if p is not None) / len(asia_parts)
        if asia_avg >= 0.5:
            sentences.append(f"Asia rally provided tailwinds \u2014 {', '.join(asia_parts)}.")
        elif asia_avg <= -0.5:
            sentences.append(f"Asia sold off \u2014 {', '.join(asia_parts)}.")
        else:
            sentences.append(f"Asia mixed \u2014 {', '.join(asia_parts)}.")

    # Brent crude
    brent_pct = d.get("brent_pct")
    if brent_pct is not None:
        if abs(brent_pct) >= 1.0:
            verb = "rose" if brent_pct > 0 else "fell"
            sentences.append(f"Brent crude {verb} {_sign(brent_pct)}{brent_pct:.1f}%.")
        else:
            sentences.append(f"Brent crude steady at {_sign(brent_pct)}{brent_pct:.1f}%.")

    body = " ".join(sentences[:3])
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
    sentences: list[str] = [f"{_open_tone(avg_pct)} for US markets."]

    # Europe cues
    europe_parts: list[str] = []
    ftse_pct = d.get("ftse_pct")
    dax_pct = d.get("dax_pct")
    if ftse_pct is not None:
        europe_parts.append(f"FTSE {_sign(ftse_pct)}{ftse_pct:.1f}%")
    if dax_pct is not None:
        europe_parts.append(f"DAX {_sign(dax_pct)}{dax_pct:.1f}%")
    if europe_parts:
        # Determine Europe tone
        eu_avg = sum(p for p in [ftse_pct, dax_pct] if p is not None) / len(europe_parts)
        if eu_avg >= 0.2:
            sentences.append(f"Europe closed positive \u2014 {', '.join(europe_parts)}.")
        elif eu_avg <= -0.2:
            sentences.append(f"Europe closed lower \u2014 {', '.join(europe_parts)}.")
        else:
            sentences.append(f"Europe closed mixed \u2014 {', '.join(europe_parts)}.")

    # Crude oil
    crude_pct = d.get("crude_pct")
    if crude_pct is not None:
        if abs(crude_pct) >= 1.0:
            verb = "rose" if crude_pct > 0 else "fell"
            sentences.append(f"Crude oil {verb} {_sign(crude_pct)}{crude_pct:.1f}%.")
        else:
            sentences.append(f"Crude oil steady at {_sign(crude_pct)}{crude_pct:.1f}%.")

    # Gift Nifty
    gift_price = d.get("gift_nifty_price")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_price is not None and gift_pct is not None:
        outlook = _nse_outlook(gift_pct)
        sentences.append(
            f"Gift Nifty at {gift_price:,.0f} ({_sign(gift_pct)}{gift_pct:.1f}% from Nifty close) \u2014 {outlook}."
        )

    body = " ".join(sentences[:4])
    return title, body


def _format_inr(value: float) -> str:
    """Format a number with Indian comma style, e.g. 2100 -> '2,100'."""
    abs_val = abs(value)
    int_part = int(abs_val)
    s = f"{int_part:,}"
    return s


async def notify_gift_nifty_move(
    change_pct: float,
    price: float,
    *,
    dedup_key: str | None = None,
) -> bool:
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
        dedup_key=dedup_key,
        notification_type="gift_nifty_alert",
    )


async def notify_fii_dii_data(
    fii_net: float,
    dii_net: float,
    *,
    dedup_key: str | None = None,
) -> bool:
    """Send notification with FII/DII activity update."""
    title = "\U0001f4ca FII/DII Activity Update"

    net = fii_net + dii_net
    abs_fii = abs(fii_net)
    abs_dii = abs(dii_net)
    fii_buying = fii_net >= 0
    dii_buying = dii_net >= 0

    sentences: list[str] = []

    # Sentence 1: Lead narrative based on pattern
    if fii_buying and dii_buying:
        # Both buying
        sentences.append(
            f"Both FIIs and DIIs turned buyers today. "
            f"FIIs bought \u20b9{_format_inr(fii_net)} Cr, "
            f"DIIs added \u20b9{_format_inr(dii_net)} Cr"
        )
    elif not fii_buying and not dii_buying:
        # Both selling
        sentences.append(
            f"Both FIIs and DIIs were net sellers today. "
            f"FIIs sold \u20b9{_format_inr(fii_net)} Cr, "
            f"DIIs offloaded \u20b9{_format_inr(dii_net)} Cr"
        )
    elif not fii_buying and dii_buying:
        # FII selling, DII buying (typical)
        if abs_fii >= 3000:
            sentences.append(
                f"Heavy FII selling at \u20b9{_format_inr(fii_net)} Cr. "
                f"DIIs stepped in with \u20b9{_format_inr(dii_net)} Cr"
            )
        else:
            sentences.append(
                f"FIIs continued selling, offloading \u20b9{_format_inr(fii_net)} Cr "
                f"while DIIs absorbed \u20b9{_format_inr(dii_net)} Cr"
            )
    else:
        # FII buying, DII selling (rotation)
        sentences.append(
            f"FIIs turned buyers at \u20b9{_format_inr(fii_net)} Cr "
            f"while DIIs booked profits, selling \u20b9{_format_inr(dii_net)} Cr"
        )

    # Sentence 2: Net flow + insight
    net_label = "inflow" if net >= 0 else "outflow"
    net_str = f"Net {net_label} of \u20b9{_format_inr(net)} Cr"

    if fii_buying and dii_buying:
        sentences.append(f"{net_str} \u2014 strong institutional support")
    elif not fii_buying and not dii_buying:
        sentences.append(f"{net_str} \u2014 broad institutional retreat")
    elif not fii_buying and dii_buying:
        if abs_dii >= abs_fii * 0.9:
            sentences.append(f"{net_str} \u2014 DIIs nearly matched foreign exit")
        elif abs_dii >= abs_fii * 0.5:
            sentences.append(f"{net_str} \u2014 domestic institutions cushioned foreign exit")
        else:
            sentences.append(f"{net_str} \u2014 DII buying insufficient to offset FII selling")
    else:
        sentences.append(f"{net_str} \u2014 foreign money returning as domestic institutions take a breather")

    body = ". ".join(sentences) + "."

    return await send_topic_notification(
        topic="market_alerts",
        title=title,
        body=body,
        data={
            "type": "fii_dii_data",
            "fii_net": f"{fii_net:.0f}",
            "dii_net": f"{dii_net:.0f}",
        },
        dedup_key=dedup_key,
        notification_type="fii_dii",
    )


async def notify_post_market_summary(
    nifty_change_pct: float,
    sensex_change_pct: float,
    advancers: int,
    decliners: int,
    top_sector: str | None,
    bottom_sector: str | None,
    *,
    dedup_key: str | None = None,
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
        dedup_key=dedup_key,
        notification_type="post_market",
    )


async def notify_pre_market_summary(
    gift_nifty_price: float,
    gift_nifty_change_pct: float,
    us_change: dict | None = None,
    europe_change: dict | None = None,
    asia_change: dict | None = None,
    *,
    dedup_key: str | None = None,
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
        dedup_key=dedup_key,
        notification_type="pre_market",
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
    *,
    dedup_key: str | None = None,
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
        dedup_key=dedup_key,
        notification_type=f"commodity_spike_{asset}",
    )


async def notify_market_close(
    market: str,
    market_data: dict | None = None,
    *,
    dedup_key: str | None = None,
) -> bool:
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
        dedup_key=dedup_key,
        notification_type=f"market_close_{market}",
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
    if nifty_pct is None:
        return _simple_close("india")

    emoji = _emoji_arrow(nifty_pct)
    title_parts = [f"Nifty {_sign(nifty_pct)}{nifty_pct:.1f}%"]
    midcap_pct = d.get("midcap_change_pct")
    smallcap_pct = d.get("smallcap_change_pct")
    if midcap_pct is not None:
        title_parts.append(f"Midcap {_sign(midcap_pct)}{midcap_pct:.1f}%")
    if smallcap_pct is not None:
        title_parts.append(f"Smallcap {_sign(smallcap_pct)}{smallcap_pct:.1f}%")
    title = f"{emoji} {' | '.join(title_parts)}"

    # --- Build narrative body ---
    sentences: list[str] = []

    adv = d.get("advancers", 0)
    dec = d.get("decliners", 0)
    total = adv + dec
    adv_ratio = adv / total if total > 0 else 0.5
    abs_nifty = abs(nifty_pct)

    # Sentence 1: Tone + outperformance context
    sc_pct = smallcap_pct if smallcap_pct is not None else 0
    mc_pct = midcap_pct if midcap_pct is not None else 0

    if abs_nifty < 0.2:
        tone = "Flat session with muted activity"
    elif nifty_pct >= 2.0:
        tone = "Strong rally with broad-based buying" if adv_ratio >= 0.6 else "Strong rally led by heavyweights"
    elif nifty_pct >= 0.3:
        if sc_pct > nifty_pct + 0.5:
            tone = "Smallcaps outperformed in a broad-based rally" if adv_ratio >= 0.6 else "Smallcaps outperformed despite narrow breadth"
        elif adv_ratio >= 0.7:
            tone = "Broad-based rally across segments"
        else:
            tone = "Positive session with selective buying"
    elif nifty_pct > -0.3:
        if sc_pct < nifty_pct - 0.5:
            tone = "Largecaps held while broader market weakened"
        elif mc_pct > 0 and sc_pct > 0 and nifty_pct < 0:
            tone = "Broader market outperformed a weak Nifty"
        else:
            tone = "Mixed session with muted activity" if adv_ratio > 0.4 else "Flat session leaning negative"
    elif nifty_pct >= -2.0:
        if sc_pct < nifty_pct - 1.0:
            tone = "Broad-based selloff with smallcaps hit hardest"
        elif adv_ratio <= 0.3:
            tone = "Broad-based selloff across segments"
        else:
            tone = "Weak session with selective selling"
    else:
        # Sharp crash
        if sc_pct < nifty_pct - 1.0:
            tone = "Sharp selloff across the board \u2014 smallcaps plunged"
        else:
            tone = "Sharp selloff across the board"

    sentences.append(tone)

    # Sentence 2: Breadth
    if total > 0:
        sentences.append(f"{adv:,} stocks advanced vs {dec:,} declined")

    # Sentence 3: Sector leaders / laggards
    top_sec = d.get("top_sector")
    top_sec_pct = d.get("top_sector_pct")
    bottom_sec = d.get("bottom_sector")
    bottom_sec_pct = d.get("bottom_sector_pct")

    # Check if all sectors same direction
    all_red = (top_sec_pct is not None and top_sec_pct < 0)
    all_green = (bottom_sec_pct is not None and bottom_sec_pct > 0)

    if all_red and abs_nifty >= 1.0:
        if bottom_sec and bottom_sec_pct is not None:
            sentences.append(
                f"All sectors in red, {bottom_sec} worst at {_sign(bottom_sec_pct)}{bottom_sec_pct:.1f}%"
            )
    elif all_green and abs_nifty >= 1.0:
        if top_sec and top_sec_pct is not None:
            sentences.append(
                f"All sectors in green, {top_sec} best at {_sign(top_sec_pct)}{top_sec_pct:.1f}%"
            )
    else:
        sec_parts = []
        if top_sec and top_sec_pct is not None:
            verb = "surged" if top_sec_pct >= 2.0 else "rose" if top_sec_pct > 0 else "fell"
            sec_parts.append(f"{top_sec} {verb} {_sign(top_sec_pct)}{top_sec_pct:.1f}%")
        if bottom_sec and bottom_sec_pct is not None:
            verb = "dragged" if bottom_sec_pct <= -1.0 else "slipped" if bottom_sec_pct < 0 else "held up"
            sec_parts.append(f"{bottom_sec} {verb} {'' if verb == 'held up' else _sign(bottom_sec_pct)}{bottom_sec_pct:.1f}%" if verb != "held up" else f"{bottom_sec} held up at {_sign(bottom_sec_pct)}{bottom_sec_pct:.1f}%")
        if sec_parts:
            sentences.append(" while ".join(sec_parts))

    # Sentence 4: Relative context + 52-week (combined as closing punch)
    ctx = d.get("relative_context")
    w52 = d.get("week52_context")
    if ctx and w52:
        sentences.append(f"{ctx.capitalize()} \u2014 Nifty {w52}")
    elif ctx:
        sentences.append(ctx.capitalize())
    elif w52:
        sentences.append(f"Nifty {w52}")

    body = ". ".join(sentences) + "." if sentences else "NSE & BSE session ended."
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

    # --- Build narrative body ---
    sentences: list[str] = []

    own_pcts = [p for p in [sp_pct, nas_pct, dow_pct] if p is not None]
    avg_pct = sum(own_pcts) / len(own_pcts) if own_pcts else sp_pct

    # Sentence 1: Tone with sector/index flavor
    base_tone = _close_tone(avg_pct)
    if nas_pct is not None and sp_pct is not None:
        if nas_pct < sp_pct - 0.3:
            sentences.append(f"Tech-led weakness dragged markets lower. NASDAQ underperformed with tech stocks under pressure.")
        elif nas_pct > sp_pct + 0.3:
            sentences.append(f"{base_tone} with tech leading gains.")
        elif avg_pct >= 0.3:
            sentences.append(f"{base_tone} across US markets.")
        elif avg_pct <= -0.3:
            sentences.append(f"{base_tone} amid selling pressure.")
        else:
            sentences.append(f"{base_tone} for US markets.")
    else:
        sentences.append(f"{base_tone} for US markets.")

    # Sentence 2: Relative context + 52-week
    ctx = d.get("relative_context")
    w52 = d.get("week52_context")
    if ctx and w52:
        # Map primary index name for 52-week context
        idx_name = "NASDAQ" if (nas_pct is not None and abs(nas_pct) > abs(sp_pct)) else "S&P 500"
        sentences.append(f"{ctx.capitalize()} \u2014 {idx_name} {w52}.")
    elif ctx:
        sentences.append(f"{ctx.capitalize()}.")
    elif w52:
        sentences.append(f"S&P 500 {w52}.")

    # Sentence 3: Gift Nifty signal for India
    gift_price = d.get("gift_nifty_price")
    gift_pct = d.get("gift_nifty_change_pct")
    if gift_price is not None and gift_pct is not None:
        outlook = _nse_outlook(gift_pct)
        sentences.append(
            f"Gift Nifty at {gift_price:,.0f} ({_sign(gift_pct)}{gift_pct:.1f}% from Nifty close) \u2014 {outlook}."
        )

    body = " ".join(sentences[:3]) if sentences else "NYSE & NASDAQ session ended."
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

    # --- Build narrative body ---
    sentences: list[str] = []

    own_pcts = [p for p in [ftse_pct, dax_pct, cac_pct] if p is not None]
    avg_pct = sum(own_pcts) / len(own_pcts) if own_pcts else primary

    # Sentence 1: Tone
    base_tone = _close_tone(avg_pct)
    if avg_pct >= 0.3:
        sentences.append(f"{base_tone} across European bourses.")
    elif avg_pct <= -0.3:
        sentences.append(f"European markets closed lower amid global uncertainty.")
    else:
        sentences.append(f"{base_tone} for European markets.")

    # Sentence 2: Brent crude context
    brent_pct = d.get("brent_change_pct")
    if brent_pct is not None:
        if abs(brent_pct) >= 1.0:
            verb = "rose" if brent_pct > 0 else "fell"
            impact = "lifting energy stocks" if brent_pct > 0 else "weighing on energy stocks"
            sentences.append(f"Brent crude {verb} {_sign(brent_pct)}{brent_pct:.1f}%, {impact}.")
        else:
            sentences.append(f"Brent crude steady at {_sign(brent_pct)}{brent_pct:.1f}%.")

    # Sentence 3: Relative context + 52-week
    ctx = d.get("relative_context")
    w52 = d.get("week52_context")
    primary_name = "FTSE" if ftse_pct is not None else "DAX"
    if ctx and w52:
        sentences.append(f"{ctx.capitalize()} \u2014 {primary_name} {w52}.")
    elif ctx:
        sentences.append(f"{ctx.capitalize()}.")
    elif w52:
        sentences.append(f"{primary_name} {w52}.")

    body = " ".join(sentences[:3]) if sentences else "European market session has ended."
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

    # --- Build narrative body ---
    sentences: list[str] = []

    own_pcts = [p for p in [nikkei_pct, topix_pct] if p is not None]
    avg_pct = sum(own_pcts) / len(own_pcts) if own_pcts else nikkei_pct

    # Sentence 1: Tone + yen context
    base_tone = _close_tone(avg_pct)
    jpy_inr = d.get("jpy_inr_price")
    jpy_inr_pct = d.get("jpy_inr_change_pct")
    if jpy_inr_pct is not None:
        # JPY/INR up = yen strengthened vs INR; JPY/INR down = yen weakened
        yen_dir = "yen strengthened" if jpy_inr_pct > 0 else "yen weakened"
        if avg_pct >= 0.3:
            sentences.append(f"Strong rally in Japanese markets as {yen_dir}." if avg_pct >= 1.5 else f"{base_tone} in Japanese markets as {yen_dir}.")
        elif avg_pct <= -0.3:
            sentences.append(f"Japanese markets sold off as {yen_dir}." if avg_pct <= -1.5 else f"{base_tone} as {yen_dir}.")
        else:
            sentences.append(f"{base_tone} for Japanese markets as {yen_dir}.")
    else:
        sentences.append(f"{base_tone} for Japanese markets.")

    # Sentence 2: JPY/INR level
    if jpy_inr is not None and jpy_inr_pct is not None:
        sentences.append(f"JPY/INR at {jpy_inr:.4f} ({_sign(jpy_inr_pct)}{jpy_inr_pct:.1f}%).")

    # Sentence 3: Relative context + 52-week
    ctx = d.get("relative_context")
    w52 = d.get("week52_context")
    if ctx and w52:
        sentences.append(f"{ctx.capitalize()} \u2014 Nikkei {w52}.")
    elif ctx:
        sentences.append(f"{ctx.capitalize()}.")
    elif w52:
        sentences.append(f"Nikkei {w52}.")

    body = " ".join(sentences[:3]) if sentences else "Japanese market session has ended."
    return title, body
