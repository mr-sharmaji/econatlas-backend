"""FCM push notification service.

Uses topic-based messaging (no device token storage needed for v1).
All devices subscribe to 'market_alerts' topic via the Flutter app.
"""
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
        # Use Application Default Credentials (set GOOGLE_APPLICATION_CREDENTIALS env var)
        # or initialize without credentials if running on GCP
        try:
            _firebase_app = firebase_admin.get_app()
        except ValueError:
            cred_path = os.environ.get("FIREBASE_CREDENTIALS_JSON") or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            if cred_path:
                cred = credentials.Certificate(cred_path)
                _firebase_app = firebase_admin.initialize_app(cred)
            else:
                _firebase_app = firebase_admin.initialize_app()
        logger.info("Firebase Admin SDK initialized")
        return _firebase_app
    except Exception:
        logger.warning("Firebase Admin SDK not available — push notifications disabled", exc_info=True)
        return None


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
