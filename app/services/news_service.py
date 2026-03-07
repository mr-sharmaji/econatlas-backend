import logging

from app.core.supabase_client import get_supabase

TABLE = "news_articles"
logger = logging.getLogger(__name__)


async def upsert_article(payload: dict) -> dict:
    """Upsert a news article row and return the saved row."""
    client = get_supabase()
    filtered_payload = {
        "title": payload["title"],
        "summary": payload.get("summary"),
        "body": payload.get("body"),
        "timestamp": payload["timestamp"],
        "source": payload.get("source"),
        "url": payload.get("url"),
        "primary_entity": payload.get("primary_entity"),
        "impact": payload.get("impact"),
        "confidence": payload.get("confidence"),
    }

    if filtered_payload.get("url"):
        try:
            result = client.table(TABLE).upsert(filtered_payload, on_conflict="url").execute()
        except Exception as exc:
            # Common case: on_conflict target missing/invalid in DB schema.
            logger.warning("News upsert failed; falling back to insert: %s", str(exc))
            result = client.table(TABLE).insert(filtered_payload).execute()
    else:
        result = client.table(TABLE).insert(filtered_payload).execute()

    return result.data[0]
