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
            logger.warning("News upsert failed; falling back to insert: %s", str(exc))
            result = client.table(TABLE).insert(filtered_payload).execute()
    else:
        result = client.table(TABLE).insert(filtered_payload).execute()

    return result.data[0]


async def get_articles(
    entity: str | None = None,
    impact: str | None = None,
    source: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch news articles with optional filters, ordered by most recent."""
    client = get_supabase()
    query = client.table(TABLE).select("*")

    if entity:
        query = query.eq("primary_entity", entity)
    if impact:
        query = query.eq("impact", impact)
    if source:
        query = query.eq("source", source)

    result = (
        query
        .order("timestamp", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return result.data
