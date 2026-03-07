from app.core.supabase_client import get_supabase

TABLE = "news_articles"


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
        result = client.table(TABLE).upsert(filtered_payload, on_conflict="url").execute()
    else:
        result = client.table(TABLE).insert(filtered_payload).execute()

    return result.data[0]
