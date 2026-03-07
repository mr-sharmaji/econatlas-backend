from app.core.supabase_client import get_supabase

TABLE = "macro_indicators"


async def get_indicators(
    country: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch macro-economic indicators, optionally filtered by country."""
    client = get_supabase()
    query = client.table(TABLE).select("*")

    if country:
        query = query.eq("country", country)

    result = (
        query
        .order("timestamp", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return result.data


async def insert_indicator(payload: dict) -> dict:
    """Insert a macro-economic indicator row and return the created row."""
    client = get_supabase()
    filtered_payload = {
        "indicator_name": payload["indicator_name"],
        "value": payload["value"],
        "country": payload["country"],
        "timestamp": payload["timestamp"],
    }
    result = client.table(TABLE).insert(filtered_payload).execute()
    return result.data[0]
