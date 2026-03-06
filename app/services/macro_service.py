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
