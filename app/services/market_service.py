from app.core.supabase_client import get_supabase

TABLE = "market_prices"


async def insert_price(
    asset: str,
    price: float,
    timestamp: str,
    source: str | None = None,
    instrument_type: str | None = None,
    unit: str | None = None,
    change_percent: float | None = None,
    previous_close: float | None = None,
) -> dict:
    """Insert one market/commodity price point and return created row."""
    client = get_supabase()
    payload = {
        "asset": asset,
        "price": price,
        "timestamp": timestamp,
        "source": source,
        "instrument_type": instrument_type,
        "unit": unit,
        "change_percent": change_percent,
        "previous_close": previous_close,
    }
    result = client.table(TABLE).insert(payload).execute()
    return result.data[0]


async def get_prices(
    instrument_type: str | None = None,
    asset: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch market prices with optional filters, ordered by most recent."""
    client = get_supabase()
    query = client.table(TABLE).select("*")

    if instrument_type:
        query = query.eq("instrument_type", instrument_type)
    if asset:
        query = query.eq("asset", asset)

    result = (
        query
        .order("timestamp", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return result.data


async def get_latest_prices(
    instrument_type: str | None = None,
) -> list[dict]:
    """Return the single most recent price per asset, optionally filtered by type.

    Uses a large fetch window then deduplicates in Python since Supabase
    REST doesn't support DISTINCT ON directly.
    """
    client = get_supabase()
    query = client.table(TABLE).select("*")

    if instrument_type:
        query = query.eq("instrument_type", instrument_type)

    result = query.order("timestamp", desc=True).limit(500).execute()

    seen: set[str] = set()
    latest: list[dict] = []
    for row in result.data:
        key = row["asset"]
        if key not in seen:
            seen.add(key)
            latest.append(row)
    return latest
