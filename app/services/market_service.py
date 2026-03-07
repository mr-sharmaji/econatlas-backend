from app.core.supabase_client import get_supabase

TABLE = "market_prices"


async def insert_price(
    asset: str,
    price: float,
    timestamp: str,
    source: str | None = None,
    instrument_type: str | None = None,
    unit: str | None = None,
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
    }
    result = client.table(TABLE).insert(payload).execute()
    return result.data[0]
