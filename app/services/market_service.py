from app.core.supabase_client import get_supabase

TABLE = "market_prices"


async def insert_price(asset: str, price: float, timestamp: str) -> dict:
    """Insert one market/commodity price point and return created row."""
    client = get_supabase()
    payload = {
        "asset": asset,
        "price": price,
        "timestamp": timestamp,
    }
    result = client.table(TABLE).insert(payload).execute()
    return result.data[0]
