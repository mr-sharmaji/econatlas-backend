from app.core.database import get_pool, record_to_dict

TABLE = "macro_indicators"


async def get_indicators(
    country: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch macro-economic indicators, optionally filtered by country."""
    pool = await get_pool()
    if country:
        rows = await pool.fetch(
            f"SELECT * FROM {TABLE} WHERE country = $1 ORDER BY timestamp DESC LIMIT $2 OFFSET $3",
            country,
            limit,
            offset,
        )
    else:
        rows = await pool.fetch(
            f"SELECT * FROM {TABLE} ORDER BY timestamp DESC LIMIT $1 OFFSET $2",
            limit,
            offset,
        )
    return [record_to_dict(r) for r in rows]


async def insert_indicator(payload: dict) -> dict:
    """Insert a macro-economic indicator row and return the created row."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE} (indicator_name, value, country, timestamp)
        VALUES ($1, $2, $3, $4::timestamptz)
        RETURNING *
        """,
        payload["indicator_name"],
        payload["value"],
        payload["country"],
        payload["timestamp"],
    )
    return record_to_dict(row)
