from app.core.database import get_pool, parse_ts, record_to_dict

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


async def get_indicators_latest(country: str | None = None) -> list[dict]:
    """Return the latest value per (indicator_name, country) for list views."""
    pool = await get_pool()
    if country:
        rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY indicator_name, country ORDER BY timestamp DESC) AS rn
                FROM {TABLE} WHERE country = $1
            ) sub WHERE rn = 1
            ORDER BY indicator_name, country
            """,
            country,
        )
    else:
        rows = await pool.fetch(
            f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY indicator_name, country ORDER BY timestamp DESC) AS rn
                FROM {TABLE}
            ) sub WHERE rn = 1
            ORDER BY indicator_name, country
            """
        )
    return [record_to_dict(r) for r in rows]


async def insert_indicator(payload: dict) -> dict | None:
    """Insert a macro-economic indicator row. Idempotent: ON CONFLICT DO NOTHING.
    Returns the created row, or None if row already existed (duplicate key)."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE} (indicator_name, value, country, timestamp, unit, source)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (indicator_name, country, "timestamp") DO NOTHING
        RETURNING *
        """,
        payload["indicator_name"],
        payload["value"],
        payload["country"],
        parse_ts(payload["timestamp"]),
        payload.get("unit"),
        payload.get("source"),
    )
    if row is None:
        return None
    return record_to_dict(row)


async def get_existing_indicator(indicator_name: str, country: str, timestamp) -> dict | None:
    """Fetch existing row by (indicator_name, country, timestamp) for 200 response on conflict."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"SELECT * FROM {TABLE} WHERE indicator_name = $1 AND country = $2 AND timestamp = $3",
        indicator_name,
        country,
        parse_ts(timestamp),
    )
    if row is None:
        return None
    return record_to_dict(row)
