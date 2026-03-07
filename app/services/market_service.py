from app.core.database import get_pool, parse_ts, record_to_dict

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
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE}
        (asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING *
        """,
        asset,
        price,
        parse_ts(timestamp),
        source,
        instrument_type,
        unit,
        change_percent,
        previous_close,
    )
    return record_to_dict(row)


async def insert_prices_batch(rows: list[dict]) -> int:
    """Insert multiple price rows in one request. Returns count inserted."""
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for r in rows:
            await conn.execute(
                f"""
                INSERT INTO {TABLE}
                (asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                r.get("asset"),
                r.get("price"),
                parse_ts(r.get("timestamp")),
                r.get("source"),
                r.get("instrument_type"),
                r.get("unit"),
                r.get("change_percent"),
                r.get("previous_close"),
            )
            count += 1
    return count


async def get_prices(
    instrument_type: str | None = None,
    asset: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """Fetch market prices with optional filters, ordered by most recent."""
    pool = await get_pool()
    conditions = []
    args = []
    n = 1
    if instrument_type:
        conditions.append(f"instrument_type = ${n}")
        args.append(instrument_type)
        n += 1
    if asset:
        conditions.append(f"asset = ${n}")
        args.append(asset)
        n += 1
    where = " AND ".join(conditions) if conditions else "TRUE"
    args.extend([limit, offset])
    rows = await pool.fetch(
        f"SELECT * FROM {TABLE} WHERE {where} ORDER BY timestamp DESC LIMIT ${n} OFFSET ${n + 1}",
        *args,
    )
    return [record_to_dict(r) for r in rows]


async def get_latest_prices(
    instrument_type: str | None = None,
) -> list[dict]:
    """Return the single most recent price per asset, optionally filtered by type."""
    pool = await get_pool()
    if instrument_type:
        rows = await pool.fetch(
            f"""
            SELECT DISTINCT ON (asset) * FROM {TABLE}
            WHERE instrument_type = $1
            ORDER BY asset, timestamp DESC
            """,
            instrument_type,
        )
    else:
        rows = await pool.fetch(
            f"""
            SELECT DISTINCT ON (asset) * FROM {TABLE}
            ORDER BY asset, timestamp DESC
            """
        )
    return [record_to_dict(r) for r in rows]
