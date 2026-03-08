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
) -> dict | None:
    """Insert one market/commodity price point. Idempotent: ON CONFLICT DO NOTHING. Returns created row or None if already existed."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE}
        (asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (asset, instrument_type, timestamp) DO NOTHING
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
    if row is None:
        return None
    return record_to_dict(row)


async def get_latest_price_per_asset_type(
    asset_type_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], float]:
    """Return map (asset, instrument_type) -> latest price for given pairs."""
    if not asset_type_pairs:
        return {}
    pool = await get_pool()
    # One row per (asset, instrument_type) with latest price; we filter in Python
    rows = await pool.fetch(
        f"""
        SELECT DISTINCT ON (asset, instrument_type) asset, instrument_type, price
        FROM {TABLE}
        ORDER BY asset, instrument_type, timestamp DESC
        """
    )
    key_to_price = {(r["asset"], r["instrument_type"] or ""): float(r["price"]) for r in rows}
    need = set(asset_type_pairs)
    return {k: key_to_price[k] for k in need if k in key_to_price}


async def insert_prices_batch(rows: list[dict]) -> int:
    """Insert multiple price rows. Idempotent: ON CONFLICT DO NOTHING. Returns count actually inserted."""
    if not rows:
        return 0
    pool = await get_pool()
    inserted = 0
    async with pool.acquire() as conn:
        for r in rows:
            result = await conn.execute(
                f"""
                INSERT INTO {TABLE}
                (asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (asset, instrument_type, timestamp) DO NOTHING
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
            # asyncpg execute returns e.g. "INSERT 0 1" or "INSERT 0 0"
            if result:
                parts = result.split()
                if len(parts) >= 3:
                    inserted += int(parts[-1])
    return inserted


# Tolerance for float price comparison (avoids duplicates when markets closed)
_PRICE_CHANGE_TOLERANCE = 1e-9


async def insert_prices_batch_skip_unchanged(
    rows: list[dict],
    tolerance: float = _PRICE_CHANGE_TOLERANCE,
) -> tuple[int, int]:
    """Insert only rows where price changed from latest for that (asset, instrument_type). Returns (inserted, skipped)."""
    if not rows:
        return 0, 0
    pairs = [(r.get("asset"), r.get("instrument_type") or "") for r in rows]
    latest = await get_latest_price_per_asset_type(pairs)
    to_insert = []
    for r in rows:
        key = (r.get("asset"), r.get("instrument_type") or "")
        new_price = r.get("price")
        if isinstance(new_price, (int, float)):
            new_price = float(new_price)
        else:
            to_insert.append(r)
            continue
        last = latest.get(key)
        if last is None:
            to_insert.append(r)
            continue
        if abs(new_price - last) <= tolerance:
            continue
        to_insert.append(r)
    inserted = await insert_prices_batch(to_insert) if to_insert else 0
    skipped = len(rows) - inserted
    return inserted, skipped


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
    """Return the single most recent price per asset, optionally filtered by type.
    When change_percent is null, it is filled from the previous day's price so the app can show % change (e.g. Nifty Bank)."""
    pool = await get_pool()
    if instrument_type:
        rows = await pool.fetch(
            """
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY asset, instrument_type ORDER BY timestamp DESC) AS rn
                FROM market_prices
                WHERE instrument_type = $1
            )
            SELECT id, asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close,
                   (SELECT price FROM market_prices m2
                    WHERE m2.asset = ranked.asset AND m2.instrument_type = ranked.instrument_type
                      AND m2.timestamp < ranked.timestamp
                    ORDER BY m2.timestamp DESC LIMIT 1) AS prev_price
            FROM ranked WHERE rn = 1
            """,
            instrument_type,
        )
    else:
        rows = await pool.fetch(
            f"""
            WITH ranked AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY asset, instrument_type ORDER BY timestamp DESC) AS rn
                FROM {TABLE}
            )
            SELECT id, asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close,
                   (SELECT price FROM market_prices m2
                    WHERE m2.asset = ranked.asset AND m2.instrument_type = ranked.instrument_type
                      AND m2.timestamp < ranked.timestamp
                    ORDER BY m2.timestamp DESC LIMIT 1) AS prev_price
            FROM ranked WHERE rn = 1
            """
        )
    out = []
    for r in rows:
        d = record_to_dict(r)
        prev = d.pop("prev_price", None)
        if d.get("change_percent") is None and prev is not None and isinstance(prev, (int, float)):
            try:
                p = float(d["price"])
                pv = float(prev)
                if pv and pv != 0:
                    d["change_percent"] = round(((p - pv) / pv) * 100, 2)
                    d["previous_close"] = pv
            except (TypeError, ZeroDivisionError):
                pass
        out.append(d)
    return out
