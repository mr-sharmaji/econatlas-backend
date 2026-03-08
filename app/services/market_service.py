from datetime import datetime, timezone, timedelta

from app.core.database import get_pool, parse_ts, record_to_dict

TABLE = "market_prices"
TABLE_INTRADAY = "market_prices_intraday"


def _one_per_day(rows: list[dict]) -> list[dict]:
    """Keep one row per (asset, instrument_type, calendar day); rows must be ordered by timestamp DESC.
    Used so chart history has one point per day even if scheduler previously wrote multiple intraday rows."""
    seen: set[tuple[str, str, str]] = set()
    out = []
    for r in rows:
        ts = r.get("timestamp")
        if ts is None:
            out.append(r)
            continue
        if hasattr(ts, "date"):
            day = ts.date().isoformat()
        elif isinstance(ts, str):
            try:
                day = ts[:10]
            except Exception:
                out.append(r)
                continue
        else:
            out.append(r)
            continue
        key = (str(r.get("asset", "")), str(r.get("instrument_type") or ""), day)
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


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
        ON CONFLICT (asset, instrument_type, "timestamp") DO NOTHING
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
                ON CONFLICT (asset, instrument_type, "timestamp") DO NOTHING
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


async def insert_prices_batch_upsert_daily(rows: list[dict]) -> int:
    """Insert or update rows so there is at most one row per (asset, instrument_type, date).
    Use for scheduler: pass rows with timestamp = today 00:00 UTC so we upsert 'today' instead of inserting many intraday rows.
    Returns count of rows processed (inserted or updated)."""
    if not rows:
        return 0
    pool = await get_pool()
    updated = 0
    async with pool.acquire() as conn:
        for r in rows:
            await conn.execute(
                f"""
                INSERT INTO {TABLE}
                (asset, price, timestamp, source, instrument_type, unit, change_percent, previous_close)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (asset, instrument_type, "timestamp")
                DO UPDATE SET
                    price = EXCLUDED.price,
                    source = EXCLUDED.source,
                    unit = COALESCE(EXCLUDED.unit, {TABLE}.unit),
                    change_percent = EXCLUDED.change_percent,
                    previous_close = EXCLUDED.previous_close
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
            updated += 1
    return updated


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
    out = [record_to_dict(r) for r in rows]
    # Chart use case: one point per day (dedupe by date so history is daily even if old intraday rows exist)
    if asset and (limit == -1 or limit > 500):
        out = _one_per_day(out)
        out.sort(key=lambda x: (x.get("timestamp") or ""), reverse=False)
    return out


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


# ---- Intraday (1D live) ----

def _round_to_minute(utc_dt: datetime) -> datetime:
    """Round to minute for intraday storage (limits rows per run)."""
    return utc_dt.replace(second=0, microsecond=0)


async def insert_intraday_batch(rows: list[dict]) -> int:
    """Insert intraday price points (asset, instrument_type, price, timestamp). Returns count inserted."""
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for r in rows:
            await conn.execute(
                f"""
                INSERT INTO {TABLE_INTRADAY} (asset, instrument_type, price, "timestamp")
                VALUES ($1, $2, $3, $4)
                """,
                r.get("asset"),
                r.get("instrument_type"),
                r.get("price"),
                parse_ts(r.get("timestamp")),
            )
            count += 1
    return count


async def get_intraday(
    asset: str,
    instrument_type: str,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
) -> list[dict]:
    """Return intraday points for the most recent session (calendar day with data).
    When market is open that is today; when closed it is the last trading day from backfill.
    Optional from_ts/to_ts override the window (e.g. last 24h)."""
    pool = await get_pool()
    if from_ts is not None and to_ts is not None:
        day_start, day_end = from_ts, to_ts
    else:
        row = await pool.fetchrow(
            f"""
            SELECT MAX("timestamp") as max_ts
            FROM {TABLE_INTRADAY}
            WHERE asset = $1 AND instrument_type = $2
            """,
            asset,
            instrument_type,
        )
        if not row or row["max_ts"] is None:
            return []
        max_ts = row["max_ts"]
        if getattr(max_ts, "tzinfo", None) is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        day_start = datetime(max_ts.year, max_ts.month, max_ts.day, 0, 0, 0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
    rows = await pool.fetch(
        f"""
        SELECT "timestamp", price
        FROM {TABLE_INTRADAY}
        WHERE asset = $1 AND instrument_type = $2
          AND "timestamp" >= $3 AND "timestamp" < $4
        ORDER BY "timestamp" ASC
        """,
        asset,
        instrument_type,
        day_start,
        day_end,
    )
    return [{"timestamp": r["timestamp"].isoformat() if hasattr(r["timestamp"], "isoformat") else r["timestamp"], "price": float(r["price"])} for r in rows]


async def cleanup_intraday_older_than(days: int = 2) -> int:
    """Delete intraday rows older than given days. Returns count deleted."""
    pool = await get_pool()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = await pool.execute(
        f'DELETE FROM {TABLE_INTRADAY} WHERE "timestamp" < $1',
        cutoff,
    )
    # e.g. "DELETE 42"
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0
