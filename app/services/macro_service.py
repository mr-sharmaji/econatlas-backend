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


async def insert_indicators_batch_upsert_source_timestamp(rows: list[dict]) -> int:
    """Insert or update macro rows using provider/source timestamps.
    Uniqueness is (indicator_name, country, timestamp)."""
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for r in rows:
            await conn.execute(
                f"""
                INSERT INTO {TABLE} (indicator_name, value, country, timestamp, unit, source)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (indicator_name, country, "timestamp")
                DO UPDATE SET
                    value = EXCLUDED.value,
                    unit = COALESCE(EXCLUDED.unit, {TABLE}.unit),
                    source = EXCLUDED.source
                """,
                r.get("indicator_name"),
                r.get("value"),
                r.get("country"),
                parse_ts(r.get("timestamp")),
                r.get("unit"),
                r.get("source"),
            )
            count += 1
    return count


async def insert_indicators_batch_upsert_daily(rows: list[dict]) -> int:
    """Backward-compatible alias.
    Prefer insert_indicators_batch_upsert_source_timestamp for new code."""
    return await insert_indicators_batch_upsert_source_timestamp(rows)


async def delete_rows_newer_than_source_timestamps(
    rows: list[dict],
    sources: set[str] | None = None,
) -> int:
    """Delete legacy rows that are newer than provider/source timestamps.
    Useful when migrating from synthetic daily timestamps to source timestamps."""
    if not rows:
        return 0
    source_filter = {s.lower() for s in (sources or set())}
    candidates: set[tuple[str, str, str, str]] = set()
    for r in rows:
        indicator = r.get("indicator_name")
        country = r.get("country")
        source = str(r.get("source") or "").lower()
        ts = r.get("timestamp")
        if not indicator or not country or not ts:
            continue
        if source_filter and source not in source_filter:
            continue
        candidates.add((str(indicator), str(country), source, str(ts)))

    if not candidates:
        return 0

    pool = await get_pool()
    deleted = 0
    async with pool.acquire() as conn:
        for indicator, country, source, ts in candidates:
            status = await conn.execute(
                f"""
                DELETE FROM {TABLE}
                WHERE indicator_name = $1
                  AND country = $2
                  AND LOWER(COALESCE(source, '')) = $3
                  AND "timestamp" > $4
                """,
                indicator,
                country,
                source,
                parse_ts(ts),
            )
            try:
                deleted += int(str(status).split()[-1])
            except Exception:
                continue
    return deleted


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


async def get_institutional_flows_overview(*, sessions: int = 7) -> dict:
    """Return latest FII/DII flows summary and short combined trend for Overview."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        latest_rows = await conn.fetch(
            f"""
            SELECT indicator_name, value, "timestamp"
            FROM (
                SELECT indicator_name, value, "timestamp",
                       ROW_NUMBER() OVER (PARTITION BY indicator_name ORDER BY "timestamp" DESC) AS rn
                FROM {TABLE}
                WHERE country = 'IN'
                  AND indicator_name = ANY($1::text[])
            ) ranked
            WHERE rn = 1
            """,
            ["fii_net_cash", "dii_net_cash"],
        )

        trend_rows = await conn.fetch(
            f"""
            WITH daily_latest AS (
                SELECT DISTINCT ON (indicator_name, session_date)
                    indicator_name,
                    session_date,
                    value,
                    "timestamp"
                FROM (
                    SELECT indicator_name,
                           value,
                           "timestamp",
                           (("timestamp" AT TIME ZONE 'Asia/Kolkata')::date) AS session_date
                    FROM {TABLE}
                    WHERE country = 'IN'
                      AND indicator_name = ANY($1::text[])
                ) raw
                ORDER BY indicator_name, session_date, "timestamp" DESC
            ),
            rollup AS (
                SELECT
                    session_date,
                    MAX(CASE WHEN indicator_name = 'fii_net_cash' THEN value END) AS fii_value,
                    MAX(CASE WHEN indicator_name = 'dii_net_cash' THEN value END) AS dii_value,
                    MAX("timestamp") AS as_of
                FROM daily_latest
                GROUP BY session_date
                ORDER BY session_date DESC
                LIMIT $2
            )
            SELECT
                session_date,
                fii_value,
                dii_value,
                COALESCE(fii_value, 0) + COALESCE(dii_value, 0) AS combined_value,
                as_of
            FROM rollup
            ORDER BY session_date ASC
            """,
            ["fii_net_cash", "dii_net_cash"],
            sessions,
        )

    latest_by_indicator: dict[str, tuple[float | None, object | None]] = {}
    for row in latest_rows:
        latest_by_indicator[str(row["indicator_name"])] = (
            float(row["value"]) if row["value"] is not None else None,
            row["timestamp"],
        )

    fii_value = latest_by_indicator.get("fii_net_cash", (None, None))[0]
    dii_value = latest_by_indicator.get("dii_net_cash", (None, None))[0]
    fii_ts = latest_by_indicator.get("fii_net_cash", (None, None))[1]
    dii_ts = latest_by_indicator.get("dii_net_cash", (None, None))[1]

    as_of = None
    if fii_ts is not None and dii_ts is not None:
        as_of = fii_ts if fii_ts >= dii_ts else dii_ts
    else:
        as_of = fii_ts or dii_ts

    trend: list[dict] = []
    for row in trend_rows:
        trend.append(
            {
                "session_date": row["session_date"],
                "fii_value": float(row["fii_value"]) if row["fii_value"] is not None else None,
                "dii_value": float(row["dii_value"]) if row["dii_value"] is not None else None,
                "combined_value": float(row["combined_value"]),
                "as_of": row["as_of"],
            }
        )

    combined_value = None
    if fii_value is not None or dii_value is not None:
        combined_value = float(fii_value or 0.0) + float(dii_value or 0.0)

    return {
        "as_of": as_of,
        "fii_value": fii_value,
        "dii_value": dii_value,
        "combined_value": combined_value,
        "trend": trend,
    }
