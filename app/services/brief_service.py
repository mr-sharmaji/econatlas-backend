from __future__ import annotations

from datetime import datetime, timezone

from app.core.database import get_pool, parse_ts, record_to_dict

TABLE = "stock_snapshots"
_VALID_MARKETS = {"IN"}


def _normalize_market(market: str | None) -> str:
    mk = (market or "IN").strip().upper()
    return mk if mk in _VALID_MARKETS else "IN"


async def upsert_stock_snapshots(rows: list[dict]) -> int:
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for r in rows:
            await conn.execute(
                f"""
                INSERT INTO {TABLE}
                (market, symbol, display_name, sector, last_price, point_change, percent_change,
                 volume, traded_value, source_timestamp, ingested_at, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11)
                ON CONFLICT (market, symbol)
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    sector = EXCLUDED.sector,
                    last_price = EXCLUDED.last_price,
                    point_change = EXCLUDED.point_change,
                    percent_change = EXCLUDED.percent_change,
                    volume = EXCLUDED.volume,
                    traded_value = EXCLUDED.traded_value,
                    source_timestamp = EXCLUDED.source_timestamp,
                    ingested_at = NOW(),
                    source = EXCLUDED.source
                """,
                _normalize_market(r.get("market")),
                str(r.get("symbol") or ""),
                str(r.get("display_name") or r.get("symbol") or ""),
                r.get("sector"),
                float(r.get("last_price") or 0.0),
                float(r.get("point_change")) if r.get("point_change") is not None else None,
                float(r.get("percent_change")) if r.get("percent_change") is not None else None,
                int(r.get("volume")) if r.get("volume") is not None else None,
                float(r.get("traded_value")) if r.get("traded_value") is not None else None,
                parse_ts(r.get("source_timestamp")) or datetime.now(timezone.utc),
                r.get("source"),
            )
            count += 1
    return count


async def _as_of(market: str) -> datetime | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        f'SELECT MAX(source_timestamp) AS as_of FROM {TABLE} WHERE market = $1',
        market,
    )
    value = row["as_of"] if row else None
    return parse_ts(value)


async def get_movers(
    market: str,
    movers_type: str,
    limit: int = 10,
) -> dict:
    market = _normalize_market(market)
    direction = "DESC" if movers_type == "gainers" else "ASC"
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT symbol, display_name, market, last_price, point_change, percent_change,
               volume, traded_value, sector, source_timestamp, ingested_at
        FROM {TABLE}
        WHERE market = $1 AND percent_change IS NOT NULL
        ORDER BY percent_change {direction}, symbol ASC
        LIMIT $2
        """,
        market,
        limit,
    )
    items = [record_to_dict(r) for r in rows]
    return {
        "market": market,
        "as_of": await _as_of(market),
        "items": items,
        "count": len(items),
    }


async def get_most_active(
    market: str,
    limit: int = 10,
) -> dict:
    market = _normalize_market(market)
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT symbol, display_name, market, last_price, point_change, percent_change,
               volume, traded_value, sector, source_timestamp, ingested_at
        FROM {TABLE}
        WHERE market = $1
        ORDER BY traded_value DESC NULLS LAST, volume DESC NULLS LAST, symbol ASC
        LIMIT $2
        """,
        market,
        limit,
    )
    items = [record_to_dict(r) for r in rows]
    return {
        "market": market,
        "as_of": await _as_of(market),
        "items": items,
        "count": len(items),
    }


async def get_sector_pulse(
    market: str,
    limit: int = 8,
) -> dict:
    market = _normalize_market(market)
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT
            COALESCE(NULLIF(sector, ''), 'Other') AS sector,
            ROUND(AVG(COALESCE(percent_change, 0))::numeric, 2) AS avg_change_percent,
            COUNT(*) FILTER (WHERE percent_change > 0) AS gainers,
            COUNT(*) FILTER (WHERE percent_change < 0) AS losers,
            COUNT(*) AS count
        FROM {TABLE}
        WHERE market = $1
        GROUP BY COALESCE(NULLIF(sector, ''), 'Other')
        ORDER BY avg_change_percent DESC, sector ASC
        LIMIT $2
        """,
        market,
        limit,
    )
    sectors = [record_to_dict(r) for r in rows]
    return {
        "market": market,
        "as_of": await _as_of(market),
        "sectors": sectors,
        "count": len(sectors),
    }


async def get_post_market_overview(market: str) -> dict:
    market = _normalize_market(market)
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT
            COUNT(*) AS total_stocks,
            COUNT(*) FILTER (WHERE percent_change > 0) AS advancers,
            COUNT(*) FILTER (WHERE percent_change < 0) AS decliners,
            COUNT(*) FILTER (WHERE percent_change = 0 OR percent_change IS NULL) AS unchanged,
            ROUND(AVG(percent_change)::numeric, 2) AS avg_change_percent
        FROM {TABLE}
        WHERE market = $1
        """,
        market,
    )
    sector_rows = await pool.fetch(
        f"""
        SELECT
            COALESCE(NULLIF(sector, ''), 'Other') AS sector,
            ROUND(AVG(COALESCE(percent_change, 0))::numeric, 2) AS avg_change_percent
        FROM {TABLE}
        WHERE market = $1
        GROUP BY COALESCE(NULLIF(sector, ''), 'Other')
        ORDER BY avg_change_percent DESC
        """,
        market,
    )
    total = int(row["total_stocks"] or 0) if row else 0
    adv = int(row["advancers"] or 0) if row else 0
    dec = int(row["decliners"] or 0) if row else 0
    unch = int(row["unchanged"] or 0) if row else 0
    avg = float(row["avg_change_percent"]) if row and row["avg_change_percent"] is not None else None

    top_sector = None
    bottom_sector = None
    driver_tags: list[str] = []
    if sector_rows:
        top_sector = str(sector_rows[0]["sector"])
        bottom_sector = str(sector_rows[-1]["sector"])
        driver_tags.append(f"Leaders: {top_sector}")
        driver_tags.append(f"Laggards: {bottom_sector}")
    breadth = "mixed"
    if adv > dec:
        breadth = "positive"
        driver_tags.insert(0, "Breadth positive")
    elif dec > adv:
        breadth = "negative"
        driver_tags.insert(0, "Breadth weak")
    else:
        driver_tags.insert(0, "Breadth balanced")

    pct_text = f"{avg:+.2f}%" if avg is not None else "N/A"
    summary = (
        f"{market} breadth {breadth}: {adv} advancing, {dec} declining, {unch} unchanged. "
        f"Average move {pct_text}."
    )

    return {
        "market": market,
        "as_of": await _as_of(market),
        "total_stocks": total,
        "advancers": adv,
        "decliners": dec,
        "unchanged": unch,
        "avg_change_percent": avg,
        "top_sector": top_sector,
        "bottom_sector": bottom_sector,
        "summary": summary,
        "driver_tags": driver_tags,
    }
