from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from app.core.database import get_pool, parse_ts, record_to_dict


def _normalize_status(status: str | None) -> str:
    s = (status or "open").strip().lower()
    return s if s in {"open", "upcoming"} else "open"


def _normalize_symbols(symbols: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in symbols:
        s = (raw or "").strip().upper()
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _default_ipo_rows() -> list[dict]:
    today = date.today()
    return [
        {
            "symbol": "HEXATECH",
            "company_name": "Hexa Tech Systems",
            "market": "IN",
            "status": "open",
            "ipo_type": "mainboard",
            "issue_size_cr": 1280.0,
            "price_band": "₹490-515",
            "gmp_percent": 18.4,
            "subscription_multiple": 12.6,
            "open_date": today - timedelta(days=1),
            "close_date": today + timedelta(days=1),
            "listing_date": today + timedelta(days=5),
            "source": "curated_feed",
        },
        {
            "symbol": "ZENSME",
            "company_name": "Zen Renewables SME",
            "market": "IN",
            "status": "open",
            "ipo_type": "sme",
            "issue_size_cr": 92.0,
            "price_band": "₹118-124",
            "gmp_percent": 5.7,
            "subscription_multiple": 1.8,
            "open_date": today - timedelta(days=1),
            "close_date": today + timedelta(days=1),
            "listing_date": today + timedelta(days=4),
            "source": "curated_feed",
        },
        {
            "symbol": "AURAAUTO",
            "company_name": "Aura Auto Components",
            "market": "IN",
            "status": "upcoming",
            "ipo_type": "mainboard",
            "issue_size_cr": 2250.0,
            "price_band": "₹360-378",
            "gmp_percent": 14.2,
            "subscription_multiple": None,
            "open_date": today + timedelta(days=2),
            "close_date": today + timedelta(days=4),
            "listing_date": today + timedelta(days=9),
            "source": "curated_feed",
        },
        {
            "symbol": "KALPASTEEL",
            "company_name": "Kalpa Steel Works",
            "market": "IN",
            "status": "upcoming",
            "ipo_type": "mainboard",
            "issue_size_cr": 640.0,
            "price_band": "₹205-214",
            "gmp_percent": 3.8,
            "subscription_multiple": None,
            "open_date": today + timedelta(days=4),
            "close_date": today + timedelta(days=6),
            "listing_date": today + timedelta(days=11),
            "source": "curated_feed",
        },
        {
            "symbol": "NEXUSSME",
            "company_name": "Nexus Precision SME",
            "market": "IN",
            "status": "upcoming",
            "ipo_type": "sme",
            "issue_size_cr": 54.0,
            "price_band": "₹86-92",
            "gmp_percent": 22.5,
            "subscription_multiple": None,
            "open_date": today + timedelta(days=3),
            "close_date": today + timedelta(days=5),
            "listing_date": today + timedelta(days=10),
            "source": "curated_feed",
        },
    ]


async def _ensure_seed_rows() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM ipo_snapshots")
        if count and int(count) > 0:
            return
        rows = _default_ipo_rows()
        for r in rows:
            await conn.execute(
                """
                INSERT INTO ipo_snapshots
                (symbol, company_name, market, status, ipo_type, issue_size_cr, price_band,
                 gmp_percent, subscription_multiple, open_date, close_date, listing_date,
                 source_timestamp, ingested_at, source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW(), $13)
                ON CONFLICT (symbol)
                DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    market = EXCLUDED.market,
                    status = EXCLUDED.status,
                    ipo_type = EXCLUDED.ipo_type,
                    issue_size_cr = EXCLUDED.issue_size_cr,
                    price_band = EXCLUDED.price_band,
                    gmp_percent = EXCLUDED.gmp_percent,
                    subscription_multiple = EXCLUDED.subscription_multiple,
                    open_date = EXCLUDED.open_date,
                    close_date = EXCLUDED.close_date,
                    listing_date = EXCLUDED.listing_date,
                    source_timestamp = NOW(),
                    ingested_at = NOW(),
                    source = EXCLUDED.source
                """,
                r["symbol"],
                r["company_name"],
                r["market"],
                r["status"],
                r["ipo_type"],
                r["issue_size_cr"],
                r["price_band"],
                r["gmp_percent"],
                r["subscription_multiple"],
                r["open_date"],
                r["close_date"],
                r["listing_date"],
                r["source"],
            )


def _recommendation(status: str, gmp_percent: float | None, subscription_multiple: float | None) -> tuple[str, str]:
    gmp = float(gmp_percent or 0.0)
    sub = float(subscription_multiple or 0.0)
    if status == "open":
        score = (gmp * 0.65) + (sub * 3.5)
        if gmp >= 16 and sub >= 6:
            return "apply", "Strong GMP with solid subscription momentum"
        if score >= 22:
            return "apply", "Healthy pricing signals across GMP and subscription"
        if gmp <= 5 and sub <= 1.5:
            return "avoid", "Weak GMP and low demand trend"
        if score <= 10:
            return "avoid", "Demand indicators are currently soft"
        return "watch", "Mixed indicators; wait for stronger demand confirmation"

    # Upcoming IPOs rely primarily on GMP trend at this stage.
    if gmp >= 18:
        return "apply", "High GMP trend indicates strong listing interest"
    if gmp <= 6:
        return "avoid", "Low GMP trend indicates limited near-term upside"
    return "watch", "Moderate GMP; monitor pre-open demand closely"


async def get_ipos(*, status: str = "open", limit: int = 20) -> dict:
    status = _normalize_status(status)
    await _ensure_seed_rows()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, company_name, market, status, ipo_type, issue_size_cr, price_band,
                   gmp_percent, subscription_multiple, open_date, close_date, listing_date,
                   source_timestamp
            FROM ipo_snapshots
            WHERE status = $1
            ORDER BY
                CASE WHEN $1 = 'open' THEN close_date END ASC NULLS LAST,
                CASE WHEN $1 = 'upcoming' THEN open_date END ASC NULLS LAST,
                symbol ASC
            LIMIT $2
            """,
            status,
            limit,
        )
        as_of = await conn.fetchval(
            "SELECT MAX(source_timestamp) FROM ipo_snapshots WHERE status = $1",
            status,
        )

    items: list[dict] = []
    for row in rows:
        item = record_to_dict(row)
        rec, reason = _recommendation(
            status=str(item.get("status") or status),
            gmp_percent=item.get("gmp_percent"),
            subscription_multiple=item.get("subscription_multiple"),
        )
        item["recommendation"] = rec
        item["recommendation_reason"] = reason
        items.append(item)

    return {
        "status": status,
        "as_of": parse_ts(as_of),
        "items": items,
        "count": len(items),
    }


async def get_ipo_alerts(device_id: str) -> list[str]:
    await _ensure_seed_rows()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol
            FROM device_ipo_alerts
            WHERE device_id = $1
            ORDER BY updated_at ASC, symbol ASC
            """,
            device_id,
        )
    return [str(r["symbol"]) for r in rows]


async def put_ipo_alerts(device_id: str, symbols: list[str]) -> list[str]:
    await _ensure_seed_rows()
    normalized = _normalize_symbols(symbols)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if normalized:
            known_rows = await conn.fetch(
                "SELECT symbol FROM ipo_snapshots WHERE symbol = ANY($1::text[])",
                normalized,
            )
            known = {str(r["symbol"]) for r in known_rows}
            unknown = [s for s in normalized if s not in known]
            if unknown:
                raise ValueError(f"Unknown IPO symbols: {', '.join(unknown)}")
        async with conn.transaction():
            await conn.execute("DELETE FROM device_ipo_alerts WHERE device_id = $1", device_id)
            for s in normalized:
                await conn.execute(
                    """
                    INSERT INTO device_ipo_alerts (device_id, symbol, updated_at)
                    VALUES ($1, $2, NOW())
                    """,
                    device_id,
                    s,
                )
    return normalized
