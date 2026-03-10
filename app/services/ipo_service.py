from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime, timedelta, timezone
from html import unescape

import requests

from app.core.database import get_pool, parse_ts, record_to_dict

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))
_LIVE_SOURCE = "investorgain_webnode"
_SYNC_TTL_SECONDS = 180
_LIVE_REPORT_ID = 331
_LIVE_BASE_URL = "https://webnodejs.investorgain.com/cloud/new/report/data-read"


def _normalize_status(status: str | None) -> str:
    s = (status or "open").strip().lower()
    return s if s in {"open", "upcoming", "closed"} else "open"


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


def _strip_text(value: object | None) -> str:
    if value is None:
        return ""
    s = unescape(str(value))
    s = re.sub(r"<[^>]*>", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _slug_symbol(name: str) -> str:
    compact = re.sub(r"[^A-Z0-9]+", "", name.upper())
    if not compact:
        return "IPOUNK"
    return compact[:12]


def _clean_company_name(value: object | None) -> str:
    name = _strip_text(value)
    if not name:
        return ""
    # Source names include suffixes like "IPO" or "NSE SME/BSE SME"; strip them for UI clarity.
    name = re.sub(r"\s+(?:NSE|BSE)\s+SME\s*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\s+IPO\s*$", "", name, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", name).strip(" -")


def _to_float(value: object | None) -> float | None:
    if value is None:
        return None
    s = _strip_text(value)
    if not s or s in {"-", "--", "N/A"}:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _parse_subscription(value: object | None) -> float | None:
    s = _strip_text(value).lower()
    if not s or s in {"-", "--", "na", "n/a"}:
        return None
    m = re.search(r"-?\d+(?:\.\d+)?", s.replace(",", ""))
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def _parse_date_value(value: object | None) -> date | None:
    s = _strip_text(value)
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _parse_source_ts(value: object | None, today_ist: date) -> datetime:
    text = _strip_text(value)
    if not text:
        return datetime.now(timezone.utc)

    for fmt in ("%d-%b %H:%M", "%d-%b %I:%M %p", "%d-%b"):
        try:
            parsed = datetime.strptime(text, fmt)
            candidate = parsed.replace(year=today_ist.year)
            if candidate.date() > (today_ist + timedelta(days=180)):
                candidate = candidate.replace(year=today_ist.year - 1)
            elif candidate.date() < (today_ist - timedelta(days=180)):
                candidate = candidate.replace(year=today_ist.year + 1)
            return candidate.replace(tzinfo=_IST).astimezone(timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)


def _parse_listing_price(row: dict) -> float | None:
    direct_keys = (
        "~listing_price",
        "~list_price",
        "~listing_at",
        "Listing Price",
        "Listing price",
        "Listing At",
        "Listed Price",
        "Listed at",
        "List Price",
    )
    for key in direct_keys:
        if key in row:
            value = _to_float(row.get(key))
            if value is not None:
                return value

    for key, raw in row.items():
        key_l = str(key).lower()
        if "list" not in key_l:
            continue
        if ("price" not in key_l) and ("at" not in key_l):
            continue
        value = _to_float(raw)
        if value is not None:
            return value

    # Listed rows often embed listing outcome inside Name HTML, e.g. "L@112.00 (0%)".
    for key in ("Name", "~ipo_name", "ipo_name"):
        raw = row.get(key)
        text = _strip_text(raw)
        if not text:
            continue
        match = re.search(r"\bL@\s*([0-9]+(?:,[0-9]{3})*(?:\.[0-9]+)?)\b", text)
        if match:
            try:
                return float(match.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def _issue_price_upper(price_band: object | None) -> float | None:
    text = _strip_text(price_band)
    if not text:
        return None
    numbers = re.findall(r"\d+(?:\.\d+)?", text.replace(",", ""))
    if not numbers:
        return None
    parsed: list[float] = []
    for n in numbers:
        try:
            parsed.append(float(n))
        except ValueError:
            continue
    if not parsed:
        return None
    return max(parsed)


def _listing_gain_pct(listing_price: object | None, price_band: object | None) -> float | None:
    try:
        listed = float(listing_price) if listing_price is not None else None
    except (TypeError, ValueError):
        listed = None
    upper = _issue_price_upper(price_band)
    if listed is None or upper is None or upper <= 0:
        return None
    return round(((listed - upper) / upper) * 100.0, 2)


def _financial_year(today_ist: date) -> str:
    if today_ist.month >= 4:
        start = today_ist.year
    else:
        start = today_ist.year - 1
    return f"{start}-{str(start + 1)[-2:]}"


def _fetch_rows_for_status(source_code: str, status: str, today_ist: date) -> list[dict]:
    url = (
        f"{_LIVE_BASE_URL}/{_LIVE_REPORT_ID}/1/"
        f"{today_ist.month}/{today_ist.year}/{_financial_year(today_ist)}/0/{source_code}"
    )
    response = requests.get(url, params={"search": ""}, timeout=15)
    response.raise_for_status()
    payload = response.json()
    if int(payload.get("msg", 0)) != 1:
        raise RuntimeError(f"IPO source returned msg={payload.get('msg')}")

    rows = payload.get("reportTableData") or []
    parsed: list[dict] = []
    for row in rows:
        ipo_name = _strip_text(row.get("~ipo_name") or row.get("Name"))
        if not ipo_name:
            continue
        company_name = _clean_company_name(ipo_name) or ipo_name

        raw_id = _strip_text(row.get("~id"))
        symbol = f"IPO{raw_id}" if raw_id else _slug_symbol(ipo_name)
        category_blob = f"{_strip_text(row.get('~IPO_Category'))} {_strip_text(row.get('Name'))}".upper()
        ipo_type = "sme" if "SME" in category_blob else "mainboard"
        gmp_percent = _to_float(row.get("~gmp_percent_calc"))
        if gmp_percent is None:
            gmp_percent = _to_float(row.get("GMP"))
        listing_price = _parse_listing_price(row)
        listing_gain_pct = _listing_gain_pct(
            listing_price=listing_price,
            price_band=row.get("Price (₹)"),
        )
        outcome_state = "listed" if listing_price is not None else None

        parsed.append(
            {
                "symbol": symbol,
                "company_name": company_name,
                "market": "IN",
                "status": status,
                "ipo_type": ipo_type,
                "issue_size_cr": _to_float(row.get("IPO Size (₹ in cr)")),
                "price_band": _strip_text(row.get("Price (₹)")) or None,
                "gmp_percent": gmp_percent,
                "subscription_multiple": _parse_subscription(row.get("Sub")),
                "listing_price": listing_price,
                "listing_gain_pct": listing_gain_pct,
                "outcome_state": outcome_state,
                "open_date": _parse_date_value(row.get("~Srt_Open")),
                "close_date": _parse_date_value(row.get("~Srt_Close")),
                "listing_date": _parse_date_value(row.get("~Str_Listing")),
                "source_timestamp": _parse_source_ts(row.get("Updated-On"), today_ist),
                "ingested_at": datetime.now(timezone.utc),
                "source": _LIVE_SOURCE,
            }
        )
    return parsed


def _fetch_live_rows() -> list[dict]:
    today_ist = datetime.now(_IST).date()
    combined: list[dict] = []
    seen: set[str] = set()
    for source_code, status in (("open", "open"), ("current", "upcoming")):
        rows = _fetch_rows_for_status(source_code=source_code, status=status, today_ist=today_ist)
        for row in rows:
            symbol = str(row["symbol"])
            if symbol in seen:
                continue
            seen.add(symbol)
            combined.append(row)
    return combined


async def _sync_live_rows(force: bool = False) -> None:
    pool = await get_pool()
    now_utc = datetime.now(timezone.utc)
    if not force:
        async with pool.acquire() as conn:
            last_ingested = await conn.fetchval(
                "SELECT MAX(ingested_at) FROM ipo_snapshots WHERE source = $1",
                _LIVE_SOURCE,
            )
        if (
            isinstance(last_ingested, datetime)
            and (now_utc - last_ingested).total_seconds() < _SYNC_TTL_SECONDS
        ):
            return

    try:
        rows = await asyncio.to_thread(_fetch_live_rows)
    except Exception:
        logger.exception("IPO live sync failed")
        return

    if not rows:
        logger.warning("IPO live sync returned zero rows; continuing with lifecycle cleanup only")

    symbols = [str(r["symbol"]) for r in rows]
    today_ist = datetime.now(_IST).date()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for r in rows:
                await conn.execute(
                    """
                    INSERT INTO ipo_snapshots
                    (symbol, company_name, market, status, ipo_type, issue_size_cr, price_band,
                     gmp_percent, subscription_multiple, listing_price, listing_gain_pct, outcome_state,
                     open_date, close_date, listing_date, source_timestamp, ingested_at, archived_at, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
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
                        listing_price = EXCLUDED.listing_price,
                        listing_gain_pct = EXCLUDED.listing_gain_pct,
                        outcome_state = EXCLUDED.outcome_state,
                        open_date = EXCLUDED.open_date,
                        close_date = EXCLUDED.close_date,
                        listing_date = EXCLUDED.listing_date,
                        source_timestamp = EXCLUDED.source_timestamp,
                        ingested_at = EXCLUDED.ingested_at,
                        archived_at = EXCLUDED.archived_at,
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
                    r.get("listing_price"),
                    r.get("listing_gain_pct"),
                    r.get("outcome_state"),
                    r["open_date"],
                    r["close_date"],
                    r["listing_date"],
                    r["source_timestamp"],
                    r["ingested_at"],
                    None,
                    r["source"],
                )
            if symbols:
                # Source no longer returns these symbols: roll them into closed lifecycle.
                await conn.execute(
                    """
                    UPDATE ipo_snapshots
                    SET status = 'closed'
                    WHERE source = $1
                      AND archived_at IS NULL
                      AND NOT (symbol = ANY($2::text[]))
                    """,
                    _LIVE_SOURCE,
                    symbols,
                )

            # Date-driven lifecycle close to retain recently completed IPOs.
            await conn.execute(
                """
                UPDATE ipo_snapshots
                SET status = 'closed'
                WHERE archived_at IS NULL
                  AND status <> 'closed'
                  AND (
                    (close_date IS NOT NULL AND close_date < $1::date)
                    OR (listing_date IS NOT NULL AND listing_date <= $1::date)
                  )
                """,
                today_ist,
            )

            # Enrich closed rows for UI outcome rendering.
            await conn.execute(
                """
                UPDATE ipo_snapshots
                SET outcome_state = CASE
                    WHEN listing_price IS NOT NULL THEN 'listed'
                    ELSE 'awaiting_listing_data'
                END,
                    listing_gain_pct = CASE
                        WHEN listing_price IS NULL THEN NULL
                        ELSE listing_gain_pct
                    END
                WHERE status = 'closed'
                  AND archived_at IS NULL
                """
            )

            await conn.execute(
                """
                UPDATE ipo_snapshots
                SET outcome_state = NULL
                WHERE status IN ('open', 'upcoming')
                  AND archived_at IS NULL
                """
            )

            # Soft-archive old closed rows after 14 days.
            await conn.execute(
                """
                UPDATE ipo_snapshots
                SET archived_at = NOW()
                WHERE status = 'closed'
                  AND archived_at IS NULL
                  AND COALESCE(close_date, listing_date) IS NOT NULL
                  AND COALESCE(close_date, listing_date) <= ($1::date - 14)
                """,
                today_ist,
            )

            await conn.execute(
                """
                DELETE FROM device_ipo_alerts
                WHERE symbol NOT IN (
                    SELECT symbol
                    FROM ipo_snapshots
                    WHERE archived_at IS NULL
                )
                """
            )
    logger.info("IPO live sync complete: %d active rows upserted", len(rows))


def _recommendation(status: str, gmp_percent: float | None, subscription_multiple: float | None) -> tuple[str, str]:
    gmp = float(gmp_percent or 0.0)
    sub = float(subscription_multiple or 0.0)
    if status == "closed":
        return "watch", "Issue closed; review listing outcome"
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
    if gmp <= 0:
        return "watch", "No GMP signal yet; wait for clearer demand trend"
    if gmp >= 18:
        return "apply", "High GMP trend indicates strong listing interest"
    if gmp <= 6:
        return "avoid", "Low GMP trend indicates limited near-term upside"
    return "watch", "Moderate GMP; monitor pre-open demand closely"


async def get_ipos(*, status: str = "open", limit: int = 20) -> dict:
    status = _normalize_status(status)
    await _sync_live_rows()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT symbol, company_name, market, status, ipo_type, issue_size_cr, price_band,
                   gmp_percent, subscription_multiple, listing_price, listing_gain_pct,
                   outcome_state, open_date, close_date, listing_date, source_timestamp
            FROM ipo_snapshots
            WHERE status = $1
              AND archived_at IS NULL
            ORDER BY
                CASE WHEN $1 = 'open' THEN close_date END ASC NULLS LAST,
                CASE WHEN $1 = 'upcoming' THEN open_date END ASC NULLS LAST,
                CASE WHEN $1 = 'closed' THEN close_date END DESC NULLS LAST,
                CASE WHEN $1 = 'closed' THEN listing_date END DESC NULLS LAST,
                CASE WHEN $1 = 'closed' THEN source_timestamp END DESC NULLS LAST,
                symbol ASC
            LIMIT $2
            """,
            status,
            limit,
        )
        as_of = await conn.fetchval(
            "SELECT MAX(source_timestamp) FROM ipo_snapshots WHERE status = $1 AND archived_at IS NULL",
            status,
        )

    items: list[dict] = []
    for row in rows:
        item = record_to_dict(row)
        listed = item.get("listing_price")
        if item.get("listing_gain_pct") is None:
            item["listing_gain_pct"] = _listing_gain_pct(
                listing_price=listed,
                price_band=item.get("price_band"),
            )
        if status == "closed":
            item["outcome_state"] = (
                "listed" if item.get("listing_price") is not None else "awaiting_listing_data"
            )
        else:
            item["outcome_state"] = None
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
    await _sync_live_rows()
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
    await _sync_live_rows()
    normalized = _normalize_symbols(symbols)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if normalized:
            known_rows = await conn.fetch(
                "SELECT symbol FROM ipo_snapshots WHERE symbol = ANY($1::text[]) AND archived_at IS NULL",
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
