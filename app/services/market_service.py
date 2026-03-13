import logging
from datetime import datetime, timezone, timedelta

from app.core.asset_catalog import get_asset_meta
from app.core.config import get_settings
from app.core.database import get_pool, parse_ts, record_to_dict
from app.scheduler.trading_calendar import (
    get_market_status,
    get_exchange_session_state,
    get_commodity_session_state,
    is_fx_session_expected_open,
    SESSION_OPEN,
    SESSION_BREAK,
    SESSION_CLOSED,
)

logger = logging.getLogger(__name__)

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


async def get_latest_daily_snapshot_per_asset_type(
    asset_type_pairs: list[tuple[str, str]],
) -> dict[tuple[str, str], dict]:
    """Return map (asset, instrument_type) -> latest daily row fields used by scheduler dedupe logic."""
    if not asset_type_pairs:
        return {}
    pool = await get_pool()
    rows = await pool.fetch(
        f"""
        SELECT DISTINCT ON (asset, instrument_type)
            asset,
            instrument_type,
            price,
            previous_close,
            change_percent
        FROM {TABLE}
        ORDER BY asset, instrument_type, timestamp DESC
        """
    )
    snapshots = {
        (r["asset"], r["instrument_type"] or ""): {
            "price": float(r["price"]) if r["price"] is not None else None,
            "previous_close": float(r["previous_close"]) if r["previous_close"] is not None else None,
            "change_percent": float(r["change_percent"]) if r["change_percent"] is not None else None,
        }
        for r in rows
    }
    need = set(asset_type_pairs)
    return {k: snapshots[k] for k in need if k in snapshots}


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
_ROLLING_24H_TYPES = {"currency", "commodity", "crypto"}

PHASE_LIVE = "live"
PHASE_CLOSED = "closed"
PHASE_STALE = "stale"


def _to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if hasattr(dt, "isoformat"):
        return dt.isoformat()
    return str(dt)


def _normalize_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = parse_ts(value)
    else:
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _live_max_age_for_instrument(instrument_type: str) -> int:
    settings = get_settings()
    if instrument_type in _ROLLING_24H_TYPES:
        return settings.effective_rolling_live_max_age_seconds()
    return settings.effective_session_live_max_age_seconds()


def _session_state(asset: str, instrument_type: str, now_utc: datetime, status: dict | None = None) -> str:
    if instrument_type == "currency":
        return SESSION_OPEN if is_fx_session_expected_open(now_utc) else SESSION_CLOSED
    if instrument_type == "commodity":
        return get_commodity_session_state(now_utc)
    meta = get_asset_meta(asset)
    if meta is not None and meta.session_policy == "predictive":
        st = status or get_market_status(now_utc)
        return SESSION_OPEN if bool(st.get("gift_nifty_open")) else SESSION_CLOSED
    if meta is not None and meta.session_policy == "session":
        return get_exchange_session_state(meta.exchange, now_utc, status=status)
    st = status or get_market_status(now_utc)
    if instrument_type == "index":
        if asset == "Gift Nifty":
            return SESSION_OPEN if bool(st.get("gift_nifty_open")) else SESSION_CLOSED
        india_indices = {
            "Sensex",
            "Nifty 50",
            "Nifty 500",
            "Nifty Bank",
            "Nifty IT",
            "Nifty Midcap 150",
            "Nifty Smallcap 250",
        }
        if asset in india_indices:
            return SESSION_OPEN if bool(st.get("nse_open")) else SESSION_CLOSED
        return SESSION_OPEN if bool(st.get("nyse_open")) else SESSION_CLOSED
    if instrument_type == "bond_yield":
        if asset == "India 10Y Bond Yield":
            return SESSION_OPEN if bool(st.get("nse_open")) else SESSION_CLOSED
        return SESSION_OPEN if bool(st.get("nyse_open")) else SESSION_CLOSED
    return SESSION_OPEN if bool(st.get("nyse_open")) else SESSION_CLOSED


def _compute_phase(asset: str, instrument_type: str, last_tick: datetime | None, now_utc: datetime, status: dict | None = None) -> tuple[str, bool]:
    live_max_age = _live_max_age_for_instrument(instrument_type)
    # FX & crypto are 24/7: never "closed", only live/stale.
    if instrument_type in ("currency", "crypto"):
        if last_tick is None:
            return PHASE_STALE, True
        age = (now_utc - last_tick).total_seconds()
        if age <= live_max_age:
            return PHASE_LIVE, False
        return PHASE_STALE, True
    session_state = _session_state(asset, instrument_type, now_utc, status=status)
    if session_state == SESSION_CLOSED:
        return PHASE_CLOSED, False
    if session_state == SESSION_BREAK:
        logger.debug(
            "Asset in maintenance/lunch break; marking stale: asset=%s type=%s tick=%s",
            asset,
            instrument_type,
            _to_iso(last_tick),
        )
        return PHASE_STALE, True
    if last_tick is None:
        return PHASE_STALE, True
    age = (now_utc - last_tick).total_seconds()
    if age <= live_max_age:
        return PHASE_LIVE, False
    return PHASE_STALE, True


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
    now_utc = datetime.now(timezone.utc)
    status = get_market_status(now_utc)
    logger.debug("Computing latest prices: instrument_type=%s rows=%d", instrument_type, len(rows))
    for r in rows:
        d = record_to_dict(r)
        prev = d.pop("prev_price", None)
        asset = str(d.get("asset") or "")
        inst = str(d.get("instrument_type") or "")
        d["change_window"] = "24h" if inst in _ROLLING_24H_TYPES else "session"
        d["last_tick_timestamp"] = None
        d["ingested_at"] = None
        d["data_quality"] = None
        d["is_predictive"] = bool(asset == "Gift Nifty")
        d["session_source"] = "gift_nifty_windows" if asset == "Gift Nifty" else None
        meta = get_asset_meta(asset)
        d["region"] = meta.region if meta is not None else None
        d["exchange"] = meta.exchange if meta is not None else None
        d["session_policy"] = meta.session_policy if meta is not None else None
        d["tradable_type"] = meta.tradable_type if meta is not None else None
        if inst in _ROLLING_24H_TYPES:
            rolling = await _get_intraday_rolling_change(
                pool,
                asset=asset,
                instrument_type=inst,
                hours=24,
            )
            if rolling is not None:
                first_price = rolling["first_price"]
                last_price = rolling["last_price"]
                last_ts = _normalize_dt(rolling.get("last_ts"))
                d["price"] = last_price
                if last_ts is not None:
                    d["timestamp"] = last_ts.isoformat()
                d["change_percent"] = rolling["pct_change"]
                d["previous_close"] = first_price
                d["last_tick_timestamp"] = _to_iso(last_ts)
                d["ingested_at"] = _to_iso(_normalize_dt(rolling.get("ingested_at")))
                d["data_quality"] = "fallback" if rolling.get("is_fallback") else "primary"
                phase, is_stale = _compute_phase(asset, inst, last_ts, now_utc, status=status)
                d["market_phase"] = phase
                d["is_stale"] = is_stale
                logger.debug(
                    "Rolling latest computed: asset=%s type=%s first=%s last=%s pct=%s last_ts=%s phase=%s stale=%s",
                    asset,
                    inst,
                    first_price,
                    last_price,
                    d["change_percent"],
                    d["last_tick_timestamp"],
                    phase,
                    is_stale,
                )
            else:
                phase, is_stale = _compute_phase(asset, inst, None, now_utc, status=status)
                d["market_phase"] = phase
                d["is_stale"] = is_stale
                logger.debug(
                    "Rolling latest missing intraday data: asset=%s type=%s phase=%s stale=%s",
                    asset,
                    inst,
                    phase,
                    is_stale,
                )
        elif d.get("change_percent") is None and prev is not None and isinstance(prev, (int, float)):
            try:
                p = float(d["price"])
                pv = float(prev)
                if pv and pv != 0:
                    d["change_percent"] = round(((p - pv) / pv) * 100, 2)
                    d["previous_close"] = pv
            except (TypeError, ZeroDivisionError):
                pass
        if inst not in _ROLLING_24H_TYPES:
            intraday_day = await get_intraday(asset=asset, instrument_type=inst)
            points = intraday_day.get("prices") or []
            if points:
                last_price = points[-1].get("price")
                try:
                    d["price"] = float(last_price)
                except (TypeError, ValueError):
                    pass

                # Prefer previous-close based session % for true market day move.
                # Fall back to first->last intraday only when previous_close is unavailable.
                pv_raw = d.get("previous_close")
                try:
                    pv = float(pv_raw) if pv_raw is not None else None
                    lp = float(d.get("price")) if d.get("price") is not None else None
                    if pv is not None and pv != 0 and lp is not None:
                        d["change_percent"] = round(((lp - pv) / pv) * 100, 2)
                        d["previous_close"] = pv
                    elif len(points) >= 2 and lp is not None:
                        f = float(points[0].get("price"))
                        if f != 0 and lp is not None:
                            d["change_percent"] = round(((lp - f) / f) * 100, 2)
                            if d.get("previous_close") is None:
                                d["previous_close"] = f
                except (TypeError, ValueError, ZeroDivisionError):
                    pass
                last_tick_ts = _normalize_dt(points[-1].get("timestamp"))
                if last_tick_ts is not None:
                    d["last_tick_timestamp"] = _to_iso(last_tick_ts)
                    d["timestamp"] = last_tick_ts.isoformat()
            phase, is_stale = _compute_phase(asset, inst, _normalize_dt(d.get("last_tick_timestamp")), now_utc, status=status)
            d["market_phase"] = phase
            d["is_stale"] = is_stale
            d["data_quality"] = "primary"
            logger.debug(
                "Session latest computed: asset=%s type=%s points=%d last_tick=%s phase=%s stale=%s",
                asset,
                inst,
                len(points),
                d["last_tick_timestamp"],
                phase,
                is_stale,
            )
        out.append(d)
    logger.debug("Latest prices ready: instrument_type=%s output_rows=%d", instrument_type, len(out))
    return out


# ---- Intraday (1D live) ----

def _round_to_minute(utc_dt: datetime) -> datetime:
    """Round to minute for intraday storage (limits rows per run)."""
    return utc_dt.replace(second=0, microsecond=0)


async def insert_intraday_batch(rows: list[dict]) -> int:
    """Insert canonical intraday ticks with provider metadata.
    Uniqueness key: (asset, instrument_type, source_timestamp, provider)."""
    if not rows:
        return 0
    pool = await get_pool()
    count = 0
    async with pool.acquire() as conn:
        for r in rows:
            source_ts = parse_ts(r.get("source_timestamp") or r.get("timestamp"))
            storage_ts = parse_ts(r.get("timestamp") or r.get("source_timestamp"))
            ingested_at = parse_ts(r.get("ingested_at")) or datetime.now(timezone.utc)
            result = await conn.execute(
                f"""
                INSERT INTO {TABLE_INTRADAY}
                (asset, instrument_type, price, "timestamp", source_timestamp, ingested_at,
                 provider, provider_priority, confidence_level, is_fallback, quality, is_predictive, session_source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                ON CONFLICT (asset, instrument_type, source_timestamp, provider)
                DO UPDATE SET
                    price = EXCLUDED.price,
                    "timestamp" = EXCLUDED."timestamp",
                    ingested_at = EXCLUDED.ingested_at,
                    provider_priority = EXCLUDED.provider_priority,
                    confidence_level = EXCLUDED.confidence_level,
                    is_fallback = EXCLUDED.is_fallback,
                    quality = EXCLUDED.quality,
                    is_predictive = EXCLUDED.is_predictive,
                    session_source = EXCLUDED.session_source
                """,
                r.get("asset"),
                r.get("instrument_type"),
                r.get("price"),
                storage_ts,
                source_ts,
                ingested_at,
                r.get("provider") or "unknown",
                int(r.get("provider_priority") or 99),
                r.get("confidence_level"),
                bool(r.get("is_fallback")) if r.get("is_fallback") is not None else False,
                r.get("quality"),
                bool(r.get("is_predictive")) if r.get("is_predictive") is not None else False,
                r.get("session_source"),
            )
            if result:
                parts = result.split()
                if len(parts) >= 3:
                    count += int(parts[-1])
    logger.debug("insert_intraday_batch processed=%d inserted_or_updated=%d", len(rows), count)
    return count


async def _get_intraday_rolling_change(
    pool,
    asset: str,
    instrument_type: str,
    hours: int = 24,
) -> dict | None:
    """Return rolling-window summary from intraday canonical ticks."""
    row = await pool.fetchrow(
        f"""
        WITH bounds AS (
            SELECT MAX(COALESCE(source_timestamp, "timestamp")) AS max_ts
            FROM {TABLE_INTRADAY}
            WHERE asset = $1 AND instrument_type = $2
        ),
        points AS (
            SELECT
                COALESCE(source_timestamp, "timestamp") AS tick_ts,
                price,
                COALESCE(provider, 'unknown') AS provider,
                COALESCE(provider_priority, 99) AS provider_priority,
                COALESCE(is_fallback, FALSE) AS is_fallback,
                quality,
                COALESCE(ingested_at, "timestamp") AS ingested_at,
                id
            FROM {TABLE_INTRADAY}
            WHERE asset = $1 AND instrument_type = $2
              AND COALESCE(source_timestamp, "timestamp") >= ((SELECT max_ts FROM bounds) - make_interval(hours => $3))
              AND COALESCE(source_timestamp, "timestamp") <= (SELECT max_ts FROM bounds)
        ),
        dedup AS (
            SELECT DISTINCT ON (tick_ts)
                tick_ts,
                price,
                provider,
                is_fallback,
                quality,
                ingested_at
            FROM points
            ORDER BY tick_ts, provider_priority ASC, is_fallback ASC, ingested_at DESC, id DESC
        )
        SELECT
            (SELECT price FROM dedup ORDER BY tick_ts ASC LIMIT 1) AS first_price,
            (SELECT price FROM dedup ORDER BY tick_ts DESC LIMIT 1) AS last_price,
            (SELECT tick_ts FROM dedup ORDER BY tick_ts DESC LIMIT 1) AS last_ts,
            (SELECT ingested_at FROM dedup ORDER BY tick_ts DESC LIMIT 1) AS ingested_at,
            (SELECT provider FROM dedup ORDER BY tick_ts DESC LIMIT 1) AS provider,
            (SELECT is_fallback FROM dedup ORDER BY tick_ts DESC LIMIT 1) AS is_fallback,
            (SELECT quality FROM dedup ORDER BY tick_ts DESC LIMIT 1) AS quality,
            (SELECT COUNT(*) FROM dedup) AS points_count
        """,
        asset,
        instrument_type,
        int(hours),
    )
    if not row:
        logger.debug("No rolling intraday rows: asset=%s type=%s hours=%d", asset, instrument_type, hours)
        return None
    first = row["first_price"]
    last = row["last_price"]
    if first is None or last is None:
        logger.debug("Incomplete rolling window points: asset=%s type=%s", asset, instrument_type)
        return None
    try:
        f = float(first)
        l = float(last)
    except (TypeError, ValueError):
        logger.debug("Invalid rolling values: asset=%s type=%s first=%s last=%s", asset, instrument_type, first, last)
        return None
    if f == 0:
        logger.debug("Rolling first price is zero: asset=%s type=%s", asset, instrument_type)
        return None
    pct = round(((l - f) / f) * 100, 2)
    return {
        "first_price": f,
        "last_price": l,
        "pct_change": pct,
        "last_ts": row["last_ts"],
        "ingested_at": row["ingested_at"],
        "provider": row["provider"] or "unknown",
        "is_fallback": bool(row["is_fallback"]),
        "quality": row["quality"],
        "points_count": int(row["points_count"] or 0),
    }


async def get_intraday(
    asset: str,
    instrument_type: str,
    from_ts: datetime | None = None,
    to_ts: datetime | None = None,
) -> dict:
    """Return intraday points.
    Defaults:
    - currency/commodity: rolling 24h ending at latest tick.
    - others: most recent UTC calendar day with data.
    Optional from_ts/to_ts override the window."""
    pool = await get_pool()
    if from_ts is not None and to_ts is not None:
        day_start, day_end = from_ts, to_ts
    else:
        row = await pool.fetchrow(
            f"""
            SELECT MAX(COALESCE(source_timestamp, "timestamp")) as max_ts
            FROM {TABLE_INTRADAY}
            WHERE asset = $1 AND instrument_type = $2
            """,
            asset,
            instrument_type,
        )
        if not row or row["max_ts"] is None:
            logger.debug("Intraday query has no data: asset=%s type=%s", asset, instrument_type)
            return {
                "prices": [],
                "window_start": None,
                "window_end": None,
                "coverage_minutes": 0,
                "expected_minutes": 0,
                "data_mode": "actual_ticks",
            }
        max_ts = row["max_ts"]
        if getattr(max_ts, "tzinfo", None) is None:
            max_ts = max_ts.replace(tzinfo=timezone.utc)
        if instrument_type in _ROLLING_24H_TYPES:
            day_start = max_ts - timedelta(hours=24)
            day_end = max_ts + timedelta(microseconds=1)
        else:
            day_start = datetime(max_ts.year, max_ts.month, max_ts.day, 0, 0, 0, tzinfo=timezone.utc)
            day_end = day_start + timedelta(days=1)
    rows = await pool.fetch(
        f"""
        WITH dedup AS (
            SELECT DISTINCT ON (tick_ts)
                tick_ts AS "timestamp",
                price
            FROM (
                SELECT
                    COALESCE(source_timestamp, "timestamp") AS tick_ts,
                    price,
                    COALESCE(provider_priority, 99) AS provider_priority,
                    COALESCE(is_fallback, FALSE) AS is_fallback,
                    COALESCE(ingested_at, "timestamp") AS ingested_at,
                    id
                FROM {TABLE_INTRADAY}
                WHERE asset = $1 AND instrument_type = $2
                  AND COALESCE(source_timestamp, "timestamp") >= $3
                  AND COALESCE(source_timestamp, "timestamp") < $4
            ) points
            ORDER BY tick_ts, provider_priority ASC, is_fallback ASC, ingested_at DESC, id DESC
        )
        SELECT "timestamp", price
        FROM dedup
        ORDER BY "timestamp" ASC
        """,
        asset,
        instrument_type,
        day_start,
        day_end,
    )
    prices = [
        {
            "timestamp": r["timestamp"].isoformat() if hasattr(r["timestamp"], "isoformat") else r["timestamp"],
            "price": float(r["price"]),
        }
        for r in rows
    ]
    expected = 1440 if instrument_type in _ROLLING_24H_TYPES else max(1, int((day_end - day_start).total_seconds() // 60))
    coverage = len(prices)
    logger.debug(
        "Intraday query: asset=%s type=%s start=%s end=%s points=%d expected=%d",
        asset,
        instrument_type,
        _to_iso(day_start),
        _to_iso(day_end),
        coverage,
        expected,
    )
    return {
        "prices": prices,
        "window_start": _to_iso(day_start),
        "window_end": _to_iso(day_end),
        "coverage_minutes": coverage,
        "expected_minutes": expected,
        "data_mode": "actual_ticks",
    }


async def cleanup_intraday_older_than(days: int = 2) -> int:
    """Delete intraday rows older than given days. Returns count deleted."""
    pool = await get_pool()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    result = await pool.execute(
        f'DELETE FROM {TABLE_INTRADAY} WHERE COALESCE(source_timestamp, "timestamp") < $1',
        cutoff,
    )
    # e.g. "DELETE 42"
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0


async def repair_intraday_ticks() -> int:
    """Repair canonical intraday table by de-duping on (asset, instrument_type, source_timestamp, provider)."""
    pool = await get_pool()
    result = await pool.execute(
        f"""
        WITH ranked AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY asset, instrument_type, COALESCE(source_timestamp, "timestamp"), COALESCE(provider, 'unknown')
                    ORDER BY COALESCE(ingested_at, "timestamp") DESC, id DESC
                ) AS rn
            FROM {TABLE_INTRADAY}
        )
        DELETE FROM {TABLE_INTRADAY} m
        USING ranked r
        WHERE m.id = r.id
          AND r.rn > 1
        """
    )
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0
