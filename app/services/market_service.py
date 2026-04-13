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
    # Gift Nifty trades extended hours — use rolling window (15 min) instead of session (5 min).
    if asset == "Gift Nifty":
        live_max_age = max(live_max_age, get_settings().effective_rolling_live_max_age_seconds())
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

    # Fast path: single-asset chart history — deduplicate in SQL
    if asset and (limit == -1 or limit > 500):
        conditions = [f"asset = $1"]
        args: list = [asset]
        n = 2
        if instrument_type:
            conditions.append(f"instrument_type = ${n}")
            args.append(instrument_type)
            n += 1
        where = " AND ".join(conditions)
        # Cap at 1825 days (5 years) for chart — no need for more
        effective_limit = 1825 if limit == -1 else limit
        rows = await pool.fetch(
            f"""
            SELECT DISTINCT ON (asset, instrument_type, ("timestamp"::date))
                *
            FROM {TABLE}
            WHERE {where}
            ORDER BY asset, instrument_type, ("timestamp"::date) DESC, "timestamp" DESC
            LIMIT ${n}
            """,
            *args,
            effective_limit,
        )
        out = [record_to_dict(r) for r in rows]
        out.sort(key=lambda x: (x.get("timestamp") or ""), reverse=False)
        return out

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
    now_utc = datetime.now(timezone.utc)
    status = get_market_status(now_utc)
    logger.debug("Computing latest prices: instrument_type=%s rows=%d", instrument_type, len(rows))
    for r in rows:
        d = record_to_dict(r)
        prev = d.pop("prev_price", None)
        asset = str(d.get("asset") or "")
        inst = str(d.get("instrument_type") or "")
        # Daily-only commodities (no intraday feed) get "session"
        # instead of "24h" — they're reference prices updated once
        # per day, not continuously traded futures.
        _DAILY_ONLY_COMMODITIES = frozenset({
            "coal", "dap fertilizer", "iron ore", "palm oil",
            "potash", "rubber", "tsp fertilizer", "urea", "zinc",
        })
        if inst in _ROLLING_24H_TYPES and asset not in _DAILY_ONLY_COMMODITIES:
            d["change_window"] = "24h"
        else:
            d["change_window"] = "session"
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
        elif inst not in _ROLLING_24H_TYPES:
            # For SESSION-based assets (indices): prefer DB-computed
            # previous day price over the source's previous_close.
            # Yahoo sometimes provides a stale previous_close after
            # holidays. The SQL subquery's prev_price is the actual
            # most-recent prior row from our own market_prices table.
            #
            # For ROLLING 24H assets (commodities, crypto, FX): trust
            # the source's own change_percent and previous_close.
            # These trade continuously and the source's prior close
            # is always from the actual prior session. DB-computed
            # values are wrong here because multi-day gaps in our
            # daily table make the "previous row" 3+ days old.
            if prev is not None and isinstance(prev, (int, float)):
                try:
                    p = float(d["price"])
                    pv = float(prev)
                    if pv and pv != 0:
                        db_change = round(((p - pv) / pv) * 100, 2)
                        src_change = d.get("change_percent")
                        if src_change is None or abs((src_change or 0) - db_change) > 0.1:
                            d["change_percent"] = db_change
                            d["previous_close"] = pv
                except (TypeError, ZeroDivisionError):
                    pass
        else:
            # Rolling 24h: only fill if source didn't provide change
            if d.get("change_percent") is None and prev is not None and isinstance(prev, (int, float)):
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

    # De-duplicate: the output can contain 2+ entries for the same
    # asset when the processing pipeline produces multiple output
    # items from a single SQL row (e.g. intraday override path
    # creates a second entry with a different previous_close).
    # Keep the FIRST occurrence per asset — the first entry is the
    # one produced by the SQL ROW_NUMBER(rn=1) query which has the
    # authoritative change_percent and previous_close from the
    # source (Yahoo/Google). Later entries may have been
    # recalculated against a stale baseline.
    seen: dict[str, int] = {}
    deduped: list[dict] = []
    for d in out:
        key = str(d.get("asset", ""))
        if key not in seen:
            seen[key] = len(deduped)
            deduped.append(d)
    if len(deduped) < len(out):
        logger.info(
            "Latest prices deduped: %d → %d (dropped %d duplicates)",
            len(out), len(deduped), len(out) - len(deduped),
        )
    return deduped


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


# ---- Market Scores & Story ----

import json
import math

TABLE_MARKET_SCORES = "market_scores"


def _compute_trend_score(prices: list[float]) -> float:
    """Score trend strength 0-100 based on SMA alignment.
    Higher = stronger uptrend. Uses all available SMAs, each contributing
    proportionally to fill the ±50 range around the baseline of 50."""
    n = len(prices)
    if n < 5:
        return 50.0  # neutral if not enough data

    def sma(data: list[float], period: int) -> float | None:
        if len(data) < period:
            return None
        return sum(data[-period:]) / period

    current = prices[-1]
    sma5 = sma(prices, 5)
    sma20 = sma(prices, 20)
    sma50 = sma(prices, 50)
    sma200 = sma(prices, 200)

    # Collect all available SMA comparisons with multipliers
    comparisons: list[tuple[float, float]] = []  # (ratio, weight)
    if sma20 and sma20 > 0:
        comparisons.append(((current - sma20) / sma20, 3.0))
    elif sma5 and sma5 > 0:
        comparisons.append(((current - sma5) / sma5, 3.0))
    if sma50 and sma50 > 0:
        comparisons.append(((current - sma50) / sma50, 2.0))
    if sma200 and sma200 > 0:
        comparisons.append(((current - sma200) / sma200, 1.0))

    if not comparisons:
        return 50.0

    # Divide ±50 range equally among available SMAs
    per_sma_range = 50.0 / len(comparisons)
    score = 50.0
    for ratio, weight in comparisons:
        # Scale ratio by weight, clamp to per-SMA range
        contribution = ratio * weight * 100
        score += max(-per_sma_range, min(per_sma_range, contribution))

    return max(0.0, min(100.0, round(score, 1)))


def _compute_volatility_score(prices: list[float]) -> float:
    """Score volatility 0-100. Higher = calmer (less volatile).
    Compares recent vol to longer-term historical vol."""
    n = len(prices)
    if n < 5:
        return 50.0

    def daily_returns(data: list[float]) -> list[float]:
        return [(data[i] - data[i-1]) / data[i-1] for i in range(1, len(data)) if data[i-1] != 0]

    returns = daily_returns(prices)
    if len(returns) < 3:
        return 50.0

    recent_returns = returns[-20:] if len(returns) >= 20 else returns
    recent_vol = (sum(r**2 for r in recent_returns) / len(recent_returns)) ** 0.5

    # Longer-term vol for comparison
    long_returns = returns[-60:] if len(returns) >= 60 else returns
    long_vol = (sum(r**2 for r in long_returns) / len(long_returns)) ** 0.5

    if long_vol == 0:
        return 50.0

    # If recent vol is lower than historical, score is higher (calmer)
    vol_ratio = recent_vol / long_vol
    # vol_ratio < 1 means calmer than average → score > 50
    # vol_ratio > 1 means more volatile → score < 50
    score = 50.0 + (1.0 - vol_ratio) * 50.0
    return max(0.0, min(100.0, round(score, 1)))


def _compute_momentum_score(prices: list[float]) -> float:
    """Score momentum 0-100. Higher = stronger positive momentum.
    Uses rate of change (ROC) + RSI-equivalent."""
    n = len(prices)
    if n < 14:
        return 50.0

    # RSI-14
    gains = []
    losses = []
    for i in range(max(1, n - 14), n):
        diff = prices[i] - prices[i - 1]
        if diff > 0:
            gains.append(diff)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(abs(diff))

    avg_gain = sum(gains) / len(gains) if gains else 0
    avg_loss = sum(losses) / len(losses) if losses else 0

    if avg_gain == 0 and avg_loss == 0:
        rsi = 50.0  # no movement = neutral
    elif avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    # Rate of change (20-day)
    roc_period = min(20, n - 1)
    if roc_period > 0 and prices[-(roc_period + 1)] != 0:
        roc = ((prices[-1] - prices[-(roc_period + 1)]) / prices[-(roc_period + 1)]) * 100
    else:
        roc = 0.0

    # Combine RSI (0-100) and ROC
    # RSI contributes 60%, ROC contributes 40%
    roc_score = 50.0 + min(max(roc * 5, -50), 50)  # Scale ROC to 0-100
    score = rsi * 0.6 + roc_score * 0.4

    return max(0.0, min(100.0, round(score, 1)))


def _fx_context(asset: str, score: float, direction: str) -> str:
    """Context-aware description for currency pairs (all vs INR)."""
    base = asset.split("/")[0] if "/" in asset else asset
    base_names = {
        "USD": "US Dollar", "EUR": "Euro", "GBP": "British Pound",
        "JPY": "Japanese Yen", "AUD": "Australian Dollar", "CAD": "Canadian Dollar",
        "CHF": "Swiss Franc", "CNY": "Chinese Yuan", "SGD": "Singapore Dollar",
    }
    name = base_names.get(base, base)
    if direction == "up":
        return f"the {name} is strengthening against the Rupee — imports and foreign expenses get costlier"
    return f"the {name} is weakening against the Rupee — imports become cheaper, but export earnings shrink"


def _bond_context(asset: str, direction: str) -> str:
    """Context-aware description for bond yields."""
    is_india = "India" in asset
    is_us = "US" in asset
    if direction == "up":
        base = "borrowing costs are rising"
        if is_india:
            return f"{base} in India — home loans and corporate debt get more expensive"
        if is_us:
            return f"{base} in the US — tighter financial conditions, pressure on equity valuations"
        return base
    base = "borrowing costs are easing"
    if is_india:
        return f"{base} in India — cheaper loans and supportive for equity markets"
    if is_us:
        return f"{base} in the US — looser financial conditions, supportive for risk assets"
    return base


def _commodity_context(asset: str, direction: str) -> str:
    """Context-aware description for commodities."""
    a = asset.lower()
    if direction == "up":
        if a in ("gold", "silver"):
            return f"{asset.capitalize()} prices are rising — often a sign of inflation fears or safe-haven demand"
        if a == "crude oil":
            return "crude oil prices are climbing — fuel and transportation costs may rise"
        if a == "natural gas":
            return "natural gas prices are rising — energy and manufacturing costs may increase"
        if a == "copper":
            return "copper prices are up — often signals industrial demand and economic expansion"
        if a in ("wheat", "corn", "soybeans", "rice", "oats"):
            return f"{asset.capitalize()} prices are rising — food inflation and input costs may increase"
        if a in ("cotton", "sugar", "coffee", "cocoa"):
            return f"{asset.capitalize()} prices are climbing — consumer goods costs may rise"
        if a in ("urea", "dap fertilizer", "potash", "tsp fertilizer"):
            return f"{asset.capitalize()} prices are rising — farming costs increase, food inflation risk"
        if a == "aluminum":
            return "Aluminum prices are rising — packaging and construction costs may increase"
        if a == "brent crude":
            return "Brent crude prices are climbing — global fuel costs rising"
        if a == "gasoline":
            return "Gasoline prices are rising — direct impact on consumer fuel costs"
        if a == "heating oil":
            return "Heating oil prices are rising — winter energy costs increasing"
        if a == "iron ore":
            return "Iron ore prices are rising — steel and infrastructure costs increasing"
        if a == "coal":
            return "Coal prices are rising — power generation costs increasing, especially for India"
        if a == "palm oil":
            return "Palm oil prices are climbing — cooking oil and FMCG costs may rise in India"
        if a == "rubber":
            return "Rubber prices are rising — auto and tyre industry costs increasing"
        if a == "zinc":
            return "Zinc prices are up — galvanizing and construction costs may increase"
        return f"{asset.capitalize()} prices are moving higher"
    if a in ("gold", "silver"):
        return f"{asset.capitalize()} prices are falling — risk appetite may be improving, or the dollar is strengthening"
    if a == "crude oil":
        return "crude oil prices are dropping — lower fuel costs, but may signal weaker global demand"
    if a == "natural gas":
        return "natural gas prices are declining — easing energy cost pressures"
    if a == "copper":
        return "copper prices are falling — may indicate slowing industrial activity"
    if a in ("wheat", "corn", "soybeans", "rice", "oats"):
        return f"{asset.capitalize()} prices are falling — easing food inflation pressures"
    if a in ("cotton", "sugar", "coffee", "cocoa"):
        return f"{asset.capitalize()} prices are dropping — input cost relief for manufacturers"
    if a in ("urea", "dap fertilizer", "potash", "tsp fertilizer"):
        return f"{asset.capitalize()} prices are falling — cheaper inputs for agriculture"
    if a == "aluminum":
        return "Aluminum prices are falling — easing industrial input costs"
    if a == "brent crude":
        return "Brent crude prices are dropping — easing transportation costs worldwide"
    if a == "gasoline":
        return "Gasoline prices are falling — relief at the pump for consumers"
    if a == "heating oil":
        return "Heating oil prices are falling — lower winter energy bills"
    if a == "iron ore":
        return "Iron ore prices are falling — lower steel costs, but may signal weak construction demand"
    if a == "coal":
        return "Coal prices are dropping — easing power generation costs"
    if a == "palm oil":
        return "Palm oil prices are falling — cooking oil and FMCG costs easing"
    if a == "rubber":
        return "Rubber prices are falling — lower input costs for auto and tyre industry"
    if a == "zinc":
        return "Zinc prices are down — cheaper galvanizing and construction inputs"
    return f"{asset.capitalize()} prices are under pressure"


def _trend_desc(score: float, inst: str, asset: str) -> str:
    """Human-readable trend description, adapted by asset class."""
    direction = "up" if score >= 50 else "down"
    if inst == "bond_yield":
        return _bond_context(asset, direction)
    if inst == "currency":
        return _fx_context(asset, score, direction)
    if inst == "commodity":
        return _commodity_context(asset, direction)
    if inst == "crypto":
        name = asset.capitalize()
        if score >= 75: return f"{name} is rallying well above key moving averages"
        if score >= 60: return f"{name} is holding above most moving averages"
        if score >= 40: return f"{name} is consolidating near its averages"
        if score >= 25: return f"{name} is slipping below key moving averages"
        return f"{name} is in a sustained sell-off, well below all averages"
    # Gift Nifty — predictive/indicative instrument
    if asset == "Gift Nifty":
        if score >= 60:
            return "Gift Nifty is signaling a positive opening for Indian markets — SGX derivatives point to gains at open"
        if score >= 40:
            return "Gift Nifty is indicating a flat to mildly negative opening for Indian markets"
        if score >= 25:
            return "Gift Nifty is pointing to a weak opening — pre-market sentiment is cautious"
        return "Gift Nifty is signaling a sharply negative opening for Indian markets — global cues are weak"
    # index
    name = asset
    if score >= 75: return f"{name} is trading well above key moving averages, in a strong uptrend"
    if score >= 60: return f"{name} is holding above most moving averages"
    if score >= 40: return f"{name} is hovering near its moving averages with no clear direction"
    if score >= 25: return f"{name} is trading below key moving averages"
    return f"{name} is well below all moving averages, in a sustained decline"


def _momentum_desc(score: float, inst: str, asset: str = "") -> str:
    """Human-readable momentum description, adapted by asset class."""
    if inst == "bond_yield":
        if score >= 70: return "Rate expectations are shifting rapidly — yields gaining fast."
        if score >= 55: return "Steady upward pressure on yields."
        if score >= 45: return "Yield momentum is flat — markets await new catalysts."
        if score >= 30: return "Yields losing steam as rate-cut expectations build."
        return "Yields falling fast — market pricing in significant easing."
    if inst == "currency":
        if score >= 70: return "Strong capital flows driving rapid movement."
        if score >= 55: return "Steady flows support the current direction."
        if score >= 45: return "Flow dynamics are balanced — no dominant direction."
        if score >= 30: return "Flows are reversing, putting pressure on the rate."
        return "Heavy flows in the opposite direction — sharp move underway."
    if inst == "commodity":
        if score >= 70: return "Demand is outpacing supply, driving prices higher rapidly."
        if score >= 55: return "Steady demand keeps prices firm."
        if score >= 45: return "Supply and demand are broadly balanced."
        if score >= 30: return "Demand is softening, putting pressure on prices."
        return "Oversupply or demand weakness is driving prices down sharply."
    # index / crypto
    if asset == "Gift Nifty":
        if score >= 60: return "Global cues and overnight US markets are supportive."
        if score >= 40: return "Mixed global signals — no strong directional cue."
        return "Overnight global markets were weak — risk-off sentiment."
    if score >= 70: return "Strong buying pressure with broad participation."
    if score >= 55: return "Positive momentum with steady buying interest."
    if score >= 45: return "Momentum is flat — neither buyers nor sellers in control."
    if score >= 30: return "Selling pressure is building as buyers step back."
    return "Heavy selling pressure across the board."


def _vol_context(score: float, inst: str) -> str:
    """One-sentence volatility context, adapted by asset class."""
    if inst == "bond_yield":
        if score >= 65: return "Rate markets are calm — low uncertainty around policy direction."
        if score >= 35: return "Yield swings are normal for the current rate environment."
        return "Bond markets are jittery — elevated swings suggest policy uncertainty."
    if inst == "currency":
        if score >= 65: return "FX volatility is low — stable macro conditions."
        if score >= 35: return "Currency swings are within normal range."
        return "FX volatility is elevated — expect wider daily swings."
    if inst == "commodity":
        if score >= 65: return "Commodity prices are moving in an orderly fashion."
        if score >= 35: return "Price swings are typical for this market."
        return "High volatility — supply shocks or geopolitical risk may be at play."
    # index / crypto
    if score >= 65: return "Market conditions are calm with orderly price action."
    if score >= 35: return "Volatility is in line with historical norms."
    return "Elevated volatility — risk-off sentiment or event-driven uncertainty."


def _extract_price_stats(prices: list[float]) -> dict:
    """Extract concrete data points from the price array for rich reasoning."""
    n = len(prices)
    current = prices[-1]
    stats: dict = {"current": current, "count": n}

    def pct(old: float, new: float) -> float | None:
        return round(((new - old) / old) * 100, 1) if old and old != 0 else None

    # Period returns
    if n >= 7:
        stats["chg_1w"] = pct(prices[-7], current)
    if n >= 30:
        stats["chg_1m"] = pct(prices[-30], current)
    if n >= 90:
        stats["chg_3m"] = pct(prices[-90], current)

    # 52-week high/low (up to 260 trading days)
    lookback = prices[-min(260, n):]
    high = max(lookback)
    low = min(lookback)
    stats["high_52w"] = high
    stats["low_52w"] = low
    stats["pct_from_high"] = pct(high, current)
    stats["pct_from_low"] = pct(low, current)

    # Consecutive direction
    streak = 0
    if n >= 2:
        direction = 1 if prices[-1] >= prices[-2] else -1
        for i in range(n - 2, max(n - 15, 0) - 1, -1):
            if i <= 0:
                break
            d = 1 if prices[i] >= prices[i - 1] else -1
            if d == direction:
                streak += 1
            else:
                break
        stats["streak"] = streak * direction  # positive = up days, negative = down days

    return stats


def _stats_sentence(stats: dict, inst: str) -> str:
    """Build a concrete data sentence from extracted stats."""
    parts = []

    # Period change
    chg_1m = stats.get("chg_1m")
    chg_3m = stats.get("chg_3m")
    if chg_1m is not None and chg_3m is not None and abs(chg_3m) > 3:
        direction = "up" if chg_1m >= 0 else "down"
        parts.append(f"{direction.capitalize()} {abs(chg_1m):.1f}% in the past month and {abs(chg_3m):.1f}% over 3 months")
    elif chg_1m is not None:
        direction = "up" if chg_1m >= 0 else "down"
        parts.append(f"{direction.capitalize()} {abs(chg_1m):.1f}% over the past month")

    # 52-week context
    pct_high = stats.get("pct_from_high")
    pct_low = stats.get("pct_from_low")
    if pct_high is not None and pct_high <= -20:
        parts.append(f"{abs(pct_high):.0f}% below its 52-week high")
    elif pct_high is not None and pct_high >= -3:
        parts.append("near its 52-week high")
    elif pct_low is not None and pct_low <= 3:
        parts.append("near its 52-week low")

    # Streak
    streak = stats.get("streak", 0)
    if abs(streak) >= 4:
        direction = "gains" if streak > 0 else "losses"
        parts.append(f"{abs(streak)} consecutive sessions of {direction}")

    if not parts:
        return ""
    return ", ".join(parts[:2]) + "."  # Keep it to 2 data points max


def _generate_market_verdict(
    trend: float, volatility: float, momentum: float,
    asset: str = "", instrument_type: str = "index",
    stats: dict | None = None,
) -> tuple[str, str, str]:
    """Generate verdict, action_tag, and action_tag_reasoning from scores.
    Returns (verdict, action_tag, action_tag_reasoning).
    Weighted: trend 40%, momentum 40%, volatility 20% — direction matters more than stability.
    Reasoning is asset-class-aware with natural language."""
    avg = trend * 0.4 + momentum * 0.4 + volatility * 0.2
    inst = instrument_type

    if avg >= 70:
        verdict = "Strong positive signals across trend and momentum"
        action_tag = "Bullish"
    elif avg >= 55:
        verdict = "Leaning positive with supportive price action"
        action_tag = "Moderately Bullish"
    elif avg >= 45:
        verdict = "No clear direction — price action is mixed"
        action_tag = "Neutral"
    elif avg >= 30:
        verdict = "Leaning negative with weakening price action"
        action_tag = "Moderately Bearish"
    else:
        verdict = "Broad weakness across trend and momentum"
        action_tag = "Bearish"

    # Sanity check: use actual price stats to override when scores don't match reality.
    # A -20% drop in 3 months should never be "Neutral" or better.
    if stats:
        chg_3m = stats.get("chg_3m")
        pct_from_high = stats.get("pct_from_high")
        pct_from_low = stats.get("pct_from_low")

        # Deep decline: if down >20% in 3M or >25% from 52W high → cap at Bearish
        if (chg_3m is not None and chg_3m <= -20) or (pct_from_high is not None and pct_from_high <= -25):
            if action_tag not in ("Bearish",):
                action_tag = "Bearish"
                verdict = "Broad weakness across trend and momentum"
        # Moderate decline: if down >10% in 3M → cap at Moderately Bearish
        elif chg_3m is not None and chg_3m <= -10:
            if action_tag in ("Neutral", "Moderately Bullish", "Bullish"):
                action_tag = "Moderately Bearish"
                verdict = "Leaning negative with weakening price action"
        # Strong rally: if up >20% in 3M or within 3% of 52W high → floor at Moderately Bullish
        elif (chg_3m is not None and chg_3m >= 20) or (pct_from_high is not None and pct_from_high >= -3):
            if action_tag in ("Neutral", "Moderately Bearish", "Bearish"):
                action_tag = "Moderately Bullish"
                verdict = "Leaning positive with supportive price action"

    trend_text = _trend_desc(trend, inst, asset)
    # Capitalize first letter
    trend_text = trend_text[0].upper() + trend_text[1:] if trend_text else ""
    data_sentence = _stats_sentence(stats or {}, inst) if stats else ""
    reasoning = (
        f"{trend_text}. "
        f"{_momentum_desc(momentum, inst, asset)} "
        f"{_vol_context(volatility, inst)}"
    )
    if data_sentence:
        reasoning += f" {data_sentence}"

    return verdict, action_tag, reasoning


def _generate_driver_tags(trend: float, volatility: float, momentum: float,
                          asset: str, instrument_type: str) -> list[str]:
    """Generate contextual driver tags based on scores."""
    tags = []

    # Trend tags
    if trend >= 75:
        tags.append("Strong Uptrend")
    elif trend >= 60:
        tags.append("Uptrend")
    elif trend <= 25:
        tags.append("Strong Downtrend")
    elif trend <= 40:
        tags.append("Downtrend")
    else:
        tags.append("Sideways")

    # Momentum tags
    if momentum >= 70:
        tags.append("High Momentum")
    elif momentum <= 30:
        tags.append("Weak Momentum")

    # Volatility tags
    if volatility >= 70:
        tags.append("Low Volatility")
    elif volatility <= 30:
        tags.append("High Volatility")

    # Type-specific tags
    if instrument_type == "bond_yield":
        if trend >= 60:
            tags.append("Rising Yields")
        elif trend <= 40:
            tags.append("Falling Yields")
    elif instrument_type == "currency":
        if momentum >= 60:
            tags.append("Currency Strengthening")
        elif momentum <= 40:
            tags.append("Currency Weakening")
    elif instrument_type == "commodity":
        if trend >= 60 and momentum >= 60:
            tags.append("Commodity Rally")
        elif trend <= 40 and momentum <= 40:
            tags.append("Commodity Slump")
    elif instrument_type == "crypto":
        if trend >= 60 and momentum >= 60:
            tags.append("Crypto Rally")
        elif trend <= 40 and momentum <= 40:
            tags.append("Crypto Selloff")

    return tags


def _generate_type_extras(asset: str, instrument_type: str,
                          trend: float, volatility: float, momentum: float) -> dict | None:
    """Generate type-specific extra data."""
    extras = {}

    if instrument_type == "index":
        if trend >= 60 and momentum >= 60:
            extras["market_regime"] = "risk_on"
        elif trend <= 40 and momentum <= 40:
            extras["market_regime"] = "risk_off"
        else:
            extras["market_regime"] = "mixed"

    elif instrument_type == "bond_yield":
        if trend >= 55:
            extras["yield_direction"] = "rising"
        elif trend <= 45:
            extras["yield_direction"] = "falling"
        else:
            extras["yield_direction"] = "stable"

    elif instrument_type == "currency":
        if momentum >= 55:
            extras["rate_signal"] = "strengthening"
        elif momentum <= 45:
            extras["rate_signal"] = "weakening"
        else:
            extras["rate_signal"] = "stable"

    elif instrument_type == "commodity":
        if trend >= 55 and momentum >= 55:
            extras["supply_demand"] = "demand_driven"
        elif trend <= 45 and momentum <= 45:
            extras["supply_demand"] = "oversupply"
        else:
            extras["supply_demand"] = "balanced"

    elif instrument_type == "crypto":
        if trend >= 60 and momentum >= 60:
            extras["sentiment"] = "risk_on"
        elif trend <= 40 and momentum <= 40:
            extras["sentiment"] = "risk_off"
        else:
            extras["sentiment"] = "mixed"

    return extras if extras else None


async def compute_and_store_market_score(asset: str, instrument_type: str) -> dict | None:
    """Compute scores for a single market instrument from its price history and store in DB."""
    pool = await get_pool()

    # Fetch most recent 365 days of price history, then sort ASC for scoring
    rows = await pool.fetch(
        f"""
        SELECT price, "timestamp" FROM {TABLE}
        WHERE asset = $1 AND instrument_type = $2
        ORDER BY "timestamp" DESC
        LIMIT 365
        """,
        asset,
        instrument_type,
    )

    if len(rows) < 14:
        logger.debug("Not enough price history for scoring: asset=%s type=%s rows=%d", asset, instrument_type, len(rows))
        return None

    # Sort oldest-first for SMA/momentum calculations
    rows_sorted = sorted(rows, key=lambda r: r["timestamp"])
    prices = [float(r["price"]) for r in rows_sorted]

    trend = _compute_trend_score(prices)
    vol = _compute_volatility_score(prices)
    mom = _compute_momentum_score(prices)
    stats = _extract_price_stats(prices)

    verdict, action_tag, reasoning = _generate_market_verdict(trend, vol, mom, asset, instrument_type, stats)
    driver_tags = _generate_driver_tags(trend, vol, mom, asset, instrument_type)
    type_extras = _generate_type_extras(asset, instrument_type, trend, vol, mom)

    # Upsert into market_scores
    await pool.execute(
        f"""
        INSERT INTO {TABLE_MARKET_SCORES}
        (asset, instrument_type, score_trend, score_volatility, score_momentum,
         verdict, action_tag, action_tag_reasoning, driver_tags, type_extras, computed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, NOW())
        ON CONFLICT (asset, instrument_type) DO UPDATE SET
            score_trend = EXCLUDED.score_trend,
            score_volatility = EXCLUDED.score_volatility,
            score_momentum = EXCLUDED.score_momentum,
            verdict = EXCLUDED.verdict,
            action_tag = EXCLUDED.action_tag,
            action_tag_reasoning = EXCLUDED.action_tag_reasoning,
            driver_tags = EXCLUDED.driver_tags,
            type_extras = EXCLUDED.type_extras,
            computed_at = EXCLUDED.computed_at
        """,
        asset,
        instrument_type,
        trend,
        vol,
        mom,
        verdict,
        action_tag,
        reasoning,
        json.dumps(driver_tags),
        json.dumps(type_extras) if type_extras else None,
    )

    result = {
        "asset": asset,
        "instrument_type": instrument_type,
        "score_trend": trend,
        "score_volatility": vol,
        "score_momentum": mom,
        "verdict": verdict,
        "action_tag": action_tag,
        "action_tag_reasoning": reasoning,
        "driver_tags": driver_tags,
        "type_extras": type_extras,
    }
    logger.info(
        "Market score computed: asset=%s type=%s trend=%.1f vol=%.1f mom=%.1f tag=%s",
        asset, instrument_type, trend, vol, mom, action_tag,
    )
    return result


async def get_all_market_scores() -> list[dict]:
    """Return action_tag for all scored instruments. Lightweight for list views."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"SELECT asset, instrument_type, action_tag FROM {TABLE_MARKET_SCORES}"
    )
    return [{"asset": r["asset"], "instrument_type": r["instrument_type"], "action_tag": r["action_tag"]} for r in rows]


async def get_market_story(asset: str, instrument_type: str) -> dict | None:
    """Read stored market scores and return story data. Generates verdict text on-the-fly."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        SELECT asset, instrument_type, score_trend, score_volatility, score_momentum,
               verdict, action_tag, action_tag_reasoning, driver_tags, type_extras, computed_at
        FROM {TABLE_MARKET_SCORES}
        WHERE asset = $1 AND instrument_type = $2
        """,
        asset,
        instrument_type,
    )
    if row is None:
        return None

    d = record_to_dict(row)
    # Parse JSONB fields
    tags = d.get("driver_tags")
    if isinstance(tags, str):
        try:
            d["driver_tags"] = json.loads(tags)
        except (json.JSONDecodeError, TypeError):
            d["driver_tags"] = []
    elif tags is None:
        d["driver_tags"] = []

    extras = d.get("type_extras")
    if isinstance(extras, str):
        try:
            d["type_extras"] = json.loads(extras)
        except (json.JSONDecodeError, TypeError):
            d["type_extras"] = None
    elif extras is None:
        d["type_extras"] = None

    return d
