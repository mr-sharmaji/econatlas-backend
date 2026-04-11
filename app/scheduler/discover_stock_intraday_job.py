"""Intraday live-price refresh for `discover_stock_snapshots`.

Purpose
-------
The main `discover_stock` job is a heavy end-of-day pipeline: it fetches
fundamentals, historical P&L/BS/CF, shareholding, and then runs a full
multi-factor rescore. Running it every 30 minutes during market hours
would be hugely expensive and would race the rescore path.

This job is the surgical alternative:

* Touches ONLY live-price columns on ``discover_stock_snapshots``
  (last_price / point_change / percent_change / volume / traded_value /
  updated_at) via a direct UPDATE.
* Also appends a tick row to ``discover_stock_intraday`` so the 1D
  chart on the stock detail screen has persistent 30-min resolution.
* Does NOT touch fundamentals, scores, score_breakdown, red flags,
  action tags, themes, history tables, or anything else that the
  daily pipeline owns.
* Skips automatically between **15:55 and 16:45 IST** so it can never
  collide with the full daily ``discover_stock`` → ``discover_stock_price``
  → rescore pipeline that runs at 16:00 / 16:20 / 16:30 IST.

Data-source strategy (NSE bulk primary, Yahoo fallback)
-------------------------------------------------------
NSE exposes a bulk endpoint
``https://www.nseindia.com/api/equity-stockIndices?index=<NAME>``
which, despite the confusing URL, returns the **live quote for every
constituent** of the named index in a single JSON payload — one call
yields ~500 per-stock live quotes (lastPrice, pChange, volume, high,
low, etc.). Fetching six broad indices in parallel (NIFTY 500, NEXT
50, MIDCAP 150, SMALLCAP 250, BANK, F&O) covers ~95% of the full
2,000-symbol Indian universe in ~3s wall-clock, using the authoritative
NSE source.

NSE requires a session cookie obtained from a browser-like GET to the
home page. httpx.AsyncClient with cookie persistence handles this.
NSE is permissive from residential/home-server IPs (our deploy target)
and blocks datacenter ranges — that's a known caveat, not a blocker
here.

Any symbol NSE did NOT return (SME boards, tiny illiquids, newly
listed names not yet in any broad index) falls back to Yahoo's v7
batch quote endpoint, which accepts up to ~50 symbols per call.

Optional Cloudflare Worker proxy for Yahoo
------------------------------------------
Set the ``INTRADAY_YAHOO_PROXY_URL`` environment variable to a
Cloudflare Worker URL (free tier) to route Yahoo calls through the
Worker's edge IPs. When unset, Yahoo is called directly. No Worker
is required for the job to function. NSE always calls direct.

Scheduling: every 30 minutes Mon-Fri 09:00-15:45 IST.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from app.core.database import get_pool
from app.scheduler.trading_calendar import is_trading_day_markets

_IST = ZoneInfo("Asia/Kolkata")

logger = logging.getLogger(__name__)

# Below this fraction of successful updates we log an ERROR so the
# ops dashboard / log pipeline picks up the degradation. Normal runs
# should be ≥ 98 %. Sustained < 80 % = something is broken upstream.
_LOW_UPDATE_RATIO_THRESHOLD = 0.80

# ── NSE configuration ───────────────────────────────────────────────
_NSE_HOME_URL = "https://www.nseindia.com"
_NSE_INDEX_URL = "https://www.nseindia.com/api/equity-stockIndices"
# Broad indices whose constituents collectively cover ~95% of the
# Indian equity universe. Order matters only for log aesthetics —
# dedupe across them happens via dict.setdefault.
_NSE_INDICES_TO_FETCH: tuple[str, ...] = (
    "NIFTY 500",
    "NIFTY NEXT 50",
    "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 250",
    "NIFTY BANK",
    "SECURITIES IN F&O",
)
_NSE_TIMEOUT_SEC = 8

# ── Yahoo fallback configuration ────────────────────────────────────
_YAHOO_BATCH_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
_YAHOO_BATCH_SIZE = 50
_YAHOO_BATCH_CONCURRENCY = 4
_YAHOO_PER_BATCH_TIMEOUT_SEC = 10

# ── Overall wall-clock ceiling ──────────────────────────────────────
_INTRADAY_TOTAL_TIMEOUT_SEC = 120

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

# Index-header rows NSE prepends to each payload — NOT real stocks.
_INDEX_HEADER_SYMBOLS: frozenset[str] = frozenset({
    "NIFTY 500", "NIFTY NEXT 50", "NIFTY MIDCAP 150",
    "NIFTY SMALLCAP 250", "NIFTY BANK", "NIFTY 50",
    "NIFTY BANK NIFTY", "SECURITIES IN F&O",
})


def _ist_hour_minute() -> tuple[int, int]:
    """Return the current hour:minute in IST (Asia/Kolkata)."""
    ist_now = datetime.now(_IST)
    return ist_now.hour, ist_now.minute


def _in_pipeline_exclusion_window() -> bool:
    """True if we are inside 15:55-16:45 IST (heavy pipeline window)."""
    h, m = _ist_hour_minute()
    start = 15 * 60 + 55
    end = 16 * 60 + 45
    now = h * 60 + m
    return start <= now < end


def _in_live_market_window() -> bool:
    """True if current IST is 09:15-15:30 inclusive (live trading).

    NSE pre-open auction (09:00-09:15) returns previous-day closing
    data so running the fetch then just rewrites stale values as
    "today". Post-close (> 15:30) the exchange returns the closing
    print which is captured by the explicit 15:45 close cron. This
    gate keeps every routine tick inside the actual live session.
    """
    h, m = _ist_hour_minute()
    start = 9 * 60 + 15
    end = 15 * 60 + 30
    now = h * 60 + m
    return start <= now <= end


def _is_trading_day_today() -> bool:
    """Check the shared trading calendar so we skip holidays.

    Mon-Fri cron alone doesn't cover Holi, Diwali, Republic Day, etc.
    On holidays NSE returns stale close data that would overwrite
    whatever fresh values we have. The daily pipeline uses the same
    calendar helper.
    """
    try:
        return is_trading_day_markets(datetime.now(timezone.utc))
    except Exception:
        logger.debug("intraday: trading calendar check failed", exc_info=True)
        # Fail-open: if the calendar raises, we'd rather run (stale
        # data is harmless) than silently skip trading days.
        return True


# ─────────────────────────────────────────────────────────────────────
# NSE bulk fetch
# ─────────────────────────────────────────────────────────────────────

async def _bootstrap_nse_session(client: httpx.AsyncClient) -> bool:
    """GET the NSE home page so `nsit` / `nseappid` cookies are set.

    NSE's API endpoints return empty 403 bodies without these session
    cookies. A single home-page GET populates them on the shared
    httpx.AsyncClient cookie jar. Returns True on success.
    """
    try:
        await client.get(
            _NSE_HOME_URL,
            headers=_BROWSER_HEADERS,
            timeout=_NSE_TIMEOUT_SEC,
            follow_redirects=True,
        )
        return True
    except Exception as exc:
        logger.warning("intraday: NSE session bootstrap failed: %s", exc)
        return False


async def _fetch_nse_index(
    client: httpx.AsyncClient,
    index_name: str,
) -> list[dict[str, Any]]:
    """Fetch one NSE index bulk endpoint and return its `data` array."""
    try:
        resp = await client.get(
            _NSE_INDEX_URL,
            params={"index": index_name},
            headers={
                **_BROWSER_HEADERS,
                "Referer": f"{_NSE_HOME_URL}/market-data/live-equity-market",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=_NSE_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning(
            "intraday: NSE index fetch failed for %s: %s", index_name, exc,
        )
        return []
    data = payload.get("data") or []
    return data if isinstance(data, list) else []


def _parse_nse_row(row: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Parse a single NSE index `data` row into (symbol, quote dict)."""
    try:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol or symbol in _INDEX_HEADER_SYMBOLS:
            return None
        last_raw = row.get("lastPrice")
        if last_raw is None:
            return None
        last_price = float(str(last_raw).replace(",", ""))
        if last_price <= 0:
            return None

        change_raw = row.get("change")
        pct_raw = row.get("pChange")
        vol_raw = row.get("totalTradedVolume")
        tv_raw = row.get("totalTradedValue")
        high_raw = row.get("dayHigh")
        low_raw = row.get("dayLow")

        def _f(v: Any) -> float | None:
            if v is None:
                return None
            try:
                return float(str(v).replace(",", ""))
            except (TypeError, ValueError):
                return None

        def _i(v: Any) -> int | None:
            if v is None:
                return None
            try:
                return int(float(str(v).replace(",", "")))
            except (TypeError, ValueError):
                return None

        return symbol, {
            "last_price": last_price,
            "point_change": _f(change_raw),
            "percent_change": _f(pct_raw),
            "volume": _i(vol_raw),
            "traded_value": _f(tv_raw),
            "day_high": _f(high_raw),
            "day_low": _f(low_raw),
            "source": "nse_bulk",
        }
    except (TypeError, ValueError):
        return None


async def _fetch_nse_bulk_quotes(
    client: httpx.AsyncClient,
) -> dict[str, dict[str, Any]]:
    """Fetch every NSE index in parallel and merge into one symbol map."""
    if not await _bootstrap_nse_session(client):
        return {}
    results = await asyncio.gather(
        *(_fetch_nse_index(client, name) for name in _NSE_INDICES_TO_FETCH),
        return_exceptions=True,
    )
    out: dict[str, dict[str, Any]] = {}
    for idx_result, idx_name in zip(results, _NSE_INDICES_TO_FETCH):
        if isinstance(idx_result, BaseException):
            continue
        before = len(out)
        for row in idx_result:
            if not isinstance(row, dict):
                continue
            parsed = _parse_nse_row(row)
            if parsed:
                sym, q = parsed
                out.setdefault(sym, q)
        logger.debug(
            "intraday: NSE %s added %d new symbols (rows=%d)",
            idx_name, len(out) - before, len(idx_result),
        )
    return out


# ─────────────────────────────────────────────────────────────────────
# Yahoo batch fallback (for symbols not in any NSE broad index)
# ─────────────────────────────────────────────────────────────────────

def _yahoo_base_url() -> str:
    """Return Yahoo base URL, honouring optional Cloudflare Worker proxy."""
    proxy = os.environ.get("INTRADAY_YAHOO_PROXY_URL", "").strip()
    if proxy:
        return proxy.rstrip("/") + "/v7/finance/quote"
    return _YAHOO_BATCH_QUOTE_URL


async def _fetch_yahoo_batch(
    client: httpx.AsyncClient,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    """Fetch up to ~50 quotes from Yahoo's v7 batch endpoint in one call."""
    if not symbols:
        return {}
    yahoo_symbols = [f"{s}.NS" for s in symbols]
    try:
        resp = await client.get(
            _yahoo_base_url(),
            params={"symbols": ",".join(yahoo_symbols)},
            headers=_BROWSER_HEADERS,
            timeout=_YAHOO_PER_BATCH_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug(
            "intraday: Yahoo fallback batch failed (%d symbols): %s",
            len(symbols), exc,
        )
        return {}

    out: dict[str, dict[str, Any]] = {}
    rows = (payload.get("quoteResponse") or {}).get("result") or []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ysym = row.get("symbol")
        if not ysym or not ysym.endswith(".NS"):
            continue
        plain = ysym[:-3]
        price = row.get("regularMarketPrice")
        if price is None:
            continue
        try:
            price_f = float(price)
        except (TypeError, ValueError):
            continue
        prev = (
            row.get("regularMarketPreviousClose")
            or row.get("previousClose")
        )
        try:
            prev_f = float(prev) if prev is not None else None
        except (TypeError, ValueError):
            prev_f = None
        point = round(price_f - prev_f, 2) if prev_f is not None else None
        pct = (
            round(((price_f - prev_f) / prev_f) * 100, 2)
            if prev_f not in (None, 0)
            else None
        )
        vol_raw = row.get("regularMarketVolume")
        try:
            volume = int(vol_raw) if vol_raw is not None else None
        except (TypeError, ValueError):
            volume = None
        out[plain] = {
            "last_price": price_f,
            "point_change": point,
            "percent_change": pct,
            "volume": volume,
            "traded_value": (
                round(price_f * volume, 2) if volume is not None else None
            ),
            "day_high": (
                float(row["regularMarketDayHigh"])
                if row.get("regularMarketDayHigh") is not None else None
            ),
            "day_low": (
                float(row["regularMarketDayLow"])
                if row.get("regularMarketDayLow") is not None else None
            ),
            "source": "yahoo_batch",
        }
    return out


async def _fetch_yahoo_fallback(
    client: httpx.AsyncClient,
    symbols: list[str],
) -> dict[str, dict[str, Any]]:
    """Parallel Yahoo batch fetch over a list of symbols."""
    if not symbols:
        return {}
    batches = [
        symbols[i:i + _YAHOO_BATCH_SIZE]
        for i in range(0, len(symbols), _YAHOO_BATCH_SIZE)
    ]
    sem = asyncio.Semaphore(_YAHOO_BATCH_CONCURRENCY)
    out: dict[str, dict[str, Any]] = {}

    async def _worker(batch: list[str]) -> None:
        async with sem:
            result = await _fetch_yahoo_batch(client, batch)
            out.update(result)

    await asyncio.gather(
        *(_worker(b) for b in batches),
        return_exceptions=True,
    )
    return out


# ─────────────────────────────────────────────────────────────────────
# Main entrypoint
# ─────────────────────────────────────────────────────────────────────

async def run_discover_stock_intraday_job() -> None:
    """Refresh live-price columns for the full Indian universe.

    Stage 0: Trading-day / market-hours gates.
    Stage 1: NSE bulk index endpoints (primary, ~6 calls, ~3s).
    Stage 2: Yahoo v7 batch fallback for whatever NSE missed.
    Stage 3: UPDATE snapshots + INSERT intraday ticks + prune old ticks.
    Stage 4: Low-update-ratio ERROR log for ops observability.
    """
    if not _is_trading_day_today():
        logger.info("discover_stock_intraday: skipping — not a trading day")
        return

    if _in_pipeline_exclusion_window():
        logger.info(
            "discover_stock_intraday: skipping — inside 15:55-16:45 IST "
            "daily pipeline window",
        )
        return

    # The explicit 15:45 close cron is allowed through the live-market
    # gate so we capture the closing print before the 16:00 rescore.
    h, m = _ist_hour_minute()
    is_close_tick = (h == 15 and m == 45)
    if not is_close_tick and not _in_live_market_window():
        logger.info(
            "discover_stock_intraday: skipping — outside live market "
            "window (09:15-15:30 IST). Now=%02d:%02d IST", h, m,
        )
        return

    t0 = time.monotonic()
    pool = await get_pool()

    rows = await pool.fetch(
        "SELECT symbol FROM discover_stock_snapshots "
        "WHERE market = 'IN' "
        "ORDER BY market_cap DESC NULLS LAST",
    )
    if not rows:
        logger.info("discover_stock_intraday: no symbols to refresh")
        return

    symbols: list[str] = [r["symbol"] for r in rows if r["symbol"]]
    logger.info(
        "discover_stock_intraday: refreshing %d symbols "
        "(NSE bulk primary, Yahoo batch fallback)",
        len(symbols),
    )

    updated = 0
    skipped = 0
    errors = 0
    nse_count = 0
    yahoo_count = 0

    async with httpx.AsyncClient(
        timeout=_NSE_TIMEOUT_SEC,
        follow_redirects=True,
    ) as client:
        nse_quotes = await _fetch_nse_bulk_quotes(client)
        nse_count = len(nse_quotes)
        logger.info(
            "intraday: NSE bulk returned %d unique symbols in %.1fs",
            nse_count, time.monotonic() - t0,
        )

        missing = [s for s in symbols if s not in nse_quotes]
        yahoo_quotes: dict[str, dict[str, Any]] = {}
        if missing and time.monotonic() - t0 < _INTRADAY_TOTAL_TIMEOUT_SEC:
            logger.info(
                "intraday: %d symbols missing from NSE, falling back to Yahoo",
                len(missing),
            )
            yahoo_quotes = await _fetch_yahoo_fallback(client, missing)
            yahoo_count = len(yahoo_quotes)

    # Bulk-write via executemany inside one transaction — the naive
    # per-symbol loop did ~4000 sequential round-trips (~60s). Batched
    # executemany collapses that to two statements (~2s).
    update_rows: list[tuple] = []
    insert_rows: list[tuple] = []
    for symbol in symbols:
        q = nse_quotes.get(symbol) or yahoo_quotes.get(symbol)
        if not q or q.get("last_price") is None:
            skipped += 1
            continue
        update_rows.append((
            symbol,
            q.get("last_price"),
            q.get("point_change"),
            q.get("percent_change"),
            q.get("volume"),
            q.get("traded_value"),
        ))
        insert_rows.append((
            symbol,
            q.get("last_price"),
            q.get("volume"),
            q.get("percent_change"),
        ))

    if update_rows:
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(
                        """
                        UPDATE discover_stock_snapshots
                        SET last_price = $2,
                            point_change = COALESCE($3, point_change),
                            percent_change = COALESCE($4, percent_change),
                            volume = COALESCE($5, volume),
                            traded_value = COALESCE($6, traded_value),
                            ingested_at = NOW()
                        WHERE symbol = $1
                        """,
                        update_rows,
                    )
                    await conn.executemany(
                        """
                        INSERT INTO discover_stock_intraday
                            (symbol, ts, price, volume, percent_change)
                        VALUES ($1, NOW(), $2, $3, $4)
                        ON CONFLICT (symbol, ts) DO NOTHING
                        """,
                        insert_rows,
                    )
            updated = len(update_rows)
        except Exception:
            logger.exception("intraday: bulk write transaction failed")
            errors = len(update_rows)
            updated = 0

    # Prune runs once per trading day at the close tick.
    if is_close_tick:
        try:
            await pool.execute(
                "DELETE FROM discover_stock_intraday "
                "WHERE ts < NOW() - INTERVAL '2 days'"
            )
        except Exception:
            logger.debug("intraday: prune failed", exc_info=True)

    elapsed = time.monotonic() - t0
    total = len(symbols)
    ratio = updated / total if total else 0.0
    log_fn = (
        logger.error
        if ratio < _LOW_UPDATE_RATIO_THRESHOLD
        else logger.info
    )
    log_fn(
        "discover_stock_intraday: done in %.1fs — "
        "updated=%d/%d (%.0f%%) nse=%d yahoo=%d skipped=%d errors=%d",
        elapsed, updated, total, ratio * 100,
        nse_count, yahoo_count, skipped, errors,
    )
    if ratio < _LOW_UPDATE_RATIO_THRESHOLD and total >= 100:
        logger.error(
            "discover_stock_intraday: LOW UPDATE RATIO — only %.0f%% of "
            "symbols updated this tick. Check NSE cookie session and "
            "Yahoo reachability.",
            ratio * 100,
        )


# ─────────────────────────────────────────────────────────────────────
# One-shot backfill: populate discover_stock_intraday from Yahoo's
# 5-minute chart history for the last trading day.
#
# Purpose
# -------
# The routine 30-min intraday scheduler only starts writing rows from
# the next 09:15 IST market open. On first deploy (or after a fresh
# migration), the table is empty, so every stock detail screen opens
# with a blank 1D chart until the scheduler has had time to paint
# enough ticks.
#
# This backfill:
#   * Fetches Yahoo chart `interval=5m&range=1d` per symbol (~78 ticks
#     per stock for today's session)
#   * Parses each tick into (symbol, ts_utc, price, volume)
#   * Bulk-inserts with `ON CONFLICT (symbol, ts) DO NOTHING` so it's
#     safe to re-run and won't clobber live scheduler rows (different
#     timestamps entirely — scheduler uses NOW() round to the tick,
#     backfill uses Yahoo's 5-minute bucket boundaries)
#   * Bounded concurrency (8 workers, ~5 min wall-clock for 2000 stocks)
#
# Triggered manually via `/ops/jobs/trigger/discover_stock_intraday_backfill`.
# NOT on the scheduler — this is a one-shot recovery tool, not a
# recurring job.
# ─────────────────────────────────────────────────────────────────────

_BACKFILL_CONCURRENCY = 8
_BACKFILL_PER_SYMBOL_TIMEOUT_SEC = 6
_BACKFILL_TOTAL_TIMEOUT_SEC = 900  # 15 min hard ceiling


async def _fetch_yahoo_5m_for_day(
    client: httpx.AsyncClient,
    symbol: str,
) -> list[tuple[datetime, float, int | None]]:
    """Fetch Yahoo's 5-min chart for the last day for one symbol.

    Returns a list of (ts_utc, price, volume). Empty list on any error.
    """
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS"
        "?interval=5m&range=1d"
    )
    try:
        resp = await client.get(
            url,
            headers=_BROWSER_HEADERS,
            timeout=_BACKFILL_PER_SYMBOL_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return []

    try:
        result = (payload.get("chart") or {}).get("result") or []
        if not result:
            return []
        r0 = result[0]
        timestamps = r0.get("timestamp") or []
        quote = (r0.get("indicators") or {}).get("quote") or [{}]
        closes = quote[0].get("close") or []
        volumes = quote[0].get("volume") or []
    except Exception:
        return []

    out: list[tuple[datetime, float, int | None]] = []
    for i, ts in enumerate(timestamps):
        if i >= len(closes):
            break
        close = closes[i]
        if close is None:
            continue
        try:
            price = float(close)
        except (TypeError, ValueError):
            continue
        try:
            ts_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except (TypeError, ValueError, OverflowError):
            continue
        vol: int | None = None
        if i < len(volumes) and volumes[i] is not None:
            try:
                vol = int(volumes[i])
            except (TypeError, ValueError):
                vol = None
        out.append((ts_utc, price, vol))
    return out


async def run_discover_stock_intraday_backfill() -> None:
    """One-shot: backfill today's 5-min intraday ticks from Yahoo.

    Safe to re-run: the ON CONFLICT (symbol, ts) DO NOTHING clause
    makes repeated executions idempotent. Does NOT touch any field
    on discover_stock_snapshots — only inserts into the ticks table.
    """
    t0 = time.monotonic()
    pool = await get_pool()

    rows = await pool.fetch(
        "SELECT symbol FROM discover_stock_snapshots "
        "WHERE market = 'IN' "
        "ORDER BY market_cap DESC NULLS LAST",
    )
    symbols: list[str] = [r["symbol"] for r in rows if r["symbol"]]
    if not symbols:
        logger.info("intraday_backfill: no symbols to backfill")
        return

    logger.info(
        "intraday_backfill: fetching Yahoo 5m range=1d for %d symbols",
        len(symbols),
    )

    sem = asyncio.Semaphore(_BACKFILL_CONCURRENCY)
    all_rows: list[tuple[str, datetime, float, int | None]] = []
    deadline = time.monotonic() + _BACKFILL_TOTAL_TIMEOUT_SEC
    fetched_ok = 0
    fetched_empty = 0

    async with httpx.AsyncClient(
        timeout=_BACKFILL_PER_SYMBOL_TIMEOUT_SEC,
        follow_redirects=True,
    ) as client:

        async def _worker(symbol: str) -> None:
            nonlocal fetched_ok, fetched_empty
            async with sem:
                if time.monotonic() > deadline:
                    return
                points = await _fetch_yahoo_5m_for_day(client, symbol)
            if points:
                fetched_ok += 1
                for ts_utc, price, vol in points:
                    all_rows.append((symbol, ts_utc, price, vol))
            else:
                fetched_empty += 1

        await asyncio.gather(
            *(_worker(s) for s in symbols),
            return_exceptions=True,
        )

    fetch_elapsed = time.monotonic() - t0
    logger.info(
        "intraday_backfill: fetch done in %.1fs — "
        "ok=%d empty=%d total_ticks=%d",
        fetch_elapsed, fetched_ok, fetched_empty, len(all_rows),
    )

    if not all_rows:
        logger.warning("intraday_backfill: no ticks collected, nothing to insert")
        return

    # Bulk INSERT with ON CONFLICT DO NOTHING. Chunk at 5k rows to keep
    # any single statement under Postgres' parameter-binding limits.
    insert_sql = (
        "INSERT INTO discover_stock_intraday "
        "(symbol, ts, price, volume, percent_change) "
        "VALUES ($1, $2, $3, $4, NULL) "
        "ON CONFLICT (symbol, ts) DO NOTHING"
    )
    inserted = 0
    chunk_size = 5000
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                for i in range(0, len(all_rows), chunk_size):
                    chunk = all_rows[i:i + chunk_size]
                    await conn.executemany(insert_sql, chunk)
                    inserted += len(chunk)
    except Exception:
        logger.exception("intraday_backfill: bulk insert failed")
        return

    logger.info(
        "intraday_backfill: DONE in %.1fs — symbols_ok=%d symbols_empty=%d "
        "rows_queued=%d (dedup on conflict)",
        time.monotonic() - t0, fetched_ok, fetched_empty, inserted,
    )
