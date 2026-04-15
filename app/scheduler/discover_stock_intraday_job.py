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
_YAHOO_BATCH_CONCURRENCY = 6
_YAHOO_PER_BATCH_TIMEOUT_SEC = 10

# ── Overall wall-clock ceiling ──────────────────────────────────────
_INTRADAY_TOTAL_TIMEOUT_SEC = 180

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
        # Extract actual exchange quote time from Yahoo (Unix epoch).
        rmt = row.get("regularMarketTime")
        quote_ts = None
        if rmt is not None:
            try:
                from datetime import datetime, timezone
                quote_ts = datetime.fromtimestamp(int(rmt), tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                pass

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
            "quote_ts": quote_ts,
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
    from datetime import datetime, timezone as _tz
    _fetch_ts = datetime.now(_tz.utc)  # fallback for NSE quotes with no per-symbol ts

    update_rows: list[tuple] = []
    insert_rows: list[tuple] = []
    for symbol in symbols:
        q = nse_quotes.get(symbol) or yahoo_quotes.get(symbol)
        if not q or q.get("last_price") is None:
            skipped += 1
            continue
        # Use actual exchange quote timestamp when available (Yahoo
        # provides regularMarketTime). For NSE bulk quotes which
        # don't carry per-symbol timestamps, fall back to the fetch
        # wall-clock time.
        ts = q.get("quote_ts") or _fetch_ts
        update_rows.append((
            symbol,
            q.get("last_price"),
            q.get("point_change"),
            q.get("percent_change"),
            q.get("volume"),
            q.get("traded_value"),
            ts,
        ))
        insert_rows.append((
            symbol,
            ts,  # actual quote timestamp, not NOW()
            q.get("last_price"),
            q.get("volume"),
            q.get("percent_change"),
        ))

    logger.info(
        "intraday: prepared %d update_rows, %d insert_rows, %d skipped "
        "(nse=%d, yahoo=%d)",
        len(update_rows), len(insert_rows), skipped, nse_count, yahoo_count,
    )

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
                            source_timestamp = $7,
                            ingested_at = NOW()
                        WHERE symbol = $1
                        """,
                        update_rows,
                    )
                    logger.info(
                        "intraday: UPDATE executemany OK (%d rows)",
                        len(update_rows),
                    )
                    await conn.executemany(
                        """
                        INSERT INTO discover_stock_intraday
                            (symbol, ts, price, volume, percent_change)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (symbol, ts) DO NOTHING
                        """,
                        insert_rows,
                    )
                    logger.info(
                        "intraday: INSERT executemany OK (%d rows into discover_stock_intraday)",
                        len(insert_rows),
                    )
            updated = len(update_rows)
            logger.info(
                "intraday: transaction COMMITTED — %d snapshots updated, %d ticks inserted",
                updated, len(insert_rows),
            )
        except Exception:
            logger.exception("intraday: bulk write transaction FAILED — rolling back")
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

# Yahoo's v8 chart endpoint rate-limits at ~2000 req/hour/IP for
# unauthenticated clients. At 8 concurrent workers we hit that in
# seconds and every subsequent call returns HTTP 429. Throttle to 2
# concurrent + deliberate inter-call pacing so we stay well under the
# quota: 2 × ~3 req/sec ≈ 360 req/min, completing 2274 symbols in
# ~6-7 min while staying within ~2000/hour.
_BACKFILL_CONCURRENCY = 2
_BACKFILL_PER_SYMBOL_TIMEOUT_SEC = 6
_BACKFILL_INTER_CALL_DELAY_SEC = 0.3
# Generous ceiling — Upstox-based backfill for ~2300 stocks with 6
# concurrent workers + 0.1s pacing completes in ~5 minutes, but on a
# cold run + slow network we want headroom so the whole universe
# always finishes inside one invocation.
_BACKFILL_TOTAL_TIMEOUT_SEC = 2400  # 40 min hard ceiling


# ── Upstox primary source ───────────────────────────────────────────
#
# Yahoo v8/chart is heavily rate-limited on datacenter IPs (every call
# returns 429). Upstox's public historical-candle API is designed for
# server-side use, works unauthenticated for market data, and returns
# clean [ts, open, high, low, close, volume, oi] tuples.
#
# Flow:
#   1. Once per run, download Upstox's instrument master CSV (~6 MB
#      gzipped) and build a {NSE_symbol → "NSE_EQ|ISIN"} lookup.
#   2. For each target stock, call
#      /v2/historical-candle/<key>/30minute/<to>/<from> to fetch the
#      previous N sessions.
#   3. Parse candles → (symbol, ts_utc, close, volume) and insert via
#      the same ON CONFLICT DO NOTHING path the Yahoo backfill used.
#
# Upstox imposes a ~100 req/minute soft limit per IP. At 0.8s inter-
# call pacing we do ~75 req/min, comfortably under.
_UPSTOX_INSTRUMENTS_CSV_URL = (
    "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
)
_UPSTOX_HISTORICAL_URL_TMPL = (
    "https://api.upstox.com/v2/historical-candle/{key}/{interval}/{to_date}/{from_date}"
)
# Intraday endpoint — returns TODAY'S in-progress candles, which the
# historical endpoint doesn't always include (Upstox's historical-
# candle returns bars that are fully closed on the server side, so
# low-liquidity or SME stocks may not appear until EOD). Use this
# for same-day data; use the historical endpoint for earlier days.
_UPSTOX_INTRADAY_URL_TMPL = (
    "https://api.upstox.com/v2/historical-candle/intraday/{key}/{interval}"
)
# Back-compat alias used by run_discover_stock_intraday_backfill.
_UPSTOX_HISTORICAL_URL = _UPSTOX_HISTORICAL_URL_TMPL.replace("{interval}", "30minute")
_UPSTOX_PER_CALL_TIMEOUT_SEC = 10
# Upstox v2 historical-candle soft limits are ~25 req/sec / ~250
# req/min per IP. The previous 6-worker × 0.1s config yielded an
# aggregate of ~60 req/sec which triggered Upstox rate-limiting and
# only 1108/2302 symbols came back with data. New config:
#   3 concurrent workers × 0.18 s inter-call delay ≈ 16 req/sec
#   aggregate — well under the 25/sec ceiling AND under the 250/min
#   rolling cap. 2300 stocks complete in ~150 s.
_UPSTOX_CONCURRENCY = 3
_UPSTOX_INTER_CALL_DELAY_SEC = 0.18
# Cache the instrument master for 24h inside the worker process —
# rebuild takes ~5s and is only needed when Upstox adds new listings.
_upstox_instrument_cache: dict[str, str] = {}
_upstox_instrument_cache_ts: float = 0.0


async def _load_upstox_instrument_map(
    client: httpx.AsyncClient,
) -> dict[str, str]:
    """Return {NSE tradingsymbol → 'NSE_EQ|ISIN'} lookup. Cached 24h."""
    global _upstox_instrument_cache, _upstox_instrument_cache_ts
    now = time.monotonic()
    if (
        _upstox_instrument_cache
        and now - _upstox_instrument_cache_ts < 24 * 3600
    ):
        return _upstox_instrument_cache
    try:
        resp = await client.get(
            _UPSTOX_INSTRUMENTS_CSV_URL,
            timeout=30,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("upstox: instrument master download failed: %s", exc)
        return _upstox_instrument_cache  # return whatever we had
    import gzip
    import io
    try:
        buf = io.BytesIO(resp.content)
        with gzip.GzipFile(fileobj=buf) as gz:
            raw = gz.read().decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning("upstox: instrument master decode failed: %s", exc)
        return _upstox_instrument_cache
    mapping: dict[str, str] = {}
    # CSV header: "instrument_key","exchange_token","tradingsymbol",...
    # We only want NSE_EQ equity rows.
    for line in raw.splitlines()[1:]:
        if not line.startswith('"NSE_EQ'):
            continue
        # Simple split — the fields we need don't contain commas.
        parts = line.split(",", 4)
        if len(parts) < 4:
            continue
        key = parts[0].strip('"')
        ts = parts[2].strip('"').upper()
        if ts and key.startswith("NSE_EQ|"):
            mapping.setdefault(ts, key)
    if mapping:
        _upstox_instrument_cache = mapping
        _upstox_instrument_cache_ts = now
        logger.warning(
            "upstox: instrument master loaded, %d NSE symbols mapped",
            len(mapping),
        )
    return mapping


async def _fetch_upstox_candles_for_symbol(
    client: httpx.AsyncClient,
    instrument_key: str,
    from_date: str,
    to_date: str,
    interval: str = "30minute",
) -> list[tuple[datetime, float, int | None]]:
    """Fetch Upstox historical candles for a symbol over a date range.

    `interval` is any valid Upstox v2 value: `1minute`, `30minute`,
    `day`, `week`, `month`. The autofill job uses `1minute` for the
    09:15-09:30 opening edge and `30minute` for the rest of the
    session.

    Returns [(ts_utc, close, volume), ...]. Empty on any error.
    """
    import urllib.parse as _up
    encoded_key = _up.quote(instrument_key, safe="")
    url = _UPSTOX_HISTORICAL_URL_TMPL.format(
        key=encoded_key,
        interval=interval,
        to_date=to_date,
        from_date=from_date,
    )
    try:
        resp = await client.get(
            url,
            headers={"Accept": "application/json"},
            timeout=_UPSTOX_PER_CALL_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug(
            "upstox: historical fetch failed for %s: %s",
            instrument_key, exc,
        )
        return []
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    candles = (
        (payload.get("data") or {}).get("candles") or []
    )
    out: list[tuple[datetime, float, int | None]] = []
    for row in candles:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts_str = row[0]
            close = float(row[4])
            volume_raw = row[5]
            volume = int(volume_raw) if volume_raw is not None else None
        except (TypeError, ValueError, IndexError):
            continue
        try:
            # ISO 8601 like "2026-04-10T15:15:00+05:30"
            ts_utc = datetime.fromisoformat(ts_str).astimezone(timezone.utc)
        except (TypeError, ValueError):
            continue
        out.append((ts_utc, close, volume))
    return out


async def _fetch_upstox_intraday_for_symbol(
    client: httpx.AsyncClient,
    instrument_key: str,
    interval: str = "30minute",
) -> list[tuple[datetime, float, int | None]]:
    """Fetch Upstox intraday candles for TODAY.

    Same shape as `_fetch_upstox_candles_for_symbol` but hits the
    `/historical-candle/intraday/` endpoint which serves current-day
    in-progress bars. Use this for today; use the historical
    endpoint for any earlier day.
    """
    import urllib.parse as _up
    encoded_key = _up.quote(instrument_key, safe="")
    url = _UPSTOX_INTRADAY_URL_TMPL.format(
        key=encoded_key,
        interval=interval,
    )
    try:
        resp = await client.get(
            url,
            headers={"Accept": "application/json"},
            timeout=_UPSTOX_PER_CALL_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.debug(
            "upstox: intraday fetch failed for %s: %s",
            instrument_key, exc,
        )
        return []
    if not isinstance(payload, dict) or payload.get("status") != "success":
        return []
    candles = (
        (payload.get("data") or {}).get("candles") or []
    )
    out: list[tuple[datetime, float, int | None]] = []
    for row in candles:
        if not isinstance(row, list) or len(row) < 6:
            continue
        try:
            ts_str = row[0]
            close = float(row[4])
            volume_raw = row[5]
            volume = int(volume_raw) if volume_raw is not None else None
        except (TypeError, ValueError, IndexError):
            continue
        try:
            ts_utc = datetime.fromisoformat(ts_str).astimezone(timezone.utc)
        except (TypeError, ValueError):
            continue
        out.append((ts_utc, close, volume))
    return out


def _yahoo_chart_base() -> str:
    """Return Yahoo v8/chart base URL, honouring the Cloudflare
    Worker proxy if INTRADAY_YAHOO_PROXY_URL is set.

    Without the proxy the backfill runs into Yahoo's datacenter-IP
    rate limits on cloud hosts and every fetch returns empty or 429.
    The proxy base must expose a ``/v8/finance/chart`` sub-path
    compatible with Yahoo's public API.
    """
    proxy = os.environ.get("INTRADAY_YAHOO_PROXY_URL", "").strip()
    if proxy:
        return proxy.rstrip("/") + "/v8/finance/chart"
    return "https://query1.finance.yahoo.com/v8/finance/chart"


async def _fetch_yahoo_5m_for_day(
    client: httpx.AsyncClient,
    symbol: str,
) -> list[tuple[datetime, float, int | None]]:
    """Fetch Yahoo's 5-min chart for the last day for one symbol.

    Returns a list of (ts_utc, price, volume). Empty list on any error.
    """
    # range=2d instead of 1d so we always capture at least the previous
    # trading session. On a weekend / holiday morning, range=1d returns
    # nothing (no active session today) while range=2d still has Friday's
    # 5-minute ticks. We keep the 5m granularity for dense chart data.
    url = (
        f"{_yahoo_chart_base()}/{symbol}.NS"
        "?interval=5m&range=2d"
    )
    try:
        resp = await client.get(
            url,
            headers=_BROWSER_HEADERS,
            timeout=_BACKFILL_PER_SYMBOL_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        # Log first-N failures at WARNING so datacenter-IP rate limits
        # surface in ops instead of silently yielding an empty backfill.
        # The logger.debug inside the caller shows ALL errors at DEBUG.
        logger.debug(
            "intraday_backfill: Yahoo v8 fetch failed for %s: %s",
            symbol, exc,
        )
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
    """One-shot: backfill the last 2 trading sessions of 30-min ticks.

    Primary source is Upstox's `/v2/historical-candle` which works
    from datacenter IPs without auth; Yahoo v8 is retained only as a
    last-ditch fallback for symbols Upstox doesn't map.

    Safe to re-run: the ON CONFLICT (symbol, ts) DO NOTHING clause
    makes repeated executions idempotent. Does NOT touch any field
    on discover_stock_snapshots — only inserts into the ticks table.
    """
    # Logged at WARNING so it's unlikely to be rotated out of the
    # ring-buffer log cache before we can diagnose it via /ops/logs.
    logger.warning("intraday_backfill: STARTED")
    t0 = time.monotonic()
    pool = await get_pool()

    rows = await pool.fetch(
        "SELECT symbol FROM discover_stock_snapshots "
        "WHERE market = 'IN' "
        "ORDER BY market_cap DESC NULLS LAST",
    )
    symbols: list[str] = [r["symbol"] for r in rows if r["symbol"]]
    if not symbols:
        logger.warning("intraday_backfill: no symbols to backfill")
        return

    # Date window for Upstox: today and the 2 previous calendar days.
    # This always captures at least the most recent trading session.
    today_ist = datetime.now(_IST).date()
    from datetime import timedelta
    from_date = (today_ist - timedelta(days=3)).isoformat()
    to_date = today_ist.isoformat()

    logger.warning(
        "intraday_backfill: fetching Upstox 30m %s→%s for %d symbols",
        from_date, to_date, len(symbols),
    )

    all_rows: list[tuple[str, datetime, float, int | None]] = []
    deadline = time.monotonic() + _BACKFILL_TOTAL_TIMEOUT_SEC
    fetched_ok = 0
    fetched_empty = 0
    fetched_upstox = 0
    fetched_yahoo = 0

    async with httpx.AsyncClient(
        timeout=_UPSTOX_PER_CALL_TIMEOUT_SEC,
        follow_redirects=True,
    ) as client:
        instr_map = await _load_upstox_instrument_map(client)
        if not instr_map:
            logger.error(
                "intraday_backfill: Upstox instrument master empty, "
                "falling back to Yahoo for every symbol",
            )

        # Concurrent Upstox fetch — bounded by _UPSTOX_CONCURRENCY.
        # Each worker honours the shared deadline and appends to the
        # shared all_rows list under a simple counter (GIL-safe for
        # list.append + integer increment).
        upstox_sem = asyncio.Semaphore(_UPSTOX_CONCURRENCY)
        missing_upstox: list[str] = []
        missing_lock = asyncio.Lock()

        async def _upstox_worker(symbol: str) -> None:
            nonlocal fetched_ok, fetched_empty, fetched_upstox
            if time.monotonic() > deadline:
                return
            key = instr_map.get(symbol)
            if key is None:
                async with missing_lock:
                    missing_upstox.append(symbol)
                return
            async with upstox_sem:
                points = await _fetch_upstox_candles_for_symbol(
                    client, key, from_date, to_date, interval="30minute",
                )
                await asyncio.sleep(_UPSTOX_INTER_CALL_DELAY_SEC)
            if points:
                fetched_ok += 1
                fetched_upstox += 1
                for ts_utc, price, vol in points:
                    all_rows.append((symbol, ts_utc, price, vol))
            else:
                fetched_empty += 1
                async with missing_lock:
                    missing_upstox.append(symbol)

        await asyncio.gather(
            *(_upstox_worker(s) for s in symbols),
            return_exceptions=True,
        )

        logger.warning(
            "intraday_backfill: Upstox pass done — upstox_ok=%d missing=%d",
            fetched_upstox, len(missing_upstox),
        )

        # Yahoo fallback for anything Upstox couldn't resolve. On a
        # datacenter IP this will mostly 429, but it's harmless to try.
        if missing_upstox and time.monotonic() < deadline:
            sem = asyncio.Semaphore(_BACKFILL_CONCURRENCY)

            async def _yahoo_worker(symbol: str) -> None:
                nonlocal fetched_ok, fetched_empty, fetched_yahoo
                async with sem:
                    if time.monotonic() > deadline:
                        return
                    points = await _fetch_yahoo_5m_for_day(client, symbol)
                    await asyncio.sleep(_BACKFILL_INTER_CALL_DELAY_SEC)
                if points:
                    fetched_ok += 1
                    fetched_yahoo += 1
                    for ts_utc, price, vol in points:
                        all_rows.append((symbol, ts_utc, price, vol))
                else:
                    fetched_empty += 1

            await asyncio.gather(
                *(_yahoo_worker(s) for s in missing_upstox),
                return_exceptions=True,
            )

    fetch_elapsed = time.monotonic() - t0
    logger.warning(
        "intraday_backfill: fetch done in %.1fs — "
        "upstox=%d yahoo=%d empty=%d total_ticks=%d",
        fetch_elapsed, fetched_upstox, fetched_yahoo,
        fetched_empty, len(all_rows),
    )

    if not all_rows:
        logger.error(
            "intraday_backfill: no ticks collected, nothing to insert — "
            "ok=%d empty=%d (check Yahoo reachability / .NS suffix)",
            fetched_ok, fetched_empty,
        )
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

    logger.warning(
        "intraday_backfill: DONE in %.1fs — symbols_ok=%d symbols_empty=%d "
        "rows_queued=%d (dedup on conflict)",
        time.monotonic() - t0, fetched_ok, fetched_empty, inserted,
    )


# ─────────────────────────────────────────────────────────────────────
# Auto-backfill (self-healing sweeper)
#
# Runs every 10 minutes during the Indian trading session and repairs
# any gaps in discover_stock_intraday left by:
#   * Outages (server/scheduler down)
#   * Symbols not in NSE bulk indices (SME, illiquids, BALUFORGE-class)
#   * Yahoo fallback rate-limit failures
#   * The inherent 09:15-09:30 opening gap — the live 30-min cron
#     first fires at 09:30 IST so the opening 15 minutes of every
#     session are otherwise unrecorded.
#
# Data source: Upstox `/v2/historical-candle/intraday/` for today's
# in-progress bars. Upstox is permissive from datacenter IPs (unlike
# Yahoo's v8 chart endpoint which 429s us) and its instrument master
# covers ~2254 of our ~2297 NSE tickers.
#
# Registered as both an ops job (`discover_stock_intraday_autofill`)
# and on the scheduler cron (every 10 min at :4, :14, :24, :34, :44,
# :54 past the hour IST — staggered 4 min past the 10-min marks to
# avoid colliding with any rounded-minute cadence, and well outside
# the live job's :00/:30 firing slots). Also added to
# _startup_collection so fresh deploys immediately repair state.
# ─────────────────────────────────────────────────────────────────────

# Run window buffers a couple minutes past market open/close so the
# first and last autofill sweeps still pick up the edge bars.
_AUTOFILL_SESSION_START_IST = (9, 15)
_AUTOFILL_SESSION_END_IST = (15, 45)


def _autofill_in_session() -> bool:
    h, m = _ist_hour_minute()
    now = h * 60 + m
    start = _AUTOFILL_SESSION_START_IST[0] * 60 + _AUTOFILL_SESSION_START_IST[1]
    end = _AUTOFILL_SESSION_END_IST[0] * 60 + _AUTOFILL_SESSION_END_IST[1]
    return start <= now <= end


async def run_discover_stock_intraday_autofill_job() -> None:
    """Self-healing sweeper. Fills gaps in discover_stock_intraday.

    Algorithm per tick:
      1. Gate on Indian trading day + market window (09:15-15:45 IST).
      2. Load the Upstox instrument master (cached 24h).
      3. For every IN symbol, call Upstox intraday 30-min to get
         today's in-progress bars and bulk-insert with ON CONFLICT
         DO NOTHING. This repairs:
           - Symbols missing from NSE bulk (BALUFORGE-class)
           - Mid-session scheduler outages (bars land in the DB as
             soon as the server is back up)
           - Incomplete data from the live 30-min cron
      4. On the FIRST tick after 09:15 IST each day, additionally
         fetch Upstox intraday 1-min candles for every symbol to
         fill the 09:15-09:30 opening edge which the 30-min bars
         don't cover with minute-level detail.
      5. Yesterday-catchup: if yesterday was a trading day and the
         DB has fewer than 80% of symbols covered for yesterday,
         run the historical-candle 30-min backfill for yesterday
         only. Cheap no-op on holidays (Upstox returns nothing).

    Rate budget:
      * _UPSTOX_CONCURRENCY=3 workers × 0.18s pacing ≈ 16 req/s.
      * ~2300 symbols × 1 req/symbol ≈ 145s per sweep.
      * ON CONFLICT DO NOTHING means most inserts are no-ops when
        the live cron already captured the data, so actual DB work
        is small.
    """
    if not _is_trading_day_today():
        logger.debug("intraday_autofill: skipping — not a trading day")
        return
    if not _autofill_in_session():
        h, m = _ist_hour_minute()
        logger.debug(
            "intraday_autofill: skipping — outside session window "
            "(now=%02d:%02d IST)", h, m,
        )
        return
    if _in_pipeline_exclusion_window():
        logger.debug(
            "intraday_autofill: skipping — inside 15:55-16:45 IST pipeline window"
        )
        return

    t0 = time.monotonic()
    pool = await get_pool()

    rows = await pool.fetch(
        "SELECT symbol FROM discover_stock_snapshots "
        "WHERE market = 'IN' "
        "ORDER BY market_cap DESC NULLS LAST"
    )
    symbols: list[str] = [r["symbol"] for r in rows if r["symbol"]]
    if not symbols:
        logger.warning("intraday_autofill: no IN symbols to sweep")
        return
    logger.warning(
        "intraday_autofill: STARTED — %d symbols", len(symbols),
    )

    # Determine if this is the first autofill tick after market open.
    # Cron minute offsets (:4,:14,:24,:34,:44,:54) mean the first
    # post-open fire lands at 09:24 IST.
    today_ist = datetime.now(_IST).date()
    h, m = _ist_hour_minute()
    # (Legacy flag retained as `_` for clarity; we now fetch 1-min
    # bars on every sweep so an explicit opening sweep is unneeded.)
    _ = (h == 9 and m <= 30)

    # Yesterday-catchup gate: read coverage from DB before touching
    # Upstox. If yesterday had a decent live pass we skip the extra
    # network work entirely.
    from datetime import timedelta
    yesterday_ist = today_ist - timedelta(days=1)
    try_yesterday_catchup = False
    try:
        yrow = await pool.fetchrow(
            "SELECT COUNT(DISTINCT symbol) AS sym_count "
            "FROM discover_stock_intraday "
            "WHERE ts::date = $1",
            yesterday_ist,
        )
        yesterday_coverage = (yrow["sym_count"] or 0) if yrow else 0
        # Threshold: 80% of IN universe. Only run catchup if both:
        #   - coverage is low (< 80%)
        #   - yesterday was actually a trading day (not a holiday)
        target = int(0.80 * len(symbols))
        if yesterday_coverage < target and _is_trading_day_on(yesterday_ist):
            try_yesterday_catchup = True
            logger.warning(
                "intraday_autofill: yesterday coverage %d/%d (<80%%) — "
                "will attempt historical catchup",
                yesterday_coverage, len(symbols),
            )
    except Exception:
        logger.debug("intraday_autofill: yesterday-coverage probe failed", exc_info=True)

    # Pre-fetch previous-day close per symbol from
    # discover_stock_price_history. Used to compute percent_change
    # for the snapshot UPDATE below — without this the card shows a
    # stale point/percent change left over from the last successful
    # live-job tick (which for BALUFORGE-class names might be days
    # old). The query pulls the most recent close < today per symbol
    # in a single pass.
    prev_closes: dict[str, float] = {}
    try:
        pc_rows = await pool.fetch(
            """
            SELECT DISTINCT ON (symbol) symbol, close::float AS close
            FROM discover_stock_price_history
            WHERE trade_date < $1
              AND symbol = ANY($2::text[])
            ORDER BY symbol, trade_date DESC
            """,
            today_ist,
            symbols,
        )
        for r in pc_rows:
            if r["close"] is not None:
                prev_closes[r["symbol"]] = float(r["close"])
    except Exception:
        logger.warning(
            "intraday_autofill: previous-close prefetch failed — "
            "percent_change will be left unchanged",
            exc_info=True,
        )

    fetched_symbols = 0
    fetched_empty = 0
    rows_inserted = 0
    snapshots_updated = 0

    async with httpx.AsyncClient(
        timeout=_UPSTOX_PER_CALL_TIMEOUT_SEC,
        follow_redirects=True,
    ) as client:
        instr_map = await _load_upstox_instrument_map(client)
        if not instr_map:
            logger.error(
                "intraday_autofill: Upstox instrument master empty — aborting"
            )
            return

        sem = asyncio.Semaphore(_UPSTOX_CONCURRENCY)
        all_rows: list[tuple[str, datetime, float, int | None]] = []
        # Per-symbol latest tick for snapshot UPDATE. Keyed by symbol
        # so each fetch result overwrites any earlier (stale) bar.
        latest_tick: dict[str, tuple[datetime, float, int | None]] = {}
        rows_lock = asyncio.Lock()
        counters = {"ok": 0, "empty": 0}
        counters_lock = asyncio.Lock()

        async def _sweep_one(symbol: str) -> None:
            key = instr_map.get(symbol)
            if key is None:
                return
            async with sem:
                # Use 1-minute bars for every sweep. With 30-minute
                # bars, a stock that isn't in the NSE bulk feed (e.g.
                # BALUFORGE) sees its snapshot age to 30 minutes old
                # before the next 30m bar closes and autofill picks
                # it up. Minute bars cut that freshness lag to
                # ≤ 1 autofill period (10 min).
                #
                # Upstox historical-candle/intraday returns all of
                # today's completed 1-min bars (~60 per symbol this
                # early in the session, up to ~375 near close).
                # Dedup via ON CONFLICT DO NOTHING means the 2nd+
                # autofill tick is cheap — it re-fetches the same
                # series and only the minute-or-two of new bars
                # actually INSERT.
                pts = await _fetch_upstox_intraday_for_symbol(
                    client, key, interval="1minute",
                )
                await asyncio.sleep(_UPSTOX_INTER_CALL_DELAY_SEC)

            combined = pts
            if not combined:
                async with counters_lock:
                    counters["empty"] += 1
                return
            async with counters_lock:
                counters["ok"] += 1
            # Pick the newest bar — latest wall-clock timestamp wins
            # regardless of whether it came from 30m or 1m feeds.
            newest = max(combined, key=lambda r: r[0])
            async with rows_lock:
                for ts_utc, price, vol in combined:
                    all_rows.append((symbol, ts_utc, price, vol))
                latest_tick[symbol] = newest

        await asyncio.gather(
            *[_sweep_one(s) for s in symbols],
            return_exceptions=True,
        )
        fetched_symbols = counters["ok"]
        fetched_empty = counters["empty"]
        logger.warning(
            "intraday_autofill: today sweep done — ok=%d empty=%d rows_queued=%d",
            fetched_symbols, fetched_empty, len(all_rows),
        )

        # Yesterday-catchup pass — uses the historical-candle endpoint
        # (not intraday) because yesterday is already closed. Same
        # semaphore so it doesn't blow the Upstox rate budget.
        if try_yesterday_catchup:
            yfrom = yesterday_ist.isoformat()
            yto = yesterday_ist.isoformat()

            async def _sweep_yesterday(symbol: str) -> None:
                key = instr_map.get(symbol)
                if key is None:
                    return
                async with sem:
                    pts = await _fetch_upstox_candles_for_symbol(
                        client, key, yfrom, yto, interval="30minute",
                    )
                    await asyncio.sleep(_UPSTOX_INTER_CALL_DELAY_SEC)
                if not pts:
                    return
                async with rows_lock:
                    for ts_utc, price, vol in pts:
                        all_rows.append((symbol, ts_utc, price, vol))

            await asyncio.gather(
                *[_sweep_yesterday(s) for s in symbols],
                return_exceptions=True,
            )
            logger.warning(
                "intraday_autofill: yesterday catchup done — rows_queued=%d",
                len(all_rows),
            )

    # Bulk upsert ticks. ON CONFLICT DO NOTHING so we never overwrite
    # an existing tick (the live NSE bulk feed, if it already wrote
    # this minute, is more authoritative than Upstox's 30-min close).
    if all_rows:
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(
                        """
                        INSERT INTO discover_stock_intraday
                            (symbol, ts, price, volume, percent_change)
                        VALUES ($1, $2, $3, $4, NULL)
                        ON CONFLICT (symbol, ts) DO NOTHING
                        """,
                        all_rows,
                    )
            rows_inserted = len(all_rows)
        except Exception:
            logger.exception("intraday_autofill: bulk INSERT failed")

    # Bulk-UPDATE discover_stock_snapshots so the card/list shows the
    # live price and real percent_change for autofilled symbols. The
    # app reads last_price/percent_change from this row, NOT from the
    # ticks table — without this block, BALUFORGE-class stocks show
    # a stale last close from the previous daily rescore even though
    # the ticks table is fresh.
    #
    # Guard: only update when our new tick is newer than the existing
    # source_timestamp. This is what keeps NSE bulk data (which is
    # usually more recent and more granular) from being clobbered by
    # our 30-min Upstox close — if the live job just ran 30 seconds
    # ago with a 09:45:12 source_timestamp, our 09:30:00 bar won't
    # overwrite it.
    update_rows: list[tuple] = []
    for symbol, (ts_utc, price, vol) in latest_tick.items():
        prev = prev_closes.get(symbol)
        if prev is None or prev <= 0:
            pct = None
            pt_chg = None
        else:
            pct = round(((price - prev) / prev) * 100, 4)
            pt_chg = round(price - prev, 4)
        update_rows.append((symbol, price, pt_chg, pct, vol, ts_utc))

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
                            source_timestamp = $6,
                            ingested_at = NOW()
                        WHERE symbol = $1
                          AND (source_timestamp IS NULL OR source_timestamp < $6)
                        """,
                        update_rows,
                    )
            snapshots_updated = len(update_rows)
        except Exception:
            logger.exception("intraday_autofill: snapshot UPDATE failed")

    # UPSERT today's row into discover_stock_price_history so the
    # daily-history table always has an "as-of-now" close for today,
    # not just the EOD job's 16:00 IST first-write. Every consumer
    # of the history table (chart endpoint, scoring, peer card,
    # chat context) then sees today without read-path patches.
    #
    # Conflict handling:
    #   - source='intraday_snapshot' marks our placeholder rows.
    #   - The EOD discover_stock_price job writes source='yahoo'
    #     and uses ON CONFLICT DO UPDATE so it always replaces our
    #     placeholder with the canonical close at 16:00 IST.
    #   - Our WHERE guard refuses to overwrite a 'yahoo' row, so
    #     a late autofill tick (last cron at 15:54 IST) can't
    #     clobber an EOD row that just landed.
    history_rows: list[tuple] = []
    today_ist = datetime.now(_IST).date()
    for symbol, (ts_utc, price, vol) in latest_tick.items():
        history_rows.append((symbol, today_ist, price, vol))

    history_upserted = 0
    if history_rows:
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.executemany(
                        """
                        INSERT INTO discover_stock_price_history
                            (symbol, trade_date, close, volume, source)
                        VALUES ($1, $2, $3, $4, 'intraday_snapshot')
                        ON CONFLICT (symbol, trade_date) DO UPDATE
                        SET close = EXCLUDED.close,
                            volume = COALESCE(EXCLUDED.volume, discover_stock_price_history.volume),
                            source = EXCLUDED.source
                        WHERE discover_stock_price_history.source != 'yahoo'
                        """,
                        history_rows,
                    )
            history_upserted = len(history_rows)
        except Exception:
            logger.exception("intraday_autofill: history UPSERT failed")

    elapsed = time.monotonic() - t0
    logger.warning(
        "intraday_autofill: DONE in %.1fs — symbols_ok=%d empty=%d "
        "rows_queued=%d snapshots_updated=%d history_upserted=%d",
        elapsed, fetched_symbols, fetched_empty, rows_inserted,
        snapshots_updated, history_upserted,
    )


def _is_trading_day_on(day) -> bool:
    """True if the given date is an Indian trading day."""
    from datetime import datetime as _dt, time as _t, timezone as _tz
    try:
        dt_noon = _dt.combine(day, _t(12, 0), tzinfo=_tz.utc)
        return is_trading_day_markets(dt_noon)
    except Exception:
        return False
