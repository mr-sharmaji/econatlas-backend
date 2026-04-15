"""Daily job to update mutual fund NAV history for the last 7 days.

Two-phase update:

1. **AMFI NAVAll.txt first.** One HTTP call to portal.amfiindia.com
   returns the latest NAV for every scheme AMFI tracks (~8000 schemes
   on a normal trading day). This is the authoritative upstream and
   is usually fresh within minutes of the 23:00 IST AMFI publish. We
   parse the semicolon-delimited file, upsert every row with
   source='amfi', and use that as the primary signal for "did we get
   today's data".

2. **mfapi.in 7-day history fallback.** mfapi.in mirrors AMFI but runs
   a scheduled scrape on its own cadence, so it's sometimes 1-2 days
   stale. It does give us 7 trailing days per request though, which
   is useful for backfilling gaps left by previous job outages. We
   still call it after AMFI for every scheme — ON CONFLICT DO NOTHING
   means AMFI's fresh row wins and mfapi fills historical holes.

This two-phase design was added after the 2026-04-15 incident where
mfapi.in stopped serving Apr 15 data even though AMFI had published
it. mfapi-only builds are vulnerable to that specific kind of
upstream lag; AMFI-first builds are not.

Runs after the main discover_mutual_fund_job (~10:30 PM IST weekdays).

Uses bounded parallelism for the mfapi phase: _MAX_CONCURRENCY scheme
fetches run concurrently in a dedicated thread pool (because mfapi.in
is hit via the blocking `requests` library), coordinated by an
asyncio.Semaphore on the coroutine side. Each worker spaces its own
requests by _PER_WORKER_SPACING_SECONDS so the aggregate request rate
stays bounded. Prior to this, the job was strictly serial with a 1 s
rate-limit sleep per scheme and took ~37 minutes for ~2,200 schemes;
with concurrency=8 and spacing=0.5 s it finishes in ~5 minutes.
_fetch_mf_nav_7d already retries with exponential backoff on 429/5xx
so the API will auto-throttle us if we're being too aggressive.
"""
from __future__ import annotations

import asyncio
import logging
import time

from app.scheduler.job_executors import get_job_executor
from datetime import datetime, timedelta, timezone

import requests

from app.core.database import get_pool

logger = logging.getLogger(__name__)

MFAPI_URL = "https://api.mfapi.in/mf/{scheme_code}"
AMFI_NAVALL_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
MAX_RETRIES = 3

# Bounded-parallelism tuning. Aggregate req/sec ≈
# _MAX_CONCURRENCY / _PER_WORKER_SPACING_SECONDS, i.e. ~16 req/sec
# with these defaults. mfapi.in has no published rate limit but
# hobby-tier free public APIs typically tolerate 10–30 req/sec
# before 429. _fetch_mf_nav_7d has retry-on-429 so if we do hit a
# cap the job self-throttles.
_MAX_CONCURRENCY = 8
_PER_WORKER_SPACING_SECONDS = 0.5

# AMFI date format in NAVAll.txt: "15-Apr-2026"
_AMFI_DATE_FORMAT = "%d-%b-%Y"

# Parameterized so both AMFI and mfapi upserts go through the same path.
INSERT_SQL = """
INSERT INTO discover_mf_nav_history (scheme_code, nav_date, nav, source)
VALUES ($1, $2, $3, $4)
ON CONFLICT (scheme_code, nav_date) DO NOTHING
"""


def _fetch_amfi_navall_sync() -> list[tuple[int, "datetime.date", float]]:
    """Fetch AMFI's NAVAll.txt and return (scheme_code, nav_date, nav) tuples.

    Format (semicolon-delimited, with section headers and blank lines):

        Scheme Code;ISIN Div Payout/ ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

        Open Ended Schemes(Debt Scheme - Banking and PSU Fund)

        Aditya Birla Sun Life Mutual Fund

        119551;INF209KA12Z1;INF209KA13Z9;Scheme Name;104.7901;15-Apr-2026
        ...

    Rows that don't match the 6-column numeric layout are silently
    skipped (headers, category dividers, fund-house banners, blank
    lines). Rows with non-numeric scheme_code or NAV, or unparseable
    dates, are also skipped with a DEBUG log — we don't want a single
    malformed line to abort the whole ingest.

    The file is ~3-5 MB and returns ~8000 parseable rows on a typical
    trading day. One HTTP call covers the entire MF universe for the
    latest business day.
    """
    logger.debug("amfi: fetching NAVAll.txt url=%s", AMFI_NAVALL_URL)
    t0 = time.monotonic()
    try:
        resp = requests.get(AMFI_NAVALL_URL, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.warning("amfi: NAVAll.txt fetch failed: %r", exc)
        return []
    text = resp.text
    logger.debug(
        "amfi: NAVAll.txt fetched bytes=%d elapsed=%.2fs",
        len(text), time.monotonic() - t0,
    )

    rows: list[tuple[int, "datetime.date", float]] = []
    lines = text.splitlines()
    dropped_format = 0
    dropped_code = 0
    dropped_nav = 0
    dropped_date = 0
    date_histogram: dict[str, int] = {}

    for raw in lines:
        line = raw.strip()
        if not line or ";" not in line:
            continue
        parts = line.split(";")
        if len(parts) < 6:
            dropped_format += 1
            continue
        # Column 0 = scheme_code, column 4 = NAV, column 5 = date.
        scheme_code_s = parts[0].strip()
        nav_s = parts[4].strip()
        date_s = parts[5].strip()
        if not scheme_code_s or not scheme_code_s.isdigit():
            # Header row and section dividers end up here — silent skip.
            dropped_code += 1
            continue
        try:
            scheme_code = int(scheme_code_s)
        except ValueError:
            dropped_code += 1
            continue
        try:
            nav_value = float(nav_s)
        except ValueError:
            dropped_nav += 1
            continue
        try:
            nav_date = datetime.strptime(date_s, _AMFI_DATE_FORMAT).date()
        except ValueError:
            dropped_date += 1
            continue
        rows.append((scheme_code, nav_date, nav_value))
        date_histogram[date_s] = date_histogram.get(date_s, 0) + 1

    logger.info(
        "amfi: parsed NAVAll.txt — kept=%d rows across %d distinct dates, "
        "dropped_format=%d dropped_code=%d dropped_nav=%d dropped_date=%d",
        len(rows), len(date_histogram),
        dropped_format, dropped_code, dropped_nav, dropped_date,
    )
    # Log the date histogram so stale-upstream states (like 2026-04-15
    # where most schemes were still on Apr 13) are visible at a glance.
    top_dates = sorted(date_histogram.items(), key=lambda kv: -kv[1])[:5]
    logger.debug("amfi: date histogram top-5 %s", top_dates)
    return rows


async def _ingest_amfi_rows(pool) -> tuple[int, int]:  # noqa: ANN001
    """Fetch AMFI's NAVAll.txt, filter to the snapshot universe, and
    upsert every row. Returns ``(fetched, actually_new)`` where
    ``actually_new`` is computed via BIGSERIAL id delta on
    discover_mf_nav_history — asyncpg's executemany doesn't return
    affected-row counts for ON CONFLICT DO NOTHING so we can't use
    len(rows) as the signal for "did we get anything new."

    The filter matters: NAVAll.txt contains ~14k schemes including
    long-dead funds from 2018 and earlier that our snapshots table
    doesn't track. Writing history for funds we never surface in the
    UI is dead weight and skews the silent-success guard's row-count
    math, so we drop anything not in discover_mutual_fund_snapshots."""
    loop = asyncio.get_event_loop()
    executor = get_job_executor("discover-mf-nav", max_workers=_MAX_CONCURRENCY)
    logger.debug("amfi: dispatching NAVAll.txt fetch to executor")
    amfi_rows = await loop.run_in_executor(executor, _fetch_amfi_navall_sync)
    if not amfi_rows:
        logger.warning(
            "amfi: NAVAll.txt returned zero rows — skipping AMFI phase entirely. "
            "mfapi fallback will still run."
        )
        return 0, 0

    # Filter to the same user-visible Direct Growth universe the mfapi
    # phase uses. Writing NAV history for IDCW variants or Income-
    # category FMPs pollutes the history table with rows that no user-
    # facing query ever reads. The 14-day nav_date filter keeps the
    # universe in sync with list/search/peer queries in discover_service.
    universe_rows = await pool.fetch(
        """
        SELECT scheme_code FROM discover_mutual_fund_snapshots
        WHERE LOWER(COALESCE(plan_type, 'direct')) = 'direct'
          AND COALESCE(option_type, '') = 'Growth'
          AND COALESCE(category, '') != 'Income'
          AND nav_date >= CURRENT_DATE - INTERVAL '14 days'
        """
    )
    universe: set[int] = {int(r["scheme_code"]) for r in universe_rows}
    logger.debug(
        "amfi: universe filter — %d Direct Growth schemes in snapshots",
        len(universe),
    )
    filtered = [(c, d, n) for (c, d, n) in amfi_rows if c in universe]
    dropped_not_in_universe = len(amfi_rows) - len(filtered)
    logger.info(
        "amfi: filtered to universe — kept=%d, dropped_not_in_universe=%d",
        len(filtered), dropped_not_in_universe,
    )
    if not filtered:
        logger.warning(
            "amfi: no NAVAll.txt rows intersect the snapshot universe — "
            "possible mismatch between scheme_codes in snapshots and AMFI."
        )
        return 0, 0

    # Count actually-new rows via ingested_at timestamp delta. We
    # originally tried MAX(id) but discover_mf_nav_history.id is a
    # UUID (gen_random_uuid() default), and Postgres has no MAX(uuid)
    # function. ingested_at defaults to NOW() and is monotonic per row,
    # so any row with ingested_at >= our pre-upsert wall-clock is a
    # row we just inserted (ON CONFLICT DO NOTHING means existing
    # rows aren't touched, so they retain their old ingested_at).
    from datetime import datetime as _dt, timezone as _tz
    t_pre = _dt.now(tz=_tz.utc)
    async with pool.acquire() as conn:
        logger.debug("amfi: pre-ingest snapshot at %s", t_pre.isoformat())
        # scheme_code column is TEXT in discover_mf_nav_history.
        # asyncpg's executemany doesn't auto-coerce int → text and
        # will raise DataError on the first int bind. Cast at the
        # boundary so the int-typed parser output (consistent with
        # mfapi) flows through to the text-typed column.
        payload = [(str(c), d, n, "amfi") for (c, d, n) in filtered]
        await conn.executemany(INSERT_SQL, payload)
        actually_new = await conn.fetchval(
            "SELECT COUNT(*) FROM discover_mf_nav_history WHERE ingested_at >= $1",
            t_pre,
        )
    actually_new = int(actually_new or 0)
    logger.info(
        "amfi: upsert complete — fetched=%d kept=%d actually_new=%d "
        "(%.1f%% new; the rest were already in DB)",
        len(amfi_rows), len(filtered), actually_new,
        100.0 * actually_new / max(len(filtered), 1),
    )
    return len(filtered), actually_new


def _fetch_mf_nav(
    scheme_code: int, cutoff_days: int | None = 7,
) -> list[tuple[int, datetime, float]]:
    """Fetch NAV history from mfapi.in.

    `cutoff_days`:
      - 7 (default) — keep only last 7 days, used by the daily append
      - None — keep ALL rows mfapi returns, used by the full backfill
        (mfapi typically serves 15+ years per scheme, some as far back
        as fund inception in the late 1990s)
    """
    url = MFAPI_URL.format(scheme_code=scheme_code)
    logger.debug(
        "mfapi fetch start scheme=%s cutoff_days=%s url=%s",
        scheme_code, cutoff_days, url,
    )
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=15)
        except requests.exceptions.RequestException as exc:
            wait = 2 ** attempt
            logger.debug(
                "mfapi network error scheme=%s attempt=%d/%d wait=%.1fs err=%r",
                scheme_code, attempt + 1, MAX_RETRIES, wait, exc,
            )
            time.sleep(wait)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            wait = (2 ** attempt) * 3
            logger.debug(
                "mfapi retryable status scheme=%s status=%d attempt=%d/%d wait=%.1fs",
                scheme_code, resp.status_code, attempt + 1, MAX_RETRIES, wait,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        logger.debug(
            "mfapi retries exhausted scheme=%s — returning empty",
            scheme_code,
        )
        return []

    payload = resp.json()
    cutoff = None
    if cutoff_days is not None:
        cutoff = datetime.now(tz=timezone.utc).date() - timedelta(days=cutoff_days)
    nav_entries = payload.get("data", [])
    logger.debug(
        "mfapi response scheme=%s status=%d raw_entries=%d cutoff=%s",
        scheme_code, resp.status_code, len(nav_entries), cutoff,
    )

    rows: list[tuple[int, datetime, float]] = []
    dropped_date_parse = 0
    dropped_cutoff = 0
    dropped_nav_parse = 0
    for entry in nav_entries:
        try:
            nav_date = datetime.strptime(entry["date"], "%d-%m-%Y").date()
        except (ValueError, KeyError):
            dropped_date_parse += 1
            continue
        if cutoff is not None and nav_date < cutoff:
            dropped_cutoff += 1
            continue
        try:
            nav_value = float(entry["nav"])
        except (ValueError, KeyError):
            dropped_nav_parse += 1
            continue
        rows.append((scheme_code, nav_date, nav_value))

    logger.debug(
        "mfapi parsed scheme=%s kept=%d dropped_date_parse=%d "
        "dropped_cutoff=%d dropped_nav_parse=%d newest_date=%s",
        scheme_code, len(rows), dropped_date_parse, dropped_cutoff,
        dropped_nav_parse, max((r[1] for r in rows), default=None),
    )
    return rows


def _fetch_mf_nav_7d(scheme_code: int) -> list[tuple[int, datetime, float]]:
    return _fetch_mf_nav(scheme_code, cutoff_days=7)


def _fetch_mf_nav_backfill(scheme_code: int) -> list[tuple[int, datetime, float]]:
    """Fetch every NAV mfapi serves for this scheme, back to inception.
    ON CONFLICT DO NOTHING on the upsert means existing rows are
    never touched — only missing historical dates get filled."""
    return _fetch_mf_nav(scheme_code, cutoff_days=None)


async def run_discover_mf_nav_job() -> None:
    """Two-phase NAV history refresh.

    **Phase 1 — AMFI NAVAll.txt.** One HTTP call, returns latest NAV
    for the entire MF universe. Upserts every parseable row with
    source='amfi'. This is the authoritative, fresh source and the
    primary reason Apr 15 (and any future stale-mfapi episode) lands
    in the DB the same day AMFI publishes.

    **Phase 2 — mfapi.in 7-day history.** Per-scheme HTTP calls through
    a bounded-parallelism thread pool (see _MAX_CONCURRENCY,
    _PER_WORKER_SPACING_SECONDS). mfapi can lag AMFI by 1-2 days but
    gives 7 trailing days per call, so it's still useful for filling
    historical gaps that the single-day AMFI phase can't cover.

    See module docstring for the full rationale — this design came
    out of the 2026-04-15 mfapi staleness incident.
    """
    logger.debug("run_discover_mf_nav_job: entry")
    pool = await get_pool()

    # ── Phase 1: AMFI NAVAll.txt ──────────────────────────────────
    t_amfi = time.monotonic()
    try:
        amfi_fetched, amfi_new = await _ingest_amfi_rows(pool)
    except Exception:
        logger.exception(
            "amfi: unexpected failure — continuing to mfapi fallback"
        )
        amfi_fetched, amfi_new = 0, 0
    logger.info(
        "MF NAV phase 1/2 (AMFI): %d rows fetched, %d actually new, %.1fs elapsed",
        amfi_fetched, amfi_new, time.monotonic() - t_amfi,
    )

    # ── Phase 2: mfapi.in 7-day history ───────────────────────────
    # Skip non-Direct-Growth + Income-category schemes at the source so
    # the mfapi phase doesn't waste ~6000 HTTP calls per run on funds
    # that will never appear in the UI. All user-facing discover
    # queries already exclude category='Income' (FMPs / Interval /
    # Fixed Term) and non-Direct-Growth variants, so refreshing their
    # NAVs is pure waste. Pipeline shrinks from ~6500 calls to ~1800.
    codes = await pool.fetch(
        """
        SELECT DISTINCT scheme_code
        FROM discover_mutual_fund_snapshots
        WHERE LOWER(COALESCE(plan_type, 'direct')) = 'direct'
          AND COALESCE(option_type, '') = 'Growth'
          AND COALESCE(category, '') != 'Income'
          AND nav_date >= CURRENT_DATE - INTERVAL '14 days'
        ORDER BY scheme_code
        """
    )
    code_list = [row["scheme_code"] for row in codes]
    total = len(code_list)
    logger.debug(
        "run_discover_mf_nav_job: fetched %d codes, first=%s last=%s",
        total,
        code_list[0] if code_list else None,
        code_list[-1] if code_list else None,
    )
    logger.info(
        "MF NAV phase 2/2 (mfapi): %d scheme codes to process (concurrency=%d, spacing=%.2fs).",
        total, _MAX_CONCURRENCY, _PER_WORKER_SPACING_SECONDS,
    )

    # Capture pre-phase-2 wall-clock so we can count actually-new rows
    # from the mfapi phase via ingested_at delta. (id is uuid — see
    # _ingest_amfi_rows for the rationale.)
    from datetime import datetime as _dt2, timezone as _tz2
    mfapi_t_pre = _dt2.now(tz=_tz2.utc)
    logger.debug(
        "run_discover_mf_nav_job: mfapi phase starting at %s",
        mfapi_t_pre.isoformat(),
    )

    # Size the thread pool to match asyncio concurrency so run_in_executor
    # calls don't queue behind a single worker. First call wins — safe
    # because this is the only caller of the "discover-mf-nav" executor.
    executor = get_job_executor("discover-mf-nav", max_workers=_MAX_CONCURRENCY)
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(_MAX_CONCURRENCY)

    # Counters mutated from concurrent coroutines. asyncio is cooperative
    # single-threaded so simple += is safe; the dict just groups them.
    counters = {"inserted": 0, "errors": 0, "processed": 0}
    t_start = time.monotonic()

    async def process_one(scheme_code: int) -> None:
        async with semaphore:
            logger.debug("process_one: acquired slot scheme=%s", scheme_code)
            try:
                rows = await loop.run_in_executor(
                    executor, _fetch_mf_nav_7d, scheme_code,
                )
                logger.debug(
                    "process_one: fetch returned scheme=%s rows=%d",
                    scheme_code, len(rows),
                )
                if rows:
                    # Attach 'mfapi' source + cast scheme_code to str
                    # (column is TEXT, see _ingest_amfi_rows note).
                    rows_with_source = [(str(c), d, n, "mfapi") for (c, d, n) in rows]
                    await pool.executemany(INSERT_SQL, rows_with_source)
                    counters["inserted"] += len(rows)
                    logger.debug(
                        "process_one: upsert done scheme=%s rows=%d (ON CONFLICT DO NOTHING, actual new rows may be fewer)",
                        scheme_code, len(rows),
                    )
                else:
                    logger.debug(
                        "process_one: empty result scheme=%s — no upsert",
                        scheme_code,
                    )
            except Exception:
                counters["errors"] += 1
                logger.exception("Error updating NAV for %s — skipping.", scheme_code)
            counters["processed"] += 1
            if counters["processed"] % 250 == 0:
                elapsed = time.monotonic() - t_start
                rate = counters["processed"] / elapsed if elapsed > 0 else 0.0
                eta = (total - counters["processed"]) / rate if rate > 0 else 0.0
                logger.info(
                    "MF NAV update progress: %d / %d scheme codes (%.1f/s, eta %.0fs).",
                    counters["processed"], total, rate, eta,
                )
            # Per-worker spacing: holds the semaphore slot long enough
            # that the aggregate request rate is bounded. With
            # concurrency=8 and spacing=0.5s the ceiling is ~16 req/s.
            await asyncio.sleep(_PER_WORKER_SPACING_SECONDS)

    tasks = [asyncio.create_task(process_one(code)) for code in code_list]
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.monotonic() - t_start
    # Compute actually-new rows from phase 2 via ingested_at delta.
    # `counters["inserted"]` is misleading (it's attempted-upsert count,
    # not actual inserts) so we report both for transparency but key
    # decisions off mfapi_new.
    async with pool.acquire() as _conn_post:
        mfapi_new_val = await _conn_post.fetchval(
            "SELECT COUNT(*) FROM discover_mf_nav_history "
            "WHERE ingested_at >= $1 AND source = 'mfapi'",
            mfapi_t_pre,
        )
    mfapi_new = int(mfapi_new_val or 0)
    total_new = amfi_new + mfapi_new
    logger.info(
        "MF NAV daily update complete: amfi_new=%d mfapi_attempted=%d "
        "mfapi_new=%d total_new=%d errors=%d elapsed=%.1fs",
        amfi_new, counters["inserted"], mfapi_new, total_new,
        counters["errors"], elapsed,
    )

    # Silent-success detector, now keyed off actually-new row count
    # across BOTH phases (AMFI + mfapi). Before the Apr 15 fix this
    # checked attempted-upsert count which wildly over-reported — the
    # job could look healthy while writing zero new rows. Now we
    # compare total_new (real DB inserts) against what a healthy
    # weekday run should produce: at minimum one new row per scheme
    # (today's NAV) from AMFI alone. A run that adds <10% of the
    # universe in new rows is almost certainly a stale-upstream
    # problem or a Saturday/Sunday/holiday, and deserves a WARNING.
    if total > 100 and total_new < (total * 0.10):
        logger.warning(
            "MF NAV: suspiciously thin ingest — only %d NEW rows across "
            "%d schemes (amfi_new=%d, mfapi_new=%d). Expected ≥ %d on a "
            "healthy trading day. Likely causes: (a) weekend/holiday — "
            "AMFI publishes few or no NAVs, (b) AMFI publish delayed, "
            "(c) both AMFI and mfapi stale at the same time (rare). "
            "Check AMFI portal and recent debug logs for the ingest "
            "histogram.",
            total_new, total, amfi_new, mfapi_new, int(total * 0.10),
        )
    if counters["errors"] > (total * 0.25):
        logger.warning(
            "MF NAV: high mfapi error rate — %d / %d schemes failed "
            "(%.1f%%). AMFI phase still provided %d new rows. Check "
            "mfapi.in availability and recent debug logs for the "
            "failure pattern.",
            counters["errors"], total,
            100.0 * counters["errors"] / max(total, 1),
            amfi_new,
        )


async def run_discover_mf_nav_backfill_job() -> None:
    """One-shot backfill: fetch every NAV mfapi.in serves for each
    scheme (back to fund inception) and upsert. Uses ON CONFLICT DO
    NOTHING so existing rows are untouched — only missing historical
    dates get filled. Same parallelism/spacing as the daily job so
    it's safe to run on top of normal traffic.

    Runs ~10-20 minutes for ~6k schemes at 16 req/s aggregate. Each
    mfapi response carries the scheme's entire history in a single
    payload (3000+ entries for older funds), so the network cost is
    the same as a 7-day fetch; DB write volume is higher on the
    first pass. Log progress every 250 schemes like the daily job.
    """
    pool = await get_pool()
    codes = await pool.fetch(
        "SELECT DISTINCT scheme_code FROM discover_mutual_fund_snapshots ORDER BY scheme_code"
    )
    code_list = [row["scheme_code"] for row in codes]
    total = len(code_list)
    logger.info(
        "MF NAV BACKFILL: %d scheme codes, window=inception (unbounded), "
        "concurrency=%d, spacing=%.2fs.",
        total, _MAX_CONCURRENCY, _PER_WORKER_SPACING_SECONDS,
    )

    executor = get_job_executor("discover-mf-nav", max_workers=_MAX_CONCURRENCY)
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(_MAX_CONCURRENCY)

    counters = {"inserted": 0, "errors": 0, "processed": 0}
    t_start = time.monotonic()

    async def process_one(scheme_code: int) -> None:
        async with semaphore:
            try:
                rows = await loop.run_in_executor(
                    executor, _fetch_mf_nav_backfill, scheme_code,
                )
                if rows:
                    # executemany preserves ON CONFLICT DO NOTHING, so
                    # existing good rows are not modified.
                    # scheme_code → str (column is TEXT).
                    rows_with_source = [(str(c), d, n, "mfapi") for (c, d, n) in rows]
                    await pool.executemany(INSERT_SQL, rows_with_source)
                    counters["inserted"] += len(rows)
            except Exception:
                counters["errors"] += 1
                logger.exception("MF NAV BACKFILL: error scheme=%s — skipping.", scheme_code)
            counters["processed"] += 1
            if counters["processed"] % 250 == 0:
                elapsed = time.monotonic() - t_start
                rate = counters["processed"] / elapsed if elapsed > 0 else 0.0
                eta = (total - counters["processed"]) / rate if rate > 0 else 0.0
                logger.info(
                    "MF NAV BACKFILL progress: %d / %d (%.1f/s, eta %.0fs), "
                    "rows_inserted_so_far=%d.",
                    counters["processed"], total, rate, eta,
                    counters["inserted"],
                )
            await asyncio.sleep(_PER_WORKER_SPACING_SECONDS)

    tasks = [asyncio.create_task(process_one(code)) for code in code_list]
    await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.monotonic() - t_start
    logger.info(
        "MF NAV BACKFILL complete: %d schemes, %d rows inserted, "
        "%d errors, %.1fs elapsed.",
        total, counters["inserted"], counters["errors"], elapsed,
    )
