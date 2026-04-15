"""Daily job to update mutual fund NAV history for the last 7 days.

Fetches recent NAV data from mfapi.in and upserts into
discover_mf_nav_history. Lightweight — only appends new data via
INSERT ON CONFLICT DO NOTHING.

Runs after the main discover_mutual_fund_job (~10:30 PM IST weekdays).

Uses bounded parallelism: _MAX_CONCURRENCY scheme fetches run
concurrently in a dedicated thread pool (because mfapi.in is hit via
the blocking `requests` library), coordinated by an asyncio.Semaphore
on the coroutine side. Each worker spaces its own requests by
_PER_WORKER_SPACING_SECONDS so the aggregate request rate stays
bounded. Prior to this, the job was strictly serial with a 1 s
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
MAX_RETRIES = 3

# Bounded-parallelism tuning. Aggregate req/sec ≈
# _MAX_CONCURRENCY / _PER_WORKER_SPACING_SECONDS, i.e. ~16 req/sec
# with these defaults. mfapi.in has no published rate limit but
# hobby-tier free public APIs typically tolerate 10–30 req/sec
# before 429. _fetch_mf_nav_7d has retry-on-429 so if we do hit a
# cap the job self-throttles.
_MAX_CONCURRENCY = 8
_PER_WORKER_SPACING_SECONDS = 0.5

INSERT_SQL = """
INSERT INTO discover_mf_nav_history (scheme_code, nav_date, nav, source)
VALUES ($1, $2, $3, 'mfapi')
ON CONFLICT (scheme_code, nav_date) DO NOTHING
"""


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
    """Fetch last 7 days of NAV for all discover mutual funds and upsert.

    Parallelized: _MAX_CONCURRENCY coroutines run concurrently,
    each dispatching blocking mfapi.in fetches to a dedicated
    thread pool (same size) and then persisting the resulting
    rows via asyncpg. Per-worker spacing provides the rate limit.
    See module docstring for rationale + tuning.
    """
    logger.debug("run_discover_mf_nav_job: entry, fetching scheme codes from snapshots")
    pool = await get_pool()
    codes = await pool.fetch(
        "SELECT DISTINCT scheme_code FROM discover_mutual_fund_snapshots ORDER BY scheme_code"
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
        "MF NAV daily update: %d scheme codes to process (concurrency=%d, spacing=%.2fs).",
        total, _MAX_CONCURRENCY, _PER_WORKER_SPACING_SECONDS,
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
                    await pool.executemany(INSERT_SQL, rows)
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
    logger.info(
        "MF NAV daily update complete: %d scheme codes, %d rows upserted, "
        "%d errors, %.1fs elapsed.",
        total, counters["inserted"], counters["errors"], elapsed,
    )

    # Silent-success detector: if the job processed the full universe
    # but upserted far fewer rows than expected, the upstream mfapi data
    # was stale when we called it (ON CONFLICT DO NOTHING swallows the
    # "nothing new" case). A healthy weekday run hits ~(total * 7)
    # upserts before de-dup, so anything <10% of that for a >0 total
    # run is almost certainly a stale-source problem, not a code bug.
    # This is the line that would have flagged Apr 15's 5-row run in
    # real time instead of us finding it via SQL archaeology a day
    # later. Kept as WARNING so it shows up in any min_level=WARNING
    # query to /ops/logs. See post-mortem in this session for context.
    if total > 100 and counters["inserted"] < (total * 0.10):
        logger.warning(
            "MF NAV: suspiciously thin upsert — %d rows across %d schemes "
            "(expected ≥ %d for a healthy weekday run). Likely stale "
            "upstream at mfapi.in (AMFI publishes ~23:00 IST; cron "
            "fires at 22:30 IST). Re-run after 01:00 IST or adjust "
            "discover_mf_daily_minute_ist.",
            counters["inserted"], total, int(total * 0.10),
        )
    if counters["errors"] > (total * 0.25):
        logger.warning(
            "MF NAV: high error rate — %d / %d schemes failed (%.1f%%). "
            "Check mfapi.in availability and recent debug logs for the "
            "failure pattern.",
            counters["errors"], total, 100.0 * counters["errors"] / max(total, 1),
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
                    await pool.executemany(INSERT_SQL, rows)
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
