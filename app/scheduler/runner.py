"""Background scheduler that enqueues data-collection jobs into ARQ via Redis."""
from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import get_settings
from app.queue.redis_pool import get_redis_pool
from app.queue.settings import canonicalize_job_id
from app.scheduler.job_executors import shutdown_job_executors

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _get_intervals() -> dict:
    settings = get_settings()
    intervals = {
        "market_seconds": getattr(settings, "market_interval_seconds", None),
        "market_minutes": getattr(settings, "market_interval_minutes", 1),
        "commodity_seconds": getattr(settings, "commodity_interval_seconds", None),
        "commodity_minutes": getattr(settings, "commodity_interval_minutes", 1),
        "crypto_seconds": getattr(settings, "crypto_interval_seconds", None),
        "crypto_minutes": getattr(settings, "crypto_interval_minutes", 1),
        "brief_minutes": getattr(settings, "brief_interval_minutes", 5),
        "discover_cron_enabled": getattr(settings, "discover_cron_enabled", False),
        "discover_stock_daily_hour_ist": getattr(settings, "discover_stock_daily_hour_ist", 16),
        "discover_stock_daily_minute_ist": getattr(settings, "discover_stock_daily_minute_ist", 0),
        "discover_stock_daily_days": getattr(settings, "discover_stock_daily_days", "mon-fri"),
        "discover_stock_retry_enabled": getattr(settings, "discover_stock_retry_enabled", True),
        "discover_stock_retry_hour_ist": getattr(settings, "discover_stock_retry_hour_ist", 16),
        "discover_stock_retry_minute_ist": getattr(settings, "discover_stock_retry_minute_ist", 20),
        "discover_mf_daily_hour_ist": getattr(settings, "discover_mf_daily_hour_ist", 22),
        "discover_mf_daily_minute_ist": getattr(settings, "discover_mf_daily_minute_ist", 0),
        "discover_mf_daily_days": getattr(settings, "discover_mf_daily_days", "mon-fri"),
        "ipo_minutes": getattr(settings, "ipo_interval_minutes", 5),
        "macro_minutes": getattr(settings, "macro_interval_minutes", 60),
        "news_minutes": getattr(settings, "news_interval_minutes", 30),
        "tax_enabled": getattr(settings, "tax_sync_enabled", True),
        "tax_minutes": getattr(settings, "tax_sync_interval_minutes", 1440),
    }
    logger.debug("Scheduler intervals resolved: %s", intervals)
    return intervals


# ── Enqueue helpers ──────────────────────────────────────────────────
# Each _run_* wrapper enqueues a named job into Redis.
# _job_id deduplication prevents queue buildup for high-frequency jobs:
# if a job with the same _job_id is already queued/running, the enqueue
# is silently skipped.


async def _enqueue(job_name: str, *, job_id: str | None = None) -> None:
    """Enqueue a named job into ARQ.  Uses job_name as default dedup key.

    Before enqueueing, checks a Redis lock key to prevent pileup when the
    same job is already running or queued.  The lock is released by the
    ARQ task wrapper (``_run_with_retry``) after the job completes.  The
    TTL is a safety net in case the worker hard-crashes.
    """
    effective_id = canonicalize_job_id(job_id or job_name) or job_name
    lock_key = f"job_lock:{effective_id}"
    try:
        pool = await get_redis_pool()
        # SET NX with TTL: only set if not already present.
        # TTL of 90s is a worker-crash safety net — under normal operation
        # the ARQ task wrapper releases the lock as soon as the job finishes.
        # 90s is long enough for slow jobs (e.g. notification_check can take
        # 10-20s on first run after restart) but short enough that the next
        # scheduled tick (typically 30s-60s) recovers quickly after a crash.
        acquired = await pool.set(lock_key, "1", nx=True, ex=90)
        if not acquired:
            logger.debug("Scheduler tick: skipping %s — already running/queued", effective_id)
            return
        await pool.enqueue_job(job_name, _job_id=effective_id)
        logger.debug("Scheduler tick: enqueued %s", job_name)
    except Exception:
        # On any error, release the lock so the next tick can try again.
        try:
            pool_for_cleanup = await get_redis_pool()
            await pool_for_cleanup.delete(lock_key)
        except Exception:
            pass
        logger.exception("Failed to enqueue job %s", job_name)


async def _run_market() -> None:
    await _enqueue("market")


async def _run_commodity() -> None:
    await _enqueue("commodity")


async def _run_crypto() -> None:
    await _enqueue("crypto")


async def _run_macro() -> None:
    await _enqueue("macro")


async def _run_imf_forecast() -> None:
    await _enqueue("imf_forecast")


async def _run_econ_calendar() -> None:
    await _enqueue("econ_calendar")


async def _run_news() -> None:
    await _enqueue("news")


async def _run_brief() -> None:
    await _enqueue("brief")


async def _run_discover_stock() -> None:
    await _enqueue("discover_stock")


async def _run_discover_stock_retry() -> None:
    await _enqueue("discover_stock", job_id="discover_stock_retry")


async def _run_discover_mutual_funds() -> None:
    await _enqueue("discover_mutual_funds")


async def _run_reconcile_stock_snapshots() -> None:
    await _enqueue("reconcile_stock_snapshots")


async def _run_db_maintenance() -> None:
    """Weekly REINDEX + VACUUM ANALYZE on high-write tables.

    Prevents index corruption from accumulating — concurrent market/
    commodity/crypto job writes can corrupt btree indexes over time
    (seen on market_prices, market_scores, market_prices_intraday).
    """
    from app.core.database import get_pool
    pool = await get_pool()
    tables = [
        "market_prices",
        "market_prices_intraday",
        "market_scores",
        "discover_stock_snapshots",
        "discover_stock_price_history",
        "discover_stock_intraday",
        "discover_mutual_fund_snapshots",
    ]
    for table in tables:
        try:
            # Remove exact duplicates first (they block REINDEX)
            if table == "market_scores":
                await pool.execute(f"""
                    DELETE FROM {table} WHERE ctid NOT IN (
                        SELECT MIN(ctid) FROM {table} GROUP BY asset, instrument_type
                    )
                """)
            elif table == "market_prices":
                await pool.execute(f"""
                    DELETE FROM {table} WHERE ctid NOT IN (
                        SELECT MIN(ctid) FROM {table}
                        GROUP BY asset, instrument_type, timestamp
                    )
                """)
            await pool.execute(f"REINDEX TABLE {table}")
            await pool.execute(f"VACUUM ANALYZE {table}")
            logger.info("DB maintenance: REINDEX + VACUUM OK for %s", table)
        except Exception as exc:
            logger.warning("DB maintenance: %s failed: %s", table, exc)


async def _run_discover_stock_price() -> None:
    await _enqueue("discover_stock_price")


async def _run_discover_stock_intraday() -> None:
    """Intraday live-price refresh (market-hours only).

    Fires every 30 minutes Mon-Fri 09:00-15:45 IST. The job itself also
    guards with a 15:55-16:45 IST exclusion window so the heavy daily
    pipeline (discover_stock → discover_stock_price → rescore) at 16:00
    is never touched by this job.
    """
    await _enqueue("discover_stock_intraday")


async def _run_discover_mf_nav() -> None:
    await _enqueue("discover_mf_nav")


async def _run_ipo() -> None:
    await _enqueue("ipo")


async def _run_tax() -> None:
    await _enqueue("tax")


async def _run_market_score() -> None:
    await _enqueue("market_score")


async def _run_fertilizer() -> None:
    await _enqueue("fertilizer")


async def _run_notification_check() -> None:
    await _enqueue("notification_check")


async def _run_ipo_notification() -> None:
    await _enqueue("ipo_notification")


async def _run_gap_backfill() -> None:
    await _enqueue("gap_backfill")


async def _run_broker_charges() -> None:
    await _enqueue("broker_charges")


# ── Startup collection ───────────────────────────────────────────────


async def _startup_collection() -> None:
    """Enqueue all jobs once at startup so the ARQ worker processes them.

    Also acts as the safety net for the stale-lock scenario: combined
    with ``clear_stale_arq_state`` at lifespan init, every backend
    restart re-runs the full data-collection suite even if APScheduler's
    misfire grace window has already expired for a missed cron. This is
    why deploys can't silently skip a day of data.
    """
    logger.info("Enqueuing startup data collection...")
    startup_jobs = [
        "market", "commodity", "crypto", "brief",
        "ipo", "macro", "tax", "news",
    ]
    settings = get_settings()
    if getattr(settings, "discover_cron_enabled", False):
        # discover_mf_nav is included in the startup safety net so a
        # SIGKILL during its 22:30 IST cron window doesn't strand a
        # day of NAV data. Its writer is idempotent (ON CONFLICT upsert),
        # so a second run on the same data is safe — at worst it's a
        # redundant upstream API call.
        #
        # Note: holdings data comes from Groww via the main
        # discover_mutual_funds job (filled_fields={'top_holdings': 1771, ...}
        # on each run), so there is no separate holdings job to add here.
        startup_jobs.extend([
            "discover_stock",
            "discover_mutual_funds",
            "discover_mf_nav",
        ])
    else:
        logger.info("Skipping discover jobs at startup (discover_cron_enabled=false)")
    for name in startup_jobs:
        await _enqueue(name, job_id=f"startup_{name}")
    logger.info("Startup data collection enqueued (%d jobs).", len(startup_jobs))


# ── Scheduler lifecycle ──────────────────────────────────────────────


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        logger.debug("Scheduler start ignored: already running")
        return

    intervals = _get_intervals()
    _scheduler = AsyncIOScheduler(timezone="UTC")
    logger.debug("Scheduler instance created with timezone=%s", _scheduler.timezone)

    if intervals["market_seconds"] and intervals["market_seconds"] > 0:
        _scheduler.add_job(_run_market, "interval", seconds=intervals["market_seconds"], id="market", replace_existing=True)
        logger.info("Scheduler: market every %ds (live accuracy)", intervals["market_seconds"])
    else:
        _scheduler.add_job(_run_market, "interval", minutes=intervals["market_minutes"], id="market", replace_existing=True)
        logger.info("Scheduler: market every %dm", intervals["market_minutes"])

    if intervals["commodity_seconds"] and intervals["commodity_seconds"] > 0:
        _scheduler.add_job(_run_commodity, "interval", seconds=intervals["commodity_seconds"], id="commodity", replace_existing=True)
        logger.info("Scheduler: commodity every %ds (live accuracy)", intervals["commodity_seconds"])
    else:
        _scheduler.add_job(_run_commodity, "interval", minutes=intervals["commodity_minutes"], id="commodity", replace_existing=True)
        logger.info("Scheduler: commodity every %dm", intervals["commodity_minutes"])

    if intervals["crypto_seconds"] and intervals["crypto_seconds"] > 0:
        _scheduler.add_job(_run_crypto, "interval", seconds=intervals["crypto_seconds"], id="crypto", replace_existing=True)
        logger.info("Scheduler: crypto every %ds (live accuracy)", intervals["crypto_seconds"])
    else:
        _scheduler.add_job(_run_crypto, "interval", minutes=intervals["crypto_minutes"], id="crypto", replace_existing=True)
        logger.info("Scheduler: crypto every %dm", intervals["crypto_minutes"])

    _scheduler.add_job(_run_brief, "interval", minutes=intervals["brief_minutes"], id="brief", replace_existing=True)
    if intervals["discover_cron_enabled"]:
        _scheduler.add_job(
            _run_discover_stock,
            "cron",
            day_of_week=intervals["discover_stock_daily_days"],
            hour=intervals["discover_stock_daily_hour_ist"],
            minute=intervals["discover_stock_daily_minute_ist"],
            timezone="Asia/Kolkata",
            id="discover_stock",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10800,
        )
        if intervals["discover_stock_retry_enabled"]:
            _scheduler.add_job(
                _run_discover_stock_retry,
                "cron",
                day_of_week=intervals["discover_stock_daily_days"],
                hour=intervals["discover_stock_retry_hour_ist"],
                minute=intervals["discover_stock_retry_minute_ist"],
                timezone="Asia/Kolkata",
                id="discover_stock_retry",
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=10800,
            )
        _scheduler.add_job(
            _run_discover_mutual_funds,
            "cron",
            day_of_week=intervals["discover_mf_daily_days"],
            hour=intervals["discover_mf_daily_hour_ist"],
            minute=intervals["discover_mf_daily_minute_ist"],
            timezone="Asia/Kolkata",
            id="discover_mutual_funds",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10800,
        )
        # Daily stock price history update (runs 30 min after stock snapshot job)
        _scheduler.add_job(
            _run_discover_stock_price,
            "cron",
            day_of_week=intervals["discover_stock_daily_days"],
            hour=intervals["discover_stock_daily_hour_ist"],
            minute=intervals["discover_stock_daily_minute_ist"] + 30,
            timezone="Asia/Kolkata",
            id="discover_stock_price",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10800,
        )
        # ── Reconciliation: fix stale snapshot prices after daily pipeline ──
        # Runs 65 min after main stock job (16:00 + 65 = 17:05 IST).
        # At this point both discover_stock (16:00) and
        # discover_stock_price (16:30) have finished. Any snapshot
        # still stale gets its price synced from price_history.
        _scheduler.add_job(
            _run_reconcile_stock_snapshots,
            "cron",
            day_of_week=intervals["discover_stock_daily_days"],
            hour=17,
            minute=5,
            timezone="Asia/Kolkata",
            id="reconcile_stock_snapshots",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
        # ── Intraday live-price refresh (Mon-Fri market hours) ──
        # Every 30 min between 09:00 and 15:45 IST. The job itself has
        # a 15:55-16:45 IST exclusion window so it can never race the
        # heavy daily pipeline that runs at 16:00 / 16:20 / 16:30 IST.
        # Lightweight: ONLY updates last_price / percent_change /
        # volume on discover_stock_snapshots — never touches scores,
        # fundamentals, or red flags.
        _scheduler.add_job(
            _run_discover_stock_intraday,
            "cron",
            day_of_week=intervals["discover_stock_daily_days"],
            hour="9-15",
            minute="0,30",
            timezone="Asia/Kolkata",
            id="discover_stock_intraday",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        # Pre-open tick at 09:00 IST + post-close tick at 15:45 IST are
        # already covered by the 9-15 cron (09:00 and 15:30), but we
        # add one extra slot at 15:45 so the close-print is captured
        # before the daily pipeline replaces it at 16:00.
        _scheduler.add_job(
            _run_discover_stock_intraday,
            "cron",
            day_of_week=intervals["discover_stock_daily_days"],
            hour=15,
            minute=45,
            timezone="Asia/Kolkata",
            id="discover_stock_intraday_close",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
        )
        # Daily MF NAV history update (runs 30 min after MF snapshot job)
        _scheduler.add_job(
            _run_discover_mf_nav,
            "cron",
            day_of_week=intervals["discover_mf_daily_days"],
            hour=intervals["discover_mf_daily_hour_ist"],
            minute=intervals["discover_mf_daily_minute_ist"] + 30,
            timezone="Asia/Kolkata",
            id="discover_mf_nav",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=10800,
        )
        logger.info(
            "Scheduler: discover cron ENABLED — stocks %s@%02d:%02d IST, MF %s@%02d:%02d IST",
            intervals["discover_stock_daily_days"],
            intervals["discover_stock_daily_hour_ist"],
            intervals["discover_stock_daily_minute_ist"],
            intervals["discover_mf_daily_days"],
            intervals["discover_mf_daily_hour_ist"],
            intervals["discover_mf_daily_minute_ist"],
        )
    else:
        logger.warning(
            "Scheduler: discover cron DISABLED — use /ops/jobs/trigger to run manually"
        )
    _scheduler.add_job(
        _run_ipo,
        "interval",
        minutes=intervals["ipo_minutes"],
        id="ipo",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    _scheduler.add_job(_run_macro, "interval", minutes=intervals["macro_minutes"], id="macro", replace_existing=True)
    _scheduler.add_job(_run_imf_forecast, "interval", hours=24, id="imf_forecast", replace_existing=True)
    _scheduler.add_job(_run_econ_calendar, "interval", hours=24, id="econ_calendar", replace_existing=True)
    _scheduler.add_job(_run_news, "interval", minutes=intervals["news_minutes"], id="news", replace_existing=True)
    if intervals["tax_enabled"]:
        _scheduler.add_job(_run_tax, "interval", minutes=intervals["tax_minutes"], id="tax", replace_existing=True)
    _scheduler.add_job(
        _run_market_score,
        "interval",
        hours=6,
        id="market_score",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    logger.info("Scheduler: market_score every 6h")
    _scheduler.add_job(
        _run_fertilizer,
        "interval",
        hours=6,
        id="fertilizer",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    logger.info("Scheduler: fertilizer every 6h")
    # Notification check runs alongside market job to detect open/close transitions
    if intervals["market_seconds"] and intervals["market_seconds"] > 0:
        _scheduler.add_job(_run_notification_check, "interval", seconds=intervals["market_seconds"], id="notification_check", replace_existing=True, max_instances=1, coalesce=True, misfire_grace_time=60)
        logger.info("Scheduler: notification_check every %ds", intervals["market_seconds"])
    else:
        _scheduler.add_job(_run_notification_check, "interval", minutes=intervals["market_minutes"], id="notification_check", replace_existing=True, max_instances=1, coalesce=True, misfire_grace_time=60)
        logger.info("Scheduler: notification_check every %dm", intervals["market_minutes"])
    # IPO notification check every 30 minutes
    _scheduler.add_job(
        _run_ipo_notification,
        "interval",
        minutes=30,
        id="ipo_notification",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
    )
    logger.info("Scheduler: ipo_notification every 30m")
    # Weekly DB maintenance: REINDEX + VACUUM on high-write tables
    # Sunday 3:00 AM IST — lowest traffic window
    _scheduler.add_job(
        _run_db_maintenance,
        "cron",
        day_of_week="sun",
        hour=3,
        minute=0,
        timezone="Asia/Kolkata",
        id="db_maintenance",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=7200,
    )
    logger.info("Scheduler: db_maintenance weekly Sun 3:00 AM IST")
    # Weekly broker charges scraper: Sunday 4:00 AM IST
    _scheduler.add_job(
        _run_broker_charges,
        "cron",
        day_of_week="sun",
        hour=4,
        minute=0,
        timezone="Asia/Kolkata",
        id="broker_charges",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=43200,
    )
    logger.info("Scheduler: broker_charges weekly Sun 4:00 AM IST")
    logger.info(
        "Scheduler: brief=%dm discover_stock=%s %02d:%02d IST retry=%s %02d:%02d IST discover_mf=%s %02d:%02d IST ipo=%dm macro=%dm news=%dm tax=%s",
        intervals["brief_minutes"],
        intervals["discover_stock_daily_days"],
        intervals["discover_stock_daily_hour_ist"],
        intervals["discover_stock_daily_minute_ist"],
        "on" if intervals["discover_stock_retry_enabled"] else "off",
        intervals["discover_stock_retry_hour_ist"],
        intervals["discover_stock_retry_minute_ist"],
        intervals["discover_mf_daily_days"],
        intervals["discover_mf_daily_hour_ist"],
        intervals["discover_mf_daily_minute_ist"],
        intervals["ipo_minutes"],
        intervals["macro_minutes"],
        intervals["news_minutes"],
        f"{intervals['tax_minutes']}m" if intervals["tax_enabled"] else "disabled",
    )
    _scheduler.start()
    logger.debug("Scheduler started with %d jobs", len(_scheduler.get_jobs()))

    async def _deferred_startup() -> None:
        logger.debug("Deferred startup data collection queued")
        await asyncio.sleep(2)
        await _startup_collection()

    async def _deferred_gap_backfill() -> None:
        logger.debug("Deferred gap backfill queued (30s delay)")
        await asyncio.sleep(30)
        await _run_gap_backfill()

    asyncio.create_task(_deferred_startup())
    asyncio.create_task(_deferred_gap_backfill())


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        shutdown_job_executors()
        logger.info("Scheduler stopped.")
