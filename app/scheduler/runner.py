"""Background scheduler that enqueues data-collection jobs into ARQ via Redis."""
from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import get_settings
from app.queue.redis_pool import get_redis_pool
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
    """Enqueue a named job into ARQ.  Uses job_name as default dedup key."""
    try:
        pool = await get_redis_pool()
        await pool.enqueue_job(job_name, _job_id=job_id or job_name)
        logger.debug("Scheduler tick: enqueued %s", job_name)
    except Exception:
        logger.exception("Failed to enqueue job %s", job_name)


async def _run_market() -> None:
    await _enqueue("market")


async def _run_commodity() -> None:
    await _enqueue("commodity")


async def _run_crypto() -> None:
    await _enqueue("crypto")


async def _run_macro() -> None:
    await _enqueue("macro")


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


async def _run_discover_stock_price() -> None:
    await _enqueue("discover_stock_price")


async def _run_discover_mf_nav() -> None:
    await _enqueue("discover_mf_nav")


async def _run_ipo() -> None:
    await _enqueue("ipo")


async def _run_tax() -> None:
    await _enqueue("tax")


# ── Startup collection ───────────────────────────────────────────────


async def _startup_collection() -> None:
    """Enqueue all jobs once at startup so the ARQ worker processes them."""
    logger.info("Enqueuing startup data collection...")
    startup_jobs = [
        "market", "commodity", "crypto", "brief",
        "discover_stock", "discover_mutual_funds",
        "ipo", "macro", "tax", "news",
    ]
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
    _scheduler.add_job(_run_news, "interval", minutes=intervals["news_minutes"], id="news", replace_existing=True)
    if intervals["tax_enabled"]:
        _scheduler.add_job(_run_tax, "interval", minutes=intervals["tax_minutes"], id="tax", replace_existing=True)
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

    asyncio.create_task(_deferred_startup())


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        shutdown_job_executors()
        logger.info("Scheduler stopped.")
