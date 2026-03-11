"""Background scheduler that runs data collection jobs inside the FastAPI process."""
from __future__ import annotations

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _get_intervals() -> dict:
    settings = get_settings()
    intervals = {
        "market_seconds": getattr(settings, "market_interval_seconds", None),
        "market_minutes": getattr(settings, "market_interval_minutes", 1),
        "commodity_seconds": getattr(settings, "commodity_interval_seconds", None),
        "commodity_minutes": getattr(settings, "commodity_interval_minutes", 1),
        "brief_minutes": getattr(settings, "brief_interval_minutes", 5),
        "macro_minutes": getattr(settings, "macro_interval_minutes", 1),
        "news_minutes": getattr(settings, "news_interval_minutes", 30),
        "tax_enabled": getattr(settings, "tax_sync_enabled", True),
        "tax_minutes": getattr(settings, "tax_sync_interval_minutes", 1440),
    }
    logger.debug("Scheduler intervals resolved: %s", intervals)
    return intervals


async def _run_market() -> None:
    logger.debug("Scheduler tick: market job started")
    from app.scheduler.market_job import run_market_job
    await run_market_job()
    logger.debug("Scheduler tick: market job finished")


async def _run_commodity() -> None:
    logger.debug("Scheduler tick: commodity job started")
    from app.scheduler.commodity_job import run_commodity_job
    await run_commodity_job()
    logger.debug("Scheduler tick: commodity job finished")


async def _run_macro() -> None:
    logger.debug("Scheduler tick: macro job started")
    from app.scheduler.macro_job import run_macro_job
    await run_macro_job()
    logger.debug("Scheduler tick: macro job finished")


async def _run_news() -> None:
    logger.debug("Scheduler tick: news job started")
    from app.scheduler.news_job import run_news_job
    await run_news_job()
    logger.debug("Scheduler tick: news job finished")


async def _run_brief() -> None:
    logger.debug("Scheduler tick: brief stock job started")
    from app.scheduler.brief_job import run_brief_job
    await run_brief_job()
    logger.debug("Scheduler tick: brief stock job finished")


async def _run_tax() -> None:
    logger.debug("Scheduler tick: tax job started")
    from app.scheduler.tax_job import run_tax_job

    settings = get_settings()
    await run_tax_job(
        timeout_seconds=settings.tax_sync_timeout_seconds,
    )
    logger.debug("Scheduler tick: tax job finished")


async def _startup_collection() -> None:
    """Run all jobs once at startup (market, commodity, brief, macro, tax, then news)."""
    logger.info("Running startup data collection...")
    await _run_market()
    await _run_commodity()
    await _run_brief()
    await _run_macro()
    await _run_tax()
    await _run_news()
    logger.info("Startup data collection complete.")


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

    _scheduler.add_job(_run_brief, "interval", minutes=intervals["brief_minutes"], id="brief", replace_existing=True)
    _scheduler.add_job(_run_macro, "interval", minutes=intervals["macro_minutes"], id="macro", replace_existing=True)
    _scheduler.add_job(_run_news, "interval", minutes=intervals["news_minutes"], id="news", replace_existing=True)
    if intervals["tax_enabled"]:
        _scheduler.add_job(_run_tax, "interval", minutes=intervals["tax_minutes"], id="tax", replace_existing=True)
    logger.info(
        "Scheduler: brief=%dm macro=%dm news=%dm tax=%s",
        intervals["brief_minutes"],
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
        logger.info("Scheduler stopped.")
