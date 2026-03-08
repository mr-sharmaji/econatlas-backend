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
    return {
        "market_seconds": getattr(settings, "market_interval_seconds", None),
        "market_minutes": getattr(settings, "market_interval_minutes", 1),
        "commodity_seconds": getattr(settings, "commodity_interval_seconds", None),
        "commodity_minutes": getattr(settings, "commodity_interval_minutes", 1),
        "macro_minutes": getattr(settings, "macro_interval_minutes", 1),
        "news_minutes": getattr(settings, "news_interval_minutes", 30),
    }


async def _run_market() -> None:
    from app.scheduler.market_job import run_market_job
    await run_market_job()


async def _run_commodity() -> None:
    from app.scheduler.commodity_job import run_commodity_job
    await run_commodity_job()


async def _run_macro() -> None:
    from app.scheduler.macro_job import run_macro_job
    await run_macro_job()


async def _run_news() -> None:
    from app.scheduler.news_job import run_news_job
    await run_news_job()


async def _startup_collection() -> None:
    """Run all jobs once at startup (market, commodity, macro first; news last)."""
    logger.info("Running startup data collection...")
    await _run_market()
    await _run_commodity()
    await _run_macro()
    await _run_news()
    logger.info("Startup data collection complete.")


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return

    intervals = _get_intervals()
    _scheduler = AsyncIOScheduler(timezone="UTC")

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

    _scheduler.add_job(_run_macro, "interval", minutes=intervals["macro_minutes"], id="macro", replace_existing=True)
    _scheduler.add_job(_run_news, "interval", minutes=intervals["news_minutes"], id="news", replace_existing=True)
    logger.info("Scheduler: macro=%dm news=%dm", intervals["macro_minutes"], intervals["news_minutes"])
    _scheduler.start()

    async def _deferred_startup() -> None:
        await asyncio.sleep(2)
        await _startup_collection()

    asyncio.create_task(_deferred_startup())


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Scheduler stopped.")
