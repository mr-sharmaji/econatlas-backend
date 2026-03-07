"""Background scheduler that runs data collection jobs inside the FastAPI process."""
from __future__ import annotations

import asyncio
import logging
import threading
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_scheduler: AsyncIOScheduler | None = None


def _get_intervals() -> dict[str, int]:
    settings = get_settings()
    return {
        "market": getattr(settings, "market_interval_minutes", 1),
        "commodity": getattr(settings, "commodity_interval_minutes", 1),
        "macro": getattr(settings, "macro_interval_minutes", 1),
        "news": getattr(settings, "news_interval_minutes", 30),
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


def _run_async_in_new_loop(coro) -> None:
    """Run an async coroutine in a new event loop (in a separate thread)."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro)
    finally:
        loop.close()


def _run_market_threaded() -> None:
    _run_async_in_new_loop(_run_market())


def _run_commodity_threaded() -> None:
    _run_async_in_new_loop(_run_commodity())


def _run_macro_threaded() -> None:
    _run_async_in_new_loop(_run_macro())


def _run_news_threaded() -> None:
    _run_async_in_new_loop(_run_news())


async def _run_market_scheduled() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_market_threaded)


async def _run_commodity_scheduled() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_commodity_threaded)


async def _run_macro_scheduled() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_macro_threaded)


async def _run_news_scheduled() -> None:
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_news_threaded)


def _startup_collection_threaded() -> None:
    time.sleep(2)
    _run_async_in_new_loop(_startup_collection())


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return

    intervals = _get_intervals()
    logger.info(
        "Starting scheduler — market=%dm commodity=%dm macro=%dm news=%dm",
        intervals["market"], intervals["commodity"],
        intervals["macro"], intervals["news"],
    )

    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(_run_market_scheduled, "interval", minutes=intervals["market"], id="market", replace_existing=True)
    _scheduler.add_job(_run_commodity_scheduled, "interval", minutes=intervals["commodity"], id="commodity", replace_existing=True)
    _scheduler.add_job(_run_macro_scheduled, "interval", minutes=intervals["macro"], id="macro", replace_existing=True)
    _scheduler.add_job(_run_news_scheduled, "interval", minutes=intervals["news"], id="news", replace_existing=True)
    _scheduler.start()

    thread = threading.Thread(target=_startup_collection_threaded, daemon=True)
    thread.start()


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Scheduler stopped.")
