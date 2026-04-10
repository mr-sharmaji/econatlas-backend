"""ARQ task wrappers — one per scheduled job.

Each task delegates to the existing ``run_*_job()`` function and adds
retry / dead-letter logic on top.  The underlying job code is unchanged.
"""
from __future__ import annotations

import logging
import traceback

from arq import Retry

from app.queue.dlq import write_dead_letter
from app.queue.settings import JOB_RETRY_POLICIES

logger = logging.getLogger(__name__)


async def _release_job_lock(job_id: str | None) -> None:
    """Release the Redis ``job_lock:{job_id}`` key set by the scheduler.

    The scheduler's ``_enqueue()`` sets this lock with a 300s TTL to dedup
    enqueues.  If we don't release it here, high-frequency jobs (e.g.
    notification_check every 30s) get blocked for ~5 minutes after every
    restart or completed run — causing missed market-open notifications.
    """
    if not job_id:
        return
    try:
        from app.queue.redis_pool import get_redis_pool
        pool = await get_redis_pool()
        await pool.delete(f"job_lock:{job_id}")
    except Exception:
        logger.debug("Failed to release job_lock:%s (non-fatal)", job_id, exc_info=True)


async def _run_with_retry(ctx: dict, job_name: str, coro_factory) -> None:  # noqa: ANN001
    """Execute *coro_factory()*, retry on failure or send to DLQ.

    Always releases the scheduler's ``job_lock:{job_id}`` key on exit, so
    the next scheduler tick can enqueue immediately instead of waiting for
    the 300s TTL to expire.
    """
    max_retries, delay_base = JOB_RETRY_POLICIES.get(job_name, (3, 10))
    job_try: int = ctx.get("job_try", 1)
    job_id = ctx.get("job_id")
    release_lock = True  # Release on success or permanent failure, not on Retry

    try:
        await coro_factory()
        # Invalidate response cache after successful job so users see fresh data
        try:
            from app.core.cache import invalidate_cache
            await invalidate_cache()
        except Exception:
            logger.debug("Cache invalidation after %s failed (non-fatal)", job_name)
    except Exception as exc:
        if max_retries > 0 and job_try <= max_retries:
            delay = delay_base * (2 ** (job_try - 1))
            logger.warning(
                "%s failed (attempt %d/%d), retrying in %ds: %s",
                job_name,
                job_try,
                max_retries + 1,
                delay,
                exc,
            )
            # Keep the lock held so the scheduler doesn't double-enqueue
            # while we wait for the retry backoff to fire.
            release_lock = False
            raise Retry(defer=delay) from exc

        logger.error(
            "%s exhausted retries (%d attempts), sending to DLQ: %s",
            job_name,
            job_try,
            exc,
        )
        await write_dead_letter(
            job_name=job_name,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
            retry_count=job_try,
        )
    finally:
        if release_lock:
            await _release_job_lock(job_id)


# ── Individual task entry-points ─────────────────────────────────────


async def task_market(ctx: dict) -> None:
    from app.scheduler.market_job import run_market_job

    await _run_with_retry(ctx, "market", run_market_job)


async def task_commodity(ctx: dict) -> None:
    from app.scheduler.commodity_job import run_commodity_job

    await _run_with_retry(ctx, "commodity", run_commodity_job)


async def task_crypto(ctx: dict) -> None:
    from app.scheduler.crypto_job import run_crypto_job

    await _run_with_retry(ctx, "crypto", run_crypto_job)


async def task_brief(ctx: dict) -> None:
    from app.scheduler.brief_job import run_brief_job

    await _run_with_retry(ctx, "brief", run_brief_job)


async def task_macro(ctx: dict) -> None:
    from app.scheduler.macro_job import run_macro_job

    await _run_with_retry(ctx, "macro", run_macro_job)


async def task_news(ctx: dict) -> None:
    from app.scheduler.news_job import run_news_job

    await _run_with_retry(ctx, "news", run_news_job)


async def task_discover_stock(ctx: dict) -> None:
    from app.scheduler.discover_stock_job import run_discover_stock_job

    await _run_with_retry(ctx, "discover_stock", run_discover_stock_job)


async def task_discover_mutual_funds(ctx: dict) -> None:
    from app.scheduler.discover_mutual_fund_job import run_discover_mutual_fund_job

    await _run_with_retry(ctx, "discover_mutual_funds", run_discover_mutual_fund_job)


async def task_discover_stock_price(ctx: dict) -> None:
    from app.scheduler.discover_stock_price_job import run_discover_stock_price_job

    await _run_with_retry(ctx, "discover_stock_price", run_discover_stock_price_job)


async def task_discover_mf_nav(ctx: dict) -> None:
    from app.scheduler.discover_mf_nav_job import run_discover_mf_nav_job

    await _run_with_retry(ctx, "discover_mf_nav", run_discover_mf_nav_job)


async def task_discover_mf_holdings(ctx: dict) -> None:
    from app.scheduler.discover_mf_holdings_job import run_discover_mf_holdings_job

    await _run_with_retry(ctx, "discover_mf_holdings", run_discover_mf_holdings_job)


async def task_rescore_stock(ctx: dict) -> None:
    from app.scheduler.discover_stock_job import rescore_discover_stocks

    await _run_with_retry(ctx, "rescore_stock", rescore_discover_stocks)


async def task_rescore_mf(ctx: dict) -> None:
    from app.scheduler.discover_mutual_fund_job import rescore_discover_mutual_funds

    await _run_with_retry(ctx, "rescore_mf", rescore_discover_mutual_funds)


async def task_ipo(ctx: dict) -> None:
    from app.services import ipo_service

    await _run_with_retry(ctx, "ipo", lambda: ipo_service.sync_ipo_cache(force=False))


async def task_imf_forecast(ctx: dict) -> None:
    from app.scheduler.imf_weo_scraper import run_imf_forecast_job

    await _run_with_retry(ctx, "imf_forecast", run_imf_forecast_job)


async def task_econ_calendar(ctx: dict) -> None:
    from app.scheduler.econ_calendar import run_econ_calendar_job

    await _run_with_retry(ctx, "econ_calendar", run_econ_calendar_job)


async def task_tax(ctx: dict) -> None:
    from app.core.config import get_settings
    from app.scheduler.tax_job import run_tax_job

    settings = get_settings()

    await _run_with_retry(
        ctx,
        "tax",
        lambda: run_tax_job(timeout_seconds=settings.tax_sync_timeout_seconds),
    )


async def task_market_score(ctx: dict) -> None:
    from app.scheduler.market_score_job import run_market_score_job

    await _run_with_retry(ctx, "market_score", run_market_score_job)


async def task_fertilizer(ctx: dict) -> None:
    from app.scheduler.commodity_job import run_fertilizer_job

    await _run_with_retry(ctx, "fertilizer", run_fertilizer_job)


async def task_notification_check(ctx: dict) -> None:
    from app.scheduler.notification_job import run_notification_job

    await _run_with_retry(ctx, "notification_check", run_notification_job)


async def task_ipo_notification(ctx: dict) -> None:
    from app.scheduler.ipo_notification_job import run_ipo_notification_job

    await _run_with_retry(ctx, "ipo_notification", run_ipo_notification_job)


async def task_gap_backfill(ctx: dict) -> None:
    from app.scheduler.gap_backfill_job import run_gap_backfill_job

    await _run_with_retry(ctx, "gap_backfill", run_gap_backfill_job)


async def task_news_embed(ctx: dict) -> None:
    from app.scheduler.news_embed_job import run_news_embed_job

    await _run_with_retry(ctx, "news_embed", run_news_embed_job)
