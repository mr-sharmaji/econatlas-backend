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

QUEUE_RETRY_LOG_MARKER = "queue-repr-exc-2026-04-16b"


async def _release_job_lock(job_id: str | None) -> None:
    """Release the Redis ``job_lock:{job_id}`` key set by the scheduler.

    The scheduler's ``_enqueue()`` sets this lock with a 300s TTL to dedup
    enqueues.  If we don't release it here, high-frequency jobs (e.g.
    notification_check every 30s) get blocked for ~5 minutes after every
    restart or completed run — causing missed market-open notifications.
    """
    if not job_id:
        logger.debug("_release_job_lock: no job_id, nothing to release")
        return
    try:
        from app.queue.redis_pool import get_redis_pool
        pool = await get_redis_pool()
        deleted = await pool.delete(f"job_lock:{job_id}")
        logger.debug(
            "_release_job_lock: deleted job_lock:%s (keys removed=%s)",
            job_id, deleted,
        )
    except Exception:
        logger.debug("Failed to release job_lock:%s (non-fatal)", job_id, exc_info=True)


async def _run_with_retry(ctx: dict, job_name: str, coro_factory) -> None:  # noqa: ANN001
    """Execute *coro_factory()*, retry on failure or send to DLQ.

    Always releases the scheduler's ``job_lock:{job_id}`` key on exit, so
    the next scheduler tick can enqueue immediately instead of waiting for
    the 300s TTL to expire.

    Also records Prometheus metrics so Grafana can chart job throughput,
    duration, success rate, and time-since-last-run per job:
      - jobs_started_total{job_name}
      - job_duration_seconds{job_name}  (histogram, observed on completion)
      - job_errors_total{job_name}      (incremented on permanent failure)
      - job_last_run_timestamp_seconds{job_name}
      - job_last_success_timestamp_seconds{job_name}
    """
    import time as _time

    from app.core.metrics import (
        JOB_DURATION,
        JOB_ERRORS,
        JOB_LAST_RUN_TIMESTAMP,
        JOB_LAST_SUCCESS_TIMESTAMP,
        JOBS_STARTED,
    )

    max_retries, delay_base = JOB_RETRY_POLICIES.get(job_name, (3, 10))
    job_try: int = ctx.get("job_try", 1)
    job_id = ctx.get("job_id")
    enqueue_time = ctx.get("enqueue_time")
    score = ctx.get("score")
    release_lock = True  # Release on success or permanent failure, not on Retry

    # Entry log with every piece of arq context that's worth seeing in
    # /ops/logs. `job_try` > 1 means we're on a retry. `score` is the
    # Redis sorted-set score arq uses for deferred execution — useful
    # for diagnosing "why didn't this fire at its scheduled time".
    queue_latency = None
    if enqueue_time is not None:
        try:
            queue_latency = _time.time() - enqueue_time.timestamp()
        except Exception:
            queue_latency = None
    logger.debug(
        "arq task enter: name=%s job_id=%s try=%d/%d max_retries=%d "
        "queue_latency=%s score=%s",
        job_name, job_id, job_try, job_try, max_retries,
        f"{queue_latency:.3f}s" if queue_latency is not None else "?",
        score,
    )

    # Only count the FIRST attempt as a started job — retries are part
    # of the same logical invocation and would otherwise double-count.
    if job_try == 1:
        JOBS_STARTED.labels(job_name=job_name).inc()
        logger.debug("arq task: first attempt, incremented JOBS_STARTED counter")
    else:
        logger.debug("arq task: retry attempt %d, skipping JOBS_STARTED increment", job_try)

    started_at = _time.monotonic()
    try:
        logger.debug("arq task: invoking %s coroutine", job_name)
        await coro_factory()
        logger.debug(
            "arq task: %s coroutine returned cleanly after %.2fs",
            job_name, _time.monotonic() - started_at,
        )
        # Invalidate response cache after successful job so users see fresh data
        try:
            from app.core.cache import invalidate_cache
            inv_result = await invalidate_cache()
            logger.debug(
                "arq task: cache invalidation after %s done (%s)",
                job_name, inv_result,
            )
        except Exception:
            logger.debug("Cache invalidation after %s failed (non-fatal)", job_name)
        # Record successful completion
        duration = _time.monotonic() - started_at
        JOB_DURATION.labels(job_name=job_name).observe(duration)
        now = _time.time()
        JOB_LAST_RUN_TIMESTAMP.labels(job_name=job_name).set(now)
        JOB_LAST_SUCCESS_TIMESTAMP.labels(job_name=job_name).set(now)
        logger.debug(
            "arq task success: name=%s duration=%.2fs — recorded JOB_DURATION, "
            "JOB_LAST_RUN_TIMESTAMP, JOB_LAST_SUCCESS_TIMESTAMP",
            job_name, duration,
        )
    except Exception as exc:
        logger.debug(
            "arq task: %s raised %s after %.2fs — retry decision: "
            "max_retries=%d, job_try=%d",
            job_name, type(exc).__name__, _time.monotonic() - started_at,
            max_retries, job_try,
        )
        if max_retries > 0 and job_try <= max_retries:
            delay = delay_base * (2 ** (job_try - 1))
            logger.warning(
                "%s failed (attempt %d/%d), retrying in %ds: %s: %r",
                job_name,
                job_try,
                max_retries + 1,
                delay,
                type(exc).__name__,
                exc,
            )
            # Keep the lock held so the scheduler doesn't double-enqueue
            # while we wait for the retry backoff to fire.
            release_lock = False
            logger.debug(
                "arq task: scheduling retry via arq.Retry(defer=%ds), "
                "holding job_lock",
                delay,
            )
            raise Retry(defer=delay) from exc

        logger.error(
            "%s exhausted retries (%d attempts), sending to DLQ: %s: %r",
            job_name,
            job_try,
            type(exc).__name__,
            exc,
        )
        await write_dead_letter(
            job_name=job_name,
            error_message=str(exc),
            traceback_text=traceback.format_exc(),
            retry_count=job_try,
        )
        # Permanent failure → bump the error counter and observe duration
        # so the histogram reflects all completed runs (success + fail).
        JOB_ERRORS.labels(job_name=job_name).inc()
        JOB_DURATION.labels(job_name=job_name).observe(_time.monotonic() - started_at)
        JOB_LAST_RUN_TIMESTAMP.labels(job_name=job_name).set(_time.time())
        logger.debug(
            "arq task permanent failure: name=%s — recorded JOB_ERRORS, "
            "wrote to DLQ",
            job_name,
        )
    finally:
        if release_lock:
            logger.debug("arq task finally: releasing job_lock:%s", job_id)
            await _release_job_lock(job_id)
        else:
            logger.debug(
                "arq task finally: NOT releasing job_lock:%s (Retry in flight)",
                job_id,
            )


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


async def task_discover_stock_intraday(ctx: dict) -> None:
    """Lightweight 30-min intraday price refresh (market hours only).

    Writes ONLY last_price / percent_change / volume to
    discover_stock_snapshots.  Auto-skips between 15:55 and 16:45 IST
    so it never races the heavy daily discover_stock → rescore pipeline
    that runs at 16:00 / 16:20 / 16:30 IST.
    """
    from app.scheduler.discover_stock_intraday_job import (
        run_discover_stock_intraday_job,
    )

    await _run_with_retry(
        ctx, "discover_stock_intraday", run_discover_stock_intraday_job,
    )


async def task_discover_stock_intraday_backfill(ctx: dict) -> None:
    """One-shot: backfill today's 5-min intraday ticks from Yahoo.

    Idempotent (ON CONFLICT DO NOTHING). Triggered manually via
    /ops/jobs/trigger/discover_stock_intraday_backfill after a first
    deploy or whenever the ticks table needs to be repopulated with
    historical intraday data for the last trading day.
    """
    from app.scheduler.discover_stock_intraday_job import (
        run_discover_stock_intraday_backfill,
    )

    await _run_with_retry(
        ctx,
        "discover_stock_intraday_backfill",
        run_discover_stock_intraday_backfill,
    )


async def task_discover_stock_intraday_autofill(ctx: dict) -> None:
    """Self-healing gap sweeper for discover_stock_intraday. Runs
    every 10 minutes during the Indian trading session and fills
    in whatever the live 30-min cron missed."""
    from app.scheduler.discover_stock_intraday_job import (
        run_discover_stock_intraday_autofill_job,
    )

    await _run_with_retry(
        ctx,
        "discover_stock_intraday_autofill",
        run_discover_stock_intraday_autofill_job,
    )


async def task_discover_mf_nav(ctx: dict) -> None:
    from app.scheduler.discover_mf_nav_job import run_discover_mf_nav_job

    await _run_with_retry(ctx, "discover_mf_nav", run_discover_mf_nav_job)


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


async def task_stock_future_prospects(ctx: dict) -> None:
    from app.scheduler.stock_future_prospects_job import run_stock_future_prospects_job

    await _run_with_retry(
        ctx,
        "stock_future_prospects",
        run_stock_future_prospects_job,
    )


async def task_stock_future_prospects_recent(ctx: dict) -> None:
    from app.scheduler.stock_future_prospects_job import (
        run_stock_future_prospects_recent_job,
    )

    await _run_with_retry(
        ctx,
        "stock_future_prospects_recent",
        run_stock_future_prospects_recent_job,
    )


async def task_stock_future_prospects_embed(ctx: dict) -> None:
    from app.scheduler.stock_future_prospects_embed_job import (
        run_stock_future_prospects_embed_job,
    )

    await _run_with_retry(
        ctx,
        "stock_future_prospects_embed",
        run_stock_future_prospects_embed_job,
    )


async def task_broker_charges(ctx: dict) -> None:
    from app.scheduler.broker_charges_job import run_broker_charges_job

    await _run_with_retry(ctx, "broker_charges", run_broker_charges_job)
