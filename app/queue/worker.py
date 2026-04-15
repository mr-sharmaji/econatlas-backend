"""In-process ARQ worker — runs alongside FastAPI in the same event loop."""
from __future__ import annotations

import asyncio
import logging

from arq.connections import RedisSettings
from arq.worker import Worker, create_worker

from app.core.config import get_settings
from app.queue.settings import expand_job_family_ids, get_arq_functions

logger = logging.getLogger(__name__)

_worker: Worker | None = None
_worker_task: asyncio.Task | None = None


async def _clear_stale_arq_state() -> None:
    """Remove ALL orphaned ARQ state left by a previous unclean shutdown.

    ARQ uses three types of Redis keys that can go stale on crash:
    1. `arq:in-progress:arq:queue` — set of job IDs the worker is processing
    2. `arq:job:{id}` — per-job hash with metadata
    3. `arq:in-progress:{job_id}` — per-job TTL key that blocks re-execution
       (THIS is what causes "already running elsewhere")
    """
    from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix

    from app.queue.redis_pool import get_redis_pool

    logger.debug("_clear_stale_arq_state: starting scan")
    pool = await get_redis_pool()
    cleared = 0
    phase_counts: dict[str, int] = {
        "in_progress_set": 0,
        "per_job_in_progress": 0,
        "known_job_hashes": 0,
        "manual_job_hashes": 0,
        "abort_flags": 0,
    }

    # 1. Clear the in-progress set
    in_progress_key = in_progress_key_prefix + default_queue_name
    raw_members = await pool.smembers(in_progress_key)
    logger.debug(
        "_clear_stale_arq_state: in-progress set %s has %d members",
        in_progress_key, len(raw_members or set()),
    )
    for raw_id in raw_members or set():
        await pool.srem(in_progress_key, raw_id)
        cleared += 1
        phase_counts["in_progress_set"] += 1
        logger.debug("_clear_stale_arq_state: removed stale in-progress id=%r", raw_id)

    # 2. Delete per-job-id in-progress keys (the actual "already running" blocker)
    #    These are `arq:in-progress:{job_id}` string keys with a TTL.
    async for key in pool.scan_iter(match=f"{in_progress_key_prefix}*"):
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        # Don't delete the queue-level set itself (handled above)
        if key_str == in_progress_key:
            continue
        await pool.delete(key)
        cleared += 1
        phase_counts["per_job_in_progress"] += 1
        logger.debug("_clear_stale_arq_state: deleted stale lock key=%s", key_str)

    # 3. Delete stale job hashes for startup/manual jobs
    from app.api.routes.ops import _VALID_JOBS

    known_ids: set[str] = set()
    for name in _VALID_JOBS:
        known_ids.update(expand_job_family_ids(name))
    logger.debug(
        "_clear_stale_arq_state: checking %d known job-family ids for stale hashes",
        len(known_ids),
    )
    for jid in known_ids:
        job_key = job_key_prefix + jid
        if await pool.exists(job_key):
            await pool.delete(job_key)
            cleared += 1
            phase_counts["known_job_hashes"] += 1
            logger.debug("_clear_stale_arq_state: deleted stale job hash %s", job_key)

    # 4. Scan for timestamped manual job keys
    async for key in pool.scan_iter(match=f"{job_key_prefix}*_manual_*"):
        await pool.delete(key)
        cleared += 1
        phase_counts["manual_job_hashes"] += 1
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        logger.debug("_clear_stale_arq_state: deleted manual job hash %s", key_str)

    # 5. Clear abort flags left over from /ops/jobs/abort calls or
    #    crashed runs. These have a 600s TTL so they usually expire on
    #    their own, but a fresh worker startup should never inherit a
    #    prior abort request. Without this, the newly-started
    #    discover_stock would see the stale flag, read-and-delete it at
    #    symbol #100, and kill itself. Rare but catastrophic.
    async for key in pool.scan_iter(match="job:abort:*"):
        await pool.delete(key)
        cleared += 1
        phase_counts["abort_flags"] += 1
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        logger.debug("_clear_stale_arq_state: cleared abort flag %s", key_str)

    if cleared:
        logger.info("Cleared %d stale ARQ keys on startup", cleared)
    logger.debug("_clear_stale_arq_state: phase breakdown %s", phase_counts)


async def start_worker() -> None:
    """Create and start the ARQ worker as a background asyncio task."""
    global _worker, _worker_task

    logger.debug("start_worker: resolving settings + redis DSN")
    settings = get_settings()
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    logger.debug(
        "start_worker: redis host=%s port=%s database=%s",
        redis_settings.host, redis_settings.port, redis_settings.database,
    )

    # Clean up any stale state from a previous unclean shutdown
    logger.debug("start_worker: running _clear_stale_arq_state()")
    await _clear_stale_arq_state()

    functions = get_arq_functions()
    logger.debug(
        "start_worker: registering %d arq functions: %s",
        len(functions),
        sorted(getattr(f, "name", getattr(f, "__name__", "?")) for f in functions),
    )
    _worker = create_worker(
        WorkerSettings,  # type: ignore[arg-type]
        redis_settings=redis_settings,
        functions=functions,
        max_jobs=10,
        job_timeout=settings.arq_job_timeout_seconds,
        poll_delay=0.5,
        keep_result=60,
        handle_signals=False,  # uvicorn owns signal handlers
    )
    logger.debug(
        "start_worker: arq.Worker created (max_jobs=10, poll_delay=0.5s, "
        "job_timeout=%ds, keep_result=60s) — launching main() task",
        settings.arq_job_timeout_seconds,
    )

    _worker_task = asyncio.create_task(_worker.main(), name="arq-worker")
    logger.info("ARQ worker started in-process (max_jobs=10, poll=0.5s)")


async def stop_worker() -> None:
    """Gracefully stop the ARQ worker."""
    global _worker, _worker_task

    if _worker is not None:
        logger.debug("stop_worker: sending graceful shutdown signal to arq worker")
        _worker.handle_sig(0)  # triggers graceful shutdown
        if _worker_task is not None:
            try:
                logger.debug("stop_worker: awaiting arq worker task (timeout=15s)")
                await asyncio.wait_for(_worker_task, timeout=15)
                logger.debug("stop_worker: arq worker task exited cleanly")
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("ARQ worker did not shut down within 15s; cancelling")
                _worker_task.cancel()
        _worker = None
        _worker_task = None
        logger.info("ARQ worker stopped")
    else:
        logger.debug("stop_worker: no active worker, nothing to stop")


class WorkerSettings:
    """Minimal class that ARQ's create_worker expects for type inspection."""

    pass
