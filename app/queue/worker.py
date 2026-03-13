"""In-process ARQ worker — runs alongside FastAPI in the same event loop."""
from __future__ import annotations

import asyncio
import logging

from arq.connections import RedisSettings
from arq.worker import Worker, create_worker

from app.core.config import get_settings
from app.queue.settings import get_arq_functions

logger = logging.getLogger(__name__)

_worker: Worker | None = None
_worker_task: asyncio.Task | None = None


async def _clear_stale_arq_state() -> None:
    """Remove orphaned in-progress markers left by a previous unclean shutdown.

    When the server restarts mid-job, ARQ leaves behind:
    - `arq:in-progress:arq:queue` set with stale job IDs
    - `arq:job:{id}` hashes with in-progress markers
    These prevent the worker from picking up new jobs for the same function.
    """
    from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    cleared = 0

    # 1. Clear the in-progress set
    in_progress_key = in_progress_key_prefix + default_queue_name
    raw_members = await pool.smembers(in_progress_key)
    for raw_id in raw_members or set():
        await pool.srem(in_progress_key, raw_id)
        cleared += 1

    # 2. Delete stale job hashes for startup/manual jobs
    from app.api.routes.ops import _VALID_JOBS

    known_ids = (
        [f"startup_{name}" for name in _VALID_JOBS]
        + [f"{name}_manual" for name in _VALID_JOBS]
    )
    for jid in known_ids:
        job_key = job_key_prefix + jid
        if await pool.exists(job_key):
            await pool.delete(job_key)
            cleared += 1

    # 3. Scan for timestamped manual job keys
    async for key in pool.scan_iter(match=f"{job_key_prefix}*_manual_*"):
        await pool.delete(key)
        cleared += 1

    if cleared:
        logger.info("Cleared %d stale ARQ keys on startup", cleared)


async def start_worker() -> None:
    """Create and start the ARQ worker as a background asyncio task."""
    global _worker, _worker_task

    settings = get_settings()
    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    # Clean up any stale state from a previous unclean shutdown
    await _clear_stale_arq_state()

    _worker = create_worker(
        WorkerSettings,  # type: ignore[arg-type]
        redis_settings=redis_settings,
        functions=get_arq_functions(),
        max_jobs=10,
        job_timeout=settings.arq_job_timeout_seconds,
        poll_delay=0.5,
        keep_result=60,
        handle_signals=False,  # uvicorn owns signal handlers
    )

    _worker_task = asyncio.create_task(_worker.main(), name="arq-worker")
    logger.info("ARQ worker started in-process (max_jobs=10, poll=0.5s)")


async def stop_worker() -> None:
    """Gracefully stop the ARQ worker."""
    global _worker, _worker_task

    if _worker is not None:
        _worker.handle_sig(0)  # triggers graceful shutdown
        if _worker_task is not None:
            try:
                await asyncio.wait_for(_worker_task, timeout=15)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("ARQ worker did not shut down within 15s; cancelling")
                _worker_task.cancel()
        _worker = None
        _worker_task = None
        logger.info("ARQ worker stopped")


class WorkerSettings:
    """Minimal class that ARQ's create_worker expects for type inspection."""

    pass
