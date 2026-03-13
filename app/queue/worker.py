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


async def start_worker() -> None:
    """Create and start the ARQ worker as a background asyncio task."""
    global _worker, _worker_task

    settings = get_settings()
    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    _worker = create_worker(
        WorkerSettings,  # type: ignore[arg-type]
        redis_settings=redis_settings,
        functions=get_arq_functions(),
        max_jobs=4,
        job_timeout=settings.arq_job_timeout_seconds,
        poll_delay=0.5,
        keep_result=60,
        handle_signals=False,  # uvicorn owns signal handlers
    )

    _worker_task = asyncio.create_task(_worker.main(), name="arq-worker")
    logger.info("ARQ worker started in-process (max_jobs=4, poll=0.5s)")


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
