import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import get_settings
from app.core.database import close_pool, init_pool
from app.core.log_stream import setup_log_stream
from app.queue.redis_pool import close_redis_pool
from app.queue.worker import start_worker, stop_worker
from app.scheduler.runner import start_scheduler, stop_scheduler

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"


def _resolve_log_level(value: str | None) -> int:
    if not value:
        return logging.INFO
    name = value.strip().upper()
    if hasattr(logging, name):
        level = getattr(logging, name)
        if isinstance(level, int):
            return level
    named = logging.getLevelName(name)
    return named if isinstance(named, int) else logging.INFO


def _configure_logging(level_name: str | None) -> int:
    level = _resolve_log_level(level_name)
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=level, format=_LOG_FORMAT)
    else:
        root.setLevel(level)
        for handler in root.handlers:
            handler.setLevel(level)
            if handler.formatter is None:
                handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    return level


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await init_pool()           # 1. PostgreSQL pool
    await start_worker()        # 2. ARQ worker (ready to pick up jobs)
    start_scheduler()           # 3. APScheduler (starts enqueuing into Redis)
    yield
    stop_scheduler()            # 1. Stop enqueuing new jobs
    await stop_worker()         # 2. Drain in-flight ARQ jobs
    await close_redis_pool()    # 3. Close Redis connection
    await close_pool()          # 4. Close PostgreSQL pool


def create_app() -> FastAPI:
    settings = get_settings()
    log_level = _configure_logging(settings.log_level)
    logging.getLogger(__name__).info("Logging configured at %s", logging.getLevelName(log_level))
    if settings.ops_logs_enabled:
        setup_log_stream(settings.ops_log_buffer_size, min_level=log_level)

    application = FastAPI(
        title=settings.app_name,
        description="Personal Economic Intelligence System — API backend",
        version="0.2.2",
        lifespan=lifespan,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Redis response cache — must be added after CORS middleware
    from app.core.cache import RedisCacheMiddleware
    application.add_middleware(RedisCacheMiddleware)

    application.include_router(api_router)

    return application


app = create_app()
