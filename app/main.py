import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import get_settings
from app.core.database import close_pool, init_pool
from app.core.log_stream import setup_log_stream
from app.scheduler.runner import start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await init_pool()
    start_scheduler()
    yield
    stop_scheduler()
    await close_pool()


def create_app() -> FastAPI:
    settings = get_settings()
    if settings.ops_logs_enabled:
        setup_log_stream(settings.ops_log_buffer_size)

    application = FastAPI(
        title=settings.app_name,
        description="Personal Economic Intelligence System — API backend",
        version="0.1.0",
        lifespan=lifespan,
    )

    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    application.include_router(api_router)

    return application


app = create_app()
