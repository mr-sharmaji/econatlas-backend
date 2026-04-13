import logging
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.core.config import get_settings
from app.core.database import close_pool, init_pool
from app.core.log_stream import setup_log_stream
from app.queue.redis_pool import close_redis_pool
from app.queue.worker import start_worker, stop_worker
from app.scheduler.runner import start_scheduler, stop_scheduler

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s - %(message)s"

logger = logging.getLogger(__name__)


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


async def _warm_artha_cache() -> None:
    """Warm the Artha suggestions POOL in the background so the first
    real client request picks 10 random items from a populated pool
    instead of falling back to the static list. Non-fatal — failures
    just mean the first request uses the static fallback until the
    next background refresh completes."""
    try:
        import asyncio as _asyncio
        # Delay slightly so it runs after the pool + worker are fully up
        await _asyncio.sleep(2)
        from app.services.chat_service import (
            _compute_suggestions_llm,
            _suggestions_pool,
        )
        import time as _time
        logger.info("artha: warming suggestions pool…")
        suggestions = await _compute_suggestions_llm(device_id=None)
        if suggestions and len(suggestions) >= 6:
            _suggestions_pool["_global"] = (suggestions, _time.time())
            logger.info(
                "artha: suggestions pool warmed with %d items",
                len(suggestions),
            )
        else:
            logger.warning(
                "artha: pool warmup returned only %d items (<6) — static fallback",
                len(suggestions) if suggestions else 0,
            )
    except Exception:
        logger.warning("artha: pool warmup failed", exc_info=True)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await init_pool()           # 1. PostgreSQL pool
    await start_worker()        # 2. ARQ worker (ready to pick up jobs)
    start_scheduler()           # 3. APScheduler (starts enqueuing into Redis)
    # 4. Warm the Artha LLM caches in the background — non-blocking
    import asyncio as _asyncio
    _asyncio.create_task(_warm_artha_cache())
    # 5. Start the Artha prefetch cache refresh loop (indices, movers,
    #    FX, commodities) — injected into every chat system prompt so
    #    simple queries answer with zero tool calls.
    from app.services.chat_service import start_prefetch_task
    start_prefetch_task()
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

    # ── Prometheus metrics endpoint ──
    from starlette.responses import Response

    @application.get("/metrics", include_in_schema=False)
    async def prometheus_metrics():
        from app.core.metrics import get_prometheus_metrics, get_prometheus_content_type
        return Response(
            content=get_prometheus_metrics(),
            media_type=get_prometheus_content_type(),
        )

    # ── Request timing middleware ──
    import time as _time

    @application.middleware("http")
    async def timing_middleware(request, call_next):
        t0 = _time.monotonic()
        response = await call_next(request)
        duration = _time.monotonic() - t0
        try:
            from app.core.metrics import record_request
            record_request(
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                duration=duration,
            )
        except Exception:
            pass  # metrics must never break the request
        return response

    # ── Grafana reverse proxy ──
    # Cloudflare Tunnel only exposes port 8000. Grafana runs on
    # localhost:3000 inside Docker. Proxy /grafana/* through FastAPI
    # so it's accessible via the same tunnel.
    import httpx as _httpx

    _grafana_client = _httpx.AsyncClient(
        base_url="http://grafana:3000",
        timeout=30.0,
    )

    @application.api_route(
        "/grafana/{path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
        include_in_schema=False,
    )
    async def grafana_proxy(request: Request, path: str):
        url = f"/grafana/{path}"
        if request.url.query:
            url = f"{url}?{request.url.query}"
        body = await request.body()
        headers = dict(request.headers)
        headers.pop("host", None)
        try:
            resp = await _grafana_client.request(
                method=request.method,
                url=url,
                headers=headers,
                content=body if body else None,
            )
            excluded = {"transfer-encoding", "content-encoding", "content-length"}
            resp_headers = {
                k: v for k, v in resp.headers.items()
                if k.lower() not in excluded
            }
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers=resp_headers,
                media_type=resp.headers.get("content-type"),
            )
        except Exception as exc:
            return Response(
                content=f"Grafana unavailable: {exc}",
                status_code=502,
            )

    return application


app = create_app()
