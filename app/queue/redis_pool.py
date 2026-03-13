from __future__ import annotations

import logging

from arq import ArqRedis, create_pool
from arq.connections import RedisSettings

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_pool: ArqRedis | None = None


async def get_redis_pool() -> ArqRedis:
    """Return (and lazily create) the singleton ArqRedis connection pool."""
    global _pool
    if _pool is None:
        settings = get_settings()
        _pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
        logger.info("ARQ Redis pool created (%s)", settings.redis_url)
    return _pool


async def close_redis_pool() -> None:
    """Close the Redis pool.  Safe to call even if never opened."""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
        logger.info("ARQ Redis pool closed")
