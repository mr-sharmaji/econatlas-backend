"""Lightweight Redis response cache for GET endpoints.

Caches JSON responses by URL path + query string with configurable TTLs.
Cache is automatically bypassed for non-GET requests and non-cacheable paths.

Call `invalidate_cache()` after discovery/rescore jobs to flush stale data.
"""
from __future__ import annotations

import hashlib
import logging
import time
from typing import Callable

import redis.asyncio as aioredis
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Path prefix → TTL in seconds
# More frequently updated data gets shorter TTLs
_CACHE_TTLS: dict[str, int] = {
    "/screener/home": 300,              # 5 min — aggregated sections
    "/screener/stocks": 120,            # 2 min — list/search
    "/screener/mutual-funds": 120,      # 2 min — list/search
    "/market/": 60,                     # 1 min — live prices
    "/commodities": 60,                 # 1 min
    "/crypto/": 60,                     # 1 min
    "/news": 300,                       # 5 min
    "/ipo": 300,                        # 5 min
    "/macro/": 600,                     # 10 min
    "/brief": 300,                      # 5 min
    "/economy/": 600,                   # 10 min
}

# Paths that should never be cached
_NO_CACHE_PREFIXES = ("/ops/", "/auth/", "/feedback/", "/watchlist/", "/docs", "/openapi")

_redis: aioredis.Redis | None = None


async def _get_redis() -> aioredis.Redis | None:
    global _redis
    if _redis is not None:
        return _redis
    try:
        settings = get_settings()
        _redis = aioredis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        await _redis.ping()
        logger.info("Cache: Redis connected (%s)", settings.redis_url)
        return _redis
    except Exception as exc:
        logger.warning("Cache: Redis unavailable, caching disabled: %s", exc)
        _redis = None
        return None


def _cache_ttl(path: str) -> int | None:
    """Return TTL for a path, or None if not cacheable."""
    for prefix in _NO_CACHE_PREFIXES:
        if path.startswith(prefix):
            return None
    for prefix, ttl in _CACHE_TTLS.items():
        if path.startswith(prefix):
            return ttl
    return None


def _cache_key(request: Request) -> str:
    """Generate cache key from method + full URL (path + query)."""
    url = str(request.url)
    url_hash = hashlib.md5(url.encode()).hexdigest()
    return f"cache:v1:{url_hash}"


class RedisCacheMiddleware(BaseHTTPMiddleware):
    """Middleware that caches GET responses in Redis."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        # Only cache GET requests
        if request.method != "GET":
            return await call_next(request)

        path = request.url.path
        ttl = _cache_ttl(path)
        if ttl is None:
            return await call_next(request)

        r = await _get_redis()
        if r is None:
            return await call_next(request)

        key = _cache_key(request)

        # Try cache hit
        try:
            cached = await r.get(key)
            if cached is not None:
                return Response(
                    content=cached,
                    media_type="application/json",
                    headers={"X-Cache": "HIT"},
                )
        except Exception:
            pass  # Redis error — fall through to origin

        # Cache miss — call origin
        response = await call_next(request)

        # Only cache successful JSON responses
        if response.status_code == 200:
            try:
                # Read the response body
                body = b""
                async for chunk in response.body_iterator:
                    body += chunk if isinstance(chunk, bytes) else chunk.encode()

                # Store in Redis with TTL
                await r.setex(key, ttl, body.decode("utf-8", errors="replace"))

                # Return new response with the body we consumed
                return Response(
                    content=body,
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )
            except Exception as exc:
                logger.debug("Cache: failed to store response: %s", exc)
                # If we consumed the body, return what we have
                if body:
                    return Response(
                        content=body,
                        status_code=response.status_code,
                        headers=dict(response.headers),
                        media_type=response.media_type,
                    )

        return response


async def invalidate_cache() -> int:
    """Flush all cached responses. Call after discovery/rescore jobs."""
    r = await _get_redis()
    if r is None:
        return 0
    try:
        keys = []
        async for key in r.scan_iter("cache:v1:*", count=500):
            keys.append(key)
        if keys:
            deleted = await r.delete(*keys)
            logger.info("Cache: invalidated %d keys", deleted)
            return deleted
        return 0
    except Exception as exc:
        logger.warning("Cache: invalidation failed: %s", exc)
        return 0
