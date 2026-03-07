"""PostgreSQL connection pool and lifecycle."""
from __future__ import annotations

import logging
from pathlib import Path
from uuid import UUID

import asyncpg

from app.core.config import get_settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None

# Project root (parent of app/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_INIT_SQL_PATH = _PROJECT_ROOT / "sql" / "init.sql"


def record_to_dict(record: asyncpg.Record) -> dict:
    """Convert asyncpg Record to a JSON-serializable dict (id/dates as str)."""
    out = {}
    for k, v in zip(record.keys(), record.values()):
        if v is None:
            out[k] = None
        elif isinstance(v, UUID):
            out[k] = str(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


async def get_pool() -> asyncpg.Pool:
    """Return the application's connection pool. Must be called after startup."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized; app may not have started.")
    return _pool


async def init_pool() -> asyncpg.Pool:
    """Create the connection pool and run schema if present. Called during app lifespan."""
    global _pool
    settings = get_settings()
    _pool = await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=10,
        command_timeout=60,
    )
    logger.info("Database pool created")
    if _INIT_SQL_PATH.exists():
        async with _pool.acquire() as conn:
            sql = _INIT_SQL_PATH.read_text()
            for raw in sql.split(";"):
                stmt = raw.strip()
                if not stmt or stmt.startswith("--"):
                    continue
                up = stmt.upper()
                if up.startswith("CREATE") or up.startswith("ALTER") or up.startswith("DROP"):
                    await conn.execute(stmt)
        logger.info("Schema init executed from sql/init.sql")
    return _pool


async def close_pool() -> None:
    """Close the connection pool. Called during app shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Database pool closed")
