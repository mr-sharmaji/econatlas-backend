"""Dead-letter queue — write failed jobs to PostgreSQL, query & retry them."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from arq import ArqRedis

from app.core.database import get_pool

logger = logging.getLogger(__name__)


async def write_dead_letter(
    *,
    job_name: str,
    error_message: str,
    traceback_text: str | None = None,
    retry_count: int = 0,
) -> None:
    """Insert a failed-job record into the DLQ table."""
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO job_dead_letters (job_name, error_message, traceback, retry_count)
        VALUES ($1, $2, $3, $4)
        """,
        job_name,
        error_message,
        traceback_text,
        retry_count,
    )
    logger.info("DLQ: recorded failure for job=%s retries=%d", job_name, retry_count)


async def get_dead_letters(
    *,
    status: str = "dead",
    limit: int = 50,
    job_name: str | None = None,
) -> list[dict[str, Any]]:
    """Query DLQ entries.  Newest first."""
    pool = await get_pool()
    if job_name:
        rows = await pool.fetch(
            """
            SELECT id, job_name, error_message, traceback, retry_count,
                   status, failed_at, retried_at, created_at
            FROM job_dead_letters
            WHERE status = $1 AND job_name = $2
            ORDER BY failed_at DESC
            LIMIT $3
            """,
            status,
            job_name,
            limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT id, job_name, error_message, traceback, retry_count,
                   status, failed_at, retried_at, created_at
            FROM job_dead_letters
            WHERE status = $1
            ORDER BY failed_at DESC
            LIMIT $2
            """,
            status,
            limit,
        )
    return [dict(r) for r in rows]


async def retry_dead_letter(dlq_id: str, redis_pool: ArqRedis) -> bool:
    """Re-enqueue a dead-lettered job and mark it as retried."""
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT job_name FROM job_dead_letters WHERE id = $1 AND status = 'dead'",
        dlq_id,
    )
    if not row:
        return False

    await redis_pool.enqueue_job(row["job_name"])
    await pool.execute(
        "UPDATE job_dead_letters SET status = 'retried', retried_at = $1 WHERE id = $2",
        datetime.now(timezone.utc),
        dlq_id,
    )
    logger.info("DLQ: retried job=%s id=%s", row["job_name"], dlq_id)
    return True


async def dismiss_dead_letter(dlq_id: str) -> bool:
    """Mark a DLQ entry as dismissed (acknowledged by admin)."""
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE job_dead_letters SET status = 'dismissed' WHERE id = $1 AND status = 'dead'",
        dlq_id,
    )
    return result == "UPDATE 1"
