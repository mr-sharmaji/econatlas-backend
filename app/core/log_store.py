"""Persistent ops-log store backed by Postgres with 7-day retention.

The in-memory ring buffer in ``log_stream`` is bounded and lost on restart.
This module mirrors every log record into an ``ops_logs`` table so that the
``/ops/logs`` endpoint can serve history beyond the live buffer — up to
``ops_log_retention_days`` (default 7) days.

Design notes
------------
- Log records are emitted from arbitrary threads (logging handlers run
  synchronously in whichever thread logged the message). We bridge into
  asyncio with ``loop.call_soon_threadsafe`` and a bounded ``asyncio.Queue``.
- A single background writer task drains the queue in batches and bulk-
  inserts via ``executemany``. Persistence failures drop the batch rather
  than blocking the logging pipeline.
- A second background task purges rows older than the retention window
  every hour so the table never grows unbounded.
- Everything here is best-effort: if the DB pool is unavailable, the
  endpoint transparently falls back to the in-memory buffer.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

_queue: asyncio.Queue[dict[str, Any]] | None = None
_writer_task: asyncio.Task | None = None
_cleanup_task: asyncio.Task | None = None
_loop: asyncio.AbstractEventLoop | None = None
_enabled: bool = False
_retention_days: int = 7

_MAX_QUEUE = 20000
_BATCH_SIZE = 500
_FLUSH_INTERVAL_S = 2.0
_CLEANUP_INTERVAL_S = 3600.0

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS ops_logs (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level TEXT NOT NULL,
    logger TEXT NOT NULL,
    message TEXT NOT NULL,
    module TEXT,
    function TEXT,
    line INTEGER,
    exception TEXT
)
"""

_INDEX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_ops_logs_timestamp ON ops_logs (timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ops_logs_id_desc ON ops_logs (id DESC)",
    "CREATE INDEX IF NOT EXISTS idx_ops_logs_level ON ops_logs (level)",
    "CREATE INDEX IF NOT EXISTS idx_ops_logs_logger ON ops_logs (logger)",
)

_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def is_enabled() -> bool:
    return _enabled


async def setup_log_persistence(retention_days: int = 7) -> None:
    """Create the ops_logs table and start writer + cleanup background tasks.

    Must be called after the database pool is initialized. Safe to call
    multiple times — becomes a no-op once set up.
    """
    global _queue, _writer_task, _cleanup_task, _loop, _enabled, _retention_days

    if _enabled:
        return

    _retention_days = max(1, int(retention_days or 7))

    try:
        from app.core.database import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(_CREATE_SQL)
            for stmt in _INDEX_SQL:
                await conn.execute(stmt)
    except Exception as exc:
        logger.warning(
            "ops_logs: persistent store unavailable (table init failed): %s",
            exc,
        )
        return

    _loop = asyncio.get_running_loop()
    _queue = asyncio.Queue(maxsize=_MAX_QUEUE)
    _writer_task = asyncio.create_task(_writer_loop(), name="ops_logs_writer")
    _cleanup_task = asyncio.create_task(_cleanup_loop(), name="ops_logs_cleanup")
    _enabled = True
    logger.info(
        "ops_logs: persistent store ready (retention=%d days)",
        _retention_days,
    )


async def stop_log_persistence() -> None:
    """Flush in-flight entries and cancel background tasks."""
    global _enabled
    if not _enabled:
        return
    _enabled = False
    for task in (_writer_task, _cleanup_task):
        if task is None:
            continue
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass


def enqueue_entry(payload: dict[str, Any]) -> None:
    """Thread-safe hand-off from a sync logging handler to the writer loop.

    Never raises — logging must never crash the caller.
    """
    if not _enabled or _queue is None or _loop is None:
        return
    try:
        _loop.call_soon_threadsafe(_enqueue_nowait, payload)
    except RuntimeError:
        # Event loop has been closed (shutdown race). Silently drop.
        return


def _enqueue_nowait(payload: dict[str, Any]) -> None:
    if _queue is None:
        return
    try:
        _queue.put_nowait(payload)
    except asyncio.QueueFull:
        # Back-pressure: drop oldest to keep the newest.
        try:
            _queue.get_nowait()
            _queue.put_nowait(payload)
        except Exception:
            pass


async def _writer_loop() -> None:
    """Drain the queue in batches and bulk-insert into ops_logs."""
    from app.core.database import get_pool

    assert _queue is not None
    batch: list[dict[str, Any]] = []

    while True:
        try:
            try:
                item = await asyncio.wait_for(_queue.get(), timeout=_FLUSH_INTERVAL_S)
                batch.append(item)
            except asyncio.TimeoutError:
                pass

            while len(batch) < _BATCH_SIZE:
                try:
                    batch.append(_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if not batch:
                continue

            records = [_row_from_payload(e) for e in batch]
            try:
                pool = await get_pool()
                async with pool.acquire() as conn:
                    await conn.executemany(
                        """
                        INSERT INTO ops_logs
                            (timestamp, level, logger, message, module, function, line, exception)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        records,
                    )
            except Exception as exc:
                # Don't let DB issues kill the writer. Drop this batch so
                # the queue keeps draining and newer logs can flow.
                logger.warning("ops_logs: batch insert failed (%d dropped): %s", len(batch), exc)
                await asyncio.sleep(5.0)
            finally:
                batch.clear()
        except asyncio.CancelledError:
            # Final best-effort flush on shutdown.
            if batch:
                try:
                    pool = await get_pool()
                    async with pool.acquire() as conn:
                        await conn.executemany(
                            """
                            INSERT INTO ops_logs
                                (timestamp, level, logger, message, module, function, line, exception)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            """,
                            [_row_from_payload(e) for e in batch],
                        )
                except Exception:
                    pass
            raise
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("ops_logs: writer loop error: %s", exc)
            batch.clear()
            await asyncio.sleep(5.0)


def _row_from_payload(e: dict[str, Any]) -> tuple:
    line_val = e.get("line")
    try:
        line_int: int | None = int(line_val) if line_val is not None else None
    except (TypeError, ValueError):
        line_int = None
    return (
        _coerce_ts(e.get("timestamp")),
        str(e.get("level") or "INFO")[:32],
        str(e.get("logger") or "")[:255],
        str(e.get("message") or ""),
        (str(e["module"])[:255] if e.get("module") else None),
        (str(e["function"])[:255] if e.get("function") else None),
        line_int,
        (str(e["exception"]) if e.get("exception") else None),
    )


def _coerce_ts(ts: Any) -> datetime:
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


async def _cleanup_loop() -> None:
    """Purge rows older than the retention window every hour."""
    from app.core.database import get_pool

    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                result = await conn.execute(
                    f"DELETE FROM ops_logs "
                    f"WHERE timestamp < NOW() - INTERVAL '{_retention_days} days'"
                )
            logger.info("ops_logs: retention purge ok (%s)", result)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("ops_logs: retention purge failed: %s", exc)

        try:
            await asyncio.sleep(_CLEANUP_INTERVAL_S)
        except asyncio.CancelledError:
            raise


def _levels_at_or_above(level: str | None) -> list[str]:
    if not level:
        return []
    name = level.strip().upper()
    if name not in _LEVELS:
        return []
    idx = _LEVELS.index(name)
    return list(_LEVELS[idx:])


async def query_log_entries(
    *,
    limit: int = 200,
    after_id: int | None = None,
    min_level: str | None = None,
    contains: str | None = None,
    logger_name: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Query persisted logs with the same filters as the in-memory variant,
    plus optional ``since`` / ``until`` timestamp bounds for 7-day tailing.
    """
    from app.core.database import get_pool

    pool = await get_pool()

    clauses: list[str] = []
    params: list[Any] = []

    if after_id is not None:
        params.append(int(after_id))
        clauses.append(f"id > ${len(params)}")

    levels = _levels_at_or_above(min_level)
    if levels:
        params.append(levels)
        clauses.append(f"level = ANY(${len(params)}::text[])")

    if contains:
        params.append(f"%{contains}%")
        clauses.append(f"message ILIKE ${len(params)}")

    if logger_name:
        params.append(f"%{logger_name}%")
        clauses.append(f"logger ILIKE ${len(params)}")

    if since is not None:
        params.append(_coerce_ts(since))
        clauses.append(f"timestamp >= ${len(params)}")

    if until is not None:
        params.append(_coerce_ts(until))
        clauses.append(f"timestamp <= ${len(params)}")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    safe_limit = max(1, min(int(limit), 5000))
    params.append(safe_limit)

    sql = (
        "SELECT id, timestamp, level, logger, message, module, function, line, exception "
        f"FROM ops_logs {where} ORDER BY id DESC LIMIT ${len(params)}"
    )

    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
        latest_id = await conn.fetchval("SELECT COALESCE(MAX(id), 0) FROM ops_logs")

    # Return in ascending-id order to match the in-memory API contract.
    entries = [
        {
            "id": int(r["id"]),
            "timestamp": r["timestamp"],
            "level": r["level"],
            "logger": r["logger"],
            "message": r["message"],
            "module": r["module"],
            "function": r["function"],
            "line": r["line"],
            "exception": r["exception"],
        }
        for r in reversed(rows)
    ]
    return entries, int(latest_id or 0)
