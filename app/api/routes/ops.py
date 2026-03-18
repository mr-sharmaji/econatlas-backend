import asyncio
import logging
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Body, Header, HTTPException, Path, Query

from app.core.config import get_settings
from app.core.log_stream import get_log_entries
from app.schemas.market_intel_schema import DataHealthResponse
from app.schemas.ops_schema import LogEntryResponse, LogListResponse
from app.services import market_intel_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ops", tags=["ops"])

# ── Direct-run tracking for long-running jobs ────────────────────────
_running_direct_jobs: dict[str, asyncio.Task] = {}

# Valid job names that can be triggered manually.
_VALID_JOBS = {
    "market", "commodity", "crypto", "brief", "macro", "news",
    "discover_stock", "discover_mutual_funds",
    "discover_stock_price", "discover_mf_nav",
    "discover_mf_holdings",
    "ipo", "tax",
}

# Tables exposed for CRUD operations.
_TABLES: dict[str, str] = {
    "macro": "macro_indicators",
    "market": "market_prices",
    "news": "news_articles",
    "brief": "stock_snapshots",
    "discover_stock": "discover_stock_snapshots",
    "discover_mf": "discover_mutual_fund_snapshots",
    "discover_stock_price": "discover_stock_price_history",
    "discover_mf_nav": "discover_mf_nav_history",
    "events": "economic_events",
}

_SAFE_COLUMN_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def _authorize(x_ops_token: str | None) -> None:
    settings = get_settings()
    if not settings.ops_logs_enabled:
        raise HTTPException(status_code=404, detail="Ops logs endpoint disabled")
    required_token = settings.ops_logs_token
    if required_token and x_ops_token != required_token:
        raise HTTPException(status_code=403, detail="Invalid ops token")


def _resolve_table(table_key: str) -> str:
    table = _TABLES.get(table_key)
    if table is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown table '{table_key}'. Valid: {sorted(_TABLES)}",
        )
    return table


def _validate_column(column: str) -> str:
    if not _SAFE_COLUMN_RE.match(column):
        raise HTTPException(status_code=400, detail=f"Invalid column name: {column}")
    return column


async def _get_pool():
    from app.core.database import get_pool
    return await get_pool()


# ═════════════════════════════════════════════════════════════════════
# Logs
# ═════════════════════════════════════════════════════════════════════

@router.get("/logs", response_model=LogListResponse)
async def ops_logs(
    limit: int = Query(default=200, ge=1, le=2000),
    after_id: int | None = Query(default=None, ge=0, description="Return logs strictly after this id"),
    min_level: str | None = Query(default=None, description="Minimum level: DEBUG/INFO/WARNING/ERROR/CRITICAL"),
    contains: str | None = Query(default=None, description="Substring filter on message"),
    logger_name: str | None = Query(default=None, description="Substring filter on logger name"),
    x_ops_token: str | None = Header(default=None),
) -> LogListResponse:
    """Tail current in-memory application logs for debugging scheduler/feed issues."""
    _authorize(x_ops_token)
    rows, latest_id = get_log_entries(
        limit=limit,
        after_id=after_id,
        min_level=min_level,
        contains=contains,
        logger_name=logger_name,
    )
    entries = [LogEntryResponse(**r) for r in rows]
    return LogListResponse(entries=entries, count=len(entries), latest_id=latest_id)


# ═════════════════════════════════════════════════════════════════════
# Jobs
# ═════════════════════════════════════════════════════════════════════

# Jobs that are run directly (bypass ARQ) because ARQ manual trigger
# fails for long-timeout functions.  Maps job_name → async import path.
_DIRECT_RUN_JOBS: dict[str, tuple[str, str]] = {
    "discover_stock": (
        "app.scheduler.discover_stock_job",
        "run_discover_stock_job",
    ),
    "discover_mutual_funds": (
        "app.scheduler.discover_mutual_fund_job",
        "run_discover_mutual_fund_job",
    ),
    "discover_stock_price": (
        "app.scheduler.discover_stock_price_job",
        "run_discover_stock_price_job",
    ),
    "discover_mf_nav": (
        "app.scheduler.discover_mf_nav_job",
        "run_discover_mf_nav_job",
    ),
    "discover_mf_holdings": (
        "app.scheduler.discover_mf_holdings_job",
        "run_discover_mf_holdings_job",
    ),
}


async def _direct_run_wrapper(job_name: str, module_path: str, func_name: str) -> None:
    """Run a job function directly as an asyncio task."""
    import importlib

    t0 = datetime.now(timezone.utc)
    logger.info("Direct-run STARTED: %s", job_name)
    try:
        mod = importlib.import_module(module_path)
        fn = getattr(mod, func_name)
        await fn()
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info("Direct-run COMPLETED: %s in %.1fs", job_name, elapsed)
    except Exception:
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.exception("Direct-run FAILED: %s after %.1fs", job_name, elapsed)
    finally:
        _running_direct_jobs.pop(job_name, None)


@router.post("/jobs/trigger/{job_name}")
async def trigger_job(
    job_name: str = Path(..., description="Name of the job to trigger"),
    force: bool = Query(default=False, description="Force enqueue even if a previous run exists"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Manually trigger a background job for immediate execution."""
    _authorize(x_ops_token)
    if job_name not in _VALID_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job '{job_name}'. Valid: {sorted(_VALID_JOBS)}",
        )

    # ── Direct-run path for discover_* jobs (ARQ manual trigger is broken
    #    for long-timeout functions) ──
    if job_name in _DIRECT_RUN_JOBS:
        existing = _running_direct_jobs.get(job_name)
        if existing is not None and not existing.done():
            if not force:
                return {
                    "status": "already_running",
                    "job_name": job_name,
                    "message": "Job is already running. Use force=true to start another.",
                }
        module_path, func_name = _DIRECT_RUN_JOBS[job_name]
        task = asyncio.create_task(
            _direct_run_wrapper(job_name, module_path, func_name),
            name=f"direct-run-{job_name}",
        )
        _running_direct_jobs[job_name] = task
        logger.info("Direct-run triggered: %s (bypassing ARQ)", job_name)
        return {
            "status": "started",
            "job_name": job_name,
            "mode": "direct",
            "message": "Job started directly (bypassing ARQ queue).",
        }

    # ── Standard ARQ enqueue for short-lived jobs ──
    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()

    job_id = f"{job_name}_manual"
    if force:
        job_id = f"{job_name}_manual_{int(datetime.now(timezone.utc).timestamp())}"

    job = await pool.enqueue_job(job_name, _job_id=job_id)
    logger.info("Manually triggered job: %s (job_id=%s)", job_name, job.job_id if job else "deduped")
    return {
        "status": "enqueued" if job else "already_queued",
        "job_name": job_name,
        "job_id": job.job_id if job else job_id,
    }


@router.post("/jobs/clear-stale")
async def clear_stale_jobs(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Clear ALL stale job state from Redis.

    When the server restarts mid-job, ARQ leaves behind:
    1. The in-progress set (`arq:in-progress:arq:queue`)
    2. Individual job hashes (`arq:job:{id}`) with stale status
    This endpoint nukes all of them so the worker can pick up fresh jobs.
    """
    _authorize(x_ops_token)
    from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    cleared_progress = []
    cleared_per_job = []
    cleared_jobs = []

    # 1. Clear the in-progress set
    in_progress_key = in_progress_key_prefix + default_queue_name
    raw_members = await pool.smembers(in_progress_key)
    for raw_id in raw_members or set():
        job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        await pool.srem(in_progress_key, raw_id)
        cleared_progress.append(job_id)

    # 2. Delete per-job-id in-progress keys (`arq:in-progress:{job_id}`)
    #    THIS is what causes "already running elsewhere" — a TTL string key.
    async for key in pool.scan_iter(match=f"{in_progress_key_prefix}*"):
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        if key_str == in_progress_key:
            continue
        await pool.delete(key)
        cleared_per_job.append(key_str.removeprefix(in_progress_key_prefix))

    # 3. Delete individual job hash keys for known startup/manual jobs
    known_prefixes = [f"startup_{name}" for name in _VALID_JOBS] + [f"{name}_manual" for name in _VALID_JOBS]
    for jid in known_prefixes:
        job_key = job_key_prefix + jid
        if await pool.exists(job_key):
            await pool.delete(job_key)
            cleared_jobs.append(jid)

    # 4. Also scan for timestamped manual job keys
    async for key in pool.scan_iter(match=f"{job_key_prefix}*_manual_*"):
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        await pool.delete(key)
        cleared_jobs.append(key_str.removeprefix(job_key_prefix))

    total = len(cleared_progress) + len(cleared_per_job) + len(cleared_jobs)
    logger.warning("Cleared stale state: %d set entries, %d per-job locks, %d job hashes", len(cleared_progress), len(cleared_per_job), len(cleared_jobs))
    return {
        "cleared_in_progress_set": cleared_progress,
        "cleared_per_job_locks": cleared_per_job,
        "cleared_job_keys": cleared_jobs,
        "total": total,
    }


@router.post("/jobs/abort/{job_name}")
async def abort_job(
    job_name: str = Path(..., description="Name of the job to abort"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Abort a running job by setting a Redis abort flag + clearing ARQ locks.

    The running job checks this flag periodically and exits early.
    ARQ locks are also cleared so a fresh job can start immediately.
    """
    _authorize(x_ops_token)
    if job_name not in _VALID_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job '{job_name}'. Valid: {sorted(_VALID_JOBS)}",
        )
    from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()

    # 1. Set abort flag (TTL 10 minutes — long enough for job to see it)
    abort_key = f"job:abort:{job_name}"
    await pool.set(abort_key, "1", ex=600)

    # 2. Clear ARQ in-progress locks for this job
    cleared = []
    in_progress_key = in_progress_key_prefix + default_queue_name
    raw_members = await pool.smembers(in_progress_key)
    for raw_id in raw_members or set():
        job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        if job_name in job_id:
            await pool.srem(in_progress_key, raw_id)
            cleared.append(job_id)

    # 3. Clear per-job in-progress keys
    async for key in pool.scan_iter(match=f"{in_progress_key_prefix}*{job_name}*"):
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        await pool.delete(key)
        cleared.append(key_str)

    # 4. Clear job hash keys
    async for key in pool.scan_iter(match=f"{job_key_prefix}*{job_name}*"):
        key_str = key.decode() if isinstance(key, bytes) else str(key)
        await pool.delete(key)
        cleared.append(key_str)

    logger.warning("Abort requested for job %s: flag set, cleared %d ARQ keys", job_name, len(cleared))
    return {
        "status": "abort_requested",
        "job_name": job_name,
        "abort_key": abort_key,
        "cleared_keys": cleared,
    }


@router.delete("/jobs/abort/{job_name}")
async def clear_abort(
    job_name: str,
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Clear an abort flag so the job can run again."""
    _authorize(x_ops_token)
    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    abort_key = f"job:abort:{job_name}"
    deleted = await pool.delete(abort_key)
    logger.info("Cleared abort flag for %s (deleted=%d)", job_name, deleted)
    return {"status": "cleared", "job_name": job_name, "deleted": bool(deleted)}


_RESCORE_JOBS: dict[str, tuple[str, str]] = {
    "discover_stock": (
        "app.scheduler.discover_stock_job",
        "rescore_discover_stocks",
    ),
    "discover_mf": (
        "app.scheduler.discover_mutual_fund_job",
        "rescore_discover_mutual_funds",
    ),
}


@router.post("/jobs/rescore/{job_name}")
async def rescore_job(
    job_name: str,
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Re-score existing data without fetching. Reads rows from DB, scores, writes back."""
    _authorize(x_ops_token)
    if job_name not in _RESCORE_JOBS:
        raise HTTPException(status_code=400, detail=f"Rescore not supported for '{job_name}'")

    task_key = f"rescore_{job_name}"
    existing = _running_direct_jobs.get(task_key)
    if existing is not None and not existing.done():
        return {
            "status": "already_running",
            "job_name": job_name,
            "message": "Rescore is already running.",
        }

    module_path, func_name = _RESCORE_JOBS[job_name]
    task = asyncio.create_task(
        _direct_run_wrapper(task_key, module_path, func_name),
        name=f"direct-run-rescore-{job_name}",
    )
    _running_direct_jobs[task_key] = task
    logger.info("Rescore triggered: %s (background task)", job_name)
    return {
        "status": "started",
        "job_name": job_name,
        "mode": "rescore",
        "message": "Rescore started in background — check logs for progress.",
    }


@router.post("/jobs/rerank-mf")
async def rerank_mf(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Recompute sub_category_rank and category_rank for all MFs."""
    _authorize(x_ops_token)
    from app.core.database import get_pool
    pool = await get_pool()
    try:
        sub_r = await pool.execute("""
            UPDATE discover_mutual_fund_snapshots AS t
            SET sub_category_rank = sub.rnk, sub_category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
        cat_r = await pool.execute("""
            UPDATE discover_mutual_fund_snapshots AS t
            SET category_rank = sub.rnk, category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
        # Verify
        check = await pool.fetchrow(
            "SELECT COUNT(*) AS total, COUNT(sub_category_rank) AS ranked FROM discover_mutual_fund_snapshots"
        )
        return {
            "status": "done",
            "sub_category_result": sub_r,
            "category_result": cat_r,
            "total_rows": check["total"],
            "rows_with_sub_rank": check["ranked"],
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@router.get("/jobs")
async def list_jobs(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List all valid job names that can be triggered."""
    _authorize(x_ops_token)
    return {"jobs": sorted(_VALID_JOBS)}


@router.get("/jobs/running")
async def running_jobs(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List currently queued, running, and recently finished ARQ jobs.

    Queries Redis for in-progress jobs, known job IDs, queued jobs,
    and recent results.
    """
    _authorize(x_ops_token)
    from arq.constants import default_queue_name, in_progress_key_prefix
    from arq.jobs import Job, JobStatus

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    running = []
    queued_list = []
    complete = []
    seen_ids: set[str] = set()

    try:
        def _make_job(job_id: str) -> Job:
            return Job(job_id, pool, _queue_name=default_queue_name)

        # 1. Check in-progress jobs via the Redis set
        in_progress_key = in_progress_key_prefix + default_queue_name
        raw_members = await pool.smembers(in_progress_key)

        for raw_id in raw_members or set():
            job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            seen_ids.add(job_id)
            try:
                job = _make_job(job_id)
                info = await job.info()
                if info:
                    running.append({
                        "job_id": job_id,
                        "function": info.function,
                        "status": "running",
                        "enqueue_time": info.enqueue_time.isoformat() if info.enqueue_time else None,
                        "start_time": info.start_time.isoformat() if info.start_time else None,
                        "elapsed_s": round((
                            datetime.now(timezone.utc) - info.start_time
                        ).total_seconds(), 1) if info.start_time else None,
                    })
            except Exception as exc:
                running.append({"job_id": job_id, "status": "running", "error": str(exc)})

        # 2. Check known job IDs (manual triggers + startup + cron jobs)
        known_ids = (
            [f"{name}_manual" for name in _VALID_JOBS]
            + [f"startup_{name}" for name in _VALID_JOBS]
        )

        for job_id in known_ids:
            if job_id in seen_ids:
                continue
            try:
                job = _make_job(job_id)
                status = await job.status()
                if status == JobStatus.not_found:
                    continue

                seen_ids.add(job_id)
                info = await job.info()
                if not info:
                    continue

                entry = {
                    "job_id": job_id,
                    "function": info.function,
                    "status": status.value,
                    "enqueue_time": info.enqueue_time.isoformat() if info.enqueue_time else None,
                }

                if status == JobStatus.in_progress:
                    entry["start_time"] = info.start_time.isoformat() if info.start_time else None
                    entry["elapsed_s"] = round((
                        datetime.now(timezone.utc) - info.start_time
                    ).total_seconds(), 1) if info.start_time else None
                    running.append(entry)
                elif status in (JobStatus.queued, JobStatus.deferred):
                    queued_list.append(entry)
                elif status == JobStatus.complete:
                    try:
                        result_info = await job.result_info()
                        if result_info:
                            entry["finish_time"] = result_info.finish_time.isoformat() if result_info.finish_time else None
                            entry["success"] = result_info.success
                            entry["result"] = str(result_info.result)[:200] if result_info.result is not None else None
                    except Exception:
                        pass
                    complete.append(entry)
            except Exception:
                continue

        # 3. Also include queued jobs from ARQ's queue
        try:
            arq_queued = await pool.queued_jobs()
            for jdef in arq_queued:
                jid = jdef.job_id
                if jid in seen_ids:
                    continue
                seen_ids.add(jid)
                queued_list.append({
                    "job_id": jid,
                    "function": jdef.function,
                    "status": "queued",
                    "enqueue_time": jdef.enqueue_time.isoformat() if jdef.enqueue_time else None,
                })
        except Exception:
            pass

        # 4. Recent job results
        try:
            results = await pool.all_job_results()
            for r in results:
                if r.job_id in seen_ids:
                    continue
                seen_ids.add(r.job_id)
                complete.append({
                    "job_id": r.job_id,
                    "function": r.function,
                    "status": "complete",
                    "enqueue_time": r.enqueue_time.isoformat() if r.enqueue_time else None,
                    "finish_time": r.finish_time.isoformat() if r.finish_time else None,
                    "success": r.success,
                    "result": str(r.result)[:200] if r.result is not None else None,
                })
        except Exception:
            pass

    except Exception as exc:
        import traceback
        return {
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "running": running,
            "queued": queued_list,
            "complete": complete,
        }

    # Include direct-run jobs in the running list
    for name, task in list(_running_direct_jobs.items()):
        if task.done():
            _running_direct_jobs.pop(name, None)
            continue
        running.append({
            "job_id": f"direct-run-{name}",
            "function": name,
            "status": "running",
            "mode": "direct",
        })

    return {
        "running": running,
        "queued": queued_list,
        "complete": complete,
        "summary": {
            "running": len(running),
            "queued": len(queued_list),
            "complete": len(complete),
        },
    }


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — GET (read)
# ═════════════════════════════════════════════════════════════════════

@router.get("/tables")
async def list_tables(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List all table aliases available for CRUD operations."""
    _authorize(x_ops_token)
    return {"tables": sorted(_TABLES.keys())}


@router.get("/table/{table_key}/row/{row_id}")
async def get_row(
    table_key: str = Path(...),
    row_id: str = Path(..., description="UUID of the row"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Fetch a single row by UUID."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    pool = await _get_pool()
    from app.core.database import record_to_dict

    row = await pool.fetchrow(f'SELECT * FROM {table} WHERE id = $1', row_id)  # noqa: S608
    if row is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return {"table": table, "row": record_to_dict(row)}


@router.get("/table/{table_key}/rows")
async def get_rows(
    table_key: str = Path(...),
    column: str | None = Query(default=None, description="Column to filter on"),
    value: str | None = Query(default=None, description="Value to match (exact)"),
    order_by: str = Query(default="id", description="Column to sort by"),
    order: str = Query(default="desc", description="asc or desc"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List rows with optional filter, ordering, and pagination."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    _validate_column(order_by)
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"
    pool = await _get_pool()
    from app.core.database import record_to_dict

    if column and value is not None:
        _validate_column(column)
        rows = await pool.fetch(
            f'SELECT * FROM {table} WHERE {column} = $1 ORDER BY {order_by} {sort_dir} LIMIT $2 OFFSET $3',  # noqa: S608
            value, limit, offset,
        )
        total = await pool.fetchval(
            f'SELECT COUNT(*) FROM {table} WHERE {column} = $1',  # noqa: S608
            value,
        )
    else:
        rows = await pool.fetch(
            f'SELECT * FROM {table} ORDER BY {order_by} {sort_dir} LIMIT $1 OFFSET $2',  # noqa: S608
            limit, offset,
        )
        total = await pool.fetchval(f'SELECT COUNT(*) FROM {table}')  # noqa: S608

    return {
        "table": table,
        "rows": [record_to_dict(r) for r in rows],
        "count": len(rows),
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/table/{table_key}/count")
async def count_rows(
    table_key: str = Path(...),
    column: str | None = Query(default=None, description="Column to filter on"),
    value: str | None = Query(default=None, description="Value to match"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Count rows, optionally filtered by column=value."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    pool = await _get_pool()

    if column and value is not None:
        _validate_column(column)
        total = await pool.fetchval(
            f'SELECT COUNT(*) FROM {table} WHERE {column} = $1',  # noqa: S608
            value,
        )
    else:
        total = await pool.fetchval(f'SELECT COUNT(*) FROM {table}')  # noqa: S608

    return {"table": table, "count": total}


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — POST (insert)
# ═════════════════════════════════════════════════════════════════════

@router.post("/table/{table_key}/row")
async def insert_row(
    table_key: str = Path(...),
    payload: dict = Body(..., description="Column-value pairs to insert"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Insert a single row. Pass JSON body with column: value pairs."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    if not payload:
        raise HTTPException(status_code=400, detail="Empty payload")

    columns = []
    placeholders = []
    values = []
    for i, (col, val) in enumerate(payload.items(), start=1):
        _validate_column(col)
        columns.append(col)
        placeholders.append(f"${i}")
        values.append(val)

    col_str = ", ".join(columns)
    ph_str = ", ".join(placeholders)
    pool = await _get_pool()
    from app.core.database import record_to_dict

    row = await pool.fetchrow(
        f'INSERT INTO {table} ({col_str}) VALUES ({ph_str}) RETURNING *',  # noqa: S608
        *values,
    )
    logger.info("Ops insert: table=%s columns=%s", table, columns)
    return {"table": table, "row": record_to_dict(row) if row else None}


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — PUT/PATCH (update)
# ═════════════════════════════════════════════════════════════════════

@router.put("/table/{table_key}/row/{row_id}")
async def update_row(
    table_key: str = Path(...),
    row_id: str = Path(..., description="UUID of the row to update"),
    payload: dict = Body(..., description="Column-value pairs to update"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Update a single row by UUID. Pass JSON body with column: value pairs to set."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    if not payload:
        raise HTTPException(status_code=400, detail="Empty payload")

    set_clauses = []
    values = []
    for i, (col, val) in enumerate(payload.items(), start=1):
        _validate_column(col)
        set_clauses.append(f"{col} = ${i}")
        values.append(val)

    set_str = ", ".join(set_clauses)
    id_placeholder = f"${len(values) + 1}"
    values.append(row_id)

    pool = await _get_pool()
    from app.core.database import record_to_dict

    row = await pool.fetchrow(
        f'UPDATE {table} SET {set_str} WHERE id = {id_placeholder} RETURNING *',  # noqa: S608
        *values,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Row not found")
    logger.info("Ops update: table=%s row_id=%s columns=%s", table, row_id, list(payload.keys()))
    return {"table": table, "row": record_to_dict(row)}


@router.patch("/table/{table_key}/rows")
async def update_rows_by_filter(
    table_key: str = Path(...),
    column: str = Query(..., description="Column to filter on"),
    value: str = Query(..., description="Value to match"),
    payload: dict = Body(..., description="Column-value pairs to update"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Bulk update rows matching column=value filter."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    _validate_column(column)
    if not payload:
        raise HTTPException(status_code=400, detail="Empty payload")

    set_clauses = []
    values = []
    for i, (col, val) in enumerate(payload.items(), start=1):
        _validate_column(col)
        set_clauses.append(f"{col} = ${i}")
        values.append(val)

    set_str = ", ".join(set_clauses)
    filter_placeholder = f"${len(values) + 1}"
    values.append(value)

    pool = await _get_pool()
    status = await pool.execute(
        f'UPDATE {table} SET {set_str} WHERE {column} = {filter_placeholder}',  # noqa: S608
        *values,
    )
    updated = int(str(status).split()[-1])
    logger.info("Ops bulk update: table=%s %s=%s updated=%d columns=%s", table, column, value, updated, list(payload.keys()))
    return {"table": table, "updated": updated, "filter": {column: value}}


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — DELETE
# ═════════════════════════════════════════════════════════════════════

@router.delete("/table/{table_key}/row/{row_id}")
async def delete_row(
    table_key: str = Path(...),
    row_id: str = Path(..., description="UUID of the row to delete"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Delete a specific row by UUID."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    pool = await _get_pool()
    status = await pool.execute(
        f'DELETE FROM {table} WHERE id = $1',  # noqa: S608
        row_id,
    )
    deleted = int(str(status).split()[-1])
    logger.info("Ops delete: table=%s row_id=%s deleted=%d", table, row_id, deleted)
    return {"deleted": deleted, "table": table, "row_id": row_id}


@router.delete("/table/{table_key}/rows")
async def delete_rows_by_filter(
    table_key: str = Path(...),
    column: str = Query(..., description="Column to filter on"),
    value: str = Query(..., description="Value to match (exact)"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Delete rows matching column=value filter."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    _validate_column(column)
    pool = await _get_pool()
    status = await pool.execute(
        f'DELETE FROM {table} WHERE {column} = $1',  # noqa: S608
        value,
    )
    deleted = int(str(status).split()[-1])
    logger.info("Ops bulk delete: table=%s %s=%s deleted=%d", table, column, value, deleted)
    return {"deleted": deleted, "table": table, "filter": {column: value}}


@router.delete("/table/{table_key}/truncate")
async def truncate_table(
    table_key: str = Path(...),
    confirm: str = Query(..., description="Must be 'yes' to confirm truncation"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Truncate (empty) an entire table. Requires confirm=yes."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    if confirm != "yes":
        raise HTTPException(status_code=400, detail="Pass confirm=yes to truncate")
    pool = await _get_pool()
    await pool.execute(f'TRUNCATE {table}')  # noqa: S608
    logger.warning("Ops truncate: table=%s", table)
    return {"truncated": True, "table": table}


# ═════════════════════════════════════════════════════════════════════
# SQL (read-only)
# ═════════════════════════════════════════════════════════════════════

@router.post("/sql")
async def execute_sql(
    payload: dict = Body(..., description='{"query": "SELECT ...", "params": []}'),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Execute a read-only SQL query. Only SELECT/WITH statements allowed."""
    _authorize(x_ops_token)
    query = str(payload.get("query", "")).strip()
    params = payload.get("params") or []

    if not query:
        raise HTTPException(status_code=400, detail="Empty query")

    first_word = query.split()[0].upper() if query.split() else ""
    _READ_ONLY = {"SELECT", "WITH", "EXPLAIN"}
    _WRITE_ALLOWED = {"ALTER", "CREATE", "DROP", "UPDATE", "INSERT", "DELETE"}

    pool = await _get_pool()
    from app.core.database import record_to_dict

    try:
        if first_word in _READ_ONLY:
            rows = await pool.fetch(query, *params)
            return {
                "rows": [record_to_dict(r) for r in rows],
                "count": len(rows),
            }
        elif first_word in _WRITE_ALLOWED:
            result = await pool.execute(query, *params)
            return {"status": "ok", "result": result}
        else:
            raise HTTPException(status_code=400, detail=f"Statement type '{first_word}' not allowed")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ═════════════════════════════════════════════════════════════════════
# Config / Debug
# ═════════════════════════════════════════════════════════════════════

@router.get("/macro-config")
async def macro_config(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Return current FRED_CPI / FRED_DIRECT config for debugging."""
    _authorize(x_ops_token)
    from app.scheduler.macro_job import FRED_CPI, FRED_DIRECT, VALUE_RANGES

    return {
        "fred_cpi": FRED_CPI,
        "fred_direct": {k: [(n, s) for n, s in v] for k, v in FRED_DIRECT.items()},
        "value_ranges": VALUE_RANGES,
    }


@router.get("/data-health", response_model=DataHealthResponse)
async def data_health(
    x_ops_token: str | None = Header(default=None),
) -> DataHealthResponse:
    _authorize(x_ops_token)
    payload = await market_intel_service.get_data_health()
    return DataHealthResponse(**payload)
