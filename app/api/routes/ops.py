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

# Valid job names that can be triggered manually.
_VALID_JOBS = {
    "market", "commodity", "crypto", "brief", "macro", "news",
    "discover_stock", "discover_mutual_funds",
    "discover_stock_price", "discover_mf_nav",
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

@router.post("/jobs/trigger/{job_name}")
async def trigger_job(
    job_name: str = Path(..., description="Name of the job to trigger"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Manually enqueue a background job for immediate execution."""
    _authorize(x_ops_token)
    if job_name not in _VALID_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job '{job_name}'. Valid: {sorted(_VALID_JOBS)}",
        )
    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    job = await pool.enqueue_job(job_name, _job_id=f"{job_name}_manual")
    logger.info("Manually triggered job: %s (job_id=%s)", job_name, job.job_id if job else "deduped")
    return {
        "status": "enqueued" if job else "already_queued",
        "job_name": job_name,
        "job_id": job.job_id if job else f"{job_name}_manual",
    }


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

    Queries Redis for all known job keys and returns their status,
    start time, and result info.
    """
    _authorize(x_ops_token)
    from arq.constants import default_queue_name, in_progress_key_prefix
    from arq.jobs import JobStatus

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    running = []
    queued = []
    complete = []

    # 1. Check in-progress jobs via the in_progress set
    in_progress_key = in_progress_key_prefix + default_queue_name
    in_progress_ids: set[bytes] = await pool.smembers(in_progress_key)  # type: ignore[assignment]

    for raw_id in in_progress_ids:
        job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        job = await pool.job(job_id)  # type: ignore[arg-type]
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

    # 2. Check known job IDs (manual triggers + startup jobs)
    known_ids = [f"{name}_manual" for name in _VALID_JOBS] + [f"startup_{name}" for name in _VALID_JOBS]

    for job_id in known_ids:
        # Skip if already found in in-progress set
        if any(r["job_id"] == job_id for r in running):
            continue
        job = await pool.job(job_id)  # type: ignore[arg-type]
        status = await job.status()
        if status == JobStatus.not_found:
            continue

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
        elif status == JobStatus.queued or status == JobStatus.deferred:
            entry["score"] = info.score if hasattr(info, "score") else None
            queued.append(entry)
        elif status == JobStatus.complete:
            entry["finish_time"] = info.finish_time.isoformat() if info.finish_time else None
            entry["success"] = info.success
            entry["result"] = str(info.result)[:200] if info.result is not None else None
            complete.append(entry)

    return {
        "running": running,
        "queued": queued,
        "complete": complete,
        "summary": {
            "running": len(running),
            "queued": len(queued),
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
    # Only allow read-only statements
    first_word = query.split()[0].upper() if query.split() else ""
    if first_word not in ("SELECT", "WITH", "EXPLAIN"):
        raise HTTPException(status_code=400, detail="Only SELECT/WITH/EXPLAIN queries allowed")

    pool = await _get_pool()
    from app.core.database import record_to_dict

    try:
        rows = await pool.fetch(query, *params)
        return {
            "rows": [record_to_dict(r) for r in rows],
            "count": len(rows),
        }
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
