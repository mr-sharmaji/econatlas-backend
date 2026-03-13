"""Admin endpoints for the dead-letter queue (DLQ)."""
from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query

from app.core.config import get_settings
from app.queue.dlq import dismiss_dead_letter, get_dead_letters, retry_dead_letter
from app.queue.redis_pool import get_redis_pool

router = APIRouter(prefix="/ops/dlq", tags=["ops"])


def _authorize(x_ops_token: str | None) -> None:
    settings = get_settings()
    required_token = settings.ops_logs_token
    if required_token and x_ops_token != required_token:
        raise HTTPException(status_code=403, detail="Invalid ops token")


@router.get("/")
async def list_dead_letters(
    status: str = Query(default="dead", description="Filter by status: dead | retried | dismissed"),
    job_name: str | None = Query(default=None, description="Filter by job name"),
    limit: int = Query(default=50, ge=1, le=500),
    x_ops_token: str | None = Header(default=None),
):
    """List dead-lettered jobs, newest first."""
    _authorize(x_ops_token)
    entries = await get_dead_letters(status=status, limit=limit, job_name=job_name)
    return {"entries": entries, "count": len(entries)}


@router.post("/{dlq_id}/retry")
async def retry_dlq_entry(
    dlq_id: str,
    x_ops_token: str | None = Header(default=None),
):
    """Re-enqueue a dead-lettered job and mark it as retried."""
    _authorize(x_ops_token)
    pool = await get_redis_pool()
    ok = await retry_dead_letter(dlq_id, pool)
    if not ok:
        raise HTTPException(status_code=404, detail="DLQ entry not found or already retried")
    return {"status": "retried", "id": dlq_id}


@router.post("/{dlq_id}/dismiss")
async def dismiss_dlq_entry(
    dlq_id: str,
    x_ops_token: str | None = Header(default=None),
):
    """Acknowledge and dismiss a dead-lettered job."""
    _authorize(x_ops_token)
    ok = await dismiss_dead_letter(dlq_id)
    if not ok:
        raise HTTPException(status_code=404, detail="DLQ entry not found or already dismissed")
    return {"status": "dismissed", "id": dlq_id}
