from fastapi import APIRouter, Header, HTTPException, Query

from app.core.config import get_settings
from app.core.log_stream import get_log_entries
from app.schemas.market_intel_schema import DataHealthResponse
from app.schemas.ops_schema import LogEntryResponse, LogListResponse
from app.services import market_intel_service

router = APIRouter(prefix="/ops", tags=["ops"])


def _authorize(x_ops_token: str | None) -> None:
    settings = get_settings()
    if not settings.ops_logs_enabled:
        raise HTTPException(status_code=404, detail="Ops logs endpoint disabled")
    required_token = settings.ops_logs_token
    if required_token and x_ops_token != required_token:
        raise HTTPException(status_code=403, detail="Invalid ops token")


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


@router.get("/data-health", response_model=DataHealthResponse)
async def data_health(
    x_ops_token: str | None = Header(default=None),
) -> DataHealthResponse:
    _authorize(x_ops_token)
    payload = await market_intel_service.get_data_health()
    return DataHealthResponse(**payload)
