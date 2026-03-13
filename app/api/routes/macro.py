import logging
import math

from fastapi import APIRouter, HTTPException, Query

from app.schemas.macro_schema import (
    InstitutionalFlowsOverviewResponse,
    MacroIndicatorCreate,
    MacroIndicatorListResponse,
    MacroIndicatorResponse,
)
from app.services import macro_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/macro", tags=["macro"])

# Value ranges for sanity checking incoming macro data (same as macro_job.py).
_VALUE_RANGES: dict[str, tuple[float, float]] = {
    "inflation": (-20.0, 40.0),
    "gdp_growth": (-50.0, 50.0),
    "unemployment": (0.0, 100.0),
    "repo_rate": (-5.0, 50.0),
    "fii_net_cash": (-1_000_000.0, 1_000_000.0),
    "dii_net_cash": (-1_000_000.0, 1_000_000.0),
}


def _is_value_valid(indicator_name: str, value: float) -> bool:
    if not math.isfinite(value):
        return False
    low, high = _VALUE_RANGES.get(indicator_name, (-1e12, 1e12))
    return low <= value <= high


@router.post("", response_model=MacroIndicatorResponse, status_code=200)
async def create_macro_indicator(payload: MacroIndicatorCreate) -> MacroIndicatorResponse:
    """Receive a macro-economic indicator from scraper. Idempotent: same row returned if already exists."""
    try:
        # Reject out-of-range values at the API layer
        if not _is_value_valid(payload.indicator_name, payload.value):
            logger.warning(
                "Macro POST rejected out-of-range: %s/%s value=%s",
                payload.country, payload.indicator_name, payload.value,
            )
            raise HTTPException(
                status_code=422,
                detail=f"Value {payload.value} out of range for {payload.indicator_name}",
            )
        row = await macro_service.insert_indicator(payload.model_dump(mode="json"))
        if row is not None:
            return MacroIndicatorResponse(**row)
        existing = await macro_service.get_existing_indicator(
            payload.indicator_name,
            payload.country,
            payload.timestamp,
        )
        if existing is not None:
            return MacroIndicatorResponse(**existing)
        raise HTTPException(status_code=500, detail="Insert conflict but row not found")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=MacroIndicatorListResponse)
async def list_macro_indicators(
    country: str | None = Query(default=None),
    limit: int = Query(default=50, ge=-1, description="Max rows. Use -1 for all."),
    offset: int = Query(default=0, ge=0),
    latest_only: bool = Query(default=False, description="Return latest value per (indicator_name, country) for list views."),
) -> MacroIndicatorListResponse:
    """Return macro-economic indicators. Use latest_only=true for one row per indicator per country."""
    try:
        if latest_only:
            rows = await macro_service.get_indicators_latest(country=country)
            indicators = [MacroIndicatorResponse(**r) for r in rows]
            return MacroIndicatorListResponse(indicators=indicators, count=len(indicators))
        effective_limit = 100_000 if limit == -1 else limit
        rows = await macro_service.get_indicators(
            country=country, limit=effective_limit, offset=offset
        )
        indicators = [MacroIndicatorResponse(**r) for r in rows]
        return MacroIndicatorListResponse(
            indicators=indicators, count=len(indicators)
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/flows/overview", response_model=InstitutionalFlowsOverviewResponse)
async def institutional_flows_overview(
    sessions: int = Query(default=7, ge=3, le=30),
) -> InstitutionalFlowsOverviewResponse:
    try:
        payload = await macro_service.get_institutional_flows_overview(sessions=sessions)
        return InstitutionalFlowsOverviewResponse(**payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
