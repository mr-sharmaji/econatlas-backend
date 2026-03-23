import logging
import math

from fastapi import APIRouter, HTTPException, Query

from app.schemas.macro_schema import (
    EconCalendarResponse,
    MacroLinkagesResponse,
    MacroMetadataResponse,
    MacroRegimeResponse,
    MacroSummaryResponse,
    InstitutionalFlowsOverviewResponse,
    MacroForecastListResponse,
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


@router.get("/forecasts", response_model=MacroForecastListResponse)
async def list_forecasts(
    country: str | None = Query(default=None),
    indicator: str | None = Query(default=None),
) -> MacroForecastListResponse:
    """Return IMF WEO forecast projections."""
    try:
        rows = await macro_service.get_forecasts(country=country, indicator=indicator)
        return MacroForecastListResponse(forecasts=rows, count=len(rows))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/calendar", response_model=EconCalendarResponse)
async def list_calendar(
    days_ahead: int = Query(default=90, ge=1, le=365),
    country: str | None = Query(default=None),
    include_past: bool = Query(default=False, description="Include recent past events for replay/timeline modules."),
) -> EconCalendarResponse:
    """Return upcoming economic events."""
    try:
        events = await macro_service.get_upcoming_events(
            days_ahead=days_ahead,
            country=country,
            include_past=include_past,
        )
        return EconCalendarResponse(events=events, count=len(events))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/metadata", response_model=MacroMetadataResponse)
async def macro_metadata() -> MacroMetadataResponse:
    """Indicator metadata for display names, units, cadence, and chart hints."""
    try:
        items = await macro_service.get_metadata()
        return MacroMetadataResponse(items=items, count=len(items))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/regime", response_model=MacroRegimeResponse)
async def macro_regime(
    country: str | None = Query(default=None),
) -> MacroRegimeResponse:
    """Normalized growth/inflation/policy regime map per country."""
    try:
        payload = await macro_service.get_regime(country=country)
        countries = payload.get("countries", [])
        return MacroRegimeResponse(
            as_of=payload.get("as_of"),
            countries=countries,
            count=len(countries),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/linkages", response_model=MacroLinkagesResponse)
async def macro_linkages(
    country: str = Query(default="IN"),
    indicator: str = Query(default="inflation"),
    window_days: int = Query(default=365, ge=30, le=3650),
) -> MacroLinkagesResponse:
    """Rolling macro-to-market linkage snapshots for compare view."""
    try:
        payload = await macro_service.get_linkages(
            country=country,
            indicator_name=indicator,
            window_days=window_days,
        )
        series = payload.get("series", [])
        return MacroLinkagesResponse(
            country=payload.get("country", country),
            indicator_name=payload.get("indicator_name", indicator),
            window_days=payload.get("window_days", window_days),
            as_of=payload.get("as_of"),
            series=series,
            count=len(series),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/summary", response_model=MacroSummaryResponse)
async def macro_summary(
    country: str | None = Query(default=None),
) -> MacroSummaryResponse:
    """Country-level Now/Next/Risk summary payload optimized for fast paint."""
    try:
        payload = await macro_service.get_summary(country=country)
        countries = payload.get("countries", [])
        return MacroSummaryResponse(
            as_of=payload.get("as_of"),
            countries=countries,
            count=len(countries),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
