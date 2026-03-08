from fastapi import APIRouter, HTTPException, Query

from app.schemas.macro_schema import (
    MacroIndicatorCreate,
    MacroIndicatorListResponse,
    MacroIndicatorResponse,
)
from app.services import macro_service

router = APIRouter(prefix="/macro", tags=["macro"])


@router.post("", response_model=MacroIndicatorResponse, status_code=201)
async def create_macro_indicator(payload: MacroIndicatorCreate) -> MacroIndicatorResponse:
    """Receive a macro-economic indicator from scraper."""
    try:
        row = await macro_service.insert_indicator(payload.model_dump(mode="json"))
        return MacroIndicatorResponse(**row)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=MacroIndicatorListResponse)
async def list_macro_indicators(
    country: str | None = Query(default=None),
    limit: int = Query(default=50, ge=-1, description="Max rows. Use -1 for all."),
    offset: int = Query(default=0, ge=0),
) -> MacroIndicatorListResponse:
    """Return the most recent macro-economic indicators."""
    try:
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
