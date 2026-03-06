from fastapi import APIRouter, HTTPException, Query

from app.schemas.macro_schema import MacroIndicatorListResponse, MacroIndicatorResponse
from app.services import macro_service

router = APIRouter(prefix="/macro", tags=["macro"])


@router.get("", response_model=MacroIndicatorListResponse)
async def list_macro_indicators(
    country: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> MacroIndicatorListResponse:
    """Return the most recent macro-economic indicators."""
    try:
        rows = await macro_service.get_indicators(
            country=country, limit=limit, offset=offset
        )
        indicators = [MacroIndicatorResponse(**r) for r in rows]
        return MacroIndicatorListResponse(
            indicators=indicators, count=len(indicators)
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
