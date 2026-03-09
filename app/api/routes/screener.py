from fastapi import APIRouter, HTTPException, Query

from app.schemas.market_intel_schema import ScreenerItemResponse, ScreenerResponse
from app.services import market_intel_service

router = APIRouter(prefix="/screener", tags=["screener"])


@router.get("", response_model=ScreenerResponse)
async def get_screener(
    preset: str = Query(default="momentum", description="momentum|reversal|volatility|macro-sensitive"),
    region: str | None = Query(default=None, description="Region filter (India, US, Europe, Japan, FX, Commodities)"),
    instrument_type: str | None = Query(default=None, description="index, currency, commodity, bond_yield"),
    limit: int = Query(default=25, ge=1, le=200),
    min_quality: float = Query(default=0.0, ge=0.0, le=1.0),
) -> ScreenerResponse:
    try:
        rows = await market_intel_service.get_screener(
            preset=preset,
            region=region,
            instrument_type=instrument_type,
            limit=limit,
            min_quality=min_quality,
        )
        items = [ScreenerItemResponse(**r) for r in rows]
        return ScreenerResponse(
            preset=preset,
            region=region,
            instrument_type=instrument_type,
            min_quality=min_quality,
            items=items,
            count=len(items),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
