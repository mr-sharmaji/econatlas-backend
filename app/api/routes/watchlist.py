from fastapi import APIRouter, HTTPException, Query

from app.schemas.market_intel_schema import WatchlistResponse, WatchlistUpdateRequest
from app.services import market_intel_service

router = APIRouter(prefix="/watchlist", tags=["watchlist"])


@router.get("", response_model=WatchlistResponse)
async def get_watchlist(
    device_id: str = Query(..., min_length=6, description="Device identifier (client-generated UUID)"),
) -> WatchlistResponse:
    try:
        assets = await market_intel_service.get_or_seed_watchlist(device_id=device_id.strip())
        return WatchlistResponse(device_id=device_id.strip(), assets=assets, count=len(assets))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("", response_model=WatchlistResponse)
async def put_watchlist(
    payload: WatchlistUpdateRequest,
    device_id: str = Query(..., min_length=6, description="Device identifier (client-generated UUID)"),
) -> WatchlistResponse:
    try:
        assets = await market_intel_service.put_watchlist(device_id=device_id.strip(), assets=payload.assets)
        return WatchlistResponse(device_id=device_id.strip(), assets=assets, count=len(assets))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
