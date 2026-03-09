from fastapi import APIRouter, HTTPException, Query

from app.schemas.market_intel_schema import AssetCatalogItemResponse, AssetCatalogResponse
from app.services import market_intel_service

router = APIRouter(prefix="/assets", tags=["assets"])


@router.get("/catalog", response_model=AssetCatalogResponse)
async def get_asset_catalog(
    region: str | None = Query(default=None, description="Region filter (India, US, Europe, Japan, FX, Commodities)"),
    instrument_type: str | None = Query(default=None, description="index, currency, commodity, bond_yield"),
) -> AssetCatalogResponse:
    try:
        rows = await market_intel_service.get_asset_catalog(region=region, instrument_type=instrument_type)
        assets = [AssetCatalogItemResponse(**r) for r in rows]
        return AssetCatalogResponse(assets=assets, count=len(assets))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
