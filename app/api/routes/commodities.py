from fastapi import APIRouter, HTTPException, Query

from app.schemas.ingest_schema import CommodityIngestPayload, IngestAck
from app.schemas.market_schema import (
    IntradayPointResponse,
    IntradayResponse,
    MarketPriceListResponse,
    MarketPriceResponse,
)
from app.services import event_service, market_service

router = APIRouter(prefix="/commodities", tags=["commodities"])

COMMODITY_UNITS = {
    "gold": "usd_per_troy_ounce",
    "silver": "usd_per_troy_ounce",
    "crude oil": "usd_per_barrel",
    "natural gas": "usd_per_mmbtu",
    "copper": "usd_per_pound",
}


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_commodity(payload: CommodityIngestPayload) -> IngestAck:
    """Receive normalized commodity record from scraper and store as event."""
    try:
        await market_service.insert_price(
            asset=payload.asset.lower(),
            price=payload.price_usd,
            timestamp=payload.timestamp.isoformat(),
            source=payload.source,
            instrument_type="commodity",
            unit=COMMODITY_UNITS.get(payload.asset.lower(), "usd"),
            change_percent=payload.change_percent,
            previous_close=payload.previous_close,
        )

        row = await event_service.insert_event_dict(
            {
                "event_type": "commodity_price_change",
                "entity": payload.asset.lower(),
                "impact": "market_signal",
                "confidence": 0.72,
            }
        )
        return IngestAck(route="/commodities", event_id=row["id"])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=MarketPriceListResponse)
async def list_commodities(
    asset: str | None = Query(default=None, description="gold, silver, crude oil, natural gas, copper"),
    limit: int = Query(default=50, ge=-1, description="Max rows. Use -1 for all."),
    offset: int = Query(default=0, ge=0),
) -> MarketPriceListResponse:
    """Return commodity prices with optional asset filter."""
    try:
        effective_limit = 100_000 if limit == -1 else limit
        rows = await market_service.get_prices(
            instrument_type="commodity",
            asset=asset,
            limit=effective_limit,
            offset=offset,
        )
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/intraday", response_model=IntradayResponse)
async def get_commodity_intraday(
    asset: str = Query(..., description="Commodity asset (e.g. Gold, Silver)"),
) -> IntradayResponse:
    """Return commodity intraday points for chart (rolling 24h window)."""
    try:
        rows = await market_service.get_intraday(asset=asset, instrument_type="commodity")
        prices = [IntradayPointResponse(timestamp=r["timestamp"], price=r["price"]) for r in rows]
        return IntradayResponse(prices=prices)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/latest", response_model=MarketPriceListResponse)
async def latest_commodities() -> MarketPriceListResponse:
    """Return the most recent price for each commodity asset."""
    try:
        rows = await market_service.get_latest_prices(instrument_type="commodity")
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
