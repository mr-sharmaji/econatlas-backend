from fastapi import APIRouter, HTTPException, Query

from app.schemas.ingest_schema import IngestAck, MarketIngestPayload
from app.schemas.market_schema import MarketPriceListResponse, MarketPriceResponse
from app.services import event_service, market_service

router = APIRouter(prefix="/market", tags=["market"])


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_market(payload: MarketIngestPayload) -> IngestAck:
    """Receive normalized market records from scraper and store as events."""
    try:
        entity_map = {
            "index": payload.name,
            "currency": payload.pair,
            "bond_yield": payload.instrument,
        }
        entity = entity_map.get(payload.type)
        if not entity:
            raise HTTPException(status_code=422, detail="Missing entity for market payload")

        if payload.type == "bond_yield":
            price_value = payload.yield_percent
        else:
            price_value = payload.value
        if price_value is None:
            raise HTTPException(status_code=422, detail="Missing numeric value for market payload")
        default_unit_by_type = {
            "index": "points",
            "currency": "fx_rate",
            "bond_yield": "percent",
        }
        unit_value = payload.unit or default_unit_by_type[payload.type]

        await market_service.insert_price(
            asset=entity,
            price=float(price_value),
            timestamp=payload.timestamp.isoformat(),
            source=payload.source,
            instrument_type=payload.type,
            unit=unit_value,
        )

        impact = "macro_signal" if payload.type == "bond_yield" else "market_signal"
        row = await event_service.insert_event_dict(
            {
                "event_type": f"{payload.type}_update",
                "entity": entity,
                "impact": impact,
                "confidence": 0.7,
            }
        )
        return IngestAck(route="/market", event_id=row["id"])
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=MarketPriceListResponse)
async def list_market_prices(
    instrument_type: str | None = Query(default=None, description="index, currency, or bond_yield"),
    asset: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> MarketPriceListResponse:
    """Return market prices (indices, currencies, bond yields) with optional filters."""
    try:
        rows = await market_service.get_prices(
            instrument_type=instrument_type,
            asset=asset,
            limit=limit,
            offset=offset,
        )
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/latest", response_model=MarketPriceListResponse)
async def latest_market_prices(
    instrument_type: str | None = Query(default=None, description="index, currency, or bond_yield"),
) -> MarketPriceListResponse:
    """Return the most recent price for each asset (de-duplicated)."""
    try:
        rows = await market_service.get_latest_prices(instrument_type=instrument_type)
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
