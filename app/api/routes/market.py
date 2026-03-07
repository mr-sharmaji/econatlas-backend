from fastapi import APIRouter, HTTPException

from app.schemas.ingest_schema import IngestAck, MarketIngestPayload
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

        await market_service.insert_price(
            asset=entity,
            price=float(price_value),
            timestamp=payload.timestamp.isoformat(),
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
