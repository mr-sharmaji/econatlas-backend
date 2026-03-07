from fastapi import APIRouter, HTTPException

from app.schemas.ingest_schema import CommodityIngestPayload, IngestAck
from app.services import event_service, market_service

router = APIRouter(prefix="/commodities", tags=["commodities"])


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_commodity(payload: CommodityIngestPayload) -> IngestAck:
    """Receive normalized commodity record from scraper and store as event."""
    try:
        await market_service.insert_price(
            asset=payload.asset.lower(),
            price=payload.price_usd,
            timestamp=payload.timestamp.isoformat(),
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
