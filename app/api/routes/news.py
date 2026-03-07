from fastapi import APIRouter, HTTPException

from app.schemas.ingest_schema import IngestAck, NewsIngestPayload
from app.services import event_service

router = APIRouter(prefix="/news", tags=["news"])


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_news(payload: NewsIngestPayload) -> IngestAck:
    """Receive normalized news item from scraper and store as event."""
    try:
        row = await event_service.insert_event_dict(
            {
                "event_type": "financial_news_signal",
                "entity": payload.primary_entity,
                "impact": payload.impact,
                "confidence": payload.confidence,
            }
        )
        return IngestAck(route="/news", event_id=row["id"])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
