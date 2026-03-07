import logging

from fastapi import APIRouter, HTTPException

from app.schemas.ingest_schema import IngestAck, NewsIngestPayload
from app.services import event_service, news_service

router = APIRouter(prefix="/news", tags=["news"])
logger = logging.getLogger(__name__)


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_news(payload: NewsIngestPayload) -> IngestAck:
    """Receive normalized news item from scraper and store article + event."""
    try:
        article_id: str | None = None
        try:
            article_row = await news_service.upsert_article(payload.model_dump(mode="json"))
            article_id = article_row.get("id")
        except Exception as exc:
            # Keep ingestion alive even before news_articles table is provisioned.
            logger.warning("News article persistence failed, continuing with event only: %s", str(exc))

        row = await event_service.insert_event_dict(
            {
                "event_type": "financial_news_signal",
                "entity": payload.primary_entity,
                "impact": payload.impact,
                "confidence": payload.confidence,
            }
        )
        return IngestAck(route="/news", event_id=row["id"], article_id=article_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
