import logging

from fastapi import APIRouter, HTTPException, Query

from app.schemas.ingest_schema import IngestAck, NewsIngestPayload
from app.schemas.news_schema import NewsArticleListResponse, NewsArticleResponse
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


@router.get("", response_model=NewsArticleListResponse)
async def list_news(
    entity: str | None = Query(default=None, description="Filter by primary_entity (e.g. gold, sp500)"),
    impact: str | None = Query(default=None, description="Filter by impact (e.g. risk_on, inflation_signal)"),
    source: str | None = Query(default=None, description="Filter by news source"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> NewsArticleListResponse:
    """Return news articles with optional filters."""
    try:
        rows = await news_service.get_articles(
            entity=entity,
            impact=impact,
            source=source,
            limit=limit,
            offset=offset,
        )
        articles = [NewsArticleResponse(**r) for r in rows]
        return NewsArticleListResponse(articles=articles, count=len(articles))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
