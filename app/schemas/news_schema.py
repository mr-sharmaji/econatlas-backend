from datetime import datetime

from pydantic import BaseModel


class NewsArticleResponse(BaseModel):
    """Single news article row."""

    id: str
    title: str
    summary: str | None = None
    body: str | None = None
    timestamp: datetime
    source: str | None = None
    url: str | None = None
    primary_entity: str | None = None
    impact: str | None = None
    confidence: float | None = None


class NewsArticleListResponse(BaseModel):
    """Wrapper for a list of news articles."""

    articles: list[NewsArticleResponse]
    count: int
