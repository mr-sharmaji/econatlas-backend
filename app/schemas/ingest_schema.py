from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class IngestAck(BaseModel):
    status: str = "accepted"
    route: str
    event_id: str | None = None
    article_id: str | None = None


class MarketIngestPayload(BaseModel):
    type: Literal["index", "currency", "bond_yield"]
    name: str | None = None
    pair: str | None = None
    instrument: str | None = None
    value: float | None = None
    yield_percent: float | None = None
    unit: str | None = None
    timestamp: datetime
    source: str | None = None
    change_percent: float | None = None
    previous_close: float | None = None


class CommodityIngestPayload(BaseModel):
    asset: str = Field(..., min_length=1)
    price_usd: float = Field(..., gt=0)
    timestamp: datetime
    source: str | None = None
    original_value: float | None = None
    original_currency: str | None = None
    fx_rate_used: float | None = None
    change_percent: float | None = None
    previous_close: float | None = None


class CryptoIngestPayload(BaseModel):
    asset: str = Field(..., min_length=1)
    price_usd: float = Field(..., gt=0)
    timestamp: datetime
    source: str | None = None
    change_percent: float | None = None
    previous_close: float | None = None


class NewsIngestPayload(BaseModel):
    title: str = Field(..., min_length=1)
    summary: str | None = None
    body: str | None = None
    timestamp: datetime
    source: str | None = None
    url: str | None = None
    primary_entity: str = "market_news"
    impact: str = "market_signal"
    confidence: float = Field(default=0.55, ge=0.0, le=1.0)
