from datetime import datetime

from pydantic import BaseModel


class MarketPriceResponse(BaseModel):
    """Single market price row."""

    id: str
    asset: str
    price: float
    timestamp: datetime
    source: str | None = None
    instrument_type: str | None = None
    unit: str | None = None


class MarketPriceListResponse(BaseModel):
    """Wrapper for a list of market prices."""

    prices: list[MarketPriceResponse]
    count: int
