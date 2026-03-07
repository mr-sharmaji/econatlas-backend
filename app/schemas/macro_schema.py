from datetime import datetime

from pydantic import BaseModel, Field


class MacroIndicatorCreate(BaseModel):
    """Payload accepted when creating a macro-economic indicator."""

    indicator_name: str = Field(..., min_length=1)
    value: float
    country: str = Field(..., min_length=2, max_length=3)
    timestamp: datetime
    unit: str | None = None
    source: str | None = None


class MacroIndicatorResponse(BaseModel):
    """Single macro-economic indicator row."""

    id: str
    indicator_name: str
    value: float
    country: str
    timestamp: datetime


class MacroIndicatorListResponse(BaseModel):
    """Wrapper for a list of macro indicators."""

    indicators: list[MacroIndicatorResponse]
    count: int
