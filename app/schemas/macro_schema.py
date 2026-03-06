from datetime import datetime

from pydantic import BaseModel


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
