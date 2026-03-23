from datetime import date, datetime

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


class InstitutionalFlowTrendPoint(BaseModel):
    session_date: date
    fii_value: float | None = None
    dii_value: float | None = None
    combined_value: float
    as_of: datetime | None = None


class InstitutionalFlowsOverviewResponse(BaseModel):
    as_of: datetime | None = None
    fii_value: float | None = None
    dii_value: float | None = None
    combined_value: float | None = None
    trend: list[InstitutionalFlowTrendPoint] = Field(default_factory=list)


# ── Forecasts ──

class MacroForecastResponse(BaseModel):
    indicator_name: str
    country: str
    forecast_year: int
    value: float
    source: str | None = None
    fetched_at: datetime | None = None


class MacroForecastListResponse(BaseModel):
    forecasts: list[MacroForecastResponse]
    count: int


# ── Economic Calendar ──

class EconCalendarEventResponse(BaseModel):
    event_name: str
    institution: str
    event_date: date
    country: str
    event_type: str
    description: str | None = None
    source: str | None = None


class EconCalendarResponse(BaseModel):
    events: list[EconCalendarEventResponse]
    count: int
