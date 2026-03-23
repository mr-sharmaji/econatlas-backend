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
    importance: str | None = None
    previous: float | None = None
    consensus: float | None = None
    actual: float | None = None
    surprise: float | None = None
    status: str | None = None


class EconCalendarResponse(BaseModel):
    events: list[EconCalendarEventResponse]
    count: int


# ── Metadata ──

class MacroIndicatorMetadataResponse(BaseModel):
    indicator_name: str
    display_name: str
    helper_text: str | None = None
    unit: str
    frequency: str
    source: str
    update_cadence: str
    chart_type: str
    thresholds: dict[str, float] = Field(default_factory=dict)


class MacroMetadataResponse(BaseModel):
    items: list[MacroIndicatorMetadataResponse]
    count: int


# ── Regime ──

class MacroRegimeCountryResponse(BaseModel):
    country: str
    growth_score: float | None = None
    inflation_score: float | None = None
    policy_score: float | None = None
    regime_label: str
    confidence: float
    freshness_hours: float | None = None
    metrics: dict[str, float] = Field(default_factory=dict)


class MacroRegimeResponse(BaseModel):
    as_of: datetime | None = None
    countries: list[MacroRegimeCountryResponse]
    count: int


# ── Linkages ──

class MacroLinkagePointResponse(BaseModel):
    date: date
    macro_value: float
    asset_value: float


class MacroLinkageSeriesResponse(BaseModel):
    asset: str
    correlation: float | None = None
    point_count: int
    points: list[MacroLinkagePointResponse] = Field(default_factory=list)


class MacroLinkagesResponse(BaseModel):
    country: str
    indicator_name: str
    window_days: int
    as_of: datetime | None = None
    series: list[MacroLinkageSeriesResponse] = Field(default_factory=list)
    count: int


# ── Summary ──

class MacroCountrySummaryResponse(BaseModel):
    country: str
    now_title: str
    now_subtitle: str
    risk_score: float
    risk_label: str
    freshness_hours: float | None = None
    next_event_name: str | None = None
    next_event_date: date | None = None
    watchouts: list[str] = Field(default_factory=list)


class MacroSummaryResponse(BaseModel):
    as_of: datetime | None = None
    countries: list[MacroCountrySummaryResponse] = Field(default_factory=list)
    count: int
