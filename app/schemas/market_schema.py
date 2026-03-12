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
    change_percent: float | None = None
    previous_close: float | None = None
    last_tick_timestamp: datetime | None = None
    ingested_at: datetime | None = None
    is_stale: bool | None = None
    market_phase: str | None = None
    change_window: str | None = None
    data_quality: str | None = None
    is_predictive: bool | None = None
    session_source: str | None = None
    region: str | None = None
    exchange: str | None = None
    session_policy: str | None = None
    tradable_type: str | None = None


class MarketPriceListResponse(BaseModel):
    """Wrapper for a list of market prices."""

    prices: list[MarketPriceResponse]
    count: int


class MarketStatusResponse(BaseModel):
    """Session status across regional and asset-class market windows."""

    nse_open: bool
    nyse_open: bool
    gift_nifty_open: bool = False
    india_open: bool = False
    us_open: bool = False
    europe_open: bool = False
    japan_open: bool = False
    fx_open: bool = False
    commodities_open: bool = False
    live: bool


class IntradayPointResponse(BaseModel):
    """Single intraday price point for 1D chart."""

    timestamp: str
    price: float


class IntradayResponse(BaseModel):
    """Intraday prices for 1D live chart (when market is open)."""

    prices: list[IntradayPointResponse]
    window_start: datetime | None = None
    window_end: datetime | None = None
    coverage_minutes: int | None = None
    expected_minutes: int | None = None
    data_mode: str | None = None
