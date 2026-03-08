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


class MarketPriceListResponse(BaseModel):
    """Wrapper for a list of market prices."""

    prices: list[MarketPriceResponse]
    count: int


class MarketStatusResponse(BaseModel):
    """Whether NSE and NYSE are currently in a trading session (market live)."""

    nse_open: bool
    nyse_open: bool
    live: bool


class IntradayPointResponse(BaseModel):
    """Single intraday price point for 1D chart."""

    timestamp: str
    price: float


class IntradayResponse(BaseModel):
    """Intraday prices for 1D live chart (when market is open)."""

    prices: list[IntradayPointResponse]
