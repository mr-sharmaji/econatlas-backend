from datetime import date, datetime

from pydantic import BaseModel, Field


class AssetCatalogItemResponse(BaseModel):
    asset: str
    instrument_type: str
    symbol: str
    region: str
    exchange: str
    session_policy: str
    priority_rank: int
    tradable_type: str
    unit: str
    default_watchlist: bool = False
    benchmark_asset: str | None = None


class AssetCatalogResponse(BaseModel):
    assets: list[AssetCatalogItemResponse]
    count: int


class WatchlistUpdateRequest(BaseModel):
    assets: list[str] = Field(default_factory=list)


class WatchlistResponse(BaseModel):
    device_id: str
    assets: list[str]
    count: int


class ScreenerItemResponse(BaseModel):
    asset: str
    instrument_type: str
    region: str
    exchange: str
    price: float
    change_percent: float | None = None
    score: float
    signal_tags: list[str]
    market_phase: str | None = None
    is_stale: bool | None = None
    last_tick_timestamp: datetime | None = None
    data_quality: str | None = None
    change_window: str | None = None
    benchmark_asset: str | None = None
    benchmark_change_percent: float | None = None
    relative_strength: float | None = None


class ScreenerResponse(BaseModel):
    preset: str
    region: str | None = None
    instrument_type: str | None = None
    min_quality: float
    items: list[ScreenerItemResponse]
    count: int


class RegionHealthResponse(BaseModel):
    total: int
    live: int
    stale: int
    closed: int
    avg_latency_seconds: float | None = None


class DataHealthResponse(BaseModel):
    timestamp: datetime
    total_assets: int
    stale_assets: int
    avg_latency_seconds: float | None = None
    by_region: dict[str, RegionHealthResponse]
    by_instrument_type: dict[str, RegionHealthResponse]
    stale_by_instrument_type: dict[str, int]
    quality_counts: dict[str, int]
    india_session_source: str | None = None
    india_session_window: str | None = None
    india_session_fallback_reason: str | None = None


class IpoItemResponse(BaseModel):
    symbol: str
    company_name: str
    market: str = "IN"
    status: str
    ipo_type: str
    issue_size_cr: float | None = None
    price_band: str | None = None
    gmp_percent: float | None = None
    subscription_multiple: float | None = None
    listing_price: float | None = None
    listing_gain_pct: float | None = None
    outcome_state: str | None = None
    open_date: date | None = None
    close_date: date | None = None
    listing_date: date | None = None
    source_timestamp: datetime | None = None
    recommendation: str
    recommendation_reason: str


class IpoListResponse(BaseModel):
    status: str
    as_of: datetime | None = None
    items: list[IpoItemResponse]
    count: int


class IpoAlertsUpdateRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)


class IpoAlertsResponse(BaseModel):
    device_id: str
    symbols: list[str]
    count: int


class RegisterDeviceRequest(BaseModel):
    device_id: str = Field(..., min_length=6)
    fcm_token: str = Field(..., min_length=10)
    platform: str = Field(default="android")


class RegisterDeviceResponse(BaseModel):
    status: str
    device_id: str
