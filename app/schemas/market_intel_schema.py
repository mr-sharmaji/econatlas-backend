from datetime import datetime

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
    quality_counts: dict[str, int]
