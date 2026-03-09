from datetime import datetime

from pydantic import BaseModel


class StockSnapshotResponse(BaseModel):
    symbol: str
    display_name: str
    market: str
    last_price: float
    point_change: float | None = None
    percent_change: float | None = None
    volume: int | None = None
    traded_value: float | None = None
    sector: str | None = None
    source_timestamp: datetime
    ingested_at: datetime


class StockSnapshotListResponse(BaseModel):
    market: str
    as_of: datetime | None = None
    items: list[StockSnapshotResponse]
    count: int


class SectorPulseItemResponse(BaseModel):
    sector: str
    avg_change_percent: float
    gainers: int
    losers: int
    count: int


class SectorPulseResponse(BaseModel):
    market: str
    as_of: datetime | None = None
    sectors: list[SectorPulseItemResponse]
    count: int


class PostMarketOverviewResponse(BaseModel):
    market: str
    as_of: datetime | None = None
    total_stocks: int
    advancers: int
    decliners: int
    unchanged: int
    avg_change_percent: float | None = None
    top_sector: str | None = None
    bottom_sector: str | None = None
    summary: str
    driver_tags: list[str]
