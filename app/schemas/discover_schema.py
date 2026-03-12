from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


SourceStatus = Literal["primary", "fallback", "limited"]


class DiscoverStockScoreBreakdown(BaseModel):
    momentum: float
    liquidity: float
    fundamentals: float
    combined_signal: float


class DiscoverStockItemResponse(BaseModel):
    symbol: str
    display_name: str
    market: str = "IN"
    sector: str | None = None
    last_price: float
    point_change: float | None = None
    percent_change: float | None = None
    volume: int | None = None
    traded_value: float | None = None
    pe_ratio: float | None = None
    roe: float | None = None
    roce: float | None = None
    debt_to_equity: float | None = None
    price_to_book: float | None = None
    eps: float | None = None
    score: float
    score_momentum: float
    score_liquidity: float
    score_fundamentals: float
    score_breakdown: DiscoverStockScoreBreakdown
    tags: list[str] = Field(default_factory=list)
    why_ranked: list[str] = Field(default_factory=list)
    source_status: SourceStatus
    source_timestamp: datetime
    ingested_at: datetime
    primary_source: str | None = None
    secondary_source: str | None = None


class DiscoverStockListResponse(BaseModel):
    preset: str
    as_of: datetime | None = None
    source_status: SourceStatus
    items: list[DiscoverStockItemResponse]
    count: int
    total_count: int | None = None


class DiscoverMutualFundScoreBreakdown(BaseModel):
    return_score: float
    risk_score: float
    cost_score: float
    consistency_score: float


class DiscoverMutualFundItemResponse(BaseModel):
    scheme_code: str
    scheme_name: str
    amc: str | None = None
    category: str | None = None
    sub_category: str | None = None
    plan_type: str
    option_type: str | None = None
    nav: float
    nav_date: date | None = None
    expense_ratio: float | None = None
    aum_cr: float | None = None
    risk_level: str | None = None
    returns_1y: float | None = None
    returns_3y: float | None = None
    returns_5y: float | None = None
    std_dev: float | None = None
    sharpe: float | None = None
    sortino: float | None = None
    score: float
    score_return: float
    score_risk: float
    score_cost: float
    score_consistency: float
    score_breakdown: DiscoverMutualFundScoreBreakdown
    tags: list[str] = Field(default_factory=list)
    why_ranked: list[str] = Field(default_factory=list)
    source_status: SourceStatus
    source_timestamp: datetime
    ingested_at: datetime
    primary_source: str | None = None
    secondary_source: str | None = None


class DiscoverMutualFundListResponse(BaseModel):
    preset: str
    as_of: datetime | None = None
    source_status: SourceStatus
    items: list[DiscoverMutualFundItemResponse]
    count: int
    total_count: int | None = None


class ScoreDistribution(BaseModel):
    excellent: int = 0
    good: int = 0
    average: int = 0
    poor: int = 0


class TopSegmentEntry(BaseModel):
    name: str
    avg_score: float
    count: int


class DiscoverOverviewResponse(BaseModel):
    segment: Literal["stocks", "mutual_funds"]
    as_of: datetime | None = None
    total_items: int
    source_status: SourceStatus
    leaders: list[str] = Field(default_factory=list)
    laggards: list[str] = Field(default_factory=list)
    avg_score: float | None = None
    score_distribution: ScoreDistribution | None = None
    top_sectors: list[TopSegmentEntry] = Field(default_factory=list)
    top_categories: list[TopSegmentEntry] = Field(default_factory=list)
    data_freshness_minutes: float | None = None


class ComparisonSummary(BaseModel):
    winner: str
    score_delta: float
    metric_winners: dict[str, str] = Field(default_factory=dict)


class DiscoverCompareResponse(BaseModel):
    segment: Literal["stocks", "mutual_funds"]
    as_of: datetime | None = None
    count: int
    source_status: SourceStatus
    stock_items: list[DiscoverStockItemResponse] = Field(default_factory=list)
    mutual_fund_items: list[DiscoverMutualFundItemResponse] = Field(default_factory=list)
    comparison_summary: ComparisonSummary | None = None
