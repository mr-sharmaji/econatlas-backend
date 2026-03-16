from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


SourceStatus = Literal["primary", "fallback", "limited"]


class TagV2Response(BaseModel):
    """Structured tag with category, severity, and explanation."""
    tag: str
    category: str   # classification | style | strength | valuation | risk | trend | ownership
    severity: str   # positive | negative | neutral
    priority: int
    confidence: float | None = None
    explanation: str | None = None
    expires_at: datetime | None = None


class DiscoverStockScoreBreakdown(BaseModel):
    quality: float | None = None
    valuation: float | None = None
    growth: float | None = None
    momentum: float
    institutional: float | None = None
    risk: float | None = None
    technical_score: float | None = None
    rsi_14: float | None = None
    action_tag: str | None = None
    action_tag_reasoning: str | None = None
    score_confidence: Literal["high", "medium", "low"] | None = None
    trend_alignment: Literal["aligned", "aligned_bullish", "aligned_bearish", "divergent", "conflicting"] | None = None
    breakout_signal: Literal["breakout", "approaching_breakout", "breakdown", "approaching_breakdown", "resistance", "support", "none"] | None = None
    entry_exit_signal: Literal["entry", "hold", "exit"] | None = None
    risk_reward_ratio: float | None = None
    risk_reward_tag: Literal["favorable", "balanced", "unfavorable", "poor", "neutral"] | None = None
    combined_signal: float
    position_52w: float | None = Field(None, alias="52w_position")
    quality_coverage: str | None = None
    data_quality: str | None = None
    sector_percentile: float | None = None
    lynch_classification: str | None = None
    market_regime: Literal["bull", "neutral", "correction", "bear", "crisis", "recovery"] | None = None
    peg_ratio: float | None = None
    sector_data_coverage_pct: float | None = None
    why_narrative: str | None = None


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
    high_52w: float | None = None
    low_52w: float | None = None
    market_cap: float | None = None
    dividend_yield: float | None = None
    score: float
    # 6-layer scores
    score_quality: float | None = None
    score_valuation: float | None = None
    score_growth: float | None = None
    score_momentum: float | None = None
    score_institutional: float | None = None
    score_risk: float | None = None
    # Percentile & classification
    sector_percentile: float | None = None
    lynch_classification: str | None = None
    # Pledged shares
    pledged_promoter_pct: float | None = None
    # Shareholding
    promoter_holding: float | None = None
    fii_holding: float | None = None
    dii_holding: float | None = None
    government_holding: float | None = None
    public_holding: float | None = None
    num_shareholders: int | None = None
    promoter_holding_change: float | None = None
    fii_holding_change: float | None = None
    dii_holding_change: float | None = None
    # Yahoo-exclusive fundamentals
    beta: float | None = None
    free_cash_flow: float | None = None
    operating_cash_flow: float | None = None
    total_cash: float | None = None
    total_debt: float | None = None
    total_revenue: float | None = None
    gross_margins: float | None = None
    operating_margins: float | None = None
    profit_margins: float | None = None
    revenue_growth: float | None = None
    earnings_growth: float | None = None
    forward_pe: float | None = None
    # Analyst data
    analyst_target_mean: float | None = None
    analyst_count: int | None = None
    analyst_recommendation: str | None = None
    analyst_recommendation_mean: float | None = None
    # Industry sub-sector
    industry: str | None = None
    payout_ratio: float | None = None
    percent_change_3m: float | None = None
    percent_change_1w: float | None = None
    percent_change_1y: float | None = None
    percent_change_3y: float | None = None
    percent_change_5y: float | None = None
    # Screener-derived signals
    sales_growth_yoy: float | None = None
    profit_growth_yoy: float | None = None
    opm_change: float | None = None
    interest_coverage: float | None = None
    compounded_sales_growth_3y: float | None = None
    compounded_profit_growth_3y: float | None = None
    total_assets: float | None = None
    asset_growth_yoy: float | None = None
    reserves_growth: float | None = None
    debt_direction: float | None = None
    cwip: float | None = None
    cash_from_operations: float | None = None
    cash_from_investing: float | None = None
    cash_from_financing: float | None = None
    num_shareholders_change_qoq: float | None = None
    num_shareholders_change_yoy: float | None = None
    synthetic_forward_pe: float | None = None
    # Complete annual financial tables (JSONB — full YoY history)
    pl_annual: dict | None = None
    bs_annual: dict | None = None
    cf_annual: dict | None = None
    shareholding_quarterly: dict | None = None
    # Technical & action tag
    technical_score: float | None = None
    rsi_14: float | None = None
    action_tag: str | None = None
    action_tag_reasoning: str | None = None
    score_confidence: Literal["high", "medium", "low"] | None = None
    trend_alignment: Literal["aligned", "aligned_bullish", "aligned_bearish", "divergent", "conflicting"] | None = None
    breakout_signal: Literal["breakout", "approaching_breakout", "breakdown", "approaching_breakdown", "resistance", "support", "none"] | None = None
    entry_exit_signal: Literal["entry", "hold", "exit"] | None = None
    risk_reward_ratio: float | None = None
    risk_reward_tag: Literal["favorable", "balanced", "unfavorable", "poor", "neutral"] | None = None
    score_breakdown: DiscoverStockScoreBreakdown
    tags: list[TagV2Response] = Field(default_factory=list)
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
    performance_score: float | None = None
    consistency_score: float | None = None
    risk_score: float | None = None
    cost_score: float | None = None
    category_fit_score: float | None = None
    # Legacy keys for backward compat
    return_score: float | None = None
    alpha_score: float | None = None
    beta_score: float | None = None


class DiscoverMutualFundItemResponse(BaseModel):
    scheme_code: str
    scheme_name: str
    display_name: str | None = None
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
    category_rank: int | None = None
    category_total: int | None = None
    sub_category_rank: int | None = None
    sub_category_total: int | None = None
    max_drawdown: float | None = None
    rolling_return_consistency: float | None = None
    alpha: float | None = None
    beta: float | None = None
    fund_age_years: float | None = None
    quality_badges: list[str] = Field(default_factory=list)
    category_avg_returns_1y: float | None = None
    category_avg_returns_3y: float | None = None
    category_avg_returns_5y: float | None = None
    score: float | None = None
    score_return: float | None = None
    score_risk: float | None = None
    score_cost: float | None = None
    score_consistency: float | None = None
    score_performance: float | None = None
    score_category_fit: float | None = None
    score_alpha: float | None = None
    score_beta: float | None = None
    sub_category_percentile: float | None = None
    fund_classification: str | None = None
    score_breakdown: DiscoverMutualFundScoreBreakdown | None = None
    tags: list[TagV2Response] = Field(default_factory=list)
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


# --- Unified Search ---

class SearchStockItem(BaseModel):
    symbol: str
    display_name: str
    sector: str | None = None
    last_price: float
    percent_change: float | None = None
    score: float = 0


class SearchMutualFundItem(BaseModel):
    scheme_code: str
    scheme_name: str
    category: str | None = None
    nav: float
    returns_3y: float | None = None
    score: float = 0


class UnifiedSearchResponse(BaseModel):
    stocks: list[SearchStockItem] = Field(default_factory=list)
    mutual_funds: list[SearchMutualFundItem] = Field(default_factory=list)


# --- Discover Home ---

class DiscoverHomeStockItem(BaseModel):
    symbol: str
    display_name: str
    sector: str | None = None
    last_price: float
    percent_change: float | None = None
    percent_change_3m: float | None = None
    percent_change_1w: float | None = None
    score: float
    score_quality: float | None = None
    score_growth: float | None = None
    high_52w: float | None = None
    low_52w: float | None = None
    market_cap: float | None = None


class DiscoverHomeMutualFundItem(BaseModel):
    scheme_code: str
    scheme_name: str
    display_name: str | None = None
    category: str | None = None
    score: float
    returns_1y: float | None = None
    quality_badges: list[str] = Field(default_factory=list)


class QuickCategory(BaseModel):
    name: str
    segment: Literal["stocks", "mutual_funds"]
    preset: str | None = None
    filter_key: str | None = None
    filter_value: str | None = None


class DiscoverHomeResponse(BaseModel):
    top_stocks: list[DiscoverHomeStockItem] = Field(default_factory=list)
    top_equity_funds: list[DiscoverHomeMutualFundItem] = Field(default_factory=list)
    top_debt_funds: list[DiscoverHomeMutualFundItem] = Field(default_factory=list)
    trending_this_week: list[DiscoverHomeStockItem] = Field(default_factory=list)
    sector_champions: list[DiscoverHomeStockItem] = Field(default_factory=list)
    gainers: list[DiscoverHomeStockItem] = Field(default_factory=list)
    gainers_3m: list[DiscoverHomeStockItem] = Field(default_factory=list)
    losers: list[DiscoverHomeStockItem] = Field(default_factory=list)
    losers_3m: list[DiscoverHomeStockItem] = Field(default_factory=list)
    quick_categories: list[QuickCategory] = Field(default_factory=list)
    market_mood: MarketMood | None = None


# --- Chart History ---

class PriceHistoryPoint(BaseModel):
    date: date
    value: float


class PriceHistoryResponse(BaseModel):
    symbol: str | None = None
    scheme_code: str | None = None
    points: list[PriceHistoryPoint] = Field(default_factory=list)
    count: int = 0


# --- Score History ---

class ScoreHistoryPoint(BaseModel):
    scored_at: datetime
    score: float


class ScoreHistoryResponse(BaseModel):
    symbol: str
    points: list[ScoreHistoryPoint] = Field(default_factory=list)
    count: int = 0


# --- Story ---

class ScoreChange(BaseModel):
    layer: str
    old_value: float | None = None
    new_value: float | None = None
    direction: str  # "up" | "down" | "unchanged"


class StockStoryResponse(BaseModel):
    symbol: str
    verdict: str | None = None
    action_tag: str | None = None
    action_tag_reasoning: str | None = None
    trend_alignment: str | None = None
    breakout_signal: str | None = None
    lynch_classification: str | None = None
    why_narrative: str | None = None
    score_confidence: str | None = None
    score_changes: list[ScoreChange] = Field(default_factory=list)


# --- Compare ---

class ComparisonDimension(BaseModel):
    metric: str
    label: str
    values: list[float | None]
    winner_index: int | None = None  # index of better value, None if equal/incomparable


class StockCompareResponse(BaseModel):
    items: list[DiscoverStockItemResponse] = Field(default_factory=list)
    comparison_dimensions: list[ComparisonDimension] = Field(default_factory=list)


# --- Market Mood ---

class MarketMood(BaseModel):
    avg_score: float | None = None
    score_distribution: ScoreDistribution | None = None
    summary: str | None = None  # "71% of tracked stocks are rated Good or above"
