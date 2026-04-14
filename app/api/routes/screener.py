from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Response

from app.schemas.discover_schema import (
    ComparisonDimension,
    DiscoverHomeResponse,
    DiscoverHomeSection,
    DiscoverMutualFundItemResponse,
    DiscoverMutualFundListResponse,
    DiscoverOverviewResponse,
    DiscoverStockItemResponse,
    DiscoverStockListResponse,
    MarketMood,
    PriceHistoryPoint,
    PriceHistoryResponse,
    QuickCategory,
    ScoreChange,
    ScoreDistribution,
    ScoreHistoryPoint,
    ScoreHistoryResponse,
    SearchMutualFundItem,
    SearchStockItem,
    StockCompareResponse,
    StockStoryResponse,
    TopSegmentEntry,
    UnifiedSearchResponse,
)
from app.schemas.market_intel_schema import ScreenerItemResponse, ScreenerResponse
from app.services import discover_service, market_intel_service

router = APIRouter(prefix="/screener", tags=["screener"])


@router.get("", response_model=ScreenerResponse)
async def get_legacy_screener(
    preset: str = Query(default="momentum", description="momentum|reversal|volatility|macro-sensitive"),
    region: str | None = Query(default=None, description="Region filter (India, US, Europe, Japan, FX, Commodities)"),
    instrument_type: str | None = Query(default=None, description="index, currency, commodity, bond_yield"),
    limit: int = Query(default=25, ge=1, le=200),
    min_quality: float = Query(default=0.0, ge=0.0, le=1.0),
) -> ScreenerResponse:
    """Legacy smart screener endpoint kept for backward compatibility."""
    try:
        rows = await market_intel_service.get_screener(
            preset=preset,
            region=region,
            instrument_type=instrument_type,
            limit=limit,
            min_quality=min_quality,
        )
        items = [ScreenerItemResponse(**r) for r in rows]
        return ScreenerResponse(
            preset=preset,
            region=region,
            instrument_type=instrument_type,
            min_quality=min_quality,
            items=items,
            count=len(items),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/overview", response_model=DiscoverOverviewResponse)
async def get_discover_overview(
    segment: Literal["stocks", "mutual_funds"] = Query(default="stocks"),
) -> DiscoverOverviewResponse:
    try:
        payload = await discover_service.get_discover_overview(segment=segment)
        dist = payload.get("score_distribution")
        return DiscoverOverviewResponse(
            segment=payload["segment"],
            as_of=payload.get("as_of"),
            total_items=payload["total_items"],
            source_status=payload.get("source_status") or "limited",
            leaders=payload.get("leaders", []),
            laggards=payload.get("laggards", []),
            avg_score=payload.get("avg_score"),
            score_distribution=ScoreDistribution(**dist) if dist else None,
            top_sectors=[TopSegmentEntry(**e) for e in payload.get("top_sectors", [])],
            top_categories=[TopSegmentEntry(**e) for e in payload.get("top_categories", [])],
            data_freshness_minutes=payload.get("data_freshness_minutes"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/search", response_model=UnifiedSearchResponse)
async def unified_search(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(default=10, ge=1, le=30),
) -> UnifiedSearchResponse:
    try:
        payload = await discover_service.unified_search(query=q, limit=limit)
        return UnifiedSearchResponse(
            stocks=[SearchStockItem(**s) for s in payload.get("stocks", [])],
            mutual_funds=[SearchMutualFundItem(**m) for m in payload.get("mutual_funds", [])],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/home", response_model=DiscoverHomeResponse)
async def get_discover_home(response: Response) -> DiscoverHomeResponse:
    response.headers["Cache-Control"] = "public, max-age=1800"
    try:
        payload = await discover_service.get_discover_home_data()
        return DiscoverHomeResponse(
            stock_sections=[DiscoverHomeSection(**s) for s in payload.get("stock_sections", [])],
            mf_sections=[DiscoverHomeSection(**s) for s in payload.get("mf_sections", [])],
            quick_categories=[QuickCategory(**c) for c in payload.get("quick_categories", [])],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks", response_model=DiscoverStockListResponse)
async def get_discover_stocks(
    preset: str = Query(default="momentum", description="momentum|value|low-volatility|high-volume|breakout|quality|dividend"),
    search: str | None = Query(default=None),
    sector: str | None = Query(default=None),
    min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    max_score: float | None = Query(default=None, ge=0.0, le=100.0),
    min_price: float | None = Query(default=None, ge=0.0),
    max_price: float | None = Query(default=None, ge=0.0),
    min_pe: float | None = Query(default=None, ge=0.0),
    max_pe: float | None = Query(default=None, ge=0.0),
    min_roe: float | None = Query(default=None),
    min_roce: float | None = Query(default=None),
    max_debt_to_equity: float | None = Query(default=None, ge=0.0),
    min_volume: int | None = Query(default=None, ge=0),
    min_traded_value: float | None = Query(default=None, ge=0.0),
    min_market_cap: float | None = Query(default=None, ge=0.0, description="Min market cap in Cr"),
    max_market_cap: float | None = Query(default=None, ge=0.0, description="Max market cap in Cr"),
    min_dividend_yield: float | None = Query(default=None, ge=0.0, description="Min dividend yield %"),
    min_pb: float | None = Query(default=None, ge=0.0, description="Min P/B ratio"),
    max_pb: float | None = Query(default=None, ge=0.0, description="Max P/B ratio"),
    source_status: str | None = Query(default=None, description="primary|fallback|limited"),
    tag: str | None = Query(default=None, description="Filter by tag name (e.g. 'High Quality', 'Debt Free')"),
    tags_category: str | None = Query(default=None, description="Filter by tag category (strength|risk|valuation|trend|ownership|style|classification)"),
    sort_by: str = Query(default="score", description="score|change|change_3m|change_1y|volume|traded_value|pe|roe|price|market_cap|quality|valuation|growth"),
    sort_order: str = Query(default="desc", description="asc|desc"),
    limit: int = Query(default=25, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> DiscoverStockListResponse:
    try:
        payload = await discover_service.list_discover_stocks(
            preset=preset,
            search=search,
            sector=sector,
            min_score=min_score,
            max_score=max_score,
            min_price=min_price,
            max_price=max_price,
            min_pe=min_pe,
            max_pe=max_pe,
            min_roe=min_roe,
            min_roce=min_roce,
            max_debt_to_equity=max_debt_to_equity,
            min_volume=min_volume,
            min_traded_value=min_traded_value,
            min_market_cap=min_market_cap,
            max_market_cap=max_market_cap,
            min_dividend_yield=min_dividend_yield,
            min_pb=min_pb,
            max_pb=max_pb,
            source_status=source_status,
            tag=tag,
            tags_category=tags_category,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
        )
        return DiscoverStockListResponse(
            preset=payload["preset"],
            as_of=payload.get("as_of"),
            source_status=payload.get("source_status") or "limited",
            items=[DiscoverStockItemResponse(**item) for item in payload["items"]],
            count=payload["count"],
            total_count=payload.get("total_count"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mutual-funds", response_model=DiscoverMutualFundListResponse)
async def get_discover_mutual_funds(
    preset: str = Query(default="all", description="all|equity|debt|hybrid|large-cap|mid-cap|small-cap|flexi-cap|multi-cap|elss|value-mf|focused|sectoral|index|low-risk|short-duration|corporate-bond|banking-psu|gilt|liquid|overnight|dynamic-bond|money-market|aggressive-hybrid|balanced-hybrid|conservative-hybrid"),
    search: str | None = Query(default=None),
    category: str | None = Query(default=None),
    risk_level: str | None = Query(default=None),
    direct_only: bool = Query(default=True),
    include_idcw: bool = Query(default=False, description="Include IDCW fund variants"),
    min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    min_aum_cr: float | None = Query(default=None, ge=0.0),
    max_expense_ratio: float | None = Query(default=None, ge=0.0),
    min_return_1y: float | None = Query(default=None, description="Min 1Y return %"),
    min_return_3y: float | None = Query(default=None),
    min_return_5y: float | None = Query(default=None, description="Min 5Y return %"),
    min_returns_3y: float | None = Query(default=None, description="Deprecated alias for min_return_3y"),
    min_fund_age: float | None = Query(default=None, ge=0.0, description="Min fund age in years"),
    source_status: str | None = Query(default=None, description="primary|fallback|limited"),
    tag: str | None = Query(default=None, description="Filter by tag name"),
    tags_category: str | None = Query(default=None, description="Filter by tag category"),
    sort_by: str = Query(default="score", description="score|returns_3y|returns_1y|returns_5y|aum|expense|nav|risk"),
    sort_order: str = Query(default="desc", description="asc|desc"),
    limit: int = Query(default=25, ge=1, le=250),
    offset: int = Query(default=0, ge=0),
) -> DiscoverMutualFundListResponse:
    try:
        payload = await discover_service.list_discover_mutual_funds(
            preset=preset,
            search=search,
            category=category,
            risk_level=risk_level,
            direct_only=direct_only,
            include_idcw=include_idcw,
            min_score=min_score,
            min_aum_cr=min_aum_cr,
            max_expense_ratio=max_expense_ratio,
            min_return_1y=min_return_1y,
            min_return_3y=min_return_3y if min_return_3y is not None else min_returns_3y,
            min_return_5y=min_return_5y,
            min_fund_age=min_fund_age,
            source_status=source_status,
            tag=tag,
            tags_category=tags_category,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            offset=offset,
        )
        return DiscoverMutualFundListResponse(
            preset=payload["preset"],
            as_of=payload.get("as_of"),
            source_status=payload.get("source_status") or "limited",
            items=[DiscoverMutualFundItemResponse(**item) for item in payload["items"]],
            count=payload["count"],
            total_count=payload.get("total_count"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/{symbol}/detail", response_model=DiscoverStockItemResponse)
async def get_stock_detail(symbol: str, response: Response) -> DiscoverStockItemResponse:
    response.headers["Cache-Control"] = "public, max-age=900"
    try:
        data = await discover_service.get_stock_by_symbol(symbol=symbol)
        if data is None:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        return DiscoverStockItemResponse(**data)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mutual-funds/{scheme_code}/detail", response_model=DiscoverMutualFundItemResponse)
async def get_mf_detail(scheme_code: str, response: Response) -> DiscoverMutualFundItemResponse:
    response.headers["Cache-Control"] = "public, max-age=900"
    try:
        data = await discover_service.get_mf_by_scheme_code(scheme_code=scheme_code)
        if data is None:
            raise HTTPException(status_code=404, detail=f"MF {scheme_code} not found")
        return DiscoverMutualFundItemResponse(**data)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/{symbol}/history", response_model=PriceHistoryResponse)
async def get_stock_history(
    symbol: str,
    days: int = Query(
        default=365, ge=7, le=20000,
        description="Window in days. Upper bound ~55y so 'All' can return full inception history.",
    ),
) -> PriceHistoryResponse:
    try:
        points = await discover_service.get_stock_price_history(symbol=symbol, days=days)
        return PriceHistoryResponse(
            symbol=symbol,
            points=[PriceHistoryPoint(date=p["trade_date"], value=p["close"]) for p in points],
            count=len(points),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/{symbol}/intraday", response_model=PriceHistoryResponse)
async def get_stock_intraday(symbol: str) -> PriceHistoryResponse:
    """Return today's intraday ticks for the 1D chart.

    Hybrid source: persistent 30-min ticks from `discover_stock_intraday`
    table, falling back to a live Yahoo 5-min fetch when the table has
    fewer than 3 points (e.g. pre-market or cold start). Backed by a
    5-minute in-process cache to absorb app-open bursts.
    """
    try:
        points = await discover_service.get_stock_intraday_history(symbol=symbol)
        return PriceHistoryResponse(
            symbol=symbol,
            points=[
                PriceHistoryPoint(date=p["ts"], value=p["price"])
                for p in points
            ],
            count=len(points),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mutual-funds/{scheme_code}/history", response_model=PriceHistoryResponse)
async def get_mf_history(
    scheme_code: str,
    days: int = Query(
        default=365, ge=7, le=20000,
        description="Window in days. Upper bound ~55y so 'All' can return full inception history.",
    ),
) -> PriceHistoryResponse:
    try:
        points = await discover_service.get_mf_nav_history(scheme_code=scheme_code, days=days)
        return PriceHistoryResponse(
            scheme_code=scheme_code,
            points=[PriceHistoryPoint(date=p["nav_date"], value=p["nav"]) for p in points],
            count=len(points),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/sparklines")
async def get_stock_sparklines(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    days: int = Query(default=7, ge=1, le=365),
    max_points: int = Query(default=30, ge=5, le=365),
) -> dict[str, list[dict]]:
    """Batch fetch price sparklines for multiple stocks."""
    try:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()][:50]
        return await discover_service.get_stock_sparklines(
            symbols=symbol_list, days=days, max_points=max_points,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mutual-funds/sparklines")
async def get_mf_sparklines(
    scheme_codes: str = Query(..., description="Comma-separated scheme codes"),
    days: int = Query(default=7, ge=1, le=365),
    max_points: int = Query(default=30, ge=5, le=365),
) -> dict[str, list[dict]]:
    """Batch fetch NAV sparklines for multiple mutual funds."""
    try:
        code_list = [s.strip() for s in scheme_codes.split(",") if s.strip()][:50]
        return await discover_service.get_mf_sparklines(
            scheme_codes=code_list, days=days, max_points=max_points,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/{symbol}/score-history", response_model=ScoreHistoryResponse)
async def get_stock_score_history(
    symbol: str,
    days: int = Query(default=30, ge=7, le=90),
) -> ScoreHistoryResponse:
    """Get historical score data points for a stock."""
    try:
        points = await discover_service.get_score_history(symbol=symbol, days=days)
        return ScoreHistoryResponse(
            symbol=symbol,
            points=[ScoreHistoryPoint(scored_at=p["scored_at"], score=p["score"]) for p in points],
            count=len(points),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/{symbol}/peers")
async def get_stock_peers(
    symbol: str,
    limit: int = Query(default=5, ge=1, le=20),
) -> list[DiscoverStockItemResponse]:
    """Get peer stocks in the same sector, sorted by score."""
    try:
        peers = await discover_service.get_stock_peers(symbol=symbol, limit=limit)
        return [DiscoverStockItemResponse(**p) for p in peers]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mutual-funds/{scheme_code}/peers")
async def get_mf_peers(
    scheme_code: str,
    limit: int = Query(default=5, ge=1, le=20),
) -> list[DiscoverMutualFundItemResponse]:
    """Get peer mutual funds in the same category, sorted by score."""
    try:
        peers = await discover_service.get_mf_peers(scheme_code=scheme_code, limit=limit)
        return [DiscoverMutualFundItemResponse(**p) for p in peers]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/{symbol}/story", response_model=StockStoryResponse)
async def get_stock_story(symbol: str) -> StockStoryResponse:
    """Get narrative story with verdict, action signals, and score changes."""
    try:
        data = await discover_service.get_stock_story(symbol=symbol)
        if data is None:
            raise HTTPException(status_code=404, detail=f"Stock {symbol} not found")
        return StockStoryResponse(
            symbol=data["symbol"],
            verdict=data.get("verdict"),
            ai_narrative=data.get("ai_narrative"),
            action_tag=data.get("action_tag"),
            action_tag_reasoning=data.get("action_tag_reasoning"),
            trend_alignment=data.get("trend_alignment"),
            breakout_signal=data.get("breakout_signal"),
            lynch_classification=data.get("lynch_classification"),
            why_narrative=data.get("why_narrative"),
            score_confidence=data.get("score_confidence"),
            score_changes=[ScoreChange(**sc) for sc in data.get("score_changes", [])],
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks/compare", response_model=StockCompareResponse)
async def compare_stocks(
    symbols: str = Query(..., description="Comma-separated symbols (2-5)"),
) -> StockCompareResponse:
    """Side-by-side comparison of multiple stocks."""
    try:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()][:5]
        if len(symbol_list) < 2:
            raise HTTPException(status_code=400, detail="At least 2 symbols required")
        data = await discover_service.compare_stocks(symbols=symbol_list)
        return StockCompareResponse(
            items=[DiscoverStockItemResponse(**item) for item in data["items"]],
            comparison_dimensions=[
                ComparisonDimension(**dim) for dim in data["comparison_dimensions"]
            ],
            ai_insight=data.get("ai_insight"),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/market-mood", response_model=MarketMood)
async def get_market_mood(response: Response) -> MarketMood:
    """Aggregate score distribution and sentiment across tracked stocks."""
    response.headers["Cache-Control"] = "public, max-age=1800"
    try:
        data = await discover_service.get_market_mood()
        dist = data.get("score_distribution")
        return MarketMood(
            avg_score=data.get("avg_score"),
            score_distribution=ScoreDistribution(**dist) if dist else None,
            summary=data.get("summary"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


