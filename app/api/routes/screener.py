from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.schemas.discover_schema import (
    DiscoverHomeMutualFundItem,
    DiscoverHomeResponse,
    DiscoverHomeStockItem,
    DiscoverMutualFundItemResponse,
    DiscoverMutualFundListResponse,
    DiscoverOverviewResponse,
    DiscoverStockItemResponse,
    DiscoverStockListResponse,
    PriceHistoryPoint,
    PriceHistoryResponse,
    QuickCategory,
    ScoreDistribution,
    SearchMutualFundItem,
    SearchStockItem,
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
async def get_discover_home() -> DiscoverHomeResponse:
    try:
        payload = await discover_service.get_discover_home_data()
        return DiscoverHomeResponse(
            top_stocks=[DiscoverHomeStockItem(**s) for s in payload.get("top_stocks", [])],
            top_equity_funds=[DiscoverHomeMutualFundItem(**m) for m in payload.get("top_equity_funds", [])],
            top_debt_funds=[DiscoverHomeMutualFundItem(**m) for m in payload.get("top_debt_funds", [])],
            trending_this_week=[DiscoverHomeStockItem(**s) for s in payload.get("trending_this_week", [])],
            gainers=[DiscoverHomeStockItem(**s) for s in payload.get("gainers", [])],
            gainers_3m=[DiscoverHomeStockItem(**s) for s in payload.get("gainers_3m", [])],
            losers=[DiscoverHomeStockItem(**s) for s in payload.get("losers", [])],
            losers_3m=[DiscoverHomeStockItem(**s) for s in payload.get("losers_3m", [])],
            hot_today_sector_name=payload.get("hot_today_sector_name"),
            hot_today_stocks=[DiscoverHomeStockItem(**s) for s in payload.get("hot_today_stocks", [])],
            leader_3m_sector_name=payload.get("leader_3m_sector_name"),
            leader_3m_stocks=[DiscoverHomeStockItem(**s) for s in payload.get("leader_3m_stocks", [])],
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
    sort_by: str = Query(default="score", description="score|change|volume|traded_value|pe|roe|price|market_cap"),
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
    min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    min_aum_cr: float | None = Query(default=None, ge=0.0),
    max_expense_ratio: float | None = Query(default=None, ge=0.0),
    min_return_1y: float | None = Query(default=None, description="Min 1Y return %"),
    min_return_3y: float | None = Query(default=None),
    min_return_5y: float | None = Query(default=None, description="Min 5Y return %"),
    min_returns_3y: float | None = Query(default=None, description="Deprecated alias for min_return_3y"),
    min_fund_age: float | None = Query(default=None, ge=0.0, description="Min fund age in years"),
    source_status: str | None = Query(default=None, description="primary|fallback|limited"),
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
            min_score=min_score,
            min_aum_cr=min_aum_cr,
            max_expense_ratio=max_expense_ratio,
            min_return_1y=min_return_1y,
            min_return_3y=min_return_3y if min_return_3y is not None else min_returns_3y,
            min_return_5y=min_return_5y,
            min_fund_age=min_fund_age,
            source_status=source_status,
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
async def get_stock_detail(symbol: str) -> DiscoverStockItemResponse:
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
async def get_mf_detail(scheme_code: str) -> DiscoverMutualFundItemResponse:
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
    days: int = Query(default=365, ge=7, le=1825),
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


@router.get("/mutual-funds/{scheme_code}/history", response_model=PriceHistoryResponse)
async def get_mf_history(
    scheme_code: str,
    days: int = Query(default=365, ge=7, le=1825),
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


