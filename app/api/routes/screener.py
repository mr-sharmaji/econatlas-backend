from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.schemas.discover_schema import (
    DiscoverCompareResponse,
    DiscoverMutualFundItemResponse,
    DiscoverMutualFundListResponse,
    DiscoverOverviewResponse,
    DiscoverStockItemResponse,
    DiscoverStockListResponse,
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
        return DiscoverOverviewResponse(**payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stocks", response_model=DiscoverStockListResponse)
async def get_discover_stocks(
    preset: str = Query(default="momentum", description="momentum|value|low-volatility|high-volume|breakout"),
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
    source_status: str | None = Query(default=None, description="primary|fallback|limited"),
    sort_by: str = Query(default="score", description="score|change|volume|traded_value|pe|roe|price"),
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
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/mutual-funds", response_model=DiscoverMutualFundListResponse)
async def get_discover_mutual_funds(
    preset: str = Query(default="all", description="all|large-cap|flexi-cap|index|low-risk"),
    search: str | None = Query(default=None),
    category: str | None = Query(default=None),
    risk_level: str | None = Query(default=None),
    direct_only: bool = Query(default=True),
    min_score: float | None = Query(default=None, ge=0.0, le=100.0),
    min_aum_cr: float | None = Query(default=None, ge=0.0),
    max_expense_ratio: float | None = Query(default=None, ge=0.0),
    min_return_3y: float | None = Query(default=None),
    min_returns_3y: float | None = Query(default=None, description="Deprecated alias for min_return_3y"),
    source_status: str | None = Query(default=None, description="primary|fallback|limited"),
    sort_by: str = Query(default="score", description="score|returns_3y|returns_1y|aum|expense|nav|risk"),
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
            min_return_3y=min_return_3y if min_return_3y is not None else min_returns_3y,
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
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/compare", response_model=DiscoverCompareResponse)
async def get_discover_compare(
    segment: Literal["stocks", "mutual_funds"] = Query(default="stocks"),
    ids: str = Query(default="", description="Comma-separated symbol(s) or scheme_code(s), max 3"),
) -> DiscoverCompareResponse:
    try:
        id_list = [part.strip() for part in ids.split(",") if part.strip()]
        payload = await discover_service.get_discover_compare(segment=segment, ids=id_list)
        return DiscoverCompareResponse(
            segment=payload["segment"],
            as_of=payload.get("as_of"),
            count=payload.get("count", 0),
            source_status=payload.get("source_status") or "limited",
            stock_items=[DiscoverStockItemResponse(**item) for item in payload.get("stock_items", [])],
            mutual_fund_items=[
                DiscoverMutualFundItemResponse(**item)
                for item in payload.get("mutual_fund_items", [])
            ],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
