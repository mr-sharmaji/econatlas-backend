from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.schemas.brief_schema import (
    PostMarketOverviewResponse,
    SectorPulseItemResponse,
    SectorPulseResponse,
    StockSnapshotListResponse,
    StockSnapshotResponse,
)
from app.services import brief_service

router = APIRouter(prefix="/brief", tags=["brief"])


@router.get("/post-market", response_model=PostMarketOverviewResponse)
async def post_market_overview(
    market: Literal["IN"] = Query(default="IN"),
) -> PostMarketOverviewResponse:
    try:
        payload = await brief_service.get_post_market_overview(market=market)
        return PostMarketOverviewResponse(**payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/movers", response_model=StockSnapshotListResponse)
async def movers(
    market: Literal["IN"] = Query(default="IN"),
    type: Literal["gainers", "losers"] = Query(default="gainers"),
    limit: int = Query(default=10, ge=1, le=50),
) -> StockSnapshotListResponse:
    try:
        payload = await brief_service.get_movers(market=market, movers_type=type, limit=limit)
        return StockSnapshotListResponse(
            market=payload["market"],
            as_of=payload["as_of"],
            items=[StockSnapshotResponse(**r) for r in payload["items"]],
            count=payload["count"],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/most-active", response_model=StockSnapshotListResponse)
async def most_active(
    market: Literal["IN"] = Query(default="IN"),
    limit: int = Query(default=10, ge=1, le=50),
) -> StockSnapshotListResponse:
    try:
        payload = await brief_service.get_most_active(market=market, limit=limit)
        return StockSnapshotListResponse(
            market=payload["market"],
            as_of=payload["as_of"],
            items=[StockSnapshotResponse(**r) for r in payload["items"]],
            count=payload["count"],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sectors", response_model=SectorPulseResponse)
async def sectors(
    market: Literal["IN"] = Query(default="IN"),
    limit: int = Query(default=8, ge=1, le=30),
) -> SectorPulseResponse:
    try:
        payload = await brief_service.get_sector_pulse(market=market, limit=limit)
        return SectorPulseResponse(
            market=payload["market"],
            as_of=payload["as_of"],
            sectors=[SectorPulseItemResponse(**r) for r in payload["sectors"]],
            count=payload["count"],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
