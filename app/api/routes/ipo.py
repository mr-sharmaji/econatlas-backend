from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.schemas.market_intel_schema import (
    IpoAlertsResponse,
    IpoAlertsUpdateRequest,
    IpoItemResponse,
    IpoListResponse,
)
from app.services import ipo_service

router = APIRouter(prefix="/ipos", tags=["ipos"])


@router.get("", response_model=IpoListResponse)
async def get_ipos(
    status: Literal["open", "upcoming"] = Query(default="open"),
    limit: int = Query(default=20, ge=1, le=100),
) -> IpoListResponse:
    try:
        payload = await ipo_service.get_ipos(status=status, limit=limit)
        return IpoListResponse(
            status=payload["status"],
            as_of=payload["as_of"],
            items=[IpoItemResponse(**r) for r in payload["items"]],
            count=payload["count"],
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/alerts", response_model=IpoAlertsResponse)
async def get_ipo_alerts(
    device_id: str = Query(..., min_length=6, description="Device identifier (client-generated UUID)"),
) -> IpoAlertsResponse:
    try:
        symbols = await ipo_service.get_ipo_alerts(device_id=device_id.strip())
        return IpoAlertsResponse(device_id=device_id.strip(), symbols=symbols, count=len(symbols))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("/alerts", response_model=IpoAlertsResponse)
async def put_ipo_alerts(
    payload: IpoAlertsUpdateRequest,
    device_id: str = Query(..., min_length=6, description="Device identifier (client-generated UUID)"),
) -> IpoAlertsResponse:
    try:
        symbols = await ipo_service.put_ipo_alerts(
            device_id=device_id.strip(),
            symbols=payload.symbols,
        )
        return IpoAlertsResponse(device_id=device_id.strip(), symbols=symbols, count=len(symbols))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
