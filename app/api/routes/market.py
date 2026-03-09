from fastapi import APIRouter, HTTPException, Query, Response

from app.core.config import get_settings
from app.schemas.ingest_schema import IngestAck, MarketIngestPayload
from app.schemas.market_schema import (
    IntradayPointResponse,
    IntradayResponse,
    MarketPriceListResponse,
    MarketPriceResponse,
    MarketStatusResponse,
)
from app.services import event_service, market_service
from app.scheduler.trading_calendar import get_market_status

router = APIRouter(prefix="/market", tags=["market"])


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_market(payload: MarketIngestPayload) -> IngestAck:
    """Receive normalized market records from scraper and store as events."""
    try:
        entity_map = {
            "index": payload.name,
            "currency": payload.pair,
            "bond_yield": payload.instrument,
        }
        entity = entity_map.get(payload.type)
        if not entity:
            raise HTTPException(status_code=422, detail="Missing entity for market payload")

        if payload.type == "bond_yield":
            price_value = payload.yield_percent
        else:
            price_value = payload.value
        if price_value is None:
            raise HTTPException(status_code=422, detail="Missing numeric value for market payload")
        default_unit_by_type = {
            "index": "points",
            "currency": "fx_rate",
            "bond_yield": "percent",
        }
        unit_value = payload.unit or default_unit_by_type[payload.type]

        await market_service.insert_price(
            asset=entity,
            price=float(price_value),
            timestamp=payload.timestamp.isoformat(),
            source=payload.source,
            instrument_type=payload.type,
            unit=unit_value,
            change_percent=payload.change_percent,
            previous_close=payload.previous_close,
        )

        impact = "macro_signal" if payload.type == "bond_yield" else "market_signal"
        row = await event_service.insert_event_dict(
            {
                "event_type": f"{payload.type}_update",
                "entity": entity,
                "impact": impact,
                "confidence": 0.7,
            }
        )
        return IngestAck(route="/market", event_id=row["id"])
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=MarketPriceListResponse)
async def list_market_prices(
    instrument_type: str | None = Query(default=None, description="index, currency, or bond_yield"),
    asset: str | None = Query(default=None),
    limit: int = Query(default=50, ge=-1, description="Max rows. Use -1 for all."),
    offset: int = Query(default=0, ge=0),
) -> MarketPriceListResponse:
    """Return market prices (indices, currencies, bond yields) with optional filters."""
    try:
        effective_limit = 100_000 if limit == -1 else limit
        rows = await market_service.get_prices(
            instrument_type=instrument_type,
            asset=asset,
            limit=effective_limit,
            offset=offset,
        )
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/status", response_model=MarketStatusResponse)
async def market_status(response: Response) -> MarketStatusResponse:
    """Return whether markets are currently live (NSE and/or NYSE in session). Cached server-side and via Cache-Control."""
    status = get_market_status()
    max_age = get_settings().market_status_cache_seconds
    if max_age > 0:
        response.headers["Cache-Control"] = f"public, max-age={max_age}"
    return MarketStatusResponse(**status)


@router.get("/intraday", response_model=IntradayResponse)
async def get_intraday(
    asset: str = Query(..., description="Asset name (e.g. Nifty 50, S&P500)"),
    instrument_type: str = Query(..., description="index, currency, or bond_yield"),
) -> IntradayResponse:
    """Return intraday points for chart.
    currency => rolling 24h, index/bond_yield => latest session day."""
    try:
        rows = await market_service.get_intraday(asset=asset, instrument_type=instrument_type)
        prices = [IntradayPointResponse(timestamp=r["timestamp"], price=r["price"]) for r in rows]
        return IntradayResponse(prices=prices)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/latest", response_model=MarketPriceListResponse)
async def latest_market_prices(
    instrument_type: str | None = Query(default=None, description="index, currency, or bond_yield"),
) -> MarketPriceListResponse:
    """Return the most recent price for each asset (de-duplicated)."""
    try:
        rows = await market_service.get_latest_prices(instrument_type=instrument_type)
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
