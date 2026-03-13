from fastapi import APIRouter, HTTPException, Query

from app.schemas.ingest_schema import CryptoIngestPayload, IngestAck
from app.schemas.market_schema import (
    IntradayPointResponse,
    IntradayResponse,
    MarketPriceListResponse,
    MarketPriceResponse,
)
from app.services import event_service, market_service

router = APIRouter(prefix="/crypto", tags=["crypto"])


@router.post("", response_model=IngestAck, status_code=201)
async def ingest_crypto(payload: CryptoIngestPayload) -> IngestAck:
    """Receive normalized crypto record from scraper and store as event."""
    try:
        await market_service.insert_price(
            asset=payload.asset.lower(),
            price=payload.price_usd,
            timestamp=payload.timestamp.isoformat(),
            source=payload.source,
            instrument_type="crypto",
            unit="usd",
            change_percent=payload.change_percent,
            previous_close=payload.previous_close,
        )

        row = await event_service.insert_event_dict(
            {
                "event_type": "crypto_price_change",
                "entity": payload.asset.lower(),
                "impact": "market_signal",
                "confidence": 0.72,
            }
        )
        return IngestAck(route="/crypto", event_id=row["id"])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=MarketPriceListResponse)
async def list_crypto(
    asset: str | None = Query(default=None, description="bitcoin, ethereum, solana, etc."),
    limit: int = Query(default=50, ge=-1, description="Max rows. Use -1 for all."),
    offset: int = Query(default=0, ge=0),
) -> MarketPriceListResponse:
    """Return crypto prices with optional asset filter."""
    try:
        effective_limit = 100_000 if limit == -1 else limit
        rows = await market_service.get_prices(
            instrument_type="crypto",
            asset=asset,
            limit=effective_limit,
            offset=offset,
        )
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/intraday", response_model=IntradayResponse)
async def get_crypto_intraday(
    asset: str = Query(..., description="Crypto asset (e.g. bitcoin, ethereum)"),
) -> IntradayResponse:
    """Return crypto intraday points for chart (rolling 24h window)."""
    try:
        payload = await market_service.get_intraday(asset=asset, instrument_type="crypto")
        rows = payload.get("prices") or []
        prices = [IntradayPointResponse(timestamp=r["timestamp"], price=r["price"]) for r in rows]
        return IntradayResponse(
            prices=prices,
            window_start=payload.get("window_start"),
            window_end=payload.get("window_end"),
            coverage_minutes=payload.get("coverage_minutes"),
            expected_minutes=payload.get("expected_minutes"),
            data_mode=payload.get("data_mode"),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/latest", response_model=MarketPriceListResponse)
async def latest_crypto() -> MarketPriceListResponse:
    """Return the most recent price for each crypto asset."""
    try:
        rows = await market_service.get_latest_prices(instrument_type="crypto")
        prices = [MarketPriceResponse(**r) for r in rows]
        return MarketPriceListResponse(prices=prices, count=len(prices))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
