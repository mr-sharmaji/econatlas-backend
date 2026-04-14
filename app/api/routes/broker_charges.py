"""Broker trade charges API."""
from fastapi import APIRouter, Response

from app.services import broker_charges_service

router = APIRouter(prefix="/broker-charges", tags=["broker-charges"])


@router.get("")
async def get_broker_charges(response: Response) -> dict:
    """Return all broker brokerage presets + statutory rates.

    Cached for 24h at the CDN/browser level. The data changes
    at most weekly (when the scraper job runs).
    """
    response.headers["Cache-Control"] = "public, max-age=86400"
    data = await broker_charges_service.get_all_broker_charges()
    if not data.get("brokers"):
        # DB empty — seed first
        from app.scheduler.broker_charges_job import run_broker_charges_job
        await run_broker_charges_job()
        data = await broker_charges_service.get_all_broker_charges()
    return data
