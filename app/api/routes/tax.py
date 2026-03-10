from fastapi import APIRouter, HTTPException

from app.schemas.tax_schema import TaxConfigResponse
from app.services import tax_service

router = APIRouter(prefix="/tax", tags=["tax"])


@router.get("/config", response_model=TaxConfigResponse)
async def get_tax_config() -> TaxConfigResponse:
    try:
        payload = await tax_service.get_tax_config_payload()
    except tax_service.TaxConfigNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return TaxConfigResponse(**payload)
