from fastapi import APIRouter

APP_VERSION = "0.2.4"

router = APIRouter(tags=["health"])


@router.get("/health")
async def health_check() -> dict:
    return {"status": "ok", "version": APP_VERSION}
