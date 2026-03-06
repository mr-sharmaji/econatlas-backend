from fastapi import APIRouter

from app.api.routes import events, health, macro

api_router = APIRouter()

api_router.include_router(health.router)
api_router.include_router(events.router)
api_router.include_router(macro.router)
