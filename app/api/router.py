from fastapi import APIRouter

from app.api.routes import commodities, events, health, macro, market, news, ops

api_router = APIRouter()

api_router.include_router(health.router)
api_router.include_router(events.router)
api_router.include_router(macro.router)
api_router.include_router(market.router)
api_router.include_router(commodities.router)
api_router.include_router(news.router)
api_router.include_router(ops.router)
