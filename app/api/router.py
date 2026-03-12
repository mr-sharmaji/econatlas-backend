from fastapi import APIRouter

from app.api.routes import assets, brief, commodities, events, feedback, health, ipo, macro, market, news, ops, screener, tax, watchlist

api_router = APIRouter()

api_router.include_router(health.router)
api_router.include_router(events.router)
api_router.include_router(macro.router)
api_router.include_router(market.router)
api_router.include_router(commodities.router)
api_router.include_router(news.router)
api_router.include_router(ops.router)
api_router.include_router(assets.router)
api_router.include_router(watchlist.router)
api_router.include_router(screener.router)
api_router.include_router(brief.router)
api_router.include_router(ipo.router)
api_router.include_router(tax.router)
api_router.include_router(feedback.router)
