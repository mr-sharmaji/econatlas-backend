"""Ingest future-looking stock evidence for Artha."""
from __future__ import annotations

import logging
from typing import Any

from app.services.future_prospects_service import (
    run_stock_future_prospects_ingestion,
)

logger = logging.getLogger(__name__)


async def run_stock_future_prospects_job() -> dict[str, Any]:
    logger.info("stock_future_prospects_job: running full refresh")
    return await run_stock_future_prospects_ingestion(recent_only=False)


async def run_stock_future_prospects_recent_job() -> dict[str, Any]:
    logger.info("stock_future_prospects_job: running recent-event refresh")
    return await run_stock_future_prospects_ingestion(recent_only=True)

