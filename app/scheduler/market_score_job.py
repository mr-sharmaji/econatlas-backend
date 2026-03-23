"""Background job: compute market scores for all tracked assets."""
from __future__ import annotations

import logging

from app.core.database import get_pool
from app.services import market_service

logger = logging.getLogger(__name__)

# All assets to score — pulled from the asset_catalog table at runtime
async def _get_scoreable_assets() -> list[tuple[str, str]]:
    """Return (asset, instrument_type) pairs that have enough price history."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT asset, instrument_type, COUNT(*) as cnt
        FROM market_prices
        WHERE instrument_type IS NOT NULL
        GROUP BY asset, instrument_type
        HAVING COUNT(*) >= 14
        ORDER BY asset
        """
    )
    return [(r["asset"], r["instrument_type"]) for r in rows]


async def run_market_score_job() -> None:
    """Compute and store scores for all market assets with sufficient history."""
    logger.info("Market score job: starting")
    pairs = await _get_scoreable_assets()
    logger.info("Market score job: found %d scoreable assets", len(pairs))

    success = 0
    failed = 0
    for asset, inst_type in pairs:
        try:
            result = await market_service.compute_and_store_market_score(asset, inst_type)
            if result is not None:
                success += 1
            else:
                logger.debug("Market score job: skipped %s/%s (not enough data)", asset, inst_type)
        except Exception:
            logger.exception("Market score job: failed for %s/%s", asset, inst_type)
            failed += 1

    logger.info("Market score job: done — success=%d failed=%d total=%d", success, failed, len(pairs))
