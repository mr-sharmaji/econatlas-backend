from __future__ import annotations

import logging

from app.services import tax_sync_service

logger = logging.getLogger(__name__)


async def run_tax_job(
    *,
    timeout_seconds: int = 30,
) -> None:
    try:
        result = await tax_sync_service.run_tax_sync_cycle(
            timeout_seconds=timeout_seconds,
        )
        if result.status == "failed":
            logger.warning(
                "Tax sync failed for version=%s: %s errors=%s",
                result.version,
                result.message,
                "; ".join(result.errors[:3]),
            )
        elif result.status == "activated":
            logger.info("Tax sync activated version=%s", result.version)
        else:
            logger.debug("Tax sync skipped: %s", result.message)
    except Exception:
        logger.exception("Tax scheduler job failed")
