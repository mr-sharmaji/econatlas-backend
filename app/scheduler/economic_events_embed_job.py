"""Backfill embeddings for economic_events (news_market_linked_signal rows).

Each row has event_type + entity + impact. We concatenate them into a
single descriptive sentence and embed it so Artha can answer queries
like "what macro events were triggered by RBI decisions last quarter?"
via semantic search rather than LIKE filters.

Idempotent: only touches rows where event_embedding IS NULL.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from app.core.database import ensure_vector_registered, get_pool

logger = logging.getLogger(__name__)

BATCH_SIZE = 64
MAX_ROWS_PER_RUN = 5000


async def run_economic_events_embed_job(
    max_rows: int = MAX_ROWS_PER_RUN,
) -> dict[str, Any]:
    start = time.monotonic()
    logger.info(
        "economic_events_embed: START batch_size=%d max_rows=%d",
        BATCH_SIZE, max_rows,
    )

    pool = await get_pool()

    try:
        from app.services.embedding_service import embed_texts, warmup
    except ImportError as e:
        logger.error("economic_events_embed: embedding service unavailable: %s", e)
        return {"status": "error", "reason": "embedding_service_unavailable", "processed": 0}

    if not await warmup():
        return {"status": "error", "reason": "embedding_model_load_failed", "processed": 0}

    total_processed = 0
    total_failed = 0
    batch_num = 0

    async with pool.acquire() as conn:
        if not await ensure_vector_registered(conn):
            return {"status": "error", "reason": "pgvector_not_available", "processed": 0}

        remaining = await conn.fetchval(
            "SELECT COUNT(*) FROM economic_events WHERE event_embedding IS NULL"
        )
        logger.info(
            "economic_events_embed: %d rows need embedding", remaining or 0,
        )

        while total_processed < max_rows:
            batch_num += 1
            batch_start = time.monotonic()
            rows = await conn.fetch(
                "SELECT id, event_type, entity, impact FROM economic_events "
                "WHERE event_embedding IS NULL "
                "ORDER BY created_at DESC LIMIT $1",
                BATCH_SIZE,
            )
            if not rows:
                logger.info(
                    "economic_events_embed: no more rows at batch=%d", batch_num,
                )
                break

            ids = [r["id"] for r in rows]
            texts = [
                # Compose: "<event_type> for <entity>: <impact>"
                # Reads naturally and anchors the embedding on all 3 fields.
                f"{r['event_type']} for {r['entity']}: {r['impact']}"
                for r in rows
            ]
            vectors = await embed_texts(texts)

            batch_ok = 0
            batch_fail = 0
            for row_id, vec in zip(ids, vectors):
                if not vec:
                    batch_fail += 1
                    continue
                try:
                    await conn.execute(
                        "UPDATE economic_events SET event_embedding = $1 WHERE id = $2",
                        np.array(vec, dtype=np.float32),
                        row_id,
                    )
                    batch_ok += 1
                except Exception as e:
                    logger.debug(
                        "economic_events_embed: skip row=%s err=%s",
                        row_id, str(e)[:100],
                    )
                    batch_fail += 1

            total_processed += batch_ok
            total_failed += batch_fail
            elapsed = time.monotonic() - batch_start
            rate = batch_ok / elapsed if elapsed > 0 else 0
            logger.info(
                "economic_events_embed: batch=%d ok=%d fail=%d "
                "total=%d/%d elapsed=%.2fs rate=%.1f/s",
                batch_num, batch_ok, batch_fail,
                total_processed, max_rows, elapsed, rate,
            )

    elapsed = time.monotonic() - start
    rate = total_processed / elapsed if elapsed > 0 else 0
    summary = {
        "status": "ok",
        "processed": total_processed,
        "failed": total_failed,
        "batches": batch_num,
        "elapsed_seconds": round(elapsed, 2),
        "rate_per_second": round(rate, 1),
    }
    logger.info(
        "economic_events_embed: COMPLETE processed=%d failed=%d batches=%d",
        total_processed, total_failed, batch_num,
    )
    return summary
