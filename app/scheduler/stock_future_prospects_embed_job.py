"""Backfill embeddings for stock future-prospect passages."""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from app.core.database import ensure_vector_registered, get_pool

logger = logging.getLogger(__name__)

BATCH_SIZE = 32
MAX_ROWS_PER_RUN = 4000


async def run_stock_future_prospects_embed_job(
    max_rows: int = MAX_ROWS_PER_RUN,
) -> dict[str, Any]:
    start = time.monotonic()
    logger.info(
        "stock_future_prospects_embed: START batch_size=%d max_rows=%d",
        BATCH_SIZE,
        max_rows,
    )

    pool = await get_pool()
    try:
        from app.services.embedding_service import embed_texts, warmup
    except ImportError as e:
        logger.error("stock_future_prospects_embed: embedding service unavailable: %s", e)
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
            """
            SELECT COUNT(*)
            FROM stock_future_prospect_passages
            WHERE embedding IS NULL
               OR embedding_status IN ('pending', 'failed')
            """
        )
        logger.info(
            "stock_future_prospects_embed: %d rows need embedding",
            remaining or 0,
        )

        while total_processed < max_rows:
            batch_num += 1
            batch_start = time.monotonic()
            rows = await conn.fetch(
                """
                SELECT id, symbol, passage_text
                FROM stock_future_prospect_passages
                WHERE embedding IS NULL
                   OR embedding_status IN ('pending', 'failed')
                ORDER BY source_published_at DESC NULLS LAST, created_at DESC
                LIMIT $1
                """,
                BATCH_SIZE,
            )
            if not rows:
                break

            ids = [r["id"] for r in rows]
            texts = [f"{r['symbol']}: {r['passage_text']}".strip() for r in rows]
            vectors = await embed_texts(texts)

            batch_ok = 0
            batch_fail = 0
            for row_id, vec in zip(ids, vectors):
                if not vec:
                    await conn.execute(
                        """
                        UPDATE stock_future_prospect_passages
                        SET embedding_status = 'failed', updated_at = NOW()
                        WHERE id = $1
                        """,
                        row_id,
                    )
                    batch_fail += 1
                    continue
                try:
                    await conn.execute(
                        """
                        UPDATE stock_future_prospect_passages
                        SET embedding = $1,
                            embedding_status = 'ready',
                            updated_at = NOW()
                        WHERE id = $2
                        """,
                        np.array(vec, dtype=np.float32),
                        row_id,
                    )
                    batch_ok += 1
                except Exception as e:
                    logger.debug(
                        "stock_future_prospects_embed: skip row=%s err=%s",
                        row_id,
                        str(e)[:100],
                    )
                    await conn.execute(
                        """
                        UPDATE stock_future_prospect_passages
                        SET embedding_status = 'failed', updated_at = NOW()
                        WHERE id = $1
                        """,
                        row_id,
                    )
                    batch_fail += 1

            total_processed += batch_ok
            total_failed += batch_fail
            elapsed = time.monotonic() - batch_start
            rate = batch_ok / elapsed if elapsed > 0 else 0
            logger.info(
                "stock_future_prospects_embed: batch=%d ok=%d fail=%d total=%d/%d elapsed=%.2fs rate=%.1f/s",
                batch_num,
                batch_ok,
                batch_fail,
                total_processed,
                max_rows,
                elapsed,
                rate,
            )

    elapsed = time.monotonic() - start
    summary = {
        "status": "ok",
        "processed": total_processed,
        "failed": total_failed,
        "batches": batch_num,
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info("stock_future_prospects_embed: COMPLETE %s", summary)
    return summary

