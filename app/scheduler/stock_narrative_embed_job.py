"""Backfill embeddings for discover_stock_snapshots.why_narrative.

`why_narrative` lives inside the `score_breakdown` JSONB column (written
by the discover stock scoring job). This backfill extracts that text,
embeds it with the local fastembed model, and writes the vector into
`why_narrative_embedding` so Artha can do semantic lookups against the
stored thesis text ("which stocks have a defensive story right now?").

Idempotent: only touches rows where why_narrative_embedding IS NULL.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from app.core.database import ensure_vector_registered, get_pool

logger = logging.getLogger(__name__)

BATCH_SIZE = 32
MAX_ROWS_PER_RUN = 2000


async def run_stock_narrative_embed_job(
    max_rows: int = MAX_ROWS_PER_RUN,
) -> dict[str, Any]:
    start = time.monotonic()
    logger.info(
        "stock_narrative_embed: START batch_size=%d max_rows=%d",
        BATCH_SIZE, max_rows,
    )

    pool = await get_pool()

    try:
        from app.services.embedding_service import embed_texts, warmup
    except ImportError as e:
        logger.error("stock_narrative_embed: embedding service unavailable: %s", e)
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
            "SELECT COUNT(*) FROM discover_stock_snapshots "
            "WHERE score_breakdown->>'why_narrative' IS NOT NULL "
            "AND why_narrative_embedding IS NULL"
        )
        logger.info(
            "stock_narrative_embed: %d rows need embedding", remaining or 0,
        )

        while total_processed < max_rows:
            batch_num += 1
            batch_start = time.monotonic()
            rows = await conn.fetch(
                "SELECT id, symbol, score_breakdown->>'why_narrative' AS narrative "
                "FROM discover_stock_snapshots "
                "WHERE score_breakdown->>'why_narrative' IS NOT NULL "
                "AND why_narrative_embedding IS NULL "
                "ORDER BY id LIMIT $1",
                BATCH_SIZE,
            )
            if not rows:
                logger.info(
                    "stock_narrative_embed: no more rows at batch=%d", batch_num,
                )
                break

            ids = [r["id"] for r in rows]
            texts = [
                # Prepend symbol so the embedding anchors on the ticker
                # and ranks symbol-specific queries higher.
                f"{r['symbol']}: {r['narrative']}".strip()
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
                        "UPDATE discover_stock_snapshots "
                        "SET why_narrative_embedding = $1 WHERE id = $2",
                        np.array(vec, dtype=np.float32),
                        row_id,
                    )
                    batch_ok += 1
                except Exception as e:
                    logger.debug(
                        "stock_narrative_embed: skip row=%s err=%s",
                        row_id, str(e)[:100],
                    )
                    batch_fail += 1

            total_processed += batch_ok
            total_failed += batch_fail
            elapsed = time.monotonic() - batch_start
            rate = batch_ok / elapsed if elapsed > 0 else 0
            logger.info(
                "stock_narrative_embed: batch=%d ok=%d fail=%d "
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
        "stock_narrative_embed: COMPLETE processed=%d failed=%d batches=%d",
        total_processed, total_failed, batch_num,
    )
    return summary
