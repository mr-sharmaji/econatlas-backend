"""Backfill + incremental embedding job for news_articles.

Reads rows from news_articles where embedding IS NULL, computes vectors
using the local fastembed model, and writes them back via a batched
UPDATE. Safe to run repeatedly — it only touches NULL rows.

Triggered manually via POST /ops/jobs/trigger/news_embed, or on a cron
schedule alongside the news scraper. Designed to be idempotent and
restart-safe: each batch commits independently so a crash mid-run only
loses the current batch.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from app.core.database import ensure_vector_registered, get_pool

logger = logging.getLogger(__name__)

BATCH_SIZE = 32  # fastembed is CPU-bound, 32 is a good tradeoff on small hosts
MAX_ARTICLES_PER_RUN = 2000  # cap so one run doesn't lock the DB for hours


async def run_news_embed_job(max_articles: int = MAX_ARTICLES_PER_RUN) -> dict[str, Any]:
    """Embed all news articles that don't yet have an embedding.

    Returns a summary dict with counts and timing — useful for the
    /ops/jobs/trigger response payload.
    """
    start = time.monotonic()
    pool = await get_pool()

    # Lazy-import embedding service so a failed import of fastembed
    # doesn't break the whole app on startup.
    try:
        from app.services.embedding_service import embed_texts, warmup
    except ImportError as e:
        logger.error("Embedding service unavailable: %s", e)
        return {"status": "error", "reason": "embedding_service_unavailable", "processed": 0}

    if not await warmup():
        return {"status": "error", "reason": "embedding_model_load_failed", "processed": 0}

    total_processed = 0
    total_failed = 0

    async with pool.acquire() as conn:
        if not await ensure_vector_registered(conn):
            logger.error("pgvector type not registered — is the 'vector' extension installed?")
            return {"status": "error", "reason": "pgvector_not_available", "processed": 0}

        while total_processed < max_articles:
            # Pull a batch of unembedded articles.
            # COALESCE title + summary as the embedding source so both
            # get factored into the vector.
            rows = await conn.fetch(
                "SELECT id, title, summary FROM news_articles "
                "WHERE embedding IS NULL "
                "ORDER BY timestamp DESC "
                "LIMIT $1",
                BATCH_SIZE,
            )
            if not rows:
                break

            ids = [r["id"] for r in rows]
            texts = [
                f"{(r['title'] or '').strip()}. {(r['summary'] or '').strip()}".strip(". ")
                for r in rows
            ]

            vectors = await embed_texts(texts)

            # Build one UPDATE per vector — asyncpg doesn't have a great
            # batched UPDATE story for vector columns, but we can use
            # executemany() with a single prepared statement.
            batch_data = []
            for row_id, vec in zip(ids, vectors):
                if not vec:
                    total_failed += 1
                    continue
                batch_data.append((np.array(vec, dtype=np.float32), row_id))

            if batch_data:
                await conn.executemany(
                    "UPDATE news_articles SET embedding = $1 WHERE id = $2",
                    batch_data,
                )
                total_processed += len(batch_data)

            logger.info(
                "News embed batch: +%d embedded, total=%d/%d, failed=%d",
                len(batch_data),
                total_processed,
                max_articles,
                total_failed,
            )

    elapsed = time.monotonic() - start
    summary = {
        "status": "ok",
        "processed": total_processed,
        "failed": total_failed,
        "elapsed_seconds": round(elapsed, 2),
    }
    logger.info("News embed job complete: %s", summary)
    return summary
