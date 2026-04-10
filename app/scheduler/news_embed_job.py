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
    logger.info(
        "news_embed: START batch_size=%d max_articles=%d",
        BATCH_SIZE, max_articles,
    )

    pool = await get_pool()

    # Lazy-import embedding service so a failed import of fastembed
    # doesn't break the whole app on startup.
    try:
        from app.services.embedding_service import embed_texts, warmup
    except ImportError as e:
        logger.error(
            "news_embed: embedding service unavailable — install fastembed. err=%s", e,
        )
        return {"status": "error", "reason": "embedding_service_unavailable", "processed": 0}

    if not await warmup():
        logger.error(
            "news_embed: embedding model warmup failed — check fastembed install "
            "and cache dir write permissions"
        )
        return {"status": "error", "reason": "embedding_model_load_failed", "processed": 0}

    total_processed = 0
    total_failed = 0
    batch_num = 0

    async with pool.acquire() as conn:
        if not await ensure_vector_registered(conn):
            logger.error(
                "news_embed: pgvector type not registered — is the 'vector' "
                "extension installed? Run 'CREATE EXTENSION vector' manually."
            )
            return {"status": "error", "reason": "pgvector_not_available", "processed": 0}

        # Report how many rows need embedding at the start so we can see
        # the scale of the backfill from a single log line.
        remaining = await conn.fetchval(
            "SELECT COUNT(*) FROM news_articles WHERE embedding IS NULL"
        )
        logger.info(
            "news_embed: %d articles without embeddings (backfill needed)",
            remaining or 0,
        )

        while total_processed < max_articles:
            batch_num += 1
            batch_start = time.monotonic()
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
                logger.info("news_embed: no more unembedded rows — stopping at batch=%d", batch_num)
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
            batch_failed = 0
            for row_id, vec in zip(ids, vectors):
                if not vec:
                    batch_failed += 1
                    continue
                batch_data.append((np.array(vec, dtype=np.float32), row_id))
            total_failed += batch_failed

            if batch_data:
                try:
                    await conn.executemany(
                        "UPDATE news_articles SET embedding = $1 WHERE id = $2",
                        batch_data,
                    )
                except Exception:
                    logger.exception(
                        "news_embed: batch=%d UPDATE failed for %d rows",
                        batch_num, len(batch_data),
                    )
                    return {
                        "status": "error",
                        "reason": "db_update_failed",
                        "processed": total_processed,
                        "failed": total_failed,
                    }
                total_processed += len(batch_data)

            batch_elapsed = time.monotonic() - batch_start
            rate = len(batch_data) / batch_elapsed if batch_elapsed > 0 else 0
            logger.info(
                "news_embed: batch=%d embedded=%d failed=%d total=%d/%d "
                "elapsed=%.2fs rate=%.1f/s",
                batch_num, len(batch_data), batch_failed,
                total_processed, max_articles, batch_elapsed, rate,
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
        "news_embed: COMPLETE processed=%d failed=%d batches=%d elapsed=%.2fs rate=%.1f/s",
        total_processed, total_failed, batch_num, elapsed, rate,
    )
    return summary
