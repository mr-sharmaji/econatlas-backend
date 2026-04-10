"""Local text embedding service using fastembed (ONNX runtime).

Uses BAAI/bge-small-en-v1.5 (384 dims) — a small, fast, high-quality
English embedding model. Runs entirely on CPU, in-process, with no
external API calls. The model is downloaded on first use (~130MB) and
cached on disk.

Usage:
    embedder = await get_embedder()
    vec = await embed_text("TCS Q2 earnings announcement")
    vecs = await embed_texts(["headline 1", "headline 2"])
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from fastembed import TextEmbedding  # noqa: F401

# bge-small-en-v1.5 produces 384-dim vectors and uses cosine similarity.
# Keep in sync with news_articles.embedding column type (vector(384)).
EMBEDDING_DIM = 384
MODEL_NAME = "BAAI/bge-small-en-v1.5"

_model: "TextEmbedding | None" = None
_model_lock = asyncio.Lock()


async def _get_model() -> "TextEmbedding":
    """Lazy-load the fastembed model on first call.

    Downloads the model weights (~130MB) on first run and caches them
    to the default fastembed cache dir. Subsequent calls return the
    cached instance immediately.
    """
    global _model
    if _model is not None:
        return _model
    async with _model_lock:
        if _model is not None:
            return _model
        try:
            from fastembed import TextEmbedding
        except ImportError as e:
            logger.error(
                "embedding: fastembed not installed — semantic search disabled. "
                "Add 'fastembed' to requirements.txt and rebuild the image."
            )
            raise RuntimeError(
                "fastembed is not installed — run `pip install fastembed`"
            ) from e

        import time as _time
        t0 = _time.monotonic()
        logger.info(
            "embedding: loading model=%s (first call, may download ~130MB)",
            MODEL_NAME,
        )
        loop = asyncio.get_running_loop()
        # TextEmbedding.__init__ is sync and may download weights — run in
        # the default executor so we don't block the event loop.
        try:
            _model = await loop.run_in_executor(
                None, lambda: TextEmbedding(model_name=MODEL_NAME)
            )
        except Exception:
            logger.exception("embedding: model load FAILED for %s", MODEL_NAME)
            raise
        elapsed = _time.monotonic() - t0
        logger.info(
            "embedding: model=%s loaded in %.1fs dim=%d",
            MODEL_NAME, elapsed, EMBEDDING_DIM,
        )
        return _model


async def embed_text(text: str) -> list[float]:
    """Embed a single text and return a 384-dim vector.

    Returns an empty list on failure — the caller should fall back to
    a non-vector search path.
    """
    text = (text or "").strip()
    if not text:
        logger.debug("embedding: embed_text skipped — empty input")
        return []
    try:
        vectors = await embed_texts([text])
        vec = vectors[0] if vectors else []
        if not vec:
            logger.debug("embedding: embed_text returned empty for len=%d", len(text))
        return vec
    except Exception:
        logger.warning(
            "embedding: embed_text FAILED (len=%d) text=%r",
            len(text), text[:80],
            exc_info=True,
        )
        return []


async def embed_texts(texts: list[str]) -> list[list[float]]:
    """Embed a batch of texts and return a list of 384-dim vectors.

    Empty inputs in the batch get an empty list in the corresponding
    output position. On model-load failure returns a list of empty
    lists so the caller can handle degradation gracefully.
    """
    if not texts:
        return []
    import time as _time
    batch_start = _time.monotonic()
    try:
        model = await _get_model()
    except Exception:
        logger.warning(
            "embedding: model unavailable — returning %d empty vectors", len(texts),
        )
        return [[] for _ in texts]

    # Filter out empty strings but remember positions
    indexed = [(i, t.strip()) for i, t in enumerate(texts) if t and t.strip()]
    if not indexed:
        logger.debug("embedding: embed_texts skipped — all inputs empty (n=%d)", len(texts))
        return [[] for _ in texts]

    inputs = [t for _, t in indexed]
    loop = asyncio.get_running_loop()

    def _encode() -> list[list[float]]:
        # TextEmbedding.embed() returns a generator of numpy arrays; convert
        # each to a plain Python list for asyncpg/JSON compatibility.
        return [list(map(float, vec)) for vec in model.embed(inputs)]

    try:
        encoded = await loop.run_in_executor(None, _encode)
    except Exception:
        logger.warning(
            "embedding: batch encode FAILED (n=%d)", len(inputs),
            exc_info=True,
        )
        return [[] for _ in texts]

    elapsed_ms = (_time.monotonic() - batch_start) * 1000
    logger.info(
        "embedding: batch OK n=%d (kept=%d) dim=%d elapsed_ms=%.1f",
        len(texts), len(inputs), EMBEDDING_DIM, elapsed_ms,
    )

    # Reinsert into full-length result list at original positions
    out: list[list[float]] = [[] for _ in texts]
    for (orig_idx, _), vec in zip(indexed, encoded):
        out[orig_idx] = vec
    return out


async def warmup() -> bool:
    """Eagerly load the model so the first request is fast.

    Safe to call at app startup. Returns True on success, False if the
    model couldn't be loaded (in which case semantic search will be
    disabled and the app falls back to trigram/ILIKE matching).
    """
    try:
        await _get_model()
        return True
    except Exception as e:
        logger.warning("Embedding model warmup failed: %s", e)
        return False
