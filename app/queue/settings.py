"""ARQ task registry and per-job retry policies."""
from __future__ import annotations

from arq import func

# Legacy discover_stock aliases that have existed in Redis/ARQ state.
# We canonicalize them to the base job ID so scheduler dedupe, worker
# in-progress tracking, and stale-state cleanup all agree on one family.
_LEGACY_JOB_ID_ALIASES: dict[str, str] = {
    "discover_stock_retry": "discover_stock",
    "startup_discover_stock": "discover_stock",
}

# (max_retries, delay_base_seconds)
# Backoff formula: delay_base * 2^(attempt - 1)
JOB_RETRY_POLICIES: dict[str, tuple[int, int]] = {
    # High-frequency (30s interval) — re-enqueued soon, no retry needed
    "market": (0, 0),
    "commodity": (0, 0),
    "crypto": (0, 0),
    # Medium-frequency
    "macro": (2, 30),
    "brief": (2, 10),
    "ipo": (2, 10),
    # Lower-frequency
    "news": (1, 15),
    "tax": (3, 30),
    "imf_forecast": (2, 30),
    "econ_calendar": (2, 30),
    # Daily cron jobs — retries matter most
    "discover_stock": (3, 60),
    "discover_mutual_funds": (3, 60),
    "discover_stock_price": (3, 60),
    # Intraday 30-min live-price refresh — 0 retries because the next
    # tick is only 30 min away and we never want two copies to pile up
    # and race the heavy daily pipeline at 16:00 IST.
    "discover_stock_intraday": (0, 0),
    # One-shot ops tool — no auto-retry because it's idempotent and
    # manually triggered. If it fails, re-trigger explicitly.
    "discover_stock_intraday_backfill": (0, 0),
    "discover_stock_intraday_autofill": (0, 0),
    "discover_mf_nav": (3, 60),
    "market_score": (2, 60),
    "fertilizer": (2, 60),
    # High-frequency (30s interval) — re-enqueued soon, no retry needed
    "notification_check": (0, 0),
    # IPO notifications — every 30m, moderate retry
    "ipo_notification": (2, 30),
    # Gap backfill — runs once at startup, retry matters
    "gap_backfill": (2, 60),
    # Intraday gap backfill — every 30 min, cheap retry
    "intraday_gap_backfill": (1, 30),
    # News embedding backfill — CPU-bound, self-idempotent
    "news_embed": (1, 30),
    "stock_future_prospects": (1, 60),
    "stock_future_prospects_recent": (1, 30),
    "stock_future_prospects_embed": (1, 30),
    # Weekly broker charges scraper — retries matter, runs once a week
    "broker_charges": (2, 60),
}


def canonicalize_job_id(job_id: str | None) -> str | None:
    """Map legacy ARQ/scheduler job ids onto their canonical family id."""
    if not job_id:
        return job_id
    return _LEGACY_JOB_ID_ALIASES.get(job_id, job_id)


def expand_job_family_ids(job_name: str) -> set[str]:
    """Return canonical + known legacy ids for a job family."""
    ids = {
        job_name,
        f"startup_{job_name}",
        f"{job_name}_manual",
    }
    if job_name == "discover_stock":
        ids.update(_LEGACY_JOB_ID_ALIASES.keys())
    return ids


def get_arq_functions() -> list:
    """Lazily import task wrappers and return ARQ function descriptors."""
    from app.queue.tasks import (
        task_brief,
        task_broker_charges,
        task_commodity,
        task_crypto,
        task_discover_mf_nav,
        task_discover_mutual_funds,
        task_discover_stock,
        task_discover_stock_intraday,
        task_discover_stock_intraday_autofill,
        task_discover_stock_intraday_backfill,
        task_discover_stock_price,
        task_econ_calendar,
        task_gap_backfill,
        task_intraday_gap_backfill,
        task_imf_forecast,
        task_ipo,
        task_ipo_notification,
        task_macro,
        task_market,
        task_news,
        task_rescore_mf,
        task_rescore_stock,
        task_fertilizer,
        task_market_score,
        task_news_embed,
        task_stock_future_prospects,
        task_stock_future_prospects_recent,
        task_stock_future_prospects_embed,
        task_notification_check,
        task_tax,
    )

    return [
        func(task_market, name="market"),
        func(task_commodity, name="commodity"),
        func(task_crypto, name="crypto"),
        func(task_brief, name="brief"),
        func(task_macro, name="macro"),
        func(task_imf_forecast, name="imf_forecast"),
        func(task_econ_calendar, name="econ_calendar"),
        func(task_news, name="news"),
        func(task_discover_stock, name="discover_stock", timeout=7200),
        func(task_discover_mutual_funds, name="discover_mutual_funds", timeout=7200),
        func(task_discover_stock_price, name="discover_stock_price", timeout=7200),
        # Intraday is capped at 5 minutes — lightweight UPDATE only.
        func(task_discover_stock_intraday, name="discover_stock_intraday", timeout=300),
        # Backfill can legitimately run up to 15 min over ~2000 symbols.
        func(
            task_discover_stock_intraday_backfill,
            name="discover_stock_intraday_backfill",
            timeout=1200,
        ),
        # Autofill sweeper: ~145s at 16 req/s for 2300 symbols, capped at 10 min.
        func(
            task_discover_stock_intraday_autofill,
            name="discover_stock_intraday_autofill",
            timeout=600,
        ),
        func(task_discover_mf_nav, name="discover_mf_nav", timeout=7200),
        func(task_rescore_stock, name="rescore_stock", timeout=600),
        func(task_rescore_mf, name="rescore_mf", timeout=600),
        func(task_ipo, name="ipo"),
        func(task_tax, name="tax"),
        func(task_fertilizer, name="fertilizer", timeout=300),
        func(task_market_score, name="market_score", timeout=600),
        func(task_notification_check, name="notification_check"),
        func(task_ipo_notification, name="ipo_notification"),
        func(task_gap_backfill, name="gap_backfill", timeout=600),
        # Intraday gap backfill — 40+ Yahoo calls at 0.2s pacing ≈ 8s,
        # plus parsing and DB upsert. 10-min ceiling is generous.
        func(task_intraday_gap_backfill, name="intraday_gap_backfill", timeout=600),
        # CPU-bound embedding — allow up to 30 min for a full backfill pass
        func(task_news_embed, name="news_embed", timeout=1800),
        func(task_stock_future_prospects, name="stock_future_prospects", timeout=3600),
        func(
            task_stock_future_prospects_recent,
            name="stock_future_prospects_recent",
            timeout=1800,
        ),
        func(
            task_stock_future_prospects_embed,
            name="stock_future_prospects_embed",
            timeout=1800,
        ),
        # Weekly broker charges scraper — scrapes 4 broker sites
        func(task_broker_charges, name="broker_charges", timeout=120),
    ]
