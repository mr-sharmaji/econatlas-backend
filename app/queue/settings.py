"""ARQ task registry and per-job retry policies."""
from __future__ import annotations

from arq import func

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
    "discover_mf_nav": (3, 60),
    "discover_mf_holdings": (3, 60),
    "market_score": (2, 60),
    "fertilizer": (2, 60),
    # High-frequency (30s interval) — re-enqueued soon, no retry needed
    "notification_check": (0, 0),
    # IPO notifications — every 30m, moderate retry
    "ipo_notification": (2, 30),
    # Gap backfill — runs once at startup, retry matters
    "gap_backfill": (2, 60),
}


def get_arq_functions() -> list:
    """Lazily import task wrappers and return ARQ function descriptors."""
    from app.queue.tasks import (
        task_brief,
        task_commodity,
        task_crypto,
        task_discover_mf_holdings,
        task_discover_mf_nav,
        task_discover_mutual_funds,
        task_discover_stock,
        task_discover_stock_price,
        task_econ_calendar,
        task_gap_backfill,
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
        func(task_discover_mf_nav, name="discover_mf_nav", timeout=7200),
        func(task_discover_mf_holdings, name="discover_mf_holdings", timeout=7200),
        func(task_rescore_stock, name="rescore_stock", timeout=600),
        func(task_rescore_mf, name="rescore_mf", timeout=600),
        func(task_ipo, name="ipo"),
        func(task_tax, name="tax"),
        func(task_fertilizer, name="fertilizer", timeout=300),
        func(task_market_score, name="market_score", timeout=600),
        func(task_notification_check, name="notification_check"),
        func(task_ipo_notification, name="ipo_notification"),
        func(task_gap_backfill, name="gap_backfill", timeout=600),
    ]
