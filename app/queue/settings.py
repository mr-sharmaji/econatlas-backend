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
    # Daily cron jobs — retries matter most
    "discover_stock": (3, 60),
    "discover_mutual_funds": (3, 60),
    "discover_stock_price": (3, 60),
    "discover_mf_nav": (3, 60),
    "discover_mf_holdings": (3, 60),
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
        task_ipo,
        task_macro,
        task_market,
        task_news,
        task_tax,
    )

    return [
        func(task_market, name="market"),
        func(task_commodity, name="commodity"),
        func(task_crypto, name="crypto"),
        func(task_brief, name="brief"),
        func(task_macro, name="macro"),
        func(task_news, name="news"),
        func(task_discover_stock, name="discover_stock", timeout=7200),
        func(task_discover_mutual_funds, name="discover_mutual_funds", timeout=7200),
        func(task_discover_stock_price, name="discover_stock_price", timeout=7200),
        func(task_discover_mf_nav, name="discover_mf_nav", timeout=7200),
        func(task_discover_mf_holdings, name="discover_mf_holdings", timeout=7200),
        func(task_ipo, name="ipo"),
        func(task_tax, name="tax"),
    ]
