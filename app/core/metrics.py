"""Prometheus metrics for EconAtlas backend.

Exposes:
- HTTP request duration histogram (by method, path, status)
- Database pool gauges
- Job duration histogram
- Data freshness gauges

Usage:
  from app.core.metrics import REQUEST_DURATION, track_job
"""
from __future__ import annotations

import time
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    REGISTRY,
)

logger = logging.getLogger(__name__)

# ── HTTP request metrics ──────────────────────────────────────────────

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path", "status"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

# ── Database pool metrics ─────────────────────────────────────────────

DB_POOL_SIZE = Gauge("db_pool_size", "Current database connection pool size")
DB_POOL_FREE = Gauge("db_pool_free", "Free connections in pool")
DB_POOL_USED = Gauge("db_pool_used", "Used connections in pool")

# ── Job metrics ───────────────────────────────────────────────────────

JOB_DURATION = Histogram(
    "job_duration_seconds",
    "Scheduler job duration",
    ["job_name"],
    buckets=[0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600, 1800],
)

JOB_ERRORS = Counter(
    "job_errors_total",
    "Total job failures",
    ["job_name"],
)

# ── Data freshness ────────────────────────────────────────────────────

DATA_FRESHNESS = Gauge(
    "data_freshness_age_seconds",
    "Seconds since latest data for each source",
    ["source"],
)

# ── In-process request stats (for /ops/health) ───────────────────────

_REQUEST_LOG: deque[dict[str, Any]] = deque(maxlen=5000)


def record_request(method: str, path: str, status: int, duration: float) -> None:
    """Record a request for both Prometheus and in-process stats."""
    # Normalize path: strip query params and IDs for grouping
    clean = _normalize_path(path)
    REQUEST_DURATION.labels(method=method, path=clean, status=str(status)).observe(duration)
    REQUEST_COUNT.labels(method=method, path=clean, status=str(status)).inc()
    _REQUEST_LOG.append({
        "ts": time.time(),
        "path": clean,
        "status": status,
        "duration": duration,
    })


def get_request_stats() -> dict:
    """Return request stats for the last minute."""
    cutoff = time.time() - 60
    recent = [r for r in _REQUEST_LOG if r["ts"] >= cutoff]
    if not recent:
        return {"requests_1m": 0, "avg_latency_ms": 0, "error_rate_1m": 0}

    total = len(recent)
    errors = sum(1 for r in recent if r["status"] >= 500)
    avg_lat = sum(r["duration"] for r in recent) / total

    # Top 5 slowest endpoints
    from collections import defaultdict
    path_latencies: dict[str, list[float]] = defaultdict(list)
    for r in recent:
        path_latencies[r["path"]].append(r["duration"])
    slowest = sorted(
        [(p, sum(lats) / len(lats)) for p, lats in path_latencies.items()],
        key=lambda x: -x[1],
    )[:5]

    return {
        "requests_1m": total,
        "avg_latency_ms": round(avg_lat * 1000, 1),
        "error_rate_1m": round(errors / total, 4) if total else 0,
        "slowest_endpoints": [{p: round(lat * 1000, 1)} for p, lat in slowest],
    }


def _normalize_path(path: str) -> str:
    """Collapse variable path segments for grouping.

    /screener/stocks/RELIANCE.NS/history → /screener/stocks/{symbol}/history
    /market/story?asset=Sensex → /market/story
    """
    # Strip query params
    path = path.split("?")[0]
    parts = path.strip("/").split("/")
    normalized = []
    for i, part in enumerate(parts):
        # Collapse known variable segments
        if i > 0 and parts[i - 1] in ("stocks", "mutual-funds", "mf"):
            normalized.append("{id}")
        elif "." in part and part.upper() == part:
            # Looks like a stock symbol (RELIANCE.NS)
            normalized.append("{symbol}")
        else:
            normalized.append(part)
    return "/" + "/".join(normalized)


def get_prometheus_metrics() -> bytes:
    """Generate Prometheus exposition format."""
    return generate_latest(REGISTRY)


def get_prometheus_content_type() -> str:
    return CONTENT_TYPE_LATEST
