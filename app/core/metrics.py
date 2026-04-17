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

# ── System metrics ────────────────────────────────────────────────────

SYSTEM_CPU_PERCENT = Gauge("system_cpu_percent", "System CPU usage %")
SYSTEM_MEMORY_USED = Gauge("system_memory_used_bytes", "System memory used")
SYSTEM_MEMORY_TOTAL = Gauge("system_memory_total_bytes", "System memory total")
SYSTEM_MEMORY_PERCENT = Gauge("system_memory_percent", "System memory usage %")
SYSTEM_DISK_USED = Gauge("system_disk_used_bytes", "Disk used")
SYSTEM_DISK_TOTAL = Gauge("system_disk_total_bytes", "Disk total")
SYSTEM_DISK_PERCENT = Gauge("system_disk_percent", "Disk usage %")
SYSTEM_LOAD_1M = Gauge("system_load_avg_1m", "1-min load average")
SYSTEM_LOAD_5M = Gauge("system_load_avg_5m", "5-min load average")
SYSTEM_LOAD_15M = Gauge("system_load_avg_15m", "15-min load average")
SYSTEM_NET_BYTES_SENT = Gauge("system_net_bytes_sent", "Network bytes sent")
SYSTEM_NET_BYTES_RECV = Gauge("system_net_bytes_recv", "Network bytes received")
SYSTEM_TCP_CONNECTIONS = Gauge("system_tcp_connections", "Active TCP connections", ["state"])
SYSTEM_SWAP_USED = Gauge("system_swap_used_bytes", "Swap used")

# ── Database pool metrics ─────────────────────────────────────────────

DB_POOL_SIZE = Gauge("db_pool_size", "Current database connection pool size (allocated, not max)")
DB_POOL_MAX = Gauge("db_pool_max_size", "Maximum database connection pool size (configured limit)")
DB_POOL_FREE = Gauge("db_pool_free", "Free connections in pool")
DB_POOL_USED = Gauge("db_pool_used", "Used connections in pool")
DB_LATENCY = Gauge("db_latency_seconds", "Database query latency")
DB_CACHE_HIT_RATIO = Gauge("db_cache_hit_ratio", "PostgreSQL buffer cache hit %")
DB_ACTIVE_QUERIES = Gauge("db_active_queries", "Number of active queries")
DB_LOCK_WAITS = Gauge("db_lock_waits", "Number of waiting locks")
DB_DEADLOCKS = Counter("db_deadlocks_total", "Total deadlocks")
DB_TX_COMMITTED = Gauge("db_transactions_committed_total", "Total committed transactions")
DB_TX_ROLLED_BACK = Gauge("db_transactions_rolled_back_total", "Total rolled back transactions")
DB_TABLE_ROWS = Gauge("db_table_rows", "Row count per table", ["table"])
DB_TABLE_SIZE = Gauge("db_table_size_bytes", "Total size per table", ["table"])
DB_DEAD_TUPLES = Gauge("db_dead_tuples", "Dead tuples per table", ["table"])
DB_DUPLICATES = Gauge("db_duplicates_count", "Duplicate rows in market_prices")

# ── Redis metrics ─────────────────────────────────────────────────────

REDIS_CONNECTED = Gauge("redis_connected", "Redis connection status (1=up, 0=down)")
REDIS_LATENCY = Gauge("redis_latency_seconds", "Redis ping latency")
REDIS_MEMORY_USED = Gauge("redis_memory_used_bytes", "Redis memory used")
REDIS_KEYS = Gauge("redis_keys_total", "Total Redis keys")

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

# Total job invocations (success or failure) — paired with JOB_ERRORS
# this gives a clean PromQL success ratio:
#   1 - (rate(job_errors_total[5m]) / rate(jobs_started_total[5m]))
JOBS_STARTED = Counter(
    "jobs_started_total",
    "Total scheduler job invocations",
    ["job_name"],
)

JOB_RUNNING = Gauge("jobs_running_count", "Number of currently running jobs")

# Unix epoch seconds of the most recent run for each job. Subtract from
# `time()` in Grafana to chart "time since last run". Useful for
# spotting silently broken cron jobs.
JOB_LAST_RUN_TIMESTAMP = Gauge(
    "job_last_run_timestamp_seconds",
    "Unix timestamp of the last run (success or failure) per job",
    ["job_name"],
)
JOB_LAST_SUCCESS_TIMESTAMP = Gauge(
    "job_last_success_timestamp_seconds",
    "Unix timestamp of the last successful run per job",
    ["job_name"],
)

# ── ARQ queue depth + DLQ size ────────────────────────────────────────
# Backed by the background collector reading Redis directly.
ARQ_QUEUE_DEPTH = Gauge(
    "arq_queue_depth",
    "Number of jobs currently queued in ARQ (waiting for a worker)",
)
ARQ_DLQ_SIZE = Gauge(
    "arq_dlq_size",
    "Number of jobs in the dead letter queue (exhausted retries)",
)

# ── Scraper rate-limiting ────────────────────────────────────────────
# Incremented from BaseScraper._mark_rate_limited so we can chart how
# often each upstream throttles us. Useful when discover_stock_intraday
# returns 22% updates — you want to know which host pushed back.
SCRAPER_RATE_LIMITED = Counter(
    "scraper_rate_limited_total",
    "Number of times a scraper got rate-limited (HTTP 429/503 or marker)",
    ["host"],
)

# ── Data freshness ────────────────────────────────────────────────────

DATA_FRESHNESS = Gauge(
    "data_freshness_age_seconds",
    "Seconds since latest data for each source",
    ["source"],
)

DATA_ROWS_TOTAL = Gauge("data_rows_total", "Total rows per data table", ["table"])
DATA_STALE_STOCKS = Gauge("data_stale_stocks", "Number of stale stock snapshots")

# ── External API health ──────────────────────────────────────────────
# Every outbound HTTP fetch to an upstream data provider is recorded
# here. Aggregated by (provider, endpoint) only — endpoint is
# collapsed to the URL's first two path segments so we don't explode
# cardinality on per-symbol URLs. Labels:
#   provider: yahoo | google_finance | upstox | mfapi | coingecko | ...
#   endpoint: normalized path prefix, e.g. /v8/finance/chart
#   status:   ok | 4xx | 5xx | timeout | error

EXT_API_REQUESTS = Counter(
    "external_api_requests_total",
    "External API requests by provider / endpoint / status",
    ["provider", "endpoint", "status"],
)
EXT_API_DURATION = Histogram(
    "external_api_duration_seconds",
    "External API call duration",
    ["provider", "endpoint"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
EXT_API_RATE_LIMITED = Counter(
    "external_api_rate_limited_total",
    "External API 429 / 503 rate-limit events",
    ["provider"],
)

# ── Stock intraday autofill coverage ─────────────────────────────────
# Updated at the end of every autofill sweep — one gauge set per run.
# Labeled by job_name so future autofill sweepers (MF, indices) can
# share the same metrics.

AUTOFILL_SYMBOLS_OK = Gauge(
    "autofill_symbols_ok",
    "Symbols with fresh data in the most recent autofill sweep",
    ["job_name"],
)
AUTOFILL_SYMBOLS_EMPTY = Gauge(
    "autofill_symbols_empty",
    "Symbols that returned no bars in the most recent autofill sweep",
    ["job_name"],
)
AUTOFILL_ROWS_QUEUED = Gauge(
    "autofill_rows_queued",
    "Rows queued for upsert in the most recent autofill sweep",
    ["job_name"],
)
AUTOFILL_SNAPSHOTS_UPDATED = Gauge(
    "autofill_snapshots_updated",
    "Snapshots updated in the most recent autofill sweep",
    ["job_name"],
)
AUTOFILL_COVERAGE_RATIO = Gauge(
    "autofill_coverage_ratio",
    "Fraction of target symbols filled in the most recent sweep (0-1)",
    ["job_name"],
)
AUTOFILL_LAST_DURATION = Gauge(
    "autofill_last_duration_seconds",
    "Wall-clock duration of the most recent autofill sweep",
    ["job_name"],
)

# ── Market gate rejections ───────────────────────────────────────────
# Counts how many intraday rows were dropped by each defensive gate
# in build_market_intraday_rows_for_open. Useful for confirming the
# stale-quote defenses are firing and for spotting regressions.

MARKET_GATE_REJECTIONS = Counter(
    "market_intraday_gate_rejections_total",
    "Stale market intraday rows dropped at build time",
    ["reason"],  # pre_session | post_session | price_equality
)

# ── Scheduler heartbeat + misfires ──────────────────────────────────
# The APScheduler loop updates SCHEDULER_HEARTBEAT every N seconds.
# If this gauge stops advancing, the scheduler is dead — alert.
# JOB_MISFIRES increments when APScheduler reports a missed job
# (i.e. the runtime ran past its scheduled fire time by more than
# misfire_grace_time).

SCHEDULER_HEARTBEAT = Gauge(
    "scheduler_heartbeat_timestamp_seconds",
    "Unix timestamp of the most recent APScheduler loop tick",
)
JOB_MISFIRES = Counter(
    "job_misfires_total",
    "APScheduler misfire events (job missed its scheduled window)",
    ["job_name"],
)

# ── Data coverage / freshness (aggregated) ──────────────────────────
# The existing DATA_FRESHNESS already tracks age-of-latest-row per
# table. These additions expose coverage as a 0-1 ratio (how many
# target assets have a row for today) and a count of "stale" assets
# above a per-table threshold.

DATA_COVERAGE_RATIO = Gauge(
    "data_coverage_ratio",
    "Fraction of expected assets with a row for today (0-1)",
    ["table"],
)
DATA_STALE_ASSETS_COUNT = Gauge(
    "data_stale_assets_count",
    "Count of assets whose latest row is older than the staleness threshold",
    ["table"],
)


# ── Artha (chat) metrics ─────────────────────────────────────────────
ARTHA_REQUESTS = Counter(
    "artha_requests_total",
    "Total Artha chat requests",
    ["endpoint"],  # /chat, /suggestions, etc.
)
ARTHA_LLM_DURATION = Histogram(
    "artha_llm_duration_seconds",
    "Artha LLM call duration",
    ["model"],
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60),
)
ARTHA_TOOL_CALLS = Counter(
    "artha_tool_calls_total",
    "Artha tool invocations by tool name",
    ["tool"],
)
ARTHA_CACHE_HITS = Counter(
    "artha_cache_hits_total",
    "Artha prefetch cache hits",
)
ARTHA_CACHE_MISSES = Counter(
    "artha_cache_misses_total",
    "Artha prefetch cache misses",
)

# ── Notification metrics ────────────────────────────────────────────
NOTIFICATION_SENT = Counter(
    "notification_sent_total",
    "Push notifications sent",
    ["type", "status"],  # type=market_open|ipo|price_alert, status=ok|failed
)

# ── Watchlist metrics ───────────────────────────────────────────────
WATCHLIST_OPS = Counter(
    "watchlist_ops_total",
    "Watchlist add/remove operations",
    ["op"],  # add | remove
)
WATCHLIST_DEVICES = Gauge(
    "watchlist_active_devices",
    "Number of distinct devices with watchlists",
)


# ── Pre-initialize labeled counters so /metrics emits them at 0 ──────
# prometheus_client only emits a labeled metric after its first .inc().
# Without this, Grafana shows "No data" until the first real event
# even when OR vector(0) is in the PromQL (because the metric name
# itself doesn't exist in the registry until first use).
ARTHA_REQUESTS.labels(endpoint="/chat")
ARTHA_REQUESTS.labels(endpoint="/suggestions")
NOTIFICATION_SENT.labels(type="market_open", status="ok")
NOTIFICATION_SENT.labels(type="market_open", status="failed")
NOTIFICATION_SENT.labels(type="unknown", status="ok")
WATCHLIST_OPS.labels(op="put")
WATCHLIST_OPS.labels(op="remove")


def record_ext_api(
    provider: str,
    endpoint: str,
    status_code: int | None,
    duration_seconds: float,
    *,
    error: str | None = None,
) -> None:
    """Record one external API call. Call from every HTTP call site.

    `status_code` is the HTTP response code when we got one. Pass None
    when the call errored without a response (timeout, DNS, connection
    refused) and use `error` to categorise ("timeout" / "error").
    """
    if error:
        status = error
    elif status_code is None:
        status = "error"
    elif status_code < 400:
        status = "ok"
    elif status_code < 500:
        status = f"{status_code // 100}xx"
    else:
        status = f"{status_code // 100}xx"
    try:
        EXT_API_REQUESTS.labels(
            provider=provider, endpoint=endpoint, status=status,
        ).inc()
        EXT_API_DURATION.labels(
            provider=provider, endpoint=endpoint,
        ).observe(max(0.0, duration_seconds))
        if status_code in (429, 503):
            EXT_API_RATE_LIMITED.labels(provider=provider).inc()
    except Exception:
        # Never let metrics failures crash the scraper
        pass


def classify_ext_api_url(url: str) -> tuple[str, str]:
    """Return (provider, endpoint) labels for an outbound URL.

    Endpoint is the URL's first ≤2 path segments so per-symbol URLs
    (e.g. /v8/finance/chart/RELIANCE.NS) collapse to a single label
    value /v8/finance/chart.
    """
    from urllib.parse import urlparse
    try:
        u = urlparse(url)
    except Exception:
        return ("unknown", "unknown")
    host = (u.hostname or "").lower()
    if "upstox.com" in host:
        provider = "upstox"
    elif "yahoo.com" in host or "yimg.com" in host:
        provider = "yahoo"
    elif "google.com" in host:
        provider = "google_finance"
    elif "mfapi.in" in host:
        provider = "mfapi"
    elif "coingecko.com" in host:
        provider = "coingecko"
    elif "groww.in" in host:
        provider = "groww"
    elif "etmoney.com" in host:
        provider = "etmoney"
    elif "nseindia.com" in host:
        provider = "nse"
    elif "bseindia.com" in host:
        provider = "bse"
    else:
        provider = host.replace("www.", "").split(".")[0] or "unknown"
    # Collapse the path to the first 2 segments
    segs = [s for s in (u.path or "/").strip("/").split("/") if s][:2]
    endpoint = "/" + "/".join(segs) if segs else "/"
    return (provider, endpoint)


# ── Log metrics ───────────────────────────────────────────────────────

LOG_ERRORS_1H = Gauge("log_errors_1h", "Errors in the last hour")
LOG_WARNINGS_1H = Gauge("log_warnings_1h", "Warnings in the last hour")

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


# ── Background collector ──────────────────────────────────────────────
# Updates system/DB/Redis gauges every 15s so Prometheus scrapes
# always get fresh values without blocking HTTP requests.

_collector_started = False


async def start_metrics_collector():
    """Launch the background metrics collector. Call once at app startup."""
    global _collector_started
    if _collector_started:
        return
    _collector_started = True

    import asyncio
    asyncio.create_task(_collect_loop())
    logger.info("Metrics background collector started (15s interval)")


async def _collect_loop():
    import asyncio
    while True:
        try:
            await _collect_once()
        except Exception as exc:
            logger.debug("Metrics collector error: %s", exc)
        await asyncio.sleep(15)


async def _collect_once():
    """Single collection pass — update all gauges."""
    import os
    import psutil
    from collections import Counter as _Ctr

    # ── System ──
    try:
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk = psutil.disk_usage("/")
        load = os.getloadavg()
        net = psutil.net_io_counters()

        SYSTEM_CPU_PERCENT.set(psutil.cpu_percent(interval=0))
        SYSTEM_MEMORY_USED.set(mem.used)
        SYSTEM_MEMORY_TOTAL.set(mem.total)
        SYSTEM_MEMORY_PERCENT.set(mem.percent)
        SYSTEM_DISK_USED.set(disk.used)
        SYSTEM_DISK_TOTAL.set(disk.total)
        SYSTEM_DISK_PERCENT.set(disk.percent)
        SYSTEM_LOAD_1M.set(load[0])
        SYSTEM_LOAD_5M.set(load[1])
        SYSTEM_LOAD_15M.set(load[2])
        SYSTEM_NET_BYTES_SENT.set(net.bytes_sent)
        SYSTEM_NET_BYTES_RECV.set(net.bytes_recv)
        SYSTEM_SWAP_USED.set(swap.used)

        conns = psutil.net_connections(kind="tcp")
        states = _Ctr(c.status for c in conns)
        for state in ("ESTABLISHED", "TIME_WAIT", "CLOSE_WAIT", "LISTEN", "SYN_SENT"):
            SYSTEM_TCP_CONNECTIONS.labels(state=state).set(states.get(state, 0))
    except Exception:
        pass

    # ── Database ──
    try:
        from app.core.database import get_pool
        pool = await get_pool()

        DB_POOL_SIZE.set(pool.get_size())
        DB_POOL_MAX.set(pool.get_max_size())
        DB_POOL_FREE.set(pool.get_idle_size())
        DB_POOL_USED.set(pool.get_size() - pool.get_idle_size())

        # Latency
        t0 = time.monotonic()
        await pool.fetchval("SELECT 1")
        DB_LATENCY.set(time.monotonic() - t0)

        # PG stats
        stats = await pool.fetchrow("""
            SELECT xact_commit, xact_rollback, deadlocks,
                   blks_hit, blks_read,
                   CASE WHEN blks_hit + blks_read > 0
                        THEN blks_hit::float / (blks_hit + blks_read) * 100
                        ELSE 100 END AS cache_hit
            FROM pg_stat_database WHERE datname = current_database()
        """)
        if stats:
            DB_CACHE_HIT_RATIO.set(stats["cache_hit"])
            DB_TX_COMMITTED.set(stats["xact_commit"])
            DB_TX_ROLLED_BACK.set(stats["xact_rollback"])

        # Active queries + locks
        active = await pool.fetchval(
            "SELECT COUNT(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND state != 'idle' "
            "AND pid != pg_backend_pid()"
        )
        DB_ACTIVE_QUERIES.set(active or 0)
        locks = await pool.fetchval("SELECT COUNT(*) FROM pg_locks WHERE NOT granted")
        DB_LOCK_WAITS.set(locks or 0)

        # Table stats (top 10 by size)
        tables = await pool.fetch("""
            SELECT s.relname, s.n_live_tup, s.n_dead_tup,
                   pg_total_relation_size(c.oid) AS size
            FROM pg_stat_user_tables s
            JOIN pg_class c ON s.relname = c.relname
                AND c.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            WHERE s.schemaname = 'public'
            ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 10
        """)
        for t in tables:
            name = t["relname"]
            DB_TABLE_ROWS.labels(table=name).set(t["n_live_tup"])
            DB_TABLE_SIZE.labels(table=name).set(t["size"])
            DB_DEAD_TUPLES.labels(table=name).set(t["n_dead_tup"])

        # Duplicates
        dupes = await pool.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT asset, instrument_type, timestamp
                FROM market_prices
                GROUP BY asset, instrument_type, timestamp
                HAVING COUNT(*) > 1
            ) sub
        """)
        DB_DUPLICATES.set(dupes or 0)

        # Data freshness — track time since latest write per critical
        # table so Grafana can chart "table last updated N minutes ago"
        # and alert when a scraper silently stops writing.
        for table, col in [
            ("market_prices", "timestamp"),
            ("market_prices_intraday", "ingested_at"),
            ("market_scores", "computed_at"),
            ("discover_stock_snapshots", "ingested_at"),
            ("discover_stock_intraday", "ts"),
            ("discover_stock_price_history", "ingested_at"),
            ("discover_mutual_fund_snapshots", "ingested_at"),
            ("discover_mf_nav_history", "ingested_at"),
            ("news_articles", "published_at"),
            ("economic_events", "event_time"),
            ("ipo_snapshots", "ingested_at"),
            ("broker_charges", "scraped_at"),
        ]:
            try:
                # Cast to timestamptz so date-typed columns (trade_date,
                # nav_date) don't raise on datetime arithmetic below.
                latest = await pool.fetchval(
                    f'SELECT MAX("{col}")::timestamptz FROM {table}'
                )
                if latest:
                    age = (datetime.now(timezone.utc) - latest).total_seconds()
                    DATA_FRESHNESS.labels(source=table).set(max(0, age))
                row_count = await pool.fetchval(f"SELECT COUNT(*) FROM {table}")
                DATA_ROWS_TOTAL.labels(table=table).set(row_count or 0)
            except Exception:
                # Table may not exist on a fresh DB — skip silently
                # rather than aborting the entire collection pass.
                pass

        stale = await pool.fetchval(
            "SELECT COUNT(*) FROM discover_stock_snapshots "
            "WHERE source_timestamp < CURRENT_DATE"
        )
        DATA_STALE_STOCKS.set(stale or 0)

    except Exception:
        pass

    # ── Redis ──
    try:
        import redis as _redis
        from app.core.config import get_settings
        settings = get_settings()
        r = _redis.from_url(settings.redis_url, decode_responses=True)
        t0 = time.monotonic()
        r.ping()
        REDIS_LATENCY.set(time.monotonic() - t0)
        REDIS_CONNECTED.set(1)
        info = r.info(section="memory")
        REDIS_MEMORY_USED.set(info.get("used_memory", 0))
        REDIS_KEYS.set(r.dbsize())

        # ARQ queue depth — ARQ stores queued jobs in a Redis sorted set
        # keyed by `arq:queue` (the default queue name). Job IDs that
        # are currently being processed live in `arq:in-progress:arq:queue`.
        # Subtracting in-progress from the total gives "waiting" count.
        try:
            from arq.constants import default_queue_name, in_progress_key_prefix
            queued = r.zcard(default_queue_name) or 0
            in_progress = r.scard(in_progress_key_prefix + default_queue_name) or 0
            ARQ_QUEUE_DEPTH.set(max(0, queued - in_progress))
        except Exception:
            pass
    except Exception:
        REDIS_CONNECTED.set(0)

    # ── Jobs ──
    try:
        from app.api.routes.ops import _running_direct_jobs
        JOB_RUNNING.set(len(_running_direct_jobs))
    except Exception:
        pass

    # ── DLQ size (Postgres job_dead_letters table) ──
    try:
        from app.core.database import get_pool
        pool = await get_pool()
        dlq_count = await pool.fetchval(
            "SELECT COUNT(*) FROM job_dead_letters WHERE status = 'dead'"
        )
        ARQ_DLQ_SIZE.set(dlq_count or 0)
    except Exception:
        pass

    # ── Watchlist active devices ──
    try:
        from app.core.database import get_pool
        pool = await get_pool()
        device_count = await pool.fetchval(
            "SELECT COUNT(DISTINCT device_id) FROM device_watchlists"
        )
        WATCHLIST_DEVICES.set(device_count or 0)
    except Exception:
        pass

    # ── Logs ──
    try:
        from app.core.log_stream import get_log_entries
        entries, _ = get_log_entries(limit=5000, min_level="WARNING")
        hour_ago = (datetime.now(timezone.utc) - __import__("datetime").timedelta(hours=1)).isoformat()
        recent = [e for e in entries if e.get("timestamp", "") >= hour_ago]
        LOG_ERRORS_1H.set(sum(1 for e in recent if e.get("level") == "ERROR"))
        LOG_WARNINGS_1H.set(sum(1 for e in recent if e.get("level") == "WARNING"))
    except Exception:
        pass
