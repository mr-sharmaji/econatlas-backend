import asyncio
import logging
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Body, Header, HTTPException, Path, Query

from app.core.config import get_settings
from app.core.log_files import (
    is_enabled as log_files_enabled,
    tail_log_files,
)
from app.core.log_stream import get_log_entries
from app.schemas.market_intel_schema import DataHealthResponse
from app.schemas.ops_schema import LogEntryResponse, LogListResponse
from app.services import market_intel_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ops", tags=["ops"])
OPS_RUNTIME_MARKER = "ops-runtime-fingerprint"

# ── Direct-run tracking for long-running jobs ────────────────────────
_running_direct_jobs: dict[str, asyncio.Task] = {}

# Valid job names that can be triggered manually.
_VALID_JOBS = {
    "market", "commodity", "crypto", "brief", "macro", "news",
    "discover_stock", "discover_mutual_funds",
    "discover_stock_price", "discover_stock_price_backfill",
    "discover_mf_nav", "discover_mf_nav_backfill",
    "recompute_mf_returns",
    "discover_stock_intraday",
    "discover_stock_intraday_backfill",
    "discover_stock_intraday_autofill",
    "rescore_stock", "rescore_mf",
    "reconcile_stock_snapshots",
    "ipo", "tax", "market_score", "fertilizer",
    "notification_check", "ipo_notification",
    "gap_backfill",
    "market_intraday_backfill",
    "news_embed",
    # Artha semantic layer backfills
    "stock_narrative_embed",
    "stock_future_prospects",
    "stock_future_prospects_recent",
    "stock_future_prospects_embed",
    "economic_events_embed",
    "educational_concepts_seed",
    # Weekly broker pricing scrape — task is registered in
    # app/queue/settings.get_arq_functions() but the API gate also
    # needs to allow-list the name here, otherwise both
    # /ops/jobs/trigger/broker_charges and the /ops/jobs listing
    # treat it as unknown.
    "broker_charges",
}

# Tables exposed for CRUD operations.
_TABLES: dict[str, str] = {
    "macro": "macro_indicators",
    "market": "market_prices",
    "news": "news_articles",
    "brief": "stock_snapshots",
    "discover_stock": "discover_stock_snapshots",
    "discover_mf": "discover_mutual_fund_snapshots",
    "discover_stock_price": "discover_stock_price_history",
    "discover_mf_nav": "discover_mf_nav_history",
    "events": "economic_events",
}

_SAFE_COLUMN_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')



def _authorize(x_ops_token: str | None) -> None:
    settings = get_settings()
    if not settings.ops_logs_enabled:
        raise HTTPException(status_code=404, detail="Ops logs endpoint disabled")
    required_token = settings.ops_logs_token
    if required_token and x_ops_token != required_token:
        raise HTTPException(status_code=403, detail="Invalid ops token")


def _resolve_table(table_key: str) -> str:
    table = _TABLES.get(table_key)
    if table is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown table '{table_key}'. Valid: {sorted(_TABLES)}",
        )
    return table


def _validate_column(column: str) -> str:
    if not _SAFE_COLUMN_RE.match(column):
        raise HTTPException(status_code=400, detail=f"Invalid column name: {column}")
    return column


async def _get_pool():
    from app.core.database import get_pool
    return await get_pool()


# ═════════════════════════════════════════════════════════════════════
# Windows host metrics via windows_exporter
# ═════════════════════════════════════════════════════════════════════
#
# When this stack is hosted on Windows 11 + Docker Desktop (which uses
# WSL2 underneath), psutil inside the container reports the WSL2 Linux
# VM, not the actual Windows 11 host. To see real Windows host stats we
# scrape `windows_exporter` running as a Windows service on the host.
#
# `host.docker.internal` is a Docker Desktop magic DNS name that routes
# from any container to the Windows host. Default windows_exporter port
# is 9182.
#
# This helper takes TWO snapshots (default 1 s apart) and computes CPU
# percent from the delta between them — same approach as
# `psutil.cpu_percent(interval=1)`. Memory / disk / net are read from
# the second snapshot only since they're already gauges or absolute
# counters at a point in time.
#
# Returns None if windows_exporter is unreachable (not installed yet,
# wrong port, network issue) — callers fall back to in-container psutil.

_WINDOWS_EXPORTER_URL = "http://host.docker.internal:9182/metrics"
_WINDOWS_EXPORTER_TIMEOUT = 2.0


def _parse_prom_text(text: str) -> dict[str, list[tuple[dict, float]]]:
    """Parse Prometheus text exposition into {metric: [(labels, value), ...]}."""
    result: dict[str, list[tuple[dict, float]]] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        # metric_name{label="v",...} value [timestamp]
        try:
            if "{" in line:
                name, rest = line.split("{", 1)
                labels_str, _, value_str = rest.partition("}")
                value_str = value_str.strip().split(" ", 1)[0]
                labels = {}
                for pair in re.findall(r'(\w+)="([^"]*)"', labels_str):
                    labels[pair[0]] = pair[1]
            else:
                parts = line.split(" ", 1)
                if len(parts) != 2:
                    continue
                name, value_str = parts[0], parts[1].strip().split(" ", 1)[0]
                labels = {}
            value = float(value_str)
        except (ValueError, IndexError):
            continue
        result.setdefault(name, []).append((labels, value))
    return result


async def _fetch_windows_exporter() -> dict | None:
    """Fetch Windows host metrics via windows_exporter.

    Takes two scrapes 1 s apart so we can derive CPU% from the
    `windows_cpu_time_total` counter delta. Returns None when
    windows_exporter is unreachable (the caller should fall back to
    in-container psutil).
    """
    try:
        import httpx
    except ImportError:
        return None

    async def _scrape() -> dict | None:
        try:
            async with httpx.AsyncClient(timeout=_WINDOWS_EXPORTER_TIMEOUT) as client:
                r = await client.get(_WINDOWS_EXPORTER_URL)
                r.raise_for_status()
                return _parse_prom_text(r.text)
        except Exception:
            return None

    snap1 = await _scrape()
    if snap1 is None:
        return None
    await asyncio.sleep(1.0)
    snap2 = await _scrape()
    if snap2 is None:
        return None

    def _sum(metric: str, label_filter=None) -> float:
        rows = snap2.get(metric, [])
        if label_filter is None:
            return sum(v for _, v in rows)
        return sum(v for lbls, v in rows if all(lbls.get(k) == val for k, val in label_filter.items()))

    def _delta_sum(metric: str, label_filter=None) -> float:
        a = snap1.get(metric, [])
        b = snap2.get(metric, [])

        def _bucketize(rows):
            d: dict[tuple, float] = {}
            for lbls, v in rows:
                if label_filter and not all(lbls.get(k) == val for k, val in label_filter.items()):
                    continue
                key = tuple(sorted(lbls.items()))
                d[key] = v
            return d

        ba, bb = _bucketize(a), _bucketize(b)
        return sum(bb[k] - ba.get(k, 0.0) for k in bb)

    # ── CPU% across all cores from the delta ──
    # windows_exporter exposes one row per (core, mode). idle ↑ during
    # the 1 s window means CPU was idle; total time ↑ regardless. The
    # busy fraction is (1 - idle_delta / total_delta).
    total_delta = _delta_sum("windows_cpu_time_total")
    idle_delta = _delta_sum("windows_cpu_time_total", {"mode": "idle"})
    if total_delta > 0:
        cpu_busy = max(0.0, min(100.0, (1.0 - idle_delta / total_delta) * 100.0))
    else:
        cpu_busy = 0.0

    # CPU core count: number of distinct `core` labels in the metric
    cores = len({lbls.get("core") for lbls, _ in snap2.get("windows_cpu_time_total", []) if lbls.get("core")})

    # ── Memory ──
    # windows_exporter ≥0.18 renamed the cs_/os_ collectors to memory_*.
    # Use the new names with a fallback to the legacy ones so this
    # works against any version on the host.
    mem_total = _sum("windows_memory_physical_total_bytes") \
        or _sum("windows_cs_physical_memory_bytes")
    # `windows_memory_available_bytes` is what Task Manager calls
    # "Available" — closer to what users expect than "physical_free"
    # which excludes the cache. windows_memory_physical_free_bytes is
    # also exposed and gives a higher "used" number; pick available
    # since it matches Task Manager's display.
    mem_free = _sum("windows_memory_available_bytes") \
        or _sum("windows_memory_physical_free_bytes") \
        or _sum("windows_os_physical_memory_free_bytes")
    mem_used = max(0.0, mem_total - mem_free)
    mem_pct = (mem_used / mem_total * 100.0) if mem_total > 0 else 0.0

    # ── Disk: aggregate across all logical drives, exclude removable ──
    # windows_exporter labels each drive with `volume` (C:, D:, …).
    disk_size_rows = snap2.get("windows_logical_disk_size_bytes", [])
    disk_free_rows = snap2.get("windows_logical_disk_free_bytes", [])
    disks = []
    free_by_vol = {lbls.get("volume"): v for lbls, v in disk_free_rows}
    for lbls, total in disk_size_rows:
        vol = lbls.get("volume", "?")
        if total <= 0:
            continue
        free = free_by_vol.get(vol, 0.0)
        used = max(0.0, total - free)
        disks.append({
            "volume": vol,
            "total_gb": round(total / 1024 / 1024 / 1024, 1),
            "used_gb": round(used / 1024 / 1024 / 1024, 1),
            "free_gb": round(free / 1024 / 1024 / 1024, 1),
            "percent": round(used / total * 100.0, 1),
        })
    disks.sort(key=lambda d: d["volume"])

    # ── Network ──
    net_sent = _sum("windows_net_bytes_sent_total")
    net_recv = _sum("windows_net_bytes_received_total")

    return {
        "source": "windows_exporter",
        "cpu_percent": round(cpu_busy, 1),
        "cpu_count": cores or None,
        "memory_used_mb": round(mem_used / 1024 / 1024),
        "memory_total_mb": round(mem_total / 1024 / 1024),
        "memory_free_mb": round(mem_free / 1024 / 1024),
        "memory_percent": round(mem_pct, 1),
        "disks": disks,
        "network": {
            "bytes_sent_mb": round(net_sent / 1024 / 1024, 1),
            "bytes_recv_mb": round(net_recv / 1024 / 1024, 1),
        },
    }


# ═════════════════════════════════════════════════════════════════════
# Server Health
# ═════════════════════════════════════════════════════════════════════

import time as _time
_BOOT_TIME = _time.time()


@router.get("/health")
async def ops_server_health(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Comprehensive server health check with system, DB, Redis, jobs, and data metrics."""
    _authorize(x_ops_token)
    import os
    import psutil
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    result: dict = {
        "status": "healthy",
        "timestamp": now.isoformat(),
        "uptime_seconds": round(_time.time() - _BOOT_TIME),
    }
    try:
        from app.core.runtime_info import get_runtime_info

        result["runtime"] = get_runtime_info()
        result["runtime"]["ops_runtime_marker"] = OPS_RUNTIME_MARKER
    except Exception as exc:
        result["runtime"] = {
            "error": str(exc),
            "ops_runtime_marker": OPS_RUNTIME_MARKER,
        }

    # ── System metrics ──
    try:
        proc = psutil.Process()
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk = psutil.disk_usage("/")
        load = os.getloadavg()
        net = psutil.net_io_counters()
        net_conns = psutil.net_connections(kind="tcp")
        from collections import Counter as _Counter
        conn_states = _Counter(c.status for c in net_conns)

        result["system"] = {
            # interval=1 gives a 1-second sample for accurate reading.
            # interval=0 returns 0% because there's no prior baseline
            # in a fresh per-request call.
            "cpu_percent": psutil.cpu_percent(interval=1),
            "cpu_count": psutil.cpu_count(),
            "memory_used_mb": round(mem.used / 1024 / 1024),
            "memory_total_mb": round(mem.total / 1024 / 1024),
            "memory_percent": mem.percent,
            "memory_available_mb": round(mem.available / 1024 / 1024),
            "swap_used_mb": round(swap.used / 1024 / 1024),
            "swap_total_mb": round(swap.total / 1024 / 1024),
            "disk_used_gb": round(disk.used / 1024 / 1024 / 1024, 1),
            "disk_total_gb": round(disk.total / 1024 / 1024 / 1024, 1),
            "disk_percent": disk.percent,
            "load_avg_1m": round(load[0], 2),
            "load_avg_5m": round(load[1], 2),
            "load_avg_15m": round(load[2], 2),
            "process_memory_mb": round(proc.memory_info().rss / 1024 / 1024),
            "process_threads": proc.num_threads(),
            "open_files": len(proc.open_files()),
            "network": {
                "bytes_sent_mb": round(net.bytes_sent / 1024 / 1024, 1),
                "bytes_recv_mb": round(net.bytes_recv / 1024 / 1024, 1),
                "packets_sent": net.packets_sent,
                "packets_recv": net.packets_recv,
                "tcp_connections": len(net_conns),
                "connection_states": dict(conn_states),
            },
        }
    except Exception as exc:
        result["system"] = {"error": str(exc)}

    # ── Windows host metrics (real Windows 11 stats, not WSL2 VM) ──
    #
    # When deployed via Docker Desktop on Windows, the `system` section
    # above reports the WSL2 Linux VM's view, not the actual host. If
    # `windows_exporter` is installed and reachable on
    # host.docker.internal:9182, surface its data here so callers can
    # see the real Windows 11 CPU / memory / disk / network usage.
    #
    # Returns None silently when windows_exporter isn't installed —
    # the `system` section above remains the source of truth in that
    # case (and on non-Windows deployments).
    try:
        host = await _fetch_windows_exporter()
        if host is not None:
            result["host"] = host
    except Exception as exc:
        logger.debug("windows_exporter scrape failed: %s", exc)

    # ── Database metrics ──
    try:
        pool = await _get_pool()
        # Pool stats
        result["database"] = {
            "pool_size": pool.get_size(),
            "pool_free": pool.get_idle_size(),
            "pool_used": pool.get_size() - pool.get_idle_size(),
            "pool_min": pool.get_min_size(),
            "pool_max": pool.get_max_size(),
        }
        # Latency check
        t0 = _time.monotonic()
        await pool.fetchval("SELECT 1")
        latency = (_time.monotonic() - t0) * 1000
        result["database"]["latency_ms"] = round(latency, 2)

        # Table sizes
        rows = await pool.fetch("""
            SELECT s.relname AS table_name,
                   s.n_live_tup AS row_count,
                   pg_total_relation_size(c.oid) AS total_bytes
            FROM pg_stat_user_tables s
            JOIN pg_class c ON s.relname = c.relname AND c.relnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = 'public'
            )
            WHERE s.schemaname = 'public'
            ORDER BY pg_total_relation_size(c.oid) DESC
            LIMIT 15
        """)
        result["database"]["tables"] = [
            {
                "name": r["table_name"],
                "rows": r["row_count"],
                "size_mb": round(r["total_bytes"] / 1024 / 1024, 1),
            }
            for r in rows
        ]

        # Index health — check for invalid indexes
        idx_rows = await pool.fetch("""
            SELECT c.relname AS index_name, i.indisvalid AS valid
            FROM pg_class c
            JOIN pg_index i ON c.oid = i.indexrelid
            JOIN pg_class t ON t.oid = i.indrelid
            WHERE t.relname IN ('market_prices', 'market_scores',
                                'market_prices_intraday', 'discover_stock_snapshots')
              AND c.relkind = 'i'
        """)
        invalid_indexes = [r["index_name"] for r in idx_rows if not r["valid"]]
        result["database"]["invalid_indexes"] = invalid_indexes

        # Duplicate check on critical tables
        dup_check = await pool.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT asset, instrument_type, timestamp
                FROM market_prices
                GROUP BY asset, instrument_type, timestamp
                HAVING COUNT(*) > 1
            ) sub
        """)
        result["database"]["market_prices_duplicates"] = dup_check

        # ── PostgreSQL internals ──
        # Active queries
        active = await pool.fetch("""
            SELECT pid, state, query, NOW() - query_start AS duration,
                   wait_event_type, wait_event
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid != pg_backend_pid()
              AND state != 'idle'
            ORDER BY query_start ASC
        """)
        result["database"]["active_queries"] = [
            {
                "pid": r["pid"],
                "state": r["state"],
                "duration_s": round(r["duration"].total_seconds(), 1) if r["duration"] else 0,
                "wait": f"{r['wait_event_type']}:{r['wait_event']}" if r["wait_event_type"] else None,
                "query": (r["query"] or "")[:120],
            }
            for r in active
        ]

        # Lock waits
        locks = await pool.fetchval("""
            SELECT COUNT(*) FROM pg_locks WHERE NOT granted
        """)
        result["database"]["lock_waits"] = locks

        # Transaction rate + cache hit ratio
        stats = await pool.fetchrow("""
            SELECT xact_commit + xact_rollback AS tx_total,
                   xact_commit AS tx_committed,
                   xact_rollback AS tx_rolled_back,
                   blks_hit, blks_read,
                   CASE WHEN blks_hit + blks_read > 0
                        THEN ROUND(blks_hit::numeric / (blks_hit + blks_read) * 100, 2)
                        ELSE 100 END AS cache_hit_ratio,
                   tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
                   deadlocks, conflicts
            FROM pg_stat_database
            WHERE datname = current_database()
        """)
        if stats:
            result["database"]["stats"] = {
                "tx_committed": stats["tx_committed"],
                "tx_rolled_back": stats["tx_rolled_back"],
                "cache_hit_ratio": float(stats["cache_hit_ratio"]),
                "rows_returned": stats["tup_returned"],
                "rows_inserted": stats["tup_inserted"],
                "rows_updated": stats["tup_updated"],
                "rows_deleted": stats["tup_deleted"],
                "deadlocks": stats["deadlocks"],
                "conflicts": stats["conflicts"],
            }

        # Dead tuples + autovacuum
        vacuum_rows = await pool.fetch("""
            SELECT relname, n_dead_tup, last_vacuum, last_autovacuum,
                   n_live_tup
            FROM pg_stat_user_tables
            WHERE n_dead_tup > 1000
            ORDER BY n_dead_tup DESC
            LIMIT 10
        """)
        result["database"]["vacuum_needed"] = [
            {
                "table": r["relname"],
                "dead_tuples": r["n_dead_tup"],
                "live_tuples": r["n_live_tup"],
                "last_vacuum": r["last_vacuum"].isoformat() if r["last_vacuum"] else None,
                "last_autovacuum": r["last_autovacuum"].isoformat() if r["last_autovacuum"] else None,
            }
            for r in vacuum_rows
        ]

    except Exception as exc:
        result["database"] = {"error": str(exc)}

    # ── Redis metrics ──
    try:
        import redis as _redis
        settings = get_settings()
        r = _redis.from_url(settings.redis_url, decode_responses=True)
        t0 = _time.monotonic()
        r.ping()
        latency = (_time.monotonic() - t0) * 1000
        info = r.info(section="memory")
        result["redis"] = {
            "connected": True,
            "latency_ms": round(latency, 2),
            "memory_used_mb": round(info.get("used_memory", 0) / 1024 / 1024, 1),
            "memory_peak_mb": round(info.get("used_memory_peak", 0) / 1024 / 1024, 1),
            "keys": r.dbsize(),
        }
    except Exception as exc:
        result["redis"] = {"connected": False, "error": str(exc)}

    # ── Job status ──
    try:
        from app.core.log_stream import get_log_entries
        from datetime import timedelta
        # Running jobs
        running_jobs = list(_running_direct_jobs.keys())
        result["jobs"] = {
            "running": running_jobs,
        }

        # Per-job last run details from ARQ
        try:
            import redis as _redis
            settings = get_settings()
            r = _redis.from_url(settings.redis_url, decode_responses=True)
            arq_jobs = {}
            # Scan ARQ result keys
            for key in r.scan_iter("arq:result:*", count=100):
                try:
                    import json as _json
                    raw = r.get(key)
                    if raw:
                        data = _json.loads(raw)
                        job_name = data.get("function", key.split(":")[-1])
                        arq_jobs[job_name] = {
                            "success": data.get("success"),
                            "start_time": data.get("start_time"),
                            "finish_time": data.get("finish_time"),
                            "duration_ms": round((data.get("finish_time", 0) or 0) - (data.get("start_time", 0) or 0), 1) if data.get("finish_time") and data.get("start_time") else None,
                        }
                except Exception:
                    pass
            result["jobs"]["arq_results"] = arq_jobs
        except Exception:
            pass

        # Errors/warnings in the last hour
        all_entries, _ = get_log_entries(limit=5000, min_level="WARNING")
        hour_ago = (now - timedelta(hours=1)).isoformat()
        recent = [e for e in all_entries if e.get("timestamp", "") >= hour_ago]
        errors_1h = sum(1 for e in recent if e.get("level") == "ERROR")
        warnings_1h = sum(1 for e in recent if e.get("level") == "WARNING")
        result["jobs"]["errors_1h"] = errors_1h
        result["jobs"]["warnings_1h"] = warnings_1h

        # Top error messages
        from collections import Counter as _ErrCounter
        err_msgs = _ErrCounter()
        for e in recent:
            if e.get("level") in ("ERROR", "WARNING"):
                err_msgs[e.get("message", "")[:80]] += 1
        result["jobs"]["top_issues"] = [
            {"message": msg, "count": cnt}
            for msg, cnt in err_msgs.most_common(5)
        ]
    except Exception as exc:
        result["jobs"] = {"error": str(exc)}

    # ── Data freshness ──
    try:
        pool = await _get_pool()
        freshness = {}
        for table, ts_col in [
            ("market_prices", "timestamp"),
            ("discover_stock_snapshots", "source_timestamp"),
            ("discover_mutual_fund_snapshots", "source_timestamp"),
        ]:
            row = await pool.fetchrow(
                f'SELECT MAX("{ts_col}") AS latest, COUNT(*) AS total FROM {table}'
            )
            freshness[table] = {
                "latest": row["latest"].isoformat() if row["latest"] else None,
                "total_rows": row["total"],
            }
        # Stock stale count
        stale = await pool.fetchval(
            "SELECT COUNT(*) FROM discover_stock_snapshots "
            "WHERE source_timestamp < CURRENT_DATE"
        )
        freshness["stocks_stale"] = stale
        result["data_freshness"] = freshness
    except Exception as exc:
        result["data_freshness"] = {"error": str(exc)}

    # ── API request stats (from prometheus if available) ──
    try:
        from app.core.metrics import get_request_stats
        result["api"] = get_request_stats()
    except Exception:
        result["api"] = {"note": "prometheus metrics not initialized"}

    # Mark unhealthy if critical issues found
    if result.get("database", {}).get("invalid_indexes"):
        result["status"] = "degraded"
    if result.get("database", {}).get("market_prices_duplicates", 0) > 0:
        result["status"] = "degraded"
    if not result.get("redis", {}).get("connected", False):
        result["status"] = "degraded"

    return result


# ═════════════════════════════════════════════════════════════════════
# Notification AI metrics
# ═════════════════════════════════════════════════════════════════════

@router.get("/notification_ai_metrics")
async def ops_notification_ai_metrics(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Return per-type AI invocation counters for push notifications.

    Each type shows total calls, success count, fallback count (when all
    LLM models returned None and the rule-based body shipped instead),
    fallback_rate (0.0–1.0), and avg_latency_ms. Counters are in-memory
    and reset on process restart.
    """
    _authorize(x_ops_token)
    from app.services.ai_service import get_notification_ai_metrics
    return {"metrics": get_notification_ai_metrics()}


# ═════════════════════════════════════════════════════════════════════
# Logs
# ═════════════════════════════════════════════════════════════════════

@router.get("/logs", response_model=LogListResponse)
async def ops_logs(
    limit: int = Query(default=200, ge=1, le=5000),
    after_id: int | None = Query(default=None, ge=0, description="Return logs strictly after this id"),
    min_level: str | None = Query(default=None, description="Minimum level: DEBUG/INFO/WARNING/ERROR/CRITICAL"),
    contains: str | None = Query(default=None, description="Substring filter on message"),
    logger_name: str | None = Query(default=None, description="Substring filter on logger name"),
    since: datetime | None = Query(
        default=None,
        description="Return logs with timestamp >= this ISO datetime (persistent store only)",
    ),
    until: datetime | None = Query(
        default=None,
        description="Return logs with timestamp <= this ISO datetime (persistent store only)",
    ),
    source: str | None = Query(
        default=None,
        description="Force 'memory' (in-memory ring buffer) or 'file' (7-day rotating file logs). "
                    "Defaults to file when available, else memory.",
    ),
    x_ops_token: str | None = Header(default=None),
) -> LogListResponse:
    """Tail application logs for debugging scheduler/feed issues.

    By default this reads from the 7-day rotating log files on disk
    (``logs/app.log`` + rotated siblings), which preserve history across
    restarts and grow far beyond the in-memory ring buffer. Pass
    ``source=memory`` to hit the live ring buffer instead — lower
    latency, bounded size, volatile, but also returns an ``after_id``
    cursor for incremental tailing.
    """
    _authorize(x_ops_token)

    requested = (source or "").lower()
    use_files = log_files_enabled() and requested != "memory"

    if use_files:
        try:
            # Run in executor so the file scan doesn't block the event
            # loop. With DEBUG-level logging the file grows fast and a
            # reverse scan can take seconds — long enough to stall
            # health checks, Prometheus scrapes, and user requests.
            import asyncio as _asyncio
            import functools as _ft
            _tail_fn = _ft.partial(
                tail_log_files,
                limit=limit,
                min_level=min_level,
                contains=contains,
                logger_name=logger_name,
                since=since,
                until=until,
            )
            loop = _asyncio.get_event_loop()
            file_rows = await loop.run_in_executor(None, _tail_fn)
            entries = [LogEntryResponse(**r) for r in file_rows]
            # File logs have no monotonic id — `latest_id` is not
            # meaningful here, so return 0. Clients that need
            # incremental tailing should use `source=memory`.
            return LogListResponse(entries=entries, count=len(entries), latest_id=0)
        except Exception as exc:
            logger.warning("ops_logs: file tail failed, falling back to memory: %s", exc)

    rows, latest_id = get_log_entries(
        limit=limit,
        after_id=after_id,
        min_level=min_level,
        contains=contains,
        logger_name=logger_name,
    )
    entries = [LogEntryResponse(**r) for r in rows]
    return LogListResponse(entries=entries, count=len(entries), latest_id=latest_id)


# ═════════════════════════════════════════════════════════════════════
# Jobs
# ═════════════════════════════════════════════════════════════════════

# Jobs that are run directly (bypass ARQ) because ARQ manual trigger
# fails for long-timeout functions.  Maps job_name → async import path.
_DIRECT_RUN_JOBS: dict[str, tuple[str, str]] = {
    "discover_stock": (
        "app.scheduler.discover_stock_job",
        "run_discover_stock_job",
    ),
    "discover_mutual_funds": (
        "app.scheduler.discover_mutual_fund_job",
        "run_discover_mutual_fund_job",
    ),
    "discover_stock_price": (
        "app.scheduler.discover_stock_price_job",
        "run_discover_stock_price_job",
    ),
    "discover_stock_price_backfill": (
        "app.scheduler.discover_stock_price_job",
        "run_discover_stock_price_backfill_job",
    ),
    "discover_mf_nav": (
        "app.scheduler.discover_mf_nav_job",
        "run_discover_mf_nav_job",
    ),
    "discover_mf_nav_backfill": (
        "app.scheduler.discover_mf_nav_job",
        "run_discover_mf_nav_backfill_job",
    ),
    "recompute_mf_returns": (
        "app.services.discover_service",
        "recompute_mf_returns_all",
    ),
    "reconcile_stock_snapshots": (
        "app.services.discover_service",
        "reconcile_stock_snapshots",
    ),
    "broker_charges_scrape": (
        "app.scheduler.broker_charges_job",
        "run_broker_charges_job",
    ),
    "discover_stock_intraday_backfill": (
        "app.scheduler.discover_stock_intraday_job",
        "run_discover_stock_intraday_backfill",
    ),
    # End-of-session catch-up for Yahoo-backed indices + FX. The live
    # intraday loop drops post-session ticks, so the NSE/BSE closing
    # auction print never lands naturally. Call this on demand to
    # backfill the full 1m bar series for today from Yahoo's chart API.
    "market_intraday_backfill": (
        "app.scheduler.market_job",
        "run_market_intraday_backfill_job",
    ),
    "discover_stock_intraday_autofill": (
        "app.scheduler.discover_stock_intraday_job",
        "run_discover_stock_intraday_autofill_job",
    ),
    # ── Artha semantic layer backfills ──
    # These are run directly because they warm the fastembed model and
    # can take minutes for large backfills (e.g. 176K economic_events).
    "stock_narrative_embed": (
        "app.scheduler.stock_narrative_embed_job",
        "run_stock_narrative_embed_job",
    ),
    "economic_events_embed": (
        "app.scheduler.economic_events_embed_job",
        "run_economic_events_embed_job",
    ),
    "educational_concepts_seed": (
        "app.scheduler.educational_concepts_seed_job",
        "run_educational_concepts_seed_job",
    ),
    # Full future-prospects refresh can legitimately run for a while and is
    # easier to debug when the ops trigger runs it directly instead of
    # enqueueing through ARQ.
    "stock_future_prospects": (
        "app.scheduler.stock_future_prospects_job",
        "run_stock_future_prospects_job",
    ),
}


async def _direct_run_wrapper(job_name: str, module_path: str, func_name: str) -> None:
    """Run a job function directly as an asyncio task."""
    import importlib

    t0 = datetime.now(timezone.utc)
    logger.info("Direct-run STARTED: %s", job_name)
    try:
        mod = importlib.import_module(module_path)
        fn = getattr(mod, func_name)
        await fn()
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.info("Direct-run COMPLETED: %s in %.1fs", job_name, elapsed)
    except Exception:
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
        logger.exception("Direct-run FAILED: %s after %.1fs", job_name, elapsed)
    finally:
        _running_direct_jobs.pop(job_name, None)


@router.post("/jobs/trigger/{job_name}")
async def trigger_job(
    job_name: str = Path(..., description="Name of the job to trigger"),
    force: bool = Query(default=False, description="Force enqueue even if a previous run exists"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Manually trigger a background job for immediate execution."""
    _authorize(x_ops_token)
    if job_name not in _VALID_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job '{job_name}'. Valid: {sorted(_VALID_JOBS)}",
        )

    # ── Direct-run path for discover_* jobs (ARQ manual trigger is broken
    #    for long-timeout functions) ──
    if job_name in _DIRECT_RUN_JOBS:
        existing = _running_direct_jobs.get(job_name)
        if existing is not None and not existing.done():
            if not force:
                return {
                    "status": "already_running",
                    "job_name": job_name,
                    "message": "Job is already running. Use force=true to start another.",
                }

        # Pre-flight: clear any stale ARQ in-progress lock + abort flag
        # for this job family. Without this, a prior deploy may have
        # left a stale lock that blocks APScheduler's next cron tick
        # from spawning a backup ARQ run, AND a prior /ops/jobs/abort
        # call may have left a `job:abort:{name}` key that the new run
        # would pick up and kill itself at the first check.
        try:
            from arq.constants import (
                default_queue_name,
                in_progress_key_prefix,
                job_key_prefix,
            )
            from app.queue.redis_pool import get_redis_pool
            from app.queue.settings import expand_job_family_ids

            redis_pool = await get_redis_pool()
            family_ids = expand_job_family_ids(job_name)
            cleared_preflight = 0
            # Remove from the queue-level in-progress set
            raw_members = await redis_pool.smembers(
                in_progress_key_prefix + default_queue_name
            )
            for raw_id in raw_members or set():
                jid = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
                if jid in family_ids:
                    await redis_pool.srem(
                        in_progress_key_prefix + default_queue_name, raw_id
                    )
                    cleared_preflight += 1
            # Delete per-job in-progress keys
            for fid in family_ids:
                async for k in redis_pool.scan_iter(match=f"{in_progress_key_prefix}{fid}*"):
                    await redis_pool.delete(k)
                    cleared_preflight += 1
                async for k in redis_pool.scan_iter(match=f"{job_key_prefix}{fid}*"):
                    await redis_pool.delete(k)
                    cleared_preflight += 1
            # Delete abort flag so the new run isn't killed on the first
            # _check_abort() call inside the job body.
            await redis_pool.delete(f"job:abort:{job_name}")
            if cleared_preflight:
                logger.info(
                    "Direct-run pre-flight cleared %d stale ARQ keys for %s",
                    cleared_preflight, job_name,
                )
        except Exception:
            logger.warning(
                "Direct-run pre-flight stale-clear failed for %s",
                job_name, exc_info=True,
            )

        module_path, func_name = _DIRECT_RUN_JOBS[job_name]
        task = asyncio.create_task(
            _direct_run_wrapper(job_name, module_path, func_name),
            name=f"direct-run-{job_name}",
        )
        _running_direct_jobs[job_name] = task
        logger.info("Direct-run triggered: %s (bypassing ARQ)", job_name)
        return {
            "status": "started",
            "job_name": job_name,
            "mode": "direct",
            "message": "Job started directly (bypassing ARQ queue).",
        }

    # ── Standard ARQ enqueue for short-lived jobs ──
    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()

    job_id = f"{job_name}_manual"
    if force:
        job_id = f"{job_name}_manual_{int(datetime.now(timezone.utc).timestamp())}"

    job = await pool.enqueue_job(job_name, _job_id=job_id)
    logger.info("Manually triggered job: %s (job_id=%s)", job_name, job.job_id if job else "deduped")
    return {
        "status": "enqueued" if job else "already_queued",
        "job_name": job_name,
        "job_id": job.job_id if job else job_id,
    }


@router.post("/jobs/clear-stale")
async def clear_stale_jobs(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Clear ALL stale job state from Redis (manual trigger).

    Delegates to ``app.queue.stale_cleanup.clear_stale_arq_state`` which
    is the same helper run automatically on every app startup. Use this
    endpoint mid-session if you suspect a wedge without waiting for the
    next deploy — rare, since startup self-heals, but still handy.
    """
    _authorize(x_ops_token)
    from app.queue.stale_cleanup import clear_stale_arq_state
    return await clear_stale_arq_state()


@router.post("/jobs/abort/{job_name}")
async def abort_job(
    job_name: str = Path(..., description="Name of the job to abort"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Abort a running job by setting a Redis abort flag + clearing ARQ locks.

    The running job checks this flag periodically and exits early.
    ARQ locks are also cleared so a fresh job can start immediately.
    """
    _authorize(x_ops_token)
    if job_name not in _VALID_JOBS:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown job '{job_name}'. Valid: {sorted(_VALID_JOBS)}",
        )
    from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix

    from app.queue.settings import expand_job_family_ids
    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    family_ids = expand_job_family_ids(job_name)

    # 1. Set abort flag (TTL 10 minutes — long enough for job to see it)
    abort_key = f"job:abort:{job_name}"
    await pool.set(abort_key, "1", ex=600)

    # 2. Clear ARQ in-progress locks for this job
    cleared = []
    in_progress_key = in_progress_key_prefix + default_queue_name
    raw_members = await pool.smembers(in_progress_key)
    for raw_id in raw_members or set():
        job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
        if job_id in family_ids:
            await pool.srem(in_progress_key, raw_id)
            cleared.append(job_id)

    # 3. Clear per-job in-progress keys
    for family_id in family_ids:
        async for key in pool.scan_iter(match=f"{in_progress_key_prefix}{family_id}*"):
            key_str = key.decode() if isinstance(key, bytes) else str(key)
            await pool.delete(key)
            cleared.append(key_str)

    # 4. Clear job hash keys
    for family_id in family_ids:
        async for key in pool.scan_iter(match=f"{job_key_prefix}{family_id}*"):
            key_str = key.decode() if isinstance(key, bytes) else str(key)
            await pool.delete(key)
            cleared.append(key_str)

    logger.warning("Abort requested for job %s: flag set, cleared %d ARQ keys", job_name, len(cleared))
    return {
        "status": "abort_requested",
        "job_name": job_name,
        "abort_key": abort_key,
        "cleared_keys": cleared,
    }


@router.delete("/jobs/abort/{job_name}")
async def clear_abort(
    job_name: str,
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Clear an abort flag so the job can run again."""
    _authorize(x_ops_token)
    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    abort_key = f"job:abort:{job_name}"
    deleted = await pool.delete(abort_key)
    logger.info("Cleared abort flag for %s (deleted=%d)", job_name, deleted)
    return {"status": "cleared", "job_name": job_name, "deleted": bool(deleted)}


_RESCORE_JOBS: dict[str, str] = {
    "discover_stock": "rescore_stock",
    "discover_mf": "rescore_mf",
}


@router.post("/jobs/rescore/{job_name}")
async def rescore_job(
    job_name: str,
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Re-score existing data without fetching. Reads rows from DB, scores, writes back."""
    _authorize(x_ops_token)
    if job_name not in _RESCORE_JOBS:
        raise HTTPException(status_code=400, detail=f"Rescore not supported for '{job_name}'")

    task_key = f"rescore_{job_name}"
    existing = _running_direct_jobs.get(task_key)
    if existing is not None and not existing.done():
        return {
            "status": "already_running",
            "job_name": job_name,
            "message": "Rescore is already running.",
        }

    arq_task_name = _RESCORE_JOBS[job_name]

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    job_id = f"{arq_task_name}_{int(datetime.now(timezone.utc).timestamp())}"
    job = await pool.enqueue_job(arq_task_name, _job_id=job_id)
    logger.info("Rescore enqueued via ARQ: %s (job_id=%s)", arq_task_name, job.job_id if job else "deduped")
    return {
        "status": "enqueued" if job else "already_queued",
        "job_name": job_name,
        "mode": "rescore",
        "job_id": job.job_id if job else job_id,
        "message": "Rescore enqueued via ARQ worker — will execute reliably.",
    }


@router.post("/jobs/rerank-mf")
async def rerank_mf(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Recompute sub_category_rank and category_rank for all MFs."""
    _authorize(x_ops_token)
    from app.core.database import get_pool
    pool = await get_pool()
    try:
        # Auto-migrate: add fund_managers column if not exists
        try:
            await pool.execute("""
                ALTER TABLE discover_mutual_fund_snapshots
                ADD COLUMN IF NOT EXISTS fund_managers JSONB
            """)
        except Exception:
            pass  # Column already exists or migration not needed

        # Fix misclassified index fund sub_categories before ranking
        fix_results = []
        index_corrections = [
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Multi Cap Index' WHERE fund_classification = 'Index' AND (scheme_name ILIKE '%Nifty 500%' OR scheme_name ILIKE '%BSE 500%') AND sub_category != 'Multi Cap Index'", ),
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Large & MidCap Index' WHERE fund_classification = 'Index' AND (scheme_name ILIKE '%LargeMidcap%' OR scheme_name ILIKE '%Large Midcap%' OR scheme_name ILIKE '%Nifty 250%' OR scheme_name ILIKE '%Nifty 200 %') AND sub_category NOT IN ('Large & MidCap Index')", ),
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Mid Cap Index' WHERE fund_classification = 'Index' AND (scheme_name ILIKE '%MidSmallcap%') AND sub_category != 'Mid Cap Index'", ),
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Small Cap Index' WHERE fund_classification = 'Index' AND (scheme_name ILIKE '%Smallcap%' OR scheme_name ILIKE '%Small Cap%') AND scheme_name NOT ILIKE '%Mid%' AND sub_category != 'Small Cap Index'", ),
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Mid Cap Index' WHERE fund_classification = 'Index' AND (scheme_name ILIKE '%Midcap%' OR scheme_name ILIKE '%Mid Cap%') AND scheme_name NOT ILIKE '%Small%' AND scheme_name NOT ILIKE '%Large%' AND sub_category NOT IN ('Mid Cap Index')", ),
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Large Cap Index' WHERE fund_classification = 'Index' AND (scheme_name ILIKE '%Nifty 50 %' OR scheme_name ILIKE '%Nifty50%' OR scheme_name ILIKE '%Sensex%' OR scheme_name ILIKE '%BSE 100%' OR scheme_name ILIKE '%Nifty Next 50%') AND scheme_name NOT ILIKE '%Nifty 500%' AND sub_category NOT IN ('Large Cap Index')", ),
            ("UPDATE discover_mutual_fund_snapshots SET sub_category = 'Multi Cap Index' WHERE fund_classification = 'Index' AND scheme_name ILIKE '%Total Market%' AND sub_category != 'Multi Cap Index'", ),
        ]
        for sql, in index_corrections:
            r = await pool.execute(sql)
            if r != "UPDATE 0":
                fix_results.append(r)

        # Only rank active funds (recent NAV, direct, non-IDCW, non-closed/FMP)
        active_filter = """
                WHERE nav_date >= CURRENT_DATE - INTERVAL '90 days'
                  AND LOWER(COALESCE(plan_type, 'direct')) = 'direct'
                  AND COALESCE(option_type, '') NOT ILIKE '%idcw%'
                  AND scheme_name NOT ILIKE '%fmp%'
                  AND scheme_name NOT ILIKE '%fixed maturity%'
                  AND scheme_name NOT ILIKE '%close ended%'
                  AND scheme_name NOT ILIKE '%closed ended%'
                  AND scheme_name NOT ILIKE '%interval%fund%'
                  AND scheme_name NOT ILIKE '%capital protection%'
                  AND scheme_name NOT ILIKE '%fixed term%'
                  AND scheme_name NOT ILIKE '%idcw%'
                  AND scheme_name NOT ILIKE '%income distribution%'
                  AND scheme_name NOT ILIKE '%unclaimed%'
                  AND scheme_name NOT ILIKE '%bonus%'
                  AND scheme_name NOT ILIKE '%payout%'
                  AND scheme_name NOT ILIKE '%- monthly%'
                  AND scheme_name NOT ILIKE '%- quarterly%'
                  AND scheme_name NOT ILIKE '%- half yearly%'
                  AND scheme_name NOT ILIKE '%- annual%'
                  AND scheme_name NOT ILIKE '%icdw%'
                  AND scheme_name NOT ILIKE '%idwc%'
                  AND scheme_name NOT ILIKE '%p f option%'
                  AND scheme_name NOT ILIKE '%weekly%'
                  AND scheme_name NOT ILIKE '%daily%'
                  AND scheme_name NOT ILIKE '%linked insurance%'
        """
        sub_r = await pool.execute(f"""
            UPDATE discover_mutual_fund_snapshots AS t
            SET sub_category_rank = sub.rnk, sub_category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(fund_classification, ''), NULLIF(sub_category, ''), NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
                {active_filter}
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
        cat_r = await pool.execute(f"""
            UPDATE discover_mutual_fund_snapshots AS t
            SET category_rank = sub.rnk, category_total = sub.total
            FROM (
                SELECT scheme_code,
                       DENSE_RANK() OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                           ORDER BY score DESC
                       ) AS rnk,
                       COUNT(*) OVER (
                           PARTITION BY COALESCE(NULLIF(category, ''), 'Other')
                       ) AS total
                FROM discover_mutual_fund_snapshots
                {active_filter}
            ) sub
            WHERE t.scheme_code = sub.scheme_code
        """)
        # Verify
        check = await pool.fetchrow(
            "SELECT COUNT(*) AS total, COUNT(sub_category_rank) AS ranked FROM discover_mutual_fund_snapshots"
        )
        return {
            "status": "done",
            "index_fixes": fix_results,
            "sub_category_result": sub_r,
            "category_result": cat_r,
            "total_rows": check["total"],
            "rows_with_sub_rank": check["ranked"],
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@router.get("/jobs")
async def list_jobs(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List all valid job names that can be triggered."""
    _authorize(x_ops_token)
    return {"jobs": sorted(_VALID_JOBS)}


@router.get("/jobs/running")
async def running_jobs(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List currently queued, running, and recently finished ARQ jobs.

    Queries Redis for in-progress jobs, known job IDs, queued jobs,
    and recent results.
    """
    _authorize(x_ops_token)
    from arq.constants import default_queue_name, in_progress_key_prefix
    from arq.jobs import Job, JobStatus

    from app.queue.redis_pool import get_redis_pool

    pool = await get_redis_pool()
    running = []
    queued_list = []
    complete = []
    seen_ids: set[str] = set()

    try:
        def _make_job(job_id: str) -> Job:
            return Job(job_id, pool, _queue_name=default_queue_name)

        # 1. Check in-progress jobs via the Redis set
        in_progress_key = in_progress_key_prefix + default_queue_name
        raw_members = await pool.smembers(in_progress_key)

        for raw_id in raw_members or set():
            job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            seen_ids.add(job_id)
            try:
                job = _make_job(job_id)
                info = await job.info()
                if info:
                    running.append({
                        "job_id": job_id,
                        "function": info.function,
                        "status": "running",
                        "enqueue_time": info.enqueue_time.isoformat() if info.enqueue_time else None,
                        "start_time": info.start_time.isoformat() if info.start_time else None,
                        "elapsed_s": round((
                            datetime.now(timezone.utc) - info.start_time
                        ).total_seconds(), 1) if info.start_time else None,
                    })
            except Exception as exc:
                running.append({"job_id": job_id, "status": "running", "error": str(exc)})

        # 2. Check known job IDs (manual triggers + startup + cron jobs)
        known_ids = (
            [f"{name}_manual" for name in _VALID_JOBS]
            + [f"startup_{name}" for name in _VALID_JOBS]
        )

        for job_id in known_ids:
            if job_id in seen_ids:
                continue
            try:
                job = _make_job(job_id)
                status = await job.status()
                if status == JobStatus.not_found:
                    continue

                seen_ids.add(job_id)
                info = await job.info()
                if not info:
                    continue

                entry = {
                    "job_id": job_id,
                    "function": info.function,
                    "status": status.value,
                    "enqueue_time": info.enqueue_time.isoformat() if info.enqueue_time else None,
                }

                if status == JobStatus.in_progress:
                    entry["start_time"] = info.start_time.isoformat() if info.start_time else None
                    entry["elapsed_s"] = round((
                        datetime.now(timezone.utc) - info.start_time
                    ).total_seconds(), 1) if info.start_time else None
                    running.append(entry)
                elif status in (JobStatus.queued, JobStatus.deferred):
                    queued_list.append(entry)
                elif status == JobStatus.complete:
                    try:
                        result_info = await job.result_info()
                        if result_info:
                            entry["finish_time"] = result_info.finish_time.isoformat() if result_info.finish_time else None
                            entry["success"] = result_info.success
                            entry["result"] = str(result_info.result)[:200] if result_info.result is not None else None
                    except Exception:
                        pass
                    complete.append(entry)
            except Exception:
                continue

        # 3. Also include queued jobs from ARQ's queue
        try:
            arq_queued = await pool.queued_jobs()
            for jdef in arq_queued:
                jid = jdef.job_id
                if jid in seen_ids:
                    continue
                seen_ids.add(jid)
                queued_list.append({
                    "job_id": jid,
                    "function": jdef.function,
                    "status": "queued",
                    "enqueue_time": jdef.enqueue_time.isoformat() if jdef.enqueue_time else None,
                })
        except Exception:
            pass

        # 4. Recent job results
        try:
            results = await pool.all_job_results()
            for r in results:
                if r.job_id in seen_ids:
                    continue
                seen_ids.add(r.job_id)
                complete.append({
                    "job_id": r.job_id,
                    "function": r.function,
                    "status": "complete",
                    "enqueue_time": r.enqueue_time.isoformat() if r.enqueue_time else None,
                    "finish_time": r.finish_time.isoformat() if r.finish_time else None,
                    "success": r.success,
                    "result": str(r.result)[:200] if r.result is not None else None,
                })
        except Exception:
            pass

    except Exception as exc:
        import traceback
        return {
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "running": running,
            "queued": queued_list,
            "complete": complete,
        }

    # Include direct-run jobs in the running list
    for name, task in list(_running_direct_jobs.items()):
        if task.done():
            _running_direct_jobs.pop(name, None)
            continue
        running.append({
            "job_id": f"direct-run-{name}",
            "function": name,
            "status": "running",
            "mode": "direct",
        })

    return {
        "running": running,
        "queued": queued_list,
        "complete": complete,
        "summary": {
            "running": len(running),
            "queued": len(queued_list),
            "complete": len(complete),
        },
    }


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — GET (read)
# ═════════════════════════════════════════════════════════════════════

@router.get("/tables")
async def list_tables(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List all table aliases available for CRUD operations."""
    _authorize(x_ops_token)
    return {"tables": sorted(_TABLES.keys())}


@router.get("/table/{table_key}/row/{row_id}")
async def get_row(
    table_key: str = Path(...),
    row_id: str = Path(..., description="UUID of the row"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Fetch a single row by UUID."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    pool = await _get_pool()
    from app.core.database import record_to_dict

    row = await pool.fetchrow(f'SELECT * FROM {table} WHERE id = $1', row_id)  # noqa: S608
    if row is None:
        raise HTTPException(status_code=404, detail="Row not found")
    return {"table": table, "row": record_to_dict(row)}


@router.get("/table/{table_key}/rows")
async def get_rows(
    table_key: str = Path(...),
    column: str | None = Query(default=None, description="Column to filter on"),
    value: str | None = Query(default=None, description="Value to match (exact)"),
    order_by: str = Query(default="id", description="Column to sort by"),
    order: str = Query(default="desc", description="asc or desc"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """List rows with optional filter, ordering, and pagination."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    _validate_column(order_by)
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"
    pool = await _get_pool()
    from app.core.database import record_to_dict

    if column and value is not None:
        _validate_column(column)
        rows = await pool.fetch(
            f'SELECT * FROM {table} WHERE {column} = $1 ORDER BY {order_by} {sort_dir} LIMIT $2 OFFSET $3',  # noqa: S608
            value, limit, offset,
        )
        total = await pool.fetchval(
            f'SELECT COUNT(*) FROM {table} WHERE {column} = $1',  # noqa: S608
            value,
        )
    else:
        rows = await pool.fetch(
            f'SELECT * FROM {table} ORDER BY {order_by} {sort_dir} LIMIT $1 OFFSET $2',  # noqa: S608
            limit, offset,
        )
        total = await pool.fetchval(f'SELECT COUNT(*) FROM {table}')  # noqa: S608

    return {
        "table": table,
        "rows": [record_to_dict(r) for r in rows],
        "count": len(rows),
        "total": total,
        "limit": limit,
        "offset": offset,
    }


@router.get("/table/{table_key}/count")
async def count_rows(
    table_key: str = Path(...),
    column: str | None = Query(default=None, description="Column to filter on"),
    value: str | None = Query(default=None, description="Value to match"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Count rows, optionally filtered by column=value."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    pool = await _get_pool()

    if column and value is not None:
        _validate_column(column)
        total = await pool.fetchval(
            f'SELECT COUNT(*) FROM {table} WHERE {column} = $1',  # noqa: S608
            value,
        )
    else:
        total = await pool.fetchval(f'SELECT COUNT(*) FROM {table}')  # noqa: S608

    return {"table": table, "count": total}


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — POST (insert)
# ═════════════════════════════════════════════════════════════════════

@router.post("/table/{table_key}/row")
async def insert_row(
    table_key: str = Path(...),
    payload: dict = Body(..., description="Column-value pairs to insert"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Insert a single row. Pass JSON body with column: value pairs."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    if not payload:
        raise HTTPException(status_code=400, detail="Empty payload")

    columns = []
    placeholders = []
    values = []
    for i, (col, val) in enumerate(payload.items(), start=1):
        _validate_column(col)
        columns.append(col)
        placeholders.append(f"${i}")
        values.append(val)

    col_str = ", ".join(columns)
    ph_str = ", ".join(placeholders)
    pool = await _get_pool()
    from app.core.database import record_to_dict

    row = await pool.fetchrow(
        f'INSERT INTO {table} ({col_str}) VALUES ({ph_str}) RETURNING *',  # noqa: S608
        *values,
    )
    logger.info("Ops insert: table=%s columns=%s", table, columns)
    return {"table": table, "row": record_to_dict(row) if row else None}


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — PUT/PATCH (update)
# ═════════════════════════════════════════════════════════════════════

@router.put("/table/{table_key}/row/{row_id}")
async def update_row(
    table_key: str = Path(...),
    row_id: str = Path(..., description="UUID of the row to update"),
    payload: dict = Body(..., description="Column-value pairs to update"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Update a single row by UUID. Pass JSON body with column: value pairs to set."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    if not payload:
        raise HTTPException(status_code=400, detail="Empty payload")

    set_clauses = []
    values = []
    for i, (col, val) in enumerate(payload.items(), start=1):
        _validate_column(col)
        set_clauses.append(f"{col} = ${i}")
        values.append(val)

    set_str = ", ".join(set_clauses)
    id_placeholder = f"${len(values) + 1}"
    values.append(row_id)

    pool = await _get_pool()
    from app.core.database import record_to_dict

    row = await pool.fetchrow(
        f'UPDATE {table} SET {set_str} WHERE id = {id_placeholder} RETURNING *',  # noqa: S608
        *values,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Row not found")
    logger.info("Ops update: table=%s row_id=%s columns=%s", table, row_id, list(payload.keys()))
    return {"table": table, "row": record_to_dict(row)}


@router.patch("/table/{table_key}/rows")
async def update_rows_by_filter(
    table_key: str = Path(...),
    column: str = Query(..., description="Column to filter on"),
    value: str = Query(..., description="Value to match"),
    payload: dict = Body(..., description="Column-value pairs to update"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Bulk update rows matching column=value filter."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    _validate_column(column)
    if not payload:
        raise HTTPException(status_code=400, detail="Empty payload")

    set_clauses = []
    values = []
    for i, (col, val) in enumerate(payload.items(), start=1):
        _validate_column(col)
        set_clauses.append(f"{col} = ${i}")
        values.append(val)

    set_str = ", ".join(set_clauses)
    filter_placeholder = f"${len(values) + 1}"
    values.append(value)

    pool = await _get_pool()
    status = await pool.execute(
        f'UPDATE {table} SET {set_str} WHERE {column} = {filter_placeholder}',  # noqa: S608
        *values,
    )
    updated = int(str(status).split()[-1])
    logger.info("Ops bulk update: table=%s %s=%s updated=%d columns=%s", table, column, value, updated, list(payload.keys()))
    return {"table": table, "updated": updated, "filter": {column: value}}


# ═════════════════════════════════════════════════════════════════════
# Generic CRUD — DELETE
# ═════════════════════════════════════════════════════════════════════

@router.delete("/table/{table_key}/row/{row_id}")
async def delete_row(
    table_key: str = Path(...),
    row_id: str = Path(..., description="UUID of the row to delete"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Delete a specific row by UUID."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    pool = await _get_pool()
    status = await pool.execute(
        f'DELETE FROM {table} WHERE id = $1',  # noqa: S608
        row_id,
    )
    deleted = int(str(status).split()[-1])
    logger.info("Ops delete: table=%s row_id=%s deleted=%d", table, row_id, deleted)
    return {"deleted": deleted, "table": table, "row_id": row_id}


@router.delete("/table/{table_key}/rows")
async def delete_rows_by_filter(
    table_key: str = Path(...),
    column: str = Query(..., description="Column to filter on"),
    value: str = Query(..., description="Value to match (exact)"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Delete rows matching column=value filter."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    _validate_column(column)
    pool = await _get_pool()
    status = await pool.execute(
        f'DELETE FROM {table} WHERE {column} = $1',  # noqa: S608
        value,
    )
    deleted = int(str(status).split()[-1])
    logger.info("Ops bulk delete: table=%s %s=%s deleted=%d", table, column, value, deleted)
    return {"deleted": deleted, "table": table, "filter": {column: value}}


@router.delete("/table/{table_key}/truncate")
async def truncate_table(
    table_key: str = Path(...),
    confirm: str = Query(..., description="Must be 'yes' to confirm truncation"),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Truncate (empty) an entire table. Requires confirm=yes."""
    _authorize(x_ops_token)
    table = _resolve_table(table_key)
    if confirm != "yes":
        raise HTTPException(status_code=400, detail="Pass confirm=yes to truncate")
    pool = await _get_pool()
    await pool.execute(f'TRUNCATE {table}')  # noqa: S608
    logger.warning("Ops truncate: table=%s", table)
    return {"truncated": True, "table": table}


# ═════════════════════════════════════════════════════════════════════
# SQL (read-only)
# ═════════════════════════════════════════════════════════════════════

@router.post("/sql")
async def execute_sql(
    payload: dict = Body(..., description='{"query": "SELECT ...", "params": []}'),
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Execute a read-only SQL query. Only SELECT/WITH statements allowed."""
    _authorize(x_ops_token)
    query = str(payload.get("query", "")).strip()
    params = payload.get("params") or []

    if not query:
        raise HTTPException(status_code=400, detail="Empty query")

    first_word = query.split()[0].upper() if query.split() else ""
    _READ_ONLY = {"SELECT", "WITH", "EXPLAIN"}
    _WRITE_ALLOWED = {"ALTER", "CREATE", "DROP", "UPDATE", "INSERT", "DELETE", "REINDEX", "SET", "VACUUM", "ANALYZE"}

    pool = await _get_pool()
    from app.core.database import record_to_dict

    try:
        if first_word in _READ_ONLY:
            rows = await pool.fetch(query, *params)
            return {
                "rows": [record_to_dict(r) for r in rows],
                "count": len(rows),
            }
        elif first_word in _WRITE_ALLOWED:
            result = await pool.execute(query, *params)
            return {"status": "ok", "result": result}
        else:
            raise HTTPException(status_code=400, detail=f"Statement type '{first_word}' not allowed")
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ═════════════════════════════════════════════════════════════════════
# Config / Debug
# ═════════════════════════════════════════════════════════════════════

@router.get("/macro-config")
async def macro_config(
    x_ops_token: str | None = Header(default=None),
) -> dict:
    """Return current FRED_CPI / FRED_DIRECT config for debugging."""
    _authorize(x_ops_token)
    from app.scheduler.macro_job import FRED_CPI, FRED_DIRECT, VALUE_RANGES

    return {
        "fred_cpi": FRED_CPI,
        "fred_direct": {k: [(n, s) for n, s in v] for k, v in FRED_DIRECT.items()},
        "value_ranges": VALUE_RANGES,
    }


@router.get("/data-health", response_model=DataHealthResponse)
async def data_health(
    x_ops_token: str | None = Header(default=None),
) -> DataHealthResponse:
    _authorize(x_ops_token)
    payload = await market_intel_service.get_data_health()
    return DataHealthResponse(**payload)


@router.post("/test-notification")
async def test_notification() -> dict:
    """Send a test push notification to all subscribed devices."""
    from app.services.notification_service import send_topic_notification
    success = await send_topic_notification(
        topic="market_alerts",
        title="🔔 EconAtlas Test",
        body="Push notifications are working! You'll receive market open/close alerts.",
        data={"type": "test"},
    )
    return {"sent": success}
