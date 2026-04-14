"""Self-heal arq state left behind by a SIGKILL'd worker.

Background
==========
arq writes one or more of these Redis keys for every job it dispatches:

* the in-progress set ``arq:in-progress:arq:queue`` (members = job ids
  currently being worked on)
* a per-job-id in-progress TTL string key ``arq:in-progress:{job_id}``
  — this is a lock whose TTL equals the job's configured timeout
* a per-job-id hash ``arq:job:{job_id}`` with the job's metadata

If the worker process is SIGKILL'd mid-run (``docker compose down``
after its 10s SIGTERM grace, OOM, host reboot, crash), none of these
get released — the TTL string lock in particular can sit orphaned for
hours on long-timeout jobs like ``startup_discover_mutual_funds``.
The next worker that dispatches the same job_id then hits the orphaned
lock and logs ``"job {job_id} already running elsewhere"``, dropping
the dispatch. The whole job family stays blocked until the TTL expires
or something clears the key manually.

This module does the clearing. It is safe to call exactly once, at app
startup, BEFORE ``start_worker()`` and ``start_scheduler()`` run — at
that moment there is by definition no job legitimately in flight, so
every ``arq:in-progress:*`` key we find is necessarily orphaned. The
``/ops/jobs/clear-stale`` endpoint is still useful for mid-session
cleanups (rare) and delegates to the same helper.

What this does NOT touch
========================
* The ARQ queue sorted set (``arq:queue:{name}``). Queued-but-not-yet-
  picked-up jobs stay intact — their metadata hashes may be cleared
  below, but the scheduler's ``_startup_collection()`` re-enqueues
  fresh instances a few seconds after startup, so nothing is lost.
* APScheduler state. That lives in-process and is reloaded on every
  ``start_scheduler()`` call.
* Application data (market_prices, discover_mf, etc.). This module
  only touches keys under the ``arq:*`` prefix.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass  # no type-only imports needed currently

logger = logging.getLogger(__name__)


async def clear_stale_arq_state() -> dict:
    """Delete orphaned arq in-progress keys and job hashes.

    Returns a dict with the lists of cleared identifiers plus a ``total``
    count. Safe to call at startup; safe to call mid-session (callers
    outside of startup should know that any job actively executing in
    another worker will briefly lose its lock — in practice we don't run
    multiple worker processes so this isn't a concern, but note it).

    Never raises on individual Redis errors — logs and continues, so a
    transient issue clearing one key can't take down app startup.
    """
    from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix

    from app.queue.redis_pool import get_redis_pool
    from app.queue.settings import expand_job_family_ids

    # Late import to keep ops.py as the single source of the allow-list.
    # If we ever want to drop this coupling we can copy the set here.
    from app.api.routes.ops import _VALID_JOBS

    pool = await get_redis_pool()
    cleared_progress: list[str] = []
    cleared_per_job: list[str] = []
    cleared_jobs: list[str] = []

    # ── 1. Clear the in-progress set ────────────────────────────────
    in_progress_key = in_progress_key_prefix + default_queue_name
    try:
        raw_members = await pool.smembers(in_progress_key)
        for raw_id in raw_members or set():
            job_id = raw_id.decode() if isinstance(raw_id, bytes) else str(raw_id)
            try:
                await pool.srem(in_progress_key, raw_id)
                cleared_progress.append(job_id)
            except Exception:
                logger.warning("clear_stale: failed to srem %s", job_id, exc_info=True)
    except Exception:
        logger.warning("clear_stale: failed to read in-progress set", exc_info=True)

    # ── 2. Delete per-job-id in-progress keys ───────────────────────
    # These are TTL string keys at arq:in-progress:{job_id}. THIS is
    # what causes "already running elsewhere" — an orphaned key here
    # blocks every subsequent dispatch of the same job_id until its
    # TTL expires. On long-timeout jobs that can be hours.
    try:
        async for key in pool.scan_iter(match=f"{in_progress_key_prefix}*"):
            key_str = key.decode() if isinstance(key, bytes) else str(key)
            if key_str == in_progress_key:
                continue  # that's the set from step 1
            try:
                await pool.delete(key)
                cleared_per_job.append(key_str.removeprefix(in_progress_key_prefix))
            except Exception:
                logger.warning("clear_stale: failed to delete %s", key_str, exc_info=True)
    except Exception:
        logger.warning("clear_stale: scan_iter for per-job locks failed", exc_info=True)

    # ── 3. Delete individual job-hash keys for known job families ───
    # These are at arq:job:{id} and hold the job's metadata. Deleting
    # them prevents a freshly-started worker from finding a stale
    # "completed" record and skipping a re-enqueue.
    known_ids: set[str] = set()
    try:
        for name in _VALID_JOBS:
            known_ids.update(expand_job_family_ids(name))
    except Exception:
        logger.warning("clear_stale: failed to expand job family ids", exc_info=True)

    for jid in known_ids:
        job_key = job_key_prefix + jid
        try:
            if await pool.exists(job_key):
                await pool.delete(job_key)
                cleared_jobs.append(jid)
        except Exception:
            logger.warning("clear_stale: failed to delete %s", job_key, exc_info=True)

    # ── 4. Scan for timestamped manual-trigger job keys ─────────────
    try:
        async for key in pool.scan_iter(match=f"{job_key_prefix}*_manual_*"):
            key_str = key.decode() if isinstance(key, bytes) else str(key)
            try:
                await pool.delete(key)
                cleared_jobs.append(key_str.removeprefix(job_key_prefix))
            except Exception:
                logger.warning("clear_stale: failed to delete manual %s", key_str, exc_info=True)
    except Exception:
        logger.warning("clear_stale: scan_iter for manual keys failed", exc_info=True)

    total = len(cleared_progress) + len(cleared_per_job) + len(cleared_jobs)
    if total:
        logger.warning(
            "clear_stale: cleared %d in-progress-set entries, %d per-job locks, "
            "%d job-hash keys (self-heal after prior SIGKILL)",
            len(cleared_progress), len(cleared_per_job), len(cleared_jobs),
        )
    else:
        logger.debug("clear_stale: no stale arq state to clean up")

    return {
        "cleared_in_progress_set": cleared_progress,
        "cleared_per_job_locks": cleared_per_job,
        "cleared_job_keys": cleared_jobs,
        "total": total,
    }
