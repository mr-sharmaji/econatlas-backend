from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

_lock = Lock()
_executors: dict[str, ThreadPoolExecutor] = {}


def get_job_executor(job_name: str, max_workers: int = 1) -> ThreadPoolExecutor:
    """Return (and cache) a ThreadPoolExecutor for the named job.

    The first call for a given name decides the pool size — subsequent
    calls return the cached executor regardless of their max_workers
    argument. Default max_workers=1 preserves legacy behavior where
    every executor was single-threaded; jobs that need real parallelism
    (e.g. discover_mf_nav, which runs ~2,000 blocking HTTP fetches)
    should pass a higher value.
    """
    name = (job_name or "default").strip().lower()
    if not name:
        name = "default"
    with _lock:
        executor = _executors.get(name)
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=max(1, int(max_workers)),
                thread_name_prefix=f"scheduler-{name}",
            )
            _executors[name] = executor
        return executor


def shutdown_job_executors() -> None:
    with _lock:
        executors = list(_executors.values())
        _executors.clear()
    for executor in executors:
        executor.shutdown(wait=False, cancel_futures=True)
