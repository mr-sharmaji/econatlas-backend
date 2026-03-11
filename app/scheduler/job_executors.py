from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock

_lock = Lock()
_executors: dict[str, ThreadPoolExecutor] = {}


def get_job_executor(job_name: str) -> ThreadPoolExecutor:
    name = (job_name or "default").strip().lower()
    if not name:
        name = "default"
    with _lock:
        executor = _executors.get(name)
        if executor is None:
            executor = ThreadPoolExecutor(
                max_workers=1,
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
