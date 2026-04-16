"""Runtime fingerprint helpers for ops/debug surfaces."""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path

from app.api.routes import ops as ops_route
from app.queue import tasks as queue_tasks
from app.services import future_prospects_service

_STARTED_AT = datetime.now(timezone.utc)
_ROOT = Path(__file__).resolve().parents[2]


def _short_sha256(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()[:12]
    except Exception:
        return "unavailable"


@lru_cache
def get_runtime_info() -> dict[str, object]:
    git_sha = (
        os.environ.get("APP_GIT_SHA")
        or os.environ.get("GIT_SHA")
        or os.environ.get("COMMIT_SHA")
        or "unknown"
    )
    files = {
        "ops_py": _short_sha256(_ROOT / "app" / "api" / "routes" / "ops.py"),
        "queue_tasks_py": _short_sha256(_ROOT / "app" / "queue" / "tasks.py"),
        "future_prospects_service_py": _short_sha256(
            _ROOT / "app" / "services" / "future_prospects_service.py"
        ),
        "main_py": _short_sha256(_ROOT / "app" / "main.py"),
    }
    return {
        "git_sha": git_sha,
        "started_at": _STARTED_AT.isoformat(),
        "fingerprint": hashlib.sha256(
            "|".join(str(files[k]) for k in sorted(files)).encode("utf-8")
        ).hexdigest()[:16],
        "files": files,
        "features": {
            "future_prospects_runtime_marker": future_prospects_service.FUTURE_PROSPECTS_RUNTIME_MARKER,
            "future_prospects_mode": future_prospects_service.FUTURE_PROSPECTS_RUNTIME_MODE,
            "ops_runtime_marker": ops_route.OPS_RUNTIME_MARKER,
            "queue_logging_marker": queue_tasks.QUEUE_RETRY_LOG_MARKER,
            "stock_future_prospects_direct_run": "stock_future_prospects" in ops_route._DIRECT_RUN_JOBS,
        },
    }
