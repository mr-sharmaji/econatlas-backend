from __future__ import annotations

import logging
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any

_lock = threading.Lock()
_entries: deque[dict[str, Any]] = deque(maxlen=5000)
_next_id = 1
_handler: "_InMemoryLogHandler | None" = None
_formatter = logging.Formatter()


class _InMemoryLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover (simple adapter)
        global _next_id
        try:
            ts = datetime.fromtimestamp(record.created, tz=timezone.utc)
            payload = {
                "id": _next_id,
                "timestamp": ts.isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }
            if record.exc_info:
                payload["exception"] = _formatter.formatException(record.exc_info)
            with _lock:
                _entries.append(payload)
                _next_id += 1
        except Exception:
            # Never break logging pipeline due to diagnostics handler.
            return


def setup_log_stream(max_entries: int = 5000) -> None:
    """Attach one in-memory ring-buffer handler to root logger."""
    global _handler, _entries
    max_entries = max(100, int(max_entries))
    with _lock:
        if _handler is not None:
            if _entries.maxlen != max_entries:
                _entries = deque(list(_entries), maxlen=max_entries)
            return
        _entries = deque(maxlen=max_entries)
        _handler = _InMemoryLogHandler()
        _handler.setLevel(logging.INFO)
        root = logging.getLogger()
        root.addHandler(_handler)


def _level_value(level: str | None) -> int | None:
    if not level:
        return None
    name = level.strip().upper()
    if not name:
        return None
    if hasattr(logging, name):
        value = getattr(logging, name)
        if isinstance(value, int):
            return value
    named = logging.getLevelName(name)
    return named if isinstance(named, int) else None


def get_log_entries(
    *,
    limit: int = 200,
    after_id: int | None = None,
    min_level: str | None = None,
    contains: str | None = None,
    logger_name: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return filtered recent log entries plus latest id in buffer."""
    with _lock:
        snapshot = list(_entries)
        latest_id = snapshot[-1]["id"] if snapshot else 0

    min_level_value = _level_value(min_level)
    contains_norm = contains.lower() if contains else None
    logger_norm = logger_name.lower() if logger_name else None

    filtered: list[dict[str, Any]] = []
    for e in snapshot:
        if after_id is not None and int(e["id"]) <= after_id:
            continue
        if min_level_value is not None:
            entry_level = _level_value(str(e.get("level")))
            if entry_level is None or entry_level < min_level_value:
                continue
        if contains_norm and contains_norm not in str(e.get("message", "")).lower():
            continue
        if logger_norm and logger_norm not in str(e.get("logger", "")).lower():
            continue
        filtered.append(e)

    if limit > 0:
        filtered = filtered[-limit:]
    return filtered, latest_id
