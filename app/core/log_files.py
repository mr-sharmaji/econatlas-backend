"""Rotating file log sink with 7-day on-disk retention.

Adds a ``TimedRotatingFileHandler`` to the root logger so every log record
lands in ``<repo>/<ops_log_dir>/<ops_log_filename>`` (default ``logs/app.log``).

Rotation happens at local midnight; the handler keeps ``ops_log_retention_days``
historical files (default 7) before deleting the oldest one, giving us a
rolling 7-day window for free — no background cleanup task needed.

Also exposes ``tail_log_files`` which the ``/ops/logs`` endpoint uses to
serve history across the live file plus its rotated siblings, with the
same filter semantics as the in-memory ring buffer.
"""
from __future__ import annotations

import logging
import logging.handlers
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Project root = parent of `app/` (matches database.py convention).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_FILE_FORMAT = "%(asctime)s %(levelname)s %(name)s %(module)s:%(funcName)s:%(lineno)d - %(message)s"
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

_handler: logging.handlers.TimedRotatingFileHandler | None = None
_log_dir: Path | None = None
_base_filename: str = "app.log"

# Parser for the standard line format above. Example line:
# 2026-04-15T12:34:56+0000 INFO app.services.foo foo:run:42 - did a thing
_LINE_RE = re.compile(
    r"^(?P<timestamp>\S+)\s+"
    r"(?P<level>DEBUG|INFO|WARNING|ERROR|CRITICAL)\s+"
    r"(?P<logger>\S+)\s+"
    r"(?P<module>[^:\s]+):(?P<function>[^:\s]+):(?P<line>\d+)\s+-\s+"
    r"(?P<message>.*)$"
)

_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def setup_rotating_file_logs(
    *,
    log_dir: str,
    filename: str,
    retention_days: int,
    level: int,
) -> Path | None:
    """Install a 7-day rotating file handler on the root logger.

    Idempotent — re-invoking updates level and retention but will not
    attach a second handler. Returns the absolute log directory on
    success, or ``None`` if the directory couldn't be created.
    """
    global _handler, _log_dir, _base_filename

    retention_days = max(1, int(retention_days or 7))
    _base_filename = filename or "app.log"

    # Resolve and create the log directory. Relative paths resolve
    # against the backend repo root so behavior matches local dev +
    # containers where the repo is mounted at /app.
    dir_path = Path(log_dir or "logs")
    if not dir_path.is_absolute():
        dir_path = _PROJECT_ROOT / dir_path
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logger.warning("ops_logs: cannot create log directory %s: %s", dir_path, exc)
        return None

    _log_dir = dir_path
    target = dir_path / _base_filename

    root = logging.getLogger()

    if _handler is not None:
        _handler.setLevel(level)
        _handler.backupCount = retention_days
        return dir_path

    try:
        handler = logging.handlers.TimedRotatingFileHandler(
            filename=str(target),
            when="midnight",
            interval=1,
            backupCount=retention_days,
            encoding="utf-8",
            delay=False,
            utc=False,
        )
    except OSError as exc:
        logger.warning("ops_logs: cannot open log file %s: %s", target, exc)
        return None

    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_DATE_FORMAT))
    root.addHandler(handler)
    _handler = handler
    logger.info(
        "ops_logs: rotating file sink ready at %s (retention=%d days)",
        target, retention_days,
    )
    return dir_path


def teardown_rotating_file_logs() -> None:
    global _handler
    if _handler is None:
        return
    try:
        logging.getLogger().removeHandler(_handler)
        _handler.close()
    except Exception:
        pass
    _handler = None


def is_enabled() -> bool:
    return _handler is not None and _log_dir is not None


def _log_file_paths() -> list[Path]:
    """Return the active log file plus any rotated siblings, oldest first."""
    if _log_dir is None:
        return []
    try:
        rotated = sorted(
            (p for p in _log_dir.iterdir()
             if p.is_file() and p.name.startswith(_base_filename + ".")),
            key=lambda p: p.name,
        )
    except OSError:
        rotated = []
    active = _log_dir / _base_filename
    paths: list[Path] = list(rotated)
    if active.exists():
        paths.append(active)
    return paths


def _parse_line(raw: str) -> dict[str, Any] | None:
    line = raw.rstrip("\n")
    if not line:
        return None
    m = _LINE_RE.match(line)
    if not m:
        # Continuation of a traceback / unparseable line — skip rather than
        # mis-attribute it. Callers can still grep the raw file if needed.
        return None
    try:
        ts = datetime.strptime(m.group("timestamp"), _DATE_FORMAT)
    except ValueError:
        try:
            ts = datetime.fromisoformat(m.group("timestamp").replace("Z", "+00:00"))
        except ValueError:
            ts = datetime.now(tz=timezone.utc)
    try:
        line_no: int | None = int(m.group("line"))
    except ValueError:
        line_no = None
    return {
        "timestamp": ts,
        "level": m.group("level"),
        "logger": m.group("logger"),
        "module": m.group("module"),
        "function": m.group("function"),
        "line": line_no,
        "message": m.group("message"),
        "exception": None,
    }


def _level_at_or_above(record_level: str, min_level: str) -> bool:
    try:
        r_idx = _LEVELS.index(record_level.upper())
        m_idx = _LEVELS.index(min_level.upper())
    except ValueError:
        return True
    return r_idx >= m_idx


def _iter_lines_reverse(path: Path, chunk_size: int = 65536) -> Iterable[str]:
    """Yield lines from ``path`` in reverse order, memory-bounded.

    Good enough for tailing the last N matching records from a possibly
    large log file without loading the whole file into memory.
    """
    try:
        with path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            pos = f.tell()
            buf = b""
            while pos > 0:
                read = min(chunk_size, pos)
                pos -= read
                f.seek(pos)
                chunk = f.read(read)
                buf = chunk + buf
                lines = buf.split(b"\n")
                # Keep the first (possibly partial) line for the next iter.
                buf = lines[0]
                for line in reversed(lines[1:]):
                    yield line.decode("utf-8", errors="replace")
            if buf:
                yield buf.decode("utf-8", errors="replace")
    except OSError:
        return


def tail_log_files(
    *,
    limit: int = 200,
    min_level: str | None = None,
    contains: str | None = None,
    logger_name: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> list[dict[str, Any]]:
    """Return up to ``limit`` most-recent parsed log entries matching filters.

    Walks files newest-first (active file, then rotated copies from newest
    to oldest), reading each in reverse so the scan can stop as soon as
    enough matches accumulate.
    """
    if _log_dir is None:
        return []

    safe_limit = max(1, min(int(limit), 5000))
    contains_norm = contains.lower() if contains else None
    logger_norm = logger_name.lower() if logger_name else None

    # Newest files first: active log, then rotated copies from newest to oldest.
    paths = list(reversed(_log_file_paths()))

    out: list[dict[str, Any]] = []
    for path in paths:
        if len(out) >= safe_limit:
            break
        for raw in _iter_lines_reverse(path):
            entry = _parse_line(raw)
            if entry is None:
                continue
            if min_level and not _level_at_or_above(entry["level"], min_level):
                continue
            if contains_norm and contains_norm not in entry["message"].lower():
                continue
            if logger_norm and logger_norm not in entry["logger"].lower():
                continue
            ts = entry["timestamp"]
            if since is not None and ts < since:
                # Files are time-ordered: once we pass `since`, older lines
                # in this file are all out of range — stop scanning it.
                break
            if until is not None and ts > until:
                continue
            out.append(entry)
            if len(out) >= safe_limit:
                break

    # Callers expect ascending time order.
    out.reverse()
    # Assign synthetic per-response ids so clients can sort reliably
    # even though file-backed entries have no real monotonic cursor.
    for i, entry in enumerate(out, start=1):
        entry["id"] = i
    return out
