from datetime import datetime

from pydantic import BaseModel


class LogEntryResponse(BaseModel):
    # `id` is monotonic for in-memory ring-buffer entries (used for
    # incremental tailing via `after_id`), but synthetic/best-effort
    # for file-backed entries which have no real cursor. Optional so
    # the file tailer can omit it and clients can fall back to
    # `timestamp` for ordering.
    id: int = 0
    timestamp: datetime
    level: str
    logger: str
    message: str
    module: str | None = None
    function: str | None = None
    line: int | None = None
    exception: str | None = None


class LogListResponse(BaseModel):
    entries: list[LogEntryResponse]
    count: int
    latest_id: int
