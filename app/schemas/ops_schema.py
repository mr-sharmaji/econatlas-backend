from datetime import datetime

from pydantic import BaseModel


class LogEntryResponse(BaseModel):
    id: int
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
