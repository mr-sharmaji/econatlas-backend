from datetime import datetime

from pydantic import BaseModel, Field


class EventCreate(BaseModel):
    """Payload accepted when creating a new economic event."""

    event_type: str = Field(..., min_length=1, examples=["oil_spike"])
    entity: str = Field(..., min_length=1, examples=["oil"])
    impact: str = Field(..., min_length=1, examples=["inflationary"])
    confidence: float = Field(..., ge=0.0, le=1.0, examples=[0.9])


class EventResponse(BaseModel):
    """Single economic event returned by the API."""

    id: str
    event_type: str
    entity: str
    impact: str
    confidence: float
    created_at: datetime


class EventListResponse(BaseModel):
    """Wrapper for a list of economic events."""

    events: list[EventResponse]
    count: int
