from fastapi import APIRouter, HTTPException, Query

from app.schemas.event_schema import (
    EventCreate,
    EventListResponse,
    EventResponse,
)
from app.services import event_service

router = APIRouter(prefix="/events", tags=["events"])


@router.post("", response_model=EventResponse, status_code=201)
async def create_event(payload: EventCreate) -> EventResponse:
    """Receive a new economic event (typically from the scraper service)."""
    try:
        row = await event_service.insert_event(payload)
        return EventResponse(**row)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=EventListResponse)
async def list_events(
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> EventListResponse:
    """Return the most recent economic events."""
    try:
        rows = await event_service.get_events(limit=limit, offset=offset)
        events = [EventResponse(**r) for r in rows]
        return EventListResponse(events=events, count=len(events))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
