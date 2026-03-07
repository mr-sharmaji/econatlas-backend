from app.core.supabase_client import get_supabase
from app.schemas.event_schema import EventCreate

TABLE = "economic_events"


async def insert_event(payload: EventCreate) -> dict:
    """Insert a new economic event and return the created row."""
    client = get_supabase()
    result = (
        client.table(TABLE)
        .insert(payload.model_dump())
        .execute()
    )
    return result.data[0]


async def insert_event_dict(payload: dict) -> dict:
    """Insert an event payload dict and return the created row."""
    client = get_supabase()
    result = client.table(TABLE).insert(payload).execute()
    return result.data[0]


async def get_events(limit: int = 50, offset: int = 0) -> list[dict]:
    """Fetch the most recent economic events ordered by creation time."""
    client = get_supabase()
    result = (
        client.table(TABLE)
        .select("*")
        .order("created_at", desc=True)
        .range(offset, offset + limit - 1)
        .execute()
    )
    return result.data
