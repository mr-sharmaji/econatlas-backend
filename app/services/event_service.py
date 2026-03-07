from app.core.database import get_pool, record_to_dict
from app.schemas.event_schema import EventCreate

TABLE = "economic_events"


async def insert_event(payload: EventCreate) -> dict:
    """Insert a new economic event and return the created row."""
    pool = await get_pool()
    data = payload.model_dump()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE} (event_type, entity, impact, confidence)
        VALUES ($1, $2, $3, $4)
        RETURNING *
        """,
        data["event_type"],
        data["entity"],
        data["impact"],
        data["confidence"],
    )
    return record_to_dict(row)


async def insert_event_dict(payload: dict) -> dict:
    """Insert an event payload dict and return the created row."""
    pool = await get_pool()
    row = await pool.fetchrow(
        f"""
        INSERT INTO {TABLE} (event_type, entity, impact, confidence)
        VALUES ($1, $2, $3, $4)
        RETURNING *
        """,
        payload["event_type"],
        payload["entity"],
        payload["impact"],
        payload["confidence"],
    )
    return record_to_dict(row)


async def get_events(limit: int = 50, offset: int = 0) -> list[dict]:
    """Fetch the most recent economic events ordered by creation time."""
    pool = await get_pool()
    rows = await pool.fetch(
        f"SELECT * FROM {TABLE} ORDER BY created_at DESC LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    return [record_to_dict(r) for r in rows]
