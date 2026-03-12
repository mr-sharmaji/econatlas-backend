from app.core.database import get_pool
from app.schemas.feedback_schema import (
    FeedbackEntryResponse,
    FeedbackListResponse,
    FeedbackSubmitRequest,
    FeedbackSubmitResponse,
)


async def submit_feedback(payload: FeedbackSubmitRequest) -> FeedbackSubmitResponse:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO feedback_submissions (
                device_id,
                category,
                message,
                app_version,
                platform
            )
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, status, created_at
            """,
            payload.device_id,
            payload.category,
            payload.message,
            payload.app_version,
            payload.platform,
        )

    if row is None:
        raise RuntimeError("Failed to store feedback")

    return FeedbackSubmitResponse(
        id=str(row["id"]),
        status=str(row["status"] or "received"),
        created_at=row["created_at"],
    )


async def list_feedback(limit: int = 80) -> FeedbackListResponse:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                id,
                device_id,
                category,
                message,
                app_version,
                platform,
                status,
                created_at
            FROM feedback_submissions
            ORDER BY created_at DESC
            LIMIT $1
            """,
            limit,
        )

    entries = [
        FeedbackEntryResponse(
            id=str(row["id"]),
            device_id=str(row["device_id"] or ""),
            category=str(row["category"] or ""),
            message=str(row["message"] or ""),
            app_version=row["app_version"],
            platform=row["platform"],
            status=str(row["status"] or "received"),
            created_at=row["created_at"],
        )
        for row in rows
    ]
    return FeedbackListResponse(entries=entries, count=len(entries))
