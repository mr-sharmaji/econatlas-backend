from fastapi import APIRouter, HTTPException, Query

from app.schemas.feedback_schema import (
    FeedbackListResponse,
    FeedbackSubmitRequest,
    FeedbackSubmitResponse,
)
from app.services import feedback_service

router = APIRouter(prefix="/feedback", tags=["feedback"])


@router.post("", response_model=FeedbackSubmitResponse)
async def post_feedback(payload: FeedbackSubmitRequest) -> FeedbackSubmitResponse:
    try:
        return await feedback_service.submit_feedback(payload)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("", response_model=FeedbackListResponse)
async def get_feedback(
    limit: int = Query(default=80, ge=1, le=500),
) -> FeedbackListResponse:
    try:
        return await feedback_service.list_feedback(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
