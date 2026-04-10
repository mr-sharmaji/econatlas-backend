"""Artha AI chat endpoints — SSE streaming + REST for session management."""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse

from app.schemas.chat_schema import (
    AutocompleteResponse,
    ChatFeedbackRequest,
    ChatGreetingResponse,
    ChatMessageRequest,
    ChatSessionDetailResponse,
    ChatSessionListResponse,
    ChatSessionResponse,
    ChatSuggestionsResponse,
)
from app.services import chat_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/chat", tags=["chat"])


# ---------------------------------------------------------------------------
# SSE streaming chat
# ---------------------------------------------------------------------------

@router.post("/stream")
async def stream_chat(req: ChatMessageRequest):
    """Main chat endpoint — streams response via Server-Sent Events.

    SSE events:
    - thinking: {"status": "Artha is thinking..."}
    - token: {"text": "word "}
    - stock_card: {symbol, display_name, last_price, ...}
    - mf_card: {scheme_code, scheme_name, nav, ...}
    - done: {"message_id": "...", "session_id": "..."}
    - error: {"message": "...", "retry": true/false}
    """
    device_id = req.device_id.strip()

    # Rate limit check
    allowed, remaining = await chat_service.check_rate_limit(device_id)
    if not allowed:
        async def rate_limit_stream():
            yield _sse_event("error", {
                "message": "You've reached your daily limit of 300 messages. Try again tomorrow!",
                "retry": False,
            })
        return StreamingResponse(
            rate_limit_stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Create or reuse session
    session_id = req.session_id
    if not session_id:
        session_id = await chat_service.create_session(device_id)

    async def event_stream():
        try:
            async for event in chat_service.stream_chat_response(
                device_id=device_id,
                session_id=session_id,
                user_message=req.message,
            ):
                yield _sse_event(event["event"], event["data"])
        except Exception as e:
            logger.error("Chat stream error: %s", e, exc_info=True)
            error_message = "Something went wrong. Please try again."
            message_id = None
            try:
                message_id = await chat_service.save_message(
                    session_id,
                    "assistant",
                    error_message,
                )
            except Exception:
                logger.exception("Failed to persist fallback chat error")
            yield _sse_event("error", {
                "message": error_message,
                "retry": True,
                "message_id": message_id,
                "session_id": session_id,
            })

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse_event(event: str, data: dict) -> str:
    """Format a single SSE event."""
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@router.get("/sessions", response_model=ChatSessionListResponse)
async def list_sessions(device_id: str = Query(..., min_length=6)):
    """List all chat sessions for a device."""
    sessions = await chat_service.list_sessions(device_id.strip())
    return ChatSessionListResponse(
        sessions=[
            ChatSessionResponse(
                id=s["id"],
                device_id=s["device_id"],
                title=s.get("title"),
                created_at=s["created_at"],
                updated_at=s["updated_at"],
                message_count=s.get("message_count", 0),
            )
            for s in sessions
        ],
        count=len(sessions),
    )


@router.get("/sessions/{session_id}", response_model=ChatSessionDetailResponse)
async def get_session(session_id: str, device_id: str = Query(..., min_length=6)):
    """Get a session with all its messages."""
    sessions = await chat_service.list_sessions(device_id.strip())
    session = next((s for s in sessions if s["id"] == session_id), None)
    if not session:
        return ChatSessionDetailResponse(
            session=ChatSessionResponse(
                id=session_id,
                device_id=device_id,
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:00:00Z",
            ),
            messages=[],
        )

    messages = await chat_service.get_session_messages(session_id, device_id.strip())
    return ChatSessionDetailResponse(
        session=ChatSessionResponse(
            id=session["id"],
            device_id=session["device_id"],
            title=session.get("title"),
            created_at=session["created_at"],
            updated_at=session["updated_at"],
            message_count=session.get("message_count", 0),
        ),
        messages=[
            {
                "id": m["id"],
                "session_id": m["session_id"],
                "role": m["role"],
                "content": m["content"],
                "thinking_text": m.get("thinking_text"),
                "stock_cards": m.get("stock_cards") or [],
                "mf_cards": m.get("mf_cards") or [],
                "feedback": m.get("feedback"),
                "created_at": m["created_at"],
            }
            for m in messages
        ],
    )


@router.delete("/sessions/{session_id}")
async def delete_session(session_id: str, device_id: str = Query(..., min_length=6)):
    """Delete a chat session and all its messages."""
    deleted = await chat_service.delete_session(session_id, device_id.strip())
    return {"deleted": deleted}


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------

@router.post("/feedback")
async def submit_feedback(req: ChatFeedbackRequest):
    """Submit thumbs up/down feedback on a message."""
    updated = await chat_service.set_feedback(
        req.message_id,
        req.device_id.strip(),
        req.feedback,
    )
    return {"updated": updated}


# ---------------------------------------------------------------------------
# Greeting & suggestions
# ---------------------------------------------------------------------------

@router.get("/greeting", response_model=ChatGreetingResponse)
async def get_greeting():
    """Get a context-aware greeting for new chat."""
    data = await chat_service.generate_greeting()
    suggestions = await chat_service.generate_suggestions(device_id=None)
    return ChatGreetingResponse(
        greeting=data["greeting"],
        suggestions=suggestions,
    )


@router.get("/suggestions", response_model=ChatSuggestionsResponse)
async def get_suggestions(device_id: str = Query(None)):
    """Get dynamic suggested prompts based on market hours and watchlist."""
    suggestions = await chat_service.generate_suggestions(device_id=device_id)
    return ChatSuggestionsResponse(suggestions=suggestions)


# ---------------------------------------------------------------------------
# Autocomplete
# ---------------------------------------------------------------------------

@router.get("/autocomplete", response_model=AutocompleteResponse)
async def autocomplete_search(q: str = Query(..., min_length=1)):
    """Search stocks and MFs for autocomplete (triggered by @ in input)."""
    items = await chat_service.autocomplete(q.strip())
    return AutocompleteResponse(items=items)
