"""Pydantic models for the Artha AI chat API."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# --- Request models ---

class ChatMessageRequest(BaseModel):
    device_id: str = Field(..., min_length=6)
    session_id: str | None = None  # None → create new session
    message: str = Field(..., min_length=1, max_length=2000)


class ChatFeedbackRequest(BaseModel):
    device_id: str = Field(..., min_length=6)
    message_id: str
    feedback: Literal[1, -1]  # 1 = thumbs up, -1 = thumbs down


# --- Response models ---

class StockCard(BaseModel):
    symbol: str
    display_name: str
    sector: str | None = None
    last_price: float | None = None
    percent_change: float | None = None
    score: float | None = None
    market_cap: float | None = None


class MFCard(BaseModel):
    scheme_code: str
    scheme_name: str
    display_name: str | None = None
    category: str | None = None
    nav: float | None = None
    returns_1y: float | None = None
    score: float | None = None


class ChatMessageResponse(BaseModel):
    id: str
    session_id: str
    role: Literal["user", "assistant"]
    content: str
    thinking_text: str | None = None
    stock_cards: list[StockCard] = Field(default_factory=list)
    mf_cards: list[MFCard] = Field(default_factory=list)
    feedback: int | None = None
    created_at: datetime


class ChatSessionResponse(BaseModel):
    id: str
    device_id: str
    title: str | None = None
    created_at: datetime
    updated_at: datetime
    message_count: int = 0


class ChatSessionDetailResponse(BaseModel):
    session: ChatSessionResponse
    messages: list[ChatMessageResponse] = Field(default_factory=list)


class ChatSessionListResponse(BaseModel):
    sessions: list[ChatSessionResponse] = Field(default_factory=list)
    count: int = 0


class ChatGreetingResponse(BaseModel):
    greeting: str
    suggestions: list[str] = Field(default_factory=list)


class ChatSuggestionsResponse(BaseModel):
    suggestions: list[str] = Field(default_factory=list)


class AutocompleteItem(BaseModel):
    symbol: str | None = None
    scheme_code: str | None = None
    name: str
    type: Literal["stock", "mf"]
    score: float | None = None


class AutocompleteResponse(BaseModel):
    items: list[AutocompleteItem] = Field(default_factory=list)
