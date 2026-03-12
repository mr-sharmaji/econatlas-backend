from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator

_FEEDBACK_CATEGORIES = {
    "bug",
    "data_issue",
    "feature_request",
    "ux",
    "other",
}


class FeedbackSubmitRequest(BaseModel):
    device_id: str = Field(min_length=6, max_length=128)
    category: Literal["bug", "data_issue", "feature_request", "ux", "other"]
    message: str = Field(min_length=8, max_length=2000)
    app_version: str | None = Field(default=None, max_length=64)
    platform: str | None = Field(default=None, max_length=32)

    @field_validator("device_id")
    @classmethod
    def _normalize_device_id(cls, value: str) -> str:
        out = (value or "").strip()
        if len(out) < 6:
            raise ValueError("device_id must be at least 6 characters")
        return out

    @field_validator("category")
    @classmethod
    def _normalize_category(cls, value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized not in _FEEDBACK_CATEGORIES:
            raise ValueError("Invalid feedback category")
        return normalized

    @field_validator("message")
    @classmethod
    def _normalize_message(cls, value: str) -> str:
        out = (value or "").strip()
        if len(out) < 8:
            raise ValueError("message must be at least 8 characters")
        return out

    @field_validator("app_version", "platform")
    @classmethod
    def _normalize_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        out = value.strip()
        return out or None


class FeedbackSubmitResponse(BaseModel):
    id: str
    status: Literal["received"]
    created_at: datetime


class FeedbackEntryResponse(BaseModel):
    id: str
    device_id: str
    category: str
    message: str
    app_version: str | None = None
    platform: str | None = None
    status: str
    created_at: datetime


class FeedbackListResponse(BaseModel):
    entries: list[FeedbackEntryResponse]
    count: int
