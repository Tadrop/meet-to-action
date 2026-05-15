"""Pydantic models for Claude's structured output.

Every action item is required to have a non-empty owner, deadline, and
supporting_quote (verbatim text from the transcript). Validation fails
fast rather than silently allowing incomplete records.
"""

from __future__ import annotations

from pydantic import BaseModel, field_validator


class ActionItem(BaseModel):
    description: str
    owner: str
    deadline: str  # ISO 8601 date string, e.g. "2026-05-30"
    supporting_quote: str  # Verbatim excerpt from the transcript

    @field_validator("owner", "deadline", "supporting_quote", "description")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("field must not be empty")
        return v.strip()


class EmailDraft(BaseModel):
    subject: str
    body: str

    @field_validator("subject", "body")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("field must not be empty")
        return v.strip()


class MeetingAnalysis(BaseModel):
    meeting_title: str
    date: str  # ISO 8601 date
    summary: str
    decisions: list[str]
    action_items: list[ActionItem]
    follow_up_email: EmailDraft

    @field_validator("meeting_title", "date", "summary")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("field must not be empty")
        return v.strip()
