"""Tests for Pydantic schema validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.llm.schema import ActionItem, EmailDraft, MeetingAnalysis


class TestActionItem:
    def test_valid_action_item(self) -> None:
        item = ActionItem(
            description="Send project brief",
            owner="Alex Chen (alex.chen@meridian.com)",
            deadline="2026-05-30",
            supporting_quote="I'll send the brief by end of May.",
        )
        assert item.deadline == "2026-05-30"

    @pytest.mark.parametrize("field", ["description", "owner", "deadline", "supporting_quote"])
    def test_empty_field_raises(self, field: str) -> None:
        data = {
            "description": "Do something",
            "owner": "Alice",
            "deadline": "2026-06-01",
            "supporting_quote": "Verbatim quote here.",
        }
        data[field] = ""
        with pytest.raises(ValidationError):
            ActionItem(**data)

    @pytest.mark.parametrize("field", ["description", "owner", "deadline", "supporting_quote"])
    def test_whitespace_only_field_raises(self, field: str) -> None:
        data = {
            "description": "Do something",
            "owner": "Alice",
            "deadline": "2026-06-01",
            "supporting_quote": "Verbatim quote here.",
        }
        data[field] = "   "
        with pytest.raises(ValidationError):
            ActionItem(**data)

    def test_strips_surrounding_whitespace(self) -> None:
        item = ActionItem(
            description="  Do something  ",
            owner="  Alice  ",
            deadline="  2026-06-01  ",
            supporting_quote="  A verbatim quote.  ",
        )
        assert item.description == "Do something"
        assert item.owner == "Alice"
        assert item.deadline == "2026-06-01"
        assert item.supporting_quote == "A verbatim quote."


class TestEmailDraft:
    def test_valid_email_draft(self) -> None:
        draft = EmailDraft(subject="Meeting Summary", body="Hello,\n\nPlease review.")
        assert draft.subject == "Meeting Summary"

    def test_empty_subject_raises(self) -> None:
        with pytest.raises(ValidationError):
            EmailDraft(subject="", body="Body text")

    def test_empty_body_raises(self) -> None:
        with pytest.raises(ValidationError):
            EmailDraft(subject="Subject", body="")


class TestMeetingAnalysis:
    def test_valid_analysis(self, valid_analysis: MeetingAnalysis) -> None:
        assert valid_analysis.meeting_title == "Q2 Marketing Strategy Review"
        assert len(valid_analysis.action_items) == 1
        assert valid_analysis.action_items[0].owner == "Jordan Lee (jordan.lee@meridian.com)"

    def test_empty_meeting_title_raises(self, valid_analysis: MeetingAnalysis) -> None:
        data = valid_analysis.model_dump()
        data["meeting_title"] = ""
        with pytest.raises(ValidationError):
            MeetingAnalysis(**data)

    def test_missing_action_item_fields_raises(self) -> None:
        with pytest.raises(ValidationError):
            MeetingAnalysis(
                meeting_title="Test Meeting",
                date="2026-05-15",
                summary="A summary.",
                decisions=["Decision one"],
                action_items=[
                    {
                        "description": "Do something",
                        "owner": "Alice",
                        "deadline": "2026-06-01",
                        # supporting_quote deliberately missing
                    }
                ],
                follow_up_email={"subject": "Subject", "body": "Body"},
            )

    def test_model_dump_round_trip(self, valid_analysis: MeetingAnalysis) -> None:
        dumped = valid_analysis.model_dump()
        restored = MeetingAnalysis(**dumped)
        assert restored.meeting_title == valid_analysis.meeting_title
        assert len(restored.action_items) == len(valid_analysis.action_items)
