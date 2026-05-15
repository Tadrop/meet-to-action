"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.llm.schema import ActionItem, EmailDraft, MeetingAnalysis

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def sample_transcript() -> str:
    return (FIXTURES_DIR / "sample_transcript.txt").read_text(encoding="utf-8")


@pytest.fixture()
def valid_action_item() -> ActionItem:
    return ActionItem(
        description="Draft creative brief for LinkedIn refresh",
        owner="Jordan Lee (jordan.lee@meridian.com)",
        deadline="2026-05-30",
        supporting_quote=(
            "Sure, I'll have a brief drafted and circulated for feedback by May 30th."
        ),
    )


@pytest.fixture()
def valid_analysis(valid_action_item: ActionItem) -> MeetingAnalysis:
    return MeetingAnalysis(
        meeting_title="Q2 Marketing Strategy Review",
        date="2026-05-15",
        summary=(
            "The team reviewed Q1 campaign performance and identified LinkedIn underperformance "
            "as the primary cost driver. Key decisions were made on Q3 budget allocation, "
            "landing page refresh, and nurture sequence template."
        ),
        decisions=[
            "Q3 digital budget approved at $65,000 with LinkedIn reduced to 15%.",
            "White paper drip structure adopted as Q3 nurture template.",
        ],
        action_items=[valid_action_item],
        follow_up_email=EmailDraft(
            subject="Q2 Marketing Strategy Review — Summary & Next Steps",
            body=(
                "Hi team,\n\nThank you for a productive session today.\n\n"
                "Please review the action items and confirm your deadlines.\n\n"
                "Best,\nAlex"
            ),
        ),
    )
