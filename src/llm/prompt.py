"""Claude prompt and tool definition for meeting analysis."""

from __future__ import annotations

from typing import Any

SYSTEM_PROMPT = """You are an expert meeting analyst for a professional consulting firm.

Your job is to read a meeting transcript and produce:
1. A concise executive summary (3-5 sentences)
2. A list of decisions made during the meeting
3. Action items — each must have an owner (name + email if available), a
   specific deadline (exact date, not "next week"), and a verbatim supporting
   quote from the transcript that proves the commitment was made
4. A polished follow-up email draft addressed to all attendees

Strict requirements:
- Every action item MUST have: owner, deadline (ISO 8601 date), supporting_quote
- supporting_quote must be copied VERBATIM from the transcript — do not paraphrase
- Deadlines must be specific dates. If the transcript says "end of May", convert
  to the last working day of May (e.g. 2026-05-29). Use the meeting date for context.
- The follow-up email must open with a brief thank-you, summarise decisions, list
  action items with owners and deadlines, and close professionally.
- Tone: professional, concise, suitable for senior client communication.
- Never invent information that is not in the transcript."""

# Tool definition that forces Claude to return structured JSON matching MeetingAnalysis.
ANALYSIS_TOOL: dict[str, Any] = {
    "name": "record_meeting_analysis",
    "description": (
        "Record the complete structured analysis of the meeting transcript. "
        "Call this tool once with all extracted information."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "meeting_title": {
                "type": "string",
                "description": "Title or subject of the meeting",
            },
            "date": {
                "type": "string",
                "description": "Meeting date in ISO 8601 format (YYYY-MM-DD)",
            },
            "summary": {
                "type": "string",
                "description": "Executive summary of the meeting in 3-5 sentences",
            },
            "decisions": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of explicit decisions made during the meeting",
            },
            "action_items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "description": {
                            "type": "string",
                            "description": "Clear description of what needs to be done",
                        },
                        "owner": {
                            "type": "string",
                            "description": "Full name and email of the responsible person",
                        },
                        "deadline": {
                            "type": "string",
                            "description": "Due date in ISO 8601 format (YYYY-MM-DD)",
                        },
                        "supporting_quote": {
                            "type": "string",
                            "description": "Verbatim quote from the transcript confirming this action",
                        },
                    },
                    "required": ["description", "owner", "deadline", "supporting_quote"],
                },
                "description": "All action items extracted from the meeting",
            },
            "follow_up_email": {
                "type": "object",
                "properties": {
                    "subject": {
                        "type": "string",
                        "description": "Email subject line",
                    },
                    "body": {
                        "type": "string",
                        "description": "Full email body in plain text",
                    },
                },
                "required": ["subject", "body"],
                "description": "Draft follow-up email to all attendees",
            },
        },
        "required": [
            "meeting_title",
            "date",
            "summary",
            "decisions",
            "action_items",
            "follow_up_email",
        ],
    },
}


def build_user_prompt(
    transcript: str,
    meeting_title: str | None = None,
    attendees: list[str] | None = None,
    calendar_description: str | None = None,
) -> str:
    """Assemble the user-turn prompt injected alongside the transcript."""
    context_lines: list[str] = []

    if meeting_title:
        context_lines.append(f"Meeting title: {meeting_title}")
    if attendees:
        context_lines.append(f"Attendees: {', '.join(attendees)}")
    if calendar_description:
        context_lines.append(f"Calendar description: {calendar_description}")

    context_block = "\n".join(context_lines)
    context_section = f"<context>\n{context_block}\n</context>\n\n" if context_lines else ""

    return (
        f"{context_section}"
        "<transcript>\n"
        f"{transcript}\n"
        "</transcript>\n\n"
        "Analyse the transcript above and call `record_meeting_analysis` with the "
        "complete structured output. Remember: every action item needs an exact "
        "verbatim supporting_quote from the transcript."
    )
