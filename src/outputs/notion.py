"""Notion output — creates a meeting entry in the Notion meetings database.

The entry is created in a standard state (not a separate draft mechanism —
Notion databases don't have a native draft concept). The human workflow is
to review the Notion page before sharing it.
"""

from __future__ import annotations

import logging
import os
import time

from notion_client import Client
from notion_client.errors import APIResponseError

from src.llm.schema import MeetingAnalysis
from src.resilience import retry

logger = logging.getLogger(__name__)


class NotionMeetingLogger:
    def __init__(self) -> None:
        token = os.getenv("NOTION_TOKEN")
        if not token:
            raise OSError("NOTION_TOKEN environment variable is not set")

        self._database_id = os.getenv("NOTION_DATABASE_ID", "")
        if not self._database_id:
            raise OSError("NOTION_DATABASE_ID environment variable is not set")

        self._client = Client(auth=token)

    @retry(max_attempts=3, base_delay=2.0, exceptions=(APIResponseError,))
    def log_meeting(self, analysis: MeetingAnalysis) -> str:
        """Create a Notion database page for the meeting and return its page ID.

        Schema expected in the Notion database:
            - Title (title): Meeting title
            - Date (date): Meeting date
            - Summary (rich_text): Executive summary
            - Decisions (rich_text): Decisions as a bulleted list
            - Action Items (rich_text): Formatted action items with owners/deadlines
            - Status (select): "Pending Review"

        Returns:
            Notion page ID string.

        Raises:
            notion_client.errors.APIResponseError: On Notion API failure.
        """
        start = time.monotonic()
        logger.info(
            "Creating Notion meeting entry",
            extra={"meeting_title": analysis.meeting_title, "date": analysis.date},
        )

        decisions_text = "\n".join(f"• {d}" for d in analysis.decisions)
        action_items_text = self._format_action_items(analysis)

        # Notion rich_text blocks are capped at 2000 chars each. Warn rather
        # than silently truncate so the consultant knows to check the entry.
        def _safe_text(value: str, field_name: str) -> str:
            if len(value) > 2000:
                logger.warning(
                    "Notion field truncated to 2000 chars",
                    extra={"field": field_name, "original_length": len(value)},
                )
            return value[:2000]

        properties = {
            "Title": {"title": [{"text": {"content": analysis.meeting_title}}]},
            "Date": {"date": {"start": analysis.date}},
            "Summary": {
                "rich_text": [{"text": {"content": _safe_text(analysis.summary, "Summary")}}]
            },
            "Decisions": {
                "rich_text": [{"text": {"content": _safe_text(decisions_text, "Decisions")}}]
            },
            "Action Items": {
                "rich_text": [{"text": {"content": _safe_text(action_items_text, "Action Items")}}]
            },
            "Status": {"select": {"name": "Pending Review"}},
        }

        try:
            page = self._client.pages.create(
                parent={"database_id": self._database_id},
                properties=properties,
            )
        except APIResponseError as exc:
            logger.error(
                "Notion API error",
                extra={"status": exc.status, "error": str(exc)},
            )
            raise

        page_id: str = page.get("id", "")
        if not page_id:
            raise ValueError("Notion response missing page ID")
        elapsed = time.monotonic() - start
        logger.info(
            "Notion meeting entry created",
            extra={"page_id": page_id, "elapsed_seconds": round(elapsed, 2)},
        )
        return page_id

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _format_action_items(analysis: MeetingAnalysis) -> str:
        lines: list[str] = []
        for i, item in enumerate(analysis.action_items, start=1):
            lines.append(
                f"{i}. {item.description}\n"
                f"   Owner: {item.owner}\n"
                f"   Due: {item.deadline}\n"
                f'   Quote: "{item.supporting_quote}"'
            )
        return "\n\n".join(lines)
