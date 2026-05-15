"""Gmail output — creates a DRAFT email. Never sends automatically."""

from __future__ import annotations

import base64
import logging
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.auth import get_google_credentials
from src.llm.schema import EmailDraft
from src.resilience import retry

logger = logging.getLogger(__name__)


class GmailDraftCreator:
    def __init__(self) -> None:
        creds = get_google_credentials()
        self._service = build("gmail", "v1", credentials=creds)

    @retry(max_attempts=3, base_delay=2.0, exceptions=(HttpError,))
    def create_draft(self, draft: EmailDraft, to_addresses: list[str]) -> str:
        """Create a Gmail DRAFT and return the draft ID.

        The draft is saved to the authenticated user's Drafts folder.
        It is NEVER sent — a human must review and send it manually.

        Args:
            draft: Subject and body from MeetingAnalysis.
            to_addresses: List of recipient email addresses.

        Returns:
            Gmail draft ID string.

        Raises:
            googleapiclient.errors.HttpError: On Gmail API failure.
        """
        message = self._build_mime(draft, to_addresses)
        raw = base64.urlsafe_b64encode(message.as_bytes()).decode("utf-8")

        start = time.monotonic()
        logger.info(
            "Creating Gmail draft",
            extra={"subject": draft.subject, "recipient_count": len(to_addresses)},
        )

        try:
            result = (
                self._service.users()
                .drafts()
                .create(userId="me", body={"message": {"raw": raw}})
                .execute()
            )
        except HttpError as exc:
            logger.error(
                "Gmail API error while creating draft",
                extra={"status": exc.status_code, "error": str(exc)},
            )
            raise

        draft_id: str = result["id"]
        elapsed = time.monotonic() - start
        logger.info(
            "Gmail draft created",
            extra={"draft_id": draft_id, "elapsed_seconds": round(elapsed, 2)},
        )
        return draft_id

    # ── private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _build_mime(draft: EmailDraft, to_addresses: list[str]) -> MIMEMultipart:
        message = MIMEMultipart("alternative")
        message["Subject"] = draft.subject
        message["To"] = ", ".join(to_addresses)
        message.attach(MIMEText(draft.body, "plain", "utf-8"))
        return message
