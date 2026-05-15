"""Google Drive transcript fetcher.

Downloads a transcript file (plain text or .docx) from Drive and returns
its content as a plain-text string. Supports both file types that Google
Meet may produce.
"""

from __future__ import annotations

import io
import logging
import time

from docx import Document
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from src.auth import get_google_credentials
from src.resilience import retry

logger = logging.getLogger(__name__)

_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"


class TranscriptFetcher:
    def __init__(self) -> None:
        creds = get_google_credentials()
        self._service = build("drive", "v3", credentials=creds)

    def fetch(self, file_id: str, mime_type: str) -> str:
        """Download a transcript file and return its plain-text content.

        Args:
            file_id: Drive file ID.
            mime_type: MIME type of the file (determines parser).

        Returns:
            Plain-text transcript string.

        Raises:
            googleapiclient.errors.HttpError: On Drive API failure.
            ValueError: If the MIME type is unsupported.
        """
        start = time.monotonic()
        logger.info(
            "Fetching transcript from Drive",
            extra={"file_id": file_id, "mime_type": mime_type},
        )

        raw_bytes = self._download_bytes(file_id)
        elapsed = time.monotonic() - start

        logger.info(
            "Transcript downloaded",
            extra={
                "file_id": file_id,
                "size_bytes": len(raw_bytes),
                "elapsed_seconds": round(elapsed, 2),
            },
        )

        if mime_type == "text/plain":
            return raw_bytes.decode("utf-8", errors="replace")
        elif mime_type == _DOCX_MIME:
            return self._extract_docx_text(raw_bytes)
        else:
            raise ValueError(f"Unsupported transcript MIME type: {mime_type}")

    # ── private helpers ───────────────────────────────────────────────────────

    @retry(max_attempts=3, base_delay=2.0, exceptions=(HttpError,))
    def _download_bytes(self, file_id: str) -> bytes:
        request = self._service.files().get_media(fileId=file_id)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        return buffer.getvalue()

    @staticmethod
    def _extract_docx_text(raw_bytes: bytes) -> str:
        doc = Document(io.BytesIO(raw_bytes))
        paragraphs = [para.text for para in doc.paragraphs if para.text.strip()]
        return "\n".join(paragraphs)
