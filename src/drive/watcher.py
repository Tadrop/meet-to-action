"""Google Drive watcher.

Polls the configured Meet Recordings folder for new transcript files
(.txt or .docx) and yields any that haven't been processed yet.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterator

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.auth import get_google_credentials
from src.idempotency import IdempotencyTracker

logger = logging.getLogger(__name__)

_FOLDER_NAME = os.getenv("DRIVE_TRANSCRIPTS_FOLDER", "Meet Recordings")
# Mime types produced by Google Meet for transcripts.
_TRANSCRIPT_MIMES = {
    "text/plain",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


class DriveWatcher:
    def __init__(self, tracker: IdempotencyTracker) -> None:
        self._tracker = tracker
        creds = get_google_credentials()
        self._service = build("drive", "v3", credentials=creds)

    def poll(self) -> Iterator[dict]:
        """Yield Drive file metadata dicts for unprocessed transcript files.

        Each yielded dict contains at minimum: id, name, mimeType, createdTime.
        """
        try:
            folder_id = self._find_folder_id(_FOLDER_NAME)
        except HttpError as exc:
            logger.error(
                "Drive API error while locating transcripts folder",
                extra={"folder": _FOLDER_NAME, "status": exc.status_code, "error": str(exc)},
            )
            return

        if not folder_id:
            logger.warning(
                "Transcripts folder not found in Drive",
                extra={"folder": _FOLDER_NAME},
            )
            return

        try:
            files = self._list_transcript_files(folder_id)
        except HttpError as exc:
            logger.error(
                "Drive API error while listing files",
                extra={"folder_id": folder_id, "status": exc.status_code, "error": str(exc)},
            )
            return

        logger.info(
            "Drive poll complete",
            extra={"total_files": len(files), "folder": _FOLDER_NAME},
        )

        for file_meta in files:
            file_id: str = file_meta["id"]
            if self._tracker.is_processed(file_id):
                logger.debug("Skipping already-processed file", extra={"file_id": file_id})
                continue
            logger.info(
                "New transcript detected",
                extra={"file_id": file_id, "file_name": file_meta.get("name")},
            )
            yield file_meta

    # ── private helpers ───────────────────────────────────────────────────────

    def _find_folder_id(self, folder_name: str) -> str | None:
        query = (
            f"name = '{folder_name}' "
            "and mimeType = 'application/vnd.google-apps.folder' "
            "and trashed = false"
        )
        result = self._service.files().list(q=query, fields="files(id, name)", pageSize=1).execute()
        files = result.get("files", [])
        return files[0]["id"] if files else None

    def _list_transcript_files(self, folder_id: str) -> list[dict]:
        mime_filter = " or ".join(f"mimeType = '{m}'" for m in _TRANSCRIPT_MIMES)
        query = f"'{folder_id}' in parents and ({mime_filter}) and trashed = false"

        all_files: list[dict] = []
        page_token: str | None = None

        while True:
            kwargs: dict = {
                "q": query,
                "fields": "nextPageToken, files(id, name, mimeType, createdTime)",
                "orderBy": "createdTime desc",
                "pageSize": 100,
            }
            if page_token:
                kwargs["pageToken"] = page_token

            response = self._service.files().list(**kwargs).execute()
            all_files.extend(response.get("files", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return all_files
