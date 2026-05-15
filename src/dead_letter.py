"""Dead letter queue (DLQ) for failed transcript processing.

When a transcript fails (fetch error, Claude error, or total output failure),
it is added to the DLQ instead of being silently dropped. On each subsequent
pipeline run, the DLQ is checked and due items are retried.

Retry schedule uses exponential backoff starting at 1 hour:
    Attempt 1 fail → retry after  1h
    Attempt 2 fail → retry after  2h
    Attempt 3 fail → retry after  4h
    Attempt 4 fail → retry after  8h
    Attempt 5 fail → retry after 24h (capped)
    Attempt 6 fail → permanently failed (needs human intervention)
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_PATH = os.getenv("DEAD_LETTER_PATH", "dead_letter_queue.json")
_MAX_RETRIES = 6
_BASE_RETRY_HOURS = 1
_MAX_RETRY_HOURS = 24


@dataclass
class FailedTranscript:
    file_id: str
    file_name: str
    mime_type: str
    created_time: str
    failure_count: int = 0
    last_error: str = ""
    next_retry_at: str = ""  # ISO 8601
    permanently_failed: bool = False

    def schedule_next_retry(self) -> None:
        hours = _BASE_RETRY_HOURS * (2 ** (self.failure_count - 1))
        hours = min(hours, _MAX_RETRY_HOURS)
        self.next_retry_at = (
            datetime.now(timezone.utc) + timedelta(hours=hours)
        ).isoformat()

    def is_due(self) -> bool:
        if self.permanently_failed or not self.next_retry_at:
            return False
        try:
            return datetime.now(timezone.utc) >= datetime.fromisoformat(self.next_retry_at)
        except ValueError:
            return False

    def as_file_meta(self) -> dict:
        """Return a Drive-style file metadata dict for pipeline consumption."""
        return {
            "id": self.file_id,
            "name": self.file_name,
            "mimeType": self.mime_type,
            "createdTime": self.created_time,
        }


class DeadLetterQueue:
    """Persistent queue of transcripts that failed to process."""

    def __init__(self, path: str | None = None) -> None:
        self._path = Path(path or _DEFAULT_PATH)
        self._items: dict[str, FailedTranscript] = self._load()

    # ── public API ────────────────────────────────────────────────────────────

    def add_failure(self, file_meta: dict, error: str) -> None:
        """Record a failed transcript. Schedules the next retry automatically."""
        file_id: str = file_meta["id"]

        if file_id in self._items:
            item = self._items[file_id]
        else:
            item = FailedTranscript(
                file_id=file_id,
                file_name=file_meta.get("name", "unknown"),
                mime_type=file_meta.get("mimeType", "text/plain"),
                created_time=file_meta.get("createdTime", ""),
            )

        item.failure_count += 1
        item.last_error = error[:500]  # cap error string size

        if item.failure_count >= _MAX_RETRIES:
            item.permanently_failed = True
            logger.error(
                "Transcript permanently failed — manual intervention required",
                extra={
                    "file_id": file_id,
                    "file_name": item.file_name,
                    "total_attempts": item.failure_count,
                    "last_error": item.last_error,
                },
            )
        else:
            item.schedule_next_retry()
            logger.warning(
                "Transcript queued for retry",
                extra={
                    "file_id": file_id,
                    "file_name": item.file_name,
                    "attempt": item.failure_count,
                    "max_retries": _MAX_RETRIES,
                    "next_retry_at": item.next_retry_at,
                },
            )

        self._items[file_id] = item
        self._save()

    def due_items(self) -> list[FailedTranscript]:
        """Return items whose retry window has elapsed."""
        return [item for item in self._items.values() if item.is_due()]

    def remove(self, file_id: str) -> None:
        """Remove an item after successful retry."""
        if file_id in self._items:
            del self._items[file_id]
            self._save()
            logger.info("Removed from DLQ after successful retry", extra={"file_id": file_id})

    def summary(self) -> dict:
        pending = sum(1 for i in self._items.values() if not i.permanently_failed)
        permanent = sum(1 for i in self._items.values() if i.permanently_failed)
        due_now = len(self.due_items())
        return {
            "pending_retries": pending,
            "due_now": due_now,
            "permanently_failed": permanent,
        }

    def has_permanently_failed(self) -> bool:
        return any(i.permanently_failed for i in self._items.values())

    # ── private helpers ───────────────────────────────────────────────────────

    def _load(self) -> dict[str, FailedTranscript]:
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            return {k: FailedTranscript(**v) for k, v in data.items()}
        except Exception as exc:
            logger.warning(
                "Could not read DLQ file — starting with empty queue",
                extra={"path": str(self._path), "error": str(exc)},
            )
            return {}

    def _save(self) -> None:
        try:
            self._path.write_text(
                json.dumps(
                    {k: asdict(v) for k, v in self._items.items()},
                    indent=2,
                    default=str,
                ),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error(
                "Failed to persist DLQ file",
                extra={"path": str(self._path), "error": str(exc)},
            )
