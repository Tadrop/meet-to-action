"""File-based idempotency tracker.

Persists a set of processed transcript file IDs to a JSON file so the
pipeline never processes the same transcript twice, even across restarts.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_DEFAULT_PATH = os.getenv("PROCESSED_TRANSCRIPTS_PATH", "processed_transcripts.json")


class IdempotencyTracker:
    def __init__(self, path: str | None = None) -> None:
        self._path = Path(path or _DEFAULT_PATH)
        self._seen: set[str] = self._load()

    # ── public API ────────────────────────────────────────────────────────────

    def is_processed(self, file_id: str) -> bool:
        return file_id in self._seen

    def mark_processed(self, file_id: str) -> None:
        self._seen.add(file_id)
        self._save()
        logger.info("Marked transcript as processed", extra={"file_id": file_id})

    def count(self) -> int:
        return len(self._seen)

    # ── private helpers ───────────────────────────────────────────────────────

    def _load(self) -> set[str]:
        if not self._path.exists():
            return set()
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            return set(data.get("processed", []))
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "Could not read idempotency file — starting fresh",
                extra={"path": str(self._path), "error": str(exc)},
            )
            return set()

    def _save(self) -> None:
        try:
            self._path.write_text(
                json.dumps({"processed": sorted(self._seen)}, indent=2),
                encoding="utf-8",
            )
        except OSError as exc:
            logger.error(
                "Failed to persist idempotency file",
                extra={"path": str(self._path), "error": str(exc)},
            )
