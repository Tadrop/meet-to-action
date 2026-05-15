"""Tests for the dead letter queue."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.dead_letter import DeadLetterQueue, FailedTranscript


@pytest.fixture()
def dlq(tmp_path: Path) -> DeadLetterQueue:
    return DeadLetterQueue(path=str(tmp_path / "dlq.json"))


def _meta(file_id: str = "file-1") -> dict:
    return {
        "id": file_id,
        "name": "test.txt",
        "mimeType": "text/plain",
        "createdTime": "2026-05-15T10:00:00Z",
    }


class TestDeadLetterQueue:
    def test_first_failure_schedules_retry(self, dlq: DeadLetterQueue) -> None:
        dlq.add_failure(_meta(), "transient error")
        summary = dlq.summary()
        assert summary["pending_retries"] == 1
        assert summary["permanently_failed"] == 0

    def test_repeat_failure_increments_count(self, dlq: DeadLetterQueue) -> None:
        meta = _meta()
        dlq.add_failure(meta, "first")
        dlq.add_failure(meta, "second")
        assert len(dlq._items) == 1
        assert dlq._items["file-1"].failure_count == 2

    def test_max_retries_marks_permanently_failed(
        self, dlq: DeadLetterQueue
    ) -> None:
        meta = _meta()
        for _ in range(6):
            dlq.add_failure(meta, "still failing")
        assert dlq._items["file-1"].permanently_failed
        assert dlq.has_permanently_failed()

    def test_due_items_excludes_future_retries(
        self, dlq: DeadLetterQueue
    ) -> None:
        dlq.add_failure(_meta(), "error")
        # Just-added items have a future next_retry_at, so none are due.
        assert dlq.due_items() == []

    def test_due_items_includes_past_retries(self, dlq: DeadLetterQueue) -> None:
        meta = _meta()
        dlq.add_failure(meta, "error")
        # Backdate the retry time so the item becomes due.
        item = dlq._items["file-1"]
        item.next_retry_at = (
            datetime.now(timezone.utc) - timedelta(minutes=1)
        ).isoformat()
        due = dlq.due_items()
        assert len(due) == 1
        assert due[0].file_id == "file-1"

    def test_due_items_excludes_permanently_failed(
        self, dlq: DeadLetterQueue
    ) -> None:
        meta = _meta()
        for _ in range(6):
            dlq.add_failure(meta, "still failing")
        assert dlq.due_items() == []

    def test_remove_deletes_item(self, dlq: DeadLetterQueue) -> None:
        dlq.add_failure(_meta(), "error")
        assert "file-1" in dlq._items
        dlq.remove("file-1")
        assert "file-1" not in dlq._items

    def test_remove_nonexistent_is_safe(self, dlq: DeadLetterQueue) -> None:
        dlq.remove("nonexistent")  # Should not raise.

    def test_persistence_round_trip(self, tmp_path: Path) -> None:
        path = str(tmp_path / "dlq.json")

        dlq1 = DeadLetterQueue(path=path)
        dlq1.add_failure(_meta("file-1"), "error 1")
        dlq1.add_failure(_meta("file-2"), "error 2")

        dlq2 = DeadLetterQueue(path=path)
        assert "file-1" in dlq2._items
        assert "file-2" in dlq2._items
        assert dlq2._items["file-1"].last_error == "error 1"

    def test_corrupt_file_starts_fresh(self, tmp_path: Path) -> None:
        path = tmp_path / "dlq.json"
        path.write_text("not json", encoding="utf-8")
        dlq = DeadLetterQueue(path=str(path))
        assert dlq._items == {}

    def test_long_error_string_is_capped(self, dlq: DeadLetterQueue) -> None:
        dlq.add_failure(_meta(), "x" * 10_000)
        assert len(dlq._items["file-1"].last_error) == 500


class TestFailedTranscript:
    def test_retry_backoff_grows(self) -> None:
        ft = FailedTranscript(
            file_id="x", file_name="x", mime_type="text/plain", created_time=""
        )
        ft.failure_count = 1
        ft.schedule_next_retry()
        first = datetime.fromisoformat(ft.next_retry_at)

        ft.failure_count = 3
        ft.schedule_next_retry()
        third = datetime.fromisoformat(ft.next_retry_at)

        # Third retry is scheduled further in the future than the first.
        assert third > first

    def test_retry_capped_at_24h(self) -> None:
        ft = FailedTranscript(
            file_id="x", file_name="x", mime_type="text/plain", created_time=""
        )
        ft.failure_count = 100
        ft.schedule_next_retry()
        retry_at = datetime.fromisoformat(ft.next_retry_at)
        delta = retry_at - datetime.now(timezone.utc)
        # Should be capped at 24 hours, never exponentially exploding.
        assert delta < timedelta(hours=25)

    def test_as_file_meta_round_trip(self) -> None:
        ft = FailedTranscript(
            file_id="abc",
            file_name="meeting.txt",
            mime_type="text/plain",
            created_time="2026-05-15T10:00:00Z",
        )
        meta = ft.as_file_meta()
        assert meta["id"] == "abc"
        assert meta["name"] == "meeting.txt"
        assert meta["mimeType"] == "text/plain"
