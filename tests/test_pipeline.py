"""Integration tests for the pipeline orchestrator with mocked external clients."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.health import HealthReport, ServiceStatus
from src.llm.schema import MeetingAnalysis
from src.pipeline import MeetingPipeline, PipelineResult


def _healthy_report() -> HealthReport:
    return HealthReport(
        statuses=[
            ServiceStatus(name="drive", healthy=True, latency_ms=10),
            ServiceStatus(name="anthropic", healthy=True, latency_ms=10),
            ServiceStatus(name="calendar", healthy=True, latency_ms=10),
            ServiceStatus(name="gmail", healthy=True, latency_ms=10),
            ServiceStatus(name="asana", healthy=True, latency_ms=10),
            ServiceStatus(name="notion", healthy=True, latency_ms=10),
        ]
    )


@pytest.fixture()
def mock_pipeline(valid_analysis: MeetingAnalysis):
    """Return a MeetingPipeline with all external dependencies mocked."""
    with (
        patch("src.pipeline.IdempotencyTracker") as mock_tracker_cls,
        patch("src.pipeline.DeadLetterQueue") as mock_dlq_cls,
        patch("src.pipeline.HealthChecker") as mock_health_cls,
        patch("src.pipeline.DriveWatcher") as mock_watcher_cls,
        patch("src.pipeline.TranscriptFetcher") as mock_fetcher_cls,
        patch("src.pipeline.CalendarEnricher") as mock_enricher_cls,
        patch("src.pipeline.ClaudeClient") as mock_claude_cls,
        patch("src.pipeline.GmailDraftCreator") as mock_gmail_cls,
        patch("src.pipeline.AsanaTaskCreator") as mock_asana_cls,
        patch("src.pipeline.NotionMeetingLogger") as mock_notion_cls,
    ):
        pipeline = MeetingPipeline()

        tracker = mock_tracker_cls.return_value
        tracker.is_processed.return_value = False

        dlq = mock_dlq_cls.return_value
        dlq.due_items.return_value = []
        dlq.has_permanently_failed.return_value = False

        health = mock_health_cls.return_value
        health.run.return_value = _healthy_report()

        watcher = mock_watcher_cls.return_value
        watcher.poll.return_value = iter([
            {
                "id": "file-abc-123",
                "name": "Q2 Marketing Strategy Review 2026-05-15.txt",
                "mimeType": "text/plain",
                "createdTime": "2026-05-15T10:00:00Z",
            }
        ])

        fetcher = mock_fetcher_cls.return_value
        fetcher.fetch.return_value = "Transcript content here."

        enricher = mock_enricher_cls.return_value
        enricher.find_event.return_value = None

        claude = mock_claude_cls.return_value
        claude.analyse_transcript.return_value = valid_analysis

        gmail = mock_gmail_cls.return_value
        gmail.create_draft.return_value = "draft-id-xyz"

        asana = mock_asana_cls.return_value
        asana.create_tasks.return_value = ["task-gid-1"]

        notion = mock_notion_cls.return_value
        notion.log_meeting.return_value = "page-id-abc"

        pipeline._tracker = tracker
        pipeline._dlq = dlq
        pipeline._health = health
        pipeline._watcher = watcher
        pipeline._fetcher = fetcher
        pipeline._enricher = enricher
        pipeline._claude = claude
        pipeline._gmail = gmail
        pipeline._asana = asana
        pipeline._notion = notion

        yield pipeline


class TestMeetingPipeline:
    def test_run_once_returns_results(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        results = mock_pipeline.run_once()
        assert len(results) == 1
        result = results[0]
        assert isinstance(result, PipelineResult)
        assert result.file_id == "file-abc-123"
        assert result.gmail_draft_id == "draft-id-xyz"
        assert result.asana_task_gids == ["task-gid-1"]
        assert result.notion_page_id == "page-id-abc"

    def test_transcript_marked_processed_after_success(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        mock_pipeline.run_once()
        mock_pipeline._tracker.mark_processed.assert_called_once_with("file-abc-123")

    def test_claude_failure_adds_to_dlq(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        mock_pipeline._claude.analyse_transcript.side_effect = ValueError("Parse error")
        results = mock_pipeline.run_once()
        assert results == []
        mock_pipeline._dlq.add_failure.assert_called_once()
        mock_pipeline._tracker.mark_processed.assert_not_called()

    def test_fetch_failure_adds_to_dlq(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        mock_pipeline._fetcher.fetch.side_effect = Exception("Drive 503")
        results = mock_pipeline.run_once()
        assert results == []
        mock_pipeline._dlq.add_failure.assert_called_once()

    def test_all_outputs_failed_does_not_mark_processed(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        mock_pipeline._gmail.create_draft.side_effect = Exception("gmail down")
        mock_pipeline._asana.create_tasks.side_effect = Exception("asana down")
        mock_pipeline._notion.log_meeting.side_effect = Exception("notion down")
        results = mock_pipeline.run_once()
        assert results == []
        mock_pipeline._tracker.mark_processed.assert_not_called()
        mock_pipeline._dlq.add_failure.assert_called_once()

    def test_partial_output_failure_still_marks_processed(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        mock_pipeline._gmail.create_draft.side_effect = Exception("gmail down")
        results = mock_pipeline.run_once()
        assert len(results) == 1
        assert results[0].gmail_draft_id is None
        assert results[0].asana_task_gids == ["task-gid-1"]
        assert results[0].notion_page_id == "page-id-abc"
        mock_pipeline._tracker.mark_processed.assert_called_once()

    def test_critical_service_down_skips_cycle(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        unhealthy = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=False, latency_ms=0, error="503"),
                ServiceStatus(name="anthropic", healthy=True, latency_ms=10),
            ]
        )
        mock_pipeline._health.run.return_value = unhealthy
        results = mock_pipeline.run_once()
        assert results == []
        mock_pipeline._watcher.poll.assert_not_called()
        mock_pipeline._claude.analyse_transcript.assert_not_called()

    def test_dlq_retry_processed_and_removed(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        from src.dead_letter import FailedTranscript

        dlq_item = FailedTranscript(
            file_id="file-retry-1",
            file_name="retry.txt",
            mime_type="text/plain",
            created_time="2026-05-15T09:00:00Z",
            failure_count=1,
            next_retry_at="2026-05-15T10:00:00Z",
        )
        mock_pipeline._dlq.due_items.return_value = [dlq_item]
        mock_pipeline._watcher.poll.return_value = iter([])

        results = mock_pipeline.run_once()
        assert len(results) == 1
        assert results[0].file_id == "file-retry-1"
        mock_pipeline._dlq.remove.assert_called_once_with("file-retry-1")

    def test_empty_poll_returns_empty_list(
        self, mock_pipeline: MeetingPipeline
    ) -> None:
        mock_pipeline._watcher.poll.return_value = iter([])
        results = mock_pipeline.run_once()
        assert results == []
