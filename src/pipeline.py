"""Main pipeline orchestrator with self-annealing capabilities.

Self-annealing behaviours integrated here:
  1. Pre-flight health checks — skip the cycle if critical services are down.
  2. Circuit breakers per output service — a failing Gmail/Asana/Notion doesn't
     cascade into other outputs or stall the run.
  3. Dead letter queue — failed transcripts (fetch or Claude errors) are queued
     for automatic retry on subsequent runs with exponential backoff.
  4. Retry with exponential backoff on all API calls (via @retry on each client).

Pipeline flow per transcript:
  Drive Watcher → Transcript Fetcher → Calendar Enricher →
  Claude Analysis → Gmail Draft + Asana Tasks + Notion Entry
"""

from __future__ import annotations

import logging

from src.dead_letter import DeadLetterQueue
from src.gcal.enricher import CalendarEnricher
from src.drive.fetcher import TranscriptFetcher
from src.drive.watcher import DriveWatcher
from src.health import HealthChecker, HealthReport
from src.idempotency import IdempotencyTracker
from src.llm.claude_client import ClaudeClient
from src.llm.schema import MeetingAnalysis
from src.outputs.asana import AsanaTaskCreator
from src.outputs.gmail import GmailDraftCreator
from src.outputs.notion import NotionMeetingLogger
from src.resilience import CircuitBreaker, CircuitOpenError

logger = logging.getLogger(__name__)


class PipelineResult:
    def __init__(
        self,
        file_id: str,
        file_name: str,
        analysis: MeetingAnalysis,
        gmail_draft_id: str | None,
        asana_task_gids: list[str],
        notion_page_id: str | None,
    ) -> None:
        self.file_id = file_id
        self.file_name = file_name
        self.analysis = analysis
        self.gmail_draft_id = gmail_draft_id
        self.asana_task_gids = asana_task_gids
        self.notion_page_id = notion_page_id

    def __repr__(self) -> str:
        return (
            f"PipelineResult(file={self.file_name!r}, "
            f"actions={len(self.analysis.action_items)}, "
            f"gmail={self.gmail_draft_id!r})"
        )


class MeetingPipeline:
    """Orchestrates the end-to-end meeting analysis pipeline."""

    def __init__(self) -> None:
        self._tracker = IdempotencyTracker()
        self._dlq = DeadLetterQueue()
        self._health = HealthChecker()

        self._watcher = DriveWatcher(tracker=self._tracker)
        self._fetcher = TranscriptFetcher()
        self._enricher = CalendarEnricher()
        self._claude = ClaudeClient()
        self._gmail = GmailDraftCreator()
        self._asana = AsanaTaskCreator()
        self._notion = NotionMeetingLogger()

        # One circuit breaker per output service — failures are isolated.
        self._cb_gmail = CircuitBreaker(name="gmail", failure_threshold=3, recovery_timeout=300)
        self._cb_asana = CircuitBreaker(name="asana", failure_threshold=3, recovery_timeout=300)
        self._cb_notion = CircuitBreaker(name="notion", failure_threshold=3, recovery_timeout=300)

    def run_once(self) -> list[PipelineResult]:
        """Poll Drive for new transcripts and process each one.

        Steps:
            1. Run pre-flight health checks.
            2. Skip the cycle if critical services (Drive, Anthropic) are down.
            3. Retry any DLQ items that are now due.
            4. Poll Drive for new transcripts and process each.

        Returns a list of PipelineResult for every successfully processed transcript.
        """
        # ── 1. Health checks ──────────────────────────────────────────────────
        report: HealthReport = self._health.run()

        if not report.critical_healthy:
            logger.error(
                "Critical services unavailable — skipping pipeline cycle",
                extra={"unhealthy": report.unhealthy_services()},
            )
            return []

        results: list[PipelineResult] = []

        # ── 2. DLQ retries ────────────────────────────────────────────────────
        due = self._dlq.due_items()
        if due:
            logger.info("Retrying DLQ items", extra={"count": len(due)})
            for item in due:
                result = self._process_transcript(item.as_file_meta(), from_dlq=True)
                if result is not None:
                    self._dlq.remove(item.file_id)
                    results.append(result)

        # Log if any items are permanently stuck.
        if self._dlq.has_permanently_failed():
            summary = self._dlq.summary()
            logger.error(
                "Some transcripts have permanently failed — manual review needed",
                extra=summary,
            )

        # ── 3. New transcripts ────────────────────────────────────────────────
        for file_meta in self._watcher.poll():
            result = self._process_transcript(file_meta, from_dlq=False)
            if result is not None:
                results.append(result)

        logger.info("Pipeline run complete", extra={"processed": len(results)})
        return results

    # ── private: per-transcript logic ─────────────────────────────────────────

    def _process_transcript(
        self, file_meta: dict, *, from_dlq: bool
    ) -> PipelineResult | None:
        file_id: str = file_meta["id"]
        file_name: str = file_meta.get("name", "unknown")
        mime_type: str = file_meta.get("mimeType", "text/plain")
        created_time: str = file_meta.get("createdTime", "")

        logger.info(
            "Processing transcript",
            extra={
                "file_id": file_id,
                "file_name": file_name,
                "from_dlq": from_dlq,
            },
        )

        # 1 — Fetch transcript text
        try:
            transcript = self._fetcher.fetch(file_id=file_id, mime_type=mime_type)
        except Exception as exc:
            logger.error(
                "Transcript fetch failed",
                extra={"file_id": file_id, "error": str(exc)},
            )
            # Always re-add to DLQ — its internal state increments the count
            # and escalates to permanently_failed once the cap is hit.
            self._dlq.add_failure(file_meta, str(exc))
            return None

        # 2 — Enrich with Calendar context (non-critical; failures are tolerated)
        meeting_context = self._enricher.find_event(created_time)
        meeting_title = meeting_context.title if meeting_context else file_name
        attendees = meeting_context.attendees if meeting_context else []
        calendar_description = meeting_context.description if meeting_context else None

        # 3 — Analyse with Claude
        try:
            analysis = self._claude.analyse_transcript(
                transcript=transcript,
                meeting_title=meeting_title,
                attendees=attendees,
                calendar_description=calendar_description,
            )
        except Exception as exc:
            logger.error(
                "Claude analysis failed",
                extra={"file_id": file_id, "error": str(exc)},
            )
            self._dlq.add_failure(file_meta, str(exc))
            return None

        # 4 — Distribute outputs (each isolated by a circuit breaker)
        gmail_draft_id = self._create_gmail_draft(analysis, attendees)
        asana_task_gids = self._create_asana_tasks(analysis)
        notion_page_id = self._create_notion_entry(analysis)

        # 5 — If every output failed, the consultant gets nothing — re-queue rather
        # than mark processed, so the next cycle retries when circuits recover.
        any_output_succeeded = (
            gmail_draft_id is not None
            or len(asana_task_gids) > 0
            or notion_page_id is not None
        )
        if not any_output_succeeded:
            logger.error(
                "All outputs failed for transcript — re-queueing for retry",
                extra={"file_id": file_id, "file_name": file_name},
            )
            self._dlq.add_failure(
                file_meta,
                "All outputs failed (Gmail, Asana, Notion all unreachable)",
            )
            return None

        self._tracker.mark_processed(file_id)

        result = PipelineResult(
            file_id=file_id,
            file_name=file_name,
            analysis=analysis,
            gmail_draft_id=gmail_draft_id,
            asana_task_gids=asana_task_gids,
            notion_page_id=notion_page_id,
        )

        logger.info(
            "Transcript processed successfully",
            extra={
                "file_id": file_id,
                "action_items": len(analysis.action_items),
                "gmail_draft_id": gmail_draft_id,
                "asana_tasks": len(asana_task_gids),
                "notion_page_id": notion_page_id,
            },
        )
        return result

    # ── output helpers (circuit-breaker-wrapped) ──────────────────────────────

    def _create_gmail_draft(
        self, analysis: MeetingAnalysis, attendees: list[str]
    ) -> str | None:
        try:
            return self._cb_gmail.call(
                self._gmail.create_draft,
                analysis.follow_up_email,
                attendees,
            )  # type: ignore[return-value]
        except CircuitOpenError as exc:
            logger.warning("Gmail circuit open — skipping draft", extra={"reason": str(exc)})
            return None
        except Exception as exc:
            logger.error("Gmail draft creation failed", extra={"error": str(exc)})
            return None

    def _create_asana_tasks(self, analysis: MeetingAnalysis) -> list[str]:
        try:
            return self._cb_asana.call(
                self._asana.create_tasks,
                analysis.action_items,
                analysis.meeting_title,
            ) or []  # type: ignore[return-value]
        except CircuitOpenError as exc:
            logger.warning("Asana circuit open — skipping tasks", extra={"reason": str(exc)})
            return []
        except Exception as exc:
            logger.error("Asana task creation failed", extra={"error": str(exc)})
            return []

    def _create_notion_entry(self, analysis: MeetingAnalysis) -> str | None:
        try:
            return self._cb_notion.call(
                self._notion.log_meeting,
                analysis,
            )  # type: ignore[return-value]
        except CircuitOpenError as exc:
            logger.warning("Notion circuit open — skipping entry", extra={"reason": str(exc)})
            return None
        except Exception as exc:
            logger.error("Notion entry creation failed", extra={"error": str(exc)})
            return None
