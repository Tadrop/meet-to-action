"""APScheduler entrypoint.

Runs the MeetingPipeline every POLL_INTERVAL_MINUTES (default: 5).
Run directly:
    python -m src.scheduler.main
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv

# Load .env before any other imports that read env vars.
load_dotenv()

from src.logging_config import configure_logging  # noqa: E402 — must follow load_dotenv
from src.pipeline import MeetingPipeline  # noqa: E402

configure_logging()
logger = logging.getLogger(__name__)


def _parse_poll_interval() -> int:
    raw = os.getenv("POLL_INTERVAL_MINUTES", "5")
    try:
        value = int(raw)
        if value < 1:
            raise ValueError("must be >= 1")
        return value
    except ValueError as exc:
        logger.error(
            "Invalid POLL_INTERVAL_MINUTES — defaulting to 5",
            extra={"raw_value": raw, "error": str(exc)},
        )
        return 5


_POLL_INTERVAL = _parse_poll_interval()


def _run_pipeline(pipeline: MeetingPipeline) -> None:
    logger.info("Scheduled pipeline run starting")
    try:
        results = pipeline.run_once()
        logger.info(
            "Scheduled pipeline run finished",
            extra={"transcripts_processed": len(results)},
        )
    except Exception as exc:
        # Never crash the scheduler — log and wait for the next interval.
        logger.error(
            "Unhandled exception in pipeline run", extra={"error": str(exc)}, exc_info=True
        )


def main() -> None:
    logger.info(
        "Meeting Notes Agent starting",
        extra={"poll_interval_minutes": _POLL_INTERVAL},
    )

    pipeline = MeetingPipeline()

    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        func=_run_pipeline,
        trigger=IntervalTrigger(minutes=_POLL_INTERVAL),
        args=[pipeline],
        id="meeting_pipeline",
        name="Meeting Notes Pipeline",
        # Run once immediately at startup, then on interval.
        next_run_time=datetime.now(timezone.utc),
    )

    def _shutdown(signum: int, frame: object) -> None:
        logger.info("Shutdown signal received — stopping scheduler")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("Scheduler started — press Ctrl+C to stop")
    scheduler.start()


if __name__ == "__main__":
    main()
