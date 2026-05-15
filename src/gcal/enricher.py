"""Google Calendar enricher.

Looks up the Calendar event whose start time is closest to the transcript's
creation timestamp and returns metadata (title, attendees, description) for
injection into the Claude prompt.

Closest-match (rather than first-in-window) handling avoids picking the
wrong event when meetings happen back-to-back.

The package name is `gcal` — not `calendar` — to avoid shadowing the
Python standard-library `calendar` module.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from dateutil.parser import parse as parse_date
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from src.auth import get_google_credentials
from src.resilience import retry

logger = logging.getLogger(__name__)

_WINDOW_HOURS = 2


class MeetingContext:
    """Enriched context from a matched Calendar event."""

    def __init__(
        self,
        title: str,
        attendees: list[str],
        description: str,
        start_time: str,
    ) -> None:
        self.title = title
        self.attendees = attendees
        self.description = description
        self.start_time = start_time

    def __repr__(self) -> str:
        return (
            f"MeetingContext(title={self.title!r}, "
            f"attendees={self.attendees!r}, "
            f"start_time={self.start_time!r})"
        )


class CalendarEnricher:
    def __init__(self) -> None:
        creds = get_google_credentials()
        self._service = build("calendar", "v3", credentials=creds)

    def find_event(self, transcript_created_time: str) -> MeetingContext | None:
        """Search primary Calendar for the event closest to the transcript's
        creation time, within ±2 hours.
        """
        try:
            created_dt = parse_date(transcript_created_time)
        except (ValueError, TypeError):
            logger.warning(
                "Could not parse transcript creation time",
                extra={"raw": transcript_created_time},
            )
            return None

        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)

        try:
            events = self._list_events(created_dt)
        except HttpError as exc:
            logger.error(
                "Calendar API error",
                extra={"status": exc.status_code, "error": str(exc)},
            )
            return None

        if not events:
            logger.info(
                "No Calendar event found near transcript time",
                extra={"window_hours": _WINDOW_HOURS},
            )
            return None

        # Pick the event whose start time is closest to the transcript's creation.
        events.sort(key=lambda e: self._delta_to(e, created_dt))
        event = events[0]

        attendees = [a.get("email", "") for a in event.get("attendees", []) if a.get("email")]
        context = MeetingContext(
            title=event.get("summary", "Untitled Meeting"),
            attendees=attendees,
            description=event.get("description", ""),
            start_time=event.get("start", {}).get("dateTime", transcript_created_time),
        )
        logger.info(
            "Calendar event matched",
            extra={"title": context.title, "attendee_count": len(context.attendees)},
        )
        return context

    # ── private helpers ───────────────────────────────────────────────────────

    @retry(max_attempts=3, base_delay=2.0, exceptions=(HttpError,))
    def _list_events(self, created_dt: datetime) -> list[dict]:
        time_min = (created_dt - timedelta(hours=_WINDOW_HOURS)).isoformat()
        time_max = (created_dt + timedelta(hours=_WINDOW_HOURS)).isoformat()

        result = (
            self._service.events()
            .list(
                calendarId="primary",
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                maxResults=10,
            )
            .execute()
        )
        return result.get("items", [])

    @staticmethod
    def _delta_to(event: dict, target: datetime) -> timedelta:
        start_str = event.get("start", {}).get("dateTime")
        if not start_str:
            return timedelta.max
        try:
            start_dt = parse_date(start_str)
            if start_dt.tzinfo is None:
                start_dt = start_dt.replace(tzinfo=timezone.utc)
            return abs(start_dt - target)
        except (ValueError, TypeError):
            return timedelta.max
