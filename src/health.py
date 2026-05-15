"""Pre-flight health check suite.

Runs a lightweight probe against every external dependency before each
pipeline cycle. If a critical service is down, the run is skipped rather
than wasting API calls and producing partial output.

Critical services (pipeline cannot run without them):
    - Google Drive   (transcript source)
    - Anthropic API  (analysis engine)

Non-critical services (run continues; failed outputs go to the error log):
    - Google Calendar
    - Gmail
    - Asana
    - Notion

Usage:
    checker = HealthChecker()
    report = checker.run()
    if not report.critical_healthy:
        return  # skip this cycle
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field

import anthropic
import requests
from googleapiclient.discovery import build

from src.auth import get_google_credentials

logger = logging.getLogger(__name__)


@dataclass
class ServiceStatus:
    name: str
    healthy: bool
    latency_ms: float
    error: str = ""


@dataclass
class HealthReport:
    statuses: list[ServiceStatus] = field(default_factory=list)

    @property
    def all_healthy(self) -> bool:
        return all(s.healthy for s in self.statuses)

    @property
    def critical_healthy(self) -> bool:
        """Drive + Anthropic must be reachable for a pipeline run to make sense."""
        critical_names = {"drive", "anthropic"}
        critical = {s.name: s.healthy for s in self.statuses if s.name in critical_names}
        return all(critical.values()) and len(critical) == len(critical_names)

    def unhealthy_services(self) -> list[str]:
        return [s.name for s in self.statuses if not s.healthy]

    def log(self) -> None:
        for status in self.statuses:
            level = logging.INFO if status.healthy else logging.WARNING
            logger.log(
                level,
                "Health check result",
                extra={
                    "service": status.name,
                    "healthy": status.healthy,
                    "latency_ms": round(status.latency_ms),
                    **({"error": status.error} if status.error else {}),
                },
            )

        if not self.all_healthy:
            logger.warning(
                "One or more services are degraded",
                extra={"unhealthy": self.unhealthy_services()},
            )


class HealthChecker:
    """Runs lightweight probes against every external dependency."""

    def run(self) -> HealthReport:
        """Execute all probes and return a HealthReport."""
        probes = [
            ("drive", self._probe_drive),
            ("calendar", self._probe_calendar),
            ("gmail", self._probe_gmail),
            ("asana", self._probe_asana),
            ("notion", self._probe_notion),
            ("anthropic", self._probe_anthropic),
        ]

        statuses: list[ServiceStatus] = []
        for name, probe in probes:
            statuses.append(self._timed_check(name, probe))

        report = HealthReport(statuses=statuses)
        report.log()
        return report

    # ── probes ────────────────────────────────────────────────────────────────

    @staticmethod
    def _probe_drive() -> None:
        creds = get_google_credentials()
        service = build("drive", "v3", credentials=creds)
        service.files().list(pageSize=1, fields="files(id)").execute()

    @staticmethod
    def _probe_calendar() -> None:
        creds = get_google_credentials()
        service = build("calendar", "v3", credentials=creds)
        service.calendarList().list(maxResults=1).execute()

    @staticmethod
    def _probe_gmail() -> None:
        creds = get_google_credentials()
        service = build("gmail", "v1", credentials=creds)
        service.users().getProfile(userId="me").execute()

    @staticmethod
    def _probe_asana() -> None:
        token = os.getenv("ASANA_ACCESS_TOKEN", "")
        if not token:
            raise OSError("ASANA_ACCESS_TOKEN not set")
        resp = requests.get(
            "https://app.asana.com/api/1.0/users/me",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        resp.raise_for_status()

    @staticmethod
    def _probe_notion() -> None:
        from notion_client import Client

        token = os.getenv("NOTION_TOKEN", "")
        db_id = os.getenv("NOTION_DATABASE_ID", "")
        if not token:
            raise OSError("NOTION_TOKEN not set")
        if not db_id:
            raise OSError("NOTION_DATABASE_ID not set")
        client = Client(auth=token)
        database = client.databases.retrieve(db_id)

        # Verify the database schema has every property the writer expects —
        # otherwise the first write call fails mid-pipeline rather than at startup.
        required = {
            "Title": "title",
            "Date": "date",
            "Summary": "rich_text",
            "Decisions": "rich_text",
            "Action Items": "rich_text",
            "Status": "select",
        }
        actual = database.get("properties", {})
        missing: list[str] = []
        wrong_type: list[str] = []
        for name, expected_type in required.items():
            if name not in actual:
                missing.append(name)
            elif actual[name].get("type") != expected_type:
                wrong_type.append(f"{name} (expected {expected_type})")
        if missing or wrong_type:
            raise RuntimeError(
                "Notion DB schema mismatch — "
                f"missing: {missing or 'none'}, wrong type: {wrong_type or 'none'}"
            )

    @staticmethod
    def _probe_anthropic() -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            raise OSError("ANTHROPIC_API_KEY not set")
        client = anthropic.Anthropic(api_key=api_key)
        # models.list() is free — no tokens consumed.
        client.models.list()

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _timed_check(name: str, probe: object) -> ServiceStatus:
        start = time.monotonic()
        try:
            probe()  # type: ignore[operator]
            return ServiceStatus(
                name=name,
                healthy=True,
                latency_ms=(time.monotonic() - start) * 1000,
            )
        except Exception as exc:
            return ServiceStatus(
                name=name,
                healthy=False,
                latency_ms=(time.monotonic() - start) * 1000,
                error=str(exc),
            )
