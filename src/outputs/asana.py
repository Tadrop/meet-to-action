"""Asana output — creates one task per action item via Asana REST API.

Uses direct HTTP (requests) rather than the Asana SDK to avoid SDK version
fragility. Tasks are created in a DRAFT/incomplete state by default.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

from src.llm.schema import ActionItem
from src.resilience import retry

logger = logging.getLogger(__name__)

_ASANA_BASE = "https://app.asana.com/api/1.0"


class AsanaTaskCreator:
    def __init__(self) -> None:
        token = os.getenv("ASANA_ACCESS_TOKEN")
        if not token:
            raise EnvironmentError("ASANA_ACCESS_TOKEN environment variable is not set")

        self._workspace_gid = os.getenv("ASANA_WORKSPACE_GID", "")
        self._project_gid = os.getenv("ASANA_PROJECT_GID", "")

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        # Cache email → user GID lookups so a 10-action-item meeting doesn't
        # trigger 10 separate typeahead calls for the same person.
        self._user_gid_cache: dict[str, str | None] = {}

    def create_tasks(
        self,
        action_items: list[ActionItem],
        meeting_title: str,
    ) -> list[str]:
        """Create one Asana task per action item. Returns list of created task GIDs.

        Args:
            action_items: Validated action items from MeetingAnalysis.
            meeting_title: Used as a tag/note prefix for traceability.

        Returns:
            List of Asana task GIDs (one per action item).
        """
        task_gids: list[str] = []
        for item in action_items:
            gid = self._create_single_task(item, meeting_title)
            if gid:
                task_gids.append(gid)
        return task_gids

    # ── private helpers ───────────────────────────────────────────────────────

    @retry(
        max_attempts=3,
        base_delay=2.0,
        exceptions=(requests.ConnectionError, requests.Timeout),
    )
    def _create_single_task(self, item: ActionItem, meeting_title: str) -> str | None:
        notes = (
            f"Meeting: {meeting_title}\n\n"
            f"Action: {item.description}\n\n"
            f'Supporting quote: "{item.supporting_quote}"'
        )

        payload: dict[str, Any] = {
            "data": {
                "name": item.description,
                "notes": notes,
                "due_on": item.deadline,  # ISO 8601 date
                "completed": False,
            }
        }

        if self._workspace_gid:
            payload["data"]["workspace"] = self._workspace_gid
        if self._project_gid:
            payload["data"]["projects"] = [self._project_gid]

        # Attempt to resolve owner email to an Asana user GID.
        assignee_gid = self._find_assignee(item.owner)
        if assignee_gid:
            payload["data"]["assignee"] = assignee_gid

        start = time.monotonic()
        logger.info(
            "Creating Asana task",
            extra={"owner": item.owner, "deadline": item.deadline},
        )

        try:
            response = self._session.post(
                f"{_ASANA_BASE}/tasks", json=payload, timeout=30
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            logger.error(
                "Asana API HTTP error",
                extra={
                    "status": exc.response.status_code if exc.response else None,
                    "response_body": exc.response.text if exc.response else None,
                    "error": str(exc),
                },
            )
            return None
        except requests.RequestException as exc:
            logger.error("Asana API request error", extra={"error": str(exc)})
            return None

        body = response.json()
        task_gid: str = body.get("data", {}).get("gid", "")
        if not task_gid:
            logger.error(
                "Asana response missing task GID",
                extra={"response_keys": list(body.keys())},
            )
            return None

        elapsed = time.monotonic() - start
        logger.info(
            "Asana task created",
            extra={"task_gid": task_gid, "elapsed_seconds": round(elapsed, 2)},
        )
        return task_gid

    def _find_assignee(self, owner_str: str) -> str | None:
        """Try to find an Asana user GID from the owner string (which may contain email)."""
        if not self._workspace_gid:
            return None

        # Extract email if present (format: "Name <email>" or just "email")
        email: str | None = None
        if "<" in owner_str and ">" in owner_str:
            email = owner_str.split("<")[1].rstrip(">").strip()
        elif "@" in owner_str:
            # Handle "Name (email@domain.com)" or plain "email@domain.com"
            for token in owner_str.split():
                clean = token.strip("()")
                if "@" in clean:
                    email = clean
                    break

        if not email:
            logger.debug(
                "Could not extract email from owner string — task created unassigned",
                extra={"owner": owner_str},
            )
            return None

        # Cache hit avoids redundant typeahead calls within a single run.
        if email in self._user_gid_cache:
            return self._user_gid_cache[email]

        gid: str | None = None
        try:
            response = self._session.get(
                f"{_ASANA_BASE}/workspaces/{self._workspace_gid}/typeahead",
                params={"resource_type": "user", "query": email, "count": 1},
                timeout=10,
            )
            response.raise_for_status()
            users = response.json().get("data", [])
            if users:
                gid = users[0].get("gid")
        except requests.RequestException as exc:
            logger.warning(
                "Could not resolve Asana user — task created unassigned",
                extra={"email": email, "error": str(exc)},
            )

        # Cache negatives too — re-querying for an unknown email is wasteful.
        self._user_gid_cache[email] = gid
        return gid
