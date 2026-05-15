"""Google OAuth2 helper.

Manages a single token.json that covers Drive, Calendar, and Gmail.
On first run the user is directed to a browser for consent; subsequent
runs load the cached token and refresh it automatically.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)

# All scopes needed by the pipeline in one OAuth consent request.
_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
]

_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
_TOKEN_PATH = os.getenv("GOOGLE_TOKEN_PATH", "token.json")


def get_google_credentials() -> Credentials:
    """Return valid Google credentials, refreshing or re-authorising as needed.

    Raises:
        FileNotFoundError: If credentials.json is missing.
        google.auth.exceptions.GoogleAuthError: On any OAuth failure.
    """
    token_path = Path(_TOKEN_PATH)
    credentials_path = Path(_CREDENTIALS_PATH)

    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google credentials file not found: {credentials_path}. "
            "Download it from Google Cloud Console → APIs & Services → Credentials."
        )

    creds: Credentials | None = None

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), _SCOPES)
        logger.debug("Loaded cached Google credentials", extra={"token_path": str(token_path)})

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logger.info("Refreshing expired Google credentials")
            creds.refresh(Request())
        else:
            logger.info("Starting Google OAuth2 consent flow")
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), _SCOPES)
            creds = flow.run_local_server(port=0)

        token_path.write_text(creds.to_json(), encoding="utf-8")
        logger.info("Google credentials saved", extra={"token_path": str(token_path)})

    return creds
