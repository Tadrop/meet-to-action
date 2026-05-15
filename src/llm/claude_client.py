"""Claude API wrapper for meeting transcript analysis.

Design choices:
  - Model is configurable via `CLAUDE_MODEL` env var (default: claude-opus-4-7).
  - Adaptive thinking: Claude decides how much reasoning to apply per call.
  - Streaming: prevents request timeouts on long transcripts; uses the SDK's
    `.get_final_message()` helper to collect the complete response.
  - Forced tool use: guarantees structured JSON output via `tool_choice`.
  - Prompt caching: the system prompt is marked ephemeral so subsequent runs
    within the 5-minute TTL skip re-processing it.
  - Transcript size guard: blocks accidental massive uploads before the API call.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import anthropic

from src.llm.prompt import ANALYSIS_TOOL, SYSTEM_PROMPT, build_user_prompt
from src.llm.schema import MeetingAnalysis
from src.resilience import retry

logger = logging.getLogger(__name__)

_DEFAULT_MODEL = "claude-opus-4-7"
_MAX_TOKENS = 8192
# Soft cap on transcript size: ~500k tokens at the conservative 4 chars/token rule.
# Well within Claude Opus 4.7's 1M context, leaves ample headroom for output.
_MAX_TRANSCRIPT_CHARS = 2_000_000


class TranscriptTooLargeError(ValueError):
    """Raised when a transcript exceeds the safe size limit."""


class ClaudeClient:
    def __init__(self) -> None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise OSError("ANTHROPIC_API_KEY environment variable is not set")
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = os.getenv("CLAUDE_MODEL", _DEFAULT_MODEL)

    @retry(
        max_attempts=3,
        base_delay=5.0,
        exceptions=(anthropic.APIConnectionError, anthropic.InternalServerError),
    )
    def analyse_transcript(
        self,
        transcript: str,
        meeting_title: str | None = None,
        attendees: list[str] | None = None,
        calendar_description: str | None = None,
    ) -> MeetingAnalysis:
        """Send transcript to Claude and return a validated MeetingAnalysis.

        Raises:
            TranscriptTooLargeError: If the transcript exceeds the size limit.
            anthropic.APIError: On any retryable API-level failure (after retries).
            ValueError: If Claude's response cannot be validated against the schema.
        """
        if len(transcript) > _MAX_TRANSCRIPT_CHARS:
            raise TranscriptTooLargeError(
                f"Transcript is {len(transcript):,} chars — exceeds limit of "
                f"{_MAX_TRANSCRIPT_CHARS:,}. Split it before processing."
            )

        user_prompt = build_user_prompt(
            transcript=transcript,
            meeting_title=meeting_title,
            attendees=attendees,
            calendar_description=calendar_description,
        )

        start = time.monotonic()
        logger.info(
            "Sending transcript to Claude",
            extra={"model": self._model, "transcript_chars": len(transcript)},
        )

        try:
            # Streaming + .get_final_message() avoids request timeouts on long inputs.
            # Adaptive thinking is GA on Opus 4.7; older SDK stubs may not
            # reflect the `thinking` arg yet, so it's passed via extra_body.
            with self._client.messages.stream(
                model=self._model,
                max_tokens=_MAX_TOKENS,
                system=[
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                tools=[ANALYSIS_TOOL],
                tool_choice={"type": "tool", "name": "record_meeting_analysis"},
                messages=[{"role": "user", "content": user_prompt}],
                extra_body={"thinking": {"type": "adaptive"}},
            ) as stream:
                response = stream.get_final_message()
        except anthropic.APIStatusError as exc:
            logger.error(
                "Claude API status error",
                extra={"status_code": exc.status_code, "error": str(exc)},
            )
            raise
        except anthropic.APIError as exc:
            logger.error("Claude API error", extra={"error": str(exc)})
            raise

        elapsed = time.monotonic() - start
        self._log_usage(response.usage, elapsed)

        return self._parse_response(response)

    # ── private helpers ───────────────────────────────────────────────────────

    def _log_usage(self, usage: Any, elapsed: float) -> None:
        usage_info: dict[str, Any] = {"elapsed_seconds": round(elapsed, 2)}
        for attr in (
            "input_tokens",
            "output_tokens",
            "cache_creation_input_tokens",
            "cache_read_input_tokens",
        ):
            if hasattr(usage, attr):
                usage_info[attr] = getattr(usage, attr)
        logger.info("Claude API call complete", extra=usage_info)

    def _parse_response(self, response: anthropic.types.Message) -> MeetingAnalysis:
        for block in response.content:
            if block.type == "tool_use" and block.name == "record_meeting_analysis":
                raw: dict[str, Any] = block.input
                logger.debug("Received tool call input from Claude")
                try:
                    return MeetingAnalysis.model_validate(raw)
                except Exception as exc:
                    logger.error(
                        "MeetingAnalysis validation failed",
                        extra={"error": str(exc), "raw_keys": list(raw.keys())},
                    )
                    raise ValueError(f"Claude output failed schema validation: {exc}") from exc

        raise ValueError(
            "Claude did not return a record_meeting_analysis tool call. "
            f"Stop reason: {response.stop_reason}. "
            f"Content types: {[b.type for b in response.content]}"
        )
