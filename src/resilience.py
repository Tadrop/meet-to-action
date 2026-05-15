"""Resilience primitives: retry with exponential backoff and circuit breaker.

Usage:

    # Retry decorator
    @retry(max_attempts=3, exceptions=(HttpError, requests.RequestException))
    def call_some_api(): ...

    # Circuit breaker (one instance per service, kept on the pipeline object)
    cb = CircuitBreaker(name="gmail", failure_threshold=5)
    result = cb.call(create_draft, draft, recipients)
"""

from __future__ import annotations

import functools
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Type, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable)


def retry(
    max_attempts: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 60.0,
    backoff_factor: float = 2.0,
    exceptions: tuple[Type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """Decorator: retry a function with exponential backoff and ±20% jitter.

    Only retries on the specified exception types. Permanent errors (e.g. 401,
    403, 404) should NOT be listed — let them propagate immediately.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            delay = base_delay
            last_exc: Exception = RuntimeError("No attempts made")

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        logger.error(
                            "All retry attempts exhausted",
                            extra={
                                "function": func.__name__,
                                "attempts": max_attempts,
                                "final_error": str(exc),
                            },
                        )
                        break

                    jitter = delay * 0.2 * (2 * random.random() - 1)
                    sleep_time = min(delay + jitter, max_delay)
                    logger.warning(
                        "Retryable error — backing off",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "backoff_seconds": round(sleep_time, 1),
                            "error": str(exc),
                        },
                    )
                    time.sleep(sleep_time)
                    delay = min(delay * backoff_factor, max_delay)

            raise last_exc

        return wrapper  # type: ignore[return-value]

    return decorator


class _CircuitState(Enum):
    CLOSED = "closed"        # Normal — calls pass through
    OPEN = "open"            # Failing — calls are rejected immediately
    HALF_OPEN = "half_open"  # Recovery probe — one call allowed through


class CircuitOpenError(Exception):
    """Raised when a circuit breaker is OPEN and rejects a call."""


@dataclass
class CircuitBreaker:
    """Per-service circuit breaker that prevents hammering a failing API.

    States:
        CLOSED  → normal operation; tracks consecutive failures.
        OPEN    → after `failure_threshold` consecutive failures; rejects all
                  calls for `recovery_timeout` seconds.
        HALF_OPEN → after the timeout; allows one probe call through. If it
                    succeeds, the circuit CLOSES; if it fails, it re-OPENs.
    """

    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 300.0  # seconds before moving to HALF_OPEN

    _state: _CircuitState = field(default=_CircuitState.CLOSED, init=False, repr=False)
    _failure_count: int = field(default=0, init=False, repr=False)
    _last_failure_time: float = field(default=0.0, init=False, repr=False)

    def call(self, func: Callable[..., object], *args: object, **kwargs: object) -> object:
        """Execute `func(*args, **kwargs)` through the circuit breaker."""
        if self._state == _CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self.recovery_timeout:
                logger.info(
                    "Circuit moving to HALF_OPEN — probing recovery",
                    extra={"service": self.name},
                )
                self._state = _CircuitState.HALF_OPEN
            else:
                remaining = round(self.recovery_timeout - elapsed)
                raise CircuitOpenError(
                    f"Circuit '{self.name}' is OPEN. "
                    f"Recovery probe in {remaining}s."
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except CircuitOpenError:
            raise
        except Exception:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        if self._state == _CircuitState.HALF_OPEN:
            logger.info(
                "Circuit CLOSED — service recovered",
                extra={"service": self.name},
            )
        self._state = _CircuitState.CLOSED
        self._failure_count = 0

    def _on_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._failure_count >= self.failure_threshold:
            if self._state != _CircuitState.OPEN:
                logger.error(
                    "Circuit OPEN — service marked as unavailable",
                    extra={
                        "service": self.name,
                        "consecutive_failures": self._failure_count,
                        "recovery_timeout_seconds": self.recovery_timeout,
                    },
                )
            self._state = _CircuitState.OPEN
        else:
            logger.warning(
                "Circuit failure recorded",
                extra={
                    "service": self.name,
                    "failure_count": self._failure_count,
                    "threshold": self.failure_threshold,
                },
            )

    @property
    def is_open(self) -> bool:
        return self._state == _CircuitState.OPEN

    @property
    def state(self) -> str:
        return self._state.value
