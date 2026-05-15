"""Tests for the retry decorator and circuit breaker state machine."""

from __future__ import annotations

import time

import pytest

from src.resilience import CircuitBreaker, CircuitOpenError, retry


class _FlakyError(Exception):
    pass


class _PermanentError(Exception):
    pass


class TestRetry:
    def test_succeeds_on_first_attempt(self) -> None:
        calls = {"n": 0}

        @retry(max_attempts=3, base_delay=0.01)
        def f() -> str:
            calls["n"] += 1
            return "ok"

        assert f() == "ok"
        assert calls["n"] == 1

    def test_succeeds_after_retries(self) -> None:
        calls = {"n": 0}

        @retry(max_attempts=3, base_delay=0.01, exceptions=(_FlakyError,))
        def f() -> str:
            calls["n"] += 1
            if calls["n"] < 3:
                raise _FlakyError("flaky")
            return "ok"

        assert f() == "ok"
        assert calls["n"] == 3

    def test_exhausts_attempts_and_reraises(self) -> None:
        calls = {"n": 0}

        @retry(max_attempts=3, base_delay=0.01, exceptions=(_FlakyError,))
        def f() -> str:
            calls["n"] += 1
            raise _FlakyError("always fails")

        with pytest.raises(_FlakyError):
            f()
        assert calls["n"] == 3

    def test_does_not_retry_unlisted_exceptions(self) -> None:
        calls = {"n": 0}

        @retry(max_attempts=3, base_delay=0.01, exceptions=(_FlakyError,))
        def f() -> str:
            calls["n"] += 1
            raise _PermanentError("not retryable")

        with pytest.raises(_PermanentError):
            f()
        assert calls["n"] == 1

    def test_backoff_grows_between_attempts(self) -> None:
        timestamps: list[float] = []

        @retry(max_attempts=3, base_delay=0.05, backoff_factor=2.0, exceptions=(_FlakyError,))
        def f() -> None:
            timestamps.append(time.monotonic())
            raise _FlakyError("flaky")

        with pytest.raises(_FlakyError):
            f()

        # Three attempts → two gaps. Second gap should be larger than the first.
        assert len(timestamps) == 3
        gap_one = timestamps[1] - timestamps[0]
        gap_two = timestamps[2] - timestamps[1]
        assert gap_two > gap_one


class TestCircuitBreaker:
    def test_passes_through_when_closed(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)
        result = cb.call(lambda: "ok")
        assert result == "ok"
        assert not cb.is_open

    def test_opens_after_threshold_failures(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3, recovery_timeout=60)

        def always_fails() -> None:
            raise RuntimeError("nope")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                cb.call(always_fails)

        assert cb.is_open

    def test_rejects_calls_when_open(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=60)

        def always_fails() -> None:
            raise RuntimeError("nope")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(always_fails)

        # Now OPEN — next call should be rejected without invoking the function.
        called = {"n": 0}

        def tracker() -> None:
            called["n"] += 1

        with pytest.raises(CircuitOpenError):
            cb.call(tracker)
        assert called["n"] == 0

    def test_moves_to_half_open_after_timeout(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=0.1)

        def always_fails() -> None:
            raise RuntimeError("nope")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(always_fails)

        time.sleep(0.15)

        # HALF_OPEN allows the probe through; if it succeeds, circuit CLOSES.
        result = cb.call(lambda: "recovered")
        assert result == "recovered"
        assert not cb.is_open

    def test_reopens_if_half_open_probe_fails(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=2, recovery_timeout=0.1)

        def always_fails() -> None:
            raise RuntimeError("nope")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(always_fails)

        time.sleep(0.15)

        # HALF_OPEN probe also fails → re-OPEN.
        with pytest.raises(RuntimeError):
            cb.call(always_fails)
        assert cb.is_open

    def test_success_resets_failure_count(self) -> None:
        cb = CircuitBreaker(name="test", failure_threshold=3)

        def always_fails() -> None:
            raise RuntimeError("nope")

        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(always_fails)

        # Successful call should reset the counter.
        cb.call(lambda: "ok")

        # Two more failures should not yet trigger OPEN — counter was reset.
        for _ in range(2):
            with pytest.raises(RuntimeError):
                cb.call(always_fails)
        assert not cb.is_open
