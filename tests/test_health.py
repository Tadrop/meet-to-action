"""Tests for the health check suite."""

from __future__ import annotations

from src.health import HealthChecker, HealthReport, ServiceStatus


class TestHealthReport:
    def test_all_healthy_true_when_every_status_healthy(self) -> None:
        report = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=True, latency_ms=10),
                ServiceStatus(name="anthropic", healthy=True, latency_ms=10),
            ]
        )
        assert report.all_healthy

    def test_all_healthy_false_when_any_status_unhealthy(self) -> None:
        report = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=True, latency_ms=10),
                ServiceStatus(name="anthropic", healthy=False, latency_ms=10, error="oops"),
            ]
        )
        assert not report.all_healthy

    def test_critical_healthy_requires_drive_and_anthropic(self) -> None:
        report = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=True, latency_ms=10),
                ServiceStatus(name="anthropic", healthy=True, latency_ms=10),
                ServiceStatus(name="gmail", healthy=False, latency_ms=10, error="oops"),
            ]
        )
        assert report.critical_healthy

    def test_critical_unhealthy_when_drive_down(self) -> None:
        report = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=False, latency_ms=10, error="503"),
                ServiceStatus(name="anthropic", healthy=True, latency_ms=10),
            ]
        )
        assert not report.critical_healthy

    def test_critical_unhealthy_when_anthropic_missing(self) -> None:
        # Anthropic probe missing entirely → critical_healthy must be False.
        report = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=True, latency_ms=10),
            ]
        )
        assert not report.critical_healthy

    def test_unhealthy_services_lists_failures(self) -> None:
        report = HealthReport(
            statuses=[
                ServiceStatus(name="drive", healthy=True, latency_ms=10),
                ServiceStatus(name="gmail", healthy=False, latency_ms=10, error="403"),
                ServiceStatus(name="notion", healthy=False, latency_ms=10, error="timeout"),
            ]
        )
        assert set(report.unhealthy_services()) == {"gmail", "notion"}


class TestHealthChecker:
    def test_timed_check_records_success(self) -> None:
        status = HealthChecker._timed_check("test", lambda: None)
        assert status.healthy
        assert status.name == "test"
        assert status.error == ""
        assert status.latency_ms >= 0

    def test_timed_check_captures_failure(self) -> None:
        def raises() -> None:
            raise RuntimeError("boom")

        status = HealthChecker._timed_check("test", raises)
        assert not status.healthy
        assert "boom" in status.error
