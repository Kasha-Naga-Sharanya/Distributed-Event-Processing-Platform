from __future__ import annotations

import logging

import pytest

from app.rate_limiting import LocalSlidingWindowRateLimiter
from app.resilience import CircuitBreaker, CircuitOpenError, RetryPolicy, retry_call
from app.schemas.registry import SchemaBoundary, SchemaValidationError
from app.observability.logging import JsonFormatter, set_log_context


def test_local_rate_limiter_is_scoped_and_expires() -> None:
    limiter = LocalSlidingWindowRateLimiter(limit=2, window_seconds=10)
    assert limiter.check("tenant-a", now=0).allowed
    assert limiter.check("tenant-a", now=1, scope="events").allowed
    assert not limiter.check("tenant-a", now=2).allowed
    assert limiter.check("tenant-b", now=2).allowed
    assert limiter.check("tenant-a", now=11).allowed


def test_schema_boundary_rejects_unknown_version() -> None:
    boundary = SchemaBoundary()
    with pytest.raises(SchemaValidationError):
        boundary.validate({"event_type": "x", "source": "test", "payload": {}, "schema_version": 2})


def test_retry_uses_jitter_and_stops_at_attempt_limit() -> None:
    calls = 0

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise TimeoutError("temporary")

    with pytest.raises(TimeoutError):
        retry_call(operation, RetryPolicy(attempts=3, base_seconds=0, jitter_ratio=0), sleep=lambda _: None)
    assert calls == 3


def test_circuit_breaker_opens_after_failures() -> None:
    breaker = CircuitBreaker(failure_threshold=2, recovery_seconds=60)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            breaker.call(lambda: (_ for _ in ()).throw(RuntimeError("down")))
    assert breaker.state == "open"
    with pytest.raises(CircuitOpenError):
        breaker.call(lambda: None)


def test_json_formatter_includes_context() -> None:
    set_log_context(tenant_id="tenant-a", trace_id="trace-1")
    record = logging.LogRecord("test", logging.INFO, "", 0, "hello", (), None)
    output = JsonFormatter().format(record)
    assert '"tenant_id":"tenant-a"' in output
    assert '"trace_id":"trace-1"' in output
