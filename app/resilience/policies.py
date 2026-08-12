"""Small dependency-light resilience policies for outbound calls."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from threading import Lock, Semaphore
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class CircuitOpenError(RuntimeError):
    pass


class BulkheadFullError(RuntimeError):
    pass


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    base_seconds: float = 0.1
    max_seconds: float = 5.0
    jitter_ratio: float = 1.0

    def __post_init__(self) -> None:
        if self.attempts < 1 or self.base_seconds < 0 or self.max_seconds < 0 or self.jitter_ratio < 0:
            raise ValueError("retry policy values are invalid")

    def delay(self, attempt: int, *, rng: Callable[[], float] = random.random) -> float:
        cap = min(self.max_seconds, self.base_seconds * (2**attempt))
        return cap * (1 - self.jitter_ratio * rng())


def retry_call(
    operation: Callable[[], T],
    policy: RetryPolicy,
    *,
    retryable: tuple[type[BaseException], ...] = (TimeoutError, OSError),
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    last_error: BaseException | None = None
    for attempt in range(policy.attempts):
        try:
            return operation()
        except retryable as exc:
            last_error = exc
            if attempt + 1 >= policy.attempts:
                raise
            sleep(policy.delay(attempt))
    raise RuntimeError("retry policy did not execute") from last_error


async def async_retry_call(
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    retryable: tuple[type[BaseException], ...] = (TimeoutError, OSError),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    last_error: BaseException | None = None
    for attempt in range(policy.attempts):
        try:
            return await operation()
        except retryable as exc:
            last_error = exc
            if attempt + 1 >= policy.attempts:
                raise
            await sleep(policy.delay(attempt))
    raise RuntimeError("retry policy did not execute") from last_error


def call_with_timeout(operation: Callable[[], T], timeout_seconds: float) -> T:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="resilience-timeout")
    try:
        future = pool.submit(operation)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError as exc:
            future.cancel()
            raise TimeoutError(f"operation exceeded {timeout_seconds}s timeout") from exc
    finally:
        # Python cannot safely kill a running thread, but do not wait for an
        # over-time operation before returning the timeout to the caller.
        pool.shutdown(wait=False, cancel_futures=True)


async def async_call_with_timeout(operation: Callable[[], Awaitable[T]], timeout_seconds: float) -> T:
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    return await asyncio.wait_for(operation(), timeout=timeout_seconds)


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_seconds: float = 30.0) -> None:
        if failure_threshold < 1 or recovery_seconds <= 0:
            raise ValueError("circuit breaker values are invalid")
        self.failure_threshold = failure_threshold
        self.recovery_seconds = recovery_seconds
        self._failures = 0
        self._opened_at: float | None = None
        self._probe_in_flight = False
        self._lock = Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._opened_at is None:
                return "closed"
            if time.monotonic() - self._opened_at < self.recovery_seconds:
                return "open"
            return "half_open"

    def before_call(self) -> None:
        with self._lock:
            if self._opened_at is None:
                return
            if time.monotonic() - self._opened_at < self.recovery_seconds:
                raise CircuitOpenError("circuit breaker is open")
            if self._probe_in_flight:
                raise CircuitOpenError("circuit breaker probe is in flight")
            self._probe_in_flight = True

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = None
            self._probe_in_flight = False

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._probe_in_flight = False
            if self._failures >= self.failure_threshold:
                self._opened_at = time.monotonic()

    def call(self, operation: Callable[[], T]) -> T:
        self.before_call()
        try:
            result = operation()
        except (TimeoutError, OSError, RuntimeError, ValueError):
            self.record_failure()
            raise
        self.record_success()
        return result


class Bulkhead:
    def __init__(self, max_concurrency: int) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self._semaphore = Semaphore(max_concurrency)

    def call(self, operation: Callable[[], T]) -> T:
        if not self._semaphore.acquire(blocking=False):
            raise BulkheadFullError("bulkhead capacity exhausted")
        try:
            return operation()
        finally:
            self._semaphore.release()


def call_with_resilience(
    operation: Callable[[], T],
    *,
    timeout_seconds: float,
    retry_policy: RetryPolicy | None = None,
    breaker: CircuitBreaker | None = None,
    bulkhead: Bulkhead | None = None,
) -> T:
    def invoke() -> T:
        call = lambda: call_with_timeout(operation, timeout_seconds)
        return breaker.call(call) if breaker else call()

    guarded = lambda: bulkhead.call(invoke) if bulkhead else invoke()
    return retry_call(guarded, retry_policy or RetryPolicy(attempts=1))
