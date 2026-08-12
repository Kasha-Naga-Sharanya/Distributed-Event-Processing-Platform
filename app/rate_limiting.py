"""Tenant-scoped sliding-window rate limiting.

Redis is used when configured, while the bounded in-process implementation is
an explicit local-development fallback.  The fallback must not be mistaken for
a distributed limiter in production.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Protocol
from uuid import uuid4

try:  # Optional at import time so SQLite-only installs remain usable.
    import redis
    from redis.exceptions import RedisError
except ImportError:  # pragma: no cover - exercised in minimal installations
    redis = None  # type: ignore[assignment]

    class RedisError(Exception):
        """Placeholder used when redis-py is not installed."""


class RateLimiter(Protocol):
    def allow(self, tenant_id: str, *, scope: str = "events") -> bool:
        ...


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    limit: int


class LocalSlidingWindowRateLimiter:
    """Thread-safe, process-local sliding window for the SQLite baseline."""

    def __init__(self, limit: int, window_seconds: float = 60.0) -> None:
        if limit < 1 or window_seconds <= 0:
            raise ValueError("limit and window_seconds must be positive")
        self.limit = limit
        self.window_seconds = window_seconds
        self._windows: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, tenant_id: str, *, scope: str = "events", now: float | None = None) -> RateLimitDecision:
        current = time.monotonic() if now is None else now
        key = f"{tenant_id}:{scope}"
        with self._lock:
            window = self._windows[key]
            while window and current - window[0] >= self.window_seconds:
                window.popleft()
            if len(window) >= self.limit:
                return RateLimitDecision(False, 0, self.limit)
            window.append(current)
            return RateLimitDecision(True, self.limit - len(window), self.limit)

    def allow(self, tenant_id: str, *, scope: str = "events") -> bool:
        return self.check(tenant_id, scope=scope).allowed


class RedisSlidingWindowRateLimiter:
    """Atomic Redis sorted-set sliding-window limiter.

    A Lua script makes prune, count, and append one operation across API
    instances.  Redis errors are deliberately surfaced to the factory caller;
    fallback policy belongs to ``FallbackRateLimiter``.
    """

    _SCRIPT = """
    local key = KEYS[1]
    local now = tonumber(ARGV[1])
    local window = tonumber(ARGV[2])
    local limit = tonumber(ARGV[3])
    redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
    local count = redis.call('ZCARD', key)
    if count >= limit then
      redis.call('EXPIRE', key, math.ceil(window))
      return {0, count}
    end
    redis.call('ZADD', key, now, ARGV[4])
    redis.call('EXPIRE', key, math.ceil(window))
    return {1, count + 1}
    """

    def __init__(self, client: object, limit: int, window_seconds: float = 60.0, prefix: str = "event-platform:rl") -> None:
        if limit < 1 or window_seconds <= 0:
            raise ValueError("limit and window_seconds must be positive")
        self.client = client
        self.limit = limit
        self.window_seconds = window_seconds
        self.prefix = prefix
        self._script = client.register_script(self._SCRIPT)

    def check(self, tenant_id: str, *, scope: str = "events", now: float | None = None) -> RateLimitDecision:
        current = time.time() if now is None else now
        key = f"{self.prefix}:{tenant_id}:{scope}"
        result = self._script(
            keys=[key],
            args=[current, self.window_seconds, self.limit, f"{current}:{uuid4().hex}"],
        )
        allowed, count = int(result[0]), int(result[1])
        return RateLimitDecision(bool(allowed), max(0, self.limit - count), self.limit)

    def allow(self, tenant_id: str, *, scope: str = "events") -> bool:
        return self.check(tenant_id, scope=scope).allowed


class FallbackRateLimiter:
    """Use Redis and switch to local only when the explicit policy allows it."""

    def __init__(self, primary: RedisSlidingWindowRateLimiter, fallback: LocalSlidingWindowRateLimiter, *, allow_fallback: bool) -> None:
        self.primary = primary
        self.fallback = fallback
        self.allow_fallback = allow_fallback
        self.using_fallback = False

    def check(self, tenant_id: str, *, scope: str = "events") -> RateLimitDecision:
        try:
            decision = self.primary.check(tenant_id, scope=scope)
            self.using_fallback = False
            return decision
        except RedisError:
            if not self.allow_fallback:
                raise
            self.using_fallback = True
            return self.fallback.check(tenant_id, scope=scope)

    def allow(self, tenant_id: str, *, scope: str = "events") -> bool:
        return self.check(tenant_id, scope=scope).allowed


def build_rate_limiter(redis_url: str, limit: int, *, local_fallback: bool = True) -> RateLimiter:
    """Build a limiter without contacting Redis during module import."""
    local = LocalSlidingWindowRateLimiter(limit)
    if redis is None:
        if local_fallback:
            return local
        raise RuntimeError("redis package is required when local fallback is disabled")
    primary: RedisSlidingWindowRateLimiter | None = None
    try:
        client = redis.Redis.from_url(redis_url, decode_responses=False, socket_connect_timeout=0.2, socket_timeout=0.2)
        primary = RedisSlidingWindowRateLimiter(client, limit)
        client.ping()
    except RedisError:
        if local_fallback:
            return FallbackRateLimiter(primary, local, allow_fallback=True) if primary else local
        raise
    if primary is None:  # defensive guard for alternate redis clients
        raise RuntimeError("failed to initialize Redis rate limiter")
    return FallbackRateLimiter(primary, local, allow_fallback=local_fallback)
