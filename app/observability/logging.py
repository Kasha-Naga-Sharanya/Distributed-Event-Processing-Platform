"""Structured JSON logging with request-scoped tenant and trace context."""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any

tenant_id_context: ContextVar[str | None] = ContextVar("tenant_id", default=None)
trace_id_context: ContextVar[str | None] = ContextVar("trace_id", default=None)


def set_log_context(*, tenant_id: str | None = None, trace_id: str | None = None) -> None:
    tenant_id_context.set(tenant_id)
    trace_id_context.set(trace_id)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "tenant_id": getattr(record, "tenant_id", None) or tenant_id_context.get(),
            "trace_id": getattr(record, "trace_id", None) or trace_id_context.get(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
