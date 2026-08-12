"""Optional OpenTelemetry setup; tracing is never required for local SQLite."""

from __future__ import annotations

from typing import Any

try:
    from opentelemetry import trace
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
except ImportError:  # pragma: no cover
    trace = None  # type: ignore[assignment]
    FastAPIInstrumentor = None  # type: ignore[assignment]
    Resource = TracerProvider = BatchSpanProcessor = ConsoleSpanExporter = None  # type: ignore[assignment]

from app.observability.logging import trace_id_context


def setup_tracing(app: Any, *, enabled: bool, service_name: str, exporter_endpoint: str = "") -> bool:
    if not enabled:
        return False
    if trace is None or FastAPIInstrumentor is None or TracerProvider is None:
        raise RuntimeError("OpenTelemetry packages are required when OTEL_ENABLED=true")
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    # Console export is intentionally opt-in via an endpoint; the hook itself
    # still creates spans and allows a deployment to install its own processor.
    if exporter_endpoint.lower() == "console":
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider)
    return True


def current_trace_id() -> str | None:
    if trace is None:
        return trace_id_context.get()
    span = trace.get_current_span()
    context = span.get_span_context()
    if not context.is_valid:
        return trace_id_context.get()
    value = format(context.trace_id, "032x")
    trace_id_context.set(value)
    return value
