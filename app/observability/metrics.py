"""Shared Prometheus metric definitions for API and worker processes."""

from prometheus_client import Counter, Gauge, Histogram

EVENTS_TOTAL = Counter("events_total", "Events observed", ["tenant_id", "status"])
PROCESSING_SECONDS = Histogram(
    "event_processing_seconds",
    "Event processing duration",
    ["tenant_id"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
KAFKA_CONSUMER_LAG = Gauge("kafka_consumer_lag", "Kafka consumer lag", ["topic", "partition", "group"])
CIRCUIT_BREAKER_STATE = Gauge("circuit_breaker_state", "Circuit state (1=open, 0=closed)", ["dependency"])
