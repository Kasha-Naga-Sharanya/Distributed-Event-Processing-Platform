"""Local-first schema validation with an optional Confluent boundary."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import ValidationError

from app.schemas.events import EventSchemaV1, SCHEMA_VERSION

logger = logging.getLogger(__name__)

try:  # The API remains usable without confluent-kafka installed.
    from confluent_kafka import KafkaException
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.error import SchemaRegistryError
except ImportError:  # pragma: no cover
    KafkaException = None  # type: ignore[misc, assignment]
    SchemaRegistryClient = None  # type: ignore[misc, assignment]
    SchemaRegistryError = None  # type: ignore[misc, assignment]


class SchemaValidationError(ValueError):
    """Raised when an event is not valid for a supported schema version."""


class SchemaBoundary:
    """Validate locally, and optionally verify that a registry subject exists.

    Registry availability is an explicit deployment concern.  When
    ``required=False`` a registry outage does not weaken local Pydantic
    validation; it only skips the external compatibility check.
    """

    def __init__(self, url: str | None = None, *, enabled: bool = False, required: bool = False) -> None:
        self.url = url
        self.enabled = enabled
        self.required = required
        self._client = None
        if enabled:
            if not url:
                raise ValueError("schema registry URL is required when enabled")
            if SchemaRegistryClient is None:
                raise RuntimeError("confluent-kafka is required when schema registry is enabled")
            self._client = SchemaRegistryClient({"url": url})

    def validate(self, event: dict[str, Any], *, subject: str | None = None) -> EventSchemaV1:
        try:
            parsed = EventSchemaV1.model_validate(event)
        except ValidationError as exc:
            raise SchemaValidationError(str(exc)) from exc
        if parsed.schema_version != SCHEMA_VERSION:
            raise SchemaValidationError(f"unsupported schema_version: {parsed.schema_version}")
        if self._client is not None and subject:
            try:
                self._client.get_latest_version(subject)
            except (OSError, TimeoutError, KafkaException, SchemaRegistryError) as exc:
                # Confluent's client exposes several transport exception types
                # across versions; preserve the cause and only apply the
                # explicitly configured fail-open policy.
                if self.required:
                    raise SchemaValidationError(f"schema registry check failed for {subject}") from exc
                logger.warning("schema_registry_unavailable", extra={"subject": subject})
        return parsed
