"""Ingestion-facing exports for versioned local and registry validation."""

from app.schemas.events import EventRequest, EventSchemaV1, SCHEMA_VERSION
from app.schemas.registry import SchemaBoundary, SchemaValidationError

__all__ = [
    "EventRequest",
    "EventSchemaV1",
    "SCHEMA_VERSION",
    "SchemaBoundary",
    "SchemaValidationError",
]
