"""Versioned event contracts and optional registry boundary."""

from app.schemas.events import EventRequest, EventSchemaV1, SCHEMA_VERSION

__all__ = ["EventRequest", "EventSchemaV1", "SCHEMA_VERSION"]
