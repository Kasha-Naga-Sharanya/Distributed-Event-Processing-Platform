from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

SCHEMA_VERSION = 1


class EventSchemaV1(BaseModel):
    """Stable local contract used when Schema Registry is not running."""

    model_config = ConfigDict(extra="forbid")

    event_type: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any]
    source: str = Field(min_length=1, max_length=200)
    schema_version: int = Field(default=SCHEMA_VERSION, ge=1)


EventRequest = EventSchemaV1
