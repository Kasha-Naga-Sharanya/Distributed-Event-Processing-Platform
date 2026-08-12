from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

try:  # Kafka is an optional runtime boundary.
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
except ImportError:  # pragma: no cover
    AIOKafkaConsumer = None  # type: ignore[misc, assignment]
    AIOKafkaProducer = None  # type: ignore[misc, assignment]


class MessagingDisabled(RuntimeError):
    """Raised when a Kafka adapter is used without Kafka being enabled."""


class KafkaProducerAdapter:
    def __init__(self, bootstrap_servers: str, topic: str, *, enabled: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.enabled = enabled
        self._producer = None

    async def start(self) -> None:
        if not self.enabled:
            raise MessagingDisabled("Kafka producer is disabled; SQLite mode is active")
        if AIOKafkaProducer is None:
            raise MessagingDisabled("aiokafka is not installed")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, event: Mapping[str, Any], *, key: str | None = None) -> Any:
        if self._producer is None:
            raise MessagingDisabled("Kafka producer has not been started")
        return await self._producer.send_and_wait(
            self.topic,
            value=dict(event),
            key=key.encode("utf-8") if key else None,
        )


class KafkaConsumerAdapter:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, *, enabled: bool = False) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.enabled = enabled
        self._consumer = None

    async def start(self) -> None:
        if not self.enabled:
            raise MessagingDisabled("Kafka consumer is disabled; SQLite mode is active")
        if AIOKafkaConsumer is None:
            raise MessagingDisabled("aiokafka is not installed")
        self._consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        await self._consumer.start()

    async def stop(self) -> None:
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def messages(self) -> AsyncIterator[dict[str, Any]]:
        if self._consumer is None:
            raise MessagingDisabled("Kafka consumer has not been started")
        async for message in self._consumer:
            if not isinstance(message.value, dict):
                raise ValueError("Kafka event payload must be a JSON object")
            yield message.value

    async def commit(self, message: Any) -> None:
        if self._consumer is None:
            raise MessagingDisabled("Kafka consumer has not been started")
        await self._consumer.commit()
