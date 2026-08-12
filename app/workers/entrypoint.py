"""Kafka worker entrypoint.

Run with ``KAFKA_ENABLED=true`` in a deployment.  The API's default SQLite
mode never imports or starts this worker.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from app.config.settings import settings
from app.messaging.kafka import KafkaConsumerAdapter

logger = logging.getLogger(__name__)


async def run_worker(handler: Callable[[dict[str, Any]], Awaitable[None]]) -> None:
    if not settings.kafka_enabled:
        raise RuntimeError("KAFKA_ENABLED must be true to run the worker")
    consumer = KafkaConsumerAdapter(
        settings.kafka_bootstrap_servers,
        settings.kafka_topic,
        settings.kafka_group_id,
        enabled=True,
    )
    await consumer.start()
    try:
        async for payload in consumer.messages():
            await handler(payload)
            await consumer.commit(None)
    finally:
        await consumer.stop()


async def _log_only_handler(payload: dict[str, Any]) -> None:
    logger.info("kafka_event_received", extra={"event_id": payload.get("event_id")})


def main() -> None:
    asyncio.run(run_worker(_log_only_handler))


if __name__ == "__main__":
    main()
