"""Optional Kafka adapters; SQLite remains the default transport."""

from app.messaging.kafka import KafkaConsumerAdapter, KafkaProducerAdapter, MessagingDisabled

__all__ = ["KafkaConsumerAdapter", "KafkaProducerAdapter", "MessagingDisabled"]
