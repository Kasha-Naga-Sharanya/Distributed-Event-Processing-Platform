"""Compatibility exports for the ingestion layer's optional Kafka boundary."""

from app.messaging.kafka import KafkaConsumerAdapter, KafkaProducerAdapter, MessagingDisabled

__all__ = ["KafkaConsumerAdapter", "KafkaProducerAdapter", "MessagingDisabled"]
