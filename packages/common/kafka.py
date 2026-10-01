"""Kafka topic names, client factories, and admin helpers.

Client tuning lives here so a scale profile change (``PROFILE=full``) alters
batch sizes and linger settings for every service at once, and so topic names
are never spelled out as literals at call sites.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from packages.common import config as cfg

# -- Topics ---------------------------------------------------------------
TELEMETRY_RAW = "telemetry.raw"
TELEMETRY_CANONICAL = "telemetry.canonical"
TELEMETRY_QUARANTINE = "telemetry.quarantine"
INCIDENTS_EVENTS = "incidents.events"
ALERTS_CRITICAL = "alerts.critical"

#: Poison messages, one per source topic, so a bad payload never blocks a partition.
DLQ_SUFFIX = ".dlq"

ALL_TOPICS = (
    TELEMETRY_RAW,
    TELEMETRY_CANONICAL,
    TELEMETRY_QUARANTINE,
    INCIDENTS_EVENTS,
    ALERTS_CRITICAL,
)

#: Partition counts when a profile does not override them.
DEFAULT_PARTITIONS = {
    TELEMETRY_RAW: 6,
    TELEMETRY_CANONICAL: 6,
    TELEMETRY_QUARANTINE: 3,
    INCIDENTS_EVENTS: 3,
    ALERTS_CRITICAL: 3,
}

# -- Consumer groups ------------------------------------------------------
GROUP_VALIDATOR = "validator"
GROUP_INCIDENT = "incident-processor"
GROUP_SINK = "telemetry-sink"
GROUP_PERSISTER = "incident-persister"


def dlq_topic(source_topic: str) -> str:
    return f"{source_topic}{DLQ_SUFFIX}"


def broker() -> str:
    return os.getenv("KAFKA_BROKER") or os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:9092"


def partitions_for(topic: str, profile: cfg.ProfileConfig | None = None) -> int:
    profile = profile or cfg.load_profile()
    return int(profile.kafka_partitions.get(topic, DEFAULT_PARTITIONS.get(topic, 3)))


@dataclass(frozen=True)
class ConsumerTuning:
    """Batch-poll parameters, resolved from the active scale profile."""

    max_batch: int
    max_wait_ms: int

    @classmethod
    def from_profile(cls, profile: cfg.ProfileConfig | None = None) -> ConsumerTuning:
        profile = profile or cfg.load_profile()
        return cls(max_batch=profile.kafka_max_batch, max_wait_ms=profile.kafka_max_wait_ms)


def producer_config(**overrides: Any) -> dict[str, Any]:
    """Producer settings: durable, compressed, idempotent.

    ``enable.idempotence`` costs nothing measurable here and removes producer
    retries as a source of duplicates, which keeps the dedup gate responsible
    only for genuine replays.
    """
    settings: dict[str, Any] = {
        "bootstrap.servers": broker(),
        "linger.ms": int(os.getenv("KAFKA_LINGER_MS", "10")),
        "compression.type": os.getenv("KAFKA_COMPRESSION", "lz4"),
        "acks": "all",
        "enable.idempotence": True,
        "batch.size": int(os.getenv("KAFKA_BATCH_SIZE", "262144")),
        "queue.buffering.max.messages": int(os.getenv("KAFKA_QUEUE_MAX_MESSAGES", "200000")),
        "queue.buffering.max.kbytes": int(os.getenv("KAFKA_QUEUE_MAX_KBYTES", "1048576")),
        "message.timeout.ms": int(os.getenv("KAFKA_MESSAGE_TIMEOUT_MS", "30000")),
    }
    settings.update(overrides)
    return settings


def consumer_config(group_id: str, **overrides: Any) -> dict[str, Any]:
    """Consumer settings: manual commit, bounded fetch.

    Auto-commit is disabled everywhere. Offsets advance only after the work is
    durable, which is what turns at-least-once delivery plus idempotent writes
    into effectively-once processing.
    """
    settings: dict[str, Any] = {
        "bootstrap.servers": broker(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": False,
        "max.poll.interval.ms": int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000")),
        "session.timeout.ms": int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "45000")),
        "fetch.min.bytes": int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1")),
        "fetch.wait.max.ms": int(os.getenv("KAFKA_FETCH_WAIT_MAX_MS", "100")),
        "max.partition.fetch.bytes": int(os.getenv("KAFKA_MAX_PARTITION_FETCH_BYTES", "4194304")),
    }
    settings.update(overrides)
    return settings


def build_producer(**overrides: Any) -> Any:
    from confluent_kafka import Producer

    return ReliableProducer(Producer(producer_config(**overrides)))


def build_consumer(group_id: str, **overrides: Any) -> Any:
    from confluent_kafka import Consumer

    return Consumer(consumer_config(group_id, **overrides))


def header_value(headers: Any, key: str) -> str | None:
    """Read one Kafka header, tolerating the None-headers case."""
    for name, value in headers or []:
        if name == key:
            if isinstance(value, bytes):
                return value.decode("utf-8", errors="ignore")
            return None if value is None else str(value)
    return None


class ReliableProducer:
    """Propagate asynchronous delivery failures to a batch's durability gate."""

    def __init__(self, producer: Any) -> None:
        self.producer = producer
        self.errors: list[str] = []

    def produce(self, *args: Any, **kwargs: Any) -> None:
        callback = kwargs.pop("on_delivery", kwargs.pop("callback", None))

        def delivered(error: Any, message: Any) -> None:
            if error is not None:
                self.errors.append(str(error))
            if callback is not None:
                callback(error, message)

        self.producer.produce(*args, on_delivery=delivered, **kwargs)

    def poll(self, timeout: float) -> Any:
        return self.producer.poll(timeout)

    def flush(self, timeout: float = 30) -> int:
        pending = self.producer.flush(timeout)
        if self.errors:
            errors, self.errors = self.errors, []
            raise RuntimeError("Kafka delivery failed: " + "; ".join(errors[:3]))
        return int(pending)
