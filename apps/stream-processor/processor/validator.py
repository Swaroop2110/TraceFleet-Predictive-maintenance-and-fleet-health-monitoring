"""Validator role: ``telemetry.raw`` to ``telemetry.canonical``.

Per batch: validate, dedup, normalise, update hot state, publish. Offsets are
committed by :class:`BatchConsumer` only after the produced batch has been
flushed to the broker, so a crash mid-batch replays rather than loses.

Invalid payloads go to ``telemetry.quarantine`` with a specific reason code and
are *not* an error for the batch — a schema fault is expected data, not a failure,
and must not stall the partition.

Duplicates are dropped before detectors see them. They still increment
``events_duplicated_total`` and the surviving event records the count, so
suppression is observable rather than invisible.
"""

from __future__ import annotations

import json
import os
from typing import Any

from packages.common import config as cfg
from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.common.timeutil import now, to_iso
from packages.schemas.models import CanonicalEvent, QuarantineRecord
from processor.consumers import BatchConsumer, decode_json_batch
from processor.dedup import DedupGate
from processor.normalizer import (
    Normalizer,
    QuarantineError,
    TenantResolver,
    kafka_timestamp,
    validate,
)

LOGGER = tf_logging.get_logger(__name__)


class Validator:
    """Raw telemetry validation, deduplication, and canonicalisation."""

    def __init__(self, broker: str | None = None) -> None:
        self.broker = broker or kafka_helpers.broker()
        correlation = cfg.correlation()
        profile = cfg.load_profile()

        self.producer = kafka_helpers.build_producer(**{"bootstrap.servers": self.broker})
        self.consumer = BatchConsumer(
            topics=[kafka_helpers.TELEMETRY_RAW],
            group_id=kafka_helpers.GROUP_VALIDATOR,
            producer=self.producer,
            consumer=kafka_helpers.build_consumer(kafka_helpers.GROUP_VALIDATOR, **{"bootstrap.servers": self.broker}),
        )

        self.redis = _redis_client()
        self.dedup = DedupGate(
            self.redis,
            ttl_s=profile.dedup_ttl_s,
            enabled=_redis_hot_state_enabled(),
        )
        self.resolver = TenantResolver()
        self.normalizer = Normalizer(self.resolver, lateness_window_s=correlation.lateness_window_s)
        self.state_ttl_s = profile.redis_state_ttl_s
        self.hot_state_enabled = _redis_hot_state_enabled()

    # -- loop ------------------------------------------------------------
    def run(self) -> None:
        self.resolver.refresh(force=True)
        LOGGER.info(
            "validator starting",
            extra={"broker": self.broker, "dedup": self.dedup.stats()},
        )
        self.consumer.run(self.handle_batch)

    def handle_batch(self, messages: list[Any]) -> None:
        """Process one batch. Raising aborts the commit and replays the batch."""
        canonical_batch: list[CanonicalEvent] = []
        self._trace_contexts = {}
        claims: list[tuple[CanonicalEvent, str]] = []
        self._erased_batch = self.redis.smembers("tf:erased:vehicles") if self.redis is not None else set()
        with metrics.processing_seconds.labels(role="validator").time():
            for message, payload in decode_json_batch(messages):
                event = self.process_message(message, payload)
                if event is not None:
                    canonical_batch.append(event)
                    claims.append((event, f"{message.topic()}:{message.partition()}:{message.offset()}"))

            for event in canonical_batch:
                self.publish_canonical(event)

            # Flush before returning so the consumer only commits offsets for
            # events the broker has actually accepted.
            outstanding = self.producer.flush(30)
            if outstanding:
                raise RuntimeError(f"{outstanding} messages unflushed; refusing to commit offsets")
            for event, claim_id in claims:
                self.dedup.complete(event.tenant_id, event.event_key, claim_id)

    def process_message(self, message: Any, payload: dict[str, Any] | None) -> CanonicalEvent | None:
        """Validate, dedup, and normalise a single message."""
        with otel.consumer_span(
            "validator.process",
            headers=message.headers(),
            **{"messaging.source.name": kafka_helpers.TELEMETRY_RAW},
        ):
            try:
                raw, schema_transformed = validate(payload)
                if raw.vehicle_id in getattr(self, "_erased_batch", set()):
                    return None
            except QuarantineError as error:
                self.quarantine(message, payload, error)
                return None

            event = self.normalizer.to_canonical(
                raw,
                ingested_at=kafka_timestamp(message),
                schema_transformed=schema_transformed,
            )

            verdict = self.dedup.check(
                event.tenant_id, event.event_key, f"{message.topic()}:{message.partition()}:{message.offset()}"
            )
            if verdict.is_duplicate:
                # Suppressed before the detectors and before ClickHouse. The
                # counter is the audit trail; the event itself adds nothing.
                LOGGER.debug(
                    "duplicate suppressed",
                    extra={
                        "vehicle_id": event.vehicle_id,
                        "event_key": event.event_key,
                        "backend": verdict.backend,
                    },
                )
                return None

            metrics.events_accepted_total.labels(tenant=event.tenant_id).inc()
            if self.hot_state_enabled:
                self.update_hot_state(event)
            if hasattr(self, "_trace_contexts"):
                self._trace_contexts[event.event_key] = otel.inject_headers()
            return event

    # -- outputs ---------------------------------------------------------
    def publish_canonical(self, event: CanonicalEvent) -> None:
        with otel.producer_span(
            "validator.publish",
            headers=getattr(self, "_trace_contexts", {}).get(event.event_key),
            **{"messaging.destination.name": kafka_helpers.TELEMETRY_CANONICAL},
        ):
            self.producer.produce(
                kafka_helpers.TELEMETRY_CANONICAL,
                key=event.vehicle_id,
                value=event.model_dump_json().encode("utf-8"),
                headers=otel.inject_headers(),
            )
            self.producer.poll(0)

    def quarantine(self, message: Any, payload: dict[str, Any] | None, error: QuarantineError) -> None:
        """Publish an invalid payload with its reason, preserving the original."""
        record = QuarantineRecord(
            reason_code=error.reason,
            reason_detail=error.detail,
            original_payload=_original(message),
            received_at=now(),
            vehicle_id=(payload or {}).get("vehicle_id") if isinstance(payload, dict) else None,
            source_topic=message.topic(),
            source_partition=message.partition(),
            source_offset=message.offset(),
        )
        metrics.events_rejected_total.labels(reason=error.reason.value).inc()
        LOGGER.info(
            "event quarantined",
            extra={
                "reason_code": error.reason.value,
                "vehicle_id": record.vehicle_id,
                "offset": message.offset(),
            },
        )
        self.producer.produce(
            kafka_helpers.TELEMETRY_QUARANTINE,
            key=record.vehicle_id or message.key(),
            value=record.model_dump_json().encode("utf-8"),
            headers=otel.inject_headers(),
        )
        self.producer.poll(0)

    def update_hot_state(self, event: CanonicalEvent) -> None:
        """Write the vehicle's latest state for ``GET /vehicles/{id}/state``.

        A Redis failure here is logged but not raised: hot state is a cache, and
        losing a dashboard update must not stop the event reaching ClickHouse and
        the incident engine.
        """
        if self.redis is None:
            return
        key = f"tf:{event.tenant_id}:veh:{event.vehicle_id}:state"
        try:
            with metrics.redis_operation_seconds.labels(operation="hset_state").time():
                pipeline = self.redis.pipeline()
                mapping = {
                    "vehicle_id": event.vehicle_id,
                    "tenant_id": event.tenant_id,
                    "fleet_id": event.fleet_id,
                    "speed": str(event.speed_kmh),
                    "lat": str(event.lat),
                    "lon": str(event.lon),
                    "geohash": event.geohash,
                    "heading": str(event.heading_deg),
                    "soc": str(event.soc_pct if event.soc_pct is not None else ""),
                    "odo_km": str(event.odo_km if event.odo_km is not None else ""),
                    "rpm": str(event.rpm if event.rpm is not None else ""),
                    "dtc": json.dumps(event.dtc or []),
                    "evt": event.evt or "",
                    "last_ts": to_iso(event.ts),
                    "ingested_at": to_iso(event.ingested_at),
                    "late": "1" if event.flags.get("late") else "0",
                }
                # Atomic event-time ordering: a delayed sample must not rewind the map.
                arguments = [str(event.ts.timestamp()), str(self.state_ttl_s)]
                for field, value in mapping.items():
                    arguments.extend([field, value])
                pipeline.eval(
                    """
                    local previous = tonumber(redis.call('HGET', KEYS[1], '_event_epoch') or '-1')
                    if tonumber(ARGV[1]) < previous then return 0 end
                    redis.call('HSET', KEYS[1], '_event_epoch', ARGV[1])
                    for i = 3, #ARGV, 2 do redis.call('HSET', KEYS[1], ARGV[i], ARGV[i+1]) end
                    redis.call('EXPIRE', KEYS[1], ARGV[2])
                    return 1
                """,
                    1,
                    key,
                    *arguments,
                )
                # A rolling counter the ops page reads as live events/s.
                rate_key = f"tf:{event.tenant_id}:metrics:rate"
                pipeline.hincrby(rate_key, str(int(event.processed_at.timestamp())), 1)
                pipeline.expire(rate_key, 300)
                pipeline.execute()
        except Exception as error:
            LOGGER.warning("hot state update failed", extra={"error": str(error)})


def _original(message: Any) -> str:
    value = message.value()
    if value is None:
        return ""
    return value.decode("utf-8", errors="replace")[:8192]


def _redis_client() -> Any:
    try:
        import redis

        return redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    except Exception as error:  # pragma: no cover
        LOGGER.warning("redis unavailable at startup", extra={"error": str(error)})
        return None


def _redis_hot_state_enabled() -> bool:
    from packages.common import feature_registry

    return feature_registry.is_enabled("redis_hot_state")
