"""Sink role: ``telemetry.canonical`` to ClickHouse.

Runs in its own consumer group so analytics lag cannot block detection. The
important property is what happens when ClickHouse is down: the batch is **not**
committed, the consumer pauses its partitions, and lag grows. Kafka is the buffer.

The previous implementation committed offsets after a failed insert, which meant
a ClickHouse outage silently discarded every event that arrived during it. That is
the one failure mode this file exists to prevent.
"""

from __future__ import annotations

import os
import time
from typing import Any

from packages.common import config as cfg
from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.common.timeutil import parse_ts
from processor.consumers import BackPressure, BatchConsumer, decode_json_batch

LOGGER = tf_logging.get_logger(__name__)

COLUMNS = (
    "tenant_id",
    "fleet_id",
    "vehicle_id",
    "event_key",
    "seq",
    "ts",
    "ingested_at",
    "processed_at",
    "lat",
    "lon",
    "geohash",
    "speed_kmh",
    "acceleration_mps2",
    "brake_force_pct",
    "heading_deg",
    "rpm",
    "soc_pct",
    "odo_km",
    "dtc",
    "evt",
    "late",
    "schema_version",
    "lateness_ms",
)


class Sink:
    """Batches canonical events into ClickHouse."""

    def __init__(self, broker: str | None = None) -> None:
        self.broker = broker or kafka_helpers.broker()
        profile = cfg.load_profile()
        self.batch_size = profile.clickhouse_batch_size
        self.flush_interval_s = profile.clickhouse_flush_ms / 1000.0

        self.consumer = BatchConsumer(
            topics=[kafka_helpers.TELEMETRY_CANONICAL, kafka_helpers.TELEMETRY_QUARANTINE],
            group_id=kafka_helpers.GROUP_SINK,
            consumer=kafka_helpers.build_consumer(kafka_helpers.GROUP_SINK, **{"bootstrap.servers": self.broker}),
        )
        import redis

        from packages.common.dsn import redis_url

        self.privacy_redis = redis.from_url(redis_url(), socket_timeout=5)
        self._client: Any = None
        self._pending: list[tuple[Any, ...]] = []
        self._last_flush = time.monotonic()
        self._consecutive_failures = 0

    # -- client ----------------------------------------------------------
    @property
    def client(self) -> Any:
        if self._client is None:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                host=os.getenv("CLICKHOUSE_HOST", "localhost"),
                port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
                user=os.getenv("CLICKHOUSE_USER", "default"),
                password=os.getenv("CLICKHOUSE_PASSWORD", ""),
                database=os.getenv("CLICKHOUSE_DB", "tracefleet"),
                connect_timeout=10,
                send_receive_timeout=60,
            )
        return self._client

    def _reset_client(self) -> None:
        """Drop the client so the next attempt reconnects."""
        try:
            if self._client is not None:
                self._client.close()
        except Exception:  # pragma: no cover
            pass
        self._client = None

    # -- loop ------------------------------------------------------------
    def run(self) -> None:
        LOGGER.info(
            "clickhouse sink starting",
            extra={"broker": self.broker, "batch_size": self.batch_size},
        )
        self.consumer.run(self.handle_batch)

    def handle_batch(self, messages: list[Any]) -> None:
        """Accumulate rows and flush when the batch or the timer is ready.

        Raising :class:`BackPressure` leaves the offsets uncommitted, so whatever
        is still buffered will be redelivered. Rows are only dropped from
        ``_pending`` after a successful insert.
        """
        with metrics.processing_seconds.labels(role="sink").time():
            self._pending = []
            quarantine_rows = []
            erased = set()
            if getattr(self, "privacy_redis", None) is not None:
                try:
                    erased = {
                        v.decode() if isinstance(v, bytes) else v
                        for v in self.privacy_redis.smembers("tf:erased:vehicles")
                    }
                except Exception as error:
                    raise BackPressure("erasure ledger unavailable") from error
            for message, payload in decode_json_batch(messages):
                if payload is None:
                    raise ValueError(f"undecodable canonical event at offset {message.offset()}")
                if payload.get("vehicle_id") in erased:
                    continue
                with otel.consumer_span("sink.row", headers=message.headers()):
                    if message.topic() == kafka_helpers.TELEMETRY_QUARANTINE:
                        quarantine_rows.append(
                            (
                                parse_ts(payload["received_at"]),
                                payload["reason_code"],
                                payload["reason_detail"],
                                payload.get("vehicle_id") or "",
                                payload["source_topic"],
                                payload["source_partition"],
                                payload["source_offset"],
                                payload["original_payload"],
                            )
                        )
                    else:
                        self._pending.append(self._to_row(payload))

            # A handler must make this batch durable before the consumer commits.
            self.flush()
            if quarantine_rows:
                try:
                    self.client.insert(
                        "telemetry_quarantine",
                        quarantine_rows,
                        column_names=[
                            "received_at",
                            "reason_code",
                            "reason_detail",
                            "vehicle_id",
                            "source_topic",
                            "source_partition",
                            "source_offset",
                            "original_payload",
                        ],
                    )
                except Exception as error:
                    self._reset_client()
                    raise BackPressure("quarantine insert unavailable") from error

    def _should_flush(self) -> bool:
        if not self._pending:
            return False
        if len(self._pending) >= self.batch_size:
            return True
        return (time.monotonic() - self._last_flush) >= self.flush_interval_s

    def flush(self) -> int:
        """Insert the pending rows. Raises BackPressure if ClickHouse rejects them."""
        if not self._pending:
            return 0
        rows = self._pending
        try:
            with metrics.clickhouse_insert_seconds.time():
                self.client.insert("telemetry_events", rows, column_names=list(COLUMNS))
        except Exception as error:
            self._consecutive_failures += 1
            metrics.clickhouse_failures_total.labels(operation="insert").inc()
            self._reset_client()
            # Exponential backoff with a ceiling, applied by the consumer's
            # back-pressure handler. The rows stay in _pending and the offsets
            # stay uncommitted, so nothing is lost.
            delay = min(30.0, 0.5 * (2 ** min(self._consecutive_failures, 6)))
            LOGGER.error(
                "clickhouse insert failed, applying back-pressure",
                extra={
                    "rows": len(rows),
                    "consecutive_failures": self._consecutive_failures,
                    "backoff_s": delay,
                    "error": str(error),
                },
            )
            raise BackPressure(f"clickhouse insert failed after {self._consecutive_failures} attempts") from error

        inserted = len(rows)
        self._pending = []
        self._last_flush = time.monotonic()
        if self._consecutive_failures:
            LOGGER.info("clickhouse recovered", extra={"after_failures": self._consecutive_failures})
            self._consecutive_failures = 0
        metrics.clickhouse_rows_inserted_total.inc(inserted)
        LOGGER.debug("clickhouse batch inserted", extra={"rows": inserted})
        return inserted

    # -- mapping ---------------------------------------------------------
    @staticmethod
    def _to_row(event: dict[str, Any]) -> tuple[Any, ...]:
        """Map a canonical event to the ClickHouse column order.

        ``event_key`` is a FixedString(64) column, so the hex digest is passed
        through unchanged rather than truncated; a shortened key would let two
        events collide on the ReplacingMergeTree sort key.
        """
        flags = event.get("flags") or {}
        return (
            event.get("tenant_id") or "",
            event.get("fleet_id") or "",
            event.get("vehicle_id") or "",
            event.get("event_key") or "",
            int(event.get("seq") or 0),
            parse_ts(event["ts"]),
            parse_ts(event["ingested_at"]),
            parse_ts(event["processed_at"]),
            float(event.get("lat") or 0.0),
            float(event.get("lon") or 0.0),
            event.get("geohash") or "",
            float(event.get("speed_kmh") or 0.0),
            float(event.get("acceleration_mps2") or 0.0),
            float(event.get("brake_force_pct") or 0.0),
            float(event.get("heading_deg") or 0.0),
            int(event.get("rpm") or 0),
            float(event.get("soc_pct") or 0.0),
            float(event.get("odo_km") or 0.0),
            list(event.get("dtc") or []),
            event.get("evt") or "",
            1 if flags.get("late") else 0,
            event.get("schema_version") or "1.0",
            int(event.get("lateness_ms") or 0),
        )
