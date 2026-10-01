"""Batch consumer loop with manual commits, back-pressure, and lag metrics.

Every role runs on this class, so the delivery guarantees are implemented once.
The contract:

* **Bounded batches.** ``consume(max_batch, timeout)`` instead of one message at
  a time, which is what makes 100K events/s achievable in Python.
* **Commit after work.** Offsets advance only once the handler has succeeded and
  produced/flushed downstream. At-least-once delivery plus idempotent writes
  gives effectively-once processing.
* **A failure never advances the offset.** The original loop committed inside a
  bare ``except``, which silently dropped an event whenever Redis blinked. Here a
  failed batch is retried in place, and only a message that keeps failing past
  ``max_retries`` goes to the dead-letter topic.
* **Back-pressure, not dropping.** When a downstream store is unavailable the
  consumer pauses its partitions and stops fetching. Kafka is the buffer; the
  broker retains the data and lag grows visibly instead of data disappearing.
* **Graceful shutdown.** SIGTERM finishes the current batch, commits, and closes,
  so a ``docker compose restart`` does not replay a batch.
"""

from __future__ import annotations

import json
import signal
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics
from packages.common.timeutil import now

LOGGER = tf_logging.get_logger(__name__)

#: How long to back off between retries of a failing batch, in seconds.
_BACKOFF_SCHEDULE = (0.5, 1.0, 2.0, 5.0, 10.0, 30.0)


class ShutdownRequested(Exception):
    """Raised internally to unwind the poll loop on SIGTERM/SIGINT."""


class BatchConsumer:
    """Kafka consumer that hands bounded batches to a handler."""

    def __init__(
        self,
        topics: Sequence[str],
        group_id: str,
        *,
        handler: Callable[[list[Any]], None] | None = None,
        max_batch: int | None = None,
        max_wait_ms: int | None = None,
        max_retries: int = 3,
        producer: Any = None,
        consumer: Any = None,
        lag_interval_s: float = 10.0,
    ) -> None:
        tuning = kafka_helpers.ConsumerTuning.from_profile()
        self.topics = list(topics)
        self.group_id = group_id
        self.handler = handler
        self.max_batch = int(max_batch or tuning.max_batch)
        self.max_wait_s = float(max_wait_ms or tuning.max_wait_ms) / 1000.0
        self.max_retries = int(max_retries)
        self.lag_interval_s = lag_interval_s

        self._consumer = consumer or kafka_helpers.build_consumer(group_id)
        self._producer = producer
        self._running = False
        self._paused = False
        self._shutdown = False
        self._failures = 0
        self._last_lag_report = 0.0
        self._install_signal_handlers()

    # -- lifecycle -------------------------------------------------------
    def _install_signal_handlers(self) -> None:
        def request_shutdown(signum: int, _frame: Any) -> None:
            LOGGER.info("shutdown signal received", extra={"signal": signum, "group": self.group_id})
            self._shutdown = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:  # noqa: SIM105 - best-effort shutdown/fallback boundary
                signal.signal(sig, request_shutdown)
            except (ValueError, OSError):
                # Not the main thread (tests, embedded use). Cooperative
                # shutdown via stop() still works.
                pass

    def stop(self) -> None:
        self._shutdown = True

    @property
    def should_stop(self) -> bool:
        return self._shutdown

    def close(self) -> None:
        try:
            if self._producer is not None:
                self._producer.flush(10)
        finally:
            try:  # noqa: SIM105 - best-effort shutdown/fallback boundary
                self._consumer.close()
            except Exception:  # pragma: no cover
                pass

    # -- main loop -------------------------------------------------------
    def run(self, handler: Callable[[list[Any]], None] | None = None) -> None:
        """Poll until shutdown, passing each batch to ``handler``."""
        work = handler or self.handler
        if work is None:
            raise ValueError("BatchConsumer.run requires a handler")

        self._consumer.subscribe(self.topics)
        self._running = True
        metrics.service_up.labels(service=self.group_id).set(1)
        LOGGER.info(
            "consumer started",
            extra={"group": self.group_id, "topics": self.topics, "max_batch": self.max_batch},
        )
        try:
            while not self._shutdown:
                batch = self._poll_batch()
                if not batch:
                    owner = getattr(work, "__self__", None)
                    if owner is not None and hasattr(owner, "run_maintenance"):
                        owner.run_maintenance()
                    self._report_lag()
                    continue
                metrics.consumer_batch_size.labels(group=self.group_id).observe(len(batch))
                self._process_with_retry(batch, work)
                self._report_lag()
        finally:
            self._running = False
            metrics.mark_down(self.group_id)
            self.close()
            LOGGER.info("consumer stopped", extra={"group": self.group_id})

    def _poll_batch(self) -> list[Any]:
        """Fetch up to ``max_batch`` healthy messages.

        ``consume()`` returns errored messages inline; partition EOF and
        transient broker errors are logged and skipped rather than treated as
        payloads.
        """
        try:
            messages = self._consumer.consume(num_messages=self.max_batch, timeout=max(self.max_wait_s, 0.05))
        except Exception as error:  # pragma: no cover - driver-level failure
            LOGGER.warning("poll failed", extra={"group": self.group_id, "error": str(error)})
            time.sleep(0.5)
            return []

        healthy: list[Any] = []
        for message in messages or []:
            error = message.error()
            if error is None:
                healthy.append(message)
                continue
            if error.code() == _partition_eof_code():
                continue
            LOGGER.warning("consumer error", extra={"group": self.group_id, "error": str(error)})
        return healthy

    def _process_with_retry(self, batch: list[Any], work: Callable[[list[Any]], None]) -> None:
        """Run the handler, retrying the whole batch before isolating a poison message."""
        attempt = 0
        while not self._shutdown:
            try:
                work(batch)
                self._commit(batch)
                self._failures = 0
                self.resume()
                return
            except BackPressure as signal_error:
                # The handler is telling us a downstream store is unavailable.
                # Hold the offset, pause, and let lag build rather than dropping.
                self.pause()
                LOGGER.warning(
                    "back-pressure, pausing partitions",
                    extra={"group": self.group_id, "reason": str(signal_error)},
                )
                time.sleep(self._backoff(attempt))
                attempt += 1
                continue
            except Exception as error:
                attempt += 1
                LOGGER.warning(
                    "batch failed",
                    extra={"group": self.group_id, "attempt": attempt, "error": str(error)},
                )
                if attempt > self.max_retries:
                    # Repeated failure on a whole batch is usually one bad
                    # message. Isolate it so a single payload cannot wedge a
                    # partition indefinitely.
                    self._isolate_poison(batch, work, error)
                    return
                time.sleep(self._backoff(attempt - 1))

    def _isolate_poison(
        self,
        batch: list[Any],
        work: Callable[[list[Any]], None],
        original_error: Exception,
    ) -> None:
        survivors: list[Any] = []
        for message in batch:
            try:
                work([message])
                survivors.append(message)
            except BackPressure:
                raise
            except Exception as error:
                self._to_dlq(message, error)
        producer = self._ensure_producer()
        if producer is None or producer.flush(30):
            raise BackPressure("dead-letter messages have not been stored")
        self._commit(batch)
        LOGGER.error(
            "batch isolated after repeated failure",
            extra={
                "group": self.group_id,
                "batch_size": len(batch),
                "recovered": len(survivors),
                "dead_lettered": len(batch) - len(survivors),
                "error": str(original_error),
            },
        )

    def _to_dlq(self, message: Any, error: Exception) -> None:
        topic = kafka_helpers.dlq_topic(message.topic())
        payload = {
            "reason": str(error),
            "error_type": type(error).__name__,
            "source_topic": message.topic(),
            "source_partition": message.partition(),
            "source_offset": message.offset(),
            "failed_at": now().isoformat(),
            "payload": _decode(message.value()),
        }
        metrics.events_dlq_total.labels(topic=message.topic()).inc()
        LOGGER.error("message dead-lettered", extra={"group": self.group_id, "dlq_topic": topic, **payload})
        producer = self._ensure_producer()
        if producer is None:
            raise BackPressure("dead-letter producer unavailable")
        try:
            producer.produce(topic, key=message.key(), value=json.dumps(payload).encode("utf-8"))
            producer.poll(0)
        except Exception as publish_error:  # pragma: no cover
            LOGGER.error("failed to publish to dlq", extra={"error": str(publish_error)})
            raise BackPressure("dead-letter publish failed") from publish_error

    def _ensure_producer(self) -> Any:
        if self._producer is None:
            try:
                self._producer = kafka_helpers.build_producer()
            except Exception:  # pragma: no cover
                return None
        return self._producer

    @staticmethod
    def _backoff(attempt: int) -> float:
        return _BACKOFF_SCHEDULE[min(attempt, len(_BACKOFF_SCHEDULE) - 1)]

    # -- offsets ---------------------------------------------------------
    def _commit(self, batch: list[Any]) -> None:
        """Commit the highest offset per partition in the batch."""
        if not batch:
            return
        try:
            from confluent_kafka import TopicPartition

            highest: dict[tuple[str, int], int] = {}
            for message in batch:
                key = (message.topic(), message.partition())
                highest[key] = max(highest.get(key, -1), message.offset() + 1)
            self._consumer.commit(
                offsets=[TopicPartition(topic, partition, offset) for (topic, partition), offset in highest.items()],
                asynchronous=False,
            )
        except Exception as error:
            metrics.consumer_commit_failures_total.labels(group=self.group_id).inc()
            LOGGER.warning("offset commit failed", extra={"group": self.group_id, "error": str(error)})
            raise BackPressure("offset commit failed") from error

    # -- back-pressure ---------------------------------------------------
    def pause(self) -> None:
        if self._paused:
            return
        try:
            assignment = self._consumer.assignment()
            if assignment:
                self._consumer.pause(assignment)
            self._paused = True
            metrics.consumer_paused.labels(group=self.group_id).set(1)
        except Exception as error:  # pragma: no cover
            LOGGER.warning("pause failed", extra={"group": self.group_id, "error": str(error)})

    def resume(self) -> None:
        if not self._paused:
            return
        try:
            assignment = self._consumer.assignment()
            if assignment:
                self._consumer.resume(assignment)
            self._paused = False
            metrics.consumer_paused.labels(group=self.group_id).set(0)
            LOGGER.info("partitions resumed", extra={"group": self.group_id})
        except Exception as error:  # pragma: no cover
            LOGGER.warning("resume failed", extra={"group": self.group_id, "error": str(error)})

    # -- observability ---------------------------------------------------
    def _report_lag(self) -> None:
        """Publish ``consumer_lag`` per assigned partition.

        Rate-limited because ``get_watermark_offsets`` is a broker round trip and
        calling it per batch at high throughput measurably slows the loop.
        """
        elapsed = time.monotonic() - self._last_lag_report
        if elapsed < self.lag_interval_s:
            return
        self._last_lag_report = time.monotonic()
        try:
            assignment = self._consumer.assignment()
            if not assignment:
                return
            committed = self._consumer.committed(assignment, timeout=5)
            for partition in committed:
                low, high = self._consumer.get_watermark_offsets(partition, timeout=5, cached=False)
                # A partition with no commit yet reports the whole backlog.
                position = partition.offset if partition.offset >= 0 else low
                metrics.consumer_lag.labels(
                    group=self.group_id,
                    topic=partition.topic,
                    partition=str(partition.partition),
                ).set(max(0, high - position))
        except Exception as error:  # pragma: no cover
            LOGGER.debug("lag report failed", extra={"group": self.group_id, "error": str(error)})


class BackPressure(Exception):
    """Raised by a handler when a downstream store cannot accept writes.

    Distinct from a generic exception because the response differs: pause and
    wait for the store to recover, rather than retry-then-dead-letter. A
    ClickHouse outage is not a poison message.
    """


def decode_json_batch(messages: Iterable[Any]) -> list[tuple[Any, dict[str, Any]]]:
    """Decode messages to (message, payload) pairs, skipping unparseable ones.

    A malformed payload is returned as ``None`` so the caller can quarantine it
    with a specific reason instead of losing it in a decode error.
    """
    decoded: list[tuple[Any, dict[str, Any] | None]] = []
    for message in messages:
        try:
            decoded.append((message, json.loads(message.value().decode("utf-8"))))
        except (UnicodeDecodeError, json.JSONDecodeError):
            decoded.append((message, None))
    return decoded  # type: ignore[return-value]


def _decode(value: bytes | None) -> str:
    if value is None:
        return ""
    return value.decode("utf-8", errors="replace")[:8192]


def _partition_eof_code() -> int:
    try:
        from confluent_kafka import KafkaError

        return KafkaError._PARTITION_EOF
    except Exception:  # pragma: no cover
        return -191
