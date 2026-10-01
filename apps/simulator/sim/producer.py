"""Rate-controlled, multi-process telemetry producer.

One Python process cannot serialise and publish 100,000 events per second, so the
fleet is sharded across worker processes — each owns a contiguous slice of vehicle
indices and its own Kafka producer. Sharding by index rather than round-robin keeps a
vehicle's events on one worker and therefore in sequence.

Rate control is a closed loop, not a fixed sleep. Each worker measures what it
actually achieved and adjusts, so the reported rate is the real one. When the host
cannot reach the target the deficit is recorded rather than hidden — that honest
measurement is what ``docs/BENCHMARKS.md`` is built from.
"""

from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Any

from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.common.timeutil import now

LOGGER = tf_logging.get_logger(__name__)

#: How often a worker reports its measured rate.
REPORT_INTERVAL_S = 5.0
#: Ceiling on the producer's internal queue before it blocks. Bounded so a stalled
#: broker applies back-pressure to the generator instead of growing memory.
MAX_QUEUED = 200_000


@dataclass
class WorkerStats:
    worker_id: int
    events: int = 0
    errors: int = 0
    elapsed_s: float = 0.0
    achieved_rate: float = 0.0
    target_rate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "worker_id": self.worker_id,
            "events": self.events,
            "errors": self.errors,
            "elapsed_s": round(self.elapsed_s, 2),
            "achieved_rate": round(self.achieved_rate, 1),
            "target_rate": round(self.target_rate, 1),
            "percent_of_target": (round((self.achieved_rate / self.target_rate) * 100, 1) if self.target_rate else 0.0),
        }


@dataclass
class RunPlan:
    """Everything a worker needs to run its shard."""

    broker: str
    topic: str
    vehicles: int
    target_events_per_s: float
    duration_s: float
    workers: int
    seed: int
    padding_bytes: int = 0
    burst_multiplier: float = 1.0
    burst_after_s: float = 0.0
    burst_duration_s: float = 0.0
    scenario_mix: dict[str, float] = field(default_factory=dict)
    tenant_id: str = "tenant_default"


class RateController:
    """Closed-loop pacing for one worker.

    Tracks cumulative deficit against the ideal schedule and sleeps only when ahead.
    A naive ``sleep(batch / rate)`` drifts, because it ignores the time generation
    itself took and the error compounds over a long run.
    """

    def __init__(self, target_rate: float) -> None:
        self.target_rate = max(target_rate, 0.001)
        self.started = time.perf_counter()
        self.emitted = 0

    def record(self, count: int) -> None:
        self.emitted += count

    def sleep_if_ahead(self) -> float:
        """Sleep to stay on schedule. Returns the seconds slept."""
        elapsed = time.perf_counter() - self.started
        expected_elapsed = self.emitted / self.target_rate
        drift = expected_elapsed - elapsed
        if drift > 0.001:
            time.sleep(min(drift, 1.0))
            return drift
        return 0.0

    @property
    def achieved_rate(self) -> float:
        elapsed = time.perf_counter() - self.started
        return self.emitted / elapsed if elapsed > 0 else 0.0

    def retarget(self, target_rate: float) -> None:
        """Change the target mid-run, for burst mode.

        The baseline is rebased so the burst does not inherit the pre-burst
        schedule and immediately try to "catch up".
        """
        self.target_rate = max(target_rate, 0.001)
        self.started = time.perf_counter()
        self.emitted = 0


def _worker(plan: RunPlan, worker_id: int, result_queue: Any) -> None:
    """Run one shard. Executed in a child process."""
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    for path in (str(root), str(root / "apps" / "simulator")):
        if path not in sys.path:
            sys.path.insert(0, path)

    from sim.vehicle_model import build_fleet, step, to_event

    tf_logging.configure(f"tracefleet-simulator-{worker_id}")
    otel.configure(f"tracefleet-simulator-{worker_id}")
    stop = {"requested": False}

    def request_stop(*_: Any) -> None:
        stop["requested"] = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(ValueError, OSError):
            signal.signal(sig, request_stop)

    # Contiguous shard, so each vehicle belongs to exactly one worker.
    shard_size = plan.vehicles // plan.workers
    remainder = plan.vehicles % plan.workers
    offset = (worker_id * shard_size) + min(worker_id, remainder)
    count = shard_size + (1 if worker_id < remainder else 0)
    if count <= 0:
        result_queue.put(WorkerStats(worker_id=worker_id).to_dict())
        return

    fleet = build_fleet(count, seed=plan.seed, offset=offset)
    worker_rate = plan.target_events_per_s / plan.workers
    controller = RateController(worker_rate)

    producer = kafka_helpers.build_producer(
        **{
            "bootstrap.servers": plan.broker,
            "queue.buffering.max.messages": MAX_QUEUED,
        }
    )

    stats = WorkerStats(worker_id=worker_id, target_rate=worker_rate)
    started = time.perf_counter()
    last_report = started
    in_burst = False

    try:
        while not stop["requested"]:
            elapsed = time.perf_counter() - started
            if plan.duration_s and elapsed >= plan.duration_s:
                break

            # Burst window: the same code path at a higher target, which is what
            # makes the burst a genuine load test rather than a separate mode.
            should_burst = (
                plan.burst_multiplier > 1.0
                and plan.burst_duration_s > 0
                and plan.burst_after_s <= elapsed < (plan.burst_after_s + plan.burst_duration_s)
            )
            if should_burst and not in_burst:
                controller.retarget(worker_rate * plan.burst_multiplier)
                in_burst = True
                LOGGER.info("burst started", extra={"worker": worker_id, "multiplier": plan.burst_multiplier})
            elif not should_burst and in_burst:
                controller.retarget(worker_rate)
                in_burst = False
                LOGGER.info("burst ended", extra={"worker": worker_id})

            timestamp = now()
            published = 0
            for vehicle in fleet:
                event = to_event(step(vehicle), timestamp, padding_bytes=plan.padding_bytes)
                if _publish(producer, plan.topic, event):
                    published += 1
                else:
                    stats.errors += 1

            stats.events += published
            controller.record(published)
            producer.poll(0)
            controller.sleep_if_ahead()

            if (time.perf_counter() - last_report) >= REPORT_INTERVAL_S:
                last_report = time.perf_counter()
                metrics.producer_rate.labels(worker=str(worker_id)).set(controller.achieved_rate)
                LOGGER.info(
                    "worker progress",
                    extra={
                        "worker": worker_id,
                        "events": stats.events,
                        "achieved_rate": round(controller.achieved_rate, 1),
                        "target_rate": round(controller.target_rate, 1),
                        "bursting": in_burst,
                    },
                )
    finally:
        producer.flush(30)
        otel.shutdown()
        stats.elapsed_s = time.perf_counter() - started
        stats.achieved_rate = stats.events / stats.elapsed_s if stats.elapsed_s > 0 else 0.0
        result_queue.put(stats.to_dict())


def _publish(producer: Any, topic: str, event: dict[str, Any]) -> bool:
    """Publish one event, blocking briefly when the local queue is full.

    A full queue means the broker is not keeping up. Polling rather than dropping
    lets back-pressure reach the generator, which is the behaviour the load test is
    supposed to measure.
    """
    payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
    key = event["vehicle_id"]
    with otel.producer_span("simulator.publish", **{"messaging.destination.name": topic}):
        headers = otel.inject_headers()
        for _ in range(50):
            try:
                producer.produce(topic, key=key, value=payload, headers=headers)
                metrics.producer_events_total.labels(scenario="baseline").inc()
                return True
            except BufferError:
                producer.poll(0.05)
            except Exception as error:
                metrics.producer_errors_total.labels(reason=type(error).__name__).inc()
                return False
        metrics.producer_errors_total.labels(reason="queue_full").inc()
        return False


def run(plan: RunPlan) -> dict[str, Any]:
    """Run the plan across worker processes and return the aggregate measurement."""
    workers = max(1, plan.workers)
    LOGGER.info(
        "starting producer",
        extra={
            "workers": workers,
            "vehicles": plan.vehicles,
            "target_events_per_s": plan.target_events_per_s,
            "duration_s": plan.duration_s,
            "padding_bytes": plan.padding_bytes,
        },
    )

    if workers == 1:
        # In-process for a single worker: simpler to debug and avoids the spawn cost
        # for the common dev case.
        queue: Any = _SimpleQueue()
        _worker(plan, 0, queue)
        results = [queue.get()]
    else:
        # "spawn" rather than "fork": forking a process that already holds a Kafka
        # producer or a database connection inherits sockets that must not be shared.
        context = mp.get_context("spawn")
        queue = context.Queue()
        processes = [
            context.Process(target=_worker, args=(plan, index, queue), daemon=False) for index in range(workers)
        ]
        for process in processes:
            process.start()
        results = []
        for _ in processes:
            try:
                results.append(queue.get(timeout=(plan.duration_s or 60) + 120))
            except Exception:
                break
        for process in processes:
            process.join(timeout=30)
            if process.is_alive():
                process.terminate()

    return _aggregate(plan, results)


def _aggregate(plan: RunPlan, results: list[dict[str, Any]]) -> dict[str, Any]:
    total_events = sum(item.get("events", 0) for item in results)
    total_errors = sum(item.get("errors", 0) for item in results)
    elapsed = max((item.get("elapsed_s", 0.0) for item in results), default=0.0)
    achieved = total_events / elapsed if elapsed > 0 else 0.0

    report = {
        "profile_target_events_per_s": plan.target_events_per_s,
        "measured_events_per_s": round(achieved, 1),
        "percent_of_target": (
            round((achieved / plan.target_events_per_s) * 100, 1) if plan.target_events_per_s else 0.0
        ),
        "total_events": total_events,
        "errors": total_errors,
        "elapsed_seconds": round(elapsed, 2),
        "workers": plan.workers,
        "vehicles": plan.vehicles,
        "event_padding_bytes": plan.padding_bytes,
        "per_worker": results,
    }
    LOGGER.info("producer finished", extra={k: v for k, v in report.items() if k != "per_worker"})
    if plan.target_events_per_s and achieved < plan.target_events_per_s * 0.9:
        # Stated plainly. The PRD requires an honest measurement with conditions,
        # not a claim that the target was met.
        LOGGER.warning(
            "target rate not reached; record the measured ceiling and host conditions",
            extra={
                "measured_events_per_s": round(achieved, 1),
                "target": plan.target_events_per_s,
                "cpu_count": os.cpu_count(),
            },
        )
    return report


class _SimpleQueue:
    """Minimal queue for the single-worker in-process path."""

    def __init__(self) -> None:
        self._items: list[Any] = []

    def put(self, item: Any) -> None:
        self._items.append(item)

    def get(self, timeout: float | None = None) -> Any:
        return self._items.pop(0)


def resolve_workers(requested: int, profile_workers: int) -> int:
    """Worker count: explicit flag, then profile, then CPU count."""
    if requested > 0:
        return requested
    if profile_workers > 0:
        return profile_workers
    return max(1, os.cpu_count() or 1)
