"""Prometheus metric definitions shared by every service.

Metrics are declared once here rather than at their point of use so that the
names in ``infra/prometheus/tracefleet-alerts.yml`` and the Grafana dashboards
have a single source of truth, and so importing a module twice cannot raise a
duplicate-registration error.

If ``prometheus_client`` is unavailable the module degrades to no-op stand-ins.
The processor must keep moving events even when its observability sidecar is
missing.
"""

from __future__ import annotations

import os
from typing import Any

try:  # pragma: no cover - exercised implicitly by the presence of the dependency
    from prometheus_client import (
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        start_http_server,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

    class _NoOp:
        """Accepts every metric call and does nothing."""

        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def labels(self, *_: Any, **__: Any) -> _NoOp:
            return self

        def inc(self, *_: Any, **__: Any) -> None:
            pass

        def dec(self, *_: Any, **__: Any) -> None:
            pass

        def set(self, *_: Any, **__: Any) -> None:
            pass

        def observe(self, *_: Any, **__: Any) -> None:
            pass

        def time(self) -> _NoOp:
            return self

        def __enter__(self) -> _NoOp:
            return self

        def __exit__(self, *_: Any) -> None:
            pass

    Counter = Gauge = Histogram = _NoOp  # type: ignore[assignment,misc]
    CollectorRegistry = _NoOp  # type: ignore[assignment,misc]

    def start_http_server(*_: Any, **__: Any) -> None:  # type: ignore[misc]
        return None


_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
#: End-to-end buckets straddle the PRD's 2 s dashboard and 5 s alert targets so
#: the SLO shows up directly as a bucket boundary in Grafana.
_PIPELINE_BUCKETS = (0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 15.0, 30.0, 60.0)

# -- Ingest ---------------------------------------------------------------
events_accepted_total = Counter("tracefleet_events_accepted_total", "Raw events that passed validation", ["tenant"])
events_rejected_total = Counter("tracefleet_events_rejected_total", "Raw events routed to quarantine", ["reason"])
events_duplicated_total = Counter(
    "tracefleet_events_duplicated_total", "Events suppressed by the dedup gate", ["tenant"]
)
events_late_total = Counter("tracefleet_events_late_total", "Events whose lateness exceeded the window", ["tenant"])
events_dlq_total = Counter("tracefleet_events_dlq_total", "Poison messages routed to a dead-letter topic", ["topic"])
schema_transformed_total = Counter(
    "tracefleet_schema_transformed_total", "Events upgraded from an older schema", ["from_version"]
)

# -- Consumers ------------------------------------------------------------
consumer_lag = Gauge(
    "tracefleet_consumer_lag",
    "Messages between committed offset and high watermark",
    ["group", "topic", "partition"],
)
consumer_batch_size = Histogram(
    "tracefleet_consumer_batch_size",
    "Messages returned per poll",
    ["group"],
    buckets=(1, 5, 10, 50, 100, 250, 500, 1000),
)
consumer_paused = Gauge("tracefleet_consumer_paused", "1 while a consumer has paused its partitions", ["group"])
consumer_commit_failures_total = Counter(
    "tracefleet_consumer_commit_failures_total", "Offset commit failures", ["group"]
)
processing_seconds = Histogram(
    "tracefleet_processing_seconds", "Time to process one batch", ["role"], buckets=_LATENCY_BUCKETS
)

# -- Detection ------------------------------------------------------------
signals_detected_total = Counter("tracefleet_signals_detected_total", "Detector signals raised", ["detector"])
incidents_detected_total = Counter(
    "tracefleet_incidents_detected_total", "Incidents crossing the trigger threshold", ["severity"]
)
incidents_suppressed_total = Counter(
    "tracefleet_incidents_suppressed_total", "Candidate incidents suppressed", ["reason"]
)
incident_reconstruction_seconds = Histogram(
    "tracefleet_incident_reconstruction_seconds",
    "Time from detection to OPEN",
    buckets=_PIPELINE_BUCKETS,
)
incident_state_transitions_total = Counter(
    "tracefleet_incident_state_transitions_total",
    "Incident lifecycle transitions",
    ["from_state", "to_state"],
)
event_to_alert_seconds = Histogram(
    "tracefleet_event_to_alert_seconds",
    "Event time to critical alert publish",
    buckets=_PIPELINE_BUCKETS,
)
event_to_dashboard_seconds = Histogram(
    "tracefleet_event_to_dashboard_seconds",
    "Event time to websocket push",
    buckets=_PIPELINE_BUCKETS,
)
active_vehicle_windows = Gauge("tracefleet_active_vehicle_windows", "Per-vehicle windows held in memory")
window_evictions_total = Counter("tracefleet_window_evictions_total", "Windows evicted by the memory guard", ["reason"])
late_evidence_added_total = Counter(
    "tracefleet_late_evidence_added_total", "Incidents re-reconstructed after a late event"
)

# -- Stores ---------------------------------------------------------------
clickhouse_insert_seconds = Histogram(
    "tracefleet_clickhouse_insert_seconds", "Batch insert latency", buckets=_LATENCY_BUCKETS
)
clickhouse_rows_inserted_total = Counter("tracefleet_clickhouse_rows_inserted_total", "Rows written to ClickHouse")
clickhouse_failures_total = Counter(
    "tracefleet_clickhouse_failures_total", "Failed ClickHouse operations", ["operation"]
)
clickhouse_query_seconds = Histogram(
    "tracefleet_clickhouse_query_seconds",
    "Analytics query latency",
    ["query"],
    buckets=_LATENCY_BUCKETS,
)
postgres_operation_seconds = Histogram(
    "tracefleet_postgres_operation_seconds",
    "Persister operation latency",
    ["operation"],
    buckets=_LATENCY_BUCKETS,
)
postgres_failures_total = Counter("tracefleet_postgres_failures_total", "Failed Postgres operations", ["operation"])
redis_operation_seconds = Histogram(
    "tracefleet_redis_operation_seconds",
    "Redis operation latency",
    ["operation"],
    buckets=(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
)
redis_cache_hits_total = Counter("tracefleet_redis_cache_hits_total", "Cache hits", ["cache"])
redis_cache_misses_total = Counter("tracefleet_redis_cache_misses_total", "Cache misses", ["cache"])

# -- Producer -------------------------------------------------------------
producer_events_total = Counter("tracefleet_producer_events_total", "Events published by the simulator", ["scenario"])
producer_errors_total = Counter("tracefleet_producer_errors_total", "Publish failures", ["reason"])
producer_rate = Gauge("tracefleet_producer_rate_events_per_second", "Measured simulator output rate", ["worker"])

# -- AI -------------------------------------------------------------------
ai_requests_total = Counter("tracefleet_ai_requests_total", "Summary generation attempts", ["provider", "outcome"])
ai_latency_seconds = Histogram(
    "tracefleet_ai_latency_seconds",
    "Provider round-trip latency",
    ["provider"],
    buckets=_LATENCY_BUCKETS,
)
ai_validation_failures_total = Counter(
    "tracefleet_ai_validation_failures_total", "Statements rejected by the validator", ["reason"]
)
ai_tokens_total = Counter("tracefleet_ai_tokens_total", "Tokens reported by the provider", ["provider", "kind"])

# -- Service health -------------------------------------------------------
service_up = Gauge("tracefleet_service_up", "1 while the service loop is running", ["service"])
service_restarts_total = Counter("tracefleet_service_restarts_total", "Recovered loop restarts", ["service"])


_server_started = False


def start_metrics_server(port: int | None = None, service: str | None = None) -> int | None:
    """Expose ``/metrics`` on a background thread. Safe to call more than once."""
    global _server_started
    if not PROMETHEUS_AVAILABLE:
        return None
    resolved_port = int(port if port is not None else (os.getenv("METRICS_PORT") or "9108"))
    if _server_started:
        return resolved_port
    try:
        start_http_server(resolved_port)
        _server_started = True
    except OSError:
        # Another role in the same container already bound the port. Metrics are
        # still collected, just served by the first listener.
        return None
    if service:
        service_up.labels(service=service).set(1)
    return resolved_port


def mark_down(service: str) -> None:
    service_up.labels(service=service).set(0)
