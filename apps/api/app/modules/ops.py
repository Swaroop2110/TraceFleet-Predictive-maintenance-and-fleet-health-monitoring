"""Operations endpoints (feature ``ops_dashboard``).

Pipeline health for the Operations page: throughput against target, consumer lag,
rejected/duplicate/late counters, store health, and the chaos hooks.

Numbers come from where they are authoritative. Consumer lag lives in Prometheus
because the processor owns it; ingest lag lives in ClickHouse because that is where
the timestamps are; store health is a live probe. Nothing here is a cached estimate,
so an operator reading "lag is bounded" is reading a measurement.
"""

from __future__ import annotations

import math
import os
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from pydantic import BaseModel, Field

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import TraceFleetError
from apps.api.app.core.security import Principal, require
from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store
from packages.common import config as cfg
from packages.common import logging as tf_logging
from packages.common.dsn import redis_url

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS = ("ops_dashboard",)
router = APIRouter(prefix="/ops", tags=["ops"])

#: Prometheus is queried for the metrics the processor exports, since the API
#: cannot read another process's registry.
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")

#: Consumer groups whose lag matters to the detection path.
TRACKED_GROUPS = ("validator", "incident-processor", "telemetry-sink", "incident-persister")


def _redis() -> Any:
    try:
        import redis

        return redis.from_url(redis_url(), decode_responses=True, socket_timeout=2)
    except Exception:
        return None


def _prometheus_query(query: str) -> list[dict[str, Any]]:
    """Run an instant query against Prometheus.

    Failures return empty rather than raising: the ops page should still render the
    parts it can, and a missing monitoring stack is a local-development reality.
    """
    try:
        import httpx

        response = httpx.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5.0)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            return []
        return payload["data"]["result"]
    except Exception as error:
        LOGGER.debug("prometheus query failed", extra={"query": query, "error": str(error)})
        return []


def _scalar(query: str, default: float = 0.0) -> float:
    rows = _prometheus_query(query)
    if not rows:
        return default
    try:
        value = float(rows[0]["value"][1])
        return value if math.isfinite(value) else default
    except (KeyError, IndexError, ValueError):
        return default


@router.get("/pipeline")
def pipeline_status(
    principal: Principal = Depends(require("ops:read")),
) -> dict[str, Any]:
    """Throughput, lag, and data-quality counters for the whole pipeline."""
    profile = cfg.load_profile()

    accepted_rate = _scalar("sum(rate(tracefleet_events_accepted_total[1m]))")
    lag_rows = _prometheus_query("tracefleet_consumer_lag")
    lag_by_group: dict[str, dict[str, Any]] = {}
    for row in lag_rows:
        group = row["metric"].get("group", "unknown")
        try:
            value = float(row["value"][1])
        except (KeyError, IndexError, ValueError):
            continue
        entry = lag_by_group.setdefault(group, {"group": group, "total": 0.0, "max_partition": 0.0, "partitions": 0})
        entry["total"] += value
        entry["max_partition"] = max(entry["max_partition"], value)
        entry["partitions"] += 1

    counters = {
        "events_accepted_total": _scalar("sum(tracefleet_events_accepted_total)"),
        "events_rejected_total": _scalar("sum(tracefleet_events_rejected_total)"),
        "events_duplicated_total": _scalar("sum(tracefleet_events_duplicated_total)"),
        "events_late_total": _scalar("sum(tracefleet_events_late_total)"),
        "events_dlq_total": _scalar("sum(tracefleet_events_dlq_total)"),
        "incidents_detected_total": _scalar("sum(tracefleet_incidents_detected_total)"),
        "window_evictions_total": _scalar("sum(tracefleet_window_evictions_total)"),
        "late_evidence_added_total": _scalar("sum(tracefleet_late_evidence_added_total)"),
    }

    rejected_by_reason = {
        row["metric"].get("reason", "unknown"): float(row["value"][1])
        for row in _prometheus_query("sum by (reason) (tracefleet_events_rejected_total)")
        if row.get("value")
    }

    latency = {
        "event_to_alert_p95_s": _scalar(
            "histogram_quantile(0.95, sum(rate(tracefleet_event_to_alert_seconds_bucket[5m])) by (le))"
        ),
        "event_to_dashboard_p95_s": _scalar(
            "histogram_quantile(0.95, sum(rate(tracefleet_event_to_dashboard_seconds_bucket[5m])) by (le))"
        ),
        "reconstruction_p95_s": _scalar(
            "histogram_quantile(0.95, sum(rate(tracefleet_incident_reconstruction_seconds_bucket[5m])) by (le))"
        ),
        "api_p95_s": _scalar(
            'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{handler!="/metrics"}[5m])) by (le))'
        ),
        "api_p99_s": _scalar(
            'histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{handler!="/metrics"}[5m])) by (le))'
        ),
    }

    ingest_lag: dict[str, Any] = {}
    quarantine: list[dict[str, Any]] = []
    degraded: list[str] = []
    try:
        ingest_lag = telemetry_store.ingest_lag(principal.tenant_id, minutes=15)
        quarantine = telemetry_store.quarantine_summary(principal.tenant_id, hours=24)
    except ClickHouseUnavailable as error:
        degraded.append(f"telemetry_unavailable: {error}")

    target = profile.target_events_per_s
    return {
        "profile": {
            "name": profile.name,
            "target_events_per_s": target,
            "vehicles": profile.vehicles,
            "burst_multiplier": profile.burst_multiplier,
        },
        "throughput": {
            "events_per_second": round(accepted_rate, 2),
            "target_events_per_second": target,
            # Reported as a fraction of target rather than as a pass/fail, because
            # the ceiling is hardware-bound on a laptop and pretending otherwise
            # would be dishonest.
            "percent_of_target": round((accepted_rate / target) * 100, 2) if target else 0.0,
        },
        "consumer_lag": sorted(lag_by_group.values(), key=lambda item: item["total"], reverse=True),
        "tracked_groups": list(TRACKED_GROUPS),
        "counters": counters,
        "rejected_by_reason": rejected_by_reason,
        "latency": latency,
        "slo": {
            "dashboard_target_s": 2.0,
            "critical_alert_target_s": 5.0,
            "api_p95_target_s": 0.2,
            "api_p99_target_s": 0.5,
        },
        "ingest_lag": ingest_lag,
        "quarantine": quarantine,
        "services": _service_health(),
        "degraded": degraded,
        "metrics_source": PROMETHEUS_URL,
    }


def _service_health() -> list[dict[str, Any]]:
    """Live health for each dependency, plus processor liveness from Prometheus."""
    services: list[dict[str, Any]] = []

    from apps.api.app.db.session import pool_status

    try:
        from sqlalchemy import text

        from apps.api.app.db.session import engine

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        services.append({"name": "postgres", "status": "up", "detail": pool_status()})
    except Exception as error:
        services.append({"name": "postgres", "status": "down", "detail": {"error": str(error)[:200]}})

    client = _redis()
    if client is None:
        services.append({"name": "redis", "status": "unconfigured", "detail": {}})
    else:
        try:
            info = client.info("clients")
            services.append(
                {"name": "redis", "status": "up", "detail": {"connected_clients": info.get("connected_clients")}}
            )
        except Exception as error:
            services.append({"name": "redis", "status": "down", "detail": {"error": str(error)[:200]}})

    try:
        telemetry_store.client.command("SELECT 1")
        services.append({"name": "clickhouse", "status": "up", "detail": {}})
    except Exception as error:
        services.append({"name": "clickhouse", "status": "down", "detail": {"error": str(error)[:200]}})

    for row in _prometheus_query("tracefleet_service_up"):
        name = row["metric"].get("service", "unknown")
        try:
            up = float(row["value"][1]) >= 1
        except (KeyError, IndexError, ValueError):
            continue
        services.append({"name": name, "status": "up" if up else "down", "detail": {"source": "prometheus"}})

    return services


@router.get("/consumer-lag")
def consumer_lag(
    principal: Principal = Depends(require("ops:read")),
) -> dict[str, Any]:
    """Per-partition consumer lag. The back-pressure gate from Phase 3."""
    rows = _prometheus_query("tracefleet_consumer_lag")
    partitions = [
        {
            "group": row["metric"].get("group"),
            "topic": row["metric"].get("topic"),
            "partition": row["metric"].get("partition"),
            "lag": float(row["value"][1]),
        }
        for row in rows
        if row.get("value")
    ]
    paused = {
        row["metric"].get("group"): float(row["value"][1]) >= 1
        for row in _prometheus_query("tracefleet_consumer_paused")
        if row.get("value")
    }
    total = sum(item["lag"] for item in partitions)
    return {
        "partitions": sorted(partitions, key=lambda item: item["lag"], reverse=True),
        "total_lag": total,
        "max_partition_lag": max((item["lag"] for item in partitions), default=0.0),
        "paused_groups": [group for group, is_paused in paused.items() if is_paused],
        # A paused consumer is back-pressure working as designed, not a fault.
        "note": (
            "A paused group means a downstream store is unavailable and Kafka is "
            "buffering. Lag growing while paused is expected; data is not lost."
        ),
    }


@router.get("/throughput")
def throughput_series(
    minutes: int = Query(default=60, ge=1, le=1440),
    principal: Principal = Depends(require("ops:read")),
) -> dict[str, Any]:
    """Event-rate series against the profile target."""
    profile = cfg.load_profile()
    try:
        series = telemetry_store.event_rate_series(principal.tenant_id, minutes=minutes)
        return {"series": series, "target_events_per_second": profile.target_events_per_s, "degraded": []}
    except ClickHouseUnavailable as error:
        return {"series": [], "target_events_per_second": profile.target_events_per_s, "degraded": [str(error)]}


# ---------------------------------------------------------------------------
# Chaos hooks (feature `chaos_hooks`, ADMIN only)
# ---------------------------------------------------------------------------
class ChaosRequest(BaseModel):
    target: str = Field(description="Service to disrupt")
    action: str = Field(default="restart", pattern="^(restart|pause|unpause|stop)$")


#: Only these may be targeted. An allow-list rather than a free-form service name,
#: because this endpoint runs container operations.
CHAOS_TARGETS = {
    "validator",
    "incident-processor",
    "telemetry-sink",
    "incident-persister",
    "kafka",
    "clickhouse",
    "redis",
}


@router.get("/chaos/targets")
def chaos_targets(principal: Principal = Depends(require("ops:chaos"))) -> dict[str, Any]:
    """What may be disrupted, and how."""
    from packages.common import feature_registry

    return {
        "enabled": feature_registry.is_enabled("chaos_hooks"),
        "targets": sorted(CHAOS_TARGETS),
        "actions": ["restart", "pause", "unpause", "stop"],
        "note": (
            "Intended for a local demo of recovery behaviour. Requires the "
            "chaos_hooks feature and the ops:chaos permission."
        ),
    }


@router.post("/chaos")
def trigger_chaos(
    payload: ChaosRequest,
    request: Request,
    principal: Principal = Depends(require("ops:chaos")),
) -> dict[str, Any]:
    """Disrupt a service to demonstrate recovery.

    Deliberately narrow: an allow-listed target, a fixed action set, and an audit
    row before anything happens. An endpoint that can stop infrastructure is worth
    constraining tightly even in a demo.
    """
    from packages.common import feature_registry

    if not feature_registry.is_enabled("chaos_hooks"):
        raise TraceFleetError(status.HTTP_404_NOT_FOUND, "Chaos hooks are not enabled", code="feature_disabled")
    if payload.target not in CHAOS_TARGETS:
        raise TraceFleetError(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Unknown chaos target: {payload.target}",
            code="validation_failed",
            details={"allowed": sorted(CHAOS_TARGETS)},
        )

    audit_from_request(
        request,
        "chaos_triggered",
        "ops",
        resource_id=payload.target,
        metadata={"action": payload.action},
    )

    if os.getenv("CHAOS_EXECUTION_ENABLED", "false").lower() not in {"1", "true", "yes"}:
        # Off by default. The endpoint returns the exact command so a demo can run
        # it deliberately, rather than the API holding a Docker socket it does not
        # otherwise need.
        return {
            "status": "not_executed",
            "reason": "CHAOS_EXECUTION_ENABLED is not set",
            "command": f"docker compose {payload.action} {payload.target}",
            "target": payload.target,
            "action": payload.action,
        }

    import subprocess

    try:
        result = subprocess.run(
            ["docker", "compose", payload.action, payload.target],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        return {
            "status": "executed" if result.returncode == 0 else "failed",
            "target": payload.target,
            "action": payload.action,
            "exit_code": result.returncode,
            "stderr": result.stderr[-500:] if result.stderr else "",
        }
    except Exception as error:
        raise TraceFleetError(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Chaos action could not be executed",
            code="chaos_failed",
            details={"error": str(error)[:200]},
        ) from error
