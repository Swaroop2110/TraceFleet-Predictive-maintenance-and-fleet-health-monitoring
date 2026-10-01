"""Analytics endpoints: incident trends, fleet risk, and telemetry aggregates.

Postgres answers questions about incidents (the record of truth an operator acts
on) and ClickHouse answers questions about telemetry volume and driving behaviour.
Each store is used for what it is good at, and a ClickHouse outage degrades the
telemetry half while leaving incident counts intact — reported through ``degraded``
rather than silently returning zeros.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.app.core.security import Principal, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import Incident, IncidentFactor
from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store
from apps.api.app.services.risk import fleet_risk
from packages.common import logging as tf_logging
from packages.common.schemas_compat import decimal_to_float
from packages.common.timeutil import add_seconds, now

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS: tuple[str, ...] = ()
router = APIRouter(prefix="/analytics", tags=["analytics"])

SEVERITIES = ("INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL")


@router.get("/incidents")
def incident_trends(
    days: int = Query(default=30, ge=1, le=365),
    granularity: str = Query(default="day", pattern="^(day|week)$"),
    fleet_id: str | None = Query(default=None, max_length=64),
    principal: Principal = Depends(require("analytics:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Incident counts over time, by severity, type, and contributing factor."""
    since = add_seconds(now(), -days * 86400)
    bucket = func.date_trunc(granularity, Incident.start_ts).label("bucket")

    base = [
        Incident.tenant_id == principal.tenant_id,
        Incident.start_ts >= since,
        Incident.status != "INVALIDATED",
    ]
    if fleet_id:
        base.append(Incident.fleet_id == fleet_id)

    trend_rows = db.execute(
        select(bucket, Incident.severity, func.count(Incident.incident_id))
        .where(*base)
        .group_by(bucket, Incident.severity)
        .order_by(bucket)
    ).all()

    series: dict[str, dict[str, Any]] = {}
    for moment, severity, count in trend_rows:
        key = moment.date().isoformat() if isinstance(moment, (date,)) else str(moment)
        entry = series.setdefault(key, {"bucket": key, "total": 0, **dict.fromkeys(SEVERITIES, 0)})
        entry[severity] = int(count)
        entry["total"] += int(count)

    type_rows = db.execute(
        select(Incident.incident_type, func.count(Incident.incident_id))
        .where(*base)
        .group_by(Incident.incident_type)
        .order_by(func.count(Incident.incident_id).desc())
    ).all()

    # Ranked by summed score rather than by count: a factor appearing in many
    # incidents with a tiny contribution is less interesting than one that
    # dominates a few.
    factor_rows = db.execute(
        select(
            IncidentFactor.factor_type,
            func.count(IncidentFactor.factor_id),
            func.avg(IncidentFactor.score),
            func.sum(IncidentFactor.score),
        )
        .join(Incident, Incident.incident_id == IncidentFactor.incident_id)
        .where(*base)
        .group_by(IncidentFactor.factor_type)
        .order_by(func.sum(IncidentFactor.score).desc())
    ).all()

    fleet_rows = db.execute(
        select(Incident.fleet_id, Incident.severity, func.count(Incident.incident_id))
        .where(*base)
        .group_by(Incident.fleet_id, Incident.severity)
    ).all()
    by_fleet: dict[str, dict[str, Any]] = {}
    for fleet, severity, count in fleet_rows:
        entry = by_fleet.setdefault(fleet, {"fleet_id": fleet, "total": 0, **dict.fromkeys(SEVERITIES, 0)})
        entry[severity] = int(count)
        entry["total"] += int(count)

    degraded: list[str] = []
    heatmap: list[dict[str, Any]] = []
    try:
        heatmap = telemetry_store.hourly_heatmap(principal.tenant_id, days=min(days, 30))
    except ClickHouseUnavailable as error:
        degraded.append(f"heatmap_unavailable: {error}")

    return {
        "window_days": days,
        "granularity": granularity,
        "series": sorted(series.values(), key=lambda item: item["bucket"]),
        "by_type": [{"incident_type": row[0], "count": int(row[1])} for row in type_rows],
        "top_factors": [
            {
                "factor_type": row[0],
                "occurrences": int(row[1]),
                "avg_score": round(decimal_to_float(row[2]) or 0.0, 4),
                "total_score": round(decimal_to_float(row[3]) or 0.0, 4),
            }
            for row in factor_rows
        ],
        "by_fleet": sorted(by_fleet.values(), key=lambda item: item["total"], reverse=True),
        "hourly_heatmap": heatmap,
        "note": "Excludes incidents marked INVALIDATED.",
        "degraded": degraded,
    }


@router.get("/fleet-risk")
def get_fleet_risk(
    days: int = Query(default=7, ge=1, le=90),
    limit: int = Query(default=50, ge=1, le=200),
    principal: Principal = Depends(require("analytics:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Risk index per fleet and the highest-ranked vehicles."""
    return fleet_risk(db, principal.tenant_id, days=days, limit=limit)


@router.get("/fleet")
def fleet_overview(
    hours: int = Query(default=24, ge=1, le=720),
    principal: Principal = Depends(require("analytics:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """KPI summary for the Fleet Overview page."""
    severity_rows = db.execute(
        select(Incident.severity, func.count(Incident.incident_id))
        .where(Incident.tenant_id == principal.tenant_id)
        .group_by(Incident.severity)
    ).all()
    by_severity = dict.fromkeys(SEVERITIES, 0)
    by_severity.update({row[0]: int(row[1]) for row in severity_rows})

    active = (
        db.execute(
            select(func.count(Incident.incident_id)).where(
                Incident.tenant_id == principal.tenant_id,
                Incident.status.in_(("DETECTED", "RECONSTRUCTING", "OPEN")),
            )
        ).scalar()
        or 0
    )

    stats: dict[str, Any] = {
        "total_incidents": sum(by_severity.values()),
        "active_incidents": int(active),
        "incidents_by_severity": by_severity,
        # Retained for the existing frontend, which reads these flat keys.
        "active_vehicles_24h": 0,
        "avg_speed_kmh": 0.0,
        "total_distance_km": 0.0,
        "degraded": [],
    }

    try:
        summary = telemetry_store.fleet_summary(principal.tenant_id, hours=hours)
        stats.update(
            {
                "active_vehicles_24h": summary.get("active_vehicles", 0),
                "avg_speed_kmh": summary.get("avg_speed_kmh", 0.0),
                "max_speed_kmh": summary.get("max_speed_kmh", 0.0),
                "total_distance_km": summary.get("total_distance_km", 0.0),
                "events": summary.get("events", 0),
                "late_events": summary.get("late_events", 0),
            }
        )
    except ClickHouseUnavailable as error:
        stats["degraded"].append(f"telemetry_unavailable: {error}")

    try:
        stats["event_rate"] = telemetry_store.event_rate_series(principal.tenant_id, minutes=60)
    except ClickHouseUnavailable:
        stats["event_rate"] = []

    return stats


@router.get("/telemetry")
def telemetry_aggregates(
    hours: int = Query(default=24, ge=1, le=720),
    principal: Principal = Depends(require("analytics:read")),
) -> dict[str, Any]:
    """Telemetry volume, per-fleet breakdown, and ingest lag."""
    try:
        return {
            "summary": telemetry_store.fleet_summary(principal.tenant_id, hours=hours),
            "by_fleet": telemetry_store.fleet_breakdown(principal.tenant_id, hours=hours),
            "ingest_lag": telemetry_store.ingest_lag(principal.tenant_id, minutes=min(hours * 60, 1440)),
            "degraded": [],
        }
    except ClickHouseUnavailable as error:
        return {"summary": {}, "by_fleet": [], "ingest_lag": {}, "degraded": [str(error)]}
