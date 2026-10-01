"""Fleet endpoints.

Fleets come from the ``fleet`` table, not from scanning Redis keys. A fleet exists
because it was registered, not because one of its vehicles happened to report
recently — the previous implementation derived the list from live hot state, so an
idle fleet disappeared from the UI entirely.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.app.core.errors import NotFound
from apps.api.app.core.security import Principal, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import Fleet, Incident, Vehicle
from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store

FEATURE_FLAGS: tuple[str, ...] = ()
router = APIRouter(prefix="/fleets", tags=["fleets"])


def _vehicle_counts(db: Session, tenant_id: str) -> dict[str, int]:
    rows = db.execute(
        select(Vehicle.fleet_id, func.count(Vehicle.vehicle_id))
        .where(Vehicle.tenant_id == tenant_id, Vehicle.active.is_(True))
        .group_by(Vehicle.fleet_id)
    ).all()
    return {row[0]: int(row[1]) for row in rows}


def _active_incident_counts(db: Session, tenant_id: str) -> dict[str, int]:
    rows = db.execute(
        select(Incident.fleet_id, func.count(Incident.incident_id))
        .where(
            Incident.tenant_id == tenant_id,
            Incident.status.in_(("DETECTED", "RECONSTRUCTING", "OPEN", "ACKNOWLEDGED")),
        )
        .group_by(Incident.fleet_id)
    ).all()
    return {row[0]: int(row[1]) for row in rows}


@router.get("")
def list_fleets(
    hours: int = Query(default=24, ge=1, le=720),
    principal: Principal = Depends(require("fleet:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Registered fleets with vehicle counts and recent activity."""
    fleets = (
        db.execute(select(Fleet).where(Fleet.tenant_id == principal.tenant_id).order_by(Fleet.fleet_id)).scalars().all()
    )

    vehicles = _vehicle_counts(db, principal.tenant_id)
    incidents = _active_incident_counts(db, principal.tenant_id)

    activity: dict[str, dict[str, Any]] = {}
    degraded: list[str] = []
    try:
        for row in telemetry_store.fleet_breakdown(principal.tenant_id, hours=hours):
            activity[row["fleet_id"]] = row
    except ClickHouseUnavailable as error:
        degraded.append(f"telemetry_unavailable: {error}")

    items = []
    for fleet in fleets:
        telemetry = activity.get(fleet.fleet_id, {})
        items.append(
            {
                "fleet_id": fleet.fleet_id,
                "name": fleet.name,
                "region": fleet.region,
                "registered_vehicles": vehicles.get(fleet.fleet_id, 0),
                "active_vehicles": telemetry.get("active_vehicles", 0),
                "active_incidents": incidents.get(fleet.fleet_id, 0),
                "events": telemetry.get("events", 0),
                "avg_speed_kmh": telemetry.get("avg_speed_kmh", 0.0),
                "total_distance_km": telemetry.get("total_distance_km", 0.0),
            }
        )

    return {"items": items, "window_hours": hours, "degraded": degraded}


@router.get("/{fleet_id}")
def get_fleet(
    fleet_id: str,
    hours: int = Query(default=24, ge=1, le=720),
    principal: Principal = Depends(require("fleet:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """One fleet, with its severity breakdown."""
    fleet = db.execute(
        select(Fleet).where(Fleet.fleet_id == fleet_id, Fleet.tenant_id == principal.tenant_id)
    ).scalar_one_or_none()
    if fleet is None:
        raise NotFound("Fleet", fleet_id)

    severity_rows = db.execute(
        select(Incident.severity, func.count(Incident.incident_id))
        .where(Incident.tenant_id == principal.tenant_id, Incident.fleet_id == fleet_id)
        .group_by(Incident.severity)
    ).all()

    payload: dict[str, Any] = {
        "fleet_id": fleet.fleet_id,
        "name": fleet.name,
        "region": fleet.region,
        "created_at": fleet.created_at.isoformat() if fleet.created_at else None,
        "registered_vehicles": _vehicle_counts(db, principal.tenant_id).get(fleet_id, 0),
        "active_incidents": _active_incident_counts(db, principal.tenant_id).get(fleet_id, 0),
        "incidents_by_severity": {row[0]: int(row[1]) for row in severity_rows},
        "degraded": [],
    }

    try:
        for row in telemetry_store.fleet_breakdown(principal.tenant_id, hours=hours):
            if row["fleet_id"] == fleet_id:
                payload["telemetry"] = row
                break
    except ClickHouseUnavailable as error:
        payload["degraded"].append(f"telemetry_unavailable: {error}")

    return payload
