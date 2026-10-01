"""Vehicle endpoints: listing, hot state, and per-vehicle incident history.

Vehicle state is read from Redis, which the validator updates on every accepted
event. ClickHouse is the fallback: Redis is a cache with a TTL, so a vehicle that
has been quiet longer than the TTL still has a last-known position worth showing —
reporting "unknown" for a parked vehicle would be wrong.

Every response passes through privacy masking. A list view is not an investigation,
so it gets the caller's ordinary precision rather than the investigation precision.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import NotFound, TraceFleetError
from apps.api.app.core.privacy import mask_record, mask_records
from apps.api.app.core.security import Principal, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import Incident, Vehicle
from apps.api.app.modules._shared import decode_hash, serialize_incident
from packages.common import logging as tf_logging
from packages.common.dsn import redis_url

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS: tuple[str, ...] = ()
router = APIRouter(prefix="/vehicles", tags=["vehicles"])


@lru_cache(maxsize=1)
def get_redis_client() -> Any:
    import redis

    return redis.from_url(redis_url(), decode_responses=True, socket_timeout=2)


def _hot_state(tenant_id: str, vehicle_id: str) -> dict[str, Any] | None:
    try:
        state = get_redis_client().hgetall(f"tf:{tenant_id}:veh:{vehicle_id}:state")
    except Exception as error:
        LOGGER.debug("hot state unavailable", extra={"error": str(error)})
        return None
    return decode_hash(state) if state else None


def _cold_state(tenant_id: str, vehicle_id: str) -> dict[str, Any] | None:
    """Last-known state from ClickHouse, when the Redis entry has expired."""
    from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store

    try:
        point = telemetry_store.latest_state(tenant_id, vehicle_id)
    except ClickHouseUnavailable:
        return None
    if point is None:
        return None
    payload = point.to_dict()
    return {
        "vehicle_id": vehicle_id,
        "tenant_id": tenant_id,
        "speed": payload["speed_kmh"],
        "lat": payload["lat"],
        "lon": payload["lon"],
        "geohash": payload["geohash"],
        "heading": payload["heading_deg"],
        "soc": payload["soc_pct"],
        "odo_km": payload["odo_km"],
        "rpm": payload["rpm"],
        "dtc": payload["dtc"],
        "evt": payload["evt"],
        "last_ts": payload["ts"],
        "ingested_at": payload["ingested_at"],
        "late": payload["late"],
        "source": "clickhouse",
    }


@router.get("")
def list_vehicles(
    request: Request,
    fleet_id: str | None = Query(default=None, max_length=64),
    q: str | None = Query(default=None, max_length=64, description="Match vehicle id or VIN prefix"),
    active_only: bool = Query(default=False, description="Only vehicles with live hot state"),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    principal: Principal = Depends(require("vehicle:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Registered vehicles, each enriched with its latest known state.

    The registry in Postgres is the source of truth for which vehicles exist;
    scanning Redis keys, as the previous implementation did, would only ever list
    vehicles that had recently reported and would miss the rest of the fleet.
    """
    query = select(Vehicle).where(Vehicle.tenant_id == principal.tenant_id, Vehicle.active.is_(True))
    if fleet_id:
        query = query.where(Vehicle.fleet_id == fleet_id)
    if q:
        pattern = f"{q}%"
        query = query.where(Vehicle.vehicle_id.ilike(pattern) | Vehicle.vin.ilike(pattern))

    total = db.execute(select(func.count()).select_from(query.subquery())).scalar() or 0
    rows = db.execute(query.order_by(Vehicle.vehicle_id.asc()).limit(limit).offset(offset)).scalars().all()

    try:
        pipeline = get_redis_client().pipeline(transaction=False)
        for vehicle in rows:
            pipeline.hgetall(f"tf:{principal.tenant_id}:veh:{vehicle.vehicle_id}:state")
        hot_states = pipeline.execute()
    except Exception as error:
        LOGGER.debug("batch hot state unavailable", extra={"error": str(error)})
        hot_states = [{} for _ in rows]
    vehicles: list[dict[str, Any]] = []
    for vehicle, raw_state in zip(rows, hot_states, strict=True):
        state = decode_hash(raw_state) if raw_state else None
        record: dict[str, Any] = {
            "vehicle_id": vehicle.vehicle_id,
            # `id` retained because the existing frontend keys on it.
            "id": vehicle.vehicle_id,
            "fleet_id": vehicle.fleet_id,
            "vin": vehicle.vin,
            "oem": vehicle.oem,
            "model": vehicle.model,
            "model_year": vehicle.model_year,
            "live": state is not None,
        }
        if state:
            record.update(state)
            record["source"] = "redis"
        if active_only and state is None:
            continue
        vehicles.append(record)

    audit_from_request(
        request,
        "fleet_read",
        "vehicle",
        metadata={"count": len(vehicles), "fleet_id": fleet_id, "query": q},
    )
    return {
        "items": mask_records(vehicles, principal.roles),
        "pagination": {"total": total, "limit": limit, "offset": offset},
    }


@router.get("/{vehicle_id}/state")
def get_vehicle_state(
    vehicle_id: str,
    request: Request,
    principal: Principal = Depends(require("vehicle:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Latest known state, from Redis with a ClickHouse fallback."""
    vehicle = db.execute(
        select(Vehicle).where(Vehicle.vehicle_id == vehicle_id, Vehicle.tenant_id == principal.tenant_id)
    ).scalar_one_or_none()

    state = _hot_state(principal.tenant_id, vehicle_id)
    source = "redis"
    if state is None:
        state = _cold_state(principal.tenant_id, vehicle_id)
        source = "clickhouse"

    if state is None:
        if vehicle is None:
            raise NotFound("Vehicle", vehicle_id)
        raise TraceFleetError(
            status.HTTP_404_NOT_FOUND,
            "No telemetry has been recorded for this vehicle",
            code="no_telemetry",
            details={"vehicle_id": vehicle_id},
        )

    if vehicle is not None:
        state.setdefault("vin", vehicle.vin)
        state.setdefault("fleet_id", vehicle.fleet_id)
        state["oem"] = vehicle.oem
        state["model"] = vehicle.model
    state["vehicle_id"] = vehicle_id
    state["source"] = source
    state["registered"] = vehicle is not None

    audit_from_request(
        request,
        "vehicle_state_read",
        "vehicle",
        resource_id=vehicle_id,
        metadata={"source": source},
    )
    return mask_record(state, principal.roles)


@router.get("/{vehicle_id}/incidents")
def get_vehicle_incidents(
    vehicle_id: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=200),
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Incident history for one vehicle, most recent first."""
    rows = (
        db.execute(
            select(Incident)
            .where(Incident.tenant_id == principal.tenant_id, Incident.vehicle_id == vehicle_id)
            .order_by(Incident.start_ts.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )

    audit_from_request(
        request,
        "incident_list",
        "vehicle_incidents",
        resource_id=vehicle_id,
        metadata={"count": len(rows)},
    )
    return {
        "vehicle_id": vehicle_id,
        "items": [serialize_incident(incident) for incident in rows],
        "count": len(rows),
    }


@router.get("/{vehicle_id}/risk")
def get_vehicle_risk(
    vehicle_id: str,
    days: int = Query(default=7, ge=1, le=90),
    principal: Principal = Depends(require("analytics:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Risk trend for one vehicle: incident counts against distance driven."""
    from apps.api.app.services.risk import vehicle_risk

    return vehicle_risk(db, principal.tenant_id, vehicle_id, days=days)
