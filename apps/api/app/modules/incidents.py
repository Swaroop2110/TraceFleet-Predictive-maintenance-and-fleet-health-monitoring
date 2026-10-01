"""Incident endpoints: list, detail, timeline, evidence, and lifecycle transitions.

The lifecycle is enforced here, not merely recorded. ``VALID_TRANSITIONS`` is the
complete state machine from the PRD, and every move writes a row to
``incident_state_history`` plus an audit entry — an operator's acknowledgement is a
decision with consequences and needs to be attributable.

Reads of an individual incident, its timeline, and its evidence are all audited.
Those are the views that expose a vehicle's precise movements, which is the reason
the PRD calls them sensitive reads rather than ordinary ones.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import Conflict, NotFound, TraceFleetError
from apps.api.app.core.privacy import mask_records, should_audit_full_precision
from apps.api.app.core.security import Principal, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import Incident, IncidentEvidence, IncidentStateHistory
from apps.api.app.modules._shared import decode_cursor, encode_cursor, serialize_incident
from apps.api.app.services import reconstruction as reconstruction_service
from packages.common import logging as tf_logging
from packages.common.timeutil import now

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS = ("incident_detection_rules",)
router = APIRouter(prefix="/incidents", tags=["incidents"])

#: The full state machine (PRD 9). CLOSED and INVALIDATED are terminal.
VALID_TRANSITIONS: dict[str, set[str]] = {
    "DETECTED": {"RECONSTRUCTING", "OPEN", "INVALIDATED"},
    "RECONSTRUCTING": {"OPEN", "INVALIDATED"},
    "OPEN": {"ACKNOWLEDGED", "CLOSED", "INVALIDATED"},
    "ACKNOWLEDGED": {"CLOSED", "INVALIDATED"},
    "CLOSED": set(),
    "INVALIDATED": set(),
}

SEVERITY_ORDER = ("INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL")


class TransitionRequest(BaseModel):
    """Optional body for a lifecycle transition."""

    reason: str | None = Field(default=None, max_length=1000)


def _redis() -> Any:
    try:
        import redis

        from packages.common.dsn import redis_url

        return redis.from_url(redis_url(), decode_responses=True, socket_timeout=2)
    except Exception:
        return None


def _incident_or_404(incident_id: str, tenant_id: str, db: Session) -> Incident:
    """Load an incident, scoped to the caller's tenant.

    The tenant predicate is here in addition to row-level security. RLS is the
    backstop; an explicit filter means the query is correct on its own terms and
    returns 404 rather than depending on the database to hide the row.
    """
    incident = db.execute(
        select(Incident).where(Incident.incident_id == incident_id, Incident.tenant_id == tenant_id)
    ).scalar_one_or_none()
    if incident is None:
        raise NotFound("Incident", incident_id)
    return incident


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
@router.get("")
def list_incidents(
    request: Request,
    vehicle_id: str | None = Query(default=None, max_length=64),
    fleet_id: str | None = Query(default=None, max_length=64),
    severity: list[str] | None = Query(default=None, description="Repeatable"),
    incident_status: list[str] | None = Query(default=None, alias="status", description="Repeatable"),
    incident_type: str | None = Query(default=None, max_length=64),
    from_ts: datetime | None = Query(default=None, alias="from"),
    to_ts: datetime | None = Query(default=None, alias="to"),
    q: str | None = Query(default=None, max_length=128, description="Match incident or vehicle id"),
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = Query(default=None, description="Opaque keyset cursor"),
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Filtered, keyset-paginated incident list."""
    query = select(Incident).where(Incident.tenant_id == principal.tenant_id)

    if vehicle_id:
        query = query.where(Incident.vehicle_id == vehicle_id)
    if fleet_id:
        query = query.where(Incident.fleet_id == fleet_id)
    if severity:
        wanted = [value.upper() for value in severity]
        invalid = sorted(set(wanted) - set(SEVERITY_ORDER))
        if invalid:
            raise TraceFleetError(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                f"Unknown severity value(s): {', '.join(invalid)}",
                code="validation_failed",
                details={"allowed": list(SEVERITY_ORDER)},
            )
        query = query.where(Incident.severity.in_(wanted))
    if incident_status:
        wanted_status = [value.upper() for value in incident_status]
        invalid = sorted(set(wanted_status) - set(VALID_TRANSITIONS))
        if invalid:
            raise TraceFleetError(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                f"Unknown status value(s): {', '.join(invalid)}",
                code="validation_failed",
                details={"allowed": sorted(VALID_TRANSITIONS)},
            )
        query = query.where(Incident.status.in_(wanted_status))
    if incident_type:
        query = query.where(Incident.incident_type == incident_type)
    if from_ts:
        query = query.where(Incident.start_ts >= from_ts)
    if to_ts:
        query = query.where(Incident.start_ts <= to_ts)
    if q:
        # Prefix match only, so the ix_incident_* indexes are usable. A leading
        # wildcard would force a sequential scan on every keystroke.
        pattern = f"{q}%"
        query = query.where(Incident.incident_id.ilike(pattern) | Incident.vehicle_id.ilike(pattern))

    if cursor:
        try:
            cursor_ts, cursor_id = decode_cursor(cursor)
        except ValueError as error:
            raise TraceFleetError(status.HTTP_400_BAD_REQUEST, str(error), code="invalid_cursor") from error
        # Row-value comparison matching the (tenant_id, start_ts DESC, incident_id)
        # index, so the seek is one index descent rather than a scan-and-skip.
        query = query.where(
            (Incident.start_ts < cursor_ts) | ((Incident.start_ts == cursor_ts) & (Incident.incident_id < cursor_id))
        )

    # One extra row tells us whether another page exists without a second query.
    rows = (
        db.execute(query.order_by(Incident.start_ts.desc(), Incident.incident_id.desc()).limit(limit + 1))
        .scalars()
        .all()
    )

    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = encode_cursor(page[-1].start_ts, page[-1].incident_id) if has_more and page else None

    audit_from_request(
        request,
        "incident_list",
        "incident",
        metadata={
            "count": len(page),
            "filters": {
                "vehicle_id": vehicle_id,
                "fleet_id": fleet_id,
                "severity": severity,
                "status": incident_status,
                "type": incident_type,
                "q": q,
            },
        },
    )

    return {
        "items": [serialize_incident(incident) for incident in page],
        "pagination": {"limit": limit, "next_cursor": next_cursor, "has_more": has_more},
    }


@router.get("/summary")
def incident_summary(
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Counts by severity and status. Backs the Fleet Overview KPI tiles.

    Placed before ``/{incident_id}`` so the literal path is not captured by the
    path parameter.
    """
    severity_rows = db.execute(
        select(Incident.severity, func.count(Incident.incident_id))
        .where(Incident.tenant_id == principal.tenant_id)
        .group_by(Incident.severity)
    ).all()
    status_rows = db.execute(
        select(Incident.status, func.count(Incident.incident_id))
        .where(Incident.tenant_id == principal.tenant_id)
        .group_by(Incident.status)
    ).all()

    by_severity = dict.fromkeys(SEVERITY_ORDER, 0)
    by_severity.update({row[0]: row[1] for row in severity_rows})
    by_status = dict.fromkeys(VALID_TRANSITIONS, 0)
    by_status.update({row[0]: row[1] for row in status_rows})

    active_states = ("DETECTED", "RECONSTRUCTING", "OPEN", "ACKNOWLEDGED")
    return {
        "total": sum(by_severity.values()),
        "by_severity": by_severity,
        "by_status": by_status,
        "active": sum(by_status.get(state, 0) for state in active_states),
        "requires_attention": by_status.get("OPEN", 0),
    }


@router.get("/{incident_id}")
def get_incident(
    incident_id: str,
    request: Request,
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Incident detail with evidence and factors. Audited."""
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    audit_from_request(
        request,
        "incident_read",
        "incident",
        resource_id=incident_id,
        metadata={"severity": incident.severity, "status": incident.status},
    )
    return serialize_incident(incident, include_evidence=True)


@router.get("/{incident_id}/timeline")
def get_timeline(
    incident_id: str,
    request: Request,
    pre_window_s: int | None = Query(default=None, ge=0, le=3600),
    post_window_s: int | None = Query(default=None, ge=0, le=3600),
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Reconstructed timeline: phases, telemetry points, evidence, and data quality.

    This is the investigation view. An analyst may see full-precision positions here
    when their role permits it, and that access is recorded.
    """
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    result = reconstruction_service.reconstruct(
        incident,
        db,
        pre_window_s=pre_window_s,
        post_window_s=post_window_s,
        redis_client=_redis(),
    )
    payload = reconstruction_service.strip_internal(result.to_dict())

    # Masking applies to the investigation view too, at the precision the caller's
    # role allows for an investigation rather than the stricter list-view precision.
    payload["points"] = mask_records(payload["points"], principal.roles, investigation=True)

    full_precision = should_audit_full_precision(principal.roles, investigation=True)
    audit_from_request(
        request,
        "location_full_precision_read" if full_precision else "timeline_read",
        "incident_timeline",
        resource_id=incident_id,
        metadata={
            "points": len(payload["points"]),
            "phases": [phase["name"] for phase in payload["phases"]],
            "full_precision": full_precision,
            "degraded": payload.get("degraded", []),
        },
    )
    return payload


@router.get("/{incident_id}/evidence")
def get_evidence(
    incident_id: str,
    request: Request,
    include_raw: bool = Query(default=False, description="Include the canonical event payload"),
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Evidence items, optionally with the raw canonical telemetry behind each.

    ``include_raw`` requires ``incident:read_raw``: the underlying event carries the
    vehicle's exact position and is the most sensitive read in the product.
    """
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    rows = (
        db.execute(
            select(IncidentEvidence)
            .where(IncidentEvidence.incident_id == incident_id)
            .order_by(IncidentEvidence.event_ts.asc())
        )
        .scalars()
        .all()
    )

    items: list[dict[str, Any]] = [
        {
            "evidence_id": row.evidence_id,
            "event_key": row.event_key,
            "event_ts": row.event_ts.isoformat(),
            "signal": row.signal,
            "value": row.value,
            "threshold": row.threshold,
            "weight": row.weight,
            "phase": row.phase,
            "flags": row.flags or {},
        }
        for row in rows
    ]

    raw_included = False
    if include_raw:
        if not principal.has("incident:read_raw"):
            raise TraceFleetError(
                status.HTTP_403_FORBIDDEN,
                "Missing required permission: incident:read_raw",
                code="forbidden",
            )
        items = _attach_raw_events(incident, items, principal)
        raw_included = True

    audit_from_request(
        request,
        "raw_telemetry_read" if raw_included else "evidence_read",
        "incident_evidence",
        resource_id=incident_id,
        metadata={"items": len(items), "raw_included": raw_included},
    )
    return {"incident_id": incident_id, "items": items, "raw_included": raw_included}


def _attach_raw_events(
    incident: Incident,
    items: list[dict[str, Any]],
    principal: Principal,
) -> list[dict[str, Any]]:
    """Attach the canonical telemetry for each evidence item, from ClickHouse."""
    from apps.api.app.core.privacy import mask_record
    from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store
    from packages.common.timeutil import add_seconds, ensure_utc

    if not items:
        return items
    timestamps = [ensure_utc(datetime.fromisoformat(item["event_ts"])) for item in items]
    try:
        points = telemetry_store.vehicle_window(
            incident.tenant_id,
            incident.vehicle_id,
            add_seconds(min(timestamps), -2),
            add_seconds(max(timestamps), 2),
        )
    except ClickHouseUnavailable as error:
        LOGGER.warning("raw telemetry unavailable", extra={"error": str(error)})
        for item in items:
            item["raw_event"] = None
            item["raw_unavailable"] = "telemetry store unreachable"
        return items

    by_key = {point.event_key: point for point in points}
    for item in items:
        point = by_key.get(item["event_key"])
        item["raw_event"] = mask_record(point.to_dict(), principal.roles, investigation=True) if point else None
    return items


@router.get("/{incident_id}/history")
def get_state_history(
    incident_id: str,
    principal: Principal = Depends(require("incident:read")),
    db: Session = Depends(get_tenant_db),
) -> list[dict[str, Any]]:
    """Lifecycle transitions for an incident, oldest first."""
    _incident_or_404(incident_id, principal.tenant_id, db)
    rows = (
        db.execute(
            select(IncidentStateHistory)
            .where(IncidentStateHistory.incident_id == incident_id)
            .order_by(IncidentStateHistory.changed_at.asc())
        )
        .scalars()
        .all()
    )
    return [
        {
            "old_status": row.old_status,
            "new_status": row.new_status,
            "changed_at": row.changed_at.isoformat() if row.changed_at else None,
            "changed_by": row.changed_by,
            "reason": row.reason,
        }
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Lifecycle transitions
# ---------------------------------------------------------------------------
def _transition(
    incident: Incident,
    target: str,
    principal: Principal,
    db: Session,
    request: Request,
    reason: str | None = None,
) -> dict[str, Any]:
    """Apply a state transition, enforcing the state machine."""
    current = incident.status

    if current == target:
        # Idempotent: a double-clicked Acknowledge should not be an error.
        return serialize_incident(incident, include_evidence=True)

    allowed = VALID_TRANSITIONS.get(current, set())
    if target not in allowed:
        audit_from_request(
            request,
            f"incident_{target.lower()}",
            "incident",
            resource_id=incident.incident_id,
            outcome="FAILURE",
            metadata={"from": current, "to": target, "reason": "invalid_transition"},
        )
        raise Conflict(
            f"Cannot move a {current} incident to {target}",
            from_status=current,
            to_status=target,
            allowed_transitions=sorted(allowed),
        )

    timestamp = now()
    incident.status = target
    if target == "ACKNOWLEDGED":
        incident.acknowledged_at = timestamp
        incident.acknowledged_by = principal.user_id
    elif target in ("CLOSED", "INVALIDATED"):
        incident.closed_at = timestamp
        incident.closed_by = principal.user_id
        if incident.end_ts is None:
            incident.end_ts = timestamp

    db.add(
        IncidentStateHistory(
            incident_id=incident.incident_id,
            old_status=current,
            new_status=target,
            changed_at=timestamp,
            changed_by=principal.user_id,
            reason=reason,
        )
    )
    db.commit()

    audit_from_request(
        request,
        f"incident_{target.lower()}",
        "incident",
        resource_id=incident.incident_id,
        metadata={"from": current, "to": target, "reason": reason},
    )
    LOGGER.info(
        "incident transitioned",
        extra={
            "incident_id": incident.incident_id,
            "from_status": current,
            "to_status": target,
            "actor": principal.user_id,
        },
    )

    # A status change alters the header of a cached timeline.
    reconstruction_service.invalidate_cache(_redis(), incident.tenant_id, incident.incident_id)
    return serialize_incident(incident, include_evidence=True)


@router.post("/{incident_id}/acknowledge")
def acknowledge_incident(
    incident_id: str,
    request: Request,
    payload: TransitionRequest | None = None,
    principal: Principal = Depends(require("incident:acknowledge")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Acknowledge an incident: an operator has seen it and owns it."""
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    return _transition(incident, "ACKNOWLEDGED", principal, db, request, (payload.reason if payload else None))


@router.post("/{incident_id}/close")
def close_incident(
    incident_id: str,
    request: Request,
    payload: TransitionRequest | None = None,
    principal: Principal = Depends(require("incident:close")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Close an incident: it happened and has been dealt with. Terminal."""
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    return _transition(incident, "CLOSED", principal, db, request, (payload.reason if payload else None))


@router.post("/{incident_id}/invalidate")
def invalidate_incident(
    incident_id: str,
    request: Request,
    payload: TransitionRequest | None = None,
    principal: Principal = Depends(require("incident:invalidate")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Invalidate an incident: the detection was wrong. Terminal.

    Distinct from CLOSED, and the distinction matters. Closing says the incident was
    real and handled; invalidating says it should never have been raised. Conflating
    them would corrupt any measurement of detection accuracy.
    """
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    reason = payload.reason if payload else None
    if not reason:
        raise TraceFleetError(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "A reason is required when invalidating an incident",
            code="reason_required",
            details={"field": "reason"},
        )
    return _transition(incident, "INVALIDATED", principal, db, request, reason)
