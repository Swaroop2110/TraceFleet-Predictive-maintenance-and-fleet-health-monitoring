"""Evidence export endpoints (feature ``evidence_export``).

``GET /incidents/{id}/export?format=json|csv|pdf`` returns a bundle that may leave
the platform. Exports are audited, because a copy of an incident's evidence
travelling outside the system is exactly the access a compliance review asks about.

The bundle is assembled from the same reconstruction the investigation view shows,
so an exported document and the screen it was taken from cannot disagree.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import NotFound, TraceFleetError
from apps.api.app.core.privacy import mask_records
from apps.api.app.core.security import Principal, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import AIRun, Incident
from apps.api.app.services import reconstruction as reconstruction_service
from apps.api.app.services.exporters import available_formats, export
from packages.common import logging as tf_logging
from packages.common.registry import RegistryError
from packages.common.timeutil import now, to_iso

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS = ("evidence_export",)
router = APIRouter(prefix="/incidents", tags=["exports"])


@router.get("/{incident_id}/export")
def export_incident(
    incident_id: str,
    request: Request,
    export_format: str = Query(default="json", alias="format"),
    include_points: bool = Query(default=False, description="Include the raw telemetry series"),
    include_ai_summary: bool = Query(default=True, description="Include the most recent validated narrative"),
    principal: Principal = Depends(require("incident:export")),
    db: Session = Depends(get_tenant_db),
) -> Response:
    """Export an incident's evidence bundle."""
    incident = db.execute(
        select(Incident).where(Incident.incident_id == incident_id, Incident.tenant_id == principal.tenant_id)
    ).scalar_one_or_none()
    if incident is None:
        raise NotFound("Incident", incident_id)

    fmt = export_format.lower().strip()
    if fmt not in available_formats():
        raise TraceFleetError(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Unsupported export format: {export_format}",
            code="unsupported_format",
            details={"available": available_formats()},
        )

    bundle = _build_bundle(
        incident,
        db,
        principal,
        include_points=include_points,
        include_ai_summary=include_ai_summary,
    )

    try:
        result = export(bundle, fmt)
    except RegistryError as error:  # pragma: no cover - guarded above
        raise TraceFleetError(status.HTTP_422_UNPROCESSABLE_ENTITY, str(error), code="unsupported_format") from error

    audit_from_request(
        request,
        "export_generated",
        "incident_export",
        resource_id=incident_id,
        metadata={
            "format": fmt,
            "bytes": len(result.content),
            "evidence_items": len(bundle.get("evidence", [])),
            "included_points": include_points,
            "included_ai_summary": bool(bundle.get("ai_summary")),
        },
    )

    return Response(
        content=result.content,
        media_type=result.media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{result.filename(incident_id)}"',
            # An evidence bundle must not be cached by a proxy on its way out.
            "Cache-Control": "no-store",
        },
    )


@router.get("/{incident_id}/export/formats")
def list_formats(
    incident_id: str,
    principal: Principal = Depends(require("incident:export")),
) -> dict[str, Any]:
    """Available export formats, from the exporter registry."""
    from apps.api.app.services.exporters import exporter_registry

    return {"formats": available_formats(), "detail": exporter_registry.metadata()}


def _build_bundle(
    incident: Incident,
    db: Session,
    principal: Principal,
    *,
    include_points: bool,
    include_ai_summary: bool,
) -> dict[str, Any]:
    """Assemble the exportable bundle."""
    result = reconstruction_service.reconstruct(incident, db, use_cache=False)
    payload = reconstruction_service.strip_internal(result.to_dict())

    bundle: dict[str, Any] = {
        "incident": payload["incident"],
        "phases": payload["phases"],
        "evidence": payload["evidence"],
        "factors": payload["factors"],
        "data_quality": payload["data_quality"],
        "window": payload["window"],
        "exported_at": to_iso(now()),
        "exported_by": principal.user_id,
        "tenant_id": principal.tenant_id,
        "degraded": payload.get("degraded", []),
    }

    if include_points:
        # Masked at the caller's investigation precision, so an export cannot be
        # used to obtain coordinates the requester could not see on screen.
        bundle["points"] = mask_records(payload["points"], principal.roles, investigation=True)

    if include_ai_summary and principal.has("incident:ai_summary"):
        narrative = _latest_validated_summary(db, incident.incident_id)
        if narrative is not None:
            bundle["ai_summary"] = narrative

    return bundle


def _latest_validated_summary(db: Session, incident_id: str) -> dict[str, Any] | None:
    """Most recent AI run that passed validation.

    Only a passing run is exported. Including a withheld or partially-rejected
    narrative in a document that leaves the platform would put unvalidated claims
    into a case file.
    """
    rows = (
        db.execute(select(AIRun).where(AIRun.incident_id == incident_id).order_by(AIRun.created_at.desc()).limit(10))
        .scalars()
        .all()
    )

    for row in rows:
        validation = row.validation_result or {}
        if not validation.get("passed"):
            continue
        from apps.api.app.services.ai.service import _rehydrate
        from apps.api.app.services.ai.validator import required_disclaimer

        summary = _rehydrate(row, row.evidence_package or {})
        payload = summary.to_dict()
        payload.update(
            {
                "run_id": row.run_id,
                "generated_at": row.created_at.isoformat() if row.created_at else None,
                "generated_by": row.actor,
                "prompt_hash": row.prompt_hash,
                "evidence_hash": row.evidence_hash,
                "validation": validation,
                "disclaimer": required_disclaimer(),
            }
        )
        return payload
    return None
