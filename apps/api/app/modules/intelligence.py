"""AI summary endpoints (feature ``ai_summary``).

``POST /incidents/{id}/ai-summary`` generates a validated narrative;
``GET /incidents/{id}/ai-summary/history`` returns the audit view of previous runs.

POST rather than GET because generating a summary has side effects: it calls an
external provider, costs money, and writes an ``ai_run`` row. A cached run is
returned when the prompt and evidence are unchanged, so repeating the call is safe.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import NotFound
from apps.api.app.core.security import Principal, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import AIRun, Incident
from apps.api.app.services import reconstruction as reconstruction_service
from apps.api.app.services.ai import provider_inventory
from apps.api.app.services.ai.service import generate_summary, run_history
from apps.api.app.services.ai.validator import FORBIDDEN_TERMS, required_disclaimer
from packages.common import logging as tf_logging

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS = ("ai_summary",)
router = APIRouter(prefix="/incidents", tags=["intelligence"])

__all__ = ["FEATURE_FLAGS", "provider_inventory", "router"]


class SummaryRequest(BaseModel):
    force: bool = Field(default=False, description="Bypass the cached run and regenerate")
    provider: str | None = Field(default=None, description="Override the configured provider")


def _incident_or_404(incident_id: str, tenant_id: str, db: Session) -> Incident:
    incident = db.execute(
        select(Incident).where(Incident.incident_id == incident_id, Incident.tenant_id == tenant_id)
    ).scalar_one_or_none()
    if incident is None:
        raise NotFound("Incident", incident_id)
    return incident


def _evidence_and_factors(incident: Incident, db: Session) -> tuple[list[dict], list[dict], dict]:
    """Collect the inputs the summary is allowed to see.

    Taken from the reconstruction service so the narrative describes exactly what
    the investigation view shows. Assembling a separate view for the model would let
    the two disagree.
    """
    result = reconstruction_service.reconstruct(incident, db, use_cache=False)
    evidence = [{key: value for key, value in item.items() if key != "event_ts_raw"} for item in result.evidence]
    return evidence, result.factors, result.data_quality


@router.post("/{incident_id}/ai-summary")
def create_ai_summary(
    incident_id: str,
    request: Request,
    payload: SummaryRequest | None = None,
    principal: Principal = Depends(require("incident:ai_summary")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Generate a validated, evidence-cited narrative for an incident."""
    options = payload or SummaryRequest()
    incident = _incident_or_404(incident_id, principal.tenant_id, db)
    evidence, factors, data_quality = _evidence_and_factors(incident, db)

    run = generate_summary(
        incident,
        evidence,
        factors,
        data_quality,
        db,
        actor=principal.user_id,
        provider_name=options.provider,
        force=options.force,
    )

    audit_from_request(
        request,
        "ai_summary_generated",
        "ai_run",
        resource_id=incident_id,
        outcome="SUCCESS" if run.validation.passed else "FAILURE",
        metadata={
            "run_id": run.run_id,
            "provider": run.summary.provider,
            "model": run.summary.model,
            "cached": run.cached,
            "statements_accepted": run.validation.statements_accepted,
            "statements_rejected": run.validation.statements_rejected,
            "validation_passed": run.validation.passed,
        },
    )
    return run.to_dict()


@router.get("/{incident_id}/ai-summary/history")
def ai_summary_history(
    incident_id: str,
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    principal: Principal = Depends(require("incident:ai_summary")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Previous runs, with hashes and validation outcomes.

    This is what makes an AI run reconstructable: the prompt and evidence hashes
    identify what was asked, and the validation report records what was accepted.
    """
    _incident_or_404(incident_id, principal.tenant_id, db)
    runs = run_history(db, incident_id, limit=limit)
    audit_from_request(
        request,
        "ai_summary_history_read",
        "ai_run",
        resource_id=incident_id,
        metadata={"runs": len(runs)},
    )
    return {"incident_id": incident_id, "runs": runs, "count": len(runs)}


@router.get("/{incident_id}/ai-summary/{run_id}")
def get_ai_run(
    incident_id: str,
    run_id: str,
    principal: Principal = Depends(require("incident:ai_summary")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """One stored run in full, including the provider's unedited output."""
    _incident_or_404(incident_id, principal.tenant_id, db)
    row = db.execute(
        select(AIRun).where(
            AIRun.run_id == run_id,
            AIRun.incident_id == incident_id,
            AIRun.tenant_id == principal.tenant_id,
        )
    ).scalar_one_or_none()
    if row is None:
        raise NotFound("AI run", run_id)

    return {
        "run_id": row.run_id,
        "incident_id": row.incident_id,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "actor": row.actor,
        "provider": row.provider,
        "model": row.model,
        "prompt_hash": row.prompt_hash,
        "evidence_hash": row.evidence_hash,
        "evidence_package": row.evidence_package or {},
        # Retained verbatim so a validator decision can be reviewed against what
        # the provider actually said.
        "raw_output": row.output,
        "cited_evidence_ids": row.cited_evidence_ids or [],
        "validation": row.validation_result or {},
        "latency_ms": row.latency_ms,
        "disclaimer": required_disclaimer(),
    }


@router.get("/{incident_id}/ai-summary-policy")
def summary_policy(
    incident_id: str,
    principal: Principal = Depends(require("incident:ai_summary")),
) -> dict[str, Any]:
    """The safety policy applied to generated narratives.

    Published so the constraints are inspectable rather than implicit, and so the UI
    can explain why a statement was removed.
    """
    from apps.api.app.services.ai.prompt import prompt_metadata
    from apps.api.app.services.ai.validator import (
        MIN_SURVIVING_RATIO,
        NUMERIC_TOLERANCE_ABSOLUTE,
        NUMERIC_TOLERANCE_RATIO,
    )

    return {
        "prompt": prompt_metadata(),
        "providers": provider_inventory(),
        "validation": {
            "checks": [
                "citation_existence: every cited evidence id must exist in the package",
                "numeric_grounding: every number must match a cited value within tolerance",
                "forbidden_language: fault, blame, liability and driver-state terms are rejected",
                "uncited_claim: a material assertion with no citation is removed",
            ],
            "numeric_tolerance_ratio": NUMERIC_TOLERANCE_RATIO,
            "numeric_tolerance_absolute": NUMERIC_TOLERANCE_ABSOLUTE,
            "min_surviving_ratio": MIN_SURVIVING_RATIO,
            "forbidden_term_count": len(FORBIDDEN_TERMS),
        },
        "guarantees": [
            "The narrative is derived only from the supplied evidence package.",
            "Free-text telemetry fields are whitelisted before prompt construction.",
            "The summariser has no tools and cannot change incident state.",
            "Deterministic evidence and factor scores are authoritative, not the narrative.",
        ],
        "disclaimer": required_disclaimer(),
    }
