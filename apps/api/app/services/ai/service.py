"""Orchestrates one AI summary run: package, prompt, provider, validate, audit.

The sequence is fixed and every step is recorded. An ``ai_run`` row holds the prompt
hash, the evidence hash, the package, the raw output, the validation report, and the
actor — enough to reconstruct exactly what was asked and what came back, without
needing the model to be reproducible.

Identical prompt and evidence hashes return the cached run unless ``force=True``.
That is not only a cost measure: repeated requests returning the same narrative is
what makes a summary quotable in an investigation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.app.db.models import AIRun, Incident
from apps.api.app.services.ai.package_builder import build_package, package_hash
from apps.api.app.services.ai.prompt import build_prompt, prompt_hash, prompt_metadata
from apps.api.app.services.ai.registry import ProviderError, SummaryResult, resolve_provider
from apps.api.app.services.ai.validator import ValidationResult, required_disclaimer, validate
from packages.common import logging as tf_logging
from packages.common import metrics
from packages.common.ids import run_id
from packages.common.schemas_compat import jsonify

LOGGER = tf_logging.get_logger(__name__)


@dataclass
class SummaryRun:
    run_id: str
    incident_id: str
    summary: SummaryResult
    validation: ValidationResult
    cached: bool
    disclaimer: str
    evidence_index: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        payload = self.summary.to_dict()
        payload.update(
            {
                "run_id": self.run_id,
                "incident_id": self.incident_id,
                "cached": self.cached,
                # Always present, and the UI is expected to show it. A generated
                # narrative must never look like a finding.
                "disclaimer": self.disclaimer,
                "validation": self.validation.to_dict(),
                "evidence_index": self.evidence_index,
                "prompt": prompt_metadata(),
            }
        )
        return payload


def generate_summary(
    incident: Incident,
    evidence: list[dict[str, Any]],
    factors: list[dict[str, Any]],
    data_quality: dict[str, Any],
    db: Session,
    *,
    actor: str,
    provider_name: str | None = None,
    force: bool = False,
) -> SummaryRun:
    """Produce (or return a cached) validated summary for an incident."""
    package, label_map = build_package(incident, evidence, factors, data_quality)
    prompt = build_prompt(package)
    p_hash = prompt_hash(prompt)
    e_hash = package_hash(package)
    provider = resolve_provider(provider_name)

    if not force:
        cached = _cached_run(
            db, incident.incident_id, p_hash, e_hash, provider.name, getattr(provider, "model", "unknown")
        )
        if cached is not None:
            metrics.ai_requests_total.labels(provider=cached.summary.provider, outcome="cached").inc()
            return cached

    started = time.perf_counter()
    try:
        result = provider.summarize(package, prompt)
    except ProviderError as error:
        # A provider failure is reported as an unavailable summary rather than a
        # 500: the deterministic evidence is unaffected and still worth showing.
        LOGGER.warning(
            "ai provider failed",
            extra={"incident_id": incident.incident_id, "provider": provider.name, "error": str(error)},
        )
        result = SummaryResult(
            model=getattr(provider, "model", "unknown"),
            provider=provider.name,
            available=False,
            unavailable_reason=f"The narrative provider could not be reached: {error}",
        )
        validation = ValidationResult(passed=False, withheld_reason=result.unavailable_reason)
        return _persist(
            db,
            incident,
            package,
            prompt,
            p_hash,
            e_hash,
            result,
            validation,
            label_map,
            actor=actor,
            latency_ms=int((time.perf_counter() - started) * 1000),
            cached=False,
        )

    latency_ms = result.latency_ms or int((time.perf_counter() - started) * 1000)

    if not result.available:
        # The null provider, or a provider reporting it has nothing to say.
        validation = ValidationResult(passed=False, withheld_reason=result.unavailable_reason)
        metrics.ai_requests_total.labels(provider=provider.name, outcome="unavailable").inc()
        return _persist(
            db,
            incident,
            package,
            prompt,
            p_hash,
            e_hash,
            result,
            validation,
            label_map,
            actor=actor,
            latency_ms=latency_ms,
            cached=False,
        )

    result, validation = validate(result, package, label_map)
    metrics.ai_requests_total.labels(
        provider=provider.name, outcome="accepted" if validation.passed else "withheld"
    ).inc()

    return _persist(
        db,
        incident,
        package,
        prompt,
        p_hash,
        e_hash,
        result,
        validation,
        label_map,
        actor=actor,
        latency_ms=latency_ms,
        cached=False,
    )


def _persist(
    db: Session,
    incident: Incident,
    package: dict[str, Any],
    prompt: str,
    p_hash: str,
    e_hash: str,
    result: SummaryResult,
    validation: ValidationResult,
    label_map: dict[str, str],
    *,
    actor: str,
    latency_ms: int,
    cached: bool,
) -> SummaryRun:
    """Write the ai_run row and return the assembled result."""
    identifier = run_id()
    row = AIRun(
        run_id=identifier,
        incident_id=incident.incident_id,
        tenant_id=incident.tenant_id,
        actor=actor,
        provider=result.provider,
        model=result.model,
        prompt=prompt,
        prompt_hash=p_hash,
        evidence_hash=e_hash,
        evidence_package=jsonify(package),
        # The provider's unedited output is stored even when validation rejected
        # it. Keeping only the accepted text would make a validator failure
        # impossible to investigate after the fact.
        output=result.raw_output or _render_text(result),
        cited_evidence_ids=validation.cited_evidence_ids,
        validation_result=validation.to_dict(),
        latency_ms=latency_ms,
    )
    db.add(row)
    db.commit()

    return SummaryRun(
        run_id=identifier,
        incident_id=incident.incident_id,
        summary=result,
        validation=validation,
        cached=cached,
        disclaimer=required_disclaimer(),
        evidence_index=label_map,
    )


def _cached_run(
    db: Session, incident_id: str, p_hash: str, e_hash: str, provider: str, model: str
) -> SummaryRun | None:
    """Return a previous run for the same prompt and evidence, if one passed."""
    row = db.execute(
        select(AIRun)
        .where(
            AIRun.incident_id == incident_id,
            AIRun.prompt_hash == p_hash,
            AIRun.evidence_hash == e_hash,
            AIRun.provider == provider,
            AIRun.model == model,
        )
        .order_by(AIRun.created_at.desc())
        .limit(1)
    ).scalar_one_or_none()
    if row is None:
        return None

    stored = row.validation_result or {}
    if not stored.get("passed"):
        # A withheld run is not served from cache: the operator asked again, and a
        # transient provider problem may since have cleared.
        return None

    package = row.evidence_package or {}
    label_map = {item["evidence_id"]: real for item, real in _relabel(package, row.cited_evidence_ids or [])}

    result = _rehydrate(row, package)
    validation = ValidationResult(
        passed=True,
        statements_total=stored.get("statements_total", 0),
        statements_accepted=stored.get("statements_accepted", 0),
        statements_rejected=stored.get("statements_rejected", 0),
        rejections=stored.get("rejections", []),
        cited_evidence_ids=stored.get("cited_evidence_ids", []),
        unknown_citations=stored.get("unknown_citations", []),
        checks=stored.get("checks", {}),
    )
    return SummaryRun(
        run_id=row.run_id,
        incident_id=incident_id,
        summary=result,
        validation=validation,
        cached=True,
        disclaimer=required_disclaimer(),
        evidence_index=label_map,
    )


def _relabel(package: dict[str, Any], cited: list[str]) -> list[tuple[dict[str, Any], str]]:
    """Rebuild the label-to-evidence-id map from a stored package."""
    timeline = package.get("timeline", [])
    pairs: list[tuple[dict[str, Any], str]] = []
    for index, item in enumerate(timeline):
        real = cited[index] if index < len(cited) else item.get("evidence_id", "")
        pairs.append((item, real))
    return pairs


def _rehydrate(row: AIRun, package: dict[str, Any]) -> SummaryResult:
    """Reconstruct a SummaryResult from a stored run.

    The stored ``output`` is the provider's raw text. Re-running the provider to
    rebuild structure would defeat the cache, so the structured form is rebuilt from
    the raw JSON when it parses and otherwise presented as a single section.
    """
    import json

    from apps.api.app.services.ai.registry import Section, Statement

    sections: list[Section] = []
    uncertainty = ""
    limitations = ""
    try:
        payload = json.loads(row.output)
        for raw_section in payload.get("summary_sections", []) or []:
            statements = [
                Statement(
                    text=str(item.get("text", "")),
                    kind=str(item.get("type", "inference")),
                    evidence_ids=[str(value) for value in (item.get("evidence_ids") or [])],
                )
                for item in (raw_section.get("statements") or [])
            ]
            if statements:
                sections.append(Section(heading=str(raw_section.get("heading", "Summary")), statements=statements))
        uncertainty = str(payload.get("uncertainty", ""))
        limitations = str(payload.get("limitations", ""))
    except (TypeError, ValueError):
        sections = []

    if not sections:
        # The mock provider stores a placeholder rather than JSON. Regenerate the
        # structure deterministically from the stored package, which yields the same
        # narrative the run originally produced.
        from apps.api.app.services.ai.providers.mock import MockProvider

        regenerated = MockProvider().summarize(package, "")
        sections = regenerated.sections
        uncertainty = regenerated.uncertainty
        limitations = regenerated.limitations

    return SummaryResult(
        sections=sections,
        uncertainty=uncertainty,
        limitations=limitations,
        model=row.model,
        provider=row.provider,
        raw_output=row.output,
        latency_ms=row.latency_ms or 0,
    )


def _render_text(result: SummaryResult) -> str:
    """Flatten a summary to plain text, for the stored output and for exports."""
    import json

    return json.dumps(result.to_dict(), indent=2, default=str)


def run_history(db: Session, incident_id: str, limit: int = 20) -> list[dict[str, Any]]:
    """Previous runs for an incident, newest first.

    Returns the audit view: who ran it, which provider and model, the hashes, and
    what validation concluded — not the narrative text, which is fetched per run.
    """
    rows = (
        db.execute(select(AIRun).where(AIRun.incident_id == incident_id).order_by(AIRun.created_at.desc()).limit(limit))
        .scalars()
        .all()
    )

    return [
        {
            "run_id": row.run_id,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "actor": row.actor,
            "provider": row.provider,
            "model": row.model,
            "prompt_hash": row.prompt_hash,
            "evidence_hash": row.evidence_hash,
            "cited_evidence_ids": row.cited_evidence_ids or [],
            "latency_ms": row.latency_ms,
            "validation": row.validation_result or {},
            "passed": bool((row.validation_result or {}).get("passed")),
        }
        for row in rows
    ]
