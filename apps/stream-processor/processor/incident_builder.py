"""Turn a correlated signal cluster into a publishable incident payload.

Separated from the processing loop so the mapping from signals to incident is a
pure function of (candidate, config) and can be unit-tested without Kafka.
"""

from __future__ import annotations

from datetime import datetime

from packages.common.config import CorrelationConfig, DetectionRulesConfig, FactorsConfig, SeverityConfig
from packages.common.ids import evidence_id, incident_id, incident_idempotency_key
from packages.common.timeutil import now
from packages.schemas.models import (
    DataQuality,
    EvidenceItem,
    FactorScore,
    IncidentPayload,
    IncidentPhase,
    IncidentStatus,
    Severity,
    SignalPayload,
)
from processor.correlation import CandidateIncident
from processor.factors import FactorScorerRegistry, ScoringContext
from processor.severity import SeverityPolicyRegistry

#: Signal name to incident type, most specific first. A sudden stop in the
#: cluster is the strongest indicator that something physical happened, so it
#: wins over a speed pattern.
INCIDENT_TYPE_PRECEDENCE = (
    ("sudden_stop", "COLLISION_LIKE"),
    ("abrupt_heading_change", "LOSS_OF_CONTROL_LIKE"),
    ("overspeed", "OVERSPEED_CHAIN"),
    ("harsh_braking", "HARSH_BRAKING_EVENT"),
    ("dtc_fault", "DIAGNOSTIC_EVENT"),
)


def classify_incident_type(signal_names: set[str]) -> str:
    for name, incident_type in INCIDENT_TYPE_PRECEDENCE:
        if name in signal_names:
            return incident_type
    return "DRIVING_BEHAVIOR"


def assign_phase(signal_ts: datetime, trigger_ts: datetime, start_ts: datetime) -> str:
    """Which reconstruction phase a piece of evidence belongs to.

    Phases describe the incident's shape rather than a fixed clock: the first
    third of the run-up is baseline, the remainder escalation, the trigger moment
    itself is the trigger, and anything after is aftermath. Deriving them from the
    cluster's own span means a two-second event and a twenty-second event are both
    described sensibly.
    """
    if signal_ts > trigger_ts:
        return IncidentPhase.IMMEDIATE_AFTERMATH.value
    if signal_ts == trigger_ts:
        return IncidentPhase.TRIGGER.value
    run_up = (trigger_ts - start_ts).total_seconds()
    if run_up <= 0:
        return IncidentPhase.TRIGGER.value
    position = (signal_ts - start_ts).total_seconds() / run_up
    return IncidentPhase.BASELINE.value if position < 0.34 else IncidentPhase.ESCALATION.value


def build_incident(
    candidate: CandidateIncident,
    *,
    rules: DetectionRulesConfig,
    severity_config: SeverityConfig,
    factors_config: FactorsConfig,
    correlation: CorrelationConfig,
    status: IncidentStatus,
    end_ts: datetime | None = None,
) -> IncidentPayload:
    """Assemble the full incident payload from a candidate."""
    signals = sorted(candidate.signals, key=lambda item: (item.ts, item.name))
    signal_names = {signal.name for signal in signals}
    incident_type = candidate.incident_type or classify_incident_type(signal_names)

    # Identity is content-derived and computed once, so every republish of this
    # incident carries the same keys and the persister upsert collapses them.
    if candidate.idempotency_key is None:
        candidate.idempotency_key = incident_idempotency_key(
            candidate.tenant_id,
            candidate.vehicle_id,
            candidate.start_ts,
            incident_type,
            rules.window_seconds,
        )
    if candidate.incident_id is None:
        candidate.incident_id = incident_id(candidate.tenant_id, candidate.idempotency_key, candidate.start_ts)
    candidate.incident_type = incident_type

    context = ScoringContext.from_config(
        factors_config,
        late_events=candidate.late_events,
        duplicates_removed=candidate.duplicates_removed,
        missing_fields=set(candidate.missing_fields),
        schema_transformed=candidate.schema_transformed,
        total_events=len(signals),
    )
    scored = FactorScorerRegistry.score_all(signals, factors_config, context)
    severity = SeverityPolicyRegistry.classify(signals, scored, severity_config)
    confidence = FactorScorerRegistry.incident_confidence(scored, context)

    trigger_ts = candidate.triggered_at or (signals[-1].ts if signals else candidate.start_ts)

    evidence = [
        EvidenceItem(
            evidence_id=evidence_id(candidate.incident_id, signal.event_key, signal.name),
            event_key=signal.event_key,
            event_ts=signal.ts,
            signal=signal.name,
            value=float(signal.value),
            threshold=float(signal.threshold),
            weight=float(signal.weight),
            phase=assign_phase(signal.ts, trigger_ts, candidate.start_ts),
            flags=dict(signal.detail or {}),
        )
        for signal in signals
    ]

    # Scorers reference signals by "event_key|signal_name"; resolve those to the
    # persisted evidence ids so clicking a factor highlights exactly its own rows.
    by_marker = {f"{item.event_key}|{item.signal}": item.evidence_id for item in evidence}

    factors = [
        FactorScore(
            factor_type=score.factor_type,
            score=min(1.0, max(0.0, score.score)),
            confidence=min(1.0, max(0.0, score.confidence)),
            evidence_ids=[by_marker[marker] for marker in score.evidence_ids if marker in by_marker],
            components=score.components,
        )
        for score in scored
    ]

    return IncidentPayload(
        incident_id=candidate.incident_id,
        idempotency_key=candidate.idempotency_key,
        tenant_id=candidate.tenant_id,
        fleet_id=candidate.fleet_id,
        vehicle_id=candidate.vehicle_id,
        incident_type=incident_type,
        severity=Severity(severity),
        status=status,
        start_ts=candidate.start_ts,
        end_ts=end_ts or candidate.last_signal_ts,
        detected_at=candidate.triggered_at or now(),
        confidence=confidence,
        signals=[
            SignalPayload(
                name=signal.name,
                event_key=signal.event_key,
                value=float(signal.value),
                threshold=float(signal.threshold),
                ts=signal.ts,
                weight=float(signal.weight),
                detail=dict(signal.detail or {}),
            )
            for signal in signals
        ],
        evidence=evidence,
        factors=factors,
        data_quality=DataQuality(
            late_events=candidate.late_events,
            duplicates_removed=candidate.duplicates_removed,
            missing_fields=sorted(candidate.missing_fields),
            schema_transformed=candidate.schema_transformed,
            out_of_order_events=candidate.out_of_order_events,
        ),
        metadata={
            "revision": candidate.revision,
            "distinct_signals": len(signal_names),
            "signal_names": sorted(signal_names),
            "late_evidence_added": candidate.late_evidence_added,
            "correlation_window_s": correlation.correlation_window_s,
            "post_window_s": correlation.post_window_s,
            "severity_disclaimer": severity_config.disclaimer,
        },
        trace_id=candidate.trace_id,
    )
