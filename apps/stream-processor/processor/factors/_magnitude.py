"""Shared magnitude/duration/confidence arithmetic for factor scorers.

Underscore-prefixed so registry discovery skips it.
"""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors.registry import FactorScore, ScoringContext

#: Seconds of sustained behaviour at which the duration bonus saturates.
DURATION_REFERENCE_S = 5.0


def normalise(value: float, config: FactorConfig) -> float:
    """Map an observed magnitude onto 0..1 across the configured range.

    The range starts at the detector threshold, not at zero. A 91 km/h reading
    against a 90 limit should score near zero, not 0.57 of the way to 160.
    """
    span = config.max_value - config.min_value
    if span <= 0:
        return 1.0
    return max(0.0, min(1.0, (value - config.min_value) / span))


def sustained_fraction(signals: list[Any]) -> float:
    """How sustained the behaviour was, as 0..1."""
    if len(signals) < 2:
        return 0.0
    span = (signals[-1].ts - signals[0].ts).total_seconds()
    return max(0.0, min(1.0, span / DURATION_REFERENCE_S))


def confidence_from_quality(signals: list[Any], context: ScoringContext) -> tuple[float, dict[str, Any]]:
    """Confidence from evidence completeness, with the deductions itemised."""
    confidence = 1.0
    deductions: dict[str, Any] = {}

    late = sum(1 for signal in signals if (signal.detail or {}).get("late"))
    if late:
        penalty = min(0.6, late * context.late_evidence_penalty)
        confidence -= penalty
        deductions["late_evidence"] = {"count": late, "penalty": round(penalty, 4)}

    if context.missing_fields:
        penalty = min(0.4, len(context.missing_fields) * context.missing_field_penalty)
        confidence -= penalty
        deductions["missing_fields"] = {
            "fields": sorted(context.missing_fields),
            "penalty": round(penalty, 4),
        }

    if context.schema_transformed:
        # A version-adapted event is usable but was not read exactly as sent.
        confidence -= 0.05
        deductions["schema_transformed"] = {"count": context.schema_transformed, "penalty": 0.05}

    # A single reading is weaker evidence than a corroborated sequence.
    if len(signals) == 1:
        confidence -= 0.1
        deductions["single_observation"] = {"penalty": 0.1}

    return max(context.min_confidence, min(1.0, confidence)), deductions


def magnitude_score(
    factor_type: str,
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
    *,
    value_selector: str = "peak",
    extra_components: dict[str, Any] | None = None,
) -> FactorScore:
    """Score = weight x normalised magnitude x duration multiplier, clamped to 1.

    ``value_selector`` picks the representative magnitude. Peak is the default
    because for a safety signal the worst reading in the cluster is the
    interesting one; averaging would let a long mild sequence mask a spike.
    """
    ordered = sorted(signals, key=lambda signal: signal.ts)
    values = [float(signal.value) for signal in ordered]
    if value_selector == "mean":
        representative = sum(values) / len(values)
    elif value_selector == "sum":
        representative = sum(values)
    else:
        representative = max(values)

    magnitude = normalise(representative, config)
    sustained = sustained_fraction(ordered)
    duration_multiplier = 1.0 + config.duration_bonus * sustained
    score = max(0.0, min(1.0, config.weight * magnitude * duration_multiplier))

    confidence, deductions = confidence_from_quality(ordered, context)

    components: dict[str, Any] = {
        "representative_value": round(representative, 4),
        "value_selector": value_selector,
        "observation_count": len(ordered),
        "normalised_magnitude": round(magnitude, 4),
        "range": {"min": config.min_value, "max": config.max_value},
        "weight": config.weight,
        "sustained_fraction": round(sustained, 4),
        "duration_multiplier": round(duration_multiplier, 4),
        "span_seconds": round((ordered[-1].ts - ordered[0].ts).total_seconds(), 3),
        "confidence_deductions": deductions,
    }
    if extra_components:
        components.update(extra_components)

    return FactorScore(
        factor_type=factor_type,
        score=score,
        confidence=confidence,
        # Identified by event_key *and* signal name. One event can raise several
        # signals, so an event_key alone would attribute every signal from that
        # moment to this factor and a UI click would highlight the wrong rows.
        evidence_ids=[signal_marker(signal) for signal in ordered],
        components=components,
    )


def signal_marker(signal: Any) -> str:
    """Composite key identifying one signal within an incident."""
    return f"{signal.event_key}|{signal.name}"
