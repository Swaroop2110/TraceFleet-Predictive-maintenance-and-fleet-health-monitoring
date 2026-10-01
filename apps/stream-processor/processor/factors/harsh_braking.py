"""Harsh braking contributing factor."""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors._magnitude import magnitude_score
from processor.factors.registry import FactorScore, ScoringContext, register_factor_scorer


@register_factor_scorer("harsh_braking", description="Brake force with corroborating deceleration")
def score_harsh_braking(
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
) -> FactorScore:
    decelerations = [float((signal.detail or {}).get("deceleration_mps2", 0.0)) for signal in signals]
    strongest_decel = min(decelerations) if decelerations else 0.0

    return magnitude_score(
        "harsh_braking",
        signals,
        config,
        context,
        value_selector="peak",
        extra_components={
            # Recorded alongside pedal force so an operator can see the two
            # independent measurements that had to agree for the signal to fire.
            "strongest_deceleration_mps2": round(strongest_decel, 3),
            "brake_applications": len(signals),
        },
    )
