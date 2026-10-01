"""Rapid acceleration contributing factor."""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors._magnitude import magnitude_score
from processor.factors.registry import FactorScore, ScoringContext, register_factor_scorer


@register_factor_scorer("rapid_acceleration", description="Acceleration above threshold")
def score_rapid_acceleration(
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
) -> FactorScore:
    return magnitude_score(
        "rapid_acceleration",
        signals,
        config,
        context,
        value_selector="peak",
        extra_components={"acceleration_events": len(signals)},
    )
