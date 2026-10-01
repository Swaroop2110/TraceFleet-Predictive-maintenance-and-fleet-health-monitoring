"""Overspeed contributing factor."""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors._magnitude import magnitude_score
from processor.factors.registry import FactorScore, ScoringContext, register_factor_scorer


@register_factor_scorer("overspeed", description="Speed above limit, weighted by how long it was held")
def score_overspeed(
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
) -> FactorScore:
    # The detector already reports how long the run lasted; prefer that over the
    # gap between signal timestamps, which only sees the events that fired.
    sustained = max(
        (float((signal.detail or {}).get("sustained_seconds", 0.0)) for signal in signals),
        default=0.0,
    )
    peak = max((float((signal.detail or {}).get("peak_speed_kmh", signal.value)) for signal in signals), default=0.0)

    return magnitude_score(
        "overspeed",
        signals,
        config,
        context,
        value_selector="peak",
        extra_components={
            "detector_sustained_seconds": round(sustained, 3),
            "peak_speed_kmh": round(peak, 2),
            "excess_over_limit_kmh": round(max(0.0, peak - config.min_value), 2),
        },
    )
