"""Sudden stop contributing factor."""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors._magnitude import magnitude_score
from processor.factors.registry import FactorScore, ScoringContext, register_factor_scorer


@register_factor_scorer("sudden_stop", description="Speed lost before coming to rest")
def score_sudden_stop(
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
) -> FactorScore:
    details = [signal.detail or {} for signal in signals]
    peak_speed = max((float(detail.get("peak_speed_kmh", 0.0)) for detail in details), default=0.0)
    final_speed = min((float(detail.get("final_speed_kmh", 0.0)) for detail in details), default=0.0)

    # Deduplicate: the detector fires on every event while the vehicle remains
    # stationary, so a long standstill would otherwise look like repeated stops.
    return magnitude_score(
        "sudden_stop",
        signals[:1] if len(signals) > 1 else signals,
        config,
        context,
        value_selector="peak",
        extra_components={
            "peak_speed_kmh": round(peak_speed, 2),
            "final_speed_kmh": round(final_speed, 2),
            "speed_lost_kmh": round(peak_speed - final_speed, 2),
            "stationary_observations": len(signals),
        },
    )
