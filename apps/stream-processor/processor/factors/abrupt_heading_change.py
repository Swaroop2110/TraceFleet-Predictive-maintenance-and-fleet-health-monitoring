"""Abrupt heading change contributing factor."""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors._magnitude import magnitude_score
from processor.factors.registry import FactorScore, ScoringContext, register_factor_scorer


@register_factor_scorer("abrupt_heading_change", description="Heading delta over a short interval")
def score_abrupt_heading_change(
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
) -> FactorScore:
    # Summed rather than peaked: two consecutive 40 degree swings are a more
    # significant loss of line than one, and a single peak would rate them equal.
    total_deflection = sum(float(signal.value) for signal in signals)
    fastest_rate = 0.0
    for signal in signals:
        detail = signal.detail or {}
        elapsed = float(detail.get("elapsed_s") or 0.0)
        if elapsed > 0:
            fastest_rate = max(fastest_rate, float(signal.value) / elapsed)

    return magnitude_score(
        "abrupt_heading_change",
        signals,
        config,
        context,
        value_selector="sum",
        extra_components={
            "total_deflection_deg": round(total_deflection, 2),
            "fastest_rate_deg_per_s": round(fastest_rate, 2),
            "direction_changes": len(signals),
        },
    )
