"""Diagnostic fault contributing factor."""

from __future__ import annotations

from typing import Any

from packages.common.config import FactorConfig
from processor.factors._magnitude import magnitude_score
from processor.factors.registry import FactorScore, ScoringContext, register_factor_scorer


@register_factor_scorer("dtc_fault", description="Newly asserted diagnostic trouble codes")
def score_dtc_fault(
    signals: list[Any],
    config: FactorConfig,
    context: ScoringContext,
) -> FactorScore:
    codes: list[str] = []
    for signal in signals:
        codes.extend((signal.detail or {}).get("new_dtc_codes", []))
    unique_codes = sorted(set(codes))

    return magnitude_score(
        "dtc_fault",
        signals,
        config,
        context,
        value_selector="sum",
        extra_components={
            # A diagnostic code records vehicle condition, not driving behaviour.
            # It is reported as context for the incident, never as a cause.
            "new_dtc_codes": unique_codes,
            "distinct_code_count": len(unique_codes),
        },
    )
