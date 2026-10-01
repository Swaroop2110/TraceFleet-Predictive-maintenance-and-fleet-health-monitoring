"""Contributing-factor scorer plug-ins."""

from processor.factors.registry import (
    FactorScore,
    FactorScorer,
    FactorScorerRegistry,
    ScoringContext,
    discover_factor_scorers,
    factor_registry,
    register_factor_scorer,
)

__all__ = [
    "FactorScore",
    "FactorScorer",
    "FactorScorerRegistry",
    "ScoringContext",
    "discover_factor_scorers",
    "factor_registry",
    "register_factor_scorer",
]

discover_factor_scorers()
