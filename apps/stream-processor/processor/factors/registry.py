"""Contributing-factor scoring contract and registry.

A factor score answers "how strongly did this behaviour contribute to the
incident", on a 0..1 scale, from evidence the operator can inspect. It is
deliberately separate from severity: severity is a bucket for triage, factors are
the breakdown for investigation.

Score and confidence are independent. A single harsh brake measured on complete
data is a high score with high confidence; the same reading assembled from late,
partially-missing events keeps its score and loses confidence. Collapsing the two
would hide the distinction that matters to a claims analyst.

Language note: these are *contributing factors*, never causes or fault.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from packages.common.config import FactorConfig, FactorsConfig
from packages.common.registry import Registry


@dataclass
class ScoringContext:
    """Data-quality information that modulates confidence, not score."""

    late_events: int = 0
    duplicates_removed: int = 0
    missing_fields: set[str] = field(default_factory=set)
    schema_transformed: int = 0
    total_events: int = 0
    late_evidence_penalty: float = 0.15
    missing_field_penalty: float = 0.10
    min_confidence: float = 0.1

    @classmethod
    def from_config(cls, config: FactorsConfig | dict[str, Any], **counters: Any) -> ScoringContext:
        if isinstance(config, FactorsConfig):
            return cls(
                late_evidence_penalty=config.late_evidence_penalty,
                missing_field_penalty=config.missing_field_penalty,
                min_confidence=config.min_confidence,
                **counters,
            )
        return cls(
            late_evidence_penalty=float(config.get("late_evidence_penalty", 0.15)),
            missing_field_penalty=float(config.get("missing_field_penalty", 0.10)),
            min_confidence=float(config.get("min_confidence", 0.1)),
            **counters,
        )


@dataclass
class FactorScore:
    """One scored factor with every intermediate value exposed."""

    factor_type: str
    score: float
    confidence: float
    evidence_ids: list[str] = field(default_factory=list)
    components: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "factor_type": self.factor_type,
            "score": round(float(self.score), 4),
            "confidence": round(float(self.confidence), 4),
            "evidence_ids": list(self.evidence_ids),
            "components": self.components,
        }


class FactorScorer(Protocol):
    """Signature every factor scorer implements."""

    def __call__(
        self,
        signals: list[Any],
        config: FactorConfig,
        context: ScoringContext,
    ) -> FactorScore: ...


factor_registry: Registry[FactorScorer] = Registry("FactorScorerRegistry")


def register_factor_scorer(name: str, **metadata: Any) -> Callable[[FactorScorer], FactorScorer]:
    return factor_registry.register(name, **metadata)


def discover_factor_scorers() -> list[str]:
    import processor.factors as package

    return factor_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


class FactorScorerRegistry:
    """Facade the incident processor calls."""

    @staticmethod
    def available() -> list[str]:
        return factor_registry.keys()

    @staticmethod
    def score_all(
        signals: list[Any],
        config: FactorsConfig | dict[str, Any],
        context: ScoringContext | None = None,
    ) -> list[FactorScore]:
        """Score every factor present in ``signals``.

        A signal with no configured factor is skipped rather than defaulted: an
        unweighted factor would silently contribute zero and look like a scoring
        bug rather than a missing config entry.
        """
        factor_configs = _factor_configs(config)
        ctx = context or ScoringContext.from_config(config, total_events=len(signals))

        grouped: dict[str, list[Any]] = {}
        for signal in signals:
            grouped.setdefault(signal.name, []).append(signal)

        results: list[FactorScore] = []
        for name, group in grouped.items():
            factor_config = factor_configs.get(name)
            if factor_config is None:
                continue
            scorer = factor_registry.get_or_none(name)
            if scorer is None:
                # A configured factor with no plug-in file falls back to the
                # generic magnitude scorer so config and code can be added in
                # either order.
                from processor.factors._magnitude import magnitude_score

                results.append(magnitude_score(name, group, factor_config, ctx))
                continue
            results.append(scorer(group, factor_config, ctx))

        results.sort(key=lambda item: item.score, reverse=True)
        return results

    @staticmethod
    def incident_confidence(factors: list[FactorScore], context: ScoringContext) -> float:
        """Overall incident confidence.

        Driven by the best-supported factor rather than the mean: one
        well-evidenced signal should not be diluted by several marginal ones.
        Data-quality penalties then apply once at the incident level.
        """
        if not factors:
            return context.min_confidence
        best = max(factor.confidence for factor in factors)
        corroboration = min(1.0, len(factors) / 3.0)
        confidence = best * (0.7 + 0.3 * corroboration)
        return round(max(context.min_confidence, min(1.0, confidence)), 4)


def _factor_configs(config: FactorsConfig | dict[str, Any]) -> dict[str, FactorConfig]:
    if isinstance(config, FactorsConfig):
        return dict(config.factors)
    raw = config.get("factors", config) if isinstance(config, dict) else {}
    parsed: dict[str, FactorConfig] = {}
    for name, values in (raw or {}).items():
        parsed[name] = values if isinstance(values, FactorConfig) else FactorConfig.model_validate(values)
    return parsed
