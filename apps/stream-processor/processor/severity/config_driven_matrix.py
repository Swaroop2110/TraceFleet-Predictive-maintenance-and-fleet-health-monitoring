"""Default severity policy: first match over a specificity-ordered matrix."""

from __future__ import annotations

from typing import Any

from packages.common.config import SEVERITIES, SeverityConfig
from processor.severity.registry import register_severity_policy


@register_severity_policy(
    "config_driven_matrix",
    description="Evaluate config/severity.yaml rows most-specific-first",
)
def config_driven_matrix(
    signals: list[Any],
    factors: list[Any],
    config: Any,
) -> str:
    """Classify an incident from its signal set.

    The matrix in ``severity.yaml`` is authored general-to-specific because that
    reads naturally. Evaluating it in file order with last-match-wins is what
    produced the bug where a repeated two-signal incident was reported LOW: the
    LOW row sits after MEDIUM and also matched. Sorting by specificity and taking
    the first match makes the outcome independent of file order.
    """
    names = [signal.name for signal in signals if signal.name != "anomaly_score"]
    distinct = set(names)
    distinct_count = len(distinct)
    repeated = len(names) > distinct_count

    rows = _rows(config)
    for when, severity in rows:
        if _matches(when, distinct, distinct_count, repeated):
            return severity

    return _default(config)


def _matches(
    when: dict[str, Any],
    distinct: set[str],
    distinct_count: int,
    repeated: bool,
) -> bool:
    minimum = when.get("distinct_signals_gte")
    if minimum is not None and distinct_count < int(minimum):
        return False

    # `includes` is all-of: the CRITICAL row naming both sudden_stop and
    # harsh_braking describes the combination, not either one.
    required = when.get("includes") or []
    if required and not set(required).issubset(distinct):
        return False

    expected_repeat = when.get("repeated")
    return not (expected_repeat is not None and bool(expected_repeat) != repeated)


def _rows(config: Any) -> list[tuple[dict[str, Any], str]]:
    """Normalise the matrix to (when, severity) pairs, most specific first."""
    if isinstance(config, SeverityConfig):
        return [(rule.when.model_dump(exclude_none=True), rule.severity) for rule in config.ordered()]

    # Plain list/dict form, used by older tests and the admin preview endpoint.
    raw = config.get("matrix", []) if isinstance(config, dict) else list(config or [])
    pairs = [(dict(row.get("when", {})), str(row.get("severity", "INFO"))) for row in raw]
    pairs.sort(
        key=lambda item: (
            int(item[0].get("distinct_signals_gte") or 0),
            len(item[0].get("includes") or []),
            1 if item[0].get("repeated") is not None else 0,
            SEVERITIES.index(item[1]) if item[1] in SEVERITIES else 0,
        ),
        reverse=True,
    )
    return pairs


def _default(config: Any) -> str:
    if isinstance(config, SeverityConfig):
        return config.default
    if isinstance(config, dict):
        return str(config.get("default", "INFO"))
    return "INFO"
