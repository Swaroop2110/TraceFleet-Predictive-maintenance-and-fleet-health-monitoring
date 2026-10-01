"""Detector contract and registry.

A detector is a pure function over a per-vehicle window plus the event that just
arrived. It returns explainable :class:`Signal` objects carrying the rule name,
the observed value, and the threshold it crossed, so every downstream claim can
be traced back to a number and a configured limit. No opaque scores.

Adding a detector: drop a file in this package with ``@register_detector`` and
add an entry to ``config/detection_rules.yaml``. Nothing else changes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from packages.common import metrics
from packages.common.registry import Registry


@dataclass
class Signal:
    """One threshold crossing, with the evidence needed to explain it."""

    name: str
    event_key: str
    value: float
    threshold: float
    ts: datetime
    weight: float
    #: Intermediate values the detector used, surfaced in the UI and exports.
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from packages.common.timeutil import to_iso

        return {
            "name": self.name,
            "event_key": self.event_key,
            "value": float(self.value),
            "threshold": float(self.threshold),
            "ts": to_iso(self.ts),
            "weight": float(self.weight),
            "detail": self.detail,
        }


class Detector(Protocol):
    """Signature every detector plug-in implements."""

    def __call__(
        self,
        window: Any,
        event: dict[str, Any],
        params: dict[str, Any],
        weight: float,
    ) -> Signal | list[Signal] | None: ...


#: Module-level registry. Import this, or the decorator below, from a plug-in.
detector_registry: Registry[Detector] = Registry("DetectorRegistry")


def register_detector(name: str, **metadata: Any) -> Callable[[Detector], Detector]:
    """Register a detector plug-in under ``name``."""
    return detector_registry.register(name, **metadata)


def discover_detectors() -> list[str]:
    """Import every detector module in this package."""
    import processor.detectors as package

    return detector_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


class DetectorRegistry:
    """Evaluation facade over the registry.

    Kept as a class because the processor and the tests both call
    ``DetectorRegistry.evaluate_all``, and because it centralises the
    enabled-check so an individual detector never has to know about config.
    """

    @staticmethod
    def register(name: str, **metadata: Any) -> Callable[[Detector], Detector]:
        """Backwards-compatible alias for :func:`register_detector`."""
        return register_detector(name, **metadata)

    @staticmethod
    def available() -> list[str]:
        return detector_registry.keys()

    @staticmethod
    def evaluate_all(
        window: Any,
        event: dict[str, Any],
        rules_config: dict[str, Any],
    ) -> list[Signal]:
        """Run every enabled detector against one event.

        ``rules_config`` may hold either ``RuleConfig`` models or plain dicts;
        the processor passes models while older tests pass dicts.
        """
        signals: list[Signal] = []
        for name, detector in detector_registry.all().items():
            rule = rules_config.get(name)
            if rule is None:
                continue
            enabled, params, weight = _rule_fields(rule)
            if not enabled:
                continue
            produced = detector(window, event, params, weight)
            if not produced:
                continue
            batch = produced if isinstance(produced, list) else [produced]
            for signal in batch:
                metrics.signals_detected_total.labels(detector=signal.name).inc()
            signals.extend(batch)
        # Sort by event time so evidence ordering is deterministic regardless of
        # the order detectors happen to be registered in.
        signals.sort(key=lambda signal: (signal.ts, signal.name))
        return signals


def _rule_fields(rule: Any) -> tuple[bool, dict[str, Any], float]:
    if isinstance(rule, dict):
        return bool(rule.get("enabled", False)), dict(rule.get("params", {})), float(rule.get("weight", 0.1))
    return bool(getattr(rule, "enabled", False)), dict(getattr(rule, "params", {})), float(getattr(rule, "weight", 0.1))
