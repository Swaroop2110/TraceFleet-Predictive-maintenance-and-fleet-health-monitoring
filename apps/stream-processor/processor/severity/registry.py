"""Severity policy contract and registry.

Severity is a classification over the set of signals in an incident, not a score.
It is config-driven so that changing what counts as CRITICAL is a YAML edit
reviewed like any other config change, never a code change.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from packages.common.registry import Registry


class SeverityPolicy(Protocol):
    """Signature every severity policy implements."""

    def __call__(
        self,
        signals: list[Any],
        factors: list[Any],
        config: Any,
    ) -> str: ...


severity_registry: Registry[SeverityPolicy] = Registry("SeverityPolicyRegistry")


def register_severity_policy(name: str, **metadata: Any) -> Callable[[SeverityPolicy], SeverityPolicy]:
    return severity_registry.register(name, **metadata)


def discover_severity_policies() -> list[str]:
    import processor.severity as package

    return severity_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


class SeverityPolicyRegistry:
    """Facade the incident processor calls."""

    @staticmethod
    def available() -> list[str]:
        return severity_registry.keys()

    @staticmethod
    def classify(
        signals: list[Any],
        factors: list[Any],
        config: Any,
        policy_name: str = "config_driven_matrix",
    ) -> str:
        policy = severity_registry.get(policy_name)
        return policy(signals, factors, config)
