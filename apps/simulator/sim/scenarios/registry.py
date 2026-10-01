"""Scenario contract and registry.

A scenario produces raw telemetry *and* the hidden ground truth describing what the
detection engine ought to conclude from it. Both come from the same place on
purpose: a label maintained separately from the generator drifts, and a drifted
label silently corrupts the accuracy figure rather than failing.

Ground truth never travels the Kafka path. It is written straight to
``ground_truth_incident`` in Postgres, so nothing in the detection chain can see it.

Adding a scenario means one file with ``@register_scenario`` plus an entry in
``config/scenarios.yaml``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from packages.common.registry import Registry


@dataclass
class GroundTruth:
    """What the engine should conclude, recorded before it runs."""

    scenario_id: str
    vehicle_id: str
    start_ts: datetime
    end_ts: datetime
    should_create_incident: bool
    expected_signals: list[str] = field(default_factory=list)
    expected_severity: str | None = None
    event_keys: list[str] = field(default_factory=list)
    #: Free-form notes describing what the scenario is testing.
    notes: str = ""

    def to_row(self, run_id: str, tenant_id: str) -> dict[str, Any]:
        from packages.common.ids import sha256_hex

        return {
            # Deterministic so re-running a seeded scenario updates its row rather
            # than appending a second copy.
            "id": sha256_hex(run_id, self.scenario_id, self.vehicle_id, self.start_ts.isoformat())[:32],
            "run_id": run_id,
            "scenario_id": self.scenario_id,
            "tenant_id": tenant_id,
            "vehicle_id": self.vehicle_id,
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "expected_signals": self.expected_signals,
            "expected_severity": self.expected_severity,
            "should_create_incident": self.should_create_incident,
            "event_keys": self.event_keys,
        }


@dataclass
class ScenarioResult:
    """Events to publish, plus the ground truth they should produce."""

    events: list[dict[str, Any]]
    ground_truth: GroundTruth | None = None

    def __len__(self) -> int:
        return len(self.events)


class Scenario(Protocol):
    """Signature every scenario implements."""

    def __call__(
        self,
        vehicle_id: str,
        start_seq: int = 0,
        start_time: datetime | None = None,
        **params: Any,
    ) -> ScenarioResult: ...


scenario_registry: Registry[Scenario] = Registry("SimulatorScenarioRegistry")


def register_scenario(name: str, **metadata: Any) -> Callable[[Scenario], Scenario]:
    """Register a scenario plug-in under ``name``.

    The name must match a key in ``config/scenarios.yaml``; the config validator
    cross-checks the two so a scenario cannot be shipped without its ground-truth
    expectations.
    """
    return scenario_registry.register(name, **metadata)


def discover_scenarios() -> list[str]:
    import sim.scenarios as package

    return scenario_registry.discover(package.__name__, list(package.__path__), skip=("registry",))


def available_scenarios() -> list[str]:
    discover_scenarios()
    return scenario_registry.keys()


def generate(
    name: str,
    vehicle_id: str,
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: Any,
) -> ScenarioResult:
    """Run one scenario by name."""
    discover_scenarios()
    return scenario_registry.get(name)(vehicle_id, start_seq, start_time, **params)


def validate_against_config() -> dict[str, list[str]]:
    """Cross-check the registry against ``config/scenarios.yaml``.

    Reports scenarios that exist in code but not in config (so their ground-truth
    expectations are undefined) and vice versa (so the config references something
    unrunnable). Called at simulator startup.
    """
    from packages.common import config as cfg

    discover_scenarios()
    registered = set(scenario_registry.keys())
    configured = set(cfg.scenarios().scenarios)
    return {
        "missing_config": sorted(registered - configured),
        "missing_implementation": sorted(configured - registered),
    }
