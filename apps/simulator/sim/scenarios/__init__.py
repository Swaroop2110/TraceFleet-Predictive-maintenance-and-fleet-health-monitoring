"""Simulator scenario plug-ins.

Importing this package discovers and registers every scenario module inside it.
"""

from sim.scenarios.registry import (
    GroundTruth,
    Scenario,
    ScenarioResult,
    available_scenarios,
    discover_scenarios,
    generate,
    register_scenario,
    scenario_registry,
    validate_against_config,
)

__all__ = [
    "GroundTruth",
    "Scenario",
    "ScenarioResult",
    "available_scenarios",
    "discover_scenarios",
    "generate",
    "generate_burst",
    # Direct references, kept because the existing tests import them by name.
    "generate_collision",
    "generate_collision_like",
    "generate_diagnostic_only",
    "generate_duplicate_storm",
    "generate_false_positive_hard_brake",
    "generate_fault",
    "generate_late_arrival",
    "generate_low_severity",
    "generate_medium_severity",
    "generate_normal",
    "generate_out_of_order",
    "generate_overspeed",
    "generate_overspeed_chain",
    "generate_schema_fault",
    "register_scenario",
    "scenario_registry",
    "validate_against_config",
]

discover_scenarios()

from sim.scenarios.collision import generate_collision, generate_collision_like  # noqa: E402
from sim.scenarios.data_faults import (  # noqa: E402
    generate_burst,
    generate_duplicate_storm,
    generate_fault,
    generate_late_arrival,
    generate_out_of_order,
    generate_schema_fault,
)
from sim.scenarios.negative import (  # noqa: E402
    generate_diagnostic_only,
    generate_false_positive_hard_brake,
    generate_normal,
)
from sim.scenarios.overspeed import generate_overspeed, generate_overspeed_chain  # noqa: E402
from sim.scenarios.severity_variants import (  # noqa: E402
    generate_low_severity,
    generate_medium_severity,
)
