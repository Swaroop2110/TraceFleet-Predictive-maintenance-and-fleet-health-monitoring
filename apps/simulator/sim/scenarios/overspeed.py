"""Sustained overspeed ending in defensive braking."""

from __future__ import annotations

from datetime import datetime

from sim.scenarios._factory import event_keys_for, from_points, scenario_start, span
from sim.scenarios.registry import GroundTruth, ScenarioResult, register_scenario

POINT_KEYS = ("speed_kmh", "acceleration_mps2", "brake_force_pct", "heading_deg", "event")

POINTS = (
    (0, 72, 0.1, 0, 42, "NORMAL"),
    (1, 98, 4.2, 0, 42, "RAPID_ACCELERATION"),
    (2, 108, 2.0, 0, 43, "OVERSPEED"),
    (3, 114, 1.0, 0, 43, "OVERSPEED"),
    (4, 116, 0.2, 0, 44, "OVERSPEED"),
    (5, 84, -5.1, 84, 45, "HARSH_BRAKE"),
    (6, 78, -1.0, 15, 46, "RECOVERY"),
)


@register_scenario("overspeed_chain", description="Sustained overspeed then harsh braking, no standstill")
def generate_overspeed_chain(
    vehicle_id: str = "veh_overspeed",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    start = scenario_start(start_time)
    events = from_points(
        vehicle_id,
        start_seq,
        start,
        POINTS,
        keys=POINT_KEYS,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="overspeed_chain",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=True,
            # Three distinct signals, but the vehicle never stops, so this lands on
            # the HIGH row rather than CRITICAL.
            expected_signals=["rapid_acceleration", "overspeed", "harsh_braking"],
            expected_severity="HIGH",
            event_keys=event_keys_for(events),
            notes="Distinguishes HIGH from CRITICAL: no sudden_stop.",
        ),
    )


generate_overspeed = generate_overspeed_chain
