"""Collision-like chain: the PRD Appendix A pattern."""

from __future__ import annotations

from datetime import datetime

from sim.scenarios._factory import event_keys_for, from_points, scenario_start, span
from sim.scenarios.registry import GroundTruth, ScenarioResult, register_scenario

POINT_KEYS = ("speed_kmh", "acceleration_mps2", "brake_force_pct", "heading_deg", "dtc", "event")

#: Acceleration, overspeed, abrupt heading change, harsh braking, rapid
#: deceleration to a stop, new DTC. The overspeed run spans seconds 1-3 so it
#: satisfies min_duration_s=2; a shorter run would not raise the signal at all.
POINTS = (
    (0, 72, 0.2, 0, 88, [], "NORMAL"),
    (1, 96, 4.5, 0, 89, [], "RAPID_ACCELERATION"),
    (2, 112, 2.0, 0, 90, [], "OVERSPEED"),
    (3, 108, -1.0, 0, 92, [], "OVERSPEED"),
    (4, 74, -3.5, 65, 138, [], "PRE_IMPACT"),
    (5, 0, -8.0, 100, 214, ["P0101"], "COLLISION_LIKE"),
    (6, 0, 0.0, 100, 214, ["P0101"], "POST_IMPACT"),
)


@register_scenario("collision_like", description="Six-signal collision-like chain ending at a standstill")
def generate_collision_like(
    vehicle_id: str = "veh_collision",
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
            scenario_id="collision_like",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=True,
            # Six distinct signals including both sudden_stop and harsh_braking,
            # which is the CRITICAL row of the severity matrix.
            expected_signals=[
                "rapid_acceleration",
                "overspeed",
                "abrupt_heading_change",
                "harsh_braking",
                "sudden_stop",
                "dtc_fault",
            ],
            expected_severity="CRITICAL",
            event_keys=event_keys_for(events),
            notes="PRD Appendix A reference pattern.",
        ),
    )


#: Retained so existing imports and tests keep working.
generate_collision = generate_collision_like
