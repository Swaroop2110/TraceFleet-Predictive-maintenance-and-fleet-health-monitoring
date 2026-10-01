"""Two- and three-signal patterns that exercise the middle of the severity matrix."""

from __future__ import annotations

from datetime import datetime

from sim.scenarios._factory import event_keys_for, from_points, scenario_start, span
from sim.scenarios.registry import GroundTruth, ScenarioResult, register_scenario

POINT_KEYS = ("speed_kmh", "acceleration_mps2", "brake_force_pct", "heading_deg", "event")

MEDIUM_POINTS = (
    (0, 65, 0.1, 0, 90, "NORMAL"),
    (1, 78, 4.2, 0, 90, "RAPID_ACCELERATION"),
    (2, 64, -5.1, 86, 90, "HARSH_BRAKE"),
)

#: Overspeed held across seconds 1-4 so the run satisfies min_duration_s, then a
#: single brake. Two distinct signals, one of them repeated.
LOW_POINTS = (
    (0, 78, 0.1, 0, 90, "NORMAL"),
    (1, 94, 0.2, 0, 90, "OVERSPEED_START"),
    (2, 96, 0.2, 0, 90, "OVERSPEED"),
    (3, 97, 0.1, 0, 90, "OVERSPEED"),
    (4, 98, 0.1, 0, 90, "OVERSPEED"),
    (5, 83, -5.1, 82, 90, "BRAKING"),
)


@register_scenario("medium_severity", description="Acceleration then harsh braking: exactly two signals")
def generate_medium_severity(
    vehicle_id: str = "veh_medium",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    start = scenario_start(start_time)
    events = from_points(
        vehicle_id,
        start_seq,
        start,
        MEDIUM_POINTS,
        keys=POINT_KEYS,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="medium_severity",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=True,
            expected_signals=["rapid_acceleration", "harsh_braking"],
            expected_severity="MEDIUM",
            event_keys=event_keys_for(events),
            notes="Minimum viable incident: exactly min_distinct_signals.",
        ),
    )


@register_scenario("low_severity", description="Repeated overspeed then one brake")
def generate_low_severity(
    vehicle_id: str = "veh_low",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    start = scenario_start(start_time)
    events = from_points(
        vehicle_id,
        start_seq,
        start,
        LOW_POINTS,
        keys=POINT_KEYS,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="low_severity",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=True,
            expected_signals=["overspeed", "harsh_braking"],
            # Named for its intent, but with the shipped matrix two distinct
            # signals resolve to MEDIUM. The LOW row is unreachable while
            # min_distinct_signals is 2; see config/severity.yaml.
            expected_severity="MEDIUM",
            event_keys=event_keys_for(events),
            notes="Repeated signal. Demonstrates that the LOW row needs min_distinct_signals=1.",
        ),
    )
