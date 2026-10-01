"""Patterns that must NOT raise an incident.

These matter more than the positive cases. A detector that fires on everything is
easy to build and useless; these scenarios are what prove the trigger rules
discriminate. ``false_positive_hard_brake`` in particular is the difference between
an incident engine and a threshold alarm.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sim.scenarios._factory import (
    event_keys_for,
    normal_events,
    scenario_start,
    span,
    telemetry,
)
from sim.scenarios.registry import GroundTruth, ScenarioResult, register_scenario


@register_scenario(
    "normal",
    description="Baseline cruising with no risk pattern",
)
def generate_normal(
    vehicle_id: str = "veh_normal",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    count = int(params.get("count", 6) or 6)
    events = normal_events(
        vehicle_id,
        start_seq,
        start_time,
        count=count,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="normal",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=False,
            event_keys=event_keys_for(events),
            notes="No signal should be raised at all.",
        ),
    )


@register_scenario(
    "false_positive_hard_brake",
    description="One isolated harsh brake with a clean recovery",
)
def generate_false_positive_hard_brake(
    vehicle_id: str = "veh_false_brake",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    start = scenario_start(start_time)
    padding = int(params.get("padding_bytes", 0) or 0)
    events = [
        telemetry(vehicle_id, start_seq, start, speed_kmh=52, heading_deg=12, padding_bytes=padding),
        telemetry(
            vehicle_id,
            start_seq + 1,
            start + timedelta(seconds=1),
            speed_kmh=40,
            acceleration_mps2=-5.2,
            brake_force_pct=90,
            heading_deg=12,
            event="HARD_BRAKE",
            padding_bytes=padding,
        ),
        telemetry(
            vehicle_id,
            start_seq + 2,
            start + timedelta(seconds=2),
            speed_kmh=48,
            acceleration_mps2=2.0,
            heading_deg=12,
            event="RECOVERED",
            padding_bytes=padding,
        ),
    ]
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="false_positive_hard_brake",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            # One signal only, below min_distinct_signals. The engine must stay
            # quiet even though a threshold was crossed.
            should_create_incident=False,
            event_keys=event_keys_for(events),
            notes="Raises harsh_braking but must not trigger: single distinct signal.",
        ),
    )


@register_scenario(
    "diagnostic_only",
    description="A new DTC on an otherwise normal stream",
)
def generate_diagnostic_only(
    vehicle_id: str = "veh_diagnostic",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    events = normal_events(
        vehicle_id,
        start_seq,
        start_time,
        count=3,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    events[-1]["dtc"] = ["P0420"]
    events[-1]["evt"] = "DIAGNOSTIC_ONLY"
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="diagnostic_only",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            # dtc_fault satisfies neither min_distinct_signals nor
            # must_include_any, so a maintenance code alone is not an incident.
            should_create_incident=False,
            event_keys=event_keys_for(events),
            notes="Vehicle condition, not driving behaviour.",
        ),
    )
