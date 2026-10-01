"""Delivery and schema faults: duplicates, lateness, reordering, invalid payloads.

These exercise the ingest path rather than the detectors. Each one targets a
specific guarantee: dedup suppression, lateness flagging, event-time reordering, and
quarantine with a specific reason code.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sim.scenarios._factory import event_keys_for, normal_events, scenario_start, span
from sim.scenarios.registry import GroundTruth, ScenarioResult, register_scenario


@register_scenario("duplicate_storm", description="The same event republished N times")
def generate_duplicate_storm(
    vehicle_id: str = "veh_duplicate",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    copies = int(params.get("copies", 8) or 8)
    original = normal_events(
        vehicle_id,
        start_seq,
        start_time,
        count=1,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )[0]
    original["evt"] = "DUPLICATE_STORM"
    # Identical payloads, so every copy derives the same event_key and all but one
    # must be suppressed by the dedup gate.
    events = [dict(original) for _ in range(copies)]
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="duplicate_storm",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=False,
            event_keys=event_keys_for(events[:1]),
            notes=f"{copies} identical events; {copies - 1} must be suppressed and counted.",
        ),
    )


@register_scenario("out_of_order", description="Events delivered with two positions swapped")
def generate_out_of_order(
    vehicle_id: str = "veh_out_of_order",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    events = normal_events(
        vehicle_id,
        start_seq,
        start_time,
        count=4,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    # Delivered 0,2,1,3. Event times are unchanged, so the window must re-sort them
    # and detector arithmetic must be unaffected.
    reordered = [events[0], events[2], events[1], events[3]]
    first, last = span(reordered)
    return ScenarioResult(
        events=reordered,
        ground_truth=GroundTruth(
            scenario_id="out_of_order",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=False,
            event_keys=event_keys_for(reordered),
            notes="Arrival order 0,2,1,3 must be re-sorted by event time.",
        ),
    )


@register_scenario("late_arrival", description="Events whose event time is well in the past")
def generate_late_arrival(
    vehicle_id: str = "veh_late",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    delay_s = float(params.get("delay_s", 45) or 45)
    # Backdated beyond lateness_window_s (30). They must be stored and flagged,
    # never dropped and never re-stamped with arrival time.
    start = scenario_start(start_time) - timedelta(seconds=delay_s)
    events = normal_events(
        vehicle_id,
        start_seq,
        start,
        count=3,
        padding_bytes=int(params.get("padding_bytes", 0) or 0),
    )
    for event in events:
        event["evt"] = "LATE_ARRIVAL"
    first, last = span(events)
    return ScenarioResult(
        events=events,
        ground_truth=GroundTruth(
            scenario_id="late_arrival",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=False,
            event_keys=event_keys_for(events),
            notes=f"Backdated {delay_s}s; must be flagged late and retained.",
        ),
    )


@register_scenario("schema_fault", description="Payloads that must land in quarantine")
def generate_schema_fault(
    vehicle_id: str = "veh_schema_fault",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    base = normal_events(vehicle_id, start_seq, start_time, count=1)[0]

    # One fault per event so each maps to a single reason code, which is what makes
    # events_rejected_total{reason} diagnosable.
    faults: list[dict] = [
        {**base, "speed_kmh": -12, "evt": "SCHEMA_FAULT"},
        {**base, "seq": base["seq"] + 1, "lat": 127.0, "evt": "SCHEMA_FAULT"},
        {**base, "seq": base["seq"] + 2, "vin": "BADVIN", "evt": "SCHEMA_FAULT"},
        {**base, "seq": base["seq"] + 3, "schema_version": "9.9", "evt": "SCHEMA_FAULT"},
        {key: value for key, value in base.items() if key != "speed_kmh"},
    ]
    faults[-1]["seq"] = base["seq"] + 4
    faults[-1]["evt"] = "SCHEMA_FAULT"

    first, last = span([base])
    return ScenarioResult(
        events=faults,
        ground_truth=GroundTruth(
            scenario_id="schema_fault",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=False,
            event_keys=[],
            notes=(
                "Five invalid payloads expecting OUT_OF_RANGE, OUT_OF_RANGE, "
                "INVALID_VIN, UNKNOWN_SCHEMA_VERSION, MISSING_FIELD."
            ),
        ),
    )


@register_scenario("burst", description="Baseline traffic at the profile burst multiplier")
def generate_burst(
    vehicle_id: str = "veh_burst",
    start_seq: int = 0,
    start_time: datetime | None = None,
    **params: object,
) -> ScenarioResult:
    """Volume, not pattern.

    Exercises back-pressure and consumer lag. The rate multiplier is applied by the
    producer; this scenario only supplies a denser block of ordinary events.
    """
    multiplier = float(params.get("multiplier", 3.0) or 3.0)
    count = max(1, int(params.get("count", 10) or 10) * int(multiplier))
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
            scenario_id="burst",
            vehicle_id=vehicle_id,
            start_ts=first,
            end_ts=last,
            should_create_incident=False,
            event_keys=event_keys_for(events),
            notes=f"{count} normal events at {multiplier}x density.",
        ),
    )


generate_fault = generate_schema_fault
