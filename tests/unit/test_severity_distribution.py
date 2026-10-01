from datetime import timedelta

import pytest
from processor.incident_builder import classify_incident_type
from processor.incident_processor import IncidentProcessor
from sim.scenarios.registry import generate

from packages.common.ids import event_key
from packages.common.timeutil import now, to_iso


@pytest.mark.parametrize(
    "name,severity",
    [
        ("collision_like", "CRITICAL"),
        ("overspeed_chain", "HIGH"),
        ("medium_severity", "MEDIUM"),
        ("low_severity", "MEDIUM"),
    ],
)
def test_scenarios_match_shipped_configuration(name, severity):
    result = generate(name, "veh_test", start_time=now())
    processor = IncidentProcessor(connect=False)
    for raw in result.events:
        raw["event_key"] = event_key(raw["vehicle_id"], raw["seq"], raw["ts"])
        processor.process_event(raw)
    assert processor.published
    assert processor.published[-1].incident.severity.value == severity
    assert processor.published[-1].incident.status.value == "RECONSTRUCTING"
    final = dict(result.events[-1])
    final.update(
        ts=to_iso(now() + timedelta(seconds=90)),
        seq=900,
        event_key="final",
        speed_kmh=20,
        brake_force_pct=0,
        acceleration_mps2=0,
    )
    processor.process_event(final)
    assert any(e.incident.status.value == "OPEN" for e in processor.published)


@pytest.mark.parametrize(
    "names,expected",
    [
        ({"harsh_braking", "abrupt_heading_change"}, "LOSS_OF_CONTROL_LIKE"),
        ({"overspeed", "harsh_braking"}, "OVERSPEED_CHAIN"),
        ({"sudden_stop", "harsh_braking", "dtc_fault"}, "COLLISION_LIKE"),
    ],
)
def test_type_follows_strongest_observed_signal(names, expected):
    assert classify_incident_type(names) == expected
