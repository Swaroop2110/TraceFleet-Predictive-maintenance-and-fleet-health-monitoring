from pathlib import Path

import processor.detectors  # noqa: F401
import yaml
from processor.detectors.registry import DetectorRegistry
from processor.window import VehicleWindow
from pytest_bdd import given, parsers, scenario, then, when
from sim.scenarios.collision import generate_collision

ROOT = Path(__file__).resolve().parents[2]


@scenario("features/incident.feature", "Collision-like chain produces enough distinct signals")
def test_collision_detection():
    pass


@given("a collision-like telemetry sequence", target_fixture="events")
def collision_events():
    return generate_collision().events


@when("the detection window evaluates the sequence", target_fixture="signal_names")
def evaluate_events(events):
    with (ROOT / "config" / "detection_rules.yaml").open(encoding="utf-8") as file:
        rules = yaml.safe_load(file)["rules"]
    window = VehicleWindow(30)
    names = set()
    for event in events:
        event["event_key"] = str(event["seq"])
        window.add_event(event)
        names.update(signal.name for signal in DetectorRegistry.evaluate_all(window, event, rules))
    return names


@then("harsh braking and sudden stop signals are present")
def assert_collision_signals(signal_names):
    assert {"harsh_braking", "sudden_stop"}.issubset(signal_names)


@scenario("features/incident.feature", "Required scenario outcomes follow the rules")
def test_required_outcomes():
    pass


@given(parsers.parse('the "{name}" scenario'), target_fixture="case")
def scenario_case(name):
    from sim.scenarios.registry import generate

    return generate(name, "bdd_" + name)


@when("the incident engine processes its events", target_fixture="engine")
def process_case(case):
    from processor.incident_processor import IncidentProcessor
    from processor.normalizer import Normalizer, QuarantineError, TenantResolver, validate

    processor = IncidentProcessor(connect=False)
    normalizer = Normalizer(TenantResolver())
    seen = set()
    for event in case.events:
        try:
            raw, transformed = validate(event)
        except QuarantineError:
            continue
        canonical = normalizer.to_canonical(raw, schema_transformed=transformed)
        if canonical.event_key not in seen:
            processor.process_event(canonical.model_dump(mode="json"))
            seen.add(canonical.event_key)
    return processor


@then("the incident outcome matches hidden ground truth")
def check_case(case, engine):
    assert bool(engine.published) == case.ground_truth.should_create_incident
    if engine.published:
        assert engine.published[-1].incident.severity.value == case.ground_truth.expected_severity
