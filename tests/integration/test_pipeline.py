import processor.detectors  # noqa: F401 - register detector functions
from processor.detectors.registry import DetectorRegistry
from processor.window import VehicleWindow
from sim.scenarios.collision import generate_collision

from packages.schemas.models import RawTelemetryV1

RULES = {
    "overspeed": {"enabled": True, "weight": 0.25, "params": {"speed_kmh_gt": 90, "min_duration_s": 2}},
    "rapid_acceleration": {"enabled": True, "weight": 0.15, "params": {"accel_mps2_gt": 3}},
    "harsh_braking": {"enabled": True, "weight": 0.3, "params": {"brake_force_pct_gt": 70, "decel_mps2_lt": -4}},
    "abrupt_heading_change": {"enabled": True, "weight": 0.15, "params": {"delta_deg_gt": 25, "within_s": 1}},
    "sudden_stop": {"enabled": True, "weight": 0.3, "params": {"speed_drop_kmh_gt": 50, "to_speed_lt": 5}},
    "dtc_fault": {"enabled": True, "weight": 0.1, "params": {"any_new_dtc": True}},
}


def test_collision_scenario_validates_and_yields_correlated_detector_signals():
    window = VehicleWindow(window_seconds=30)
    names = set()
    for event in generate_collision().events:
        RawTelemetryV1.model_validate(event)
        event["event_key"] = str(event["seq"])
        window.add_event(event)
        names.update(signal.name for signal in DetectorRegistry.evaluate_all(window, event, RULES))
    assert {"rapid_acceleration", "harsh_braking", "sudden_stop", "dtc_fault"}.issubset(names)
