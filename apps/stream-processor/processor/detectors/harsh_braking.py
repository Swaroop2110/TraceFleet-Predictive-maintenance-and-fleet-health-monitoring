"""Heavy brake application corroborated by actual deceleration."""

from __future__ import annotations

from typing import Any

from processor.detectors._helpers import event_ts
from processor.detectors.registry import Signal, register_detector


@register_detector("harsh_braking", description="Brake force above threshold with corroborating deceleration")
def detect_harsh_braking(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    brake_threshold = float(params.get("brake_force_pct_gt", 70))
    decel_threshold = float(params.get("decel_mps2_lt", -4.0))
    brake = float(event.get("brake_force_pct") or 0.0)
    acceleration = float(event.get("acceleration_mps2") or 0.0)

    # Both conditions are required. Pedal pressure alone can be a stuck sensor,
    # and deceleration alone can be a hill or engine braking; together they are
    # a brake event.
    if brake <= brake_threshold or acceleration >= decel_threshold:
        return None

    return Signal(
        name="harsh_braking",
        event_key=event["event_key"],
        value=brake,
        threshold=brake_threshold,
        ts=event_ts(event),
        weight=weight,
        detail={
            "deceleration_mps2": acceleration,
            "decel_threshold_mps2": decel_threshold,
            "speed_kmh": float(event.get("speed_kmh") or 0.0),
        },
    )
