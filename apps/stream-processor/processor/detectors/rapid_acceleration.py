"""Longitudinal acceleration above a configured limit."""

from __future__ import annotations

from typing import Any

from processor.detectors._helpers import event_ts
from processor.detectors.registry import Signal, register_detector


@register_detector("rapid_acceleration", description="Acceleration above threshold")
def detect_rapid_acceleration(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    threshold = float(params.get("accel_mps2_gt", 3.0))
    acceleration = float(event.get("acceleration_mps2") or 0.0)
    if acceleration <= threshold:
        return None

    return Signal(
        name="rapid_acceleration",
        event_key=event["event_key"],
        value=acceleration,
        threshold=threshold,
        ts=event_ts(event),
        weight=weight,
        detail={"speed_kmh": float(event.get("speed_kmh") or 0.0)},
    )
