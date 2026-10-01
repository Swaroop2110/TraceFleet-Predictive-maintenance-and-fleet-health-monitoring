"""Large speed loss terminating at near-zero speed."""

from __future__ import annotations

from typing import Any

from processor.detectors._helpers import event_ts, window_max_speed_within
from processor.detectors.registry import Signal, register_detector


@register_detector("sudden_stop", description="Speed drop above threshold ending below a floor speed")
def detect_sudden_stop(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    to_speed_lt = float(params.get("to_speed_lt", 5))
    drop_threshold = float(params.get("speed_drop_kmh_gt", 50))
    within_s = float(params.get("within_s", 3))

    speed = float(event.get("speed_kmh") or 0.0)
    if speed >= to_speed_lt:
        return None

    # Bounded to within_s rather than the whole 30 s window. Using the window
    # maximum would report a sudden stop for a vehicle that was fast half a
    # minute ago and has since parked normally.
    peak = window_max_speed_within(window, event, within_s)
    drop = peak - speed
    if drop <= drop_threshold:
        return None

    return Signal(
        name="sudden_stop",
        event_key=event["event_key"],
        value=drop,
        threshold=drop_threshold,
        ts=event_ts(event),
        weight=weight,
        detail={
            "peak_speed_kmh": peak,
            "final_speed_kmh": speed,
            "within_s": within_s,
            "to_speed_lt": to_speed_lt,
        },
    )
