"""Sustained speed above a configured limit."""

from __future__ import annotations

from typing import Any

from processor.detectors._helpers import event_ts, run_duration_s, trailing_run
from processor.detectors.registry import Signal, register_detector


@register_detector("overspeed", description="Speed above threshold for a minimum duration")
def detect_overspeed(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    threshold = float(params.get("speed_kmh_gt", 90))
    min_duration = float(params.get("min_duration_s", 0))
    speed = float(event.get("speed_kmh") or 0.0)
    if speed <= threshold:
        return None

    # Only an unbroken run counts. A momentary spike above the limit is not the
    # same behaviour as holding 30 km/h over for several seconds, and the
    # min_duration_s parameter exists precisely to separate them.
    run = trailing_run(window.events, lambda candidate: float(candidate.get("speed_kmh") or 0.0) > threshold)
    duration = run_duration_s(run)
    if duration < min_duration:
        return None

    return Signal(
        name="overspeed",
        event_key=event["event_key"],
        value=speed,
        threshold=threshold,
        ts=event_ts(event),
        weight=weight,
        detail={
            "sustained_seconds": round(duration, 3),
            "min_duration_s": min_duration,
            "events_in_run": len(run),
            "peak_speed_kmh": max((float(item.get("speed_kmh") or 0.0) for item in run), default=speed),
        },
    )
