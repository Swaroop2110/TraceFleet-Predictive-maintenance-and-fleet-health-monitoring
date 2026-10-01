"""Large bearing change over a short interval."""

from __future__ import annotations

from typing import Any

from processor.detectors._helpers import event_ts, heading_delta, previous_event
from processor.detectors.registry import Signal, register_detector


@register_detector("abrupt_heading_change", description="Heading delta above threshold within a time bound")
def detect_abrupt_heading_change(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    threshold = float(params.get("delta_deg_gt", 25))
    within_s = float(params.get("within_s", 1))

    previous = previous_event(window, event)
    if previous is None:
        return None

    elapsed = (event_ts(event) - event_ts(previous)).total_seconds()
    # A 40 degree change over 10 seconds is a curve; over one second it is a
    # swerve. Without the time bound the detector would fire on ordinary turns.
    if elapsed <= 0 or elapsed > within_s:
        return None

    delta = heading_delta(
        float(previous.get("heading_deg") or 0.0),
        float(event.get("heading_deg") or 0.0),
    )
    if delta <= threshold:
        return None

    return Signal(
        name="abrupt_heading_change",
        event_key=event["event_key"],
        value=delta,
        threshold=threshold,
        ts=event_ts(event),
        weight=weight,
        detail={
            "from_heading_deg": float(previous.get("heading_deg") or 0.0),
            "to_heading_deg": float(event.get("heading_deg") or 0.0),
            "elapsed_s": round(elapsed, 3),
            "within_s": within_s,
        },
    )
