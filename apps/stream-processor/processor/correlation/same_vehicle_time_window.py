"""Default correlation policy: one vehicle, one bounded time window."""

from __future__ import annotations

from typing import Any

from processor.correlation.registry import CandidateIncident, register_policy


@register_policy(
    "same_vehicle_time_window",
    description="Group signals for the same vehicle within correlation_window_s",
)
def same_vehicle_time_window(
    candidate: CandidateIncident,
    signal: Any,
    config: Any,
) -> bool:
    window_s = float(getattr(config, "correlation_window_s", 60))

    # Measured from the last signal, not the first. A collision chain unfolds
    # over several seconds; anchoring to start_ts would cut off the tail of a
    # sequence longer than the window, splitting one event into two incidents.
    since_last = abs((signal.ts - candidate.last_signal_ts).total_seconds())
    if since_last <= window_s:
        return True

    # A late signal that belongs inside the already-observed span still groups,
    # which is what lets out-of-order delivery land in the right incident.
    return candidate.start_ts <= signal.ts <= candidate.last_signal_ts
