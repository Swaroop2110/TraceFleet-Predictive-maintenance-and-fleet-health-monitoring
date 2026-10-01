"""Shared window arithmetic for detector plug-ins.

Underscore-prefixed so the registry's discovery pass skips it.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any


def event_ts(event: dict[str, Any]) -> datetime:
    """Event time, preferring the window's parsed copy."""
    from packages.common.timeutil import parse_ts

    cached = event.get("_ts")
    if isinstance(cached, datetime):
        return cached
    return parse_ts(event["ts"])


def trailing_run(
    events: Sequence[dict[str, Any]],
    predicate,
) -> list[dict[str, Any]]:
    """Longest unbroken run of matching events ending at the newest event.

    "Unbroken" is the point. Filtering the whole window instead lets a dip below
    the threshold pass unnoticed, so a car that speeds, slows to legal, then
    speeds again would report one long sustained violation.
    """
    run: list[dict[str, Any]] = []
    for candidate in reversed(events):
        if not predicate(candidate):
            break
        run.append(candidate)
    run.reverse()
    return run


def run_duration_s(run: Sequence[dict[str, Any]]) -> float:
    """Elapsed event time spanned by a run of events."""
    if len(run) < 2:
        return 0.0
    return (event_ts(run[-1]) - event_ts(run[0])).total_seconds()


def previous_event(window: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    """The event immediately preceding ``event`` in event-time order.

    Indexing ``events[-2]`` is wrong once a late event has been inserted: the
    newest arrival is not necessarily the last element. Matching on event_key
    keeps the comparison anchored to the event actually being evaluated.
    """
    events = list(getattr(window, "events", []))
    if len(events) < 2:
        return None
    key = event.get("event_key")
    for index in range(len(events) - 1, 0, -1):
        if events[index].get("event_key") == key:
            return events[index - 1]
    return events[-2]


def heading_delta(first: float, second: float) -> float:
    """Smallest absolute angle between two bearings, in degrees."""
    delta = abs((second - first) % 360.0)
    return 360.0 - delta if delta > 180.0 else delta


def window_max_speed_within(window: Any, event: dict[str, Any], seconds: float) -> float:
    """Peak speed observed within ``seconds`` before the event.

    Bounding the lookback is what makes ``sudden_stop`` mean "dropped from speed
    just now" rather than "has been fast at some point in this window".
    """
    reference = event_ts(event)
    peak = 0.0
    for candidate in getattr(window, "events", []):
        elapsed = (reference - event_ts(candidate)).total_seconds()
        if 0 <= elapsed <= seconds:
            peak = max(peak, float(candidate.get("speed_kmh") or 0.0))
    return peak
