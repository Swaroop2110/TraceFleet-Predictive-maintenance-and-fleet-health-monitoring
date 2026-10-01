"""Per-vehicle sliding window and the bounded store that holds them.

Two concerns live here.

:class:`VehicleWindow` keeps the recent events for one vehicle in event-time
order. In-order arrivals append in O(1); a late arrival is placed with
``bisect.insort`` in O(log W) search plus the list shift. W is the number of
events inside ``window_seconds`` (30 at 1 Hz), so the window stays cheap while
still presenting detectors with correctly ordered history. Ordering matters
concretely: the heading-change detector compares against the event that
*precedes* the new one in time, which is not the last element once an
out-of-order event has been inserted.

:class:`WindowStore` bounds total memory. At 100,000 active vehicles an
unbounded dict of windows is the process's largest allocation and grows without
limit as vehicle ids churn, so the store evicts least-recently-used windows past
a configured ceiling and on inactivity.
"""

from __future__ import annotations

import bisect
from collections import OrderedDict
from collections.abc import Iterator
from datetime import datetime
from typing import Any

from packages.common import metrics
from packages.common.timeutil import parse_ts


def _event_time(event: dict[str, Any]) -> datetime:
    cached = event.get("_ts")
    if isinstance(cached, datetime):
        return cached
    return parse_ts(event["ts"])


class VehicleWindow:
    """Recent events for one vehicle, ordered by event time."""

    __slots__ = (
        "active_dtcs",
        "events",
        "events_seen",
        "last_activity",
        "late_inserts",
        "max_speed_kmh",
        "min_accel_mps2",
        "window_seconds",
    )

    def __init__(self, window_seconds: int) -> None:
        self.window_seconds = int(window_seconds)
        #: Event dicts, always sorted by ``_ts``.
        self.events: list[dict[str, Any]] = []
        self.max_speed_kmh = 0.0
        self.min_accel_mps2 = 0.0
        self.active_dtcs: set[str] = set()
        self.last_activity: datetime | None = None
        self.events_seen = 0
        self.late_inserts = 0

    # -- mutation --------------------------------------------------------
    def add_event(self, event: dict[str, Any]) -> bool:
        """Insert an event. Returns True if it arrived out of order.

        The DTC delta is computed here, before the code is folded into the active
        set, because detectors run after insertion and would otherwise always see
        an empty delta.
        """
        ts = _event_time(event)
        event["_ts"] = ts
        event["_new_dtcs"] = set(event.get("dtc") or []) - self.active_dtcs

        out_of_order = False
        if not self.events or ts >= self.events[-1]["_ts"]:
            self.events.append(event)
        else:
            bisect.insort(self.events, event, key=lambda item: item["_ts"])
            out_of_order = True
            self.late_inserts += 1

        self.events_seen += 1
        self._fold_into_aggregates(event)

        # Eviction is driven by the newest event time in the window, not by wall
        # clock: a replay from an earlier offset must reproduce the same window
        # contents it produced the first time.
        self.last_activity = self.events[-1]["_ts"]
        self._evict_expired()
        return out_of_order

    def _evict_expired(self) -> None:
        if not self.events:
            return
        cutoff = self.events[-1]["_ts"].timestamp() - self.window_seconds
        drop = 0
        for event in self.events:
            if event["_ts"].timestamp() >= cutoff:
                break
            drop += 1
        if drop:
            del self.events[:drop]
            # A running maximum cannot be un-done incrementally, so it is
            # recomputed whenever something actually left the window. That is at
            # most once per event over at most `window_seconds` entries.
            self._recompute_aggregates()

    def _fold_into_aggregates(self, event: dict[str, Any]) -> None:
        speed = float(event.get("speed_kmh") or 0.0)
        accel = float(event.get("acceleration_mps2") or 0.0)
        if speed > self.max_speed_kmh:
            self.max_speed_kmh = speed
        if accel < self.min_accel_mps2:
            self.min_accel_mps2 = accel
        self.active_dtcs.update(event.get("dtc") or [])

    def _recompute_aggregates(self) -> None:
        self.max_speed_kmh = max((float(e.get("speed_kmh") or 0.0) for e in self.events), default=0.0)
        self.min_accel_mps2 = min((float(e.get("acceleration_mps2") or 0.0) for e in self.events), default=0.0)
        self.active_dtcs = {code for event in self.events for code in (event.get("dtc") or [])}

    # -- read ------------------------------------------------------------
    def __len__(self) -> int:
        return len(self.events)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.events)

    @property
    def newest(self) -> dict[str, Any] | None:
        return self.events[-1] if self.events else None

    @property
    def oldest(self) -> dict[str, Any] | None:
        return self.events[0] if self.events else None

    def span_seconds(self) -> float:
        if len(self.events) < 2:
            return 0.0
        return (self.events[-1]["_ts"] - self.events[0]["_ts"]).total_seconds()

    def events_within(self, reference: datetime, seconds: float) -> list[dict[str, Any]]:
        """Events in ``[reference - seconds, reference]``, in order."""
        lower = reference.timestamp() - seconds
        upper = reference.timestamp()
        return [e for e in self.events if lower <= e["_ts"].timestamp() <= upper]

    @property
    def peak_speed_kmh(self) -> float:
        """Highest speed currently inside the window."""
        return self.max_speed_kmh

    @property
    def strongest_deceleration_mps2(self) -> float:
        """Most negative acceleration currently inside the window."""
        return self.min_accel_mps2


class WindowStore:
    """LRU-bounded collection of per-vehicle windows.

    Two eviction triggers: a hard ceiling on active vehicles, and inactivity.
    Without them a long ``full``-profile run grows until the container is OOM
    killed, which reads as a Kafka rebalance loop rather than a memory bug.
    """

    def __init__(
        self,
        window_seconds: int,
        max_vehicles: int = 200_000,
        inactivity_seconds: int | None = None,
    ) -> None:
        self.window_seconds = int(window_seconds)
        self.max_vehicles = int(max_vehicles)
        # Default: a vehicle silent for ten windows is not coming back soon
        # enough for its history to matter to correlation.
        self.inactivity_seconds = int(inactivity_seconds or window_seconds * 10)
        self._windows: OrderedDict[str, VehicleWindow] = OrderedDict()

    def get(self, vehicle_id: str) -> VehicleWindow:
        window = self._windows.get(vehicle_id)
        if window is None:
            window = VehicleWindow(self.window_seconds)
            self._windows[vehicle_id] = window
            self._enforce_capacity()
        else:
            self._windows.move_to_end(vehicle_id)
        metrics.active_vehicle_windows.set(len(self._windows))
        return window

    def _enforce_capacity(self) -> None:
        while len(self._windows) > self.max_vehicles:
            self._windows.popitem(last=False)
            metrics.window_evictions_total.labels(reason="capacity").inc()

    def evict_inactive(self, reference: datetime) -> int:
        """Drop windows whose newest event is older than the inactivity bound.

        Called on a timer from the processing loop rather than per event, because
        scanning the whole store on every event would dominate the hot path.
        """
        cutoff = reference.timestamp() - self.inactivity_seconds
        stale = [
            vehicle_id
            for vehicle_id, window in self._windows.items()
            if window.last_activity is None or window.last_activity.timestamp() < cutoff
        ]
        for vehicle_id in stale:
            self._windows.pop(vehicle_id, None)
            metrics.window_evictions_total.labels(reason="inactivity").inc()
        if stale:
            metrics.active_vehicle_windows.set(len(self._windows))
        return len(stale)

    def discard(self, vehicle_id: str) -> None:
        if self._windows.pop(vehicle_id, None) is not None:
            metrics.active_vehicle_windows.set(len(self._windows))

    def __len__(self) -> int:
        return len(self._windows)

    def __contains__(self, vehicle_id: object) -> bool:
        return vehicle_id in self._windows

    def keys(self) -> list[str]:
        return list(self._windows)
