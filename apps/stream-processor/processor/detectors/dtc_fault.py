"""Newly asserted diagnostic trouble codes."""

from __future__ import annotations

from typing import Any

from processor.detectors._helpers import event_ts
from processor.detectors.registry import Signal, register_detector


@register_detector("dtc_fault", description="One or more diagnostic codes not previously active")
def detect_dtc_fault(
    window: Any,
    event: dict[str, Any],
    params: dict[str, Any],
    weight: float,
) -> Signal | None:
    if not params.get("any_new_dtc", True):
        return None

    # The window computes the delta before folding the codes into its active set,
    # because by the time a detector runs the code is already "active".
    new_codes = sorted(event.get("_new_dtcs") or set())
    if not new_codes:
        return None

    return Signal(
        name="dtc_fault",
        event_key=event["event_key"],
        value=float(len(new_codes)),
        threshold=0.0,
        ts=event_ts(event),
        weight=weight,
        detail={"new_dtc_codes": new_codes, "active_dtc_count": len(getattr(window, "active_dtcs", ()))},
    )
