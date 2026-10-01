"""Record what was missing, late, or adapted about each event.

These flags are the platform's honesty mechanism. Every timeline, factor
confidence, and AI summary is qualified by them, so an operator can tell a clean
reconstruction from one assembled out of partial data.
"""

from __future__ import annotations

from typing import Any

from processor.enrichers.registry import register_enricher

#: Fields that are optional in the schema but whose absence degrades analysis.
OPTIONAL_BUT_USEFUL = ("rpm", "soc_pct", "odo_km", "evt")


@register_enricher("data_quality_flags", description="Late, duplicate, missing-field and schema flags")
def enrich_data_quality(event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    flags: dict[str, Any] = event.setdefault("flags", {})
    missing: list[str] = list(flags.get("missing_fields") or [])

    for field in OPTIONAL_BUT_USEFUL:
        if event.get(field) in (None, "") and field not in missing:
            missing.append(field)

    lateness_window_s = float(context.get("lateness_window_s", 30))
    lateness_ms = int(event.get("lateness_ms") or 0)

    flags["missing_fields"] = sorted(missing)
    # Late means "arrived after the window we promised to wait". The event is
    # still stored and still used; the flag tells the UI the timeline was revised.
    flags["late"] = lateness_ms > lateness_window_s * 1000
    flags.setdefault("duplicate", False)
    flags["schema_transformed"] = bool(flags.get("schema_transformed", False))
    flags["complete"] = not missing

    if flags["late"]:
        flags["lateness_seconds"] = round(lateness_ms / 1000.0, 3)

    event["flags"] = flags
    return event
