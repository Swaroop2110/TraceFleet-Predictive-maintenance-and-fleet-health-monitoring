"""Attach a geohash to each event."""

from __future__ import annotations

from typing import Any

from packages.common import geo
from processor.enrichers.registry import register_enricher

#: Full storage precision. Masking for a given role truncates this string; the
#: stored value stays precise so an authorised investigation is still possible.
STORAGE_PRECISION = 9


@register_enricher("geohash", description="Geohash for spatial bucketing and privacy masking")
def enrich_geohash(event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    lat = event.get("lat")
    lon = event.get("lon")
    if lat is None or lon is None:
        event["geohash"] = ""
        return event
    try:
        event["geohash"] = geo.encode(float(lat), float(lon), STORAGE_PRECISION)
    except (TypeError, ValueError):
        # Validation should have rejected an out-of-range coordinate already.
        # Reaching here means the field is unusable, so record its absence rather
        # than guessing a position.
        event["geohash"] = ""
        flags = event.setdefault("flags", {})
        missing = flags.setdefault("missing_fields", [])
        if "geohash" not in missing:
            missing.append("geohash")
    return event
