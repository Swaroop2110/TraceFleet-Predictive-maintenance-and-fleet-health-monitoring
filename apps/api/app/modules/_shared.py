"""Shared helpers for API modules. Underscore-prefixed so discovery skips it."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from typing import Any

from packages.common.schemas_compat import decimal_to_float, json_value, jsonify
from packages.common.timeutil import ensure_utc, parse_ts, to_iso

__all__ = [
    "decimal_to_float",
    "decode_cursor",
    "decode_hash",
    "encode_cursor",
    "json_value",
    "jsonify",
    "serialize_incident",
]


def decode_hash(values: dict[Any, Any]) -> dict[str, Any]:
    """Normalise a Redis hash into typed JSON values."""
    decoded = {
        (key.decode("utf-8") if isinstance(key, bytes) else str(key)): (
            value.decode("utf-8") if isinstance(value, bytes) else value
        )
        for key, value in (values or {}).items()
    }
    for field in ("lat", "lon", "speed", "soc", "heading", "odo_km"):
        raw = decoded.get(field)
        if raw not in ("", "None", None):
            try:
                decoded[field] = float(raw)
            except (TypeError, ValueError):
                decoded[field] = None
        elif field in decoded:
            decoded[field] = None
    if "rpm" in decoded:
        try:
            decoded["rpm"] = int(float(decoded["rpm"])) if decoded["rpm"] not in ("", "None", None) else None
        except (TypeError, ValueError):
            decoded["rpm"] = None
    try:
        decoded["dtc"] = json.loads(decoded.get("dtc") or "[]")
    except (TypeError, json.JSONDecodeError):
        decoded["dtc"] = []
    if "late" in decoded:
        decoded["late"] = str(decoded["late"]) in ("1", "true", "True")
    return decoded


def serialize_incident(
    incident: Any,
    include_evidence: bool = False,
    include_factors: bool = True,
) -> dict[str, Any]:
    """Serialise an incident row for an API response."""
    result: dict[str, Any] = {
        "incident_id": incident.incident_id,
        "tenant_id": incident.tenant_id,
        "fleet_id": getattr(incident, "fleet_id", None),
        "vehicle_id": incident.vehicle_id,
        "incident_type": incident.incident_type,
        "severity": incident.severity,
        "status": incident.status,
        "start_ts": json_value(incident.start_ts),
        "end_ts": json_value(incident.end_ts),
        "detected_at": json_value(incident.detected_at),
        "confidence": decimal_to_float(incident.confidence),
        "acknowledged_at": json_value(getattr(incident, "acknowledged_at", None)),
        "acknowledged_by": getattr(incident, "acknowledged_by", None),
        "closed_at": json_value(getattr(incident, "closed_at", None)),
        "closed_by": getattr(incident, "closed_by", None),
        "metadata": incident.metadata_json or {},
    }

    if include_factors:
        result["factors"] = [
            {
                "factor_id": factor.factor_id,
                "factor_type": factor.factor_type,
                "score": decimal_to_float(factor.score),
                "confidence": decimal_to_float(factor.confidence),
                "evidence_ids": factor.evidence_ids or [],
                "components": factor.components or {},
            }
            for factor in sorted(
                getattr(incident, "factors", []),
                key=lambda item: decimal_to_float(item.score) or 0.0,
                reverse=True,
            )
        ]

    if include_evidence:
        result["evidence"] = [
            {
                "evidence_id": item.evidence_id,
                "event_key": item.event_key,
                "event_ts": json_value(item.event_ts),
                "signal": item.signal,
                "value": item.value,
                "threshold": item.threshold,
                "weight": item.weight,
                "phase": item.phase,
                "flags": item.flags or {},
            }
            for item in sorted(getattr(incident, "evidence", []), key=lambda item: item.event_ts)
        ]
    return result


# ---------------------------------------------------------------------------
# Keyset pagination
# ---------------------------------------------------------------------------
def encode_cursor(start_ts: datetime, incident_id: str) -> str:
    """Opaque cursor for keyset pagination.

    Keyset rather than OFFSET: at a hundred thousand incidents an OFFSET scan gets
    linearly slower, and rows inserted between pages shift the window so a client
    silently skips or repeats records. The cursor carries the exact sort key, so the
    next page starts precisely where the previous one ended.

    Base64 is encoding, not security — it signals "do not construct this by hand"
    and keeps the URL clean. The values inside are already tenant-filtered by RLS.
    """
    payload = json.dumps({"ts": to_iso(ensure_utc(start_ts)), "id": incident_id})
    return base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    """Decode a pagination cursor. Raises ValueError when malformed."""
    padding = "=" * (-len(cursor) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(cursor + padding))
        return parse_ts(payload["ts"]), str(payload["id"])
    except Exception as error:
        raise ValueError(f"invalid cursor: {error}") from error
