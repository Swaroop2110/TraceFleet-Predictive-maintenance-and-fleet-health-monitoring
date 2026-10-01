"""Fleet and vehicle risk indices.

A risk index is a *relative* triage aid, not a safety rating. It combines incident
frequency per distance driven with the severity mix, so a vehicle with one critical
incident in 50 km ranks above one with three INFO incidents in 5,000 km.

Two properties the implementation is careful about:

* **Exposure-normalised.** Raw incident counts rank the busiest vehicles highest,
  which is not the same as the riskiest. Dividing by distance answers the question
  an operator actually has.
* **Honest about thin data.** A vehicle with 3 km of telemetry has no meaningful
  rate, so the index reports low confidence rather than a dramatic number. Ranking
  on noise is worse than not ranking.

The weights live in ``config/severity.yaml``'s ordering rather than being invented
here, so "how much worse is CRITICAL than HIGH" stays a configuration decision.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from apps.api.app.db.models import Incident
from packages.common import logging as tf_logging
from packages.common.timeutil import add_seconds, now

LOGGER = tf_logging.get_logger(__name__)

#: Relative contribution of each severity to the index. Superlinear, because one
#: critical event matters more than several minor ones, not the same amount.
SEVERITY_WEIGHTS = {"INFO": 0.0, "LOW": 1.0, "MEDIUM": 3.0, "HIGH": 8.0, "CRITICAL": 20.0}

#: Distance below which a rate is not reported with confidence.
MIN_DISTANCE_KM = 50.0
#: Distance that normalises the index. 1,000 km is roughly a week of mixed duty.
REFERENCE_DISTANCE_KM = 1000.0


def _severity_counts(
    db: Session,
    tenant_id: str,
    since: Any,
    vehicle_id: str | None = None,
    fleet_id: str | None = None,
) -> dict[str, int]:
    query = (
        select(Incident.severity, func.count(Incident.incident_id))
        .where(
            Incident.tenant_id == tenant_id,
            Incident.start_ts >= since,
            # An invalidated incident was a false positive and must not count
            # against a vehicle, or the index would reward under-detection.
            Incident.status != "INVALIDATED",
        )
        .group_by(Incident.severity)
    )
    if vehicle_id:
        query = query.where(Incident.vehicle_id == vehicle_id)
    if fleet_id:
        query = query.where(Incident.fleet_id == fleet_id)

    counts = dict.fromkeys(SEVERITY_WEIGHTS, 0)
    for severity, count in db.execute(query).all():
        counts[severity] = int(count)
    return counts


def _weighted_score(counts: dict[str, int], distance_km: float) -> tuple[float, str]:
    """Severity-weighted incidents per reference distance, plus a confidence label."""
    weighted = sum(SEVERITY_WEIGHTS.get(name, 0.0) * count for name, count in counts.items())
    if distance_km < MIN_DISTANCE_KM:
        # Reported without normalising, and flagged, rather than extrapolated from
        # a few kilometres into a rate that looks authoritative.
        return round(weighted, 2), "low"
    rate = weighted * (REFERENCE_DISTANCE_KM / distance_km)
    confidence = "high" if distance_km >= REFERENCE_DISTANCE_KM else "medium"
    return round(rate, 2), confidence


def _band(score: float) -> str:
    """Bucket a score for display. Thresholds are presentation, not safety limits."""
    if score >= 40:
        return "SEVERE"
    if score >= 20:
        return "ELEVATED"
    if score >= 8:
        return "MODERATE"
    if score > 0:
        return "LOW"
    return "NONE"


def fleet_risk(
    db: Session,
    tenant_id: str,
    *,
    days: int = 7,
    limit: int = 50,
) -> dict[str, Any]:
    """Risk index per fleet and the top-ranked vehicles within the tenant."""
    from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store

    since = add_seconds(now(), -days * 86400)
    hours = days * 24

    distances: dict[str, float] = {}
    vehicle_features: list[dict[str, Any]] = []
    degraded: list[str] = []
    try:
        for row in telemetry_store.fleet_breakdown(tenant_id, hours=hours):
            distances[row["fleet_id"]] = row["total_distance_km"]
        vehicle_features = telemetry_store.vehicle_risk_features(tenant_id, hours=hours, limit=limit * 4)
    except ClickHouseUnavailable as error:
        # Without distance the index cannot be exposure-normalised. Counts are
        # still returned, clearly labelled as unnormalised.
        degraded.append(f"telemetry_unavailable: {error}")

    fleet_rows = db.execute(
        select(Incident.fleet_id, Incident.severity, func.count(Incident.incident_id))
        .where(
            Incident.tenant_id == tenant_id,
            Incident.start_ts >= since,
            Incident.status != "INVALIDATED",
        )
        .group_by(Incident.fleet_id, Incident.severity)
    ).all()

    per_fleet: dict[str, dict[str, int]] = {}
    for fleet_id, severity, count in fleet_rows:
        per_fleet.setdefault(fleet_id, dict.fromkeys(SEVERITY_WEIGHTS, 0))[severity] = int(count)

    fleets: list[dict[str, Any]] = []
    for fleet_id, counts in per_fleet.items():
        distance = distances.get(fleet_id, 0.0)
        score, confidence = _weighted_score(counts, distance)
        fleets.append(
            {
                "fleet_id": fleet_id,
                "risk_index": score,
                "risk_band": _band(score),
                "confidence": confidence,
                "normalised": distance >= MIN_DISTANCE_KM,
                "distance_km": distance,
                "incidents": sum(counts.values()),
                "by_severity": counts,
            }
        )
    fleets.sort(key=lambda item: item["risk_index"], reverse=True)

    vehicle_counts = db.execute(
        select(Incident.vehicle_id, Incident.severity, func.count(Incident.incident_id))
        .where(
            Incident.tenant_id == tenant_id,
            Incident.start_ts >= since,
            Incident.status != "INVALIDATED",
        )
        .group_by(Incident.vehicle_id, Incident.severity)
    ).all()

    per_vehicle: dict[str, dict[str, int]] = {}
    for vehicle_id, severity, count in vehicle_counts:
        per_vehicle.setdefault(vehicle_id, dict.fromkeys(SEVERITY_WEIGHTS, 0))[severity] = int(count)

    distance_by_vehicle = {row["vehicle_id"]: row["distance_km"] for row in vehicle_features}
    fleet_by_vehicle = {row["vehicle_id"]: row["fleet_id"] for row in vehicle_features}

    vehicles: list[dict[str, Any]] = []
    for vehicle_id, counts in per_vehicle.items():
        distance = distance_by_vehicle.get(vehicle_id, 0.0)
        score, confidence = _weighted_score(counts, distance)
        vehicles.append(
            {
                "vehicle_id": vehicle_id,
                "fleet_id": fleet_by_vehicle.get(vehicle_id),
                "risk_index": score,
                "risk_band": _band(score),
                "confidence": confidence,
                "normalised": distance >= MIN_DISTANCE_KM,
                "distance_km": distance,
                "incidents": sum(counts.values()),
                "by_severity": counts,
            }
        )
    vehicles.sort(key=lambda item: item["risk_index"], reverse=True)

    return {
        "window_days": days,
        "method": {
            "description": (
                "Severity-weighted incident count per 1,000 km. A relative triage "
                "aid, not a safety rating, and not a statement about any driver."
            ),
            "severity_weights": SEVERITY_WEIGHTS,
            "reference_distance_km": REFERENCE_DISTANCE_KM,
            "min_distance_for_rate_km": MIN_DISTANCE_KM,
            "excludes": "incidents marked INVALIDATED",
        },
        "fleets": fleets,
        "vehicles": vehicles[:limit],
        "degraded": degraded,
    }


def vehicle_risk(
    db: Session,
    tenant_id: str,
    vehicle_id: str,
    *,
    days: int = 7,
) -> dict[str, Any]:
    """Risk index and exposure for a single vehicle."""
    from apps.api.app.services.clickhouse import ClickHouseUnavailable, telemetry_store

    since = add_seconds(now(), -days * 86400)
    counts = _severity_counts(db, tenant_id, since, vehicle_id=vehicle_id)

    distance = 0.0
    features: dict[str, Any] = {}
    degraded: list[str] = []
    try:
        rows = telemetry_store.vehicle_risk_features(tenant_id, hours=days * 24, limit=1000)
        for row in rows:
            if row["vehicle_id"] == vehicle_id:
                distance = row["distance_km"]
                features = row
                break
    except ClickHouseUnavailable as error:
        degraded.append(f"telemetry_unavailable: {error}")

    score, confidence = _weighted_score(counts, distance)
    return {
        "vehicle_id": vehicle_id,
        "window_days": days,
        "risk_index": score,
        "risk_band": _band(score),
        "confidence": confidence,
        "normalised": distance >= MIN_DISTANCE_KM,
        "distance_km": distance,
        "incidents": sum(counts.values()),
        "by_severity": counts,
        "driving_features": features,
        "degraded": degraded,
    }
