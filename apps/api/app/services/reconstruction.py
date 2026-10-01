"""Timeline reconstruction (PRD 8.3).

Given an incident, assemble an ordered account of what the telemetry shows around
it: the raw samples from ClickHouse, the evidence and factors from Postgres, and a
phase decomposition that turns a flat series into a story an operator can read —
baseline, escalation, trigger, immediate aftermath, recovery.

Three decisions worth stating:

**Phases are derived from the data, not from fixed offsets.** A two-second event
and a twenty-second event should both be described sensibly, so boundaries come
from the first signal, the trigger, the point of minimum speed, and the return to
normal — not from "trigger minus 10 seconds".

**Merging is by event time.** Every point carries all three timestamps so the UI can
switch between event time and ingestion time, but the ordering that determines the
narrative is the vehicle's own clock. Ordering by arrival would rewrite history
whenever the network hiccuped.

**Data quality travels with the result.** A timeline assembled from late or
incomplete telemetry says so. The reconstruction is only as trustworthy as its
inputs, and hiding that would make the AI summary's confidence meaningless.

Results are cached in Redis and invalidated when late evidence revises an incident.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from apps.api.app.db.models import Incident, IncidentEvidence, IncidentFactor
from apps.api.app.services.clickhouse import ClickHouseUnavailable, TelemetryPoint, telemetry_store
from packages.common import config as cfg
from packages.common import logging as tf_logging
from packages.common.schemas_compat import decimal_to_float
from packages.common.timeutil import add_seconds, ensure_utc, to_iso
from packages.schemas.models import IncidentPhase

LOGGER = tf_logging.get_logger(__name__)

#: Speed at or below which a vehicle is treated as stopped, in km/h.
STOPPED_SPEED_KMH = 3.0
#: Cache lifetime. Short, because a late event can revise a timeline and the
#: explicit invalidation is best-effort across replicas.
CACHE_TTL_S = 300


@dataclass
class Phase:
    """A labelled interval of the reconstructed timeline."""

    name: str
    start_ts: datetime
    end_ts: datetime
    description: str
    point_count: int = 0
    evidence_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "start_ts": to_iso(self.start_ts),
            "end_ts": to_iso(self.end_ts),
            "duration_s": round((self.end_ts - self.start_ts).total_seconds(), 3),
            "description": self.description,
            "point_count": self.point_count,
            "evidence_ids": self.evidence_ids,
        }


@dataclass
class Reconstruction:
    incident: dict[str, Any]
    phases: list[Phase]
    points: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    factors: list[dict[str, Any]]
    data_quality: dict[str, Any]
    window: dict[str, Any]
    degraded: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "incident": self.incident,
            "phases": [phase.to_dict() for phase in self.phases],
            "points": self.points,
            "evidence": self.evidence,
            "factors": self.factors,
            "data_quality": self.data_quality,
            "window": self.window,
            # Non-empty when part of the reconstruction could not be assembled.
            # Reporting this is the difference between "nothing happened" and
            # "we could not look".
            "degraded": self.degraded,
        }


def reconstruct(
    incident: Incident,
    db: Session,
    *,
    pre_window_s: int | None = None,
    post_window_s: int | None = None,
    redis_client: Any = None,
    use_cache: bool = True,
) -> Reconstruction:
    """Build the full reconstruction for an incident."""
    correlation = cfg.correlation()
    pre = pre_window_s if pre_window_s is not None else correlation.pre_window_s
    post = post_window_s if post_window_s is not None else correlation.post_window_s

    cache_key = _cache_key(incident, pre, post)
    if use_cache and redis_client is not None:
        cached = _read_cache(redis_client, cache_key)
        if cached is not None:
            return cached

    evidence_rows = _evidence(db, incident.incident_id)
    factor_rows = _factors(db, incident.incident_id)

    start = add_seconds(ensure_utc(incident.start_ts), -pre)
    end = add_seconds(ensure_utc(incident.end_ts or incident.start_ts), post)

    degraded: list[str] = []
    points: list[TelemetryPoint] = []
    try:
        points = telemetry_store.vehicle_window(incident.tenant_id, incident.vehicle_id, start, end)
    except ClickHouseUnavailable as error:
        # Evidence lives in Postgres, so the investigation is still usable without
        # the raw series; the caller is told the chart will be empty and why.
        degraded.append(f"telemetry_unavailable: {error}")
        LOGGER.warning(
            "reconstruction without telemetry series",
            extra={"incident_id": incident.incident_id, "error": str(error)},
        )

    trigger_ts = _trigger_ts(evidence_rows, incident)
    phases = derive_phases(points, evidence_rows, incident, trigger_ts, start, end)
    annotated = annotate_points(points, evidence_rows, phases)
    quality = data_quality(points, evidence_rows, incident)

    result = Reconstruction(
        incident=_incident_summary(incident, trigger_ts),
        phases=phases,
        points=annotated,
        evidence=evidence_rows,
        factors=factor_rows,
        data_quality=quality,
        window={
            "pre_window_s": pre,
            "post_window_s": post,
            "from_ts": to_iso(start),
            "to_ts": to_iso(end),
            "trigger_ts": to_iso(trigger_ts) if trigger_ts else None,
            "point_count": len(annotated),
        },
        degraded=degraded,
    )

    # Only a complete reconstruction is cached. Caching a degraded one would make a
    # transient ClickHouse outage look persistent for the whole TTL.
    if use_cache and redis_client is not None and not degraded:
        _write_cache(redis_client, cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Phase derivation
# ---------------------------------------------------------------------------
def derive_phases(
    points: Sequence[TelemetryPoint],
    evidence: Sequence[dict[str, Any]],
    incident: Incident,
    trigger_ts: datetime | None,
    window_start: datetime,
    window_end: datetime,
) -> list[Phase]:
    """Split the window into the five narrative phases.

    Boundaries, in order of preference for each:

    * ``baseline``  — window start until the first signal (the last normal driving).
    * ``escalation`` — first signal until the trigger (the behaviour building up).
    * ``trigger``   — the moment itself, widened to one sample either side so a
      single instant is visible on a chart.
    * ``immediate_aftermath`` — trigger until the vehicle settles: stopped, or
      speed stabilising.
    * ``recovery``  — settling until the window closes.

    A phase with no time span is omitted rather than emitted as a zero-length band,
    because an empty band on a chart reads as missing data.
    """
    incident_start = ensure_utc(incident.start_ts)
    incident_end = ensure_utc(incident.end_ts or incident.start_ts)

    signal_times = sorted(ensure_utc(item["event_ts_raw"]) for item in evidence) if evidence else []
    first_signal = signal_times[0] if signal_times else incident_start
    trigger = trigger_ts or (signal_times[-1] if signal_times else incident_end)

    settled = _settling_point(points, trigger) or incident_end
    sample_gap = _median_gap(points)

    phases: list[Phase] = []

    if first_signal > window_start:
        phases.append(
            Phase(
                IncidentPhase.BASELINE.value,
                window_start,
                first_signal,
                "Normal operation before the first detected signal.",
            )
        )

    if trigger > first_signal:
        phases.append(
            Phase(
                IncidentPhase.ESCALATION.value,
                first_signal,
                trigger,
                "Signals accumulating towards the incident trigger.",
            )
        )

    # Widened by one sample interval so the trigger is visible rather than a
    # zero-width line on the chart.
    trigger_start = add_seconds(trigger, -sample_gap)
    trigger_end = add_seconds(trigger, sample_gap)
    phases.append(
        Phase(
            IncidentPhase.TRIGGER.value,
            max(trigger_start, first_signal),
            min(trigger_end, window_end),
            "The threshold crossings that caused the incident to be raised.",
        )
    )

    if settled > trigger_end:
        phases.append(
            Phase(
                IncidentPhase.IMMEDIATE_AFTERMATH.value,
                trigger_end,
                settled,
                "Immediately following the trigger, until the vehicle state settles.",
            )
        )

    recovery_start = max(settled, trigger_end)
    if window_end > recovery_start:
        phases.append(
            Phase(
                IncidentPhase.RECOVERY.value,
                recovery_start,
                window_end,
                "Vehicle state after the incident.",
            )
        )

    _attach_counts(phases, points, evidence)
    return phases


def _settling_point(points: Sequence[TelemetryPoint], trigger: datetime) -> datetime | None:
    """When the vehicle settled after the trigger.

    Either the first sample at rest, or the first point where speed stops changing
    materially. A collision ends at a standstill; a hard-braking event ends when the
    driver resumes a steady speed, and both need to be recognised.
    """
    after = [point for point in points if point.ts > trigger]
    if not after:
        return None

    for point in after:
        if point.speed_kmh <= STOPPED_SPEED_KMH:
            return point.ts

    # Speed varying by under 5 km/h across three consecutive samples counts as steady.
    for index in range(len(after) - 2):
        window = after[index : index + 3]
        speeds = [item.speed_kmh for item in window]
        if max(speeds) - min(speeds) < 5.0:
            return window[-1].ts
    return None


def _median_gap(points: Sequence[TelemetryPoint]) -> float:
    """Typical sampling interval in seconds, for widening the trigger band."""
    if len(points) < 2:
        return 1.0
    gaps = sorted((points[index].ts - points[index - 1].ts).total_seconds() for index in range(1, len(points)))
    middle = gaps[len(gaps) // 2]
    return max(0.25, min(5.0, middle))


def _attach_counts(
    phases: list[Phase],
    points: Sequence[TelemetryPoint],
    evidence: Sequence[dict[str, Any]],
) -> None:
    for phase in phases:
        phase.point_count = sum(1 for point in points if phase.start_ts <= point.ts <= phase.end_ts)
        phase.evidence_ids = [
            item["evidence_id"]
            for item in evidence
            if phase.start_ts <= ensure_utc(item["event_ts_raw"]) <= phase.end_ts
        ]


# ---------------------------------------------------------------------------
# Point annotation
# ---------------------------------------------------------------------------
def annotate_points(
    points: Sequence[TelemetryPoint],
    evidence: Sequence[dict[str, Any]],
    phases: Sequence[Phase],
) -> list[dict[str, Any]]:
    """Attach evidence references and phase labels to each telemetry point.

    Linking by ``event_key`` rather than by timestamp: two samples can share a
    timestamp at sub-second resolution, and the event key is the identity the
    detectors recorded.
    """
    by_event_key: dict[str, list[dict[str, Any]]] = {}
    for item in evidence:
        by_event_key.setdefault(item["event_key"], []).append(item)

    annotated: list[dict[str, Any]] = []
    for point in points:
        payload = point.to_dict()
        matches = by_event_key.get(point.event_key, [])
        payload["evidence_ids"] = [item["evidence_id"] for item in matches]
        payload["signals"] = [item["signal"] for item in matches]
        payload["phase"] = _phase_for(point.ts, phases)
        payload["data_quality"] = {
            "late": point.late,
            "lateness_ms": point.lateness_ms,
            "schema_transformed": point.schema_version != "1.0",
        }
        annotated.append(payload)
    return annotated


def _phase_for(moment: datetime, phases: Sequence[Phase]) -> str | None:
    # Iterated in reverse so the narrow trigger band wins over the wider bands it
    # sits inside.
    for phase in reversed(list(phases)):
        if phase.start_ts <= moment <= phase.end_ts:
            return phase.name
    return None


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------
def data_quality(
    points: Sequence[TelemetryPoint],
    evidence: Sequence[dict[str, Any]],
    incident: Incident,
) -> dict[str, Any]:
    """Summarise how trustworthy this reconstruction's inputs were."""
    stored = (incident.metadata_json or {}).get("data_quality") or {}
    late_points = [point for point in points if point.late]
    out_of_order = sum(
        1 for index in range(1, len(points)) if points[index].ingested_at < points[index - 1].ingested_at
    )
    missing: set[str] = set()
    for point in points:
        if point.rpm is None:
            missing.add("rpm")
        if point.soc_pct is None:
            missing.add("soc_pct")
        if point.odo_km is None:
            missing.add("odo_km")

    return {
        "late_events": len(late_points),
        "max_lateness_ms": max((point.lateness_ms for point in late_points), default=0),
        # Duplicates are suppressed at ingest and never reach ClickHouse, so this
        # count comes from what the processor recorded at detection time.
        "duplicates_removed": int(stored.get("duplicates_removed", 0)),
        "out_of_order_events": out_of_order or int(stored.get("out_of_order_events", 0)),
        "schema_transformed": sum(1 for point in points if point.schema_version != "1.0"),
        "missing_fields": sorted(missing),
        "telemetry_points": len(points),
        "evidence_items": len(evidence),
        "late_evidence_added": bool((incident.metadata_json or {}).get("late_evidence_added")),
        "complete": not late_points and not missing and not out_of_order,
    }


# ---------------------------------------------------------------------------
# Postgres reads
# ---------------------------------------------------------------------------
def _evidence(db: Session, incident_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(IncidentEvidence)
        .filter(IncidentEvidence.incident_id == incident_id)
        .order_by(IncidentEvidence.event_ts.asc(), IncidentEvidence.signal.asc())
        .all()
    )
    return [
        {
            "evidence_id": row.evidence_id,
            "event_key": row.event_key,
            "event_ts": to_iso(ensure_utc(row.event_ts)),
            # Kept as a datetime for internal comparisons; stripped before the
            # response is serialised.
            "event_ts_raw": ensure_utc(row.event_ts),
            "signal": row.signal,
            "value": row.value,
            "threshold": row.threshold,
            "weight": row.weight,
            "phase": row.phase,
            "flags": row.flags or {},
        }
        for row in rows
    ]


def _factors(db: Session, incident_id: str) -> list[dict[str, Any]]:
    rows = (
        db.query(IncidentFactor)
        .filter(IncidentFactor.incident_id == incident_id)
        .order_by(IncidentFactor.score.desc())
        .all()
    )
    return [
        {
            "factor_id": row.factor_id,
            "factor_type": row.factor_type,
            "score": decimal_to_float(row.score),
            "confidence": decimal_to_float(row.confidence),
            "evidence_ids": row.evidence_ids or [],
            "components": row.components or {},
        }
        for row in rows
    ]


def _trigger_ts(evidence: Sequence[dict[str, Any]], incident: Incident) -> datetime | None:
    """The trigger moment: the evidence item the processor labelled ``trigger``."""
    for item in evidence:
        if item.get("phase") == IncidentPhase.TRIGGER.value:
            return ensure_utc(item["event_ts_raw"])
    if evidence:
        return ensure_utc(evidence[-1]["event_ts_raw"])
    return ensure_utc(incident.end_ts or incident.start_ts)


def _incident_summary(incident: Incident, trigger_ts: datetime | None) -> dict[str, Any]:
    return {
        "incident_id": incident.incident_id,
        "tenant_id": incident.tenant_id,
        "fleet_id": incident.fleet_id,
        "vehicle_id": incident.vehicle_id,
        "incident_type": incident.incident_type,
        "severity": incident.severity,
        "status": incident.status,
        "start_ts": to_iso(ensure_utc(incident.start_ts)),
        "end_ts": to_iso(ensure_utc(incident.end_ts)) if incident.end_ts else None,
        "detected_at": to_iso(ensure_utc(incident.detected_at)),
        "trigger_ts": to_iso(trigger_ts) if trigger_ts else None,
        "confidence": decimal_to_float(incident.confidence),
        "metadata": incident.metadata_json or {},
    }


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
def _cache_key(incident: Incident, pre: int, post: int) -> str:
    # The revision is part of the key, so a re-reconstruction after late evidence
    # lands on a different entry and cannot serve a stale timeline even if the
    # explicit invalidation is missed.
    revision = (incident.metadata_json or {}).get("revision", 0)
    return f"tf:{incident.tenant_id}:timeline:{incident.incident_id}:{pre}:{post}:r{revision}"


def _read_cache(redis_client: Any, key: str) -> Reconstruction | None:
    from packages.common import metrics

    try:
        raw = redis_client.get(key)
    except Exception:
        return None
    if not raw:
        metrics.redis_cache_misses_total.labels(cache="timeline").inc()
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    metrics.redis_cache_hits_total.labels(cache="timeline").inc()
    return Reconstruction(
        incident=payload["incident"],
        phases=[
            Phase(
                name=item["name"],
                start_ts=ensure_utc(datetime.fromisoformat(item["start_ts"].replace("Z", "+00:00"))),
                end_ts=ensure_utc(datetime.fromisoformat(item["end_ts"].replace("Z", "+00:00"))),
                description=item["description"],
                point_count=item.get("point_count", 0),
                evidence_ids=item.get("evidence_ids", []),
            )
            for item in payload["phases"]
        ],
        points=payload["points"],
        evidence=payload["evidence"],
        factors=payload["factors"],
        data_quality=payload["data_quality"],
        window=payload["window"],
    )


def _write_cache(redis_client: Any, key: str, result: Reconstruction) -> None:
    try:
        payload = result.to_dict()
        payload["evidence"] = [{k: v for k, v in item.items() if k != "event_ts_raw"} for item in payload["evidence"]]
        redis_client.setex(key, CACHE_TTL_S, json.dumps(payload, default=str))
    except Exception as error:
        LOGGER.debug("timeline cache write failed", extra={"error": str(error)})


def invalidate_cache(redis_client: Any, tenant_id: str, incident_id: str) -> int:
    """Drop every cached timeline for an incident. Called when evidence changes."""
    if redis_client is None:
        return 0
    try:
        pattern = f"tf:{tenant_id}:timeline:{incident_id}:*"
        keys = list(redis_client.scan_iter(match=pattern, count=100))
        if keys:
            redis_client.delete(*keys)
        return len(keys)
    except Exception as error:
        LOGGER.debug("timeline cache invalidation failed", extra={"error": str(error)})
        return 0


def strip_internal(payload: dict[str, Any]) -> dict[str, Any]:
    """Remove internal-only fields before returning a reconstruction over HTTP."""
    cleaned = dict(payload)
    cleaned["evidence"] = [
        {k: v for k, v in item.items() if k != "event_ts_raw"} for item in payload.get("evidence", [])
    ]
    return cleaned
