"""Pydantic contracts for telemetry and incident messages.

Three shapes matter:

* :class:`RawTelemetryV1` — what a vehicle publishes to ``telemetry.raw``.
* :class:`CanonicalEvent` — the enriched form on ``telemetry.canonical``.
  It adds identity, timing, and data-quality flags but **never** rewrites the
  vehicle's own ``ts``.
* :class:`IncidentEnvelope` — the lifecycle message on ``incidents.events``.

Validation bounds are the PRD's physical plausibility ranges. A value outside
them is a data fault, not a reading, and belongs in quarantine.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SCHEMA_VERSION_CURRENT = "1.0"
#: Older versions the normaliser can upgrade. Reading a 1.0 event must keep
#: working after the platform moves to 1.1 (PRD extension recipe 8).
SCHEMA_VERSIONS_SUPPORTED = ("1.0", "1.1")


class Severity(StrEnum):
    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class IncidentStatus(StrEnum):
    DETECTED = "DETECTED"
    RECONSTRUCTING = "RECONSTRUCTING"
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    CLOSED = "CLOSED"
    INVALIDATED = "INVALIDATED"


class IncidentPhase(StrEnum):
    """Timeline phases derived during reconstruction (PRD 8.3)."""

    BASELINE = "baseline"
    ESCALATION = "escalation"
    TRIGGER = "trigger"
    IMMEDIATE_AFTERMATH = "immediate_aftermath"
    RECOVERY = "recovery"


class QuarantineReason(StrEnum):
    """Reason codes for ``telemetry.quarantine``.

    Distinct codes matter because ``events_rejected_total{reason}`` is how an
    operator tells a simulator misconfiguration from an upstream schema change.
    """

    MALFORMED_JSON = "MALFORMED_JSON"
    MISSING_FIELD = "MISSING_FIELD"
    WRONG_TYPE = "WRONG_TYPE"
    OUT_OF_RANGE = "OUT_OF_RANGE"
    UNKNOWN_SCHEMA_VERSION = "UNKNOWN_SCHEMA_VERSION"
    INVALID_VIN = "INVALID_VIN"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    VALIDATION_ERROR = "VALIDATION_ERROR"


class RawTelemetryV1(BaseModel):
    """A single telemetry reading as published by a vehicle."""

    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(min_length=1, max_length=128)
    vehicle_id: str = Field(min_length=1, max_length=64)
    vin: str = Field(min_length=17, max_length=17)
    ts: datetime
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    speed_kmh: float = Field(ge=0, le=300)
    acceleration_mps2: float = Field(ge=-20, le=20)
    brake_force_pct: float = Field(ge=0, le=100)
    heading_deg: float = Field(ge=0, le=360)
    rpm: int | None = Field(default=None, ge=0, le=20_000)
    soc_pct: float | None = Field(default=None, ge=0, le=100)
    odo_km: float | None = Field(default=None, ge=0)
    dtc: list[str] | None = None
    evt: str | None = None
    seq: int = Field(ge=0)
    schema_version: str = Field(default=SCHEMA_VERSION_CURRENT)
    #: Free-form padding so the simulator can reach a realistic ~1 KB payload.
    extras: dict[str, Any] | None = None

    @field_validator("schema_version")
    @classmethod
    def _known_schema_version(cls, value: str) -> str:
        if value not in SCHEMA_VERSIONS_SUPPORTED:
            raise ValueError(f"unsupported schema_version {value!r}; supported: {SCHEMA_VERSIONS_SUPPORTED}")
        return value

    @field_validator("heading_deg")
    @classmethod
    def _wrap_heading(cls, value: float) -> float:
        # 360 and 0 are the same bearing; normalising here keeps the abrupt-turn
        # detector from reading a 360 degree delta on a compass wrap.
        return value % 360.0

    @field_validator("ts")
    @classmethod
    def _timestamp_is_aware(cls, value: datetime) -> datetime:
        from packages.common.timeutil import ensure_utc

        return ensure_utc(value)

    @field_validator("dtc")
    @classmethod
    def _clean_dtc(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        # DTC codes reach the AI evidence package, so restrict them to the OBD-II
        # shape rather than letting arbitrary text through.
        return [code.strip().upper()[:16] for code in value if code and code.strip()]


class EventFlags(BaseModel):
    """Data-quality annotations attached during normalisation."""

    model_config = ConfigDict(extra="allow")

    late: bool = False
    duplicate: bool = False
    schema_transformed: bool = False
    missing_fields: list[str] = Field(default_factory=list)


class CanonicalEvent(RawTelemetryV1):
    """Validated, enriched, identity-bearing event."""

    ingested_at: datetime
    processed_at: datetime
    tenant_id: str
    fleet_id: str
    event_key: str = Field(min_length=16)
    flags: dict[str, Any] = Field(default_factory=dict)
    lateness_ms: int
    geohash: str
    trace_id: str | None = None

    @model_validator(mode="after")
    def _event_time_is_preserved(self) -> CanonicalEvent:
        # Guards the PRD's hardest rule: ts is the vehicle's clock and must never
        # be replaced by an ingestion time.
        if self.ingested_at < self.ts and self.lateness_ms > 0:
            raise ValueError("lateness_ms disagrees with ingested_at/ts ordering")
        return self


class QuarantineRecord(BaseModel):
    """Payload written to ``telemetry.quarantine``."""

    reason_code: QuarantineReason
    reason_detail: str
    original_payload: str
    received_at: datetime
    vehicle_id: str | None = None
    source_topic: str = "telemetry.raw"
    source_partition: int | None = None
    source_offset: int | None = None


class SignalPayload(BaseModel):
    """One explainable detector output."""

    name: str
    event_key: str
    value: float
    threshold: float
    ts: datetime
    weight: float = Field(ge=0.0, le=1.0)
    detail: dict[str, Any] = Field(default_factory=dict)


class EvidenceItem(BaseModel):
    """A signal, persisted and addressable by id so narratives can cite it."""

    evidence_id: str
    event_key: str
    event_ts: datetime
    signal: str
    value: float
    threshold: float | None = None
    weight: float | None = None
    phase: str = IncidentPhase.TRIGGER.value
    flags: dict[str, Any] = Field(default_factory=dict)


class FactorScore(BaseModel):
    """Contributing-factor score with its inputs exposed for inspection."""

    factor_type: str
    score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    evidence_ids: list[str] = Field(default_factory=list)
    components: dict[str, Any] = Field(default_factory=dict)


class DataQuality(BaseModel):
    """Per-incident data-quality roll-up surfaced in the UI and to the LLM."""

    late_events: int = 0
    duplicates_removed: int = 0
    missing_fields: list[str] = Field(default_factory=list)
    schema_transformed: int = 0
    out_of_order_events: int = 0


class IncidentPayload(BaseModel):
    """Incident state as detected by the stream processor."""

    incident_id: str
    idempotency_key: str
    tenant_id: str
    fleet_id: str = "fleet_default"
    vehicle_id: str
    incident_type: str
    severity: Severity
    status: IncidentStatus
    start_ts: datetime
    end_ts: datetime | None = None
    detected_at: datetime
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    signals: list[SignalPayload] = Field(default_factory=list)
    evidence: list[EvidenceItem] = Field(default_factory=list)
    factors: list[FactorScore] = Field(default_factory=list)
    data_quality: DataQuality = Field(default_factory=DataQuality)
    metadata: dict[str, Any] = Field(default_factory=dict)
    trace_id: str | None = None


class IncidentEnvelope(BaseModel):
    """Typed wrapper on ``incidents.events``.

    The persister needs to distinguish a first sighting from an update or a
    state change; a single untyped payload shape cannot express that.
    """

    event_type: Literal["incident.created", "incident.updated", "incident.state_changed"]
    occurred_at: datetime
    incident: IncidentPayload
    previous_status: IncidentStatus | None = None
    actor: str = "stream-processor"
    reason: str | None = None
