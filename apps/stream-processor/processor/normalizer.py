"""Raw to canonical mapping: validation, identity, ownership, enrichment.

This is where an untrusted payload becomes a trustworthy event. Four things
happen, in order, and each has a reason to be here rather than downstream:

1. **Validation** against ``RawTelemetryV1``, with the failure classified into a
   specific :class:`QuarantineReason`. A generic "VALIDATION_ERROR" cannot tell an
   operator whether the simulator is misconfigured or an OEM changed a field type.
2. **Identity** — ``event_key`` is derived from content so replays are detectable.
3. **Ownership** — vehicle to tenant/fleet, from a cached Postgres lookup. Doing
   this per event against the database would make Postgres the ingest bottleneck.
4. **Enrichment** via the enricher registry (geohash, data-quality flags).

The vehicle's own ``ts`` is never modified. ``ingested_at`` and ``processed_at``
are recorded alongside it, and ``lateness_ms`` is their difference.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.common.ids import event_key as build_event_key
from packages.common.timeutil import from_epoch_ms, now, parse_ts
from packages.schemas.models import (
    SCHEMA_VERSION_CURRENT,
    CanonicalEvent,
    QuarantineReason,
    RawTelemetryV1,
)
from processor.enrichers import EnricherRegistry

LOGGER = tf_logging.get_logger(__name__)

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT_ID", "tenant_default")
DEFAULT_FLEET = os.getenv("DEFAULT_FLEET_ID", "fleet_default")


class QuarantineError(Exception):
    """A payload that must not enter the canonical stream."""

    def __init__(self, reason: QuarantineReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass
class Ownership:
    tenant_id: str
    fleet_id: str


class TenantResolver:
    """Vehicle to tenant/fleet lookup, cached in process.

    The mapping changes rarely (a vehicle moves fleet occasionally) but is needed
    on every event, so it is loaded in bulk and refreshed on an interval. An
    unknown vehicle falls back to the default tenant and is logged: telemetry from
    a vehicle that has not been seeded is still real data and dropping it would
    lose evidence, but it must be visible that ownership was assumed.
    """

    def __init__(self, dsn: str | None = None, refresh_interval_s: int = 300) -> None:
        self.dsn = dsn or _postgres_dsn()
        self.refresh_interval_s = refresh_interval_s
        self._cache: dict[str, Ownership] = {}
        self._loaded_at = 0.0
        self._unknown_reported: set[str] = set()

    def refresh(self, force: bool = False) -> int:
        if not force and (time.monotonic() - self._loaded_at) < self.refresh_interval_s:
            return len(self._cache)
        try:
            import psycopg2

            with psycopg2.connect(self.dsn) as connection, connection.cursor() as cursor:
                cursor.execute("SELECT vehicle_id, tenant_id, fleet_id FROM vehicle")
                self._cache = {row[0]: Ownership(tenant_id=row[1], fleet_id=row[2]) for row in cursor.fetchall()}
            self._loaded_at = time.monotonic()
            LOGGER.info("vehicle ownership cache loaded", extra={"vehicles": len(self._cache)})
        except Exception as error:
            # A cold cache is not fatal. Events keep flowing against the default
            # tenant, and the next refresh picks the mapping up.
            self._loaded_at = time.monotonic()
            LOGGER.warning("vehicle ownership cache refresh failed", extra={"error": str(error)})
        return len(self._cache)

    def resolve(self, vehicle_id: str) -> Ownership:
        self.refresh()
        ownership = self._cache.get(vehicle_id)
        if ownership is not None:
            return ownership
        if vehicle_id not in self._unknown_reported:
            self._unknown_reported.add(vehicle_id)
            LOGGER.info(
                "vehicle not in ownership cache, assigning default tenant",
                extra={"vehicle_id": vehicle_id, "tenant_id": DEFAULT_TENANT},
            )
        return Ownership(tenant_id=DEFAULT_TENANT, fleet_id=DEFAULT_FLEET)

    def seed(self, mapping: dict[str, Ownership]) -> None:
        """Populate directly. Used by tests to avoid a database."""
        self._cache.update(mapping)
        self._loaded_at = time.monotonic()


def classify_validation_error(error: ValidationError, payload: dict[str, Any]) -> QuarantineError:
    """Turn a Pydantic error into a specific, countable reason code.

    The reason drives ``events_rejected_total{reason}`` and the quarantine record,
    which is what makes a schema fault diagnosable without reading the payload.
    """
    details = error.errors()
    fields = sorted({str(item["loc"][0]) for item in details if item.get("loc")})
    messages = "; ".join(f"{'.'.join(str(p) for p in item.get('loc', ()))}: {item.get('msg')}" for item in details)

    types = {item.get("type", "") for item in details}
    if any("schema_version" in str(item.get("loc", ())) for item in details):
        reason = QuarantineReason.UNKNOWN_SCHEMA_VERSION
    elif any(field == "vin" for field in fields):
        reason = QuarantineReason.INVALID_VIN
    elif any(field == "ts" for field in fields):
        reason = QuarantineReason.INVALID_TIMESTAMP
    elif any(kind == "missing" for kind in types):
        reason = QuarantineReason.MISSING_FIELD
    elif any(kind.startswith(("greater_than", "less_than")) for kind in types):
        reason = QuarantineReason.OUT_OF_RANGE
    elif any("_type" in kind or "parsing" in kind for kind in types):
        reason = QuarantineReason.WRONG_TYPE
    else:
        reason = QuarantineReason.VALIDATION_ERROR

    return QuarantineError(reason, f"fields={fields} {messages}"[:2000])


def upgrade_schema(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Adapt an older schema version to the current one.

    Returns the payload and whether a transformation occurred. The
    ``schema_transformed`` flag matters downstream because a version-adapted
    event was not read exactly as the vehicle sent it, which lowers factor
    confidence.
    """
    version = str(payload.get("schema_version") or SCHEMA_VERSION_CURRENT)
    if version == SCHEMA_VERSION_CURRENT:
        return payload, False
    # 1.1 is a superset of 1.0; reading it as 1.0 only means ignoring new
    # optional fields, which Pydantic already does. Recorded as transformed so
    # the provenance is not lost.
    if version == "1.1":
        metrics.schema_transformed_total.labels(from_version=version).inc()
        return payload, True
    raise QuarantineError(
        QuarantineReason.UNKNOWN_SCHEMA_VERSION,
        f"schema_version {version!r} has no registered adapter",
    )


def validate(payload: dict[str, Any] | None) -> tuple[RawTelemetryV1, bool]:
    """Validate a decoded payload, raising :class:`QuarantineError` on failure."""
    if payload is None:
        raise QuarantineError(QuarantineReason.MALFORMED_JSON, "payload is not valid JSON")
    if not isinstance(payload, dict):
        raise QuarantineError(QuarantineReason.WRONG_TYPE, "payload is not a JSON object")

    upgraded, transformed = upgrade_schema(payload)
    try:
        return RawTelemetryV1.model_validate(upgraded), transformed
    except ValidationError as error:
        raise classify_validation_error(error, upgraded) from error


class Normalizer:
    """Builds canonical events from validated raw telemetry."""

    def __init__(
        self,
        resolver: TenantResolver | None = None,
        lateness_window_s: int = 30,
    ) -> None:
        self.resolver = resolver or TenantResolver()
        self.lateness_window_s = int(lateness_window_s)

    def to_canonical(
        self,
        raw: RawTelemetryV1,
        *,
        ingested_at: Any = None,
        schema_transformed: bool = False,
        duplicate: bool = False,
    ) -> CanonicalEvent:
        """Map raw telemetry to a canonical event."""
        processed_at = now()
        # Prefer the broker's timestamp: it is the moment the platform took
        # custody, which is what lateness should be measured against. Falling
        # back to wall clock would make lateness depend on processing delay.
        arrival = parse_ts(ingested_at) if ingested_at is not None else processed_at

        key = build_event_key(raw.vehicle_id, raw.seq, raw.ts)
        ownership = self.resolver.resolve(raw.vehicle_id)
        lateness_ms = max(0, int((arrival - raw.ts).total_seconds() * 1000))

        payload: dict[str, Any] = raw.model_dump()
        payload.update(
            {
                "ingested_at": arrival,
                "processed_at": processed_at,
                "tenant_id": ownership.tenant_id,
                "fleet_id": ownership.fleet_id,
                "event_key": key,
                "lateness_ms": lateness_ms,
                "geohash": "",
                "trace_id": otel.current_trace_id(),
                "flags": {"duplicate": duplicate, "schema_transformed": schema_transformed},
            }
        )

        enriched = EnricherRegistry.enrich(payload, {"lateness_window_s": self.lateness_window_s})
        event = CanonicalEvent.model_validate(enriched)

        if event.flags.get("late"):
            metrics.events_late_total.labels(tenant=event.tenant_id).inc()
        return event


def kafka_timestamp(message: Any) -> Any:
    """Broker timestamp for a message, or None when not available."""
    try:
        timestamp_type, value = message.timestamp()
    except Exception:  # pragma: no cover
        return None
    if timestamp_type == 0 or value is None or value <= 0:
        return None
    return from_epoch_ms(value)


def _postgres_dsn() -> str:
    from packages.common.dsn import libpq_url

    return libpq_url()
