"""Data contracts shared across service boundaries.

Anything that crosses a Kafka topic or an HTTP boundary is defined here as a
Pydantic model so producer and consumer cannot drift. The JSON Schema files in
this package are the language-neutral copy used for contract tests and for
generating TypeScript types.
"""

from packages.schemas.models import (
    CanonicalEvent,
    DataQuality,
    EvidenceItem,
    FactorScore,
    IncidentEnvelope,
    IncidentPayload,
    QuarantineRecord,
    RawTelemetryV1,
    SignalPayload,
)

__all__ = [
    "CanonicalEvent",
    "DataQuality",
    "EvidenceItem",
    "FactorScore",
    "IncidentEnvelope",
    "IncidentPayload",
    "QuarantineRecord",
    "RawTelemetryV1",
    "SignalPayload",
]
