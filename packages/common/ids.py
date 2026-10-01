"""Deterministic identifier construction.

Every identity in the pipeline is derived from content, not from a counter or a
clock, so that a Kafka replay reconstructs exactly the same keys. That property
is what makes at-least-once delivery safe to persist.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime

from packages.common.timeutil import ensure_utc, floor_to_window, to_iso

#: Stable namespace so ``uuid5`` output is reproducible across processes.
TRACEFLEET_NAMESPACE = uuid.UUID("6f0f1f7e-6b6d-5c2a-9a7e-2f1a4c6d8b30")


def sha256_hex(*parts: object) -> str:
    """Hash the pipe-joined parts. Used wherever the PRD specifies sha256."""
    joined = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def event_key(vehicle_id: str, seq: int, ts: datetime | str) -> str:
    """Canonical event identity: ``sha256(vehicle_id|seq|ts)``.

    ``ts`` is normalised to a fixed ISO rendering first, otherwise two encodings
    of the same instant (``+00:00`` vs ``Z``) would hash differently and defeat
    deduplication.
    """
    rendered = to_iso(ts) if isinstance(ts, datetime) else to_iso(_coerce(ts))
    return sha256_hex(vehicle_id, seq, rendered)


def incident_idempotency_key(
    tenant_id: str,
    vehicle_id: str,
    start_ts: datetime,
    incident_type: str,
    window_seconds: int,
) -> str:
    """``sha256(tenant|vehicle|floor(start_ts/window)|type)`` per PRD 5.4.

    Bucketing the start time means a re-detection that begins a second or two
    earlier after a replay still collapses onto the same incident row.
    """
    bucket = floor_to_window(start_ts, window_seconds)
    return sha256_hex(tenant_id, vehicle_id, int(bucket.timestamp()), incident_type)


def incident_id(tenant_id: str, idempotency_key: str, start_ts: datetime) -> str:
    """Human-facing ``INC-YYYYMMDD-NNNNNN`` identifier.

    The six digits are derived from the idempotency key rather than a sequence:
    a database counter would not survive a replay, and an operator only needs
    the id to be short, stable, and unique within a day.
    """
    day = ensure_utc(start_ts).strftime("%Y%m%d")
    numeric = int(sha256_hex(tenant_id, idempotency_key)[:12], 16) % 1_000_000
    return f"INC-{day}-{numeric:06d}"


def evidence_id(incident_id_value: str, event_key_value: str, signal: str) -> str:
    """Deterministic evidence identity so replays replace rather than duplicate."""
    return sha256_hex(incident_id_value, event_key_value, signal)[:32]


def factor_id(incident_id_value: str, factor_type: str) -> str:
    return str(uuid.uuid5(TRACEFLEET_NAMESPACE, f"{incident_id_value}:{factor_type}"))


def run_id() -> str:
    """Random identifier for a non-reproducible action such as an AI run."""
    return str(uuid.uuid4())


def short_id(length: int = 12) -> str:
    """Compact random identifier, for prefixed keys like ``user_`` or ``era_``.

    Hex rather than a sliced UUID string, because slicing ``str(uuid4())`` can cut
    through a hyphen and produce ids like ``user_1f9a92d8-cc4``.
    """
    return uuid.uuid4().hex[: max(4, min(32, length))]


def request_id() -> str:
    return uuid.uuid4().hex


def _coerce(value: str) -> datetime:
    from packages.common.timeutil import parse_ts

    return parse_ts(value)
