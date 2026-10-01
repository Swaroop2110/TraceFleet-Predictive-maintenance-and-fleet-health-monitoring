"""Time handling. Every timestamp in TraceFleet is timezone-aware UTC.

The platform tracks three distinct times per event and must never conflate
them: ``ts`` (event time, produced by the vehicle), ``ingested_at`` (when the
broker accepted it), and ``processed_at`` (when a service handled it). Helpers
here parse and format without ever silently substituting one for another.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

UTC = UTC


def now() -> datetime:
    """Current instant as an aware UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(value: datetime) -> datetime:
    """Attach UTC to a naive datetime, convert an aware one.

    A naive datetime reaching this function is a bug upstream, but treating it
    as UTC is safer than raising in the middle of a Kafka batch.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def parse_ts(value: str | datetime | int | float) -> datetime:
    """Parse an ISO-8601 string, epoch number, or datetime into aware UTC.

    Accepts the trailing ``Z`` form that the simulator and most OEM feeds emit,
    which ``datetime.fromisoformat`` only learned to handle in Python 3.11.
    """
    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return ensure_utc(datetime.fromisoformat(text))


def to_iso(value: datetime) -> str:
    """Render as ISO-8601 with millisecond precision and a ``Z`` suffix."""
    aware = ensure_utc(value)
    return aware.strftime("%Y-%m-%dT%H:%M:%S.") + f"{aware.microsecond // 1000:03d}Z"


def to_epoch_ms(value: datetime) -> int:
    return int(ensure_utc(value).timestamp() * 1000)


def from_epoch_ms(value: int | float) -> datetime:
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def seconds_between(earlier: datetime, later: datetime) -> float:
    return (ensure_utc(later) - ensure_utc(earlier)).total_seconds()


def floor_to_window(value: datetime, window_seconds: int) -> datetime:
    """Snap a timestamp down to a window boundary.

    Used by the incident idempotency key so two replays of the same event
    cluster derive the same bucket regardless of which event arrived first.
    """
    if window_seconds <= 0:
        raise ValueError("window_seconds must be positive")
    aware = ensure_utc(value)
    epoch = int(aware.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % window_seconds), tz=UTC)


def add_seconds(value: datetime, seconds: float) -> datetime:
    return ensure_utc(value) + timedelta(seconds=seconds)
