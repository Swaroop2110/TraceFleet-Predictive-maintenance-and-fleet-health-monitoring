"""Shared telemetry builders for scenarios.

Underscore-prefixed so registry discovery skips it.

Scenarios are written as explicit point tables — speed, acceleration, brake,
heading — rather than generated from the kinematic model. That is deliberate: a
scenario has to reproduce an exact detector outcome on every run, and a model with
randomness in it cannot guarantee that. The kinematic model produces the *baseline*
stream; scenarios produce the specific patterns being tested.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timedelta
from typing import Any

from packages.common.timeutil import ensure_utc, now, to_iso

#: Default origin, per the PRD's example coordinates.
SURAT_CENTER = (21.1702, 72.8311)


def scenario_start(start_time: datetime | None = None) -> datetime:
    return ensure_utc(start_time or now())


def telemetry(
    vehicle_id: str,
    seq: int,
    timestamp: datetime,
    *,
    speed_kmh: float = 55.0,
    acceleration_mps2: float = 0.0,
    brake_force_pct: float = 0.0,
    heading_deg: float = 90.0,
    dtc: list[str] | None = None,
    event: str = "NORMAL",
    lat: float | None = None,
    lon: float | None = None,
    soc_pct: float = 74.0,
    padding_bytes: int = 0,
) -> dict[str, Any]:
    """Build one raw telemetry event."""
    suffix = "".join(character for character in vehicle_id.upper() if character.isalnum())[-5:].rjust(5, "0")
    partial = f"1HGCM82633A0{suffix}"[:17].ljust(17, "0")
    from sim.vehicle_model import vin_check_digit

    vin = partial[:8] + vin_check_digit(partial) + partial[9:]

    event_payload: dict[str, Any] = {
        "event_id": f"{vehicle_id}:{seq}",
        "vehicle_id": vehicle_id,
        "vin": vin,
        "ts": to_iso(timestamp),
        "lat": SURAT_CENTER[0] if lat is None else lat,
        "lon": SURAT_CENTER[1] if lon is None else lon,
        "speed_kmh": speed_kmh,
        "acceleration_mps2": acceleration_mps2,
        "brake_force_pct": brake_force_pct,
        "heading_deg": heading_deg % 360,
        "rpm": max(700, int(800 + max(speed_kmh, 0) * 28)),
        "soc_pct": soc_pct,
        "odo_km": round(12543.2 + seq * max(speed_kmh, 0) / 3600, 3),
        "dtc": list(dtc or []),
        "evt": event,
        "seq": seq,
        "schema_version": "1.0",
    }
    if padding_bytes > 0:
        event_payload["extras"] = {
            "diag": {f"sensor_{index:02d}": 0.0 for index in range(max(1, padding_bytes // 40))},
            "firmware": "2.14.7-fleet",
        }
    return event_payload


def from_points(
    vehicle_id: str,
    start_seq: int,
    start: datetime,
    points: Iterable[tuple[Any, ...]],
    *,
    keys: tuple[str, ...],
    drift: bool = True,
    padding_bytes: int = 0,
) -> list[dict[str, Any]]:
    """Expand a point table into events, one per second.

    ``keys`` names each tuple position after the leading offset, so a scenario reads
    as a table rather than a list of positional arguments.
    """
    events: list[dict[str, Any]] = []
    for point in points:
        offset, values = point[0], point[1:]
        attributes = dict(zip(keys, values, strict=False))
        if drift:
            # A little positional movement so a map view shows a path rather than a
            # single stacked marker.
            attributes.setdefault("lat", SURAT_CENTER[0] + offset * 0.00003)
            attributes.setdefault("lon", SURAT_CENTER[1] + offset * 0.00002)
        events.append(
            telemetry(
                vehicle_id,
                start_seq + offset,
                start + timedelta(seconds=offset),
                padding_bytes=padding_bytes,
                **attributes,
            )
        )
    return events


def normal_events(
    vehicle_id: str,
    start_seq: int = 0,
    start_time: datetime | None = None,
    count: int = 5,
    *,
    padding_bytes: int = 0,
) -> list[dict[str, Any]]:
    """Baseline cruising, comfortably inside every detector threshold."""
    start = scenario_start(start_time)
    return [
        telemetry(
            vehicle_id,
            start_seq + offset,
            start + timedelta(seconds=offset),
            speed_kmh=54 + offset,
            heading_deg=88 + offset,
            padding_bytes=padding_bytes,
        )
        for offset in range(count)
    ]


def event_keys_for(events: list[dict[str, Any]]) -> list[str]:
    """Compute the canonical event keys the pipeline will derive.

    Recorded in ground truth so an evaluation can match detected evidence to the
    exact events that produced it, rather than guessing by timestamp.
    """
    from packages.common.ids import event_key

    return [event_key(item["vehicle_id"], item["seq"], item["ts"]) for item in events]


def span(events: list[dict[str, Any]]) -> tuple[datetime, datetime]:
    """First and last event time in a scenario's output."""
    from packages.common.timeutil import parse_ts

    stamps = sorted(parse_ts(item["ts"]) for item in events)
    return stamps[0], stamps[-1]
