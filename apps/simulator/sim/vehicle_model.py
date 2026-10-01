"""Kinematic vehicle model for baseline telemetry.

Produces plausible driving rather than random numbers, because the detectors are
threshold-based and noise around a threshold produces a stream of false signals.
A vehicle here accelerates and decelerates within road-class limits, turns
smoothly, drains charge in proportion to work done, and integrates odometer from
its own speed.

Fully deterministic under a seed: the same seed and vehicle id reproduce the same
drive, which is what makes a benchmark or a ground-truth evaluation repeatable.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from packages.common.timeutil import now

#: Road classes with a cruising range and a turn rate, in km/h and deg/s.
#
#: Every ceiling sits below the overspeed threshold in config/detection_rules.yaml
#: (90 km/h). Baseline traffic must be *unremarkable*: if ordinary highway cruising
#: raised overspeed, the detector would fire continuously and a real overspeed
#: pattern would be indistinguishable from background noise. Scenarios, not the
#: baseline model, are what cross thresholds.
ROAD_CLASSES: dict[str, dict[str, float]] = {
    "urban": {"min_speed": 15.0, "max_speed": 52.0, "turn_rate": 6.0, "stop_probability": 0.05},
    "arterial": {"min_speed": 35.0, "max_speed": 72.0, "turn_rate": 3.0, "stop_probability": 0.015},
    "highway": {"min_speed": 70.0, "max_speed": 86.0, "turn_rate": 1.2, "stop_probability": 0.002},
}

#: Largest heading change the model will apply in one second. Below the
#: abrupt_heading_change threshold (25 degrees) so a routine turn is not a signal.
MAX_HEADING_DELTA_DEG = 18.0

#: City centres the fleet clusters around. Surat first, per the PRD.
CITY_CENTRES: tuple[tuple[str, float, float], ...] = (
    ("Surat", 21.1702, 72.8311),
    ("Ahmedabad", 23.0225, 72.5714),
    ("Mumbai", 19.0760, 72.8777),
    ("Pune", 18.5204, 73.8567),
    ("Bengaluru", 12.9716, 77.5946),
)

#: Comfortable acceleration bounds, well inside the detector thresholds (3.0 and
#: -4.0 m/s^2) so ordinary driving does not trip them.
COMFORT_ACCEL_MAX = 1.8
COMFORT_DECEL_MAX = -2.2

#: Degrees of latitude per metre.
_DEG_PER_METRE = 1.0 / 111_320.0


@dataclass
class VehicleState:
    """Mutable state of one simulated vehicle."""

    vehicle_id: str
    vin: str
    road_class: str
    lat: float
    lon: float
    speed_kmh: float
    heading_deg: float
    soc_pct: float
    odo_km: float
    seq: int = 0
    acceleration_mps2: float = 0.0
    brake_force_pct: float = 0.0
    dtc: list[str] = field(default_factory=list)
    evt: str = "NORMAL"
    #: Per-vehicle RNG, so one vehicle's draws do not shift another's.
    rng: random.Random = field(default_factory=random.Random)
    _stopped_for: int = 0
    #: True while the vehicle is braking toward a planned stop.
    _stopping: bool = False

    @property
    def limits(self) -> dict[str, float]:
        return ROAD_CLASSES[self.road_class]


def vin_check_digit(partial: str) -> str:
    """Compute the ISO 3779 check digit for positions 1-8 and 10-17.

    Real VINs carry a check digit, and a validator or downstream consumer may
    verify it. Generating structurally invalid VINs would make every event look
    like a data fault.
    """
    transliteration = {
        **{str(digit): digit for digit in range(10)},
        "A": 1,
        "B": 2,
        "C": 3,
        "D": 4,
        "E": 5,
        "F": 6,
        "G": 7,
        "H": 8,
        "J": 1,
        "K": 2,
        "L": 3,
        "M": 4,
        "N": 5,
        "P": 7,
        "R": 9,
        "S": 2,
        "T": 3,
        "U": 4,
        "V": 5,
        "W": 6,
        "X": 7,
        "Y": 8,
        "Z": 9,
    }
    weights = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
    total = 0
    for index, character in enumerate(partial):
        if index == 8:
            continue
        total += transliteration.get(character, 0) * weights[index]
    remainder = total % 11
    return "X" if remainder == 10 else str(remainder)


def generate_vin(seed_value: int, oem_prefix: str = "1HG") -> str:
    """Build a 17-character VIN with a valid check digit."""
    alphabet = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
    rng = random.Random(seed_value)
    middle = "".join(rng.choice(alphabet) for _ in range(5))
    year_code = "NPRSTVWXY"[seed_value % 9]
    plant = rng.choice("ABCDEFGH")
    serial = f"{seed_value % 1_000_000:06d}"
    partial = f"{oem_prefix}{middle}0{year_code}{plant}{serial}"[:17].ljust(17, "0")
    return partial[:8] + vin_check_digit(partial) + partial[9:]


OEMS = (
    ("1HG", "Honda", ("Accord", "Civic", "CR-V")),
    ("1FT", "Ford", ("Transit", "F-150", "Ranger")),
    ("WVW", "Volkswagen", ("Transporter", "Caddy", "Crafter")),
    ("MAT", "Tata", ("Ace", "Intra", "Ultra")),
    ("MA3", "Maruti", ("Eeco", "Super Carry")),
)


def create_vehicle(index: int, seed: int = 42, road_class: str | None = None) -> VehicleState:
    """Create a deterministic vehicle. ``index`` identifies it within the fleet."""
    rng = random.Random((seed * 1_000_003) + index)
    oem_prefix, _oem, _models = OEMS[index % len(OEMS)]
    resolved_class = road_class or ("urban", "arterial", "highway")[index % 3]
    _city, base_lat, base_lon = CITY_CENTRES[index % len(CITY_CENTRES)]

    limits = ROAD_CLASSES[resolved_class]
    # Scattered within roughly 15 km of the city centre.
    spread = 0.15
    return VehicleState(
        vehicle_id=f"veh_{index:06d}",
        vin=generate_vin((seed * 7919) + index, oem_prefix),
        road_class=resolved_class,
        lat=round(base_lat + rng.uniform(-spread, spread), 6),
        lon=round(base_lon + rng.uniform(-spread, spread), 6),
        speed_kmh=round(rng.uniform(limits["min_speed"], limits["max_speed"]), 1),
        heading_deg=round(rng.uniform(0, 360), 1),
        soc_pct=round(rng.uniform(35.0, 100.0), 1),
        odo_km=round(rng.uniform(1_000.0, 250_000.0), 1),
        rng=rng,
    )


def vehicle_metadata(index: int, seed: int = 42) -> dict[str, Any]:
    """Registry attributes for a vehicle, used by the fleet seeder."""
    rng = random.Random((seed * 104_729) + index)
    oem_prefix, oem, models = OEMS[index % len(OEMS)]
    resolved_class = ("urban", "arterial", "highway")[index % 3]
    return {
        "vehicle_id": f"veh_{index:06d}",
        "vin": generate_vin((seed * 7919) + index, oem_prefix),
        "oem": oem,
        "model": rng.choice(models),
        "model_year": 2016 + (index % 10),
        "road_class": resolved_class,
    }


def step(state: VehicleState, elapsed_s: float = 1.0) -> VehicleState:
    """Advance one vehicle by ``elapsed_s`` seconds.

    Acceleration is drawn as a bounded random walk toward the road class's cruising
    band rather than uniformly, so the vehicle behaves like it is being driven and
    stays clear of the detector thresholds unless a scenario overrides it.
    """
    limits = state.limits
    rng = state.rng

    # Slowing to a halt, then idling. Done over several ticks at comfort
    # deceleration: dropping straight to zero is both physically wrong and
    # indistinguishable from the collision pattern, so it would make every routine
    # stop raise sudden_stop.
    if state._stopping:
        return _decelerate_to_stop(state, elapsed_s)

    if state._stopped_for > 0:
        state._stopped_for -= 1
        state.speed_kmh = 0.0
        state.acceleration_mps2 = 0.0
        state.brake_force_pct = 0.0
        state.evt = "IDLE"
        state.seq += 1
        return state

    target = rng.uniform(limits["min_speed"], limits["max_speed"])
    # Pull toward the target, then clamp to comfort limits. Converting km/h to
    # m/s^2 needs the 3.6 factor; omitting it produces accelerations an order of
    # magnitude too large and a constant stream of false signals.
    delta_kmh = target - state.speed_kmh
    desired_accel = (delta_kmh / 3.6) / max(elapsed_s, 0.1) * 0.25
    accel = max(COMFORT_DECEL_MAX, min(COMFORT_ACCEL_MAX, desired_accel))
    accel += rng.gauss(0, 0.15)
    accel = max(COMFORT_DECEL_MAX, min(COMFORT_ACCEL_MAX, accel))

    new_speed = state.speed_kmh + (accel * 3.6 * elapsed_s)
    new_speed = max(0.0, min(limits["max_speed"] * 1.05, new_speed))

    # Light braking when slowing meaningfully, so brake_force correlates with
    # deceleration the way a real vehicle's would.
    state.brake_force_pct = round(min(45.0, abs(accel) * 12.0), 1) if accel < -0.4 else 0.0

    # Clamped, because an unbounded Gaussian occasionally draws a tail value large
    # enough to look like a swerve.
    turn = rng.gauss(0, limits["turn_rate"] * elapsed_s)
    turn = max(-MAX_HEADING_DELTA_DEG, min(MAX_HEADING_DELTA_DEG, turn))
    state.heading_deg = (state.heading_deg + turn) % 360.0

    distance_km = ((state.speed_kmh + new_speed) / 2.0) * (elapsed_s / 3600.0)
    state.odo_km = round(state.odo_km + distance_km, 3)
    _advance_position(state, distance_km)

    # Consumption rises with speed; the quadratic term stands in for drag.
    drain = (distance_km * 0.18) + ((new_speed / 100.0) ** 2) * 0.004
    state.soc_pct = round(max(0.0, state.soc_pct - drain), 2)
    if state.soc_pct <= 0.5:
        state.soc_pct = round(rng.uniform(80.0, 100.0), 1)

    state.speed_kmh = round(new_speed, 1)
    state.acceleration_mps2 = round(accel, 2)
    state.evt = "NORMAL"
    state.seq += 1

    if rng.random() < limits["stop_probability"]:
        state._stopping = True
        state._stopped_for = rng.randint(2, 8)

    # Diagnostic codes appear and clear on their own, which is what the dtc_fault
    # detector's "newly asserted" logic exists to handle.
    if rng.random() < 0.0004:
        code = rng.choice(["P0301", "P0420", "P0171", "U0100", "B1318", "C1234"])
        if code not in state.dtc:
            state.dtc = [*state.dtc, code][:4]
    elif state.dtc and rng.random() < 0.002:
        state.dtc = state.dtc[:-1]

    return state


def _decelerate_to_stop(state: VehicleState, elapsed_s: float) -> VehicleState:
    """Brake toward a halt at comfort deceleration.

    Held below the harsh-braking deceleration threshold and spread over enough ticks
    that the speed lost in any 3-second window stays under the sudden-stop threshold.
    A routine stop at a junction must not look like a collision.
    """
    decel = COMFORT_DECEL_MAX * 0.8
    new_speed = max(0.0, state.speed_kmh + (decel * 3.6 * elapsed_s))

    distance_km = ((state.speed_kmh + new_speed) / 2.0) * (elapsed_s / 3600.0)
    state.odo_km = round(state.odo_km + distance_km, 3)
    _advance_position(state, distance_km)

    state.speed_kmh = round(new_speed, 1)
    state.acceleration_mps2 = round(decel, 2)
    state.brake_force_pct = round(min(40.0, abs(decel) * 14.0), 1)
    state.evt = "BRAKING"
    state.seq += 1

    if new_speed <= 0.5:
        state.speed_kmh = 0.0
        state._stopping = False
        state.evt = "IDLE"
    return state


def _advance_position(state: VehicleState, distance_km: float) -> None:
    """Move the vehicle along its heading.

    Longitude degrees shrink with latitude, so the cosine correction keeps the
    travelled distance correct rather than stretching east-west movement.
    """
    metres = distance_km * 1000.0
    radians = math.radians(state.heading_deg)
    state.lat = round(state.lat + (metres * math.cos(radians) * _DEG_PER_METRE), 6)
    cos_lat = max(0.01, math.cos(math.radians(state.lat)))
    state.lon = round(state.lon + (metres * math.sin(radians) * _DEG_PER_METRE / cos_lat), 6)

    # Keep the fleet inside plausible bounds rather than letting it drift into the
    # ocean over a long run.
    state.lat = max(-85.0, min(85.0, state.lat))
    state.lon = max(-179.0, min(179.0, state.lon))


def to_event(
    state: VehicleState,
    timestamp: datetime | None = None,
    *,
    padding_bytes: int = 0,
) -> dict[str, Any]:
    """Render the current state as a raw telemetry event."""
    from packages.common.timeutil import to_iso

    moment = timestamp or now()
    event: dict[str, Any] = {
        "event_id": f"{state.vehicle_id}:{state.seq}",
        "vehicle_id": state.vehicle_id,
        "vin": state.vin,
        "ts": to_iso(moment),
        "lat": state.lat,
        "lon": state.lon,
        "speed_kmh": state.speed_kmh,
        "acceleration_mps2": state.acceleration_mps2,
        "brake_force_pct": state.brake_force_pct,
        "heading_deg": round(state.heading_deg, 1),
        "rpm": max(700, int(800 + state.speed_kmh * 28)),
        "soc_pct": state.soc_pct,
        "odo_km": state.odo_km,
        "dtc": list(state.dtc),
        "evt": state.evt,
        "seq": state.seq,
        "schema_version": "1.0",
    }
    if padding_bytes > 0:
        event["extras"] = _padding(state, padding_bytes)
    return event


#: Serialised cost of one padding entry, e.g. `"sensor_07":-12.345,`. Measured
#: rather than guessed, so `padding_bytes` means roughly what it says.
_PADDING_BYTES_PER_FIELD = 21


def _padding(state: VehicleState, target_bytes: int) -> dict[str, Any]:
    """Pad an event toward the PRD's ~1 KB realistic size.

    Shaped like plausible extra signals rather than filler text, so the payload
    exercises realistic JSON parsing cost instead of one long string.
    """
    fields = max(1, target_bytes // _PADDING_BYTES_PER_FIELD)
    return {
        "diag": {f"sensor_{index:02d}": round(state.rng.uniform(-100.0, 100.0), 3) for index in range(fields)},
        "firmware": "2.14.7-fleet",
        "network": {"rssi_dbm": state.rng.randint(-110, -50), "carrier": "sim-net"},
    }


def advance_fleet(
    fleet: list[VehicleState],
    timestamp: datetime,
    *,
    elapsed_s: float = 1.0,
    padding_bytes: int = 0,
) -> list[dict[str, Any]]:
    """Step every vehicle once and return the resulting events."""
    return [to_event(step(vehicle, elapsed_s), timestamp, padding_bytes=padding_bytes) for vehicle in fleet]


def build_fleet(count: int, seed: int = 42, offset: int = 0) -> list[VehicleState]:
    """Create ``count`` vehicles. ``offset`` shards a fleet across worker processes."""
    return [create_vehicle(offset + index, seed) for index in range(count)]
