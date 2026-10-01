"""Geohash encoding, used for both analytics bucketing and privacy masking.

Geohash is the right primitive here because truncating the string reduces
precision predictably: 5 characters is roughly a 5 km cell, 7 about 150 m. That
lets ``config/privacy.yaml`` express "a VIEWER may see a 5-character cell"
without needing per-role coordinate arithmetic.
"""

from __future__ import annotations

_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

#: Approximate cell dimensions in metres, for documentation and UI hints.
PRECISION_METRES = {
    1: 5_000_000,
    2: 1_250_000,
    3: 156_000,
    4: 39_000,
    5: 4_900,
    6: 1_200,
    7: 152,
    8: 38,
    9: 4.8,
    10: 1.2,
    11: 0.15,
    12: 0.037,
}


def encode(lat: float, lon: float, precision: int = 9) -> str:
    """Encode a coordinate pair to a geohash of ``precision`` characters."""
    if not -90.0 <= lat <= 90.0:
        raise ValueError(f"latitude out of range: {lat}")
    if not -180.0 <= lon <= 180.0:
        raise ValueError(f"longitude out of range: {lon}")
    precision = max(1, min(12, int(precision)))

    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    out: list[str] = []
    bits = 0
    bit_count = 0
    use_lon = True

    while len(out) < precision:
        if use_lon:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon > mid:
                bits = (bits << 1) | 1
                lon_range[0] = mid
            else:
                bits <<= 1
                lon_range[1] = mid
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat > mid:
                bits = (bits << 1) | 1
                lat_range[0] = mid
            else:
                bits <<= 1
                lat_range[1] = mid
        use_lon = not use_lon
        bit_count += 1
        if bit_count == 5:
            out.append(_BASE32[bits])
            bits = 0
            bit_count = 0

    return "".join(out)


def decode(geohash: str) -> tuple[float, float]:
    """Return the centre coordinate of a geohash cell."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    use_lon = True
    for char in geohash.lower():
        index = _BASE32.find(char)
        if index < 0:
            raise ValueError(f"invalid geohash character: {char!r}")
        for mask in (16, 8, 4, 2, 1):
            target = lon_range if use_lon else lat_range
            mid = (target[0] + target[1]) / 2
            if index & mask:
                target[0] = mid
            else:
                target[1] = mid
            use_lon = not use_lon
    return (
        (lat_range[0] + lat_range[1]) / 2,
        (lon_range[0] + lon_range[1]) / 2,
    )


def truncate(geohash: str, precision: int) -> str:
    return geohash[: max(1, min(len(geohash), int(precision)))]


def mask_coordinate(lat: float, lon: float, precision: int) -> tuple[float, float]:
    """Reduce a coordinate to the centre of its geohash cell at ``precision``.

    Returning a cell centre rather than a rounded coordinate keeps the masked
    point inside the true cell, so a map marker never lands on the wrong side of
    a road from the real position by more than the cell size.
    """
    return decode(encode(lat, lon, precision))
