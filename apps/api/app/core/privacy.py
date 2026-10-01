"""Location and identifier masking, driven by ``config/privacy.yaml``.

Precision is reduced by re-encoding a coordinate as a geohash at the role's
allowed character length and returning that cell's centre. Rounding decimal
degrees, as the previous implementation did, degrades differently with latitude
and gives no stable statement about the area a point could be in; a geohash cell
does — 5 characters is about 5 km, 7 about 150 m.

Two rules shape the API:

* Masking resolves the caller's **most permissive** role. Reading ``roles[0]``
  made the result depend on list order, so the same user could see different
  precision on different requests.
* An investigation view may reveal full precision to a permitted role, and that
  access is audited. Precision is a decision that needs a record, not just a flag.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from packages.common import config as cfg
from packages.common import geo
from packages.common import logging as tf_logging

LOGGER = tf_logging.get_logger(__name__)

#: Fields that reveal a position and must be masked together. Masking `lat` while
#: leaving `geohash` at storage precision would leak the exact position anyway.
LOCATION_FIELDS = ("lat", "lon")


def _config() -> cfg.PrivacyConfig:
    try:
        return cfg.privacy()
    except cfg.ConfigError as error:  # pragma: no cover
        LOGGER.error("privacy configuration unavailable, masking to lowest precision", extra={"error": str(error)})
        return cfg.PrivacyConfig(roles={"VIEWER": cfg.PrivacyRoleConfig(geohash_precision=3)})


def role_settings(roles: Iterable[str]) -> cfg.PrivacyRoleConfig:
    """Resolve the effective privacy settings for a set of roles."""
    return _config().for_role(*[str(role) for role in roles])


def is_masking_enabled() -> bool:
    from packages.common import feature_registry

    return feature_registry.is_enabled("privacy_location_masking")


def effective_precision(roles: Iterable[str], *, investigation: bool = False) -> int:
    """Geohash characters this caller may see.

    ``investigation=True`` is the incident-investigation context, where a role
    configured with ``full_precision_on_investigation`` gets the stored precision.
    """
    if not is_masking_enabled():
        return 12
    settings = role_settings(roles)
    if investigation and settings.full_precision_on_investigation:
        return 12
    return settings.geohash_precision


def mask_coordinates(
    lat: float | None,
    lon: float | None,
    roles: Iterable[str],
    *,
    investigation: bool = False,
) -> tuple[float | None, float | None]:
    """Reduce a coordinate pair to the caller's permitted precision."""
    if lat is None or lon is None:
        return lat, lon
    precision = effective_precision(roles, investigation=investigation)
    if precision >= 12:
        return float(lat), float(lon)
    try:
        return geo.mask_coordinate(float(lat), float(lon), precision)
    except (TypeError, ValueError):
        # An unusable coordinate is returned as absent rather than as a guess.
        return None, None


def mask_location(value: float | None, roles: Iterable[str] | str = "VIEWER") -> float | None:
    """Mask a single coordinate component.

    Kept for call sites that only hold one axis. Masking a pair through
    :func:`mask_coordinates` is preferable because the two components come from one
    cell and should be reduced together.
    """
    if value is None:
        return None
    role_list = [roles] if isinstance(roles, str) else list(roles)
    precision = effective_precision(role_list)
    if precision >= 12:
        return float(value)
    # Approximate mapping from geohash length to decimal places, for the rare
    # single-axis case.
    decimals = max(1, min(6, precision // 2))
    return round(float(value), decimals)


def mask_vin(vin: str | None, roles: Iterable[str]) -> str | None:
    """Mask a VIN unless the role is permitted to see it.

    The first three characters (world manufacturer identifier) and the last two
    are kept: enough to recognise make and match against a known record, not
    enough to identify the vehicle on its own.
    """
    if not vin:
        return vin
    settings = role_settings(roles)
    if settings.show_vin or not is_masking_enabled():
        return vin
    pattern = _config().vin_mask_pattern
    return f"{vin[:3]}{pattern}{vin[-2:]}"


def mask_record(
    record: dict[str, Any],
    roles: Iterable[str] | str = "VIEWER",
    *,
    investigation: bool = False,
) -> dict[str, Any]:
    """Return a copy of ``record`` with privacy-sensitive fields reduced."""
    role_list = [roles] if isinstance(roles, str) else list(roles)
    masked = dict(record)

    if "lat" in masked or "lon" in masked:
        lat, lon = mask_coordinates(masked.get("lat"), masked.get("lon"), role_list, investigation=investigation)
        if "lat" in masked:
            masked["lat"] = lat
        if "lon" in masked:
            masked["lon"] = lon

    precision = effective_precision(role_list, investigation=investigation)
    if masked.get("geohash"):
        # Truncated to the same precision, otherwise the full-resolution geohash
        # would undo the coordinate masking.
        masked["geohash"] = geo.truncate(str(masked["geohash"]), precision)

    if "vin" in masked:
        masked["vin"] = mask_vin(masked.get("vin"), role_list)

    settings = role_settings(role_list)
    if not settings.show_driver:
        for field in ("driver_id", "driver_pseudonym", "driver"):
            if field in masked:
                masked[field] = None

    masked["_privacy"] = {
        "geohash_precision": precision,
        "approximate_radius_m": geo.PRECISION_METRES.get(precision),
        "masked": precision < 12,
    }
    return masked


def mask_records(
    records: list[dict[str, Any]],
    roles: Iterable[str] | str = "VIEWER",
    *,
    investigation: bool = False,
) -> list[dict[str, Any]]:
    role_list = [roles] if isinstance(roles, str) else list(roles)
    return [mask_record(record, role_list, investigation=investigation) for record in records]


def should_audit_full_precision(roles: Iterable[str], *, investigation: bool) -> bool:
    """Whether this read reveals full precision and therefore needs an audit row."""
    if not _config().audit_full_precision_access:
        return False
    return effective_precision(roles, investigation=investigation) >= 12
