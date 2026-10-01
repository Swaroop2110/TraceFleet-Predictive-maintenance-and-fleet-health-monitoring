"""Typed, validated access to everything under ``config/``.

The build rules say nothing the PRD calls "configuration-driven" may be
hard-coded. That only holds if a malformed YAML file fails loudly at startup
rather than silently yielding ``{}`` and letting a detector fall back to a
default threshold. So every config file has a Pydantic model here, and services
load through :func:`load_all` during boot.

Loaders are cached per resolved path and expose ``reload()`` for the
hot-reloadable files (detection rules, severity, factors), which is what
``POST /api/v1/admin/config/reload`` drives.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

SEVERITIES = ("INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL")
Severity = Literal["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"]

INCIDENT_STATUSES = (
    "DETECTED",
    "RECONSTRUCTING",
    "OPEN",
    "ACKNOWLEDGED",
    "CLOSED",
    "INVALIDATED",
)


def project_root() -> Path:
    """Repository root, overridable so tests can point at a fixture tree."""
    override = os.getenv("TRACEFLEET_ROOT")
    if override:
        return Path(override).resolve()
    return Path(__file__).resolve().parents[2]


def config_dir() -> Path:
    override = os.getenv("TRACEFLEET_CONFIG_DIR")
    if override:
        return Path(override).resolve()
    return project_root() / "config"


class _Strict(BaseModel):
    """Reject unknown keys.

    A typo in a threshold name is the failure mode this catches: without
    ``extra="forbid"``, ``speed_kmh_get: 90`` would be silently ignored and the
    detector would quietly use its default.
    """

    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------
# detection_rules.yaml
# --------------------------------------------------------------------------
class RuleConfig(_Strict):
    enabled: bool = True
    weight: float = Field(default=0.1, ge=0.0, le=1.0)
    params: dict[str, Any] = Field(default_factory=dict)


class IncidentTriggerConfig(_Strict):
    min_distinct_signals: int = Field(default=2, ge=1)
    must_include_any: list[str] = Field(default_factory=list)
    cooldown_seconds: int = Field(default=60, ge=0)


class DetectionRulesConfig(_Strict):
    version: int = 1
    window_seconds: int = Field(default=30, ge=1, le=3600)
    rules: dict[str, RuleConfig] = Field(default_factory=dict)
    incident_trigger: IncidentTriggerConfig = Field(default_factory=IncidentTriggerConfig)

    @model_validator(mode="after")
    def _trigger_signals_are_known(self) -> DetectionRulesConfig:
        unknown = set(self.incident_trigger.must_include_any) - set(self.rules)
        if unknown:
            raise ValueError(f"incident_trigger.must_include_any references unknown rules: {sorted(unknown)}")
        # A trigger that can never fire is a silent outage, so refuse it here.
        enabled = {name for name, rule in self.rules.items() if rule.enabled}
        required = set(self.incident_trigger.must_include_any)
        if required and not (required & enabled):
            raise ValueError(
                "incident_trigger.must_include_any lists only disabled rules; no incident could ever be triggered"
            )
        if self.incident_trigger.min_distinct_signals > max(len(enabled), 1):
            raise ValueError(
                f"min_distinct_signals={self.incident_trigger.min_distinct_signals} exceeds "
                f"the {len(enabled)} enabled rule(s)"
            )
        return self

    def enabled_rules(self) -> dict[str, RuleConfig]:
        return {name: rule for name, rule in self.rules.items() if rule.enabled}


# --------------------------------------------------------------------------
# correlation.yaml
# --------------------------------------------------------------------------
class CorrelationConfig(_Strict):
    correlation_window_s: int = Field(default=60, ge=1)
    lateness_window_s: int = Field(default=30, ge=0)
    pre_window_s: int = Field(default=60, ge=0)
    post_window_s: int = Field(default=30, ge=0)
    reconstruction_timeout_s: int = Field(default=300, ge=1)
    policy: str = "same_vehicle_time_window"
    max_active_vehicles: int = Field(default=200_000, ge=1)
    max_active_incidents: int = Field(default=50_000, ge=1)

    @model_validator(mode="after")
    def _timeout_covers_post_window(self) -> CorrelationConfig:
        if self.reconstruction_timeout_s < self.post_window_s:
            raise ValueError(
                "reconstruction_timeout_s must be at least post_window_s, otherwise "
                "reconstruction always times out before the post-incident window closes"
            )
        return self


# --------------------------------------------------------------------------
# severity.yaml
# --------------------------------------------------------------------------
class SeverityWhen(_Strict):
    distinct_signals_gte: int | None = Field(default=None, ge=1)
    includes: list[str] = Field(default_factory=list)
    repeated: bool | None = None

    @model_validator(mode="after")
    def _not_empty(self) -> SeverityWhen:
        if self.distinct_signals_gte is None and not self.includes and self.repeated is None:
            raise ValueError("severity matrix 'when' clause must constrain something")
        return self

    def specificity(self) -> tuple[int, int, int]:
        """Rank how specific this clause is, most specific first.

        The matrix is authored most-general-first for readability, so the policy
        must not be "last match wins" — that downgrades a repeated 2-signal
        incident to LOW. Sorting by specificity makes the intended row win
        regardless of file order.
        """
        return (
            self.distinct_signals_gte or 0,
            len(self.includes),
            1 if self.repeated is not None else 0,
        )


class SeverityRule(_Strict):
    when: SeverityWhen
    severity: Severity


class SeverityConfig(_Strict):
    matrix: list[SeverityRule] = Field(default_factory=list)
    default: Severity = "INFO"
    disclaimer: str = ""

    @model_validator(mode="after")
    def _matrix_present(self) -> SeverityConfig:
        if not self.matrix:
            raise ValueError("severity.yaml must define at least one matrix row")
        return self

    def ordered(self) -> list[SeverityRule]:
        """Matrix sorted most-specific-first for deterministic first-match wins."""
        return sorted(
            self.matrix,
            key=lambda rule: (rule.when.specificity(), SEVERITIES.index(rule.severity)),
            reverse=True,
        )


# --------------------------------------------------------------------------
# factors.yaml
# --------------------------------------------------------------------------
class FactorConfig(_Strict):
    weight: float = Field(default=0.1, ge=0.0, le=1.0)
    max_value: float = Field(default=1.0, gt=0.0)
    min_value: float = 0.0
    duration_bonus: float = Field(default=0.0, ge=0.0, le=1.0)
    description: str = ""

    @model_validator(mode="after")
    def _range_is_ordered(self) -> FactorConfig:
        if self.min_value >= self.max_value:
            raise ValueError("factor min_value must be below max_value")
        return self


class FactorsConfig(_Strict):
    factors: dict[str, FactorConfig] = Field(default_factory=dict)
    #: Penalty applied to confidence for each late or missing evidence item.
    late_evidence_penalty: float = Field(default=0.15, ge=0.0, le=1.0)
    missing_field_penalty: float = Field(default=0.10, ge=0.0, le=1.0)
    min_confidence: float = Field(default=0.1, ge=0.0, le=1.0)


# --------------------------------------------------------------------------
# scenarios.yaml
# --------------------------------------------------------------------------
class ScenarioConfig(_Strict):
    enabled: bool = True
    probability_per_vehicle_hour: float = Field(default=0.0, ge=0.0)
    params: dict[str, Any] = Field(default_factory=dict)
    should_create_incident: bool = False
    expected_signals: list[str] = Field(default_factory=list)
    expected_severity: Severity | None = None
    description: str = ""

    @model_validator(mode="after")
    def _expectations_are_consistent(self) -> ScenarioConfig:
        # Ground-truth accuracy is measured against these fields, so an
        # inconsistent label would corrupt the evaluation rather than the system.
        if self.should_create_incident and not self.expected_signals:
            raise ValueError("a scenario expected to create an incident must declare expected_signals")
        if not self.should_create_incident and self.expected_severity is not None:
            raise ValueError("expected_severity is meaningless when should_create_incident is false")
        return self


class ScenariosConfig(_Strict):
    scenarios: dict[str, ScenarioConfig] = Field(default_factory=dict)
    default_mix: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _mix_is_known(self) -> ScenariosConfig:
        unknown = set(self.default_mix) - set(self.scenarios)
        if unknown:
            raise ValueError(f"default_mix references unknown scenarios: {sorted(unknown)}")
        return self

    def enabled_scenarios(self) -> dict[str, ScenarioConfig]:
        return {name: cfg for name, cfg in self.scenarios.items() if cfg.enabled}


# --------------------------------------------------------------------------
# profiles/*.yaml
# --------------------------------------------------------------------------
class ProfileConfig(_Strict):
    name: str = "dev"
    vehicles: int = Field(default=2_000, ge=1)
    target_events_per_s: int = Field(default=2_000, ge=1)
    tenants: int = Field(default=3, ge=1)
    fleets_per_tenant: int = Field(default=20, ge=1)
    burst_multiplier: float = Field(default=3.0, ge=1.0)
    burst_duration_s: int = Field(default=300, ge=0)
    simulator_workers: int = Field(default=0, ge=0, description="0 means use CPU count")
    kafka_partitions: dict[str, int] = Field(default_factory=dict)
    consumer_replicas: dict[str, int] = Field(default_factory=dict)
    clickhouse_batch_size: int = Field(default=10_000, ge=1)
    clickhouse_flush_ms: int = Field(default=500, ge=1)
    kafka_max_batch: int = Field(default=500, ge=1)
    kafka_max_wait_ms: int = Field(default=50, ge=1)
    redis_state_ttl_s: int = Field(default=86_400, ge=1)
    dedup_ttl_s: int = Field(default=3_600, ge=1)
    event_padding_bytes: int = Field(default=0, ge=0)


# --------------------------------------------------------------------------
# retention.yaml
# --------------------------------------------------------------------------
class RetentionConfig(_Strict):
    telemetry_hot_days: int = Field(default=7, ge=1)
    telemetry_warm_days: int = Field(default=30, ge=1)
    telemetry_cold_days: int = Field(default=365, ge=1)
    incident_days: int = Field(default=730, ge=1)
    audit_days: int = Field(default=2_555, ge=1)
    ai_run_days: int = Field(default=365, ge=1)
    quarantine_days: int = Field(default=14, ge=1)
    redis_state_ttl_s: int = Field(default=86_400, ge=1)

    @model_validator(mode="after")
    def _tiers_increase(self) -> RetentionConfig:
        if not self.telemetry_hot_days <= self.telemetry_warm_days <= self.telemetry_cold_days:
            raise ValueError("telemetry retention tiers must be hot <= warm <= cold")
        return self


# --------------------------------------------------------------------------
# privacy.yaml
# --------------------------------------------------------------------------
class PrivacyRoleConfig(_Strict):
    #: Geohash characters retained. Higher means more precise.
    geohash_precision: int = Field(default=5, ge=1, le=12)
    show_vin: bool = False
    show_driver: bool = False
    #: Whether full precision is available inside an incident investigation.
    full_precision_on_investigation: bool = False


class PrivacyConfig(_Strict):
    default_role: str = "VIEWER"
    roles: dict[str, PrivacyRoleConfig] = Field(default_factory=dict)
    vin_mask_pattern: str = "***"
    audit_full_precision_access: bool = True

    @model_validator(mode="after")
    def _default_role_exists(self) -> PrivacyConfig:
        if self.roles and self.default_role not in self.roles:
            raise ValueError(f"privacy default_role {self.default_role!r} is not defined in roles")
        return self

    def for_role(self, *role_names: str) -> PrivacyRoleConfig:
        """Most permissive configuration among the caller's roles.

        A user holding both VIEWER and ADMIN should get ADMIN precision. Taking
        the first element of the role list, as the previous implementation did,
        made masking depend on list ordering.
        """
        candidates = [self.roles[name.upper()] for name in role_names if name.upper() in self.roles]
        if not candidates:
            return self.roles.get(self.default_role, PrivacyRoleConfig())
        return max(candidates, key=lambda cfg: (cfg.geohash_precision, cfg.show_vin))


# --------------------------------------------------------------------------
# roles.yaml
# --------------------------------------------------------------------------
class RoleConfig(_Strict):
    description: str = ""
    permissions: list[str] = Field(default_factory=list)
    inherits: list[str] = Field(default_factory=list)


class RolesConfig(_Strict):
    permissions: list[str] = Field(default_factory=list)
    roles: dict[str, RoleConfig] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _permissions_and_inheritance_resolve(self) -> RolesConfig:
        known = set(self.permissions)
        for name, role in self.roles.items():
            unknown = set(role.permissions) - known - {"*"}
            if known and unknown:
                raise ValueError(f"role {name} grants undeclared permissions: {sorted(unknown)}")
            missing_parents = set(role.inherits) - set(self.roles)
            if missing_parents:
                raise ValueError(f"role {name} inherits unknown roles: {sorted(missing_parents)}")
        for name in self.roles:
            self._resolve(name, set())
        return self

    def _resolve(self, name: str, seen: set[str]) -> set[str]:
        if name in seen:
            raise ValueError(f"role inheritance cycle involving {name!r}")
        role = self.roles.get(name)
        if role is None:
            return set()
        granted = set(role.permissions)
        for parent in role.inherits:
            granted |= self._resolve(parent, seen | {name})
        return granted

    def permissions_for(self, *role_names: str) -> set[str]:
        """Union of permissions across the caller's roles, following inheritance."""
        granted: set[str] = set()
        for name in role_names:
            granted |= self._resolve(name.upper(), set())
        if "*" in granted:
            return set(self.permissions) | {"*"}
        return granted

    def allows(self, role_names: list[str], permission: str) -> bool:
        granted = self.permissions_for(*role_names)
        return "*" in granted or permission in granted


# --------------------------------------------------------------------------
# Loading
# --------------------------------------------------------------------------
_CACHE: dict[str, Any] = {}
_LOCK = threading.RLock()

_FILES: dict[str, tuple[str, type[BaseModel]]] = {
    "detection_rules": ("detection_rules.yaml", DetectionRulesConfig),
    "correlation": ("correlation.yaml", CorrelationConfig),
    "severity": ("severity.yaml", SeverityConfig),
    "factors": ("factors.yaml", FactorsConfig),
    "scenarios": ("scenarios.yaml", ScenariosConfig),
    "retention": ("retention.yaml", RetentionConfig),
    "privacy": ("privacy.yaml", PrivacyConfig),
    "roles": ("roles.yaml", RolesConfig),
}


class ConfigError(RuntimeError):
    """Raised when a configuration file is missing or fails validation."""


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"configuration file not found: {path}")
    try:
        with path.open(encoding="utf-8") as handle:
            data = yaml.safe_load(handle)
    except yaml.YAMLError as error:
        raise ConfigError(f"{path.name} is not valid YAML: {error}") from error
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name} must contain a mapping at the top level")
    return data


def load(name: str, *, refresh: bool = False) -> Any:
    """Load and validate one configuration file by logical name."""
    if name not in _FILES:
        raise ConfigError(f"unknown configuration {name!r}; expected one of {sorted(_FILES)}")
    filename, model = _FILES[name]
    path = config_dir() / filename
    cache_key = f"{name}:{path}"
    with _LOCK:
        if not refresh and cache_key in _CACHE:
            return _CACHE[cache_key]
        raw = _read_yaml(path)
        try:
            parsed = model.model_validate(raw)
        except Exception as error:
            raise ConfigError(f"{filename} failed validation:\n{error}") from error
        _CACHE[cache_key] = parsed
        return parsed


def load_profile(name: str | None = None, *, refresh: bool = False) -> ProfileConfig:
    """Load a scale profile. ``PROFILE`` env var is the default source."""
    profile_name = name or os.getenv("PROFILE") or os.getenv("TRACEFLEET_PROFILE") or "dev"
    path = config_dir() / "profiles" / f"{profile_name}.yaml"
    cache_key = f"profile:{path}"
    with _LOCK:
        if not refresh and cache_key in _CACHE:
            return _CACHE[cache_key]
        if not path.exists():
            available = sorted(p.stem for p in (config_dir() / "profiles").glob("*.yaml"))
            raise ConfigError(f"unknown profile {profile_name!r}; available: {available}")
        raw = _read_yaml(path)
        raw.setdefault("name", profile_name)
        try:
            parsed = ProfileConfig.model_validate(raw)
        except Exception as error:
            raise ConfigError(f"profiles/{profile_name}.yaml failed validation:\n{error}") from error
        _CACHE[cache_key] = parsed
        return parsed


def detection_rules(refresh: bool = False) -> DetectionRulesConfig:
    return load("detection_rules", refresh=refresh)


def correlation(refresh: bool = False) -> CorrelationConfig:
    return load("correlation", refresh=refresh)


def severity(refresh: bool = False) -> SeverityConfig:
    return load("severity", refresh=refresh)


def factors(refresh: bool = False) -> FactorsConfig:
    return load("factors", refresh=refresh)


def scenarios(refresh: bool = False) -> ScenariosConfig:
    return load("scenarios", refresh=refresh)


def retention(refresh: bool = False) -> RetentionConfig:
    return load("retention", refresh=refresh)


def privacy(refresh: bool = False) -> PrivacyConfig:
    return load("privacy", refresh=refresh)


def roles(refresh: bool = False) -> RolesConfig:
    return load("roles", refresh=refresh)


def reload_all() -> dict[str, str]:
    """Re-read every configuration file. Backs the admin reload endpoint.

    Validation runs against the new content before the cache is replaced, so a
    bad edit leaves the running configuration intact and reports the error.
    """
    results: dict[str, str] = {}
    for name in _FILES:
        try:
            load(name, refresh=True)
            results[name] = "ok"
        except ConfigError as error:
            results[name] = f"error: {error}"
    return results


def clear_cache() -> None:
    with _LOCK:
        _CACHE.clear()


def load_all(*, profile: str | None = None) -> dict[str, Any]:
    """Validate the whole configuration tree. Call once during service startup.

    Failing here is intentional: a service that cannot trust its thresholds
    should not start and begin emitting incidents based on defaults.
    """
    bundle: dict[str, Any] = {name: load(name, refresh=True) for name in _FILES}
    bundle["profile"] = load_profile(profile, refresh=True)
    return bundle
