"""SQLAlchemy models for the transactional store.

Postgres holds the entities that need ACID guarantees and referential integrity:
tenancy, identity, and the incident record an operator acts on. Raw telemetry
lives in ClickHouse — see ADR-001 for why the split exists.

Conventions applied throughout:

* Every tenant-scoped table carries ``tenant_id``, which is both the application
  filter and the Row-Level Security predicate.
* ``CHECK`` constraints encode the enums. The state machine is enforced in the
  API, but the database refuses an impossible value even if a bug or a manual
  ``UPDATE`` tries to write one.
* Dependent rows cascade on delete at the *database* level, not only through the
  ORM. The retention and erasure jobs run raw SQL and would otherwise orphan
  evidence or fail on a foreign key.
"""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()

#: JSONB on Postgres, plain JSON elsewhere so the unit tests can run on SQLite.
JsonType = JSON().with_variant(JSONB, "postgresql")

INCIDENT_STATUSES = ("DETECTED", "RECONSTRUCTING", "OPEN", "ACKNOWLEDGED", "CLOSED", "INVALIDATED")
SEVERITIES = ("INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL")
ERASURE_STATUSES = ("PENDING", "IN_PROGRESS", "COMPLETED", "FAILED")


def _in_list(column: str, values: tuple[str, ...]) -> str:
    rendered = ", ".join(f"'{value}'" for value in values)
    return f"{column} IN ({rendered})"


# ---------------------------------------------------------------------------
# Tenancy and fleet structure
# ---------------------------------------------------------------------------
class Tenant(Base):
    __tablename__ = "tenant"

    tenant_id = Column(String(64), primary_key=True)
    name = Column(String(255), nullable=False)
    region = Column(String(64))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    settings = Column(JsonType, default=dict)

    fleets = relationship("Fleet", back_populates="tenant", cascade="all, delete-orphan")


class Fleet(Base):
    __tablename__ = "fleet"

    fleet_id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    region = Column(String(64))
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    metadata_json = Column(JsonType, default=dict)

    tenant = relationship("Tenant", back_populates="fleets")
    vehicles = relationship("Vehicle", back_populates="fleet", cascade="all, delete-orphan")


class Vehicle(Base):
    __tablename__ = "vehicle"

    vehicle_id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False)
    fleet_id = Column(String(64), ForeignKey("fleet.fleet_id", ondelete="CASCADE"), nullable=False)
    # VIN is personal-adjacent data: it identifies a specific asset and often its
    # keeper. Exposure is gated by config/privacy.yaml, and the erasure job
    # pseudonymises it rather than deleting the row, so incident history survives.
    vin = Column(String(17), nullable=False)
    oem = Column(String(64))
    model = Column(String(128))
    model_year = Column(Integer)
    road_class = Column(String(32))
    registered_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    metadata_json = Column(JsonType, default=dict)

    fleet = relationship("Fleet", back_populates="vehicles")

    __table_args__ = (
        # The ownership cache in the normaliser reads (tenant_id, fleet_id) in bulk.
        Index("ix_vehicle_tenant_fleet", "tenant_id", "fleet_id"),
        Index("ix_vehicle_vin", "vin"),
    )


class Driver(Base):
    """Pseudonymous driver association. Feature-gated by ``driver_identity``.

    Deliberately holds no name, licence number, or contact detail. The platform
    associates behaviour with a rotating pseudonym so an incident can be linked
    across trips without the system becoming a driver-surveillance tool.
    """

    __tablename__ = "driver"

    driver_id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False, index=True)
    pseudonym = Column(String(64), nullable=False, unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    active = Column(Boolean, nullable=False, default=True)


# ---------------------------------------------------------------------------
# Identity and access
# ---------------------------------------------------------------------------
class AppUser(Base):
    __tablename__ = "app_user"

    user_id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), ForeignKey("tenant.tenant_id", ondelete="CASCADE"), nullable=False, index=True)
    username = Column(String(128), nullable=False)
    email = Column(String(255))
    # Argon2/PBKDF2 digest, never a reversible value. Null means the account
    # authenticates through an external OIDC provider.
    password_hash = Column(String(255))
    display_name = Column(String(255))
    active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_login_at = Column(DateTime(timezone=True))

    roles = relationship("UserRole", back_populates="user", cascade="all, delete-orphan", lazy="selectin")

    __table_args__ = (
        # Usernames are unique per tenant, not globally: two customers may both
        # have an "admin".
        UniqueConstraint("tenant_id", "username", name="uq_app_user_tenant_username"),
    )


class Role(Base):
    """Role catalogue mirroring ``config/roles.yaml``.

    The YAML file remains the source of truth for permissions; this table exists
    so role assignments can be foreign-keyed and audited.
    """

    __tablename__ = "role"

    role_name = Column(String(64), primary_key=True)
    description = Column(Text)
    permissions = Column(JsonType, default=list)


class UserRole(Base):
    __tablename__ = "user_role"

    user_id = Column(String(64), ForeignKey("app_user.user_id", ondelete="CASCADE"), primary_key=True)
    role_name = Column(String(64), ForeignKey("role.role_name", ondelete="CASCADE"), primary_key=True)
    granted_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    granted_by = Column(String(64))

    user = relationship("AppUser", back_populates="roles")


# ---------------------------------------------------------------------------
# Incidents
# ---------------------------------------------------------------------------
class Incident(Base):
    __tablename__ = "incident"

    incident_id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), nullable=False, index=True)
    fleet_id = Column(String(64), nullable=False, default="fleet_default")
    vehicle_id = Column(String(64), nullable=False, index=True)
    incident_type = Column(String(64), nullable=False)
    severity = Column(String(16), nullable=False)
    status = Column(String(32), nullable=False)
    # Three distinct times. start_ts/end_ts are event time (the vehicle's clock),
    # detected_at is when the platform concluded an incident existed.
    start_ts = Column(DateTime(timezone=True), nullable=False)
    end_ts = Column(DateTime(timezone=True))
    detected_at = Column(DateTime(timezone=True), nullable=False)
    # sha256(tenant|vehicle|floor(start_ts/window)|type). The unique constraint is
    # what makes an at-least-once Kafka replay safe to persist.
    idempotency_key = Column(String(64), unique=True, nullable=False)
    confidence = Column(Numeric(5, 4))
    metadata_json = Column(JsonType, default=dict)
    acknowledged_at = Column(DateTime(timezone=True))
    acknowledged_by = Column(String(64))
    closed_at = Column(DateTime(timezone=True))
    closed_by = Column(String(64))

    factors = relationship("IncidentFactor", back_populates="incident", lazy="selectin", cascade="all, delete-orphan")
    evidence = relationship(
        "IncidentEvidence", back_populates="incident", lazy="selectin", cascade="all, delete-orphan"
    )
    state_history = relationship("IncidentStateHistory", back_populates="incident", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(_in_list("status", INCIDENT_STATUSES), name="ck_incident_status"),
        CheckConstraint(_in_list("severity", SEVERITIES), name="ck_incident_severity"),
        CheckConstraint("confidence IS NULL OR (confidence >= 0 AND confidence <= 1)", name="ck_incident_confidence"),
        CheckConstraint("end_ts IS NULL OR end_ts >= start_ts", name="ck_incident_time_order"),
        Index("ix_incident_vehicle_start_ts", "vehicle_id", "start_ts"),
        Index("ix_incident_status_severity_start", "status", "severity", "start_ts"),
        # Keyset pagination on the incident list reads this index directly.
        Index("ix_incident_tenant_start_desc", "tenant_id", "start_ts", "incident_id"),
        Index("ix_incident_tenant_fleet", "tenant_id", "fleet_id"),
    )


class IncidentEvidence(Base):
    """One signal, addressable by id so a narrative can cite it."""

    __tablename__ = "incident_evidence"

    evidence_id = Column(String(64), primary_key=True)
    incident_id = Column(String(64), ForeignKey("incident.incident_id", ondelete="CASCADE"), nullable=False, index=True)
    event_key = Column(String(64), nullable=False)
    event_ts = Column(DateTime(timezone=True), nullable=False)
    signal = Column(String(64), nullable=False)
    value = Column(Float, nullable=False)
    threshold = Column(Float)
    weight = Column(Float)
    phase = Column(String(32))
    flags = Column(JsonType, default=dict)

    incident = relationship("Incident", back_populates="evidence")

    __table_args__ = (
        # An incident cannot hold the same signal from the same event twice; this
        # is the constraint that makes re-reconstruction idempotent.
        UniqueConstraint("incident_id", "event_key", "signal", name="uq_evidence_incident_event_signal"),
        Index("ix_incident_evidence_event_key", "event_key"),
    )


class IncidentFactor(Base):
    __tablename__ = "incident_factor"

    factor_id = Column(String(64), primary_key=True)
    incident_id = Column(String(64), ForeignKey("incident.incident_id", ondelete="CASCADE"), nullable=False, index=True)
    factor_type = Column(String(64), nullable=False)
    score = Column(Numeric(5, 4), nullable=False)
    confidence = Column(Numeric(5, 4), nullable=False)
    evidence_ids = Column(JsonType, default=list)
    components = Column(JsonType, default=dict)

    incident = relationship("Incident", back_populates="factors")

    __table_args__ = (
        UniqueConstraint("incident_id", "factor_type", name="uq_factor_incident_type"),
        CheckConstraint("score >= 0 AND score <= 1", name="ck_factor_score_range"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_factor_confidence_range"),
    )


class IncidentStateHistory(Base):
    __tablename__ = "incident_state_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    incident_id = Column(String(64), ForeignKey("incident.incident_id", ondelete="CASCADE"), nullable=False, index=True)
    old_status = Column(String(32))
    new_status = Column(String(32), nullable=False)
    changed_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    changed_by = Column(String(64))
    reason = Column(Text)

    incident = relationship("Incident", back_populates="state_history")

    __table_args__ = (
        CheckConstraint(_in_list("new_status", INCIDENT_STATUSES), name="ck_state_history_new_status"),
        # Lets the persister use ON CONFLICT DO NOTHING so a replayed envelope
        # does not append a duplicate transition.
        UniqueConstraint("incident_id", "new_status", "changed_at", name="uq_state_history_transition"),
    )


# ---------------------------------------------------------------------------
# Audit, AI, compliance
# ---------------------------------------------------------------------------
class AuditLog(Base):
    """Append-only record of sensitive reads and every state change.

    Append-only is enforced in the database by a trigger and by revoking
    UPDATE/DELETE from the application role (see the audit-hardening migration).
    An application-level convention would not survive a compromised API.
    """

    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True)
    request_id = Column(String(64))
    actor = Column(String(64))
    tenant = Column(String(64), index=True)
    resource = Column(String(128))
    resource_id = Column(String(64))
    action = Column(String(64), nullable=False)
    outcome = Column(String(16), nullable=False, default="SUCCESS")
    source_ip = Column(String(64))
    metadata_json = Column(JsonType, default=dict)

    __table_args__ = (
        Index("ix_audit_log_tenant_timestamp", "tenant", "timestamp"),
        Index("ix_audit_log_actor_timestamp", "actor", "timestamp"),
    )


class AIRun(Base):
    """One AI summary generation, reconstructable after the fact.

    Storing both hashes means an auditor can prove which prompt ran against which
    evidence without the run having to be repeatable against a non-deterministic
    model. Identical hashes also let a repeat request return the cached run.
    """

    __tablename__ = "ai_run"

    run_id = Column(String(64), primary_key=True)
    incident_id = Column(String(64), ForeignKey("incident.incident_id", ondelete="CASCADE"), nullable=False, index=True)
    tenant_id = Column(String(64), nullable=False, index=True)
    actor = Column(String(64), nullable=False)
    provider = Column(String(64), nullable=False, default="mock")
    model = Column(String(128), nullable=False)
    prompt = Column(Text, nullable=False)
    prompt_hash = Column(String(64), nullable=False)
    evidence_hash = Column(String(64), nullable=False)
    evidence_package = Column(JsonType, default=dict)
    output = Column(Text, nullable=False)
    cited_evidence_ids = Column(JsonType, default=list)
    validation_result = Column(JsonType, default=dict)
    latency_ms = Column(Integer)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_ai_run_incident_created", "incident_id", "created_at"),
        Index("ix_ai_run_cache", "incident_id", "prompt_hash", "evidence_hash"),
    )


class ErasureRequest(Base):
    __tablename__ = "erasure_request"

    id = Column(String(64), primary_key=True)
    tenant_id = Column(String(64), nullable=False, index=True)
    subject_type = Column(String(32), nullable=False, default="vehicle")
    subject_id = Column(String(64), nullable=False)
    actor = Column(String(64), nullable=False)
    status = Column(String(32), nullable=False, default="PENDING")
    reason = Column(Text)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    processed_at = Column(DateTime(timezone=True))
    # Per-store outcome, so "erased" is a verifiable claim rather than an assertion.
    verification_report = Column(JsonType, default=dict)
    metadata_json = Column(JsonType, default=dict)

    __table_args__ = (
        CheckConstraint(_in_list("status", ERASURE_STATUSES), name="ck_erasure_status"),
        Index("ix_erasure_tenant_status", "tenant_id", "status"),
    )


# ---------------------------------------------------------------------------
# Evaluation and optional features
# ---------------------------------------------------------------------------
class GroundTruthIncident(Base):
    """Hidden labels written by the simulator, for evaluation only.

    This never travels the Kafka path and nothing in the detection chain reads
    it. If it did, the accuracy figure in docs/BENCHMARKS.md would be measuring
    the platform against itself.
    """

    __tablename__ = "ground_truth_incident"

    id = Column(String(64), primary_key=True)
    run_id = Column(String(64), nullable=False, index=True)
    scenario_id = Column(String(64), nullable=False)
    tenant_id = Column(String(64), nullable=False)
    vehicle_id = Column(String(64), nullable=False, index=True)
    start_ts = Column(DateTime(timezone=True), nullable=False)
    end_ts = Column(DateTime(timezone=True))
    expected_signals = Column(JsonType, default=list)
    expected_severity = Column(String(16))
    should_create_incident = Column(Boolean, nullable=False, default=False)
    event_keys = Column(JsonType, default=list)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        Index("ix_ground_truth_vehicle_start", "vehicle_id", "start_ts"),
        Index("ix_ground_truth_run_scenario", "run_id", "scenario_id"),
    )


class IncidentEmbedding(Base):
    """Similarity vectors. Feature-gated by ``pgvector_similarity`` (off by default).

    The column is plain JSON here so the schema loads without the pgvector
    extension; the feature's own migration swaps it for ``vector(N)`` and adds an
    index when enabled.
    """

    __tablename__ = "incident_embedding"

    incident_id = Column(String(64), ForeignKey("incident.incident_id", ondelete="CASCADE"), primary_key=True)
    tenant_id = Column(String(64), nullable=False, index=True)
    model = Column(String(128), nullable=False)
    dimensions = Column(Integer, nullable=False)
    embedding = Column(JsonType, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)


#: Tables carrying tenant data, used by the RLS migration and the erasure job.
TENANT_SCOPED_TABLES = (
    "fleet",
    "vehicle",
    "driver",
    "app_user",
    "incident",
    "audit_log",
    "ai_run",
    "erasure_request",
    "ground_truth_incident",
    "incident_embedding",
)
