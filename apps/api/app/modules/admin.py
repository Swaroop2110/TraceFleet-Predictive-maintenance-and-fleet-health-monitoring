"""Administration endpoints (feature ``admin_ui``): users, roles, config, erasure.

Every write here is audited and permission-gated separately — ``admin:users`` does
not imply ``admin:config``, so an operator who manages accounts cannot silently
change detection thresholds.

Configuration edits are validated before they are written. Persisting a malformed
threshold file would leave the processor running on the last good copy while the API
reported the broken one, so validation happens against a candidate parse first.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import Conflict, NotFound, TraceFleetError
from apps.api.app.core.security import Principal, describe_authorization, require
from apps.api.app.db.deps import get_tenant_db
from apps.api.app.db.models import AppUser, AuditLog, ErasureRequest, Role, UserRole, Vehicle
from packages.common import config as cfg
from packages.common import logging as tf_logging
from packages.common.ids import short_id
from packages.common.passwords import hash_password
from packages.common.timeutil import now

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS = ("admin_ui",)
router = APIRouter(prefix="/admin", tags=["admin"])


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------
class UserCreate(BaseModel):
    username: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._-]+$")
    password: str = Field(min_length=12, max_length=256)
    roles: list[str] = Field(min_length=1)
    email: str | None = Field(default=None, max_length=255)
    display_name: str | None = Field(default=None, max_length=255)


class UserUpdate(BaseModel):
    roles: list[str] | None = None
    active: bool | None = None
    display_name: str | None = Field(default=None, max_length=255)
    password: str | None = Field(default=None, min_length=12, max_length=256)


def _serialize_user(user: AppUser, roles: list[str]) -> dict[str, Any]:
    return {
        "user_id": user.user_id,
        "username": user.username,
        "email": user.email,
        "display_name": user.display_name,
        "tenant_id": user.tenant_id,
        "active": user.active,
        "roles": sorted(roles),
        "created_at": user.created_at.isoformat() if user.created_at else None,
        "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
        # The hash is never returned, in any form.
        "has_password": bool(user.password_hash),
    }


def _roles_of(db: Session, user_id: str) -> list[str]:
    return list(db.execute(select(UserRole.role_name).where(UserRole.user_id == user_id)).scalars().all())


def _validate_roles(roles: list[str]) -> list[str]:
    known = set(cfg.roles().roles)
    wanted = [role.upper() for role in roles]
    unknown = sorted(set(wanted) - known)
    if unknown:
        raise TraceFleetError(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            f"Unknown role(s): {', '.join(unknown)}",
            code="unknown_role",
            details={"available": sorted(known)},
        )
    return wanted


@router.get("/users")
def list_users(
    principal: Principal = Depends(require("admin:users")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Users in the caller's tenant."""
    users = (
        db.execute(select(AppUser).where(AppUser.tenant_id == principal.tenant_id).order_by(AppUser.username))
        .scalars()
        .all()
    )
    return {"items": [_serialize_user(user, _roles_of(db, user.user_id)) for user in users]}


@router.post("/users", status_code=status.HTTP_201_CREATED)
def create_user(
    payload: UserCreate,
    request: Request,
    principal: Principal = Depends(require("admin:users")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Create a user in the caller's tenant.

    The tenant is taken from the caller, never from the request body: an admin of
    one tenant must not be able to provision accounts in another.
    """
    roles = _validate_roles(payload.roles)
    existing = db.execute(
        select(AppUser).where(AppUser.tenant_id == principal.tenant_id, AppUser.username == payload.username)
    ).scalar_one_or_none()
    if existing is not None:
        raise Conflict("A user with that username already exists", username=payload.username)

    user = AppUser(
        user_id=f"user_{short_id(12)}",
        tenant_id=principal.tenant_id,
        username=payload.username,
        email=payload.email,
        display_name=payload.display_name or payload.username,
        password_hash=hash_password(payload.password),
        active=True,
    )
    db.add(user)
    db.flush()
    for role in roles:
        db.add(UserRole(user_id=user.user_id, role_name=role, granted_by=principal.user_id))
    db.commit()

    audit_from_request(
        request,
        "user_created",
        "app_user",
        resource_id=user.user_id,
        metadata={"username": user.username, "roles": roles},
    )
    return _serialize_user(user, roles)


@router.put("/users/{user_id}")
def update_user(
    user_id: str,
    payload: UserUpdate,
    request: Request,
    principal: Principal = Depends(require("admin:users")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Update a user's roles, status, name, or password."""
    user = db.execute(
        select(AppUser).where(AppUser.user_id == user_id, AppUser.tenant_id == principal.tenant_id)
    ).scalar_one_or_none()
    if user is None:
        raise NotFound("User", user_id)

    changes: dict[str, Any] = {}

    if payload.active is not None:
        if user.user_id == principal.user_id and not payload.active:
            # Refusing self-deactivation avoids the case where the last admin locks
            # everyone out of the tenant.
            raise Conflict("You cannot deactivate your own account")
        user.active = payload.active
        changes["active"] = payload.active

    if payload.display_name is not None:
        user.display_name = payload.display_name
        changes["display_name"] = payload.display_name

    if payload.password is not None:
        user.password_hash = hash_password(payload.password)
        changes["password"] = "rotated"

    if payload.roles is not None:
        roles = _validate_roles(payload.roles)
        db.execute(UserRole.__table__.delete().where(UserRole.user_id == user.user_id))
        for role in roles:
            db.add(UserRole(user_id=user.user_id, role_name=role, granted_by=principal.user_id))
        changes["roles"] = roles

    db.commit()
    audit_from_request(request, "user_updated", "app_user", resource_id=user_id, metadata={"changes": changes})
    return _serialize_user(user, _roles_of(db, user.user_id))


@router.delete("/users/{user_id}")
def deactivate_user(
    user_id: str,
    request: Request,
    principal: Principal = Depends(require("admin:users")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Deactivate a user.

    Deactivation, not deletion. Audit rows reference the actor, and deleting the
    account would leave an unresolvable identity in the trail.
    """
    user = db.execute(
        select(AppUser).where(AppUser.user_id == user_id, AppUser.tenant_id == principal.tenant_id)
    ).scalar_one_or_none()
    if user is None:
        raise NotFound("User", user_id)
    if user.user_id == principal.user_id:
        raise Conflict("You cannot deactivate your own account")

    user.active = False
    db.commit()
    audit_from_request(request, "user_deactivated", "app_user", resource_id=user_id)
    return {"user_id": user_id, "active": False}


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------
@router.get("/roles")
def list_roles(
    principal: Principal = Depends(require("admin:roles")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Role catalogue and the effective permission map.

    ``config/roles.yaml`` is authoritative; the table is a queryable mirror. Both are
    returned so a drift between them is visible rather than hidden.
    """
    stored = db.execute(select(Role).order_by(Role.role_name)).scalars().all()
    model = describe_authorization()
    return {
        "authoritative_source": "config/roles.yaml",
        "permissions": model["permissions"],
        "roles": model["roles"],
        "database_mirror": [
            {"role_name": row.role_name, "description": row.description, "permissions": row.permissions or []}
            for row in stored
        ],
    }


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class RulesUpdate(BaseModel):
    """Detection rules payload. Validated against the Pydantic schema before write."""

    version: int = 1
    window_seconds: int = Field(ge=1, le=3600)
    rules: dict[str, Any]
    incident_trigger: dict[str, Any]


@router.get("/config/{name}")
def get_config(
    name: str,
    principal: Principal = Depends(require("admin:config")),
) -> dict[str, Any]:
    """Read one validated configuration file."""
    try:
        if name == "profile":
            return cfg.load_profile().model_dump()
        return cfg.load(name).model_dump()
    except cfg.ConfigError as error:
        raise NotFound("Configuration", name) from error


@router.put("/config/detection_rules")
def update_detection_rules(
    payload: RulesUpdate,
    request: Request,
    principal: Principal = Depends(require("admin:config")),
) -> dict[str, Any]:
    """Replace the detection rules.

    Validated as a candidate first. Writing an invalid file would leave the running
    processor on its last good copy while the API reported the broken one, so the
    write only happens once the parse succeeds.
    """
    import yaml

    candidate = payload.model_dump()
    try:
        parsed = cfg.DetectionRulesConfig.model_validate(candidate)
    except Exception as error:
        raise TraceFleetError(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "Detection rules failed validation",
            code="invalid_configuration",
            details={"error": str(error)[:1000]},
        ) from error

    path = cfg.config_dir() / "detection_rules.yaml"
    previous = path.read_text(encoding="utf-8") if path.exists() else ""
    path.write_text(yaml.safe_dump(candidate, sort_keys=False), encoding="utf-8")
    cfg.load("detection_rules", refresh=True)

    audit_from_request(
        request,
        "config_updated",
        "detection_rules",
        resource_id="detection_rules.yaml",
        metadata={
            "window_seconds": parsed.window_seconds,
            "enabled_rules": sorted(parsed.enabled_rules()),
            "previous_bytes": len(previous),
        },
    )
    LOGGER.info(
        "detection rules updated",
        extra={"actor": principal.user_id, "enabled_rules": sorted(parsed.enabled_rules())},
    )
    return {
        "status": "updated",
        "applies_to": (
            "the API immediately; stream-processor replicas pick it up on restart or at their next configuration reload"
        ),
        "detection_rules": parsed.model_dump(),
    }


@router.post("/config/reload")
def reload_configuration(
    request: Request,
    principal: Principal = Depends(require("admin:config")),
) -> dict[str, Any]:
    """Re-read every configuration file, reporting per-file results.

    Validation runs before the cache is replaced, so a bad edit leaves the running
    configuration intact and is reported instead of applied.
    """
    results = cfg.reload_all()
    failed = {name: detail for name, detail in results.items() if detail != "ok"}
    audit_from_request(
        request,
        "config_reloaded",
        "configuration",
        outcome="FAILURE" if failed else "SUCCESS",
        metadata={"results": results},
    )
    return {"results": results, "failed": failed, "reloaded": not failed}


# ---------------------------------------------------------------------------
# Erasure
# ---------------------------------------------------------------------------
class ErasureCreate(BaseModel):
    subject_type: str = Field(default="vehicle", pattern="^vehicle$")
    subject_id: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=1, max_length=1000)


@router.get("/erasure")
def list_erasure_requests(
    request_status: str | None = Query(default=None, alias="status"),
    principal: Principal = Depends(require("admin:erasure")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Erasure requests for the caller's tenant."""
    query = select(ErasureRequest).where(ErasureRequest.tenant_id == principal.tenant_id)
    if request_status:
        query = query.where(ErasureRequest.status == request_status.upper())
    rows = db.execute(query.order_by(ErasureRequest.created_at.desc())).scalars().all()
    return {
        "items": [
            {
                "id": row.id,
                "subject_type": row.subject_type,
                "subject_id": row.subject_id,
                "status": row.status,
                "reason": row.reason,
                "actor": row.actor,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "processed_at": row.processed_at.isoformat() if row.processed_at else None,
                "verification_report": row.verification_report or {},
            }
            for row in rows
        ]
    }


@router.post("/erasure", status_code=status.HTTP_202_ACCEPTED)
def request_erasure(
    payload: ErasureCreate,
    request: Request,
    principal: Principal = Depends(require("admin:erasure")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Record a right-to-erasure request.

    Accepted, not executed. Erasure spans Postgres, ClickHouse, and Redis, and the
    ClickHouse part is an asynchronous mutation, so it runs as a job
    (``scripts/erasure_job.py``) that writes a verification report back to this row.
    Returning 200 here would claim a deletion that has not happened yet.
    """
    from packages.common import feature_registry

    if not feature_registry.is_enabled("right_to_erasure"):
        raise TraceFleetError(status.HTTP_404_NOT_FOUND, "Right to erasure is not enabled", code="feature_disabled")

    subject = db.execute(
        select(Vehicle).where(Vehicle.tenant_id == principal.tenant_id, Vehicle.vehicle_id == payload.subject_id)
    ).scalar_one_or_none()
    if subject is None:
        raise NotFound("Vehicle", payload.subject_id)

    pending = db.execute(
        select(ErasureRequest).where(
            ErasureRequest.tenant_id == principal.tenant_id,
            ErasureRequest.subject_id == payload.subject_id,
            ErasureRequest.status.in_(("PENDING", "IN_PROGRESS")),
        )
    ).scalar_one_or_none()
    if pending is not None:
        raise Conflict(
            "An erasure request for this subject is already in progress",
            existing_request_id=pending.id,
        )

    record = ErasureRequest(
        id=f"era_{short_id(16)}",
        tenant_id=principal.tenant_id,
        subject_type=payload.subject_type,
        subject_id=payload.subject_id,
        actor=principal.user_id,
        status="PENDING",
        reason=payload.reason,
        created_at=now(),
    )
    db.add(record)
    db.commit()

    audit_from_request(
        request,
        "erasure_requested",
        "erasure_request",
        resource_id=record.id,
        metadata={"subject_type": payload.subject_type, "subject_id": payload.subject_id},
    )
    return {
        "id": record.id,
        "status": "PENDING",
        "subject_type": record.subject_type,
        "subject_id": record.subject_id,
        "next_step": "run `make erasure` (scripts/erasure_job.py) to process pending requests",
    }


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------
@router.get("/audit")
def read_audit_log(
    action: str | None = Query(default=None, max_length=64),
    actor: str | None = Query(default=None, max_length=64),
    resource_id: str | None = Query(default=None, max_length=64),
    limit: int = Query(default=100, ge=1, le=1000),
    principal: Principal = Depends(require("audit:read")),
    db: Session = Depends(get_tenant_db),
) -> dict[str, Any]:
    """Read the audit trail for the caller's tenant.

    Reading the audit log is itself an audited action, which is what stops the trail
    from having a blind spot around who inspected it.
    """
    query = select(AuditLog).where(AuditLog.tenant == principal.tenant_id)
    if action:
        query = query.where(AuditLog.action == action)
    if actor:
        query = query.where(AuditLog.actor == actor)
    if resource_id:
        query = query.where(AuditLog.resource_id == resource_id)

    rows = db.execute(query.order_by(AuditLog.timestamp.desc()).limit(limit)).scalars().all()

    from apps.api.app.core.audit import write_audit

    write_audit(
        "audit_read",
        "audit_log",
        principal.tenant_id,
        principal.user_id,
        {"filters": {"action": action, "actor": actor, "resource_id": resource_id}, "returned": len(rows)},
    )

    return {
        "items": [
            {
                "id": row.id,
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "request_id": row.request_id,
                "actor": row.actor,
                "action": row.action,
                "resource": row.resource,
                "resource_id": row.resource_id,
                "outcome": row.outcome,
                "source_ip": row.source_ip,
                "metadata": row.metadata_json or {},
            }
            for row in rows
        ],
        "append_only": True,
        "note": "audit_log is append-only; UPDATE and DELETE are rejected by the database.",
    }
