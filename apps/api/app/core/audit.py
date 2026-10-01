"""Audit logging for sensitive reads, state changes, and AI actions.

Audit rows are written on their own short-lived session, separate from the
request's transaction. That is deliberate: if a request fails and rolls back, the
fact that someone *attempted* to read an incident or generate a summary must still
be recorded. An audit trail that disappears with the transaction it describes is
not much of a trail.

Rows are append-only at the database level (trigger plus revoked grants), so this
module has no update or delete path by construction.
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import Request

from apps.api.app.db.models import AuditLog
from apps.api.app.db.session import SessionLocal, set_tenant_context
from packages.common import logging as tf_logging

LOGGER = tf_logging.get_logger(__name__)
AUDIT_LOGGER = tf_logging.audit_logger()

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT_ID", "tenant_default")

#: Actions that must always be recorded, from PRD 12: authentication, sensitive
#: reads, lifecycle changes, configuration changes, AI runs, and erasure.
SENSITIVE_READS = frozenset(
    {
        "incident_read",
        "incident_list",
        "timeline_read",
        "evidence_read",
        "raw_telemetry_read",
        "vehicle_state_read",
        "export_generated",
        "ai_summary_generated",
        "audit_read",
        "location_full_precision_read",
    }
)


def write_audit(
    action: str,
    resource: str,
    tenant: str | None,
    actor: str,
    metadata: dict[str, Any] | None = None,
    request_id: str | None = None,
    *,
    resource_id: str | None = None,
    outcome: str = "SUCCESS",
    source_ip: str | None = None,
) -> AuditLog | None:
    """Record one audit event.

    Returns None when the write fails. A failed audit write is logged loudly but
    does not raise: taking the API down because the audit table is unavailable
    would turn an observability problem into an outage. The parallel write to the
    audit log stream is why the record is not lost entirely.
    """
    # Every audit row carries a tenant. The row-level-security policy on
    # audit_log compares `tenant` to the session's tenant context, so a NULL
    # tenant is rejected outright — and a failed login, which has no authenticated
    # principal yet, is exactly the event that must not go unrecorded. It is
    # attributed to the tenant the caller *claimed*.
    resolved_tenant = tenant or DEFAULT_TENANT
    entry = AuditLog(
        request_id=request_id,
        actor=actor,
        tenant=resolved_tenant,
        resource=resource,
        resource_id=resource_id,
        action=action,
        outcome=outcome,
        source_ip=source_ip,
        metadata_json=metadata or {},
    )

    # Emitted to the log stream first, so the record exists even if the database
    # write fails. Two independent copies is the point.
    AUDIT_LOGGER.info(
        "audit",
        extra={
            "audit_action": action,
            "audit_resource": resource,
            "audit_resource_id": resource_id,
            "actor": actor,
            "tenant": tenant,
            "outcome": outcome,
            "request_id": request_id,
            "source_ip": source_ip,
            "audit_metadata": metadata or {},
        },
    )

    db = SessionLocal()
    try:
        # This session is separate from the request's, so it needs its own tenant
        # context for the RLS write check to pass.
        set_tenant_context(db, resolved_tenant)
        db.add(entry)
        db.commit()
        # No refresh: `SET LOCAL` expires with the transaction, so the reload
        # SELECT would run without a tenant context and RLS would hide the row we
        # just wrote. The session is configured with expire_on_commit=False, so
        # the in-memory object stays readable.
        return entry
    except Exception as error:
        db.rollback()
        LOGGER.error(
            "audit write failed",
            extra={"action": action, "resource": resource, "error": str(error)[:400]},
        )
        return None
    finally:
        db.close()


def audit_from_request(
    request: Request,
    action: str,
    resource: str,
    *,
    resource_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    outcome: str = "SUCCESS",
    actor: str | None = None,
    tenant: str | None = None,
) -> AuditLog | None:
    """Write an audit row, taking actor, tenant, id, and address from the request.

    Preferred over :func:`write_audit` in route handlers: pulling the context from
    one place means a route cannot accidentally record the wrong actor.

    ``actor`` and ``tenant`` can be supplied for events that happen *before* a
    principal exists — a login, successful or not, has no authenticated identity on
    the request yet but is precisely the event that needs attributing.
    """
    principal = getattr(request.state, "principal", None)
    return write_audit(
        action=action,
        resource=resource,
        resource_id=resource_id,
        tenant=tenant or getattr(principal, "tenant_id", None),
        actor=actor or getattr(principal, "user_id", "unknown"),
        metadata=metadata,
        request_id=getattr(request.state, "request_id", None),
        outcome=outcome,
        source_ip=getattr(request.state, "client_ip", None),
    )
