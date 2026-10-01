"""Request-scoped database dependencies.

``get_tenant_db`` is the one every tenant-scoped route should use. It resolves the
caller, then sets ``app.tenant_id`` on the connection so Row-Level Security applies
for the rest of the transaction. Routes still filter by tenant in their queries;
this is the layer that makes forgetting to do so return nothing rather than
everything.

Kept separate from ``session.py`` to avoid an import cycle: this module needs the
security dependencies, and security must not depend on the database.
"""

from __future__ import annotations

from collections.abc import Iterator

from fastapi import Depends
from sqlalchemy.orm import Session

from apps.api.app.core.security import Principal, get_current_principal
from apps.api.app.db.session import SessionLocal, set_tenant_context


def get_tenant_db(
    principal: Principal = Depends(get_current_principal),
) -> Iterator[Session]:
    """Session bound to the caller's tenant for the life of the transaction."""
    db = SessionLocal()
    try:
        set_tenant_context(db, principal.tenant_id)
        yield db
    finally:
        db.close()


def scoped_session(tenant_id: str) -> Iterator[Session]:
    """Session for an explicitly named tenant.

    Used by the authentication routes, which must read ``app_user`` before a
    principal exists. Binding the context to the *claimed* tenant means a login
    attempt can only ever see that tenant's accounts, so the endpoint cannot be
    used to enumerate users across tenants.
    """
    db = SessionLocal()
    try:
        set_tenant_context(db, tenant_id)
        yield db
    finally:
        db.close()
