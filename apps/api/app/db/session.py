"""Engine, session factory, and the per-request tenant context for RLS.

The important piece here is :func:`get_tenant_db`. Row-Level Security only works
if every request sets ``app.tenant_id`` on its connection, so that is done in a
dependency rather than left to each route to remember. ``SET LOCAL`` scopes the
value to the transaction, which means a pooled connection cannot leak one
tenant's context into the next request.

Schema creation is Alembic's job. ``init_db`` verifies the schema is present and
says so clearly when it is not, instead of quietly creating a divergent one with
``create_all``.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from packages.common import logging as tf_logging
from packages.common.dsn import redacted_url, sqlalchemy_url

LOGGER = tf_logging.get_logger(__name__)


def database_url() -> str:
    return sqlalchemy_url()


POSTGRES_URL = database_url()

engine = create_engine(
    POSTGRES_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
    pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "1800")),
    # Keeps the API responsive under load: a saturated pool should fail fast with
    # a 503 rather than pile up requests waiting on a connection.
    pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "10")),
)

try:
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    SQLAlchemyInstrumentor().instrument(engine=engine)
except Exception as error:  # pragma: no cover
    LOGGER.debug("sqlalchemy instrumentation unavailable", extra={"error": str(error)})

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, expire_on_commit=False)

#: Tables the baseline migration must have created, checked at startup.
_REQUIRED_TABLES = (
    "tenant",
    "fleet",
    "vehicle",
    "app_user",
    "role",
    "user_role",
    "incident",
    "incident_evidence",
    "incident_factor",
    "incident_state_history",
    "audit_log",
    "ai_run",
    "erasure_request",
    "ground_truth_incident",
)


class SchemaNotReady(RuntimeError):
    """Raised when the database has not been migrated."""


def init_db() -> dict[str, Any]:
    """Verify the schema exists. Returns a short report for the health endpoint."""
    inspector = inspect(engine)
    present = set(inspector.get_table_names())
    missing = [table for table in _REQUIRED_TABLES if table not in present]
    if missing:
        raise SchemaNotReady(
            f"database is not migrated; missing tables: {missing}. Run `make migrate` (alembic upgrade head)."
        )
    revision = current_revision()
    rls = rls_status()
    LOGGER.info(
        "schema verified",
        extra={
            "tables": len(present),
            "alembic_revision": revision,
            "database": redacted_url(),
            "rls_enforced": rls.get("enforced"),
        },
    )
    if not rls.get("enforced"):
        # Loud, because a deployment that believes it has tenant isolation and
        # does not is worse than one that knows it has none.
        LOGGER.warning("row-level security is not enforced for this connection", extra=rls)
    return {"tables": len(present), "alembic_revision": revision, "rls": rls}


def rls_status() -> dict[str, Any]:
    """Report whether this connection is actually subject to RLS.

    "RLS is enabled" is a claim about the schema; this checks the thing that
    matters, which is whether *our* role is constrained by it. A superuser, a
    BYPASSRLS role, or the table owner sees every tenant no matter what the
    policies say, so the API logs a warning rather than letting the deployment
    believe it is isolated when it is not.
    """
    try:
        with engine.connect() as connection:
            row = connection.execute(text("SELECT * FROM tracefleet_rls_status()")).mappings().first()
            if row is None:
                return {"enforced": False, "reason": "role not found"}
            bypasses = bool(row["is_superuser"] or row["bypasses_rls"] or row["owns_tables"])
            status: dict[str, Any] = {
                "role": row["role_name"],
                "is_superuser": bool(row["is_superuser"]),
                "bypasses_rls": bool(row["bypasses_rls"]),
                "owns_tables": bool(row["owns_tables"]),
                "enforced": not bypasses,
            }
            if bypasses:
                status["reason"] = (
                    "connection role bypasses row-level security; set POSTGRES_URL to the "
                    "tracefleet_app role for tenant isolation to be enforced by the database"
                )
            return status
    except Exception as error:
        return {"enforced": False, "reason": f"status unavailable: {error}"}


def current_revision() -> str | None:
    try:
        with engine.connect() as connection:
            row = connection.execute(text("SELECT version_num FROM alembic_version LIMIT 1")).fetchone()
            return row[0] if row else None
    except Exception:
        return None


def get_db() -> Iterator[Session]:
    """Session with no tenant context. For unauthenticated and admin paths."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def set_tenant_context(db: Session, tenant_id: str | None) -> None:
    """Bind ``app.tenant_id`` for the current transaction.

    ``SET LOCAL`` rather than ``SET``: the value must die with the transaction so
    a recycled pool connection never carries one tenant's context into another
    request. Parameter binding is used because the tenant id reaches here from a
    JWT claim, and string interpolation into SQL would be an injection point.
    """
    if not tenant_id:
        return
    db.info["tenant_id"] = tenant_id
    db.execute(text("SELECT set_config('app.tenant_id', :tenant_id, true)"), {"tenant_id": tenant_id})


@contextmanager
def tenant_session(tenant_id: str | None) -> Iterator[Session]:
    """Session scoped to one tenant. Used by jobs and scripts."""
    db = SessionLocal()
    try:
        set_tenant_context(db, tenant_id)
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def pool_status() -> dict[str, Any]:
    """Connection-pool utilisation, exported on the ops endpoint."""
    pool = engine.pool
    try:
        return {
            "size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
        }
    except Exception:  # pragma: no cover
        return {}


@event.listens_for(engine, "connect")
def _set_session_defaults(dbapi_connection: Any, _record: Any) -> None:
    """Apply per-connection defaults once, at checkout from the driver."""
    try:
        with dbapi_connection.cursor() as cursor:
            # Everything in the platform is UTC. Pinning it here removes any
            # dependence on the server's or container's local timezone.
            cursor.execute("SET TIME ZONE 'UTC'")
            # A runaway analytics query should not hold a connection forever.
            cursor.execute(f"SET statement_timeout = '{os.getenv('DB_STATEMENT_TIMEOUT_MS', '30000')}'")
            cursor.execute("SET idle_in_transaction_session_timeout = '60000'")
    except Exception as error:  # pragma: no cover - SQLite in unit tests
        LOGGER.debug("session defaults not applied", extra={"error": str(error)})


@event.listens_for(Session, "after_begin")
def _restore_tenant_context(session: Session, _transaction: Any, connection: Any) -> None:
    """Restore SET LOCAL when a request begins another transaction after commit."""
    tenant_id = session.info.get("tenant_id")
    if tenant_id and connection.dialect.name == "postgresql":
        connection.execute(text("SELECT set_config('app.tenant_id', :tenant_id, true)"), {"tenant_id": tenant_id})
