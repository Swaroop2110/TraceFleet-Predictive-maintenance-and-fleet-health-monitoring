"""Row-Level Security and append-only audit enforcement.

Revision ID: 0003_rls_audit
Revises: 0002_seed_reference
Create Date: 2026-09-30

Two independent controls, both in the database because both must hold even when
the application has a bug.

**Row-Level Security.** Every tenant-scoped table gets a policy comparing its
tenant column to the ``app.tenant_id`` setting the API applies per request. The
application still filters by tenant in its queries; RLS is the backstop for the
query that forgets, turning a cross-tenant leak into an empty result.

Three details make this actually work rather than merely look enabled:

1. *A separate runtime role.* Postgres lets SUPERUSER and BYPASSRLS roles ignore
   every policy, and a table's owner ignores them too unless FORCE is set. An API
   connecting as the database owner therefore has RLS "enabled" and enforcing
   nothing. So the migration creates ``tracefleet_app`` — NOSUPERUSER,
   NOBYPASSRLS, not the owner — and the API connects as that.
2. *Fail closed.* The predicate is ``tenant_id = tracefleet_current_tenant()``
   with no ``IS NULL`` escape hatch. With no tenant context set, the comparison is
   NULL rather than true, so a request that forgot the context reads nothing.
3. *Owner bypass is deliberate, not accidental.* FORCE is intentionally **not**
   set, so the owner role still sees everything. Migrations, the incident
   persister (which legitimately writes across tenants), and the retention job
   connect as the owner. The split is the authorisation boundary, and
   ``tracefleet_rls_status()`` lets the API assert at startup that it is on the
   restricted side of it.

**Append-only audit.** A trigger rejects UPDATE and DELETE on ``audit_log``, and
those grants are revoked from the application role. An audit trail the
application can rewrite is not evidence. Retention still needs to age rows out,
so it goes through ``tracefleet_purge_audit()``, a SECURITY DEFINER function that
disables the trigger for the duration and records its own deletion in the log.
"""

from __future__ import annotations

import os
import re

import sqlalchemy as sa
from alembic import op

revision = "0003_rls_audit"
down_revision = "0002_seed_reference"
branch_labels = None
depends_on = None

APP_ROLE = os.getenv("POSTGRES_APP_ROLE", "tracefleet_app")
APP_PASSWORD = os.getenv("POSTGRES_APP_PASSWORD", "tracefleet_app").replace("'", "''")
if not re.fullmatch(r"[a-z_][a-z0-9_]*", APP_ROLE):
    raise ValueError("POSTGRES_APP_ROLE must be a simple SQL identifier")

#: Tables whose rows belong to exactly one tenant, with the column naming it.
TENANT_TABLES = (
    ("fleet", "tenant_id"),
    ("vehicle", "tenant_id"),
    ("driver", "tenant_id"),
    ("app_user", "tenant_id"),
    ("incident", "tenant_id"),
    ("audit_log", "tenant"),
    ("ai_run", "tenant_id"),
    ("erasure_request", "tenant_id"),
    ("ground_truth_incident", "tenant_id"),
    ("incident_embedding", "tenant_id"),
)

#: Reached only through their parent incident, so they inherit its tenant.
CHILD_TABLES = ("incident_evidence", "incident_factor", "incident_state_history")


def upgrade() -> None:
    connection = op.get_bind()

    # -- runtime role ----------------------------------------------------
    connection.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{APP_ROLE}') THEN
                    CREATE ROLE {APP_ROLE} LOGIN PASSWORD '{APP_PASSWORD}'
                        NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
                ELSE
                    ALTER ROLE {APP_ROLE} LOGIN PASSWORD '{APP_PASSWORD}'
                        NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
                END IF;
            END
            $$;
            """  # nosec B608 # static identifiers, parameter-bound values
        )
    )
    connection.execute(sa.text(f"GRANT USAGE ON SCHEMA public TO {APP_ROLE}"))
    connection.execute(
        sa.text(f"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {APP_ROLE}")  # nosec B608 # static identifiers, parameter-bound values
    )
    connection.execute(sa.text(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE}"))  # nosec B608 # static identifiers, parameter-bound values
    # Tables added by a later feature migration should be reachable without
    # having to remember to re-grant.
    connection.execute(
        sa.text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {APP_ROLE}")  # nosec B608 # static identifiers, parameter-bound values
    )

    # -- tenant context helper -------------------------------------------
    connection.execute(
        sa.text(
            """
            CREATE OR REPLACE FUNCTION tracefleet_current_tenant() RETURNS text
            LANGUAGE sql STABLE AS $$
                SELECT NULLIF(current_setting('app.tenant_id', true), '')
            $$;
            """
        )
    )

    # -- policies --------------------------------------------------------
    for table, column in TENANT_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"""
            CREATE POLICY {table}_tenant_isolation ON {table}
                TO {APP_ROLE}
                USING ({column} = tracefleet_current_tenant())
                WITH CHECK ({column} = tracefleet_current_tenant())
            """  # nosec B608 # static identifiers, parameter-bound values
        )

    for table in CHILD_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        # Joining back to the incident keeps the tenant in one place. Denormalising
        # tenant_id onto evidence would be faster but gives two sources of truth
        # that can disagree.
        op.execute(
            f"""
            CREATE POLICY {table}_tenant_isolation ON {table}
                TO {APP_ROLE}
                USING (
                    EXISTS (
                        SELECT 1 FROM incident i
                        WHERE i.incident_id = {table}.incident_id
                          AND i.tenant_id = tracefleet_current_tenant()
                    )
                )
                WITH CHECK (
                    EXISTS (
                        SELECT 1 FROM incident i
                        WHERE i.incident_id = {table}.incident_id
                          AND i.tenant_id = tracefleet_current_tenant()
                    )
                )
            """  # nosec B608 # static identifiers, parameter-bound values
        )

    # -- startup self-check ----------------------------------------------
    # Lets the API report whether its own connection is actually subject to RLS.
    # "We enabled RLS" is a claim; this makes it checkable at runtime.
    connection.execute(
        sa.text(
            """
            CREATE OR REPLACE FUNCTION tracefleet_rls_status()
            RETURNS TABLE (role_name text, is_superuser boolean, bypasses_rls boolean, owns_tables boolean)
            LANGUAGE sql STABLE AS $$
                SELECT
                    current_user::text,
                    r.rolsuper,
                    r.rolbypassrls,
                    EXISTS (
                        SELECT 1 FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE n.nspname = 'public'
                          AND c.relkind = 'r'
                          AND c.relowner = r.oid
                    )
                FROM pg_roles r
                WHERE r.rolname = current_user
            $$;
            """
        )
    )

    # -- append-only audit_log -------------------------------------------
    connection.execute(
        sa.text(
            """
            CREATE OR REPLACE FUNCTION tracefleet_audit_append_only() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION
                    'audit_log is append-only; % on audit_log is not permitted', TG_OP
                    USING ERRCODE = 'insufficient_privilege';
            END
            $$;
            """
        )
    )
    op.execute(
        """
        CREATE TRIGGER audit_log_no_update
            BEFORE UPDATE ON audit_log
            FOR EACH ROW EXECUTE FUNCTION tracefleet_audit_append_only()
        """
    )
    op.execute(
        """
        CREATE TRIGGER audit_log_no_delete
            BEFORE DELETE ON audit_log
            FOR EACH ROW EXECUTE FUNCTION tracefleet_audit_append_only()
        """
    )
    connection.execute(sa.text(f"REVOKE UPDATE, DELETE ON audit_log FROM {APP_ROLE}"))  # nosec B608 # static identifiers, parameter-bound values

    connection.execute(
        sa.text(
            """
            CREATE OR REPLACE FUNCTION tracefleet_purge_audit(cutoff timestamptz)
            RETURNS bigint
            LANGUAGE plpgsql SECURITY DEFINER AS $$
            DECLARE
                removed bigint;
            BEGIN
                ALTER TABLE audit_log DISABLE TRIGGER audit_log_no_delete;
                DELETE FROM audit_log WHERE timestamp < cutoff;
                GET DIAGNOSTICS removed = ROW_COUNT;
                ALTER TABLE audit_log ENABLE TRIGGER audit_log_no_delete;
                -- The purge logs itself, so aging rows out is never invisible.
                INSERT INTO audit_log (actor, tenant, resource, action, outcome, metadata_json)
                VALUES ('retention-job', NULL, 'audit_log', 'audit_purge', 'SUCCESS',
                        jsonb_build_object('cutoff', cutoff, 'rows_removed', removed));
                RETURN removed;
            END
            $$;
            """
        )
    )


def downgrade() -> None:
    connection = op.get_bind()
    op.execute("DROP TRIGGER IF EXISTS audit_log_no_update ON audit_log")
    op.execute("DROP TRIGGER IF EXISTS audit_log_no_delete ON audit_log")
    connection.execute(sa.text("DROP FUNCTION IF EXISTS tracefleet_purge_audit(timestamptz)"))
    connection.execute(sa.text("DROP FUNCTION IF EXISTS tracefleet_audit_append_only()"))
    connection.execute(sa.text("DROP FUNCTION IF EXISTS tracefleet_rls_status()"))

    for table in CHILD_TABLES:
        op.execute(f"DROP POLICY IF EXISTS {table}_tenant_isolation ON {table}")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
    for table, _ in TENANT_TABLES:
        op.execute(f"DROP POLICY IF EXISTS {table}_tenant_isolation ON {table}")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")

    connection.execute(sa.text("DROP FUNCTION IF EXISTS tracefleet_current_tenant()"))
    connection.execute(
        sa.text(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM {APP_ROLE}")  # nosec B608 # static identifiers, parameter-bound values
    )
