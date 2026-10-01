"""Seed roles from config/roles.yaml and the local demo tenant.

Revision ID: 0002_seed_reference
Revises: 0001_baseline
Create Date: 2026-09-30

Roles are seeded from ``config/roles.yaml`` so the catalogue in the database
cannot drift from the file the API authorises against.

Demo accounts are created only when ``TRACEFLEET_SEED_DEMO_USERS`` is truthy
(the default, for a fresh clone). Their password comes from
``DEMO_USER_PASSWORD``. The API refuses to start with
``TRACEFLEET_ENV=production`` while any default-password account exists, so a
convenient local demo cannot quietly become a production foothold.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import sqlalchemy as sa
from alembic import op

ROOT = Path(__file__).resolve().parents[4]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.common import config as cfg  # noqa: E402
from packages.common.passwords import hash_password  # noqa: E402

revision = "0002_seed_reference"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT_ID", "tenant_default")
DEFAULT_FLEET = os.getenv("DEFAULT_FLEET_ID", "fleet_default")

#: username -> role. Deliberately one account per role so the RBAC and privacy
#: matrices can be exercised end to end without hand-building users.
DEMO_USERS = {
    "admin": "ADMIN",
    "manager": "FLEET_MANAGER",
    "analyst": "ANALYST",
    "viewer": "VIEWER",
}


def _seed_demo_users() -> bool:
    return os.getenv("TRACEFLEET_SEED_DEMO_USERS", "true").lower() in {"1", "true", "yes"}


def upgrade() -> None:
    connection = op.get_bind()

    # -- roles from the config file --------------------------------------
    roles_config = cfg.roles()
    for name, role in roles_config.roles.items():
        connection.execute(
            sa.text(
                """
                INSERT INTO role (role_name, description, permissions)
                VALUES (:name, :description, CAST(:permissions AS jsonb))
                ON CONFLICT (role_name) DO UPDATE
                    SET description = EXCLUDED.description,
                        permissions = EXCLUDED.permissions
                """
            ),
            {
                "name": name,
                "description": role.description,
                # Resolved through inheritance so the row shows the effective
                # grant rather than only the directly-declared permissions.
                "permissions": json.dumps(sorted(roles_config.permissions_for(name))),
            },
        )

    # -- default tenant and fleet ---------------------------------------
    # The validator falls back to this tenant for telemetry from a vehicle that
    # has not been seeded, so the row has to exist or those events would violate
    # a foreign key.
    connection.execute(
        sa.text(
            """
            INSERT INTO tenant (tenant_id, name, region, active, settings)
            VALUES (:tenant_id, 'Default Tenant', 'local', true, CAST('{}' AS jsonb))
            ON CONFLICT (tenant_id) DO NOTHING
            """
        ),
        {"tenant_id": DEFAULT_TENANT},
    )
    connection.execute(
        sa.text(
            """
            INSERT INTO fleet (fleet_id, tenant_id, name, region, metadata_json)
            VALUES (:fleet_id, :tenant_id, 'Default Fleet', 'local', CAST('{}' AS jsonb))
            ON CONFLICT (fleet_id) DO NOTHING
            """
        ),
        {"fleet_id": DEFAULT_FLEET, "tenant_id": DEFAULT_TENANT},
    )

    if not _seed_demo_users():
        return

    password = os.getenv("DEMO_USER_PASSWORD", "tracefleet-demo")
    digest = hash_password(password)
    for username, role_name in DEMO_USERS.items():
        user_id = f"user_{username}"
        connection.execute(
            sa.text(
                """
                INSERT INTO app_user (user_id, tenant_id, username, email, password_hash, display_name, active)
                VALUES (:user_id, :tenant_id, :username, :email, :password_hash, :display_name, true)
                ON CONFLICT (tenant_id, username) DO NOTHING
                """
            ),
            {
                "user_id": user_id,
                "tenant_id": DEFAULT_TENANT,
                "username": username,
                "email": f"{username}@tracefleet.local",
                "password_hash": digest,
                "display_name": username.title(),
            },
        )
        connection.execute(
            sa.text(
                """
                INSERT INTO user_role (user_id, role_name, granted_by)
                VALUES (:user_id, :role_name, 'migration')
                ON CONFLICT (user_id, role_name) DO NOTHING
                """
            ),
            {"user_id": user_id, "role_name": role_name},
        )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        sa.text("DELETE FROM user_role WHERE granted_by = 'migration'")
    )
    connection.execute(
        sa.text("DELETE FROM app_user WHERE user_id = ANY(:ids)"),
        {"ids": [f"user_{name}" for name in DEMO_USERS]},
    )
    connection.execute(sa.text("DELETE FROM fleet WHERE fleet_id = :fleet_id"), {"fleet_id": DEFAULT_FLEET})
    connection.execute(sa.text("DELETE FROM tenant WHERE tenant_id = :tenant_id"), {"tenant_id": DEFAULT_TENANT})
    connection.execute(sa.text("DELETE FROM role"))
