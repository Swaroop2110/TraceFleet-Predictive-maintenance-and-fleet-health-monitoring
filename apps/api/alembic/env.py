"""Alembic environment.

The database URL comes from the environment, never from ``alembic.ini``, so the
same migration set runs unchanged in Compose, CI, and against a managed instance.

Feature-owned migrations use Alembic *branch labels* (see the build rules): a
feature's schema lives on its own branch so removing the feature does not break
the head of another. ``version_table`` is shared, and branches merge at the head.
"""

from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Run from the repository root so `apps.*` and `packages.*` resolve.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.app.db.models import Base  # noqa: E402

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


from packages.common.dsn import owner_sqlalchemy_url  # noqa: E402


def database_url() -> str:
    """Migrations run as the table owner, not as the restricted runtime role.

    DDL and the RLS policies themselves require ownership, and the seed migration
    writes rows for every tenant.
    """
    return owner_sqlalchemy_url()


def run_migrations_offline() -> None:
    context.configure(
        url=database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = database_url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            # compare_type catches a widened column that autogenerate would
            # otherwise miss; include_schemas stays off because everything lives
            # in the default schema.
            compare_type=True,
            compare_server_default=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
