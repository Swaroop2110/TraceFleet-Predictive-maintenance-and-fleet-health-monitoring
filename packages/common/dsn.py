"""Database connection strings, in one place.

Four call sites need a Postgres URL (API session, Alembic, the normaliser's
ownership cache, the persister) and they must agree. They also need *different
forms* of it: SQLAlchemy wants a URL with an explicit driver, psycopg2 wants a
libpq URL or keyword arguments.

The driver is always named explicitly. SQLAlchemy 2.0 resolves a bare
``postgresql://`` to psycopg2 while 2.1 resolves it to psycopg3, so relying on
the default makes the connection depend on which minor version happens to be
installed.
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlsplit

#: The driver pinned in requirements.txt.
SQLALCHEMY_DRIVER = "postgresql+psycopg2"


def _raw_url() -> str | None:
    return os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL")


def _raw_owner_url() -> str | None:
    """Owner/migration connection.

    Migrations, the incident persister, and the retention job connect as the table
    owner: they legitimately operate across every tenant, and RLS deliberately
    does not constrain the owner. Falls back to ``POSTGRES_URL`` for a local
    single-role setup.
    """
    return os.getenv("POSTGRES_OWNER_URL") or os.getenv("POSTGRES_MIGRATION_URL") or _raw_url()


def _from_parts() -> str:
    user = os.getenv("POSTGRES_USER", "tracefleet")
    password = os.getenv("POSTGRES_PASSWORD", "tracefleet")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "tracefleet")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _strip_driver(url: str) -> str:
    """Reduce any ``postgresql+driver://`` form to plain ``postgresql://``."""
    scheme, _, remainder = url.partition("://")
    base = scheme.split("+", 1)[0]
    if base in ("postgres", "postgresql"):
        base = "postgresql"
    return f"{base}://{remainder}"


def sqlalchemy_url() -> str:
    """Runtime URL for ``create_engine``, with the driver named.

    This is the API's connection, and it should be the restricted
    ``tracefleet_app`` role so Row-Level Security applies to it.
    """
    plain = _strip_driver(_raw_url() or _from_parts())
    return plain.replace("postgresql://", f"{SQLALCHEMY_DRIVER}://", 1)


def owner_sqlalchemy_url() -> str:
    """Owner URL for Alembic and cross-tenant jobs."""
    plain = _strip_driver(_raw_owner_url() or _from_parts())
    return plain.replace("postgresql://", f"{SQLALCHEMY_DRIVER}://", 1)


def libpq_url(owner: bool = False) -> str:
    """Plain ``postgresql://`` URL for ``psycopg2.connect``."""
    raw = _raw_owner_url() if owner else _raw_url()
    return _strip_driver(raw or _from_parts())


def psycopg2_kwargs(owner: bool = False) -> dict[str, Any]:
    """Keyword arguments for ``psycopg2.connect``.

    Preferred over a URL when the value came from discrete environment variables,
    because it avoids having to percent-encode a password.
    """
    raw = _raw_owner_url() if owner else _raw_url()
    if raw:
        return {"dsn": libpq_url(owner=owner)}
    return {
        "dbname": os.getenv("POSTGRES_DB", "tracefleet"),
        "user": os.getenv("POSTGRES_USER", "tracefleet"),
        "password": os.getenv("POSTGRES_PASSWORD", "tracefleet"),
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
    }


def redact(url: str) -> str:
    """Replace the password in a URL so it is safe to log."""
    parts = urlsplit(url)
    if not parts.password:
        return url
    netloc = parts.netloc.replace(f":{parts.password}@", ":***@")
    return f"{parts.scheme}://{netloc}{parts.path}"


def redacted_url() -> str:
    return redact(sqlalchemy_url())


def clickhouse_settings() -> dict[str, Any]:
    """Connection settings for ``clickhouse_connect.get_client``."""
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "user": os.getenv("CLICKHOUSE_USER", "default"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
        "database": os.getenv("CLICKHOUSE_DB", "tracefleet"),
    }


def redis_url() -> str:
    return os.getenv("REDIS_URL", "redis://localhost:6379/0")
