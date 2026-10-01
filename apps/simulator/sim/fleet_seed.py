"""Seed tenants, fleets, and vehicles into Postgres.

``make seed`` must complete 100,000 vehicles in under two minutes, which rules out
row-by-row inserts. This uses ``COPY FROM`` with an in-memory CSV buffer: one
statement, no per-row round trip, no statement parsing per vehicle.

Existing rows are handled by copying into a temporary table and then upserting, so
re-seeding is idempotent and does not have to drop a fleet that already has incident
history pointing at it.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Iterator
from typing import Any

from packages.common import logging as tf_logging
from sim.vehicle_model import CITY_CENTRES, vehicle_metadata

LOGGER = tf_logging.get_logger(__name__)

#: Rows per COPY batch. Large enough to amortise the round trip, small enough that
#: the CSV buffer stays well inside memory at 100K vehicles.
BATCH_SIZE = 20_000

VEHICLE_COLUMNS = (
    "vehicle_id",
    "tenant_id",
    "fleet_id",
    "vin",
    "oem",
    "model",
    "model_year",
    "road_class",
    "active",
)


def seed(
    vehicles: int,
    tenants: int = 3,
    fleets_per_tenant: int = 20,
    *,
    seed_value: int = 42,
    progress_every: int = 50_000,
) -> dict[str, Any]:
    """Seed the registry. Returns a report including elapsed time."""
    import psycopg2

    from packages.common.dsn import psycopg2_kwargs

    started = time.perf_counter()
    connection = psycopg2.connect(**psycopg2_kwargs(owner=True))
    connection.autocommit = False

    try:
        with connection.cursor() as cursor:
            tenant_ids = _seed_tenants(cursor, tenants)
            fleet_ids = _seed_fleets(cursor, tenant_ids, fleets_per_tenant)
            connection.commit()

            inserted = _copy_vehicles(
                cursor,
                vehicles,
                tenant_ids,
                fleet_ids,
                seed_value=seed_value,
                progress_every=progress_every,
            )
            connection.commit()

            # ANALYZE after a bulk load so the planner does not keep using
            # pre-load statistics for the ownership-cache query.
            cursor.execute("ANALYZE vehicle")
            connection.commit()

        elapsed = time.perf_counter() - started
        report = {
            "tenants": len(tenant_ids),
            "fleets": len(fleet_ids),
            "vehicles": inserted,
            "elapsed_seconds": round(elapsed, 2),
            "vehicles_per_second": round(inserted / elapsed, 1) if elapsed > 0 else 0,
        }
        LOGGER.info("fleet seeded", extra=report)
        return report
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _seed_tenants(cursor: Any, count: int) -> list[str]:
    """Create tenants, always including the default the validator falls back to."""
    tenant_ids = ["tenant_default"] + [f"tenant_{index:02d}" for index in range(1, count)]
    for index, tenant_id in enumerate(tenant_ids):
        city = CITY_CENTRES[index % len(CITY_CENTRES)][0]
        cursor.execute(
            """
            INSERT INTO tenant (tenant_id, name, region, active)
            VALUES (%s, %s, %s, true)
            ON CONFLICT (tenant_id) DO UPDATE SET name = EXCLUDED.name, region = EXCLUDED.region
            """,
            (tenant_id, f"Tenant {index}" if index else "Default Tenant", city),
        )
    return tenant_ids


def _seed_fleets(cursor: Any, tenant_ids: list[str], per_tenant: int) -> list[tuple[str, str]]:
    """Create fleets. Returns (fleet_id, tenant_id) pairs."""
    pairs: list[tuple[str, str]] = [("fleet_default", "tenant_default")]
    for tenant_index, tenant_id in enumerate(tenant_ids):
        for fleet_index in range(per_tenant):
            fleet_id = f"fleet_{tenant_index:02d}_{fleet_index:03d}"
            pairs.append((fleet_id, tenant_id))

    for fleet_id, tenant_id in pairs:
        city = CITY_CENTRES[hash(fleet_id) % len(CITY_CENTRES)][0]
        cursor.execute(
            """
            INSERT INTO fleet (fleet_id, tenant_id, name, region)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fleet_id) DO UPDATE SET name = EXCLUDED.name, region = EXCLUDED.region
            """,
            (fleet_id, tenant_id, fleet_id.replace("_", " ").title(), city),
        )
    return pairs


def _copy_vehicles(
    cursor: Any,
    count: int,
    tenant_ids: list[str],
    fleet_ids: list[tuple[str, str]],
    *,
    seed_value: int,
    progress_every: int,
) -> int:
    """Bulk-load vehicles via COPY into a staging table, then upsert."""
    # Session-local staging prevents concurrent seed runs from truncating each other.
    cursor.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS vehicle_staging (
            vehicle_id text, tenant_id text, fleet_id text, vin text,
            oem text, model text, model_year integer, road_class text, active boolean
        )
        """
    )
    cursor.execute("TRUNCATE vehicle_staging")

    total = 0
    for batch_start in range(0, count, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, count)
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        for row in _vehicle_rows(batch_start, batch_end, tenant_ids, fleet_ids, seed_value):
            writer.writerow(row)
        buffer.seek(0)
        cursor.copy_expert(
            f"COPY vehicle_staging ({', '.join(VEHICLE_COLUMNS)}) FROM STDIN WITH CSV",
            buffer,
        )
        total = batch_end
        if progress_every and batch_end % progress_every == 0:
            LOGGER.info("seeding progress", extra={"vehicles": batch_end, "of": count})

    cursor.execute(
        f"""
        INSERT INTO vehicle ({", ".join(VEHICLE_COLUMNS)})
        SELECT {", ".join(VEHICLE_COLUMNS)} FROM vehicle_staging
        ON CONFLICT (vehicle_id) DO UPDATE SET
            tenant_id = EXCLUDED.tenant_id,
            fleet_id  = EXCLUDED.fleet_id,
            vin       = EXCLUDED.vin,
            oem       = EXCLUDED.oem,
            model     = EXCLUDED.model,
            active    = true
        WHERE NOT (COALESCE(vehicle.metadata_json, '{{}}'::jsonb) ? 'erased_at')
        """  # nosec B608 # static identifiers, parameter-bound values
    )
    cursor.execute("DROP TABLE IF EXISTS vehicle_staging")
    return total


def _vehicle_rows(
    start: int,
    end: int,
    tenant_ids: list[str],
    fleet_ids: list[tuple[str, str]],
    seed_value: int,
) -> Iterator[tuple[Any, ...]]:
    """Generate vehicle rows, distributing them across fleets."""
    # Skips fleet_default so the seeded population is spread over real fleets while
    # leaving the fallback fleet available for unregistered telemetry.
    assignable = [pair for pair in fleet_ids if pair[0] != "fleet_default"] or fleet_ids
    for index in range(start, end):
        metadata = vehicle_metadata(index, seed_value)
        fleet_id, tenant_id = assignable[index % len(assignable)]
        yield (
            metadata["vehicle_id"],
            tenant_id,
            fleet_id,
            metadata["vin"],
            metadata["oem"],
            metadata["model"],
            metadata["model_year"],
            metadata["road_class"],
            True,
        )


def counts() -> dict[str, int]:
    """Current registry sizes, for the seed report and the ops page."""
    import psycopg2

    from packages.common.dsn import psycopg2_kwargs

    with psycopg2.connect(**psycopg2_kwargs(owner=True)) as connection, connection.cursor() as cursor:
        result: dict[str, int] = {}
        for table in ("tenant", "fleet", "vehicle"):
            cursor.execute(f"SELECT count(*) FROM {table}")  # nosec B608 - fixed table allowlist  # nosec B608 # static identifiers, parameter-bound values
            result[table] = int(cursor.fetchone()[0])
        return result
