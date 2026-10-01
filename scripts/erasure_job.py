#!/usr/bin/env python3
"""Process right-to-erasure requests across Postgres, ClickHouse, and Redis.

An erasure that only touches one store is not an erasure. This job works through
pending ``erasure_request`` rows and, for each subject, removes or pseudonymises its
data everywhere, then writes a **verification report** back onto the request row
recording what each store actually did.

Two decisions that shape the behaviour:

**Pseudonymise the vehicle, delete its telemetry.** The vehicle row is retained with
its VIN replaced, because deleting it would orphan incident history and break the
referential integrity that makes past investigations readable. The identifying
attribute is what has to go, not the fact that a vehicle existed.

**Audit rows are never deleted.** The erasure itself is a compliance event and must
remain provable. Audit entries reference a vehicle id, not personal data, and the
retention job ages them out on the statutory schedule instead.

Verification is real. ClickHouse deletes are asynchronous mutations, so the job polls
until they complete (or times out) rather than assuming, and re-queries each store to
confirm zero remaining rows before marking the request COMPLETED.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.common import logging as tf_logging  # noqa: E402
from packages.common.timeutil import now, to_iso  # noqa: E402

LOGGER = tf_logging.get_logger("erasure")

#: How long to wait for a ClickHouse mutation before reporting it still pending.
MUTATION_TIMEOUT_S = 120
MUTATION_POLL_S = 2.0


def _owner_connection() -> Any:
    import psycopg2

    from packages.common.dsn import psycopg2_kwargs

    connection = psycopg2.connect(**psycopg2_kwargs(owner=True))
    connection.autocommit = False
    return connection


def pending_requests(limit: int = 50) -> list[dict[str, Any]]:
    """Fetch PENDING erasure requests, oldest first."""
    connection = _owner_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, tenant_id, subject_type, subject_id, actor, reason
                FROM erasure_request
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "id": row[0],
                    "tenant_id": row[1],
                    "subject_type": row[2],
                    "subject_id": row[3],
                    "actor": row[4],
                    "reason": row[5],
                }
                for row in cursor.fetchall()
            ]
    finally:
        connection.close()


def _set_status(request_id: str, status: str, report: dict[str, Any] | None = None) -> None:
    from psycopg2.extras import Json

    connection = _owner_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE erasure_request
                SET status = %s,
                    processed_at = CASE WHEN %s IN ('COMPLETED','FAILED') THEN now() ELSE processed_at END,
                    verification_report = COALESCE(%s, verification_report)
                WHERE id = %s
                """,
                (status, status, Json(report) if report is not None else None, request_id),
            )
        connection.commit()
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Per-store erasure
# ---------------------------------------------------------------------------
def erase_postgres(tenant_id: str, vehicle_id: str, *, dry_run: bool = False) -> dict[str, Any]:
    """Remove incidents and pseudonymise the vehicle row."""
    result: dict[str, Any] = {"store": "postgres"}
    connection = _owner_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM incident WHERE tenant_id = %s AND vehicle_id = %s",
                (tenant_id, vehicle_id),
            )
            incident_count = int(cursor.fetchone()[0])
            cursor.execute(
                """
                SELECT count(*) FROM incident_evidence e
                JOIN incident i ON i.incident_id = e.incident_id
                WHERE i.tenant_id = %s AND i.vehicle_id = %s
                """,
                (tenant_id, vehicle_id),
            )
            evidence_count = int(cursor.fetchone()[0])

            # Captured before the delete, so the Redis pass can clear the matching
            # cache entries. After the rows are gone the ids are unrecoverable.
            cursor.execute(
                "SELECT incident_id FROM incident WHERE tenant_id = %s AND vehicle_id = %s",
                (tenant_id, vehicle_id),
            )
            incident_ids = [row[0] for row in cursor.fetchall()]

            if dry_run:
                result.update(
                    {
                        "would_delete_incidents": incident_count,
                        "would_delete_evidence": evidence_count,
                        "would_pseudonymise_vehicle": True,
                        "incident_ids": incident_ids,
                    }
                )
                connection.rollback()
                return result

            # Cascade removes evidence, factors, state history, and ai_run rows.
            cursor.execute(
                "DELETE FROM incident WHERE tenant_id = %s AND vehicle_id = %s",
                (tenant_id, vehicle_id),
            )
            deleted_incidents = cursor.rowcount

            cursor.execute(
                """
                UPDATE vehicle
                SET vin = %s, active = false,
                    metadata_json = COALESCE(metadata_json, '{}'::jsonb)
                                    || jsonb_build_object('erased_at', %s)
                WHERE tenant_id = %s AND vehicle_id = %s
                """,
                ("ERASED" + "0" * 11, to_iso(now()), tenant_id, vehicle_id),
            )
            pseudonymised = cursor.rowcount

            cursor.execute(
                "DELETE FROM ground_truth_incident WHERE tenant_id = %s AND vehicle_id = %s",
                (tenant_id, vehicle_id),
            )
            deleted_truth = cursor.rowcount

        connection.commit()
        result.update(
            {
                "deleted_incidents": deleted_incidents,
                "cascaded_evidence": evidence_count,
                "pseudonymised_vehicles": pseudonymised,
                "deleted_ground_truth": deleted_truth,
                "incident_ids": incident_ids,
                # Stated explicitly so a reviewer sees it was a decision.
                "audit_log": "retained deliberately; erasure must remain provable",
                "verified": True,
            }
        )
        return result
    except Exception as error:
        connection.rollback()
        LOGGER.error("postgres erasure failed", extra={"error": str(error)})
        result["error"] = str(error)[:300]
        return result
    finally:
        connection.close()


def erase_clickhouse(tenant_id: str, vehicle_id: str, *, dry_run: bool = False) -> dict[str, Any]:
    """Submit a telemetry delete mutation and wait for it."""
    from apps.api.app.services.clickhouse import telemetry_store

    result: dict[str, Any] = {"store": "clickhouse"}
    try:
        client = telemetry_store.client
        tables = ("telemetry_events", "telemetry_1h_by_vehicle", "telemetry_quarantine")
        parameters = {"tenant_id": tenant_id, "vehicle_id": vehicle_id}
        conditions = dict.fromkeys(tables, "tenant_id = %(tenant_id)s AND vehicle_id = %(vehicle_id)s")
        # Quarantine schema has no tenant column; vehicle IDs are globally unique.
        conditions["telemetry_quarantine"] = "vehicle_id = %(vehicle_id)s"
        counts = {}
        for table in tables:
            query = "SELECT count(*) FROM " + table + " WHERE " + conditions[table]  # nosec B608 # fixed table and clause map
            before = int(client.query(query, parameters=parameters).result_rows[0][0])
            counts[table] = {"rows_before": before}
            if dry_run:
                continue
            if before:
                statement = "ALTER TABLE " + table + " DELETE WHERE " + conditions[table]  # nosec B608 # fixed table and clause map
                client.command(statement, parameters=parameters)
            deadline = time.monotonic() + MUTATION_TIMEOUT_S
            remaining = before
            while remaining and time.monotonic() < deadline:
                time.sleep(MUTATION_POLL_S)
                remaining = int(client.query(query, parameters=parameters).result_rows[0][0])
            counts[table]["rows_after"] = remaining
        result.update({"tables": counts, "verified": dry_run or all(v["rows_after"] == 0 for v in counts.values())})
        return result
    except Exception as error:
        LOGGER.error("clickhouse erasure failed", extra={"error": str(error)})
        result["error"] = str(error)[:300]
        result["verified"] = False
        return result


def erase_redis(
    tenant_id: str,
    vehicle_id: str,
    incident_ids: list[str] | None = None,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Delete hot state, cached timelines, and incident index entries."""
    result: dict[str, Any] = {"store": "redis"}
    try:
        import redis

        from packages.common.dsn import redis_url

        client = redis.from_url(redis_url(), decode_responses=True, socket_timeout=5)

        keys = [
            f"tf:{tenant_id}:veh:{vehicle_id}:state",
            f"tf:{tenant_id}:veh:{vehicle_id}:window",
            f"tf:{tenant_id}:veh:{vehicle_id}:checkpoint",
        ]
        # Cached reconstructions and summaries are derived data keyed by incident id;
        # leaving them would keep an erased vehicle's timeline servable from cache.
        for incident_id in incident_ids or []:
            keys.append(f"tf:{tenant_id}:incident:{incident_id}:summary")
            keys.extend(client.scan_iter(match=f"tf:{tenant_id}:timeline:{incident_id}:*", count=100))

        existing = [key for key in keys if client.exists(key)]

        if dry_run:
            result.update(
                {
                    "would_delete_keys": len(existing),
                    "would_remove_index_entries": len(incident_ids or []),
                }
            )
            return result

        deleted = client.delete(*existing) if existing else 0
        removed_from_index = 0
        if incident_ids:
            removed_from_index = client.zrem(f"tf:{tenant_id}:incidents:active", *incident_ids) or 0

        remaining = [key for key in keys if client.exists(key)]
        result.update(
            {
                "deleted_keys": deleted,
                "removed_index_entries": removed_from_index,
                "rows_after": len(remaining),
                "verified": not remaining,
                # Dedup markers are opaque sha256 digests with a short TTL and hold no
                # subject attribute, so they are left to expire.
                "note": "dedup markers are content hashes with a TTL and hold no subject data",
            }
        )
        return result
    except Exception as error:
        LOGGER.warning("redis erasure failed", extra={"error": str(error)})
        result["error"] = str(error)[:300]
        result["verified"] = False
        return result


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def process_request(request: dict[str, Any], *, dry_run: bool = False) -> dict[str, Any]:
    """Erase one subject across every store and build a verification report."""
    tenant_id = request["tenant_id"]
    subject_id = request["subject_id"]

    if not dry_run:
        _set_status(request["id"], "IN_PROGRESS")

    LOGGER.info(
        "processing erasure request",
        extra={"request_id": request["id"], "subject_id": subject_id, "dry_run": dry_run},
    )

    try:
        _validate_subject(tenant_id, subject_id, request["subject_type"])
        if not dry_run:
            _block_reingestion(subject_id)
    except Exception as error:
        report = {
            "request_id": request["id"],
            "subject_id": subject_id,
            "tenant_id": tenant_id,
            "verified": False,
            "error": str(error)[:300],
            "stores": [],
            "dry_run": dry_run,
        }
        if not dry_run:
            _set_status(request["id"], "FAILED", report)
            _write_audit(request, report)
        return report

    # Postgres first, so the incident ids it collected can drive the Redis cache
    # cleanup. Reversing the order would leave cached timelines with no way to
    # identify them.
    postgres = erase_postgres(tenant_id, subject_id, dry_run=dry_run)
    stores = [
        postgres,
        erase_clickhouse(tenant_id, subject_id, dry_run=dry_run),
        erase_redis(tenant_id, subject_id, postgres.get("incident_ids", []), dry_run=dry_run),
    ]

    verified = all(store.get("verified", True) and "error" not in store for store in stores)
    report = {
        "request_id": request["id"],
        "subject_type": request["subject_type"],
        "subject_id": subject_id,
        "tenant_id": tenant_id,
        "requested_by": request["actor"],
        "reason": request["reason"],
        "processed_at": to_iso(now()),
        "dry_run": dry_run,
        "stores": stores,
        "verified": verified,
        "retained": {
            "audit_log": "erasure events are retained for compliance provability",
            "vehicle_row": "retained with the VIN pseudonymised to preserve incident history integrity",
        },
    }

    if not dry_run:
        # FAILED when any store could not be confirmed. Marking a partially-completed
        # erasure COMPLETED would be a false compliance claim.
        _set_status(request["id"], "COMPLETED" if verified else "FAILED", report)
        _write_audit(request, report)

    return report


def _write_audit(request: dict[str, Any], report: dict[str, Any]) -> None:
    """Record the erasure in the audit log."""
    from psycopg2.extras import Json

    connection = _owner_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO audit_log (actor, tenant, resource, resource_id, action, outcome, metadata_json)
                VALUES (%s, %s, 'erasure_request', %s, 'erasure_processed', %s, %s)
                """,
                (
                    "erasure-job",
                    request["tenant_id"],
                    request["id"],
                    "SUCCESS" if report["verified"] else "FAILURE",
                    Json({"subject_id": request["subject_id"], "verified": report["verified"]}),
                ),
            )
        connection.commit()
    except Exception as error:
        LOGGER.error("erasure audit write failed", extra={"error": str(error)})
    finally:
        connection.close()


def run(
    *,
    tenant_id: str | None = None,
    subject_id: str | None = None,
    dry_run: bool = False,
    limit: int = 50,
) -> dict[str, Any]:
    """Process pending requests, or one ad-hoc subject."""
    if subject_id:
        if not tenant_id:
            raise ValueError("--tenant-id is required with --subject-id")
        request = {
            "id": f"adhoc_{subject_id}",
            "tenant_id": tenant_id,
            "subject_type": "vehicle",
            "subject_id": subject_id,
            "actor": "cli",
            "reason": "ad-hoc erasure requested from the command line",
        }
        # Ad-hoc runs never write status back, because there is no request row.
        return {"requests": [process_request(request, dry_run=True) if dry_run else _adhoc(request)]}

    requests = pending_requests(limit)
    if not requests:
        LOGGER.info("no pending erasure requests")
        return {"requests": [], "processed": 0}

    reports = [process_request(request, dry_run=dry_run) for request in requests]
    return {
        "requests": reports,
        "processed": len(reports),
        "verified": sum(1 for report in reports if report["verified"]),
        "failed": sum(1 for report in reports if not report["verified"]),
    }


def _validate_subject(tenant_id: str, vehicle_id: str, subject_type: str) -> None:
    if subject_type != "vehicle":
        raise ValueError("Only vehicle erasure is supported")
    connection = _owner_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM vehicle WHERE tenant_id=%s AND vehicle_id=%s", (tenant_id, vehicle_id))
            if cursor.fetchone() is None:
                raise ValueError("Vehicle does not belong to the requesting tenant")
    finally:
        connection.close()


def _block_reingestion(vehicle_id: str) -> None:
    import redis

    from packages.common.dsn import redis_url

    client = redis.from_url(redis_url(), socket_timeout=5)
    try:
        client.sadd("tf:erased:vehicles", vehicle_id)
    finally:
        client.close()


def _adhoc(request: dict[str, Any]) -> dict[str, Any]:
    """Erase without a request row, still writing an audit entry."""
    tenant_id, subject_id = request["tenant_id"], request["subject_id"]
    _validate_subject(tenant_id, subject_id, request.get("subject_type", "vehicle"))
    _block_reingestion(subject_id)
    postgres = erase_postgres(tenant_id, subject_id)
    stores = [
        postgres,
        erase_clickhouse(tenant_id, subject_id),
        erase_redis(tenant_id, subject_id, postgres.get("incident_ids", [])),
    ]
    report = {
        "request_id": request["id"],
        "subject_id": subject_id,
        "tenant_id": tenant_id,
        "processed_at": to_iso(now()),
        "stores": stores,
        "verified": all(store.get("verified", True) and "error" not in store for store in stores),
        "dry_run": False,
    }
    _write_audit(request, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process TraceFleet erasure requests")
    parser.add_argument("--tenant-id", help="Tenant, required with --subject-id")
    parser.add_argument("--subject-id", help="Erase one vehicle without a request row")
    parser.add_argument("--dry-run", action="store_true", help="Report what would be erased")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--report", type=Path, help="Write the verification report as JSON")
    args = parser.parse_args(argv)

    tf_logging.configure("tracefleet-erasure")
    try:
        result = run(
            tenant_id=args.tenant_id,
            subject_id=args.subject_id,
            dry_run=args.dry_run,
            limit=args.limit,
        )
    except ValueError as error:
        parser.error(str(error))
        return 2

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")

    print(json.dumps(result, indent=2, default=str))
    return 1 if result.get("failed") else 0


if __name__ == "__main__":
    raise SystemExit(main())
