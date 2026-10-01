#!/usr/bin/env python3
"""Apply retention policies across Postgres, ClickHouse, and Redis.

Windows come from ``config/retention.yaml``, not from a hard-coded constant, so the
policy is reviewable and the same file drives the ClickHouse table TTL.

Three properties this job has that the previous version did not:

**It spans every store.** Deleting incidents from Postgres while leaving the
underlying telemetry in ClickHouse is not retention; it is a partial deletion that
leaves the data discoverable.

**It respects the append-only audit log.** Audit rows are aged out through
``tracefleet_purge_audit()``, a privileged function that disables the guard trigger
for the duration and records its own deletion. A plain DELETE is rejected, which is
the point of the constraint.

**It is honest about asynchrony.** ClickHouse deletes are mutations, not statements.
The job reports the mutation as submitted rather than complete, and ``--verify``
polls ``system.mutations`` for the real answer.

Run with ``--dry-run`` first: it reports what would be removed and touches nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from packages.common import config as cfg  # noqa: E402
from packages.common import logging as tf_logging  # noqa: E402
from packages.common.timeutil import add_seconds, now, to_iso  # noqa: E402

LOGGER = tf_logging.get_logger("retention")

#: Table, timestamp column, and which retention window governs it.
POSTGRES_POLICIES: tuple[tuple[str, str, str], ...] = (
    ("incident", "start_ts", "incident_days"),
    ("ai_run", "created_at", "ai_run_days"),
    ("ground_truth_incident", "start_ts", "incident_days"),
)


def _owner_connection() -> Any:
    """Connect as the owner.

    Retention operates across every tenant and calls a SECURITY DEFINER function, so
    it cannot run under the restricted API role.
    """
    import psycopg2

    from packages.common.dsn import psycopg2_kwargs

    connection = psycopg2.connect(**psycopg2_kwargs(owner=True))
    connection.autocommit = False
    return connection


def prune_postgres(retention: cfg.RetentionConfig, *, dry_run: bool = False) -> dict[str, Any]:
    """Age out Postgres rows. Evidence and factors follow by cascade."""
    results: dict[str, Any] = {}
    connection = _owner_connection()
    try:
        with connection.cursor() as cursor:
            for table, column, policy in POSTGRES_POLICIES:
                days = getattr(retention, policy)
                cutoff = add_seconds(now(), -days * 86400)
                cursor.execute(
                    f"SELECT count(*) FROM {table} WHERE {column} < %s",  # nosec B608 # fixed identifiers
                    (cutoff,),  # nosec B608 # fixed identifiers
                )  # nosec B608 # static policy identifiers
                candidates = int(cursor.fetchone()[0])
                if dry_run:
                    results[table] = {"would_delete": candidates, "cutoff": to_iso(cutoff), "retention_days": days}
                    continue
                cursor.execute(
                    f"DELETE FROM {table} WHERE {column} < %s",  # nosec B608 # fixed identifiers
                    (cutoff,),  # nosec B608 # fixed identifiers
                )  # nosec B608 # static policy identifiers
                results[table] = {
                    "deleted": cursor.rowcount,
                    "cutoff": to_iso(cutoff),
                    "retention_days": days,
                }

            # Audit rows go through the privileged function; a direct DELETE is
            # rejected by the append-only trigger by design.
            audit_cutoff = add_seconds(now(), -retention.audit_days * 86400)
            cursor.execute("SELECT count(*) FROM audit_log WHERE timestamp < %s", (audit_cutoff,))
            audit_candidates = int(cursor.fetchone()[0])
            if dry_run:
                results["audit_log"] = {
                    "would_delete": audit_candidates,
                    "cutoff": to_iso(audit_cutoff),
                    "retention_days": retention.audit_days,
                    "method": "tracefleet_purge_audit()",
                }
            else:
                cursor.execute("SELECT tracefleet_purge_audit(%s)", (audit_cutoff,))
                removed = int(cursor.fetchone()[0])
                results["audit_log"] = {
                    "deleted": removed,
                    "cutoff": to_iso(audit_cutoff),
                    "retention_days": retention.audit_days,
                    "method": "tracefleet_purge_audit()",
                }

        if dry_run:
            connection.rollback()
        else:
            connection.commit()
        return results
    except Exception as error:
        connection.rollback()
        LOGGER.error("postgres retention failed", extra={"error": str(error)})
        results["error"] = str(error)
        return results
    finally:
        connection.close()


def prune_clickhouse(retention: cfg.RetentionConfig, *, dry_run: bool = False) -> dict[str, Any]:
    """Align the ClickHouse TTL with config and submit an out-of-band delete.

    The table already carries a TTL, but a TTL only applies during merges, so a
    partition that is not being merged can outlive its window. Submitting an explicit
    mutation makes the bound enforced rather than eventual.
    """
    from apps.api.app.services.clickhouse import telemetry_store

    result: dict[str, Any] = {"telemetry_warm_days": retention.telemetry_warm_days}
    cutoff = add_seconds(now(), -retention.telemetry_warm_days * 86400)

    try:
        client = telemetry_store.client
        rows = client.query(
            "SELECT count(*) FROM telemetry_events WHERE ts < %(cutoff)s",
            parameters={"cutoff": cutoff},
        ).result_rows
        candidates = int(rows[0][0]) if rows else 0
        result["candidate_rows"] = candidates
        result["cutoff"] = to_iso(cutoff)

        if dry_run:
            result["would_submit_mutation"] = candidates > 0
            return result

        if candidates:
            client.command(
                "ALTER TABLE telemetry_events DELETE WHERE ts < %(cutoff)s",
                parameters={"cutoff": cutoff},
            )
            # Submitted, not finished. Claiming completion here would be wrong.
            result["mutation"] = "submitted"
            result["note"] = "ClickHouse deletes are asynchronous; run with --verify to confirm"
        else:
            result["mutation"] = "not_required"

        quarantine_cutoff = add_seconds(now(), -retention.quarantine_days * 86400)
        client.command(
            "ALTER TABLE telemetry_quarantine DELETE WHERE received_at < %(cutoff)s",
            parameters={"cutoff": quarantine_cutoff},
        )
        result["quarantine_mutation"] = "submitted"
        result["quarantine_days"] = retention.quarantine_days
        return result
    except Exception as error:
        LOGGER.warning("clickhouse retention skipped", extra={"error": str(error)})
        result["error"] = str(error)[:300]
        return result


def prune_redis(retention: cfg.RetentionConfig, *, dry_run: bool = False) -> dict[str, Any]:
    """Re-apply TTLs to hot-state keys.

    Redis expires keys on its own, but a key written before a TTL change keeps the
    old one. This re-stamps them so shortening the window in config takes effect on
    existing keys rather than only on new ones.
    """
    result: dict[str, Any] = {"redis_state_ttl_s": retention.redis_state_ttl_s}
    try:
        import redis

        from packages.common.dsn import redis_url

        client = redis.from_url(redis_url(), decode_responses=True, socket_timeout=5)
        scanned = 0
        restamped = 0
        for key in client.scan_iter(match="tf:*:veh:*:state", count=500):
            scanned += 1
            ttl = client.ttl(key)
            if ttl == -1 or ttl > retention.redis_state_ttl_s:
                if not dry_run:
                    client.expire(key, retention.redis_state_ttl_s)
                restamped += 1
        result.update({"keys_scanned": scanned, "ttl_restamped": restamped, "dry_run": dry_run})
        return result
    except Exception as error:
        LOGGER.warning("redis retention skipped", extra={"error": str(error)})
        result["error"] = str(error)[:300]
        return result


def verify_mutations() -> dict[str, Any]:
    """Report outstanding ClickHouse mutations."""
    from apps.api.app.services.clickhouse import telemetry_store

    try:
        mutations = telemetry_store.mutation_status("telemetry_events")
        pending = [item for item in mutations if not item["is_done"]]
        return {
            "mutations": mutations[:10],
            "pending": len(pending),
            "all_complete": not pending,
        }
    except Exception as error:
        return {"error": str(error)[:300]}


def run(*, dry_run: bool = False, verify: bool = False) -> dict[str, Any]:
    """Apply every retention policy and return a report."""
    retention = cfg.retention()
    started = now()

    report: dict[str, Any] = {
        "started_at": to_iso(started),
        "dry_run": dry_run,
        "policy": retention.model_dump(),
        "postgres": prune_postgres(retention, dry_run=dry_run),
        "clickhouse": prune_clickhouse(retention, dry_run=dry_run),
        "redis": prune_redis(retention, dry_run=dry_run),
    }
    if verify:
        report["verification"] = verify_mutations()
    report["completed_at"] = to_iso(now())

    LOGGER.info(
        "retention job complete",
        extra={"dry_run": dry_run, "stores": list(report.keys())},
    )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply TraceFleet retention policies")
    parser.add_argument("--dry-run", action="store_true", help="Report what would be removed, change nothing")
    parser.add_argument("--verify", action="store_true", help="Poll ClickHouse mutation status")
    parser.add_argument("--report", type=Path, help="Write the report as JSON")
    args = parser.parse_args(argv)

    tf_logging.configure("tracefleet-retention")
    report = run(dry_run=args.dry_run, verify=args.verify)

    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print(json.dumps(report, indent=2, default=str))
    failed = any("error" in section for section in report.values() if isinstance(section, dict))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
