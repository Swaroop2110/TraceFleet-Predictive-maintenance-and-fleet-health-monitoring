"""Remove only synthetic veh_sim_* records before a clean local demo run."""

from __future__ import annotations

import os

import clickhouse_connect
import redis
from sqlalchemy import text

from apps.api.app.db.session import SessionLocal, set_tenant_context

TENANT_ID = "tenant_default"
VEHICLE_PREFIX = "veh_sim_"


def reset_demo_data() -> dict[str, int]:
    db = SessionLocal()
    try:
        set_tenant_context(db, TENANT_ID)
        incident_ids = [
            row[0]
            for row in db.execute(
                text("SELECT incident_id FROM incident WHERE tenant_id = :tenant AND vehicle_id LIKE :pattern"),
                {"tenant": TENANT_ID, "pattern": f"{VEHICLE_PREFIX}%"},
            )
        ]
        if incident_ids:
            db.execute(
                text("DELETE FROM ai_run WHERE incident_id = ANY(:incident_ids)"), {"incident_ids": incident_ids}
            )
            db.execute(
                text("DELETE FROM incident_state_history WHERE incident_id = ANY(:incident_ids)"),
                {"incident_ids": incident_ids},
            )
            db.execute(
                text("DELETE FROM incident_evidence WHERE incident_id = ANY(:incident_ids)"),
                {"incident_ids": incident_ids},
            )
            db.execute(
                text("DELETE FROM incident_factor WHERE incident_id = ANY(:incident_ids)"),
                {"incident_ids": incident_ids},
            )
        incident_count = (
            db.execute(
                text("DELETE FROM incident WHERE tenant_id = :tenant AND vehicle_id LIKE :pattern"),
                {"tenant": TENANT_ID, "pattern": f"{VEHICLE_PREFIX}%"},
            ).rowcount
            or 0
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://redis:6379/0"))
    redis_count = 0
    batch: list[str] = []
    for key in redis_client.scan_iter(match=f"tf:{TENANT_ID}:veh:{VEHICLE_PREFIX}*:*", count=500):
        batch.append(key.decode("utf-8") if isinstance(key, bytes) else key)
        if len(batch) == 500:
            redis_count += redis_client.unlink(*batch)
            batch.clear()
    if batch:
        redis_count += redis_client.unlink(*batch)
    redis_client.close()

    clickhouse = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DB", "tracefleet"),
    )
    clickhouse_rows = clickhouse.query(
        "SELECT count() FROM telemetry_events "
        "WHERE tenant_id = {tenant:String} AND startsWith(vehicle_id, {prefix:String})",
        parameters={"tenant": TENANT_ID, "prefix": VEHICLE_PREFIX},
    ).result_rows[0][0]
    clickhouse.command(
        "ALTER TABLE telemetry_events DELETE "
        "WHERE tenant_id = {tenant:String} AND startsWith(vehicle_id, {prefix:String}) "
        "SETTINGS mutations_sync = 1",
        parameters={"tenant": TENANT_ID, "prefix": VEHICLE_PREFIX},
    )
    clickhouse.close()
    return {"incidents": incident_count, "redis_states": redis_count, "clickhouse_rows": clickhouse_rows}


if __name__ == "__main__":
    print(reset_demo_data())
