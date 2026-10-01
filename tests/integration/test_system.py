"""Real-store acceptance gates. Run inside Compose with TRACEFLEET_INTEGRATION=1."""

import json
import os
import time
import uuid
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from packages.common.timeutil import now, to_iso

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("TRACEFLEET_INTEGRATION") != "1", reason="requires migrated Compose stack"),
]


@pytest.fixture(scope="module")
def system():
    import clickhouse_connect
    import psycopg2
    import redis

    from apps.api.app.main import app
    from packages.common.dsn import psycopg2_kwargs

    prefix = "verify_" + uuid.uuid4().hex[:10]
    owner = psycopg2.connect(**psycopg2_kwargs(owner=True))
    owner.autocommit = True
    with owner.cursor() as cur:
        cur.execute("INSERT INTO tenant(tenant_id,name) VALUES(%s,%s)", (prefix, prefix))
        cur.execute("INSERT INTO fleet(fleet_id,tenant_id,name) VALUES(%s,%s,%s)", (prefix, prefix, prefix))
    hot = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
    cold = clickhouse_connect.get_client(host=os.getenv("CLICKHOUSE_HOST", "clickhouse"), database="tracefleet")
    with TestClient(app) as client:
        tokens = {}
        for user in ("viewer", "analyst", "manager", "admin"):
            response = client.post(
                "/api/v1/auth/token",
                json={"username": user, "password": "tracefleet-demo", "tenant_id": "tenant_default"},
            )
            assert response.status_code == 200, response.text
            tokens[user] = response.json()
        yield {"owner": owner, "redis": hot, "cold": cold, "client": client, "tokens": tokens, "prefix": prefix}
    owner.close()
    cold.close()
    hot.close()


def headers(system, user="admin"):
    return {"Authorization": "Bearer " + system["tokens"][user]["access_token"]}


def wait_until(work, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = work()
        if result:
            return result
        time.sleep(0.2)
    raise AssertionError("timed out waiting for pipeline")


@pytest.fixture(scope="module")
def detected(system):
    from sim.scenarios.registry import generate

    from packages.common import kafka

    vehicle = system["prefix"] + "_collision"
    with system["owner"].cursor() as cur:
        cur.execute(
            "INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,'tenant_default','fleet_default',%s,'TEST','Verification',2026)",
            (vehicle, "1HGCM82633A004352"),
        )
    producer = kafka.build_producer()
    result = generate("collision_like", vehicle, start_time=now())
    for event in result.events:
        producer.produce(kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(event).encode())
    final = dict(
        result.events[-1],
        seq=99,
        ts=to_iso(now() + timedelta(seconds=60)),
        speed_kmh=20,
        acceleration_mps2=0,
        brake_force_pct=0,
    )
    producer.produce(kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(final).encode())
    assert producer.flush(30) == 0

    def read():
        response = system["client"].get("/api/v1/incidents", params={"vehicle_id": vehicle}, headers=headers(system))
        assert response.status_code == 200, response.text
        return next((i for i in response.json()["items"] if i["status"] == "OPEN"), None)

    incident = wait_until(read)
    assert incident["severity"] == "CRITICAL"
    return incident


def test_actual_pipeline_reaches_every_store(system, detected):
    state = system["client"].get(f"/api/v1/vehicles/{detected['vehicle_id']}/state", headers=headers(system))
    assert state.status_code == 200 and state.json()["source"] == "redis"
    listed = (
        system["client"]
        .get("/api/v1/vehicles", params={"q": detected["vehicle_id"]}, headers=headers(system))
        .json()["items"]
    )
    assert listed and listed[0]["live"] and listed[0]["lat"] == state.json()["lat"]

    def rows():
        return (
            system["cold"]
            .query(
                "SELECT count() FROM telemetry_events FINAL WHERE vehicle_id={v:String}",
                parameters={"v": detected["vehicle_id"]},
            )
            .first_row[0]
        )

    assert wait_until(rows) >= 7
    detail = system["client"].get(f"/api/v1/incidents/{detected['incident_id']}", headers=headers(system))
    assert detail.status_code == 200 and len(detail.json()["factors"]) >= 4


def test_raw_faults_and_duplicates_are_observable(system):
    from sim.scenarios.registry import generate

    from packages.common import kafka

    producer = kafka.build_producer()
    vehicle = system["prefix"] + "_fault"
    for event in generate("schema_fault", vehicle).events:
        producer.produce(kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(event).encode())
    duplicate = generate("normal", vehicle).events[0]
    for _ in range(8):
        producer.produce(kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(duplicate).encode())
    assert producer.flush(30) == 0

    def rejected():
        return (
            system["cold"]
            .query("SELECT count() FROM telemetry_quarantine WHERE vehicle_id={v:String}", parameters={"v": vehicle})
            .first_row[0]
        )

    assert wait_until(rejected) >= 4

    def accepted():
        return (
            system["cold"]
            .query("SELECT count() FROM telemetry_events FINAL WHERE vehicle_id={v:String}", parameters={"v": vehicle})
            .first_row[0]
        )

    assert wait_until(accepted) == 1


def test_investigation_ai_and_exports(system, detected):
    id = detected["incident_id"]
    client = system["client"]
    auth = headers(system)
    for suffix in ("timeline", "evidence?include_raw=true", "history"):
        response = client.get(f"/api/v1/incidents/{id}/{suffix}", headers=auth)
        assert response.status_code == 200, response.text
    timeline = client.get(f"/api/v1/incidents/{id}/timeline", headers=auth).json()
    assert timeline["points"] and timeline["evidence"] and len(timeline["phases"]) >= 3
    summary = client.post(f"/api/v1/incidents/{id}/ai-summary", json={}, headers=auth)
    assert summary.status_code == 200, summary.text
    data = summary.json()
    assert data["validation"]["passed"], data
    known = {e["evidence_id"] for e in timeline["evidence"]}
    assert all(set(s["evidence_ids"]) <= known for section in data["summary_sections"] for s in section["statements"])
    cached = client.post(f"/api/v1/incidents/{id}/ai-summary", json={}, headers=auth).json()
    assert cached["cached"] and cached["run_id"] == data["run_id"]
    for suffix in ("ai-summary/history", "ai-summary/" + data["run_id"], "ai-summary-policy", "export/formats"):
        assert client.get(f"/api/v1/incidents/{id}/{suffix}", headers=auth).status_code == 200
    for fmt, mime in [("json", "application/json"), ("csv", "text/csv"), ("pdf", "application/pdf")]:
        response = client.get(f"/api/v1/incidents/{id}/export?format={fmt}", headers=auth)
        assert response.status_code == 200, response.text
        assert response.headers["content-type"].startswith(mime)
        assert len(response.content) > 100
        if fmt == "pdf":
            assert response.content.startswith(b"%PDF")


def test_role_matrix_and_lifecycle(system, detected):
    client = system["client"]
    id = detected["incident_id"]
    assert client.get("/api/v1/incidents").status_code == 401
    assert client.get("/api/v1/analytics/fleet", headers=headers(system, "viewer")).status_code == 403
    assert client.post(f"/api/v1/incidents/{id}/acknowledge", headers=headers(system, "viewer")).status_code == 403
    assert client.post(f"/api/v1/incidents/{id}/close", headers=headers(system, "analyst")).status_code == 403
    for _ in range(2):
        response = client.post(f"/api/v1/incidents/{id}/acknowledge", headers=headers(system, "analyst"))
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "ACKNOWLEDGED"
    response = client.post(f"/api/v1/incidents/{id}/close", headers=headers(system, "manager"))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "CLOSED"
    assert client.post(f"/api/v1/incidents/{id}/acknowledge", headers=headers(system, "analyst")).status_code == 409


def test_tenant_isolation_and_privacy(system, detected):
    from packages.common.tokens import issue_token

    other = {
        "Authorization": "Bearer " + issue_token(subject="test-other", tenant_id=system["prefix"], roles=["ADMIN"])
    }
    client = system["client"]
    assert client.get(f"/api/v1/incidents/{detected['incident_id']}", headers=other).status_code == 404
    assert client.get("/api/v1/incidents", headers=other).json()["items"] == []
    response = client.get(f"/api/v1/vehicles/{detected['vehicle_id']}/state", headers=headers(system, "viewer"))
    assert response.json()["_privacy"]["masked"]
    assert len(response.json()["geohash"]) <= 5
    import psycopg2

    from packages.common.dsn import psycopg2_kwargs

    with psycopg2.connect(**psycopg2_kwargs()) as db, db.cursor() as cur:
        cur.execute("SELECT count(*) FROM incident")
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT set_config('app.tenant_id',%s,true)", (system["prefix"],))
        cur.execute("SELECT count(*) FROM incident")
        assert cur.fetchone()[0] == 0
        with pytest.raises(psycopg2.Error):
            cur.execute("SELECT tracefleet_purge_audit(now())")
    for sql in ("UPDATE audit_log SET actor='tampered'", "DELETE FROM audit_log"):
        with psycopg2.connect(**psycopg2_kwargs()) as db, db.cursor() as cur:
            cur.execute("SELECT set_config('app.tenant_id','tenant_default',true)")
            with pytest.raises(psycopg2.Error):
                cur.execute(sql)


@pytest.mark.parametrize(
    "path",
    [
        "/fleets",
        "/vehicles",
        "/incidents/summary",
        "/analytics/fleet",
        "/analytics/incidents",
        "/analytics/fleet-risk",
        "/analytics/telemetry",
        "/ops/pipeline",
        "/ops/consumer-lag",
        "/ops/throughput",
        "/ops/chaos/targets",
        "/admin/users",
        "/admin/roles",
        "/admin/erasure",
        "/admin/audit",
        "/config/rules",
        "/config/plugins",
        "/auth/me",
        "/auth/authorization-model",
    ],
)
def test_api_surfaces(system, path):
    response = system["client"].get("/api/v1" + path, headers=headers(system))
    assert response.status_code == 200, response.text


@pytest.mark.parametrize("query", ["severity=UNKNOWN", "status=UNKNOWN", "cursor=not-valid", "limit=99999"])
def test_bad_filters_are_rejected(system, query):
    assert system["client"].get("/api/v1/incidents?" + query, headers=headers(system)).status_code in (400, 422)


def test_admin_user_and_erasure_workflow(system):
    client = system["client"]
    auth = headers(system)
    username = system["prefix"]
    created = client.post(
        "/api/v1/admin/users",
        json={"username": username, "password": "verification-password", "roles": ["VIEWER"]},
        headers=auth,
    )
    assert created.status_code == 201, created.text
    user = created.json()
    assert (
        client.put(
            "/api/v1/admin/users/" + user["user_id"], json={"display_name": "Verification"}, headers=auth
        ).status_code
        == 200
    )
    assert client.delete("/api/v1/admin/users/" + user["user_id"], headers=auth).status_code == 200
    assert (
        client.post("/api/v1/auth/token", json={"username": username, "password": "verification-password"}).status_code
        == 401
    )
    assert client.post("/api/v1/admin/config/reload", json={}, headers=auth).status_code == 200
    result = client.post(
        "/api/v1/admin/erasure",
        json={
            "subject_type": "vehicle",
            "subject_id": system["prefix"] + "_collision",
            "reason": "Verification fixture",
        },
        headers=auth,
    )
    assert result.status_code == 202, result.text


def test_refresh_rejects_access_token_and_rotates_refresh(system):
    client = system["client"]
    tokens = system["tokens"]["analyst"]
    assert client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["access_token"]}).status_code == 401
    assert client.post("/api/v1/auth/refresh", json={"refresh_token": tokens["refresh_token"]}).status_code == 200
    assert client.post("/api/v1/auth/token", json={"username": "analyst", "password": "wrong"}).status_code == 401
    assert client.get("/api/v1/auth/jwks").status_code == 200
    assert client.get("/api/v1/auth/.well-known/openid-configuration").status_code == 200


@pytest.mark.parametrize(
    "scenario",
    [
        "collision_like",
        "overspeed_chain",
        "false_positive_hard_brake",
        "diagnostic_only",
        "duplicate_storm",
        "out_of_order",
        "late_arrival",
        "schema_fault",
    ],
)
def test_hidden_ground_truth_scenario_matrix(system, scenario):
    from sim.ground_truth import GroundTruthWriter
    from sim.scenarios.registry import generate

    from packages.common import kafka

    vehicle = system["prefix"] + "_" + scenario
    with system["owner"].cursor() as cur:
        cur.execute(
            "INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,'tenant_default','fleet_default',%s,'TEST','Scenario',2026)",
            (vehicle, "1HGCM82633A004352"),
        )
    result = generate(scenario, vehicle, start_time=now())
    writer = GroundTruthWriter(run_id=system["prefix"], tenant_id="tenant_default")
    assert writer.write(result.ground_truth)
    writer.close()
    producer = kafka.build_producer()
    for event in result.events:
        producer.produce(kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(event).encode())
    assert producer.flush(30) == 0
    if result.ground_truth.should_create_incident:

        def read():
            return (
                system["client"]
                .get("/api/v1/incidents", params={"vehicle_id": vehicle}, headers=headers(system))
                .json()["items"]
            )

        found = wait_until(read)
        assert found[0]["severity"] == result.ground_truth.expected_severity
    else:
        # Wait until the durable validator/sink processed the complete stream.
        def processed():
            table = "telemetry_quarantine" if scenario == "schema_fault" else "telemetry_events FINAL"
            return (
                system["cold"]
                .query(f"SELECT count() FROM {table} WHERE vehicle_id={{v:String}}", parameters={"v": vehicle})
                .first_row[0]
            )

        wait_until(processed)
        time.sleep(1)
        assert (
            system["client"]
            .get("/api/v1/incidents", params={"vehicle_id": vehicle}, headers=headers(system))
            .json()["items"]
            == []
        )


def test_erasure_job_removes_identifying_data_from_all_stores(system):
    from sim.scenarios.registry import generate

    from packages.common import kafka
    from scripts.erasure_job import process_request

    vehicle = system["prefix"] + "_erase"
    with system["owner"].cursor() as cur:
        cur.execute(
            "INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,'tenant_default','fleet_default','1HGCM82633A004352','TEST','Erasure',2026)",
            (vehicle,),
        )
    events = generate("normal", vehicle).events + generate("schema_fault", vehicle).events
    producer = kafka.build_producer()

    def publish():
        for event in events:
            producer.produce(kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(event).encode())
        assert producer.flush(30) == 0

    publish()
    wait_until(
        lambda: (
            system["cold"]
            .query("SELECT count() FROM telemetry_events WHERE vehicle_id={v:String}", parameters={"v": vehicle})
            .first_row[0]
        )
    )
    wait_until(
        lambda: (
            system["cold"]
            .query("SELECT count() FROM telemetry_quarantine WHERE vehicle_id={v:String}", parameters={"v": vehicle})
            .first_row[0]
            >= 4
        )
    )
    system["redis"].set(f"tf:tenant_default:veh:{vehicle}:checkpoint", "private-location-history")
    response = system["client"].post(
        "/api/v1/admin/erasure",
        json={"subject_type": "vehicle", "subject_id": vehicle, "reason": "Isolated erasure acceptance fixture"},
        headers=headers(system),
    )
    assert response.status_code == 202, response.text
    request_id = response.json()["request_id"] if "request_id" in response.json() else response.json()["id"]
    report = process_request(
        {
            "id": request_id,
            "tenant_id": "tenant_default",
            "subject_type": "vehicle",
            "subject_id": vehicle,
            "actor": "test",
            "reason": "Isolated erasure acceptance fixture",
        }
    )
    with system["owner"].cursor() as cur:
        cur.execute("SELECT status,verification_report FROM erasure_request WHERE id=%s", (request_id,))
        status, persisted = cur.fetchone()
    assert status == "COMPLETED" and persisted["verified"]
    assert report["verified"], report
    for table in ["telemetry_events", "telemetry_1h_by_vehicle", "telemetry_quarantine"]:
        assert (
            system["cold"]
            .query(f"SELECT count() FROM {table} WHERE vehicle_id={{v:String}}", parameters={"v": vehicle})
            .first_row[0]
            == 0
        )
    assert not system["redis"].exists(f"tf:tenant_default:veh:{vehicle}:checkpoint")
    assert not system["redis"].exists(f"tf:tenant_default:veh:{vehicle}:state")
    with system["owner"].cursor() as cur:
        cur.execute("SELECT active,metadata_json,vin FROM vehicle WHERE vehicle_id=%s", (vehicle,))
        active, metadata, vin = cur.fetchone()
    assert not active and metadata["erased_at"] and vin.startswith("ERASED")
    publish()
    time.sleep(3)
    assert (
        system["cold"]
        .query("SELECT count() FROM telemetry_events WHERE vehicle_id={v:String}", parameters={"v": vehicle})
        .first_row[0]
        == 0
    )
    from pathlib import Path

    Path("artifacts/erasure-verification.json").write_text(json.dumps(report, indent=2, default=str) + "\n")


def test_erasure_cannot_block_or_delete_another_tenants_vehicle(system, detected):
    from packages.common.tokens import issue_token

    token = issue_token(subject="other-admin", tenant_id=system["prefix"], roles=["ADMIN"])
    response = system["client"].post(
        "/api/v1/admin/erasure",
        json={"subject_type": "vehicle", "subject_id": detected["vehicle_id"], "reason": "Must be rejected"},
        headers={"Authorization": "Bearer " + token},
    )
    assert response.status_code == 404, response.text
    from scripts.erasure_job import _adhoc

    with pytest.raises(ValueError, match="requesting tenant"):
        _adhoc({"tenant_id": system["prefix"], "subject_id": detected["vehicle_id"], "id": "invalid", "actor": "test"})
    assert not system["redis"].sismember("tf:erased:vehicles", detected["vehicle_id"])
    response = system["client"].post(
        "/api/v1/admin/erasure",
        json={"subject_type": "driver", "subject_id": "unsupported", "reason": "Must be rejected"},
        headers=headers(system),
    )
    assert response.status_code == 422


def test_bulk_seeding_is_idempotent_and_cannot_resurrect_erased_vehicle(system, monkeypatch):
    import psycopg2
    from sim.fleet_seed import _copy_vehicles, _vehicle_rows, counts, seed

    from packages.common.dsn import psycopg2_kwargs

    with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db, db.cursor() as cur:
        # Connection-local shadow prevents touching the user's seeded registry.
        for table in ("vehicle", "fleet", "tenant"):
            cur.execute(f"CREATE TEMP TABLE {table} (LIKE public.{table} INCLUDING DEFAULTS INCLUDING INDEXES)")
        tenant_ids = [system["prefix"]]
        fleet_ids = [(system["prefix"], system["prefix"])]
        assert _copy_vehicles(cur, 3, tenant_ids, fleet_ids, seed_value=42, progress_every=0) == 3
        erased_id = next(_vehicle_rows(0, 1, tenant_ids, fleet_ids, 42))[0]
        cur.execute(
            "UPDATE vehicle SET active=false,vin='ERASED00000000000',metadata_json='{}'::jsonb || jsonb_build_object('erased_at',now()) WHERE vehicle_id=%s",
            (erased_id,),
        )
        assert _copy_vehicles(cur, 3, tenant_ids, fleet_ids, seed_value=42, progress_every=0) == 3
        cur.execute("SELECT count(*) FROM vehicle")
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT active,vin FROM vehicle WHERE vehicle_id=%s", (erased_id,))
        assert cur.fetchone() == (False, "ERASED00000000000")

        class IsolatedConnection:
            def cursor(self):
                return db.cursor()

            def commit(self):
                db.commit()

            def rollback(self):
                db.rollback()

            def close(self):
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        isolated = IsolatedConnection()
        monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: isolated)
        report = seed(3, tenants=1, fleets_per_tenant=1, progress_every=0)
        assert report["vehicles"] == 3 and report["tenants"] == 1
        assert counts()["vehicle"] == 3
        cur.execute("SELECT active,vin FROM vehicle WHERE vehicle_id=%s", (erased_id,))
        assert cur.fetchone() == (False, "ERASED00000000000")
        db.rollback()
