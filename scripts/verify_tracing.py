"""Inject a sampled collision and verify one trace spans every Kafka processing role."""

import json
import time
import uuid
from pathlib import Path

import httpx
import psycopg2
from sim.scenarios.registry import generate

from packages.common import kafka, otel
from packages.common.dsn import psycopg2_kwargs


def main():
    vehicle = "trace_" + uuid.uuid4().hex[:10]
    with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db, db.cursor() as cursor:
        cursor.execute(
            "INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,'tenant_default','fleet_default','1HGCM82633A004352','TEST','Trace',2026)",
            (vehicle,),
        )
    otel.configure("tracefleet-verification")
    producer = kafka.build_producer()
    ids = []
    for event in generate("collision_like", vehicle).events:
        with otel.producer_span("verification.publish"):
            ids.append(otel.current_trace_id())
            producer.produce(
                kafka.TELEMETRY_RAW, key=vehicle, value=json.dumps(event).encode(), headers=otel.inject_headers()
            )
    assert producer.flush(30) == 0
    otel.shutdown()
    expected = {
        "tracefleet-verification",
        "tracefleet-validator",
        "tracefleet-incident",
        "tracefleet-persister",
        "tracefleet-sink",
    }
    seen = {}
    deadline = time.monotonic() + 60
    with httpx.Client(base_url="http://tempo:3200", timeout=10) as client:
        while time.monotonic() < deadline:
            for trace_id in ids:
                response = client.get("/api/traces/" + trace_id, headers={"Accept": "application/json"})
                if response.status_code != 200:
                    continue
                data = response.json()
                services = set()
                for batch in data.get("batches", data.get("resourceSpans", [])):
                    for attribute in batch.get("resource", {}).get("attributes", []):
                        if attribute["key"] == "service.name":
                            services.add(attribute["value"].get("stringValue"))
                seen[trace_id] = sorted(services)
                if expected <= services:
                    report = {
                        "verified": True,
                        "trace_id": trace_id,
                        "services": sorted(services),
                        "expected": sorted(expected),
                    }
                    Path("artifacts/trace-verification.json").write_text(json.dumps(report, indent=2) + "\n")
                    print(json.dumps(report))
                    return
            time.sleep(1)
    raise AssertionError(seen)


if __name__ == "__main__":
    main()
