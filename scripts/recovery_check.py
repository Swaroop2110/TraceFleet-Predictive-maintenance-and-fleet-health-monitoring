"""Exercise a real Compose interruption, reconcile incident + telemetry, save evidence."""

import argparse
import json
import subprocess
import time
import uuid
from pathlib import Path


def compose(*args):
    result = subprocess.run(["docker", "compose", *args], text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def remote(code):
    return compose("exec", "-T", "-e", "OTEL_SDK_DISABLED=true", "api", "python", "-c", code)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", choices=["incident-processor", "kafka"], default="incident-processor")
    parser.add_argument("--report", type=Path, default=Path("artifacts/chaos.json"))
    args = parser.parse_args()
    vehicle = "chaos_" + uuid.uuid4().hex[:10]
    setup = f"""import psycopg2,json
from packages.common.dsn import psycopg2_kwargs
from packages.common import kafka
from sim.scenarios.registry import generate
from packages.common.timeutil import now,to_iso
from datetime import timedelta
v={vehicle!r}
with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db, db.cursor() as c:
 c.execute("INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,'tenant_default','fleet_default','1HGCM82633A004352','TEST','Recovery',2026)",(v,))
r=generate('collision_like',v,start_time=now())
open('/tmp/'+v+'.json','w').write(json.dumps(r.events))
p=kafka.build_producer()
for e in r.events[:4]: p.produce(kafka.TELEMETRY_RAW,key=v,value=json.dumps(e).encode())
assert p.flush(30)==0
"""  # nosec B608 # static identifiers, parameter-bound values
    remote(setup)
    # Wait for the first half to be checkpointed before abrupt process loss.
    remote(f"""import redis,time,os
r=redis.from_url(os.environ['REDIS_URL'])
for _ in range(100):
 if r.exists('tf:tenant_default:veh:{vehicle}:checkpoint'): break
 time.sleep(.2)
else: raise AssertionError('no checkpoint before interruption')
""")
    start = time.monotonic()
    compose("kill", args.service)
    try:
        compose("up", "-d", "--no-deps", "--wait", "--wait-timeout", "90", args.service)
        remote(f"""import json
from packages.common import kafka
p=kafka.build_producer()
for e in json.load(open('/tmp/{vehicle}.json'))[4:]: p.produce(kafka.TELEMETRY_RAW,key={vehicle!r},value=json.dumps(e).encode())
assert p.flush(30)==0
""")
        outcome = remote(f"""import psycopg2,time,json,clickhouse_connect,os
from packages.common.dsn import psycopg2_kwargs
c=clickhouse_connect.get_client(host=os.environ['CLICKHOUSE_HOST'],database='tracefleet')
with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db,db.cursor() as q:
 for _ in range(450):
  q.execute("SELECT count(*),max(severity) FROM incident WHERE vehicle_id=%s",({vehicle!r},))
  incidents,severity=q.fetchone()
  events=c.query("SELECT count() FROM telemetry_events FINAL WHERE vehicle_id={{v:String}}",parameters={{'v':{vehicle!r}}}).first_row[0]
  if incidents==1 and severity=='CRITICAL' and events==7: break
  time.sleep(.2)
 else: raise AssertionError((incidents,severity,events))
 print(json.dumps({{'incidents':incidents,'severity':severity,'unique_events':events,'expected_events':7}}))
""")  # nosec B608 # static identifiers, parameter-bound values
        report = dict(
            json.loads(outcome.strip().splitlines()[-1]),
            service=args.service,
            recovery_s=round(time.monotonic() - start, 3),
            verified=True,
        )
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))
    finally:
        compose("up", "-d", "--no-deps", args.service)


if __name__ == "__main__":
    main()
