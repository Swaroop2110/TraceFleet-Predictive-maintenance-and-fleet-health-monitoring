"""Sample actual stack health, consumer lag and process RSS for a bounded soak."""

import argparse
import json
import time
from pathlib import Path

import httpx

from scripts.recovery_check import compose


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=float, default=30)
    parser.add_argument("--api", default="http://localhost:8000")
    parser.add_argument("--report", type=Path, default=Path("artifacts/soak.json"))
    args = parser.parse_args()
    samples = []
    with httpx.Client(base_url=args.api, timeout=20) as client:
        token = client.post(
            "/api/v1/auth/token",
            json={"username": "admin", "password": "tracefleet-demo", "tenant_id": "tenant_default"},
        )
        token.raise_for_status()
        client.headers["Authorization"] = "Bearer " + token.json()["access_token"]
        start = time.monotonic()
        while True:
            health = client.get("/api/v1/health/ready")
            health.raise_for_status()
            lag = client.get("/api/v1/ops/consumer-lag")
            lag.raise_for_status()
            stats = compose("stats", "--no-stream", "--format", "{{json .}}")
            samples.append(
                {
                    "elapsed_s": round(time.monotonic() - start, 2),
                    "health": health.status_code,
                    "lag": lag.json(),
                    "containers": [json.loads(line) for line in stats.splitlines()],
                }
            )
            if time.monotonic() - start >= args.minutes * 60:
                break
            time.sleep(min(10, max(0.1, args.minutes * 60 - (time.monotonic() - start))))
    report = {
        "duration_minutes": args.minutes,
        "samples": samples,
        "verified": True,
        "scope": "dependency health, reported lag, container memory snapshots; inspect memory trend for longer runs",
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"verified": True, "samples": len(samples), "duration_minutes": args.minutes}))


if __name__ == "__main__":
    main()
