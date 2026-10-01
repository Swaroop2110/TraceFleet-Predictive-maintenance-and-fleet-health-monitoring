"""Measure simulator throughput and authenticated API latency; report actual ceilings."""

from __future__ import annotations

import argparse
import json
import threading
import time
from pathlib import Path

import httpx
from sim.producer import RunPlan, run

from packages.common import config as cfg


def percentile(values, q):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * q))] if ordered else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--rate", type=float)
    parser.add_argument("--vehicles", type=int)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--api", default="http://api:8000")
    parser.add_argument("--report", type=Path, default=Path("artifacts/benchmark.json"))
    args = parser.parse_args()
    profile = cfg.load_profile(args.profile)
    plan = RunPlan(
        broker="kafka:9092",
        topic="telemetry.raw",
        seed=42,
        padding_bytes=profile.event_padding_bytes,
        duration_s=args.duration,
        vehicles=args.vehicles or profile.vehicles,
        target_events_per_s=args.rate or profile.target_events_per_s,
        workers=args.workers,
    )
    samples = []
    lag_samples = []
    failures = []
    stop = threading.Event()

    def sample():
        with httpx.Client(base_url=args.api, timeout=15) as client:
            auth = client.post(
                "/api/v1/auth/token",
                json={"username": "admin", "password": "tracefleet-demo", "tenant_id": "tenant_default"},
            )
            auth.raise_for_status()
            client.headers["Authorization"] = "Bearer " + auth.json()["access_token"]
            while not stop.is_set():
                try:
                    start = time.perf_counter()
                    response = client.get("/api/v1/incidents?limit=50")
                    response.raise_for_status()
                    samples.append((time.perf_counter() - start) * 1000)
                    lag = client.get("/api/v1/ops/consumer-lag")
                    lag.raise_for_status()
                    lag_samples.append(lag.json().get("total_lag", 0))
                except Exception as error:
                    failures.append(str(error))
                stop.wait(0.5)

    sampler = threading.Thread(target=sample)
    sampler.start()
    try:
        report = run(plan)
    finally:
        stop.set()
        sampler.join()
    report["api"] = {
        "samples": len(samples),
        "p95_ms": percentile(samples, 0.95),
        "p99_ms": percentile(samples, 0.99),
        "scope": "authenticated incident list sampled during ingestion",
        "errors": failures,
    }
    report["consumer_lag"] = {
        "samples": len(lag_samples),
        "maximum": max(lag_samples, default=0),
        "last": lag_samples[-1] if lag_samples else None,
        "source": "Prometheus-backed Operations API, scrape interval applies",
    }
    if not samples or failures:
        raise RuntimeError("benchmark API sampling failed: " + str(failures))
    with httpx.Client(base_url=args.api, timeout=15) as client:
        auth = client.post(
            "/api/v1/auth/token",
            json={"username": "admin", "password": "tracefleet-demo", "tenant_id": "tenant_default"},
        )
        auth.raise_for_status()
        client.headers["Authorization"] = "Bearer " + auth.json()["access_token"]
        start = time.monotonic()
        final_lag = None
        while time.monotonic() - start < 30:
            response = client.get("/api/v1/ops/consumer-lag")
            response.raise_for_status()
            final_lag = response.json().get("total_lag")
            if final_lag == 0:
                break
            time.sleep(1)
        report["consumer_lag"]["after_drain"] = final_lag
        report["consumer_lag"]["drain_seconds"] = round(time.monotonic() - start, 2)
        if final_lag != 0:
            raise RuntimeError("consumer lag did not drain within 30 seconds")
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, default=str))
    Path("docs/BENCHMARKS.md").write_text(
        "# Measured benchmarks\n\nMeasured on the local Docker environment. Target values are not claimed as achieved unless the measurements support them.\n\n```json\n"
        + json.dumps(report, indent=2, default=str)
        + "\n```\n"
    )
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
