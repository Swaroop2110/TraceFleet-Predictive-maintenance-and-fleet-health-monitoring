"""Simulator entrypoint.

    python apps/simulator/sim/main.py --profile dev --duration 600 --seed 42
    python apps/simulator/sim/main.py --scenario collision_like --batches 1
    python apps/simulator/sim/main.py --seed-fleet --profile full
    python apps/simulator/sim/main.py --control-api

Three modes:

* **stream** (default) — the kinematic model driving a sharded fleet at the profile's
  target rate, with optional burst. This is the load generator.
* **scenario** — a specific pattern injected a fixed number of times, with ground
  truth written. This is what the demo and the accuracy evaluation use.
* **seed / control-api** — provision the vehicle registry, or expose the HTTP control
  plane.

Everything is deterministic under ``--seed`` so a benchmark or an accuracy
measurement can be repeated.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

_HERE = Path(__file__).resolve()
for _path in (_HERE.parents[1], _HERE.parents[3]):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from packages.common import config as cfg  # noqa: E402
from packages.common import kafka as kafka_helpers  # noqa: E402
from packages.common import logging as tf_logging  # noqa: E402
from packages.common import metrics, otel  # noqa: E402
from packages.common.timeutil import now  # noqa: E402

LOGGER = tf_logging.get_logger("sim.main")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="TraceFleet telemetry simulator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("broker", nargs="?", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--profile", default=os.getenv("PROFILE", "dev"), choices=["dev", "demo", "full"])
    parser.add_argument("--topic", default=kafka_helpers.TELEMETRY_RAW)
    parser.add_argument("--tenant-id", default=os.getenv("DEFAULT_TENANT_ID", "tenant_default"))
    parser.add_argument("--seed", type=int, default=42, help="Makes a run reproducible")

    parser.add_argument("--scenario", help="Inject a named scenario instead of streaming")
    parser.add_argument("--batches", type=int, default=1, help="Scenario instances (0 = until interrupted)")
    parser.add_argument("--vehicle-prefix", default="veh_sim")

    parser.add_argument("--duration", type=float, default=0.0, help="Stream duration in seconds (0 = forever)")
    parser.add_argument("--vehicles", type=int, default=0, help="Override the profile vehicle count")
    parser.add_argument("--rate", type=float, default=0.0, help="Override the profile target events/s")
    parser.add_argument("--workers", type=int, default=0, help="0 = profile setting, else CPU count")
    parser.add_argument("--burst", action="store_true", help="Run a burst window during the stream")
    parser.add_argument("--burst-after", type=float, default=30.0)

    parser.add_argument("--seed-fleet", action="store_true", help="Seed tenants/fleets/vehicles and exit")
    parser.add_argument("--control-api", action="store_true", help="Serve the HTTP control plane")
    parser.add_argument("--list-scenarios", action="store_true")
    parser.add_argument("--check-config", action="store_true")

    parser.add_argument("--ground-truth", type=Path, help="JSONL fallback path for labels")
    parser.add_argument("--no-ground-truth", action="store_true", help="Skip writing labels")
    parser.add_argument("--report", type=Path, help="Write the run report as JSON")
    parser.add_argument("--metrics-port", type=int, default=9112)
    return parser


def bootstrap(args: argparse.Namespace) -> cfg.ProfileConfig:
    os.environ.setdefault("OTEL_SERVICE_NAME", "tracefleet-simulator")
    os.environ["PROFILE"] = args.profile
    tf_logging.configure("tracefleet-simulator")
    tf_logging.bind(service="tracefleet-simulator", profile=args.profile)

    try:
        cfg.load_all(profile=args.profile)
    except cfg.ConfigError as error:
        LOGGER.error("configuration invalid", extra={"error": str(error)})
        raise SystemExit(2) from error

    otel.configure("tracefleet-simulator")
    metrics.start_metrics_server(args.metrics_port, "tracefleet-simulator")
    return cfg.load_profile(args.profile)


def run_scenario_mode(args: argparse.Namespace, profile: cfg.ProfileConfig) -> dict[str, Any]:
    """Inject a named scenario, or the configured mix, writing ground truth."""
    from sim.ground_truth import GroundTruthWriter, default_jsonl_path
    from sim.scenarios.registry import available_scenarios, generate

    scenarios_config = cfg.scenarios()
    if args.scenario in ("mixed", "mix"):
        names = scenarios_config.default_mix or available_scenarios()
    else:
        names = [args.scenario]

    unknown = [name for name in names if name not in available_scenarios()]
    if unknown:
        LOGGER.error(
            "unknown scenario",
            extra={"requested": unknown, "available": available_scenarios()},
        )
        raise SystemExit(2)

    producer = kafka_helpers.build_producer(**{"bootstrap.servers": args.broker})
    run_identifier = f"sim-{now().strftime('%Y%m%d%H%M%S')}-{args.seed}"
    writer = GroundTruthWriter(
        run_id=run_identifier,
        tenant_id=args.tenant_id,
        jsonl_path=args.ground_truth or default_jsonl_path(),
        enabled=not args.no_ground_truth,
    )

    LOGGER.info(
        "injecting scenarios",
        extra={"scenarios": names, "batches": args.batches, "run_id": run_identifier},
    )

    published = 0
    truths = []
    batch = 0
    try:
        while args.batches == 0 or batch < args.batches:
            name = names[batch % len(names)]
            params = dict(scenarios_config.scenarios[name].params) if name in scenarios_config.scenarios else {}
            params["padding_bytes"] = profile.event_padding_bytes
            vehicle_id = f"{args.vehicle_prefix}_{name}_{batch:05d}"

            result = generate(name, vehicle_id, start_seq=batch * 1000, **params)
            # Scenario vehicles must be discoverable from the dashboard registry.
            import psycopg2

            from packages.common.dsn import psycopg2_kwargs

            with psycopg2.connect(**psycopg2_kwargs(owner=True)) as db, db.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO vehicle(vehicle_id,tenant_id,fleet_id,vin,oem,model,model_year) VALUES(%s,%s,%s,%s,'SIM','Scenario',2026) ON CONFLICT(vehicle_id) DO NOTHING",
                    (vehicle_id, args.tenant_id, "fleet_default", result.events[0]["vin"]),
                )
            for event in result.events:
                payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
                _produce(producer, args.topic, event["vehicle_id"], payload)
                published += 1
                metrics.producer_events_total.labels(scenario=name).inc()
            if result.ground_truth is not None:
                truths.append(result.ground_truth)
            batch += 1
    except KeyboardInterrupt:
        LOGGER.info("interrupted")
    finally:
        producer.flush(30)
        written = writer.write_many(truths)
        writer.close()

    report = {
        "mode": "scenario",
        "run_id": run_identifier,
        "scenarios": names,
        "instances": batch,
        "events_published": published,
        "ground_truth_written": written,
        "ground_truth_expected": len(truths),
    }
    LOGGER.info("scenario injection complete", extra=report)
    return report


def run_stream_mode(args: argparse.Namespace, profile: cfg.ProfileConfig) -> dict[str, Any]:
    """Stream baseline telemetry from the kinematic model at the profile rate."""
    from sim.producer import RunPlan, resolve_workers, run

    plan = RunPlan(
        broker=args.broker,
        topic=args.topic,
        vehicles=args.vehicles or profile.vehicles,
        target_events_per_s=args.rate or profile.target_events_per_s,
        duration_s=args.duration,
        workers=resolve_workers(args.workers, profile.simulator_workers),
        seed=args.seed,
        padding_bytes=profile.event_padding_bytes,
        burst_multiplier=profile.burst_multiplier if args.burst else 1.0,
        burst_after_s=args.burst_after if args.burst else 0.0,
        burst_duration_s=profile.burst_duration_s if args.burst else 0.0,
        tenant_id=args.tenant_id,
    )
    report = run(plan)
    report["mode"] = "stream"
    report["profile"] = profile.name
    return report


def run_seed_mode(args: argparse.Namespace, profile: cfg.ProfileConfig) -> dict[str, Any]:
    """Seed the vehicle registry."""
    from sim.fleet_seed import counts, seed

    target = args.vehicles or profile.vehicles
    LOGGER.info(
        "seeding fleet",
        extra={"vehicles": target, "tenants": profile.tenants, "fleets_per_tenant": profile.fleets_per_tenant},
    )
    report = seed(
        target,
        tenants=profile.tenants,
        fleets_per_tenant=profile.fleets_per_tenant,
        seed_value=args.seed,
    )
    report["mode"] = "seed"
    report["totals"] = counts()
    # The PRD sets a two-minute budget for 100K vehicles; state the result rather
    # than assume it was met.
    report["within_two_minutes"] = report["elapsed_seconds"] <= 120
    return report


def _produce(producer: Any, topic: str, key: str, payload: bytes) -> None:
    """Publish, waiting rather than dropping when the local queue is full."""
    with otel.producer_span("simulator.publish", **{"messaging.destination.name": topic}):
        headers = otel.inject_headers()
        while True:
            try:
                producer.produce(topic, key=key, value=payload, headers=headers)
                producer.poll(0)
                return
            except BufferError:
                producer.poll(0.1)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.list_scenarios:
        tf_logging.configure("tracefleet-simulator")
        from sim.scenarios.registry import scenario_registry, validate_against_config

        discovered = scenario_registry.metadata()
        consistency = validate_against_config()
        print(json.dumps({"scenarios": discovered, "consistency": consistency}, indent=2))
        return 0 if not any(consistency.values()) else 1

    profile = bootstrap(args)

    if args.check_config:
        from sim.scenarios.registry import validate_against_config

        consistency = validate_against_config()
        problems = {key: value for key, value in consistency.items() if value}
        if problems:
            LOGGER.error("scenario registry and config disagree", extra=problems)
            return 1
        LOGGER.info("configuration and scenario registry are consistent")
        return 0

    started = time.perf_counter()
    try:
        if args.seed_fleet:
            report = run_seed_mode(args, profile)
        elif args.control_api:
            from sim import control_api

            if not control_api.is_enabled():
                LOGGER.error("control api is disabled; set SIMULATOR_CONTROL_ENABLED=true to enable it")
                return 2
            control_api.serve(args.broker, run_id=f"control-{args.seed}", tenant_id=args.tenant_id)
            return 0
        elif args.scenario:
            report = run_scenario_mode(args, profile)
        else:
            report = run_stream_mode(args, profile)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        LOGGER.info("interrupted")
        return 0
    except Exception as error:
        LOGGER.exception("simulator failed", extra={"error": str(error)})
        return 1
    finally:
        otel.shutdown()

    report["wall_clock_seconds"] = round(time.perf_counter() - started, 2)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        LOGGER.info("report written", extra={"path": str(args.report)})

    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
