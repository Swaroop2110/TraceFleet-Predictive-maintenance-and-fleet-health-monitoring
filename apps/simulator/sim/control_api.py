"""HTTP control surface for the simulator (PRD 7).

Lets the 5-minute demo inject a specific incident on cue instead of waiting for a
probabilistic scenario to fire, and lets a load test change rate or trigger a burst
without a restart.

This is a test-harness control plane, not a product surface. It binds to localhost
by default, is disabled unless ``SIMULATOR_CONTROL_ENABLED`` is set, and accepts only
registered scenario names — it publishes to Kafka, so an unconstrained endpoint here
would be a way to inject arbitrary telemetry.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any

from packages.common import kafka as kafka_helpers
from packages.common import logging as tf_logging
from packages.common import metrics, otel
from packages.common.timeutil import now, to_iso
from sim.ground_truth import GroundTruthWriter
from sim.scenarios.registry import available_scenarios, generate

LOGGER = tf_logging.get_logger(__name__)


def is_enabled() -> bool:
    return os.getenv("SIMULATOR_CONTROL_ENABLED", "false").lower() in {"1", "true", "yes"}


def create_app(
    broker: str,
    *,
    topic: str = kafka_helpers.TELEMETRY_RAW,
    run_id: str = "control",
    tenant_id: str = "tenant_default",
    state: dict[str, Any] | None = None,
) -> Any:
    """Build the control application.

    FastAPI is imported lazily so the simulator can run in an image that does not
    carry the web stack.
    """
    from fastapi import FastAPI, HTTPException
    from pydantic import BaseModel, Field

    app = FastAPI(
        title="TraceFleet Simulator Control",
        version="1.0.0",
        description=(
            "Test-harness control plane for the telemetry simulator. Not a product "
            "API: it publishes directly to the raw telemetry topic."
        ),
    )
    shared = state if state is not None else {"target_rate": 0.0, "burst_until": None}
    lock = threading.Lock()
    producer = kafka_helpers.build_producer(**{"bootstrap.servers": broker})
    writer = GroundTruthWriter(run_id=run_id, tenant_id=tenant_id)

    class InjectRequest(BaseModel):
        scenario: str = Field(description="A registered scenario name")
        vehicle_id: str | None = Field(default=None, max_length=64)
        count: int = Field(default=1, ge=1, le=100, description="How many instances to inject")
        write_ground_truth: bool = True

    class RateRequest(BaseModel):
        events_per_second: float = Field(gt=0, le=1_000_000)

    class BurstRequest(BaseModel):
        multiplier: float = Field(default=3.0, ge=1.0, le=20.0)
        duration_s: int = Field(default=300, ge=1, le=3600)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "broker": broker,
            "topic": topic,
            "run_id": run_id,
            "scenarios": available_scenarios(),
            "state": _snapshot(shared),
        }

    @app.get("/scenarios")
    def scenarios() -> dict[str, Any]:
        """Registered scenarios with their declared ground-truth expectations."""
        from packages.common import config as cfg
        from sim.scenarios.registry import scenario_registry, validate_against_config

        configured = cfg.scenarios().scenarios
        return {
            "scenarios": {
                name: {
                    "module": entry.module,
                    "description": entry.metadata.get("description", ""),
                    "expectations": (configured[name].model_dump() if name in configured else None),
                }
                for name, entry in scenario_registry.entries().items()
            },
            "consistency": validate_against_config(),
        }

    @app.post("/scenario/inject")
    def inject(request: InjectRequest) -> dict[str, Any]:
        """Publish one or more instances of a scenario on demand."""
        if request.scenario not in available_scenarios():
            raise HTTPException(
                status_code=422,
                detail={
                    "error": f"unknown scenario: {request.scenario}",
                    "available": available_scenarios(),
                },
            )

        published = 0
        vehicles: list[str] = []
        truths = []
        stamp = now().strftime("%H%M%S")

        for index in range(request.count):
            # A unique vehicle id per injection, so repeated demo injections do not
            # collide with the cooldown window on a single vehicle.
            vehicle_id = request.vehicle_id or f"veh_inject_{stamp}_{index:03d}"
            vehicles.append(vehicle_id)
            result = generate(request.scenario, vehicle_id, start_seq=index * 1000)
            for event in result.events:
                payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
                with otel.producer_span("simulator.inject", **{"scenario": request.scenario}):
                    producer.produce(topic, key=event["vehicle_id"], value=payload, headers=otel.inject_headers())
                published += 1
                metrics.producer_events_total.labels(scenario=request.scenario).inc()
            if result.ground_truth is not None and request.write_ground_truth:
                truths.append(result.ground_truth)

        producer.flush(15)
        written = writer.write_many(truths) if truths else 0

        LOGGER.info(
            "scenario injected",
            extra={
                "scenario": request.scenario,
                "instances": request.count,
                "events": published,
                "ground_truth_rows": written,
            },
        )
        return {
            "scenario": request.scenario,
            "instances": request.count,
            "events_published": published,
            "vehicles": vehicles,
            "ground_truth_written": written,
            "injected_at": to_iso(now()),
        }

    @app.post("/rate")
    def set_rate(request: RateRequest) -> dict[str, Any]:
        """Change the target event rate.

        Recorded in shared state; the producer workers read it on their next cycle.
        """
        with lock:
            previous = shared.get("target_rate", 0.0)
            shared["target_rate"] = request.events_per_second
        LOGGER.info("rate changed", extra={"from": previous, "to": request.events_per_second})
        return {"previous_events_per_second": previous, "events_per_second": request.events_per_second}

    @app.post("/burst")
    def trigger_burst(request: BurstRequest) -> dict[str, Any]:
        """Start a burst window."""
        from packages.common.timeutil import add_seconds

        with lock:
            shared["burst_multiplier"] = request.multiplier
            shared["burst_until"] = add_seconds(now(), request.duration_s)
        LOGGER.info(
            "burst requested",
            extra={"multiplier": request.multiplier, "duration_s": request.duration_s},
        )
        return {
            "multiplier": request.multiplier,
            "duration_s": request.duration_s,
            "until": to_iso(shared["burst_until"]),
        }

    @app.get("/state")
    def get_state() -> dict[str, Any]:
        return _snapshot(shared)

    @app.on_event("shutdown")
    def shutdown() -> None:
        producer.flush(10)
        writer.close()

    return app


def _snapshot(state: dict[str, Any]) -> dict[str, Any]:
    burst_until = state.get("burst_until")
    return {
        "target_rate": state.get("target_rate", 0.0),
        "burst_multiplier": state.get("burst_multiplier", 1.0),
        "burst_until": to_iso(burst_until) if burst_until else None,
        "bursting": bool(burst_until and burst_until > now()),
    }


def serve(
    broker: str,
    *,
    host: str | None = None,
    port: int | None = None,
    run_id: str = "control",
    tenant_id: str = "tenant_default",
) -> None:
    """Run the control API. Blocks."""
    import uvicorn

    # Localhost by default. This endpoint can publish telemetry, so it should not be
    # reachable off-host unless someone deliberately binds it wider.
    resolved_host = host or os.getenv("SIMULATOR_CONTROL_HOST", "127.0.0.1")
    resolved_port = int(port or os.getenv("SIMULATOR_CONTROL_PORT", "8100"))
    app = create_app(broker, run_id=run_id, tenant_id=tenant_id)
    LOGGER.info("simulator control api listening", extra={"host": resolved_host, "port": resolved_port})
    uvicorn.run(app, host=resolved_host, port=resolved_port, log_config=None)
