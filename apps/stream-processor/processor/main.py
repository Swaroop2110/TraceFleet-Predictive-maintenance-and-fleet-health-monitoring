"""Stream-processor entrypoint. One image, four roles.

    python apps/stream-processor/processor/main.py --role validator

Startup order matters. Configuration is validated first, so a bad threshold stops
the service rather than letting it run on defaults and emit wrong incidents. Then
logging, metrics, and tracing are installed before any role object is built, so a
failure during construction is still captured as structured output.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# The processor is launched as a script, so `processor.*` and `packages.*` need to
# resolve from the repository layout rather than an installed distribution.
_HERE = Path(__file__).resolve()
for _path in (_HERE.parents[1], _HERE.parents[3]):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from packages.common import config as cfg  # noqa: E402
from packages.common import logging as tf_logging  # noqa: E402
from packages.common import metrics, otel  # noqa: E402

ROLES = ("validator", "incident", "sink", "persister")

#: Distinct metrics ports so several roles can share a host during local runs.
DEFAULT_METRICS_PORT = {"validator": 9108, "incident": 9109, "sink": 9110, "persister": 9111}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TraceFleet stream processor")
    parser.add_argument("--role", choices=ROLES, required=True)
    parser.add_argument("--broker", default=os.getenv("KAFKA_BROKER", "localhost:9092"))
    parser.add_argument("--profile", default=os.getenv("PROFILE", "dev"))
    parser.add_argument("--metrics-port", type=int, default=None)
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate configuration and exit without connecting to Kafka",
    )
    return parser


def bootstrap(role: str, profile: str, metrics_port: int | None) -> None:
    """Install logging, metrics, and tracing, then validate configuration."""
    service = f"tracefleet-{role}"
    os.environ.setdefault("OTEL_SERVICE_NAME", service)
    os.environ.setdefault("PROFILE", profile)

    tf_logging.configure(service)
    tf_logging.bind(service=service, role=role, profile=profile)
    logger = tf_logging.get_logger("processor.main")

    # Fail fast on configuration. A processor that cannot trust its thresholds
    # should not start and begin publishing incidents based on defaults.
    try:
        bundle = cfg.load_all(profile=profile)
    except cfg.ConfigError as error:
        logger.error("configuration invalid", extra={"error": str(error)})
        raise SystemExit(2) from error

    otel.configure(service)
    port = metrics.start_metrics_server(metrics_port or DEFAULT_METRICS_PORT.get(role), service)
    logger.info(
        "bootstrap complete",
        extra={
            "metrics_port": port,
            "window_seconds": bundle["detection_rules"].window_seconds,
            "enabled_rules": sorted(bundle["detection_rules"].enabled_rules()),
            "target_events_per_s": bundle["profile"].target_events_per_s,
        },
    )


def build_role(role: str, broker: str):
    """Import and construct a role. Imports are local so an unused role's
    dependencies (ClickHouse, psycopg2) are not required to run another."""
    if role == "validator":
        from processor.validator import Validator

        return Validator(broker)
    if role == "incident":
        from processor.incident_processor import IncidentProcessor

        return IncidentProcessor(broker)
    if role == "sink":
        from processor.sink_clickhouse import Sink

        return Sink(broker)
    if role == "persister":
        from processor.incident_persister import IncidentPersister

        return IncidentPersister(broker)
    raise ValueError(f"unknown role {role!r}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    bootstrap(args.role, args.profile, args.metrics_port)
    logger = tf_logging.get_logger("processor.main")

    if args.check_config:
        logger.info("configuration check passed")
        return 0

    try:
        role = build_role(args.role, args.broker)
        role.run()
    except KeyboardInterrupt:
        logger.info("interrupted")
    except SystemExit:
        raise
    except Exception as error:
        logger.exception("role failed", extra={"role": args.role, "error": str(error)})
        return 1
    finally:
        metrics.mark_down(f"tracefleet-{args.role}")
        otel.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
