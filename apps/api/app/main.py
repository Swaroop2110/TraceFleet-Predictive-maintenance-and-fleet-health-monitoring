"""TraceFleet API: app factory and feature-aware module loader.

Modules are *discovered*, not imported by a hand-maintained list. Each package in
``app/modules`` exposes a ``router`` and declares the feature flags it needs; the
loader consults ``config/features.yaml`` and mounts only what is enabled. A
disabled feature's routes are absent from routing *and* from the OpenAPI document,
which is what makes "delete a folder and the rest still works" true rather than
aspirational.

A module may also expose ``register(context)`` for setup that is not a router —
background state, a cache warm-up, metrics. Both forms are supported because most
modules only need the router.

Construction is a factory, ``create_app()``, so the feature-matrix test can build
an app per flag combination in one process. The module-level ``app`` exists for
uvicorn.
"""

from __future__ import annotations

import importlib
import os
import pkgutil
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from packages.common import config as cfg
from packages.common import logging as tf_logging
from packages.common import otel
from packages.common.features import Features, feature_registry

LOGGER = tf_logging.get_logger("tracefleet.api")
PROJECT_ROOT = Path(__file__).resolve().parents[3]

API_PREFIX = "/api/v1"


@dataclass
class AppContext:
    """Handed to a module's ``register()`` so it never reaches for globals."""

    app: FastAPI
    features: Features
    config: dict[str, Any] = field(default_factory=dict)
    redis: Any = None
    registered_modules: list[str] = field(default_factory=list)
    skipped_modules: dict[str, list[str]] = field(default_factory=dict)


def _sync_redis() -> Any:
    """Synchronous Redis client, shared by the rate limiter and the read paths."""
    try:
        import redis

        from packages.common.dsn import redis_url

        client = redis.from_url(redis_url(), decode_responses=True, socket_timeout=2)
        client.ping()
        return client
    except Exception as error:
        LOGGER.warning("redis unavailable at startup", extra={"error": str(error)})
        return None


def register_modules(application: FastAPI, context: AppContext) -> list[str]:
    """Discover and mount every enabled module under ``app.modules``.

    Skips are recorded rather than silent: ``/health`` reports which modules were
    left out and why, so a missing endpoint is diagnosable without reading logs.
    """
    import apps.api.app.modules as modules_package

    context.features.reload()
    registered: list[str] = []
    skipped: dict[str, list[str]] = {}

    for info in sorted(pkgutil.iter_modules(modules_package.__path__), key=lambda item: item.name):
        if info.name.startswith("_"):
            continue
        module_name = f"{modules_package.__name__}.{info.name}"
        try:
            module = importlib.import_module(module_name)
        except Exception as error:
            # One broken module must not stop the whole API from starting; the
            # others may be all an operator needs to diagnose it.
            LOGGER.error("module failed to import", extra={"module": info.name, "error": str(error)})
            skipped[info.name] = [f"import_error: {error}"]
            continue

        required = tuple(getattr(module, "FEATURE_FLAGS", ()))
        missing = [flag for flag in required if not context.features.is_enabled(flag)]
        if missing:
            LOGGER.info("module skipped", extra={"module": info.name, "disabled_features": missing})
            skipped[info.name] = missing
            continue

        router = getattr(module, "router", None)
        if router is not None:
            application.include_router(router, prefix=API_PREFIX)

        setup = getattr(module, "register", None)
        if callable(setup):
            try:
                setup(context)
            except Exception as error:
                LOGGER.error("module register() failed", extra={"module": info.name, "error": str(error)})
                skipped[info.name] = [f"register_error: {error}"]
                continue

        if router is not None or callable(setup):
            registered.append(info.name)

    context.registered_modules = registered
    context.skipped_modules = skipped
    application.state.registered_modules = registered
    application.state.skipped_modules = skipped
    return registered


def create_app(*, features: Features | None = None, validate_config: bool = True) -> FastAPI:
    """Build the application.

    ``validate_config`` is on by default and deliberately fatal: an API serving
    thresholds from a malformed file would report configuration that does not match
    what the detectors are using.
    """
    service = os.getenv("OTEL_SERVICE_NAME", "tracefleet-api")
    tf_logging.configure(service)

    config_bundle: dict[str, Any] = {}
    if validate_config:
        config_bundle = cfg.load_all()

    registry = features or feature_registry
    redis_client = _sync_redis()
    context = AppContext(app=None, features=registry, config=config_bundle, redis=redis_client)  # type: ignore[arg-type]

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        from apps.api.app.db.session import SchemaNotReady, init_db

        try:
            report = init_db()
            application.state.schema = report
        except SchemaNotReady as error:
            # Not fatal. Health reports unready and says to run migrations, which
            # is more useful than a container that crash-loops before it can
            # explain itself.
            LOGGER.error("database schema not ready", extra={"error": str(error)})
            application.state.schema = {"error": str(error)}
        except Exception as error:
            LOGGER.error("database unreachable at startup", extra={"error": str(error)})
            application.state.schema = {"error": str(error)}

        _warn_on_insecure_defaults()
        yield
        from apps.api.app.ws.manager import manager

        await manager.shutdown()
        otel.shutdown()

    application = FastAPI(
        title="TraceFleet API",
        version=os.getenv("TRACEFLEET_VERSION", "1.0.0"),
        description=(
            "Fleet Incident Reconstruction & Causal Intelligence Platform.\n\n"
            "Incidents are *detected* from telemetry against configured thresholds. "
            "Evidence and contributing-factor scores are deterministic; AI narrative "
            "is explanatory only and never assigns fault."
        ),
        lifespan=lifespan,
        docs_url="/docs",
        openapi_url="/openapi.json",
    )
    context.app = application
    application.state.context = context

    otel.configure(service)
    _install_instrumentation(application)

    from apps.api.app.core.errors import register_exception_handlers
    from apps.api.app.core.middleware import install_middleware

    install_middleware(application, redis_client=redis_client)
    register_exception_handlers(application)

    _register_platform_routes(application, context)
    register_modules(application, context)

    LOGGER.info(
        "api ready",
        extra={
            "modules": context.registered_modules,
            "skipped": context.skipped_modules,
            "features_enabled": sorted(name for name, detail in registry.get_all().items() if detail.get("enabled")),
        },
    )
    return application


def _install_instrumentation(application: FastAPI) -> None:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(application, excluded_urls="/api/v1/health,/metrics")
    except Exception as error:  # pragma: no cover
        LOGGER.debug("fastapi instrumentation unavailable", extra={"error": str(error)})
    try:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator().instrument(application).expose(application, endpoint="/metrics", include_in_schema=False)
    except Exception as error:  # pragma: no cover
        LOGGER.debug("prometheus instrumentation unavailable", extra={"error": str(error)})


def _warn_on_insecure_defaults() -> None:
    """Refuse to run in production with local-demo defaults still in place.

    A convenient default that silently survives into a deployment is how a demo
    becomes an incident. In production these are fatal; elsewhere they are warnings.
    """
    from apps.api.app.core.security import allow_anonymous
    from packages.common.tokens import load_keypair

    problems: list[str] = []
    if allow_anonymous():
        problems.append("TRACEFLEET_ALLOW_ANONYMOUS is enabled")
    if os.getenv("DEMO_USER_PASSWORD", "tracefleet-demo") == "tracefleet-demo" and os.getenv(
        "TRACEFLEET_SEED_DEMO_USERS", "true"
    ).lower() in {"1", "true", "yes"}:
        problems.append("demo accounts exist with the default password")
    try:
        if load_keypair().ephemeral:
            problems.append("JWT signing key is ephemeral and not shared between replicas")
    except Exception:
        problems.append("JWT signing key could not be loaded")

    if not problems:
        return
    if os.getenv("TRACEFLEET_ENV", "development").lower() == "production":
        raise RuntimeError("refusing to start in production with insecure defaults: " + "; ".join(problems))
    LOGGER.warning("insecure local defaults in use", extra={"problems": problems})


def _register_platform_routes(application: FastAPI, context: AppContext) -> None:
    """Routes that are not owned by a feature module: health, config, metrics."""
    from apps.api.app.core.security import Principal, get_current_principal, require

    @application.get(f"{API_PREFIX}/health", tags=["platform"])
    def health(request: Request) -> JSONResponse:
        """Liveness plus a dependency summary. Public, and says nothing sensitive."""
        schema = getattr(application.state, "schema", {}) or {}
        healthy = "error" not in schema
        body = {
            "status": "ok" if healthy else "degraded",
            "version": application.version,
            "modules": context.registered_modules,
            "skipped_modules": context.skipped_modules,
            "schema": {k: v for k, v in schema.items() if k != "rls"},
            "dependencies": _dependency_health(context),
        }
        return JSONResponse(status_code=200 if healthy else 503, content=body)

    @application.get(f"{API_PREFIX}/health/live", tags=["platform"])
    def liveness() -> dict[str, str]:
        """Process is running. Never touches a dependency, so a database blip
        cannot cause the orchestrator to kill a working container."""
        return {"status": "alive"}

    @application.get(f"{API_PREFIX}/health/ready", tags=["platform"])
    def readiness() -> JSONResponse:
        """Ready to serve traffic: schema present and reachable."""
        schema = getattr(application.state, "schema", {}) or {}
        ready = "error" not in schema
        return JSONResponse(
            status_code=200 if ready else 503,
            content={"status": "ready" if ready else "not_ready", **schema},
        )

    @application.get(f"{API_PREFIX}/config/features", tags=["platform"])
    def get_features() -> dict[str, Any]:
        """Feature manifest. Drives the frontend's navigation and route registry."""
        context.features.reload()
        return context.features.get_all()

    @application.get(f"{API_PREFIX}/config/rules", tags=["platform"])
    def get_rules(_: Principal = Depends(get_current_principal)) -> dict[str, Any]:
        """Active detection thresholds.

        Exposed so the UI can label evidence with the threshold that was crossed
        instead of hard-coding a copy that can drift from the engine.
        """
        rules = cfg.detection_rules()
        return {
            "version": rules.version,
            "window_seconds": rules.window_seconds,
            "rules": {name: rule.model_dump() for name, rule in rules.rules.items()},
            "incident_trigger": rules.incident_trigger.model_dump(),
            "severity": {
                "matrix": [rule.model_dump(exclude_none=True) for rule in cfg.severity().matrix],
                "disclaimer": cfg.severity().disclaimer,
            },
            "factors": {name: factor.model_dump() for name, factor in cfg.factors().factors.items()},
        }

    @application.get(f"{API_PREFIX}/config/plugins", tags=["platform"])
    def get_plugins(_: Principal = Depends(require("admin:config"))) -> dict[str, Any]:
        """Registered plug-ins, so an operator can confirm what is actually loaded."""
        return _plugin_inventory()


def _dependency_health(context: AppContext) -> dict[str, Any]:
    from apps.api.app.db.session import pool_status

    health: dict[str, Any] = {}

    try:
        from sqlalchemy import text

        from apps.api.app.db.session import engine

        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        health["postgres"] = {"status": "up", "pool": pool_status()}
    except Exception as error:
        health["postgres"] = {"status": "down", "error": str(error)[:200]}

    if context.redis is not None:
        try:
            context.redis.ping()
            health["redis"] = {"status": "up"}
        except Exception as error:
            health["redis"] = {"status": "down", "error": str(error)[:200]}
    else:
        health["redis"] = {"status": "unconfigured"}

    return health


def _plugin_inventory() -> dict[str, Any]:
    """Inventory of the detection plug-ins the processor has registered.

    Imported lazily and tolerantly: the API and the processor are separate
    deployables, and the API must not fail because a processor-only dependency is
    absent from its image.
    """
    inventory: dict[str, Any] = {}
    for label, module_path, attribute in (
        ("detectors", "processor.detectors", "detector_registry"),
        ("correlation_policies", "processor.correlation", "correlation_registry"),
        ("severity_policies", "processor.severity", "severity_registry"),
        ("factor_scorers", "processor.factors", "factor_registry"),
        ("enrichers", "processor.enrichers", "enricher_registry"),
    ):
        try:
            module = importlib.import_module(module_path)
            inventory[label] = getattr(module, attribute).metadata()
        except Exception as error:
            inventory[label] = {"error": f"unavailable: {error}"}

    try:
        from apps.api.app.modules.intelligence import provider_inventory

        inventory["llm_providers"] = provider_inventory()
    except Exception:
        inventory["llm_providers"] = {}
    return inventory


#: Module-level instance for uvicorn (`apps.api.app.main:app`).
app = create_app()
