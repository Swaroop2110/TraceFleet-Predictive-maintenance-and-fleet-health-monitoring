"""OpenTelemetry bootstrap and Kafka context propagation.

A single trace has to survive the hop through Kafka, which is not an HTTP call
and carries no headers of its own unless we put them there. :func:`inject_headers`
writes ``traceparent`` into the message headers and :func:`extract_context`
reads it back on the consumer side, so one span tree spans
simulator -> validator -> incident processor -> persister -> API.

Every function degrades quietly when the SDK or collector is absent; tracing is
diagnostic and must never take the pipeline down.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

try:
    from opentelemetry import context as otel_context
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.propagate import extract, inject
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
    from opentelemetry.trace import SpanKind, Status, StatusCode

    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    OTEL_AVAILABLE = False
    trace = None  # type: ignore[assignment]
    SpanKind = None  # type: ignore[misc, assignment]

_provider: Any = None


def configure(service: str | None = None, *, environment: str | None = None) -> Any:
    """Install a tracer provider exporting over OTLP/gRPC. Idempotent."""
    global _provider
    if not OTEL_AVAILABLE:
        return None
    if _provider is not None:
        return _provider

    service_name = service or os.getenv("OTEL_SERVICE_NAME") or "tracefleet"
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": os.getenv("TRACEFLEET_VERSION") or "1.0.0",
            "deployment.environment": environment or os.getenv("OTEL_ENVIRONMENT") or "dev",
        }
    )
    provider = TracerProvider(
        resource=resource, sampler=ParentBased(TraceIdRatioBased(float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "0.1"))))
    )
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    if os.getenv("OTEL_SDK_DISABLED", "").lower() not in {"1", "true", "yes"}:
        try:  # noqa: SIM105 - best-effort shutdown/fallback boundary
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
        except Exception:  # pragma: no cover - collector absent in unit tests
            pass
    trace.set_tracer_provider(provider)
    _provider = provider
    return provider


def tracer(name: str = "tracefleet") -> Any:
    if not OTEL_AVAILABLE:
        return _NullTracer()
    return trace.get_tracer(name)


def shutdown() -> None:
    """Flush pending spans. Called from service shutdown handlers."""
    if _provider is not None:
        try:  # noqa: SIM105 - best-effort shutdown/fallback boundary
            _provider.shutdown()
        except Exception:  # pragma: no cover
            pass


class _NullSpan:
    def set_attribute(self, *_: Any, **__: Any) -> None:
        pass

    def set_attributes(self, *_: Any, **__: Any) -> None:
        pass

    def add_event(self, *_: Any, **__: Any) -> None:
        pass

    def set_status(self, *_: Any, **__: Any) -> None:
        pass

    def record_exception(self, *_: Any, **__: Any) -> None:
        pass

    def get_span_context(self) -> Any:
        return None


class _NullTracer:
    @contextmanager
    def start_as_current_span(self, *_: Any, **__: Any) -> Iterator[_NullSpan]:
        yield _NullSpan()


def inject_headers(headers: list[tuple[str, bytes]] | None = None) -> list[tuple[str, bytes]]:
    """Return Kafka headers carrying the active trace context."""
    result: list[tuple[str, bytes]] = list(headers or [])
    if not OTEL_AVAILABLE:
        return result
    carrier: dict[str, str] = {}
    inject(carrier)
    existing = {key for key, _ in result}
    for key, value in carrier.items():
        if key not in existing:
            result.append((key, value.encode("utf-8")))
    return result


def extract_context(headers: Sequence[tuple[str, bytes | None]] | None) -> Any:
    """Rebuild a trace context from Kafka message headers."""
    if not OTEL_AVAILABLE or not headers:
        return None
    carrier = {
        key: (value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value))
        for key, value in headers
        if value is not None
    }
    if not carrier:
        return None
    try:
        return extract(carrier)
    except Exception:  # pragma: no cover
        return None


@contextmanager
def consumer_span(
    name: str,
    headers: Sequence[tuple[str, bytes | None]] | None = None,
    **attributes: Any,
) -> Iterator[Any]:
    """Continue an upstream trace for a message consumed from Kafka."""
    if not OTEL_AVAILABLE:
        yield _NullSpan()
        return
    parent = extract_context(headers)
    token = otel_context.attach(parent) if parent is not None else None
    span_tracer = trace.get_tracer("tracefleet.processor")
    try:
        with span_tracer.start_as_current_span(name, kind=SpanKind.CONSUMER) as span:
            for key, value in attributes.items():
                if value is not None:
                    span.set_attribute(key, value)
            try:
                yield span
            except Exception as error:
                span.record_exception(error)
                span.set_status(Status(StatusCode.ERROR, str(error)))
                raise
    finally:
        if token is not None:
            otel_context.detach(token)


@contextmanager
def producer_span(
    name: str, *, headers: Sequence[tuple[str, bytes | None]] | None = None, **attributes: Any
) -> Iterator[Any]:
    """Span around a Kafka publish, so the child consumer span links back."""
    if not OTEL_AVAILABLE:
        yield _NullSpan()
        return
    span_tracer = trace.get_tracer("tracefleet.processor")
    with span_tracer.start_as_current_span(name, context=extract_context(headers), kind=SpanKind.PRODUCER) as span:
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)
        yield span


def current_trace_id() -> str | None:
    """Hex trace id of the active span, for stamping onto canonical events."""
    if not OTEL_AVAILABLE:
        return None
    span = trace.get_current_span()
    context = span.get_span_context() if span else None
    if context is None or not getattr(context, "trace_id", 0):
        return None
    return format(context.trace_id, "032x")
