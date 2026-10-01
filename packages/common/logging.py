"""Structured JSON logging with correlation-id propagation.

Log lines are machine-parseable JSON on stdout so Docker, Loki, or CloudWatch
can index them without a regex. A ``contextvars`` binding carries the request or
event correlation id through async call stacks and Kafka handlers, which is what
makes a single trace readable end to end.

Secrets and raw PII must never be logged. ``_REDACT_KEYS`` scrubs the obvious
offenders defensively; the real control is not passing them in.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import MutableMapping
from contextvars import ContextVar
from datetime import UTC
from typing import Any

_context: ContextVar[dict[str, Any] | None] = ContextVar("tracefleet_log_context", default=None)

_REDACT_KEYS = {
    "password",
    "secret",
    "token",
    "authorization",
    "api_key",
    "apikey",
    "gemini_api_key",
    "jwt_secret",
    "private_key",
    "vin",
}

#: Attribute names ``logging.LogRecord`` owns. Passing any of these through
#: ``extra=`` makes the stdlib raise ``KeyError: Attempt to overwrite ...``, so
#: :class:`_SafeLogger` renames them instead of letting a log call crash the
#: caller. A logging statement must never be the thing that breaks a request.
_RESERVED = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
    }
)


def bind(**values: Any) -> None:
    """Add fields to the logging context for the current task/thread."""
    _context.set({**(_context.get() or {}), **{k: v for k, v in values.items() if v is not None}})


def unbind(*keys: str) -> None:
    current = dict(_context.get() or {})
    for key in keys:
        current.pop(key, None)
    _context.set(current)


def clear() -> None:
    _context.set({})


def get_context() -> dict[str, Any]:
    return dict(_context.get() or {})


class _Redactor:
    @staticmethod
    def scrub(value: Any, key: str = "") -> Any:
        if key.lower() in _REDACT_KEYS:
            return "***"
        if isinstance(value, dict):
            return {k: _Redactor.scrub(v, k) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_Redactor.scrub(item) for item in value]
        return value


class JsonFormatter(logging.Formatter):
    """Render a record as a single-line JSON object."""

    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            # Built from the record's epoch time rather than strftime, because
            # strftime has no millisecond directive and `%03d` renders literally.
            "timestamp": _iso_timestamp(record),
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(_Redactor.scrub(get_context()))
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = _Redactor.scrub(value, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        try:
            return json.dumps(payload, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            return json.dumps({"level": record.levelname, "service": self.service, "message": record.getMessage()})


def configure(service: str | None = None, level: str | None = None) -> logging.Logger:
    """Install the JSON handler on the root logger. Idempotent."""
    service_name = service or (os.getenv("OTEL_SERVICE_NAME") or "tracefleet")
    log_level = (level or (os.getenv("LOG_LEVEL") or "INFO")).upper()

    root = logging.getLogger()
    root.setLevel(log_level)
    for handler in list(root.handlers):
        if getattr(handler, "_tracefleet", False):
            root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service_name))
    handler._tracefleet = True  # type: ignore[attr-defined]
    root.addHandler(handler)

    # Kafka and HTTP client chatter drowns out application events at DEBUG.
    for noisy in ("urllib3", "kafka", "asyncio", "clickhouse_connect"):
        logging.getLogger(noisy).setLevel(max(logging.WARNING, root.level))
    return root


class _SafeLogger(logging.LoggerAdapter):
    """Logger that will not raise because of a colliding ``extra`` key.

    ``logger.error("...", extra={"module": x})`` raises ``KeyError`` in the stdlib
    because ``module`` is a ``LogRecord`` attribute. The names that collide are
    ordinary domain words — module, filename, process, message — so the adapter
    renames them to ``<name>_`` rather than requiring every call site to know the
    reserved list.
    """

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple[Any, MutableMapping[str, Any]]:
        extra = kwargs.get("extra")
        if extra:
            kwargs["extra"] = {(f"{key}_" if key in _RESERVED else key): value for key, value in extra.items()}
        return msg, kwargs


def get_logger(name: str) -> logging.Logger:
    """Logger that tolerates any ``extra`` key."""
    return _SafeLogger(logging.getLogger(name), {})  # type: ignore[return-value]


def _iso_timestamp(record: logging.LogRecord) -> str:
    from datetime import datetime

    moment = datetime.fromtimestamp(record.created, tz=UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"


def audit_logger() -> logging.Logger:
    """Separate stream for audit events.

    Audit records also go to the database; the log stream exists so a compromised
    or unavailable database is not the only copy.
    """
    base = logging.getLogger("tracefleet.audit")
    base.propagate = True
    return _SafeLogger(base, {})  # type: ignore[return-value]
