"""HTTP middleware: correlation ids, security headers, rate limiting, body limits.

Order matters and is the reverse of registration in Starlette, so
:func:`install_middleware` registers from innermost outward. The correlation-id
middleware is registered last and therefore runs first, which is what makes a
request id available to every other layer including the error handlers.
"""

from __future__ import annotations

import os
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware

from apps.api.app.core.errors import error_body
from packages.common import logging as tf_logging
from packages.common.ids import request_id as new_request_id

LOGGER = tf_logging.get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"

#: Paths excluded from access logging and rate limiting. Health and metrics are
#: polled continuously by Compose and Prometheus and would drown the log.
_QUIET_PATHS = {"/api/v1/health", "/api/v1/health/live", "/api/v1/health/ready", "/metrics"}


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Assign or propagate a request id and bind it to the logging context.

    An inbound ``X-Request-ID`` is honoured so a trace spans the gateway and the
    browser, but it is length-capped and stripped: it is attacker-controlled input
    that ends up in log lines and audit rows.
    """

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        inbound = (request.headers.get(REQUEST_ID_HEADER) or "").strip()
        request_id = inbound[:64] if inbound.isascii() and inbound else new_request_id()
        request.state.request_id = request_id
        request.state.client_ip = _client_ip(request)

        tf_logging.bind(request_id=request_id)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            tf_logging.unbind("request_id", "actor", "tenant")

        duration_ms = (time.perf_counter() - started) * 1000
        response.headers[REQUEST_ID_HEADER] = request_id
        response.headers["Server-Timing"] = f"app;dur={duration_ms:.1f}"

        if request.url.path not in _QUIET_PATHS:
            principal = getattr(request.state, "principal", None)
            LOGGER.info(
                "request completed",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round(duration_ms, 2),
                    "actor": getattr(principal, "user_id", None),
                    "tenant": getattr(principal, "tenant_id", None),
                    "client_ip": request.state.client_ip,
                },
            )
        return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Standard hardening headers."""

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
        response.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
        # The API returns JSON, never markup, so the strictest CSP applies. The
        # frontend is served separately and carries its own policy.
        response.headers.setdefault(
            "Content-Security-Policy",
            "default-src 'none'; frame-ancestors 'none'; base-uri 'none'",
        )
        # HSTS only over TLS: sending it on plain HTTP is ignored by browsers and
        # would pin a local demo to a scheme it does not serve.
        if request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https":
            response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        return response


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Reject oversized bodies before they are buffered."""

    def __init__(self, app: Any, max_bytes: int) -> None:
        super().__init__(app)
        self.max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > self.max_bytes:
            return JSONResponse(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                content=error_body(
                    "payload_too_large",
                    f"Request body exceeds {self.max_bytes} bytes",
                    getattr(request.state, "request_id", None),
                ),
            )
        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Token bucket per caller.

    Redis-backed so the limit is shared across API replicas; a per-process counter
    would multiply the effective limit by the replica count. When Redis is
    unavailable it falls back to an in-process bucket, which is weaker but still
    bounds a single instance — failing *open* on a rate limiter is the wrong
    default when the endpoint being protected is ``/auth/token``.
    """

    def __init__(
        self,
        app: Any,
        requests_per_minute: int,
        burst: int | None = None,
        redis_client: Any = None,
    ) -> None:
        super().__init__(app)
        self.rate_per_second = requests_per_minute / 60.0
        self.capacity = burst or max(requests_per_minute // 2, 10)
        self.redis = redis_client
        self._local: OrderedDict[str, tuple[float, float]] = OrderedDict()
        self._degraded = False

    async def dispatch(self, request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        if request.url.path in _QUIET_PATHS:
            return await call_next(request)

        identity = self._identity(request)
        allowed, retry_after = self._consume(identity)
        if not allowed:
            LOGGER.info("rate limited", extra={"identity": identity, "path": request.url.path})
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content=error_body(
                    "rate_limited",
                    "Too many requests",
                    getattr(request.state, "request_id", None),
                    {"retry_after_seconds": retry_after},
                ),
                headers={"Retry-After": str(retry_after)},
            )
        return await call_next(request)

    @staticmethod
    def _identity(request: Request) -> str:
        """Bucket by authenticated subject where possible, else by client address.

        Keying on the bearer token's subject means one noisy tenant cannot exhaust
        another's budget, and a shared NAT address does not throttle everyone
        behind it.
        """
        header = request.headers.get("authorization") or ""
        if header.lower().startswith("bearer "):
            from packages.common.tokens import TokenError, decode_unverified

            try:
                claims = decode_unverified(header[7:])
                subject = claims.get("sub")
                tenant = claims.get("tenant_id")
                if subject:
                    # Unverified on purpose: this is only a bucket key, never an
                    # authorisation decision, and verification happens downstream.
                    return f"user:{tenant}:{subject}"
            except TokenError:
                pass
        return f"ip:{_client_ip(request)}"

    def _consume(self, identity: str) -> tuple[bool, int]:
        if self.redis is not None and not self._degraded:
            try:
                return self._consume_redis(identity)
            except Exception as error:
                self._degraded = True
                LOGGER.warning(
                    "redis rate limiter unavailable, using in-process buckets",
                    extra={"error": str(error)},
                )
        return self._consume_local(identity)

    def _consume_redis(self, identity: str) -> tuple[bool, int]:
        key = f"tf:ratelimit:{identity}"
        now = time.time()
        # Executed server-side so the read-modify-write of the bucket is atomic.
        # Doing it with GET then SET would let concurrent requests each see the
        # same remaining budget.
        script = """
        local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
        local updated = tonumber(redis.call('HGET', KEYS[1], 'updated'))
        local rate = tonumber(ARGV[1])
        local capacity = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        if tokens == nil then tokens = capacity; updated = now end
        tokens = math.min(capacity, tokens + (now - updated) * rate)
        local allowed = 0
        if tokens >= 1 then tokens = tokens - 1; allowed = 1 end
        redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now)
        redis.call('EXPIRE', KEYS[1], ARGV[4])
        return {allowed, tostring(tokens)}
        """
        ttl = int(self.capacity / max(self.rate_per_second, 0.001)) + 60
        allowed, _remaining = self.redis.eval(script, 1, key, self.rate_per_second, self.capacity, now, ttl)
        retry_after = max(1, int(1 / max(self.rate_per_second, 0.001)))
        return bool(int(allowed)), retry_after

    def _consume_local(self, identity: str) -> tuple[bool, int]:
        now = time.monotonic()
        tokens, updated = self._local.get(identity, (float(self.capacity), now))
        tokens = min(self.capacity, tokens + (now - updated) * self.rate_per_second)
        allowed = tokens >= 1
        if allowed:
            tokens -= 1
        self._local[identity] = (tokens, now)
        self._local.move_to_end(identity)
        # Bounded so a flood of distinct identities cannot grow this without limit.
        while len(self._local) > 10_000:
            self._local.popitem(last=False)
        return allowed, max(1, int(1 / max(self.rate_per_second, 0.001)))


def cors_origins() -> list[str]:
    """Allow-list from ``CORS_ORIGINS``.

    An allow-list, not ``*``. Credentialed requests from any origin is what makes
    a cross-site request able to act as the logged-in user.
    """
    raw = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


def install_middleware(app: FastAPI, redis_client: Any = None) -> None:
    """Register the middleware stack.

    Starlette runs middleware in reverse registration order, so these are added
    innermost-first and the correlation id ends up outermost.
    """
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    app.add_middleware(
        RateLimitMiddleware,
        requests_per_minute=int(os.getenv("RATE_LIMIT_PER_MINUTE", "600")),
        burst=int(os.getenv("RATE_LIMIT_BURST", "120")),
        redis_client=redis_client,
    )
    app.add_middleware(
        BodySizeLimitMiddleware,
        max_bytes=int(os.getenv("MAX_REQUEST_BYTES", str(1024 * 1024))),
    )
    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins(),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", REQUEST_ID_HEADER],
        expose_headers=[REQUEST_ID_HEADER, "Server-Timing"],
        max_age=600,
    )
    # Registered last so it runs first and every layer above has a request id.
    app.add_middleware(CorrelationIdMiddleware)


def _client_ip(request: Request) -> str:
    """Caller address, honouring one layer of reverse proxy.

    Only the first entry of ``X-Forwarded-For`` is taken, and only when
    ``TRUST_PROXY_HEADERS`` is set; otherwise a client could spoof its own address
    into the audit log by sending the header itself.
    """
    if os.getenv("TRUST_PROXY_HEADERS", "false").lower() in {"1", "true", "yes"}:
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()[:64]
    client = request.client
    return client.host if client else "unknown"
