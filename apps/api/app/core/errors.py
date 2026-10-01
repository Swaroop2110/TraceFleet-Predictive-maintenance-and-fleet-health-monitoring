"""Structured error envelope.

Every failure returns the same shape::

    {"error": {"code": "...", "message": "...", "details": {...}, "request_id": "..."}}

A stable machine-readable ``code`` lets the frontend branch without parsing prose,
and the ``request_id`` is what a user quotes in a support request to make their
failure findable in the logs.

Internal errors deliberately do not leak their message. A stack trace or a raw SQL
error in an HTTP response is an information-disclosure finding; it goes to the log,
keyed by the same request id.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from packages.common import logging as tf_logging

LOGGER = tf_logging.get_logger(__name__)

#: HTTP status to stable error code.
_STATUS_CODES = {
    400: "bad_request",
    401: "unauthenticated",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    413: "payload_too_large",
    422: "validation_failed",
    429: "rate_limited",
    500: "internal_error",
    503: "service_unavailable",
}


class TraceFleetError(HTTPException):
    """HTTP error carrying an explicit code and details."""

    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(status_code=status_code, detail=message, headers=headers)
        self.code = code or _STATUS_CODES.get(status_code, "error")
        self.details = details or {}


class NotFound(TraceFleetError):
    def __init__(self, resource: str, identifier: str) -> None:
        super().__init__(
            status.HTTP_404_NOT_FOUND,
            f"{resource} not found",
            code="not_found",
            details={"resource": resource, "id": identifier},
        )


class Conflict(TraceFleetError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__(status.HTTP_409_CONFLICT, message, code="conflict", details=details)


class FeatureDisabled(TraceFleetError):
    def __init__(self, feature: str) -> None:
        super().__init__(
            status.HTTP_404_NOT_FOUND,
            f"Feature '{feature}' is not enabled",
            code="feature_disabled",
            details={"feature": feature},
        )


def error_body(
    code: str,
    message: str,
    request_id: str | None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or {},
            "request_id": request_id,
        }
    }


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def register_exception_handlers(app: FastAPI) -> None:
    """Install handlers so no route can return an unstructured error."""

    @app.exception_handler(TraceFleetError)
    async def _tracefleet_error(request: Request, exc: TraceFleetError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=error_body(exc.code, str(exc.detail), _request_id(request), exc.details),
            headers=exc.headers,
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _STATUS_CODES.get(exc.status_code, "error")
        return JSONResponse(
            status_code=exc.status_code,
            content=error_body(code, str(exc.detail), _request_id(request)),
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Field paths are returned so a client can highlight the offending input,
        # but the submitted values are not echoed back.
        fields = [
            {
                "field": ".".join(str(part) for part in error.get("loc", ()) if part != "body"),
                "message": error.get("msg", ""),
                "type": error.get("type", ""),
            }
            for error in exc.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=error_body(
                "validation_failed",
                "Request validation failed",
                _request_id(request),
                {"fields": fields},
            ),
        )

    @app.exception_handler(IntegrityError)
    async def _integrity_error(request: Request, exc: IntegrityError) -> JSONResponse:
        # A unique or check violation is the caller's problem (409), not a server
        # fault, but the constraint name is internal so it is logged, not returned.
        LOGGER.warning("database integrity error", extra={"error": str(exc.orig)})
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=error_body(
                "conflict",
                "The request conflicts with existing data",
                _request_id(request),
            ),
        )

    @app.exception_handler(OperationalError)
    async def _operational_error(request: Request, exc: OperationalError) -> JSONResponse:
        LOGGER.error("database unavailable", extra={"error": str(exc.orig)})
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=error_body(
                "service_unavailable",
                "A dependency is temporarily unavailable",
                _request_id(request),
            ),
            headers={"Retry-After": "5"},
        )

    @app.exception_handler(SQLAlchemyError)
    async def _database_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        LOGGER.exception("database error", extra={"error": str(exc)})
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_body("internal_error", "An internal error occurred", _request_id(request)),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception(
            "unhandled exception",
            extra={"path": request.url.path, "error": str(exc), "error_type": type(exc).__name__},
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=error_body("internal_error", "An internal error occurred", _request_id(request)),
        )
