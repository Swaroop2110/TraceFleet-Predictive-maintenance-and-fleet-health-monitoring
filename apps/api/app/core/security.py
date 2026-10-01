"""Authentication and authorisation dependencies.

Authorisation is expressed as *permissions*, never role names. A route declares
``Depends(require("incident:close"))`` and ``config/roles.yaml`` decides which
roles hold that permission. Adding a role, or moving a permission between roles,
is then a config change with no code edit — which is the whole point of the
modularity contract.

Two things the previous implementation got wrong and this one does not:

* There is no anonymous-admin fallback. ``TRACEFLEET_ALLOW_ANONYMOUS`` previously
  defaulted to true, so an unauthenticated request was silently treated as an
  ADMIN on the default tenant. Here anonymous access is off unless explicitly
  enabled, and when enabled the identity it grants is a read-only VIEWER.
* Tokens are RS256 and their issuer, audience, and type are all validated.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import Depends, HTTPException, Request, WebSocket, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from packages.common import config as cfg
from packages.common import logging as tf_logging
from packages.common.tokens import TokenError, verify_token

LOGGER = tf_logging.get_logger(__name__)

#: auto_error=False so a missing header reaches our own handler and can produce
#: the structured error envelope rather than FastAPI's bare {"detail": ...}.
bearer_scheme = HTTPBearer(auto_error=False)

DEFAULT_TENANT = os.getenv("DEFAULT_TENANT_ID", "tenant_default")


@dataclass
class Principal:
    """The authenticated caller."""

    user_id: str
    tenant_id: str
    roles: list[str] = field(default_factory=list)
    permissions: set[str] = field(default_factory=set)
    token_id: str | None = None
    anonymous: bool = False

    def has(self, permission: str) -> bool:
        return "*" in self.permissions or permission in self.permissions

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "tenant_id": self.tenant_id,
            "roles": sorted(self.roles),
            "permissions": sorted(self.permissions),
            "anonymous": self.anonymous,
        }

    # The rest of the codebase indexes principals like the old dict. Supporting
    # both keeps this change from rippling into every route at once.
    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


def allow_anonymous() -> bool:
    """Whether unauthenticated requests are accepted.

    Defaults to **off**. A demo can switch it on, and gets VIEWER, not ADMIN.
    """
    return os.getenv("TRACEFLEET_ALLOW_ANONYMOUS", "false").lower() in {"1", "true", "yes"}


def permissions_for(roles: list[str]) -> set[str]:
    """Resolve roles to permissions through ``config/roles.yaml``."""
    try:
        return cfg.roles().permissions_for(*roles)
    except cfg.ConfigError as error:  # pragma: no cover
        LOGGER.error("roles configuration unavailable", extra={"error": str(error)})
        return set()


def principal_from_claims(claims: dict[str, Any]) -> Principal:
    roles = [str(role).upper() for role in (claims.get("roles") or [])]
    return Principal(
        user_id=str(claims["sub"]),
        tenant_id=str(claims["tenant_id"]),
        roles=roles,
        permissions=permissions_for(roles),
        token_id=claims.get("jti"),
    )


def anonymous_principal() -> Principal:
    roles = ["VIEWER"]
    return Principal(
        user_id="anonymous",
        tenant_id=DEFAULT_TENANT,
        roles=roles,
        permissions=permissions_for(roles),
        anonymous=True,
    )


def get_current_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> Principal:
    """Resolve the caller from the Authorization header."""
    if credentials and credentials.credentials:
        try:
            claims = verify_token(credentials.credentials, expected_type="access")
        except TokenError as error:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=str(error),
                headers={"WWW-Authenticate": "Bearer"},
            ) from error
        principal = principal_from_claims(claims)
        request.state.principal = principal
        return principal

    if allow_anonymous():
        principal = anonymous_principal()
        request.state.principal = principal
        return principal

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_current_user(principal: Principal = Depends(get_current_principal)) -> Principal:
    """Alias kept so existing route signatures continue to read naturally."""
    return principal


def get_current_tenant(principal: Principal = Depends(get_current_principal)) -> str:
    """The caller's tenant. Every tenant-scoped query filters on this."""
    return principal.tenant_id


def require(*permissions: str, require_all: bool = True) -> Callable[..., Principal]:
    """Dependency enforcing one or more permissions.

    ``require_all`` defaults to True: a route asking for two permissions means
    both. The alternative is available for endpoints that serve several roles by
    different routes to the same data.
    """

    def dependency(
        request: Request,
        principal: Principal = Depends(get_current_principal),
    ) -> Principal:
        held = [permission for permission in permissions if principal.has(permission)]
        satisfied = len(held) == len(permissions) if require_all else bool(held)
        if not satisfied:
            missing = sorted(set(permissions) - set(held))
            # Logged, because a permission denial is a security-relevant event and
            # the audit trail should not depend on the client reporting it.
            LOGGER.info(
                "permission denied",
                extra={
                    "actor": principal.user_id,
                    "tenant": principal.tenant_id,
                    "roles": principal.roles,
                    "missing_permissions": missing,
                    "path": request.url.path,
                },
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Missing required permission(s): {', '.join(missing)}",
            )
        return principal

    return dependency


def require_roles(*roles: str) -> Callable[..., Principal]:
    """Role-name check, retained only for the legacy config routes.

    Prefer :func:`require` with a permission. Checking role names hard-codes the
    authorisation model into the route.
    """
    wanted = {role.upper() for role in roles}

    def dependency(principal: Principal = Depends(get_current_principal)) -> Principal:
        if not wanted.intersection(principal.roles):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Requires one of: {', '.join(sorted(wanted))}",
            )
        return principal

    return dependency


async def principal_from_websocket(websocket: WebSocket) -> Principal | None:
    """Authenticate a WebSocket connection.

    Browsers cannot set headers on a WebSocket handshake, so the token arrives as
    a query parameter or the ``Sec-WebSocket-Protocol`` header. It is still a real
    token and still fully verified — the previous implementation trusted a
    ``tenant_id`` query parameter, which let any client read any tenant's stream.
    """
    token = websocket.query_params.get("token") or websocket.query_params.get("access_token")
    if not token:
        header = websocket.headers.get("authorization") or ""
        if header.lower().startswith("bearer "):
            token = header[7:]
    if not token:
        header = websocket.headers.get("sec-websocket-protocol") or ""
        for part in (piece.strip() for piece in header.split(",")):
            if part.startswith("bearer."):
                token = part[len("bearer.") :]
                break

    if not token:
        return anonymous_principal() if allow_anonymous() else None

    try:
        claims = verify_token(token, expected_type="access")
    except TokenError as error:
        LOGGER.info("websocket token rejected", extra={"error": str(error)})
        return None
    return principal_from_claims(claims)


def describe_authorization() -> dict[str, Any]:
    """Effective permission map, for the admin UI and the acceptance tests."""
    roles_config = cfg.roles()
    return {
        "permissions": sorted(roles_config.permissions),
        "roles": {
            name: {
                "description": role.description,
                "inherits": role.inherits,
                "effective_permissions": sorted(roles_config.permissions_for(name)),
            }
            for name, role in roles_config.roles.items()
        },
        "anonymous_access": allow_anonymous(),
    }
