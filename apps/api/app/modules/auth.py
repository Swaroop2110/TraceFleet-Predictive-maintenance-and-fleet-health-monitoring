"""Authentication endpoints: token, refresh, me, JWKS, discovery.

A local OIDC-compatible issuer. Credentials are verified against ``app_user``, and
the response is an RS256 token whose public key is published at ``/auth/jwks`` so
a gateway or sidecar can validate without holding the signing key.

The previous implementation minted a token for any ``user_id`` with any requested
roles and no credential check, which made every other authorisation control
decorative.
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Depends, Request, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.api.app.core.audit import audit_from_request
from apps.api.app.core.errors import TraceFleetError
from apps.api.app.core.security import (
    Principal,
    describe_authorization,
    get_current_principal,
    permissions_for,
)
from apps.api.app.db.models import AppUser, UserRole
from apps.api.app.db.session import get_db, set_tenant_context
from packages.common import logging as tf_logging
from packages.common.passwords import hash_password, needs_rehash, verify_password
from packages.common.timeutil import now
from packages.common.tokens import (
    ACCESS_TOKEN_TTL_S,
    TokenError,
    issue_token,
    jwks,
    openid_configuration,
    verify_token,
)

LOGGER = tf_logging.get_logger(__name__)

#: Auth is always available. Gating it on `multi_tenant`, as before, meant turning
#: off multi-tenancy removed the ability to log in.
FEATURE_FLAGS: tuple[str, ...] = ()

router = APIRouter(prefix="/auth", tags=["auth"])


class TokenRequest(BaseModel):
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=256)
    # Optional, so a single-tenant deployment can omit it.
    tenant_id: str | None = None


class RefreshRequest(BaseModel):
    refresh_token: str = Field(min_length=16)


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int
    tenant_id: str
    roles: list[str]
    permissions: list[str]


def _default_tenant() -> str:
    return os.getenv("DEFAULT_TENANT_ID", "tenant_default")


def _load_user(db: Session, username: str, tenant_id: str) -> AppUser | None:
    return db.execute(
        select(AppUser).where(
            AppUser.username == username,
            AppUser.tenant_id == tenant_id,
            AppUser.active.is_(True),
        )
    ).scalar_one_or_none()


def _roles_for(db: Session, user: AppUser) -> list[str]:
    rows = db.execute(select(UserRole.role_name).where(UserRole.user_id == user.user_id)).scalars().all()
    return [str(role).upper() for role in rows]


@router.post("/token", response_model=TokenResponse)
def issue_access_token(
    payload: TokenRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> TokenResponse:
    """Exchange credentials for an access and refresh token pair."""
    tenant_id = payload.tenant_id or _default_tenant()
    # Bind the row-level-security context to the *claimed* tenant before touching
    # app_user. The lookup can then only match accounts in that tenant, so this
    # endpoint cannot be used to enumerate users across tenants.
    set_tenant_context(db, tenant_id)
    user = _load_user(db, payload.username, tenant_id)

    # The same generic message and the same work for a missing user as for a wrong
    # password. Distinguishing them turns this endpoint into a username oracle, and
    # returning early would make it a timing oracle.
    stored_hash = user.password_hash if user else None
    password_ok = verify_password(payload.password, stored_hash)
    if user is None or not password_ok:
        audit_from_request(
            request,
            "login_failed",
            "auth",
            resource_id=payload.username,
            outcome="FAILURE",
            actor=payload.username,
            tenant=tenant_id,
            metadata={"reason": "invalid_credentials"},
        )
        raise TraceFleetError(
            status.HTTP_401_UNAUTHORIZED,
            "Invalid username or password",
            code="invalid_credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    roles = _roles_for(db, user)
    if not roles:
        # An account with no role can authenticate but can do nothing, which is a
        # confusing state. Refuse it and say so.
        audit_from_request(
            request,
            "login_failed",
            "auth",
            resource_id=user.user_id,
            outcome="FAILURE",
            actor=user.user_id,
            tenant=tenant_id,
            metadata={"reason": "no_roles_assigned"},
        )
        raise TraceFleetError(
            status.HTTP_403_FORBIDDEN,
            "Account has no roles assigned",
            code="no_roles_assigned",
        )

    # Opportunistic upgrade if the stored hash predates the current iteration count.
    if needs_rehash(user.password_hash):
        user.password_hash = hash_password(payload.password)

    user.last_login_at = now()
    db.commit()

    granted = permissions_for(roles)
    audit_from_request(
        request,
        "login_succeeded",
        "auth",
        resource_id=user.user_id,
        actor=user.user_id,
        tenant=tenant_id,
        metadata={"roles": roles, "username": user.username},
    )
    return TokenResponse(
        access_token=issue_token(subject=user.user_id, tenant_id=tenant_id, roles=roles),
        refresh_token=issue_token(subject=user.user_id, tenant_id=tenant_id, roles=roles, token_type="refresh"),
        expires_in=ACCESS_TOKEN_TTL_S,
        tenant_id=tenant_id,
        roles=roles,
        permissions=sorted(granted),
    )


@router.post("/refresh", response_model=TokenResponse)
def refresh_access_token(
    payload: RefreshRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> TokenResponse:
    """Exchange a refresh token for a new pair.

    Roles are re-read from the database rather than copied from the token. A role
    revoked ten minutes ago must not survive in a refreshed token, which is the
    main reason refresh exists rather than issuing very long-lived access tokens.
    """
    try:
        claims = verify_token(payload.refresh_token, expected_type="refresh")
    except TokenError as error:
        audit_from_request(
            request,
            "token_refresh_failed",
            "auth",
            outcome="FAILURE",
            metadata={"reason": str(error)},
        )
        raise TraceFleetError(
            status.HTTP_401_UNAUTHORIZED,
            "Refresh token is invalid or expired",
            code="invalid_refresh_token",
        ) from error

    # The tenant comes from the verified token, so RLS still scopes the lookup.
    set_tenant_context(db, str(claims["tenant_id"]))
    user = db.get(AppUser, str(claims["sub"]))
    if user is None or not user.active:
        raise TraceFleetError(status.HTTP_401_UNAUTHORIZED, "Account is no longer active", code="account_inactive")

    roles = _roles_for(db, user)
    if not roles:
        raise TraceFleetError(status.HTTP_403_FORBIDDEN, "Account has no roles assigned", code="no_roles_assigned")

    tenant_id = user.tenant_id
    audit_from_request(
        request,
        "token_refreshed",
        "auth",
        resource_id=user.user_id,
        actor=user.user_id,
        tenant=tenant_id,
        metadata={"roles": roles},
    )
    return TokenResponse(
        access_token=issue_token(subject=user.user_id, tenant_id=tenant_id, roles=roles),
        refresh_token=issue_token(subject=user.user_id, tenant_id=tenant_id, roles=roles, token_type="refresh"),
        expires_in=ACCESS_TOKEN_TTL_S,
        tenant_id=tenant_id,
        roles=roles,
        permissions=sorted(permissions_for(roles)),
    )


@router.get("/me")
def whoami(
    principal: Principal = Depends(get_current_principal),
    db: Session = Depends(get_db),
) -> dict[str, Any]:
    """The caller's identity, roles, and effective permissions.

    The frontend uses the permission list to decide which controls to render, so
    it never has to duplicate the authorisation model.
    """
    profile: dict[str, Any] = {
        "user_id": principal.user_id,
        "tenant_id": principal.tenant_id,
        "roles": sorted(principal.roles),
        "permissions": sorted(principal.permissions),
        "anonymous": principal.anonymous,
    }
    if not principal.anonymous:
        set_tenant_context(db, principal.tenant_id)
        user = db.get(AppUser, principal.user_id)
        if user is not None:
            profile.update(
                {
                    "username": user.username,
                    "display_name": user.display_name,
                    "email": user.email,
                    "last_login_at": user.last_login_at.isoformat() if user.last_login_at else None,
                }
            )
    return profile


@router.get("/jwks")
def jwks_document() -> dict[str, Any]:
    """Public key set. Public by design so external verifiers can fetch it."""
    return jwks()


@router.get("/.well-known/openid-configuration")
def discovery(request: Request) -> dict[str, Any]:
    base = str(request.base_url).rstrip("/")
    return openid_configuration(base)


@router.get("/authorization-model")
def authorization_model(principal: Principal = Depends(get_current_principal)) -> dict[str, Any]:
    """The role-to-permission map, for the admin UI and the acceptance tests.

    Readable by any authenticated caller: it describes the policy, not any data,
    and the frontend needs it to render an accurate administration view.
    """
    return describe_authorization()
