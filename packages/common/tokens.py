"""RS256 JWT issuing, verification, and JWKS publication.

Asymmetric signing rather than a shared secret: the API signs with a private key
and anything that needs to *verify* a token (a sidecar, a gateway, the simulator's
control API) can do so from the published JWKS without being able to mint tokens.
With HS256 every verifier is also an issuer.

Key material, in order of preference:

1. ``JWT_PRIVATE_KEY_PATH`` / ``JWT_PUBLIC_KEY_PATH`` — files, the production path.
2. ``JWT_PRIVATE_KEY`` — a PEM string, for secret managers that inject env vars.
3. A keypair generated at startup and cached under ``JWT_KEY_DIR``.

Option 3 exists so a fresh clone runs with no setup. It is a real trade-off and is
logged as a warning, because an ephemeral key means tokens do not survive a
restart and multiple replicas would each mint tokens the others reject. Mounting
``JWT_KEY_DIR`` on a volume, as Compose does, makes it stable for a local demo.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from packages.common import logging as tf_logging

LOGGER = tf_logging.get_logger(__name__)

ALGORITHM = "RS256"
DEFAULT_ISSUER = "https://tracefleet.local/auth"
DEFAULT_AUDIENCE = "tracefleet-api"
ACCESS_TOKEN_TTL_S = int(os.getenv("JWT_ACCESS_TTL_S", "3600"))
REFRESH_TOKEN_TTL_S = int(os.getenv("JWT_REFRESH_TTL_S", "86400"))
#: Tolerance for clock skew between the issuer and a verifier.
LEEWAY_S = int(os.getenv("JWT_LEEWAY_S", "30"))


class TokenError(Exception):
    """Raised when a token is malformed, expired, or fails verification."""


@dataclass
class KeyPair:
    private_pem: bytes
    public_pem: bytes
    kid: str
    ephemeral: bool = False


_lock = threading.RLock()
_keypair: KeyPair | None = None


def _crypto() -> Any:
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        return serialization, rsa
    except ImportError as error:  # pragma: no cover
        raise TokenError("RS256 requires the `cryptography` package; install requirements.txt") from error


def _kid_for(public_pem: bytes) -> str:
    """Key id derived from the public key, so it changes when the key does."""
    import hashlib

    return hashlib.sha256(public_pem).hexdigest()[:16]


def load_keypair(refresh: bool = False) -> KeyPair:
    """Resolve the signing keypair. Cached after the first call."""
    global _keypair
    with _lock:
        if _keypair is not None and not refresh:
            return _keypair
        _keypair = _resolve_keypair()
        return _keypair


def _resolve_keypair() -> KeyPair:
    serialization, rsa = _crypto()

    private_path = os.getenv("JWT_PRIVATE_KEY_PATH")
    public_path = os.getenv("JWT_PUBLIC_KEY_PATH")
    if private_path and Path(private_path).exists():
        private_pem = Path(private_path).read_bytes()
        public_pem = (
            Path(public_path).read_bytes()
            if public_path and Path(public_path).exists()
            else _public_from_private(private_pem)
        )
        return KeyPair(private_pem, public_pem, _kid_for(public_pem))

    inline = os.getenv("JWT_PRIVATE_KEY")
    if inline:
        private_pem = inline.encode("utf-8") if isinstance(inline, str) else inline
        public_pem = _public_from_private(private_pem)
        return KeyPair(private_pem, public_pem, _kid_for(public_pem))

    # Cache generated keys on disk so a container restart does not invalidate
    # every outstanding token when the directory is a mounted volume.
    key_dir = Path(os.getenv("JWT_KEY_DIR", str(Path.home() / ".cache" / "tracefleet" / "keys")))
    cached_private = key_dir / "jwt_private.pem"
    cached_public = key_dir / "jwt_public.pem"
    if cached_private.exists() and cached_public.exists():
        private_pem = cached_private.read_bytes()
        public_pem = cached_public.read_bytes()
        return KeyPair(private_pem, public_pem, _kid_for(public_pem))

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    ephemeral = True
    try:
        key_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        cached_private.write_bytes(private_pem)
        cached_private.chmod(0o600)
        cached_public.write_bytes(public_pem)
        ephemeral = False
    except OSError as error:
        LOGGER.warning("could not persist generated JWT key", extra={"error": str(error)})

    LOGGER.warning(
        "no JWT signing key configured; generated one",
        extra={
            "key_dir": str(key_dir),
            "ephemeral": ephemeral,
            "impact": (
                "tokens will not survive a restart and replicas will not share a key; "
                "set JWT_PRIVATE_KEY_PATH for anything beyond local use"
            ),
        },
    )
    return KeyPair(private_pem, public_pem, _kid_for(public_pem), ephemeral=ephemeral)


def _public_from_private(private_pem: bytes) -> bytes:
    serialization, _ = _crypto()
    key = serialization.load_pem_private_key(private_pem, password=None)
    return key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )


# ---------------------------------------------------------------------------
# Issue / verify
# ---------------------------------------------------------------------------
def issue_token(
    *,
    subject: str,
    tenant_id: str,
    roles: list[str],
    token_type: str = "access",
    expires_in_s: int | None = None,
    extra_claims: dict[str, Any] | None = None,
) -> str:
    """Sign a token. ``token_type`` separates access from refresh tokens.

    The type is a claim rather than a convention so a refresh token cannot be
    presented as a bearer token on a data endpoint.
    """
    import jwt as pyjwt

    keypair = load_keypair()
    now = int(time.time())
    ttl = expires_in_s or (REFRESH_TOKEN_TTL_S if token_type == "refresh" else ACCESS_TOKEN_TTL_S)
    claims: dict[str, Any] = {
        "sub": subject,
        "tenant_id": tenant_id,
        "roles": list(roles),
        "token_type": token_type,
        "iss": os.getenv("JWT_ISSUER", DEFAULT_ISSUER),
        "aud": os.getenv("JWT_AUDIENCE", DEFAULT_AUDIENCE),
        "iat": now,
        "nbf": now,
        "exp": now + ttl,
        # A unique id per token, so a future revocation list has something to key on.
        "jti": base64.urlsafe_b64encode(os.urandom(12)).decode("ascii").rstrip("="),
    }
    if extra_claims:
        claims.update(extra_claims)
    return pyjwt.encode(
        claims,
        keypair.private_pem,
        algorithm=ALGORITHM,
        headers={"kid": keypair.kid},
    )


def verify_token(token: str, *, expected_type: str | None = "access") -> dict[str, Any]:
    """Verify a token and return its claims.

    Signature, expiry, issuer, and audience are all checked. Skipping issuer or
    audience validation would let a token minted for a different service be
    replayed here.
    """
    import jwt as pyjwt

    keypair = load_keypair()
    try:
        claims = pyjwt.decode(
            token,
            keypair.public_pem,
            algorithms=[ALGORITHM],
            audience=os.getenv("JWT_AUDIENCE", DEFAULT_AUDIENCE),
            issuer=os.getenv("JWT_ISSUER", DEFAULT_ISSUER),
            leeway=LEEWAY_S,
            options={"require": ["exp", "iat", "sub"]},
        )
    except pyjwt.ExpiredSignatureError as error:
        raise TokenError("token has expired") from error
    except pyjwt.InvalidAudienceError as error:
        raise TokenError("token audience is not this service") from error
    except pyjwt.InvalidIssuerError as error:
        raise TokenError("token issuer is not trusted") from error
    except pyjwt.InvalidTokenError as error:
        raise TokenError(f"token is invalid: {error}") from error

    if expected_type is not None and claims.get("token_type") != expected_type:
        raise TokenError(f"expected a {expected_type} token, got {claims.get('token_type') or 'untyped'}")
    if not claims.get("tenant_id"):
        raise TokenError("token is missing the tenant_id claim")
    claims["roles"] = [str(role).upper() for role in (claims.get("roles") or [])]
    return claims


# ---------------------------------------------------------------------------
# JWKS
# ---------------------------------------------------------------------------
def jwks() -> dict[str, Any]:
    """Public key set, for external verifiers.

    Publishing this is what makes the issuer OIDC-compatible and lets a gateway
    validate tokens without holding the signing key.
    """
    serialization, _ = _crypto()
    keypair = load_keypair()
    public_key = serialization.load_pem_public_key(keypair.public_pem)
    numbers = public_key.public_numbers()
    return {
        "keys": [
            {
                "kty": "RSA",
                "use": "sig",
                "alg": ALGORITHM,
                "kid": keypair.kid,
                "n": _b64_uint(numbers.n),
                "e": _b64_uint(numbers.e),
            }
        ]
    }


def openid_configuration(base_url: str) -> dict[str, Any]:
    """Minimal OIDC discovery document."""
    base = base_url.rstrip("/")
    return {
        "issuer": os.getenv("JWT_ISSUER", DEFAULT_ISSUER),
        "token_endpoint": f"{base}/api/v1/auth/token",
        "jwks_uri": f"{base}/api/v1/auth/jwks",
        "userinfo_endpoint": f"{base}/api/v1/auth/me",
        "grant_types_supported": ["password", "refresh_token"],
        "id_token_signing_alg_values_supported": [ALGORITHM],
        "response_types_supported": ["token"],
        "scopes_supported": ["openid", "profile"],
    }


def _b64_uint(value: int) -> str:
    """Big-endian base64url encoding of an unsigned integer, per RFC 7518."""
    length = (value.bit_length() + 7) // 8
    return base64.urlsafe_b64encode(value.to_bytes(length, "big")).decode("ascii").rstrip("=")


def decode_unverified(token: str) -> dict[str, Any]:
    """Read claims without verifying. Diagnostics only, never for authorisation."""
    try:
        payload = token.split(".")[1]
        padding = "=" * (-len(payload) % 4)
        return json.loads(base64.urlsafe_b64decode(payload + padding))
    except Exception as error:
        raise TokenError(f"token is not decodable: {error}") from error
