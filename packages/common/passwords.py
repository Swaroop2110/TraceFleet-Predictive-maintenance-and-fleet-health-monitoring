"""Password hashing, standard library only.

PBKDF2-HMAC-SHA256 rather than Argon2 so the platform has no native build
dependency; the iteration count is set well above the OWASP floor to compensate.
Swapping in Argon2 means adding a branch to :func:`verify` keyed on the algorithm
prefix, which is why the prefix is stored.

Stored form::

    pbkdf2_sha256$<iterations>$<salt_b64>$<digest_b64>

Verification is constant-time. Comparing digests with ``==`` leaks timing
information that can be used to recover a hash byte by byte.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets

ALGORITHM = "pbkdf2_sha256"
#: OWASP's 2023 guidance for PBKDF2-HMAC-SHA256 is 600,000 iterations.
DEFAULT_ITERATIONS = 600_000
SALT_BYTES = 16


def hash_password(password: str, *, iterations: int | None = None) -> str:
    """Hash a password for storage."""
    if not password:
        raise ValueError("password must not be empty")
    rounds = int(iterations or os.getenv("PASSWORD_HASH_ITERATIONS") or DEFAULT_ITERATIONS)
    salt = secrets.token_bytes(SALT_BYTES)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return f"{ALGORITHM}${rounds}${_b64(salt)}${_b64(digest)}"


def verify_password(password: str, stored: str | None) -> bool:
    """Check a password against a stored hash.

    A missing hash returns False rather than raising: an account provisioned for
    external OIDC has no local password, and that is a normal state, not an error.
    """
    if not stored or not password:
        return False
    try:
        algorithm, rounds, salt_b64, digest_b64 = stored.split("$")
    except ValueError:
        return False
    if algorithm != ALGORITHM:
        return False
    try:
        salt = _unb64(salt_b64)
        expected = _unb64(digest_b64)
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(rounds))
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(candidate, expected)


def needs_rehash(stored: str | None, *, iterations: int | None = None) -> bool:
    """Whether a stored hash was produced with fewer iterations than current policy."""
    if not stored:
        return False
    try:
        algorithm, rounds, _, _ = stored.split("$")
    except ValueError:
        return True
    target = int(iterations or os.getenv("PASSWORD_HASH_ITERATIONS") or DEFAULT_ITERATIONS)
    return algorithm != ALGORITHM or int(rounds) < target


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)
