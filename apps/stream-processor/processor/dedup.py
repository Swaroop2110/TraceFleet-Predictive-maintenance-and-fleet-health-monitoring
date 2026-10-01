"""Idempotency gate for telemetry events.

A duplicate is an event whose ``event_key`` has been seen before.
``event_key = sha256(vehicle_id|seq|ts)`` is content-derived, so a Kafka replay,
a retrying producer, and a genuinely duplicated OEM message all collapse onto the
same key.

Redis ``SET key value NX EX ttl`` is the primitive. It is a single atomic
round trip that both tests and claims the key, so two validator replicas racing
on the same event cannot both conclude they are first. Doing this as ``EXISTS``
then ``SET`` would be a textbook race.

When Redis is unavailable the gate degrades to a bounded in-process LRU. That is
weaker — it is per-replica and forgets on restart — so it is reported distinctly
rather than silently substituted.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

from packages.common import logging as tf_logging
from packages.common import metrics

LOGGER = tf_logging.get_logger(__name__)


@dataclass(frozen=True)
class DedupVerdict:
    """Outcome of a dedup check."""

    is_new: bool
    backend: str
    #: Times this key has been seen, when the backend can tell us.
    occurrences: int = 1

    @property
    def is_duplicate(self) -> bool:
        return not self.is_new


class BoundedLRU:
    """Fixed-capacity key set with time-based expiry.

    Fallback only. Capacity is what makes it safe: an unbounded set of seen keys
    is a memory leak that takes hours to show up at 100K events/s.
    """

    def __init__(self, capacity: int = 500_000, ttl_s: int = 3600) -> None:
        self.capacity = int(capacity)
        self.ttl_s = int(ttl_s)
        self._entries: OrderedDict[str, float] = OrderedDict()

    def add(self, key: str) -> bool:
        """Insert ``key``. Returns True if it was not already present."""
        now = time.monotonic()
        self._expire(now)
        if key in self._entries:
            self._entries.move_to_end(key)
            return False
        self._entries[key] = now + self.ttl_s
        while len(self._entries) > self.capacity:
            self._entries.popitem(last=False)
        return True

    def _expire(self, now: float) -> None:
        # Entries are appended in expiry order, so stopping at the first live one
        # keeps this amortised O(1) rather than a full scan.
        while self._entries:
            key, expires_at = next(iter(self._entries.items()))
            if expires_at > now:
                break
            self._entries.pop(key, None)

    def __contains__(self, key: object) -> bool:
        return key in self._entries

    def __len__(self) -> int:
        return len(self._entries)


class DedupGate:
    """Decides whether an event has been processed before."""

    def __init__(
        self,
        redis_client: Any = None,
        *,
        ttl_s: int = 3600,
        lru_capacity: int = 500_000,
        enabled: bool = True,
    ) -> None:
        self.redis = redis_client
        self.ttl_s = int(ttl_s)
        self.enabled = enabled
        self._lru = BoundedLRU(capacity=lru_capacity, ttl_s=ttl_s)
        self._redis_degraded = False

    @staticmethod
    def key(tenant_id: str, event_key: str) -> str:
        """Tenant-scoped Redis key.

        Namespacing by tenant keeps the erasure job able to delete one tenant's
        dedup state without scanning every key, and prevents a hash collision
        across tenants from suppressing a real event.
        """
        return f"tf:{tenant_id}:dedup:{event_key}"

    def check(self, tenant_id: str, event_key: str, claim_id: str | None = None) -> DedupVerdict:
        """Atomically claim ``event_key`` for ``tenant_id``."""
        if not self.enabled:
            return DedupVerdict(is_new=True, backend="disabled")

        if self.redis is not None:
            try:
                if claim_id is None:
                    claimed = self.redis.set(self.key(tenant_id, event_key), "1", nx=True, ex=self.ttl_s)
                else:
                    # A source offset owns a pending claim. Its replay may retry
                    # until the canonical output is acknowledged by Kafka.
                    claimed = self.redis.eval(
                        "local v=redis.call('GET',KEYS[1]); "
                        "if not v then redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2]); return 1 end; "
                        "if v==ARGV[1] then return 1 end; return 0",
                        1,
                        self.key(tenant_id, event_key),
                        "pending:" + claim_id,
                        self.ttl_s,
                    )
                if self._redis_degraded:
                    LOGGER.info("dedup backend recovered to redis")
                    self._redis_degraded = False
                if claimed:
                    return DedupVerdict(is_new=True, backend="redis")
                metrics.events_duplicated_total.labels(tenant=tenant_id).inc()
                return DedupVerdict(is_new=False, backend="redis")
            except Exception as error:
                if not self._redis_degraded:
                    # Logged once per outage, not once per event.
                    LOGGER.warning(
                        "redis dedup unavailable, falling back to in-process LRU",
                        extra={"error": str(error)},
                    )
                    self._redis_degraded = True

        is_new = self._lru.add(self.key(tenant_id, event_key))
        if not is_new:
            metrics.events_duplicated_total.labels(tenant=tenant_id).inc()
        backend = "lru_fallback" if self._redis_degraded else "lru"
        return DedupVerdict(is_new=is_new, backend=backend)

    def complete(self, tenant_id: str, event_key: str, claim_id: str) -> None:
        if self.redis is not None:
            self.redis.eval(
                "if redis.call('GET',KEYS[1])==ARGV[1] then "
                "redis.call('SET',KEYS[1],'1','EX',ARGV[2]); return 1 end; return 0",
                1,
                self.key(tenant_id, event_key),
                "pending:" + claim_id,
                self.ttl_s,
            )

    def forget(self, tenant_id: str, event_key: str) -> None:
        """Drop a claim. Used by tests and by the erasure job."""
        if self.redis is not None:
            try:  # noqa: SIM105 - best-effort shutdown/fallback boundary
                self.redis.delete(self.key(tenant_id, event_key))
            except Exception:  # pragma: no cover
                pass

    @property
    def degraded(self) -> bool:
        return self._redis_degraded

    def stats(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "backend": "lru_fallback" if self._redis_degraded else ("redis" if self.redis else "lru"),
            "lru_size": len(self._lru),
            "lru_capacity": self._lru.capacity,
            "ttl_s": self.ttl_s,
        }
