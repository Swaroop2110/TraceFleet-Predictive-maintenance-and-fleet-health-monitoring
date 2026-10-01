"""WebSocket connection registry and the Redis fan-out that feeds it.

The design problem: every browser tab wants the same tenant's incident stream, and
Redis pub/sub delivers to subscribers, not to sockets. The previous version opened
a Redis subscription *per connection* and then broadcast each message to every
socket on the tenant, so with N tabs open each message arrived N times per tab.

Here there is exactly one Redis subscription per tenant, held by a single pump
task, and it fans out to that tenant's sockets once. The subscription is created
when the first client for a tenant connects and torn down when the last leaves, so
an idle deployment holds no subscriptions at all.

A slow client is dropped rather than allowed to block the pump. Live push is a
convenience — the dashboard also polls — and one stalled browser must not delay
alerts for everyone else.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any

from fastapi import WebSocket

from packages.common import logging as tf_logging

LOGGER = tf_logging.get_logger(__name__)

#: Messages buffered per client before it is considered stalled and dropped.
CLIENT_QUEUE_SIZE = 100
#: How long a single send may take before the client is treated as unresponsive.
SEND_TIMEOUT_S = 5.0


class ConnectionManager:
    """Per-tenant socket registry with one Redis subscription per tenant."""

    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = defaultdict(set)
        self._pumps: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, tenant_id: str, redis_factory: Any = None) -> None:
        await websocket.accept()
        async with self._lock:
            first_for_tenant = not self._connections[tenant_id]
            self._connections[tenant_id].add(websocket)
            if first_for_tenant and redis_factory is not None:
                self._pumps[tenant_id] = asyncio.create_task(
                    self._pump(tenant_id, redis_factory),
                    name=f"ws-pump-{tenant_id}",
                )
        LOGGER.info(
            "websocket connected",
            extra={"tenant": tenant_id, "clients": len(self._connections[tenant_id])},
        )

    async def disconnect(self, websocket: WebSocket, tenant_id: str) -> None:
        async with self._lock:
            self._connections[tenant_id].discard(websocket)
            if not self._connections[tenant_id]:
                self._connections.pop(tenant_id, None)
                pump = self._pumps.pop(tenant_id, None)
                if pump is not None:
                    pump.cancel()
        LOGGER.info("websocket disconnected", extra={"tenant": tenant_id})

    def client_count(self, tenant_id: str | None = None) -> int:
        if tenant_id is not None:
            return len(self._connections.get(tenant_id, ()))
        return sum(len(clients) for clients in self._connections.values())

    def tenants(self) -> list[str]:
        return sorted(self._connections)

    async def broadcast(self, payload: str, tenant_id: str) -> int:
        """Send to every socket on a tenant. Returns the delivery count."""
        clients = tuple(self._connections.get(tenant_id, ()))
        if not clients:
            return 0

        async def send(client: WebSocket) -> bool:
            try:
                await asyncio.wait_for(client.send_text(payload), timeout=SEND_TIMEOUT_S)
                return True
            except (TimeoutError, Exception):
                return False

        results = await asyncio.gather(*(send(client) for client in clients))
        delivered = 0
        for client, ok in zip(clients, results, strict=False):
            if ok:
                delivered += 1
            else:
                # Dropped here rather than retried: the socket is already gone or
                # the peer is not reading, and either way further sends will fail.
                await self.disconnect(client, tenant_id)
        return delivered

    async def _pump(self, tenant_id: str, redis_factory: Any) -> None:
        """Relay ``tf:{tenant}:incidents`` to the tenant's sockets.

        Reconnects with backoff. Redis dropping out should degrade the dashboard to
        its polling fallback, not permanently silence the live stream.
        """
        backoff = 1.0
        while True:
            client = None
            pubsub = None
            channel = f"tf:{tenant_id}:incidents"
            try:
                client = redis_factory()
                pubsub = client.pubsub()
                await pubsub.subscribe(channel)
                LOGGER.info("subscribed to incident channel", extra={"channel": channel})
                backoff = 1.0
                while True:
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if message is None:
                        # No traffic. Stop the pump if the last client has gone.
                        if not self._connections.get(tenant_id):
                            return
                        continue
                    data = message.get("data")
                    payload = data.decode("utf-8") if isinstance(data, bytes) else str(data)
                    await self.broadcast(payload, tenant_id)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOGGER.warning(
                    "incident pump failed, retrying",
                    extra={"tenant": tenant_id, "backoff_s": backoff, "error": str(error)},
                )
                await asyncio.sleep(backoff)
                backoff = min(30.0, backoff * 2)
            finally:
                for closer in (pubsub, client):
                    try:
                        if closer is not None:
                            await closer.aclose()
                    except Exception:
                        pass

    async def shutdown(self) -> None:
        """Cancel every pump. Called from application shutdown."""
        async with self._lock:
            for pump in self._pumps.values():
                pump.cancel()
            self._pumps.clear()
            self._connections.clear()


manager = ConnectionManager()
