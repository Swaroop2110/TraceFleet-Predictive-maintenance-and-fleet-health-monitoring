"""Live push over WebSocket (feature ``websocket_live_push``).

``GET /api/v1/ws/live?token=<access token>`` streams incident lifecycle events and
a periodic rate sample for the caller's tenant.

The tenant comes from the verified token, never from a query parameter. The
previous implementation read ``tenant_id`` straight off the query string, which
meant any client could subscribe to any tenant's incident stream — the one place
where a missing check bypasses every other tenant control in the system.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

from apps.api.app.core.security import principal_from_websocket
from apps.api.app.ws.manager import manager
from packages.common import logging as tf_logging
from packages.common.dsn import redis_url
from packages.common.timeutil import now, to_iso

LOGGER = tf_logging.get_logger(__name__)

FEATURE_FLAGS = ("websocket_live_push",)

router = APIRouter(tags=["live"])

#: How often a client receives an event-rate sample when nothing else is happening.
HEARTBEAT_INTERVAL_S = 15.0


def _async_redis() -> Any:
    import redis.asyncio as redis_async

    return redis_async.from_url(redis_url(), decode_responses=True)


@router.websocket("/ws/live")
async def live_stream(websocket: WebSocket) -> None:
    """Tenant-scoped live incident stream."""
    principal = await principal_from_websocket(websocket)
    if principal is None:
        # Closed before accept, so an unauthenticated peer never reaches the
        # registry and cannot consume a connection slot.
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Authentication required")
        return

    if not principal.has("incident:read"):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Insufficient permission")
        return

    tenant_id = principal.tenant_id
    await manager.connect(websocket, tenant_id, redis_factory=_async_redis)

    try:
        await websocket.send_text(
            json.dumps(
                {
                    "type": "connected",
                    "tenant_id": tenant_id,
                    "user_id": principal.user_id,
                    "server_time": to_iso(now()),
                    "clients": manager.client_count(tenant_id),
                }
            )
        )
        await _serve(websocket, tenant_id)
    except WebSocketDisconnect:
        pass
    except Exception as error:
        LOGGER.warning("live stream error", extra={"tenant": tenant_id, "error": str(error)})
    finally:
        await manager.disconnect(websocket, tenant_id)


async def _serve(websocket: WebSocket, tenant_id: str) -> None:
    """Hold the connection open, answering pings and sending rate samples.

    Incident messages arrive from the manager's per-tenant Redis pump, so this loop
    only handles client traffic and the heartbeat. The heartbeat doubles as liveness
    detection: a peer that has vanished without closing surfaces as a failed send.
    """
    heartbeat = asyncio.create_task(_heartbeat(websocket, tenant_id))
    try:
        while True:
            message = await websocket.receive_text()
            if message == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "server_time": to_iso(now())}))
    finally:
        heartbeat.cancel()


async def _heartbeat(websocket: WebSocket, tenant_id: str) -> None:
    client = None
    try:
        client = _async_redis()
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_S)
            rate = await _event_rate(client, tenant_id)
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "rate",
                        "tenant_id": tenant_id,
                        "events_per_second": rate,
                        "server_time": to_iso(now()),
                    }
                )
            )
    except asyncio.CancelledError:
        raise
    except Exception:
        # The connection has gone; the outer handler performs the cleanup.
        return
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                await client.aclose()


async def _event_rate(client: Any, tenant_id: str) -> float:
    """Recent ingest rate, from the per-second counters the validator writes."""
    try:
        counters = await client.hgetall(f"tf:{tenant_id}:metrics:rate")
    except Exception:
        return 0.0
    if not counters:
        return 0.0
    current = int(now().timestamp())
    # A 10 s trailing window: long enough to be stable, short enough to look live.
    recent = [int(value) for key, value in counters.items() if str(key).isdigit() and 0 <= current - int(key) <= 10]
    return round(sum(recent) / 10.0, 2) if recent else 0.0
