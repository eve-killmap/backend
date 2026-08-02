import asyncio
import json

from fastapi import APIRouter, WebSocket

from app.config import config
from app.metrics import metrics
from app.security import origin_allowed, at_capacity
from app.redis_client import broadcaster
from app import prometheus_metrics

router = APIRouter()


async def _ws_stream(websocket: WebSocket, q: asyncio.Queue) -> None:
    """Accept a WebSocket and stream kills from q until the client disconnects."""
    await websocket.accept()

    async def _send() -> None:
        while True:
            kill = await q.get()
            await websocket.send_text(json.dumps(kill))
            await asyncio.sleep(0.25 if q.qsize() > 10 else 0.5)

    send_task = asyncio.create_task(_send())
    try:
        while True:
            msg = await websocket.receive()
            if msg["type"] == "websocket.disconnect":
                break
    except Exception:
        pass
    finally:
        send_task.cancel()
        await asyncio.gather(send_task, return_exceptions=True)


async def _ws_guard(websocket: WebSocket) -> bool:
    """Reject the socket (returns False) if the Origin is not allowed, the server
    is over its connection cap, or the broadcaster is not running.
    Otherwise returns True WITHOUT accepting; the caller accepts via _ws_stream.
    On rejection the socket is already accepted+closed."""
    origin = websocket.headers.get("origin")
    if not origin_allowed(origin, config.cors.allow_origins):
        await websocket.accept()
        await websocket.close(code=1008, reason="Origin not allowed")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="rejected_origin"
        ).inc()
        return False
    current = metrics.ws_global_connections + metrics.ws_system_connections
    if at_capacity(current, config.limits.max_ws_connections):
        await websocket.accept()
        await websocket.close(code=1013, reason="Server at capacity")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="rejected_capacity"
        ).inc()
        return False
    if not broadcaster.is_running:
        await websocket.accept()
        await websocket.close(code=1011, reason="Live kill streaming unavailable")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="unavailable"
        ).inc()
        return False
    prometheus_metrics.ws_connections.labels(transport="ws", outcome="accepted").inc()
    return True


@router.websocket("/ws/global/kills")
async def ws_kills_live(websocket: WebSocket):
    """Stream every new kill across all solar systems."""
    if not await _ws_guard(websocket):
        return
    q = broadcaster.subscribe_global()
    try:
        await _ws_stream(websocket, q)
    finally:
        broadcaster.unsubscribe_global(q)


@router.websocket("/ws/systems/{solar_system_id}/kills")
async def ws_system_kills(websocket: WebSocket, solar_system_id: int):
    """Stream new kills for a specific solar system."""
    if not await _ws_guard(websocket):
        return
    q = broadcaster.subscribe_system(solar_system_id)
    try:
        await _ws_stream(websocket, q)
    finally:
        broadcaster.unsubscribe_system(solar_system_id, q)
