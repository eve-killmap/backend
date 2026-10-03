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
    origin = websocket.headers.get("origin")
    if not origin_allowed(origin, config.cors.allow_origins):
        await websocket.accept()
        await websocket.close(code=1008, reason="Origin not allowed")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="rejected_origin"
        ).inc()
        return False
    if at_capacity(metrics.ws_global_connections, config.limits.max_ws_connections):
        await websocket.accept()
        await websocket.close(code=1013, reason="Server at capacity")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="rejected_capacity"
        ).inc()
        return False
    if not broadcaster.is_running:
        await websocket.accept()
        await websocket.close(code=1011, reason="Live streaming unavailable")
        prometheus_metrics.ws_connections.labels(
            transport="ws", outcome="unavailable"
        ).inc()
        return False
    prometheus_metrics.ws_connections.labels(transport="ws", outcome="accepted").inc()
    return True


@router.websocket("/ws/global/kills")
async def ws_kills_live(websocket: WebSocket):
    if not await _ws_guard(websocket):
        return
    q = broadcaster.subscribe_global()
    try:
        await _ws_stream(websocket, q)
    finally:
        broadcaster.unsubscribe_global(q)
