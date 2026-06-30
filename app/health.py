from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import time

import redis.asyncio as aioredis

from app.metrics import metrics

logger = logging.getLogger(__name__)

HEARTBEAT_PREFIX = "health:worker:"

_redis: aioredis.Redis | None = None


def set_redis(redis: aioredis.Redis) -> None:
    global _redis
    _redis = redis


async def redis_ok() -> bool:
    if _redis is None:
        return False
    try:
        return bool(await _redis.ping())
    except Exception:
        return False


def health_token_ok(provided: str | None, expected: str | None) -> bool:
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided, expected)


def extract_bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() or None


_COUNTER_KEYS = (
    "requests",
    "db_queries",
    "cache_hits",
    "cache_misses",
    "ws_global_connections",
    "ws_system_connections",
)


def aggregate_workers(payloads: list[dict], expected_workers: int | None) -> dict:
    totals = {key: 0 for key in _COUNTER_KEYS}
    for p in payloads:
        for key in _COUNTER_KEYS:
            totals[key] += int(p.get(key, 0))
    hits = totals["cache_hits"]
    misses = totals["cache_misses"]
    hit_rate = round(hits / (hits + misses), 4) if (hits + misses) else None
    count = len(payloads)
    return {
        "worker_count": count,
        "degraded": expected_workers is not None and count < expected_workers,
        "totals": totals,
        "cache_hit_rate": hit_rate,
        "workers": payloads,
    }


async def read_worker_heartbeats() -> list[dict]:
    if _redis is None:
        return []
    payloads: list[dict] = []
    async for key in _redis.scan_iter(match=f"{HEARTBEAT_PREFIX}*"):
        raw = await _redis.get(key)
        if raw is not None:
            try:
                payloads.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    return payloads


async def redis_info() -> dict:
    if _redis is None:
        return {"available": False}
    try:
        info = await _redis.info()
        return {
            "available": True,
            "used_memory": info.get("used_memory"),
            "connected_clients": info.get("connected_clients"),
            "uptime_in_seconds": info.get("uptime_in_seconds"),
        }
    except Exception as exc:
        return {"available": False, "error": str(exc)}


def build_heartbeat(pid: int, snapshot: dict, now: float) -> dict:
    return {"pid": pid, "ts": now, **snapshot}


async def heartbeat_loop(redis: aioredis.Redis, interval: int, ttl: int) -> None:
    """Write this worker's heartbeat every `interval` seconds with a `ttl` expiry."""
    pid = os.getpid()
    key = f"{HEARTBEAT_PREFIX}{pid}"
    logger.info("Health heartbeat started (pid=%s, interval=%ss)", pid, interval)
    while True:
        try:
            payload = build_heartbeat(pid, metrics.snapshot(), time.time())
            await redis.set(key, json.dumps(payload), ex=ttl)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Heartbeat write failed: %s", exc)
        await asyncio.sleep(interval)
