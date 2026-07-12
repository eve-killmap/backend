import asyncio
import hashlib
import json
import time

import redis.asyncio as aioredis

from app.config import config
from app.metrics import metrics
from app import prometheus_metrics as pm


def _hash_params(params: dict) -> str:
    return hashlib.md5(
        json.dumps(params, sort_keys=True, default=str).encode()
    ).hexdigest()[:16]


class QueryCache:
    """Redis-backed cache for serialized Pydantic API responses."""

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    def set_redis(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def get(self, prefix: str, params: dict) -> str | None:
        if self._redis is None:
            return None
        _start = time.perf_counter()
        value = await self._redis.get(f"query:{prefix}:{_hash_params(params)}")
        pm.redis_command_seconds.labels(op="get").observe(time.perf_counter() - _start)
        if value is None:
            metrics.cache_misses += 1
            pm.cache_misses.labels(cache=prefix).inc()
        else:
            metrics.cache_hits += 1
            pm.cache_hits.labels(cache=prefix).inc()
        # Redis may return bytes when decode_responses=False; normalise to str.
        return value.decode() if isinstance(value, bytes) else value

    async def set(
        self, prefix: str, params: dict, value: str, ttl: int | None = None
    ) -> None:
        if self._redis is None:
            return
        ttl = config.cache.query_ttl if ttl is None else ttl
        _start = time.perf_counter()
        await self._redis.set(f"query:{prefix}:{_hash_params(params)}", value, ex=ttl)
        pm.redis_command_seconds.labels(op="set").observe(time.perf_counter() - _start)


class KillsBinaryCache:
    """Redis-backed cache for binary-encoded kill payloads (raw bytes)."""

    def __init__(self) -> None:
        self._redis: aioredis.Redis | None = None

    def set_redis(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    async def get(self, params: dict) -> bytes | None:
        if self._redis is None:
            return None
        _start = time.perf_counter()
        val = await self._redis.get(f"kills:binary:{_hash_params(params)}")
        pm.redis_command_seconds.labels(op="get").observe(time.perf_counter() - _start)
        if val is None:
            metrics.cache_misses += 1
            pm.cache_misses.labels(cache="binary").inc()
            return None
        metrics.cache_hits += 1
        pm.cache_hits.labels(cache="binary").inc()
        # Redis may return str when decode_responses=True; normalise to bytes.
        return val.encode() if isinstance(val, str) else val

    async def set(self, params: dict, value: bytes) -> None:
        if self._redis is None:
            return
        _start = time.perf_counter()
        await self._redis.set(
            f"kills:binary:{_hash_params(params)}", value, ex=config.cache.binary_ttl
        )
        pm.redis_command_seconds.labels(op="set").observe(time.perf_counter() - _start)


class SingleFlight:
    """Per-key asyncio lock registry to collapse concurrent rebuilds in a worker."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}

    def lock(self, key: str) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock


single_flight = SingleFlight()

query_cache = QueryCache()
kills_binary_cache = KillsBinaryCache()


def should_short_circuit(since: int | None, latest: int | None) -> bool:
    return since is not None and latest is not None and since >= latest


async def get_system_latest(solar_system_id: int) -> int | None:
    """Return the per-system MAX(inserted_time) epoch, Redis-cached with a short TTL."""
    if query_cache._redis is None:
        return None
    key = f"kills:system_latest:{solar_system_id}"
    cached = await query_cache._redis.get(key)
    if cached is not None:
        return int(cached)
    from app.queries import fetch_system_latest_inserted

    latest = await fetch_system_latest_inserted(solar_system_id)
    if latest is not None:
        await query_cache._redis.set(
            key, str(latest), ex=config.cache.system_latest_ttl
        )
    return latest
