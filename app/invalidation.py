from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Awaitable, Callable

import redis.asyncio as aioredis

from app import prometheus_metrics as pm
from app.cache import QUERY_KEY_VERSION

if TYPE_CHECKING:
    from app.redis_client import KillBroadcaster

logger = logging.getLogger(__name__)

_TARGET_PREFIXES = (
    "system_rankings",
    "system_kills",
    "global_kills",
    "farthest_kill",
    "leaderboards",
    "sov",
    "sov_map",
    "system_jumps",
)
INVALIDATION_PATTERNS = {
    t: f"query:{QUERY_KEY_VERSION}:{t}:*" for t in _TARGET_PREFIXES
}

# Only the leader flushes/warms these, avoiding a flush/warm race between workers
WARMABLE_TARGETS = {"system_rankings", "system_kills", "global_kills", "leaderboards"}


def patterns_for_targets(targets: list[str]) -> list[str]:
    return [INVALIDATION_PATTERNS[t] for t in targets if t in INVALIDATION_PATTERNS]


async def _delete_pattern(redis: aioredis.Redis, pattern: str) -> int:
    deleted = 0
    keys: list[str] = []
    async for key in redis.scan_iter(match=pattern, count=500):
        keys.append(key)
        if len(keys) >= 500:
            deleted += await redis.delete(*keys)
            keys = []
    if keys:
        deleted += await redis.delete(*keys)
    return deleted


async def subscriber_loop(
    bus: aioredis.Redis,
    cache: aioredis.Redis,
    channel: str,
    broadcaster: "KillBroadcaster",
    warm: Callable[[], Awaitable[None]] | None = None,
) -> None:
    pubsub = bus.pubsub()
    await pubsub.subscribe(channel)
    logger.info("Cache invalidation subscriber listening on %s", channel)
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            try:
                targets = json.loads(message["data"]).get("targets", [])
            except (json.JSONDecodeError, AttributeError) as exc:
                logger.warning("Bad invalidation message: %s", exc)
                continue
            saw_warmable = False
            for target in targets:
                pattern = INVALIDATION_PATTERNS.get(target)
                if pattern is None:
                    continue
                if target in WARMABLE_TARGETS and not broadcaster.is_leader:
                    continue
                pm.cache_invalidations_received.labels(target=target).inc()
                try:
                    n = await _delete_pattern(cache, pattern)
                    pm.cache_keys_evicted.labels(target=target).inc(n)
                    logger.debug("Invalidated %s key(s) for %s", n, pattern)
                except Exception as exc:
                    pm.errors.labels(component="invalidation").inc()
                    logger.warning(
                        "Invalidation delete failed for %s: %s", pattern, exc
                    )
                if target in WARMABLE_TARGETS:
                    saw_warmable = True
            if saw_warmable and broadcaster.is_leader and warm is not None:
                try:
                    await warm()
                except Exception as exc:
                    pm.errors.labels(component="cache_warm").inc()
                    logger.warning("Cache warm callback failed: %s", exc)
    except asyncio.CancelledError:
        raise
    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass
