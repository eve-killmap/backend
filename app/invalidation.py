from __future__ import annotations

import asyncio
import json
import logging

import redis.asyncio as aioredis

from app import prometheus_metrics as pm

logger = logging.getLogger(__name__)

INVALIDATION_PATTERNS = {
    "system_rankings": "query:system_rankings:*",
    "farthest_kill": "query:farthest_kill:*",
    "sov": "query:sov:*",
}


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
    bus: aioredis.Redis, cache: aioredis.Redis, channel: str
) -> None:
    """Listen for invalidation messages on ``bus`` and evict matching keys from
    ``cache``.

    ``bus`` is the shared pub/sub server every worker and publisher can reach (the
    stream Redis); ``cache`` is this worker's own response-cache Redis where the
    ``query:*`` keys live. They may be the same server or different ones.
    """
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
            for target in targets:
                pattern = INVALIDATION_PATTERNS.get(target)
                if pattern is None:
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
    except asyncio.CancelledError:
        raise
    finally:
        try:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        except Exception:
            pass
