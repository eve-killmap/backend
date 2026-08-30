from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Awaitable, Callable

import redis.asyncio as aioredis

from app import prometheus_metrics as pm

if TYPE_CHECKING:
    from app.redis_client import KillBroadcaster

logger = logging.getLogger(__name__)

INVALIDATION_PATTERNS = {
    "system_rankings": "query:v2:system_rankings:*",
    "system_kills": "query:v2:system_kills:*",
    "global_kills": "query:v2:global_kills:*",
    "farthest_kill": "query:v2:farthest_kill:*",
    "sov": "query:v2:sov:*",
    "sov_map": "query:v2:sov_map:*",
}

# Targets whose response cache is repopulated ("warmed") right after a flush,
# rather than left to be rebuilt lazily on the next request. Only the leader
# flushes+warms these — every other worker keeps serving the shared cache
# until the leader's warm completes, avoiding duplicate warm work and a
# flush/warm race between workers.
WARMABLE_TARGETS = {"system_rankings", "system_kills", "global_kills"}


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
    warm: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    """Listen for invalidation messages on ``bus`` and evict matching keys from
    ``cache``.

    ``bus`` is the shared pub/sub server every worker and publisher can reach (the
    stream Redis); ``cache`` is this worker's own response-cache Redis where the
    ``query:*`` keys live. They may be the same server or different ones.

    ``broadcaster`` supplies ``is_leader`` so that WARMABLE_TARGETS are only
    flushed by the leader worker (which owns flush+warm for those targets);
    non-leaders skip them entirely and keep serving the shared cache. ``warm``
    is an optional callback invoked after a warmable target's flush (wired up
    in a later task); it is unused for now.
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
                if target in WARMABLE_TARGETS and not broadcaster.is_leader:
                    continue  # leader owns flush+warm for these; avoids the flush/warm race
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
