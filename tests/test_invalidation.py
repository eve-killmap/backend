import asyncio
import json

import app.invalidation as invalidation
from app.invalidation import patterns_for_targets


def test_patterns_for_known_targets():
    assert patterns_for_targets(["system_rankings"]) == ["query:system_rankings:*"]
    out = patterns_for_targets(["sov", "farthest_kill"])
    assert set(out) == {"query:sov:*", "query:farthest_kill:*"}


def test_patterns_ignores_unknown():
    assert patterns_for_targets(["nope"]) == []


class _FakePubSub:
    def __init__(self, messages):
        self._messages = messages
        self.subscribed = None
        self.unsubscribed = None
        self.closed = False

    async def subscribe(self, channel):
        self.subscribed = channel

    async def listen(self):
        for m in self._messages:
            yield m

    async def unsubscribe(self, channel):
        self.unsubscribed = channel

    async def aclose(self):
        self.closed = True


class _FakeBus:
    """Stands in for the shared pub/sub (stream) Redis. Has no scan/delete."""

    def __init__(self, pubsub):
        self._pubsub = pubsub

    def pubsub(self):
        return self._pubsub


class _FakeCache:
    """Stands in for the response-cache Redis where query:* keys live."""

    def __init__(self, keys):
        self._keys = list(keys)
        self.deleted: list[str] = []

    async def scan_iter(self, match=None, count=None):
        prefix = match[:-1] if match and match.endswith("*") else match
        for key in self._keys:
            if prefix is None or key.startswith(prefix):
                yield key

    async def delete(self, *keys):
        self.deleted.extend(keys)
        return len(keys)


def test_subscriber_subscribes_on_bus_and_deletes_on_cache():
    # One sov invalidation message arrives on the bus.
    msg = {"type": "message", "data": json.dumps({"targets": ["sov"]})}
    pubsub = _FakePubSub([{"type": "subscribe"}, msg])
    bus = _FakeBus(pubsub)
    cache = _FakeCache(["query:sov:abc", "query:sov:def", "query:system_rankings:x"])

    asyncio.run(invalidation.subscriber_loop(bus, cache, "cache:invalidate"))

    # Subscription happened on the bus connection...
    assert pubsub.subscribed == "cache:invalidate"
    # ...and only the matching sov keys were deleted, on the CACHE connection.
    assert set(cache.deleted) == {"query:sov:abc", "query:sov:def"}
    # ...and the subscriber cleaned up on exit.
    assert pubsub.unsubscribed == "cache:invalidate"
    assert pubsub.closed is True


def test_subscriber_ignores_unknown_targets():
    msg = {"type": "message", "data": json.dumps({"targets": ["bogus"]})}
    pubsub = _FakePubSub([msg])
    cache = _FakeCache(["query:sov:abc"])

    asyncio.run(
        invalidation.subscriber_loop(_FakeBus(pubsub), cache, "cache:invalidate")
    )

    assert cache.deleted == []


def test_subscriber_skips_malformed_message():
    # A non-JSON payload must be logged + skipped, never raised, and never delete.
    bad = {"type": "message", "data": "not json{"}
    pubsub = _FakePubSub([bad])
    cache = _FakeCache(["query:sov:abc"])

    asyncio.run(
        invalidation.subscriber_loop(_FakeBus(pubsub), cache, "cache:invalidate")
    )

    assert cache.deleted == []
    assert pubsub.closed is True
