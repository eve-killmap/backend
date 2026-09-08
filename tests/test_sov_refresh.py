import asyncio
import json

import pytest
from prometheus_client import REGISTRY

from app import redis_client as rc
from app.esi import SOV_MAP, SOV_STRUCTURES, EsiFeedRefreshError


def _sample(name, labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeRedis:
    def __init__(self):
        self.published = []

    async def publish(self, channel, data):
        self.published.append((channel, data))


def test_refresh_once_publishes_invalidation_and_returns_cadence(monkeypatch):
    b = rc.KillBroadcaster()
    b._redis = _FakeRedis()

    async def fake_refresh(feed):
        return 3600, {"1": {}}

    monkeypatch.setattr(rc.esi_client, "refresh", fake_refresh)
    ok0 = _sample(
        "eve_killmap_esi_feed_refreshes_total", {"feed": "sov", "outcome": "ok"}
    )
    delay = asyncio.run(b._esi_refresh_once(SOV_MAP))

    assert delay == 3540  # clamp(3600 - 60, 60, 3600)
    channel, data = b._redis.published[0]
    assert json.loads(data)["targets"] == ["sov", "sov_map"]
    # the ok counter is incremented inside esi_client.refresh, which is faked here
    assert (
        _sample("eve_killmap_esi_feed_refreshes_total", {"feed": "sov", "outcome": "ok"})
        == ok0
    )


def test_refresh_once_broadcasts_when_channel_set(monkeypatch):
    from app.esi import STATUS

    b = rc.KillBroadcaster()
    b._redis = _FakeRedis()

    async def fake_refresh(feed):
        return 30, {"online": True, "players": 5}

    monkeypatch.setattr(rc.esi_client, "refresh", fake_refresh)
    delay = asyncio.run(b._esi_refresh_once(STATUS))

    assert delay == 28  # clamp(30 - 2, 15, 60)
    channels = [c for c, _ in b._redis.published]
    assert rc.config.streaming.status_channel in channels


def test_retry_delay_backs_off_and_caps():
    b = rc.KillBroadcaster()
    assert b._retry_delay(1) == 30
    assert b._retry_delay(2) == 60
    assert b._retry_delay(3) == 120
    assert b._retry_delay(99) == 600  # capped


def test_refresh_once_propagates_feed_error(monkeypatch):
    b = rc.KillBroadcaster()
    b._redis = _FakeRedis()

    async def boom(feed):
        raise EsiFeedRefreshError(feed.name)

    monkeypatch.setattr(rc.esi_client, "refresh", boom)
    with pytest.raises(EsiFeedRefreshError):
        asyncio.run(b._esi_refresh_once(SOV_STRUCTURES))
    assert b._redis.published == []  # nothing published on failure
