import asyncio
import json

import pytest
from prometheus_client import REGISTRY

from app import redis_client as rc


def _sample(name, labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeRedis:
    def __init__(self):
        self.published = []

    async def publish(self, channel, data):
        self.published.append((channel, data))


def test_refresh_once_publishes_both_targets_and_sleeps_min_ttl(monkeypatch):
    b = rc.KillBroadcaster()
    b._redis = _FakeRedis()

    async def fmap():
        return 3600

    async def fstruct():
        return 1800

    monkeypatch.setattr(rc.esi_client, "refresh_sov_map", fmap)
    monkeypatch.setattr(rc.esi_client, "refresh_sov_structures", fstruct)

    ok0 = _sample("eve_killmap_sov_refreshes_total", {"outcome": "ok"})
    sleep_s = asyncio.run(b._sov_refresh_once())

    assert sleep_s == 1740  # min(max(min(3600,1800)-60,60),3600)
    _, data = b._redis.published[0]
    assert json.loads(data)["targets"] == ["sov", "sov_map"]
    assert _sample("eve_killmap_sov_refreshes_total", {"outcome": "ok"}) - ok0 == 1


def test_refresh_once_degrades_when_structures_fail(monkeypatch):
    b = rc.KillBroadcaster()
    b._redis = _FakeRedis()

    async def fmap():
        return 3600

    async def fstruct():
        raise RuntimeError("structures down")

    monkeypatch.setattr(rc.esi_client, "refresh_sov_map", fmap)
    monkeypatch.setattr(rc.esi_client, "refresh_sov_structures", fstruct)

    d0 = _sample("eve_killmap_sov_refreshes_total", {"outcome": "degraded"})
    sleep_s = asyncio.run(b._sov_refresh_once())

    assert sleep_s == 3540  # adm_ttl None -> min(3600,3600)-60
    assert b._redis.published  # still invalidated despite the ADM failure
    assert _sample("eve_killmap_sov_refreshes_total", {"outcome": "degraded"}) - d0 == 1


def test_refresh_once_propagates_map_failure(monkeypatch):
    b = rc.KillBroadcaster()
    b._redis = _FakeRedis()

    async def fmap():
        raise RuntimeError("map down")

    monkeypatch.setattr(rc.esi_client, "refresh_sov_map", fmap)
    with pytest.raises(RuntimeError):
        asyncio.run(b._sov_refresh_once())
    assert b._redis.published == []  # nothing published on a map failure
