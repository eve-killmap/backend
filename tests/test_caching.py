import asyncio
import asyncio as _asyncio

from app.cache import SingleFlight
from app.esi import EsiClient


def test_single_flight_returns_same_lock_per_key():
    sf = SingleFlight()

    async def go():
        a = sf.lock("k1")
        b = sf.lock("k1")
        c = sf.lock("k2")
        return a is b, a is c

    same, diff = asyncio.run(go())
    assert same is True
    assert diff is False


class _FakeRedisStr:
    def __init__(self, value):
        self._value = value

    async def get(self, key):
        return self._value


def test_get_sov_map_cached_int_keys():
    client = EsiClient()
    client._redis = _FakeRedisStr('{"30000142": {"system_id": 30000142}}')  # type: ignore[attr-defined]
    result = _asyncio.run(client.get_sov_map_cached())
    assert result == {30000142: {"system_id": 30000142}}


def test_get_sov_map_cached_none_without_redis():
    client = EsiClient()
    assert _asyncio.run(client.get_sov_map_cached()) is None


from app.cache import should_short_circuit


def test_should_short_circuit():
    assert should_short_circuit(since=100, latest=90) is True  # client up to date
    assert should_short_circuit(since=100, latest=100) is True
    assert should_short_circuit(since=100, latest=101) is False  # newer kill exists
    assert should_short_circuit(since=None, latest=100) is False  # full fetch
    assert (
        should_short_circuit(since=100, latest=None) is False
    )  # unknown -> must query


from app.models import KillDetail, Victim, RawKillDetailResponse
from app.queries import merge_kill_details
from datetime import datetime, timezone


def _kill(kid):
    return KillDetail(
        killmail_id=kid,
        killmail_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
        position=(0.0, 0.0, 0.0),
        war_id=None,
        victim=Victim(
            character_id=1,
            corporation_id=None,
            alliance_id=None,
            faction_id=None,
            damage_taken=10,
            ship_type_id=587,
        ),
        attackers=[],
        inserted_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_merge_kill_details_counts():
    resp = merge_kill_details([_kill(1), _kill(2)])
    assert isinstance(resp, RawKillDetailResponse)
    assert resp.count == 2
    assert {k.killmail_id for k in resp.kills} == {1, 2}


import app.prometheus_metrics as pm  # noqa: E402
from prometheus_client import REGISTRY  # noqa: E402


def _sample(name, labels=None):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeRedisGet:
    def __init__(self, value):
        self._value = value

    async def get(self, key):
        return self._value


def test_query_cache_hit_and_miss_metrics():
    from app.cache import QueryCache

    hit = QueryCache(); hit.set_redis(_FakeRedisGet("cached"))
    miss = QueryCache(); miss.set_redis(_FakeRedisGet(None))

    h0 = _sample("eve_killmap_cache_hits_total", {"cache": "sov"})
    m0 = _sample("eve_killmap_cache_misses_total", {"cache": "sov"})
    g0 = _sample("eve_killmap_redis_command_seconds_count", {"op": "get"})

    asyncio.run(hit.get("sov", {"a": 1}))
    asyncio.run(miss.get("sov", {"a": 2}))

    assert _sample("eve_killmap_cache_hits_total", {"cache": "sov"}) - h0 == 1
    assert _sample("eve_killmap_cache_misses_total", {"cache": "sov"}) - m0 == 1
    assert _sample("eve_killmap_redis_command_seconds_count", {"op": "get"}) - g0 == 2


class _FakeRedisSet:
    async def set(self, *a, **k):
        return True


def test_query_cache_set_times_op():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisSet())
    s0 = _sample("eve_killmap_redis_command_seconds_count", {"op": "set"})
    asyncio.run(qc.set("sov", {"a": 1}, "value", ttl=10))
    assert _sample("eve_killmap_redis_command_seconds_count", {"op": "set"}) - s0 == 1


def test_binary_cache_uses_binary_label():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisGet(None))
    m0 = _sample("eve_killmap_cache_misses_total", {"cache": "binary"})
    asyncio.run(kbc.get({"a": 1}))
    assert _sample("eve_killmap_cache_misses_total", {"cache": "binary"}) - m0 == 1


def test_esi_corp_cache_hit_metric():
    from app.esi import EsiClient
    import json as _json

    client = EsiClient()
    client._redis = _FakeRedisGet(_json.dumps(["CorpName", "TICK"]))  # type: ignore[attr-defined]
    h0 = _sample("eve_killmap_esi_cache_hits_total", {"entity": "corporation"})
    result = asyncio.run(client.get_corporation_info(123))
    assert result == ("CorpName", "TICK")
    assert (
        _sample("eve_killmap_esi_cache_hits_total", {"entity": "corporation"}) - h0
        == 1
    )


from redis.exceptions import RedisError  # noqa: E402


class _FakeRedisRaising:
    async def get(self, key):
        raise RedisError("boom")

    async def set(self, *a, **k):
        raise RedisError("boom")


def test_query_cache_get_degrades_on_redis_error():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisRaising())
    e0 = _sample("eve_killmap_errors_total", {"component": "cache"})
    result = asyncio.run(qc.get("sov", {"a": 1}))  # must not raise
    assert result is None
    assert _sample("eve_killmap_errors_total", {"component": "cache"}) - e0 == 1


def test_query_cache_set_degrades_on_redis_error():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisRaising())
    e0 = _sample("eve_killmap_errors_total", {"component": "cache"})
    asyncio.run(qc.set("sov", {"a": 1}, "value", ttl=10))  # must not raise
    assert _sample("eve_killmap_errors_total", {"component": "cache"}) - e0 == 1


def test_binary_cache_get_degrades_on_redis_error():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisRaising())
    result = asyncio.run(kbc.get({"a": 1}))  # must not raise
    assert result is None
