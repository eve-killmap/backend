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


import hashlib as _hashlib


def _query_frame(body_str: str, *, gzipped: bool = False, body_bytes: bytes | None = None) -> bytes:
    raw = body_str.encode()
    digest = _hashlib.md5(raw).digest()
    stored_body = body_bytes if body_bytes is not None else raw
    return bytes([1 if gzipped else 0]) + digest + stored_body


def test_query_cache_hit_returns_etag_gzip_body():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisGet(_query_frame("cached")))
    res = asyncio.run(qc.get("sov", {"a": 1}))
    assert res is not None
    etag, gzipped, body = res
    assert etag == '"' + _hashlib.md5(b"cached").hexdigest() + '"'
    assert gzipped is False
    assert body == b"cached"


def test_query_cache_miss_returns_none():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisGet(None))
    assert asyncio.run(qc.get("sov", {"a": 2})) is None


def test_query_cache_hit_and_miss_metrics():
    from app.cache import QueryCache

    hit = QueryCache(); hit.set_redis(_FakeRedisGet(_query_frame("cached")))
    miss = QueryCache(); miss.set_redis(_FakeRedisGet(None))

    h0 = _sample("eve_killmap_cache_hits_total", {"cache": "sov"})
    m0 = _sample("eve_killmap_cache_misses_total", {"cache": "sov"})
    g0 = _sample("eve_killmap_redis_command_seconds_count", {"op": "get"})

    asyncio.run(hit.get("sov", {"a": 1}))
    asyncio.run(miss.get("sov", {"a": 2}))

    assert _sample("eve_killmap_cache_hits_total", {"cache": "sov"}) - h0 == 1
    assert _sample("eve_killmap_cache_misses_total", {"cache": "sov"}) - m0 == 1
    assert _sample("eve_killmap_redis_command_seconds_count", {"op": "get"}) - g0 == 2


class _FakeRedisStore:
    def __init__(self):
        self.stored: dict[str, bytes] = {}

    async def set(self, key, value, ex=None):
        self.stored[key] = value
        return True


def test_query_cache_set_frames_and_returns_shape():
    from app.cache import QueryCache

    store = _FakeRedisStore()
    qc = QueryCache(); qc.set_redis(store)
    etag, gzipped, body = asyncio.run(qc.set("sov", {"a": 1}, '{"x":1}', ttl=10))

    assert etag == '"' + _hashlib.md5(b'{"x":1}').hexdigest() + '"'
    assert gzipped is False               # 7 bytes < 1000
    assert body == b'{"x":1}'
    (frame,) = store.stored.values()
    assert frame == bytes([0]) + _hashlib.md5(b'{"x":1}').digest() + b'{"x":1}'


def test_query_cache_set_gzips_large_body():
    import gzip as _gzip
    from app.cache import QueryCache

    store = _FakeRedisStore()
    qc = QueryCache(); qc.set_redis(store)
    big = "a" * 5000
    etag, gzipped, body = asyncio.run(qc.set("system_rankings", {"limit": 10}, big, ttl=10))
    assert gzipped is True
    assert _gzip.decompress(body) == big.encode()
    assert etag == '"' + _hashlib.md5(big.encode()).hexdigest() + '"'


def test_query_cache_set_times_op():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisStore())
    s0 = _sample("eve_killmap_redis_command_seconds_count", {"op": "set"})
    asyncio.run(qc.set("sov", {"a": 1}, "value", ttl=10))
    assert _sample("eve_killmap_redis_command_seconds_count", {"op": "set"}) - s0 == 1


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


def test_query_cache_set_degrades_but_returns_body():
    from app.cache import QueryCache

    qc = QueryCache(); qc.set_redis(_FakeRedisRaising())
    e0 = _sample("eve_killmap_errors_total", {"component": "cache"})
    res = asyncio.run(qc.set("sov", {"a": 1}, "value", ttl=10))  # must not raise
    assert res is not None
    etag, gzipped, body = res
    assert body == b"value"
    assert _sample("eve_killmap_errors_total", {"component": "cache"}) - e0 == 1


import struct as _struct


def _binary_frame(body: bytes, *, fresh_to: int, gzipped: bool = False) -> bytes:
    return bytes([1 if gzipped else 0]) + _struct.pack(">Q", fresh_to) + body


def test_binary_cache_hit_returns_fresh_to_and_body():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisGet(_binary_frame(b"\x01\x02", fresh_to=1234)))
    res = asyncio.run(kbc.get({"solar_system_id": 1}))
    assert res is not None
    fresh_to, gzipped, body = res
    assert fresh_to == 1234
    assert gzipped is False
    assert body == b"\x01\x02"


def test_binary_cache_miss_uses_binary_label():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisGet(None))
    m0 = _sample("eve_killmap_cache_misses_total", {"cache": "binary"})
    assert asyncio.run(kbc.get({"a": 1})) is None
    assert _sample("eve_killmap_cache_misses_total", {"cache": "binary"}) - m0 == 1


def test_binary_cache_set_frames_and_gzips_large():
    import gzip as _gzip
    from app.cache import KillsBinaryCache

    store = _FakeRedisStore()
    kbc = KillsBinaryCache(); kbc.set_redis(store)
    payload = b"k" * 5000
    fresh_to, gzipped, body = asyncio.run(kbc.set({"solar_system_id": 1}, payload, 999))
    assert fresh_to == 999
    assert gzipped is True
    assert _gzip.decompress(body) == payload
    (frame,) = store.stored.values()
    assert frame[0] == 1
    assert _struct.unpack(">Q", frame[1:9])[0] == 999


def test_binary_cache_set_small_stays_raw():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisStore())
    fresh_to, gzipped, body = asyncio.run(kbc.set({"solar_system_id": 1}, b"small", 42))
    assert (fresh_to, gzipped, body) == (42, False, b"small")


def test_binary_cache_get_degrades_on_redis_error():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisRaising())
    assert asyncio.run(kbc.get({"a": 1})) is None


def test_binary_cache_set_degrades_but_returns_body():
    from app.cache import KillsBinaryCache

    kbc = KillsBinaryCache(); kbc.set_redis(_FakeRedisRaising())
    res = asyncio.run(kbc.set({"a": 1}, b"payload", 7))  # must not raise
    assert res == (7, False, b"payload")
