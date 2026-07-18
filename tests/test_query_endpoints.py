import asyncio

import app.main as main
from app.cache import QueryCache


class _FakeRedisRaising:
    async def get(self, key):
        from redis.exceptions import RedisError

        raise RedisError("boom")

    async def set(self, *a, **k):
        from redis.exceptions import RedisError

        raise RedisError("boom")


def test_sov_serves_from_cache_hit(monkeypatch):
    qc = QueryCache()

    async def fake_get(prefix, params):
        etag = '"deadbeef"'
        return etag, False, b'{"claimed":false}'

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    resp = asyncio.run(main.get_system_sov(30000142, if_none_match=None))
    assert resp.status_code == 200
    assert resp.body == b'{"claimed":false}'
    assert resp.headers["ETag"] == '"deadbeef"'
    assert resp.headers["Cache-Control"] == f"public, max-age={main.config.cache.sov_ttl}"


def test_sov_single_flight_builds_once(monkeypatch):
    calls: list[int] = []
    store: dict = {}

    async def fake_get(prefix, params):
        return store.get("sov")

    async def fake_set(prefix, params, value, ttl=None):
        res = ('"e"', False, value.encode())
        store["sov"] = res
        return res

    async def fake_map():
        calls.append(1)
        await asyncio.sleep(0.02)
        return {}  # unclaimed → SovResponse(claimed=False)

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    monkeypatch.setattr(main.query_cache, "set", fake_set)
    monkeypatch.setattr(main.esi_client, "get_sov_map_cached", fake_map)

    async def go():
        return await asyncio.gather(*[main.get_system_sov(30000200, if_none_match=None) for _ in range(6)])

    resps = asyncio.run(go())
    assert len(calls) == 1
    assert all(r.status_code == 200 for r in resps)


def test_sov_graceful_degradation_returns_200(monkeypatch):
    # Real QueryCache backed by a raising Redis → get None, set returns body anyway.
    qc = QueryCache(); qc.set_redis(_FakeRedisRaising())
    monkeypatch.setattr(main, "query_cache", qc)

    async def fake_map():
        return {}

    monkeypatch.setattr(main.esi_client, "get_sov_map_cached", fake_map)
    resp = asyncio.run(main.get_system_sov(30000300, if_none_match=None))
    assert resp.status_code == 200
    assert b"claimed" in resp.body


def test_rankings_serves_gzipped_when_large(monkeypatch):
    import gzip

    async def fake_get(prefix, params):
        raw = b'{"top":[],"bottom":[]}'
        return '"rank"', True, gzip.compress(raw, 6)

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    resp = asyncio.run(main.get_system_rankings(limit=10, if_none_match=None))
    assert resp.status_code == 200
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["ETag"] == '"rank"'
    assert resp.headers["Cache-Control"] == f"public, max-age={main.config.cache.rankings_ttl}"


def test_farthest_kill_304_on_matching_etag(monkeypatch):
    async def fake_get(prefix, params):
        return '"far"', False, b'{"farthest_kill":-1}'

    monkeypatch.setattr(main.query_cache, "get", fake_get)
    resp = asyncio.run(main.get_farthest_kill(30000142, if_none_match='"far"'))
    assert resp.status_code == 304
    assert resp.headers["Cache-Control"] == f"public, max-age={main.config.cache.farthest_kill_ttl}"
