import asyncio

import app.routers.stats as stats
import app.routers.systems as systems
from app.config import config
from app.cache import QueryCache
from app.filters import parse_filter


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

    monkeypatch.setattr(systems.query_cache, "get", fake_get)
    resp = asyncio.run(systems.get_system_sov(30000142, if_none_match=None))
    assert resp.status_code == 200
    assert resp.body == b'{"claimed":false}'
    assert resp.headers["ETag"] == '"deadbeef"'
    assert resp.headers["Cache-Control"] == f"public, max-age={config.cache.sov_ttl}"


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

    monkeypatch.setattr(systems.query_cache, "get", fake_get)
    monkeypatch.setattr(systems.query_cache, "set", fake_set)
    monkeypatch.setattr(systems.esi_client, "get_sov_map_cached", fake_map)

    async def go():
        return await asyncio.gather(
            *[systems.get_system_sov(30000200, if_none_match=None) for _ in range(6)]
        )

    resps = asyncio.run(go())
    assert len(calls) == 1
    assert all(r.status_code == 200 for r in resps)


def test_sov_graceful_degradation_returns_200(monkeypatch):
    # Real QueryCache backed by a raising Redis → get None, set returns body anyway.
    qc = QueryCache()
    qc.set_redis(_FakeRedisRaising())
    monkeypatch.setattr(systems, "query_cache", qc)

    async def fake_map():
        return {}

    monkeypatch.setattr(systems.esi_client, "get_sov_map_cached", fake_map)
    resp = asyncio.run(systems.get_system_sov(30000300, if_none_match=None))
    assert resp.status_code == 200
    assert b"claimed" in resp.body


def test_sov_claimed_includes_adm(monkeypatch):
    from datetime import datetime, timezone

    start = int(datetime(2026, 8, 19, 9, 30, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2026, 8, 19, 12, 30, tzinfo=timezone.utc).timestamp())

    async def fake_get(prefix, params):
        return None  # force a build

    async def fake_set(prefix, params, value, ttl=None):
        return '"e"', False, value.encode()

    async def fake_map():
        return {30000142: {"alliance_id": 99}}

    async def fake_alliance(aid):
        return ("Goonswarm", "CONDI")

    async def fake_adm():
        return {30000142: {"adm": 4.5, "start": start, "end": end}}

    monkeypatch.setattr(systems.query_cache, "get", fake_get)
    monkeypatch.setattr(systems.query_cache, "set", fake_set)
    monkeypatch.setattr(systems.esi_client, "get_sov_map_cached", fake_map)
    monkeypatch.setattr(systems.esi_client, "get_alliance_info", fake_alliance)
    monkeypatch.setattr(systems.esi_client, "get_sov_structures_cached", fake_adm)

    resp = asyncio.run(systems.get_system_sov(30000142, if_none_match=None))
    assert resp.status_code == 200
    import json as _json

    body = _json.loads(resp.body)
    assert body["adm"] == 4.5
    assert body["vulnerable_start"] == start
    assert body["vulnerable_end"] == end


def test_sov_claimed_without_structures_omits_adm(monkeypatch):
    import json as _json

    async def fake_get(prefix, params):
        return None

    async def fake_set(prefix, params, value, ttl=None):
        return '"e"', False, value.encode()

    async def fake_map():
        return {30000142: {"alliance_id": 99}}

    async def fake_alliance(aid):
        return ("Goonswarm", "CONDI")

    async def fake_adm():
        return {}  # feed present, this system reports nothing

    monkeypatch.setattr(systems.query_cache, "get", fake_get)
    monkeypatch.setattr(systems.query_cache, "set", fake_set)
    monkeypatch.setattr(systems.esi_client, "get_sov_map_cached", fake_map)
    monkeypatch.setattr(systems.esi_client, "get_alliance_info", fake_alliance)
    monkeypatch.setattr(systems.esi_client, "get_sov_structures_cached", fake_adm)

    resp = asyncio.run(systems.get_system_sov(30000142, if_none_match=None))
    body = _json.loads(resp.body)
    assert body["claimed"] is True
    assert "adm" not in body  # exclude_none omits the missing values
    assert "vulnerable_start" not in body
    assert "vulnerable_end" not in body


def test_rankings_serves_gzipped_when_large(monkeypatch):
    import gzip

    async def fake_get(prefix, params):
        raw = b'{"top":[],"bottom":[]}'
        return '"rank"', True, gzip.compress(raw, 6)

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    resp = asyncio.run(stats.get_system_rankings(limit=10, if_none_match=None))
    assert resp.status_code == 200
    assert resp.headers["Content-Encoding"] == "gzip"
    assert resp.headers["ETag"] == '"rank"'
    assert (
        resp.headers["Cache-Control"] == f"public, max-age={config.cache.rankings_ttl}"
    )


def test_farthest_kill_304_on_matching_etag(monkeypatch):
    async def fake_get(prefix, params):
        return '"far"', False, b'{"farthest_kill":-1}'

    monkeypatch.setattr(systems.query_cache, "get", fake_get)
    resp = asyncio.run(systems.get_farthest_kill(30000142, if_none_match='"far"'))
    assert resp.status_code == 304
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={config.cache.farthest_kill_ttl}"
    )


def test_system_kills_serves_from_cache_hit(monkeypatch):
    async def fake_get(prefix, params):
        return (
            '"sk"',
            False,
            (
                b'{"system_ids":[1],"all":[5],"day":[1],"week":[2],'
                b'"month":[3],"six_months":[3],"year":[4]}'
            ),
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    flt = parse_filter([], max_conditions=8, max_ids=50)  # empty -> unfiltered path
    resp = asyncio.run(stats.get_system_kills_stats(flt=flt, if_none_match=None))
    assert resp.status_code == 200
    assert b'"system_ids"' in resp.body
    assert resp.headers["ETag"] == '"sk"'
    # cached exactly like system-rankings -> same TTL
    assert (
        resp.headers["Cache-Control"] == f"public, max-age={config.cache.rankings_ttl}"
    )


def test_system_kills_single_flight_builds_once(monkeypatch):
    from app.models import SystemKillsResponse

    calls = []
    store = {}

    async def fake_get(prefix, params):
        return store.get("sk")

    async def fake_set(prefix, params, value, ttl=None):
        res = ('"e"', False, value.encode())
        store["sk"] = res
        return res

    async def fake_fetch():
        calls.append(1)
        await asyncio.sleep(0.02)
        return SystemKillsResponse(
            system_ids=[1],
            all=[5],
            day=[1],
            week=[2],
            month=[3],
            six_months=[3],
            year=[4],
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    monkeypatch.setattr(stats.query_cache, "set", fake_set)
    monkeypatch.setattr(stats, "fetch_system_kills", fake_fetch)

    flt = parse_filter([], max_conditions=8, max_ids=50)  # empty -> unfiltered path

    async def go():
        return await asyncio.gather(
            *[
                stats.get_system_kills_stats(flt=flt, if_none_match=None)
                for _ in range(6)
            ]
        )

    resps = asyncio.run(go())
    assert len(calls) == 1
    assert all(r.status_code == 200 for r in resps)


def test_system_kills_filtered_cache_hit(monkeypatch):
    async def fake_get(prefix, params):
        assert prefix == "system_kills_filtered"
        return (
            '"fk"',
            False,
            b'{"system_ids":[1],"all":[2],"day":[0],"week":[0],"month":[0],"six_months":[0],"year":[0]}',
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    flt = parse_filter(["alliance:attacker:99005338"], max_conditions=8, max_ids=50)
    resp = asyncio.run(stats.get_system_kills_stats(flt=flt, if_none_match=None))
    assert resp.status_code == 200
    assert resp.headers["ETag"] == '"fk"'
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={stats.config.cache.filtered_map_ttl}"
    )


def test_system_kills_unfiltered_still_uses_rankings_ttl(monkeypatch):
    async def fake_get(prefix, params):
        assert prefix == "system_kills"
        return (
            '"sk"',
            False,
            b'{"system_ids":[],"all":[],"day":[],"week":[],"month":[],"six_months":[],"year":[]}',
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    flt = parse_filter([], max_conditions=8, max_ids=50)  # empty
    resp = asyncio.run(stats.get_system_kills_stats(flt=flt, if_none_match=None))
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={stats.config.cache.rankings_ttl}"
    )


def test_system_kills_filtered_endpoint(monkeypatch):
    from app.models import SystemKillIdsResponse

    async def fake_fetch(system_id, flt):
        return SystemKillIdsResponse(count=1, killmail_ids=[777])

    monkeypatch.setattr(systems, "fetch_filtered_system_kill_ids", fake_fetch)
    flt = parse_filter(["ship:victim:670"], max_conditions=8, max_ids=50)
    resp = asyncio.run(systems.get_system_kills_filtered(30000142, flt=flt))
    assert resp.status_code == 200
    assert b'"killmail_ids":[777]' in resp.body
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={systems.config.cache.filtered_system_ttl}"
    )


def test_get_filter_dependency_rejects_malformed_filter():
    import pytest
    from fastapi import HTTPException
    from app.routers.dependencies import get_filter

    with pytest.raises(HTTPException) as e:
        get_filter(f=["bogus:victim:1"])  # unknown attribute -> FilterError -> 400
    assert e.value.status_code == 400


def test_system_kills_filtered_rejects_empty_filter():
    import pytest
    from fastapi import HTTPException

    empty = parse_filter([], max_conditions=8, max_ids=50)
    with pytest.raises(HTTPException) as e:
        asyncio.run(systems.get_system_kills_filtered(30000142, flt=empty))
    assert e.value.status_code == 400
