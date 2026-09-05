import asyncio

import app.routers.stats as stats
import app.routers.systems as systems
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
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={systems.config.cache.sov_max_age}"
    )


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
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_farthest_kill_304_on_matching_etag(monkeypatch):
    async def fake_get(prefix, params):
        return '"far"', False, b'{"farthest_kill":-1}'

    monkeypatch.setattr(systems.query_cache, "get", fake_get)
    resp = asyncio.run(systems.get_farthest_kill(30000142, if_none_match='"far"'))
    assert resp.status_code == 304
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={systems.config.cache.farthest_kill_max_age}"
    )


def test_system_kills_serves_from_cache_hit(monkeypatch):
    async def fake_get(prefix, params):
        return (
            '"sk"',
            False,
            b'{"system_ids":[1],"counts":[5]}',
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    flt = parse_filter([], max_conditions=8, max_ids=50)  # empty -> unfiltered path
    resp = asyncio.run(stats.get_system_kills_stats(flt=flt, if_none_match=None))
    assert resp.status_code == 200
    assert b'"counts"' in resp.body
    assert resp.headers["ETag"] == '"sk"'
    # cached exactly like system-rankings -> same TTL
    assert resp.headers["Cache-Control"] == "public, no-cache"


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

    async def fake_fetch(start=None, end=None):
        calls.append(1)
        await asyncio.sleep(0.02)
        return SystemKillsResponse(system_ids=[1], counts=[5])

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
            b'{"system_ids":[1],"counts":[2]}',
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    flt = parse_filter(["alliance:attacker:99005338"], max_conditions=8, max_ids=50)
    resp = asyncio.run(stats.get_system_kills_stats(flt=flt, if_none_match=None))
    assert resp.status_code == 200
    assert resp.headers["ETag"] == '"fk"'
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_system_kills_unfiltered_revalidates(monkeypatch):
    # Both the unfiltered (invalidation-driven) and filtered paths tell the
    # browser to revalidate; the server-side TTL still differs by prefix, but
    # that's internal and no longer exposed via Cache-Control.
    async def fake_get(prefix, params):
        assert prefix == "system_kills"
        return (
            '"sk"',
            False,
            b'{"system_ids":[],"counts":[]}',
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    flt = parse_filter([], max_conditions=8, max_ids=50)  # empty
    resp = asyncio.run(stats.get_system_kills_stats(flt=flt, if_none_match=None))
    assert resp.headers["Cache-Control"] == "public, no-cache"


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


def test_system_kills_response_is_single_count():
    from app.models import SystemKillsResponse

    r = SystemKillsResponse(system_ids=[1, 2], counts=[5, 0])
    assert r.counts == [5, 0]
    assert not hasattr(r, "day")  # six-bucket fields gone


def test_fetch_system_kills_windowed_uses_daily_rollup(monkeypatch):
    import asyncio
    from datetime import date
    import app.queries as q

    captured = {}

    class _FakeDb:
        async def fetch(self, sql, *args):
            captured["sql"] = sql
            captured["args"] = args
            return []

    monkeypatch.setattr(q, "db", _FakeDb())
    asyncio.run(q.fetch_system_kills(date(2026, 1, 1), date(2026, 3, 1)))
    assert "mv_kills_per_system_daily" in captured["sql"]
    assert "day >=" in captured["sql"] and "day <" in captured["sql"]
    assert captured["args"] == (date(2026, 1, 1), date(2026, 3, 1))


def test_fetch_system_kills_no_window_uses_alltime(monkeypatch):
    import asyncio
    import app.queries as q

    captured = {}

    class _FakeDb:
        async def fetch(self, sql, *args):
            captured["sql"] = sql
            return []

    monkeypatch.setattr(q, "db", _FakeDb())
    asyncio.run(q.fetch_system_kills(None, None))
    assert (
        "mv_kills_per_system " in captured["sql"]
        or "mv_kills_per_system\n" in captured["sql"]
    )
    assert "mv_kills_per_system_daily" not in captured["sql"]


def test_top_systems_windowed_intervals_use_rollup(monkeypatch):
    import asyncio
    import app.queries as q

    seen = []

    class _FakeDb:
        async def fetch(self, sql, *a):
            seen.append(sql)
            return []

    monkeypatch.setattr(q, "db", _FakeDb())
    asyncio.run(q.fetch_top_systems(limit=10))
    joined = "\n".join(seen)
    assert "mv_kills_per_system_daily" in joined  # windowed intervals
    assert (
        "mv_kills_per_system " in joined or "FROM mv_kills_per_system\n" in joined
    )  # all
    assert "mv_kills_per_system_24h" not in joined  # interval MVs gone
    assert "CURRENT_DATE - INTERVAL '7 days'" in joined


def test_rankings_order_by_has_stable_tiebreaker(monkeypatch):
    # Ties in kill_count must break deterministically (by solar_system_id) so a
    # rebuild can't reshuffle equal rows and churn the response ETag.
    import asyncio
    import app.queries as q

    seen = []

    class _FakeDb:
        async def fetch(self, sql, *a):
            seen.append(sql)
            return []

    monkeypatch.setattr(q, "db", _FakeDb())
    asyncio.run(q.fetch_top_systems(limit=10))
    asyncio.run(q.fetch_bottom_systems(limit=10))

    order_bys = [s for s in seen if "ORDER BY" in s]
    assert order_bys  # sanity: the ranking queries do order
    for sql in order_bys:
        clause = sql.split("ORDER BY", 1)[1]
        assert "kill_count" in clause
        assert "solar_system_id" in clause  # secondary sort makes the order stable


def _empty_filter():
    return parse_filter([], max_conditions=8, max_ids=50)


def test_system_kills_endpoint_rejects_bad_window():
    import asyncio, pytest
    from fastapi import HTTPException
    import app.routers.stats as stats

    with pytest.raises(HTTPException) as e:
        asyncio.run(
            stats.get_system_kills_stats(
                flt=_empty_filter(), start="nope", end=None, if_none_match=None
            )
        )
    assert e.value.status_code == 400


def test_system_kills_endpoint_rejects_end_le_start():
    import asyncio, pytest
    from fastapi import HTTPException
    import app.routers.stats as stats

    with pytest.raises(HTTPException) as e:
        asyncio.run(
            stats.get_system_kills_stats(
                flt=_empty_filter(),
                start="2026-03-01",
                end="2026-01-01",
                if_none_match=None,
            )
        )
    assert e.value.status_code == 400
