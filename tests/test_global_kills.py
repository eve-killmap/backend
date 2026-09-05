import asyncio
import json
from decimal import Decimal

import pytest
from fastapi import HTTPException

import app.global_kills as gk
import app.routers.stats as stats
from app.config import config
from app.filters import parse_filter, _bin_expr as _filters_bin_expr
from app.filters import Filter

_L = dict(max_conditions=8, max_ids=50)


class _FakeDb:
    def __init__(self, rows):
        self.rows, self.sql, self.args = rows, None, None

    async def fetch(self, sql, *args):
        self.sql, self.args = sql, args
        return self.rows


def test_map_ranges_filter_by_id_range(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(gk, "db", fake)
    asyncio.run(gk.fetch_global_kills("abyssal-deadspace", 4))
    assert 32000000 in fake.args and 33000000 in fake.args
    assert (
        "solar_system_id" in fake.sql and "/ 1000000" not in fake.sql
    )  # range, not digit


def test_zero_filled_dense_array(monkeypatch):
    # rows give bin->count; handler must return a dense length-N array oldest->newest
    fake = _FakeDb([{"bin": 0, "kill_count": 5}, {"bin": 2, "kill_count": 7}])
    monkeypatch.setattr(gk, "db", fake)
    out = asyncio.run(gk.fetch_global_kills("new-eden", 4))
    assert out == [5, 0, 7, 0]


def test_decimal_kill_count_coerced_to_int(monkeypatch):
    # SUM(bigint) in Postgres returns numeric, which asyncpg decodes as
    # decimal.Decimal. The handler must coerce to a plain int at the boundary
    # so the bare-array response stays stdlib-json-serializable.
    fake = _FakeDb([{"bin": 0, "kill_count": Decimal("5")}])
    monkeypatch.setattr(gk, "db", fake)
    out = asyncio.run(gk.fetch_global_kills("new-eden", 4))
    assert out[0] == 5 and type(out[0]) is int
    assert json.loads(json.dumps(out)) == [5, 0, 0, 0]  # raises TypeError pre-fix


def test_unknown_map_raises():
    with pytest.raises(KeyError):
        asyncio.run(gk.fetch_global_kills("nope", 4))


def test_global_kills_endpoint_unknown_map_400(monkeypatch):
    with pytest.raises(HTTPException) as e:
        asyncio.run(
            stats.get_global_kills(
                map="nope", flt=Filter(()), bins=10, if_none_match=None
            )
        )
    assert e.value.status_code == 400


def test_global_kills_endpoint_cache_hit(monkeypatch):
    async def fake_get(prefix, params):
        assert prefix == "global_kills"
        assert params == {"bins": 10, "map": "new-eden"}
        return '"gk"', False, b"[1,2,3]"

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    resp = asyncio.run(
        stats.get_global_kills(
            map="new-eden", flt=Filter(()), bins=10, if_none_match=None
        )
    )
    assert resp.status_code == 200
    assert resp.body == b"[1,2,3]"
    assert resp.headers["ETag"] == '"gk"'
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_global_kills_endpoint_single_flight_builds_once_default_bins(monkeypatch):
    calls: list[int] = []
    store: dict = {}

    async def fake_get(prefix, params):
        return store.get("gk")

    async def fake_set(prefix, params, value, ttl=None):
        res = ('"e"', False, value.encode())
        store["gk"] = res
        return res

    async def fake_fetch(map_type, bins):
        calls.append(bins)
        await asyncio.sleep(0.02)
        return [0] * bins

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    monkeypatch.setattr(stats.query_cache, "set", fake_set)
    monkeypatch.setattr(stats, "fetch_global_kills", fake_fetch)

    async def go():
        return await asyncio.gather(
            *[
                stats.get_global_kills(
                    map="new-eden", flt=Filter(()), bins=None, if_none_match=None
                )
                for _ in range(6)
            ]
        )

    resps = asyncio.run(go())
    assert len(calls) == 1
    assert calls[0] == config.limits.global_kills_default_bins
    assert all(r.status_code == 200 for r in resps)


def test_filtered_global_kills_builds_sql(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(gk, "db", fake)
    f = parse_filter(["alliance:attacker:99005338"], **_L)
    asyncio.run(gk.fetch_filtered_global_kills(f, "new-eden", 300))
    assert "kill_facets" in fake.sql
    assert "GROUP BY bin" in fake.sql
    assert "killmail_time::date" in fake.sql
    assert 30000000 in fake.args and 31000000 in fake.args  # new-eden id-range


def test_filtered_global_kills_zero_filled_dense(monkeypatch):
    fake = _FakeDb([{"bin": 0, "kill_count": 5}, {"bin": 2, "kill_count": 7}])
    monkeypatch.setattr(gk, "db", fake)
    f = parse_filter(["war:any"], **_L)
    out = asyncio.run(gk.fetch_filtered_global_kills(f, "new-eden", 4))
    assert out == [5, 0, 7, 0]


def test_filtered_global_kills_coerces_count_to_int(monkeypatch):
    # Defensive parity with the unfiltered path: coerce the DB count to a plain
    # int so the bare-array response stays stdlib-json-serializable.
    fake = _FakeDb([{"bin": 1, "kill_count": Decimal("3")}])
    monkeypatch.setattr(gk, "db", fake)
    f = parse_filter(["war:any"], **_L)
    out = asyncio.run(gk.fetch_filtered_global_kills(f, "new-eden", 3))
    assert out[1] == 3 and type(out[1]) is int
    assert json.loads(json.dumps(out)) == [0, 3, 0]


def test_filtered_global_kills_unknown_map_raises(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(gk, "db", fake)
    f = parse_filter(["war:any"], **_L)
    with pytest.raises(KeyError):
        asyncio.run(gk.fetch_filtered_global_kills(f, "nope", 4))


def test_both_paths_use_shared_bin_expr(monkeypatch):
    # The unfiltered rollup path and the filtered kill_facets path bucket days with
    # the identical _bin_expr math, so their bins align on the same axis.
    unfiltered = _FakeDb([])
    monkeypatch.setattr(gk, "db", unfiltered)
    asyncio.run(gk.fetch_global_kills("new-eden", 300))
    assert _filters_bin_expr("day", "$3", "$4") in unfiltered.sql

    filtered = _FakeDb([])
    monkeypatch.setattr(gk, "db", filtered)
    f = parse_filter(["war:any"], **_L)
    asyncio.run(gk.fetch_filtered_global_kills(f, "new-eden", 300))
    # war:any -> kind $1, lo $2, hi $3, bins $4, earliest $5
    assert _filters_bin_expr("killmail_time::date", "$4", "$5") in filtered.sql


def test_global_kills_endpoint_filtered_uses_filtered_prefix(monkeypatch):
    seen = {}

    async def fake_get(prefix, params):
        seen["prefix"], seen["params"] = prefix, params
        return '"e"', False, b"[9]"

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    f = parse_filter(["alliance:attacker:99005338"], **_L)
    resp = asyncio.run(
        stats.get_global_kills(map="new-eden", flt=f, bins=10, if_none_match=None)
    )
    assert seen["prefix"] == "global_kills_filtered"
    assert seen["params"] == {"filter": f.canonical(), "map": "new-eden", "bins": 10}
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_global_kills_endpoint_filtered_builds_and_caches(monkeypatch):
    store: dict = {}
    captured: dict = {}

    async def fake_get(prefix, params):
        return store.get("k")

    async def fake_set(prefix, params, value, ttl=None):
        captured["ttl"] = ttl
        store["k"] = ('"e"', False, value.encode())
        return store["k"]

    async def fake_filtered(f, map_type, bins):
        return [1, 2, 3]

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    monkeypatch.setattr(stats.query_cache, "set", fake_set)
    monkeypatch.setattr(stats, "fetch_filtered_global_kills", fake_filtered)
    f = parse_filter(["war:any"], **_L)
    resp = asyncio.run(
        stats.get_global_kills(map="anoikis", flt=f, bins=3, if_none_match=None)
    )
    assert resp.status_code == 200
    assert resp.body == json.dumps([1, 2, 3]).encode()
    # the cache entry itself is written with the filtered TTL (not just the header)
    assert captured["ttl"] == config.cache.filtered_map_ttl


def test_filtered_global_kills_records_metrics(monkeypatch):
    # Parity with fetch_filtered_map: the live kill_facets aggregate records its
    # latency (query="global_kills") and the filter-condition count.
    from prometheus_client import REGISTRY

    def _hist_count(name, labels=None):
        return REGISTRY.get_sample_value(name, labels) or 0.0

    fake = _FakeDb([])
    monkeypatch.setattr(gk, "db", fake)
    q0 = _hist_count(
        "eve_killmap_facet_query_seconds_count", {"query": "global_kills"}
    )
    c0 = _hist_count("eve_killmap_filter_conditions_count")
    f = parse_filter(["alliance:attacker:99005338"], **_L)
    asyncio.run(gk.fetch_filtered_global_kills(f, "new-eden", 10))
    assert (
        _hist_count("eve_killmap_facet_query_seconds_count", {"query": "global_kills"})
        - q0
        == 1
    )
    assert _hist_count("eve_killmap_filter_conditions_count") - c0 == 1
