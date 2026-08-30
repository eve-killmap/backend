import asyncio
import json
from decimal import Decimal

import pytest
from fastapi import HTTPException

import app.global_kills as gk
import app.routers.stats as stats
from app.config import config


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
        asyncio.run(stats.get_global_kills(map="nope", bins=10, if_none_match=None))
    assert e.value.status_code == 400


def test_global_kills_endpoint_cache_hit(monkeypatch):
    async def fake_get(prefix, params):
        assert prefix == "global_kills"
        assert params == {"bins": 10, "map": "new-eden"}
        return '"gk"', False, b"[1,2,3]"

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    resp = asyncio.run(
        stats.get_global_kills(map="new-eden", bins=10, if_none_match=None)
    )
    assert resp.status_code == 200
    assert resp.body == b"[1,2,3]"
    assert resp.headers["ETag"] == '"gk"'
    assert (
        resp.headers["Cache-Control"] == f"public, max-age={config.cache.rankings_ttl}"
    )


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
                stats.get_global_kills(map="new-eden", bins=None, if_none_match=None)
                for _ in range(6)
            ]
        )

    resps = asyncio.run(go())
    assert len(calls) == 1
    assert calls[0] == config.limits.global_kills_default_bins
    assert all(r.status_code == 200 for r in resps)
