import asyncio
import json
from datetime import datetime, timezone
from typing import Annotated, get_args

import app.cache as app_cache
import app.routers.systems as systems
import app.system_activity as sa
from app.config import config
from app.models import SystemActivityResponse

NOW = 1_758_900_000
START = NOW - sa.WINDOW_SECONDS
SID = 30000142


def _dt(epoch: int) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class _FakeDb:
    def __init__(self, rows):
        self.rows, self.sql, self.args = rows, None, None

    async def fetch(self, sql, *args):
        self.sql, self.args = sql, args
        return self.rows


def test_bin_index_window_start_is_first_bin():
    assert sa.bin_index(START, START, 48) == 0


def test_bin_index_just_before_computed_at_is_last_bin():
    assert sa.bin_index(NOW - 1, START, 48) == 47


def test_bin_index_boundary_between_bins():
    assert sa.bin_index(START + 899, START, 48) == 0
    assert sa.bin_index(START + 900, START, 48) == 1


def test_bin_index_clamps_out_of_window_timestamps():
    assert sa.bin_index(START - 5, START, 48) == 0
    assert sa.bin_index(NOW + 5, START, 48) == 47


def test_fetch_queries_the_system_over_the_trailing_window(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(sa, "db", fake)
    asyncio.run(sa.fetch_system_activity(SID, 48, now=NOW))
    assert "FROM kills" in fake.sql
    assert "solar_system_id" in fake.sql and "killmail_time" in fake.sql
    assert fake.args == (SID, _dt(START), _dt(NOW))


def test_fetch_bins_rows_densely_oldest_first(monkeypatch):
    fake = _FakeDb(
        [
            {"killmail_time": _dt(START)},
            {"killmail_time": _dt(START + 10)},
            {"killmail_time": _dt(START + 2 * 10800)},
            {"killmail_time": _dt(NOW - 1)},
        ]
    )
    monkeypatch.setattr(sa, "db", fake)
    out = asyncio.run(sa.fetch_system_activity(SID, 4, now=NOW))
    assert out.counts == [2, 0, 1, 1]
    assert out.computed_at == NOW


def test_fetch_zero_fills_a_quiet_system(monkeypatch):
    monkeypatch.setattr(sa, "db", _FakeDb([]))
    out = asyncio.run(sa.fetch_system_activity(SID, 6, now=NOW))
    assert out.counts == [0] * 6


def test_fetch_defaults_computed_at_to_wall_clock(monkeypatch):
    monkeypatch.setattr(sa, "db", _FakeDb([]))
    monkeypatch.setattr(sa.time, "time", lambda: NOW + 0.7)
    out = asyncio.run(sa.fetch_system_activity(SID, 2))
    assert out.computed_at == NOW


def test_response_serializes_like_global_kills():
    body = SystemActivityResponse(computed_at=NOW, counts=[1, 0, 2]).model_dump_json()
    assert json.loads(body) == {"computed_at": NOW, "counts": [1, 0, 2]}


def test_endpoint_cache_hit_serves_stored_body_with_revalidation(monkeypatch):
    async def fake_get(prefix, params):
        assert prefix == "system_activity"
        assert params == {"solar_system_id": SID, "bins": 10}
        return '"sa"', False, b'{"computed_at":1,"counts":[1,2,3]}'

    monkeypatch.setattr(app_cache.query_cache, "get", fake_get)
    resp = asyncio.run(systems.get_system_activity(SID, bins=10, if_none_match=None))
    assert resp.status_code == 200
    assert resp.body == b'{"computed_at":1,"counts":[1,2,3]}'
    assert resp.headers["ETag"] == '"sa"'
    assert resp.headers["Cache-Control"] == "public, no-cache"


def test_endpoint_returns_304_when_etag_matches(monkeypatch):
    async def fake_get(prefix, params):
        return '"sa"', False, b"{}"

    monkeypatch.setattr(app_cache.query_cache, "get", fake_get)
    resp = asyncio.run(systems.get_system_activity(SID, bins=10, if_none_match='"sa"'))
    assert resp.status_code == 304
    assert resp.headers["ETag"] == '"sa"'


def test_endpoint_single_flight_builds_once_with_default_bins_and_ttl(monkeypatch):
    calls: list[int] = []
    store: dict = {}
    captured: dict = {}

    async def fake_get(prefix, params):
        return store.get("k")

    async def fake_set(prefix, params, value, ttl=None):
        captured["ttl"] = ttl
        store["k"] = ('"e"', False, value.encode())
        return store["k"]

    async def fake_fetch(solar_system_id, bins):
        calls.append(bins)
        await asyncio.sleep(0.02)
        return SystemActivityResponse(computed_at=NOW, counts=[0] * bins)

    monkeypatch.setattr(app_cache.query_cache, "get", fake_get)
    monkeypatch.setattr(app_cache.query_cache, "set", fake_set)
    monkeypatch.setattr(systems, "fetch_system_activity", fake_fetch)

    async def go():
        return await asyncio.gather(
            *[
                systems.get_system_activity(SID, bins=None, if_none_match=None)
                for _ in range(6)
            ]
        )

    resps = asyncio.run(go())
    assert calls == [config.limits.system_activity_default_bins]
    assert captured["ttl"] == config.cache.system_activity_ttl
    assert all(r.status_code == 200 for r in resps)
    assert json.loads(resps[0].body)["counts"] == [0] * calls[0]


def test_endpoint_bins_are_bounded_one_to_720():
    import inspect

    ann = inspect.signature(systems.get_system_activity).parameters["bins"].annotation
    bounds = {type(m).__name__: m for m in get_args(ann)[1].metadata}
    assert bounds["Ge"].ge == 1 and bounds["Le"].le == 720
