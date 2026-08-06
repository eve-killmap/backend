import asyncio

import app.entities as entities


class _FakeDb:
    def __init__(self):
        self.query = None
        self.args = None

    async def fetch(self, query, *args):
        self.query = query
        self.args = args
        return []


def test_aggressor_matches_aggressor_column_only(monkeypatch):
    fake = _FakeDb()
    monkeypatch.setattr(entities, "db", fake)
    asyncio.run(entities.search_wars(("alliance", 99005338), None, 500))
    assert "aggressor_alliance_id = $1" in fake.query
    # The SELECT list always names defender/ally columns; the aggressor-only
    # predicate must not reference them, so check the WHERE clause only.
    where_clause = fake.query.split("WHERE", 1)[1]
    assert "defender" not in where_clause and "ally_" not in where_clause
    assert fake.args[0] == 99005338


def test_defender_matches_defender_and_ally_arrays(monkeypatch):
    fake = _FakeDb()
    monkeypatch.setattr(entities, "db", fake)
    asyncio.run(entities.search_wars(None, ("corporation", 98000001), 500))
    assert "defender_corporation_id = $1" in fake.query
    assert "ally_corporation_ids @> $2::int[]" in fake.query
    assert fake.args[0] == 98000001 and fake.args[1] == [98000001]


def test_both_sides_are_anded(monkeypatch):
    fake = _FakeDb()
    monkeypatch.setattr(entities, "db", fake)
    asyncio.run(entities.search_wars(("alliance", 1), ("alliance", 2), 500))
    assert " AND " in fake.query


import app.routers.wars as warsr
from fastapi import HTTPException, Response
import pytest


def _row(**kw):
    base = dict(
        war_id=1, declared=None, started=None, finished=None, retracted=None,
        mutual=False, aggressor_corporation_id=None, aggressor_alliance_id=99,
        defender_corporation_id=None, defender_alliance_id=100,
        ally_corporation_ids=[], ally_alliance_ids=[],
    )
    base.update(kw)
    return base


def test_endpoint_requires_at_least_one_side():
    with pytest.raises(HTTPException) as e:
        asyncio.run(warsr.war_search(aggressor=None, defender=None, response=Response()))
    assert e.value.status_code == 400


def test_endpoint_returns_war_summaries(monkeypatch):
    from datetime import datetime, timezone

    async def fake_search(aggressor, defender, limit):
        return [_row(declared=datetime(2025, 3, 1, tzinfo=timezone.utc))]
    monkeypatch.setattr(warsr, "search_wars", fake_search)
    resp = Response()
    out = asyncio.run(warsr.war_search(aggressor="alliance:99", defender=None, response=resp))
    assert out[0].war_id == 1
    assert out[0].declared == int(datetime(2025, 3, 1, tzinfo=timezone.utc).timestamp())
    assert resp.headers["Cache-Control"] == f"public, max-age={warsr.config.cache.war_search_ttl}"


class _FakeRedis:
    def __init__(self, store=None):
        self.store = dict(store or {})
        self.sets = {}

    async def mget(self, *keys):
        return [self.store.get(k) for k in keys]

    def pipeline(self):
        return _FakePipe(self)


class _FakePipe:
    def __init__(self, redis):
        self._redis = redis
        self._ops = []

    def set(self, key, value, ex=None):
        self._ops.append((key, value, ex))

    async def execute(self):
        for key, value, ex in self._ops:
            self._redis.sets[key] = (value, ex)
            self._redis.store[key] = value


def test_get_war_details_live_no_redis(monkeypatch):
    from datetime import datetime, timezone
    dt = datetime(2025, 3, 1, tzinfo=timezone.utc)

    class _DB:
        def __init__(self):
            self.query = None
            self.args = None

        async def fetch(self, query, *args):
            self.query = query
            self.args = args
            return [_row(war_id=12345, declared=dt, resolved_at=dt)]

    db = _DB()
    monkeypatch.setattr(entities, "_redis", None)
    monkeypatch.setattr(entities, "db", db)
    out = asyncio.run(entities.get_war_details([12345]))
    assert out[0].war_id == 12345
    assert out[0].declared == int(dt.timestamp())
    assert "war_id = ANY($1)" in db.query
    assert db.args[0] == [12345]


def test_get_war_details_hits_cache_no_db(monkeypatch):
    from app.models import WarSummary
    cached = WarSummary(
        war_id=10, declared=1, started=None, finished=None, retracted=None,
        mutual=False, aggressor_corporation_id=None, aggressor_alliance_id=99,
        defender_corporation_id=None, defender_alliance_id=100,
        ally_corporation_ids=[], ally_alliance_ids=[],
    )
    fake_r = _FakeRedis({"war:details:10": cached.model_dump_json()})

    class _DB:
        def __init__(self):
            self.calls = 0

        async def fetch(self, *a):
            self.calls += 1
            return []

    db = _DB()
    monkeypatch.setattr(entities, "_redis", fake_r)
    monkeypatch.setattr(entities, "db", db)
    out = asyncio.run(entities.get_war_details([10]))
    assert out[0].war_id == 10 and out[0].declared == 1
    assert db.calls == 0            # served from cache, no DB read


def test_get_war_details_caches_resolved_not_stub(monkeypatch):
    from datetime import datetime, timezone
    dt = datetime(2025, 3, 1, tzinfo=timezone.utc)
    fake_r = _FakeRedis()

    class _DB:
        async def fetch(self, *a):
            return [
                _row(war_id=10, declared=dt, resolved_at=dt),      # enriched
                _row(war_id=20, declared=None, resolved_at=None),  # stub
            ]

    monkeypatch.setattr(entities, "_redis", fake_r)
    monkeypatch.setattr(entities, "db", _DB())
    out = asyncio.run(entities.get_war_details([10, 20]))
    assert {w.war_id for w in out} == {10, 20}
    assert "war:details:10" in fake_r.sets       # enriched war cached
    assert "war:details:20" not in fake_r.sets   # stub NOT cached


def test_war_details_endpoint_rejects_over_limit():
    big = list(range(1, warsr.config.limits.max_war_ids + 2))  # positive ids over the cap
    with pytest.raises(HTTPException) as e:
        asyncio.run(warsr.war_details(ids=big))
    assert e.value.status_code == 400


def test_war_details_endpoint_returns_summaries(monkeypatch):
    from app.models import WarSummary

    async def fake_details(ids):
        return [WarSummary(
            war_id=7, declared=None, started=None, finished=None, retracted=None,
            mutual=False, aggressor_corporation_id=None, aggressor_alliance_id=1,
            defender_corporation_id=None, defender_alliance_id=2,
            ally_corporation_ids=[], ally_alliance_ids=[],
        )]
    monkeypatch.setattr(warsr, "get_war_details", fake_details)
    out = asyncio.run(warsr.war_details(ids=[7]))
    assert out[0].war_id == 7
