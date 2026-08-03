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
