import asyncio
import json
from datetime import datetime, timezone
from typing import get_args

import pytest

import app.leaderboards as lb
import app.routers.stats as stats
from app.config import config
from app.filters import ATTRIBUTE_KINDS, SIDE_ROLES
from app.models import LeaderboardEntry, LeaderboardsResponse

TS = datetime(2026, 9, 12, 12, 30, tzinfo=timezone.utc)
TS_EPOCH = int(TS.timestamp())


def _row(kind, rank, value, kills, ts=TS):
    return {
        "facet_kind": kind,
        "rank": rank,
        "facet_value": value,
        "kill_count": kills,
        "computed_at": ts,
    }


ROWS = [
    _row(1, 1, 91000000, 42),
    _row(1, 2, 91000001, 40),
    _row(2, 1, 98000001, 40),
    _row(2, 2, 98000002, 12),
    _row(3, 1, 99005338, 39),
    _row(5, 1, 587, 30),
]


class _FakeDb:
    def __init__(self, rows):
        self.rows, self.sql, self.args = rows, None, None

    async def fetch(self, sql, *args):
        self.sql, self.args = sql, args
        return self.rows


def _patch_resolvers(monkeypatch, captured=None):
    async def fake_entities(chars, corps, alliances, factions, **kwargs):
        if captured is not None:
            captured.update(chars=chars, corps=corps, alliances=alliances, factions=factions)
        return (
            {91000000: "Pilot"},
            {98000001: ("Corp", "TCK"), 98000002: ("Tickerless", None)},
            {99005338: ("Goonswarm Federation", "CONDI")},
            {},
        )

    async def fake_types(ids):
        if captured is not None:
            captured["types"] = set(ids)
        return {587: "Rifter"}

    monkeypatch.setattr(lb.entities, "fetch_entity_names", fake_entities)
    monkeypatch.setattr(lb, "get_type_names", fake_types)


def test_window_literal_matches_window_map():
    assert set(get_args(lb.Window)) == set(lb.WINDOWS)
    assert set(get_args(lb.Role)) == set(lb.ROLES)
    assert lb.WINDOWS == {
        "all": "all", "1d": "day", "7d": "week", "30d": "month",
        "6m": "six_months", "1y": "year",
    }
    assert lb.KINDS == ("character", "corporation", "alliance", "faction", "ship", "weapon")


def test_fetch_leaderboards_query_shape_and_params(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(lb, "db", fake)
    _patch_resolvers(monkeypatch)
    asyncio.run(lb.fetch_leaderboards("7d", "attacker", 10))
    assert "FROM entity_leaderboard" in fake.sql
    assert "rank <= $4" in fake.sql
    assert "ORDER BY facet_kind, rank" in fake.sql
    assert fake.args == ([ATTRIBUTE_KINDS[k] for k in lb.KINDS], SIDE_ROLES["attacker"], "week", 10)
    assert fake.args[0] == [1, 2, 3, 4, 5, 6] and fake.args[1] == 1

    asyncio.run(lb.fetch_leaderboards("all", "victim", 3))
    assert fake.args == ([1, 2, 3, 4, 5, 6], 0, "all", 3)


def test_fetch_leaderboards_assembles_boards(monkeypatch):
    captured = {}
    monkeypatch.setattr(lb, "db", _FakeDb(ROWS))
    _patch_resolvers(monkeypatch, captured)
    out = asyncio.run(lb.fetch_leaderboards("7d", "attacker", 10))

    assert captured["chars"] == {91000000, 91000001}
    assert captured["corps"] == {98000001, 98000002}
    assert captured["alliances"] == {99005338}
    assert captured["factions"] == set()
    assert captured["types"] == {587}

    body = json.loads(out.model_dump_json(exclude_none=True))
    assert set(body) == {"computed_at", *lb.KINDS}
    assert body["computed_at"] == TS_EPOCH
    assert body["character"] == [
        {"id": 91000000, "name": "Pilot", "kills": 42},
        {"id": 91000001, "kills": 40},  # unresolved: name key absent, entry kept
    ]
    assert body["corporation"] == [
        {"id": 98000001, "name": "Corp", "ticker": "TCK", "kills": 40},
        {"id": 98000002, "name": "Tickerless", "kills": 12},
    ]
    assert body["alliance"] == [
        {"id": 99005338, "name": "Goonswarm Federation", "ticker": "CONDI", "kills": 39}
    ]
    assert body["ship"] == [{"id": 587, "name": "Rifter", "kills": 30}]
    assert body["faction"] == [] and body["weapon"] == []
    assert "ticker" not in body["character"][0] and "ticker" not in body["ship"][0]


def test_fetch_leaderboards_keeps_rank_order_not_id_order(monkeypatch):
    rows = [_row(1, 1, 500, 9), _row(1, 2, 100, 8), _row(1, 3, 300, 7)]
    monkeypatch.setattr(lb, "db", _FakeDb(rows))
    _patch_resolvers(monkeypatch)
    out = asyncio.run(lb.fetch_leaderboards("1d", "victim", 10))
    assert [e.id for e in out.character] == [500, 100, 300]


def test_fetch_leaderboards_empty_window(monkeypatch):
    monkeypatch.setattr(lb, "db", _FakeDb([]))
    _patch_resolvers(monkeypatch)
    out = asyncio.run(lb.fetch_leaderboards("1d", "victim", 10))
    assert out.computed_at is None
    assert all(getattr(out, k) == [] for k in lb.KINDS)
    body = json.loads(out.model_dump_json(exclude_none=True))
    assert "computed_at" not in body and set(body) == set(lb.KINDS)


def test_entry_model_field_defaults():
    e = LeaderboardEntry(id=1, kills=2)
    assert e.model_dump_json(exclude_none=True) == '{"id":1,"kills":2}'
    r = LeaderboardsResponse(
        character=[], corporation=[], alliance=[], faction=[], ship=[], weapon=[]
    )
    assert r.computed_at is None


# --- endpoint / builder ---------------------------------------------------


def test_endpoint_serves_cache_hit_with_revalidate(monkeypatch):
    body = b'{"computed_at":1,"character":[],"corporation":[],"alliance":[],"faction":[],"ship":[],"weapon":[]}'

    async def fake_get(prefix, params):
        assert prefix == "leaderboards"
        assert params == {"window": "7d", "role": "attacker", "limit": 10}
        return '"lb"', False, body

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    resp = asyncio.run(
        stats.get_leaderboards(window="7d", role="attacker", limit=10, if_none_match=None)
    )
    assert resp.status_code == 200
    assert resp.body == body
    assert resp.headers["ETag"] == '"lb"'
    assert resp.headers["Cache-Control"] == "public, no-cache"

    resp = asyncio.run(
        stats.get_leaderboards(window="7d", role="attacker", limit=10, if_none_match='"lb"')
    )
    assert resp.status_code == 304


def test_builder_single_flight_and_cache_params(monkeypatch):
    calls = []
    store: dict = {}
    captured: dict = {}

    async def fake_get(prefix, params):
        return store.get("lb")

    async def fake_set(prefix, params, value, ttl=None):
        captured.update(prefix=prefix, params=params, value=value, ttl=ttl)
        store["lb"] = ('"e"', False, value.encode())
        return store["lb"]

    async def fake_fetch(window, role, limit):
        calls.append((window, role, limit))
        await asyncio.sleep(0.02)
        return LeaderboardsResponse(
            character=[LeaderboardEntry(id=1, kills=2)],
            corporation=[], alliance=[], faction=[], ship=[], weapon=[],
        )

    monkeypatch.setattr(stats.query_cache, "get", fake_get)
    monkeypatch.setattr(stats.query_cache, "set", fake_set)
    monkeypatch.setattr(stats, "fetch_leaderboards", fake_fetch)

    async def go():
        return await asyncio.gather(
            *[
                stats.get_leaderboards(window="30d", role="victim", limit=5, if_none_match=None)
                for _ in range(6)
            ]
        )

    resps = asyncio.run(go())
    assert calls == [("30d", "victim", 5)]
    assert all(r.status_code == 200 for r in resps)
    assert captured["prefix"] == "leaderboards"
    assert captured["params"] == {"window": "30d", "role": "victim", "limit": 5}
    assert captured["ttl"] == config.cache.rankings_ttl
    assert '"character":[{"id":1,"kills":2}]' in captured["value"]
    assert "computed_at" not in captured["value"] and '"name"' not in captured["value"]


def test_endpoint_default_limit_is_config_knob():
    import inspect

    sig = inspect.signature(stats.get_leaderboards)
    assert sig.parameters["limit"].default == config.limits.leaderboards_default_limit
    assert sig.parameters["window"].default is inspect.Parameter.empty
    assert sig.parameters["role"].default is inspect.Parameter.empty
