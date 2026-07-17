import asyncio
from datetime import datetime, timezone

from prometheus_client import REGISTRY

import app.entities as entities


def _sample(name, labels=None):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeDB:
    def __init__(self, rows=None, war_row="absent"):
        self._rows = rows or {}
        self._war_row = war_row  # "absent" sentinel -> None

    async def fetch(self, sql, *args):
        for table in ("characters", "corporations", "alliances", "factions"):
            if f"FROM {table}" in sql:
                return self._rows.get(table, [])
        return []

    async def fetchrow(self, sql, *args):
        return None if self._war_row == "absent" else self._war_row


def test_fetch_entity_names_found_and_missing(monkeypatch):
    fake = _FakeDB({
        "characters": [{"id": 1, "name": "Pilot"}, {"id": 3, "name": None}],  # 3 = tombstone
        "corporations": [{"id": 10, "name": "Corp", "ticker": "TIC"}],
        "alliances": [],
        "factions": [{"id": 500, "name": "Amarr"}],
    })
    monkeypatch.setattr(entities, "db", fake)
    f0 = _sample("eve_killmap_entity_lookups_total", {"kind": "character", "result": "found"})
    m0 = _sample("eve_killmap_entity_lookups_total", {"kind": "character", "result": "missing"})

    names, corp, alli, fac = asyncio.run(
        entities.fetch_entity_names({1, 2, 3}, {10}, {20}, {500})
    )
    assert names == {1: "Pilot"}                 # 2 absent, 3 NULL -> omitted
    assert corp == {10: ("Corp", "TIC")}
    assert alli == {}                            # 20 absent
    assert fac == {500: "Amarr"}
    assert _sample("eve_killmap_entity_lookups_total", {"kind": "character", "result": "found"}) - f0 == 1
    assert _sample("eve_killmap_entity_lookups_total", {"kind": "character", "result": "missing"}) - m0 == 2  # ids 2 and 3


def test_fetch_entity_names_empty_sets_skip(monkeypatch):
    fake = _FakeDB()
    monkeypatch.setattr(entities, "db", fake)
    names, corp, alli, fac = asyncio.run(entities.fetch_entity_names(set(), set(), set(), set()))
    assert (names, corp, alli, fac) == ({}, {}, {}, {})


def test_fetch_war_resolved(monkeypatch):
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    monkeypatch.setattr(entities, "db", _FakeDB(war_row={"war_id": 1, "resolved_at": ts}))
    r0 = _sample("eve_killmap_war_lookups_total", {"result": "resolved"})
    row = asyncio.run(entities.fetch_war(1))
    assert row is not None
    assert _sample("eve_killmap_war_lookups_total", {"result": "resolved"}) - r0 == 1


def test_fetch_war_stub(monkeypatch):
    monkeypatch.setattr(entities, "db", _FakeDB(war_row={"war_id": 1, "resolved_at": None}))
    s0 = _sample("eve_killmap_war_lookups_total", {"result": "stub"})
    assert asyncio.run(entities.fetch_war(1)) is None
    assert _sample("eve_killmap_war_lookups_total", {"result": "stub"}) - s0 == 1


def test_fetch_war_absent(monkeypatch):
    monkeypatch.setattr(entities, "db", _FakeDB(war_row="absent"))
    a0 = _sample("eve_killmap_war_lookups_total", {"result": "absent"})
    assert asyncio.run(entities.fetch_war(1)) is None
    assert _sample("eve_killmap_war_lookups_total", {"result": "absent"}) - a0 == 1


def test_build_war_processed():
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    row = {
        "aggressor_corporation_id": 10, "aggressor_alliance_id": 20, "aggressor_ships_killed": 5,
        "defender_corporation_id": 11, "defender_alliance_id": None, "defender_ships_killed": None,
        "declared": ts, "started": ts, "finished": None, "retracted": None, "mutual": True,
    }
    corp = {10: ("CorpA", "AAA"), 11: ("CorpB", "BBB")}
    alli = {20: ("AlliA", "AL1")}
    wp = entities.build_war_processed(row, corp, alli)
    assert wp.declared == int(ts.timestamp())
    assert wp.finished is None and wp.retracted is None
    assert wp.aggressor.corporation == "CorpA" and wp.aggressor.corporation_ticker == "AAA"
    assert wp.aggressor.alliance == "AlliA" and wp.aggressor.ships_killed == 5
    assert wp.defender.corporation == "CorpB" and wp.defender.alliance is None
    assert wp.defender.ships_killed == 0   # NULL -> 0
    assert wp.mutual is True
