import asyncio

import app.redis_client as rc
from app.redis_client import broadcaster


def _kill(px, py, pz):
    return {
        "killmail_id": 1,
        "killmail_time": "2019-04-03T06:40:07Z",
        "solar_system_id": 32000089,
        "position_x": px,
        "position_y": py,
        "position_z": pz,
        "victim_ship_type_id": 17715,
        "attackers": [],
    }


def test_parse_kill_zeroes_out_of_range_position(monkeypatch):
    async def fake_enrich(_kill):
        return {}

    monkeypatch.setattr(rc, "_enrich_kill", fake_enrich)
    out = asyncio.run(
        rc._parse_kill(
            _kill(1.4048816610602347e32, 6.919427609529127e31, 1.4049075104032597e32)
        )
    )
    assert (out["x"], out["y"], out["z"]) == (0, 0, 0)


def test_parse_kill_keeps_in_range_position(monkeypatch):
    async def fake_enrich(_kill):
        return {}

    monkeypatch.setattr(rc, "_enrich_kill", fake_enrich)
    out = asyncio.run(rc._parse_kill(_kill(-4.5e12, 1.0e11, 0.0)))
    assert (out["x"], out["y"], out["z"]) == (-4_500_000_000_000, 100_000_000_000, 0)


def test_enrich_kill_resolves_faction_names(monkeypatch):
    captured = {}

    async def fake_fetch(char_ids, corp_ids, alliance_ids, faction_ids, **k):
        captured["faction_ids"] = set(faction_ids)
        return {}, {}, {}, {500001: "Caldari State", 500002: "Gallente Federation"}

    async def fake_types(_type_ids):
        return {}

    monkeypatch.setattr(rc.entities, "fetch_entity_names", fake_fetch)
    monkeypatch.setattr(rc, "get_type_names", fake_types)

    kill = {
        "killmail_id": 1,
        "victim_character_id": None,
        "victim_ship_type_id": 670,
        "victim_corporation_id": None,
        "victim_alliance_id": None,
        "victim_faction_id": 500001,
        "attackers": [
            {
                "final_blow": True,
                "character_id": None,
                "ship_type_id": 587,
                "corporation_id": None,
                "alliance_id": None,
                "faction_id": 500002,
            }
        ],
    }
    out = asyncio.run(rc._enrich_kill(kill))
    assert captured["faction_ids"] == {500001, 500002}
    assert out["v_faction_name"] == "Caldari State"
    assert out["fb_faction_id"] == 500002
    assert out["fb_faction_name"] == "Gallente Federation"


def test_enrich_kill_faction_fields_none_when_absent(monkeypatch):
    async def fake_fetch(char_ids, corp_ids, alliance_ids, faction_ids, **k):
        return {}, {}, {}, {}

    async def fake_types(_type_ids):
        return {}

    monkeypatch.setattr(rc.entities, "fetch_entity_names", fake_fetch)
    monkeypatch.setattr(rc, "get_type_names", fake_types)

    kill = {
        "killmail_id": 2,
        "victim_character_id": 100,
        "victim_ship_type_id": 670,
        "victim_corporation_id": None,
        "victim_alliance_id": None,
        "victim_faction_id": None,
        "attackers": [
            {
                "final_blow": True,
                "character_id": 200,
                "ship_type_id": 587,
                "faction_id": None,
            }
        ],
    }
    out = asyncio.run(rc._enrich_kill(kill))
    assert out["v_faction_name"] is None
    assert out["fb_faction_id"] is None
    assert out["fb_faction_name"] is None


def test_facet_ids_dedup_and_null_strip():
    kill = {
        "victim_faction_id": 500003,
        "war_id": 12345,
        "attackers": [
            {
                "character_id": 1,
                "corporation_id": 98,
                "alliance_id": 99,
                "faction_id": None,
                "ship_type_id": 670,
                "weapon_type_id": 2929,
            },
            {
                "character_id": 1,
                "corporation_id": 98,
                "alliance_id": None,
                "faction_id": None,
                "ship_type_id": 17738,
                "weapon_type_id": 2929,
            },
        ],
    }
    out = rc._facet_ids(kill)
    assert out["v_faction_id"] == 500003 and out["war_id"] == 12345
    assert sorted(out["a_character_ids"]) == [1]
    assert sorted(out["a_corporation_ids"]) == [98]
    assert sorted(out["a_alliance_ids"]) == [99]
    assert out["a_faction_ids"] == []
    assert sorted(out["a_ship_type_ids"]) == [670, 17738]
    assert sorted(out["a_weapon_type_ids"]) == [2929]


def test_facet_ids_and_position_reach_global_subscribers():
    payload = {
        "solar_system_id": 30000142,
        "killmail_id": 5,
        "killmail_time": 1,
        "x": 10,
        "y": -20,
        "z": 30,
        "v_ship_type_id": 670,
        "v_character_id": 1,
        "v_corporation_id": 2,
        "v_alliance_id": 3,
        "v_faction_id": 4,
        "war_id": 12345,
        "a_character_ids": [1],
        "a_corporation_ids": [2],
        "a_alliance_ids": [3],
        "a_faction_ids": [],
        "a_ship_type_ids": [670],
        "a_weapon_type_ids": [2929],
    }
    gq = broadcaster.subscribe_global()
    try:
        broadcaster._fanout(payload)
        g = gq.get_nowait()
    finally:
        broadcaster.unsubscribe_global(gq)
    for field in (
        "v_alliance_id",
        "v_faction_id",
        "war_id",
        "a_character_ids",
        "a_weapon_type_ids",
    ):
        assert field in g, field
    assert g["solar_system_id"] == 30000142
    assert (g["x"], g["y"], g["z"]) == (10, -20, 30)


def test_fanout_omits_none_fields_but_keeps_empty_lists():
    payload = {
        "solar_system_id": 30000142,
        "killmail_id": 5,
        "killmail_time": 1,
        "x": 0,
        "y": 0,
        "z": 0,
        "v_ship_type_id": 670,
        "total_value": 12.5,
        "war_id": None,
        "fitted_value": None,
        "v_alliance_id": None,
        "fb_alliance_name": None,
        "a_character_ids": [],
        "a_ship_type_ids": [670],
    }
    gq = broadcaster.subscribe_global()
    try:
        broadcaster._fanout(payload)
        g = gq.get_nowait()
    finally:
        broadcaster.unsubscribe_global(gq)
    assert "war_id" not in g
    assert "fitted_value" not in g
    assert "v_alliance_id" not in g
    assert "fb_alliance_name" not in g
    assert g["total_value"] == 12.5
    assert g["a_character_ids"] == []
    assert g["a_ship_type_ids"] == [670]
    assert g["solar_system_id"] == 30000142
    assert (g["x"], g["y"], g["z"]) == (0, 0, 0)
