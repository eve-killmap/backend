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
    # Live-feed counterpart of the REST fix: an abyssal position (~1e32) is sent
    # to WS subscribers as the (0,0,0) no-position sentinel, not a garbage int.
    async def fake_enrich(_kill):
        return {}

    monkeypatch.setattr(rc, "_enrich_kill", fake_enrich)
    out = asyncio.run(
        rc._parse_kill(_kill(1.4048816610602347e32, 6.919427609529127e31, 1.4049075104032597e32))
    )
    assert (out["x"], out["y"], out["z"]) == (0, 0, 0)


def test_parse_kill_keeps_in_range_position(monkeypatch):
    async def fake_enrich(_kill):
        return {}

    monkeypatch.setattr(rc, "_enrich_kill", fake_enrich)
    out = asyncio.run(rc._parse_kill(_kill(-4.5e12, 1.0e11, 0.0)))
    assert (out["x"], out["y"], out["z"]) == (-4_500_000_000_000, 100_000_000_000, 0)


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


def test_new_fields_reach_global_and_system_subscribers():
    payload = {
        "solar_system_id": 30000142,
        "killmail_id": 5,
        "killmail_time": 1,
        "x": 0,
        "y": 0,
        "z": 0,
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
    sq = broadcaster.subscribe_system(30000142)
    try:
        broadcaster._fanout(payload)
        g = gq.get_nowait()
        s = sq.get_nowait()
    finally:
        broadcaster.unsubscribe_global(gq)
        broadcaster.unsubscribe_system(30000142, sq)
    for field in (
        "v_alliance_id",
        "v_faction_id",
        "war_id",
        "a_character_ids",
        "a_weapon_type_ids",
    ):
        assert field in g, field
        assert field in s, field
