import app.redis_client as rc
from app.redis_client import broadcaster


def test_facet_ids_dedup_and_null_strip():
    kill = {
        "victim_faction_id": 500003,
        "war_id": 12345,
        "attackers": [
            {"character_id": 1, "corporation_id": 98, "alliance_id": 99,
             "faction_id": None, "ship_type_id": 670, "weapon_type_id": 2929},
            {"character_id": 1, "corporation_id": 98, "alliance_id": None,
             "faction_id": None, "ship_type_id": 17738, "weapon_type_id": 2929},
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
        "solar_system_id": 30000142, "killmail_id": 5, "killmail_time": 1,
        "x": 0, "y": 0, "z": 0, "v_ship_type_id": 670,
        "v_character_id": 1, "v_corporation_id": 2, "v_alliance_id": 3, "v_faction_id": 4,
        "war_id": 12345,
        "a_character_ids": [1], "a_corporation_ids": [2], "a_alliance_ids": [3],
        "a_faction_ids": [], "a_ship_type_ids": [670], "a_weapon_type_ids": [2929],
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
    for field in ("v_alliance_id", "v_faction_id", "war_id",
                  "a_character_ids", "a_weapon_type_ids"):
        assert field in g, field
        assert field in s, field
