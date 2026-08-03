import asyncio
import app.routers.universe as uni


def test_universe_names_routes_by_range_and_probes_ambiguous(monkeypatch):
    captured = {}

    async def fake_type_names(ids):
        captured["types"] = set(ids)
        return {587: "Rifter"}

    async def fake_entity_names(char_ids, corp_ids, alliance_ids, faction_ids):
        captured["chars"] = set(char_ids)
        captured["corps"] = set(corp_ids)
        captured["alliances"] = set(alliance_ids)
        captured["factions"] = set(faction_ids)
        # 91000000 resolves as a character; the ambiguous 150000000 resolves as a corp
        return (
            {91000000: "Some Pilot"},
            {150000000: ("Old Corp", "OLD")},
            {99005338: ("Goonswarm", "CONDI")},
            {500003: "Amarr Empire"},
        )

    monkeypatch.setattr(uni, "get_type_names", fake_type_names)
    monkeypatch.setattr(uni.entities, "fetch_entity_names", fake_entity_names)

    ids = [587, 500003, 91000000, 99005338, 150000000]
    out = asyncio.run(uni.resolve_universe_names(ids))

    # ambiguous id was probed against all three entity tables
    assert 150000000 in captured["chars"] and 150000000 in captured["corps"] and 150000000 in captured["alliances"]
    assert out[587].category == "type" and out[587].image_url.endswith("/types/587/icon?size=32")
    assert out[500003].category == "faction" and out[500003].image_url.endswith("/corporations/500003/logo?size=32")
    assert out[91000000].category == "character"
    assert out[99005338].category == "alliance" and out[99005338].ticker == "CONDI"
    assert out[150000000].category == "corporation" and out[150000000].ticker == "OLD"
