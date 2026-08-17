from app.routers.universe import build_sovereignty_response


def _rec(alliance_id=None, corporation_id=None, faction_id=None):
    return {
        "alliance_id": alliance_id,
        "corporation_id": corporation_id,
        "faction_id": faction_id,
    }


def test_owner_precedence_alliance_over_corp_over_faction():
    sov_map = {30000142: _rec(alliance_id=99, corporation_id=98, faction_id=500003)}
    resp = build_sovereignty_response(sov_map, {}, {}, 1000)
    assert resp.owner_kinds == [0]
    assert resp.owner_ids == [99]


def test_corp_over_faction_when_no_alliance():
    sov_map = {1: _rec(corporation_id=98, faction_id=500003)}
    resp = build_sovereignty_response(sov_map, {}, {}, 1000)
    assert resp.owner_kinds == [1] and resp.owner_ids == [98]


def test_records_without_owner_are_omitted():
    sov_map = {1: _rec(), 2: _rec(alliance_id=99)}
    resp = build_sovereignty_response(sov_map, {}, {}, 1000)
    assert resp.system_ids == [2]


def test_owner_table_deduped_and_indexed():
    sov_map = {
        10: _rec(alliance_id=99),
        20: _rec(alliance_id=99),
        30: _rec(corporation_id=98),
    }
    resp = build_sovereignty_response(sov_map, {}, {}, 1000)
    assert resp.owner_ids == [99, 98]       # dedup, first-appearance in system order
    assert resp.owner_kinds == [0, 1]
    assert resp.system_ids == [10, 20, 30]
    assert resp.owner_idx == [0, 0, 1]


def test_sorted_by_system_id_and_byte_stable_under_shuffle():
    a = {
        30000200: _rec(alliance_id=1),
        30000100: _rec(corporation_id=2),
        30000150: _rec(faction_id=500003),
    }
    b = dict(reversed(list(a.items())))  # different dict iteration order
    ra = build_sovereignty_response(a, {}, {}, 1755345600)
    rb = build_sovereignty_response(b, {}, {}, 1755345600)
    assert ra.system_ids == [30000100, 30000150, 30000200]
    assert ra.model_dump_json() == rb.model_dump_json()  # ETag-stability property


def test_adm_present_absent_and_feed_missing():
    sov_map = {10: _rec(alliance_id=99), 20: _rec(alliance_id=99)}
    # feed present: sys10 has a level, sys20 absent -> 1.0 default
    resp = build_sovereignty_response(sov_map, {10: 6.0}, {}, 1000)
    assert resp.adm_available is True
    assert resp.adm == [6.0, 1.0]
    # whole feed absent -> 3.0 fallback, adm_available false
    resp2 = build_sovereignty_response(sov_map, None, {}, 1000)
    assert resp2.adm_available is False
    assert resp2.adm == [3.0, 3.0]


def test_owner_name_null_when_unresolved_but_entry_kept():
    sov_map = {10: _rec(alliance_id=99)}
    resp = build_sovereignty_response(sov_map, {}, {}, 1000)  # empty name_lookups
    assert resp.owner_ids == [99]
    assert resp.owner_names == [None]
    assert resp.owner_tickers == [None]


def test_names_and_tickers_from_lookups():
    sov_map = {10: _rec(alliance_id=99), 20: _rec(faction_id=500003)}
    lookups = {
        (0, 99): ("Goonswarm Federation", "CONDI"),
        (2, 500003): ("Amarr Empire", None),
    }
    resp = build_sovereignty_response(sov_map, {}, lookups, 1000)
    assert resp.owner_names == ["Goonswarm Federation", "Amarr Empire"]
    assert resp.owner_tickers == ["CONDI", None]
