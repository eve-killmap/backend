import pytest
from app.filters import parse_filter, Filter, Condition, FilterError

L = dict(max_conditions=8, max_ids=50)


def test_empty_is_empty():
    f = parse_filter([], **L)
    assert f.is_empty and f.conditions == ()


def test_entity_condition_with_side_and_or_ids():
    f = parse_filter(["alliance:attacker:99005338,99003581"], **L)
    (c,) = f.conditions
    assert c.facet_kind == 3 and c.role == 1
    assert c.values == (99003581, 99005338)  # sorted + deduped
    assert c.war_any is False


def test_involved_omits_role():
    (c,) = parse_filter(["character:involved:12345"], **L).conditions
    assert c.facet_kind == 1 and c.role is None


def test_weapon_has_no_side_and_forces_attacker_role():
    (c,) = parse_filter(["weapon:2929"], **L).conditions
    assert c.facet_kind == 6 and c.role == 1
    with pytest.raises(FilterError):
        parse_filter(["weapon:attacker:2929"], **L)


def test_war_specific_and_any():
    (c,) = parse_filter(["war:12345,23456"], **L).conditions
    assert c.facet_kind == 7 and c.role is None and c.values == (12345, 23456)
    (a,) = parse_filter(["war:any"], **L).conditions
    assert a.facet_kind == 7 and a.war_any is True and a.values == ()


def test_canonical_is_order_independent():
    a = parse_filter(["ship:victim:670", "alliance:attacker:99005338"], **L)
    b = parse_filter(["alliance:attacker:99005338", "ship:victim:670"], **L)
    assert a.canonical() == b.canonical()


def test_duplicate_conditions_deduped():
    f = parse_filter(["war:any", "war:any"], **L)
    assert len(f.conditions) == 1


def test_validation_errors():
    with pytest.raises(FilterError):
        parse_filter(["bogus:victim:1"], **L)  # unknown attribute
    with pytest.raises(FilterError):
        parse_filter(["alliance:sideways:1"], **L)  # unknown side
    with pytest.raises(FilterError):
        parse_filter(["alliance:victim:notanint"], **L)
    with pytest.raises(FilterError):
        parse_filter(["alliance:victim:-3"], **L)
    with pytest.raises(FilterError):
        parse_filter(["alliance:victim:"], **L)  # missing ids
    with pytest.raises(FilterError):
        parse_filter(
            [f"alliance:victim:{','.join(str(i) for i in range(1, 52))}"], **L
        )  # >max_ids
    with pytest.raises(FilterError):
        parse_filter([f"war:{i}" for i in range(1, 10)], **L)  # >max_conditions


from app.filters import build_map_sql, build_system_sql

M = dict(max_conditions=8, max_ids=50)


def test_map_single_condition_sql():
    f = parse_filter(["alliance:attacker:99005338"], **M)
    sql, params = build_map_sql(f)
    assert "FROM kill_facets" in sql
    assert "GROUP BY solar_system_id" in sql
    assert "COUNT(DISTINCT killmail_id)" in sql
    assert "FILTER (WHERE killmail_time > now()-interval '24 hours')" in sql
    assert "facet_value = ANY($2::bigint[])" in sql
    assert params == [3, [99005338], 1]  # kind, values, role


def test_map_war_any_has_no_value_param():
    sql, params = build_map_sql(parse_filter(["war:any"], **M))
    assert "facet_value" not in sql
    assert params == [7]


def test_map_multi_condition_uses_driver_and_exists():
    # character (rank 0) is more selective than ship (rank 4) -> character drives
    f = parse_filter(["ship:attacker:670", "character:victim:12345"], **M)
    sql, params = build_map_sql(f)
    assert "FROM kill_facets f" in sql
    assert "EXISTS (SELECT 1 FROM kill_facets g" in sql
    assert "g.killmail_id = f.killmail_id" in sql
    # driver params come first: character kind=1, value, role=0
    assert params[0] == 1 and params[1] == [12345] and params[2] == 0


def test_system_single_condition_sql():
    f = parse_filter(["character:involved:12345"], **M)
    sql, params = build_system_sql(f, 30000142)
    assert "SELECT DISTINCT killmail_id" in sql
    assert "solar_system_id = $" in sql
    assert "role" not in sql  # involved omits role
    assert 30000142 in params


def test_system_multi_condition_sql():
    f = parse_filter(["ship:attacker:670", "character:victim:12345"], **M)
    sql, params = build_system_sql(f, 30000142)
    assert "SELECT DISTINCT f.killmail_id" in sql
    assert "f.solar_system_id = $" in sql
    assert "EXISTS (SELECT 1 FROM kill_facets g" in sql
    assert 30000142 in params
