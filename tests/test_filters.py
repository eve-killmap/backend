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
        parse_filter(["bogus:victim:1"], **L)          # unknown attribute
    with pytest.raises(FilterError):
        parse_filter(["alliance:sideways:1"], **L)     # unknown side
    with pytest.raises(FilterError):
        parse_filter(["alliance:victim:notanint"], **L)
    with pytest.raises(FilterError):
        parse_filter(["alliance:victim:-3"], **L)
    with pytest.raises(FilterError):
        parse_filter(["alliance:victim:"], **L)        # missing ids
    with pytest.raises(FilterError):
        parse_filter([f"alliance:victim:{','.join(str(i) for i in range(1, 52))}"], **L)  # >max_ids
    with pytest.raises(FilterError):
        parse_filter([f"war:{i}" for i in range(1, 10)], **L)  # >max_conditions
