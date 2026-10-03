from app.id_ranges import classify_id


def test_clean_ranges():
    assert classify_id(587) == "type"
    assert classify_id(500003) == "faction"
    assert classify_id(1000125) == "corporation"
    assert classify_id(3019582) == "character"
    assert classify_id(91072482) == "character"
    assert classify_id(98000001) == "corporation"
    assert classify_id(99005338) == "alliance"
    assert classify_id(2117000000) == "character"


def test_legacy_and_hybrid_ranges_are_ambiguous():
    assert classify_id(150000000) == "ambiguous"
    assert classify_id(2105000000) == "ambiguous"


def test_out_of_table_ids_fall_back_to_ambiguous():
    assert classify_id(30000142) == "ambiguous"
    assert classify_id(-1) == "ambiguous"
