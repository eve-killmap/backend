from app.queries import normalize_farthest_kill


def test_normalize_none_is_minus_one():
    assert normalize_farthest_kill(None) == -1


def test_normalize_rounds_to_int():
    assert normalize_farthest_kill(1234.0) == 1234
    assert normalize_farthest_kill(0.0) == 0
