from app.security import (
    at_capacity,
    origin_allowed,
    security_headers,
    validate_id_list,
)


def test_validate_id_list_ok():
    assert validate_id_list([1, 2, 3], maximum=100) is None


def test_validate_id_list_empty():
    assert validate_id_list([], maximum=100) is not None


def test_validate_id_list_too_many():
    assert validate_id_list(list(range(1, 102)), maximum=100) is not None


def test_validate_id_list_non_positive():
    assert validate_id_list([1, 0, 2], maximum=100) is not None
    assert validate_id_list([-5], maximum=100) is not None


def test_origin_allowed():
    allowed = ["https://eve-killmap.com", "http://localhost:3000"]
    assert origin_allowed("https://eve-killmap.com", allowed) is True
    assert origin_allowed("https://evil.example", allowed) is False
    assert origin_allowed(None, allowed) is False
    assert origin_allowed("https://anything", ["*"]) is True


def test_at_capacity():
    assert at_capacity(999, 1000) is False
    assert at_capacity(1000, 1000) is True
    assert at_capacity(1001, 1000) is True


def test_security_headers():
    h = security_headers()
    assert h["X-Content-Type-Options"] == "nosniff"
    assert h["Referrer-Policy"] == "no-referrer"
    assert h["X-Frame-Options"] == "DENY"
