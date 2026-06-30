import pytest
from fastapi import HTTPException

from app.main import _parse_killmail_ids


def test_parse_valid_ids():
    assert _parse_killmail_ids("1, 2,3") == [1, 2, 3]


def test_parse_invalid_ids_raises_400():
    with pytest.raises(HTTPException) as exc:
        _parse_killmail_ids("1,abc")
    assert exc.value.status_code == 400


def test_get_kill_details_rejects_non_positive(monkeypatch):
    import asyncio
    import app.main as main
    from fastapi import HTTPException

    try:
        asyncio.run(main.get_kill_details(killmail_ids="1,-5"))
        assert False, "expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 400
