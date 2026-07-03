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


def test_request_middleware_adds_wildcard_acao(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from starlette.responses import Response
    import app.main as main

    monkeypatch.setattr(main, "config", SimpleNamespace(cors=SimpleNamespace(allow_origins=["*"])))

    async def call_next(_req):
        return Response("ok")

    resp = asyncio.run(main._request_middleware(object(), call_next))
    # Set on every response (even this Origin-less one) so caches stay CORS-valid.
    assert resp.headers["access-control-allow-origin"] == "*"


def test_request_middleware_no_acao_for_specific_origins(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from starlette.responses import Response
    import app.main as main

    monkeypatch.setattr(
        main, "config", SimpleNamespace(cors=SimpleNamespace(allow_origins=["https://eve-killmap.com"]))
    )

    async def call_next(_req):
        return Response("ok")

    resp = asyncio.run(main._request_middleware(object(), call_next))
    # Not wildcard → leave CORS entirely to Starlette's per-Origin handling.
    assert "access-control-allow-origin" not in resp.headers
