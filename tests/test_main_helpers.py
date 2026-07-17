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


import app.prometheus_metrics as pm  # noqa: E402
from prometheus_client import REGISTRY  # noqa: E402


def _sample(name, labels=None):
    return REGISTRY.get_sample_value(name, labels) or 0.0


class _FakeWS:
    def __init__(self, origin):
        self.headers = {"origin": origin} if origin is not None else {}
        self.accepted = False
        self.closed = None

    async def accept(self):
        self.accepted = True

    async def close(self, code=None, reason=None):
        self.closed = (code, reason)


def test_ws_guard_rejected_origin_metric():
    import asyncio
    from app.main import _ws_guard

    ws = _FakeWS("https://evil.example")
    r0 = _sample(
        "eve_killmap_ws_connections_total",
        {"transport": "ws", "outcome": "rejected_origin"},
    )
    ok = asyncio.run(_ws_guard(ws))
    assert ok is False
    assert (
        _sample(
            "eve_killmap_ws_connections_total",
            {"transport": "ws", "outcome": "rejected_origin"},
        )
        - r0
        == 1
    )


def test_main_no_war_esi_reference():
    import pathlib

    src = pathlib.Path("app/main.py").read_text(encoding="utf-8")
    assert "get_war_info" not in src
    assert "raw_war" not in src
