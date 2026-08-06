import asyncio
import app.autocomplete as ac


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows
        self.query = None
        self.args = None

    async def fetch(self, query, *args):
        self.query = query
        self.args = args
        return self._rows


def test_autocomplete_entities_builds_candidates(monkeypatch):
    rows = [{"id": 99005338, "name": "Goonswarm", "ticker": "CONDI"}]
    fake = _FakeDb(rows)
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_entities("alliance", "goon", 20))
    assert out[0].id == 99005338 and out[0].ticker == "CONDI"
    assert out[0].image_url.endswith("/alliances/99005338/logo?size=32")
    assert "ILIKE" in fake.query and "ticker" in fake.query


def test_autocomplete_entities_escapes_like_wildcards(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(ac, "db", fake)
    asyncio.run(ac.autocomplete_entities("character", "50%_x", 20))
    assert fake.args[0] == r"50\%\_x"      # % and _ escaped


def test_autocomplete_types_uses_icon(monkeypatch):
    fake = _FakeDb([{"id": 2929, "name": "Large Energy Neutralizer"}])
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_types("neut", 20))
    assert out[0].image_url.endswith("/types/2929/icon?size=32")
    assert "published" in fake.query


import app.routers.autocomplete as acr
from fastapi import Response


def test_entities_endpoint_short_circuits_below_min_length(monkeypatch):
    called = {"n": 0}

    async def boom(*a, **k):
        called["n"] += 1
        return []
    monkeypatch.setattr(acr, "autocomplete_entities", boom)
    resp = Response()
    out = asyncio.run(acr.autocomplete_entities_endpoint(kind="alliance", q="go", limit=20, response=resp))
    assert out == [] and called["n"] == 0            # no DB hit


def test_entities_endpoint_sets_cache_control(monkeypatch):
    from app.models import EntityCandidate

    async def fake(kind, q, limit):
        return [EntityCandidate(id=1, name="X", ticker=None, image_url="u")]
    monkeypatch.setattr(acr, "autocomplete_entities", fake)
    resp = Response()
    asyncio.run(acr.autocomplete_entities_endpoint(kind="alliance", q="goon", limit=20, response=resp))
    assert resp.headers["Cache-Control"] == f"public, max-age={acr.config.cache.autocomplete_ttl}"


from prometheus_client import REGISTRY


def _ac(kind, outcome):
    return REGISTRY.get_sample_value(
        "eve_killmap_autocomplete_requests_total", {"kind": kind, "outcome": outcome}
    ) or 0.0


def test_entities_endpoint_metric_served(monkeypatch):
    from app.models import EntityCandidate
    from fastapi import Response

    async def fake(kind, q, limit):
        return [EntityCandidate(id=1, name="X", ticker=None, image_url="u")]
    monkeypatch.setattr(acr, "autocomplete_entities", fake)
    before = _ac("alliance", "served")
    asyncio.run(acr.autocomplete_entities_endpoint(kind="alliance", q="goon", limit=20, response=Response()))
    assert _ac("alliance", "served") - before == 1


def test_entities_endpoint_metric_short_circuit(monkeypatch):
    from fastapi import Response
    before = _ac("alliance", "short_circuit")
    asyncio.run(acr.autocomplete_entities_endpoint(kind="alliance", q="go", limit=20, response=Response()))
    assert _ac("alliance", "short_circuit") - before == 1


def test_types_endpoint_metric(monkeypatch):
    from app.models import TypeCandidate
    from fastapi import Response

    async def fake(q, limit):
        return [TypeCandidate(id=2929, name="Neut", image_url="u")]
    monkeypatch.setattr(acr, "autocomplete_types", fake)
    s0, x0 = _ac("type", "served"), _ac("type", "short_circuit")
    asyncio.run(acr.autocomplete_types_endpoint(q="neut", limit=20, response=Response()))
    asyncio.run(acr.autocomplete_types_endpoint(q="ne", limit=20, response=Response()))
    assert _ac("type", "served") - s0 == 1
    assert _ac("type", "short_circuit") - x0 == 1


def test_autocomplete_weapons_uses_mv(monkeypatch):
    fake = _FakeDb([{"id": 2929, "name": "Large Energy Neutralizer II"}])
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_weapons("neut", 20))
    assert out[0].id == 2929
    assert out[0].image_url.endswith("/types/2929/icon?size=32")
    assert "mv_weapon_search" in fake.query
    assert "published" not in fake.query


def test_weapons_endpoint_metric(monkeypatch):
    from app.models import TypeCandidate
    from fastapi import Response

    async def fake(q, limit):
        return [TypeCandidate(id=2929, name="Neut", image_url="u")]
    monkeypatch.setattr(acr, "autocomplete_weapons", fake)
    s0, x0 = _ac("weapon", "served"), _ac("weapon", "short_circuit")
    asyncio.run(acr.autocomplete_weapons_endpoint(q="neut", limit=20, response=Response()))
    asyncio.run(acr.autocomplete_weapons_endpoint(q="ne", limit=20, response=Response()))
    assert _ac("weapon", "served") - s0 == 1
    assert _ac("weapon", "short_circuit") - x0 == 1
