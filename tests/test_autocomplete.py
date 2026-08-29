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
    rows = [
        {
            "id": 99005338,
            "name": "Goonswarm",
            "ticker": "CONDI",
            "member_count": 32000,
            "date_founded": None,
        }
    ]
    fake = _FakeDb(rows)
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_entities("alliance", "goon", 20))
    assert out[0].id == 99005338 and out[0].ticker == "CONDI"
    assert out[0].member_count == 32000
    assert out[0].image_url.endswith("/alliances/99005338/logo?size=32")
    assert "ILIKE" in fake.query and "ticker" in fake.query


def test_autocomplete_corporation_query_selects_metadata_and_ranks(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(ac, "db", fake)
    asyncio.run(ac.autocomplete_entities("corporation", "goon", 20))
    assert "member_count" in fake.query and "date_founded" in fake.query
    # corp member_count is raw-nullable (NULL = not yet fetched from ESI); no COALESCE
    assert "member_count DESC NULLS LAST" in fake.query
    assert "coalesce" not in fake.query.lower()


def test_autocomplete_alliance_query_joins_member_count_and_ranks(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(ac, "db", fake)
    asyncio.run(ac.autocomplete_entities("alliance", "goon", 20))
    # alliance member count comes from the mv; absent row COALESCEs to 0 (genuinely closed)
    assert "mv_alliance_member_count" in fake.query
    assert "coalesce" in fake.query.lower()
    assert "date_founded" in fake.query
    assert "member_count DESC NULLS LAST" in fake.query


def test_autocomplete_corporation_returns_metadata(monkeypatch):
    from datetime import datetime, timezone

    dt = datetime(2010, 6, 1, tzinfo=timezone.utc)
    rows = [
        {"id": 1, "name": "X Corp", "ticker": "XC", "member_count": 12, "date_founded": dt}
    ]
    fake = _FakeDb(rows)
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_entities("corporation", "x c", 20))
    assert out[0].member_count == 12
    # date_founded is emitted as a Unix epoch int, like the rest of the API
    assert out[0].date_founded == int(dt.timestamp())


def test_autocomplete_character_returns_null_metadata(monkeypatch):
    # Characters have neither metric; the query returns the columns as NULL and
    # still orders by name (no member_count ranking).
    rows = [
        {"id": 5, "name": "Pilot", "ticker": None, "member_count": None, "date_founded": None}
    ]
    fake = _FakeDb(rows)
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_entities("character", "pil", 20))
    assert out[0].member_count is None and out[0].date_founded is None
    assert "member_count DESC" not in fake.query  # characters are not ranked by size
    assert "ORDER BY name" in fake.query


def test_autocomplete_entities_escapes_like_wildcards(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(ac, "db", fake)
    asyncio.run(ac.autocomplete_entities("character", "50%_x", 20))
    assert fake.args[0] == r"50\%\_x"  # % and _ escaped


def test_autocomplete_entities_matches_space_insensitively(monkeypatch):
    fake = _FakeDb([])
    monkeypatch.setattr(ac, "db", fake)
    asyncio.run(ac.autocomplete_entities("alliance", "CCP", 20))
    assert "replace(name, ' ', '')" in fake.query
    assert "replace(ticker, ' ', '')" in fake.query
    assert "replace($1, ' ', '')" in fake.query


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
    out = asyncio.run(
        acr.autocomplete_entities_endpoint(
            kind="alliance", q="go", limit=20, response=resp
        )
    )
    assert out == [] and called["n"] == 0  # no DB hit


def test_entities_endpoint_default_limit_is_20(monkeypatch):
    captured = {}

    async def fake(kind, q, limit):
        captured["limit"] = limit
        return []

    monkeypatch.setattr(acr, "autocomplete_entities", fake)
    asyncio.run(
        acr.autocomplete_entities_endpoint(
            kind="alliance", q="goon", response=Response()
        )
    )
    assert captured["limit"] == 20  # callers pass an explicit limit when they want more


def test_entities_endpoint_sets_cache_control(monkeypatch):
    from app.models import EntityCandidate

    async def fake(kind, q, limit):
        return [EntityCandidate(id=1, name="X", ticker=None, image_url="u")]

    monkeypatch.setattr(acr, "autocomplete_entities", fake)
    resp = Response()
    asyncio.run(
        acr.autocomplete_entities_endpoint(
            kind="alliance", q="goon", limit=20, response=resp
        )
    )
    assert (
        resp.headers["Cache-Control"]
        == f"public, max-age={acr.config.cache.autocomplete_ttl}"
    )


from prometheus_client import REGISTRY


def _ac(kind, outcome):
    return (
        REGISTRY.get_sample_value(
            "eve_killmap_autocomplete_requests_total",
            {"kind": kind, "outcome": outcome},
        )
        or 0.0
    )


def test_entities_endpoint_metric_served(monkeypatch):
    from app.models import EntityCandidate
    from fastapi import Response

    async def fake(kind, q, limit):
        return [EntityCandidate(id=1, name="X", ticker=None, image_url="u")]

    monkeypatch.setattr(acr, "autocomplete_entities", fake)
    before = _ac("alliance", "served")
    asyncio.run(
        acr.autocomplete_entities_endpoint(
            kind="alliance", q="goon", limit=20, response=Response()
        )
    )
    assert _ac("alliance", "served") - before == 1


def test_entities_endpoint_metric_short_circuit(monkeypatch):
    from fastapi import Response

    before = _ac("alliance", "short_circuit")
    asyncio.run(
        acr.autocomplete_entities_endpoint(
            kind="alliance", q="go", limit=20, response=Response()
        )
    )
    assert _ac("alliance", "short_circuit") - before == 1


def test_types_endpoint_metric(monkeypatch):
    from app.models import TypeCandidate
    from fastapi import Response

    async def fake(q, limit):
        return [TypeCandidate(id=2929, name="Neut", image_url="u")]

    monkeypatch.setattr(acr, "autocomplete_types", fake)
    s0, x0 = _ac("type", "served"), _ac("type", "short_circuit")
    asyncio.run(
        acr.autocomplete_types_endpoint(q="neut", limit=20, response=Response())
    )
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


def test_autocomplete_ships_uses_mv(monkeypatch):
    fake = _FakeDb([{"id": 670, "name": "Capsule"}])
    monkeypatch.setattr(ac, "db", fake)
    out = asyncio.run(ac.autocomplete_ships("caps", 20))
    assert out[0].id == 670
    assert out[0].image_url.endswith("/types/670/icon?size=32")
    assert "mv_ship_search" in fake.query
    assert "published" not in fake.query


def test_weapons_endpoint_metric(monkeypatch):
    from app.models import TypeCandidate
    from fastapi import Response

    async def fake(q, limit):
        return [TypeCandidate(id=2929, name="Neut", image_url="u")]

    monkeypatch.setattr(acr, "autocomplete_weapons", fake)
    s0, x0 = _ac("weapon", "served"), _ac("weapon", "short_circuit")
    asyncio.run(
        acr.autocomplete_weapons_endpoint(q="neut", limit=20, response=Response())
    )
    asyncio.run(
        acr.autocomplete_weapons_endpoint(q="ne", limit=20, response=Response())
    )
    assert _ac("weapon", "served") - s0 == 1
    assert _ac("weapon", "short_circuit") - x0 == 1
