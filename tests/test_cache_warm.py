import asyncio

import app.cache_warm as cw


def test_warm_all_builds_the_six_entries(monkeypatch):
    calls = {"sk": 0, "rank": 0, "gk": []}

    async def fake_sk(start, end, flt):
        calls["sk"] += 1
        return ("e", False, b"[]")

    async def fake_rank(limit):
        calls["rank"] += 1
        return ("e", False, b"[]")

    async def fake_gk(m, bins):
        calls["gk"].append(m)
        return ("e", False, b"[]")

    monkeypatch.setattr(cw, "build_system_kills", fake_sk)
    monkeypatch.setattr(cw, "build_system_rankings", fake_rank)
    monkeypatch.setattr(cw, "build_global_kills", fake_gk)
    asyncio.run(cw.warm_all())
    assert calls["sk"] == 1 and calls["rank"] == 1
    assert sorted(calls["gk"]) == ["abyssal-deadspace", "anoikis", "new-eden", "tutorials"]


def test_warm_all_respects_toggle(monkeypatch):
    import dataclasses
    from app.config import config as real

    patched = dataclasses.replace(real, cache=dataclasses.replace(real.cache, warm_on_signal=False))
    monkeypatch.setattr(cw, "config", patched)
    called = {"n": 0}

    async def fake_sk(*a):
        called["n"] += 1
        return ("e", False, b"[]")

    monkeypatch.setattr(cw, "build_system_kills", fake_sk)
    asyncio.run(cw.warm_all())
    assert called["n"] == 0
