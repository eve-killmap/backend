import asyncio

import app.cache_warm as cw


def test_warm_all_builds_the_warm_set(monkeypatch):
    from app.leaderboards import ROLES, WINDOWS

    calls = {"sk": 0, "rank": 0, "gk": [], "lb": []}

    async def fake_sk(start, end, flt):
        calls["sk"] += 1
        return ("e", False, b"[]")

    async def fake_rank(limit):
        calls["rank"] += 1
        return ("e", False, b"[]")

    async def fake_gk(m, bins, flt):
        calls["gk"].append(m)
        assert flt is None
        return ("e", False, b"[]")

    async def fake_lb(window, role, limit):
        calls["lb"].append((window, role, limit))
        return ("e", False, b"{}")

    monkeypatch.setattr(cw, "build_system_kills", fake_sk)
    monkeypatch.setattr(cw, "build_system_rankings", fake_rank)
    monkeypatch.setattr(cw, "build_global_kills", fake_gk)
    monkeypatch.setattr(cw, "build_leaderboards", fake_lb)
    asyncio.run(cw.warm_all())
    assert calls["sk"] == 1 and calls["rank"] == 1
    assert sorted(calls["gk"]) == [
        "abyssal-deadspace",
        "anoikis",
        "new-eden",
        "tutorials",
    ]
    expected = sorted(
        (w, r, cw.config.limits.leaderboards_default_limit)
        for w in WINDOWS
        for r in ROLES
    )
    assert sorted(calls["lb"]) == expected and len(expected) == 12


def test_warm_all_respects_toggle(monkeypatch):
    import dataclasses
    from app.config import config as real

    patched = dataclasses.replace(
        real, cache=dataclasses.replace(real.cache, warm_on_signal=False)
    )
    monkeypatch.setattr(cw, "config", patched)
    called = {"n": 0}

    async def fake_sk(*a):
        called["n"] += 1
        return ("e", False, b"[]")

    monkeypatch.setattr(cw, "build_system_kills", fake_sk)
    asyncio.run(cw.warm_all())
    assert called["n"] == 0
