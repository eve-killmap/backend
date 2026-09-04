import asyncio
import dataclasses

import asyncpg
import pytest

import app.database as database_mod
from app.database import Database
from app.config import config as real_config


def _patch_dsn(monkeypatch):
    # Avoid depending on a real DATABASE_URL in the test environment.
    monkeypatch.setattr(database_mod, "require_database_url", lambda _c: "postgres://test")


def test_connect_retries_transient_then_succeeds(monkeypatch):
    # A Postgres restart mid-startup surfaces as CannotConnectNowError; connect()
    # must ride it out with backoff and succeed once the DB is back, not crash.
    _patch_dsn(monkeypatch)
    calls = {"n": 0}
    exc = asyncpg.exceptions.CannotConnectNowError(
        "the database system is shutting down"
    )

    async def fake_create_pool(*_a, **_k):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise exc
        return "POOL"

    sleeps: list[float] = []

    async def fake_sleep(d):
        sleeps.append(d)

    monkeypatch.setattr(database_mod.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(database_mod.asyncio, "sleep", fake_sleep)

    db = Database()
    asyncio.run(db.connect())

    assert db._pool == "POOL"
    assert calls["n"] == 3  # 2 transient failures, then success
    assert sleeps == [1.0, 2.0]  # exponential backoff between the three attempts


def test_connect_retries_on_oserror(monkeypatch):
    # Connection refused / DNS / reset (the DB fully down) is an OSError and is
    # also transient.
    _patch_dsn(monkeypatch)
    calls = {"n": 0}

    async def fake_create_pool(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionRefusedError("connection refused")
        return "POOL"

    async def fake_sleep(_d):
        pass

    monkeypatch.setattr(database_mod.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(database_mod.asyncio, "sleep", fake_sleep)

    db = Database()
    asyncio.run(db.connect())
    assert db._pool == "POOL"
    assert calls["n"] == 2


def test_connect_gives_up_after_budget_and_raises(monkeypatch):
    # With the retry budget exhausted, connect() re-raises the transient error
    # loudly rather than looping forever.
    _patch_dsn(monkeypatch)
    patched = dataclasses.replace(
        real_config,
        database=dataclasses.replace(
            real_config.database, connect_max_retry_seconds=0
        ),
    )
    monkeypatch.setattr(database_mod, "config", patched)

    calls = {"n": 0}
    exc = asyncpg.exceptions.CannotConnectNowError("the database system is shutting down")

    async def always_fail(*_a, **_k):
        calls["n"] += 1
        raise exc

    slept = {"n": 0}

    async def fake_sleep(_d):
        slept["n"] += 1

    monkeypatch.setattr(database_mod.asyncpg, "create_pool", always_fail)
    monkeypatch.setattr(database_mod.asyncio, "sleep", fake_sleep)

    db = Database()
    with pytest.raises(asyncpg.exceptions.CannotConnectNowError):
        asyncio.run(db.connect())
    assert calls["n"] == 1  # budget 0 -> one attempt, no retry
    assert slept["n"] == 0


def test_connect_does_not_retry_non_transient(monkeypatch):
    # A non-transient error (e.g. bad password) must fail immediately, not be
    # retried for the whole budget.
    _patch_dsn(monkeypatch)
    calls = {"n": 0}

    async def fake_create_pool(*_a, **_k):
        calls["n"] += 1
        raise asyncpg.exceptions.InvalidPasswordError("password authentication failed")

    monkeypatch.setattr(database_mod.asyncpg, "create_pool", fake_create_pool)

    db = Database()
    with pytest.raises(asyncpg.exceptions.InvalidPasswordError):
        asyncio.run(db.connect())
    assert calls["n"] == 1
