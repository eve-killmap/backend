from app.health import (
    build_heartbeat,
    aggregate_workers,
    extract_bearer,
    health_token_ok,
)
import asyncio
import app.health as health


def test_build_heartbeat_merges_snapshot():
    snap = {"requests": 5, "broadcaster_role": "leader"}
    hb = build_heartbeat(pid=1234, snapshot=snap, now=100.0)
    assert hb["pid"] == 1234
    assert hb["ts"] == 100.0
    assert hb["requests"] == 5
    assert hb["broadcaster_role"] == "leader"


class _FakeRedis:
    def __init__(self, ok=True):
        self._ok = ok

    async def ping(self):
        if not self._ok:
            raise RuntimeError("down")
        return True


def test_redis_ok_true(monkeypatch):
    health.set_redis(_FakeRedis(ok=True))
    assert asyncio.run(health.redis_ok()) is True


def test_redis_ok_false_when_unset(monkeypatch):
    health.set_redis(None)  # type: ignore[arg-type]
    assert asyncio.run(health.redis_ok()) is False


class _FakeInfoRedis:
    def __init__(self, info):
        self._info = info

    async def info(self):
        return self._info


def test_redis_info_reports_cache_and_stream():
    health.set_redis(
        _FakeInfoRedis({"used_memory": 100, "connected_clients": 2, "uptime_in_seconds": 30})
    )
    health.set_bus_redis(
        _FakeInfoRedis({"used_memory": 200, "connected_clients": 5, "uptime_in_seconds": 60})
    )
    out = asyncio.run(health.redis_info())
    assert out["cache"]["available"] is True
    assert out["cache"]["used_memory"] == 100
    assert out["stream"]["available"] is True
    assert out["stream"]["connected_clients"] == 5


def test_redis_info_stream_unavailable_when_unset():
    health.set_redis(
        _FakeInfoRedis({"used_memory": 1, "connected_clients": 1, "uptime_in_seconds": 1})
    )
    health.set_bus_redis(None)
    out = asyncio.run(health.redis_info())
    assert out["stream"] == {"available": False}
    assert out["cache"]["available"] is True


def test_health_token_ok():
    assert health_token_ok("secret", "secret") is True
    assert health_token_ok("nope", "secret") is False
    assert health_token_ok(None, "secret") is False
    assert health_token_ok("secret", None) is False


def test_extract_bearer():
    assert extract_bearer("Bearer abc123") == "abc123"
    assert extract_bearer("bearer abc123") == "abc123"
    assert extract_bearer("Basic abc") is None
    assert extract_bearer(None) is None


def test_aggregate_workers_sums_and_flags_degraded():
    payloads = [
        {
            "pid": 1,
            "requests": 10,
            "db_queries": 4,
            "cache_hits": 6,
            "cache_misses": 2,
            "ws_global_connections": 1,
            "ws_system_connections": 0,
            "broadcaster_role": "leader",
        },
        {
            "pid": 2,
            "requests": 5,
            "db_queries": 1,
            "cache_hits": 3,
            "cache_misses": 1,
            "ws_global_connections": 0,
            "ws_system_connections": 2,
            "broadcaster_role": "follower",
        },
    ]
    agg = aggregate_workers(payloads, expected_workers=3)
    assert agg["worker_count"] == 2
    assert agg["degraded"] is True
    assert agg["totals"]["requests"] == 15
    assert agg["totals"]["cache_hits"] == 9
    assert agg["cache_hit_rate"] == round(9 / 12, 4)


def test_aggregate_workers_not_degraded_when_no_expectation():
    agg = aggregate_workers([], expected_workers=None)
    assert agg["worker_count"] == 0
    assert agg["degraded"] is False
    assert agg["cache_hit_rate"] is None


def test_health_detail_404_when_token_unset(monkeypatch):
    import asyncio
    import app.main as main
    import app.config
    from fastapi import HTTPException

    # config is a frozen dataclass; patch the reference main uses.
    class _Cfg:
        health_token = None

    monkeypatch.setattr(main, "config", _Cfg())
    try:
        asyncio.run(main.health_detail(authorization=None))
        assert False, "expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 404


def test_health_check_503_body_is_generic(monkeypatch):
    import asyncio
    import app.main as main
    from app.database import db
    import app.health as health
    from fastapi import HTTPException

    async def _no(*a, **k):
        return False

    monkeypatch.setattr(db, "is_healthy", _no)
    monkeypatch.setattr(health, "redis_ok", _no)

    try:
        asyncio.run(main.health_check())
        assert False, "expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 503
        assert "database" not in exc.detail
        assert "redis" not in exc.detail
        assert exc.detail == {"status": "unavailable"}
