from app.metrics import Metrics


def test_snapshot_has_expected_keys():
    m = Metrics()
    m.requests += 3
    m.cache_hits += 2
    snap = m.snapshot()
    assert snap["requests"] == 3
    assert snap["cache_hits"] == 2
    assert set(snap) == {
        "uptime",
        "requests",
        "db_queries",
        "cache_hits",
        "cache_misses",
        "ws_global_connections",
        "ws_system_connections",
        "broadcaster_role",
    }
    assert snap["broadcaster_role"] == "disabled"
    assert snap["uptime"] >= 0


def test_unsubscribe_global_gauge_no_negative_drift():
    from app.metrics import metrics
    from app.redis_client import KillBroadcaster

    b = KillBroadcaster()
    base = metrics.ws_global_connections
    q = b.subscribe_global()
    assert metrics.ws_global_connections == base + 1
    b.unsubscribe_global(q)
    assert metrics.ws_global_connections == base
    b.unsubscribe_global(q)  # double unsubscribe must be a no-op for the gauge
    assert metrics.ws_global_connections == base
