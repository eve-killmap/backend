from prometheus_client import REGISTRY

import app.prometheus_metrics as pm
from app.config import load_config


def _cfg(tmp_path, text="", env=None):
    p = tmp_path / "config.yml"
    p.write_text(text, encoding="utf-8")
    return load_config(yaml_path=p, env=env or {}, base_dir=tmp_path)


def test_module_imports_and_registers():
    # Importing the module registers the singletons in the default registry.
    assert pm.cache_hits is not None
    assert pm.service_info is not None


def test_singletons_accept_calls():
    # Every labeled metric accepts a call over its documented enum without raising.
    for cache in (
        "system_rankings", "farthest_kill", "sov", "kill_details",
        "kill_details_processed", "binary", "type_name",
    ):
        pm.cache_hits.labels(cache=cache).inc()
        pm.cache_misses.labels(cache=cache).inc()
    for op in ("get", "set"):
        pm.redis_command_seconds.labels(op=op).observe(0.001)
    for target in ("system_rankings", "farthest_kill", "sov"):
        pm.cache_invalidations_received.labels(target=target).inc()
        pm.cache_keys_evicted.labels(target=target).inc(3)
    for endpoint in ("names", "corporation", "alliance", "war", "sov"):
        for outcome in ("ok", "not_found", "rate_limited", "error"):
            pm.esi_requests.labels(endpoint=endpoint, outcome=outcome).inc()
        pm.esi_request_seconds.labels(endpoint=endpoint).observe(0.05)
    for entity in ("character", "corporation", "alliance", "war", "sov"):
        pm.esi_cache_hits.labels(entity=entity).inc()
        pm.esi_cache_misses.labels(entity=entity).inc()
    pm.esi_rate_limit_tokens.set(100)
    pm.broadcaster_is_leader.set(1)
    pm.leader_promotions.inc()
    pm.stream_read_interruptions.inc()
    for outcome in ("ok", "error"):
        pm.sov_refreshes.labels(outcome=outcome).inc()
    pm.stream_entries_read.inc()
    pm.live_events_pushed.inc()
    pm.stream_consumer_lag_seconds.set(1.5)
    for outcome in ("accepted", "rejected_origin", "rejected_capacity", "unavailable"):
        pm.ws_connections.labels(transport="ws", outcome=outcome).inc()
    pm.live_clients.labels(transport="ws").inc()
    pm.ws_messages_dropped.inc()
    pm.since_short_circuits.inc()
    pm.kills_binary_response_bytes.observe(2048)
    for component in ("esi", "broadcaster", "invalidation"):
        pm.errors.labels(component=component).inc()


def test_start_exporter_disabled_is_noop(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(pm, "start_http_server", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(pm, "_started", False)
    cfg = _cfg(tmp_path)  # enabled defaults False
    pm.start_exporter(cfg)
    assert calls == []


def test_start_exporter_enabled_binds_once(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(pm, "start_http_server", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(pm, "_started", False)
    cfg = _cfg(tmp_path, "metrics:\n  enabled: true\n  host: 127.0.0.1\n  port: 9109\n")
    pm.start_exporter(cfg)
    pm.start_exporter(cfg)  # idempotent
    assert calls == [((9109,), {"addr": "127.0.0.1"})]
    assert REGISTRY.get_sample_value(
        "eve_killmap_service_start_timestamp_seconds"
    ) > 0
    assert (
        REGISTRY.get_sample_value(
            "eve_killmap_service_info", {"version": "1.0.0"}
        )
        == 1.0
    )


def test_instrument_app_does_not_raise():
    from fastapi import FastAPI

    pm.instrument_app(FastAPI())  # attaches middleware; must not raise
