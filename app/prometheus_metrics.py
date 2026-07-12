"""Prometheus metrics for the FastAPI backend.

Per-worker scrape model: the backend runs as N single-worker uvicorn processes
(systemd @%i), and each worker exposes its own /metrics HTTP endpoint on a
dedicated port (METRICS_PORT). Aggregation across workers is done at query time
in PromQL/Grafana (sum(...)); prometheus_client multiprocess mode is deliberately
NOT used. Metric objects are module-level singletons (the intended
prometheus_client pattern) and are cheap no-ops until start_exporter binds the
port. HTTP request metrics come from prometheus-fastapi-instrumentator.

Metric names are prefixed eve_killmap_ (no per-service segment); Prometheus
distinguishes services by the scrape target's job/instance labels. All labels are
bounded enums (the cardinality rule). Instrumentation .inc()/.observe()/.set()
calls must never raise into request/business logic.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI
from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import SERVICE_VERSION, Config

logger = logging.getLogger(__name__)

_BYTE_BUCKETS = (0, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576)


# Shared / convention

errors = Counter(
    "eve_killmap_errors",
    "Unhandled errors swallowed in a handler/loop, by component.",
    ["component"],  # esi|broadcaster|invalidation
)
service_start_timestamp = Gauge(
    "eve_killmap_service_start_timestamp_seconds",
    "Unix time this worker started.",
)
service_info = Info(
    "eve_killmap_service",
    "Static service information (version).",
)


# Response caches

cache_hits = Counter(
    "eve_killmap_cache_hits",
    "Response-cache hits, by cache.",
    ["cache"],  # system_rankings|farthest_kill|sov|kill_details|kill_details_processed|binary|type_name
)
cache_misses = Counter(
    "eve_killmap_cache_misses",
    "Response-cache misses, by cache.",
    ["cache"],
)
redis_command_seconds = Histogram(
    "eve_killmap_redis_command_seconds",
    "Latency of a response-cache Redis command, by op.",
    ["op"],  # get|set
)


# Cache invalidation (pairs with process-kills)

cache_invalidations_received = Counter(
    "eve_killmap_cache_invalidations_received",
    "Cache-invalidation messages received, by target.",
    ["target"],  # system_rankings|farthest_kill|sov
)
cache_keys_evicted = Counter(
    "eve_killmap_cache_keys_evicted",
    "Cache keys evicted by invalidation, by target.",
    ["target"],
)


# ESI client

esi_requests = Counter(
    "eve_killmap_esi_requests",
    "ESI HTTP responses, by endpoint and outcome.",
    ["endpoint", "outcome"],  # endpoint: names|corporation|alliance|war|sov  outcome: ok|not_found|rate_limited|error
)
esi_request_seconds = Histogram(
    "eve_killmap_esi_request_seconds",
    "Latency of a single ESI HTTP request, by endpoint.",
    ["endpoint"],
)
esi_cache_hits = Counter(
    "eve_killmap_esi_cache_hits",
    "ESI Redis-cache hits, by entity.",
    ["entity"],  # character|corporation|alliance|war|sov
)
esi_cache_misses = Counter(
    "eve_killmap_esi_cache_misses",
    "ESI Redis-cache misses, by entity.",
    ["entity"],
)
esi_rate_limit_tokens = Gauge(
    "eve_killmap_esi_rate_limit_tokens",
    "Remaining tokens reported by the ESI war endpoint rate-limit header.",
)


# Broadcaster / leader

broadcaster_is_leader = Gauge(
    "eve_killmap_broadcaster_is_leader",
    "1 while this worker is the elected broadcaster leader, else 0 "
    "(sum across instances == 1).",
)
leader_promotions = Counter(
    "eve_killmap_leader_promotions",
    "Times this worker was promoted to broadcaster leader.",
)
stream_read_interruptions = Counter(
    "eve_killmap_stream_read_interruptions",
    "Redis timeout/connection interruptions in the leader stream read loop.",
)
sov_refreshes = Counter(
    "eve_killmap_sov_refreshes",
    "Sov-map refresh cycles at the leader, by outcome.",
    ["outcome"],  # ok|error
)
stream_entries_read = Counter(
    "eve_killmap_stream_entries_read",
    "Kill stream entries read by the leader.",
)
live_events_pushed = Counter(
    "eve_killmap_live_events_pushed",
    "Enriched kills published to the internal fan-out channel by the leader.",
)
stream_consumer_lag_seconds = Gauge(
    "eve_killmap_stream_consumer_lag_seconds",
    "Age (now - stream entry timestamp) of the last kill read by the leader.",
)


# WebSockets / live

ws_connections = Counter(
    "eve_killmap_ws_connections",
    "WebSocket connection attempts, by transport and outcome.",
    ["transport", "outcome"],  # transport: ws  outcome: accepted|rejected_origin|rejected_capacity|unavailable
)
live_clients = Gauge(
    "eve_killmap_live_clients",
    "Currently connected live-map WebSocket clients, by transport.",
    ["transport"],  # ws
)
ws_messages_dropped = Counter(
    "eve_killmap_ws_messages_dropped",
    "Live kill messages dropped because a client queue was full.",
)


# Optimization visibility

since_short_circuits = Counter(
    "eve_killmap_since_short_circuits",
    "since-poll requests short-circuited by the per-system latest-insert cache.",
)
kills_binary_response_bytes = Histogram(
    "eve_killmap_kills_binary_response_bytes",
    "Size in bytes of binary kills payloads returned by the systems kills endpoint.",
    buckets=_BYTE_BUCKETS,
)


_started = False


def instrument_app(app: FastAPI) -> None:
    """Attach the HTTP-request instrumentator (middleware + eve_killmap_http_* metrics).

    Called once at app construction, gated on config.metrics.enabled. Does NOT
    expose an endpoint on the app; the metrics register in the default REGISTRY,
    which start_exporter serves on the dedicated port.

    Only ``metric_namespace="eve_killmap"`` is set (no ``metric_subsystem``): the
    library's default metric names already start with ``http_``, so the namespace
    alone yields the convention's ``eve_killmap_http_requests_total`` /
    ``eve_killmap_http_request_duration_seconds``. Adding ``metric_subsystem="http"``
    would double the segment (``eve_killmap_http_http_*``).
    """
    Instrumentator(should_group_status_codes=True).instrument(
        app, metric_namespace="eve_killmap"
    )


def start_exporter(config: Config) -> None:
    """Start the per-worker Prometheus scrape endpoint if enabled (idempotent)."""
    global _started
    if not config.metrics.enabled:
        logger.info("Prometheus metrics exporter disabled (metrics.enabled=false).")
        return
    if _started:
        return

    service_info.info({"version": SERVICE_VERSION})
    service_start_timestamp.set_to_current_time()

    start_http_server(config.metrics.port, addr=config.metrics.host)
    _started = True
    logger.info(
        "Prometheus metrics exporter listening on %s:%d",
        config.metrics.host,
        config.metrics.port,
    )
