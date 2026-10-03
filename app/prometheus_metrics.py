from __future__ import annotations

import logging

from fastapi import FastAPI
from prometheus_client import Counter, Gauge, Histogram, Info, start_http_server
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import SERVICE_VERSION, Config

logger = logging.getLogger(__name__)

_BYTE_BUCKETS = (0, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576)


errors = Counter(
    "eve_killmap_errors",
    "Unhandled errors swallowed in a handler/loop, by component.",
    ["component"],
)
service_start_timestamp = Gauge(
    "eve_killmap_service_start_timestamp_seconds",
    "Unix time this worker started.",
)
service_info = Info(
    "eve_killmap_service",
    "Static service information (version).",
)


cache_hits = Counter(
    "eve_killmap_cache_hits",
    "Response-cache hits, by cache.",
    ["cache"],
)
cache_misses = Counter(
    "eve_killmap_cache_misses",
    "Response-cache misses, by cache.",
    ["cache"],
)
redis_command_seconds = Histogram(
    "eve_killmap_redis_command_seconds",
    "Latency of a response-cache Redis command, by op.",
    ["op"],
)


cache_invalidations_received = Counter(
    "eve_killmap_cache_invalidations_received",
    "Cache-invalidation messages received, by target.",
    ["target"],
)
cache_keys_evicted = Counter(
    "eve_killmap_cache_keys_evicted",
    "Cache keys evicted by invalidation, by target.",
    ["target"],
)

cache_warm_runs_total = Counter(
    "eve_killmap_cache_warm_runs_total",
    "Leader cache-warm cycles, by outcome.",
    ["outcome"],
)
cache_warm_seconds = Histogram(
    "eve_killmap_cache_warm_seconds",
    "Duration of a leader cache-warm cycle.",
)
cache_warm_last_success_timestamp_seconds = Gauge(
    "eve_killmap_cache_warm_last_success_timestamp_seconds",
    "Unix time of the last successful cache-warm cycle.",
)


esi_requests = Counter(
    "eve_killmap_esi_requests",
    "ESI HTTP responses, by endpoint and outcome.",
    [
        "endpoint",
        "outcome",
    ],
)
esi_request_seconds = Histogram(
    "eve_killmap_esi_request_seconds",
    "Latency of a single ESI HTTP request, by endpoint.",
    ["endpoint"],
)
esi_cache_hits = Counter(
    "eve_killmap_esi_cache_hits",
    "ESI Redis-cache hits, by entity.",
    ["entity"],
)
esi_cache_misses = Counter(
    "eve_killmap_esi_cache_misses",
    "ESI Redis-cache misses, by entity.",
    ["entity"],
)

entity_lookups = Counter(
    "eve_killmap_entity_lookups",
    "Entity name resolutions from the DB reference tables, by kind and result.",
    [
        "kind",
        "result",
    ],
)
war_lookups = Counter(
    "eve_killmap_war_lookups",
    "War lookups from the wars table, by result.",
    ["result"],
)


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
esi_feed_refreshes = Counter(
    "eve_killmap_esi_feed_refreshes",
    "ESI feed refresh cycles at the leader, by feed and outcome.",
    ["feed", "outcome"],
)
esi_feed_last_success_timestamp_seconds = Gauge(
    "eve_killmap_esi_feed_last_success_timestamp_seconds",
    "Unix time of the last refresh that produced a value, by feed.",
    ["feed"],
)
esi_error_limit_remain = Gauge(
    "eve_killmap_esi_error_limit_remain",
    "Errors remaining in ESI's current error-limit window (X-ESI-Error-Limit-Remain).",
)
broadcaster_subscriber_connected = Gauge(
    "eve_killmap_broadcaster_subscriber_connected",
    "1 while this worker's pubsub subscriber is subscribed and delivering, else 0 "
    "(0 means live kill sockets are rejected on this worker).",
)
broadcaster_subscriber_reconnects = Counter(
    "eve_killmap_broadcaster_subscriber_reconnects",
    "Times the pubsub subscriber re-subscribed after an error or a closed stream.",
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


ws_connections = Counter(
    "eve_killmap_ws_connections",
    "WebSocket connection attempts, by transport and outcome.",
    [
        "transport",
        "outcome",
    ],
)
live_clients = Gauge(
    "eve_killmap_live_clients",
    "Currently connected live-map WebSocket clients, by transport.",
    ["transport"],
)
ws_messages_dropped = Counter(
    "eve_killmap_ws_messages_dropped",
    "Live kill messages dropped because a client queue was full.",
)


since_short_circuits = Counter(
    "eve_killmap_since_short_circuits",
    "since-poll requests short-circuited by the per-system latest-insert cache.",
)
kills_binary_response_bytes = Histogram(
    "eve_killmap_kills_binary_response_bytes",
    "Size in bytes of binary kills payloads returned by the systems kills endpoint.",
    buckets=_BYTE_BUCKETS,
)


facet_query_seconds = Histogram(
    "eve_killmap_facet_query_seconds",
    "Latency of a kill_facets filtered query, by query type.",
    ["query"],
)
autocomplete_requests = Counter(
    "eve_killmap_autocomplete_requests",
    "Autocomplete requests, by picker kind and outcome.",
    [
        "kind",
        "outcome",
    ],
)
filter_conditions = Histogram(
    "eve_killmap_filter_conditions",
    "Number of conditions in a filtered (non-empty) request.",
    buckets=(1, 2, 3, 4, 5, 6, 7, 8),
)


_started = False


def instrument_app(app: FastAPI) -> None:
    Instrumentator(should_group_status_codes=True).instrument(
        app, metric_namespace="eve_killmap"
    )


def start_exporter(config: Config) -> None:
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
