# backend

The public HTTP/WebSocket API for [EVE Killmap](https://eve-killmap.com): it serves
killmail data to the [frontend client](https://github.com/eve-killmap/frontend)
from the shared kills database. The database is kept current by
[process-kills](https://github.com/eve-killmap/process-kills) (whose `kills:live`
Redis stream this service tails for live updates), and type/name metadata is
produced by [process-sde](https://github.com/eve-killmap/process-sde).

It runs as multiple uvicorn workers; one elected leader tails the live stream and
refreshes sovereignty data, while every worker serves requests and fans out live
kills over WebSockets.

## Requirements

- Python 3.12+
- PostgreSQL (the shared kills database; read-only from here)
- Redis (response/ESI cache + live stream; the API degrades gracefully without it)

## Setup

```sh
python -m venv venv
venv/Scripts/activate             # Windows; use source venv/bin/activate on POSIX
pip install -r requirements.txt   # add -dev for the test suite: requirements-dev.txt

cp .env.example .env              # then edit secrets (DATABASE_URL, REDIS_*, USER_AGENT, HEALTH_TOKEN)
cp config.example.yml config.yml  # optional; omit to use built-in defaults
```

## Running

```sh
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

Endpoints (selected): `GET /kills/details/raw`, `GET /kills/details/processed`,
`GET /systems/{id}/kills` (binary), `GET /systems/{id}/sov`,
`GET /stats/system-rankings`, `GET /systems/{id}/farthest_kill`,
`POST /universe/names`, WebSockets `/ws/global/kills` and `/ws/systems/{id}/kills`,
and health (below). Logs go to the rotating file at `LOG_FILE` (default
`./backend.log`) and to stdout.

## Configuration

Settings resolve with the precedence **code defaults < `config.yml` <
environment / `.env`**. Every value has a behavior-preserving default, so both
files are optional (though `DATABASE_URL` is required to serve data).

### `.env`: secrets and machine/deployment-specific values

| Variable          | Purpose                                                            |
| ----------------- | ----------------------------------------------------------------- |
| `DATABASE_URL`    | PostgreSQL connection string (required).                          |
| `REDIS_URL`       | Redis for the live kill stream (broadcaster). Omit to disable.    |
| `REDIS_CACHE_URL` | Redis for the response/ESI cache. Omit to disable caching.        |
| `USER_AGENT`      | Contact-bearing User-Agent for ESI (CCP rule).                    |
| `HEALTH_TOKEN`    | Bearer token for `GET /health/detail`; unset disables that route. |
| `LOG_FILE`        | Log file path (default `./backend.log`).                          |
| `LOG_LEVEL`       | Optional override of `logging.level` from `config.yml`.           |

Secrets are never written to the log.

### `config.yml`: non-secret, operator-tweakable settings

Sections: `logging`, `cors`, `database` (pool sizing), `cache` (per-namespace
TTLs), `esi` (war rate-limit low-water), `streaming` (cross-app Redis names),
`health` (heartbeat interval/TTL, expected workers), and `limits` (input/WS caps).
See [`config.example.yml`](config.example.yml). Invalid/out-of-range values fail
fast with a clear `ConfigError`.

## Health

- `GET /health`: public liveness (`200` if this worker's DB + Redis are reachable,
  else `503`); for nginx/uptime probes.
- `GET /health/detail`: token-gated (`Authorization: Bearer <HEALTH_TOKEN>`);
  aggregates the live worker fleet (per-worker + global counters, cache hit rate),
  DB pool + `pg_stat_database` stats, and domain stats (kill counts, ingestion
  lag). Disabled (`404`) when `HEALTH_TOKEN` is unset.

## Caching

Hot shared endpoints (`system-rankings`, `sov`, `farthest_kill`, full-system
binary) are Redis-cached and carry `Cache-Control` / `ETag` / `Vary` headers for
nginx/browser reuse. Per-killmail details are cached per id (immutable). Sov data
is refreshed by the elected leader only. `since`-polls short-circuit via a
DB-derived per-system latest-insert timestamp. Caches are evicted via the
`cache:invalidate` Redis pub/sub channel.

**Cross-app contract:** `process-kills` publishes `{"targets":
["system_rankings", "farthest_kill"]}` to `cache:invalidate` after each
materialized-view refresh; this service publishes `{"targets": ["sov"]}` after
each sov refresh. Changing the `streaming.*` names is a breaking change to land in
lockstep with `process-kills`.

## Metrics

Optional Prometheus instrumentation, off by default (`metrics.enabled: false`).
Each worker exposes its own `/metrics` endpoint on a dedicated port
(`METRICS_PORT`, set per-worker via systemd, e.g. `Environment=METRICS_PORT=910%i`
→ `@1`=9101, `@2`=9102). Metrics are `eve_killmap_*`-prefixed and line up with
`process-kills` on one Prometheus/Grafana stack.

Because each worker is its own scrape target, aggregate across workers at query
time in PromQL (`sum(eve_killmap_...)`) and keep per-worker visibility via the
`instance` label, which is useful for "which worker is the leader"
(`eve_killmap_broadcaster_is_leader`) or per-worker Redis latency. Multiprocess
mode is deliberately not used.

The metrics port is internal-only: bind loopback (default) or a private
interface, firewall it, and do **not** put it behind nginx or expose it publicly.

## Security

A public-facing API: input is capped, WebSockets enforce an Origin allow-list and
connection cap, responses carry baseline security headers, and upstream errors are
not echoed. Rate limiting is expected at nginx. See
[`docs/superpowers/security-audit.md`](docs/superpowers/security-audit.md).
The optional Prometheus metrics port (see [Metrics](#metrics)) is internal-only.
Bind loopback/private and firewall it; never expose it publicly.

## Testing

```sh
pip install -r requirements-dev.txt
python -m pytest
```

The suite covers configuration, the pure caching/health/security helpers, and the
time parser. It needs no network, credentials, or database.
