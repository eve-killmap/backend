from __future__ import annotations

import logging
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.yml"

SERVICE_VERSION = "1.0.0"

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

# Generic, PII-free default. Operators MUST override USER_AGENT in .env with a
# real contact per CCP's API rules.
_DEFAULT_USER_AGENT = (
    "eve-killmap:backend/1.0.0 (+https://github.com/eve-killmap/backend)"
)

_DEFAULT_CORS_ORIGINS = ["http://127.0.0.1:3000", "http://localhost:3000"]


class ConfigError(Exception):
    """Raised when configuration is missing or invalid."""


@dataclass(frozen=True)
class LoggingConfig:
    level: str
    file: Path
    max_bytes: int
    backup_count: int


@dataclass(frozen=True)
class CorsConfig:
    allow_origins: list[str]
    allow_methods: list[str]
    allow_headers: list[str]


@dataclass(frozen=True)
class DatabaseConfig:
    pool_min_size: int
    pool_max_size: int


@dataclass(frozen=True)
class CacheConfig:
    query_ttl: int
    binary_ttl: int
    type_name_ttl: int
    esi_corp_ttl: int
    esi_alliance_ttl: int
    esi_sov_fallback_ttl: int
    rankings_ttl: int
    sov_ttl: int
    farthest_kill_ttl: int
    kill_detail_ttl: int
    kill_detail_processed_ttl: int
    system_latest_ttl: int


@dataclass(frozen=True)
class StreamingConfig:
    stream_name: str
    pubsub_channel: str
    invalidate_channel: str


@dataclass(frozen=True)
class HealthConfig:
    heartbeat_interval: int
    heartbeat_ttl: int
    expected_workers: int | None


@dataclass(frozen=True)
class LimitsConfig:
    max_killmail_ids: int
    max_name_ids: int
    max_ws_connections: int


@dataclass(frozen=True)
class MetricsConfig:
    enabled: bool
    host: str
    port: int


@dataclass(frozen=True)
class Config:
    logging: LoggingConfig
    cors: CorsConfig
    database: DatabaseConfig
    cache: CacheConfig
    streaming: StreamingConfig
    health: HealthConfig
    limits: LimitsConfig
    metrics: MetricsConfig
    user_agent: str
    database_url: str | None
    redis_url: str | None
    redis_cache_url: str | None
    health_token: str | None
    worker_id: str | None
    leader_election: bool


def worker_log_file(path: Path, worker_id: str | None) -> Path:
    if not worker_id:
        return path
    return path.with_name(f"{path.stem}.worker-{worker_id}{path.suffix}")


def _section(data: dict[str, Any], name: str) -> dict[str, Any]:
    section = data.get(name) or {}
    if not isinstance(section, dict):
        raise ConfigError(f"Config section '{name}' must be a mapping")
    return section


def _as_int(
    value: Any, label: str, *, minimum: int | None = None, maximum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"Config value '{label}' must be an integer, got {value!r}")
    if minimum is not None and value < minimum:
        raise ConfigError(f"Config value '{label}' must be >= {minimum}, got {value}")
    if maximum is not None and value > maximum:
        raise ConfigError(f"Config value '{label}' must be <= {maximum}, got {value}")
    return value


def _as_str_list(value: Any, label: str) -> list[str]:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, list):
        return [str(item) for item in value]
    raise ConfigError(
        f"Config value '{label}' must be a list or comma-separated string"
    )


def _load_yaml(yaml_path: Path) -> dict[str, Any]:
    if not yaml_path.exists():
        return {}
    try:
        loaded = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"Could not parse config file {yaml_path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"Config file {yaml_path} must contain a top-level mapping")
    return loaded


def load_config(
    yaml_path: Path | None = None,
    env: Mapping[str, str] | None = None,
    base_dir: Path | None = None,
) -> Config:
    """Build a :class:`Config` from defaults, the YAML file, and the environment.

    Raises :class:`ConfigError` for missing/invalid values. ``DATABASE_URL`` is
    not required here; validate it lazily via :func:`require_database_url`.
    """
    base_dir = base_dir or BASE_DIR
    env = os.environ if env is None else env
    data = _load_yaml(yaml_path or DEFAULT_CONFIG_PATH)

    log_cfg = _section(data, "logging")
    cors_cfg = _section(data, "cors")
    db_cfg = _section(data, "database")
    cache_cfg = _section(data, "cache")
    stream_cfg = _section(data, "streaming")
    health_cfg = _section(data, "health")
    limits_cfg = _section(data, "limits")
    metrics_cfg = _section(data, "metrics")

    level = (env.get("LOG_LEVEL") or log_cfg.get("level") or "INFO").upper()
    if level not in VALID_LOG_LEVELS:
        raise ConfigError(
            f"Invalid log level {level!r}; expected one of {sorted(VALID_LOG_LEVELS)}"
        )

    log_file = Path(env.get("LOG_FILE") or log_cfg.get("file") or "backend.log")
    if not log_file.is_absolute():
        log_file = base_dir / log_file

    worker_id = env.get("WORKER_ID") or None
    log_file = worker_log_file(log_file, worker_id)

    leader_election = (env.get("LEADER_ELECTION") or "true").strip().lower() != "false"

    logging_config = LoggingConfig(
        level=level,
        file=log_file,
        max_bytes=_as_int(
            log_cfg.get("max_bytes", 10 * 1024 * 1024), "logging.max_bytes", minimum=1
        ),
        backup_count=_as_int(
            log_cfg.get("backup_count", 5), "logging.backup_count", minimum=0
        ),
    )

    cors_config = CorsConfig(
        allow_origins=_as_str_list(
            cors_cfg.get("allow_origins", _DEFAULT_CORS_ORIGINS), "cors.allow_origins"
        ),
        allow_methods=_as_str_list(
            cors_cfg.get("allow_methods", ["GET"]), "cors.allow_methods"
        ),
        allow_headers=_as_str_list(
            cors_cfg.get("allow_headers", ["*"]), "cors.allow_headers"
        ),
    )

    database_config = DatabaseConfig(
        pool_min_size=_as_int(
            db_cfg.get("pool_min_size", 5), "database.pool_min_size", minimum=1
        ),
        pool_max_size=_as_int(
            db_cfg.get("pool_max_size", 20), "database.pool_max_size", minimum=1
        ),
    )
    if database_config.pool_max_size < database_config.pool_min_size:
        raise ConfigError("database.pool_max_size must be >= database.pool_min_size")

    cache_config = CacheConfig(
        query_ttl=_as_int(
            cache_cfg.get("query_ttl", 300), "cache.query_ttl", minimum=1
        ),
        binary_ttl=_as_int(
            cache_cfg.get("binary_ttl", 60), "cache.binary_ttl", minimum=1
        ),
        type_name_ttl=_as_int(
            cache_cfg.get("type_name_ttl", 2592000), "cache.type_name_ttl", minimum=1
        ),
        esi_corp_ttl=_as_int(
            cache_cfg.get("esi_corp_ttl", 86400), "cache.esi_corp_ttl", minimum=1
        ),
        esi_alliance_ttl=_as_int(
            cache_cfg.get("esi_alliance_ttl", 86400),
            "cache.esi_alliance_ttl",
            minimum=1,
        ),
        esi_sov_fallback_ttl=_as_int(
            cache_cfg.get("esi_sov_fallback_ttl", 3600),
            "cache.esi_sov_fallback_ttl",
            minimum=1,
        ),
        rankings_ttl=_as_int(
            cache_cfg.get("rankings_ttl", 3600), "cache.rankings_ttl", minimum=1
        ),
        sov_ttl=_as_int(cache_cfg.get("sov_ttl", 3600), "cache.sov_ttl", minimum=1),
        farthest_kill_ttl=_as_int(
            cache_cfg.get("farthest_kill_ttl", 21600),
            "cache.farthest_kill_ttl",
            minimum=1,
        ),
        kill_detail_ttl=_as_int(
            cache_cfg.get("kill_detail_ttl", 604800), "cache.kill_detail_ttl", minimum=1
        ),
        kill_detail_processed_ttl=_as_int(
            cache_cfg.get("kill_detail_processed_ttl", 3600),
            "cache.kill_detail_processed_ttl",
            minimum=1,
        ),
        system_latest_ttl=_as_int(
            cache_cfg.get("system_latest_ttl", 10), "cache.system_latest_ttl", minimum=1
        ),
    )

    streaming_config = StreamingConfig(
        stream_name=stream_cfg.get("stream_name", "kills:live"),
        pubsub_channel=stream_cfg.get("pubsub_channel", "kills:enriched"),
        invalidate_channel=stream_cfg.get("invalidate_channel", "cache:invalidate"),
    )

    expected_workers_raw = health_cfg.get("expected_workers")
    health_config = HealthConfig(
        heartbeat_interval=_as_int(
            health_cfg.get("heartbeat_interval", 10),
            "health.heartbeat_interval",
            minimum=1,
        ),
        heartbeat_ttl=_as_int(
            health_cfg.get("heartbeat_ttl", 30), "health.heartbeat_ttl", minimum=1
        ),
        expected_workers=(
            None
            if expected_workers_raw is None
            else _as_int(expected_workers_raw, "health.expected_workers", minimum=1)
        ),
    )

    limits_config = LimitsConfig(
        max_killmail_ids=_as_int(
            limits_cfg.get("max_killmail_ids", 100),
            "limits.max_killmail_ids",
            minimum=1,
        ),
        max_name_ids=_as_int(
            limits_cfg.get("max_name_ids", 100), "limits.max_name_ids", minimum=1
        ),
        max_ws_connections=_as_int(
            limits_cfg.get("max_ws_connections", 1000),
            "limits.max_ws_connections",
            minimum=1,
        ),
    )

    metrics_enabled = metrics_cfg.get("enabled", False)
    if not isinstance(metrics_enabled, bool):
        raise ConfigError("Config value 'metrics.enabled' must be a boolean")

    metrics_host = metrics_cfg.get("host") or "127.0.0.1"

    metrics_port_env = env.get("METRICS_PORT")
    if metrics_port_env:
        try:
            metrics_port_value: Any = int(metrics_port_env)
        except ValueError:
            raise ConfigError(
                f"METRICS_PORT must be an integer, got {metrics_port_env!r}"
            )
    else:
        metrics_port_value = metrics_cfg.get("port", 9109)
    metrics_port = _as_int(
        metrics_port_value, "metrics.port", minimum=1, maximum=65535
    )

    metrics_config = MetricsConfig(
        enabled=metrics_enabled, host=metrics_host, port=metrics_port
    )

    return Config(
        logging=logging_config,
        cors=cors_config,
        database=database_config,
        cache=cache_config,
        streaming=streaming_config,
        health=health_config,
        limits=limits_config,
        metrics=metrics_config,
        user_agent=env.get("USER_AGENT") or _DEFAULT_USER_AGENT,
        database_url=env.get("DATABASE_URL") or None,
        redis_url=env.get("REDIS_URL") or None,
        redis_cache_url=env.get("REDIS_CACHE_URL") or None,
        health_token=env.get("HEALTH_TOKEN") or None,
        worker_id=worker_id,
        leader_election=leader_election,
    )


def require_database_url(config: Config) -> str:
    """Return the database URL or raise :class:`ConfigError` if unset."""
    if not config.database_url:
        raise ConfigError("DATABASE_URL is required but not set (define it in .env)")
    return config.database_url


def setup_logging(config: Config) -> None:
    """Configure root logging from ``config`` (idempotent)."""
    root_logger = logging.getLogger()
    root_logger.setLevel(config.logging.level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    if config.worker_id:
        fmt = f"[%(asctime)s] [worker {config.worker_id}] [%(levelname)s] [%(name)s] %(message)s"
    else:
        fmt = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    formatter = logging.Formatter(fmt)

    config.logging.file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        config.logging.file,
        maxBytes=config.logging.max_bytes,
        backupCount=config.logging.backup_count,
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


# Module-level singleton: loaded once from .env + config.yml for normal app use.
# Tests call load_config() directly with explicit arguments instead.
load_dotenv(BASE_DIR / ".env")
config = load_config()
