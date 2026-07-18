from pathlib import Path

import pytest

from app.config import ConfigError, load_config, require_database_url, worker_log_file


def _write_yaml(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "config.yml"
    p.write_text(text, encoding="utf-8")
    return p


def test_worker_log_file_unchanged_without_id():
    p = Path("/var/log/backend.log")
    assert worker_log_file(p, None) == p
    assert worker_log_file(p, "") == p


def test_worker_log_file_inserts_id():
    assert worker_log_file(Path("/var/log/backend.log"), "2") == Path(
        "/var/log/backend.worker-2.log"
    )


def test_worker_id_derives_per_worker_file(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml",
        env={"WORKER_ID": "3", "LOG_FILE": "backend.log"},
        base_dir=tmp_path,
    )
    assert cfg.worker_id == "3"
    assert cfg.logging.file == tmp_path / "backend.worker-3.log"


def test_no_worker_id_by_default(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert cfg.worker_id is None
    assert cfg.logging.file == tmp_path / "backend.log"


def test_defaults_reproduce_behavior(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "missing.yml", env={}, base_dir=tmp_path)
    assert cfg.logging.level == "INFO"
    assert cfg.database.pool_min_size == 5
    assert cfg.database.pool_max_size == 20
    assert cfg.cache.query_ttl == 300
    assert cfg.cache.binary_ttl == 60
    assert cfg.streaming.stream_name == "kills:live"
    assert cfg.streaming.invalidate_channel == "cache:invalidate"
    assert cfg.limits.max_killmail_ids == 100
    assert cfg.health.expected_workers is None
    assert cfg.database_url is None
    assert cfg.redis_url is None
    assert "magicmq" not in cfg.user_agent  # PII-free default


def test_env_overrides_yaml(tmp_path):
    yaml_path = _write_yaml(tmp_path, "logging:\n  level: WARNING\n")
    cfg = load_config(
        yaml_path=yaml_path, env={"LOG_LEVEL": "debug"}, base_dir=tmp_path
    )
    assert cfg.logging.level == "DEBUG"


def test_yaml_overrides_default(tmp_path):
    yaml_path = _write_yaml(tmp_path, "database:\n  pool_max_size: 50\n")
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.database.pool_max_size == 50


def test_invalid_log_level_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(
            yaml_path=tmp_path / "x.yml", env={"LOG_LEVEL": "BOGUS"}, base_dir=tmp_path
        )


def test_non_integer_pool_size_raises(tmp_path):
    yaml_path = _write_yaml(tmp_path, "database:\n  pool_min_size: notanint\n")
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_pool_max_below_min_raises(tmp_path):
    yaml_path = _write_yaml(
        tmp_path, "database:\n  pool_min_size: 10\n  pool_max_size: 5\n"
    )
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_section_must_be_mapping(tmp_path):
    yaml_path = _write_yaml(tmp_path, "logging: not-a-mapping\n")
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_cors_parsed_as_list(tmp_path):
    yaml_path = _write_yaml(
        tmp_path,
        "cors:\n  allow_origins:\n    - https://a.example\n    - https://b.example\n",
    )
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.cors.allow_origins == ["https://a.example", "https://b.example"]


def test_cors_comma_string_parsed_as_list(tmp_path):
    yaml_path = _write_yaml(
        tmp_path, "cors:\n  allow_origins: 'https://a.example, https://b.example'\n"
    )
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.cors.allow_origins == ["https://a.example", "https://b.example"]


def test_log_file_relative_resolves_against_base_dir(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml", env={"LOG_FILE": "sub/app.log"}, base_dir=tmp_path
    )
    assert cfg.logging.file == tmp_path / "sub" / "app.log"


def test_require_database_url(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    with pytest.raises(ConfigError):
        require_database_url(cfg)
    cfg2 = load_config(
        yaml_path=tmp_path / "x.yml",
        env={"DATABASE_URL": "postgresql://u:p@h/d"},
        base_dir=tmp_path,
    )
    assert require_database_url(cfg2) == "postgresql://u:p@h/d"


def test_metrics_defaults(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "missing.yml", env={}, base_dir=tmp_path)
    assert cfg.metrics.enabled is False
    assert cfg.metrics.host == "127.0.0.1"
    assert cfg.metrics.port == 9109


def test_metrics_port_env_override(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml", env={"METRICS_PORT": "9101"}, base_dir=tmp_path
    )
    assert cfg.metrics.port == 9101  # coerced str -> int


def test_metrics_port_yaml(tmp_path):
    yaml_path = _write_yaml(tmp_path, "metrics:\n  port: 9200\n")
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.metrics.port == 9200


def test_metrics_env_beats_yaml(tmp_path):
    yaml_path = _write_yaml(tmp_path, "metrics:\n  port: 9200\n")
    cfg = load_config(
        yaml_path=yaml_path, env={"METRICS_PORT": "9102"}, base_dir=tmp_path
    )
    assert cfg.metrics.port == 9102


def test_metrics_enabled_and_host_from_yaml(tmp_path):
    yaml_path = _write_yaml(
        tmp_path, "metrics:\n  enabled: true\n  host: 10.0.0.5\n"
    )
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.metrics.enabled is True
    assert cfg.metrics.host == "10.0.0.5"


def test_metrics_port_invalid_env_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(
            yaml_path=tmp_path / "x.yml",
            env={"METRICS_PORT": "not-a-port"},
            base_dir=tmp_path,
        )


def test_metrics_port_out_of_range_raises(tmp_path):
    with pytest.raises(ConfigError):
        load_config(
            yaml_path=tmp_path / "x.yml",
            env={"METRICS_PORT": "70000"},
            base_dir=tmp_path,
        )


def test_metrics_enabled_not_bool_raises(tmp_path):
    yaml_path = _write_yaml(tmp_path, "metrics:\n  enabled: yes-please\n")
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_leader_election_defaults_true(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "x.yml", env={}, base_dir=tmp_path)
    assert cfg.leader_election is True


def test_leader_election_false(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml",
        env={"LEADER_ELECTION": "False"},
        base_dir=tmp_path,
    )
    assert cfg.leader_election is False


def test_leader_election_true_explicit(tmp_path):
    cfg = load_config(
        yaml_path=tmp_path / "x.yml",
        env={"LEADER_ELECTION": "True"},
        base_dir=tmp_path,
    )
    assert cfg.leader_election is True


def test_cache_redis_defaults(tmp_path):
    cfg = load_config(yaml_path=tmp_path / "missing.yml", env={}, base_dir=tmp_path)
    cr = cfg.cache_redis
    assert cr.max_connections == 200
    assert cr.pool_timeout == 5
    assert cr.socket_connect_timeout == 5
    assert cr.socket_timeout == 10
    assert cr.socket_keepalive is True
    assert cr.health_check_interval == 30


def test_cache_redis_overrides(tmp_path):
    yaml_path = _write_yaml(
        tmp_path,
        "cache_redis:\n  max_connections: 500\n  pool_timeout: 3\n"
        "  socket_keepalive: false\n",
    )
    cfg = load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
    assert cfg.cache_redis.max_connections == 500
    assert cfg.cache_redis.pool_timeout == 3
    assert cfg.cache_redis.socket_keepalive is False


def test_cache_redis_keepalive_not_bool_raises(tmp_path):
    yaml_path = _write_yaml(tmp_path, "cache_redis:\n  socket_keepalive: maybe\n")
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)


def test_cache_redis_bad_int_raises(tmp_path):
    yaml_path = _write_yaml(tmp_path, "cache_redis:\n  max_connections: 0\n")
    with pytest.raises(ConfigError):
        load_config(yaml_path=yaml_path, env={}, base_dir=tmp_path)
