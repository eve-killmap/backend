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
