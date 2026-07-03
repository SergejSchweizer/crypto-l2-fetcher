"""Tests for YAML-backed project configuration."""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.config import (
    Config,
    config_bool,
    config_int,
    config_section,
    config_str_list,
    load_config,
    typed_http_config,
    typed_project_config,
    typed_runtime_config,
)


def test_load_config_reads_yaml_defaults(tmp_path: Path) -> None:
    """Verify project config values are loaded from config.yaml."""

    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
http:
  timeout_s: 30
ingestion:
  symbols: [BTC, SOL]
  snapshot_count: 6
  save_parquet_lake: true
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_path))
    runtime_config = config_section(config, "runtime")
    ingestion_config = config_section(config, "ingestion")

    assert config_int(runtime_config, "log_backup_count", 0) == 3
    assert config_str_list(ingestion_config, "symbols", []) == ["BTC", "SOL"]
    assert config_int(ingestion_config, "snapshot_count", 0) == 6
    assert config_bool(ingestion_config, "save_parquet_lake", False) is True


def test_typed_runtime_config_preserves_logfile_precedence() -> None:
    """Verify typed runtime config keeps legacy top-level logfile behavior."""

    config: Config = {
        "logfile": "/tmp/project-logs/ignored.log",
        "runtime": {
            "log_dir": ".logs",
            "log_rotation_days": 14,
            "log_backup_count": 5,
        },
    }

    runtime_config = typed_runtime_config(config)

    assert runtime_config.log_dir == Path("/tmp/project-logs")
    assert runtime_config.log_rotation_days == 14
    assert runtime_config.log_backup_count == 5


def test_typed_http_config_reads_http_section() -> None:
    """Verify typed HTTP config resolves timeout and retry values."""

    config: Config = {
        "http": {
            "timeout_s": 2.5,
            "max_retries": 4,
            "retry_backoff_s": 0.25,
        },
    }

    http_config = typed_http_config(config)

    assert http_config.timeout_s == 2.5
    assert http_config.max_retries == 4
    assert http_config.retry_backoff_s == 0.25


def test_typed_project_config_rejects_invalid_sections() -> None:
    """Verify typed config fails explicitly when shared sections are malformed."""

    with pytest.raises(ValueError, match="Config section 'runtime' must be a mapping"):
        typed_project_config({"runtime": "bad"})
