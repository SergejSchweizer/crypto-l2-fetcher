"""Tests for index price bronze CLI command behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import INDEX_PRICE_BRONZE_BUILDER_COMMAND
from ingestion.config import Config


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config: Config = {
        "http": {"timeout_s": 8, "max_retries": 2, "retry_backoff_s": 1},
        "runtime": {"log_dir": str(tmp_path), "log_rotation_days": 7, "log_backup_count": 0},
        "ingestion": {
            "exchange": "deribit",
            "symbols": ["BTC", "ETH"],
            "levels": 50,
            "snapshot_count": 5,
            "poll_interval_s": 10,
            "max_runtime_s": 50,
            "save_parquet_lake": False,
            "lake_root": "lake/bronze",
            "json_output": True,
            "index_price": {
                "symbols": ["btc_usd", "eth_usd", "sol_usdc"],
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_index_price",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def test_cli_index_price_bronze_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(bronze, "fetch_index_price", lambda _: 12345.67)
    monkeypatch.setattr(
        bronze,
        "save_index_price_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/index_price.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            INDEX_PRICE_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "btc_usd,eth_usd",
            "--save-parquet-lake",
        ],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == INDEX_PRICE_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "index_price_snapshot_1m"
    assert output["output_files"] == ["/tmp/index_price.parquet"]
