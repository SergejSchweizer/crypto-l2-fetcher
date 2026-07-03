"""Tests for instrument metadata bronze CLI command behavior."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND
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
            "instrument_metadata": {
                "currencies": ["BTC", "ETH", "SOL"],
                "kind": "option",
                "include_inactive": False,
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_instruments",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def test_cli_instrument_metadata_bronze_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        bronze,
        "fetch_instruments",
        lambda currency, kind, expired: [{"instrument_name": f"{currency}-30JUN26-120000-C", "kind": kind}],
    )
    monkeypatch.setattr(
        bronze,
        "save_instrument_metadata_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/instrument_metadata.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--save-parquet-lake",
        ],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["command"] == INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "instrument_metadata_snapshot_daily"
    assert output["output_files"] == ["/tmp/instrument_metadata.parquet"]


def test_cli_future_instrument_metadata_reports_future_dataset(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Future metadata should report the future-specific dataset type."""

    monkeypatch.setattr(
        bronze,
        "fetch_instruments",
        lambda currency, kind, expired: [{"instrument_name": f"{currency}-PERPETUAL", "kind": kind}],
    )
    monkeypatch.setattr(bronze, "save_instrument_metadata_snapshot_parquet_lake", lambda **kwargs: [])
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "SOL",
            "--kind",
            "future",
            "--no-save-parquet-lake",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["dataset_type"] == "future_instrument_metadata_snapshot_daily"
