"""Tests for volatility-index Bronze CLI command behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND
from ingestion.config import Config
from ingestion.volatility_index import VolatilityIndexSnapshotRow


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config: Config = {
        "http": {"timeout_s": 8, "max_retries": 2, "retry_backoff_s": 1},
        "runtime": {"log_dir": str(tmp_path), "log_rotation_days": 7, "log_backup_count": 0},
        "ingestion": {
            "json_output": True,
            "volatility_index": {
                "currencies": ["BTC", "ETH", "SOL"],
                "resolution": 60,
                "lookback_seconds": 600,
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_volatility_index_data",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _sample_row(currency: str, source_currency: str) -> VolatilityIndexSnapshotRow:
    timestamp = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return VolatilityIndexSnapshotRow(
        schema_version="v1",
        dataset_type="volatility_index_snapshot_1m",
        exchange="deribit",
        source="rest_get_volatility_index_data",
        currency=currency,
        source_currency=source_currency,
        timestamp=timestamp,
        open=50.0,
        high=51.0,
        low=49.0,
        close=50.5,
        resolution=60,
        snapshot_time=timestamp,
        ingested_at=timestamp,
        run_id="run",
        raw_payload_hash="h",
    )


def test_volatility_index_cli_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify volatility-index CLI fetches and writes normalized rows."""

    monkeypatch.setattr(
        bronze,
        "fetch_volatility_index_candles",
        lambda currency, **kwargs: ([[1, 2, 3, 4, 5]], currency),
    )
    monkeypatch.setattr(
        bronze,
        "normalize_volatility_index_candles",
        lambda candles, **kwargs: ([_sample_row(str(kwargs["currency"]), str(kwargs["source_currency"]))], []),
    )
    monkeypatch.setattr(bronze, "save_volatility_index_snapshot_parquet_lake", lambda **kwargs: ["/tmp/vol.parquet"])
    monkeypatch.setattr(
        bronze,
        "volatility_index_snapshot_time_floor_minute",
        lambda: datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
    )
    monkeypatch.setattr("sys.argv", ["main.py", VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND, "--symbols", "BTC"])

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["command"] == VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "volatility_index_snapshot_1m"
    assert output["rows_written"] == 1
    assert output["output_files"] == ["/tmp/vol.parquet"]
