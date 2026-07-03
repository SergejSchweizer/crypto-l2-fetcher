"""Tests for futures summary Bronze CLI command behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND
from ingestion.config import Config
from ingestion.futures_summary import FuturesSummarySnapshotRow


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config: Config = {
        "http": {"timeout_s": 8, "max_retries": 2, "retry_backoff_s": 1},
        "runtime": {"log_dir": str(tmp_path), "log_rotation_days": 7, "log_backup_count": 0},
        "ingestion": {
            "json_output": True,
            "futures_summary": {
                "currencies": ["BTC", "ETH", "SOL"],
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_book_summary_by_currency",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _sample_row(currency: str, source_currency: str) -> FuturesSummarySnapshotRow:
    timestamp = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return FuturesSummarySnapshotRow(
        schema_version="v1",
        dataset_type="futures_summary_snapshot_1m",
        exchange="deribit",
        source="rest_get_book_summary_by_currency",
        currency=currency,
        requested_currency=currency,
        source_currency=source_currency,
        instrument_name=f"{currency}-PERPETUAL",
        instrument_type="perp",
        snapshot_time=timestamp,
        exchange_creation_time=None,
        ingested_at=timestamp,
        run_id="run",
        bid_price=1.0,
        ask_price=1.1,
        mid_price=1.05,
        mark_price=1.04,
        last=1.03,
        open_interest=10.0,
        volume=20.0,
        volume_usd=1000.0,
        high=None,
        low=None,
        price_change=None,
        underlying_price=1.0,
        estimated_delivery_price=1.0,
        interest_rate=None,
        raw_payload_hash="h",
    )


def test_futures_summary_cli_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """Verify futures summary CLI fetches and writes normalized rows."""

    monkeypatch.setattr(
        bronze,
        "fetch_futures_book_summary_rows",
        lambda currency: ([{"instrument_name": currency}], currency),
    )
    monkeypatch.setattr(
        bronze,
        "normalize_futures_summary_rows",
        lambda rows, **kwargs: ([_sample_row(str(kwargs["requested_currency"]), str(kwargs["source_currency"]))], []),
    )
    monkeypatch.setattr(bronze, "save_futures_summary_snapshot_parquet_lake", lambda **kwargs: ["/tmp/futures.parquet"])
    monkeypatch.setattr("sys.argv", ["main.py", FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND, "--symbols", "BTC"])

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["command"] == FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "futures_summary_snapshot_1m"
    assert output["rows_written"] == 1
    assert output["output_files"] == ["/tmp/futures.parquet"]
