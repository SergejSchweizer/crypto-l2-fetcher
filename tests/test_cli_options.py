"""Tests for options bronze CLI command behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import OPTIONS_BRONZE_BUILDER_COMMAND
from ingestion.config import Config
from ingestion.options import OptionTickerSnapshotRow

FetchResult = tuple[dict[str, list[dict[str, object]]], dict[str, str], dict[str, str]]


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
            "options": {
                "enabled": True,
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


def _sample_option_row(currency: str, source_currency: str, instrument_name: str) -> OptionTickerSnapshotRow:
    return OptionTickerSnapshotRow(
        exchange="deribit",
        dataset_type="options_ticker_snapshot_1m",
        source="rest_get_book_summary_by_currency",
        currency=currency,
        requested_currency=currency,
        source_currency=source_currency,
        instrument_name=instrument_name,
        base_currency=currency,
        quote_currency=currency,
        instrument_type="option",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        exchange_creation_time=datetime(2026, 5, 24, 7, 14, 59, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        run_id="run",
        bid_price=None,
        ask_price=None,
        mid_price=None,
        mark_price=0.1,
        mark_iv=None,
        underlying_price=100.0,
        underlying_index="IDX",
        interest_rate=None,
        open_interest=1.0,
        volume=0.0,
        volume_usd=0.0,
        high=None,
        low=None,
        last=None,
        price_change=None,
        raw_payload_hash="h",
        schema_version="v1",
    )


def test_cli_options_bronze_builder_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """CLI should render concise JSON summary for options bronze run."""

    def fake_fetch(currencies: list[str]) -> FetchResult:
        _ = currencies
        return (
            {"BTC": [{"instrument_name": "BTC-30JUN26-120000-C"}], "ETH": [], "SOL": []},
            {"BTC": "BTC", "ETH": "ETH", "SOL": "USDC"},
            {},
        )

    monkeypatch.setattr(
        bronze,
        "_fetch_options_rows_for_currencies",
        fake_fetch,
    )
    monkeypatch.setattr(
        bronze,
        "normalize_options_ticker_rows",
        lambda rows, **kwargs: (
            [
                _sample_option_row(
                    kwargs["requested_currency"],
                    kwargs["source_currency"],
                    "X",
                )
            ]
            if rows
            else [],
            [],
        ),
    )
    monkeypatch.setattr(bronze, "save_options_ticker_snapshot_parquet_lake", lambda **kwargs: ["/tmp/options.parquet"])
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTIONS_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC,ETH,SOL",
            "--save-parquet-lake",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["command"] == OPTIONS_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "options_ticker_snapshot_1m"
    assert output["currency_results"]["BTC"]["status"] == "ok"
    assert output["output_files"] == ["/tmp/options.parquet"]


def test_partial_currency_failure_still_writes_successful_assets(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed currency should not block successful currency writes."""

    def fake_fetch(currencies: list[str]) -> FetchResult:
        _ = currencies
        return (
            {
                "BTC": [{"instrument_name": "BTC-30JUN26-120000-C"}],
                "SOL": [{"instrument_name": "SOL_USDC-30JUN26-250-C"}],
            },
            {"BTC": "BTC", "ETH": "ETH", "SOL": "USDC"},
            {"ETH": "upstream timeout"},
        )

    monkeypatch.setattr(
        bronze,
        "_fetch_options_rows_for_currencies",
        fake_fetch,
    )
    monkeypatch.setattr(
        bronze,
        "normalize_options_ticker_rows",
        lambda rows, **kwargs: (
            [
                _sample_option_row(
                    kwargs["requested_currency"],
                    kwargs["source_currency"],
                    row["instrument_name"],
                )
                for row in rows
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        bronze,
        "save_options_ticker_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/btc.parquet", "/tmp/sol.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTIONS_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "ETH",
            "SOL",
            "--save-parquet-lake",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["currency_results"]["ETH"]["status"] == "error"
    assert output["currency_results"]["BTC"]["rows"] == 1
    assert output["rows_written"] == 2
    assert output["output_files"] == ["/tmp/btc.parquet", "/tmp/sol.parquet"]


def test_options_bronze_legacy_currencies_flag_is_still_supported(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Legacy --currencies alias should remain accepted."""

    def fake_fetch(currencies: list[str]) -> FetchResult:
        return ({currency: [] for currency in currencies}, {currency: currency for currency in currencies}, {})

    monkeypatch.setattr(bronze, "_fetch_options_rows_for_currencies", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_options_ticker_rows",
        lambda rows, **kwargs: ([], []),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTIONS_BRONZE_BUILDER_COMMAND,
            "--currencies",
            "BTC",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["requested_currencies"] == ["BTC"]
