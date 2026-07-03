"""Tests for per-instrument option ticker Bronze CLI command behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND
from ingestion.config import Config
from ingestion.option_instrument_ticker import OptionInstrumentTickerSnapshotRow
from sources.deribit_options import OptionsCurrencyRequest


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
            "option_instrument_ticker": {
                "currencies": ["BTC", "ETH", "SOL"],
                "instruments": [],
                "max_instruments_per_run": 60,
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_ticker",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _sample_row(instrument_name: str) -> OptionInstrumentTickerSnapshotRow:
    return OptionInstrumentTickerSnapshotRow(
        exchange="deribit",
        dataset_type="option_instrument_ticker_snapshot_1m",
        source="rest_ticker",
        currency=instrument_name.split("-", 1)[0].removesuffix("_USDC"),
        instrument_name=instrument_name,
        instrument_type="option",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        exchange_creation_time=None,
        exchange_timestamp=None,
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        run_id="run",
        state=None,
        bid_price=None,
        ask_price=None,
        best_bid_price=None,
        best_ask_price=None,
        best_bid_amount=None,
        best_ask_amount=None,
        bid_iv=54.1,
        ask_iv=55.2,
        mark_iv=54.8,
        mark_price=None,
        last_price=None,
        underlying_price=76839.1,
        underlying_index="BTC-30JUN26",
        index_price=None,
        interest_rate=0.03,
        open_interest=None,
        volume=None,
        volume_usd=None,
        high=None,
        low=None,
        price_change=None,
        delta=0.42,
        gamma=None,
        theta=None,
        vega=None,
        rho=None,
        raw_payload_hash="h",
        schema_version="v1",
    )


def test_option_instrument_ticker_cli_uses_explicit_instruments(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify explicit instruments skip discovery and write normalized ticker rows."""

    def fake_fetch(instruments: list[str]) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
        assert instruments == ["BTC-30JUN26-120000-C"]
        return ({"BTC-30JUN26-120000-C": {"instrument_name": "BTC-30JUN26-120000-C"}}, {})

    monkeypatch.setattr(bronze, "_fetch_option_ticker_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_option_instrument_ticker_rows",
        lambda rows, **kwargs: ([_sample_row(next(iter(rows)))], []),
    )
    monkeypatch.setattr(
        bronze,
        "save_option_instrument_ticker_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/option_instrument.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
            "--instruments",
            "btc-30jun26-120000-c",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["command"] == OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "option_instrument_ticker_snapshot_1m"
    assert output["instruments_requested"] == 1
    assert output["instruments_discovered"] == 1
    assert output["rows_written"] == 1
    assert output["output_files"] == ["/tmp/option_instrument.parquet"]


def test_option_instrument_ticker_cli_selects_currency_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify selection maps SOL to USDC summaries and keeps SOL instruments."""

    calls: list[str] = []

    def fake_fetch_summary(request: OptionsCurrencyRequest) -> list[dict[str, object]]:
        calls.append(request.source_currency)
        return [
            {
                "instrument_name": "SOL_USDC-30SEP26-250-C",
                "underlying_price": 250.0,
                "ask_price": 0.1,
                "bid_price": 0.09,
                "open_interest": 10,
            }
        ]

    monkeypatch.setattr(bronze, "fetch_option_book_summary_rows", fake_fetch_summary)

    instruments_by_currency, errors = bronze._select_option_ticker_prediction_universe_by_currency(
        currencies=["SOL"],
        explicit_instruments=[],
        max_instruments_per_currency=2,
    )

    assert errors == []
    assert calls == ["USDC"]
    assert instruments_by_currency == {"SOL": ["SOL_USDC-30SEP26-250-C"]}


def test_option_instrument_ticker_cli_fetches_prediction_universe_for_each_currency(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify every requested currency gets a bounded selected prediction universe."""

    selected_by_currency = {
        "BTC": ["BTC-30JUN26-100000-C", "BTC-30JUN26-110000-C", "BTC-30JUN26-120000-C"],
        "ETH": ["ETH-30JUN26-4000-C", "ETH-30JUN26-4200-C"],
        "SOL": ["SOL_USDC-30JUN26-200-C", "SOL_USDC-30JUN26-220-C"],
    }
    fetched_batches: list[list[str]] = []

    monkeypatch.setattr(
        bronze,
        "_select_option_ticker_prediction_universe_by_currency",
        lambda **kwargs: (selected_by_currency, []),
    )

    def fake_fetch(instruments: list[str]) -> tuple[dict[str, dict[str, object]], dict[str, str]]:
        fetched_batches.append(instruments)
        return ({instrument: {"instrument_name": instrument} for instrument in instruments}, {})

    monkeypatch.setattr(bronze, "_fetch_option_ticker_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_option_instrument_ticker_rows",
        lambda rows, **kwargs: ([_sample_row(instrument) for instrument in rows], []),
    )
    monkeypatch.setattr(
        bronze,
        "save_option_instrument_ticker_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/option_instrument.parquet"],
    )

    argv = [
        "main.py",
        OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
        "--symbols",
        "BTC",
        "ETH",
        "SOL",
        "--max-instruments-per-run",
        "2",
    ]
    monkeypatch.setattr("sys.argv", argv)
    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert fetched_batches == [
        [
            "BTC-30JUN26-100000-C",
            "BTC-30JUN26-110000-C",
            "ETH-30JUN26-4000-C",
            "ETH-30JUN26-4200-C",
            "SOL_USDC-30JUN26-200-C",
            "SOL_USDC-30JUN26-220-C",
        ]
    ]
    assert output["instruments_discovered"] == 7
    assert output["instruments_requested"] == 6
    assert output["currency_results"]["BTC"]["instruments_requested"] == 2
    assert output["currency_results"]["ETH"]["instruments_requested"] == 2
    assert output["currency_results"]["SOL"]["instruments_requested"] == 2
