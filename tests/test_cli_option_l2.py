"""Tests for option order-book Bronze CLI command behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from api import cli
from api.commands import bronze
from api.constants import OPTION_L2_BRONZE_BUILDER_COMMAND
from ingestion.config import Config
from ingestion.option_l2 import OptionL2SnapshotRow
from ingestion.option_ticker_universe import OPTION_L2_TARGET_DTE_DAYS


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
            "option_l2": {
                "currencies": ["BTC", "ETH", "SOL"],
                "instruments": [],
                "depth": 50,
                "max_instruments_per_run": 60,
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_order_book",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _sample_row(instrument_name: str, depth: int = 50) -> OptionL2SnapshotRow:
    return OptionL2SnapshotRow(
        schema_version="v1",
        dataset_type="options_l2_snapshot_1m",
        exchange="deribit",
        source="rest_order_book",
        currency=instrument_name.split("-", 1)[0].removesuffix("_USDC"),
        instrument_name=instrument_name,
        instrument_type="option",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        exchange_timestamp=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        run_id="run",
        depth=depth,
        fetch_duration_s=0.123,
        state="open",
        bids=[{"price": 0.1, "amount": 4.0}],
        asks=[{"price": 0.11, "amount": 5.0}],
        bid_levels=1,
        ask_levels=1,
        best_bid_price=0.1,
        best_ask_price=0.11,
        best_bid_amount=4.0,
        best_ask_amount=5.0,
        mark_price=None,
        index_price=None,
        underlying_price=76839.1,
        underlying_index="BTC-30JUN26",
        interest_rate=0.03,
        bid_iv=54.1,
        ask_iv=55.2,
        mark_iv=54.8,
        open_interest=None,
        last_price=None,
        settlement_price=None,
        min_price=None,
        max_price=None,
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
    )


def test_option_l2_cli_uses_explicit_instruments(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify explicit instruments skip discovery and write normalized order books."""

    def fake_fetch(
        instruments: list[str],
        *,
        depth: int,
    ) -> tuple[dict[str, dict[str, object]], dict[str, float], dict[str, str]]:
        assert instruments == ["BTC-30JUN26-120000-C"]
        assert depth == 50
        return (
            {"BTC-30JUN26-120000-C": {"instrument_name": "BTC-30JUN26-120000-C"}},
            {"BTC-30JUN26-120000-C": 0.123},
            {},
        )

    def fake_normalize(
        rows: dict[str, dict[str, object]],
        **kwargs: object,
    ) -> tuple[list[OptionL2SnapshotRow], list[str]]:
        _ = kwargs
        return [_sample_row(next(iter(rows)))], []

    monkeypatch.setattr(bronze, "_fetch_option_l2_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(bronze, "normalize_option_l2_snapshot_rows", fake_normalize)
    monkeypatch.setattr(
        bronze,
        "save_option_l2_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/option_l2.parquet"],
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTION_L2_BRONZE_BUILDER_COMMAND,
            "--instruments",
            "btc-30jun26-120000-c",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["command"] == OPTION_L2_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "options_l2_snapshot_1m"
    assert output["depth"] == 50
    assert output["instruments_requested"] == 1
    assert output["instruments_discovered"] == 1
    assert output["rows_written"] == 1
    assert output["output_files"] == ["/tmp/option_l2.parquet"]


def test_option_l2_cli_fetches_prediction_universe_for_each_currency(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify every requested currency gets a bounded selected option L2 universe."""

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

    def fake_fetch(
        instruments: list[str],
        *,
        depth: int,
    ) -> tuple[dict[str, dict[str, object]], dict[str, float], dict[str, str]]:
        assert depth == 5
        fetched_batches.append(instruments)
        return (
            {instrument: {"instrument_name": instrument} for instrument in instruments},
            {instrument: 0.01 for instrument in instruments},
            {},
        )

    monkeypatch.setattr(bronze, "_fetch_option_l2_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_option_l2_snapshot_rows",
        lambda rows, **kwargs: ([_sample_row(instrument, depth=kwargs["depth"]) for instrument in rows], []),
    )
    monkeypatch.setattr(
        bronze,
        "save_option_l2_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/option_l2.parquet"],
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTION_L2_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "ETH",
            "SOL",
            "--max-instruments-per-run",
            "2",
            "--depth",
            "5",
        ],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)

    expected_batch = [
        "BTC-30JUN26-100000-C",
        "BTC-30JUN26-110000-C",
        "ETH-30JUN26-4000-C",
        "ETH-30JUN26-4200-C",
        "SOL_USDC-30JUN26-200-C",
        "SOL_USDC-30JUN26-220-C",
    ]
    assert fetched_batches == [
        expected_batch,
        expected_batch,
        expected_batch,
        expected_batch,
        expected_batch,
    ]
    assert output["snapshot_count"] == 5
    assert output["snapshots_requested"] == 30
    assert output["rows_written"] == 30
    assert output["depth"] == 5
    assert output["instruments_discovered"] == 7
    assert output["instruments_requested"] == 6
    assert output["currency_results"]["BTC"]["instruments_requested"] == 2
    assert output["currency_results"]["ETH"]["instruments_requested"] == 2
    assert output["currency_results"]["SOL"]["instruments_requested"] == 2


def test_option_l2_cli_uses_option_l2_target_dte_days(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify option L2 discovery uses the configured target DTE maturity buckets."""

    observed_target_tenors: list[tuple[int, ...] | None] = []
    observed_exact_tenor: list[bool] = []
    observed_fallback: list[bool] = []

    monkeypatch.setattr(
        bronze,
        "fetch_option_book_summary_rows",
        lambda request: [{"instrument_name": f"{request.requested_currency}-13JUN26-100000-C"}],
    )

    def fake_select(
        rows: list[dict[str, object]],
        *,
        max_instruments: int,
        target_tenors_days: tuple[int, ...] | None = None,
        require_exact_target_tenor: bool = False,
        allow_liquidity_fallback: bool = True,
    ) -> list[str]:
        _ = (rows, max_instruments)
        observed_target_tenors.append(target_tenors_days)
        observed_exact_tenor.append(require_exact_target_tenor)
        observed_fallback.append(allow_liquidity_fallback)
        return ["BTC-13JUN26-100000-C"]

    monkeypatch.setattr(bronze, "select_option_ticker_prediction_universe", fake_select)

    def fake_fetch(
        instruments: list[str],
        *,
        depth: int,
    ) -> tuple[dict[str, dict[str, object]], dict[str, float], dict[str, str]]:
        _ = depth
        return ({instrument: {"instrument_name": instrument} for instrument in instruments}, {}, {})

    monkeypatch.setattr(bronze, "_fetch_option_l2_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_option_l2_snapshot_rows",
        lambda rows, **kwargs: ([_sample_row(instrument, depth=cast(int, kwargs["depth"])) for instrument in rows], []),
    )
    monkeypatch.setattr(bronze, "save_option_l2_snapshot_parquet_lake", lambda **kwargs: [])
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTION_L2_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--snapshot-count",
            "1",
        ],
    )

    cli.main()
    _ = capsys.readouterr()

    assert observed_target_tenors == [OPTION_L2_TARGET_DTE_DAYS]
    assert observed_exact_tenor == [True]
    assert observed_fallback == [False]


def test_option_l2_cli_can_run_single_polling_tick(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify option L2 can still run one explicit tick for low-volume captures."""

    selected_by_currency = {
        "BTC": ["BTC-30JUN26-100000-C", "BTC-30JUN26-110000-C"],
    }
    fetched_batches: list[list[str]] = []

    monkeypatch.setattr(
        bronze,
        "_select_option_ticker_prediction_universe_by_currency",
        lambda **kwargs: (selected_by_currency, []),
    )

    def fake_fetch(
        instruments: list[str],
        *,
        depth: int,
    ) -> tuple[dict[str, dict[str, object]], dict[str, float], dict[str, str]]:
        assert depth == 5
        fetched_batches.append(instruments)
        return (
            {instrument: {"instrument_name": instrument} for instrument in instruments},
            {instrument: 0.01 for instrument in instruments},
            {},
        )

    monkeypatch.setattr(bronze, "_fetch_option_l2_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_option_l2_snapshot_rows",
        lambda rows, **kwargs: ([_sample_row(instrument, depth=kwargs["depth"]) for instrument in rows], []),
    )
    monkeypatch.setattr(
        bronze,
        "save_option_l2_snapshot_parquet_lake",
        lambda **kwargs: ["/tmp/option_l2.parquet"],
    )

    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTION_L2_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--max-instruments-per-run",
            "2",
            "--depth",
            "5",
            "--snapshot-count",
            "1",
        ],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert fetched_batches == [["BTC-30JUN26-100000-C", "BTC-30JUN26-110000-C"]]
    assert output["snapshot_count"] == 1
    assert output["snapshots_requested"] == 2
    assert output["rows_written"] == 2


def test_option_l2_cli_collects_bounded_polling_ticks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify option L2 can collect repeated snapshots like the perpetual L2 builder."""

    selected_by_currency = {"BTC": ["BTC-30JUN26-100000-C"]}
    tick = 0

    monkeypatch.setattr(
        bronze,
        "_select_option_ticker_prediction_universe_by_currency",
        lambda **kwargs: (selected_by_currency, []),
    )

    def fake_fetch(
        instruments: list[str],
        *,
        depth: int,
    ) -> tuple[dict[str, dict[str, object]], dict[str, float], dict[str, str]]:
        nonlocal tick
        tick += 1
        assert instruments == ["BTC-30JUN26-100000-C"]
        assert depth == 5
        return (
            {"BTC-30JUN26-100000-C": {"instrument_name": "BTC-30JUN26-100000-C", "tick": tick}},
            {"BTC-30JUN26-100000-C": 0.01},
            {},
        )

    def fake_normalize(
        rows: dict[str, dict[str, object]],
        **kwargs: object,
    ) -> tuple[list[OptionL2SnapshotRow], list[str]]:
        depth = cast(int, kwargs["depth"])
        return [_sample_row(str(row["instrument_name"]), depth=depth) for row in rows.values()], []

    monkeypatch.setattr(bronze, "_fetch_option_l2_rows_for_instruments", fake_fetch)
    monkeypatch.setattr(bronze, "normalize_option_l2_snapshot_rows", fake_normalize)
    monkeypatch.setattr(bronze, "save_option_l2_snapshot_parquet_lake", lambda **kwargs: [])
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            OPTION_L2_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--depth",
            "5",
            "--snapshot-count",
            "3",
            "--poll-interval-s",
            "0",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert tick == 3
    assert output["snapshot_count"] == 3
    assert output["snapshots_requested"] == 3
    assert output["rows_written"] == 3
