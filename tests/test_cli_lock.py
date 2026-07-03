"""Tests for L2 CLI parsing, runtime logging, and builder dispatch."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import (
    BRONZE_BUILDER_COMMAND,
    FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND,
    INDEX_PRICE_BRONZE_BUILDER_COMMAND,
    INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND,
    LEGACY_OPTION_L2_BRONZE_BUILDER_COMMAND,
    OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
    OPTION_L2_BRONZE_BUILDER_COMMAND,
    OPTIONS_BRONZE_BUILDER_COMMAND,
    RECENT_TRADES_BRONZE_BUILDER_COMMAND,
    VALIDATE_SYMBOLS_COMMAND,
    VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND,
)
from api.logging_common import COMMAND_LOG_SCOPES, ModuleScopeFilter, log_file_stem_for_module_name
from api.runtime import configure_logging
from domain.models import OrderLevel, RawSnapshot
from ingestion.config import Config
from ingestion.l2 import L2Snapshot


@pytest.fixture(autouse=True)
def _isolate_cli_test_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep CLI tests from writing to the configured runtime log directory."""

    config = _config(log_dir=str(tmp_path / "logs"))
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _config(
    *,
    log_dir: str = "/tmp/crypto-live-loader-test-logs",
    symbols: list[str] | None = None,
    levels: int = 50,
    snapshot_count: int = 5,
    poll_interval_s: float = 10.0,
    max_runtime_s: float = 50.0,
    save_parquet_lake: bool = False,
    lake_root: str = "lake/bronze",
    json_output: bool = True,
) -> Config:
    """Build a minimal test config."""

    return {
        "http": {
            "timeout_s": 8,
            "max_retries": 2,
            "retry_backoff_s": 1,
        },
        "runtime": {
            "log_dir": log_dir,
            "log_rotation_days": 7,
            "log_backup_count": 0,
        },
        "ingestion": {
            "exchange": "deribit",
            "symbols": symbols or ["BTC", "ETH"],
            "levels": levels,
            "snapshot_count": snapshot_count,
            "poll_interval_s": poll_interval_s,
            "max_runtime_s": max_runtime_s,
            "save_parquet_lake": save_parquet_lake,
            "lake_root": lake_root,
            "json_output": json_output,
        },
    }


def test_configure_logging_rotates_weekly_and_keeps_old_logs(tmp_path: Path) -> None:
    """Verify runtime logging rotates weekly and keeps rotated logs."""

    logger = configure_logging(
        module_name="test-weekly-rotation",
        config=_config(log_dir=str(tmp_path / "logs")),
    )
    try:
        file_handler = next(handler for handler in logger.handlers if isinstance(handler, TimedRotatingFileHandler))

        assert file_handler.when == "D"
        assert file_handler.interval == 7 * 24 * 60 * 60
        assert file_handler.backupCount == 0
    finally:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


def test_configure_logging_uses_module_specific_logfile(tmp_path: Path) -> None:
    """Verify runtime logging writes to one logfile per module."""

    log_dir = tmp_path / "logs"
    config = _config(log_dir=str(tmp_path / "ignored"))
    config["logfile"] = str(log_dir / "ignored.log")

    logger = configure_logging(
        module_name="test-shared-logfile",
        config=config,
    )
    try:
        file_handler = next(handler for handler in logger.handlers if isinstance(handler, TimedRotatingFileHandler))
        assert Path(file_handler.baseFilename) == log_dir / "test-shared-logfile.log"
    finally:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


def test_configure_logging_uses_dataset_logfile_for_commands(tmp_path: Path) -> None:
    """Verify command loggers write to dataset-named logfiles."""

    log_dir = tmp_path / "logs"

    logger = configure_logging(
        module_name=BRONZE_BUILDER_COMMAND,
        config=_config(log_dir=str(log_dir)),
    )
    try:
        file_handler = next(handler for handler in logger.handlers if isinstance(handler, TimedRotatingFileHandler))
        assert Path(file_handler.baseFilename) == log_dir / "perps_l2_snapshot_1m.log"
    finally:
        for handler in list(logger.handlers):
            handler.close()
            logger.removeHandler(handler)


def test_log_module_name_for_future_instrument_metadata_uses_dataset() -> None:
    """Verify future metadata writes to the future metadata dataset log."""

    args = cli.build_parser().parse_args([INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND, "--kind", "future"])

    assert bronze.log_module_name_for_args(args) == "future_instrument_metadata_snapshot_daily"


def test_l2_parser_defaults_can_come_from_config() -> None:
    """Verify bronze-builder defaults are configurable through config."""

    args = cli.build_parser(
        _config(
            symbols=["BTC", "ETH"],
            levels=25,
            snapshot_count=5,
            poll_interval_s=10,
            max_runtime_s=50,
            save_parquet_lake=True,
            lake_root="custom/bronze",
            json_output=False,
        )
    ).parse_args([BRONZE_BUILDER_COMMAND])

    assert args.symbols == ["BTC", "ETH"]
    assert args.levels == 25
    assert args.snapshot_count == 5
    assert args.poll_interval_s == 10.0
    assert args.max_runtime_s == 50.0
    assert args.save_parquet_lake is True
    assert args.lake_root == "custom/bronze"
    assert args.json_output is False


def test_l2_parser_defaults_to_five_snapshots_per_run() -> None:
    """Verify the default L2 cadence leaves room for one-minute cron runs."""

    args = cli.build_parser().parse_args([BRONZE_BUILDER_COMMAND])

    assert args.snapshot_count == 5
    assert args.poll_interval_s == 10.0
    assert args.max_runtime_s == 50.0


def test_job_event_logs_use_uniform_key_value_shape(caplog: pytest.LogCaptureFixture) -> None:
    """Verify CLI job logs use one event envelope and stable key ordering."""

    logger = logging.getLogger("test_uniform_job_event")

    with caplog.at_level(logging.INFO, logger=logger.name):
        bronze._log_job_event(
            logger,
            logging.INFO,
            "example-builder",
            "run_summary",
            status="complete",
            elapsed_s=1.23456,
            symbols=["BTC", "ETH"],
            files=2,
        )

    assert caplog.messages == [
        "job_event command=example-builder event=run_summary elapsed_s=1.235 files=2 status=complete symbols=BTC,ETH"
    ]


def test_dataset_event_logs_include_dataset_envelope(caplog: pytest.LogCaptureFixture) -> None:
    """Verify dataset logs consistently include exchange and dataset_type fields."""

    logger = logging.getLogger("test_dataset_event_logs_include_dataset_envelope")

    with caplog.at_level(logging.INFO, logger=logger.name):
        bronze._log_dataset_event(
            logger,
            logging.INFO,
            "example-builder",
            "run_summary",
            dataset_type="example_snapshot",
            rows_written=3,
            status="complete",
        )

    assert caplog.messages == [
        "job_event command=example-builder event=run_summary "
        "dataset_type=example_snapshot exchange=deribit rows_written=3 status=complete"
    ]


def test_dataset_debug_event_is_expressive_and_debug_only(caplog: pytest.LogCaptureFixture) -> None:
    """Verify debug dataset events stay quiet at INFO and expose diagnostic fields at DEBUG."""

    logger = logging.getLogger("test_dataset_debug_event_is_expressive_and_debug_only")
    logger.setLevel(logging.INFO)

    with caplog.at_level(logging.INFO, logger=logger.name):
        bronze._log_dataset_debug_event(
            logger,
            "example-builder",
            "run_start",
            dataset_type="example_snapshot",
            lake_root="lake/bronze",
            save_parquet_lake=True,
            source="rest_example",
            symbols=["BTC", "ETH"],
        )
    assert caplog.messages == []

    logger.setLevel(logging.DEBUG)
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        bronze._log_dataset_debug_event(
            logger,
            "example-builder",
            "run_start",
            dataset_type="example_snapshot",
            lake_root="lake/bronze",
            save_parquet_lake=True,
            source="rest_example",
            symbols=["BTC", "ETH"],
        )

    assert caplog.messages == [
        "job_event command=example-builder event=run_start "
        "dataset_type=example_snapshot exchange=deribit lake_root=lake/bronze save_parquet_lake=true "
        "source=rest_example symbols=BTC,ETH"
    ]


def test_module_scope_filter_maps_every_dataset_command() -> None:
    """Verify every dataset command writes with its own module scope."""

    scope_filter = ModuleScopeFilter()

    for command, expected_scope in COMMAND_LOG_SCOPES.items():
        record = logging.LogRecord(
            name=f"crypto_live_loader.{command}",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="message",
            args=(),
            exc_info=None,
        )
        assert scope_filter.filter(record)
        assert record.__dict__["module_scope"] == expected_scope
        assert log_file_stem_for_module_name(command) == expected_scope


def test_cron_layer_commands_accept_debug_flag() -> None:
    """Verify every scheduled Bronze command accepts --debug."""

    parser = cli.build_parser(_config())
    commands = [
        [BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "BTC", "ETH", "SOL"],
        [OPTIONS_BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "BTC", "ETH", "SOL"],
        [FUTURES_SUMMARY_BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "BTC", "ETH", "SOL"],
        [OPTION_L2_BRONZE_BUILDER_COMMAND, "--debug", "--instruments", "BTC-26JUN26-120000-C"],
        [LEGACY_OPTION_L2_BRONZE_BUILDER_COMMAND, "--debug", "--instruments", "BTC-26JUN26-120000-C"],
        [
            OPTION_INSTRUMENT_TICKER_BRONZE_BUILDER_COMMAND,
            "--debug",
            "--instruments",
            "BTC-26JUN26-120000-C",
        ],
        [INDEX_PRICE_BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "btc_usd", "eth_usd", "sol_usdc"],
        [VOLATILITY_INDEX_BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "BTC", "ETH", "SOL"],
        [RECENT_TRADES_BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "BTC", "ETH", "SOL"],
        [INSTRUMENT_METADATA_BRONZE_BUILDER_COMMAND, "--debug", "--symbols", "BTC", "ETH", "SOL", "--kind", "option"],
    ]

    parsed = [parser.parse_args(command) for command in commands]

    assert all(bool(args.debug) for args in parsed)


def test_l2_parser_cli_can_override_boolean_config_defaults() -> None:
    """Verify paired boolean flags can override config defaults."""

    args = cli.build_parser(_config(save_parquet_lake=True, json_output=False)).parse_args(
        [BRONZE_BUILDER_COMMAND, "--no-save-parquet-lake", "--json-output"]
    )

    assert args.save_parquet_lake is False
    assert args.json_output is True


def test_l2_symbols_accept_comma_delimited_cli_values() -> None:
    """Verify the CLI symbol normalizer accepts comma-delimited values."""

    args = cli.build_parser().parse_args([BRONZE_BUILDER_COMMAND, "--symbols", "btc,eth", "SOL"])

    assert bronze._normalize_cli_symbols(args.symbols) == ["BTC", "ETH", "SOL"]


def test_cli_normalizers_reject_empty_values() -> None:
    """Verify CLI list normalizers fail clearly on empty inputs."""

    with pytest.raises(ValueError, match="at least one symbol"):
        bronze._normalize_cli_symbols(["", ","])
    with pytest.raises(ValueError, match="at least one currency"):
        bronze._normalize_cli_currencies(["", ","])
    with pytest.raises(ValueError, match="at least one index symbol"):
        bronze._normalize_cli_index_symbols(["", ","])


def test_debug_logging_updates_logger_and_handlers() -> None:
    """Verify debug mode lifts both logger and handler levels."""

    logger = logging.getLogger("test_debug_logging_updates_logger_and_handlers")
    handler = logging.StreamHandler()
    logger.addHandler(handler)
    try:
        logger.setLevel(logging.INFO)
        handler.setLevel(logging.INFO)
        bronze._enable_debug_logging(cli.build_parser().parse_args([BRONZE_BUILDER_COMMAND, "--debug"]), logger)

        assert logger.level == logging.DEBUG
        assert handler.level == logging.DEBUG
    finally:
        logger.removeHandler(handler)
        handler.close()


def test_persist_bronze_snapshots_reports_parquet_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Bronze parquet failures are surfaced in command output."""

    def raise_parquet_error(**_: object) -> list[str]:
        raise RuntimeError("disk full")

    monkeypatch.setattr(bronze, "save_perps_l2_snapshot_1m_parquet_lake", raise_parquet_error)
    output: dict[str, object] = {}

    files, error = bronze._persist_bronze_snapshots(
        snapshots_by_symbol={},
        lake_root="lake/bronze",
        depth=50,
        enabled=True,
        output=output,
        logger=logging.getLogger("test_persist_bronze_snapshots_reports_parquet_errors"),
    )

    assert files == []
    assert error == "disk full"
    assert output["_parquet_error"] == "disk full"


def test_warn_for_long_poll_schedule_logs_cron_overlap(caplog: pytest.LogCaptureFixture) -> None:
    """Verify long polling schedules emit an operational warning."""

    logger = logging.getLogger("test_l2_schedule_warning")

    with caplog.at_level("WARNING", logger=logger.name):
        bronze._warn_for_long_poll_schedule(
            logger=logger,
            snapshot_count=7,
            poll_interval_s=10,
            max_runtime_s=0,
        )

    assert "cron runs may overlap" in caplog.text


def test_warn_for_long_poll_schedule_logs_runtime_budget(caplog: pytest.LogCaptureFixture) -> None:
    """Verify polling schedules warn when estimated sleep exceeds runtime budget."""

    logger = logging.getLogger("test_l2_runtime_budget_warning")

    with caplog.at_level("WARNING", logger=logger.name):
        bronze._warn_for_long_poll_schedule(
            logger=logger,
            snapshot_count=5,
            poll_interval_s=20,
            max_runtime_s=50,
        )

    assert "may exceed max runtime" in caplog.text


def test_validate_symbols_reports_valid_books(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify symbol validation returns normalized symbols and book status."""

    class Adapter:
        @property
        def source_name(self) -> str:
            return "deribit"

        def normalize_symbol(self, symbol: str) -> str:
            assert symbol == "SOL"
            return "SOL_USDC-PERPETUAL"

        def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
            assert symbol == "SOL"
            assert depth == 1
            return RawSnapshot(
                exchange="deribit",
                symbol="SOL_USDC-PERPETUAL",
                timestamp_ms=1_700_000_000_000,
                bids=[OrderLevel(price=84.0, amount=2.0)],
                asks=[OrderLevel(price=84.1, amount=3.0)],
                mark_price=84.05,
                index_price=84.0,
                open_interest=1000.0,
                funding_8h=0.0001,
                current_funding=0.00001,
            )

    monkeypatch.setattr(bronze, "source_adapter_for_exchange", lambda exchange: Adapter())

    result = bronze._validate_symbol(exchange="deribit", symbol="SOL", depth=1)

    assert result["normalized_symbol"] == "SOL_USDC-PERPETUAL"
    assert result["valid_book"] is True
    assert result["error"] is None


def test_validate_symbols_reports_fetch_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify symbol validation returns actionable errors for failed fetches."""

    class Adapter:
        @property
        def source_name(self) -> str:
            return "deribit"

        def normalize_symbol(self, symbol: str) -> str:
            return f"{symbol}-PERPETUAL"

        def fetch_snapshot(self, symbol: str, depth: int) -> RawSnapshot:
            _ = symbol, depth
            raise RuntimeError("boom")

    monkeypatch.setattr(bronze, "source_adapter_for_exchange", lambda exchange: Adapter())

    result = bronze._validate_symbol(exchange="deribit", symbol="BTC", depth=1)

    assert result["normalized_symbol"] == "BTC-PERPETUAL"
    assert result["valid_book"] is False
    assert result["error"] == "boom"


def test_run_validate_symbols_rejects_non_positive_depth() -> None:
    """Verify validate-symbols rejects invalid depth before fetching."""

    args = cli.build_parser().parse_args([VALIDATE_SYMBOLS_COMMAND, "--levels", "0"])

    with pytest.raises(ValueError, match="levels must be positive"):
        bronze._run_validate_symbols(args=args, logger=logging.getLogger("test_run_validate_symbols_rejects_depth"))


def test_main_validate_symbols_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the validate-symbols command prints a validation summary."""

    monkeypatch.setattr(
        bronze,
        "_validate_symbol",
        lambda exchange, symbol, depth: {
            "symbol": symbol,
            "normalized_symbol": f"{symbol}-PERPETUAL",
            "valid_book": True,
            "bid_levels": depth,
            "ask_levels": depth,
            "error": None,
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            "validate-symbols",
            "--symbols",
            "BTC,SOL",
            "--levels",
            "1",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["all_valid"] is True
    assert [item["symbol"] for item in output["symbols"]] == ["BTC", "SOL"]


def test_main_loader_l2_outputs_raw_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the CLI writes raw snapshots to JSON output."""

    snapshot = L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 4, 29, 10, 0, 1, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=[(100.0, 1.0)],
        asks=[(101.0, 1.0)],
        mark_price=100.5,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )

    monkeypatch.setattr(bronze, "fetch_perps_l2_snapshot_1m_for_symbols", lambda **kwargs: {"BTC": [snapshot]})
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--snapshot-count",
            "1",
            "--poll-interval-s",
            "0",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["deribit"]["BTC"][0]["symbol"] == "BTC-PERPETUAL"
    assert output["deribit"]["BTC"][0]["timestamp"] == "2026-04-29T10:00:01+00:00"
    assert output["deribit"]["BTC"][0]["bids"] == [[100.0, 1.0]]


def test_main_loader_l2_persists_raw_snapshots_to_lake(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the parquet save flag writes raw snapshots with the requested depth."""

    snapshot = L2Snapshot(
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        timestamp=datetime(2026, 5, 5, 10, 0, 1, tzinfo=UTC),
        fetch_duration_s=0.1,
        bids=[(100.0, 1.0)],
        asks=[(101.0, 1.0)],
        mark_price=100.5,
        index_price=100.0,
        open_interest=1000.0,
        funding_8h=0.0001,
        current_funding=0.00001,
    )
    calls: list[dict[str, object]] = []

    def fake_save_perps_l2_snapshot_1m_parquet_lake(**kwargs: object) -> list[str]:
        calls.append(kwargs)
        return ["/tmp/lake/bronze/dataset_type=perps_l2_snapshot_1m/data.parquet"]

    monkeypatch.setattr(bronze, "fetch_perps_l2_snapshot_1m_for_symbols", lambda **kwargs: {"BTC": [snapshot]})
    monkeypatch.setattr(bronze, "save_perps_l2_snapshot_1m_parquet_lake", fake_save_perps_l2_snapshot_1m_parquet_lake)
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "--levels",
            "50",
            "--snapshot-count",
            "1",
            "--poll-interval-s",
            "0",
            "--save-parquet-lake",
        ],
    )

    cli.main()

    output = json.loads(capsys.readouterr().out)
    assert output["_parquet_files"] == ["/tmp/lake/bronze/dataset_type=perps_l2_snapshot_1m/data.parquet"]
    assert calls[0]["snapshots_by_symbol"] == {"BTC": [snapshot]}
    assert calls[0]["depth"] == 50
