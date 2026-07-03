"""Tests for recent trades Bronze CLI command behavior."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from api import cli
from api.commands import bronze
from api.constants import RECENT_TRADES_BRONZE_BUILDER_COMMAND
from ingestion.config import Config
from ingestion.recent_trades import RecentTradeSnapshotRow
from sources.deribit_trades import TradesCurrencyRequest


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
            "recent_trades": {
                "currencies": ["BTC", "ETH", "SOL"],
                "kinds": ["option", "future"],
                "count": 1000,
                "lookback_seconds": 90,
                "sorting": "asc",
                "save_parquet_lake": True,
                "lake_root": "lake/bronze",
                "source": "rest_get_last_trades_by_currency",
                "schema_version": "v1",
                "json_output": True,
            },
        },
    }
    monkeypatch.setattr(cli, "load_config", lambda: config)


def _sample_row(requested_currency: str, source_currency: str, kind: str) -> RecentTradeSnapshotRow:
    timestamp = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return RecentTradeSnapshotRow(
        schema_version="v1",
        dataset_type="recent_trade_snapshot_1m",
        exchange="deribit",
        source="rest_get_last_trades_by_currency",
        requested_currency=requested_currency,
        source_currency=source_currency,
        currency=requested_currency,
        instrument_name=f"{requested_currency}-PERPETUAL",
        instrument_type="perp" if kind == "future" else "option",
        kind=kind,
        trade_id=f"{requested_currency}-{kind}-1",
        trade_sequence=1,
        exchange_timestamp=timestamp,
        snapshot_time=timestamp,
        ingested_at=timestamp,
        run_id="run",
        price=1.0,
        amount=2.0,
        direction="buy",
        tick_direction=0,
        mark_price=1.0,
        index_price=1.0,
        iv=50.0 if kind == "option" else None,
        liquidation=None,
        block_trade_id=None,
        signed_amount=2.0,
        notional=2.0,
        raw_payload_hash="h",
    )


def test_recent_trades_cli_fetches_all_requested_currency_kind_scopes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify the CLI loops sequentially over BTC/ETH/SOL and option/future kinds."""

    seen_requests: list[tuple[str, str, str]] = []

    def fake_fetch(
        requests: list[TradesCurrencyRequest],
        *,
        count: int,
        start_timestamp: int | None,
        sorting: str,
    ) -> tuple[dict[str, list[dict[str, object]]], dict[str, str]]:
        assert count == 5
        assert start_timestamp == 1_779_606_810_000
        assert sorting == "asc"
        for request in requests:
            seen_requests.append((request.requested_currency, request.source_currency, request.kind))
        return ({f"{request.requested_currency}:{request.kind}": [{"trade_id": "1"}] for request in requests}, {})

    def fake_normalize(
        rows: list[dict[str, object]],
        **kwargs: object,
    ) -> tuple[list[RecentTradeSnapshotRow], list[str]]:
        if not rows:
            return [], []
        return (
            [
                _sample_row(
                    requested_currency=str(kwargs["requested_currency"]),
                    source_currency=str(kwargs["source_currency"]),
                    kind=str(kwargs["kind"]),
                )
            ],
            [],
        )

    monkeypatch.setattr(bronze, "_fetch_recent_trade_rows_for_requests", fake_fetch)
    monkeypatch.setattr(bronze, "normalize_recent_trade_rows", fake_normalize)
    monkeypatch.setattr(bronze, "save_recent_trade_snapshot_parquet_lake", lambda **kwargs: ["/tmp/trades.parquet"])
    monkeypatch.setattr(
        bronze,
        "recent_trade_snapshot_time_floor_minute",
        lambda: datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            RECENT_TRADES_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "ETH",
            "SOL",
            "--kinds",
            "option",
            "future",
            "--count",
            "5",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert seen_requests == [
        ("BTC", "BTC", "future"),
        ("BTC", "BTC", "option"),
        ("ETH", "ETH", "future"),
        ("ETH", "ETH", "option"),
        ("SOL", "USDC", "future"),
        ("SOL", "USDC", "option"),
    ]
    assert output["command"] == RECENT_TRADES_BRONZE_BUILDER_COMMAND
    assert output["dataset_type"] == "recent_trade_snapshot_1m"
    assert output["rows_written"] == 6
    assert output["scope_results"]["SOL:future"]["source_currency"] == "USDC"
    assert output["output_files"] == ["/tmp/trades.parquet"]


def test_recent_trades_cli_reports_partial_scope_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify one failed scope does not block successful trade writes."""

    def fake_fetch(
        requests: list[TradesCurrencyRequest],
        *,
        count: int,
        start_timestamp: int | None,
        sorting: str,
    ) -> tuple[dict[str, list[dict[str, object]]], dict[str, str]]:
        _ = (count, start_timestamp, sorting)
        return ({"BTC:future": [{"trade_id": "1"}]}, {"ETH:future": "upstream timeout"})

    monkeypatch.setattr(bronze, "_fetch_recent_trade_rows_for_requests", fake_fetch)
    monkeypatch.setattr(
        bronze,
        "normalize_recent_trade_rows",
        lambda rows, **kwargs: ([_sample_row("BTC", "BTC", "future")] if rows else [], []),
    )
    monkeypatch.setattr(bronze, "save_recent_trade_snapshot_parquet_lake", lambda **kwargs: ["/tmp/trades.parquet"])
    monkeypatch.setattr(
        "sys.argv",
        [
            "main.py",
            RECENT_TRADES_BRONZE_BUILDER_COMMAND,
            "--symbols",
            "BTC",
            "ETH",
            "--kinds",
            "future",
        ],
    )

    cli.main()
    output = json.loads(capsys.readouterr().out)

    assert output["scope_results"]["ETH:future"]["status"] == "error"
    assert output["scope_results"]["BTC:future"]["rows"] == 1
    assert output["rows_written"] == 1
    assert output["errors"] == ["upstream timeout"]
