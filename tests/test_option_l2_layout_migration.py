"""Tests for option L2 Bronze layout migration."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.migrate_option_l2_layout import migrate_option_l2_layout


def test_migrate_option_l2_layout_rewrites_legacy_partition_and_record(tmp_path: Path) -> None:
    """Verify legacy option L2 files move to symbol/source partitions with canonical columns."""

    source_file = (
        tmp_path
        / "dataset_type=options_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=option"
        / "currency=BTC"
        / "instrument_name=BTC-30JUN26-120000-C"
        / "depth=20"
        / "source=rest_get_order_book"
        / "year=2026"
        / "month=06"
        / "date=12"
        / "hour=03"
        / "data.parquet"
    )
    source_file.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([_legacy_option_l2_row(datetime(2026, 6, 12, 3, 15, tzinfo=UTC))]),
        source_file,
    )  # type: ignore[no-untyped-call]

    dry_run_summary = migrate_option_l2_layout(tmp_path)
    summary = migrate_option_l2_layout(tmp_path, dry_run=False)

    target_file = (
        tmp_path
        / "dataset_type=options_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=option"
        / "symbol=BTC"
        / "depth=20"
        / "source=rest_order_book"
        / "year=2026"
        / "month=06"
        / "date=12"
        / "hour=03"
        / "data.parquet"
    )
    rows = pq.ParquetFile(target_file).read().to_pylist()  # type: ignore[no-untyped-call]

    assert dry_run_summary.dry_run is True
    assert dry_run_summary.source_files == 1
    assert dry_run_summary.target_files == 1
    assert dry_run_summary.rows == 1
    assert summary.dry_run is False
    assert summary.source_files == 1
    assert summary.target_files == 1
    assert summary.rows == 1
    assert not source_file.exists()
    assert rows[0]["symbol"] == "BTC-30JUN26-120000-C"
    assert rows[0]["event_time"] == datetime(2026, 6, 12, 3, 15, tzinfo=UTC)
    assert rows[0]["source"] == "rest_order_book"


def test_migrate_option_l2_layout_groups_instrument_partitions_by_asset(tmp_path: Path) -> None:
    """Verify old per-instrument option L2 partitions are consolidated by asset and hour."""

    first_file = _legacy_symbol_source_file(tmp_path, "BTC-30JUN26-120000-C")
    second_file = _legacy_symbol_source_file(tmp_path, "BTC-30JUN26-90000-P")
    first_file.parent.mkdir(parents=True)
    second_file.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([_legacy_option_l2_row(datetime(2026, 6, 12, 3, 15, tzinfo=UTC))]),
        first_file,
    )  # type: ignore[no-untyped-call]
    pq.write_table(
        pa.Table.from_pylist(
            [
                _legacy_option_l2_row(
                    datetime(2026, 6, 12, 3, 16, tzinfo=UTC),
                    instrument_name="BTC-30JUN26-90000-P",
                )
            ]
        ),
        second_file,
    )  # type: ignore[no-untyped-call]

    summary = migrate_option_l2_layout(tmp_path, dry_run=False)

    target_file = (
        tmp_path
        / "dataset_type=options_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=option"
        / "symbol=BTC"
        / "depth=20"
        / "source=rest_order_book"
        / "year=2026"
        / "month=06"
        / "date=12"
        / "hour=03"
        / "data.parquet"
    )
    rows = pq.ParquetFile(target_file).read().to_pylist()  # type: ignore[no-untyped-call]

    assert summary.source_files == 2
    assert summary.target_files == 1
    assert len(rows) == 2
    assert {row["symbol"] for row in rows} == {"BTC-30JUN26-120000-C", "BTC-30JUN26-90000-P"}


def _legacy_symbol_source_file(tmp_path: Path, instrument_name: str) -> Path:
    return (
        tmp_path
        / "dataset_type=options_l2_snapshot_1m"
        / "exchange=deribit"
        / "instrument_type=option"
        / f"symbol={instrument_name}"
        / "depth=20"
        / "source=rest_order_book"
        / "year=2026"
        / "month=06"
        / "date=12"
        / "hour=03"
        / "data.parquet"
    )


def _legacy_option_l2_row(
    exchange_timestamp: datetime,
    *,
    instrument_name: str = "BTC-30JUN26-120000-C",
) -> dict[str, object]:
    return {
        "schema_version": "v1",
        "dataset_type": "options_l2_snapshot_1m",
        "exchange": "deribit",
        "source": "rest_get_order_book",
        "currency": "BTC",
        "instrument_name": instrument_name,
        "instrument_type": "option",
        "snapshot_time": exchange_timestamp,
        "exchange_timestamp": exchange_timestamp,
        "ingested_at": exchange_timestamp,
        "run_id": "run",
        "depth": 20,
        "fetch_duration_s": 0.1,
        "state": "open",
        "bids": [],
        "asks": [],
        "bid_levels": 0,
        "ask_levels": 0,
        "best_bid_price": None,
        "best_ask_price": None,
        "best_bid_amount": None,
        "best_ask_amount": None,
        "raw_payload_hash": "hash",
    }
