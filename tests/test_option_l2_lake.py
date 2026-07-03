"""Tests for option order-book Bronze parquet lake functions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.lake import snapshot_partition_path
from ingestion.option_l2 import OptionL2SnapshotRow
from ingestion.option_l2_lake import option_l2_partition_path, save_option_l2_snapshot_parquet_lake


def _sample_row(raw_payload_hash: str = "abc", instrument_name: str = "BTC-30JUN26-120000-C") -> OptionL2SnapshotRow:
    snapshot_time = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return OptionL2SnapshotRow(
        schema_version="v1",
        dataset_type="options_l2_snapshot_1m",
        exchange="deribit",
        source="rest_order_book",
        currency=instrument_name.split("-", maxsplit=1)[0].removesuffix("_USDC"),
        instrument_name=instrument_name,
        instrument_type="option",
        snapshot_time=snapshot_time,
        exchange_timestamp=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        run_id="20260524T071500000000Z",
        depth=50,
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
        mark_price=0.105,
        index_price=76840.2,
        underlying_price=76839.1,
        underlying_index="BTC-30JUN26",
        interest_rate=0.03,
        bid_iv=54.1,
        ask_iv=55.2,
        mark_iv=54.8,
        open_interest=10.0,
        last_price=0.106,
        settlement_price=None,
        min_price=None,
        max_price=None,
        volume=1.0,
        volume_usd=80.0,
        high=0.12,
        low=0.08,
        price_change=1.5,
        delta=0.42,
        gamma=0.01,
        theta=-0.2,
        vega=1.3,
        rho=0.5,
        raw_payload_hash=raw_payload_hash,
    )


def test_option_l2_partition_path() -> None:
    """Verify option L2 paths bucket each asset into one hourly L2 partition."""

    result = option_l2_partition_path(
        "lake/bronze",
        (
            "options_l2_snapshot_1m",
            "deribit",
            "option",
            "BTC",
            50,
            "rest_order_book",
            "2026",
            "05",
            "24",
            "07",
        ),
    )

    assert str(result).endswith(
        "dataset_type=options_l2_snapshot_1m/exchange=deribit/instrument_type=option/"
        "symbol=BTC/depth=50/source=rest_order_book/year=2026/month=05/"
        "date=24/hour=07"
    )


def test_option_l2_partition_shape_matches_perps_l2_partition_shape() -> None:
    """Verify option and perpetual L2 use the same partition key order after dataset type."""

    perps_parts = snapshot_partition_path(
        "lake/bronze",
        ("deribit", "perp", "BTC-PERPETUAL", 50, "rest_order_book", "2026", "05", "24", "07"),
    ).parts[2:]
    option_parts = option_l2_partition_path(
        "lake/bronze",
        (
            "options_l2_snapshot_1m",
            "deribit",
            "option",
            "BTC",
            50,
            "rest_order_book",
            "2026",
            "05",
            "24",
            "07",
        ),
    ).parts[2:]

    assert [part.split("=", maxsplit=1)[0] for part in option_parts] == [
        part.split("=", maxsplit=1)[0] for part in perps_parts
    ]
    assert "source=rest_order_book" in option_parts
    assert not any(part.startswith(("currency=", "instrument_name=")) for part in option_parts)


def test_save_option_l2_snapshot_parquet_lake_upserts_hourly_file(tmp_path: Path) -> None:
    """Verify option L2 rows are upserted by natural key."""

    first_files = save_option_l2_snapshot_parquet_lake(rows=[_sample_row()], lake_root=str(tmp_path))
    second_files = save_option_l2_snapshot_parquet_lake(
        rows=[replace(_sample_row(), raw_payload_hash="replacement")],
        lake_root=str(tmp_path),
    )

    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTC-30JUN26-120000-C"
    assert rows[0]["event_time"] == datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    assert rows[0]["instrument_name"] == "BTC-30JUN26-120000-C"
    assert rows[0]["exchange_timestamp"] == datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    assert rows[0]["depth"] == 50
    assert rows[0]["bid_levels"] == 1
    assert rows[0]["ask_levels"] == 1
    assert rows[0]["best_bid_amount"] == 4.0
    assert rows[0]["best_ask_amount"] == 5.0
    assert rows[0]["raw_payload_hash"] == "replacement"


def test_save_option_l2_snapshot_parquet_lake_groups_asset_symbols_in_hourly_file(tmp_path: Path) -> None:
    """Verify all same-asset option contracts share one hourly Bronze L2 file."""

    output_files = save_option_l2_snapshot_parquet_lake(
        rows=[
            _sample_row(instrument_name="BTC-30JUN26-120000-C"),
            _sample_row(instrument_name="BTC-30JUN26-90000-P"),
        ],
        lake_root=str(tmp_path),
    )

    records = pq.ParquetFile(output_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert len(output_files) == 1
    assert "/symbol=BTC/depth=50/source=rest_order_book/" in output_files[0]
    assert {record["symbol"] for record in records} == {
        "BTC-30JUN26-120000-C",
        "BTC-30JUN26-90000-P",
    }
