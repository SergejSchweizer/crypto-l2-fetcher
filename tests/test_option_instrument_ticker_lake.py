"""Tests for per-instrument option ticker Bronze parquet lake functions."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from ingestion.option_instrument_ticker import OptionInstrumentTickerSnapshotRow
from ingestion.option_instrument_ticker_lake import (
    option_instrument_ticker_partition_path,
    save_option_instrument_ticker_snapshot_parquet_lake,
)


def _sample_row(raw_payload_hash: str = "abc") -> OptionInstrumentTickerSnapshotRow:
    snapshot_time = datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    return OptionInstrumentTickerSnapshotRow(
        exchange="deribit",
        dataset_type="options_instrument_ticker_snapshot_1m",
        source="rest_ticker",
        currency="BTC",
        instrument_name="BTC-30JUN26-120000-C",
        instrument_type="option",
        snapshot_time=snapshot_time,
        exchange_creation_time=datetime(2026, 5, 24, 7, 14, 59, tzinfo=UTC),
        exchange_timestamp=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
        run_id="20260524T071500000000Z",
        state="open",
        bid_price=0.1,
        ask_price=0.11,
        best_bid_price=0.1,
        best_ask_price=0.11,
        best_bid_amount=4.0,
        best_ask_amount=5.0,
        bid_iv=54.1,
        ask_iv=55.2,
        mark_iv=54.8,
        mark_price=0.105,
        last_price=0.106,
        underlying_price=76839.1,
        underlying_index="BTC-30JUN26",
        index_price=76840.2,
        interest_rate=0.03,
        open_interest=10.0,
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
        schema_version="v1",
    )


def test_option_instrument_ticker_partition_path() -> None:
    """Verify per-instrument ticker paths include instrument and hour partitions."""

    result = option_instrument_ticker_partition_path(
        "lake/bronze",
        (
            "options_instrument_ticker_snapshot_1m",
            "deribit",
            "option",
            "BTC",
            "BTC-30JUN26-120000-C",
            "rest_ticker",
            "2026",
            "05",
            "24",
            "07",
        ),
    )

    assert str(result).endswith(
        "dataset_type=options_instrument_ticker_snapshot_1m/exchange=deribit/instrument_type=option/"
        "currency=BTC/instrument_name=BTC-30JUN26-120000-C/source=rest_ticker/year=2026/month=05/"
        "date=24/hour=07"
    )


def test_save_option_instrument_ticker_snapshot_parquet_lake_upserts_hourly_file(tmp_path: Path) -> None:
    """Verify per-instrument ticker rows are upserted by natural key."""

    first_files = save_option_instrument_ticker_snapshot_parquet_lake(rows=[_sample_row()], lake_root=str(tmp_path))
    second_files = save_option_instrument_ticker_snapshot_parquet_lake(
        rows=[replace(_sample_row(), raw_payload_hash="replacement")],
        lake_root=str(tmp_path),
    )

    rows = pq.ParquetFile(second_files[0]).read().to_pylist()  # type: ignore[no-untyped-call]

    assert first_files == second_files
    assert len(rows) == 1
    assert rows[0]["bid_iv"] == 54.1
    assert rows[0]["state"] == "open"
    assert rows[0]["best_bid_amount"] == 4.0
    assert rows[0]["best_ask_amount"] == 5.0
    assert rows[0]["index_price"] == 76840.2
    assert rows[0]["high"] == 0.12
    assert rows[0]["low"] == 0.08
    assert rows[0]["price_change"] == 1.5
    assert rows[0]["raw_payload_hash"] == "replacement"
