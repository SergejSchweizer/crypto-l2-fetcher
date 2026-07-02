"""Parquet lake writer for Bronze per-instrument option ticker snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.lake_writer import upsert_partitioned_records
from ingestion.option_instrument_ticker import OptionInstrumentTickerSnapshotRow

OptionInstrumentTickerPartitionKey = tuple[str, str, str, str, str, str, str, str, str, str]
OptionInstrumentTickerNaturalKey = tuple[str, str, str, datetime]


def option_instrument_ticker_partition_path(
    lake_root: str,
    key: OptionInstrumentTickerPartitionKey,
) -> Path:
    """Return the Bronze destination directory for one option instrument ticker partition."""

    (
        dataset_type,
        exchange,
        instrument_type,
        currency,
        instrument_name,
        source,
        year_partition,
        month_partition,
        date_partition,
        hour_partition,
    ) = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"currency={currency}"
        / f"instrument_name={instrument_name}"
        / f"source={source}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def option_instrument_ticker_snapshot_record(row: OptionInstrumentTickerSnapshotRow) -> dict[str, object]:
    """Convert one typed per-instrument option ticker row to a parquet record."""

    return {
        "exchange": row.exchange,
        "dataset_type": row.dataset_type,
        "source": row.source,
        "currency": row.currency,
        "instrument_name": row.instrument_name,
        "instrument_type": row.instrument_type,
        "snapshot_time": row.snapshot_time,
        "exchange_creation_time": row.exchange_creation_time,
        "exchange_timestamp": row.exchange_timestamp,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "state": row.state,
        "bid_price": row.bid_price,
        "ask_price": row.ask_price,
        "best_bid_price": row.best_bid_price,
        "best_ask_price": row.best_ask_price,
        "best_bid_amount": row.best_bid_amount,
        "best_ask_amount": row.best_ask_amount,
        "bid_iv": row.bid_iv,
        "ask_iv": row.ask_iv,
        "mark_iv": row.mark_iv,
        "mark_price": row.mark_price,
        "last_price": row.last_price,
        "underlying_price": row.underlying_price,
        "underlying_index": row.underlying_index,
        "index_price": row.index_price,
        "interest_rate": row.interest_rate,
        "open_interest": row.open_interest,
        "volume": row.volume,
        "volume_usd": row.volume_usd,
        "high": row.high,
        "low": row.low,
        "price_change": row.price_change,
        "delta": row.delta,
        "gamma": row.gamma,
        "theta": row.theta,
        "vega": row.vega,
        "rho": row.rho,
        "raw_payload_hash": row.raw_payload_hash,
        "schema_version": row.schema_version,
    }


def _natural_key(record: dict[str, object]) -> OptionInstrumentTickerNaturalKey:
    snapshot_time = record["snapshot_time"]
    if not isinstance(snapshot_time, datetime):
        raise ValueError("snapshot_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_name"]),
        str(record["source"]),
        snapshot_time,
    )


def _sort_key(record: dict[str, object]) -> str:
    snapshot_time = record["snapshot_time"]
    if not isinstance(snapshot_time, datetime):
        raise ValueError("snapshot_time must be datetime")
    return f"{snapshot_time.isoformat()}|{record['instrument_name']}"


def save_option_instrument_ticker_snapshot_parquet_lake(
    rows: list[OptionInstrumentTickerSnapshotRow],
    lake_root: str,
) -> list[str]:
    """Upsert per-instrument option ticker rows into hourly Bronze parquet files."""

    def partition_key(row: OptionInstrumentTickerSnapshotRow) -> OptionInstrumentTickerPartitionKey:
        return (
            row.dataset_type,
            row.exchange,
            row.instrument_type,
            row.currency,
            row.instrument_name,
            row.source,
            row.snapshot_time.strftime("%Y"),
            row.snapshot_time.strftime("%m"),
            row.snapshot_time.strftime("%d"),
            row.snapshot_time.strftime("%H"),
        )

    return upsert_partitioned_records(
        rows=rows,
        lake_root=lake_root,
        partition_key=partition_key,
        partition_path=lambda root, key: option_instrument_ticker_partition_path(
            lake_root=root,
            key=cast(OptionInstrumentTickerPartitionKey, key),
        ),
        record_builder=option_instrument_ticker_snapshot_record,
        natural_key=_natural_key,
        sort_key=_sort_key,
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )
