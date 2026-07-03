"""Parquet lake writer for Bronze option order-book snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.lake_writer import upsert_partitioned_records
from ingestion.option_l2 import OptionL2SnapshotRow

OptionL2PartitionKey = tuple[str, str, str, str, str, int, str, str, str, str, str]
OptionL2NaturalKey = tuple[str, str, str, int, datetime]


def option_l2_partition_path(lake_root: str, key: OptionL2PartitionKey) -> Path:
    """Return the Bronze destination directory for one option L2 partition."""

    (
        dataset_type,
        exchange,
        instrument_type,
        currency,
        instrument_name,
        depth,
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
        / f"depth={depth}"
        / f"source={source}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def option_l2_snapshot_record(row: OptionL2SnapshotRow) -> dict[str, object]:
    """Convert one typed option L2 row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "currency": row.currency,
        "instrument_name": row.instrument_name,
        "instrument_type": row.instrument_type,
        "snapshot_time": row.snapshot_time,
        "exchange_timestamp": row.exchange_timestamp,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "depth": row.depth,
        "fetch_duration_s": row.fetch_duration_s,
        "state": row.state,
        "bids": row.bids,
        "asks": row.asks,
        "bid_levels": row.bid_levels,
        "ask_levels": row.ask_levels,
        "best_bid_price": row.best_bid_price,
        "best_ask_price": row.best_ask_price,
        "best_bid_amount": row.best_bid_amount,
        "best_ask_amount": row.best_ask_amount,
        "mark_price": row.mark_price,
        "index_price": row.index_price,
        "underlying_price": row.underlying_price,
        "underlying_index": row.underlying_index,
        "interest_rate": row.interest_rate,
        "bid_iv": row.bid_iv,
        "ask_iv": row.ask_iv,
        "mark_iv": row.mark_iv,
        "open_interest": row.open_interest,
        "last_price": row.last_price,
        "settlement_price": row.settlement_price,
        "min_price": row.min_price,
        "max_price": row.max_price,
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
    }


def _natural_key(record: dict[str, object]) -> OptionL2NaturalKey:
    exchange_timestamp = record["exchange_timestamp"]
    if not isinstance(exchange_timestamp, datetime):
        raise ValueError("exchange_timestamp must be datetime")
    depth = record["depth"]
    if not isinstance(depth, int):
        raise ValueError("depth must be int")
    return (
        str(record["exchange"]),
        str(record["instrument_name"]),
        str(record["source"]),
        depth,
        exchange_timestamp,
    )


def _sort_key(record: dict[str, object]) -> str:
    exchange_timestamp = record["exchange_timestamp"]
    if not isinstance(exchange_timestamp, datetime):
        raise ValueError("exchange_timestamp must be datetime")
    return f"{exchange_timestamp.isoformat()}|{record['instrument_name']}"


def save_option_l2_snapshot_parquet_lake(rows: list[OptionL2SnapshotRow], lake_root: str) -> list[str]:
    """Upsert option order-book rows into hourly Bronze parquet files."""

    def partition_key(row: OptionL2SnapshotRow) -> OptionL2PartitionKey:
        return (
            row.dataset_type,
            row.exchange,
            row.instrument_type,
            row.currency,
            row.instrument_name,
            row.depth,
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
        partition_path=lambda root, key: option_l2_partition_path(
            lake_root=root,
            key=cast(OptionL2PartitionKey, key),
        ),
        record_builder=option_l2_snapshot_record,
        natural_key=_natural_key,
        sort_key=_sort_key,
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )
