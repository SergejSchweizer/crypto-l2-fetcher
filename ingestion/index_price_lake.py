"""Parquet lake writer for bronze index price snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.index_price import IndexPriceSnapshotRow
from ingestion.lake_writer import upsert_partitioned_records

IndexPricePartitionKey = tuple[str, str, str, str, str, str, str]
IndexPriceNaturalKey = tuple[str, str, datetime, str]


def index_price_partition_path(lake_root: str, key: IndexPricePartitionKey) -> Path:
    """Return destination directory for one index price partition."""

    dataset_type, exchange, index_name, year_partition, month_partition, date_partition, hour_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"index_name={index_name}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def index_price_snapshot_record(row: IndexPriceSnapshotRow) -> dict[str, object]:
    """Convert one typed index price snapshot row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "index_name": row.index_name,
        "snapshot_time": row.snapshot_time,
        "event_time": row.event_time,
        "price": row.price,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> IndexPriceNaturalKey:
    event_time = record["event_time"]
    if not isinstance(event_time, datetime):
        raise ValueError("event_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["index_name"]),
        event_time,
        str(record["source"]),
    )


def save_index_price_snapshot_parquet_lake(
    rows_by_index: dict[str, list[IndexPriceSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Persist index price rows into idempotent parquet partitions."""

    def partition_key(row: IndexPriceSnapshotRow) -> IndexPricePartitionKey:
        return (
            row.dataset_type,
            row.exchange,
            row.index_name,
            row.snapshot_time.strftime("%Y"),
            row.snapshot_time.strftime("%m"),
            row.snapshot_time.strftime("%d"),
            row.snapshot_time.strftime("%H"),
        )

    return upsert_partitioned_records(
        rows=(row for rows in rows_by_index.values() for row in rows),
        lake_root=lake_root,
        partition_key=partition_key,
        partition_path=lambda root, key: index_price_partition_path(
            lake_root=root,
            key=cast(IndexPricePartitionKey, key),
        ),
        record_builder=index_price_snapshot_record,
        natural_key=_natural_key,
        sort_key=lambda item: cast(datetime, item["event_time"]),
        staging_name=lambda _records: ".staging-data.parquet",
    )
