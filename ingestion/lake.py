"""Parquet lake writer for raw perpetual L2 snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from ingestion.l2 import (
    L2Snapshot,
    perps_l2_snapshot_1m_partition_key,
    perps_l2_snapshot_1m_record,
)
from ingestion.lake_writer import upsert_partitioned_records

SnapshotPartitionKey = tuple[str, str, str, int, str, str, str, str, str]
SnapshotNaturalKey = tuple[str, str, str, int, str, datetime]


def utc_run_id() -> str:
    """Create a UTC run identifier for lake writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_partition_path(lake_root: str, key: SnapshotPartitionKey) -> Path:
    """Return the Bronze destination directory for one perpetual L2 snapshot partition."""

    (
        exchange,
        instrument_type,
        symbol,
        depth,
        source,
        year_partition,
        month_partition,
        date_partition,
        hour_partition,
    ) = key
    return (
        Path(lake_root)
        / "dataset_type=perps_l2_snapshot_1m"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"depth={depth}"
        / f"source={source}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def snapshot_record_natural_key(record: dict[str, object]) -> SnapshotNaturalKey:
    """Build the idempotent natural key for one perpetual L2 snapshot row."""

    event_time = record["event_time"]
    if not isinstance(event_time, datetime):
        raise ValueError("event_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_type"]),
        str(record["symbol"]),
        int(cast(int, record["depth"])),
        str(record["source"]),
        event_time,
    )


def save_perps_l2_snapshot_1m_parquet_lake(
    snapshots_by_symbol: dict[str, list[L2Snapshot]],
    lake_root: str,
    depth: int,
    source: str = "rest_order_book",
) -> list[str]:
    """Save raw perpetual L2 snapshots to hourly Bronze parquet lake partitions."""

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)

    def record_builder(snapshot: L2Snapshot) -> dict[str, object]:
        return perps_l2_snapshot_1m_record(
            snapshot=snapshot,
            depth=depth,
            run_id=run_id,
            ingested_at=ingested_at,
            source=source,
        )

    return upsert_partitioned_records(
        rows=(snapshot for snapshots in snapshots_by_symbol.values() for snapshot in snapshots),
        lake_root=lake_root,
        partition_key=lambda snapshot: perps_l2_snapshot_1m_partition_key(
            snapshot=snapshot,
            depth=depth,
            source=source,
        ),
        partition_path=lambda root, key: snapshot_partition_path(
            lake_root=root,
            key=cast(SnapshotPartitionKey, key),
        ),
        record_builder=record_builder,
        natural_key=snapshot_record_natural_key,
        sort_key=lambda item: cast(datetime, item["event_time"]),
        staging_name=lambda _records: f".staging-{run_id}.parquet",
    )
