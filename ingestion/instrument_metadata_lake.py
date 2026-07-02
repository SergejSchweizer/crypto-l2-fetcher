"""Parquet lake writer for bronze instrument metadata snapshots."""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ingestion.instrument_metadata import InstrumentMetadataSnapshotRow
from ingestion.lake_writer import upsert_partitioned_records

InstrumentMetadataPartitionKey = tuple[str, str, str, str, str, str]
InstrumentMetadataNaturalKey = tuple[str, str, str]


def instrument_metadata_partition_path(lake_root: str, key: InstrumentMetadataPartitionKey) -> Path:
    """Return destination directory for one hourly instrument metadata partition."""

    dataset_type, exchange, year_partition, month_partition, date_partition, hour_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def instrument_metadata_snapshot_record(row: InstrumentMetadataSnapshotRow) -> dict[str, object]:
    """Convert one typed instrument metadata row to a parquet record."""

    return {
        "schema_version": row.schema_version,
        "dataset_type": row.dataset_type,
        "exchange": row.exchange,
        "source": row.source,
        "snapshot_date": row.snapshot_date,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "instrument_name": row.instrument_name,
        "kind": row.kind,
        "base_currency": row.base_currency,
        "quote_currency": row.quote_currency,
        "counter_currency": row.counter_currency,
        "settlement_currency": row.settlement_currency,
        "instrument_type": row.instrument_type,
        "settlement_period": row.settlement_period,
        "price_index": row.price_index,
        "state": row.state,
        "tick_size": row.tick_size,
        "contract_size": row.contract_size,
        "min_trade_amount": row.min_trade_amount,
        "is_active": row.is_active,
        "creation_timestamp": row.creation_timestamp,
        "expiration_timestamp": row.expiration_timestamp,
        "option_type": row.option_type,
        "strike": row.strike,
        "raw_payload_hash": row.raw_payload_hash,
    }


def _natural_key(record: dict[str, object]) -> InstrumentMetadataNaturalKey:
    return (
        str(record["exchange"]),
        str(record["instrument_name"]),
        str(record["snapshot_date"]),
    )


def save_instrument_metadata_snapshot_parquet_lake(
    rows_by_date: dict[str, list[InstrumentMetadataSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Persist instrument metadata rows as idempotent hourly bronze parquet files."""

    def partition_key(row: InstrumentMetadataSnapshotRow) -> InstrumentMetadataPartitionKey:
        return (
            row.dataset_type,
            row.exchange,
            row.snapshot_date.strftime("%Y"),
            row.snapshot_date.strftime("%m"),
            row.snapshot_date.strftime("%d"),
            "00",
        )

    return upsert_partitioned_records(
        rows=(row for rows in rows_by_date.values() for row in rows),
        lake_root=lake_root,
        partition_key=partition_key,
        partition_path=lambda root, key: instrument_metadata_partition_path(
            lake_root=root,
            key=cast(InstrumentMetadataPartitionKey, key),
        ),
        record_builder=instrument_metadata_snapshot_record,
        natural_key=_natural_key,
        sort_key=lambda item: str(item["instrument_name"]),
        staging_name=lambda _records: ".staging-data.parquet",
    )
