"""Parquet lake writer for bronze option ticker snapshots."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import cast

from ingestion.lake_writer import upsert_partitioned_records
from ingestion.options import OptionTickerSnapshotRow

OptionPartitionKey = tuple[str, str, str, str, str, str, str, str]
OptionNaturalKey = tuple[str, str, str, str, datetime]


def option_snapshot_partition_path(lake_root: str, key: OptionPartitionKey) -> Path:
    """Return the bronze destination directory for one option snapshot partition."""

    (
        dataset_type,
        exchange,
        instrument_type,
        currency,
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
        / "source=rest_get_book_summary_by_currency"
        / f"year={year_partition}"
        / f"month={month_partition}"
        / f"date={date_partition}"
        / f"hour={hour_partition}"
    )


def options_ticker_snapshot_record(row: OptionTickerSnapshotRow) -> dict[str, object]:
    """Convert one typed option snapshot row to a parquet record."""

    return {
        "exchange": row.exchange,
        "dataset_type": row.dataset_type,
        "source": row.source,
        "currency": row.currency,
        "requested_currency": row.requested_currency,
        "source_currency": row.source_currency,
        "instrument_name": row.instrument_name,
        "base_currency": row.base_currency,
        "quote_currency": row.quote_currency,
        "instrument_type": row.instrument_type,
        "snapshot_time": row.snapshot_time,
        "exchange_creation_time": row.exchange_creation_time,
        "ingested_at": row.ingested_at,
        "run_id": row.run_id,
        "bid_price": row.bid_price,
        "ask_price": row.ask_price,
        "mid_price": row.mid_price,
        "mark_price": row.mark_price,
        "mark_iv": row.mark_iv,
        "underlying_price": row.underlying_price,
        "underlying_index": row.underlying_index,
        "interest_rate": row.interest_rate,
        "open_interest": row.open_interest,
        "volume": row.volume,
        "volume_usd": row.volume_usd,
        "high": row.high,
        "low": row.low,
        "last": row.last,
        "price_change": row.price_change,
        "raw_payload_hash": row.raw_payload_hash,
        "schema_version": row.schema_version,
    }


def _option_snapshot_natural_key(record: dict[str, object]) -> OptionNaturalKey:
    """Build the idempotent natural key for one option ticker snapshot row."""

    snapshot_time = record["snapshot_time"]
    if not isinstance(snapshot_time, datetime):
        raise ValueError("snapshot_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["currency"]),
        str(record["instrument_name"]),
        str(record["source"]),
        snapshot_time,
    )


def _option_snapshot_sort_key(record: dict[str, object]) -> str:
    snapshot_time = record["snapshot_time"]
    if not isinstance(snapshot_time, datetime):
        raise ValueError("snapshot_time must be datetime")
    return f"{snapshot_time.isoformat()}|{record['currency']}|{record['instrument_name']}"


def save_options_ticker_snapshot_parquet_lake(
    rows_by_currency: dict[str, list[OptionTickerSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Upsert option snapshot rows into one hourly bronze parquet file per partition."""

    def partition_key(row: OptionTickerSnapshotRow) -> OptionPartitionKey:
        return (
            row.dataset_type,
            row.exchange,
            row.instrument_type,
            row.currency,
            row.snapshot_time.strftime("%Y"),
            row.snapshot_time.strftime("%m"),
            row.snapshot_time.strftime("%d"),
            row.snapshot_time.strftime("%H"),
        )

    return upsert_partitioned_records(
        rows=(row for rows in rows_by_currency.values() for row in rows),
        lake_root=lake_root,
        partition_key=partition_key,
        partition_path=lambda root, key: option_snapshot_partition_path(
            lake_root=root,
            key=cast(OptionPartitionKey, key),
        ),
        record_builder=options_ticker_snapshot_record,
        natural_key=_option_snapshot_natural_key,
        sort_key=_option_snapshot_sort_key,
        staging_name=lambda records: f".staging-{records[0]['run_id']}.parquet",
    )


def option_ticker_snapshot_record(row: OptionTickerSnapshotRow) -> dict[str, object]:
    """Backward-compatible alias for options ticker record serialization."""

    return options_ticker_snapshot_record(row)


def save_option_ticker_snapshot_parquet_lake(
    rows_by_currency: dict[str, list[OptionTickerSnapshotRow]],
    lake_root: str,
) -> list[str]:
    """Backward-compatible alias for options ticker parquet persistence."""

    return save_options_ticker_snapshot_parquet_lake(rows_by_currency=rows_by_currency, lake_root=lake_root)
