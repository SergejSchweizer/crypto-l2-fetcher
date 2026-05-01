"""Parquet lake writer for aggregated L2 minute bars."""

from __future__ import annotations

import concurrent.futures
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from ingestion.l2 import L2MinuteBar, l2_m1_partition_key, l2_m1_record

PartitionKey = tuple[str, str, str, str, str]
NaturalKey = tuple[str, str, str, str, datetime]


def utc_run_id() -> str:
    """Create a UTC run identifier for lake writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def partition_path(lake_root: str, dataset_type: str, key: PartitionKey) -> Path:
    """Return the destination directory for one parquet partition."""

    exchange, instrument_type, symbol, timeframe, date_partition = key
    return (
        Path(lake_root)
        / f"dataset_type={dataset_type}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"timeframe={timeframe}"
        / f"date={date_partition}"
    )


def record_natural_key(record: dict[str, object]) -> NaturalKey:
    """Build the idempotent natural key for one L2 parquet row."""

    open_time = record["open_time"]
    if not isinstance(open_time, datetime):
        raise ValueError("open_time must be datetime")
    return (
        str(record["exchange"]),
        str(record["instrument_type"]),
        str(record["symbol"]),
        str(record["timeframe"]),
        open_time,
    )


def merge_and_deduplicate_rows(
    existing: list[dict[str, object]],
    new: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Merge old and new rows while keeping the latest row for duplicate keys."""

    merged: dict[NaturalKey, dict[str, object]] = {}
    for record in existing:
        merged[record_natural_key(record)] = record
    for record in new:
        merged[record_natural_key(record)] = record

    rows = list(merged.values())
    rows.sort(key=lambda item: cast(datetime, item["open_time"]))
    return rows


def save_l2_m1_parquet_lake(
    rows_by_exchange: dict[str, dict[str, list[L2MinuteBar]]],
    lake_root: str,
) -> list[str]:
    """Save aggregated L2 M1 rows to parquet lake partitions."""

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for parquet lake output. Install project dependencies.") from exc

    run_id = utc_run_id()
    ingested_at = datetime.now(UTC)
    dataset_type = "l2_m1"

    grouped: defaultdict[PartitionKey, list[dict[str, object]]] = defaultdict(list)
    for symbol_map in rows_by_exchange.values():
        for rows in symbol_map.values():
            for row in rows:
                key = l2_m1_partition_key(row)
                grouped[key].append(l2_m1_record(row=row, run_id=run_id, ingested_at=ingested_at))

    def _write_one_partition(key: PartitionKey, rows: list[dict[str, object]]) -> str:
        part_dir = partition_path(lake_root=lake_root, dataset_type=dataset_type, key=key)
        part_dir.mkdir(parents=True, exist_ok=True)
        file_path = part_dir / "data.parquet"
        staging_path = part_dir / f".staging-{run_id}.parquet"

        existing_rows: list[dict[str, object]] = []
        if file_path.exists():
            existing_table = pq.ParquetFile(file_path).read()  # type: ignore[no-untyped-call]
            existing_rows = existing_table.to_pylist()

        merged_rows = merge_and_deduplicate_rows(existing=existing_rows, new=rows)
        table = pa.Table.from_pylist(merged_rows)
        pq.write_table(table, staging_path)  # type: ignore[no-untyped-call]
        staging_path.replace(file_path)
        return str(file_path.resolve())

    written_files: list[str] = []
    if grouped:
        max_workers = min(4, len(grouped))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_write_one_partition, key, rows) for key, rows in grouped.items()]
            for future in concurrent.futures.as_completed(futures):
                written_files.append(future.result())

    return sorted(written_files)
