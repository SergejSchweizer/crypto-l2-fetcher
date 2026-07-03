"""Migrate option L2 Bronze files to the shared L2 partition contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from ingestion.parquet_repository import ParquetRecord, ParquetUpsertRepository

OPTION_L2_DATASET_TYPE = "options_l2_snapshot_1m"
CANONICAL_SOURCE = "rest_order_book"
LEGACY_SOURCE = "rest_get_order_book"


def main() -> None:
    """Parse arguments and migrate option L2 Bronze partition directories."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-lake-root", default="lake/bronze", help="Bronze lake root to migrate")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rewrite files and remove migrated legacy source files. Without this flag, run a dry-run.",
    )
    args = parser.parse_args()
    summary = migrate_option_l2_layout(
        bronze_lake_root=Path(args.bronze_lake_root),
        dry_run=not args.apply,
    )
    mode = "dry_run" if summary.dry_run else "applied"
    print(
        f"option L2 layout migration {mode} "
        f"source_files={summary.source_files} target_files={summary.target_files} rows={summary.rows}"
    )


@dataclass
class OptionL2LayoutMigrationSummary:
    """Counters for an option L2 layout migration run."""

    dry_run: bool
    source_files: int = 0
    target_files: int = 0
    rows: int = 0
    targets: set[Path] = field(default_factory=set)


def migrate_option_l2_layout(
    bronze_lake_root: Path,
    *,
    dry_run: bool = True,
) -> OptionL2LayoutMigrationSummary:
    """Migrate option L2 Bronze files from legacy partitions to the L2 contract.

    Args:
        bronze_lake_root (Path): Bronze lake root containing partitioned datasets.
        dry_run (bool): When true, only report files and rows that would be migrated.

    Returns:
        OptionL2LayoutMigrationSummary: Counters for the migration run.

    Raises:
        RuntimeError: If ``pyarrow`` is unavailable.
        ValueError: If a migrated row lacks an option instrument or timestamp.
    """

    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required for option L2 layout migration.") from exc

    dataset_root = bronze_lake_root / f"dataset_type={OPTION_L2_DATASET_TYPE}"
    summary = OptionL2LayoutMigrationSummary(dry_run=dry_run)
    if not dataset_root.exists():
        return summary

    repository = ParquetUpsertRepository()
    source_files_by_target: dict[Path, list[Path]] = {}
    for source_file in sorted(dataset_root.glob("**/data.parquet")):
        target_file = _target_file_for(source_file=source_file, bronze_lake_root=bronze_lake_root)
        if target_file == source_file:
            continue

        parquet_file = pq.ParquetFile(source_file)  # type: ignore[no-untyped-call]  # pyarrow readers are untyped.
        row_count = parquet_file.metadata.num_rows
        summary.source_files += 1
        summary.rows += row_count
        summary.targets.add(target_file)
        summary.target_files = len(summary.targets)
        source_files_by_target.setdefault(target_file, []).append(source_file)

    if dry_run:
        return summary

    for target_file, source_files in source_files_by_target.items():
        rows: list[ParquetRecord] = []
        for source_file in source_files:
            parquet_file = pq.ParquetFile(source_file)  # type: ignore[no-untyped-call]  # pyarrow readers are untyped.
            if parquet_file.metadata.num_rows == 0:
                continue
            rows.extend(_canonical_record(row) for row in parquet_file.read().to_pylist())  # type: ignore[no-untyped-call]
        if rows:
            repository.upsert(
                file_path=target_file,
                records=rows,
                natural_key=_natural_key,
                sort_key=_sort_key,
                staging_name=".staging-option-l2-layout-migration.parquet",
            )
        for source_file in source_files:
            source_file.unlink()
            _remove_empty_parents(source_file.parent, stop_at=dataset_root)

    return summary


def _target_file_for(*, source_file: Path, bronze_lake_root: Path) -> Path:
    exchange = _partition_value(source_file, "exchange")
    instrument_type = _partition_value(source_file, "instrument_type")
    symbol = _asset_symbol_from_path(source_file)
    depth = _partition_value(source_file, "depth")
    year = _partition_value(source_file, "year")
    month = _partition_value(source_file, "month")
    date = _partition_value(source_file, "date")
    hour = _partition_value(source_file, "hour")
    return (
        bronze_lake_root
        / f"dataset_type={OPTION_L2_DATASET_TYPE}"
        / f"exchange={exchange}"
        / f"instrument_type={instrument_type}"
        / f"symbol={symbol}"
        / f"depth={depth}"
        / f"source={CANONICAL_SOURCE}"
        / f"year={year}"
        / f"month={month}"
        / f"date={date}"
        / f"hour={hour}"
        / "data.parquet"
    )


def _canonical_record(row: ParquetRecord) -> ParquetRecord:
    symbol = str(row.get("symbol") or row.get("instrument_name") or "").strip()
    if not symbol:
        raise ValueError("Option L2 row is missing symbol/instrument_name")

    event_time = row.get("event_time") or row.get("exchange_timestamp")
    if not isinstance(event_time, datetime):
        raise ValueError(f"Option L2 row is missing datetime event_time/exchange_timestamp: {event_time!r}")

    canonical = dict(row)
    canonical["symbol"] = symbol
    canonical["instrument_name"] = str(canonical.get("instrument_name") or symbol)
    canonical["event_time"] = event_time
    canonical["exchange_timestamp"] = canonical.get("exchange_timestamp") or event_time
    canonical["source"] = CANONICAL_SOURCE
    return canonical


def _asset_symbol_from_path(source_file: Path) -> str:
    currency = _partition_value_or_none(source_file, "currency")
    if currency is not None:
        return currency
    partition_symbol = _partition_value_or_none(source_file, "symbol") or _partition_value(
        source_file, "instrument_name"
    )
    return _asset_symbol_from_instrument(partition_symbol)


def _asset_symbol_from_instrument(instrument_name: str) -> str:
    return instrument_name.split("-", maxsplit=1)[0].removesuffix("_USDC")


def _natural_key(record: ParquetRecord) -> tuple[object, ...]:
    return (
        record["exchange"],
        record["symbol"],
        record["source"],
        record["depth"],
        record["event_time"],
    )


def _sort_key(record: ParquetRecord) -> str:
    event_time = record["event_time"]
    if not isinstance(event_time, datetime):
        raise ValueError(f"Expected datetime event_time, got {event_time!r}")
    return f"{event_time.isoformat()}|{record['symbol']}"


def _partition_value(path: Path, name: str) -> str:
    value = _partition_value_or_none(path, name)
    if value is None:
        raise ValueError(f"Missing {name} partition in {path}")
    return value


def _partition_value_or_none(path: Path, name: str) -> str | None:
    prefix = f"{name}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _remove_empty_parents(start: Path, stop_at: Path) -> None:
    current = start
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


if __name__ == "__main__":
    main()
