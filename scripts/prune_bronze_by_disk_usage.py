#!/usr/bin/env python3
"""Prune oldest Bronze partitions when lake disk usage exceeds a threshold."""

from __future__ import annotations

import argparse
import json
import logging
import shutil
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts.logging_utils import configure_logger

DEFAULT_BRONZE_LAKE_ROOT = Path("lake/bronze")
DEFAULT_TRIGGER_PERCENT = 93.0
DEFAULT_TARGET_PERCENT = 90.0
DEFAULT_EVIDENCE_DIR = Path(".logs/bronze-retention-evidence")
DATA_FILE_NAME = "data.parquet"


@dataclass(frozen=True)
class DiskUsageSnapshot:
    """Filesystem usage observed for the Bronze lake mount."""

    total_bytes: int
    used_bytes: int
    free_bytes: int
    percent: float


@dataclass(frozen=True)
class BronzePartitionCandidate:
    """One safe Bronze partition directory eligible for deletion."""

    path: Path
    data_file: Path
    partition_time: datetime
    estimated_bytes: int


@dataclass(frozen=True)
class DeletedPartition:
    """Audit record for one deleted or dry-run Bronze partition."""

    path: str
    data_file: str
    partition_time: str
    estimated_bytes: int


@dataclass(frozen=True)
class PruneSummary:
    """Outcome of one disk-pressure pruning run."""

    bronze_lake_root: str
    trigger_percent: float
    target_percent: float
    dry_run: bool
    triggered: bool
    disk_usage_before: DiskUsageSnapshot
    disk_usage_after: DiskUsageSnapshot
    deleted_partitions: list[DeletedPartition]
    skipped_reason: str | None = None


DiskUsageReader = Callable[[Path], DiskUsageSnapshot]
PartitionRemover = Callable[[Path], None]


def main() -> None:
    """Parse command-line arguments and prune Bronze partitions under disk pressure."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bronze-lake-root",
        "--lake-root",
        dest="bronze_lake_root",
        default=str(DEFAULT_BRONZE_LAKE_ROOT),
        help="Bronze lake root whose filesystem usage controls pruning",
    )
    parser.add_argument(
        "--trigger-percent",
        type=float,
        default=DEFAULT_TRIGGER_PERCENT,
        help="Start pruning when disk usage is greater than or equal to this percent",
    )
    parser.add_argument(
        "--target-percent",
        type=float,
        default=DEFAULT_TARGET_PERCENT,
        help="Stop pruning once disk usage is less than or equal to this percent",
    )
    parser.add_argument(
        "--evidence-dir",
        default=str(DEFAULT_EVIDENCE_DIR),
        help="Directory for JSON audit evidence describing triggered pruning runs",
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=0.0,
        help="Only delete partitions older than this many hours",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report candidate deletions without removing files")
    args = parser.parse_args()

    logger = configure_logger("bronze-retention")
    summary = prune_bronze_by_disk_usage(
        bronze_lake_root=Path(args.bronze_lake_root),
        trigger_percent=args.trigger_percent,
        target_percent=args.target_percent,
        dry_run=args.dry_run,
        evidence_dir=Path(args.evidence_dir),
        min_age_hours=args.min_age_hours,
        logger=logger,
    )
    logger.info(
        "bronze retention complete triggered=%s dry_run=%s before=%.2f after=%.2f deleted_partitions=%s",
        summary.triggered,
        summary.dry_run,
        summary.disk_usage_before.percent,
        summary.disk_usage_after.percent,
        len(summary.deleted_partitions),
    )


def prune_bronze_by_disk_usage(
    *,
    bronze_lake_root: Path,
    trigger_percent: float = DEFAULT_TRIGGER_PERCENT,
    target_percent: float = DEFAULT_TARGET_PERCENT,
    dry_run: bool = False,
    evidence_dir: Path | None = DEFAULT_EVIDENCE_DIR,
    min_age_hours: float = 0.0,
    disk_usage_reader: DiskUsageReader | None = None,
    partition_remover: PartitionRemover = shutil.rmtree,
    logger: logging.Logger | None = None,
) -> PruneSummary:
    """Delete oldest Bronze partitions until disk usage falls to the target threshold.

    Args:
        bronze_lake_root (Path): Root directory containing Bronze dataset partitions.
        trigger_percent (float): Disk usage percent that starts pruning when reached.
        target_percent (float): Disk usage percent at or below which pruning stops.
        dry_run (bool): Whether to report deletions without removing files.
        evidence_dir (Path | None): Optional directory for JSON audit evidence.
        min_age_hours (float): Minimum partition age in hours before deletion.
        disk_usage_reader (DiskUsageReader): Dependency-injected filesystem usage reader.
        partition_remover (PartitionRemover): Dependency-injected partition remover.
        logger (logging.Logger | None): Optional logger for deletion events.

    Returns:
        PruneSummary: Audit-ready outcome of the pruning run.

    Raises:
        ValueError: If thresholds or age limits are invalid.
    """

    _validate_thresholds(trigger_percent=trigger_percent, target_percent=target_percent)
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be greater than or equal to 0")

    root = bronze_lake_root.resolve()
    usage_reader = disk_usage_reader or read_disk_usage
    before = usage_reader(root)
    if before.percent < trigger_percent:
        summary = PruneSummary(
            bronze_lake_root=str(root),
            trigger_percent=trigger_percent,
            target_percent=target_percent,
            dry_run=dry_run,
            triggered=False,
            disk_usage_before=before,
            disk_usage_after=before,
            deleted_partitions=[],
            skipped_reason="disk_usage_below_trigger",
        )
        return summary

    cutoff = datetime.now(UTC) - timedelta(hours=min_age_hours)
    candidates = discover_bronze_partition_candidates(root)
    deleted_partitions: list[DeletedPartition] = []
    current = before
    for candidate in candidates:
        if current.percent <= target_percent:
            break
        if candidate.partition_time > cutoff:
            continue

        deleted = DeletedPartition(
            path=str(candidate.path),
            data_file=str(candidate.data_file),
            partition_time=candidate.partition_time.isoformat(),
            estimated_bytes=candidate.estimated_bytes,
        )
        deleted_partitions.append(deleted)
        if logger is not None:
            logger.warning(
                "bronze retention deleting partition dry_run=%s usage_percent=%.2f path=%s estimated_bytes=%s",
                dry_run,
                current.percent,
                candidate.path,
                candidate.estimated_bytes,
            )
        if not dry_run:
            partition_remover(candidate.path)
            _remove_empty_parents(start=candidate.path.parent, stop_at=root)
            current = usage_reader(root)

    after = usage_reader(root)
    summary = PruneSummary(
        bronze_lake_root=str(root),
        trigger_percent=trigger_percent,
        target_percent=target_percent,
        dry_run=dry_run,
        triggered=True,
        disk_usage_before=before,
        disk_usage_after=after,
        deleted_partitions=deleted_partitions,
        skipped_reason=None if deleted_partitions else "no_eligible_bronze_partitions",
    )
    _write_evidence_if_requested(summary=summary, evidence_dir=evidence_dir)
    return summary


def read_disk_usage(path: Path) -> DiskUsageSnapshot:
    """Read disk usage for the filesystem that contains ``path``."""

    usage_path = _nearest_existing_path(path)
    raw_usage = shutil.disk_usage(usage_path)
    percent = (raw_usage.used / raw_usage.total) * 100 if raw_usage.total else 0.0
    return DiskUsageSnapshot(
        total_bytes=raw_usage.total,
        used_bytes=raw_usage.used,
        free_bytes=raw_usage.free,
        percent=percent,
    )


def discover_bronze_partition_candidates(bronze_lake_root: Path) -> list[BronzePartitionCandidate]:
    """Return safe Bronze partition directories sorted from oldest to newest."""

    root = bronze_lake_root.resolve()
    candidates: list[BronzePartitionCandidate] = []
    if not root.exists():
        return candidates

    for data_file in sorted(root.glob("dataset_type=*/**/data.parquet")):
        resolved_data_file = data_file.resolve()
        if not _is_safe_bronze_data_file(root=root, data_file=resolved_data_file):
            continue
        partition_dir = resolved_data_file.parent
        candidates.append(
            BronzePartitionCandidate(
                path=partition_dir,
                data_file=resolved_data_file,
                partition_time=_partition_time(resolved_data_file),
                estimated_bytes=_directory_size(partition_dir),
            )
        )
    return sorted(candidates, key=lambda item: (item.partition_time, str(item.path)))


def _validate_thresholds(*, trigger_percent: float, target_percent: float) -> None:
    if not 0 < target_percent < trigger_percent < 100:
        raise ValueError("thresholds must satisfy 0 < target_percent < trigger_percent < 100")


def _nearest_existing_path(path: Path) -> Path:
    current = path.resolve()
    while not current.exists() and current != current.parent:
        current = current.parent
    return current


def _is_safe_bronze_data_file(*, root: Path, data_file: Path) -> bool:
    if data_file.name != DATA_FILE_NAME or not data_file.is_file():
        return False
    try:
        relative_parts = data_file.relative_to(root).parts
    except ValueError:
        return False
    if len(relative_parts) < 3 or not relative_parts[0].startswith("dataset_type="):
        return False
    if relative_parts[-1] != DATA_FILE_NAME:
        return False
    return all("=" in part and not part.startswith(".") for part in relative_parts[:-1])


def _partition_time(data_file: Path) -> datetime:
    partitions = _partition_values(data_file)
    year = partitions.get("year")
    month = partitions.get("month")
    date = partitions.get("date")
    hour = partitions.get("hour", "00")
    if year is not None and month is not None and date is not None:
        return datetime(
            year=int(year),
            month=int(month),
            day=int(date),
            hour=int(hour),
            tzinfo=UTC,
        )
    return datetime.fromtimestamp(data_file.stat().st_mtime, tz=UTC)


def _partition_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for part in path.parts:
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        values[name] = value
    return values


def _directory_size(path: Path) -> int:
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def _remove_empty_parents(*, start: Path, stop_at: Path) -> None:
    current = start
    while current != stop_at and current.exists():
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent


def _write_evidence_if_requested(*, summary: PruneSummary, evidence_dir: Path | None) -> None:
    if evidence_dir is None:
        return
    evidence_dir.mkdir(parents=True, exist_ok=True)
    event_time = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    evidence_path = evidence_dir / f"bronze-retention-{event_time}.json"
    evidence_path.write_text(
        json.dumps(_json_ready(asdict(summary)), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _json_ready(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


if __name__ == "__main__":
    main()
