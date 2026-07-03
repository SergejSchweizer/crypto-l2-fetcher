"""Tests for disk-pressure Bronze partition pruning."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.prune_bronze_by_disk_usage import (
    DiskUsageSnapshot,
    discover_bronze_partition_candidates,
    prune_bronze_by_disk_usage,
)


def test_prune_bronze_by_disk_usage_deletes_oldest_partitions_until_target(tmp_path: Path) -> None:
    """Verify oldest Bronze partitions are removed until the stop threshold is reached."""

    bronze_root = tmp_path / "lake" / "bronze"
    newest = _write_partition(bronze_root, year="2026", month="07", date="01", hour="00", payload="new")
    oldest = _write_partition(bronze_root, year="2026", month="06", date="29", hour="00", payload="old")
    middle = _write_partition(bronze_root, year="2026", month="06", date="30", hour="00", payload="mid")
    usage = iter(
        [
            DiskUsageSnapshot(total_bytes=100, used_bytes=94, free_bytes=6, percent=94.0),
            DiskUsageSnapshot(total_bytes=100, used_bytes=92, free_bytes=8, percent=92.0),
            DiskUsageSnapshot(total_bytes=100, used_bytes=90, free_bytes=10, percent=90.0),
            DiskUsageSnapshot(total_bytes=100, used_bytes=90, free_bytes=10, percent=90.0),
        ]
    )

    summary = prune_bronze_by_disk_usage(
        bronze_lake_root=bronze_root,
        trigger_percent=93.0,
        target_percent=90.0,
        evidence_dir=tmp_path / "evidence",
        disk_usage_reader=lambda _: next(usage),
    )

    assert summary.triggered is True
    assert summary.disk_usage_before.percent == 94.0
    assert summary.disk_usage_after.percent == 90.0
    assert [Path(item.path).name for item in summary.deleted_partitions] == ["hour=00", "hour=00"]
    assert not oldest.exists()
    assert not middle.exists()
    assert newest.exists()
    evidence_files = list((tmp_path / "evidence").glob("*.json"))
    assert len(evidence_files) == 1
    evidence = json.loads(evidence_files[0].read_text(encoding="utf-8"))
    assert evidence["trigger_percent"] == 93.0
    assert evidence["target_percent"] == 90.0
    assert len(evidence["deleted_partitions"]) == 2


def test_prune_bronze_by_disk_usage_skips_when_below_trigger(tmp_path: Path) -> None:
    """Verify no Bronze data is removed when disk usage is below the trigger."""

    bronze_root = tmp_path / "lake" / "bronze"
    partition = _write_partition(bronze_root, year="2026", month="06", date="29", hour="00", payload="old")

    summary = prune_bronze_by_disk_usage(
        bronze_lake_root=bronze_root,
        trigger_percent=93.0,
        target_percent=90.0,
        evidence_dir=None,
        disk_usage_reader=lambda _: DiskUsageSnapshot(total_bytes=100, used_bytes=92, free_bytes=8, percent=92.0),
    )

    assert summary.triggered is False
    assert summary.skipped_reason == "disk_usage_below_trigger"
    assert summary.deleted_partitions == []
    assert partition.exists()


def test_discover_bronze_partition_candidates_ignores_unsafe_files(tmp_path: Path) -> None:
    """Verify pruning candidates are constrained to Hive-style Bronze partition files."""

    bronze_root = tmp_path / "lake" / "bronze"
    safe_partition = _write_partition(bronze_root, year="2026", month="06", date="29", hour="02", payload="old")
    unsafe_file = bronze_root / "notes" / "data.parquet"
    unsafe_file.parent.mkdir(parents=True)
    unsafe_file.write_text("not a partition", encoding="utf-8")

    candidates = discover_bronze_partition_candidates(bronze_root)

    assert [candidate.path for candidate in candidates] == [safe_partition]
    assert candidates[0].partition_time.isoformat() == "2026-06-29T02:00:00+00:00"


def _write_partition(
    bronze_root: Path,
    *,
    year: str,
    month: str,
    date: str,
    hour: str,
    payload: str,
) -> Path:
    partition = (
        bronze_root
        / "dataset_type=perps_l2_snapshot_1m"
        / "exchange=deribit"
        / "symbol=BTC-PERPETUAL"
        / f"year={year}"
        / f"month={month}"
        / f"date={date}"
        / f"hour={hour}"
    )
    partition.mkdir(parents=True)
    (partition / "data.parquet").write_text(payload, encoding="utf-8")
    return partition
