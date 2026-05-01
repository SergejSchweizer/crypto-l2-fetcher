"""Tests for L2 parquet lake helper functions."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ingestion.l2 import L2MinuteBar
from ingestion.lake import merge_and_deduplicate_rows, partition_path, save_l2_m1_parquet_lake


def _sample_l2_row(minute: int, mid_close: float) -> L2MinuteBar:
    """Build a representative L2 minute bar for persistence tests."""

    return L2MinuteBar(
        minute_ts=datetime(2026, 4, 29, 10, minute, tzinfo=UTC),
        exchange="deribit",
        symbol="BTC-PERPETUAL",
        snapshot_count=5,
        mid_open=100.0,
        mid_high=101.0,
        mid_low=99.0,
        mid_close=mid_close,
        mark_close=100.4,
        index_close=100.3,
        spread_bps_mean=10.0,
        spread_bps_max=12.0,
        spread_bps_last=11.0,
        bid_depth_1_mean=10.0,
        ask_depth_1_mean=11.0,
        bid_depth_10_mean=100.0,
        ask_depth_10_mean=110.0,
        bid_depth_50_mean=500.0,
        ask_depth_50_mean=510.0,
        imbalance_1_mean=0.1,
        imbalance_10_mean=0.2,
        imbalance_50_mean=0.3,
        imbalance_10_last=0.25,
        imbalance_50_last=0.35,
        microprice_close=100.45,
        microprice_minus_mid_mean=0.01,
        bid_vwap_10_mean=99.5,
        ask_vwap_10_mean=101.5,
        open_interest_last=1000.0,
        funding_8h_last=0.0001,
        current_funding_last=0.00001,
        fetch_duration_s_mean=0.11,
        fetch_duration_s_max=0.22,
        fetch_duration_s_last=0.12,
    )


def test_l2_partition_path() -> None:
    """Verify L2 partition paths use the canonical dataset layout."""

    result = partition_path(
        "lake/bronze",
        "l2_m1",
        ("deribit", "perp", "BTC-PERPETUAL", "1m", "2026-04"),
    )

    assert str(result).endswith(
        "dataset_type=l2_m1/exchange=deribit/instrument_type=perp/"
        "symbol=BTC-PERPETUAL/timeframe=1m/date=2026-04"
    )


def test_merge_and_deduplicate_rows_keeps_latest_record() -> None:
    """Verify parquet rewrites keep the latest row for a duplicate natural key."""

    first_time = datetime(2026, 4, 29, 10, 0, tzinfo=UTC)
    second_time = datetime(2026, 4, 29, 10, 1, tzinfo=UTC)
    base = {
        "exchange": "deribit",
        "instrument_type": "perp",
        "symbol": "BTC-PERPETUAL",
        "timeframe": "1m",
        "open_time": first_time,
        "mid_close": 100.0,
    }
    existing = [base, {**base, "open_time": second_time, "mid_close": 101.0}]
    new = [{**base, "mid_close": 102.0}]

    merged = merge_and_deduplicate_rows(existing=existing, new=new)

    assert len(merged) == 2
    assert merged[0]["open_time"] == first_time
    assert merged[0]["mid_close"] == 102.0


def test_save_l2_m1_parquet_lake_rewrites_single_partition_file(tmp_path: Path) -> None:
    """Verify repeated L2 parquet writes rewrite one partition idempotently."""

    row_1 = _sample_l2_row(minute=0, mid_close=100.5)
    row_2 = _sample_l2_row(minute=1, mid_close=101.5)

    first = {"deribit": {"BTC-PERPETUAL": [row_1]}}
    second = {"deribit": {"BTC-PERPETUAL": [row_1, row_2]}}

    files_1 = save_l2_m1_parquet_lake(first, lake_root=str(tmp_path))
    files_2 = save_l2_m1_parquet_lake(second, lake_root=str(tmp_path))

    assert files_1 == files_2
    assert len(files_2) == 1
    assert files_2[0].endswith("/data.parquet")
