"""Tests for shared Bronze normalization primitives."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.normalization import (
    looks_like_option_instrument,
    option_currency,
    raw_payload_hash,
    snapshot_time_floor_minute,
    timestamp_ms_to_datetime,
    to_optional_float,
    to_optional_int,
    to_optional_str,
)


def test_shared_normalization_primitives_preserve_dataset_semantics() -> None:
    """Verify shared helpers keep the former per-module conversion behavior."""

    assert snapshot_time_floor_minute(datetime(2026, 5, 24, 7, 15, 33, 123, tzinfo=UTC)) == datetime(
        2026,
        5,
        24,
        7,
        15,
        tzinfo=UTC,
    )
    assert timestamp_ms_to_datetime(1_779_606_900_000) == datetime(2026, 5, 24, 7, 15, tzinfo=UTC)
    assert to_optional_float("1.25") == 1.25
    assert to_optional_int("7") == 7
    assert to_optional_str(123) == "123"
    assert looks_like_option_instrument("SOL_USDC-30JUN26-250-C")
    assert not looks_like_option_instrument("BTC-PERPETUAL")
    assert option_currency("SOL_USDC-30JUN26-250-C") == "SOL"
    assert raw_payload_hash({"b": 2, "a": 1}) == raw_payload_hash({"a": 1, "b": 2})
