"""Tests for spot ingestion parsing and validation."""

from __future__ import annotations

from datetime import timezone

import pytest

from ingestion.spot import fetch_binance_spot_candles, normalize_timeframe, parse_binance_kline



def test_parse_binance_kline_maps_fields() -> None:
    row = [
        1714478400000,
        "64000.0",
        "64200.0",
        "63850.0",
        "64100.0",
        "120.5",
        1714481999999,
        "7720000.0",
        2300,
        "60.0",
        "3850000.0",
        "0",
    ]

    candle = parse_binance_kline("BTCUSDT", "1h", row)

    assert candle.symbol == "BTCUSDT"
    assert candle.interval == "1h"
    assert candle.open_time.tzinfo == timezone.utc
    assert candle.close_price == pytest.approx(64100.0)
    assert candle.volume == pytest.approx(120.5)
    assert candle.trade_count == 2300



def test_fetch_binance_spot_candles_rejects_invalid_limit() -> None:
    with pytest.raises(ValueError):
        fetch_binance_spot_candles("BTCUSDT", limit=0)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("M1", "1m"),
        ("m5", "5m"),
        ("H1", "1h"),
        ("D1", "1d"),
        ("W1", "1w"),
        ("MN1", "1M"),
        ("1M", "1M"),
    ],
)
def test_normalize_timeframe_aliases(value: str, expected: str) -> None:
    assert normalize_timeframe(value) == expected


def test_normalize_timeframe_rejects_unknown_value() -> None:
    with pytest.raises(ValueError):
        normalize_timeframe("M2")
