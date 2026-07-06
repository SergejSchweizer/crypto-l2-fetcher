"""Tests for per-instrument option ticker normalization."""

from __future__ import annotations

from datetime import UTC, datetime

from ingestion.option_instrument_ticker import normalize_option_instrument_ticker_rows


def test_option_instrument_ticker_normalizes_iv_and_greeks() -> None:
    """Verify Deribit ticker IV and Greeks fields are preserved in Bronze rows."""

    rows, errors = normalize_option_instrument_ticker_rows(
        {
            "BTC-30JUN26-120000-C": {
                "instrument_name": "BTC-30JUN26-120000-C",
                "creation_timestamp": 1_779_606_711_643,
                "timestamp": 1_779_606_712_000,
                "state": "open",
                "best_bid_amount": 4.2,
                "best_ask_amount": 3.7,
                "bid_iv": 54.1,
                "ask_iv": 55.2,
                "mark_iv": 54.8,
                "underlying_price": 76839.1,
                "index_price": 76840.2,
                "interest_rate": 0.03,
                "greeks": {"delta": 0.42, "gamma": 0.01, "theta": -0.2, "vega": 1.3, "rho": 0.5},
                "stats": {"volume": 10, "volume_usd": 1000, "high": 0.12, "low": 0.08, "price_change": 1.5},
            }
        },
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert errors == []
    assert rows[0].dataset_type == "options_instrument_ticker_snapshot_1m"
    assert rows[0].currency == "BTC"
    assert rows[0].bid_iv == 54.1
    assert rows[0].ask_iv == 55.2
    assert rows[0].state == "open"
    assert rows[0].best_bid_amount == 4.2
    assert rows[0].best_ask_amount == 3.7
    assert rows[0].exchange_timestamp == datetime(2026, 5, 24, 7, 11, 52, tzinfo=UTC)
    assert rows[0].index_price == 76840.2
    assert rows[0].delta == 0.42
    assert rows[0].high == 0.12
    assert rows[0].low == 0.08
    assert rows[0].price_change == 1.5
    assert rows[0].volume_usd == 1000.0


def test_option_instrument_ticker_rejects_non_option_name() -> None:
    """Verify non-option instruments are rejected structurally."""

    rows, errors = normalize_option_instrument_ticker_rows(
        {"BTC-PERPETUAL": {"instrument_name": "BTC-PERPETUAL"}},
        run_id="run",
        snapshot_time=datetime(2026, 5, 24, 7, 15, tzinfo=UTC),
        ingested_at=datetime(2026, 5, 24, 7, 15, 1, tzinfo=UTC),
    )

    assert rows == []
    assert len(errors) == 1
