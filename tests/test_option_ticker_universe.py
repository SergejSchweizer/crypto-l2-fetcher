"""Tests for option ticker prediction-universe selection."""

from __future__ import annotations

from datetime import date, datetime

from ingestion.option_ticker_universe import (
    OPTION_L2_TARGET_DTE_DAYS,
    select_option_ticker_prediction_universe,
)


def _summary_row(
    instrument_name: str,
    *,
    underlying_price: float = 100.0,
    bid_price: float = 0.1,
    ask_price: float = 0.11,
    open_interest: float = 10.0,
    volume: float = 1.0,
) -> dict[str, object]:
    return {
        "instrument_name": instrument_name,
        "underlying_price": underlying_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "open_interest": open_interest,
        "volume": volume,
    }


def test_select_option_ticker_prediction_universe_prefers_surface_buckets() -> None:
    """Verify selection spans option type and moneyness buckets before liquidity fallback."""

    rows = [
        _summary_row("BTC-13JUN26-90000-C", underlying_price=100_000),
        _summary_row("BTC-13JUN26-90000-P", underlying_price=100_000),
        _summary_row("BTC-13JUN26-100000-C", underlying_price=100_000),
        _summary_row("BTC-13JUN26-100000-P", underlying_price=100_000),
        _summary_row("BTC-13JUN26-110000-C", underlying_price=100_000),
        _summary_row("BTC-13JUN26-110000-P", underlying_price=100_000),
    ]

    selected = select_option_ticker_prediction_universe(
        rows,
        max_instruments=4,
        today=date(2026, 6, 12),
    )

    assert selected == [
        "BTC-13JUN26-90000-C",
        "BTC-13JUN26-100000-C",
        "BTC-13JUN26-110000-C",
        "BTC-13JUN26-90000-P",
    ]


def test_select_option_ticker_prediction_universe_reserves_target_tenors() -> None:
    """Verify small caps still cover the IV/RV target tenor set."""

    rows = [
        _summary_row("BTC-13JUN26-100000-C"),
        _summary_row("BTC-14JUN26-100000-C"),
        _summary_row("BTC-19JUN26-100000-C"),
        _summary_row("BTC-26JUN26-100000-C"),
        _summary_row("BTC-12JUL26-100000-C"),
        _summary_row("BTC-11AUG26-100000-C"),
        _summary_row("BTC-13JUN26-100000-P"),
        _summary_row("BTC-14JUN26-100000-P"),
        _summary_row("BTC-19JUN26-100000-P"),
        _summary_row("BTC-26JUN26-100000-P"),
        _summary_row("BTC-12JUL26-100000-P"),
        _summary_row("BTC-11AUG26-100000-P"),
    ]

    selected = select_option_ticker_prediction_universe(
        rows,
        max_instruments=12,
        today=date(2026, 6, 12),
    )

    selected_tenors = sorted(
        {
            (datetime.strptime(instrument_name.split("-")[1], "%d%b%y").date() - date(2026, 6, 12)).days
            for instrument_name in selected
        }
    )

    assert selected_tenors == [1, 2, 7, 14, 30, 60]


def test_select_option_ticker_prediction_universe_supports_option_l2_target_dtes() -> None:
    """Verify option L2 can reserve the configured DTE target maturities."""

    rows = [
        _summary_row("BTC-13JUN26-100000-C"),
        _summary_row("BTC-15JUN26-100000-C"),
        _summary_row("BTC-19JUN26-100000-C"),
        _summary_row("BTC-26JUN26-100000-C"),
        _summary_row("BTC-12JUL26-100000-C"),
        _summary_row("BTC-11AUG26-100000-C"),
        _summary_row("BTC-10SEP26-100000-C"),
        _summary_row("BTC-09DEC26-100000-C"),
        _summary_row("BTC-12JUN27-100000-C"),
    ]

    selected = select_option_ticker_prediction_universe(
        rows,
        max_instruments=9,
        today=date(2026, 6, 12),
        target_tenors_days=OPTION_L2_TARGET_DTE_DAYS,
    )

    selected_tenors = sorted(
        {
            (datetime.strptime(instrument_name.split("-")[1], "%d%b%y").date() - date(2026, 6, 12)).days
            for instrument_name in selected
        }
    )

    assert selected_tenors == list(OPTION_L2_TARGET_DTE_DAYS)


def test_select_option_ticker_prediction_universe_can_restrict_to_exact_target_dtes() -> None:
    """Verify exact-DTE selection excludes maturities outside the configured target set."""

    rows = [
        _summary_row("BTC-13JUN26-100000-C"),
        _summary_row("BTC-14JUN26-100000-C"),
        _summary_row("BTC-15JUN26-100000-C"),
        _summary_row("BTC-16JUN26-100000-C"),
    ]

    selected = select_option_ticker_prediction_universe(
        rows,
        max_instruments=10,
        today=date(2026, 6, 12),
        target_tenors_days=(1, 3),
        require_exact_target_tenor=True,
        allow_liquidity_fallback=False,
    )

    assert selected == ["BTC-13JUN26-100000-C", "BTC-15JUN26-100000-C"]


def test_select_option_ticker_prediction_universe_uses_nearest_listed_expiry() -> None:
    """Verify 30D and 60D buckets use nearest listed expiries when exact tenors are absent."""

    rows = [
        _summary_row("BTC-13JUN26-100000-C"),
        _summary_row("BTC-14JUN26-100000-C"),
        _summary_row("BTC-19JUN26-100000-C"),
        _summary_row("BTC-26JUN26-100000-C"),
        _summary_row("BTC-03JUL26-100000-C"),
        _summary_row("BTC-31JUL26-100000-C"),
    ]

    selected = select_option_ticker_prediction_universe(
        rows,
        max_instruments=6,
        today=date(2026, 6, 12),
    )

    selected_tenors = sorted(
        {
            (datetime.strptime(instrument_name.split("-")[1], "%d%b%y").date() - date(2026, 6, 12)).days
            for instrument_name in selected
        }
    )

    assert selected_tenors == [1, 2, 7, 14, 21, 49]


def test_select_option_ticker_prediction_universe_rejects_unusable_quotes() -> None:
    """Verify stale rows without tradable quote or mark are not selected."""

    rows = [
        _summary_row("BTC-13JUN26-100000-C", ask_price=0.0, bid_price=0.0),
        _summary_row("BTC-13JUN26-100000-P", ask_price=0.2, bid_price=0.1),
    ]

    selected = select_option_ticker_prediction_universe(
        rows,
        max_instruments=5,
        today=date(2026, 6, 12),
    )

    assert selected == ["BTC-13JUN26-100000-P"]
