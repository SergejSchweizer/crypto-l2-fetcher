"""Prediction-universe selection for per-instrument option ticker collection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

DEFAULT_TARGET_TENORS_DAYS = (1, 2, 7, 14, 30, 60)
OPTION_L2_TARGET_DTE_DAYS = (1, 3, 7, 14, 30, 60, 90, 180, 365)
DEFAULT_TARGET_MONEYNESS = (0.9, 0.95, 1.0, 1.05, 1.1)
OPTION_TYPES = ("C", "P")


@dataclass(frozen=True, slots=True)
class OptionTickerCandidate:
    """One option summary row ranked for IV/RV ticker collection."""

    instrument_name: str
    expiry: date
    tenor_days: int
    strike: float
    option_type: str
    moneyness: float
    liquidity_score: float


def select_option_ticker_prediction_universe(
    rows: list[dict[str, object]],
    *,
    max_instruments: int,
    today: date | None = None,
    target_tenors_days: tuple[int, ...] | None = None,
    require_exact_target_tenor: bool = False,
    allow_liquidity_fallback: bool = True,
) -> list[str]:
    """Select a liquid, tenor/moneyness-diverse option ticker universe.

    Target tenor buckets choose the nearest available listed expiry, because
    Deribit does not always list contracts exactly 30 or 60 calendar days out.

    Args:
        rows (list[dict[str, object]]): Raw Deribit book-summary rows for one requested currency.
        max_instruments (int): Maximum instruments to select for the currency.
        today (date | None): UTC date used for deterministic tenor calculations.
        target_tenors_days (tuple[int, ...] | None): DTE buckets used to reserve maturity coverage.
        require_exact_target_tenor (bool): When true, reject candidates outside the target DTE set.
        allow_liquidity_fallback (bool): When true, fill unused capacity with liquid remaining contracts.

    Returns:
        list[str]: Selected instrument names ordered by model-priority buckets.
    """

    if max_instruments <= 0:
        return []
    target_tenors = DEFAULT_TARGET_TENORS_DAYS if target_tenors_days is None else target_tenors_days
    if not target_tenors:
        return []

    reference_date = today or datetime.now(UTC).date()
    candidates = [
        candidate
        for row in rows
        if (candidate := _candidate_from_summary_row(row=row, today=reference_date)) is not None
    ]
    if not candidates:
        return []

    selected: list[OptionTickerCandidate] = []
    used: set[str] = set()
    # Allocate by target moneyness and option type first so small per-currency
    # caps still reserve coverage across all IV/RV forecast tenors.
    for target_moneyness in DEFAULT_TARGET_MONEYNESS:
        for option_type in OPTION_TYPES:
            for target_tenor in target_tenors:
                _append_best_bucket_candidate(
                    candidates=candidates,
                    selected=selected,
                    used=used,
                    target_tenor=target_tenor,
                    target_moneyness=target_moneyness,
                    option_type=option_type,
                    require_exact_target_tenor=require_exact_target_tenor,
                )
                if len(selected) >= max_instruments:
                    return [candidate.instrument_name for candidate in selected]

    if len(selected) >= max_instruments:
        return [candidate.instrument_name for candidate in selected]
    if not allow_liquidity_fallback:
        return [candidate.instrument_name for candidate in selected]

    # If the full bucket grid did not fill the cap, use the most liquid
    # remaining contracts as fallback without disturbing already-covered tenors.
    for target_tenor in target_tenors:
        for target_moneyness in DEFAULT_TARGET_MONEYNESS:
            for option_type in OPTION_TYPES:
                _append_best_bucket_candidate(
                    candidates=candidates,
                    selected=selected,
                    used=used,
                    target_tenor=target_tenor,
                    target_moneyness=target_moneyness,
                    option_type=option_type,
                    require_exact_target_tenor=require_exact_target_tenor,
                )
                if len(selected) >= max_instruments:
                    return [candidate.instrument_name for candidate in selected]

    if len(selected) >= max_instruments:
        return [candidate.instrument_name for candidate in selected]

    remaining = sorted(
        (candidate for candidate in candidates if candidate.instrument_name not in used),
        key=lambda candidate: (-candidate.liquidity_score, candidate.expiry, candidate.strike, candidate.option_type),
    )
    selected.extend(remaining[: max_instruments - len(selected)])
    return [candidate.instrument_name for candidate in selected]


def _append_best_bucket_candidate(
    *,
    candidates: list[OptionTickerCandidate],
    selected: list[OptionTickerCandidate],
    used: set[str],
    target_tenor: int,
    target_moneyness: float,
    option_type: str,
    require_exact_target_tenor: bool,
) -> None:
    candidate = _best_bucket_candidate(
        candidates=candidates,
        used=used,
        target_tenor=target_tenor,
        target_moneyness=target_moneyness,
        option_type=option_type,
        require_exact_target_tenor=require_exact_target_tenor,
    )
    if candidate is None:
        return
    selected.append(candidate)
    used.add(candidate.instrument_name)


def _best_bucket_candidate(
    *,
    candidates: list[OptionTickerCandidate],
    used: set[str],
    target_tenor: int,
    target_moneyness: float,
    option_type: str,
    require_exact_target_tenor: bool,
) -> OptionTickerCandidate | None:
    bucket_candidates = [
        candidate
        for candidate in candidates
        if candidate.option_type == option_type and candidate.instrument_name not in used
        if not require_exact_target_tenor or candidate.tenor_days == target_tenor
    ]
    if not bucket_candidates:
        return None
    return min(
        bucket_candidates,
        key=lambda candidate: (
            abs(candidate.tenor_days - target_tenor),
            abs(candidate.moneyness - target_moneyness),
            -candidate.liquidity_score,
            candidate.expiry,
            candidate.strike,
        ),
    )


def _candidate_from_summary_row(row: dict[str, object], today: date) -> OptionTickerCandidate | None:
    instrument_name = row.get("instrument_name")
    if not isinstance(instrument_name, str):
        return None
    parsed = _parse_deribit_option_instrument(instrument_name)
    if parsed is None:
        return None
    expiry, strike, option_type = parsed
    tenor_days = (expiry - today).days
    if tenor_days < 0:
        return None
    underlying_price = _to_optional_float(row.get("underlying_price"))
    if underlying_price is None or underlying_price <= 0:
        return None
    bid_price = _to_optional_float(row.get("bid_price"))
    ask_price = _to_optional_float(row.get("ask_price"))
    mark_price = _to_optional_float(row.get("mark_price"))
    if not _has_usable_quote(bid_price=bid_price, ask_price=ask_price, mark_price=mark_price):
        return None
    return OptionTickerCandidate(
        instrument_name=instrument_name.upper(),
        expiry=expiry,
        tenor_days=tenor_days,
        strike=strike,
        option_type=option_type,
        moneyness=strike / underlying_price,
        liquidity_score=_liquidity_score(row),
    )


def _parse_deribit_option_instrument(instrument_name: str) -> tuple[date, float, str] | None:
    parts = instrument_name.upper().split("-")
    if len(parts) < 4:
        return None
    option_type = parts[-1]
    if option_type not in OPTION_TYPES:
        return None
    try:
        expiry = datetime.strptime(parts[-3], "%d%b%y").date()  # noqa: DTZ007
        strike = float(parts[-2])
    except ValueError:
        return None
    return expiry, strike, option_type


def _has_usable_quote(*, bid_price: float | None, ask_price: float | None, mark_price: float | None) -> bool:
    if ask_price is not None and ask_price > 0 and (bid_price is None or bid_price >= 0):
        return True
    return mark_price is not None and mark_price > 0


def _liquidity_score(row: dict[str, object]) -> float:
    open_interest = _to_optional_float(row.get("open_interest")) or 0.0
    volume = _to_optional_float(row.get("volume")) or 0.0
    volume_usd = _to_optional_float(row.get("volume_usd")) or 0.0
    bid_price = _to_optional_float(row.get("bid_price"))
    ask_price = _to_optional_float(row.get("ask_price"))
    quote_bonus = 1.0 if bid_price is not None and ask_price is not None and ask_price > 0 else 0.0
    spread_penalty = _spread_penalty(bid_price=bid_price, ask_price=ask_price)
    return open_interest + volume + (volume_usd / 10_000.0) + quote_bonus - spread_penalty


def _spread_penalty(*, bid_price: float | None, ask_price: float | None) -> float:
    if bid_price is None or ask_price is None or ask_price <= 0:
        return 0.0
    mid_price = (bid_price + ask_price) / 2.0
    if mid_price <= 0:
        return 0.0
    return max(0.0, (ask_price - bid_price) / mid_price)


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(str(value))
