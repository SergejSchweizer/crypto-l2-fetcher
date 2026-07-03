"""Option order-book snapshot normalization for Bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import cast

from ingestion import normalization as _normalization
from ingestion.normalization import (
    looks_like_option_instrument as _looks_like_option_instrument,
)
from ingestion.normalization import (
    option_currency as _option_currency,
)
from ingestion.normalization import (
    raw_payload_hash as _raw_payload_hash,
)
from ingestion.normalization import (
    timestamp_ms_to_datetime as _timestamp_ms_to_datetime,
)
from ingestion.normalization import (
    to_optional_float as _to_optional_float,
)
from ingestion.normalization import (
    to_optional_str as _to_optional_str,
)

OPTION_L2_DATASET_TYPE = "options_l2_snapshot_1m"
OPTION_L2_SCHEMA_VERSION = "v1"
OPTION_L2_SOURCE = "rest_get_order_book"
snapshot_time_floor_minute = _normalization.snapshot_time_floor_minute
utc_run_id = _normalization.utc_run_id

BookLevels = list[dict[str, float]]


@dataclass(frozen=True, slots=True)
class OptionL2SnapshotRow:
    """One normalized option order-book snapshot record."""

    schema_version: str
    dataset_type: str
    exchange: str
    source: str
    currency: str
    instrument_name: str
    instrument_type: str
    snapshot_time: datetime
    exchange_timestamp: datetime
    ingested_at: datetime
    run_id: str
    depth: int
    fetch_duration_s: float
    state: str | None
    bids: BookLevels
    asks: BookLevels
    bid_levels: int
    ask_levels: int
    best_bid_price: float | None
    best_ask_price: float | None
    best_bid_amount: float | None
    best_ask_amount: float | None
    mark_price: float | None
    index_price: float | None
    underlying_price: float | None
    underlying_index: str | None
    interest_rate: float | None
    bid_iv: float | None
    ask_iv: float | None
    mark_iv: float | None
    open_interest: float | None
    last_price: float | None
    settlement_price: float | None
    min_price: float | None
    max_price: float | None
    volume: float | None
    volume_usd: float | None
    high: float | None
    low: float | None
    price_change: float | None
    delta: float | None
    gamma: float | None
    theta: float | None
    vega: float | None
    rho: float | None
    raw_payload_hash: str


def normalize_option_l2_snapshot_rows(
    rows: dict[str, dict[str, object]],
    *,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    depth: int,
    fetch_durations_s: dict[str, float],
    source: str = OPTION_L2_SOURCE,
    schema_version: str = OPTION_L2_SCHEMA_VERSION,
) -> tuple[list[OptionL2SnapshotRow], list[str]]:
    """Normalize raw Deribit option order-book payloads into typed Bronze rows."""

    normalized: list[OptionL2SnapshotRow] = []
    errors: list[str] = []
    for requested_instrument, row in rows.items():
        normalized_row = _normalize_option_l2_snapshot_row(
            requested_instrument=requested_instrument,
            row=row,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            depth=depth,
            fetch_duration_s=fetch_durations_s.get(requested_instrument, 0.0),
            source=source,
            schema_version=schema_version,
        )
        if normalized_row is None:
            instrument_name = str(row.get("instrument_name", requested_instrument))
            errors.append(f"Rejected option L2 row instrument_name={instrument_name}")
            continue
        normalized.append(normalized_row)
    return normalized, errors


def _normalize_option_l2_snapshot_row(
    *,
    requested_instrument: str,
    row: dict[str, object],
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    depth: int,
    fetch_duration_s: float,
    source: str,
    schema_version: str,
) -> OptionL2SnapshotRow | None:
    instrument_name = _to_optional_str(row.get("instrument_name")) or requested_instrument.strip().upper()
    if not _looks_like_option_instrument(instrument_name):
        return None
    exchange_timestamp = _timestamp_ms_to_datetime(row.get("timestamp"))
    if exchange_timestamp is None:
        return None

    bids = _book_levels(row.get("bids"))
    asks = _book_levels(row.get("asks"))
    stats = row.get("stats")
    stats_payload = cast(dict[str, object], stats) if isinstance(stats, dict) else {}
    greeks = row.get("greeks")
    greeks_payload = cast(dict[str, object], greeks) if isinstance(greeks, dict) else {}
    return OptionL2SnapshotRow(
        schema_version=schema_version,
        dataset_type=OPTION_L2_DATASET_TYPE,
        exchange="deribit",
        source=source,
        currency=_option_currency(instrument_name),
        instrument_name=instrument_name,
        instrument_type="option",
        snapshot_time=snapshot_time,
        exchange_timestamp=exchange_timestamp,
        ingested_at=ingested_at,
        run_id=run_id,
        depth=depth,
        fetch_duration_s=fetch_duration_s,
        state=_to_optional_str(row.get("state")),
        bids=bids,
        asks=asks,
        bid_levels=len(bids),
        ask_levels=len(asks),
        best_bid_price=_to_optional_float(row.get("best_bid_price")),
        best_ask_price=_to_optional_float(row.get("best_ask_price")),
        best_bid_amount=_to_optional_float(row.get("best_bid_amount")),
        best_ask_amount=_to_optional_float(row.get("best_ask_amount")),
        mark_price=_to_optional_float(row.get("mark_price")),
        index_price=_to_optional_float(row.get("index_price")),
        underlying_price=_to_optional_float(row.get("underlying_price")),
        underlying_index=_to_optional_str(row.get("underlying_index")),
        interest_rate=_to_optional_float(row.get("interest_rate")),
        bid_iv=_to_optional_float(row.get("bid_iv")),
        ask_iv=_to_optional_float(row.get("ask_iv")),
        mark_iv=_to_optional_float(row.get("mark_iv")),
        open_interest=_to_optional_float(row.get("open_interest")),
        last_price=_to_optional_float(row.get("last_price")),
        settlement_price=_to_optional_float(row.get("settlement_price")),
        min_price=_to_optional_float(row.get("min_price")),
        max_price=_to_optional_float(row.get("max_price")),
        volume=_to_optional_float(stats_payload.get("volume")),
        volume_usd=_to_optional_float(stats_payload.get("volume_usd")),
        high=_to_optional_float(stats_payload.get("high")),
        low=_to_optional_float(stats_payload.get("low")),
        price_change=_to_optional_float(stats_payload.get("price_change")),
        delta=_to_optional_float(greeks_payload.get("delta")),
        gamma=_to_optional_float(greeks_payload.get("gamma")),
        theta=_to_optional_float(greeks_payload.get("theta")),
        vega=_to_optional_float(greeks_payload.get("vega")),
        rho=_to_optional_float(greeks_payload.get("rho")),
        raw_payload_hash=_raw_payload_hash(row),
    )


def _book_levels(value: object) -> BookLevels:
    if not isinstance(value, list):
        return []
    levels: BookLevels = []
    for level in value:
        if not isinstance(level, list) or len(level) < 2:
            continue
        levels.append({"price": float(str(level[0])), "amount": float(str(level[1]))})
    return levels
