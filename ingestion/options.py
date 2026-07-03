"""Option ticker snapshot normalization for bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from ingestion import normalization as _normalization
from ingestion.normalization import (
    looks_like_option_instrument as _looks_like_option_instrument,
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
from sources.deribit_options import DERIBIT_OPTIONS_SOURCE

OPTIONS_TICKER_DATASET_TYPE = "options_ticker_snapshot_1m"
OPTION_TICKER_DATASET_TYPE = OPTIONS_TICKER_DATASET_TYPE
OPTION_TICKER_SCHEMA_VERSION = "v1"
OPTION_TICKER_SOURCE = "rest_get_book_summary_by_currency"
snapshot_time_floor_minute = _normalization.snapshot_time_floor_minute
utc_run_id = _normalization.utc_run_id


@dataclass(frozen=True, slots=True)
class OptionTickerSnapshotRow:
    """One normalized option ticker snapshot record for bronze."""

    exchange: str
    dataset_type: str
    source: str
    currency: str
    requested_currency: str
    source_currency: str
    instrument_name: str
    base_currency: str | None
    quote_currency: str | None
    instrument_type: str
    snapshot_time: datetime
    exchange_creation_time: datetime | None
    ingested_at: datetime
    run_id: str
    bid_price: float | None
    ask_price: float | None
    mid_price: float | None
    mark_price: float | None
    mark_iv: float | None
    underlying_price: float | None
    underlying_index: str | None
    interest_rate: float | None
    open_interest: float | None
    volume: float | None
    volume_usd: float | None
    high: float | None
    low: float | None
    last: float | None
    price_change: float | None
    raw_payload_hash: str
    schema_version: str


def normalize_options_ticker_rows(
    rows: list[dict[str, object]],
    *,
    requested_currency: str,
    source_currency: str,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = OPTION_TICKER_SOURCE,
    schema_version: str = OPTION_TICKER_SCHEMA_VERSION,
) -> tuple[list[OptionTickerSnapshotRow], list[str]]:
    """Normalize raw Deribit summary rows into typed bronze records."""

    normalized: list[OptionTickerSnapshotRow] = []
    errors: list[str] = []
    for row in rows:
        normalized_row = _normalize_options_ticker_row(
            row=row,
            requested_currency=requested_currency,
            source_currency=source_currency,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            source=source,
            schema_version=schema_version,
        )
        if normalized_row is None:
            instrument_name = str(row.get("instrument_name", "<missing>"))
            errors.append(
                f"Rejected option row requested_currency={requested_currency} instrument_name={instrument_name}"
            )
            continue
        normalized.append(normalized_row)
    return normalized, errors


def _normalize_options_ticker_row(
    *,
    row: dict[str, object],
    requested_currency: str,
    source_currency: str,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str,
    schema_version: str,
) -> OptionTickerSnapshotRow | None:
    instrument_name = row.get("instrument_name")
    if not isinstance(instrument_name, str) or not _looks_like_option_instrument(instrument_name):
        return None
    if requested_currency == "SOL" and not instrument_name.startswith("SOL_USDC-"):
        return None
    if requested_currency in {"BTC", "ETH"} and not instrument_name.startswith(f"{requested_currency}-"):
        return None

    return OptionTickerSnapshotRow(
        exchange="deribit",
        dataset_type=OPTIONS_TICKER_DATASET_TYPE,
        source=source,
        currency=requested_currency,
        requested_currency=requested_currency,
        source_currency=source_currency,
        instrument_name=instrument_name,
        base_currency=_to_optional_str(row.get("base_currency")),
        quote_currency=_to_optional_str(row.get("quote_currency")),
        instrument_type="option",
        snapshot_time=snapshot_time,
        exchange_creation_time=_timestamp_ms_to_datetime(row.get("creation_timestamp")),
        ingested_at=ingested_at,
        run_id=run_id,
        bid_price=_to_optional_float(row.get("bid_price")),
        ask_price=_to_optional_float(row.get("ask_price")),
        mid_price=_to_optional_float(row.get("mid_price")),
        mark_price=_to_optional_float(row.get("mark_price")),
        mark_iv=_to_optional_float(row.get("mark_iv")),
        underlying_price=_to_optional_float(row.get("underlying_price")),
        underlying_index=_to_optional_str(row.get("underlying_index")),
        interest_rate=_to_optional_float(row.get("interest_rate")),
        open_interest=_to_optional_float(row.get("open_interest")),
        volume=_to_optional_float(row.get("volume")),
        volume_usd=_to_optional_float(row.get("volume_usd")),
        high=_to_optional_float(row.get("high")),
        low=_to_optional_float(row.get("low")),
        last=_to_optional_float(row.get("last")),
        price_change=_to_optional_float(row.get("price_change")),
        raw_payload_hash=_raw_payload_hash(row),
        schema_version=schema_version,
    )


def source_endpoint_name() -> str:
    """Return the external source endpoint identifier."""

    return DERIBIT_OPTIONS_SOURCE


def normalize_option_ticker_rows(
    rows: list[dict[str, object]],
    *,
    requested_currency: str,
    source_currency: str,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = OPTION_TICKER_SOURCE,
    schema_version: str = OPTION_TICKER_SCHEMA_VERSION,
) -> tuple[list[OptionTickerSnapshotRow], list[str]]:
    """Backward-compatible alias for options ticker row normalization."""

    return normalize_options_ticker_rows(
        rows,
        requested_currency=requested_currency,
        source_currency=source_currency,
        run_id=run_id,
        snapshot_time=snapshot_time,
        ingested_at=ingested_at,
        source=source,
        schema_version=schema_version,
    )
