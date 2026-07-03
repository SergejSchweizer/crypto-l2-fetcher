"""Futures summary snapshot normalization for Bronze ingestion."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime

from domain.datasets import FUTURES_SUMMARY_SNAPSHOT_1M

FUTURES_SUMMARY_DATASET_TYPE = FUTURES_SUMMARY_SNAPSHOT_1M.dataset_type
FUTURES_SUMMARY_SCHEMA_VERSION = "v1"
FUTURES_SUMMARY_SOURCE = "rest_get_book_summary_by_currency"


@dataclass(frozen=True, slots=True)
class FuturesSummarySnapshotRow:
    """One normalized Deribit futures summary snapshot record."""

    schema_version: str
    dataset_type: str
    exchange: str
    source: str
    currency: str
    requested_currency: str
    source_currency: str
    instrument_name: str
    instrument_type: str
    snapshot_time: datetime
    exchange_creation_time: datetime | None
    ingested_at: datetime
    run_id: str
    bid_price: float | None
    ask_price: float | None
    mid_price: float | None
    mark_price: float | None
    last: float | None
    open_interest: float | None
    volume: float | None
    volume_usd: float | None
    high: float | None
    low: float | None
    price_change: float | None
    underlying_price: float | None
    estimated_delivery_price: float | None
    interest_rate: float | None
    raw_payload_hash: str


def utc_run_id() -> str:
    """Create a UTC run identifier for futures summary Bronze writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_time_floor_minute(now: datetime | None = None) -> datetime:
    """Return a UTC snapshot time floored to minute."""

    base = now or datetime.now(UTC)
    utc_base = base.astimezone(UTC)
    return utc_base.replace(second=0, microsecond=0)


def normalize_futures_summary_rows(
    rows: list[dict[str, object]],
    *,
    requested_currency: str,
    source_currency: str,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = FUTURES_SUMMARY_SOURCE,
    schema_version: str = FUTURES_SUMMARY_SCHEMA_VERSION,
) -> tuple[list[FuturesSummarySnapshotRow], list[str]]:
    """Normalize raw Deribit futures summary rows into typed Bronze records."""

    normalized: list[FuturesSummarySnapshotRow] = []
    errors: list[str] = []
    for row in rows:
        instrument_name = _to_optional_str(row.get("instrument_name"))
        if instrument_name is None:
            errors.append("Rejected futures summary row with missing instrument_name")
            continue
        bid_price = _to_optional_float(row.get("bid_price"))
        ask_price = _to_optional_float(row.get("ask_price"))
        normalized.append(
            FuturesSummarySnapshotRow(
                schema_version=schema_version,
                dataset_type=FUTURES_SUMMARY_DATASET_TYPE,
                exchange="deribit",
                source=source,
                currency=_futures_currency(instrument_name),
                requested_currency=requested_currency.strip().upper(),
                source_currency=source_currency.strip().upper(),
                instrument_name=instrument_name,
                instrument_type=_instrument_type(instrument_name),
                snapshot_time=snapshot_time,
                exchange_creation_time=_timestamp_ms_to_datetime(row.get("creation_timestamp")),
                ingested_at=ingested_at,
                run_id=run_id,
                bid_price=bid_price,
                ask_price=ask_price,
                mid_price=_mid_price(bid_price=bid_price, ask_price=ask_price),
                mark_price=_to_optional_float(row.get("mark_price")),
                last=_to_optional_float(row.get("last")),
                open_interest=_to_optional_float(row.get("open_interest")),
                volume=_to_optional_float(row.get("volume")),
                volume_usd=_to_optional_float(row.get("volume_usd")),
                high=_to_optional_float(row.get("high")),
                low=_to_optional_float(row.get("low")),
                price_change=_to_optional_float(row.get("price_change")),
                underlying_price=_to_optional_float(row.get("underlying_price")),
                estimated_delivery_price=_to_optional_float(row.get("estimated_delivery_price")),
                interest_rate=_to_optional_float(row.get("interest_rate")),
                raw_payload_hash=_raw_payload_hash(row),
            )
        )
    return normalized, errors


def _instrument_type(instrument_name: str) -> str:
    if instrument_name.endswith("-PERPETUAL"):
        return "perp"
    return "future"


def _futures_currency(instrument_name: str) -> str:
    base = instrument_name.split("-", 1)[0]
    return base.removesuffix("_USDC")


def _mid_price(bid_price: float | None, ask_price: float | None) -> float | None:
    if bid_price is None or ask_price is None:
        return None
    return (bid_price + ask_price) / 2


def _timestamp_ms_to_datetime(value: object) -> datetime | None:
    if value is None or not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def _to_optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(str(value))


def _to_optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _raw_payload_hash(row: dict[str, object]) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
