"""Instrument metadata snapshot normalization for bronze ingestion."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime

from domain.datasets import (
    FUTURE_INSTRUMENT_METADATA_SNAPSHOT_DAILY,
    INSTRUMENT_METADATA_SNAPSHOT_DAILY,
)

INSTRUMENT_METADATA_DATASET_TYPE = INSTRUMENT_METADATA_SNAPSHOT_DAILY.dataset_type
FUTURE_INSTRUMENT_METADATA_DATASET_TYPE = FUTURE_INSTRUMENT_METADATA_SNAPSHOT_DAILY.dataset_type
INSTRUMENT_METADATA_SCHEMA_VERSION = "v1"
INSTRUMENT_METADATA_SOURCE = "rest_get_instruments"


@dataclass(frozen=True, slots=True)
class InstrumentMetadataSnapshotRow:
    """One normalized instrument metadata snapshot record."""

    schema_version: str
    dataset_type: str
    exchange: str
    source: str
    snapshot_date: date
    ingested_at: datetime
    run_id: str
    instrument_name: str
    kind: str | None
    base_currency: str | None
    quote_currency: str | None
    counter_currency: str | None
    settlement_currency: str | None
    instrument_type: str | None
    settlement_period: str | None
    price_index: str | None
    state: str | None
    tick_size: float | None
    contract_size: float | None
    min_trade_amount: float | None
    is_active: bool | None
    creation_timestamp: datetime | None
    expiration_timestamp: datetime | None
    option_type: str | None
    strike: float | None
    raw_payload_hash: str


def utc_run_id() -> str:
    """Create a UTC run identifier for metadata bronze writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_date_utc(now: datetime | None = None) -> date:
    """Return current UTC snapshot date."""

    return (now or datetime.now(UTC)).astimezone(UTC).date()


def normalize_instrument_metadata_rows(
    rows: list[dict[str, object]],
    *,
    run_id: str,
    snapshot_date: date,
    ingested_at: datetime,
    source: str = INSTRUMENT_METADATA_SOURCE,
    schema_version: str = INSTRUMENT_METADATA_SCHEMA_VERSION,
) -> tuple[list[InstrumentMetadataSnapshotRow], list[str]]:
    """Normalize raw Deribit instrument rows into typed bronze records."""

    normalized: list[InstrumentMetadataSnapshotRow] = []
    errors: list[str] = []
    for row in rows:
        instrument_name = row.get("instrument_name")
        if not isinstance(instrument_name, str) or not instrument_name:
            errors.append("Rejected instrument metadata row with missing instrument_name")
            continue
        normalized.append(
            InstrumentMetadataSnapshotRow(
                schema_version=schema_version,
                dataset_type=_metadata_dataset_type(row.get("kind")),
                exchange="deribit",
                source=source,
                snapshot_date=snapshot_date,
                ingested_at=ingested_at,
                run_id=run_id,
                instrument_name=instrument_name,
                kind=_to_optional_str(row.get("kind")),
                base_currency=_to_optional_str(row.get("base_currency")),
                quote_currency=_to_optional_str(row.get("quote_currency")),
                counter_currency=_to_optional_str(row.get("counter_currency")),
                settlement_currency=_to_optional_str(row.get("settlement_currency")),
                instrument_type=_to_optional_str(row.get("instrument_type")),
                settlement_period=_to_optional_str(row.get("settlement_period")),
                price_index=_to_optional_str(row.get("price_index")),
                state=_to_optional_str(row.get("state")),
                tick_size=_to_optional_float(row.get("tick_size")),
                contract_size=_to_optional_float(row.get("contract_size")),
                min_trade_amount=_to_optional_float(row.get("min_trade_amount")),
                is_active=_to_optional_bool(row.get("is_active")),
                creation_timestamp=_timestamp_ms_to_datetime(row.get("creation_timestamp")),
                expiration_timestamp=_timestamp_ms_to_datetime(row.get("expiration_timestamp")),
                option_type=_to_optional_str(row.get("option_type")),
                strike=_to_optional_float(row.get("strike")),
                raw_payload_hash=_raw_payload_hash(row),
            )
        )
    return normalized, errors


def _metadata_dataset_type(kind: object) -> str:
    if _to_optional_str(kind) == "future":
        return FUTURE_INSTRUMENT_METADATA_DATASET_TYPE
    return INSTRUMENT_METADATA_DATASET_TYPE


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


def _to_optional_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _raw_payload_hash(row: dict[str, object]) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
