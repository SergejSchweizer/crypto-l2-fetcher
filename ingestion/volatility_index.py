"""Volatility index candle normalization for Bronze ingestion."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from domain.datasets import VOLATILITY_INDEX_SNAPSHOT_1M

VOLATILITY_INDEX_DATASET_TYPE = VOLATILITY_INDEX_SNAPSHOT_1M.dataset_type
VOLATILITY_INDEX_SCHEMA_VERSION = "v1"
VOLATILITY_INDEX_SOURCE = "rest_get_volatility_index_data"


@dataclass(frozen=True, slots=True)
class VolatilityIndexSnapshotRow:
    """One normalized Deribit volatility-index candle record."""

    schema_version: str
    dataset_type: str
    exchange: str
    source: str
    currency: str
    source_currency: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    resolution: int
    snapshot_time: datetime
    ingested_at: datetime
    run_id: str
    raw_payload_hash: str


def utc_run_id() -> str:
    """Create a UTC run identifier for volatility-index Bronze writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_time_floor_minute(now: datetime | None = None) -> datetime:
    """Return a UTC snapshot time floored to minute."""

    base = now or datetime.now(UTC)
    utc_base = base.astimezone(UTC)
    return utc_base.replace(second=0, microsecond=0)


def overlap_start_timestamp_ms(snapshot_time: datetime, lookback_seconds: int) -> int:
    """Return Unix milliseconds for the volatility-index overlap window start."""

    if lookback_seconds < 0:
        raise ValueError("lookback_seconds must be non-negative")
    return int((snapshot_time.astimezone(UTC) - timedelta(seconds=lookback_seconds)).timestamp() * 1000)


def snapshot_timestamp_ms(snapshot_time: datetime) -> int:
    """Return Unix milliseconds for the volatility-index window end."""

    return int(snapshot_time.astimezone(UTC).timestamp() * 1000)


def normalize_volatility_index_candles(
    candles: list[list[object]],
    *,
    currency: str,
    source_currency: str,
    resolution: int,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = VOLATILITY_INDEX_SOURCE,
    schema_version: str = VOLATILITY_INDEX_SCHEMA_VERSION,
) -> tuple[list[VolatilityIndexSnapshotRow], list[str]]:
    """Normalize Deribit volatility-index candle arrays into typed Bronze rows."""

    rows: list[VolatilityIndexSnapshotRow] = []
    errors: list[str] = []
    for candle in candles:
        if len(candle) < 5:
            errors.append(f"Rejected volatility index candle with {len(candle)} values")
            continue
        timestamp = _timestamp_ms_to_datetime(candle[0])
        if timestamp is None:
            errors.append("Rejected volatility index candle with invalid timestamp")
            continue
        rows.append(
            VolatilityIndexSnapshotRow(
                schema_version=schema_version,
                dataset_type=VOLATILITY_INDEX_DATASET_TYPE,
                exchange="deribit",
                source=source,
                currency=currency.strip().upper(),
                source_currency=source_currency.strip().upper(),
                timestamp=timestamp,
                open=float(str(candle[1])),
                high=float(str(candle[2])),
                low=float(str(candle[3])),
                close=float(str(candle[4])),
                resolution=resolution,
                snapshot_time=snapshot_time,
                ingested_at=ingested_at,
                run_id=run_id,
                raw_payload_hash=_raw_payload_hash(candle),
            )
        )
    return rows, errors


def _timestamp_ms_to_datetime(value: object) -> datetime | None:
    if value is None or not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def _raw_payload_hash(candle: list[object]) -> str:
    encoded = json.dumps(candle, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
