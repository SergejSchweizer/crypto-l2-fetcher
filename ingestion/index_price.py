"""Index price snapshot normalization for bronze ingestion."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from domain.datasets import INDEX_PRICE_SNAPSHOT_1M

INDEX_PRICE_DATASET_TYPE = INDEX_PRICE_SNAPSHOT_1M.dataset_type
INDEX_PRICE_SCHEMA_VERSION = "v1"
INDEX_PRICE_SOURCE = "rest_get_index_price"


@dataclass(frozen=True, slots=True)
class IndexPriceSnapshotRow:
    """One normalized index price snapshot record."""

    schema_version: str
    dataset_type: str
    exchange: str
    source: str
    index_name: str
    snapshot_time: datetime
    event_time: datetime
    price: float
    ingested_at: datetime
    run_id: str
    raw_payload_hash: str


def utc_run_id() -> str:
    """Create a UTC run identifier for index price bronze writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_time_floor_minute(now: datetime | None = None) -> datetime:
    """Return a UTC snapshot timestamp floored to minute."""

    base = now or datetime.now(UTC)
    utc_base = base.astimezone(UTC)
    return utc_base.replace(second=0, microsecond=0)


def normalize_index_price_snapshot_row(
    *,
    index_name: str,
    price: float,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = INDEX_PRICE_SOURCE,
    schema_version: str = INDEX_PRICE_SCHEMA_VERSION,
) -> IndexPriceSnapshotRow:
    """Create one typed index price snapshot row."""

    payload = {
        "index_name": index_name,
        "price": price,
        "snapshot_time": snapshot_time.isoformat(),
        "source": source,
    }
    return IndexPriceSnapshotRow(
        schema_version=schema_version,
        dataset_type=INDEX_PRICE_DATASET_TYPE,
        exchange="deribit",
        source=source,
        index_name=index_name,
        snapshot_time=snapshot_time,
        event_time=snapshot_time,
        price=float(price),
        ingested_at=ingested_at,
        run_id=run_id,
        raw_payload_hash=_raw_payload_hash(payload),
    )


def _raw_payload_hash(row: Mapping[str, object]) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
