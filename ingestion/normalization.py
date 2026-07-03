"""Shared pure normalization helpers for Bronze ingestion rows."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime


def utc_run_id() -> str:
    """Create a UTC run identifier for Bronze writes."""

    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def snapshot_time_floor_minute(now: datetime | None = None) -> datetime:
    """Return a UTC snapshot time floored to minute."""

    base = now or datetime.now(UTC)
    return base.astimezone(UTC).replace(second=0, microsecond=0)


def timestamp_ms_to_datetime(value: object) -> datetime | None:
    """Convert a Unix millisecond timestamp into UTC datetime when present."""

    if value is None or not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(float(value) / 1000.0, tz=UTC)


def to_optional_float(value: object) -> float | None:
    """Convert nullable scalar values into float using Deribit string semantics."""

    if value is None:
        return None
    return float(str(value))


def to_optional_int(value: object) -> int | None:
    """Convert nullable scalar values into int using Deribit string semantics."""

    if value is None:
        return None
    return int(str(value))


def to_optional_str(value: object) -> str | None:
    """Convert nullable scalar values into strings."""

    if value is None:
        return None
    return str(value)


def looks_like_option_instrument(instrument_name: str) -> bool:
    """Return whether an instrument name resembles Deribit option naming."""

    parts = instrument_name.split("-")
    if len(parts) < 4:
        return False
    return parts[-1] in {"C", "P"} and parts[-2].replace(".", "", 1).isdigit()


def option_currency(instrument_name: str) -> str:
    """Return the logical option currency from a Deribit option instrument name."""

    base = instrument_name.split("-", 1)[0]
    return base.removesuffix("_USDC")


def raw_payload_hash(row: Mapping[str, object] | list[object]) -> str:
    """Return a stable SHA-256 hash for raw Deribit payload replay checks."""

    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
