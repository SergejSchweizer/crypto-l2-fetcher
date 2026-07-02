"""Recent trade tape normalization for Bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ingestion import normalization as _normalization
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
    to_optional_int as _to_optional_int,
)
from ingestion.normalization import (
    to_optional_str as _to_optional_str,
)

RECENT_TRADE_DATASET_TYPE = "recent_trade_snapshot_1m"
RECENT_TRADE_SCHEMA_VERSION = "v1"
RECENT_TRADE_SOURCE = "rest_get_last_trades_by_currency"
snapshot_time_floor_minute = _normalization.snapshot_time_floor_minute
utc_run_id = _normalization.utc_run_id


@dataclass(frozen=True, slots=True)
class RecentTradeSnapshotRow:
    """One normalized Deribit public trade tape record."""

    schema_version: str
    dataset_type: str
    exchange: str
    source: str
    requested_currency: str
    source_currency: str
    currency: str
    instrument_name: str
    instrument_type: str
    kind: str
    trade_id: str
    trade_sequence: int | None
    exchange_timestamp: datetime
    snapshot_time: datetime
    ingested_at: datetime
    run_id: str
    price: float | None
    amount: float | None
    direction: str | None
    tick_direction: int | None
    mark_price: float | None
    index_price: float | None
    iv: float | None
    liquidation: str | None
    block_trade_id: str | None
    signed_amount: float | None
    notional: float | None
    raw_payload_hash: str


def overlap_start_timestamp_ms(snapshot_time: datetime, lookback_seconds: int) -> int:
    """Return Unix milliseconds for the overlap-window lower bound.

    Args:
        snapshot_time (datetime): UTC snapshot minute used for the collection run.
        lookback_seconds (int): Number of seconds to look back from snapshot_time.

    Returns:
        int: Unix epoch timestamp in milliseconds.

    Raises:
        ValueError: If lookback_seconds is negative.
    """

    if lookback_seconds < 0:
        raise ValueError("lookback_seconds must be non-negative")
    start_time = snapshot_time.astimezone(UTC) - timedelta(seconds=lookback_seconds)
    return int(start_time.timestamp() * 1000)


def normalize_recent_trade_rows(
    rows: list[dict[str, object]],
    *,
    requested_currency: str,
    source_currency: str,
    kind: str,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str = RECENT_TRADE_SOURCE,
    schema_version: str = RECENT_TRADE_SCHEMA_VERSION,
) -> tuple[list[RecentTradeSnapshotRow], list[str]]:
    """Normalize Deribit public trade rows into typed Bronze rows.

    Args:
        rows (list[dict[str, object]]): Raw Deribit trade rows.
        requested_currency (str): Logical requested currency, such as SOL.
        source_currency (str): Deribit endpoint currency, such as USDC for SOL.
        kind (str): Deribit instrument kind requested from the source.
        run_id (str): Idempotent run identifier for this collection pass.
        snapshot_time (datetime): UTC minute timestamp assigned to the fetch batch.
        ingested_at (datetime): UTC ingestion timestamp.
        source (str): Source identifier written to Bronze rows.
        schema_version (str): Schema version written to Bronze rows.

    Returns:
        tuple[list[RecentTradeSnapshotRow], list[str]]: Normalized rows and rejection messages.
    """

    normalized_rows: list[RecentTradeSnapshotRow] = []
    errors: list[str] = []
    for row in rows:
        normalized = _normalize_recent_trade_row(
            row=row,
            requested_currency=requested_currency,
            source_currency=source_currency,
            kind=kind,
            run_id=run_id,
            snapshot_time=snapshot_time,
            ingested_at=ingested_at,
            source=source,
            schema_version=schema_version,
        )
        if normalized is None:
            errors.append(
                f"Rejected trade row instrument_name={row.get('instrument_name')} trade_id={row.get('trade_id')}"
            )
            continue
        normalized_rows.append(normalized)
    return normalized_rows, errors


def _normalize_recent_trade_row(
    *,
    row: dict[str, object],
    requested_currency: str,
    source_currency: str,
    kind: str,
    run_id: str,
    snapshot_time: datetime,
    ingested_at: datetime,
    source: str,
    schema_version: str,
) -> RecentTradeSnapshotRow | None:
    instrument_name = _to_optional_str(row.get("instrument_name"))
    trade_id = _to_optional_str(row.get("trade_id"))
    exchange_timestamp = _timestamp_ms_to_datetime(row.get("timestamp"))
    if instrument_name is None or trade_id is None or exchange_timestamp is None:
        return None

    price = _to_optional_float(row.get("price"))
    amount = _to_optional_float(row.get("amount"))
    direction = _to_optional_str(row.get("direction"))
    return RecentTradeSnapshotRow(
        schema_version=schema_version,
        dataset_type=RECENT_TRADE_DATASET_TYPE,
        exchange="deribit",
        source=source,
        requested_currency=requested_currency.strip().upper(),
        source_currency=source_currency.strip().upper(),
        currency=_trade_currency(instrument_name),
        instrument_name=instrument_name,
        instrument_type=_instrument_type(instrument_name, kind),
        kind=kind.strip().lower(),
        trade_id=trade_id,
        trade_sequence=_to_optional_int(row.get("trade_seq")),
        exchange_timestamp=exchange_timestamp,
        snapshot_time=snapshot_time,
        ingested_at=ingested_at,
        run_id=run_id,
        price=price,
        amount=amount,
        direction=direction,
        tick_direction=_to_optional_int(row.get("tick_direction")),
        mark_price=_to_optional_float(row.get("mark_price")),
        index_price=_to_optional_float(row.get("index_price")),
        iv=_to_optional_float(row.get("iv")),
        liquidation=_to_optional_str(row.get("liquidation")),
        block_trade_id=_to_optional_str(row.get("block_trade_id")),
        signed_amount=_signed_amount(amount=amount, direction=direction),
        notional=_notional(price=price, amount=amount),
        raw_payload_hash=_raw_payload_hash(row),
    )


def _instrument_type(instrument_name: str, kind: str) -> str:
    normalized_kind = kind.strip().lower()
    if normalized_kind == "option":
        return "option"
    if instrument_name.endswith("-PERPETUAL"):
        return "perp"
    return "future"


def _trade_currency(instrument_name: str) -> str:
    base = instrument_name.split("-", 1)[0]
    return base.removesuffix("_USDC")


def _signed_amount(amount: float | None, direction: str | None) -> float | None:
    if amount is None or direction is None:
        return None
    normalized_direction = direction.strip().lower()
    if normalized_direction == "buy":
        return amount
    if normalized_direction == "sell":
        return -amount
    return None


def _notional(price: float | None, amount: float | None) -> float | None:
    if price is None or amount is None:
        return None
    return price * amount
