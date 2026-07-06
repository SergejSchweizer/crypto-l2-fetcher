"""Dataset contracts shared by ingestion, logging, and documentation checks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DatasetContract:
    """Stable contract for one Bronze dataset.

    Attributes:
        dataset_type (str): Canonical Bronze ``dataset_type`` partition value.
        layer (str): Lake layer that owns the dataset.
        log_stem (str): Logfile stem used below the shared ``.logs`` directory.
        partition_columns (tuple[str, ...]): Ordered semantic partition columns for lake layout.
        source (str): Canonical Deribit REST source name.
        description (str): Short operational description for docs and tests.
    """

    dataset_type: str
    layer: str
    log_stem: str
    partition_columns: tuple[str, ...]
    source: str
    description: str


PERPS_L2_SNAPSHOT_1M = DatasetContract(
    dataset_type="perps_l2_snapshot_1m",
    layer="bronze",
    log_stem="perps_l2_snapshot_1m",
    partition_columns=("exchange", "instrument_type", "symbol", "depth", "source", "event_time"),
    source="rest_order_book",
    description="Raw perpetual order-book snapshots.",
)
OPTIONS_TICKER_SNAPSHOT_1M = DatasetContract(
    dataset_type="options_ticker_snapshot_1m",
    layer="bronze",
    log_stem="options_ticker_snapshot_1m",
    partition_columns=("exchange", "currency", "instrument_name", "source", "snapshot_time"),
    source="rest_get_book_summary_by_currency",
    description="Broad option-chain summary rows.",
)
OPTION_INSTRUMENT_TICKER_SNAPSHOT_1M = DatasetContract(
    dataset_type="options_instrument_ticker_snapshot_1m",
    layer="bronze",
    log_stem="options_instrument_ticker_snapshot_1m",
    partition_columns=("exchange", "instrument_name", "source", "snapshot_time"),
    source="rest_ticker",
    description="Selected per-option ticker rows with IV and Greeks.",
)
OPTIONS_L2_SNAPSHOT_1M = DatasetContract(
    dataset_type="options_l2_snapshot_1m",
    layer="bronze",
    log_stem="options_l2_snapshot_1m",
    partition_columns=("exchange", "symbol", "source", "depth", "event_time"),
    source="rest_order_book",
    description="Selected per-option bid/ask depth and top-of-book IV context.",
)
INSTRUMENT_METADATA_SNAPSHOT_DAILY = DatasetContract(
    dataset_type="instrument_metadata_snapshot_daily",
    layer="bronze",
    log_stem="instrument_metadata_snapshot_daily",
    partition_columns=("exchange", "instrument_name", "snapshot_date"),
    source="rest_get_instruments",
    description="Active option instrument metadata snapshots.",
)
FUTURE_INSTRUMENT_METADATA_SNAPSHOT_DAILY = DatasetContract(
    dataset_type="futures_instrument_metadata_snapshot_daily",
    layer="bronze",
    log_stem="futures_instrument_metadata_snapshot_daily",
    partition_columns=("exchange", "instrument_name", "snapshot_date"),
    source="rest_get_instruments",
    description="Active future and perpetual instrument metadata snapshots.",
)
INDEX_PRICE_SNAPSHOT_1M = DatasetContract(
    dataset_type="index_price_snapshot_1m",
    layer="bronze",
    log_stem="index_price_snapshot_1m",
    partition_columns=("exchange", "index_name", "event_time"),
    source="rest_get_index_price",
    description="Raw Deribit index-price observations.",
)
VOLATILITY_INDEX_SNAPSHOT_1M = DatasetContract(
    dataset_type="volatility_index_snapshot_1m",
    layer="bronze",
    log_stem="volatility_index_snapshot_1m",
    partition_columns=("exchange", "currency", "source", "event_time"),
    source="rest_get_volatility_index_data",
    description="Deribit volatility-index candles.",
)
FUTURES_SUMMARY_SNAPSHOT_1M = DatasetContract(
    dataset_type="futures_summary_snapshot_1m",
    layer="bronze",
    log_stem="futures_summary_snapshot_1m",
    partition_columns=("exchange", "instrument_name", "source", "snapshot_time"),
    source="rest_get_book_summary_by_currency",
    description="Dated futures and perpetual curve summary rows.",
)
RECENT_TRADE_SNAPSHOT_1M = DatasetContract(
    dataset_type="recent_trade_snapshot_1m",
    layer="bronze",
    log_stem="recent_trade_snapshot_1m",
    partition_columns=("exchange", "instrument_name", "trade_id"),
    source="rest_get_last_trades_by_currency",
    description="Recent trade tape for options, futures, and perpetuals.",
)

BRONZE_DATASET_CONTRACTS: tuple[DatasetContract, ...] = (
    PERPS_L2_SNAPSHOT_1M,
    OPTIONS_TICKER_SNAPSHOT_1M,
    OPTION_INSTRUMENT_TICKER_SNAPSHOT_1M,
    OPTIONS_L2_SNAPSHOT_1M,
    INSTRUMENT_METADATA_SNAPSHOT_DAILY,
    FUTURE_INSTRUMENT_METADATA_SNAPSHOT_DAILY,
    INDEX_PRICE_SNAPSHOT_1M,
    VOLATILITY_INDEX_SNAPSHOT_1M,
    FUTURES_SUMMARY_SNAPSHOT_1M,
    RECENT_TRADE_SNAPSHOT_1M,
)

BRONZE_DATASET_TYPES = frozenset(contract.dataset_type for contract in BRONZE_DATASET_CONTRACTS)


def bronze_contract_by_dataset_type(dataset_type: str) -> DatasetContract:
    """Return the Bronze dataset contract for a canonical ``dataset_type``.

    Args:
        dataset_type (str): Canonical dataset type string.

    Returns:
        DatasetContract: Matching Bronze dataset contract.

    Raises:
        KeyError: If ``dataset_type`` is not registered.
    """

    for contract in BRONZE_DATASET_CONTRACTS:
        if contract.dataset_type == dataset_type:
            return contract
    raise KeyError(f"Unknown Bronze dataset_type: {dataset_type}")
